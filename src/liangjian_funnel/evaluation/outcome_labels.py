"""Deterministic forward-outcome labels for the A1--A4 funnel.

This module is an observation layer only.  It does not call a model, mutate a
research decision, create a plan, or connect to a broker.  A decision is
written once through :class:`~liangjian_funnel.runtime.state.RuntimeStore`;
later runs may append a forward measurement, but may never rewrite the
decision identity or its source hashes.

Price inputs use the project's ``RAW_PLUS_EXPLICIT_FACTOR`` convention:
``close/high/low`` are raw (unadjusted) values and an optional
``adjust_factor`` is applied explicitly.  A source marked as adjusted without
an explicit factor is rejected from the calculation rather than silently
treated as raw data.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..runtime.state import RuntimeStore


OUTCOME_LABEL_SCHEMA_VERSION = "liangjian-outcome-labels/1.0.0"
FORWARD_WINDOWS = (1, 3, 5, 10)
BASELINE_SAMPLE_SIZE = 50
BASELINE_INSUFFICIENT = "INSUFFICIENT_SAMPLE"
_DATE_FIELDS = ("trade_date", "session_date", "bar_date", "date")
_TIMESTAMP_FIELDS = ("bar_timestamp", "timestamp", "bar_end", "datetime", "time")
_CLOSE_FIELDS = ("raw_close", "close", "close_price", "price")
_HIGH_FIELDS = ("raw_high", "high", "high_price")
_LOW_FIELDS = ("raw_low", "low", "low_price")
_FACTOR_FIELDS = (
    "adjust_factor",
    "adj_factor",
    "adjustment_factor",
    "factor",
    "cum_factor",
    "cumulative_factor",
)
_INDUSTRY_FIELDS = (
    "industry",
    "industry_code",
    "industry_name",
    "ths_industry",
    "sw_industry",
    "sector",
)
_MARKET_CAP_FIELDS = ("market_cap", "market_value", "total_market_cap", "市值")
_VOLATILITY_FIELDS = (
    "volatility",
    "volatility_20d",
    "volatility_annualized",
    "vol_20d",
)
_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


class OutcomeLabelError(ValueError):
    """A malformed offline outcome-label input."""

    def __init__(self, reason_code: str, message: str | None = None) -> None:
        self.reason_code = reason_code
        super().__init__(message or reason_code)


class PriceSourceContractError(OutcomeLabelError):
    """A price source cannot prove raw prices plus explicit factors."""

    pass


class ConditionalBaselineResult(dict[str, Any]):
    """JSON-ready baseline result with a convenient numeric ``value`` view."""

    @property
    def value(self) -> float | None:
        raw = self.get("benchmark_return_5d")
        return float(raw) if raw is not None else None


@dataclass(frozen=True, slots=True)
class _Observation:
    symbol: str
    trade_date: date
    close: float
    high: float | None
    low: float | None
    adjust_factor: float
    context: Mapping[str, Any]
    tradable: bool


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _symbol(value: Any) -> str:
    result = _text(value).upper()
    if not result:
        raise OutcomeLabelError("OUTCOME_SYMBOL_MISSING")
    return result


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _as_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return (
            value.astimezone(_MARKET_TIMEZONE).date()
            if value.tzinfo is not None and value.utcoffset() is not None
            else value.date()
        )
    if isinstance(value, date):
        return value
    raw = _text(value)
    if not raw:
        raise OutcomeLabelError("OUTCOME_TRADE_DATE_MISSING", f"{field} is required")
    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        if "T" in normalized or " " in normalized:
            parsed = datetime.fromisoformat(normalized)
            return (
                parsed.astimezone(_MARKET_TIMEZONE).date()
                if parsed.tzinfo is not None and parsed.utcoffset() is not None
                else parsed.date()
            )
        return date.fromisoformat(normalized[:10])
    except ValueError as exc:
        raise OutcomeLabelError("OUTCOME_TRADE_DATE_INVALID", f"invalid {field}") from exc


def _as_cutoff(value: date | datetime | str) -> date:
    return _as_date(value, field="as_of_date")


def _first(raw: Mapping[str, Any], names: Sequence[str], default: Any = None) -> Any:
    for name in names:
        if name in raw and raw[name] is not None:
            return raw[name]
    return default


def _flatten(raw: Mapping[str, Any], *, symbol_hint: str | None = None) -> dict[str, Any]:
    payload = raw.get("payload")
    result: dict[str, Any] = dict(payload) if isinstance(payload, Mapping) else {}
    result.update(dict(raw))
    if symbol_hint and not _text(result.get("symbol") or result.get("code")):
        result["symbol"] = symbol_hint
    return result


def _context_from_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    context: dict[str, Any] = {}
    industry = _first(raw, _INDUSTRY_FIELDS)
    if industry is not None and _text(industry):
        context["industry"] = _text(industry).upper()
    market_cap = _first(raw, _MARKET_CAP_FIELDS)
    if _as_float(market_cap) is not None:
        context["market_cap"] = _as_float(market_cap)
    volatility = _first(raw, _VOLATILITY_FIELDS)
    if _as_float(volatility) is not None:
        context["volatility"] = _as_float(volatility)
    for target, names in (
        ("industry_quintile", ("industry_quintile", "industry_q")),
        ("market_cap_quintile", ("market_cap_quintile", "market_cap_q", "市值分位")),
        ("volatility_quintile", ("volatility_quintile", "volatility_q", "波动率分位")),
    ):
        value = _first(raw, names)
        if value is not None:
            context[target] = value
    # A label may carry its original A1/A2/A3 context in a small metadata
    # object.  Keep only fields useful to the conditional baseline.
    metadata = raw.get("metadata", raw.get("context"))
    if isinstance(metadata, Mapping):
        nested = _context_from_row(dict(metadata))
        context = {**nested, **context}
    return context


def _tradable(raw: Mapping[str, Any]) -> bool:
    for name in ("tradable", "is_tradable", "available"):
        if name in raw:
            value = raw[name]
            if isinstance(value, str):
                return value.strip().lower() not in {"false", "0", "no", "n", "停牌"}
            return bool(value)
    return True


def _observation(raw: Mapping[str, Any], *, symbol_hint: str | None = None) -> _Observation:
    row = _flatten(raw, symbol_hint=symbol_hint)
    symbol = _symbol(row.get("symbol") or row.get("code") or symbol_hint)
    raw_date = _first(row, _DATE_FIELDS + _TIMESTAMP_FIELDS)
    trade_date = _as_date(raw_date, field="trade_date")
    close = _as_float(_first(row, _CLOSE_FIELDS))
    if close is None or close <= 0:
        # An adjusted close is never accepted as a raw close fallback.
        if any(name in row for name in ("adj_close", "adjusted_close", "pre_close")):
            raise PriceSourceContractError("RAW_PLUS_EXPLICIT_FACTOR_REQUIRED")
        raise OutcomeLabelError("OUTCOME_CLOSE_INVALID")
    high = _as_float(_first(row, _HIGH_FIELDS))
    low = _as_float(_first(row, _LOW_FIELDS))
    mode = _text(_first(row, ("adjust", "adjust_mode", "adjustment"), "none")).lower()
    factor_value = _first(row, _FACTOR_FIELDS)
    if factor_value is None:
        if mode not in {"", "none", "raw", "unadjusted", "不复权"}:
            raise PriceSourceContractError("RAW_PLUS_EXPLICIT_FACTOR_REQUIRED")
        factor = 1.0
    else:
        factor = _as_float(factor_value)
        if factor is None or factor <= 0:
            raise PriceSourceContractError("ADJUST_FACTOR_INVALID")
    return _Observation(
        symbol=symbol,
        trade_date=trade_date,
        close=close,
        high=high,
        low=low,
        adjust_factor=factor,
        context=_context_from_row(row),
        tradable=_tradable(row),
    )


def _rows_from_path(
    path: Path,
    *,
    start_date: date | None = None,
    cutoff: date | None = None,
) -> list[Mapping[str, Any]]:
    if not path.is_file():
        raise OutcomeLabelError("PRICE_SOURCE_NOT_FOUND")
    if path.suffix.lower() in {".csv", ".tsv"}:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle, delimiter="\t" if path.suffix.lower() == ".tsv" else ",")]
    if path.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
        with sqlite3.connect(path) as connection:
            connection.row_factory = sqlite3.Row
            tables = {
                str(row[0])
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "daily_bars" not in tables:
                raise OutcomeLabelError("PRICE_SOURCE_TABLE_MISSING")
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(daily_bars)")
            }
            order_fields = ["bar_timestamp", "symbol"]
            # The fact cache keeps append-only provider revisions. Iterating
            # oldest to newest makes the later overwrite in _normalise_source
            # deterministic and leaves the newest fetched revision active.
            order_fields.extend(
                field for field in ("fetched_at", "content_hash") if field in columns
            )
            clauses: list[str] = []
            parameters: list[str] = []
            if start_date is not None:
                clauses.append("bar_timestamp>=?")
                parameters.append(
                    datetime.combine(start_date, time.min, tzinfo=_MARKET_TIMEZONE)
                    .astimezone(timezone.utc)
                    .isoformat()
                )
            if cutoff is not None:
                clauses.append("bar_timestamp<?")
                parameters.append(
                    datetime.combine(cutoff + timedelta(days=1), time.min, tzinfo=_MARKET_TIMEZONE)
                    .astimezone(timezone.utc)
                    .isoformat()
                )
            rows: list[Mapping[str, Any]] = []
            for row in connection.execute(
                "SELECT symbol,bar_timestamp,adjust,payload_json FROM daily_bars"
                + (" WHERE " + " AND ".join(clauses) if clauses else "")
                + " ORDER BY "
                + ",".join(order_fields),
                parameters,
            ):
                item = dict(row)
                try:
                    payload = json.loads(str(item.pop("payload_json") or "{}"))
                except json.JSONDecodeError:
                    payload = {}
                item["payload"] = payload if isinstance(payload, Mapping) else {}
                rows.append(item)
            return rows
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OutcomeLabelError("PRICE_SOURCE_INVALID") from exc
    if isinstance(parsed, Mapping):
        for key in ("rows", "bars", "prices", "data", "universe"):
            value = parsed.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return [item for item in value if isinstance(item, Mapping)]
        # A symbol keyed JSON mapping is handled by _source_rows below.
        return [parsed]
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes, bytearray)):
        return [item for item in parsed if isinstance(item, Mapping)]
    raise OutcomeLabelError("PRICE_SOURCE_INVALID")


def _source_rows(
    price_source: Any,
    *,
    start_date: date | None = None,
    cutoff: date | None = None,
) -> list[Mapping[str, Any]]:
    if isinstance(price_source, (str, Path)):
        return _rows_from_path(
            Path(price_source).expanduser().resolve(),
            start_date=start_date,
            cutoff=cutoff,
        )
    if isinstance(price_source, Mapping):
        # One row is the most common in-memory test/source form.
        if any(name in price_source for name in (*_CLOSE_FIELDS, "payload", "trade_date", "date")):
            return [price_source]
        for key in ("rows", "bars", "prices", "data", "universe"):
            value = price_source.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return [item for item in value if isinstance(item, Mapping)]
        rows: list[Mapping[str, Any]] = []
        for key, value in price_source.items():
            if isinstance(value, Mapping):
                nested = value.get("bars", value.get("rows", value.get("prices")))
                if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
                    inherited = {
                        name: value[name]
                        for name in (*_INDUSTRY_FIELDS, *_MARKET_CAP_FIELDS, *_VOLATILITY_FIELDS,
                                     "industry_quintile", "market_cap_quintile", "volatility_quintile", "tradable")
                        if name in value
                    }
                    for item in nested:
                        if isinstance(item, Mapping):
                            rows.append({**inherited, **dict(item), "symbol": item.get("symbol", key)})
                elif any(name in value for name in (*_CLOSE_FIELDS, "trade_date", "date", "payload")):
                    rows.append({"symbol": key, **dict(value)})
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                for item in value:
                    if isinstance(item, Mapping):
                        rows.append({"symbol": key, **dict(item)})
        return rows
    if isinstance(price_source, Sequence) and not isinstance(price_source, (str, bytes, bytearray)):
        return [item for item in price_source if isinstance(item, Mapping)]
    for method_name in ("iter_rows", "rows", "get_rows", "load_rows"):
        method = getattr(price_source, method_name, None)
        if callable(method):
            value = method()
            if isinstance(value, Iterable):
                return [item for item in value if isinstance(item, Mapping)]
    query = getattr(price_source, "query_daily_bars", None)
    if callable(query):
        value = query(adjust="none")
        if isinstance(value, Iterable):
            return [item for item in value if isinstance(item, Mapping)]
    if callable(price_source):
        value = price_source()
        if isinstance(value, Iterable):
            return [item for item in value if isinstance(item, Mapping)]
    raise OutcomeLabelError("PRICE_SOURCE_UNSUPPORTED")


def _normalise_source(
    price_source: Any,
    *,
    start_date: date | None = None,
    cutoff: date | None = None,
) -> tuple[dict[str, list[_Observation]], list[str]]:
    observations: dict[str, dict[date, _Observation]] = {}
    errors: list[str] = []
    try:
        rows = _source_rows(price_source, start_date=start_date, cutoff=cutoff)
    except OutcomeLabelError as exc:
        return {}, [exc.reason_code]
    for raw in rows:
        try:
            item = _observation(raw)
        except OutcomeLabelError as exc:
            errors.append(exc.reason_code)
            continue
        observations.setdefault(item.symbol, {})[item.trade_date] = item
    return (
        {symbol: [by_date[key] for key in sorted(by_date)] for symbol, by_date in observations.items()},
        sorted(set(errors)),
    )


def _decision_value(raw: Any) -> str:
    if isinstance(raw, bool):
        return "PASSED" if raw else "REJECTED"
    value = _text(raw).upper()
    if value in {"PASS", "PASSED", "ACTIVE", "QUALIFIED", "FOCUS", "CORE", "TRUE"}:
        return "PASSED"
    if value in {"NOT_SENT", "NOT_SENT_TO_LLM", "UNSENT", "NOT_REVIEWED"}:
        return "NOT_SENT_TO_LLM"
    return "REJECTED"


def record_stage_decisions(
    store: RuntimeStore,
    *,
    trade_date: date | datetime | str,
    stage: str,
    decisions: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    snapshot_id: str,
    config_hash: str,
    run_id: str | None = None,
    lane_id: str | None = None,
    decision_run_id: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Persist *all* decisions for one stage, including rejected symbols.

    ``run_id`` and ``lane_id`` are the durable execution identity.  The
    ``decision_run_id`` alias is accepted for callers that use the explicit
    name from the outcome contract.  For legacy/offline callers that omit an
    identity, a snapshot-scoped fallback is used; production integrations
    should always pass both values so same-day reruns remain distinguishable.
    """

    resolved_date = _as_cutoff(trade_date)
    stage_value = _text(stage).upper()
    if stage_value not in {"G0", "A1", "A2", "A3", "A4"}:
        raise OutcomeLabelError("OUTCOME_STAGE_INVALID")
    snapshot_value = _text(snapshot_id)
    config_value = _text(config_hash)
    if not snapshot_value or not config_value:
        raise OutcomeLabelError("OUTCOME_SOURCE_HASH_MISSING")
    resolved_run_id = _text(run_id or decision_run_id) or f"snapshot:{snapshot_value}"
    resolved_lane_id = _text(lane_id) or "default"
    if isinstance(decisions, Mapping):
        iterable: list[Mapping[str, Any]] = []
        for symbol, value in decisions.items():
            if isinstance(value, Mapping):
                iterable.append({"symbol": symbol, **dict(value)})
            else:
                iterable.append({"symbol": symbol, "decision": value})
    elif isinstance(decisions, Iterable) and not isinstance(decisions, (str, bytes, bytearray)):
        iterable = [item for item in decisions if isinstance(item, Mapping)]
    else:
        raise TypeError("stage decisions must be a mapping or iterable of mappings")
    rows: list[dict[str, Any]] = []
    for item in iterable:
        symbol = _symbol(item.get("symbol") or item.get("code"))
        sent_to_llm = item.get("sent_to_llm", item.get("sentToLlm"))
        explicit_decision = item.get("decision")
        if explicit_decision is not None:
            decision = _decision_value(
                explicit_decision
            )
        else:
            status_value = item.get("status", item.get("eligibility", item.get("passed")))
            reason_values = item.get("reason_codes", item.get("reasons", item.get("reason_code", ())))
            if isinstance(reason_values, str):
                reason_names = {reason_values.strip().upper()} if reason_values.strip() else set()
            elif isinstance(reason_values, Sequence) and not isinstance(reason_values, (str, bytes, bytearray)):
                reason_names = {_text(value).upper() for value in reason_values if _text(value)}
            else:
                reason_names = set()
            transport_truncated = bool(
                sent_to_llm is False
                and (
                    "NOT_SENT_TO_LLM" in _text(status_value).upper()
                    or any("NOT_SENT_TO_LLM" in reason for reason in reason_names)
                )
            )
            decision = "NOT_SENT_TO_LLM" if transport_truncated else _decision_value(status_value)
        reasons = item.get("reason_codes", item.get("reasons", item.get("reason_code", ())))
        if "gate_results" in item:
            reasons = {
                "reason_codes": (
                    [reasons]
                    if isinstance(reasons, str) and reasons.strip()
                    else list(reasons)
                    if isinstance(reasons, Sequence) and not isinstance(reasons, (str, bytes, bytearray))
                    else dict(reasons)
                    if isinstance(reasons, Mapping)
                    else []
                ),
                "gate_results": item.get("gate_results"),
                "first_blocking_gate": item.get("first_blocking_gate"),
                "all_failed_gates": item.get("all_failed_gates"),
            }
        score = item.get(
            "score",
            item.get("structural_score", item.get("theme_score", item.get("a3_score"))),
        )
        basis = item.get("selection_basis", item.get("source_basis"))
        metadata = item.get("metadata", item.get("context", {}))
        if not isinstance(metadata, Mapping):
            metadata = {}
        # Keep baseline dimensions and durable lineage only; model response
        # text is never copied into the outcome ledger.
        metadata = {
            **dict(metadata),
            **{
                key: item[key]
                for key in (
                    "industry", "industry_code", "industry_name", "ths_industry", "sw_industry",
                    "market_cap", "market_value", "volatility", "volatility_20d",
                    "market_cap_quintile", "volatility_quintile", "candidate_origin",
                )
                if key in item
            },
        }
        rows.append(
            {
                "run_id": resolved_run_id,
                "lane_id": resolved_lane_id,
                "trade_date": resolved_date.isoformat(),
                "stage": stage_value,
                "symbol": symbol,
                "decision": decision,
                "reason_codes": reasons,
                "selection_basis": basis,
                "score": score,
                "snapshot_id": snapshot_value,
                "config_hash": config_value,
                "metadata": metadata,
            }
        )
    return store.record_outcome_labels(rows)


def _metadata_from_label(label: Mapping[str, Any]) -> dict[str, Any]:
    raw = label.get("metadata_json")
    if isinstance(raw, Mapping):
        return _context_from_row(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {}
        return _context_from_row(parsed) if isinstance(parsed, Mapping) else {}
    return _context_from_row(label)


def _normalise_quintile(value: Any) -> int | None:
    number = _as_float(value)
    if number is None or not number.is_integer():
        return None
    integer = int(number)
    if 1 <= integer <= 5:
        integer -= 1
    return integer if 0 <= integer <= 4 else None


def _assign_quintiles(rows: list[dict[str, Any]], field: str, output: str) -> None:
    explicit: dict[str, int] = {}
    missing: list[dict[str, Any]] = []
    for row in rows:
        parsed = _normalise_quintile(row.get(output))
        if parsed is not None:
            explicit[row["symbol"]] = parsed
        else:
            missing.append(row)
    values = [(_as_float(item.get(field)), item["symbol"], item) for item in missing]
    values = [item for item in values if item[0] is not None]
    values.sort(key=lambda item: (float(item[0]), item[1]))
    count = len(values)
    for index, (_value, symbol, _row) in enumerate(values):
        explicit[symbol] = min(4, (index * 5) // count) if count else 0
    for row in rows:
        row[output] = explicit.get(row["symbol"])


def _forward_metrics(
    observations: Sequence[_Observation],
    *,
    trade_date: date,
    cutoff: date,
) -> dict[str, float | None]:
    ordered = sorted(
        (item for item in observations if trade_date < item.trade_date <= cutoff),
        key=lambda item: item.trade_date,
    )
    entry = next((item for item in observations if item.trade_date == trade_date), None)
    if entry is None:
        return {f"fwd_return_{window}d": None for window in FORWARD_WINDOWS} | {"mfe_5d": None, "mae_5d": None}
    entry_close = entry.close * entry.adjust_factor
    result: dict[str, float | None] = {}
    for window in FORWARD_WINDOWS:
        if len(ordered) < window:
            result[f"fwd_return_{window}d"] = None
            continue
        terminal = ordered[window - 1]
        terminal_close = terminal.close * terminal.adjust_factor
        result[f"fwd_return_{window}d"] = terminal_close / entry_close - 1.0
    if len(ordered) < 5:
        result["mfe_5d"] = None
        result["mae_5d"] = None
    else:
        window = ordered[:5]
        highs = [item.high * item.adjust_factor for item in window if item.high is not None]
        lows = [item.low * item.adjust_factor for item in window if item.low is not None]
        result["mfe_5d"] = max(highs) / entry_close - 1.0 if highs else None
        result["mae_5d"] = min(lows) / entry_close - 1.0 if lows else None
    return result


def _baseline_seed(trade_date: date, symbol: str) -> int:
    digest = hashlib.sha256(f"{trade_date.isoformat()}|{symbol.upper()}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def _baseline_context(row: Mapping[str, Any]) -> tuple[str | None, float | None, float | None, int | None, int | None]:
    context = _context_from_row(row)
    industry = _text(context.get("industry")).upper() or None
    market_cap = _as_float(context.get("market_cap"))
    volatility = _as_float(context.get("volatility"))
    market_q = _normalise_quintile(context.get("market_cap_quintile"))
    vol_q = _normalise_quintile(context.get("volatility_quintile"))
    return industry, market_cap, volatility, market_q, vol_q


def conditional_random_baseline(
    target: Mapping[str, Any],
    universe: Iterable[Mapping[str, Any]],
    *,
    n: int = BASELINE_SAMPLE_SIZE,
) -> ConditionalBaselineResult:
    """Return a deterministic conditional random 5-day baseline.

    ``universe`` must contain one row per symbol for the target trading day,
    with ``fwd_return_5d`` already computed (or raw OHLC rows from which it
    can be computed by the caller).  Candidates need to be in the same
    industry and the same market-cap and volatility quintiles.  The target is
    never sampled as its own control.  Fewer than ``n`` eligible peers is an
    explicit ``INSUFFICIENT_SAMPLE`` result, never a smaller hidden sample.
    """

    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError("baseline sample size must be a positive integer")
    symbol = _symbol(target.get("symbol") or target.get("code"))
    trade_date = _as_date(target.get("trade_date") or target.get("date"), field="trade_date")
    rows: list[dict[str, Any]] = []
    for raw in universe:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        try:
            row_symbol = _symbol(row.get("symbol") or row.get("code"))
            row_date = _as_date(row.get("trade_date") or row.get("date"), field="trade_date")
        except OutcomeLabelError:
            continue
        if row_date != trade_date:
            continue
        row["symbol"] = row_symbol
        rows.append(row)
    target_row = dict(target)
    target_row["symbol"] = symbol
    target_row["trade_date"] = trade_date.isoformat()
    if not any(item.get("symbol") == symbol for item in rows):
        rows.append(target_row)
    # Compute deterministic quintiles separately within this same date.
    _assign_quintiles(rows, "market_cap", "market_cap_quintile")
    _assign_quintiles(rows, "volatility", "volatility_quintile")
    target_item = next(item for item in rows if item["symbol"] == symbol)
    target_industry, _cap, _vol, target_cap_q, target_vol_q = _baseline_context(target_item)
    if target_industry is None or target_cap_q is None or target_vol_q is None:
        return ConditionalBaselineResult(
            status=BASELINE_INSUFFICIENT,
            reason_code="BASELINE_CONTEXT_INCOMPLETE",
            benchmark_return_5d=None,
            sample_size=0,
            required_sample_size=n,
            seed=_baseline_seed(trade_date, symbol),
        )
    peers: list[tuple[str, float]] = []
    for row in rows:
        if row["symbol"] == symbol or not _tradable(row):
            continue
        industry, _cap, _vol, cap_q, vol_q = _baseline_context(row)
        forward = _as_float(row.get("fwd_return_5d"))
        if (
            industry == target_industry
            and cap_q == target_cap_q
            and vol_q == target_vol_q
            and forward is not None
        ):
            peers.append((row["symbol"], forward))
    seed = _baseline_seed(trade_date, symbol)
    if len(peers) < n:
        return ConditionalBaselineResult(
            status=BASELINE_INSUFFICIENT,
            reason_code="BASELINE_PEER_COUNT_BELOW_N",
            benchmark_return_5d=None,
            sample_size=len(peers),
            required_sample_size=n,
            seed=seed,
        )
    peers.sort(key=lambda item: item[0])
    rng = random.Random(seed)
    sampled = rng.sample(peers, n)
    mean = sum(value for _symbol_value, value in sampled) / n
    return ConditionalBaselineResult(
        status="OK",
        reason_code="OK",
        benchmark_return_5d=mean,
        sample_size=n,
        required_sample_size=n,
        seed=seed,
    )


def _build_baseline_universe(
    source: Mapping[str, Sequence[_Observation]],
    labels: Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    cutoff: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    labels_by_symbol = {
        _text(item.get("symbol")).upper(): item
        for item in labels
        if _text(item.get("symbol"))
    }
    for symbol, observations in source.items():
        entry = next((item for item in observations if item.trade_date == trade_date), None)
        if entry is None or not entry.tradable:
            continue
        metrics = _forward_metrics(observations, trade_date=trade_date, cutoff=cutoff)
        row: dict[str, Any] = {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "fwd_return_5d": metrics.get("fwd_return_5d"),
            **dict(entry.context),
            "tradable": entry.tradable,
        }
        label = labels_by_symbol.get(symbol)
        if label is not None:
            row = {**_metadata_from_label(label), **row}
        rows.append(row)
    return rows


def backfill_forward_returns(
    store: RuntimeStore,
    *,
    as_of_date: date | datetime | str,
    price_source: Any,
) -> dict[str, Any]:
    """Backfill all available forward windows without future-data leakage.

    A 10-day label remains ``labeled_at = NULL`` until its 10th subsequent
    trading observation is available.  Shorter windows are still persisted
    as soon as they are available, so repeated daily runs are incremental.
    """

    cutoff = _as_cutoff(as_of_date)
    # The decision row is immutable and a completed label is a historical
    # observation.  Only rows whose 10-day window is still open are eligible
    # for another backfill pass; this also prevents a revised provider payload
    # from rewriting a closed measurement.
    labels = tuple(
        row for row in store.list_outcome_labels(labeled_only=False)
        if not row.get("labeled_at")
    )
    earliest_trade_date = min(
        (_as_date(row.get("trade_date"), field="trade_date") for row in labels),
        default=cutoff,
    )
    source, source_errors = _normalise_source(
        price_source,
        start_date=earliest_trade_date,
        cutoff=cutoff,
    )
    updates: list[dict[str, Any]] = []
    metrics_cache: dict[tuple[str, date], dict[str, float | None]] = {}
    baseline_universe_cache: dict[date, list[dict[str, Any]]] = {}
    counts = {
        "labels_seen": len(labels),
        "labels_updated": 0,
        "fully_labeled": 0,
        "partial_labels": 0,
        "baseline_ok": 0,
        "baseline_insufficient": 0,
    }
    for label in labels:
        symbol = _text(label.get("symbol")).upper()
        trade_date = _as_date(label.get("trade_date"), field="trade_date")
        observations = source.get(symbol, ())
        metric_key = (symbol, trade_date)
        metrics = metrics_cache.get(metric_key)
        if metrics is None:
            metrics = _forward_metrics(observations, trade_date=trade_date, cutoff=cutoff)
            metrics_cache[metric_key] = metrics
        update: dict[str, Any] = {
            "label_id": label.get("label_id"),
            **metrics,
        }
        if not observations or not any(item.trade_date == trade_date for item in observations):
            update["baseline_status"] = "DATA_UNAVAILABLE"
        else:
            target = {
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                **_metadata_from_label(label),
            }
            entry = next(item for item in observations if item.trade_date == trade_date)
            target = {**dict(entry.context), **target}
            target["fwd_return_5d"] = metrics.get("fwd_return_5d")
            if metrics.get("fwd_return_5d") is not None:
                universe = baseline_universe_cache.get(trade_date)
                if universe is None:
                    universe = _build_baseline_universe(
                        source,
                        labels,
                        trade_date=trade_date,
                        cutoff=cutoff,
                    )
                    baseline_universe_cache[trade_date] = universe
                baseline = conditional_random_baseline(target, universe)
                update["baseline_status"] = baseline["status"]
                update["baseline_sample_size"] = baseline["sample_size"]
                if baseline.value is not None:
                    update["benchmark_return_5d"] = baseline.value
                    update["excess_return_5d"] = metrics["fwd_return_5d"] - baseline.value
                    counts["baseline_ok"] += 1
                else:
                    counts["baseline_insufficient"] += 1
            else:
                update["baseline_status"] = "FORWARD_5D_NOT_READY"
        # The state layer ignores null updates, and rejects conflicting
        # non-null values.  ``labeled_at`` is deliberately delayed until 10d.
        if all(metrics.get(f"fwd_return_{window}d") is not None for window in FORWARD_WINDOWS):
            update["labeled_at"] = datetime.now().astimezone().isoformat()
            counts["fully_labeled"] += 1
        elif any(value is not None for value in metrics.values()):
            counts["partial_labels"] += 1
        if any(value is not None for key, value in update.items() if key != "label_id"):
            updates.append(update)
    if updates:
        store.update_outcome_label_metrics(updates)
        counts["labels_updated"] = len(updates)
    return {
        "schema_version": OUTCOME_LABEL_SCHEMA_VERSION,
        "status": "COMPLETED" if labels else "EMPTY",
        "as_of_date": cutoff.isoformat(),
        "source_rows": sum(len(value) for value in source.values()),
        "source_errors": source_errors,
        **counts,
        "network_used": False,
        "models_called": False,
        "runtime_mutation": True,
    }


__all__ = [
    "BASELINE_INSUFFICIENT",
    "BASELINE_SAMPLE_SIZE",
    "ConditionalBaselineResult",
    "FORWARD_WINDOWS",
    "OUTCOME_LABEL_SCHEMA_VERSION",
    "OutcomeLabelError",
    "PriceSourceContractError",
    "backfill_forward_returns",
    "conditional_random_baseline",
    "record_stage_decisions",
]
