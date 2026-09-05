"""Point-in-time A2 market facts that remain separate from price proxies.

Tencent's order-size fund-flow fields are the primary free stock-level source.
Eastmoney remains a bounded fallback and the board-flow source.  Both are
vendor-derived rather than exchange-reported facts.  A missing row is never
replaced with turnover, return, attention, or ordinary OHLCV data.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from threading import local
from typing import Any
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
CAPITAL_FLOW_SCHEMA = "a2-capital-flow/1.0.0"
CAPITAL_FLOW_PROVIDER = "EASTMONEY_CAPITAL_FLOW_RANK"
TENCENT_CAPITAL_FLOW_PROVIDER = "TENCENT_QQ_FINANCE_FUND_FLOW"
PROVIDER_METHOD = "VENDOR_DERIVED"
# A2 facts are point-in-time observations.  These states are intentionally
# narrower than a boolean ``available`` flag: an empty limit-up set is useful
# evidence, while an unavailable provider must never be interpreted as zero.
OBSERVED_VALUE = "OBSERVED_VALUE"
OBSERVED_EMPTY = "OBSERVED_EMPTY"
SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
STALE = "STALE"
MALFORMED = "MALFORMED"
OUTSIDE_SCOPE = "OUTSIDE_SCOPE"

A2_MARKET_FACT_SCHEMA = "a2-market-fact/1.0.0"
BOARD_FLOW_SCHEMA = "a2-board-capital-flow/1.0.0"
BOARD_FLOW_PROVIDER = "EASTMONEY_BOARD_CAPITAL_FLOW"
_A2_FACT_STATES = frozenset({
    OBSERVED_VALUE,
    OBSERVED_EMPTY,
    SOURCE_UNAVAILABLE,
    STALE,
    MALFORMED,
    OUTSIDE_SCOPE,
})
_DATASET_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,80}")
_BOARD_TYPES = frozenset({"industry", "concept", "region"})
_BOARD_PERIODS: Mapping[str, tuple[str, str, str, str | None]] = {
    "today": ("f62", "f184", "f3", "f204"),
    "5d": ("f164", "f165", "f109", "f257"),
    "10d": ("f174", "f175", "f160", None),
}
_BOARD_FS = {"industry": "m:90+t:2", "concept": "m:90+t:3", "region": "m:90+t:1"}
_EASTMONEY_URLS = (
    # The delayed host is the stable equivalent clist contract on the
    # deployment network.  Try it first so a dead TLS path cannot consume the
    # retry budget for every board/window page.
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
)
_EASTMONEY_UNIVERSE = "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2"
_EASTMONEY_PAGE_SIZE = 5000
_EASTMONEY_FIELDS: Mapping[str, tuple[str, str, str, str, str, str, str]] = {
    "today": ("f62", "f12,f14,f62,f184,f66,f69,f72,f75", "f62", "f184", "f72", "f75", "f69"),
    "3d": ("f267", "f12,f14,f267,f268,f269,f270,f271,f272", "f267", "f268", "f271", "f272", "f270"),
    "5d": ("f164", "f12,f14,f164,f165,f166,f167,f168,f169", "f164", "f165", "f168", "f169", "f167"),
    "10d": ("f174", "f12,f14,f174,f175,f176,f177,f178,f179", "f174", "f175", "f178", "f179", "f177"),
}
WINDOWS: tuple[tuple[str, str, float], ...] = (
    ("today", "今日", 0.35),
    ("3d", "3日", 0.25),
    ("5d", "5日", 0.25),
    ("10d", "10日", 0.15),
)
_TENCENT_THREAD_LOCAL = local()


class CapitalFlowError(RuntimeError):
    """Raised for a malformed provider payload, never for an empty market."""


def collect_eastmoney_capital_flow(
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    cache_dir: str | Path,
    fetch_rank: Callable[[str], Any] | None = None,
    now: datetime | None = None,
    minimum_coverage: float = 0.90,
    cache_max_age_seconds: float | None = None,
    allow_historical_recovery: bool = False,
) -> dict[str, Any]:
    """Load one cached trade date or collect the current all-market ranking.

    The upstream ranking does not expose a caller-selected historical date.
    Therefore a historical replay may only use a previously persisted file;
    it must not silently query the current ranking and relabel it as history.
    """

    cutoff = _aware(as_of)
    current = _aware(now or datetime.now(SHANGHAI))
    root = Path(cache_dir)
    cached, cache_state = _load_capital_flow_cache_state(
        root,
        cutoff.date().isoformat(),
        now=current,
        max_age_seconds=cache_max_age_seconds,
    )
    if cached is not None:
        return cached
    historical_recovery = cutoff.date() != current.date()
    if historical_recovery and not allow_historical_recovery:
        return unavailable_capital_flow_snapshot(
            as_of=cutoff,
            reason_code=(
                "HISTORICAL_CAPITAL_FLOW_CACHE_STALE"
                if cache_state == STALE
                else "HISTORICAL_CAPITAL_FLOW_CACHE_MALFORMED"
                if cache_state == MALFORMED
                else "HISTORICAL_CAPITAL_FLOW_CACHE_MISSING"
            ),
            expected_symbols=expected_symbols,
        )

    fetch = fetch_rank or _eastmoney_rank_fetcher
    frames: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for key, indicator, _weight in WINDOWS:
        try:
            frames[key] = fetch(indicator)
        except Exception as exc:  # provider boundary; error text is not persisted
            failures[key] = type(exc).__name__.upper()
    provider_dates = _capital_flow_provider_dates(frames)
    if historical_recovery and not _provider_dates_prove_trade_date(
        provider_dates,
        cutoff.date(),
        require_today=True,
    ):
        return unavailable_capital_flow_snapshot(
            as_of=cutoff,
            reason_code="HISTORICAL_CAPITAL_FLOW_PROVIDER_DATE_MISMATCH",
            expected_symbols=expected_symbols,
        )
    snapshot = build_capital_flow_snapshot(
        frames,
        as_of=cutoff,
        expected_symbols=expected_symbols,
        minimum_coverage=minimum_coverage,
        failures=failures,
        ingested_at=current,
    )
    snapshot = _with_provider_date_proof(
        snapshot,
        provider_dates=provider_dates,
        target_date=cutoff.date(),
        historical_recovery=historical_recovery,
    )
    # Persist partial and failed observations too.  The reason code and hash
    # are required to reproduce why A2 was blocked on that trade date.
    write_capital_flow_snapshot(root, snapshot)
    return snapshot


def collect_tencent_capital_flow(
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    cache_dir: str | Path,
    fetch_symbol: Callable[[str], Any] | None = None,
    fetch_trade_timestamp: Callable[[], Any] | None = None,
    now: datetime | None = None,
    minimum_coverage: float = 0.90,
    workers: int = 16,
    cache_max_age_seconds: float | None = None,
    allow_historical_recovery: bool = False,
) -> dict[str, Any]:
    """Collect Tencent's actual order-size fund-flow fields cross-sectionally.

    Tencent exposes one symbol per request. Collection is bounded and
    concurrent, then normalized by the same cross-sectional percentile
    contract as other vendors. A paired Tencent quote timestamp proves the
    trade date; price or turnover is never converted into capital flow.
    """

    cutoff = _aware(as_of)
    current = _aware(now or datetime.now(SHANGHAI))
    root = Path(cache_dir)
    cached, cache_state = _load_capital_flow_cache_state(
        root,
        cutoff.date().isoformat(),
        now=current,
        max_age_seconds=cache_max_age_seconds,
    )
    if cached is not None:
        return cached
    historical_recovery = cutoff.date() != current.date()
    if historical_recovery and not allow_historical_recovery:
        return unavailable_capital_flow_snapshot(
            as_of=cutoff,
            reason_code=(
                "HISTORICAL_CAPITAL_FLOW_CACHE_STALE"
                if cache_state == STALE
                else "HISTORICAL_CAPITAL_FLOW_CACHE_MALFORMED"
                if cache_state == MALFORMED
                else "HISTORICAL_CAPITAL_FLOW_CACHE_MISSING"
            ),
            expected_symbols=expected_symbols,
            source_id=TENCENT_CAPITAL_FLOW_PROVIDER,
        )
    if isinstance(workers, bool) or not 1 <= int(workers) <= 32:
        raise ValueError("Tencent capital-flow workers must be between 1 and 32")
    trade_timestamp_fetch = fetch_trade_timestamp or _tencent_trade_timestamp_fetcher
    provider_timestamp = trade_timestamp_fetch()
    provider_date = _provider_timestamp_date(provider_timestamp)
    if provider_date is None or provider_date != cutoff.date():
        return unavailable_capital_flow_snapshot(
            as_of=cutoff,
            reason_code=(
                "HISTORICAL_CAPITAL_FLOW_PROVIDER_DATE_MISMATCH"
                if historical_recovery
                else "CAPITAL_FLOW_PROVIDER_DATE_MISMATCH"
            ),
            expected_symbols=expected_symbols,
            source_id=TENCENT_CAPITAL_FLOW_PROVIDER,
        )

    symbols = tuple(dict.fromkeys(
        normalized
        for value in expected_symbols
        if (normalized := _normalize_symbol(value))
    ))
    fetch = fetch_symbol or _tencent_symbol_flow_fetcher
    rows: list[Mapping[str, Any]] = []
    failed_symbols: list[str] = []
    with ThreadPoolExecutor(max_workers=int(workers), thread_name_prefix="tencent-flow") as executor:
        futures = {executor.submit(fetch, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                raw = future.result()
            except Exception:
                failed_symbols.append(symbol)
                continue
            if not isinstance(raw, Mapping):
                failed_symbols.append(symbol)
                continue
            row = dict(raw)
            row.setdefault("symbol", symbol)
            row["provider_timestamp"] = provider_timestamp
            rows.append(row)

    failures = {
        "3d": "TENCENT_WINDOW_UNAVAILABLE",
        "5d": "TENCENT_WINDOW_UNAVAILABLE",
        "10d": "TENCENT_WINDOW_UNAVAILABLE",
    }
    if not rows:
        failures["today"] = "TENCENT_FLOW_UNAVAILABLE"
    snapshot = build_capital_flow_snapshot(
        {"today": rows},
        as_of=cutoff,
        expected_symbols=symbols,
        minimum_coverage=minimum_coverage,
        failures=failures,
        ingested_at=current,
        source_id=TENCENT_CAPITAL_FLOW_PROVIDER,
        source_ref_prefix="tencent",
    )
    snapshot = _with_provider_date_proof(
        snapshot,
        provider_dates={"today": (provider_date,)},
        target_date=cutoff.date(),
        historical_recovery=historical_recovery,
    )
    snapshot.pop("content_hash", None)
    snapshot["provider_capabilities"] = {
        "today": True,
        "3d": False,
        "5d": False,
        "10d": False,
    }
    snapshot["requested_symbol_count"] = len(symbols)
    snapshot["failed_symbol_count"] = len(failed_symbols)
    snapshot["failed_symbols_sample"] = sorted(failed_symbols)[:100]
    snapshot["content_hash"] = _content_hash(snapshot)
    write_capital_flow_snapshot(root, snapshot)
    return snapshot


def build_capital_flow_snapshot(
    frames: Mapping[str, Any],
    *,
    as_of: datetime,
    expected_symbols: Sequence[str],
    minimum_coverage: float = 0.90,
    failures: Mapping[str, str] | None = None,
    ingested_at: datetime | None = None,
    source_id: str = CAPITAL_FLOW_PROVIDER,
    source_ref_prefix: str = "eastmoney",
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    ingested = _aware(ingested_at or cutoff)
    expected = tuple(dict.fromkeys(_normalize_symbol(value) for value in expected_symbols if _normalize_symbol(value)))
    normalized: dict[str, dict[str, dict[str, Any]]] = {}
    window_diagnostics: dict[str, Any] = {}
    failures = dict(failures or {})
    for key, _indicator, _weight in WINDOWS:
        source_present = key in frames
        raw_frame = frames.get(key)
        source_malformed = False
        if source_present:
            try:
                rows = _records(raw_frame)
            except CapitalFlowError:
                rows = ()
                source_malformed = True
        else:
            rows = ()
        by_symbol: dict[str, dict[str, Any]] = {}
        invalid_rows = 0
        for row in rows:
            symbol = _normalize_symbol(_pick(row, "代码", "股票代码", "symbol", "ts_code", "thscode"))
            if not symbol:
                invalid_rows += 1
                continue
            ratio = _number(_pick_flow(row, key, "主力净流入-净占比", "主力净流入占比", "net_inflow_ratio"))
            amount = _number(_pick_flow(row, key, "主力净流入-净额", "主力净流入净额", "net_inflow_amount"))
            large_ratio = _number(_pick_flow(row, key, "大单净流入-净占比", "大单净流入占比", "large_inflow_ratio"))
            super_ratio = _number(_pick_flow(row, key, "超大单净流入-净占比", "超大单净流入占比", "super_inflow_ratio"))
            if ratio is None and amount is None:
                invalid_rows += 1
                continue
            by_symbol[symbol] = {
                "symbol": symbol,
                "name": str(_pick(row, "名称", "股票简称", "name") or ""),
                "net_inflow_amount_cny": amount,
                "net_inflow_ratio_pct": ratio,
                "large_inflow_ratio_pct": large_ratio,
                "super_inflow_ratio_pct": super_ratio,
                "provider_timestamp": _pick(row, "provider_timestamp", "f124"),
            }
        ratios = {
            symbol: values["net_inflow_ratio_pct"]
            for symbol, values in by_symbol.items()
            if values.get("net_inflow_ratio_pct") is not None
        }
        percentiles = _percentiles(ratios)
        for symbol, score in percentiles.items():
            by_symbol[symbol]["cross_section_percentile"] = score
        normalized[key] = by_symbol
        observed_expected = len(set(expected).intersection(by_symbol))
        coverage = observed_expected / len(expected) if expected else 1.0
        if key in failures:
            availability_state = _failure_state(failures[key])
            window_reason = failures[key]
        elif not source_present:
            availability_state = SOURCE_UNAVAILABLE
            window_reason = "SOURCE_NOT_PROVIDED"
        elif source_malformed or (rows and not by_symbol):
            availability_state = MALFORMED
            window_reason = "SOURCE_ROWS_MALFORMED"
        elif not rows:
            # Eastmoney's successful empty response is a real observation of
            # an empty ranking, not evidence that every stock had zero flow.
            availability_state = OBSERVED_EMPTY
            window_reason = "SOURCE_EMPTY"
        else:
            availability_state = OBSERVED_VALUE
            window_reason = "OK"
        window_diagnostics[key] = {
            "available": bool(by_symbol),
            "availability_state": availability_state,
            "reason_code": window_reason,
            "provider_record_count": len(by_symbol),
            "invalid_row_count": invalid_rows,
            "eligible_universe_count": len(expected),
            "observed_eligible_count": observed_expected,
            "coverage_ratio": round(coverage, 6),
        }

    by_symbol_output: dict[str, dict[str, Any]] = {}
    for symbol in expected:
        metrics: dict[str, Any] = {}
        weighted = 0.0
        available_weight = 0.0
        source_refs: list[str] = []
        for key, _indicator, weight in WINDOWS:
            value = normalized.get(key, {}).get(symbol)
            if value is None:
                metrics[key] = {
                    # A capital-flow ranking is not an event set.  A missing
                    # eligible symbol therefore does not prove zero flow or
                    # an observed absence; it means the supposedly complete
                    # provider cross-section cannot support this symbol.
                    # Keep the legacy per-symbol state for ranking rows: a
                    # missing symbol cannot prove zero flow.  The enclosing
                    # window diagnostics carries the precise source state.
                    "availability_state": "SOURCE_FAILED",
                    "reason_code": failures.get(key, "SYMBOL_MISSING_FROM_PROVIDER_CROSS_SECTION"),
                }
                continue
            score = _number(value.get("cross_section_percentile"))
            metrics[key] = {**value, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"}
            if score is not None:
                weighted += score * weight
                available_weight += weight
                source_refs.append(f"{source_ref_prefix}:capital-flow:{cutoff.date().isoformat()}:{key}")
        today = metrics.get("today", {})
        today_observed = today.get("availability_state") == "OBSERVED_VALUE"
        score = weighted / available_weight if today_observed and available_weight > 0 else None
        by_symbol_output[symbol] = {
            "symbol": symbol,
            "available": score is not None,
            "availability_state": "OBSERVED_VALUE" if score is not None else today.get("availability_state", "SOURCE_FAILED"),
            "reason_code": "OK" if score is not None else str(today.get("reason_code") or "CAPITAL_FLOW_UNAVAILABLE"),
            "capital_flow_score": round(score, 4) if score is not None else None,
            "available_weight": round(available_weight, 4),
            "metrics": metrics,
            "source_refs": source_refs,
        }

    today_coverage = float(window_diagnostics.get("today", {}).get("coverage_ratio") or 0.0)
    available = bool(normalized.get("today")) and today_coverage >= minimum_coverage
    reason = "OK" if available and not failures else "PARTIAL_WINDOWS" if available else "CAPITAL_FLOW_COVERAGE_INSUFFICIENT"
    states = {str(item.get("availability_state")) for item in window_diagnostics.values()}
    if available:
        # Tencent intentionally exposes only today's order-size split. Missing
        # optional history is a capability declaration, not a contradiction
        # of the fully covered today cross-section.
        snapshot_state = OBSERVED_VALUE
    elif any(state == MALFORMED for state in states):
        snapshot_state = MALFORMED
    elif any(state == SOURCE_UNAVAILABLE for state in states):
        snapshot_state = SOURCE_UNAVAILABLE
    elif any(state == STALE for state in states):
        snapshot_state = STALE
    elif any(state == OBSERVED_VALUE for state in states):
        snapshot_state = OBSERVED_VALUE
    elif any(state == OBSERVED_EMPTY for state in states):
        snapshot_state = OBSERVED_EMPTY
    else:
        snapshot_state = SOURCE_UNAVAILABLE
    payload: dict[str, Any] = {
        "schema_version": CAPITAL_FLOW_SCHEMA,
        "available": available,
        "availability_state": snapshot_state,
        "reason_code": reason,
        "source_id": source_id,
        "source_tier": "T2",
        "provider_method": PROVIDER_METHOD,
        "authority_class": "VENDOR_DERIVED_NOT_EXCHANGE_FACT",
        "turnover_is_capital_flow": False,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "ingested_at": ingested.isoformat(),
        "minimum_coverage": minimum_coverage,
        "coverage_by_window": window_diagnostics,
        "failures": failures,
        "by_symbol": by_symbol_output,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def with_capital_flow_provider_attempts(
    snapshot: Mapping[str, Any],
    attempts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Attach redacted provider-routing evidence and preserve hash integrity."""

    result = dict(snapshot)
    result.pop("content_hash", None)
    result["provider_attempts"] = [
        {
            "source_id": str(item.get("source_id") or "UNKNOWN"),
            "available": item.get("available") is True,
            "availability_state": str(item.get("availability_state") or "UNKNOWN"),
            "reason_code": str(item.get("reason_code") or "UNKNOWN"),
            "today_coverage_ratio": float(
                ((item.get("coverage_by_window") or {}).get("today") or {}).get("coverage_ratio")
                or 0.0
            ),
        }
        for item in attempts
        if isinstance(item, Mapping)
    ]
    result["content_hash"] = _content_hash(result)
    return result


def unavailable_capital_flow_snapshot(
    *,
    as_of: datetime,
    reason_code: str,
    expected_symbols: Sequence[str],
    source_id: str = CAPITAL_FLOW_PROVIDER,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    availability_state = _state_for_reason(reason_code)
    payload: dict[str, Any] = {
        "schema_version": CAPITAL_FLOW_SCHEMA,
        "available": False,
        "availability_state": availability_state,
        "reason_code": reason_code,
        "source_id": source_id,
        "source_tier": "T2",
        "provider_method": PROVIDER_METHOD,
        "authority_class": "VENDOR_DERIVED_NOT_EXCHANGE_FACT",
        "turnover_is_capital_flow": False,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "minimum_coverage": 0.90,
        "coverage_by_window": {},
        "by_symbol": {
            symbol: {
                "symbol": symbol,
                "available": False,
                # Preserve NOT_CONFIGURED for existing callers while making
                # actual provider/cache failures distinguishable.
                "availability_state": (
                    "NOT_CONFIGURED"
                    if reason_code == "SOURCE_NOT_CONFIGURED"
                    else availability_state
                ),
                "reason_code": reason_code,
                "capital_flow_score": None,
                "source_refs": [],
            }
            for value in expected_symbols
            if (symbol := _normalize_symbol(value))
        },
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def write_capital_flow_snapshot(cache_dir: str | Path, snapshot: Mapping[str, Any]) -> Path:
    root = Path(cache_dir)
    trade_date = str(snapshot.get("trade_date") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade_date):
        raise CapitalFlowError("capital-flow snapshot requires an ISO trade_date")
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"capital-flow-{trade_date}.json"
    atomic_write_json(path, dict(snapshot))
    return path


def load_capital_flow_snapshot(
    cache_dir: str | Path,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    """Load a hash-validated capital-flow snapshot.

    ``max_age_seconds`` is opt-in for compatibility.  A historical replay
    should normally omit it because a cache is bound to its trade date; a
    same-day caller may provide it to reject an old partial/current response.
    Invalid and stale files return ``None`` and are never used as fresh data.
    ``collect_eastmoney_capital_flow`` uses the private status-aware reader so
    it can report ``STALE`` versus ``MALFORMED`` without networking on a
    historical date.
    """

    payload, state = _load_capital_flow_cache_state(
        cache_dir,
        trade_date,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    # A valid cache can be an observed empty/failed observation.  It still
    # must be returned so callers do not repeatedly hit the provider and so
    # the exact source status remains replayable.  Only stale/malformed files
    # are rejected here.
    return payload if state not in {STALE, MALFORMED, None} else None


def _load_capital_flow_cache_state(
    cache_dir: str | Path,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Return ``(payload, state)`` while retaining cache failure semantics."""

    path = Path(cache_dir) / f"capital-flow-{trade_date}.json"
    if not path.is_file():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, MALFORMED
    if not isinstance(payload, dict) or payload.get("schema_version") != CAPITAL_FLOW_SCHEMA:
        return None, MALFORMED
    expected = str(payload.get("content_hash") or "")
    body = dict(payload)
    body.pop("content_hash", None)
    if not expected or expected != _content_hash(body):
        return None, MALFORMED
    if str(payload.get("trade_date") or "") != str(trade_date):
        return None, MALFORMED
    provider_dates = payload.get("provider_trade_dates")
    if isinstance(provider_dates, Mapping):
        observed_provider_dates = {
            str(value)
            for values in provider_dates.values()
            if isinstance(values, Sequence)
            and not isinstance(values, (str, bytes, bytearray))
            for value in values
            if str(value).strip()
        }
        if observed_provider_dates and observed_provider_dates != {str(trade_date)}:
            # A cache filename and its local trade_date are not sufficient
            # proof of market identity.  Older collectors could persist the
            # latest provider response under the requested day before the
            # provider timestamp was checked.  Never replay that mislabeled
            # observation as point-in-time data.
            return None, MALFORMED
    if max_age_seconds is not None:
        if isinstance(max_age_seconds, bool) or float(max_age_seconds) < 0:
            raise ValueError("max_age_seconds must be non-negative")
        reference = _aware(now or datetime.now(SHANGHAI))
        ingested = _parse_datetime(payload.get("ingested_at"))
        if ingested is None:
            return None, MALFORMED
        if (reference - ingested).total_seconds() > float(max_age_seconds):
            return None, STALE
    return payload, str(payload.get("availability_state") or OBSERVED_VALUE)


def write_trade_date_fact(
    cache_dir: str | Path,
    dataset: str,
    trade_date: str,
    payload: Any,
    *,
    source_id: str,
    source_kind: str,
    published_scope: str = "FULL_MARKET",
    availability_state: str | None = None,
    reason_code: str = "OK",
    as_of: datetime | None = None,
    event_time: datetime | None = None,
    fetch_time: datetime | None = None,
    ingested_at: datetime | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Path:
    """Persist one A2 fact under a validated dataset/date key.

    The file is an immutable point-in-time envelope from the workflow's
    perspective: a later collection for the same date may replace a failed
    current observation, but historical readers only ever open the exact
    requested date.  ``atomic_write_json`` prevents readers from observing a
    partially written JSON document.
    """

    dataset_name = _validate_dataset(dataset)
    date_text = _validate_trade_date(trade_date)
    raw_payload = _jsonable(payload)
    if isinstance(raw_payload, Mapping):
        payload_body = dict(raw_payload)
        records = payload_body.get("records")
    elif isinstance(raw_payload, Sequence) and not isinstance(raw_payload, (str, bytes, bytearray)):
        payload_body = {"records": list(raw_payload)}
        records = payload_body["records"]
    else:
        payload_body = {"value": raw_payload}
        records = None
    if records is not None and (
        not isinstance(records, list)
        or any(not isinstance(item, Mapping) for item in records)
    ):
        raise ValueError("trade-date fact records must be a list of objects")
    state = availability_state or _infer_fact_state(payload_body, reason_code=reason_code)
    if state not in _A2_FACT_STATES:
        raise ValueError(f"unsupported A2 fact availability_state: {state}")
    cutoff = _aware(as_of or datetime.now(SHANGHAI))
    fetched = _aware(fetch_time or cutoff)
    ingested = _aware(ingested_at or fetched)
    event = _aware(event_time or cutoff)
    envelope: dict[str, Any] = {
        "schema_version": A2_MARKET_FACT_SCHEMA,
        "dataset": dataset_name,
        "trade_date": date_text,
        "as_of": cutoff.isoformat(),
        "event_time": event.isoformat(),
        "fetch_time": fetched.isoformat(),
        "ingested_at": ingested.isoformat(),
        "source_id": str(source_id or "UNKNOWN"),
        "source_kind": str(source_kind or "UNKNOWN"),
        "published_scope": str(published_scope or "SYMBOL"),
        "availability_state": state,
        "available": state in {OBSERVED_VALUE, OBSERVED_EMPTY},
        "reason_code": str(reason_code or ("OK" if state in {OBSERVED_VALUE, OBSERVED_EMPTY} else state)),
        "records": records if records is not None else [],
        "payload": payload_body,
        "metadata": dict(metadata or {}),
    }
    envelope["content_hash"] = _content_hash(envelope)
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{dataset_name}-{date_text}.json"
    atomic_write_json(path, envelope)
    return path


def inspect_trade_date_fact(
    cache_dir: str | Path,
    dataset: str,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    """Inspect a date-bound cache without turning failures into empty data."""

    dataset_name = _validate_dataset(dataset)
    date_text = _validate_trade_date(trade_date)
    path = Path(cache_dir) / f"{dataset_name}-{date_text}.json"
    payload, state, reason = _read_trade_date_fact(
        path,
        dataset=dataset_name,
        trade_date=date_text,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    return {
        "path": str(path),
        "exists": path.is_file(),
        "available": payload is not None,
        "availability_state": state,
        "reason_code": reason,
        "fact": payload,
    }


def load_trade_date_fact(
    cache_dir: str | Path,
    dataset: str,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    """Read only the requested trade date's hash-validated fact envelope."""

    inspected = inspect_trade_date_fact(
        cache_dir,
        dataset,
        trade_date,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    return inspected["fact"] if inspected["available"] else None


def collect_trade_date_fact(
    *,
    cache_dir: str | Path,
    dataset: str,
    as_of: datetime,
    fetch: Callable[[], Any] | None = None,
    source_id: str = "UNKNOWN",
    source_kind: str = "UNKNOWN",
    published_scope: str = "FULL_MARKET",
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    """Load/cache/collect a point-in-time fact with historical no-network guard.

    ``fetch`` is called only for today's Shanghai date.  For any historical
    date, a valid cache is returned; a missing, stale, or malformed cache is
    represented by a status envelope and the callback is never invoked.
    """

    cutoff = _aware(as_of)
    current = _aware(now or datetime.now(SHANGHAI))
    date_text = cutoff.date().isoformat()
    inspected = inspect_trade_date_fact(
        cache_dir,
        dataset,
        date_text,
        now=current,
        max_age_seconds=max_age_seconds,
    )
    if inspected["available"]:
        return dict(inspected["fact"])
    if cutoff.date() != current.date():
        return _unavailable_trade_date_fact(
            dataset=dataset,
            trade_date=date_text,
            as_of=cutoff,
            source_id=source_id,
            source_kind=source_kind,
            published_scope=published_scope,
            state=inspected["availability_state"] or SOURCE_UNAVAILABLE,
            reason_code=inspected["reason_code"] or "HISTORICAL_CACHE_MISSING",
        )
    if fetch is None:
        result = _unavailable_trade_date_fact(
            dataset=dataset,
            trade_date=date_text,
            as_of=cutoff,
            source_id=source_id,
            source_kind=source_kind,
            published_scope=published_scope,
            state=SOURCE_UNAVAILABLE,
            reason_code="SOURCE_NOT_CONFIGURED",
        )
        write_trade_date_fact(
            cache_dir,
            dataset,
            date_text,
            result,
            source_id=source_id,
            source_kind=source_kind,
            published_scope=published_scope,
            availability_state=SOURCE_UNAVAILABLE,
            reason_code="SOURCE_NOT_CONFIGURED",
            as_of=cutoff,
            ingested_at=current,
        )
        return result
    try:
        fetched_result = fetch()
        result = _normalize_collected_fact(
            fetched_result,
            dataset=dataset,
            trade_date=date_text,
            as_of=cutoff,
            source_id=source_id,
            source_kind=source_kind,
            published_scope=published_scope,
            ingested_at=current,
        )
    except Exception as exc:  # provider boundary; never leak response text
        result = _unavailable_trade_date_fact(
            dataset=dataset,
            trade_date=date_text,
            as_of=cutoff,
            source_id=source_id,
            source_kind=source_kind,
            published_scope=published_scope,
            state=_failure_state(type(exc).__name__),
            reason_code=type(exc).__name__.upper(),
        )
    write_trade_date_fact(
        cache_dir,
        dataset,
        date_text,
        result,
        source_id=source_id,
        source_kind=source_kind,
        published_scope=published_scope,
        availability_state=str(result.get("availability_state") or SOURCE_UNAVAILABLE),
        reason_code=str(result.get("reason_code") or "UNKNOWN"),
        as_of=cutoff,
        event_time=_parse_datetime(result.get("event_time")) or cutoff,
        fetch_time=_parse_datetime(result.get("fetch_time")) or current,
        ingested_at=current,
    )
    persisted = load_trade_date_fact(cache_dir, dataset, date_text)
    return persisted if persisted is not None else result


def persist_ths_market_fact(
    cache_dir: str | Path,
    dataset: str,
    result: Any,
    *,
    as_of: datetime,
    published_scope: str = "FULL_MARKET",
    source_id: str = "HITHINK",
    source_kind: str = "THS",
    ingested_at: datetime | None = None,
) -> Path:
    """Persist a HiThink endpoint result as a replayable A2 fact envelope."""

    normalized = _normalize_collected_fact(
        result,
        dataset=dataset,
        trade_date=_aware(as_of).date().isoformat(),
        as_of=_aware(as_of),
        source_id=source_id,
        source_kind=source_kind,
        published_scope=published_scope,
        ingested_at=_aware(ingested_at or as_of),
    )
    return write_trade_date_fact(
        cache_dir,
        dataset,
        normalized["trade_date"],
        normalized,
        source_id=source_id,
        source_kind=source_kind,
        published_scope=published_scope,
        availability_state=normalized["availability_state"],
        reason_code=normalized["reason_code"],
        as_of=_aware(as_of),
        event_time=_parse_datetime(normalized.get("event_time")) or _aware(as_of),
        fetch_time=_parse_datetime(normalized.get("fetch_time")) or _aware(as_of),
        ingested_at=_aware(ingested_at or as_of),
    )


def collect_ths_market_fact(
    *,
    cache_dir: str | Path,
    dataset: str,
    as_of: datetime,
    fetch: Callable[[], Any] | None,
    now: datetime | None = None,
    published_scope: str = "FULL_MARKET",
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    """Convenience wrapper for THS ladder/membership/market fact collectors."""

    return collect_trade_date_fact(
        cache_dir=cache_dir,
        dataset=dataset,
        as_of=as_of,
        fetch=fetch,
        source_id="HITHINK",
        source_kind="THS",
        published_scope=published_scope,
        now=now,
        max_age_seconds=max_age_seconds,
    )


def build_board_capital_flow_snapshot(
    rows: Any,
    *,
    as_of: datetime,
    board_type: str,
    period: str,
    source_id: str = BOARD_FLOW_PROVIDER,
    ingested_at: datetime | None = None,
) -> dict[str, Any]:
    """Normalize Eastmoney industry/concept/region flow rows by trade date."""

    if board_type not in _BOARD_TYPES:
        raise ValueError(f"unsupported board_type: {board_type}")
    if period not in _BOARD_PERIODS:
        raise ValueError(f"unsupported board flow period: {period}")
    cutoff = _aware(as_of)
    ingested = _aware(ingested_at or cutoff)
    extracted, provider_total, malformed = _board_records(rows)
    normalized: list[dict[str, Any]] = []
    for row in extracted:
        code = str(_pick(row, "code", "板块代码", "f12", "index_code") or "").strip()
        name = str(_pick(row, "name", "板块名称", "f14", "index_name") or "").strip()
        if not code and not name:
            malformed += 1
            continue
        normalized.append({
            "rank": int(_number(_pick(row, "rank", "排名")) or len(normalized) + 1),
            "code": code,
            "name": name,
            "change_pct": _number(_pick(row, "change_pct", "涨跌幅", "f3", "f109", "f160")),
            "main_net_cny": _number(_pick(row, "main_net", "main_net_cny", "主力净流入", "f62", "f164", "f174")),
            "main_pct": _number(_pick(row, "main_pct", "主力净占比", "f184", "f165", "f175")),
            "leader": str(_pick(row, "leader", "领涨股", "f204", "f257") or ""),
            "super_large_net_cny": _number(_pick(row, "super_large_net", "超大单净流入", "f66")),
            "large_net_cny": _number(_pick(row, "large_net", "大单净流入", "f72")),
            "medium_net_cny": _number(_pick(row, "medium_net", "中单净流入", "f78")),
            "small_net_cny": _number(_pick(row, "small_net", "小单净流入", "f84")),
            "provider_timestamp": _pick(row, "provider_timestamp", "f124"),
        })
    # Stable ordering protects the hash and makes a replay diff meaningful.
    normalized.sort(key=lambda item: (int(item.get("rank") or 0), item["code"], item["name"]))
    if malformed and not normalized:
        state, reason = MALFORMED, "BOARD_FLOW_ROWS_MALFORMED"
    elif not normalized:
        state, reason = OBSERVED_EMPTY, "SOURCE_EMPTY"
    else:
        state, reason = OBSERVED_VALUE, "OK"
    payload: dict[str, Any] = {
        "schema_version": BOARD_FLOW_SCHEMA,
        "dataset": "BOARD_CAPITAL_FLOW",
        "board_type": board_type,
        "period": period,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "ingested_at": ingested.isoformat(),
        "source_id": source_id,
        "source_kind": "EASTMONEY_VENDOR_DERIVED",
        "availability_state": state,
        "available": state in {OBSERVED_VALUE, OBSERVED_EMPTY},
        "reason_code": reason,
        "provider_record_count": len(normalized),
        "provider_total": provider_total,
        "malformed_row_count": malformed,
        "records": normalized,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def write_board_capital_flow_snapshot(cache_dir: str | Path, snapshot: Mapping[str, Any]) -> Path:
    """Atomically persist one board-flow period under its trade date."""

    board_type = str(snapshot.get("board_type") or "")
    period = str(snapshot.get("period") or "")
    trade_date = _validate_trade_date(snapshot.get("trade_date"))
    if board_type not in _BOARD_TYPES or period not in _BOARD_PERIODS:
        raise ValueError("board-flow snapshot identity is invalid")
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"board-flow-{board_type}-{period}-{trade_date}.json"
    atomic_write_json(path, dict(snapshot))
    return path


def load_board_capital_flow_snapshot(
    cache_dir: str | Path,
    board_type: str,
    period: str,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    """Load a hash-bound board flow cache; stale/malformed files are rejected."""

    inspected = inspect_board_capital_flow_snapshot(
        cache_dir,
        board_type,
        period,
        trade_date,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    return inspected["snapshot"] if inspected["available"] else None


def inspect_board_capital_flow_snapshot(
    cache_dir: str | Path,
    board_type: str,
    period: str,
    trade_date: str,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, Any]:
    if board_type not in _BOARD_TYPES or period not in _BOARD_PERIODS:
        raise ValueError("board-flow identity is invalid")
    date_text = _validate_trade_date(trade_date)
    path = Path(cache_dir) / f"board-flow-{board_type}-{period}-{date_text}.json"
    if not path.is_file():
        return {"path": str(path), "exists": False, "available": False, "availability_state": None, "reason_code": None, "snapshot": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {"path": str(path), "exists": True, "available": False, "availability_state": MALFORMED, "reason_code": "BOARD_FLOW_CACHE_JSON_MALFORMED", "snapshot": None}
    state = str(payload.get("availability_state") or "") if isinstance(payload, Mapping) else MALFORMED
    reason = str(payload.get("reason_code") or "OK") if isinstance(payload, Mapping) else "BOARD_FLOW_CACHE_ENVELOPE_MALFORMED"
    body = dict(payload) if isinstance(payload, Mapping) else {}
    expected = str(body.pop("content_hash", ""))
    valid = (
        isinstance(payload, Mapping)
        and payload.get("schema_version") == BOARD_FLOW_SCHEMA
        and payload.get("board_type") == board_type
        and payload.get("period") == period
        and payload.get("trade_date") == date_text
        and state in _A2_FACT_STATES
        and expected
        and expected == _content_hash(body)
        and isinstance(payload.get("records"), list)
        and all(isinstance(item, Mapping) for item in payload["records"])
    )
    if not valid:
        return {"path": str(path), "exists": True, "available": False, "availability_state": MALFORMED, "reason_code": "BOARD_FLOW_CACHE_INVALID", "snapshot": None}
    provider_dates = payload.get("provider_trade_dates")
    if isinstance(provider_dates, Mapping):
        observed_provider_dates = {
            str(value)
            for values in provider_dates.values()
            if isinstance(values, Sequence)
            and not isinstance(values, (str, bytes, bytearray))
            for value in values
            if str(value).strip()
        }
        if observed_provider_dates and observed_provider_dates != {date_text}:
            return {
                "path": str(path),
                "exists": True,
                "available": False,
                "availability_state": MALFORMED,
                "reason_code": "BOARD_FLOW_CACHE_PROVIDER_DATE_MISMATCH",
                "snapshot": None,
            }
    if max_age_seconds is not None:
        if isinstance(max_age_seconds, bool) or float(max_age_seconds) < 0:
            raise ValueError("max_age_seconds must be non-negative")
        ingested = _parse_datetime(payload.get("ingested_at"))
        if ingested is None:
            return {"path": str(path), "exists": True, "available": False, "availability_state": MALFORMED, "reason_code": "BOARD_FLOW_CACHE_TIMESTAMP_MALFORMED", "snapshot": None}
        reference = _aware(now or datetime.now(SHANGHAI))
        if (reference - ingested).total_seconds() > float(max_age_seconds):
            return {"path": str(path), "exists": True, "available": False, "availability_state": STALE, "reason_code": "BOARD_FLOW_CACHE_STALE", "snapshot": None}
    return {"path": str(path), "exists": True, "available": True, "availability_state": state, "reason_code": reason, "snapshot": dict(payload)}


def collect_eastmoney_board_flow(
    *,
    as_of: datetime,
    board_type: str,
    period: str,
    cache_dir: str | Path,
    fetch_board: Callable[[str, str], Any] | None = None,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
    allow_historical_recovery: bool = False,
) -> dict[str, Any]:
    """Collect one board-flow period; historical dates are cache-only."""

    if board_type not in _BOARD_TYPES or period not in _BOARD_PERIODS:
        raise ValueError("board-flow identity is invalid")
    cutoff = _aware(as_of)
    current = _aware(now or datetime.now(SHANGHAI))
    date_text = cutoff.date().isoformat()
    inspected = inspect_board_capital_flow_snapshot(
        cache_dir,
        board_type,
        period,
        date_text,
        now=current,
        max_age_seconds=max_age_seconds,
    )
    if inspected["available"]:
        return dict(inspected["snapshot"])
    historical_recovery = cutoff.date() != current.date()
    if historical_recovery and not allow_historical_recovery:
        return _unavailable_board_flow_snapshot(
            as_of=cutoff,
            board_type=board_type,
            period=period,
            state=inspected["availability_state"] or SOURCE_UNAVAILABLE,
            reason_code=inspected["reason_code"] or "HISTORICAL_BOARD_FLOW_CACHE_MISSING",
        )
    fetch = fetch_board or _eastmoney_board_flow_fetcher
    try:
        raw = fetch(board_type, period)
        provider_dates = _board_flow_provider_dates(raw)
        if historical_recovery and not _provider_dates_prove_trade_date(
            {period: provider_dates},
            cutoff.date(),
        ):
            return _unavailable_board_flow_snapshot(
                as_of=cutoff,
                board_type=board_type,
                period=period,
                state=SOURCE_UNAVAILABLE,
                reason_code="HISTORICAL_BOARD_FLOW_PROVIDER_DATE_MISMATCH",
            )
        snapshot = build_board_capital_flow_snapshot(
            raw,
            as_of=cutoff,
            board_type=board_type,
            period=period,
            ingested_at=current,
        )
        snapshot = _with_provider_date_proof(
            snapshot,
            provider_dates={period: provider_dates},
            target_date=cutoff.date(),
            historical_recovery=historical_recovery,
        )
    except Exception as exc:  # provider boundary
        snapshot = _unavailable_board_flow_snapshot(
            as_of=cutoff,
            board_type=board_type,
            period=period,
            state=_failure_state(type(exc).__name__),
            reason_code=type(exc).__name__.upper(),
        )
    write_board_capital_flow_snapshot(cache_dir, snapshot)
    return snapshot


def _unavailable_board_flow_snapshot(
    *,
    as_of: datetime,
    board_type: str,
    period: str,
    state: str,
    reason_code: str,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    state = state if state in _A2_FACT_STATES else SOURCE_UNAVAILABLE
    payload: dict[str, Any] = {
        "schema_version": BOARD_FLOW_SCHEMA,
        "dataset": "BOARD_CAPITAL_FLOW",
        "board_type": board_type,
        "period": period,
        "trade_date": cutoff.date().isoformat(),
        "as_of": cutoff.isoformat(),
        "ingested_at": cutoff.isoformat(),
        "source_id": BOARD_FLOW_PROVIDER,
        "source_kind": "EASTMONEY_VENDOR_DERIVED",
        "availability_state": state,
        "available": False,
        "reason_code": reason_code,
        "provider_record_count": 0,
        "provider_total": None,
        "malformed_row_count": 0,
        "records": [],
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def _eastmoney_board_flow_fetcher(board_type: str, period: str) -> dict[str, Any]:
    """Fetch all requested board rows from Eastmoney's public clist endpoint."""

    if board_type not in _BOARD_TYPES or period not in _BOARD_PERIODS:
        raise ValueError("board-flow identity is invalid")
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    fid, _pct_field, change_field, leader_field = _BOARD_PERIODS[period]
    fields = ["f12", "f14", change_field, fid, _pct_field, "f124"]
    if leader_field:
        fields.append(leader_field)
    if period == "today":
        fields.extend(("f66", "f72", "f78", "f84"))
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    rows: list[dict[str, Any]] = []
    page = 1
    total: int | None = None
    while total is None or len(rows) < total:
        params = {
                "pz": "200",
                "po": "1",
                "np": "1",
                "pn": str(page),
                "fltt": "2",
                "invt": "2",
                "fid": fid,
                "fs": _BOARD_FS[board_type],
                "fields": ",".join(dict.fromkeys(fields)),
            }
        body, provider_url = _eastmoney_get_json(
            session,
            params=params,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)", "Referer": "https://data.eastmoney.com/"},
        )
        data = body.get("data") if isinstance(body, Mapping) else None
        if not isinstance(data, Mapping) or not isinstance(data.get("diff"), list):
            raise CapitalFlowError("board capital-flow provider envelope invalid")
        total = int(data.get("total") or 0)
        page_rows = [item for item in data["diff"] if isinstance(item, Mapping)]
        if not page_rows and len(rows) < total:
            raise CapitalFlowError("board capital-flow provider page incomplete")
        for item in page_rows:
            rows.append({
                "code": item.get("f12"),
                "name": item.get("f14"),
                "change_pct": item.get(change_field),
                "main_net": item.get(fid),
                "main_pct": item.get(_pct_field),
                "leader": item.get(leader_field) if leader_field else "",
                "super_large_net": item.get("f66"),
                "large_net": item.get("f72"),
                "medium_net": item.get("f78"),
                "small_net": item.get("f84"),
                "provider_timestamp": item.get("f124"),
                "provider_host": provider_url,
            })
        page += 1
        if page > 100:
            raise CapitalFlowError("board capital-flow provider page bound exceeded")
        if not page_rows:
            break
    return {"rows": rows, "total": total or len(rows)}


def _eastmoney_get_json(
    session: Any,
    *,
    params: Mapping[str, Any],
    headers: Mapping[str, str],
) -> tuple[Mapping[str, Any], str]:
    """Read one Eastmoney payload through an ordered, equivalent host set.

    Some deployment networks terminate ``push2`` while the vendor's delayed
    public host remains reachable. Both hosts expose the same clist contract;
    the selected URL is retained in normalized rows for audit.
    """

    for url in _EASTMONEY_URLS:
        try:
            response = session.get(
                url,
                params=dict(params),
                headers=dict(headers),
                timeout=(5, 20),
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, Mapping):
                return payload, url
        except Exception:
            continue
    raise CapitalFlowError("eastmoney public hosts unavailable")


def _board_records(value: Any) -> tuple[list[Mapping[str, Any]], int | None, int]:
    if value is None:
        return [], None, 0
    raw = _jsonable(value)
    provider_total: int | None = None
    if isinstance(raw, Mapping):
        provider_total = int(_number(raw.get("total")) or 0) or None
        records = raw.get("rows")
        if records is None:
            records = raw.get("records")
        if records is None and isinstance(raw.get("data"), Mapping):
            data = raw["data"]
            provider_total = int(_number(data.get("total")) or provider_total or 0) or None
            records = data.get("diff")
        if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
            return [], provider_total, 1
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        records = raw
    else:
        return [], provider_total, 1
    parsed = [item for item in records if isinstance(item, Mapping)]
    return parsed, provider_total, len(records) - len(parsed)


def _eastmoney_rank_fetcher(indicator: str) -> list[dict[str, Any]]:
    """Fetch the complete vendor cross-section in bounded, retried pages.

    Calling the vendor endpoint directly keeps the transport contract explicit
    while the persisted fact remains labelled vendor-derived rather than an
    exchange fact.
    """

    import requests  # lazy: doctor and historical replay remain network-free
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    key = {"今日": "today", "3日": "3d", "5日": "5d", "10日": "10d"}.get(indicator)
    if key is None:
        raise CapitalFlowError("unsupported capital-flow window")
    fid, fields, amount_field, ratio_field, large_amount_field, large_ratio_field, super_ratio_field = _EASTMONEY_FIELDS[key]
    fields = ",".join(dict.fromkeys([*fields.split(","), "f124"]))
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
        "Referer": "https://data.eastmoney.com/",
    }
    rows: list[dict[str, Any]] = []
    page = 1
    total = None
    while total is None or len(rows) < total:
        params = {
                "fid": fid,
                "po": "1",
                # The endpoint accepts a large bounded page.  A full A-share
                # cross-section then needs about two pages per window instead
                # of roughly sixty, while the loop still follows the provider
                # total if it applies a smaller server-side cap.
                "pz": str(_EASTMONEY_PAGE_SIZE),
                "pn": str(page),
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "ut": "b2884a393a59ad64002292a3e90d46a5",
                "fs": _EASTMONEY_UNIVERSE,
                "fields": fields,
            }
        payload, provider_url = _eastmoney_get_json(
            session,
            params=params,
            headers=headers,
        )
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping) or not isinstance(data.get("diff"), list):
            raise CapitalFlowError("capital-flow provider envelope invalid")
        total = int(data.get("total") or 0)
        page_rows = [item for item in data["diff"] if isinstance(item, Mapping)]
        if not page_rows and len(rows) < total:
            raise CapitalFlowError("capital-flow provider page incomplete")
        for item in page_rows:
            rows.append({
                "symbol": item.get("f12"),
                "name": item.get("f14"),
                "net_inflow_amount": item.get(amount_field),
                "net_inflow_ratio": item.get(ratio_field),
                "large_inflow_amount": item.get(large_amount_field),
                "large_inflow_ratio": item.get(large_ratio_field),
                "super_inflow_ratio": item.get(super_ratio_field),
                "provider_timestamp": item.get("f124"),
                "provider_host": provider_url,
            })
        page += 1
        if page > 100:
            raise CapitalFlowError("capital-flow provider page bound exceeded")
    return rows


def _tencent_trade_timestamp_fetcher() -> str:
    """Return Tencent's own quote timestamp for point-in-time date proof."""

    session = _tencent_http_session()
    response = session.get(
        "https://qt.gtimg.cn/q=sh000001",
        timeout=(5, 12),
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
            "Referer": "https://gu.qq.com/",
        },
    )
    response.raise_for_status()
    response.encoding = "gbk"
    return _parse_tencent_quote_timestamp(response.text)


def _tencent_symbol_flow_fetcher(symbol: str) -> dict[str, Any]:
    """Fetch and normalize one Tencent todayFundFlow record."""

    provider_symbol = _tencent_symbol(symbol)
    session = _tencent_http_session()
    response = session.get(
        "https://proxy.finance.qq.com/cgi/cgi-bin/fundflow/hsfundtab",
        params={
            "code": provider_symbol,
            "type": "todayFundFlow",
            "klineNeedDay": "1",
        },
        timeout=(5, 12),
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LiangjianResearch/2.0)",
            "Referer": f"https://gu.qq.com/{provider_symbol}/gp",
        },
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise CapitalFlowError("Tencent capital-flow response is not JSON") from exc
    return _parse_tencent_flow_payload(payload, expected_symbol=symbol)


def _tencent_http_session() -> Any:
    """Reuse one bounded retrying session per worker thread."""

    session = getattr(_TENCENT_THREAD_LOCAL, "session", None)
    if session is not None:
        return session
    import requests  # lazy: cached replays remain network-free
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.25,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1))
    _TENCENT_THREAD_LOCAL.session = session
    return session


def _parse_tencent_quote_timestamp(raw: Any) -> str:
    text = str(raw or "").strip()
    match = re.search(r'=\s*"([^"]*)"', text)
    if not match:
        raise CapitalFlowError("Tencent quote envelope invalid")
    fields = match.group(1).split("~")
    if len(fields) <= 30 or not re.fullmatch(r"\d{14}", fields[30].strip()):
        raise CapitalFlowError("Tencent quote timestamp missing")
    return fields[30].strip()


def _parse_tencent_flow_payload(
    payload: Any,
    *,
    expected_symbol: str,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise CapitalFlowError("Tencent capital-flow envelope invalid")
    data = payload.get("data")
    today = data.get("todayFundFlow") if isinstance(data, Mapping) else None
    if not isinstance(today, Mapping):
        raise CapitalFlowError("Tencent today capital-flow row missing")

    actual_symbol = _normalize_symbol(today.get("stockCode") or expected_symbol)
    normalized_expected = _normalize_symbol(expected_symbol)
    if not actual_symbol or actual_symbol != normalized_expected:
        raise CapitalFlowError("Tencent capital-flow symbol mismatch")

    main_net = _number(today.get("mainNetIn"))
    main_in = _number(today.get("mainIn"))
    main_out = _number(today.get("mainOut"))
    retail_in = _number(today.get("retailIn"))
    retail_out = _number(today.get("retailOut"))
    super_net = _number(today.get("superFlow"))
    big_net = _number(today.get("bigFlow"))
    if main_net is None:
        raise CapitalFlowError("Tencent main net inflow missing")

    # Tencent documents main/retail as mutually exclusive order-size buckets.
    # Their gross sum is therefore a vendor flow denominator, not turnover or
    # a value reconstructed from OHLCV. Preserve amounts even when a ratio
    # cannot be computed.
    gross_values = (main_in, main_out, retail_in, retail_out)
    gross_flow = (
        sum(float(value) for value in gross_values if value is not None)
        if all(value is not None and value >= 0 for value in gross_values)
        else None
    )
    denominator = gross_flow if gross_flow is not None and gross_flow > 0 else None
    return {
        "symbol": normalized_expected,
        "name": "",
        "net_inflow_amount": main_net,
        "net_inflow_ratio": main_net / denominator * 100.0 if denominator else None,
        "large_inflow_amount": big_net,
        "large_inflow_ratio": big_net / denominator * 100.0 if denominator and big_net is not None else None,
        "super_inflow_ratio": (
            super_net / denominator * 100.0
            if denominator and super_net is not None
            else None
        ),
        "provider_symbol": str(today.get("stockCode") or ""),
        "vendor_rank": str(today.get("rank") or ""),
    }


def _tencent_symbol(value: Any) -> str:
    symbol = _normalize_symbol(value)
    if not symbol:
        raise CapitalFlowError("Tencent capital-flow symbol invalid")
    code, exchange = symbol.split(".", 1)
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(exchange)
    if prefix is None:
        raise CapitalFlowError("Tencent capital-flow exchange unsupported")
    return f"{prefix}{code}"


def _records(value: Any) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if hasattr(value, "to_dict"):
        try:
            value = value.to_dict(orient="records")
        except TypeError:
            value = value.to_dict("records")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(item for item in value if isinstance(item, Mapping))
    raise CapitalFlowError("capital-flow provider result must be a record collection")


def _pick(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _pick_flow(row: Mapping[str, Any], window: str, *suffixes: str) -> Any:
    prefix = {"today": "今日", "3d": "3日", "5d": "5日", "10d": "10日"}[window]
    aliases: list[str] = []
    for suffix in suffixes:
        aliases.extend((f"{prefix}{suffix}", suffix))
    return _pick(row, *aliases)


def _normalize_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", raw):
        return raw
    raw_digits = re.sub(r"\D", "", raw)
    if not raw_digits or len(raw_digits) > 6:
        return ""
    digits = raw_digits.zfill(6)
    if not re.fullmatch(r"\d{6}", digits) or digits == "000000":
        return ""
    exchange = "SH" if digits.startswith(("5", "6", "9")) else "BJ" if digits.startswith(("4", "8")) else "SZ"
    return f"{digits}.{exchange}"


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"-", "--", "None", "nan", "NaN"}:
        return None
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier, text = 100_000_000.0, text[:-1]
    elif text.endswith("万"):
        multiplier, text = 10_000.0, text[:-1]
    try:
        number = float(text) * multiplier
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _percentiles(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted((float(value), symbol) for symbol, value in values.items() if math.isfinite(float(value)))
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][1]: 50.0}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = (index + end - 1) / 2.0
        score = average_rank / (len(ordered) - 1) * 100.0
        for _value, symbol in ordered[index:end]:
            result[symbol] = round(score, 4)
        index = end
    return result


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CapitalFlowError("A2 market timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _parse_datetime(value: Any) -> datetime | None:
    """Parse a persisted timestamp without ever assuming a local timezone."""

    if isinstance(value, datetime):
        candidate = value
    elif isinstance(value, str) and value.strip():
        try:
            candidate = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if candidate.tzinfo is None or candidate.utcoffset() is None:
        return None
    return candidate.astimezone(SHANGHAI)


def _provider_timestamp_date(value: Any) -> date | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if not math.isfinite(timestamp) or timestamp <= 0:
            return None
        if timestamp > 100_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.fromtimestamp(timestamp, SHANGHAI).date()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{14}", text):
        try:
            return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI).date()
        except ValueError:
            return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            return _provider_timestamp_date(float(text))
        except ValueError:
            return None
    parsed = _parse_datetime(text)
    if parsed is not None:
        return parsed.date()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _capital_flow_provider_dates(frames: Mapping[str, Any]) -> dict[str, tuple[date, ...]]:
    result: dict[str, tuple[date, ...]] = {}
    for window, raw in frames.items():
        try:
            rows = _records(raw)
        except CapitalFlowError:
            continue
        dates = {
            parsed
            for row in rows
            if (parsed := _provider_timestamp_date(_pick(row, "provider_timestamp", "f124")))
            is not None
        }
        if dates:
            result[str(window)] = tuple(sorted(dates))
    return result


def _board_flow_provider_dates(raw: Any) -> tuple[date, ...]:
    rows, _total, _malformed = _board_records(raw)
    dates = {
        parsed
        for row in rows
        if (parsed := _provider_timestamp_date(_pick(row, "provider_timestamp", "f124")))
        is not None
    }
    return tuple(sorted(dates))


def _provider_dates_prove_trade_date(
    provider_dates: Mapping[str, Sequence[date]],
    target_date: date,
    *,
    require_today: bool = False,
) -> bool:
    if require_today and not provider_dates.get("today"):
        return False
    observed = [item for dates in provider_dates.values() for item in dates]
    return bool(observed) and all(item == target_date for item in observed)


def _with_provider_date_proof(
    snapshot: Mapping[str, Any],
    *,
    provider_dates: Mapping[str, Sequence[date]],
    target_date: date,
    historical_recovery: bool,
) -> dict[str, Any]:
    result = dict(snapshot)
    result.pop("content_hash", None)
    result["provider_trade_dates"] = {
        key: [item.isoformat() for item in dates]
        for key, dates in sorted(provider_dates.items())
    }
    result["provider_trade_date_verified"] = _provider_dates_prove_trade_date(
        provider_dates,
        target_date,
        require_today="today" in provider_dates,
    )
    result["historical_recovery"] = bool(historical_recovery)
    result["content_hash"] = _content_hash(result)
    return result


def _validate_dataset(value: Any) -> str:
    dataset = str(value or "").strip()
    if not _DATASET_RE.fullmatch(dataset):
        raise ValueError("A2 fact dataset must be a safe filename component")
    return dataset


def _validate_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        raise ValueError("A2 fact trade_date must be an ISO date")
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("A2 fact trade_date must be a calendar date") from exc
    return text


def _jsonable(value: Any) -> Any:
    """Convert provider/Pydantic rows to JSON-compatible values safely."""

    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json")
        except TypeError:
            return value.model_dump()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _aware(value).isoformat()
    return value


def _infer_fact_state(payload: Mapping[str, Any], *, reason_code: str = "OK") -> str:
    explicit = str(payload.get("availability_state") or "").strip().upper()
    if explicit in _A2_FACT_STATES:
        return explicit
    available = payload.get("available")
    records = payload.get("records")
    if available is False:
        return _state_for_reason(reason_code)
    if isinstance(records, list):
        return OBSERVED_VALUE if records else OBSERVED_EMPTY
    if available is True:
        return OBSERVED_VALUE
    return MALFORMED


def _unavailable_trade_date_fact(
    *,
    dataset: str,
    trade_date: str,
    as_of: datetime,
    source_id: str,
    source_kind: str,
    published_scope: str,
    state: str,
    reason_code: str,
) -> dict[str, Any]:
    safe_dataset = _validate_dataset(dataset)
    safe_date = _validate_trade_date(trade_date)
    state = state if state in _A2_FACT_STATES else SOURCE_UNAVAILABLE
    payload: dict[str, Any] = {
        "schema_version": A2_MARKET_FACT_SCHEMA,
        "dataset": safe_dataset,
        "trade_date": safe_date,
        "as_of": _aware(as_of).isoformat(),
        "event_time": _aware(as_of).isoformat(),
        "fetch_time": None,
        "ingested_at": None,
        "source_id": str(source_id or "UNKNOWN"),
        "source_kind": str(source_kind or "UNKNOWN"),
        "published_scope": str(published_scope or "SYMBOL"),
        "availability_state": state,
        "available": state in {OBSERVED_VALUE, OBSERVED_EMPTY},
        "reason_code": str(reason_code or state),
        "records": [],
        "payload": {},
        "metadata": {"historical_read_only": True},
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


def _normalize_collected_fact(
    result: Any,
    *,
    dataset: str,
    trade_date: str,
    as_of: datetime,
    source_id: str,
    source_kind: str,
    published_scope: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    raw = _jsonable(result)
    metadata: dict[str, Any] = {}
    fetched = ingested_at
    event = _aware(as_of)
    if isinstance(raw, Mapping):
        raw_map = dict(raw)
        metadata_value = raw_map.get("metadata")
        metadata = dict(metadata_value) if isinstance(metadata_value, Mapping) else {}
        fetched = _parse_datetime(raw_map.get("fetch_time")) or _parse_datetime(raw_map.get("fetched_at")) or fetched
        event = _parse_datetime(raw_map.get("event_time")) or _parse_datetime(raw_map.get("as_of")) or event
        records_value = raw_map.get("records")
        if records_value is None:
            records_value = raw_map.get("items")
        if records_value is None:
            records_value = raw_map.get("rows")
        if records_value is None and isinstance(raw_map.get("payload"), Mapping):
            records_value = raw_map["payload"].get("records")
        if records_value is None:
            records: list[dict[str, Any]] | None = None
        elif isinstance(records_value, Sequence) and not isinstance(records_value, (str, bytes, bytearray)):
            records = [dict(item) for item in records_value if isinstance(item, Mapping)]
            if len(records) != len(records_value):
                return _unavailable_trade_date_fact(
                    dataset=dataset,
                    trade_date=trade_date,
                    as_of=as_of,
                    source_id=source_id,
                    source_kind=source_kind,
                    published_scope=published_scope,
                    state=MALFORMED,
                    reason_code="FACT_RECORDS_MALFORMED",
                )
        else:
            return _unavailable_trade_date_fact(
                dataset=dataset,
                trade_date=trade_date,
                as_of=as_of,
                source_id=source_id,
                source_kind=source_kind,
                published_scope=published_scope,
                state=MALFORMED,
                reason_code="FACT_RECORDS_MALFORMED",
            )
        ok = raw_map.get("ok")
        complete = raw_map.get("complete")
        if ok is False or complete is False or raw_map.get("available") is False:
            state = _state_for_reason(raw_map.get("reason_code") or "SOURCE_UNAVAILABLE")
            # Hithink may expose rows from pages fetched before a later page
            # failed.  Keep the failure metadata, but do not let partial rows
            # enter a downstream sector join as if they were complete facts.
            records = []
        elif records is None:
            return _unavailable_trade_date_fact(
                dataset=dataset,
                trade_date=trade_date,
                as_of=as_of,
                source_id=source_id,
                source_kind=source_kind,
                published_scope=published_scope,
                state=MALFORMED,
                reason_code="FACT_RECORDS_MISSING",
            )
        else:
            state = OBSERVED_VALUE if records else OBSERVED_EMPTY
        reason_code = str(raw_map.get("reason_code") or ("OK" if state in {OBSERVED_VALUE, OBSERVED_EMPTY} else state))
        if state in {OBSERVED_VALUE, OBSERVED_EMPTY} and raw_map.get("availability_state") in _A2_FACT_STATES:
            state = str(raw_map["availability_state"])
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        records = [dict(item) for item in raw if isinstance(item, Mapping)]
        if len(records) != len(raw):
            state, reason_code = MALFORMED, "FACT_RECORDS_MALFORMED"
        else:
            state = OBSERVED_VALUE if records else OBSERVED_EMPTY
            reason_code = "OK" if records else "SOURCE_EMPTY"
    else:
        state, reason_code, records = MALFORMED, "FACT_PAYLOAD_MALFORMED", []
    return {
        "schema_version": A2_MARKET_FACT_SCHEMA,
        "dataset": _validate_dataset(dataset),
        "trade_date": _validate_trade_date(trade_date),
        "as_of": _aware(as_of).isoformat(),
        "event_time": event.isoformat(),
        "fetch_time": fetched.isoformat(),
        "ingested_at": _aware(ingested_at).isoformat(),
        "source_id": str(source_id or "UNKNOWN"),
        "source_kind": str(source_kind or "UNKNOWN"),
        "published_scope": str(published_scope or "SYMBOL"),
        "availability_state": state,
        "available": state in {OBSERVED_VALUE, OBSERVED_EMPTY},
        "reason_code": reason_code,
        "records": records,
        "payload": raw if isinstance(raw, Mapping) else {"records": records},
        "metadata": metadata,
    }


def _read_trade_date_fact(
    path: Path,
    *,
    dataset: str,
    trade_date: str,
    now: datetime | None,
    max_age_seconds: float | None,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    if not path.is_file():
        return None, None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None, MALFORMED, "FACT_CACHE_JSON_MALFORMED"
    if not isinstance(payload, dict):
        return None, MALFORMED, "FACT_CACHE_ENVELOPE_MALFORMED"
    if (
        payload.get("schema_version") != A2_MARKET_FACT_SCHEMA
        or payload.get("dataset") != dataset
        or payload.get("trade_date") != trade_date
    ):
        return None, MALFORMED, "FACT_CACHE_IDENTITY_MISMATCH"
    state = str(payload.get("availability_state") or OBSERVED_VALUE)
    if state not in _A2_FACT_STATES:
        return None, MALFORMED, "FACT_CACHE_STATE_MALFORMED"
    records = payload.get("records")
    if not isinstance(records, list) or any(not isinstance(item, Mapping) for item in records):
        return None, MALFORMED, "FACT_CACHE_RECORDS_MALFORMED"
    expected = str(payload.get("content_hash") or "")
    body = dict(payload)
    body.pop("content_hash", None)
    if not expected or expected != _content_hash(body):
        return None, MALFORMED, "FACT_CACHE_HASH_MISMATCH"
    if max_age_seconds is not None:
        if isinstance(max_age_seconds, bool) or float(max_age_seconds) < 0:
            raise ValueError("max_age_seconds must be non-negative")
        ingested = _parse_datetime(payload.get("ingested_at"))
        if ingested is None:
            return None, MALFORMED, "FACT_CACHE_TIMESTAMP_MALFORMED"
        reference = _aware(now or datetime.now(SHANGHAI))
        if (reference - ingested).total_seconds() > float(max_age_seconds):
            return None, STALE, "FACT_CACHE_STALE"
    return payload, state, str(payload.get("reason_code") or "OK")


def _failure_state(reason_code: Any) -> str:
    reason = str(reason_code or "").upper()
    if any(token in reason for token in ("MALFORM", "INVALID", "ENVELOPE")):
        return MALFORMED
    if "STALE" in reason:
        return STALE
    return SOURCE_UNAVAILABLE


def _state_for_reason(reason_code: Any) -> str:
    reason = str(reason_code or "").upper()
    if reason in {"SOURCE_EMPTY", "OBSERVED_EMPTY", "NO_RECORDS"}:
        return OBSERVED_EMPTY
    if reason == "SOURCE_NOT_CONFIGURED":
        return SOURCE_UNAVAILABLE
    return _failure_state(reason)


def _content_hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = [
    "A2_MARKET_FACT_SCHEMA",
    "BOARD_FLOW_PROVIDER",
    "BOARD_FLOW_SCHEMA",
    "CAPITAL_FLOW_PROVIDER",
    "CAPITAL_FLOW_SCHEMA",
    "CapitalFlowError",
    "MALFORMED",
    "OBSERVED_EMPTY",
    "OBSERVED_VALUE",
    "OUTSIDE_SCOPE",
    "SOURCE_UNAVAILABLE",
    "STALE",
    "build_board_capital_flow_snapshot",
    "build_capital_flow_snapshot",
    "collect_eastmoney_board_flow",
    "collect_eastmoney_capital_flow",
    "collect_tencent_capital_flow",
    "collect_ths_market_fact",
    "collect_trade_date_fact",
    "inspect_board_capital_flow_snapshot",
    "inspect_trade_date_fact",
    "load_board_capital_flow_snapshot",
    "load_capital_flow_snapshot",
    "load_trade_date_fact",
    "persist_ths_market_fact",
    "unavailable_capital_flow_snapshot",
    "with_capital_flow_provider_attempts",
    "write_board_capital_flow_snapshot",
    "write_capital_flow_snapshot",
    "write_trade_date_fact",
]
