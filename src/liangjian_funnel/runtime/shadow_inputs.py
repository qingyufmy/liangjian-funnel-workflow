"""Publisher-only closed daily observations; no execution authority or I/O."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from typing import Any

from ..pipeline.factors import _canonical_symbol, _daily_bars, _row_datetime
from ..pipeline.local_fact_cache import canonical_json_hash
from .strategies import _daily_context

SCHEMA = "a4-shadow-inputs/1"
ALGORITHM = "raw-daily-ma5-wilder-tr14/1"


def limited_shadow_inputs(code: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA, "status": "DATA_LIMITED",
        "daily_ma5": None, "atr14": None, "previous_daily_closes": None,
        "previous_daily_close_dates": None, "daily_as_of": None,
        "daily_as_of_semantics": "SOURCE_OBSERVATION_AS_OF",
        "last_closed_daily_bar_end": None,
        "atr_source_hash": None, "adjust": "none",
        "atr_method": "WILDER_TR14_SEED_MEAN", "version": ALGORITHM,
        "execution_authority": False, "gap_codes": [code],
    }


def _aware(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("TIME_NOT_AWARE")
    return parsed


def _positive(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("INDICATOR_INVALID")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("INDICATOR_INVALID")
    return number


def build_shadow_inputs(
    *, symbol: str, rows: Sequence[Mapping[str, Any]], source_as_of: datetime,
    bar_cutoff: datetime, target_trade_date: date, production_plan: Mapping[str, Any],
    expected_close_dates: Sequence[date],
) -> dict[str, Any]:
    """Validate real LocalFactCache envelopes, then compute bounded raw history.

    ``expected_close_dates`` must be the actual exchange calendar's latest four
    prior sessions. Neither the factor parser's silent filtering nor a caller's
    positive closed marker can manufacture an available observation.
    """
    code = "DAILY_SOURCE_INVALID"
    try:
        source_as_of, bar_cutoff = _aware(source_as_of), _aware(bar_cutoff)
        if (not isinstance(target_trade_date, date) or isinstance(target_trade_date, datetime)
                or bar_cutoff > source_as_of or source_as_of.date() > target_trade_date
                or bar_cutoff.date() >= target_trade_date):
            raise ValueError(code)
        if _canonical_symbol(symbol) != symbol or _canonical_symbol(production_plan.get("symbol")) != symbol:
            raise ValueError(code)
        expected = list(expected_close_dates)
        if len(expected) != 4 or expected != sorted(set(expected)) or any(day >= target_trade_date for day in expected):
            raise ValueError(code)
        payloads = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError(code)
            payload = row.get("payload")
            if not isinstance(payload, Mapping) or _canonical_symbol(row.get("symbol")) != symbol:
                raise ValueError(code)
            identities = [payload[key] for key in ("symbol", "thscode", "ticker", "code") if payload.get(key) is not None]
            if not identities or any(_canonical_symbol(value) != symbol for value in identities):
                raise ValueError(code)
            if row.get("adjust") != "none" or payload.get("adjust", "none") != "none":
                raise ValueError("ADJUSTMENT_BASIS_UNPROVEN")
            fetched, timestamp = _aware(row.get("fetched_at")), _aware(row.get("timestamp"))
            end = _row_datetime(payload, daily=True)
            if (fetched > source_as_of or end is None or end > bar_cutoff or end > source_as_of
                    or timestamp.date() != end.date() or timestamp > source_as_of
                    or payload.get("closed") is False or payload.get("is_closed") is False):
                raise ValueError(code)
            if row.get("content_hash") != canonical_json_hash(payload):
                raise ValueError("SOURCE_ROW_HASH_MISMATCH")
            payloads.append(payload)
        bars, reasons = _daily_bars(payloads, symbol, bar_cutoff)
        if reasons or len(bars) != len(rows) or len(bars) < 15:
            raise ValueError("DAILY_HISTORY_INCOMPLETE")
        if bars[-1].end != bar_cutoff or [bar.end.date() for bar in bars[-4:]] != expected:
            raise ValueError("LATEST_FOUR_SESSIONS_UNPROVEN")
        ma5 = sum(bar.close for bar in bars[-5:]) / 5
        context = _daily_context(production_plan)
        code = "FROZEN_MA5_CONFLICT"
        if not math.isclose(_positive(context.get("ma5")), ma5, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError(code)
        ranges = [max(bar.high - bar.low, abs(bar.high - bars[i-1].close),
                      abs(bar.low - bars[i-1].close)) for i, bar in enumerate(bars) if i]
        atr = sum(ranges[:14]) / 14
        for value in ranges[14:]:
            atr = (atr * 13 + value) / 14
        _positive(atr)
        code = "FROZEN_ATR_CONFLICT"
        if context.get("atr14") is not None and not math.isclose(_positive(context["atr14"]), atr, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError(code)
        # Bind ORIGINAL envelopes, not only normalized bars. No source payload
        # or model text is repeated in the published projection.
        code = "SOURCE_BINDING_INVALID"
        rows_hash = hashlib.sha256(json.dumps(list(rows), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
        binding = {"symbol": symbol, "source_rows_hash": rows_hash, "adjust": "none",
            "algorithm": ALGORITHM, "source_as_of": source_as_of.isoformat(),
            "bar_cutoff": bar_cutoff.isoformat(), "target_trade_date": target_trade_date.isoformat(),
            "seed_start": bars[0].end.isoformat(), "source_rows_count": len(rows)}
        result = limited_shadow_inputs("")
        result.update(status="AVAILABLE", daily_ma5=ma5, atr14=atr,
            previous_daily_closes=[bar.close for bar in bars[-4:]],
            previous_daily_close_dates=[day.isoformat() for day in expected],
            daily_as_of=source_as_of.isoformat(), atr_source_hash=canonical_json_hash(binding),
            last_closed_daily_bar_end=bar_cutoff.isoformat(),
            source_ref=binding, gap_codes=[])
        return result
    except (ValueError, TypeError, KeyError, OverflowError):
        # Fixed codes only: exceptions may contain raw source/model text.
        return limited_shadow_inputs(code)


def shadow_inputs_for_publication(
    *, cache: Any, calendar: Any, production_plan: Mapping[str, Any],
    snapshot_data: Mapping[str, Any] | None, target_trade_date: date, observed_at: datetime,
) -> dict[str, Any]:
    """Read only the existing local cache, constrained by the frozen factor.

    Does not instantiate Settings/RuntimeStore or acquire missing market data.
    The cache query uses the same raw/800-bar history bound as the A3 enricher.
    """
    try:
        if production_plan.get("strategy_profile") == "LEADER_INTRADAY":
            return limited_shadow_inputs("SHADOW_PROFILE_NOT_APPLICABLE")
        if cache is None or calendar is None:
            return limited_shadow_inputs("LOCAL_DAILY_SOURCE_UNAVAILABLE")
        if not isinstance(snapshot_data, Mapping):
            return limited_shadow_inputs("FROZEN_FACTOR_MISSING")
        symbol = str(production_plan.get("symbol") or "")
        factor = snapshot_data.get("FACTOR_SNAPSHOT", {}).get(symbol)
        if not isinstance(factor, Mapping) or factor.get("symbol") != symbol:
            return limited_shadow_inputs("FROZEN_FACTOR_MISSING")
        source_as_of = _aware(factor["as_of"])
        latest = factor["timeframes"]["daily"]["latest"]
        if latest.get("symbol") != symbol:
            return limited_shadow_inputs("FROZEN_FACTOR_IDENTITY_CONFLICT")
        cutoff = _aware(latest["end"])
        manifest_as_of = _aware(snapshot_data["snapshot_manifest"]["as_of"])
        if source_as_of > manifest_as_of or source_as_of > _aware(observed_at) or cutoff > source_as_of:
            return limited_shadow_inputs("FROZEN_SOURCE_TIME_CONFLICT")
        day, previous = target_trade_date, []
        for _ in range(4):
            day = calendar.previous_trading_day(day)
            previous.append(day)
        rows = cache.query_daily_bars(symbol, adjust="none", end=cutoff + timedelta(microseconds=1),
            as_of=source_as_of, limit=800, descending=True)
        return build_shadow_inputs(symbol=symbol, rows=list(reversed(rows)), source_as_of=source_as_of,
            bar_cutoff=cutoff, target_trade_date=target_trade_date, production_plan=production_plan,
            expected_close_dates=list(reversed(previous)))
    except Exception:
        # Shadow evidence can fail, but must not change original publication.
        return limited_shadow_inputs("LOCAL_DAILY_SOURCE_FAILED")
