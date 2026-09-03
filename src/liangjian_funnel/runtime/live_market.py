"""Current-session market state for A4 entry risk.

A3 owns next-session stock plans and prior-session position guidance.  This
module is the only bridge from current-session market facts to A4's entry
permission.  It deliberately uses a few transparent conditions instead of a
composite score.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..pipeline.data_source import HithinkClient
from ..reporting import atomic_write_json


SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "a4-live-market-state/1.0.0"
_INDEX_SYMBOLS = ("000001.SH", "399001.SZ", "399006.SZ", "000300.SH")


def classify_full_market(
    records: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
    expected_count: int | None = None,
    source: str = "HITHINK_FULL_MARKET",
) -> dict[str, Any]:
    """Classify fresh full-market rows with auditable, non-scored rules."""

    current = _aware(as_of)
    changes = [value for row in records if (value := _change(row)) is not None]
    expected = max(len(records), int(expected_count or 0))
    coverage = len(changes) / expected if expected else 0.0
    if len(changes) < 500 or coverage < 0.75:
        return _unavailable(
            current,
            "A4_LIVE_MARKET_COVERAGE_INSUFFICIENT",
            source=source,
            observed=len(changes),
            expected=expected,
            coverage=coverage,
        )
    advances = sum(value > 0 for value in changes)
    declines = sum(value < 0 for value in changes)
    flats = len(changes) - advances - declines
    breadth = advances / (advances + declines) if advances + declines else 0.5
    median_change = _median(changes)

    # A hard entry stop requires two independent current-session symptoms.
    # Ordinary weak/rotation sessions remain CAUTION and can still execute a
    # valid A3 plan at reduced priority/size.
    if breadth <= 0.20 and median_change <= -1.5:
        regime = "PANIC"
        permission = "BLOCK_NEW_ENTRY"
        reason = "A4_LIVE_SYSTEMIC_SELL_OFF"
        position_cap = 0.0
    elif breadth < 0.45 or median_change < 0:
        regime = "WEAK_ROTATION"
        permission = "CAUTION"
        reason = "A4_LIVE_MARKET_CAUTION"
        position_cap = 0.5
    else:
        regime = "NORMAL_REPAIR"
        permission = "ALLOW"
        reason = "A4_LIVE_MARKET_ALLOWS_PLAN_CHECK"
        position_cap = 0.7
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "READY",
        "reason_code": reason,
        "source": source,
        "as_of": current.isoformat(),
        "trade_date": current.date().isoformat(),
        "current_session": True,
        "entry_permission": permission,
        "regime": regime,
        "suggested_position_cap_pct": position_cap,
        "breadth": round(breadth, 6),
        "median_change_pct": round(median_change, 6),
        "advances": advances,
        "declines": declines,
        "flats": flats,
        "observed_count": len(changes),
        "expected_count": expected,
        "coverage": round(coverage, 6),
        "fallback": False,
    }


def classify_index_fallback(
    quotes: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    """Fallback when full-market breadth is unavailable.

    Index-only evidence can identify an extreme synchronized sell-off, but it
    is labelled degraded and never claims to provide market breadth.
    """

    current = _aware(as_of)
    changes = [value for row in quotes if (value := _quote_change(row)) is not None]
    if len(changes) < 3:
        return _unavailable(
            current,
            "A4_LIVE_MARKET_SOURCE_UNAVAILABLE",
            source="TENCENT_INDEX_FALLBACK",
            observed=len(changes),
            expected=len(_INDEX_SYMBOLS),
            coverage=len(changes) / len(_INDEX_SYMBOLS),
        )
    median_change = _median(changes)
    all_negative = all(value < 0 for value in changes)
    if all_negative and median_change <= -2.0:
        regime = "PANIC"
        permission = "BLOCK_NEW_ENTRY"
        reason = "A4_LIVE_INDEX_SYSTEMIC_SELL_OFF"
        position_cap = 0.0
    elif median_change < 0:
        regime = "WEAK_ROTATION"
        permission = "CAUTION"
        reason = "A4_LIVE_INDEX_CAUTION"
        position_cap = 0.5
    else:
        regime = "NORMAL_REPAIR"
        permission = "ALLOW"
        reason = "A4_LIVE_INDEX_ALLOWS_PLAN_CHECK"
        position_cap = 0.6
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "READY_DEGRADED",
        "reason_code": reason,
        "source": "TENCENT_INDEX_FALLBACK",
        "as_of": current.isoformat(),
        "trade_date": current.date().isoformat(),
        "current_session": True,
        "entry_permission": permission,
        "regime": regime,
        "suggested_position_cap_pct": position_cap,
        "breadth": None,
        "median_change_pct": round(median_change, 6),
        "observed_count": len(changes),
        "expected_count": len(_INDEX_SYMBOLS),
        "coverage": round(len(changes) / len(_INDEX_SYMBOLS), 6),
        "fallback": True,
    }


def load_or_refresh_live_market_state(
    settings: Any,
    market_data: Any,
    *,
    as_of: datetime,
) -> dict[str, Any]:
    """Read one five-minute file bucket or collect a new bounded snapshot."""

    current = _aware(as_of).replace(second=0, microsecond=0)
    bucket_minute = current.minute - current.minute % 5
    bucket = current.replace(minute=bucket_minute)
    path = Path(settings.fact_store_dir) / "a4_live_market" / current.date().isoformat() / f"{bucket.strftime('%H%M')}.json"
    cached = _read_cache(path, current=current)
    if cached is not None:
        return cached

    state: dict[str, Any] | None = None
    try:
        with HithinkClient(settings) as client:
            result = client.market_snapshot(limit=1000, max_pages=10)
        if result.ok and result.complete:
            rows = [item.model_dump(mode="json") for item in result.items]
            state = classify_full_market(
                rows,
                as_of=current,
                expected_count=result.total,
            )
    except Exception:
        state = None

    if state is None or state.get("status") != "READY":
        quotes: list[dict[str, Any]] = []
        for symbol in _INDEX_SYMBOLS:
            try:
                result = market_data.fetch_quote(symbol, as_of=current, max_age_seconds=180.0)
                quote = result.quote if result.complete else None
            except Exception:
                quote = None
            if quote is not None:
                quotes.append(quote.model_dump(mode="json"))
        fallback = classify_index_fallback(quotes, as_of=current)
        if fallback.get("status") in {"READY", "READY_DEGRADED"}:
            state = fallback
        elif state is None:
            state = fallback

    state = dict(state or _unavailable(current, "A4_LIVE_MARKET_SOURCE_UNAVAILABLE"))
    state["cache_bucket"] = bucket.isoformat()
    try:
        atomic_write_json(path, state)
    except OSError:
        state["cache_write_reason_code"] = "A4_LIVE_MARKET_CACHE_WRITE_FAILED"
    return state


def _read_cache(path: Path, *, current: datetime) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        observed = _aware(datetime.fromisoformat(str(raw.get("as_of") or "")))
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return None
    if raw.get("schema_version") != SCHEMA_VERSION:
        return None
    if observed.date() != current.date() or abs((current - observed).total_seconds()) > 5 * 60:
        return None
    return dict(raw)


def _unavailable(
    current: datetime,
    reason: str,
    *,
    source: str = "NONE",
    observed: int = 0,
    expected: int = 0,
    coverage: float = 0.0,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "DATA_BLOCKED",
        "reason_code": reason,
        "source": source,
        "as_of": current.isoformat(),
        "trade_date": current.date().isoformat(),
        "current_session": True,
        "entry_permission": "UNKNOWN",
        "regime": "UNRESOLVED",
        "suggested_position_cap_pct": None,
        "breadth": None,
        "median_change_pct": None,
        "observed_count": observed,
        "expected_count": expected,
        "coverage": round(coverage, 6),
        "fallback": source != "NONE",
    }


def _change(row: Mapping[str, Any]) -> float | None:
    for key in (
        "change_ratio_pct",
        "price_change_ratio_pct",
        "change_pct",
        "pct_change",
        "pct_chg",
        "涨跌幅",
    ):
        value = _number(row.get(key))
        if value is not None:
            return value
    price = _number(row.get("last_price") or row.get("price"))
    previous = _number(row.get("prev_price") or row.get("previous_close"))
    return ((price / previous) - 1.0) * 100.0 if price and previous and previous > 0 else None


def _quote_change(row: Mapping[str, Any]) -> float | None:
    price = _number(row.get("price"))
    previous = _number(row.get("previous_close"))
    return ((price / previous) - 1.0) * 100.0 if price and previous and previous > 0 else None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("AS_OF_TIMEZONE_REQUIRED")
    return value.astimezone(SHANGHAI)


__all__ = [
    "SCHEMA_VERSION",
    "classify_full_market",
    "classify_index_fallback",
    "load_or_refresh_live_market_state",
]
