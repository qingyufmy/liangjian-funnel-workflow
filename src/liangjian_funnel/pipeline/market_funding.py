"""Deterministic market funding regime from closed daily turnover bars.

This module intentionally measures only a price-volume proxy.  Turnover is
not capital-flow data and the resulting regime is context for execution, not
an execution gate.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
MARKET_FUNDING_SCHEMA = "market-funding/1.0.0"
_EXPANSION_BREADTH = 0.55


def build_market_funding_regime(
    universe_records: Sequence[Any],
    daily_bars: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    as_of: datetime | date,
    lookback_sessions: int = 5,
    min_coverage: float = 0.85,
    expansion_ratio: float = 1.08,
    contraction_ratio: float = 0.92,
) -> dict[str, Any]:
    """Classify the market turnover regime using one comparable symbol pool.

    Bars are grouped by local trading date and only bars on or before the
    ``as_of`` date are eligible.  The latest eligible date is never silently
    replaced with an older date when its coverage is insufficient.  Historical
    sessions are selected from the same symbol intersection as the latest
    session, so the latest total and baseline cannot compare changing pools.
    """

    lookback, minimum, expansion, contraction = _validate_parameters(
        lookback_sessions=lookback_sessions,
        min_coverage=min_coverage,
        expansion_ratio=expansion_ratio,
        contraction_ratio=contraction_ratio,
    )
    cutoff_date, as_of_value = _normalise_as_of(as_of)

    universe = _universe_snapshot(universe_records)
    symbols = universe["symbols"]
    symbol_set = set(symbols)
    breadth = universe["breadth"]
    breadth_coverage = universe["breadth_coverage"]
    parsed, parse_stats = _normalise_daily_bars(daily_bars, symbol_set, cutoff_date)
    by_day = _by_day(parsed)
    all_days = sorted(by_day)
    latest_day = all_days[-1] if all_days else None
    latest_values = by_day.get(latest_day, {}) if latest_day is not None else {}
    latest_raw_coverage = _coverage(len(latest_values), len(symbols))

    reason_codes: list[str] = []
    data_gaps: list[str] = []
    if parse_stats["future_bars_dropped"]:
        data_gaps.append("FUTURE_BARS_EXCLUDED")
    if parse_stats["malformed_bars"]:
        data_gaps.append("MALFORMED_BARS_IGNORED")
    if parse_stats["invalid_amount_bars"]:
        data_gaps.append("INVALID_AMOUNT_BARS_IGNORED")
    if parse_stats["out_of_universe_symbols"]:
        data_gaps.append("OUT_OF_UNIVERSE_BARS_IGNORED")
    if parse_stats["invalid_symbol_keys"]:
        data_gaps.append("INVALID_SYMBOL_KEYS_IGNORED")
    if universe["missing_symbol_records"]:
        data_gaps.append("UNIVERSE_SYMBOLS_MISSING")

    comparable_symbols = set(latest_values)
    latest_total = _sum_values(latest_values, comparable_symbols)
    selected_days: list[date] = []
    skipped_days: list[date] = []
    baseline_totals: list[float] = []

    if not symbols:
        reason_codes.append("UNIVERSE_EMPTY")
    if breadth is None:
        reason_codes.append("BREADTH_UNAVAILABLE")
        data_gaps.append("BREADTH_CHANGE_RATIO_MISSING")
    elif breadth_coverage < minimum:
        reason_codes.append("BREADTH_COVERAGE_INSUFFICIENT")
        data_gaps.append("BREADTH_COVERAGE_BELOW_THRESHOLD")

    if latest_day is None:
        reason_codes.append("LATEST_SESSION_UNAVAILABLE")
        data_gaps.append("DAILY_BARS_MISSING")
    elif latest_raw_coverage < minimum:
        reason_codes.append("LATEST_SESSION_COVERAGE_INSUFFICIENT")
        data_gaps.append("LATEST_SESSION_COVERAGE_BELOW_THRESHOLD")
    elif lookback > 0:
        # A prior date with insufficient raw coverage is not a usable session.
        # It is skipped, while the latest date remains authoritative.  The
        # final intersection check below still enforces coverage for the exact
        # pool used in every selected total.
        for day in reversed(all_days[:-1]):
            if len(selected_days) >= lookback:
                break
            values = by_day[day]
            if _coverage(len(values), len(symbols)) < minimum:
                skipped_days.append(day)
                continue
            candidate = comparable_symbols.intersection(values)
            if _coverage(len(candidate), len(symbols)) < minimum:
                skipped_days.append(day)
                continue
            comparable_symbols = candidate
            selected_days.append(day)

        if skipped_days:
            data_gaps.append("INCOMPLETE_BASELINE_SESSIONS_SKIPPED")
        if not selected_days:
            reason_codes.append("BASELINE_INSUFFICIENT")
            data_gaps.append("BASELINE_SESSIONS_MISSING")
        else:
            # The latest total is recomputed after the common pool is known.
            latest_total = _sum_values(latest_values, comparable_symbols)
            baseline_totals = [
                total
                for day in selected_days
                if (total := _sum_values(by_day[day], comparable_symbols)) is not None
            ]
            if len(baseline_totals) != len(selected_days):
                reason_codes.append("BASELINE_TOTAL_UNAVAILABLE")
                data_gaps.append("BASELINE_AMOUNT_MISSING")
    elif lookback == 0:
        reason_codes.append("BASELINE_LOOKBACK_ZERO")
        data_gaps.append("BASELINE_SESSIONS_NOT_REQUESTED")

    coverage = _coverage(len(comparable_symbols), len(symbols))
    baseline_total = (
        math.fsum(baseline_totals) / len(baseline_totals)
        if baseline_totals
        else None
    )
    amount_ratio = (
        latest_total / baseline_total
        if latest_total is not None and baseline_total is not None and baseline_total > 0
        else None
    )
    if baseline_total is not None and not math.isfinite(baseline_total):
        baseline_total = None
        amount_ratio = None
        reason_codes.append("BASELINE_TOTAL_INVALID")
        data_gaps.append("BASELINE_TOTAL_NON_FINITE")
    if amount_ratio is None and latest_day is not None and selected_days:
        reason_codes.append("AMOUNT_RATIO_UNAVAILABLE")
        data_gaps.append("BASELINE_TOTAL_ZERO_OR_INVALID")

    available = (
        bool(symbols)
        and latest_day is not None
        and latest_raw_coverage >= minimum
        and coverage >= minimum
        and breadth is not None
        and breadth_coverage >= minimum
        and bool(selected_days)
        and len(baseline_totals) == len(selected_days)
        and latest_total is not None
        and baseline_total is not None
        and amount_ratio is not None
    )
    if available:
        if amount_ratio >= expansion and breadth >= _EXPANSION_BREADTH:
            state = "INCREMENTAL_EXPANSION"
        elif amount_ratio <= contraction:
            state = "LIQUIDITY_CONTRACTION"
        else:
            state = "EXISTING_FUNDS_ROTATION"
        reason_codes.append("OK")
    else:
        state = "UNRESOLVED"

    evidence = {
        "algorithm": MARKET_FUNDING_SCHEMA,
        "scoring_used": False,
        "turnover_metric_role": "PRICE_VOLUME_PROXY_ONLY",
        "execution_context_only": True,
        "expansion_breadth_threshold": _EXPANSION_BREADTH,
        "thresholds": {
            "min_coverage": minimum,
            "expansion_ratio": expansion,
            "contraction_ratio": contraction,
        },
        "universe": {
            "record_count": universe["record_count"],
            "symbol_count": len(symbols),
            "symbols": list(symbols),
            "missing_symbol_records": universe["missing_symbol_records"],
        },
        "breadth": {
            "value": breadth,
            "coverage": breadth_coverage,
            "observed_symbol_count": universe["breadth_observed_count"],
            "advances": universe["advances"],
            "declines": universe["declines"],
            "flats": universe["flats"],
            "formula": "advances / (advances + declines); flats excluded",
        },
        "latest_session": {
            "trade_date": latest_day.isoformat() if latest_day is not None else None,
            "raw_symbol_count": len(latest_values),
            "raw_coverage": latest_raw_coverage,
            "total_amount_on_comparable_pool": latest_total,
        },
        "baseline_sessions": [
            {
                "trade_date": day.isoformat(),
                "raw_symbol_count": len(by_day[day]),
                "raw_coverage": _coverage(len(by_day[day]), len(symbols)),
                "total_amount_on_comparable_pool": (
                    _sum_values(by_day[day], comparable_symbols)
                ),
                "selected": day in selected_days,
            }
            for day in reversed(all_days[:-1])
            if day in selected_days or day in skipped_days
        ],
        "selected_baseline_dates": [day.isoformat() for day in selected_days],
        "skipped_baseline_dates": [day.isoformat() for day in skipped_days],
        "comparable_symbols": sorted(comparable_symbols),
        "comparable_symbol_count": len(comparable_symbols),
        "lookback_sessions_used": len(selected_days),
        "latest_total_amount": latest_total,
        "baseline_total_amount": baseline_total,
        "amount_ratio": amount_ratio,
        "future_bars_dropped": parse_stats["future_bars_dropped"],
        "duplicate_bars_replaced": parse_stats["duplicate_bars_replaced"],
    }

    result = {
        "schema": MARKET_FUNDING_SCHEMA,
        "available": available,
        "state": state,
        "as_of": as_of_value,
        "latest_trade_date": latest_day.isoformat() if latest_day is not None else None,
        "latest_total_amount": latest_total,
        "baseline_total_amount": baseline_total,
        "amount_ratio": amount_ratio,
        "coverage": coverage,
        "comparable_symbol_count": len(comparable_symbols),
        "lookback_sessions_used": len(selected_days),
        "breadth": breadth,
        "breadth_coverage": breadth_coverage,
        "reason_codes": _unique(reason_codes),
        "data_gaps": _unique(data_gaps),
        "evidence": evidence,
        "turnover_is_capital_flow": False,
        "state_is_execution_context_only": True,
    }
    return result


def _validate_parameters(
    *,
    lookback_sessions: int,
    min_coverage: float,
    expansion_ratio: float,
    contraction_ratio: float,
) -> tuple[int, float, float, float]:
    if isinstance(lookback_sessions, bool) or not isinstance(lookback_sessions, int):
        raise ValueError("lookback_sessions must be a non-negative integer")
    if lookback_sessions < 0:
        raise ValueError("lookback_sessions must be a non-negative integer")
    values: list[float] = []
    for name, raw in (
        ("min_coverage", min_coverage),
        ("expansion_ratio", expansion_ratio),
        ("contraction_ratio", contraction_ratio),
    ):
        if isinstance(raw, bool):
            raise ValueError(f"{name} must be finite")
        try:
            number = float(raw)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite") from exc
        if not math.isfinite(number):
            raise ValueError(f"{name} must be finite")
        values.append(number)
    minimum, expansion, contraction = values
    if not 0 < minimum <= 1:
        raise ValueError("min_coverage must be in (0, 1]")
    if expansion <= 0 or contraction <= 0:
        raise ValueError("ratio thresholds must be positive")
    return lookback_sessions, minimum, expansion, contraction


def _normalise_as_of(value: datetime | date) -> tuple[date, str]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.date(), value.isoformat()
        local = value.astimezone(SHANGHAI)
        return local.date(), local.isoformat()
    if isinstance(value, date):
        return value, value.isoformat()
    raise ValueError("as_of must be a date or datetime")


def _universe_snapshot(records: Sequence[Any]) -> dict[str, Any]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        records = ()
    symbols: list[str] = []
    changes: dict[str, float | None] = {}
    missing_symbol_records = 0
    for item in records:
        symbol = _symbol(item)
        if symbol is None:
            missing_symbol_records += 1
            continue
        if symbol not in changes:
            symbols.append(symbol)
            changes[symbol] = _number(_value(item, "change_ratio_pct"))
        elif changes[symbol] is None:
            # Retain one deterministic symbol row, but let a later valid
            # observation repair a missing duplicate field.
            changes[symbol] = _number(_value(item, "change_ratio_pct"))
    observed = [value for value in changes.values() if value is not None]
    advances = sum(value > 0 for value in observed)
    declines = sum(value < 0 for value in observed)
    flats = len(observed) - advances - declines
    breadth = advances / (advances + declines) if advances + declines else (0.5 if observed else None)
    return {
        "symbols": tuple(symbols),
        "record_count": len(records),
        "missing_symbol_records": missing_symbol_records,
        "breadth": breadth,
        "breadth_coverage": _coverage(len(observed), len(symbols)),
        "breadth_observed_count": len(observed),
        "advances": advances,
        "declines": declines,
        "flats": flats,
    }


def _normalise_daily_bars(
    daily_bars: Mapping[str, Sequence[Mapping[str, Any]]],
    universe_symbols: set[str],
    cutoff_date: date,
) -> tuple[dict[str, dict[date, float]], dict[str, int]]:
    stats = {
        "future_bars_dropped": 0,
        "malformed_bars": 0,
        "invalid_amount_bars": 0,
        "out_of_universe_symbols": 0,
        "invalid_symbol_keys": 0,
        "duplicate_bars_replaced": 0,
    }
    parsed: dict[str, dict[date, float]] = {}
    if not isinstance(daily_bars, Mapping):
        return parsed, stats
    for raw_symbol, raw_bars in daily_bars.items():
        symbol = _normalise_symbol(raw_symbol)
        if symbol is None:
            stats["invalid_symbol_keys"] += 1
            continue
        if symbol not in universe_symbols:
            stats["out_of_universe_symbols"] += 1
            continue
        if not isinstance(raw_bars, Sequence) or isinstance(raw_bars, (str, bytes, bytearray)):
            stats["malformed_bars"] += 1
            continue
        series: dict[date, float] = {}
        for raw_bar in raw_bars:
            row = _mapping(raw_bar)
            payload = row.get("payload")
            if not any(key in row for key in ("date", "trade_date", "date_ms")) and isinstance(payload, Mapping):
                row = payload
            if not row:
                stats["malformed_bars"] += 1
                continue
            day = _bar_day(row)
            if day is None:
                stats["malformed_bars"] += 1
                continue
            if day > cutoff_date:
                stats["future_bars_dropped"] += 1
                continue
            amount = _bar_amount(row)
            if amount is None:
                stats["invalid_amount_bars"] += 1
                continue
            if day in series:
                stats["duplicate_bars_replaced"] += 1
            series[day] = amount
        if series:
            parsed[symbol] = series
    return parsed, stats


def _by_day(parsed: Mapping[str, Mapping[date, float]]) -> dict[date, dict[str, float]]:
    result: dict[date, dict[str, float]] = {}
    for symbol, series in parsed.items():
        for day, amount in series.items():
            result.setdefault(day, {})[symbol] = amount
    return result


def _bar_day(row: Mapping[str, Any]) -> date | None:
    for key in ("date", "trade_date", "date_ms", "trade_date_ms"):
        if key in row:
            day = _parse_day(row.get(key))
            if day is not None:
                return day
    return None


def _parse_day(value: Any) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(SHANGHAI).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(
                    text[:10] if pattern != "%Y%m%d" else text[:8], pattern
                ).date()
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            return parsed.astimezone(SHANGHAI).date()
        return parsed.date()
    number = _number(value)
    if number is None or not number.is_integer() or number < 0:
        return None
    integer = int(number)
    if 10_000_000 <= integer < 100_000_000:
        try:
            return datetime.strptime(str(integer), "%Y%m%d").date()
        except ValueError:
            return None
    try:
        seconds = integer / 1000 if integer >= 10_000_000_000 else integer
        return datetime.fromtimestamp(seconds, tz=SHANGHAI).date()
    except (OSError, OverflowError, ValueError):
        return None


def _bar_amount(row: Mapping[str, Any]) -> float | None:
    for key in ("amount", "turnover", "turnover_amount", "成交额"):
        if key not in row:
            continue
        value = _number(row.get(key))
        if value is not None and value >= 0:
            return value
    return None


def _sum_values(values: Mapping[str, float], symbols: set[str]) -> float | None:
    if not symbols or any(symbol not in values for symbol in symbols):
        return None
    total = math.fsum(values[symbol] for symbol in symbols)
    return total if math.isfinite(total) else None


def _coverage(observed: int, expected: int) -> float:
    return observed / expected if expected else 0.0


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump(mode="python")
        except TypeError:
            dumped = model_dump()
        return dumped if isinstance(dumped, Mapping) else {}
    return {}


def _value(value: Any, key: str) -> Any:
    mapping = _mapping(value)
    if mapping:
        return mapping.get(key)
    return getattr(value, key, None)


def _symbol(value: Any) -> str | None:
    mapping = _mapping(value)
    keys = ("symbol", "thscode", "ths_code", "thsCode", "ticker", "security_code", "code")
    for key in keys:
        raw = mapping.get(key) if mapping else getattr(value, key, None)
        symbol = _normalise_symbol(raw)
        if symbol:
            return symbol
    return None


def _normalise_symbol(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


__all__ = ["MARKET_FUNDING_SCHEMA", "build_market_funding_regime"]
