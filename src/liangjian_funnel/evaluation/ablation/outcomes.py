"""Offline counterfactual prices, never virtual-account performance.

Reuse the paper broker's adverse price and full-minute clock without creating
a broker/account/store. Forward labels may provide dated raw close evidence;
their decision-anchored returns cannot be re-anchored to a hypothetical fill.
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any

from ...data.mootdx import MinuteBar, map_symbol
from ...runtime.calendar import ExchangeTradingCalendar, TradingCalendarError
from ...runtime.simulation import (
    SHANGHAI, PaperBroker, SimulationAction, SimulationConfig, _first_complete_bar_end,
)
from ...runtime.stock_trading_rules import stock_trading_rules
from .price_limits import resolve_price_limits


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _lookup(row: Mapping[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value: Any = row
        for key in path:
            value = value.get(key) if isinstance(value, Mapping) else None
        if value is not None:
            return value
    return None


def _at(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("OUTCOME_TIMESTAMP_MUST_BE_AWARE")
    return parsed.astimezone(SHANGHAI)


def _session_minute(at: datetime) -> bool:
    clock = at.time().replace(tzinfo=None)
    return time(9, 30) < clock <= time(11, 30) or time(13) < clock <= time(15)


def _executable(bar: MinuteBar, raw: Mapping[str, Any], *, buy: bool,
                limit_evidence: Mapping[str, Any]) -> str | None:
    if bar.evidence_kind != "MARKET_BAR" or bar.source_id.endswith(":RISK_ONLY"):
        return "FILL_EVIDENCE_INVALID"
    if limit_evidence['status'] != 'KNOWN':
        return limit_evidence['reason_code']
    upper, lower = limit_evidence['upper'], limit_evidence['lower']
    # Compare exchange ticks exactly; derivation is independently evidence-gated.
    rules = stock_trading_rules(bar.symbol)
    if buy and bar.open == bar.high == bar.low == bar.close == rules.adverse_tick(upper, buy=True):
        return "LIMIT_UP_LOCKED"
    if not buy and bar.open == bar.high == bar.low == bar.close == rules.adverse_tick(lower, buy=False):
        return "LIMIT_DOWN_LOCKED"
    if bar.low < lower or bar.high > upper:
        return "BAR_OUTSIDE_PRICE_LIMITS"
    if bar.volume <= 0 or bar.high <= bar.low:
        return "BAR_NOT_EXECUTABLE"
    return None


def _price(symbol: str, bar: MinuteBar, *, buy: bool, config: SimulationConfig) -> float | None:
    # _adverse_price is stateless apart from config, so no SQLite account is created.
    projection = object.__new__(PaperBroker)
    projection.config = config
    action = SimulationAction(account_id="offline-research", signal_id="counterfactual",
        symbol=symbol, action="BUY" if buy else "FORCED_RISK_EXIT",
        signal_bar_end=bar.bar_end - timedelta(minutes=1), entry_reference=bar.open)
    return projection._adverse_price(action, bar)


def evaluate_outcome(plan: Mapping[str, Any], trigger_at: datetime,
                     future_bars: Sequence[Mapping[str, Any]], *,
                     outcome_labels: Sequence[Mapping[str, Any]] = (),
                     slippage_bps: float | None = None) -> dict[str, Any]:
    """Measure a first trigger using archived prices only; absent legs stay null.

    A bar timestamp is its end. The entry is exactly the first full session
    minute after trigger knowledge, never a later convenient available bar.
    Stops touched on the buy date are carried to T+1's opening minute. An
    intraminute stop on a sellable date executes on the next full minute.
    Returns are gross price observations, excluding account sizing and fees.
    """
    trigger = _at(trigger_at)
    result: dict[str, Any] = {
        "return_kind": "RESEARCH_COUNTERFACTUAL", "price_basis": "RAW_GROSS_EXCLUDING_FEES",
        "fill_model_version": "wp1-next-complete-minute-shared-paper-price/1",
        "fill_status": "INSUFFICIENT_EVIDENCE", "fill_price": None, "fill_bar_end": None,
        "stop_level": None, "risk_per_share": None, "stop_r": None,
        "stop_exit_price": None, "stop_exit_bar_end": None,
        "same_day_hard_exit_touched": None, "first_stop_touch_at": None,
        "t1_close_return": None, "t3_close_return": None, "t5_close_return": None,
        "forward_r_t1": None, "forward_r_t3": None, "forward_r_t5": None,
        "forward_r_basis": "HYPOTHETICAL_HOLD_TO_HORIZON_RAW_CLOSE; NOT_STOP_STRATEGY_OR_ACCOUNT_PNL",
        "mfe": None, "mae": None, "excursion_status": "INSUFFICIENT_EVIDENCE",
        "reason_codes": [], "minute_gaps": [], "source_outcome_label_count": len(outcome_labels),
        "outcome_labels_use": "DATED_RAW_CLOSE_ONLY; ORIGINAL_ANCHORED_RETURNS_NOT_REUSED",
        "price_limit_evidence": [], "derived_limit_prices": [],
    }
    reasons: list[str] = result["reason_codes"]
    try:
        symbol = map_symbol(str(plan.get("symbol") or plan.get("code"))).canonical
        stock_trading_rules(symbol)
        config = SimulationConfig() if slippage_bps is None else SimulationConfig(slippage_bps=slippage_bps)
    except ValueError:
        reasons.append("OUTCOME_SECURITY_OR_SLIPPAGE_INVALID")
        return result
    expected = _first_complete_bar_end(trigger)
    if expected is None:
        reasons.append("ORDER_NO_ELIGIBLE_SESSION")
        return result
    expiry_value = _lookup(plan, ("valid_until",), ("expires_at",), ("expire_at",), ("expiry_at",))
    if expiry_value is not None and expected > _at(expiry_value):
        reasons.append("ORDER_EXPIRED")
        return result
    if expected.time().replace(tzinfo=None) >= time(14, 45):
        result["fill_status"] = "NOT_FILLED"
        reasons.append("BUY_AFTER_CLOSE")
        return result
    bars: dict[datetime, tuple[MinuteBar, Mapping[str, Any]]] = {}
    conflicting: set[datetime] = set()
    for raw in future_bars:
        try:
            at = _at(_lookup(raw, ("bar_end",), ("timestamp",), ("datetime",)))
            if at < expected or raw.get("complete", raw.get("is_complete", True)) is not True:
                continue
            mode = str(raw.get("adjust_mode", raw.get("adjust", "none"))).lower()
            if mode not in {"none", "raw", "unadjusted"}:
                reasons.append("RAW_PRICE_EVIDENCE_REQUIRED")
                continue
            data = dict(raw)
            data["adjust_mode"] = "none"
            data.setdefault("symbol", symbol)
            data.setdefault("interval", "1m")
            data.setdefault("amount", 0)
            data.setdefault("source_id", "offline-archive")
            data["bar_end"] = at
            parsed = MinuteBar.model_validate(data)
            if parsed.symbol != symbol or parsed.interval != "1m" or not _session_minute(at):
                continue
            if parsed.evidence_kind != "MARKET_BAR" or parsed.source_id.endswith(":RISK_ONLY"):
                reasons.append("FILL_EVIDENCE_INVALID")
                continue
            if at in bars and (bars[at][0] != parsed or
                    resolve_price_limits(bars[at][1], symbol=symbol, at=at) !=
                    resolve_price_limits(raw, symbol=symbol, at=at)):
                conflicting.add(at)
            bars[at] = (parsed, raw)
        except (ValueError, TypeError):
            reasons.append("INVALID_FUTURE_BAR")
    for at in conflicting:
        bars.pop(at, None)
    if conflicting:
        reasons.append("CONFLICTING_FUTURE_BAR")
    if expected not in bars:
        reasons.append("NEXT_COMPLETE_MINUTE_MISSING")
        result["minute_gaps"].append({"expected_bar_end": expected.isoformat(), "purpose": "ENTRY"})
        return result
    entry, entry_raw = bars[expected]
    entry_limits = resolve_price_limits(entry_raw, symbol=symbol, at=expected, fallback_plan=plan)
    result['price_limit_evidence'].append({'purpose': 'ENTRY', **entry_limits})
    if entry_limits['derived_limit_prices'] is not None:
        result['derived_limit_prices'].append({'purpose': 'ENTRY', **entry_limits['derived_limit_prices']})
    failure = _executable(entry, entry_raw, buy=True, limit_evidence=entry_limits)
    if failure:
        result["fill_status"] = "INSUFFICIENT_EVIDENCE" if failure in {"PRICE_LIMITS_UNKNOWN", "PRICE_LIMITS_CONFLICT"} else "NOT_FILLED"
        reasons.append(failure)
        return result
    fill = _price(symbol, entry, buy=True, config=config)
    if fill is None:
        result["fill_status"] = "NOT_FILLED"
        reasons.append("PRICE_OUTSIDE_BAR")
        return result
    result.update(fill_status="FILLED", fill_price=fill, fill_bar_end=expected.isoformat())
    stop = _number(_lookup(plan, ("stop_level",), ("invalidation_level",), ("daily_invalidation",),
                           ("risk", "stop_level"), ("risk", "invalidation_level")))
    result["stop_level"] = stop
    if stop is None or not 0 < stop < fill:
        reasons.append("STOP_DISTANCE_INVALID_OR_MISSING")
    else:
        result["risk_per_share"] = fill - stop
    calendar = ExchangeTradingCalendar()
    horizons: dict[int, Any] = {}
    try:
        day = expected.date()
        if not calendar.is_trading_day(day):
            result.update(fill_status="INSUFFICIENT_EVIDENCE", fill_price=None, fill_bar_end=None)
            reasons.append("ENTRY_NOT_EXCHANGE_SESSION")
            return result
        for n in range(1, 6):
            day = calendar.next_trading_day(day)
            horizons[n] = day
    except TradingCalendarError as exc:
        reasons.append(exc.reason_code)
        return result
    # Only exchange closing bars prove horizon closes; a sparse 14:00 row is not a close.
    for n in (1, 3, 5):
        close_at = datetime.combine(horizons[n], time(15), SHANGHAI)
        close = bars[close_at][0].close if close_at in bars else None
        if close is None:
            candidates = set()
            for label in outcome_labels:
                try:
                    if map_symbol(str(label.get("symbol"))).canonical != symbol:
                        continue
                    date_value = label.get("target_trade_date") or label.get("trade_date")
                    if str(date_value)[:10] != horizons[n].isoformat():
                        continue
                    raw_close = _number(label.get("raw_close"))
                    if (raw_close is not None and raw_close > 0
                            and str(label.get("adjust_mode", label.get("adjust", "none"))).lower() in {"none", "raw", "unadjusted"}):
                        candidates.add(raw_close)
                except ValueError:
                    continue
            if len(candidates) == 1:
                close = candidates.pop()
        if close is not None:
            result[f"t{n}_close_return"] = close / fill - 1
        else:
            reasons.append(f"T{n}_CLOSE_MISSING")
    observed = [item[0] for at, item in sorted(bars.items()) if expected <= at <= datetime.combine(horizons[5], time(15), SHANGHAI)]
    if observed:
        result["mfe"] = max(item.high for item in observed) / fill - 1
        result["mae"] = min(item.low for item in observed) / fill - 1
        result["excursion_status"] = "OBSERVED_BARS_ONLY"
    if stop is not None and 0 < stop < fill:
        for n in (1, 3, 5):
            close_return = result[f"t{n}_close_return"]
            if close_return is not None:
                result[f"forward_r_t{n}"] = close_return * fill / (fill - stop)
        same_day = [item for item in observed if item.bar_end.date() == expected.date()]
        # False is only established when every remaining minute is present.
        touched = any(item.low <= stop for item in same_day)
        cursor = expected
        complete_same_day = True
        while cursor is not None:
            if cursor not in bars:
                complete_same_day = False
                break
            cursor = _first_complete_bar_end(cursor)
        result["same_day_hard_exit_touched"] = True if touched else (False if complete_same_day else None)
        pending_at: datetime | None = None
        for item in observed:
            if item.low <= stop:
                result["first_stop_touch_at"] = item.bar_end.isoformat()
                if item.bar_end.date() == expected.date():
                    pending_at = datetime.combine(horizons[1], time(9, 31), SHANGHAI)
                elif item.open <= stop:
                    pending_at = item.bar_end
                else:
                    pending_at = _first_complete_bar_end(item.bar_end)
                    if pending_at is None:
                        try:
                            pending_at = datetime.combine(calendar.next_trading_day(item.bar_end.date()), time(9, 31), SHANGHAI)
                        except TradingCalendarError as exc:
                            reasons.append(exc.reason_code)
                break
        if pending_at is not None:
            while pending_at is not None:
                if pending_at not in bars:
                    reasons.append("STOP_EXIT_MINUTE_MISSING")
                    result["minute_gaps"].append({"expected_bar_end": pending_at.isoformat(), "purpose": "STOP_EXIT"})
                    break
                sell, sell_raw = bars[pending_at]
                sell_limits = resolve_price_limits(sell_raw, symbol=symbol, at=pending_at)
                result['price_limit_evidence'].append({'purpose': 'STOP_EXIT', **sell_limits})
                if sell_limits['derived_limit_prices'] is not None:
                    result['derived_limit_prices'].append({'purpose': 'STOP_EXIT', **sell_limits['derived_limit_prices']})
                failure = _executable(sell, sell_raw, buy=False, limit_evidence=sell_limits)
                if failure:
                    reasons.append(failure)
                    # A locked/zero-volume minute proves no fill; later observed
                    # minutes may fill only when the clock chain remains complete.
                    if failure not in {"LIMIT_DOWN_LOCKED", "BAR_NOT_EXECUTABLE"}:
                        break
                    next_at = _first_complete_bar_end(pending_at)
                    if next_at is None:
                        try:
                            next_at = datetime.combine(calendar.next_trading_day(pending_at.date()), time(9, 31), SHANGHAI)
                        except TradingCalendarError as exc:
                            reasons.append(exc.reason_code)
                    pending_at = next_at
                    continue
                sell_price = _price(symbol, sell, buy=False, config=config)
                if sell_price is None:
                    reasons.append("STOP_PRICE_OUTSIDE_BAR")
                    break
                result["stop_exit_price"] = sell_price
                result["stop_exit_bar_end"] = pending_at.isoformat()
                result["stop_r"] = (sell_price - fill) / (fill - stop)
                break
        elif result["first_stop_touch_at"] is None:
            reasons.append("STOP_NOT_OBSERVED_NO_REALIZED_R")
    result["reason_codes"] = sorted(set(reasons))
    return result


def opportunity_cost(plan: Mapping[str, Any], bars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Highest observed price inside explicit validity / entry upper - 1."""
    zone = _lookup(plan, ("entry_reference_zone",), ("trigger_zone",), ("entry_zone",),
                   ("pullback_zone",), ("strategy_facts", "entry_reference_zone"))
    upper = _number(zone.get("upper", zone.get("high"))) if isinstance(zone, Mapping) else (
        _number(zone[1]) if isinstance(zone, (list, tuple)) and len(zone) == 2 else None)
    start = _lookup(plan, ("valid_from",), ("activated_at",), ("activation_at",))
    end = _lookup(plan, ("valid_until",), ("expires_at",), ("expire_at",), ("expiry_at",))
    result: dict[str, Any] = {"value": None, "status": "INSUFFICIENT_EVIDENCE", "entry_zone_upper": upper,
                              "observed_high": None, "basis": "VALIDITY_OBSERVED_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE"}
    if upper is None or upper <= 0 or start is None or end is None:
        result["reason_code"] = "VALIDITY_OR_ENTRY_ZONE_MISSING"
        return result
    start_at, end_at = _at(start), _at(end)
    values = []
    symbol = str(plan.get("symbol") or "")
    for raw in bars:
        try:
            at = _at(_lookup(raw, ("bar_end",), ("timestamp",), ("datetime",)))
            if (raw.get("complete", raw.get("is_complete", True)) is not True
                    or raw.get("interval", "1m") != "1m"
                    or raw.get("evidence_kind", "MARKET_BAR") != "MARKET_BAR"
                    or str(raw.get("source_id", "")).endswith(":RISK_ONLY")
                    or str(raw.get("adjust_mode", raw.get("adjust", "none"))).lower() not in {"none", "raw", "unadjusted"}):
                continue
            if symbol and raw.get("symbol") and map_symbol(str(raw["symbol"])).canonical != map_symbol(symbol).canonical:
                continue
            high = _number(raw.get("high"))
            if start_at <= at <= end_at and high is not None and high > 0:
                values.append(high)
        except (ValueError, TypeError):
            continue
    if values:
        result.update(value=max(values) / upper - 1, observed_high=max(values), status="OBSERVED_VALIDITY_BARS_ONLY")
    else:
        result["reason_code"] = "VALIDITY_BARS_MISSING"
    return result


__all__ = ["evaluate_outcome", "opportunity_cost"]
