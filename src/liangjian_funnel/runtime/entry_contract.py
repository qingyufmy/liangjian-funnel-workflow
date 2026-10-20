"""Immutable entry instructions; existing events keep their legacy replay model."""
from math import isfinite
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Mapping, Any

from .stock_trading_rules import stock_trading_rules


def next_entry_minute(signal_time):
    return next_complete_minute(signal_time)


def next_complete_minute(review_time: datetime) -> datetime | None:
    """First exchange minute whose entire window follows review completion."""

    value = review_time.astimezone(ZoneInfo("Asia/Shanghai"))
    clock = value.timetz().replace(tzinfo=None)
    if clock < time(9, 30):
        return value.replace(hour=9, minute=31, second=0, microsecond=0)
    if clock < time(11, 30):
        floor = value.replace(second=0, microsecond=0)
        candidate = floor + timedelta(minutes=1 if value == floor else 2)
        return candidate if candidate.time() <= time(11, 30) else value.replace(hour=13, minute=1, second=0, microsecond=0)
    if clock < time(13):
        return value.replace(hour=13, minute=1, second=0, microsecond=0)
    if clock < time(15):
        floor = value.replace(second=0, microsecond=0)
        candidate = floor + timedelta(minutes=1 if value == floor else 2)
        return candidate if candidate.time() <= time(15) else None
    return None


def entry_bar_for(signal: Mapping[str, Any]) -> datetime | None:
    """One immutable entry-window definition for matching and expiration.

    Retain the original contract's eligible end; reject malformed clocks rather
    than moving an order into a different minute. Legacy events use signal time.
    """
    contract = signal.get("entry_contract") or {}
    if not isinstance(contract, Mapping):
        raise ValueError("ENTRY_CONTRACT_CLOCK_INVALID")

    def stamp(raw):
        try:
            value = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
        except (ValueError,TypeError):
            raise ValueError("ENTRY_CONTRACT_CLOCK_INVALID") from None
        if value.tzinfo is None:
            raise ValueError("ENTRY_CONTRACT_CLOCK_INVALID")
        return value.astimezone(ZoneInfo("Asia/Shanghai"))

    references = [contract.get("order_created_at"),contract.get("review_completed_at"),
                  signal.get("signal_time") or signal.get("minute_end")]
    clocks = [stamp(reference) for reference in references if reference is not None]
    if not clocks:raise ValueError("ENTRY_CONTRACT_CLOCK_INVALID")
    calculated = next_complete_minute(max(clocks))
    if contract.get("eligible_bar_end") is None:
        return calculated
    frozen = stamp(contract["eligible_bar_end"])
    if frozen.second or frozen.microsecond or frozen != calculated:
        raise ValueError("ENTRY_CONTRACT_CLOCK_CONFLICT")
    return frozen


def freeze_entry_contract(
    symbol: str,
    plan: Mapping[str, Any],
    strategy: Mapping[str, Any],
    *,
    at=None,
    review_completed_at: datetime | None = None,
    source_plan_sha256: str | None = None,
    source_plan_id: str | None = None,
) -> dict:
    def price(value):
        try:
            number = float(value)
            return number if isfinite(number) and number > 0 else None
        except (TypeError, ValueError):
            return None

    reference = price(strategy.get("live_entry_price")) or price(strategy.get("reference_price"))
    stop = price(strategy.get("live_stop_level")) or price(plan.get("stop_level"))
    pattern_binding=strategy.get("entry_pattern_binding")
    pattern_valid=True
    if pattern_binding is not None:
        from .entry_pattern_binding import checked_binding,make_b_execution_binding,digest
        try:
            checked_binding(pattern_binding)
            expected=make_b_execution_binding(plan,strategy.get("entry_confirmation"),
                source_sha256=source_plan_sha256 or digest(plan),plan_id=source_plan_id)
            pattern_valid=(expected==pattern_binding and reference==price(pattern_binding['entry_price'])
                and stop==price(pattern_binding['stop']))
        except (ValueError,TypeError):pattern_valid=False
    # The limit protects the already-confirmed price, not the old pullback
    # zone's lower edge. Never increase the existing no-chase ceiling.
    ceiling = price(plan.get("no_chase_price")) or price(plan.get("max_chase_price")) or price(plan.get("no_chase_above"))
    limit = min(reference, ceiling) if reference and ceiling else reference
    if limit:
        limit = stock_trading_rules(symbol).adverse_tick(limit, buy=False)
    valid = bool(pattern_valid and reference and stop and limit and stop < limit)
    reason = "ENTRY_PATTERN_BINDING_INVALID" if not pattern_valid else "OK" if valid else "ENTRY_PRICE_CONTRACT_INVALID"
    eligible = (
        next_complete_minute(review_completed_at)
        if review_completed_at is not None
        else next_entry_minute(at) if at else None
    )
    if eligible is None and (review_completed_at is not None or at is not None):
        valid, reason = False, "ENTRY_NO_NEXT_TRADING_MINUTE"
    if eligible is not None:
        clock = eligible.timetz().replace(tzinfo=None)
        if not (time(9,31) <= clock <= time(11,30) or time(13,1) <= clock <= time(15)):
            valid, reason = False, "ENTRY_NO_NEXT_TRADING_MINUTE"
        expiry = plan.get("expires_at") or plan.get("plan_expiry")
        if expiry:
            try:
                if eligible > datetime.fromisoformat(str(expiry)):
                    valid, reason = False, "ENTRY_NEXT_MINUTE_AFTER_PLAN_EXPIRY"
            except (TypeError, ValueError):
                valid, reason = False, "ENTRY_PLAN_EXPIRY_INVALID"
    result = {
        "version": "a4-entry/2" if review_completed_at is not None else "a4-entry/1",
        "status": "READY" if valid else "INVALID",
        "reason_code": reason,
        "order_type": "LIMIT", "time_in_force": "NEXT_COMPLETE_MINUTE_ONLY",
        "eligible_bar_end": eligible.isoformat() if eligible else None,
        "review_completed_at": review_completed_at.isoformat() if review_completed_at else None,
        "order_created_at": review_completed_at.isoformat() if review_completed_at else None,
        "clock_quality": "OBSERVED_WALL_CLOCK" if review_completed_at else "TIME_UNVERIFIED_LEGACY",
        "signal_reference": reference, "limit_price": limit, "stop_level": stop,
        "risk_unit": 0.33 if pattern_binding is not None or plan.get("risk_unit") == "PROBE" else 1.0,
        "fill_model": "NEXT_OPEN_OR_STRICT_LIMIT_PENETRATION",
        "fee_model_version": "paper-config/2" if review_completed_at else "legacy-paper-fee/1",
        "fill_model_version": "next-complete-minute-capacity/2" if review_completed_at else "legacy-next-minute/1",
        "performance_basis": "SIMULATION_ESTIMATE_NOT_EXCHANGE_FILL",
    }
    if pattern_binding is not None:
        result['entry_pattern_binding']=pattern_binding
        result['stop_basis']='B_RETEST_ONLY_RAISED'
    return result
