"""Immutable entry instructions; existing events keep their legacy replay model."""
from math import isfinite
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Mapping, Any

from .stock_trading_rules import stock_trading_rules


def next_entry_minute(signal_time):
    value = signal_time.astimezone(ZoneInfo("Asia/Shanghai"))
    return value.replace(hour=13, minute=1) if value.hour == 11 and value.minute == 30 else value + timedelta(minutes=1)


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


def freeze_entry_contract(
    symbol: str,
    plan: Mapping[str, Any],
    strategy: Mapping[str, Any],
    *,
    at=None,
    review_completed_at: datetime | None = None,
) -> dict:
    def price(value):
        try:
            number = float(value)
            return number if isfinite(number) and number > 0 else None
        except (TypeError, ValueError):
            return None

    reference = price(strategy.get("live_entry_price")) or price(strategy.get("reference_price"))
    stop = price(strategy.get("live_stop_level")) or price(plan.get("stop_level"))
    # The limit protects the already-confirmed price, not the old pullback
    # zone's lower edge. Never increase the existing no-chase ceiling.
    ceiling = price(plan.get("no_chase_price")) or price(plan.get("max_chase_price")) or price(plan.get("no_chase_above"))
    limit = min(reference, ceiling) if reference and ceiling else reference
    if limit:
        limit = stock_trading_rules(symbol).adverse_tick(limit, buy=False)
    valid = bool(reference and stop and limit and stop < limit)
    reason = "OK" if valid else "ENTRY_PRICE_CONTRACT_INVALID"
    eligible = (
        next_complete_minute(review_completed_at)
        if review_completed_at is not None
        else next_entry_minute(at) if at else None
    )
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
    return {
        "version": "a4-entry/2" if review_completed_at is not None else "a4-entry/1",
        "status": "READY" if valid else "INVALID",
        "reason_code": reason,
        "order_type": "LIMIT", "time_in_force": "NEXT_COMPLETE_MINUTE_ONLY",
        "eligible_bar_end": eligible.isoformat() if eligible else None,
        "review_completed_at": review_completed_at.isoformat() if review_completed_at else None,
        "order_created_at": review_completed_at.isoformat() if review_completed_at else None,
        "clock_quality": "OBSERVED_WALL_CLOCK" if review_completed_at else "TIME_UNVERIFIED_LEGACY",
        "signal_reference": reference, "limit_price": limit, "stop_level": stop,
        "risk_unit": 0.33 if plan.get("risk_unit") == "PROBE" else 1.0,
        "fill_model": "NEXT_OPEN_OR_STRICT_LIMIT_PENETRATION",
        "fee_model_version": "paper-config/2" if review_completed_at else "legacy-paper-fee/1",
        "fill_model_version": "next-complete-minute-capacity/2" if review_completed_at else "legacy-next-minute/1",
        "performance_basis": "SIMULATION_ESTIMATE_NOT_EXCHANGE_FILL",
    }
