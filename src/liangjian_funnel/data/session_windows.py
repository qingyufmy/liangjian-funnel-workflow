"""Canonical SH/SZ end-labelled closed windows; never fabricate lunch bars.

The workflow validates exchange session dates before acquisition. Standalone
callers may supply the same calendar explicitly (no weekday fallback).
"""
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")


def closed_window_ends(as_of: datetime, interval: str, *, calendar=None) -> tuple[datetime, ...]:
    if as_of.tzinfo is None:
        raise ValueError("TIMEZONE_REQUIRED")
    if interval not in {"1m", "5m", "15m"}:
        raise ValueError("UNSUPPORTED_INTERVAL")
    current = as_of.astimezone(TZ)
    if calendar is not None and not calendar.is_trading_day(current.date()):
        return ()
    step = timedelta(minutes=int(interval[:-1]))
    result = []
    for start, end in ((time(9, 30), time(11, 30)), (time(13), time(15))):
        stamp = datetime.combine(current.date(), start, TZ) + step
        stop = min(current, datetime.combine(current.date(), end, TZ))
        while stamp <= stop:
            result.append(stamp)
            stamp += step
    return tuple(result)


def latest_closed_end(as_of: datetime, interval: str) -> datetime | None:
    ends = closed_window_ends(as_of, interval)
    return ends[-1] if ends else None
