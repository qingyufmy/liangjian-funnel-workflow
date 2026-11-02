"""Invocation-local clocks for the explicit, verified pre-open recovery path.

This grants no closed-session capture proof or late close publication. Native
snapshot timestamps stay unchanged. Market identity is independently proved by
the complete G0 closed daily series; child recipes must replay against that
exact native root before Team A2/A3 can consume it.
"""
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, time
from zoneinfo import ZoneInfo

from .closed_session_capture import aware
from .publication_receipt import object_hash

_SOURCE = ContextVar('same_day_recovery_source', default=None)


def require(value, reason):
    if not value:
        raise ValueError('TEAM_RECOVERY_' + reason)


def require_window(now, calendar):
    now = aware(now)
    require(calendar.is_trading_day(now.date()) is True
        and now.time().replace(tzinfo=None) < time(9, 26), 'WINDOW_CLOSED')
    return now


def validate_root(base, calendar, *, now):
    now = require_window(now, calendar)
    captured = aware(base.as_of)
    require(captured.date() == now.date() and captured <= now
        and captured.time().replace(tzinfo=None) < time(9, 26), 'SOURCE_TIME_INVALID')
    require(not getattr(base, 'proof_chain', ()) and getattr(base, 'capture_root', None) is None,
        'ROOT_REQUIRED')
    require(object_hash(base.data) == base.snapshot_hash, 'ROOT_HASH_CHANGED')
    require('CLOSED_SESSION_CAPTURE' not in base.data and 'AUCTION_REFRESH_CONTEXT' not in base.data,
        'SOURCE_SCOPE_CONFLICT')
    manifest = base.data.get('snapshot_manifest')
    require(isinstance(manifest, Mapping) and manifest.get('frozen') is True
        and manifest.get('as_of') == captured.isoformat()
        and base.data.get('MARKET_DATA_AS_OF') == captured.isoformat(), 'NATIVE_CAPTURE_UNPROVEN')
    source_day = calendar.previous_trading_day(captured.date())
    scope, bars = base.data.get('g0_symbols'), base.data.get('RECENT_DAILY_BARS')
    require(isinstance(scope, (list, tuple)) and bool(scope) and isinstance(bars, Mapping),
        'CLOSED_DAILY_REQUIRED')
    for symbol in scope:
        rows = bars.get(symbol)
        require(isinstance(rows, (list, tuple)) and bool(rows), 'CLOSED_DAILY_REQUIRED')
        dates = []
        for row in rows:
            require(isinstance(row, Mapping) and type(row.get('date_ms')) in (int, float),
                'DAILY_DATE_INVALID')
            stamp = datetime.fromtimestamp(row['date_ms'] / 1000, ZoneInfo('Asia/Shanghai'))
            require(stamp.date() <= source_day, 'LIVE_OR_FUTURE_DAILY_FORBIDDEN')
            dates.append(stamp.date())
        require(max(dates) == source_day, 'LATEST_CLOSED_DAILY_REQUIRED')
    return datetime.combine(source_day, time(15), tzinfo=ZoneInfo('Asia/Shanghai'))


@contextmanager
def recovery_time_context(prepared, calendar, *, clock):
    from ..pipeline.research.common import _freeze_snapshot, _coerce_snapshot
    base = _freeze_snapshot(_coerce_snapshot(prepared.snapshot))
    market = validate_root(base, calendar, now=clock())
    token = _SOURCE.set((base, market, clock))
    try:
        yield
    finally:
        _SOURCE.reset(token)


def recovery_times(snapshot, calendar):
    source = _SOURCE.get()
    if source is None:
        return None
    base, market, clock = source
    now = require_window(clock(), calendar)
    require(aware(base.as_of).date() == now.date()
        and object_hash(base.data) == base.snapshot_hash, 'ROOT_CHANGED_OR_STALE')
    from .research_update import replay_snapshot_recipes
    replay_snapshot_recipes(snapshot, base, now=now, require=require)
    require(snapshot.data.get('MARKET_DATA_AS_OF') == base.data['MARKET_DATA_AS_OF']
        and 'CLOSED_SESSION_CAPTURE' not in snapshot.data, 'MARKET_BINDING_CHANGED')
    return aware(snapshot.as_of), market
