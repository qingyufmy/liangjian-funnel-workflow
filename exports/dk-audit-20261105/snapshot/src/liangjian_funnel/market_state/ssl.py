"""Self-researched SSL, not a site clone. Immutable append and batch paths.

Only canonical Tencent CLOSED 15m bars enter this module. First 3N bars
are preheat and cannot establish a direction; equality holds the prior state.
No global state, future data, source fetch, trading rule or runtime wiring.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from decimal import Decimal, localcontext
import re

from .bars15m import Bar15m, ENDS, TZ

CANONICAL_SOURCE = 'TENCENT:ifzq.gtimg.cn'
PERIODS = (10, 20, 30)


@dataclass(frozen=True)
class SslEvent:
    type: str
    bar_end: datetime
    close: Decimal
    trigger_level: Decimal


@dataclass(frozen=True)
class SslPoint:
    period: int
    bar_end: datetime
    status: str
    state: str
    upper: Decimal | None
    lower: Decimal | None
    event: SslEvent | None
    last_event: SslEvent | None
    bars_since_event: int | None
    preheat_bars: int


@dataclass(frozen=True)
class SslMemory:
    period: int
    symbol: str | None = None
    adjust_mode: str | None = None
    count: int = 0
    previous_end: datetime | None = None
    highs: tuple[Decimal, ...] = ()
    lows: tuple[Decimal, ...] = ()
    state: str = 'UNKNOWN'
    last_event: SslEvent | None = None
    bars_since_event: int | None = None


def initial_ssl(period=20):
    if type(period) is not int or period not in PERIODS:
        raise ValueError('SSL_PERIOD_NOT_APPROVED')
    return SslMemory(period)


def _next_end(previous, calendar):
    slot = previous.astimezone(TZ).strftime('%H:%M')
    if slot not in ENDS:
        raise ValueError('SSL_SESSION_GRID_INVALID')
    i = ENDS.index(slot)
    day = previous.astimezone(TZ).date()
    if i == len(ENDS) - 1:
        day, slot = calendar.next_trading_day(day), ENDS[0]
    else:
        slot = ENDS[i+1]
    return datetime.combine(day, time.fromisoformat(slot), tzinfo=TZ)


def ssl_append(memory: SslMemory, bar: Bar15m, *, observed_at, calendar):
    """Return new memory and this bar's output, leaving both inputs untouched."""
    if (not isinstance(memory, SslMemory) or memory.period not in PERIODS
            or type(memory.period) is not int or memory.count < 0
            or memory.state not in {'D', 'K', 'UNKNOWN'}):
        raise ValueError('SSL_MEMORY_INVALID')
    if (not isinstance(bar, Bar15m) or bar.status != 'CLOSED'
            or bar.source_id != CANONICAL_SOURCE or bar.adjust_mode not in {'none', 'raw'}
            or re.fullmatch(r'\d{6}\.(SH|SZ)', bar.symbol) is None):
        raise ValueError('SSL_CLOSED_CANONICAL_BAR_REQUIRED')
    if (not isinstance(observed_at, datetime) or observed_at.utcoffset() is None
            or not isinstance(bar.bar_end, datetime) or bar.bar_end.utcoffset() is None):
        raise ValueError('SSL_CLOCK_INVALID')
    end = bar.bar_end.astimezone(TZ)
    if (end.second or end.microsecond or end.strftime('%H:%M') not in ENDS
            or calendar.is_trading_day(end.date()) is not True):
        raise ValueError('SSL_SESSION_GRID_INVALID')
    if observed_at < end + timedelta(seconds=60):
        raise ValueError('SSL_BAR_NOT_CLOSED')
    parts = tuple(end - timedelta(minutes=n) for n in (10, 5, 0))
    if bar.expected_5m != parts or bar.present_5m != parts:
        raise ValueError('SSL_THREE_PARTS_REQUIRED')
    if memory.previous_end is not None:
        if memory.symbol != bar.symbol or memory.adjust_mode != bar.adjust_mode:
            raise ValueError('SSL_SERIES_IDENTITY_MISMATCH')
        if _next_end(memory.previous_end, calendar) != end:
            raise ValueError('SSL_SERIES_GAP_DUPLICATE_OR_REORDERED')
    for value in (bar.open, bar.high, bar.low, bar.close):
        if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
            raise ValueError('SSL_BAR_NUMBER_INVALID')
    if bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
        raise ValueError('SSL_BAR_OHLC_INVALID')
    n, count = memory.period, memory.count + 1
    highs, lows = (*memory.highs, bar.high)[-n:], (*memory.lows, bar.low)[-n:]
    with localcontext() as ctx:
        ctx.prec = 34
        upper = None if count < n else sum(highs, Decimal(0)) / n
        lower = None if count < n else sum(lows, Decimal(0)) / n
    state, event = memory.state, None
    warmed = memory.count >= 3 * n
    if warmed:
        state = 'D' if bar.close > upper else 'K' if bar.close < lower else state
        if state != memory.state:
            event = SslEvent(state, end, bar.close, upper if state == 'D' else lower)
    last_event = event or memory.last_event
    since = 0 if event else None if memory.bars_since_event is None else memory.bars_since_event + 1
    updated = replace(memory, symbol=bar.symbol, adjust_mode=bar.adjust_mode, count=count,
        previous_end=end, highs=highs, lows=lows, state=state,
        last_event=last_event, bars_since_event=since)
    point = SslPoint(n, end, 'READY' if warmed else 'DK_WARMUP_INSUFFICIENT',
        state, upper, lower, event, last_event, since, min(memory.count, 3*n))
    return updated, point


def ssl_series(bars, *, period=20, observed_at, calendar):
    memory, result = initial_ssl(period), []
    for bar in bars:
        memory, point = ssl_append(memory, bar, observed_at=observed_at, calendar=calendar)
        result.append(point)
    return tuple(result)


def dk_state(bars_15m_closed, *, observed_at, calendar, period=20):
    points = ssl_series(bars_15m_closed, period=period, observed_at=observed_at, calendar=calendar)
    return points[-1] if points else None


def evaluate_ssl(bars, *, period=20, observed_at, calendar):
    """B3.5 returns and 4-bar reversals with right-censored denominators.

    Returns are raw forward close returns, split by D/K; K is not a short
    trade. Only events with a complete horizon enter that horizon's sample.
    """
    bars = tuple(bars)
    points = ssl_series(bars, period=period, observed_at=observed_at, calendar=calendar)
    indices = [i for i, point in enumerate(points) if point.event is not None]
    distributions = {}
    for direction in ('D', 'K'):
        distributions[direction] = {}
        events = [i for i in indices if points[i].event.type == direction]
        for horizon in (1, 4, 16):
            with localcontext() as ctx:
                ctx.prec = 34
                returns = [bars[i+horizon].close / bars[i].close - 1 for i in events if i+horizon < len(bars)]
                mean = sum(returns, Decimal(0)) / len(returns) if returns else None
            distributions[direction][str(horizon)] = dict(n=len(returns), right_censored=len(events)-len(returns),
                returns=[str(value) for value in returns], mean=None if mean is None else str(mean),
                minimum=str(min(returns)) if returns else None, maximum=str(max(returns)) if returns else None)
    eligible = [i for i in indices if i + 4 < len(bars)]
    reversals = sum(any(points[j].event is not None and points[j].event.type != points[i].event.type
        for j in range(i+1, i+5)) for i in eligible)
    warmed_days = sorted({point.bar_end.date().isoformat() for point in points if point.status == 'READY'})
    full_days = [day for day in warmed_days if
        {point.bar_end.strftime('%H:%M') for point in points
         if point.status == 'READY' and point.bar_end.date().isoformat() == day} == set(ENDS)]
    return dict(schema_version='ssl-evaluation/1', authority='SHADOW_ONLY', label='自研、非站点同款',
        period=period, source_id=CANONICAL_SOURCE, bars=len(bars),
        observed_trade_dates=sorted({bar.bar_end.date().isoformat() for bar in bars}),
        ready_trade_dates=warmed_days, ready_complete_session_dates=full_days, required_evaluation_trade_days=20,
        coverage_status='COVERAGE_20_COMPLETE_SESSIONS' if len(full_days) >= 20 else 'HISTORY_INSUFFICIENT_20_TRADE_DAYS',
        event_count=len(indices), distributions=distributions,
        signal_intervals_bars=[b-a for a, b in zip(indices, indices[1:])],
        false_breakout=dict(reversed_within_4=reversals, eligible_events=len(eligible),
            right_censored=len(indices)-len(eligible), rate=str(Decimal(reversals)/len(eligible)) if eligible else None),
        source_calls=0, model_calls=0, production_writes=0)
