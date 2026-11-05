"""Controlled sequences prove causality and append equivalence, not profit."""
from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal, localcontext

import pytest

from liangjian_funnel.market_state.bars15m import Bar15m, ENDS, TZ
from liangjian_funnel.market_state.ssl import CANONICAL_SOURCE, evaluate_ssl, initial_ssl, ssl_append, ssl_series
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

CAL = ExchangeTradingCalendar()


def bars(count=400):
    day = datetime(2026, 9, 1).date()
    result = []
    for i in range(count):
        if i and i % 16 == 0:
            day = CAL.next_trading_day(day)
        end = datetime.combine(day, time.fromisoformat(ENDS[i % 16]), tzinfo=TZ)
        # Long flat warmup, then controlled, alternating breaks every 20 bars.
        close = Decimal(100 if i < 100 else 110 if (i//20) % 2 else 90)
        parts = tuple(end-timedelta(minutes=n) for n in (10, 5, 0))
        result.append(Bar15m('000001.SH', end, 'CLOSED', parts, parts,
            open=close, high=close+1, low=close-1, close=close,
            source_id=CANONICAL_SOURCE, adjust_mode='none'))
    return tuple(result)


def clock(series):
    return series[-1].bar_end + timedelta(seconds=60)


@pytest.mark.parametrize('period', [10, 20, 30])
def test_every_prefix_matches_incremental_and_later_bars_do_not_repaint(period):
    source = bars(180)
    before = source
    memory = initial_ssl(period)
    appended = []
    full = ssl_series(source, period=period, observed_at=clock(source), calendar=CAL)
    for i, bar in enumerate(source):
        previous = memory
        memory, point = ssl_append(memory, bar, observed_at=clock(source), calendar=CAL)
        assert previous.count == i
        appended.append(point)
        prefix = ssl_series(source[:i+1], period=period, observed_at=clock(source), calendar=CAL)
        assert prefix == full[:i+1] == tuple(appended)
    assert source == before


def test_first_3n_bars_unknown_equality_holds_and_events_alternate():
    source = bars()
    points = ssl_series(source, observed_at=clock(source), calendar=CAL)
    assert all(p.state == 'UNKNOWN' and p.event is None and p.status == 'DK_WARMUP_INSUFFICIENT' for p in points[:60])
    assert points[60].status == 'READY' and points[60].state == 'UNKNOWN'
    events = [p.event for p in points if p.event]
    assert len(events) > 4
    assert all(a.type != b.type for a, b in zip(events, events[1:]))
    assert all(p.event.trigger_level == (p.upper if p.event.type == 'D' else p.lower) for p in points if p.event)


@pytest.mark.parametrize('mutation', ['source', 'forward', 'not_closed', 'missing_part', 'nan', 'ohlc', 'future'])
def test_canonical_closed_three_parts_and_clock_required(mutation):
    source = bars(1)
    bar, now = source[0], clock(source)
    if mutation == 'source': bar = replace(bar, source_id='EASTMONEY_EQUITY_M5_UNADJUSTED')
    elif mutation == 'forward': bar = replace(bar, adjust_mode='forward')
    elif mutation == 'not_closed': bar = replace(bar, status='NOT_CLOSED')
    elif mutation == 'missing_part': bar = replace(bar, present_5m=bar.present_5m[:2])
    elif mutation == 'nan': bar = replace(bar, close=Decimal('NaN'))
    elif mutation == 'ohlc': bar = replace(bar, low=Decimal(101))
    else: now -= timedelta(seconds=1)
    with pytest.raises(ValueError):
        ssl_append(initial_ssl(), bar, observed_at=now, calendar=CAL)


@pytest.mark.parametrize('change', ['gap', 'duplicate', 'reorder', 'symbol'])
def test_series_gaps_and_identities_cannot_carry_old_state(change):
    source = list(bars(90))
    if change == 'gap': source.pop(40)
    elif change == 'duplicate': source.insert(40, source[40])
    elif change == 'reorder': source[40], source[41] = source[41], source[40]
    else: source[40] = replace(source[40], symbol='000300.SH')
    with pytest.raises(ValueError, match='SSL_SERIES_'):
        ssl_series(source, observed_at=clock(source), calendar=CAL)


def test_right_censored_denominators_and_returns_are_recomputable():
    source = bars(142)
    result = evaluate_ssl(source, observed_at=clock(source), calendar=CAL)
    points = ssl_series(source, observed_at=clock(source), calendar=CAL)
    events = [i for i, p in enumerate(points) if p.event]
    assert result['coverage_status'] == 'HISTORY_INSUFFICIENT_20_TRADE_DAYS'
    assert result['false_breakout']['eligible_events'] == sum(i+4 < len(source) for i in events)
    for direction in ('D', 'K'):
        indices = [i for i in events if points[i].event.type == direction]
        for horizon in (1, 4, 16):
            row = result['distributions'][direction][str(horizon)]
            assert row['n'] + row['right_censored'] == len(indices)
            with localcontext() as ctx:
                ctx.prec = 34
                expected = [str(source[i+horizon].close/source[i].close-1) for i in indices if i+horizon < len(source)]
            assert row['returns'] == expected


@pytest.mark.parametrize('period', [True, 5, 19, 40])
def test_only_three_approved_periods(period):
    with pytest.raises(ValueError, match='SSL_PERIOD_NOT_APPROVED'):
        initial_ssl(period)


@pytest.mark.parametrize('period',[10,20,30])
def test_hand_derived_thresholds_equality_and_first_post_warmup_signal(period):
    # The windows contain constant 110/90 boundaries, apart from one 111 high
    # and one 89 low. Their exact arithmetic is independent of append state.
    source=list(bars(4*period+4))
    for i,bar in enumerate(source):
        close=Decimal(100)
        if i==3*period:close=Decimal(110)  # equality must remain UNKNOWN
        if i==3*period+1:close=Decimal(111)
        if i==4*period+2:close=Decimal(90)  # equality must retain D
        if i==4*period+3:close=Decimal(89)
        source[i]=replace(bar,open=close,close=close,high=max(Decimal(110),close),low=min(Decimal(90),close))
    points=ssl_series(source,period=period,observed_at=clock(source),calendar=CAL)
    assert points[3*period].status=='READY' and points[3*period].state=='UNKNOWN'
    assert points[4*period+2].state=='D' and points[4*period+2].event is None
    events=[p.event for p in points if p.event]
    assert [(e.type,e.bar_end) for e in events]==[('D',source[3*period+1].bar_end),('K',source[-1].bar_end)]
    with localcontext() as ctx:
        ctx.prec=34
        assert events[0].trigger_level==Decimal(110)+Decimal(1)/period
        assert events[1].trigger_level==Decimal(90)-Decimal(1)/period
