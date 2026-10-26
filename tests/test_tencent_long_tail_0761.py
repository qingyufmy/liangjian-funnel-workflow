from datetime import date,datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import FetchResult,MinuteBar
from liangjian_funnel.data.tencent_minute import ResilientIntradayAdapter
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.history_freshness import session_grid


def fixture(conflict=False):
    day=date(2026,10,26);cal=ExchangeTradingCalendar();ends=[]
    for _ in range(7):ends.extend(session_grid(day));day=cal.previous_trading_day(day)
    rows=tuple(MinuteBar(symbol='300817.SZ',interval='5m',bar_end=t,open=10,high=11,low=9,close=10,
        volume=100,amount=1000,source_id='CONTROLLED_TENCENT_NATIVE') for t in sorted(ends)[-320:])
    calls=[]
    def primary(s,i,n,**kw):
        bars=(rows[0].model_copy(update={'close':10.5}),) if conflict else ()
        return FetchResult(symbol=s,interval=i,requested_bars=n,returned_bars=len(bars),bars=bars,
            complete=False,reason_code='NODE_PROTOCOL_RESPONSE_INVALID')
    def tail(s,i,n,**kw):
        calls.append(n)
        return FetchResult(symbol=s,interval=i,requested_bars=n,returned_bars=320,bars=rows,complete=True,reason_code='OK')
    return ResilientIntradayAdapter(SimpleNamespace(fetch_bars=primary),SimpleNamespace(fetch_bars=tail)),calls


def test_long_request_gets_native_tail_once_without_fabricating_12240_bars():
    adapter,calls=fixture();r=adapter.fetch_bars('300817.SZ','5m',12240,
        as_of=datetime(2026,10,26,15,tzinfo=ZoneInfo('Asia/Shanghai')))
    assert calls==[320] and r.returned_bars==320 and r.requested_bars==12240
    assert r.complete is False and r.reason_code=='LONG_HISTORY_PARTIAL_TENCENT_WINDOW'
    assert len({b.bar_end for b in r.bars})==320


def test_overlapping_native_conflict_is_not_merged_or_relabelled_complete():
    adapter,calls=fixture(conflict=True);r=adapter.fetch_bars('300817.SZ','5m',520)
    assert calls==[320] and r.complete is False
    assert r.reason_code=='LONG_HISTORY_PROVIDER_BAR_CONFLICT'
