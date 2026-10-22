"""LJ-052 local native-cache contracts; no source/model calls."""
from datetime import date,datetime,timedelta
from types import SimpleNamespace
import hashlib
import json

import pytest

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.history_freshness import (
    session_grid,validate_session_seed,read_previous_session_seed,inspect_plan_seeds)

NOW=datetime.fromisoformat('2026-10-22T08:33:00+08:00')
CAL=SimpleNamespace(previous_trading_day=lambda day:date(2026,10,21))


def bars(symbol='002354.SZ',day=date(2026,10,21)):
    return tuple(MinuteBar(symbol=symbol,interval='5m',bar_end=end,open=10,high=11,low=9,close=10,
        volume=100,amount=1000,source_id='tencent:native',adjust_mode='none') for end in session_grid(day))


@pytest.mark.parametrize('symbol,day,count,status',[
    ('002354.SZ',date(2026,9,23),48,'DATA_BLOCKED'),
    ('300556.SZ',date(2026,10,21),48,'READY'),
    ('600201.SH',date(2026,10,21),0,'DATA_BLOCKED')])
def test_today_three_original_date_and_count_mechanisms(symbol,day,count,status):
    original=bars(symbol,day)[:count]
    selected,proof=validate_session_seed(symbol,original,target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert proof['status']==status and len(selected)==(48 if status=='READY' else 0)
    assert proof['source_trade_date']=='2026-10-21' and proof['source_requests']==0
    assert proof['actual_latest_date']==(day.isoformat() if count else None)


@pytest.mark.parametrize('mutation',['47','duplicate','off-grid','wrong-symbol','derived','future'])
def test_no_partial_or_stale_or_foreign_fallback(mutation):
    values=list(bars())
    if mutation=='47':values.pop()
    if mutation=='duplicate':values[-1]=values[0]
    if mutation=='off-grid':values[-1]=values[-1].model_copy(update={'bar_end':values[-1].bar_end+timedelta(minutes=1)})
    if mutation=='wrong-symbol':values[-1]=values[-1].model_copy(update={'symbol':'600201.SH'})
    if mutation=='derived':values[-1]=values[-1].model_copy(update={'source_id':'derived:fake'})
    if mutation=='future':values[-1]=values[-1].model_copy(update={'bar_end':NOW+timedelta(days=1)})
    selected,proof=validate_session_seed('002354.SZ',values,target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert selected==() and proof['status']=='DATA_BLOCKED'


def test_date_bounded_sql_ignores_older_rows_and_repair_is_real_complete_day(tmp_path):
    store=MinuteBarStore(tmp_path/'minute')
    store.write(bars(day=date(2026,9,23)))
    selected,proof=read_previous_session_seed(store.path,'002354.SZ',target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert selected==() and proof['actual_latest_date']=='2026-09-23'
    assert proof['available_5m_count']==0 and proof['status']=='DATA_BLOCKED'
    store.write(bars())
    before=hashlib.sha256(store.path.read_bytes()).hexdigest()
    selected,proof=read_previous_session_seed(store.path,'002354.SZ',target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert len(selected)==48 and proof['status']=='READY' and proof['actual_latest_date']=='2026-10-21'
    assert hashlib.sha256(store.path.read_bytes()).hexdigest()==before
    assert [b.bar_end for b in selected]==list(session_grid(date(2026,10,21)))


def test_calendar_holiday_previous_session_not_yesterday():
    calendar=SimpleNamespace(previous_trading_day=lambda day:date(2026,9,30))
    selected,proof=validate_session_seed('002354.SZ',bars(day=date(2026,9,30)),
        target_trade_date=date(2026,10,9),calendar=calendar,as_of=NOW)
    assert len(selected)==48 and proof['source_trade_date']=='2026-09-30'


def test_calendar_failure_is_blocked_not_weekday_fallback(tmp_path):
    def fail(_):raise RuntimeError('calendar unavailable')
    selected,proof=read_previous_session_seed(tmp_path/'absent','002354.SZ',
        target_trade_date=NOW.date(),calendar=SimpleNamespace(previous_trading_day=fail),as_of=NOW)
    assert selected==() and proof['status']=='DATA_BLOCKED' and proof['source_trade_date'] is None
    assert proof['reason_code']=='HISTORICAL_5M_PREVIOUS_SESSION_UNPROVEN'
    assert proof['source_read_status']=='NOT_ATTEMPTED' and proof['available_5m_count'] is None
    assert proof['actual_latest_date_known'] is False


def test_absent_cache_is_unavailable_not_observed_zero_rows(tmp_path):
    selected,proof=read_previous_session_seed(tmp_path/'absent','002354.SZ',
        target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert selected==() and proof['status']=='DATA_BLOCKED'
    assert proof['reason_code']=='HISTORICAL_5M_CACHE_UNAVAILABLE' and proof['source_read_status']=='UNAVAILABLE'
    assert proof['available_5m_count'] is None and proof['actual_latest_date_known'] is False
    assert proof['source_requests']==0


@pytest.mark.parametrize('sqlite_code,reason,status',[
    (5,'HISTORICAL_5M_READ_FAILED','READ_FAILED'),
    (9,'HISTORICAL_5M_TIME_BUDGET','TIME_BUDGET')])
def test_sql_lock_and_interrupt_are_read_failures_not_missing_market_bars(tmp_path,monkeypatch,sqlite_code,reason,status):
    from liangjian_funnel.runtime import history_freshness
    import sqlite3
    path=tmp_path/'present';path.write_bytes(b'owned mock file')
    def fail(*a,**kw):
        exc=sqlite3.OperationalError('SECRET_TEST_DETAIL_MUST_NOT_APPEAR')
        exc.sqlite_errorcode=sqlite_code
        raise exc
    monkeypatch.setattr(history_freshness.sqlite3,'connect',fail)
    selected,proof=read_previous_session_seed(path,'002354.SZ',target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert selected==() and proof['status']=='DATA_BLOCKED' and proof['source_requests']==0
    assert proof['reason_code']==reason and proof['source_read_status']==status
    assert proof['available_5m_count'] is None and proof['available_5m_count_known'] is False
    assert proof['actual_latest_date'] is None and proof['actual_latest_date_known'] is False
    assert 'SECRET_TEST_DETAIL' not in json.dumps(proof)


def test_all_plan_preflight_includes_non_trend_and_missing_cache(tmp_path):
    report=inspect_plan_seeds([dict(plan_id='p1',symbol='002354.SZ'),dict(plan_id='p2',symbol='600201.SH')],
        path=tmp_path/'absent',target_trade_date=NOW.date(),calendar=CAL,as_of=NOW)
    assert report['status']=='DATA_LIMITED' and len(report['plans'])==2
    assert all(row['status']=='DATA_BLOCKED' for row in report['plans']) and report['source_requests']==0


@pytest.mark.parametrize('held',[False,True])
def test_server_readiness_blocks_new_entry_but_does_not_replace_held_exit(monkeypatch,held):
    from liangjian_funnel.runtime import strategies
    original=dict(state='SELL_SIGNAL' if held else 'BUY_SIGNAL',action='SELL_SIGNAL' if held else 'BUY_SIGNAL',reason_codes=[])
    monkeypatch.setattr(strategies,'evaluate_a4_plan',lambda *a,**kw:dict(original))
    result=strategies.evaluate_strategy(dict(symbol='002354.SZ'),{},now=NOW,
        position={'qty':100} if held else None,
        market_context={'historical_5m_readiness':dict(status='DATA_BLOCKED',actual_latest_date='2026-09-23',
            reason_code='HISTORICAL_5M_PREVIOUS_SESSION_INCOMPLETE')})
    assert result.action==('SELL_SIGNAL' if held else 'DATA_BLOCK')
    if not held:assert result.historical_5m_readiness['actual_latest_date']=='2026-09-23'


def test_missing_seed_blocks_add_even_with_holding(monkeypatch):
    from liangjian_funnel.runtime import strategies
    monkeypatch.setattr(strategies,'evaluate_a4_plan',lambda *a,**kw:dict(action='ADD_SIGNAL',reason_codes=[]))
    result=strategies.evaluate_strategy(dict(symbol='002354.SZ'),{},now=NOW,position={'qty':100},
        market_context={'historical_5m_readiness':dict(status='DATA_BLOCKED')})
    assert result.action=='DATA_BLOCK'


def test_real_morning_missing_seed_is_frozen_limited_and_queues_once(tmp_path):
    from test_team_v2_morning import fixture,CAL as morning_calendar
    from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
    import sqlite3
    settings,clock=fixture(tmp_path)
    # Test-owned cache only: remove the valid prior day, leaving the real
    # morning entry to discover absence without any source call.
    with sqlite3.connect(settings.minute_cache_dir/'minute_bars.sqlite3') as db:
        db.execute('DELETE FROM minute_bars')
    first=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=morning_calendar)
    second=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=morning_calendar)
    assert first==second and first['seed_freshness']['status']=='DATA_LIMITED'
    envelope=first['envelopes'][0]
    assert envelope['eligible'] is False and envelope['risk_unit']=='NONE' and envelope['risk_budget']=='0'
    assert 'DATA_LIMITED' in envelope['degradation_tags']
    with sqlite3.connect(settings.root/'state/team_v2/team.db') as db:
        rows=db.execute("SELECT source_id,status FROM team_notifications WHERE source_id=?",('morning-seed:2026-10-19',)).fetchall()
    assert rows==[('morning-seed:2026-10-19','PENDING')]
