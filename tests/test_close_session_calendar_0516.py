"""Permanent source/target session contract; no clock patching or model calls."""
from datetime import date,datetime
import pytest
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar,closed_research_session,TradingCalendarError
from liangjian_funnel.runtime.close_regeneration import bind_close_regeneration,require_regeneration_publication_window
from test_close_regeneration_20261021 import app_fixture,HEAD,NOW

@pytest.mark.parametrize('clock',['2026-10-21T15:10:00+08:00','2026-10-21T23:59:59+08:00',
 '2026-10-22T00:00:00+08:00','2026-10-22T06:29:59+08:00'])
def test_evening_and_next_preopen_bind_same_sessions(clock):
    real=datetime.fromisoformat(clock);s=closed_research_session(real,ExchangeTradingCalendar())
    assert s.source_trade_date==date(2026,10,21) and s.target_trade_date==date(2026,10,22)
    assert s.wall_clock==real and s.publication_deadline.isoformat()=='2026-10-22T06:30:00+08:00'

@pytest.mark.parametrize('clock',['2026-10-22T06:30:00+08:00','2026-10-22T07:04:00+08:00',
 '2026-10-22T12:00:00+08:00'])
def test_deadline_rejects_without_changing_source_date(clock):
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE_REACHED'):
        closed_research_session(datetime.fromisoformat(clock),ExchangeTradingCalendar())

def test_weekend_and_exchange_holiday_use_calendar_not_weekday():
    cal=ExchangeTradingCalendar()
    for clock,source,target in [('2026-10-24T22:00:00+08:00','2026-10-23','2026-10-26'),
        ('2026-10-01T22:00:00+08:00','2026-09-30','2026-10-08')]:
        s=closed_research_session(datetime.fromisoformat(clock),cal)
        assert (s.source_trade_date.isoformat(),s.target_trade_date.isoformat())==(source,target)

def test_regen_optional_date_is_assertion_not_unlock_and_keeps_data_clock(tmp_path):
    app,gen,_=app_fixture(tmp_path);real=NOW.replace(day=22,hour=1)
    p=bind_close_regeneration(app,'old',now=real,head=HEAD,source_trade_date='2026-10-21')
    assert p.wall_clock==real and p.source_data_as_of==gen.as_of and p.source_data_as_of.date()!=real.date()
    assert p.run_id=='regen-20261021-'+HEAD
    assert p.old_plan_ids==('0','1','2')
    with pytest.raises(ValueError,match='REGEN_SOURCE_TRADE_DATE_MISMATCH'):
        bind_close_regeneration(app,'old',now=real,head=HEAD,source_trade_date='2026-10-22')
    with pytest.raises(ValueError,match='REGEN_SOURCE_TRADE_DATE_MISMATCH'):
        bind_close_regeneration(app,'old',now=real,head=HEAD,source_trade_date='bad')
    require_regeneration_publication_window(p,app.trading_calendar,now=real.replace(hour=6,minute=29))
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE_REACHED'):
        require_regeneration_publication_window(p,app.trading_calendar,now=real.replace(hour=6,minute=30))
    assert len(app.store.list_execution_plans(status='PENDING_MORNING_REVIEW'))==3

def test_new_close_after_deadline_targets_following_session():
    s=closed_research_session(datetime.fromisoformat('2026-10-22T15:10:00+08:00'),ExchangeTradingCalendar())
    assert (s.source_trade_date,s.target_trade_date)==(date(2026,10,22),date(2026,10,23))

def test_calendar_failure_never_guesses_weekdays():
    def fail(*args,**kwargs):raise RuntimeError('unavailable')
    with pytest.raises(TradingCalendarError):
        closed_research_session(NOW,ExchangeTradingCalendar(factory=fail))

def test_naive_clock_rejected():
    with pytest.raises(ValueError,match='CLOCK_INVALID'):
        closed_research_session(datetime(2026,10,21,23),ExchangeTradingCalendar())

def test_actual_cli_forwards_optional_source_assertion_without_choosing_target(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.close_regeneration as regen
    from test_cli_workflow_coverage import _FakeApplication,_settings,_ready_response
    calls=[]
    monkeypatch.setattr(cli,'WorkflowApplication',_FakeApplication)
    def execute(app,source_run_id,**kwargs):
        calls.append((source_run_id,kwargs));return _ready_response()
    monkeypatch.setattr(regen,'run_close_regeneration',execute)
    assert cli.main(['run-close','--regenerate-from','old','--source-trade-date','2026-10-21'],settings=_settings(tmp_path))==0
    assert calls==[('old',{'source_trade_date':'2026-10-21'})]
    assert cli.main(['run-close','--source-trade-date','2026-10-21'],settings=_settings(tmp_path))==2
    assert len(calls)==1

def test_wall_clock_audit_is_durable_before_research_and_no_clock_is_patched(tmp_path,monkeypatch):
    import json
    import liangjian_funnel.runtime.close_regeneration as regen
    app,_,_=app_fixture(tmp_path)
    # This test binds pure input dates; production's datetime.now is untouched.
    real=NOW.replace(day=22,hour=1);proof=bind_close_regeneration(app,'old',now=real,head=HEAD)
    monkeypatch.setattr(regen,'bind_close_regeneration',lambda *args,**kwargs:proof)
    monkeypatch.setattr(regen.subprocess,'check_output',lambda *args,**kwargs:HEAD+'\n')
    seen=[]
    def research(*args,**kwargs):
        path=app.settings.workflow_output_dir/'runs'/f'{proof.run_id}.binding.json'
        saved=json.loads(path.read_text())
        assert saved['wall_clock']==real.isoformat() and saved['caller']=='cli:run-close'
        assert saved['source_trade_date']=='2026-10-21' and saved['target_trade_date']=='2026-10-22'
        assert saved['clock_override'] is False
        assert kwargs['as_of']==proof.source_data_as_of and kwargs['regeneration'] is proof
        seen.append(kwargs);return {'status':'READY'}
    app.run_research=research;app.settings.root=tmp_path
    assert regen.run_close_regeneration(app,'old')['status']=='READY'
    with pytest.raises(ValueError,match='REGEN_RUN_ALREADY_STARTED'):regen.run_close_regeneration(app,'old')
    assert len(seen)==1
