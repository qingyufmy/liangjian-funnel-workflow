from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.morning_research_contract import inspect_morning_research, enqueue_morning_research_alert
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.workflow import WorkflowApplication

TZ=ZoneInfo('Asia/Shanghai')
NOW=datetime(2026,10,21,8,30,tzinfo=TZ)


def fixture(tmp_path, *, status='PUBLISHED', slot='close', trade_date='2026-10-20'):
    store=RuntimeStore(tmp_path/'workflow.db')
    store.record_workflow_run(run_id='source',lane_id='lane_1',trade_date=trade_date,slot=slot,
        model='fixture',status=status,snapshot_hash='fixture')
    settings=SimpleNamespace(root=tmp_path,workflow_output_dir=tmp_path/'outputs',research_primary_lane_id='lane_1')
    row=store.create_execution_plan('p','lane_1','000001.SZ',status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=NOW.replace(hour=15),payload={'source_run_id':'source','stop_level':9,'trigger_high':11,'no_chase':11.2})
    return store,settings,row


@pytest.mark.parametrize('status',['PUBLISHED','READY','READY_DEGRADED'])
@pytest.mark.parametrize('slot',['close','CLOSE'])
def test_only_completed_previous_close_is_qualified(tmp_path,status,slot):
    store,s,row=fixture(tmp_path,status=status,slot=slot)
    result=inspect_morning_research(store,s,[row],now=NOW)
    assert result['status']=='READY' and result['rejected_plans']==[]
    assert result['research_model_calls']==0


@pytest.mark.parametrize('slot,day,reason',[('morning','2026-10-20','MORNING_SOURCE_NOT_PREVIOUS_CLOSE'),
    ('UNKNOWN','2026-10-20','MORNING_SOURCE_NOT_PREVIOUS_CLOSE'),
    ('close','2026-10-21','MORNING_SOURCE_NOT_PREVIOUS_CLOSE'),
    ('close','2026-10-19','MORNING_SOURCE_NOT_PREVIOUS_CLOSE')])
def test_same_day_research_older_day_and_morning_cannot_be_morning_plan_source(tmp_path,slot,day,reason):
    store,s,row=fixture(tmp_path,slot=slot,trade_date=day)
    result=inspect_morning_research(store,s,[row],now=NOW)
    assert result['rejected_plans']==[{'plan_id':'p','reason_code':reason}]


@pytest.mark.parametrize('slot',['close','CLOSE'])
def test_failed_close_not_market_empty_and_alert_once_across_0830_and_0926(tmp_path,slot):
    store,s,row=fixture(tmp_path,status='BLOCKED',slot=slot)
    meeting=TeamMeetingStore(tmp_path/'state/team_v2/team.db')
    result=inspect_morning_research(store,s,[],now=NOW)
    assert result['status']=='BLOCKED' and result['reason_code']=='PREVIOUS_CLOSE_RESEARCH_FAILED'
    one=enqueue_morning_research_alert(s,result,now=NOW)
    two=enqueue_morning_research_alert(s,result,now=NOW.replace(hour=9,minute=26))
    assert one['event_id']==two['event_id'] and one['actual_http']==two['actual_http']==0
    with meeting._connect() as db:
        assert db.execute('SELECT kind,status,COUNT(*) FROM team_notifications GROUP BY kind,status').fetchall()==[('ALERT','PENDING',1)]


@pytest.mark.parametrize('slot',['close','CLOSE'])
def test_no_research_is_explicit_unavailable_but_healthy_zero_is_legitimate(tmp_path,slot):
    store,s,_=fixture(tmp_path,status='READY',slot=slot)
    result=inspect_morning_research(store,s,[],now=NOW)
    assert result['status']=='READY'
    result=inspect_morning_research(store,s,[],now=NOW.replace(day=22))
    assert result['reason_code']=='PREVIOUS_CLOSE_RESEARCH_UNAVAILABLE'


def test_late_night_recovery_uses_original_market_session_not_creation_date(tmp_path):
    store,s,row=fixture(tmp_path,trade_date='2026-10-21')
    runs=s.workflow_output_dir/'runs';runs.mkdir(parents=True)
    (runs/'source.json').write_text(json.dumps(dict(run_id='source',slot='close',status='READY',
        market_trade_date='2026-10-20',source_as_of='2026-10-21T00:30:00+08:00')))
    assert not inspect_morning_research(store,s,[row],now=NOW)['rejected_plans']


def test_workflow_failed_close_no_pending_returns_blocked_not_silent_ready(tmp_path):
    store,s,_=fixture(tmp_path,status='BLOCKED')
    store.invalidate_plan('p')
    app=SimpleNamespace(store=store,settings=s,brokers={'lane_1':None},_ensure_trading_day=lambda _:None)
    result=WorkflowApplication.review_pending_morning(app,now=NOW.replace(hour=9,minute=26))
    assert result['status']=='BLOCKED' and result['reason_code']=='PREVIOUS_CLOSE_RESEARCH_FAILED'
    assert result['research_contract']['research_model_calls']==0 and result['activated']==[]


def test_workflow_no_quote_or_model_for_known_same_day_plan_source(tmp_path):
    store,s,_=fixture(tmp_path,trade_date='2026-10-21')
    def forbidden(*a,**kw):raise AssertionError('No quote or model before source contract')
    app=SimpleNamespace(store=store,settings=s,brokers={'lane_1':None},_ensure_trading_day=lambda _:None,
        market_data=SimpleNamespace(fetch_quote=forbidden),run_research=forbidden)
    result=WorkflowApplication.review_pending_morning(app,now=NOW.replace(hour=9,minute=26))
    assert result['status']=='BLOCKED' and result['activated']==[]
    assert store.get_execution_plan('p')['status']=='PENDING_MORNING_REVIEW'


def test_real_exchange_calendar_monday_uses_friday(tmp_path):
    store,s,row=fixture(tmp_path,trade_date='2026-10-16')
    result=inspect_morning_research(store,s,[row],now=NOW.replace(day=19))
    assert result['previous_session']=='2026-10-16' and result['rejected_plans']==[]
