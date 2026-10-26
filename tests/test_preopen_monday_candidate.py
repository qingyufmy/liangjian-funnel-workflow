"""Next-session rehearsal: actual 10-23 source -> isolated 10-26 morning."""
from copy import deepcopy
from datetime import datetime
from contextlib import closing
import json,sqlite3,socket
import test_team_v2_morning as morning_fixture
from test_natural_reviewed_batch_0595 import api
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.state import RuntimeStore
from fixture_candidate_artifacts_0761 import stage_natural_candidate
from liangjian_funnel.runtime.reviewed_batch_natural import read_natural_candidate
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.reviewed_batch_switch import load_active_reviewed_batch


def test_monday_original_candidate_requires_actual_controlled_a6_pm_before_cas(tmp_path,monkeypatch):
    calendar=ExchangeTradingCalendar();monkeypatch.setattr(morning_fixture,'CAL',calendar)
    monkeypatch.setattr(socket,'create_connection',lambda *a,**k:(_ for _ in ()).throw(AssertionError('REAL_NETWORK_FORBIDDEN')))
    settings,clock=morning_fixture.fixture(tmp_path,trade_date='2026-10-26')
    with closing(sqlite3.connect(settings.state_db_path)) as db:
        original=json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
    settings.state_db_path.unlink();store=RuntimeStore(settings.state_db_path)
    raw=deepcopy(original);raw['source_run_id']='monday-existing-source'
    row=dict(plan_id='p1',lane_id=settings.research_primary_lane_id,symbol=raw['symbol'],
        status='PENDING_MORNING_REVIEW',expires_at='2026-10-26T15:00:00+08:00',payload=raw)
    candidate=stage_natural_candidate(store,[row],run_id='monday-existing-source',
        observed_at=datetime.fromisoformat('2026-10-25T20:00:00+08:00'))
    store.record_workflow_run(run_id='monday-existing-source',lane_id=settings.research_primary_lane_id,
        trade_date='2026-10-23',slot='close',model='controlled',status='READY_TO_PUBLISH',snapshot_hash='a'*64)
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now',lambda:clock.now)
    class RehearsalClock(datetime):
        @classmethod
        def now(cls,tz=None):return clock.now.astimezone(tz) if tz is not None else clock.now
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_evidence.datetime',RehearsalClock)
    client,calls=api(settings)
    engine=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=calendar)
    assert engine['status']=='COMPLETE' and engine['source']['source_trade_date']=='2026-10-23'
    assert engine['seed_freshness']['plans'][0]['available_5m_count']==48
    assert store.list_execution_plans()==()
    morning_fixture.advance(clock,8,35)
    assert run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=calendar,client=client)['status']=='READY'
    assert store.list_execution_plans()==()
    morning_fixture.advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=calendar,client=client)
    assert result['natural_batch_switch']['scope_id']==candidate['scope_id']
    assert calls==['A6','A3']
    assert read_natural_candidate(settings,day='2026-10-26')['state']=='SWITCHED'
    assert len(store.list_execution_plans())==1
