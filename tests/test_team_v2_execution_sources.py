from copy import deepcopy
from contextlib import closing
import hashlib
import json
import sqlite3

import pytest

from test_team_v2_morning import fixture,CAL,advance,source_bytes
from liangjian_funnel.runtime.team_v2_sources import read_planning_scope,read_execution_scope,decode_plan


def setup(tmp_path):
    settings,clock=fixture(tmp_path)
    frozen=read_planning_scope(settings,now=clock.now,calendar=CAL)
    advance(clock,10,1)
    return settings,clock,frozen


def read(settings,clock,frozen,positions=()):
    return read_execution_scope(settings,now=clock.now,calendar=CAL,morning_source=frozen,positions=positions)


def update(settings,sql,*args):
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:db.execute(sql,args)


def test_current_activation_uses_original_payload_and_leaves_legacy_bytes_untouched(tmp_path):
    settings,clock,frozen=setup(tmp_path)
    update(settings,"UPDATE execution_plans SET status='ACTIVE_TODAY',valid_from=?",'2026-10-19T09:32:00+08:00')
    before=source_bytes(settings);result=read(settings,clock,frozen)
    assert result['status']=='COMPLETE' and result['active_plan_ids']==['p1']
    assert result['plan_rows'][0]['payload_json']==frozen['plan_rows'][0]['payload_json']
    plan=decode_plan(result['plans'][0])
    assert plan.status=='ACTIVE_TODAY' and not plan.tradable and not plan.data_ready
    assert plan.expires_at.date()==clock.now.date() and source_bytes(settings)==before
    assert result['plan_rowset_sha256']!=result['morning_rowset_sha256']


def test_null_pending_valid_from_and_expired_or_future_activation_never_become_active(tmp_path):
    settings,clock,frozen=setup(tmp_path)
    update(settings,'UPDATE execution_plans SET valid_from=NULL')
    assert read(settings,clock,frozen)['active_plan_ids']==[]
    for start in ('2026-10-19T10:02:00+08:00','2026-10-18T09:32:00+08:00'):
        update(settings,"UPDATE execution_plans SET status='ACTIVE_TODAY',valid_from=?",start)
        assert read(settings,clock,frozen)['active_plan_ids']==[]
    clock.now=clock.now.replace(hour=15,minute=0)
    update(settings,"UPDATE execution_plans SET valid_from=?",'2026-10-19T09:32:00+08:00')
    assert read(settings,clock,frozen)['active_plan_ids']==[]


@pytest.mark.parametrize('column,value',[('payload_json','{}'),('expires_at','2026-10-20T15:00:00+08:00'),
    ('symbol','000002.SZ')])
def test_changed_payload_geometry_or_database_expiry_is_not_silently_accepted(tmp_path,column,value):
    settings,clock,frozen=setup(tmp_path)
    update(settings,'UPDATE execution_plans SET '+column+'=?',value)
    result=read(settings,clock,frozen)
    assert result['status']=='DATA_LIMITED' and result['plans']==[] and not result['active_plan_ids']
    assert result['reason_codes'][0]['reason_code']=='EXECUTION_MORNING_PAYLOAD_CHANGED'


def test_historical_held_owner_preserved_without_reactivation_or_PM_admission(tmp_path):
    settings,clock,frozen=setup(tmp_path)
    row=deepcopy(frozen['plan_rows'][0]);row.update(plan_id='old',status='INVALIDATED',
        valid_from='2026-10-16T09:32:00+08:00',expires_at='2026-10-16T15:00:00+08:00')
    raw=json.loads(row['payload_json']);raw['symbol']='000002.SZ';row.update(symbol=raw['symbol'],payload_json=json.dumps(raw))
    with closing(sqlite3.connect(settings.state_db_path)) as db,db:
        db.execute('INSERT INTO execution_plans VALUES (?,?,?,?,?,?,?)',tuple(row[k] for k in
            ('plan_id','lane_id','symbol','status','valid_from','expires_at','payload_json')))
    before=source_bytes(settings)
    result=read(settings,clock,frozen,[dict(plan_id='old',symbol='000002.SZ')])
    owner=next(p for p in result['plans'] if p['plan_id']=='old')
    assert owner['status']=='INVALIDATED' and owner['expires_at'].startswith('2026-10-16')
    assert result['held_plan_ids']=={'old':'000002.SZ'} and not result['active_plan_ids']
    assert source_bytes(settings)==before


def test_missing_expected_row_or_unowned_new_active_row_is_explicit_data_gap(tmp_path):
    settings,clock,frozen=setup(tmp_path)
    update(settings,"UPDATE execution_plans SET plan_id='other',status='ACTIVE_TODAY'")
    result=read(settings,clock,frozen)
    assert {r['reason_code'] for r in result['reason_codes']}=={
        'EXECUTION_BOUND_PLAN_MISSING','EXECUTION_PLAN_OUTSIDE_MORNING_SCOPE'}
    assert not result['plans'] and not result['active_plan_ids']


def test_invalid_morning_hash_cannot_become_current_execution_binding(tmp_path):
    settings,clock,frozen=setup(tmp_path);frozen['plan_rows'][0]['payload_json']='{}'
    with pytest.raises(ValueError,match='EXECUTION_MORNING_SOURCE_HASH_MISMATCH'):read(settings,clock,frozen)


@pytest.mark.parametrize('hour,minute',[(9,30),(10,0),(10,2)])
def test_future_or_late_baseline_cannot_become_morning_authority(tmp_path,hour,minute):
    settings,clock,frozen=setup(tmp_path)
    frozen['observed_at']=clock.now.replace(hour=hour,minute=minute).isoformat()
    with pytest.raises(ValueError,match='EXECUTION_MORNING_SOURCE_UNPROVEN'):read(settings,clock,frozen)


def test_actual_readonly_activation_to_morning_caps_PM_and_real_trader_paper_admission(tmp_path):
    from dataclasses import replace
    from decimal import Decimal as D
    from test_team_v2_morning import client
    from test_team_v2_trader_session import fixture as trader_fixture
    from test_team_v2_paper_ledger import bar
    from liangjian_funnel.runtime.team_v2_morning import run_morning_stage,_envelopes
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.runtime.broker.local_paper import create_broker
    from liangjian_funnel.runtime.risk_engine import AccountRisk
    from liangjian_funnel.runtime.team_v2_envelope_refresh import refresh_execution_envelopes
    from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
    settings,clock=fixture(tmp_path/'production-copy');api,morning_seen=client(settings)
    engine=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=api)
    advance(clock,8,38);run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=api)
    update(settings,"UPDATE execution_plans SET status='ACTIVE_TODAY',valid_from=?",'2026-10-19T09:32:00+08:00')
    advance(clock,10,1);before=source_bytes(settings)
    source=read(settings,clock,engine['source'])
    # Isolated current market proof for this integration test, not a claim
    # that the production rule/quote collector has already been wired.
    plan=replace(decode_plan(source['plans'][0]),price=D('10'),tradable=True,data_ready=True)
    args,seen,raw=trader_fixture(tmp_path/'trader-copy')
    directory=settings.root/'state/team_v2';store=TeamMeetingStore(directory/'team.db')
    broker=create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=lambda:clock.now,calendar=CAL,run_id='integration')
    reviewed=store.load_morning_risk('local-paper','2026-10-19',source_call_id='A6:morning:2026-10-19')
    envelopes=refresh_execution_envelopes((plan,),morning_envelopes=reviewed.envelopes,
        account=AccountRisk(D(1000000),D(1000000),D(1000000),D(1000000)),positions=(),
        limits=args['limits'],now=clock.now,regime=engine['source']['market_regime'])
    args.update(store=store,journal=ModelJudgmentJournal(directory/'model.db'),broker=broker,
        account_id='local-paper',pm_meeting_id='morning-pm:2026-10-19',clock=lambda:clock.now,
        all_plans={'p1':plan},envelopes={'p1':envelopes[0]},active_plan_ids=set(source['active_plan_ids']),
        regime=engine['source']['market_regime'])
    result=run_control_minute(session_kwargs=args,closed_bars={plan.symbol:bar(tm='10:00:00')},
        deadline_monotonic=100,monotonic=lambda:0)
    assert result['trader']['model_validated'] and result['trader']['admissions'][0]['dispatched']
    assert len(morning_seen)==2 and len(seen)==1
    assert broker.get_orders('2026-10-19')[0]['qty']<=envelopes[0].max_shares_cap
    assert source_bytes(settings)==before and broker.reconcile()['status']=='MATCH'
