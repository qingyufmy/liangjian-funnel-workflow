"""Real natural edges with controlled HTTP, no production/network/OS clock."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import json
import sqlite3

import httpx
import pytest

from fixture_model_stream import sse_model_response
from test_team_v2_morning import fixture, advance, CAL
from test_team_v2_risk_schema import output as risk_output
from test_team_v2_pm_session import response as pm_output
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.state import RuntimeStore, StateTransitionError
from fixture_candidate_artifacts_0761 import stage_natural_candidate
from liangjian_funnel.runtime.reviewed_batch_natural import  read_natural_candidate
from liangjian_funnel.runtime.reviewed_batch_switch import load_active_reviewed_batch
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage

DAY='2026-10-19'


def setup(tmp_path, monkeypatch, *, old=True, count=1):
    settings,clock=fixture(tmp_path,trade_date=DAY)
    with closing(sqlite3.connect(settings.state_db_path)) as db:
        original=json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
    settings.state_db_path.unlink()
    store=RuntimeStore(settings.state_db_path)
    if old:
        prior=deepcopy(original);prior['source_run_id']='old-source'
        store.publish_plan_batch([dict(plan_id='old-plan',lane_id=settings.research_primary_lane_id,
            symbol=prior['symbol'],status='PENDING_MORNING_REVIEW',valid_from=None,
            expires_at=DAY+'T15:00:00+08:00',payload=prior)])
    raw=deepcopy(original);raw['source_run_id']='new-close'
    batch=[dict(plan_id='p1',lane_id=settings.research_primary_lane_id,symbol=raw['symbol'],
        status='PENDING_MORNING_REVIEW',valid_from=None,expires_at=DAY+'T15:00:00+08:00',payload=raw)]
    if count>1:
        batch=[]
        for i in range(1,count+1):
            payload=deepcopy(raw);payload.update(symbol=f'{i:06d}.SZ',plan_priority_rank=i)
            batch.append(dict(plan_id=f'p{i}',lane_id=settings.research_primary_lane_id,symbol=payload['symbol'],
                status='PENDING_MORNING_REVIEW',valid_from=None,expires_at=DAY+'T15:00:00+08:00',payload=payload))
    store.record_workflow_run(run_id='new-close',lane_id=settings.research_primary_lane_id,
        trade_date=CAL.previous_trading_day(datetime.fromisoformat(DAY).date()).isoformat(),slot='close',model='controlled',status='READY_TO_PUBLISH',snapshot_hash='a'*64)
    stage=stage_natural_candidate(store,batch,run_id='new-close',observed_at=datetime.fromisoformat('2026-10-16T16:00:00+08:00'))
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now',lambda:clock.now)
    return settings,clock,store,batch,stage


def api(settings, *, bad_a6=False, bad_pm=False, empty_pm=False):
    calls=[]
    def handler(request):
        body=json.loads(request.read())
        runtime=json.loads(body['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
        envelope=runtime['required_envelope'];role=envelope['stage_id'];calls.append(role)
        raw=risk_output() if role=='A6' else pm_output();raw['envelope']=envelope
        if role=='A6':
            raw['utilization_cap']['cap_pct']=65
            raw['capacity_for_tomorrow'].update(plans_acceptable=1,cluster_quota={'bank':1})
            if bad_a6:raw['envelope']['run_id']='bad-model-identity'
        else:
            raw['plan_decisions'][0].update(plan_ref='MORNING:PLAN:p1',size_tier='PROBE')
            raw['deployment_target'].update(target_pct=60,baseline_range=[40,65])
            if bad_pm:raw['plan_decisions'][0]['size_tier']='BAD_UNIT'
            if empty_pm:
                raw['plan_decisions'][0].update(decision='WATCH',size_tier='NONE',reason_code='WATCH_TRIGGER')
                raw['shortfall_reason']='NO_ELIGIBLE_PLANS'
        text=json.dumps(raw).replace('"C1"','"MORNING:ACCOUNT"').replace('"M1"','"MORNING:MARKET"')
        text=text.replace('"A"','"MORNING:ACCOUNT"').replace('"M"','"MORNING:MARKET"')
        return sse_model_response(200,json={'choices':[{'message':{'content':text}}]},request=request)
    return OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),calls


def current_plans(store):
    return list(store.list_execution_plans())


@pytest.mark.parametrize('old',[True,False])
def test_actual_three_natural_edges_switch_only_after_original_valid_pm(tmp_path,monkeypatch,old):
    settings,clock,store,batch,staged=setup(tmp_path,monkeypatch,old=old)
    before=current_plans(store);model,calls=api(settings)
    assert store.get_execution_plan('p1') is None
    engine=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert engine['source']['source_namespace']=='NATURAL_MORNING_CANDIDATE' and calls==[]
    assert current_plans(store)==before
    advance(clock,8,35)
    risk=run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    assert risk['status']=='READY' and calls==['A6'] and current_plans(store)==before
    advance(clock,8,38)
    pm=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert pm['model_validated'] is True and calls==['A6','A3']
    assert pm['natural_batch_switch']['scope_id']==staged['scope_id']
    assert store.get_execution_plan('p1')['status']=='PENDING_MORNING_REVIEW'
    assert store.get_execution_plan('p1')['valid_from'] is None
    if old:assert store.get_execution_plan('old-plan')['status']=='SUPERSEDED'
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY)['scope_id']==staged['scope_id']
    assert read_natural_candidate(settings,day=DAY)['state']=='SWITCHED'
    assert store.get_workflow_run('new-close',settings.research_primary_lane_id)['status']=='PUBLISHED'
    same=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert same['natural_batch_switch']['idempotent'] and calls==['A6','A3']


@pytest.mark.parametrize('bad_role',['A6','PM'])
def test_failed_natural_review_never_publishes_or_retires_old_batch(tmp_path,monkeypatch,bad_role):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch)
    before=current_plans(store);model,calls=api(settings,bad_a6=bad_role=='A6',bad_pm=bad_role=='PM')
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert result['status']=='BLOCKED' and current_plans(store)==before
    assert calls==(['A6'] if bad_role=='A6' else ['A6','A3'])
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY) is None
    assert store.get_workflow_run('new-close',settings.research_primary_lane_id)['status']=='READY_TO_PUBLISH'


def test_empty_candidate_never_changes_existing_head_or_old_plan(tmp_path,monkeypatch):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch)
    before=(read_natural_candidate(settings,day=DAY),current_plans(store))
    result=stage_natural_candidate(store,[],run_id='empty-close',observed_at=clock.now)
    assert result['status']=='EMPTY_CANDIDATE_NOT_STAGED'
    assert (read_natural_candidate(settings,day=DAY),current_plans(store))==before


def test_new_candidate_between_engine_and_review_is_not_mixed(tmp_path,monkeypatch):
    settings,clock,store,batch,_=setup(tmp_path,monkeypatch)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    newer=deepcopy(batch);newer[0]['plan_id']='newer';newer[0]['payload']['source_run_id']='newer-close'
    stage_natural_candidate(store,newer,run_id='newer-close',observed_at=clock.now)
    model,calls=api(settings);advance(clock,8,35)
    result=run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    assert result['status']=='BLOCKED' and calls==[] and store.get_execution_plan('p1') is None


def test_missing_actual_a6_never_calls_pm_or_publishes(tmp_path,monkeypatch):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch)
    before=current_plans(store)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,38);model,calls=api(settings)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert result['reason_code']=='NATURAL_CANDIDATE_A6_NOT_ORIGINAL_READY'
    assert current_plans(store)==before and calls==[]


def test_old_row_changed_during_morning_rolls_back_all_publication(tmp_path,monkeypatch):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch)
    model,calls=api(settings)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    store._write(lambda db:db.execute("UPDATE execution_plans SET status='ACTIVE_TODAY' WHERE plan_id='old-plan'").rowcount)
    changed=current_plans(store);advance(clock,8,38)
    with pytest.raises(StateTransitionError,match='OLD_SCOPE_CAS_FAILED'):
        run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert current_plans(store)==changed and calls==['A6','A3']
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY) is None
    assert read_natural_candidate(settings,day=DAY)['state']=='PENDING'


def test_candidate_cas_rejects_replaced_head_at_last_transaction_boundary(tmp_path,monkeypatch):
    settings,clock,store,batch,_=setup(tmp_path,monkeypatch)
    model,calls=api(settings)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    original=RuntimeStore.commit_reviewed_batch_switch
    def race(self,*args,**kwargs):
        newer=deepcopy(batch);newer[0]['plan_id']='newer';newer[0]['payload']['source_run_id']='newer-close'
        stage_natural_candidate(self,newer,run_id='newer-close',observed_at=clock.now)
        return original(self,*args,**kwargs)
    monkeypatch.setattr(RuntimeStore,'commit_reviewed_batch_switch',race)
    before=current_plans(store);advance(clock,8,38)
    with pytest.raises(StateTransitionError,match='CANDIDATE_CAS_FAILED'):
        run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert current_plans(store)==before and calls==['A6','A3']
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY) is None


def test_night_and_missed_edges_never_run_candidate_model_or_switch(tmp_path,monkeypatch):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch);before=current_plans(store)
    model,calls=api(settings)
    for hour,minute in [(0,30),(8,32),(8,46),(15,40)]:
        advance(clock,hour,minute)
        for stage in ('engine','risk','pm'):
            result=run_morning_stage(settings,stage,clock=lambda:clock.now,calendar=CAL,client=model)
            assert result['reason_code']=='MORNING_STAGE_WINDOW_MISSED'
    assert calls==[] and current_plans(store)==before


def test_real_workflow_team_publication_stages_only_candidate_and_candidate_proof(tmp_path,monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace
    from test_w2_shadow_inputs_publication import publisher_fixture,AT,TARGET
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.pipeline.research import StageAudit
    from liangjian_funnel.runtime.publication_receipt import object_hash
    from liangjian_funnel.runtime.history_freshness import session_grid
    from liangjian_funnel.data.cache import MinuteBarStore
    from liangjian_funnel.data.mootdx import MinuteBar
    app,result,data,raw=publisher_fixture()
    app.store=RuntimeStore(tmp_path/'workflow.sqlite3')
    app.settings=SimpleNamespace(root=tmp_path,workflow_output_dir=tmp_path,
        minute_cache_dir=tmp_path/'minute',research_primary_lane_id='lane_1',publish_comparison_lanes=False)
    raw['first_resistance']=20;data['MIN_REWARD_RISK']=2.5
    source_rows=app.fact_cache.query_daily_bars(raw['symbol'])
    data['RECENT_DAILY_BARS']={raw['symbol']:[row['payload'] for row in source_rows[::-1]]}
    audit=StageAudit(lane='lane_1',model='fixture',stage='A3',status='VALIDATED',snapshot_id=result.snapshot_id,
        prompt_hash=None,input_hash=None,output_hash=object_hash(result.lanes[0].final_output),latency_ms=0,
        attempts=1,thinking_variant='team_v2',symbols=(raw['symbol'],),reason_codes=(),output=result.lanes[0].final_output)
    lane=replace(result.lanes[0],stages=(audit,),audit_path=tmp_path/'a3.json')
    lane.audit_path.write_text(json.dumps(lane.as_dict()),encoding='utf-8')
    result=replace(result,snapshot_hash=object_hash(data),lanes=(lane,))
    (tmp_path/'snapshot.json').write_text(json.dumps(dict(snapshot_id=result.snapshot_id,
        snapshot_hash=result.snapshot_hash,as_of=AT.isoformat(),data=data)),encoding='utf-8')
    app.minute_store=MinuteBarStore(app.settings.minute_cache_dir)
    app.minute_store.write([MinuteBar(symbol=raw['symbol'],interval='5m',bar_end=t,open=10,high=11,low=9,
        close=10,volume=1000,amount=10000,source_id='CONTROLLED_TENCENT') for t in session_grid(AT.date())])
    app.market_data=SimpleNamespace(fetch_bars=lambda *a,**kw:pytest.fail('native cache must be reused'))
    app.store.record_workflow_run(run_id=result.run_id,lane_id='lane_1',trade_date=AT.date().isoformat(),
        slot='close',model='controlled',status='READY_TO_PUBLISH',snapshot_hash='a'*64)
    summary=WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,
        minimum_trade_date=TARGET,publication_snapshot_path=tmp_path/'snapshot.json')
    assert summary['publication_state']=='CANDIDATE_PENDING_REVIEW'
    assert summary['created']==summary['activated']==[] and current_plans(app.store)==[]
    assert app.store.get_workflow_run(result.run_id,'lane_1')['status']=='READY_TO_PUBLISH'
    assert summary['projection_manifests'][0]['publication_state']=='UNREVIEWED_CANDIDATE_NOT_EXECUTION_PLANS'
    candidate=read_natural_candidate(SimpleNamespace(state_db_path=app.store.path,research_primary_lane_id='lane_1',
        workflow_output_dir=tmp_path,minute_cache_dir=tmp_path/'minute'),day=TARGET.isoformat())
    assert candidate['state']=='PENDING' and len(candidate['body']['plan_rows'])==1


def test_atomic_failure_after_plan_insert_keeps_old_and_candidate_pending(tmp_path,monkeypatch):
    settings,clock,store,_,_=setup(tmp_path,monkeypatch);before=current_plans(store)
    model,_=api(settings)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    store._write(lambda db:db.execute("CREATE TRIGGER reject_candidate_switch BEFORE UPDATE OF state ON natural_review_candidates WHEN NEW.state='SWITCHED' BEGIN SELECT RAISE(ABORT,'controlled switch failure'); END"))
    # The test intentionally changed schema; authorize the ordinary explicit
    # maintenance check, rather than bypassing the real runtime integrity gate.
    RuntimeStore(store.path,integrity_maintenance=True)
    advance(clock,8,38)
    with pytest.raises(StateTransitionError,match='STATE_CONFLICT'):
        run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert current_plans(store)==before
    assert read_natural_candidate(settings,day=DAY)['state']=='PENDING'
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY) is None
    assert store.get_workflow_run('new-close',settings.research_primary_lane_id)['status']=='READY_TO_PUBLISH'


def test_inconsistent_candidate_stored_sha_failclosed(tmp_path,monkeypatch):
    settings,_,store,_,staged=setup(tmp_path,monkeypatch)
    store._write(lambda db:db.execute('UPDATE natural_review_candidates SET body_sha256=? WHERE scope_id=?',('0'*64,staged['scope_id'])).rowcount)
    with pytest.raises(ValueError,match='HEAD_UNPROVEN'):read_natural_candidate(settings,day=DAY)


def test_real_valid_pm_watch_only_result_is_saved_but_cannot_replace_old_domain(tmp_path,monkeypatch):
    settings,clock,store,_,staged=setup(tmp_path,monkeypatch);before=current_plans(store)
    model,calls=api(settings,empty_pm=True)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35);run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=model)
    advance(clock,8,38)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert result['status']=='NOOP' and result['reason_code']=='NATURAL_CANDIDATE_REVIEW_ZERO_ALLOCATION'
    assert result['old_batch_preserved']
    again=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=model)
    assert again['status']=='NOOP'
    assert current_plans(store)==before and calls==['A6','A3']
    with closing(sqlite3.connect(settings.root/'state/team_v2/model.db')) as db:
        assert db.execute('SELECT validated FROM model_calls WHERE call_id=?',(staged['scope_id']+':PM',)).fetchone()==(1,)
    assert load_active_reviewed_batch(store.path,account_id='local-paper',trade_date=DAY) is None
