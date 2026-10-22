"""0603 natural READY source is not a fabricated failed regeneration receipt."""
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace as NS
import hashlib
import json

import pytest

from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.close_regeneration import (
    bind_close_regeneration,regeneration_manifest,run_close_regeneration,
)
from liangjian_funnel.runtime.reviewed_batch_natural import stage_natural_candidate,read_natural_candidate
from liangjian_funnel.runtime.state import RuntimeStore

NOW=datetime.fromisoformat('2026-10-22T23:00:00+08:00')
HEAD='a'*40


def plan(key,run='source',day='2026-10-23'):
    return dict(plan_id=key,lane_id='lane_1',symbol='002354.SZ',status='PENDING_MORNING_REVIEW',
        valid_from=None,expires_at=day+'T15:00:00+08:00',
        payload=dict(symbol='002354.SZ',source_run_id=run,risk_unit='PROBE'))


def fixture(tmp_path,*,candidate=False):
    store=RuntimeStore(tmp_path/'state.db');out=tmp_path/'outputs'
    (out/'runs').mkdir(parents=True);(out/'research').mkdir()
    receipt=(stage_natural_candidate(store,[plan('c1')],run_id='source',observed_at=NOW)
        if candidate else dict(status='EMPTY_CANDIDATE_NOT_STAGED',plan_ids=[]))
    summary=dict(run_id='source',market_trade_date='2026-10-22',target_trade_date='2026-10-23',
        status='READY',slot='close',plan_publication=dict(created=[],natural_candidate=receipt))
    audit=dict(lane='lane_1',status='READY',stages=[dict(stage='A2',status='VALIDATED',
        input_hash='b'*64,output_hash='c'*64,snapshot_id='original',
        diagnostics=dict(provider_validation_history=[[]],validation_history=[[]],team_call_ids=['A2:source']))])
    summary_path=out/'runs/source.json';audit_path=out/'research/research_source_lane_1.json'
    summary_path.write_text(json.dumps(summary));audit_path.write_text(json.dumps(audit))
    generation=NS(as_of=NOW.replace(hour=22),snapshot_id='fresh',snapshot_hash='d'*64)
    settings=NS(workflow_output_dir=out,research_primary_lane_id='lane_1',state_db_path=store.path)
    app=NS(store=store,settings=settings,trading_calendar=ExchangeTradingCalendar(),
        a1_registry=NS(require_active=lambda **_:generation))
    return app,summary,audit,generation


def bind(app,**kwargs):
    return bind_close_regeneration(app,'source',now=NOW,head=HEAD,recovery_attempt=2,**kwargs)


@pytest.mark.parametrize('candidate',[False,True])
def test_ready_original_source_binds_without_fictional_failed_terminal(tmp_path,candidate):
    app,summary,_,_=fixture(tmp_path,candidate=candidate)
    # An expired prior date's three pending rows must not become Friday's source.
    app.store.publish_plan_batch([plan(str(i),'expired','2026-10-21') for i in range(3)])
    proof=bind(app)
    assert proof.old_plan_ids==() and proof.recovers=='source'
    assert proof.recovery_authority=='0603' and proof.recovery_terminal_sha256 is None
    assert proof.run_id=='regen-20261022-'+HEAD+'-r2'
    assert proof.source_kind==('NATURAL_CANDIDATE_SOURCE_REGEN' if candidate else 'NATURAL_ZERO_PLAN_SOURCE_REGEN')
    assert proof.source_a2_evidence['provider_validation_history']==[[]]
    assert proof.source_summary_sha256==hashlib.sha256((app.settings.workflow_output_dir/'runs/source.json').read_bytes()).hexdigest()
    assert not list((app.settings.workflow_output_dir/'runs').glob('*.binding.json'))


@pytest.mark.parametrize('candidate',[False,True])
@pytest.mark.parametrize('nonempty',[False,True])
def test_new_result_stages_or_preserves_without_execution_writes(tmp_path,candidate,nonempty):
    app,_,_,_=fixture(tmp_path,candidate=candidate);proof=bind(app)
    before=read_natural_candidate(app.settings,day='2026-10-23')
    receipt=stage_natural_candidate(app.store,[plan('new',proof.run_id)] if nonempty else [],
        run_id=proof.run_id,observed_at=NOW,regeneration=proof)
    assert list(app.store.list_execution_plans())==[]
    after=read_natural_candidate(app.settings,day='2026-10-23')
    assert (after['body']['run_id']==proof.run_id) if nonempty else after==before
    manifest=regeneration_manifest(proof,NS(data={},as_of=NOW),raw_pass=True,created=[],
        publication=dict(natural_candidate=receipt))
    assert manifest['status']==('CANDIDATE_PENDING_REVIEW' if nonempty else 'EMPTY_NOT_PUBLISHED')
    assert manifest['replaced_plan_ids']==manifest['new_plan_ids']==[]
    assert manifest['source_kind']==proof.source_kind


@pytest.mark.parametrize('candidate',[False,True])
@pytest.mark.parametrize('nonempty',[False,True])
def test_concurrent_candidate_head_change_rejects_at_same_write_transaction(tmp_path,candidate,nonempty):
    app,_,_,_=fixture(tmp_path,candidate=candidate);proof=bind(app)
    newer=stage_natural_candidate(app.store,[plan('newer','concurrent')],run_id='concurrent',observed_at=NOW)
    with pytest.raises(ValueError,match='REGEN_SOURCE_CANDIDATE_SCOPE_CHANGED'):
        stage_natural_candidate(app.store,[plan('new',proof.run_id)] if nonempty else [],
            run_id=proof.run_id,observed_at=NOW,regeneration=proof)
    assert read_natural_candidate(app.settings,day='2026-10-23')['scope_id']==newer['scope_id']
    assert list(app.store.list_execution_plans())==[]


def test_concurrent_target_execution_row_is_not_replaced(tmp_path):
    app,_,_,_=fixture(tmp_path);proof=bind(app)
    app.store.publish_plan_batch([plan('concurrent','other')])
    with pytest.raises(ValueError,match='REGEN_SOURCE_CANDIDATE_SCOPE_CHANGED'):
        stage_natural_candidate(app.store,[plan('new',proof.run_id)],run_id=proof.run_id,
            observed_at=NOW,regeneration=proof)
    assert [row['plan_id'] for row in app.store.list_execution_plans()]==['concurrent']


def test_source_candidate_body_change_cannot_hide_behind_unchanged_head(tmp_path):
    app,_,_,_=fixture(tmp_path,candidate=True);proof=bind(app)
    app.store._write(lambda db:db.execute("UPDATE natural_review_candidates SET body_json=body_json || ' '").rowcount)
    with pytest.raises(ValueError,match='REGEN_SOURCE_CANDIDATE_SCOPE_CHANGED'):
        stage_natural_candidate(app.store,[],run_id=proof.run_id,observed_at=NOW,regeneration=proof)


@pytest.mark.parametrize('field,value',[
    ('market_trade_date','2026-10-21'),('target_trade_date','2026-10-24'),('run_id','other'),
    ('status','BLOCKED'),('slot','premarket')])
def test_source_original_identity_date_and_state_are_strict(tmp_path,field,value):
    app,summary,_,_=fixture(tmp_path);summary[field]=value
    (app.settings.workflow_output_dir/'runs/source.json').write_text(json.dumps(summary))
    with pytest.raises(ValueError,match='REGEN_SOURCE_DATE_OR_STATE_INVALID'):bind(app)


@pytest.mark.parametrize('change',[
    lambda a:a.update(status='BLOCKED'),
    lambda a:a['stages'][0].update(status='A2_REVIEW_DEGRADED'),
    lambda a:a['stages'][0]['diagnostics'].update(provider_validation_history=[['raw-invalid']]),
    lambda a:a['stages'][0]['diagnostics'].update(validation_history=[['postpolicy-invalid']]),
    lambda a:a['stages'][0].update(input_hash='bad'),
])
def test_raw_and_postpolicy_validated_original_is_mandatory(tmp_path,change):
    app,_,audit,_=fixture(tmp_path);change(audit)
    (app.settings.workflow_output_dir/'research/research_source_lane_1.json').write_text(json.dumps(audit))
    with pytest.raises(ValueError,match='REGEN_SOURCE_RAW_A2_UNPROVEN'):bind(app)


@pytest.mark.parametrize('name',['runs/source.json','research/research_source_lane_1.json'])
def test_original_byte_change_after_binding_cannot_stage(tmp_path,name):
    app,_,_,_=fixture(tmp_path);proof=bind(app)
    path=app.settings.workflow_output_dir/name;path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='REGEN_SOURCE_EVIDENCE_CHANGED'):
        stage_natural_candidate(app.store,[plan('new',proof.run_id)],run_id=proof.run_id,
            observed_at=NOW,regeneration=proof)


@pytest.mark.parametrize('kwargs',[
    dict(recovery_from='invented-failure'),dict(recovery_terminal='invented-terminal'),
])
def test_natural_source_cannot_claim_prior_failure(tmp_path,kwargs):
    app,_,_,_=fixture(tmp_path)
    with pytest.raises(ValueError,match='REGEN_NATURAL_RECOVERY_ARGUMENTS_INVALID'):bind(app,**kwargs)


def test_source_pointer_receipt_cannot_be_substituted_and_current_scope_cannot_be_nonempty(tmp_path):
    app,summary,_,_=fixture(tmp_path,candidate=True)
    summary['plan_publication']['natural_candidate']['body_sha256']='f'*64
    (app.settings.workflow_output_dir/'runs/source.json').write_text(json.dumps(summary))
    with pytest.raises(ValueError,match='REGEN_SOURCE_CANDIDATE_SCOPE_CHANGED'):bind(app)
    app.store.publish_plan_batch([plan('target')])
    with pytest.raises(ValueError,match='REGEN_SOURCE_PENDING_SCOPE_CHANGED'):bind(app)


def test_fresh_a1_and_six_thirty_deadline_remain_hard(tmp_path):
    app,_,_,generation=fixture(tmp_path);generation.as_of=NOW.replace(day=21)
    with pytest.raises(ValueError,match='REGEN_FRESH_A1_REQUIRED'):bind(app)
    generation.as_of=NOW.replace(hour=22)
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE_REACHED'):
        bind_close_regeneration(app,'source',now=NOW.replace(day=23,hour=6,minute=30),head=HEAD,recovery_attempt=2)


@pytest.mark.parametrize('change',['run','date','lane'])
def test_new_candidate_cannot_leave_bound_target_or_run(tmp_path,change):
    app,_,_,_=fixture(tmp_path);proof=bind(app);row=plan('new',proof.run_id)
    run=proof.run_id
    if change=='run':run='other';row['payload']['source_run_id']=run
    if change=='date':row['expires_at']='2026-10-26T15:00:00+08:00'
    if change=='lane':row['lane_id']='other'
    with pytest.raises(ValueError,match='REGEN_TARGET_'):
        stage_natural_candidate(app.store,[row],run_id=run,observed_at=NOW,regeneration=proof)
    assert read_natural_candidate(app.settings,day='2026-10-23') is None


@pytest.mark.parametrize('nonempty',[False,True])
def test_actual_workflow_publication_of_regeneration_never_inserts_execution(tmp_path,monkeypatch,nonempty):
    from dataclasses import replace
    from datetime import date
    from test_w2_shadow_inputs_publication import publisher_fixture
    from liangjian_funnel.workflow import WorkflowApplication
    import liangjian_funnel.workflow as workflow
    app,_,_,_=fixture(tmp_path);proof=bind(app)
    publishing,result,data,raw=publisher_fixture()
    publishing.store=app.store;publishing.settings=app.settings
    result=replace(result,run_id=proof.run_id,generated_at=NOW,
        lanes=(replace(result.lanes[0],stages=(NS(stage='A3',thinking_variant='team_v2',output={}),),
            final_output=dict(core_watch_pool=[raw] if nonempty else [],secondary_watch_pool=[])),))
    raw['first_resistance']=20;data['MIN_REWARD_RISK']=2.5
    data['snapshot_manifest'].update(as_of=NOW.isoformat(),source_failures={})
    class Clock(datetime):
        @classmethod
        def now(cls,*_):return NOW
    monkeypatch.setattr(workflow,'datetime',Clock)
    receipt=WorkflowApplication._publish_plans(publishing,result,'close',NOW,snapshot_data=data,
        minimum_trade_date=date(2026,10,23),regeneration=proof)
    assert receipt['created']==receipt['activated']==[] and list(app.store.list_execution_plans())==[]
    assert receipt['publication_state']==('CANDIDATE_PENDING_REVIEW' if nonempty else 'EMPTY_NOT_PUBLISHED')
    candidate=read_natural_candidate(app.settings,day='2026-10-23')
    assert bool(candidate)==nonempty
    if candidate:assert candidate['body']['run_id']==proof.run_id


def test_last_publication_boundary_rechecks_actual_deadline(tmp_path,monkeypatch):
    from dataclasses import replace
    from datetime import date
    from test_w2_shadow_inputs_publication import publisher_fixture
    from liangjian_funnel.workflow import WorkflowApplication
    import liangjian_funnel.workflow as workflow
    app,_,_,_=fixture(tmp_path);proof=bind(app);publishing,result,data,_=publisher_fixture()
    publishing.store=app.store;publishing.settings=app.settings
    result=replace(result,run_id=proof.run_id,lanes=(replace(result.lanes[0],
        final_output=dict(core_watch_pool=[],secondary_watch_pool=[])),))
    class Clock(datetime):
        @classmethod
        def now(cls,*_):return NOW.replace(day=23,hour=6,minute=30)
    monkeypatch.setattr(workflow,'datetime',Clock)
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE_REACHED'):
        WorkflowApplication._publish_plans(publishing,result,'close',NOW,snapshot_data=data,
            minimum_trade_date=date(2026,10,23),regeneration=proof)
    assert list(app.store.list_execution_plans())==[]


def test_exclusive_real_entry_preserves_ready_source_and_records_honest_lineage(tmp_path,monkeypatch):
    app,_,_,_=fixture(tmp_path);app.settings.root=tmp_path
    source=(app.settings.workflow_output_dir/'runs/source.json').read_bytes()
    import liangjian_funnel.runtime.close_regeneration as module
    class Clock(datetime):
        @classmethod
        def now(cls,*_):return NOW
    monkeypatch.setattr(module,'datetime',Clock)
    monkeypatch.setattr(module.subprocess,'check_output',lambda *a,**k:HEAD)
    calls=[];app.run_research=lambda *a,**kw:calls.append((a,kw)) or {'status':'READY'}
    assert run_close_regeneration(app,'source',recovery_attempt=2)=={'status':'READY'}
    record=json.loads(next((app.settings.workflow_output_dir/'runs').glob('*.binding.json')).read_text())
    assert record['source_kind']=='NATURAL_ZERO_PLAN_SOURCE_REGEN' and record['recovery_authority']=='0603'
    assert record['recovery_terminal_sha256'] is None and record['source_a2_evidence']['status']=='VALIDATED'
    assert calls[0][1]['from_active_a1'] and calls[0][1]['regeneration'].source_scope
    assert (app.settings.workflow_output_dir/'runs/source.json').read_bytes()==source
    with pytest.raises(ValueError,match='REGEN_RUN_ALREADY_STARTED'):
        run_close_regeneration(app,'source',recovery_attempt=2)
    assert len(calls)==1
