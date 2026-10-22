"""Post-window research changes neither the old execution source nor clocks."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
import yaml

import liangjian_funnel.workflow as workflow
from liangjian_funnel.runtime.calendar import (closed_research_session,
    latest_closed_research_session, ExchangeTradingCalendar)
from liangjian_funnel.runtime.closed_session_capture import (
    validate_capture_proof, validate_research_capture_proof, KEY)
from liangjian_funnel.runtime.research_update import (
    bind_research_update, load_bound_seed, require_active_generation)
from liangjian_funnel.pipeline.team_v2_a2_score import POLICY
from liangjian_funnel.pipeline.research import LaneResult, ResearchRunResult
from test_a1_dual_clock_0635 import seed_fixture, clock, at, proof

NOW = at('2026-10-23T07:10:00')
CAL = ExchangeTradingCalendar()


def fixture(tmp_path, monkeypatch):
    app, old, prepared, _ = seed_fixture(tmp_path, monkeypatch, '2026-10-23T03:13:00')
    clock(monkeypatch, NOW)
    kwargs = dict(snapshot_id=prepared.snapshot.snapshot_id,
        snapshot_hash=prepared.snapshot.snapshot_hash,
        snapshot_file_sha256=hashlib.sha256(prepared.path.read_bytes()).hexdigest(),
        source_trade_date='2026-10-22', expected_a1_generation_id=old.generation_id,
        run_id='research-update-a1')
    app.prepare_snapshot = lambda **_: (_ for _ in ()).throw(AssertionError('NO_RECOLLECTION'))
    return app, old, prepared, kwargs


@pytest.mark.parametrize('stamp,source,target', [
    ('2026-10-23T06:30:00','2026-10-22','2026-10-23'),
    ('2026-10-23T14:59:59','2026-10-22','2026-10-23'),
    ('2026-10-24T12:00:00','2026-10-23','2026-10-26'),
    ('2026-10-02T12:00:00','2026-09-30','2026-10-08'),
])
def test_source_derivation_has_no_execution_permission(stamp,source,target):
    session=latest_closed_research_session(at(stamp),CAL)
    assert session.source_trade_date.isoformat()==source
    assert session.target_trade_date.isoformat()==target
    if session.wall_clock>=session.publication_deadline:
        with pytest.raises(ValueError,match='PUBLICATION_DEADLINE'):
            closed_research_session(at(stamp),CAL)


def test_research_proof_preserves_original_seed_and_ordinary_deadline():
    original=proof('2026-10-23T03:13:00');frozen=deepcopy(original)
    assert validate_research_capture_proof(original,captured_at=at('2026-10-23T03:13:00'),
        calendar=CAL,now=NOW,source_trade_date='2026-10-22')==original
    assert original==frozen
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE'):
        validate_capture_proof(original,captured_at=at('2026-10-23T03:13:00'),calendar=CAL,now=NOW)
    with pytest.raises(ValueError):
        validate_research_capture_proof(original,captured_at=at('2026-10-23T03:13:00'),
            calendar=CAL,now=at('2026-10-23T15:00:00'),source_trade_date='2026-10-22')


def test_a1_real_registry_reuses_seed_after_window_and_keeps_old_bytes(tmp_path,monkeypatch):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    old_identity=(old.manifest_hash,old.payload_hash,deepcopy(old.payload))
    result=app.run_a1_research_update(**kwargs)
    assert result['status']=='PUBLISHED',result
    assert result['publication']=='RESEARCH_ONLY_NOT_EXECUTION'
    assert result['processing_started_at']==NOW.isoformat()
    assert result['seed_capture_as_of']==prepared.snapshot.as_of.isoformat()
    active=app.a1_registry.get_active_generation()
    assert active.generation_id!=old.generation_id and active.as_of==prepared.snapshot.as_of
    assert active.payload['lanes']==old.payload['lanes']
    original=app.a1_registry.get_generation(old.generation_id)
    assert (original.manifest_hash,original.payload_hash,original.payload)==old_identity
    assert not app.settings.workflow_progress_path.exists()
    assert not list(app.settings.research_checkpoint_dir.rglob('*close.json'))
    assert not app.settings.state_db_path.exists()
    assert list((app.settings.workflow_output_dir/'research_updates').glob('*.claim.json'))
    with pytest.raises(ValueError,match='RUN_ID_ALREADY_USED'):app.run_a1_research_update(**{**kwargs,
        'expected_a1_generation_id':active.generation_id})


@pytest.mark.parametrize('field,value',[
    ('snapshot_hash','f'*64),('snapshot_file_sha256','0'*64),
    ('source_trade_date','2026-10-21'),('expected_a1_generation_id','other'),
    ('snapshot_id','../escape'),('run_id','../bad'),
])
def test_bad_identity_fails_before_models_or_pointer_changes(tmp_path,monkeypatch,field,value):
    app,old,_,kwargs=fixture(tmp_path,monkeypatch)
    with pytest.raises(ValueError):app.run_a1_research_update(**{**kwargs,field:value})
    assert app.a1_registry.get_active_generation().generation_id==old.generation_id
    assert not list(app.settings.workflow_output_dir.rglob('*.claim.json'))


@pytest.mark.parametrize('mutation',['future','bad_market','bad_proof','bad_manifest','duplicate_g0'])
def test_semantically_bad_frozen_seed_cannot_be_rebound(tmp_path,monkeypatch,mutation):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    body=json.loads(prepared.path.read_bytes());data=body['data']
    if mutation=='future':
        p=proof('2026-10-23T05:00:00');p['capture_completed_at']=at('2026-10-23T08:00:00').isoformat()
        data[KEY]=p
    elif mutation=='bad_market':data['MARKET_DATA_AS_OF']=at('2026-10-21T15:00:00').isoformat()
    elif mutation=='bad_proof':data[KEY]['authority']='FAKE'
    elif mutation=='bad_manifest':data['snapshot_manifest']['as_of']=NOW.isoformat()
    else:data['g0_symbols'].append(data['g0_symbols'][0])
    body['snapshot_hash']=workflow._hash_json(data);prepared.path.write_text(json.dumps(body),encoding='utf-8')
    kwargs.update(snapshot_hash=body['snapshot_hash'],snapshot_file_sha256=hashlib.sha256(prepared.path.read_bytes()).hexdigest())
    with pytest.raises(ValueError):app.run_a1_research_update(**kwargs)
    assert app.a1_registry.get_active_generation().generation_id==old.generation_id


def research_fixture(tmp_path,monkeypatch):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    first=app.run_a1_research_update(**kwargs);assert first['status']=='PUBLISHED',first
    active=app.a1_registry.get_active_generation()
    kwargs.update(expected_a1_generation_id=active.generation_id,run_id='research-update-a2a3')
    root=Path(__file__).resolve().parents[1]
    app.settings=app.settings.model_copy(update={'prompt_dir':root/'prompts','close_resource_sampling_enabled':False})
    app.settings.source_config_path.parent.mkdir(parents=True,exist_ok=True)
    app.settings.source_config_path.write_text(yaml.safe_dump({'agent_1':{'team_role_mode':'production'},
        'agent_2':{'team_theme_score_policy':POLICY}}),encoding='utf-8')
    del app._new_research_pipeline
    app._ensure_trading_day=lambda *a,**k:(_ for _ in ()).throw(AssertionError('NO_ACCOUNT_SYNC'))
    app._publish_plans=lambda *a,**k:(_ for _ in ()).throw(AssertionError('NO_EXECUTION_PUBLICATION'))
    app._write_research_resume_marker=lambda *a,**k:(_ for _ in ()).throw(AssertionError('NO_NATURAL_MARKER'))
    app._stage_snapshot_enricher=lambda **_:None
    monkeypatch.setattr(workflow,'_write_broker_gold_benchmark',lambda *a,**k:None)
    monkeypatch.setattr(workflow,'write_stage_markdown_reports',lambda *a,**k:())
    seen=[]
    class Pipeline:
        def __init__(self,settings,**kw):
            seen.append(kw)
            assert kw['team_research_adapter'].supports('A2') and kw['team_research_adapter'].supports('A3')
            assert not kw['team_research_adapter'].supports('A1')
            assert kw['runtime_store'] is None and kw['stage_snapshot_enricher'] is app._stage_snapshot_enricher
        def run_from_active_a1(self,snapshot,generation,**kw):
            seen.append((snapshot,generation,kw))
            lane=LaneResult(lane='lane_1',model=app.settings.research_models[0],status='READY',stages=(),
                final_output={'core_watch_pool':[]})
            return ResearchRunResult(run_id=kw['run_id'],generated_at=NOW,snapshot_id=snapshot.snapshot_id,
                snapshot_hash=snapshot.snapshot_hash,status='READY',lanes=(lane,),audit_paths=(),markdown_path=None,
                primary_lane_ids=('lane_1',))
    monkeypatch.setattr(workflow,'ResearchPipeline',Pipeline)
    return app,prepared,kwargs,seen


def test_actual_central_research_route_keeps_enricher_no_publication_or_account(tmp_path,monkeypatch):
    app,prepared,kwargs,seen=research_fixture(tmp_path,monkeypatch)
    old_progress=app.settings.workflow_output_dir/'research_updates'/'research-update-a1.progress.json'
    old_bytes=old_progress.read_bytes()
    result=app.run_research_update(**kwargs)
    assert result['research_only'] is True and result['execution_eligible'] is False
    assert result['plan_publication']==dict(atomic=True,created=[],activated=[],blocked=[],publication='RESEARCH_ONLY_NOT_EXECUTION')
    assert result['market_trade_date']=='2026-10-22' and result['target_trade_date']=='2026-10-23'
    assert result['source_as_of']==NOW.isoformat() and result['seed_capture_as_of']==prepared.snapshot.as_of.isoformat()
    assert result['snapshot']['snapshot_hash']==prepared.snapshot.snapshot_hash
    assert len(seen)==2 and seen[1][0].as_of==prepared.snapshot.as_of
    assert old_progress.read_bytes()==old_bytes and not app.settings.workflow_progress_path.exists()
    assert not app.settings.state_db_path.exists()


def test_research_checks_same_source_and_a1_pointer_after_model_work(tmp_path,monkeypatch):
    app,_,kwargs,seen=research_fixture(tmp_path,monkeypatch)
    original=workflow.ResearchPipeline.run_from_active_a1
    def run(*args,**kw):
        result=original(*args,**kw)
        clock(monkeypatch,at('2026-10-23T15:00:00'))
        return result
    monkeypatch.setattr(workflow.ResearchPipeline,'run_from_active_a1',run)
    with pytest.raises(ValueError,match='SOURCE_SESSION_CHANGED'):app.run_research_update(**kwargs)
    assert not (app.settings.workflow_output_dir/'runs'/f"{kwargs['run_id']}.json").exists()
    assert not app.settings.state_db_path.exists()


@pytest.mark.parametrize('field,value',[('publish_plans',True),('record_runtime',True),
    ('from_active_a1',False),('primary_only',False),('reuse_resume_snapshot',True),
    ('schedule_comparison',True),('regeneration',NS()),('market_data_as_of',NOW)])
def test_typed_internal_branch_cannot_publish_or_change_normal_flags(tmp_path,monkeypatch,field,value):
    app,_,kwargs,_=research_fixture(tmp_path,monkeypatch)
    binding=bind_research_update(app,purpose='A2_A3',now=NOW,**kwargs)
    options=dict(primary_only=True,from_active_a1=True,active_a1_generation_id=binding.generation_id,
        publish_plans=False,record_runtime=False,reuse_resume_snapshot=False,run_id_override=binding.run_id)
    options[field]=value
    with pytest.raises(workflow.WorkflowError,match='ARGUMENTS_INVALID'):
        app.run_research('close',_research_update=binding,**options)


def test_old_plain_close_and_regeneration_keep_publication_deadline(tmp_path,monkeypatch):
    app,_,_,_=fixture(tmp_path,monkeypatch)
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE'):app.run_research('close')
    from liangjian_funnel.runtime.close_regeneration import require_regeneration_publication_window
    with pytest.raises(ValueError,match='PUBLICATION_DEADLINE'):
        require_regeneration_publication_window(NS(),CAL,now=NOW)


def test_pointer_and_seed_are_rechecked_with_no_identity_relaxation(tmp_path,monkeypatch):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    binding=bind_research_update(app,purpose='A1',now=NOW,**kwargs)
    prepared.path.write_bytes(prepared.path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='SEED_FILE_CHANGED'):load_bound_seed(app,binding,now=NOW)
    monkeypatch.setattr(app.a1_registry,'get_active_generation',lambda:replace(old,payload_hash='f'*64))
    with pytest.raises(ValueError,match='A1_POINTER_CHANGED'):require_active_generation(app,binding)


@pytest.mark.parametrize('command',['run-a1-research-update','run-research-update'])
def test_cli_dispatches_exact_identity_not_as_of_or_execution_flags(tmp_path,monkeypatch,command):
    from liangjian_funnel import cli
    from test_cli_workflow_coverage import _FakeApplication, _ready_response
    from liangjian_funnel.settings import Settings
    class Fake(_FakeApplication):
        def run_a1_research_update(self,**kw):return self._invoke('a1_update',**kw)
        def run_research_update(self,**kw):return self._invoke('research_update',**kw)
    Fake.response={'status':'PUBLISHED'} if command=='run-a1-research-update' else _ready_response()
    monkeypatch.setattr(cli,'WorkflowApplication',Fake)
    fields=dict(snapshot_id='snapshot-daily-20261022',snapshot_hash='a'*64,snapshot_file_sha256='b'*64,
        source_trade_date='2026-10-22',expected_a1_generation_id='A1_ORIGINAL',run_id='NEW_IDENTITY')
    argv=[command]
    for key,value in fields.items():argv.extend(['--'+key.replace('_','-'),value])
    assert cli.main(argv,settings=Settings.from_env({},root=tmp_path))==0
    assert Fake.calls[0][2]==fields


def news_child(app,prepared,monkeypatch):
    from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot
    calls=[]
    app._collect_open_news=lambda symbols,**kw:calls.append((symbols,kw)) or {}
    parent=FrozenInputSnapshot(snapshot_id=prepared.snapshot.snapshot_id,
        snapshot_hash=prepared.snapshot.snapshot_hash,as_of=prepared.snapshot.as_of,data=prepared.snapshot.data)
    # Exercise the unchanged real stage enricher, not a handcrafted child clock.
    child=workflow.WorkflowApplication._stage_snapshot_enricher(app,stage='A2',lane_id='lane_1',
        model='SYNTHETIC_OFFLINE_ONLY',upstream_symbols=frozenset({'000001.SZ'}),snapshot=parent)
    assert calls==[(['000001.SZ'],{'include_market':False})]
    return FrozenInputSnapshot(snapshot_id=child['snapshot_id'],snapshot_hash=child['snapshot_hash'],
        as_of=child['as_of'],data=child['data'],proof_chain=child['proof_chain'])


def test_real_news_enrichment_keeps_capture_market_and_child_clocks_separate(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.research_update import research_time_context, _CLOCK_SOURCE
    from liangjian_funnel.pipeline.team_v2_research_time import research_times
    from liangjian_funnel.pipeline.research.common import _with_authorized_rotation_policy, _capture_snapshot_recipe
    app,prepared,kwargs,_=research_fixture(tmp_path,monkeypatch)
    binding=bind_research_update(app,purpose='A2_A3',now=NOW,**kwargs)
    child=news_child(app,prepared,monkeypatch)
    assert child.as_of==NOW and child.data[KEY]['capture_as_of']==prepared.snapshot.as_of.isoformat()
    with research_time_context(app,binding,prepared,clock=lambda:NOW):
        captured,market=research_times(child,CAL)
        assert captured==NOW and market==at('2026-10-22T15:00:00')
        policy=_with_authorized_rotation_policy(child)
        assert research_times(policy,CAL)==(NOW,market)
        from liangjian_funnel.runtime.publication_receipt import object_hash
        overlay={'FACTOR_SNAPSHOT':{'000001.SZ':{'available':False}}}
        recipe=dict(base_snapshot_hash=policy.snapshot_hash,stage='A3',overlay=overlay)
        projected=replace(policy,snapshot_id=policy.snapshot_id+':a3:'+object_hash(recipe)[:12],
            snapshot_hash=object_hash(recipe),data={**policy.data,**overlay})
        projected=_capture_snapshot_recipe(policy,projected,recipe_kind='STAGE_OVERLAY',hash_recipe=recipe,
            data_recipe={'operation':'SHALLOW_OVERLAY','overlay':overlay})
        assert research_times(projected,CAL)==(NOW,market)
    assert _CLOCK_SOURCE.get() is None
    # Outside this exact request context the original no-unproven-clock guard remains.
    with pytest.raises(ValueError):research_times(child,CAL)


@pytest.mark.parametrize('mutation',['parent_id','parent_hash','parent_asof','child_id','child_hash',
    'recipe','data_recipe','backdated','future','unknown_transform','terminal_data','market'])
def test_news_proof_chain_tamper_is_not_a_clock_escape(tmp_path,monkeypatch,mutation):
    from liangjian_funnel.runtime.research_update import research_time_context
    from liangjian_funnel.pipeline.team_v2_research_time import research_times
    app,prepared,kwargs,_=research_fixture(tmp_path,monkeypatch)
    binding=bind_research_update(app,purpose='A2_A3',now=NOW,**kwargs)
    child=news_child(app,prepared,monkeypatch)
    chain=deepcopy(child.proof_chain);node=chain[0]
    if mutation=='parent_id':node['parent_snapshot_id']='OTHER'
    elif mutation=='parent_hash':node['parent_snapshot_hash']='f'*64
    elif mutation=='parent_asof':node['parent_as_of']=NOW.isoformat()
    elif mutation=='child_id':node['snapshot_id']='FAKE'
    elif mutation=='child_hash':node['snapshot_hash']='0'*64
    elif mutation=='recipe':node['hash_recipe']['stage']='A3'
    elif mutation=='data_recipe':node['data_recipe']['A2_STOCK_NEWS_SCOPE']=['OTHER']
    elif mutation=='backdated':node['as_of']=at('2026-10-23T01:00:00').isoformat()
    elif mutation=='future':node['as_of']=at('2026-10-23T08:00:00').isoformat()
    elif mutation=='unknown_transform':node['recipe_kind']='UNAPPROVED'
    if mutation=='terminal_data':child=replace(child,data={**child.data,'FAKE_FACT':True})
    elif mutation=='market':child=replace(child,data={**child.data,'MARKET_DATA_AS_OF':NOW.isoformat()})
    child=replace(child,proof_chain=chain)
    with research_time_context(app,binding,prepared,clock=lambda:NOW):
        with pytest.raises(ValueError):research_times(child,CAL)


def test_request_clock_context_is_reset_on_actual_pipeline_exception(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.research_update import _CLOCK_SOURCE
    app,_,kwargs,_=research_fixture(tmp_path,monkeypatch)
    def fail(*a,**kw):
        assert _CLOCK_SOURCE.get() is not None
        raise RuntimeError('SYNTHETIC_MODEL_FAILURE_NO_HTTP')
    monkeypatch.setattr(workflow.ResearchPipeline,'run_from_active_a1',fail)
    with pytest.raises(RuntimeError):app.run_research_update(**kwargs)
    assert _CLOCK_SOURCE.get() is None
    assert (app.settings.workflow_output_dir/'research_updates'/f"{kwargs['run_id']}.claim.json").is_file()
    # No output summary exists on failure, but its immutable claim still blocks retry.
    with pytest.raises(FileExistsError):app.run_research_update(**kwargs)


def test_a1_update_uses_original_compare_and_swap_and_retains_failed_claim(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline.a1_registry import A1RegistryError
    app,old,_,kwargs=fixture(tmp_path,monkeypatch)
    seen=[]
    def conflicting(gid,**kw):
        seen.append(kw)
        raise A1RegistryError('A1_ACTIVE_POINTER_CONFLICT')
    monkeypatch.setattr(app.a1_registry,'activate_generation',conflicting)
    result=app.run_a1_research_update(**kwargs)
    assert result['status']=='FAILED' and result['reason_code']=='A1_ACTIVE_POINTER_CONFLICT'
    assert seen[0]['expected_current_id']==old.generation_id
    assert app.a1_registry.get_active_generation().generation_id==old.generation_id
    assert (app.settings.workflow_output_dir/'research_updates'/f"{kwargs['run_id']}.claim.json").exists()
