"""Real maintenance and immutable registry, no provider/model/network calls."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timedelta
from types import SimpleNamespace
import json

import pytest

import liangjian_funnel.workflow as workflow
from liangjian_funnel.pipeline.a1_registry import A1Registry,build_a1_manifest
from liangjian_funnel.pipeline.research import ResearchRunResult,LaneResult,StageAudit
from liangjian_funnel.settings import Settings

AT=datetime.fromisoformat('2026-10-22T15:00:00+08:00')
SYMS=['000001.SZ','600000.SH','600519.SH']


def app_fixture(tmp_path,monkeypatch,*,removed=(),global_changed=False,changed=False):
    app=object.__new__(workflow.WorkflowApplication)
    app.settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    app.a1_registry=A1Registry(tmp_path/'a1.db')
    app.trading_calendar=SimpleNamespace(is_trading_day=lambda d:d.weekday()<5,
        previous_trading_day=lambda d:d-timedelta(days=1))
    base_data=dict(g0_symbols=SYMS,g0_candidates=[dict(symbol=s,research_eligible=True) for s in SYMS],
        COMPANY_FUNDAMENTALS={s:dict(value=1) for s in SYMS},MACRO_POLICY_FEED={'policy':'original'},A1_POOL_TARGETS={})
    output=dict(active_research_pool=[dict(symbol=SYMS[0],supporting_refs=['ORIGINAL:1'])],
        monitor_pool=[dict(symbol=SYMS[1],supporting_refs=['ORIGINAL:2'])],
        rejected_candidates=[dict(symbol=SYMS[2],reason_codes=['ORIGINAL_REJECT'])])
    manifest=build_a1_manifest(base_data,{'lane_1':output},mode='FULL',snapshot_id='original',snapshot_hash='a'*64,as_of=AT-timedelta(days=1))
    manifest['last_full_period']='2026-10'
    payload=dict(schema_version='liangjian-a1-registry/1.0.0',mode='FULL',snapshot_id='original',snapshot_hash='a'*64,
        lanes={'lane_1':dict(lane='lane_1',model='original-model',status='VALIDATED',prompt_hash='old-prompt',
            output_hash=workflow._hash_json(output),input_hash='old-input',output=output)})
    staging=app.a1_registry.create_generation(mode='FULL',snapshot_id='original',snapshot_hash='a'*64,
        as_of=AT-timedelta(days=1),manifest=manifest,payload=payload)
    app.a1_registry.seal_generation(staging.generation_id,manifest=manifest,payload=payload)
    original=app.a1_registry.activate_generation(staging.generation_id,expected_current_id=None)
    data=deepcopy(base_data);data['g0_symbols']=[s for s in SYMS if s not in removed]
    data['g0_candidates']=[r for r in data['g0_candidates'] if r['symbol'] not in removed]
    data['snapshot_manifest']=dict(as_of=AT.isoformat(),full_universe_count=3,
        research_universe_count=len(data['g0_symbols']),trade_universe_count=len(data['g0_symbols']))
    if global_changed:data['MACRO_POLICY_FEED']['policy']='new-policy'
    if changed:data['COMPANY_FUNDAMENTALS'][SYMS[0]]['value']=2
    app.settings.snapshot_dir.mkdir(parents=True,exist_ok=True)
    sid='snapshot-daily-20261022'
    path=app.settings.snapshot_dir/(sid+'.json')
    path.write_text(json.dumps(dict(snapshot_id=sid,snapshot_hash=workflow._hash_json(data),as_of=AT.isoformat(),data=data)),encoding='utf-8')
    prepared=app._load_research_snapshot_by_id(sid,expected_date=AT.date().isoformat())
    app.prepare_snapshot=lambda **kw:prepared
    app._new_research_pipeline=lambda **kw:(_ for _ in ()).throw(AssertionError('unexpected model pipeline'))
    app.prompts=app.model_client=app.store=app.research_checkpoints=app._stage_snapshot_enricher=None
    monkeypatch.setattr(workflow,'evaluate_resources',lambda *a:SimpleNamespace(allowed=True,snapshot=SimpleNamespace(as_dict=lambda:{})))
    monkeypatch.setattr(workflow,'measure_resources',lambda *a:SimpleNamespace(as_dict=lambda:{}))
    monkeypatch.setattr(workflow,'_progress_stdout',lambda *a:None)
    return app,original,prepared,data


def test_daily_unchanged_seals_new_verified_generation_without_a_model_or_rewriting_old(tmp_path,monkeypatch):
    app,original,prepared,_=app_fixture(tmp_path,monkeypatch)
    old_bytes=(original.manifest_hash,original.payload_hash,deepcopy(original.payload),original.as_of)
    result=app.run_a1_maintenance(now=AT,daily_refresh=True)
    assert result['status']=='PUBLISHED' and result['research_run_id'] is None,result
    assert result['delta']['processed_count']==0 and result['delta']['model_calls']==0
    assert result['delta']['revalidation_status']=='DAILY_REVALIDATED_UNCHANGED'
    active=app.a1_registry.get_active_generation()
    assert active.generation_id!=original.generation_id and active.base_generation_id==original.generation_id
    assert active.as_of==AT and active.snapshot_hash==prepared.snapshot.snapshot_hash
    assert active.payload['lanes']==original.payload['lanes']
    proof=active.manifest['delta']['daily_refresh_proof']
    assert proof['base_payload_hash']==original.payload_hash and proof['base_manifest_hash']==original.manifest_hash
    assert len(proof['snapshot_file_sha256'])==len(proof['manifest_sha256'])==64
    archived=app.a1_registry.get_generation(original.generation_id)
    assert (archived.manifest_hash,archived.payload_hash,archived.payload,archived.as_of)==old_bytes


@pytest.mark.parametrize('daily',[True,False])
def test_removals_only_publish_exact_merged_partition_without_a_model(tmp_path,monkeypatch,daily):
    app,original,_,_=app_fixture(tmp_path,monkeypatch,removed=[SYMS[1]])
    result=app.run_a1_maintenance(now=AT,mode='INCREMENTAL',daily_refresh=daily)
    assert result['status']=='PUBLISHED' and result['delta']['processed_count']==0,result
    active=app.a1_registry.get_active_generation()
    output=active.payload['lanes']['lane_1']['output']
    assert output['monitor_pool']==[] and output['active_research_pool']==original.payload['lanes']['lane_1']['output']['active_research_pool']
    assert active.manifest['g0_symbols']==[SYMS[0],SYMS[2]]
    assert original.payload['lanes']['lane_1']['output']['monitor_pool']


def test_ordinary_incremental_unchanged_remains_noop(tmp_path,monkeypatch):
    app,original,_,_=app_fixture(tmp_path,monkeypatch)
    result=app.run_a1_maintenance(now=AT,mode='INCREMENTAL')
    assert result['status']=='NOOP'
    assert app.a1_registry.get_active_generation().generation_id==original.generation_id


@pytest.mark.parametrize('mutation',['subset','bad_hash','stale_manifest','future_manifest','unclosed','empty','missing_candidates','naive'])
def test_daily_refresh_rejects_unproven_full_original_without_changing_pointer(tmp_path,monkeypatch,mutation):
    app,original,prepared,data=app_fixture(tmp_path,monkeypatch)
    if mutation=='subset':prepared=replace(prepared,selected_count=1)
    elif mutation=='bad_hash':prepared=replace(prepared,snapshot=replace(prepared.snapshot,snapshot_hash='0'*64))
    else:
        if mutation=='empty':data['g0_symbols']=[]
        elif mutation=='missing_candidates':data['g0_candidates']=[]
        elif mutation=='naive':data['snapshot_manifest']['as_of']=AT.replace(tzinfo=None).isoformat()
        else:data['snapshot_manifest']['as_of']={'stale_manifest':(AT-timedelta(days=1)).isoformat(),
            'future_manifest':(AT+timedelta(days=1)).isoformat(),'unclosed':AT.replace(hour=14).isoformat()}[mutation]
        prepared=replace(prepared,snapshot=replace(prepared.snapshot,data=data,snapshot_hash=workflow._hash_json(data)))
    app.prepare_snapshot=lambda **kw:prepared
    result=app.run_a1_maintenance(now=AT,daily_refresh=True)
    assert result['status']=='FAILED' and app.a1_registry.get_active_generation().generation_id==original.generation_id


def test_global_change_model_scope_is_only_old_active_monitor_inside_new_g0(tmp_path,monkeypatch):
    app,_,_,_=app_fixture(tmp_path,monkeypatch,global_changed=True)
    seen=[]
    def run(snapshot,**kw):
        seen.append(kw)
        output=dict(active_research_pool=[{'symbol':SYMS[0]}],monitor_pool=[{'symbol':SYMS[1]}],rejected_candidates=[])
        audit=StageAudit(lane='lane_1',model='controlled',stage='A1',status='VALIDATED',snapshot_id=snapshot.snapshot_id,
            prompt_hash=None,input_hash=None,output_hash=None,latency_ms=0,attempts=0,thinking_variant=None,
            symbols=tuple(kw['scope_symbols']),reason_codes=(),output=output)
        return ResearchRunResult(run_id=kw['run_id'],generated_at=AT,snapshot_id=snapshot.snapshot_id,
            snapshot_hash=snapshot.snapshot_hash,status='READY',lanes=(LaneResult('lane_1','controlled','READY',(audit,),None),),
            audit_paths=(),markdown_path=None)
    app._new_research_pipeline=lambda **kw:SimpleNamespace(run_a1_only=run)
    result=app.run_a1_maintenance(now=AT,daily_refresh=True)
    assert result['status']=='PUBLISHED' and seen[0]['scope_symbols']==tuple(SYMS[:2]),result
    assert result['delta']['macro_revalidation_count']==2 and result['delta']['processed_count']==2
    assert app.a1_registry.get_active_generation().payload['lanes']['lane_1']['output']['rejected_candidates']==[{'symbol':SYMS[2],'reason_codes':['ORIGINAL_REJECT']}]


def test_daily_generation_pointer_conflict_never_replaces_newer_publication(tmp_path,monkeypatch):
    app,original,_,_=app_fixture(tmp_path,monkeypatch)
    activate=app.a1_registry.activate_generation
    competing=[None]
    def race(gid,**kwargs):
        active=app.a1_registry.get_generation(gid)
        other=app.a1_registry.create_generation(mode='INCREMENTAL',snapshot_id=active.snapshot_id,
            snapshot_hash=active.snapshot_hash,as_of=AT,base_generation_id=original.generation_id,
            manifest=active.manifest,payload={**active.payload,'generation_id':'competing'},generation_id='competing')
        app.a1_registry.seal_generation(other.generation_id,manifest=other.manifest,payload=other.payload)
        competing[0]=activate(other.generation_id,expected_current_id=original.generation_id)
        return activate(gid,**kwargs)
    monkeypatch.setattr(app.a1_registry,'activate_generation',race)
    result=app.run_a1_maintenance(now=AT,daily_refresh=True)
    assert result['status']=='FAILED' and result['reason_code']=='A1_ACTIVE_POINTER_CONFLICT'
    assert app.a1_registry.get_active_generation().generation_id==competing[0].generation_id


def test_daily_refresh_flag_is_explicitly_forwarded_by_cli(tmp_path,monkeypatch,capsys):
    from liangjian_funnel import cli
    from test_cli_workflow_coverage import _FakeApplication
    monkeypatch.setattr(cli,'WorkflowApplication',_FakeApplication)
    _FakeApplication.response={'status':'PUBLISHED'}
    settings=Settings.from_env({},root=tmp_path)
    assert cli.main(['run-a1-maintenance','--daily-refresh','--mode','incremental'],settings=settings)==0
    assert _FakeApplication.calls[0][2]['daily_refresh'] is True
    assert _FakeApplication.calls[0][2]['mode']=='incremental'
