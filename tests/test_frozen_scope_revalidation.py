"""Explicit exact-seed semantic revalidation is not ordinary delta maintenance."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import pytest

import liangjian_funnel.runtime.research_update as update
from liangjian_funnel.pipeline.research import StageAudit, LaneResult, ResearchRunResult
from liangjian_funnel.runtime.publication_receipt import object_hash
from test_research_update_closed_session import fixture, NOW


def write(path, body):
    path.write_text(json.dumps(body), encoding='utf-8')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def receipt(tmp_path, app, old, prepared, kwargs):
    authority = tmp_path/'actual-authority.json'
    authority_sha = write(authority, {'schema': 'CONTROLLED_OFFLINE_AUTHORITY_FIXTURE'})
    package = Path(update.__file__).resolve().parent.parent
    names = ('data/hot100_observation.py', 'pipeline/deterministic.py',
        'pipeline/research/common.py', 'pipeline/a1_registry.py')
    body = dict(schema='research-frozen-scope-revalidation/1', purpose='A1',
        operation='HOT100_CLOSED_SESSION_CLOCK_CORRECTION', run_id=kwargs['run_id'],
        source_trade_date='2026-10-22', target_trade_date='2026-10-23',
        research_only=True, execution_eligible=False, task_deadline='2026-10-23T15:00:00+08:00',
        seed={k: kwargs[k] for k in ('snapshot_id','snapshot_hash','snapshot_file_sha256')},
        expected_generation=dict(generation_id=old.generation_id,manifest_hash=old.manifest_hash,payload_hash=old.payload_hash),
        scope_count=len(prepared.snapshot.data['g0_symbols']),
        scope_sha256=object_hash(sorted(prepared.snapshot.data['g0_symbols'])),
        authority_original=dict(path=str(authority),sha256=authority_sha),
        implementation_sources={name: hashlib.sha256((package/name).read_bytes()).hexdigest() for name in names})
    path=tmp_path/'scope-revalidation.json'
    extra=dict(force_revalidate_frozen_scope=True,revalidation_receipt=str(path),
        revalidation_receipt_sha256=write(path,body))
    return path,body,extra


def test_unchanged_exact_seed_runs_real_pipeline_full_scope_and_preserves_identity_delta(tmp_path, monkeypatch):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    path,body,extra=receipt(tmp_path,app,old,prepared,kwargs)
    seen=[]; original=deepcopy(old.payload); raw=prepared.path.read_bytes()
    def run(snapshot, **kw):
        seen.append(kw)
        output=deepcopy(old.payload['lanes']['lane_1']['output'])
        output['active_research_pool'][0]['supporting_refs']=['CONTROLLED_NEW_RESULT']
        audit=StageAudit(lane='lane_1',model='controlled',stage='A1',status='VALIDATED',snapshot_id=snapshot.snapshot_id,
            prompt_hash=None,input_hash=None,output_hash=None,latency_ms=0,attempts=0,thinking_variant=None,
            symbols=tuple(kw['scope_symbols']),reason_codes=(),output=output)
        return ResearchRunResult(run_id=kw['run_id'],generated_at=NOW,snapshot_id=snapshot.snapshot_id,
            snapshot_hash=snapshot.snapshot_hash,status='READY',lanes=(LaneResult('lane_1','controlled','READY',(audit,),None),),
            audit_paths=(),markdown_path=None)
    app._new_research_pipeline=lambda **kw:SimpleNamespace(run_a1_only=run)
    result=app.run_a1_research_update(**kwargs,**extra)
    assert result['status']=='PUBLISHED', result
    assert len(seen)==1 and seen[0]['scope_symbols']==tuple(sorted(prepared.snapshot.data['g0_symbols']))
    assert result['delta']['changed_count']==0 and result['delta']['processed_count']==3
    assert result['delta']['frozen_scope_revalidation']['scope_sha256']==body['scope_sha256']
    assert 'revalidation_status' not in result['delta']
    active=app.a1_registry.get_active_generation()
    assert active.base_generation_id==old.generation_id and active.generation_id!=old.generation_id
    assert active.payload['lanes']['lane_1']['output']['active_research_pool'][0]['supporting_refs']==['CONTROLLED_NEW_RESULT']
    assert app.a1_registry.get_generation(old.generation_id).payload==original and prepared.path.read_bytes()==raw
    assert result['publication']=='RESEARCH_ONLY_NOT_EXECUTION' and not app.settings.state_db_path.exists()


@pytest.mark.parametrize('case',['scope','seed','base','source','target','run','algorithm','deadline','expired','authority','operation','publication'])
def test_receipt_conflicts_fail_before_claim_and_pointer_changes(tmp_path,monkeypatch,case):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    path,body,extra=receipt(tmp_path,app,old,prepared,kwargs)
    if case=='scope': body['scope_sha256']='0'*64
    elif case=='seed': body['seed']['snapshot_hash']='0'*64
    elif case=='base': body['expected_generation']['generation_id']='pick-old-generation'
    elif case=='source': body['source_trade_date']='2026-10-21'
    elif case=='target': body['target_trade_date']='2026-10-26'
    elif case=='run': body['run_id']='some-other-batch'
    elif case=='algorithm': body['implementation_sources']['data/hot100_observation.py']='0'*64
    elif case=='deadline': body['task_deadline']='2026-10-23T15:01:00+08:00'
    elif case=='expired': body['task_deadline']=NOW.isoformat()
    elif case=='authority': body['authority_original']['sha256']='0'*64
    elif case=='operation': body['operation']='UNBOUNDED_FORCE'
    elif case=='publication': body['execution_eligible']=True
    extra['revalidation_receipt_sha256']=write(path,body)
    with pytest.raises(ValueError): app.run_a1_research_update(**kwargs,**extra)
    assert app.a1_registry.get_active_generation().generation_id==old.generation_id
    assert not list(app.settings.workflow_output_dir.rglob('*.claim.json'))


@pytest.mark.parametrize('case',['missing_receipt','unused_receipt','wrong_type','a2','changed_receipt'])
def test_force_intent_requires_exact_receipt_only_on_a1(tmp_path,monkeypatch,case):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    path,body,extra=receipt(tmp_path,app,old,prepared,kwargs)
    operation=app.run_a1_research_update
    if case=='missing_receipt':extra.pop('revalidation_receipt')
    elif case=='unused_receipt':extra['force_revalidate_frozen_scope']=False
    elif case=='wrong_type':extra['force_revalidate_frozen_scope']='true'
    elif case=='a2':operation=app.run_research_update
    elif case=='changed_receipt':path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError):operation(**kwargs,**extra)
    assert app.a1_registry.get_active_generation().generation_id==old.generation_id


def test_bound_receipt_is_rechecked_before_reusing_loaded_seed(tmp_path,monkeypatch):
    app,old,prepared,kwargs=fixture(tmp_path,monkeypatch)
    path,body,extra=receipt(tmp_path,app,old,prepared,kwargs)
    binding=update.bind_research_update(app,purpose='A1',now=NOW,**kwargs,**extra)
    path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='RECEIPT_CHANGED'):
        update.load_bound_seed(app,binding,now=NOW,prepared=prepared)


def test_cli_passes_exact_revalidation_fields_only_to_research_a1(tmp_path,monkeypatch):
    from liangjian_funnel import cli
    from liangjian_funnel.settings import Settings
    from test_cli_workflow_coverage import _FakeApplication
    class Fake(_FakeApplication):
        def run_a1_research_update(self,**kw): return self._invoke('a1_update',**kw)
    Fake.response={'status':'PUBLISHED'}
    monkeypatch.setattr(cli,'WorkflowApplication',Fake)
    fields=dict(snapshot_id='snapshot-daily-20261022',snapshot_hash='a'*64,snapshot_file_sha256='b'*64,
        source_trade_date='2026-10-22',expected_a1_generation_id='CURRENT_ID',run_id='NEW_BATCH',
        revalidation_receipt=str(tmp_path/'receipt.json'),revalidation_receipt_sha256='c'*64)
    argv=['run-a1-research-update','--force-revalidate-frozen-scope']
    for key,value in fields.items():argv.extend(['--'+key.replace('_','-'),value])
    assert cli.main(argv,settings=Settings.from_env({},root=tmp_path))==0
    assert Fake.calls[0][2]==dict(fields,force_revalidate_frozen_scope=True)
    with pytest.raises(SystemExit): cli.main(['run-research-update',*argv[1:]],settings=Settings.from_env({},root=tmp_path))
