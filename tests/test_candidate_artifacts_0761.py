from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from fixture_candidate_artifacts_0761 import prepare
from liangjian_funnel.runtime.candidate_artifacts import verify_candidate_artifacts,summary_binding
from liangjian_funnel.runtime.publication_receipt import object_hash
from liangjian_funnel.runtime.reviewed_batch_natural import stage_natural_candidate
from liangjian_funnel.runtime.state import RuntimeStore


NOW=datetime(2026,10,26,16,tzinfo=ZoneInfo('Asia/Shanghai'))


def fixture(tmp_path,*,record_slot='close'):
    store=RuntimeStore(tmp_path/'workflow.db')
    store.record_workflow_run(run_id='natural-close',lane_id='lane_1',trade_date='2026-10-26',slot=record_slot,
        model='CONTROLLED',status='READY',snapshot_hash='a'*64)
    batch=[dict(plan_id='natural-close:lane_1:p',lane_id='lane_1',symbol='300817.SZ',
        status='PENDING_MORNING_REVIEW',valid_from=None,expires_at='2026-10-27T15:00:00+08:00',
        payload=dict(symbol='300817.SZ',source_run_id='natural-close',stop_level=9,trigger_high=10,no_chase=11))]
    return store,batch


def test_only_candidate_table_is_never_a_complete_publication(tmp_path):
    store,batch=fixture(tmp_path)
    with pytest.raises(ValueError,match='ARTIFACT_CONTRACT_REQUIRED'):
        stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW)
    assert store._read(lambda db:db.execute('SELECT count(*) FROM natural_review_candidates').fetchone()[0])==0


@pytest.mark.parametrize('record_slot',['close','CLOSE'])
def test_real_archive_summary_native_readback_then_stage(tmp_path,record_slot):
    store,batch=fixture(tmp_path,record_slot=record_slot); proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    # Production ResearchSlot.CLOSE.value is uppercase; older fixtures and
    # manual workflow records used lowercase for the same close session.
    result=stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=proof)
    assert result['status']=='CANDIDATE_PENDING_REVIEW' and result['execution_plans_written']==0
    assert verify_candidate_artifacts(batch,run_id='natural-close',proof=proof)['daily_facts']=={
        batch[0]['plan_id']:'READY'}
    # Exercise the next actual entry, not only the producer's staging gate.
    from types import SimpleNamespace
    from liangjian_funnel.runtime.reviewed_batch_natural import read_natural_candidate
    settings=SimpleNamespace(state_db_path=store.path,research_primary_lane_id='lane_1',
        workflow_output_dir=Path(proof['workflow_output_dir']))
    loaded=read_natural_candidate(settings,day='2026-10-27')
    assert loaded['state']=='PENDING' and len(loaded['body']['plan_rows'])==1


@pytest.mark.parametrize('record_slot',['open','MIDDAY',''])
def test_non_close_workflow_record_still_cannot_stage(tmp_path,record_slot):
    store,batch=fixture(tmp_path,record_slot=record_slot);proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    with pytest.raises(ValueError,match='CANDIDATE_WORKFLOW_SOURCE_CONFLICT'):
        stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=proof)
    assert store._read(lambda db:db.execute('SELECT count(*) FROM natural_review_candidates').fetchone()[0])==0


@pytest.mark.parametrize('slot',['CLOSE','Close','unknown'])
def test_summary_case_normalization_preserves_original_binding_and_rejects_unknown(tmp_path,slot):
    store,batch=fixture(tmp_path,record_slot='CLOSE');proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    path=Path(proof['workflow_output_dir'])/'runs/natural-close.json'
    summary=json.loads(path.read_bytes());summary['slot']=slot;path.write_text(json.dumps(summary))
    proof['summary_binding_sha256']=object_hash(summary_binding(summary))
    if slot=='unknown':
        with pytest.raises(ValueError,match='CANDIDATE_SUMMARY_BINDING_CONFLICT'):
            verify_candidate_artifacts(batch,run_id='natural-close',proof=proof)
    else:
        assert verify_candidate_artifacts(batch,run_id='natural-close',proof=proof)['source_trade_date']=='2026-10-26'


@pytest.mark.parametrize('change',['wrong_source','unknown_slot'])
def test_morning_reader_still_rejects_conflicting_source_after_staging(tmp_path,change):
    from types import SimpleNamespace
    from liangjian_funnel.runtime.reviewed_batch_natural import read_natural_candidate
    store,batch=fixture(tmp_path,record_slot='CLOSE')
    proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=proof)
    column,value=('trade_date','2026-10-23') if change=='wrong_source' else ('slot','UNKNOWN')
    store._write(lambda db:db.execute('UPDATE workflow_runs SET '+column+'=? WHERE run_id=?',
        (value,'natural-close')).rowcount)
    settings=SimpleNamespace(state_db_path=store.path,research_primary_lane_id='lane_1',
        workflow_output_dir=Path(proof['workflow_output_dir']))
    with pytest.raises(ValueError,match='CANDIDATE_WORKFLOW_SOURCE_CONFLICT'):
        read_natural_candidate(settings,day='2026-10-27')


@pytest.mark.parametrize('change',['summary_missing','wrong_source','manifest_missing','payload_changed','seed_missing'])
def test_missing_or_changed_artifact_refuses_before_candidate_write(tmp_path,change):
    store,batch=fixture(tmp_path); proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    root=Path(proof['workflow_output_dir']); summary=root/'runs/natural-close.json'
    if change=='summary_missing':summary.unlink()
    elif change=='wrong_source':
        value=json.loads(summary.read_bytes());value['market_trade_date']='2026-10-23';summary.write_text(json.dumps(value))
    elif change=='manifest_missing':next((root/'publication_proofs').iterdir()).unlink()
    elif change=='payload_changed':batch[0]['payload']['stop_level']=8
    elif change=='seed_missing':proof['minute_cache_path']=str(root/'missing-minute-cache.sqlite3')
    with pytest.raises(ValueError):
        stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=proof)
    assert store._read(lambda db:db.execute('SELECT count(*) FROM natural_review_candidates').fetchone()[0])==0


def test_legacy_production_close_also_refuses_missing_archive_before_plan_insert(tmp_path):
    from types import SimpleNamespace
    from test_w2_shadow_inputs_publication import publisher_fixture,AT,TARGET
    from fixture_candidate_artifacts_0761 import complete_publisher_inputs
    from dataclasses import replace
    from liangjian_funnel.workflow import WorkflowApplication
    app,result,data,_=publisher_fixture()
    app.store=RuntimeStore(tmp_path/'workflow.sqlite3')
    app.settings=SimpleNamespace(root=tmp_path,workflow_output_dir=tmp_path/'outputs',
        minute_cache_dir=tmp_path/'minute',research_primary_lane_id='lane_1',publish_comparison_lanes=False)
    result,snapshot=complete_publisher_inputs(app,result,data,now=AT,target=TARGET)
    lane=result.lanes[0]
    lane.audit_path.unlink()
    result=replace(result,lanes=(replace(lane,stages=tuple(replace(a,thinking_variant=None) for a in lane.stages)),))
    with pytest.raises(ValueError):
        WorkflowApplication._publish_plans(app,result,'close',AT,snapshot_data=data,
            minimum_trade_date=TARGET,publication_snapshot_path=snapshot)
    assert app.store.list_execution_plans()==()


def test_large_intermediate_lane_audits_are_referenced_not_duplicated_in_discovery_summary(tmp_path):
    """The real pipeline as_dict includes full lanes; final run summary does not.

    Keep the compact discovery contract readable without relaxing any reader's
    byte limit, modifying source objects, or dropping the original audit pins.
    """
    import hashlib
    from liangjian_funnel.runtime.candidate_artifacts import prepare_candidate_artifacts
    store,batch=fixture(tmp_path,record_slot='CLOSE')
    proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    root=Path(proof['workflow_output_dir']);path=root/'runs/natural-close.json'
    old=json.loads(path.read_bytes());path.unlink()
    audit=root/'natural-close-audit.json';original_audit=hashlib.sha256(audit.read_bytes()).hexdigest()
    outcome={'market_regime':'ROTATION_NO_MAINLINE','status':'READY'}
    intermediate=dict(lanes=[dict(stages=[dict(output={'large_original':'x'*10_000_000})])],
        audit_paths=[str(audit)],outcome_v2=outcome,snapshot_id='controlled',snapshot_hash='a'*64)
    prepared=prepare_candidate_artifacts(batch,workflow_output_dir=root,run_id='natural-close',status='READY',
        market_trade_date='2026-10-26',projection_manifests=old['plan_publication']['projection_manifests'],
        pattern_preparation=old['plan_publication']['pattern_preparation'],minute_cache_path=proof['minute_cache_path'],
        observed_at=NOW,research_summary=intermediate)
    saved=json.loads(path.read_bytes())
    assert path.stat().st_size<2_000_000 and 'lanes' not in saved
    assert saved['outcome_v2']==outcome and saved['audit_paths']==[str(audit)]
    assert len(intermediate['lanes'][0]['stages'][0]['output']['large_original'])==10_000_000
    assert hashlib.sha256(audit.read_bytes()).hexdigest()==original_audit
    staged=stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=prepared)
    assert staged['execution_plans_written']==0


def test_oversized_discovery_metadata_still_rejected_by_original_read_bound(tmp_path):
    from liangjian_funnel.runtime.candidate_artifacts import prepare_candidate_artifacts
    store,batch=fixture(tmp_path)
    proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    root=Path(proof['workflow_output_dir']);path=root/'runs/natural-close.json'
    old=json.loads(path.read_bytes());path.unlink()
    with pytest.raises(ValueError,match='CANDIDATE_ARTIFACT_FILE_UNAVAILABLE'):
        prepare_candidate_artifacts(batch,workflow_output_dir=root,run_id='natural-close',status='READY',
            market_trade_date='2026-10-26',projection_manifests=old['plan_publication']['projection_manifests'],
            pattern_preparation=old['plan_publication']['pattern_preparation'],minute_cache_path=proof['minute_cache_path'],
            observed_at=NOW,research_summary={'unexpected_discovery_metadata':'x'*2_000_001})
    assert store._read(lambda db:db.execute('SELECT count(*) FROM natural_review_candidates').fetchone()[0])==0
