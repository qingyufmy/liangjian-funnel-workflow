from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from fixture_candidate_artifacts_0761 import prepare
from liangjian_funnel.runtime.candidate_artifacts import verify_candidate_artifacts
from liangjian_funnel.runtime.reviewed_batch_natural import stage_natural_candidate
from liangjian_funnel.runtime.state import RuntimeStore


NOW=datetime(2026,10,26,16,tzinfo=ZoneInfo('Asia/Shanghai'))


def fixture(tmp_path):
    store=RuntimeStore(tmp_path/'workflow.db')
    store.record_workflow_run(run_id='natural-close',lane_id='lane_1',trade_date='2026-10-26',slot='close',
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


def test_real_archive_summary_native_readback_then_stage(tmp_path):
    store,batch=fixture(tmp_path); proof=prepare(store,batch,run_id='natural-close',observed_at=NOW)
    result=stage_natural_candidate(store,batch,run_id='natural-close',observed_at=NOW,artifact_proof=proof)
    assert result['status']=='CANDIDATE_PENDING_REVIEW' and result['execution_plans_written']==0
    assert verify_candidate_artifacts(batch,run_id='natural-close',proof=proof)['daily_facts']=={
        batch[0]['plan_id']:'READY'}


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
