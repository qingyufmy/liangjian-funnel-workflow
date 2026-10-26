"""Monday EMPTY rehearses real read/morning functions, never future business."""
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import importlib.util,json,sqlite3
import pytest
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime.reviewed_batch_natural import stage_formal_empty,read_natural_candidate,finish_natural_candidate
from fixture_candidate_artifacts_0761 import stage_natural_candidate
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage

DAY='2026-10-26';CAL=ExchangeTradingCalendar()


def source(tmp_path):
    settings=SimpleNamespace(root=tmp_path,state_db_path=tmp_path/'state/workflow.sqlite3',research_primary_lane_id='lane_1',
        workflow_output_dir=tmp_path/'outputs',minute_cache_dir=tmp_path/'outputs/minute')
    store=RuntimeStore(settings.state_db_path)
    args=dict(run_id='controlled-zero-source',source_trade_date='2026-10-23',target_trade_date=DAY,
        lane_id='lane_1',source_evidence={'a1_manifest':'a'*64,'a2_a3_original':'b'*64},
        blocked=[dict(symbol='002164.SZ',reason='PLAN_GEOMETRY_RR_UNPROVEN')],
        observed_at=datetime.fromisoformat('2026-10-25T20:00:00+08:00'),calendar=CAL)
    return settings,store,args


def test_formal_empty_persists_truth_without_execution_rows_or_activation(tmp_path):
    settings,store,args=source(tmp_path);first=stage_formal_empty(store,**args)
    second=stage_formal_empty(store,**dict(args,observed_at=datetime.fromisoformat('2026-10-25T21:00:00+08:00')))
    assert first==second and first['execution_plans_written']==0
    candidate=read_natural_candidate(settings,day=DAY)
    assert candidate['state']=='EMPTY' and candidate['body']['plan_rows']==[]
    assert store.list_execution_plans()==()
    with pytest.raises(ValueError,match='EMPTY_CANNOT_SWITCH'):finish_natural_candidate(settings,candidate,calendar=CAL)
    assert read_natural_candidate(settings,day='2026-10-27') is None


@pytest.mark.parametrize('stage,clock',[('engine','08:33'),('risk','08:35'),('pm','08:38')])
def test_monday_zero_source_is_noop_without_model_account_or_lock(tmp_path,monkeypatch,stage,clock):
    settings,store,args=source(tmp_path);stage_formal_empty(store,**args)
    calls=[]
    monkeypatch.setattr('liangjian_funnel.runtime.team_v2_emotion_baseline.prepare_emotion_baseline',lambda *a,**k:dict(status='CONTROLLED_CACHE_ONLY'))
    class Deny:
        def complete(self,*a,**kw):calls.append('MODEL');raise AssertionError('NO_MODEL_ON_EMPTY')
    result=run_morning_stage(settings,stage,clock=lambda:datetime.fromisoformat(DAY+'T'+clock+':00+08:00'),calendar=CAL,client=Deny())
    assert result['status']=='NOOP' and result['reason_code']=='NATURAL_ZERO_PLAN_SOURCE'
    assert not result['account_opened'] and not calls
    assert not (settings.root/'state/team_v2').exists()
    assert store.list_execution_plans()==()


def test_empty_never_replaces_pending_candidate(tmp_path):
    settings,store,args=source(tmp_path)
    row=dict(plan_id='controlled-plan',lane_id='lane_1',symbol='000001.SZ',status='PENDING_MORNING_REVIEW',
        expires_at=DAY+'T15:00:00+08:00',payload=dict(source_run_id='existing',symbol='000001.SZ'))
    stage_natural_candidate(store,[row],run_id='existing',observed_at=args['observed_at'])
    with pytest.raises(ValueError,match='EXISTING_CANDIDATE_PRESERVED'):stage_formal_empty(store,**args)
    assert read_natural_candidate(settings,day=DAY)['state']=='PENDING'


def test_ui_distinguishes_not_generated_from_formal_empty(tmp_path):
    settings,store,args=source(tmp_path)
    path=Path(__file__).parents[1]/'scripts/team_ui_formal_source.py'
    spec=importlib.util.spec_from_file_location('formal_ui',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def connect(root,relative):
        db=sqlite3.connect((root/relative).as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row;db.execute('pragma query_only=ON');return db
    assert module.formal_source(tmp_path,DAY,connect)['status']=='NOT_GENERATED'
    stage_formal_empty(store,**args);view=module.formal_source(tmp_path,DAY,connect)
    assert view['status']=='FORMAL_EMPTY' and view['candidate_count']==0
    assert view['blocked']==[dict(symbol='002164.SZ',reason_code='PLAN_GEOMETRY_RR_UNPROVEN')]


def test_ui_keeps_large_candidate_lineage_out_of_small_status_response(tmp_path):
    settings,store,args=source(tmp_path)
    row=dict(plan_id='large-originals',lane_id='lane_1',symbol='000001.SZ',
        status='PENDING_MORNING_REVIEW',expires_at=DAY+'T15:00:00+08:00',
        payload=dict(source_run_id='full-originals',symbol='000001.SZ',
            formal_research_originals={'controlled_evidence':'x'*(1500*1024)}))
    stage_natural_candidate(store,[row],run_id='full-originals',observed_at=args['observed_at'])
    path=Path(__file__).parents[1]/'scripts/team_ui_formal_source.py'
    spec=importlib.util.spec_from_file_location('large_formal_ui',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def connect(root,relative):
        db=sqlite3.connect((root/relative).as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row;db.execute('pragma query_only=ON');return db
    view=module.formal_source(tmp_path,DAY,connect)
    assert view['status']=='CANDIDATE_PENDING_REVIEW' and len(json.dumps(view))<300
    assert view['execution_authority'] is False
