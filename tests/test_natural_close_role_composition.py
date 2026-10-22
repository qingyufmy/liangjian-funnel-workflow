"""Actual central run_research dispatch and persistence, no external providers."""
from datetime import datetime,date
from pathlib import Path
from types import SimpleNamespace
import json

import pytest
import yaml

import liangjian_funnel.workflow as workflow
from liangjian_funnel.pipeline.team_v2_a2_score import POLICY
from liangjian_funnel.pipeline.research import FrozenInputSnapshot,LaneResult,ResearchRunResult
from liangjian_funnel.settings import Settings

NOW=datetime.fromisoformat('2026-10-20T16:30:00+08:00')
ROOT=Path(__file__).resolve().parents[1]


def shell(tmp_path,monkeypatch,*,policy=POLICY):
    settings=Settings.from_env({},root=tmp_path).model_copy(update={
        'prompt_dir':ROOT/'prompts','close_resource_sampling_enabled':False})
    settings.source_config_path.parent.mkdir(parents=True)
    a2={} if policy=='ABSENT' else {'team_theme_score_policy':policy}
    settings.source_config_path.write_text(yaml.safe_dump({'agent_1':{'team_role_mode':'production'},'agent_2':a2}),encoding='utf-8')
    app=object.__new__(workflow.WorkflowApplication);app.settings=settings
    app.trading_calendar=SimpleNamespace(is_trading_day=lambda _:True,next_trading_day=lambda _:date(2026,10,21),
        previous_trading_day=lambda _:date(2026,10,19))
    app._ensure_trading_day=lambda *a,**k:None
    generation=SimpleNamespace(as_of=NOW,generation_id='FROZEN_TEST_A1',payload={},payload_hash='a'*64)
    app.a1_registry=SimpleNamespace(require_active=lambda **k:generation,get_active_generation=lambda:generation)
    snapshot=FrozenInputSnapshot(snapshot_id='FROZEN_TEST',snapshot_hash='b'*64,as_of=NOW,data={'g0_symbols':['000333.SZ']})
    prepared=workflow.PreparedSnapshot(snapshot=snapshot,path=tmp_path/'frozen.json',full_universe_count=1,
        research_universe_count=1,trade_universe_count=1,selected_count=1,factor_ready_count=1)
    app.prompts=object();app.model_client=object();app.store=None;app.research_checkpoints=None
    app._stage_snapshot_enricher=None
    app._write_research_resume_marker=lambda *a,**k:None
    app._load_research_resume_snapshot=lambda *a,**k:prepared
    app._load_research_snapshot_by_id=lambda *a,**k:prepared
    sync=[]
    def prepare(**kwargs):sync.append(kwargs);return prepared
    app.prepare_snapshot=prepare
    publication=[]
    def publish(result,*a,**k):publication.append(result);return {'atomic':True,'created':['SYNTHETIC_PLAN'],'activated':[],'blocked':[]}
    app._publish_plans=publish
    resources=SimpleNamespace(allowed=True,snapshot=SimpleNamespace(as_dict=lambda:{}))
    monkeypatch.setattr(workflow,'evaluate_resources',lambda _:resources)
    monkeypatch.setattr(workflow,'measure_resources',lambda _:resources.snapshot)
    monkeypatch.setattr(workflow,'_active_a1_downstream_scope',lambda _:('000333.SZ',))
    monkeypatch.setattr(workflow,'_write_broker_gold_benchmark',lambda *a,**k:None)
    monkeypatch.setattr(workflow,'write_stage_markdown_reports',lambda *a,**k:())
    dispatched=[]
    class Pipeline:
        def __init__(self,settings,**kwargs):self.adapter=kwargs.get('team_research_adapter')
        def result(self,snapshot,kwargs):
            dispatched.append(self.adapter)
            if self.adapter is not None:
                assert self.adapter.supports('A2') and self.adapter.supports('A3') and not self.adapter.supports('A1')
            lane=LaneResult(lane='lane_1',model=settings.research_models[0],status='READY',stages=(),
                final_output={'core_watch_pool':[]})
            return ResearchRunResult(run_id=kwargs['run_id'],generated_at=NOW,snapshot_id=snapshot.snapshot_id,
                snapshot_hash=snapshot.snapshot_hash,status='READY',lanes=(lane,),audit_paths=(),markdown_path=None,
                primary_lane_ids=('lane_1',))
        def run_from_active_a1(self,snapshot,generation,**kwargs):
            assert generation.generation_id=='FROZEN_TEST_A1';return self.result(snapshot,kwargs)
        def run(self,snapshot,**kwargs):return self.result(snapshot,kwargs)
    monkeypatch.setattr(workflow,'ResearchPipeline',Pipeline)
    return app,dispatched,sync,publication


@pytest.mark.parametrize('flag',['ABSENT',False,True])
def test_real_natural_close_api_composes_team_from_contract_without_caller_flag(tmp_path,monkeypatch,flag):
    app,dispatched,sync,publication=shell(tmp_path,monkeypatch)
    if flag!='ABSENT':app.team_research_enabled=flag
    result=app.run_research('close',as_of=NOW,from_active_a1=True,primary_only=True,
        schedule_comparison=False,reuse_resume_snapshot=False,run_id_override='NATURAL_TEST')
    assert len(dispatched)==len(sync)==len(publication)==1 and dispatched[0] is not None
    assert result['a1_reused'] and result['plan_publication']['created']==['SYNTHETIC_PLAN']
    saved=json.loads((app.settings.workflow_output_dir/'runs/NATURAL_TEST.json').read_bytes())
    assert saved['run_id']==result['run_id'] and saved['status']=='READY'
    if flag=='ABSENT':assert not hasattr(app,'team_research_enabled')
    else:assert app.team_research_enabled is flag
    # Owned role journals appear only at this actual research boundary.
    assert (tmp_path/'state/team_v2/model.db').is_file()


@pytest.mark.parametrize('fault',[None,{},'UNAPPROVED','MISSING_FILE','BAD_YAML','MISSING_AGENT2'])
def test_bad_natural_role_contract_fails_before_snapshot_or_pipeline(tmp_path,monkeypatch,fault):
    app,dispatched,sync,_=shell(tmp_path,monkeypatch,policy=fault)
    if fault=='MISSING_FILE':app.settings.source_config_path.unlink()
    elif fault=='BAD_YAML':app.settings.source_config_path.write_text('agent_2: [unterminated',encoding='utf-8')
    elif fault=='MISSING_AGENT2':app.settings.source_config_path.write_text('agent_1: {}',encoding='utf-8')
    with pytest.raises(workflow.WorkflowError,match='RESEARCH_ROLE_CONFIGURATION_UNAVAILABLE|TEAM_RESEARCH_CONFIGURATION_INVALID'):
        app.run_research('close',as_of=NOW,from_active_a1=True,primary_only=True,reuse_resume_snapshot=False)
    assert not sync and not dispatched and not (tmp_path/'state/team_v2').exists()


def test_explicit_existing_legacy_configuration_survives_cli_flag(tmp_path,monkeypatch):
    app,dispatched,_,_=shell(tmp_path,monkeypatch,policy='ABSENT')
    app.team_research_enabled=True
    result=app.run_research('close',as_of=NOW,primary_only=True)
    assert result['status']=='READY' and dispatched==[None]
    assert app.team_research_enabled is True and not (tmp_path/'state/team_v2').exists()


@pytest.mark.parametrize('mode',['historical','comparison'])
def test_non_natural_close_keeps_existing_explicit_role_choice(tmp_path,monkeypatch,mode):
    app,dispatched,_,_=shell(tmp_path,monkeypatch)
    app.team_research_enabled=False
    app._natural_close_team_research=lambda:(_ for _ in ()).throw(AssertionError('natural selection leaked'))
    result=app.run_research('close',as_of=NOW.replace(year=2025),primary_only=True,
        snapshot_id='FROZEN_TEST',historical_replay=mode=='historical',comparison_run=mode=='comparison')
    assert result['status']=='READY' and dispatched==[None]


def test_workflow_construction_does_not_resolve_or_open_team_roles(tmp_path,monkeypatch):
    # Preserve the established deferred factory contract, independent of the
    # new central natural close opt-in.
    app=object.__new__(workflow.WorkflowApplication);app.settings=Settings.from_env({},root=tmp_path)
    pipeline=app._new_research_pipeline(enable_feature_store=False)
    assert pipeline.team_research_adapter is None and not (tmp_path/'state/team_v2').exists()


def test_natural_daily_a1_cannot_be_bypassed_by_explicit_old_sealed_id(tmp_path,monkeypatch):
    app,dispatched,sync,publication=shell(tmp_path,monkeypatch)
    with pytest.raises(workflow.WorkflowError,match='A1_EXPLICIT_GENERATION_NOT_CURRENT_DAILY'):
        app.run_research('close',as_of=NOW,active_a1_generation_id='OLD_SEALED',
            primary_only=True,schedule_comparison=False,reuse_resume_snapshot=False)
    assert not dispatched and not sync and not publication


def test_natural_daily_a1_is_consumed_even_without_caller_reuse_flag(tmp_path,monkeypatch):
    app,dispatched,sync,publication=shell(tmp_path,monkeypatch)
    result=app.run_research('close',as_of=NOW,from_active_a1=False,primary_only=True,
        schedule_comparison=False,reuse_resume_snapshot=False)
    assert result['a1_reused'] and len(dispatched)==len(sync)==len(publication)==1
