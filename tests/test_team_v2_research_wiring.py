"""Actual Workflow factory -> three role adapter -> A2 HTTP mock/hard gates."""
from contextlib import closing
from pathlib import Path
import shutil

import pytest

from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter,RESEARCH_COMMANDS
from liangjian_funnel.workflow import WorkflowApplication
from test_team_v2_a2_research import fixture,NOW


def test_workflow_factory_composes_actual_a2_role_and_its_owned_capture(tmp_path):
    original,args,seen=fixture(tmp_path)
    root=Path(__file__).resolve().parents[1]
    app=object.__new__(WorkflowApplication)
    app.settings=original.settings.model_copy(update={'source_config_path':root/'config/funnel_config_v2.yaml',
        'prompt_dir':root/'prompts'})
    app.trading_calendar=original.team_research_adapter.calendar;app.team_research_enabled=True
    pipeline=app._new_research_pipeline(model_client=original.model_client,prompt_repository=root/'prompts',
        enable_feature_store=False,now=lambda:NOW)
    adapter=pipeline.team_research_adapter
    assert not adapter.supports('A1') and all(adapter.supports(s) for s in ('A2','A3'))
    result=pipeline._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==1,result.reason_codes
    assert adapter.journal.path==tmp_path/'state/team_v2/model.db'
    with closing(adapter.journal._connect()) as db:
        assert db.execute('SELECT role,validated FROM model_calls').fetchall()==[('A2',1)]
    from liangjian_funnel.pipeline.team_v2_prompts import _VERSIONS
    assert _VERSIONS['A2'] in seen[0]['messages'][0]['content']
    assert not (tmp_path/'state/workflow.sqlite3').exists()


def test_library_legacy_pipeline_remains_explicit_without_own_journal(tmp_path):
    from liangjian_funnel.settings import Settings
    app=object.__new__(WorkflowApplication);app.settings=Settings.from_env({},root=tmp_path)
    result=app._new_research_pipeline(enable_feature_store=False)
    assert result.team_research_adapter is None and not (tmp_path/'state/team_v2').exists()


@pytest.mark.parametrize('mode',['production','team'])
def test_0323_explicit_a1_mode_does_not_disable_actual_team_a2(tmp_path,mode):
    original,args,seen=fixture(tmp_path)
    root=Path(__file__).resolve().parents[1]
    config=(root/'config/funnel_config_v2.yaml').read_text(encoding='utf-8')
    target=tmp_path/'fallback.yaml'
    target.write_text(config.replace('team_role_mode: production','team_role_mode: '+mode,1),encoding='utf-8')
    original.settings=original.settings.model_copy(update={'source_config_path':target,'prompt_dir':root/'prompts'})
    adapter=configured_research_adapter(original.settings,calendar=original.team_research_adapter.calendar)
    original.team_research_adapter=adapter
    assert adapter.supports('A1')==(mode=='team') and adapter.supports('A2') and adapter.supports('A3')
    legacy={'structural_themes':[{'theme_id':'OLD','policy_verifiability':'VERIFIED'}]}
    if mode=='production':assert adapter.finalize_a1_output(legacy) is legacy
    result=original._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==1
    with closing(adapter.journal._connect()) as db:
        assert db.execute('SELECT role FROM model_calls').fetchall()==[('A2',)]
    if mode=='production':
        with pytest.raises(ValueError,match='TEAM_A1_ROLE_DISABLED'):
            adapter.run_stage(original,**(args|{'stage':'A1'}))


def test_0323_production_a1_dispatch_reaches_original_preparation_without_team_call(tmp_path,monkeypatch):
    from test_team_v2_a1_research import fixture as a1_fixture
    from liangjian_funnel.pipeline.research.common import ResearchPipelineError
    app,args,seen=a1_fixture(tmp_path)
    app.team_research_adapter.a1_role_mode='production'
    prepared=[]
    def original_prepare(**kwargs):
        prepared.append(kwargs)
        raise ResearchPipelineError('ORIGINAL_A1_PREPARATION_PROBE')
    monkeypatch.setattr(app,'_prepare_stage_request',original_prepare)
    result=app._run_stage(**args)
    assert result.reason_codes==('ORIGINAL_A1_PREPARATION_PROBE',)
    assert len(prepared)==1 and prepared[0]['bundle'] is args['bundle'] and not seen
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute('SELECT count(*) FROM model_calls').fetchone()[0]==0


@pytest.mark.parametrize('mode',['RETRY',False,None,{},[]])
def test_0323_invalid_a1_mode_cannot_create_owned_database(tmp_path,mode):
    import yaml
    from liangjian_funnel.settings import Settings
    from test_team_v2_feedback import CAL
    root=Path(__file__).resolve().parents[1]
    config=yaml.safe_load((root/'config/funnel_config_v2.yaml').read_text(encoding='utf-8'))
    config['agent_1']['team_role_mode']=mode
    target=tmp_path/'invalid.yaml';target.write_text(yaml.safe_dump(config),encoding='utf-8')
    settings=Settings.from_env({},root=tmp_path).model_copy(update={'source_config_path':target})
    with pytest.raises(ValueError,match='TEAM_A1_ROLE_MODE_INVALID'):
        configured_research_adapter(settings,calendar=CAL)
    assert not (tmp_path/'state/team_v2').exists()


def test_real_research_commands_opt_in_but_nonresearch_cli_cannot_initialize_models():
    assert {'run-a1-maintenance','run-research','run-next-session-prep','run-close','run-due'}<=RESEARCH_COMMANDS
    assert not {'status','run-auction-base','run-premarket','run-morning','run-monitor','prepare-snapshot'} & RESEARCH_COMMANDS


@pytest.mark.parametrize('command,enabled',[
    (['run-close'],True),(['run-research','--slot','close'],True),(['run-next-session-prep'],True),
    (['run-a1-maintenance','--mode','full'],True),(['run-premarket'],False),(['run-morning'],False)])
def test_actual_cli_sets_role_composition_before_dispatch_without_a_new_env_switch(tmp_path,monkeypatch,command,enabled):
    import liangjian_funnel.cli as cli
    from test_cli_workflow_coverage import _FakeApplication,_settings,_ready_response
    seen=[]
    class Application(_FakeApplication):
        response=_ready_response();error=None
        def _invoke(self,name,*args,**kwargs):
            seen.append((name,self.team_research_enabled))
            return super()._invoke(name,*args,**kwargs)
    monkeypatch.setattr(cli,'WorkflowApplication',Application)
    cli.main(command,settings=_settings(tmp_path))
    assert len(seen)==1 and seen[0][1] is enabled
    assert not (tmp_path/'state/team_v2').exists()


@pytest.mark.parametrize('fault',['prompt-hash','manifest-domain','score-policy','owned-symlink'])
def test_invalid_bundle_or_config_cannot_fallback_or_open_owned_journal(tmp_path,fault):
    from liangjian_funnel.settings import Settings
    from test_team_v2_feedback import CAL
    root=Path(__file__).resolve().parents[1]
    shutil.copytree(root/'prompts/team_v2',tmp_path/'prompts/team_v2')
    config=(root/'config/funnel_config_v2.yaml').read_text(encoding='utf-8')
    target=tmp_path/'funnel.yaml';target.write_text(config,encoding='utf-8')
    if fault=='prompt-hash':(tmp_path/'prompts/team_v2/agent_2_theme_sentiment_v3.txt').write_text('tampered',encoding='utf-8')
    elif fault=='manifest-domain':
        import json
        path=tmp_path/'prompts/team_v2/manifest.json';body=json.loads(path.read_text(encoding='utf-8'))
        body['files']['unexpected.txt']=body['files'].pop('agent_2_theme_sentiment_v3.txt')
        path.write_text(json.dumps(body),encoding='utf-8')
    elif fault=='score-policy':target.write_text(config.replace('MIDPOINT_FIXED_V1','UNAPPROVED'),encoding='utf-8')
    else:
        (tmp_path/'outside').mkdir();(tmp_path/'state').mkdir()
        try:(tmp_path/'state/team_v2').symlink_to(tmp_path/'outside',target_is_directory=True)
        except OSError:pytest.skip('symlink privilege unavailable; other scope checks covered separately')
    settings=Settings.from_env({},root=tmp_path).model_copy(update={'source_config_path':target})
    with pytest.raises(ValueError):configured_research_adapter(settings,calendar=CAL)
    assert not (tmp_path/'state/team_v2/model.db').exists() and not (tmp_path/'outside/model.db').exists()
