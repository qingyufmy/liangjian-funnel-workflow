from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
from liangjian_funnel.settings import Settings

NOW=datetime.fromisoformat('2026-10-16T16:30:00+08:00')


def fixture(tmp_path,mutate=None,*,rotation=False,values=(40,80,90)):
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.pipeline.team_v2_research import TeamResearchAdapter
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    root=Path(__file__).resolve().parents[1]
    config=yaml.safe_load((root/'config/funnel_config_v2.yaml').read_text(encoding='utf-8'))['agent_2']
    contexts={f'{i:06}.SZ':dict(company_name='TEST',theme_id='T',industry_chain_node='N',
        upstream_candidate_id='a1:'+str(i),deterministic_status='REVIEW_CANDIDATE',
        deterministic_route='MARKET_CORE',preferred_route='MARKET_CORE',eligible_routes=['MARKET_CORE'],
        deterministic_market_role='TREND_CORE',
        top_rotation_theme=rotation,trend_core_eligible=True,rotation_direction_id='D',
        selected_board=dict(selected_for_rotation=rotation),
        a2_factor_scores={n:dict(available=True,score=v,source_refs=['original:'+str(i)]) for n in config['score_weights']})
        for i,v in enumerate(values,1)}
    seen=[]
    def handler(request):
        body=json.loads(request.read());seen.append(body)
        runtime=json.loads(body['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        raw=dict(envelope=runtime['required_envelope'],reason_codes=[],
            market_read=dict(regime='ROTATION_NO_MAINLINE',dominant_themes=['T'],emotion_cycle='CONFIRMATION',
                supporting_refs=['A2:MARKET'],opposing_refs=['A2:NEWS']),
            theme_reviews=[dict(theme_id='T',stage='CONFIRMATION',new_entry_policy='ALLOW',penalty_codes=[],
                support_codes=[],risk_codes=[],supporting_refs=['A2:QUANT:000001.SZ'],opposing_refs=['A2:NEWS'],
                what_would_change_my_mind=['Official evidence reverses'])],
            focus_decisions=[dict(symbol='000001.SZ',support_codes=[],risk_codes=[],
                supporting_refs=['A2:QUANT:000001.SZ'],opposing_refs=['A2:NEWS'])],
            rotation_reviews=[],reject_decisions=[],model_nominations=[],handoff_notes={'to_a3':['Check technical facts']},
            what_would_change_my_mind=['Official facts change'],unresolved_questions=[])
        if mutate:mutate(raw,len(seen))
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    adapter=TeamResearchAdapter(repository=TeamPromptRepository(root/'prompts/team_v2'),
        journal=ModelJudgmentJournal(tmp_path/'models.db'),store=TeamMeetingStore(tmp_path/'team.db'),
        calendar=SimpleNamespace(is_trading_day=lambda _:True,next_trading_day=lambda _:datetime.fromisoformat('2026-10-19').date()),
        config_version='frozen-test',config_hash='a'*64,a2_config=config)
    app=ResearchPipeline(settings,model_client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),
        now=lambda:NOW,enable_feature_store=False,prompt_repository=root/'prompts',team_research_adapter=adapter)
    data=dict(A2_BOTTLENECK_CONTEXT=contexts,CAPITAL_FLOW_SNAPSHOT={'available':True},STRICT_AGENT_RULES=True,
        THEME_SCORE_WEIGHTS=config['score_weights'],PENALTY_RULES=config['penalty_rules'],MIN_THEME_SCORE=60,
        MARKET_REGIME_SNAPSHOT={'regime':'ROTATION_NO_MAINLINE'},NEWS_HEAT_SNAPSHOT={'status':'FROZEN','value':'original-news'},
        PRIOR_OUTCOME_FEEDBACK={'status':'READY','arbitrary_stock_return':999})
    args=dict(lane_id='lane_1',model=settings.research_models[0],stage='A2',snapshot=FrozenInputSnapshot('original',data,as_of=NOW),
        upstream_output={'active_research_pool':[dict(symbol=s,primary_theme='T',industry_chain_node='N',company_name='TEST') for s in contexts]},
        upstream_symbols=set(contexts),projection_symbols=set(contexts),bundle=app.prompts.load(),run_id='isolated-close')
    return app,args,seen


def test_actual_role_journal_and_global_fact_score_before_model_choice(tmp_path):
    app,args,seen=fixture(tmp_path);before=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    theme=result.output['active_themes'][0]
    assert float(theme['theme_score'])==70 and len(result.output['focus_pool'])==1,result.output['watch_only_pool']
    assert float(theme['score_breakdown']['breadth'])==70
    assert theme['team_score_evidence']['score_snapshot_sha256']==result.diagnostics['score_snapshot_sha256']
    from liangjian_funnel.pipeline.team_v2_prompts import _VERSIONS
    assert len(seen)==1 and _VERSIONS['A2'] in seen[0]['messages'][0]['content']
    assert 'SERVER-AUTHORIZED ROTATION POLICY' not in seen[0]['messages'][0]['content']
    assert 'arbitrary_stock_return' not in seen[0]['messages'][0]['content']
    with closing(app.team_research_adapter.journal._connect()) as db:
        output=json.loads(db.execute("SELECT output_json FROM model_calls WHERE role='A2'").fetchone()[0])
        assert 'theme_score' not in output['theme_reviews'][0]
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()[0]>=3
    assert app._run_stage(**args).output==result.output and len(seen)==1
    assert dict(args['snapshot'].data)==before


@pytest.mark.parametrize('side',['supporting_refs','opposing_refs'])
def test_directory_foreign_reference_uses_only_one_feedback_round_and_keeps_original(tmp_path,side):
    def mutate(raw,attempt):
        if attempt==1:raw['market_read'][side]=['A2:QUANT:601288.SZ']
    app,args,seen=fixture(tmp_path,mutate)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==2,result.reason_codes
    assert result.diagnostics['first_round_failed'] is True
    assert result.diagnostics['reference_feedback_rounds']==1
    assert 'A2:QUANT:601288.SZ' in seen[1]['messages'][0]['content']
    with closing(app.team_research_adapter.journal._connect()) as db:
        outputs=[json.loads(r[0]) for r in db.execute("SELECT output_json FROM model_calls ORDER BY captured_at")]
    assert outputs[0]['market_read'][side]==['A2:QUANT:601288.SZ']
    assert len(outputs)==2 and result.latency_ms>=0
    assert app._run_stage(**args).output==result.output and len(seen)==2


def test_foreign_reference_three_times_stays_invalid_not_dropped_or_default_pass(tmp_path):
    app,args,seen=fixture(tmp_path,lambda raw,n:raw['market_read'].update(opposing_refs=['A2:FOREIGN']))
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert result.reason_codes==('TEAM_OPPOSING_REFS_INVALID:market_read',)


def test_empty_refs_get_bounded_feedback_but_are_never_repaired(tmp_path):
    app,args,seen=fixture(tmp_path,lambda raw,n:raw['market_read'].update(opposing_refs=[]))
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3


@pytest.mark.parametrize('missing,limited',[(('leader_structure',),False),(('leader_structure','breadth'),True)])
def test_minor_missing_null_not_zero_and_major_missing_forced_watch(tmp_path,missing,limited):
    app,args,_=fixture(tmp_path)
    for row in args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
        for name in missing:row['a2_factor_scores'][name]['available']=False
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert all(result.output['active_themes'][0]['score_breakdown'][n] is None for n in missing)
    assert bool(result.output['focus_pool']) is not limited,result.output['watch_only_pool']
    if limited:assert result.output['active_themes'][0]['theme_score'] is None


@pytest.mark.parametrize('kind',['extra-score','outside-stock','foreign-ref','unknown-penalty','regime-lie','missing-envelope'])
def test_bad_model_never_repaired_into_approved_output(tmp_path,kind):
    def change(raw,_):
        if kind=='extra-score':raw['theme_reviews'][0]['theme_score']=100
        if kind=='outside-stock':raw['focus_decisions'][0]['symbol']='999999.SZ'
        if kind=='foreign-ref':raw['focus_decisions'][0]['supporting_refs']=['INVENTED']
        if kind=='unknown-penalty':raw['theme_reviews'][0]['penalty_codes']=['INVENTED']
        if kind=='regime-lie':raw['market_read']['regime']='TREND_MAINLINE'
        if kind=='missing-envelope':raw.pop('envelope')
    app,args,seen=fixture(tmp_path,change)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and result.output is None and len(seen)==3


def test_three_missing_rotation_captures_stay_invalid_for_existing_degradation(tmp_path):
    def change(raw,_):
        raw['focus_decisions']=[]  # all frozen themes still require stage reviews (0283)
    app,args,seen=fixture(tmp_path,change,rotation=True)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and len(seen)==3
    assert result.diagnostics['validation_history']==[['A2_ROTATION_COVERAGE_MISMATCH']]*3
    assert result.diagnostics['server_normalization'] is None
    assert 'required_no_focus_if_focus_unchanged' in seen[1]['messages'][0]['content']
    again=app._run_stage(**args)
    assert again.reason_codes==result.reason_codes and len(seen)==3


def test_pre_split_freeze_uses_all_members_not_batch_mean(tmp_path):
    app,args,_=fixture(tmp_path)
    original=args['snapshot'];args['snapshot']=app.team_research_adapter.prepare_a2_snapshot(original,args['upstream_symbols'])
    args['upstream_symbols']=args['projection_symbols']={'000001.SZ'}
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert float(result.output['active_themes'][0]['theme_score'])==70


def test_missing_or_changed_config_blocks_before_http(tmp_path):
    app,args,seen=fixture(tmp_path)
    args['snapshot']=replace(args['snapshot'],data={**args['snapshot'].data,'PENALTY_RULES':{}})
    result=app._run_stage(**args)
    assert result.reason_codes==('TEAM_A2_CONFIG_SOURCE_MISMATCH',) and not seen


def test_original_hard_route_and_no_focus_demotion_not_repaired(tmp_path):
    app,args,_=fixture(tmp_path)
    for context in args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
        context['eligible_routes']=[]
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and result.output['focus_pool']==[]
    assert 'A2_ROUTE_MISSING_OR_INVALID' in result.output['watch_only_pool'][-1]['reason_codes']


def test_valid_nomination_stays_research_only_and_not_a3_candidate(tmp_path):
    def change(raw,_):
        raw['model_nominations']=[dict(symbol='000004.SZ',theme_id='T',nomination_thesis='Verified sector evidence',
            quant_filter_reason='SCORE_BELOW_GATE',why_i_disagree='Frozen disclosed catalyst',falsifiers=['Catalyst withdrawn'],
            supporting_refs=['A2:NOMINATION:000004.SZ'],opposing_refs=['A2:NEWS'])]
    app,args,_=fixture(tmp_path,change)
    data=deepcopy(dict(args['snapshot'].data))
    data['A2_BOTTLENECK_CONTEXT']['000004.SZ']={**deepcopy(data['A2_BOTTLENECK_CONTEXT']['000001.SZ']),
        'deterministic_status':'LOCAL_MONITOR','deterministic_reason_codes':['SCORE_BELOW_GATE'],
        'role_breakdown':{'relative_strength':85}}
    args['snapshot']=replace(args['snapshot'],data=data)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    from liangjian_funnel.pipeline.research.common import _build_a3_candidate_domain
    _,domain=_build_a3_candidate_domain(result.output)
    assert '000004.SZ' not in domain and result.output['model_nominations'][0]['symbol']=='000004.SZ'


def test_restart_smaller_budget_reuses_capture_but_new_request_cannot_expand(tmp_path):
    import time
    app,args,seen=fixture(tmp_path)
    result=app._run_stage(**args);assert result.status=='VALIDATED'
    app._deadline_started_monotonic=time.monotonic()-app.settings.research_close_deadline_seconds+5
    assert app._run_stage(**args).output==result.output and len(seen)==1
    app._deadline_started_monotonic=time.monotonic()-app.settings.research_close_deadline_seconds-1
    assert app._run_stage(**args).reason_codes==('RESEARCH_DEADLINE_EXCEEDED',) and len(seen)==1


def test_pre_split_freeze_cannot_hide_changed_original_factor(tmp_path):
    app,args,seen=fixture(tmp_path)
    args['snapshot']=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    args['snapshot'].data['A2_BOTTLENECK_CONTEXT']['000002.SZ']['a2_factor_scores']['breadth']['score']=100
    assert app._run_stage(**args).reason_codes==('TEAM_A2_FROZEN_SOURCE_MISMATCH',) and not seen


def test_legacy_rotation_validator_conflict_after_server_missing_data_demotion_is_not_hidden(tmp_path):
    app,args,seen=fixture(tmp_path,rotation=True)
    for row in args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
        for name in ('leader_structure','breadth'):row['a2_factor_scores'][name]['available']=False
    result=app._run_stage(**args)
    # New missing-score policy requires WATCH, but original coverage validator
    # now expects NO_FOCUS for a direction the model had actually focused.
    # Do not invent a model review: expose this contract question to Claude.
    assert result.status=='BLOCKED' and any(r.startswith('A2_ROTATION_') for r in result.reason_codes)
    from liangjian_funnel.pipeline.review_degraded import rotation_review_failed_twice
    assert rotation_review_failed_twice(result.diagnostics['validation_history']) and len(seen)==3
    assert 'server_demoted_focus_symbols' in seen[1]['messages'][0]['content']


def test_second_review_must_actually_explain_no_focus_after_data_demotion(tmp_path):
    def change(raw,attempt):
        if attempt==2:
            raw['focus_decisions']=[]
            raw['rotation_reviews']=[dict(rotation_direction_id='D',decision='NO_FOCUS',reviewed_symbols=['000001.SZ'],
                reason_codes=['SCORE_DATA_LIMITED'],explanation='Frozen member fields have insufficient coverage',
                supporting_refs=['A2:QUANT:000001.SZ'],opposing_refs=['A2:NEWS'])]
    app,args,seen=fixture(tmp_path,change,rotation=True)
    for row in args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
        for name in ('leader_structure','breadth'):row['a2_factor_scores'][name]['available']=False
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==2 and result.output['focus_pool']==[] and len(result.output['rotation_reviews'])==1
    assert app._run_stage(**args).output==result.output and len(seen)==2


def test_actual_quant_downstream_three_unrepairable_reviews_yield_degraded_watch_not_fake_pass(tmp_path):
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    def change(raw,_):
        raw['focus_decisions']=[]
        row=dict(rotation_direction_id='D',decision='NO_FOCUS',reviewed_symbols=['000001.SZ'],
            reason_codes=['LOW_CONVICTION'],explanation='Frozen evidence remains insufficient for focus',
            supporting_refs=['A2:QUANT:000001.SZ'],opposing_refs=['A2:NEWS'])
        raw['rotation_reviews']=[deepcopy(row),deepcopy(row)]
    app,args,seen=fixture(tmp_path,change,rotation=True)
    rows=tuple(dict(symbol=s,name='TEST',theme_id='T',node_id='N',upstream_candidate_id='a1:'+s,
        status='REVIEW_CANDIDATE',role='TREND_CORE',route='MARKET_CORE',eligible_routes=['MARKET_CORE'],
        data_sufficiency_state='SUFFICIENT',reason_codes=[]) for s in sorted(args['upstream_symbols']))
    gate=DeterministicGateResult('A2_LOCAL_ROLE',rows,tuple(sorted(args['upstream_symbols'])),(),())
    selected={k:v for k,v in args.items() if k not in {'projection_symbols','upstream_symbols'}}
    result=app._run_v2_downstream_review(**selected,full_upstream_symbols=args['upstream_symbols'],gate=gate)
    assert result.status=='A2_REVIEW_DEGRADED',result.reason_codes
    assert result.output['focus_pool']==[] and result.diagnostics['model_review_passed'] is False
    assert all(r['A2_REVIEW_DEGRADED'] is True for r in result.output['watch_only_pool'])
    assert len(seen)==3
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute("SELECT count(*) FROM model_calls WHERE role='A2'").fetchone()[0]==3


def test_forged_server_score_proof_is_rejected_by_original_lineage_validator(tmp_path):
    app,args,_=fixture(tmp_path);result=app._run_stage(**args)
    snapshot=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    from liangjian_funnel.pipeline.research import _a2_theme_reasons
    theme=deepcopy(result.output['active_themes'][0]);theme['theme_score']='100'
    assert 'A2_TEAM_SCORE_PROOF_MISMATCH' in _a2_theme_reasons(theme,snapshot.data)
