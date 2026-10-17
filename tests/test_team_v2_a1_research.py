from copy import deepcopy
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline
from liangjian_funnel.settings import Settings

NOW=datetime.fromisoformat('2026-10-16T18:00:00+08:00')


def test_author319_catalog_uses_only_original_family_values_and_same_ref_domain():
    from liangjian_funnel.pipeline.team_v2_a1_research import _catalog
    values={'A1_RESEARCH_PACKET':{'policy':[dict(fact_id='p',official_category='bumenfile')],
        'broker':dict(source_type='PUBLIC_INSTITUTION_STRATEGY_AGGREGATION',source_ref='b'),
        'unknown':dict(source_ref='u',title='policy broker industry news is not metadata')}}
    before=deepcopy(values);catalog=_catalog(values)
    assert {r['id']:r['category'] for r in catalog['items']}=={
        'p':'bumenfile','b':'PUBLIC_INSTITUTION_STRATEGY_AGGREGATION','u':'RESEARCH_PACKET'}
    assert values==before


def fixture(tmp_path, *, mutate=None,discovery=False):
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.pipeline.team_v2_research import TeamResearchAdapter
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    root=Path(__file__).resolve().parents[1];seen=[]
    def handler(request):
        body=json.loads(request.read());seen.append(body)
        runtime=json.loads(body['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        output=dict(envelope=runtime['required_envelope'],structural_themes=[],industry_chain_graph=[],
            active_research_pool=[],monitor_pool=[dict(symbol='000001.SZ',company_name='TEST',
                primary_theme='T',industry_chain_node='N',status='MONITOR',source_refs=['policy-ref'],
                supporting_refs=['policy-ref'],opposing_refs=['risk-ref'],
                what_would_change_my_mind=['A verified disclosure reverses the thesis'],
                data_quality_score=100,evidence_confidence=1)],rejected_candidates=[],
            handoff_notes=['A2 must confirm current market participation'],
            what_would_change_my_mind=['Formal policy is revoked'])
        if mutate:mutate(output)
        if discovery:
            from test_a1_contract import _discovery
            output.update(_discovery(theme_count=12,node_count=40))
            for pool in ('active_research_pool','monitor_pool','rejected_candidates'):output.pop(pool)
            for row in output['structural_themes']:
                row.update(source_refs=['policy-ref'],supporting_refs=['policy-ref'],opposing_refs=['policy-ref'],
                    catalysts=[dict(event='Official catalyst',expected_date='UNKNOWN',source_ref='policy-ref',priced_in_assessment='UNKNOWN')],
                    falsifiers=['Policy revoked'],handoff_to_a2=['Verify market participation'])
            for i,row in enumerate(output['industry_chain_graph']):
                row.update(source_refs=['policy-ref'],chain_position=('UPSTREAM','MIDSTREAM','DOWNSTREAM')[(i//12)%3])
            if mutate:mutate(output)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(output)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    journal=ModelJudgmentJournal(tmp_path/'model.db');store=TeamMeetingStore(tmp_path/'team.db')
    calendar=SimpleNamespace(is_trading_day=lambda _:True,next_trading_day=lambda _:datetime.fromisoformat('2026-10-19').date())
    adapter=TeamResearchAdapter(repository=TeamPromptRepository(root/'prompts/team_v2'),journal=journal,
        store=store,calendar=calendar,config_version='test-frozen',config_hash='a'*64)
    app=ResearchPipeline(settings,model_client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1),
        now=lambda:NOW,enable_feature_store=False,prompt_repository=root/'prompts',team_research_adapter=adapter)
    data=dict(MACRO_POLICY_FEED={'source_ref':'policy-ref'},RISK_EVENTS={'source_ref':'risk-ref'},
        PRIOR_OUTCOME_FEEDBACK={'status':'READY','symbol':'000001.SZ','t1_return':999})
    if discovery:data['MACRO_POLICY_FEED']={'official_documents':[dict(fact_id='policy-ref',title='Official industry policy',
        summary='Official frozen source',publish_time='2026-10-15T10:00:00+08:00',source_url='https://example.invalid/policy')]}
    context=dict(mode='COMPANY_MAPPING',structural_themes=[{'theme_id':'T'}],industry_chain_graph=[{'node_id':'N'}],
        local_candidates={'000001.SZ':dict(symbol='000001.SZ',data_quality_score=62,research_route='HALF_YEAR_FUNDAMENTAL')})
    args=dict(lane_id='lane_1',model=settings.research_models[0],stage='A1',
        snapshot=FrozenInputSnapshot('original-a1',data,as_of=NOW),upstream_output=None,upstream_symbols={'000001.SZ'},
        projection_symbols={'000001.SZ'},bundle=app.prompts.load(),run_id='team-a1-isolated',a1_discovery_context=context)
    if discovery:args.update(projection_symbols=set(),upstream_symbols=set(),
        a1_discovery_context=dict(mode='POLICY_MACRO_DISCOVERY',g0_symbol_count=500,
            authorized_discovery_source_refs=('invented-caller-ref',)))
    return app,args,seen


def test_actual_a1_stage_uses_authored_role_durable_journal_and_server_quality(tmp_path):
    app,args,seen=fixture(tmp_path);before=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1 and 'agent-1/3.0.5' in seen[0]['messages'][0]['content']
    assert '"t1_return":999' not in seen[0]['messages'][0]['content']
    assert '"status":"INSUFFICIENT"' in seen[0]['messages'][0]['content']
    row=result.output['monitor_pool'][0]
    assert row['data_quality_score']==62 and row['evidence_confidence']==.62
    assert result.output['envelope']['prompt_version']=='agent-1/3.0.5'
    with closing(app.team_research_adapter.journal._connect()) as db:
        stored=json.loads(db.execute("SELECT output_json FROM model_calls WHERE role='A1'").fetchone()[0])
        assert stored['monitor_pool'][0]['data_quality_score']==100
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()[0]==1
    again=app._run_stage(**args)
    assert again.output==result.output and len(seen)==1 and args['snapshot'].data==before


@pytest.mark.parametrize('kind',['empty-handoff','foreign-ref','wrong-envelope','extra-root','pool-overlap','unknown-theme'])
def test_invalid_model_or_old_hard_contract_never_becomes_active(tmp_path,kind):
    def mutate(raw):
        if kind=='empty-handoff':raw['handoff_notes']=[]
        if kind=='foreign-ref':raw['monitor_pool'][0]['supporting_refs']=['invented']
        if kind=='wrong-envelope':raw['envelope']['model_name']='other-model'
        if kind=='extra-root':raw['allow_trades']=True
        if kind=='pool-overlap':raw['rejected_candidates']=[deepcopy(raw['monitor_pool'][0])]
        if kind=='unknown-theme':raw['structural_themes']=[{'theme_id':'OUTSIDE','source_refs':['policy-ref']}]
    app,args,seen=fixture(tmp_path,mutate=mutate)
    result=app._run_stage(**args)
    assert result.status!='VALIDATED' and not result.symbols and len(seen)==1
    assert app.team_research_adapter.supports('A3')


def test_old_no_adapter_and_company_context_not_forwarded_are_not_silent_success(tmp_path):
    app,args,seen=fixture(tmp_path)
    args['a1_discovery_context']=None
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and not seen


def test_a1_parent_deadline_prevents_request_and_does_not_expand_on_restart(tmp_path):
    import time
    app,args,seen=fixture(tmp_path);app._deadline_started_monotonic=time.monotonic()-app.settings.research_close_deadline_seconds-1
    assert app._run_stage(**args).status=='BLOCKED' and not seen


def test_normalized_company_alias_cannot_lose_original_quality_binding(tmp_path):
    app,args,seen=fixture(tmp_path,mutate=lambda r:r['monitor_pool'][0].update(symbol='SZSE.000001'))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert result.output['monitor_pool'][0]['symbol']=='000001.SZ'
    assert result.output['monitor_pool'][0]['data_quality_score']==62


def test_new_model_high_quality_echo_does_not_bypass_existing_active_threshold(tmp_path):
    def promote(r):
        row=r['monitor_pool'].pop();row.update(status='ACTIVE',structural_score=100)
        r['active_research_pool']=[row]
    app,args,seen=fixture(tmp_path,mutate=promote);result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert result.output['active_research_pool']==[]
    assert 'A1_DATA_QUALITY_BELOW_MINIMUM' in result.output['monitor_pool'][0]['reason_codes']


def test_discovery_keeps_12_themes_40_nodes_original_allowlist_and_no_stocks(tmp_path):
    app,args,seen=fixture(tmp_path,discovery=True);result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(result.output['structural_themes'])==12 and len(result.output['industry_chain_graph'])==40
    assert not any(k in result.output for k in ('active_research_pool','monitor_pool','rejected_candidates'))
    assert 'invented-caller-ref' not in seen[0]['messages'][0]['content']
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()[0]==12
    assert len(seen)==1


def test_discovery_original_numeric_coverage_limits_are_in_the_model_input(tmp_path):
    from liangjian_funnel.pipeline.a1_contract import A1_CONTRACT_VERSION,A1_THEME_TARGET,A1_NODE_TARGET
    app,args,seen=fixture(tmp_path,discovery=True)
    args['a1_discovery_context']['monthly_coverage_contract']={'minimum_structural_themes':1}
    before=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args);assert result.status=='VALIDATED',result.reason_codes
    with closing(app.team_research_adapter.store._connect()) as db:
        body=json.loads(db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a1-research-source:%'").fetchone()[0])
    assert body['values']['A1_BATCH_CONTEXT']['monthly_coverage_contract']==dict(
        contract_version=A1_CONTRACT_VERSION,minimum_structural_themes=A1_THEME_TARGET[0],
        maximum_structural_themes=A1_THEME_TARGET[1],minimum_industry_chain_nodes=A1_NODE_TARGET[0],
        maximum_industry_chain_nodes=A1_NODE_TARGET[1])
    assert '"minimum_structural_themes":12' in seen[0]['messages'][0]['content']
    assert args['snapshot'].data==before and len(seen)==1


def test_original_discovery_source_gate_still_rejects_non_catalog_node_refs(tmp_path):
    def invalid(raw):
        if raw.get('industry_chain_graph'):raw['industry_chain_graph'][0]['source_refs']=['invented-caller-ref']
    app,args,seen=fixture(tmp_path,discovery=True,mutate=invalid);result=app._run_stage(**args)
    assert result.status!='VALIDATED' and not result.symbols and len(seen)==1
    assert 'A1_DISCOVERY_NODE_EVIDENCE_INVALID' in str(result.diagnostics)


def test_capture_then_local_discovery_audit_failure_resumes_without_a_model_recall(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline.research import common
    app,args,seen=fixture(tmp_path,discovery=True);original=common.atomic_write_json
    def fail(*args,**kwargs):raise OSError('UNIT_TEST_DISK_FAILURE')
    monkeypatch.setattr(common,'atomic_write_json',fail)
    first=app._run_stage(**args)
    assert first.reason_codes==('A1_DISCOVERY_AUDIT_WRITE_FAILED',) and len(seen)==1
    monkeypatch.setattr(common,'atomic_write_json',original)
    assert app._run_stage(**args).status=='VALIDATED' and len(seen)==1


def test_captured_result_can_resume_inside_a_smaller_parent_remaining_budget(tmp_path):
    import time
    app,args,seen=fixture(tmp_path)
    first=app._run_stage(**args);assert first.status=='VALIDATED'
    app._deadline_started_monotonic=time.monotonic()-app.settings.research_close_deadline_seconds+10
    assert app._run_stage(**args).output==first.output and len(seen)==1


@pytest.mark.parametrize('change',['missing-catalysts','bad-date','missing-falsifier','sentinel-one-category'])
def test_authored_a1_new_fields_are_not_optional(tmp_path,change):
    def mutate(raw):
        if not raw.get('structural_themes'):return
        theme=raw['structural_themes'][0]
        if change=='missing-catalysts':theme['catalysts']=[]
        if change=='bad-date':theme['catalysts'][0]['expected_date']='2026-02-30'
        if change=='missing-falsifier':theme['falsifiers']=[]
        if change=='sentinel-one-category':
            theme.update(opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND'],opposing_search_scope=['RESEARCH_PACKET'])
    app,args,seen=fixture(tmp_path,discovery=True,mutate=mutate)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and not result.symbols and len(seen)==1
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute('SELECT validated FROM model_calls').fetchone()[0]==0
