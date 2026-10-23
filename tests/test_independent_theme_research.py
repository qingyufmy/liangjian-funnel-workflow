from copy import deepcopy
from dataclasses import replace
import json

import pytest

from liangjian_funnel.pipeline.a2_theme_research import (
    theme_hypotheses, add_unscored_theme_hypotheses, independent_theme_reviews,
)
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.research import _project_a2_bottleneck_context
from test_team_v2_a2_research import fixture


def binding(symbol, *themes):
    rows = [dict(theme_id=theme, node_id='node:'+theme, taxonomy='CONCEPT',
        taxonomy_code='CODE:'+theme, match_method='MATURE_THEME_REGISTRY_EXACT_NAME') for theme in themes]
    return dict(schema_version='emotion-theme-binding/1.1.0', research_resolved=bool(rows),
        resolved=len(rows)==1, requires_theme_selection=len(rows)>1,
        source_hash=content_hash(dict(symbol=symbol, matches=rows)), matches=rows,
        candidate_routes=[dict(theme_id=r['theme_id'], node_id=r['node_id'], evidence=[r]) for r in rows])


def attach(args, symbol='000001.SZ'):
    context=args['snapshot'].data['A2_BOTTLENECK_CONTEXT'][symbol]
    context.update(emotion_core_eligible=True, stock_behavior_type='EMOTION',
        decision_id='test-decision-000001',as_of='2026-10-16T16:30:00+08:00',
        execution_permission='BLOCKED', research_only_reason='A2_EMOTION_THEME_SELECTION_REQUIRED',
        route_permission=['LEADER_INTRADAY'],
        theme_research_binding=binding(symbol, 'T', 'ROBOTICS_ADVANCED_MANUFACTURING'))
    return context


def test_non_primary_robotics_is_mandatory_theme_review_not_new_rotation_direction(tmp_path):
    app,args,_=fixture(tmp_path);attach(args)
    context=args['snapshot'].data['A2_BOTTLENECK_CONTEXT']
    projected=_project_a2_bottleneck_context(context,set(context))
    assert projected['_theme_review_scope']['ROBOTICS_ADVANCED_MANUFACTURING']==['000001.SZ']
    assert projected['_theme_review_scope']['T']==sorted(context)
    assert projected['_rotation_review_scope']=={}
    assert context['000001.SZ']['theme_id']=='T'
    assert projected['_independent_theme_research_contract']['execution_authority'] is False


@pytest.mark.parametrize('defect',['hash','foreign_evidence','outside_symbol','hard_risk','not_reviewable'])
def test_hypothesis_requires_original_symbol_membership_and_eligibility(tmp_path,defect):
    app,args,_=fixture(tmp_path);row=attach(args)
    if defect=='hash':row['theme_research_binding']['source_hash']='f'*64
    if defect=='foreign_evidence':row['theme_research_binding']['candidate_routes'][0]['evidence'][0]={'theme_id':'FORGED'}
    if defect=='outside_symbol':row['theme_research_binding']=binding('000002.SZ','T')
    if defect=='hard_risk':row['hard_risk_events']=[{'kind':'HARD'}]
    if defect=='not_reviewable':row['deterministic_status']='DATA_GAP'
    hypotheses=theme_hypotheses({'000001.SZ':row},{'000001.SZ'})
    if defect=='foreign_evidence':
        assert all(h['theme_id']!='T' for h in hypotheses.get('000001.SZ',[]))
    else:assert not hypotheses


def test_added_theme_never_borrows_primary_score_and_freeze_revalidates(tmp_path):
    app,args,_=fixture(tmp_path)
    original=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols']).data['_TEAM_A2_SCORE_FREEZE']
    attach(args)
    prepared=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    frozen=prepared.data['_TEAM_A2_SCORE_FREEZE']
    assert frozen['themes']['T']==original['themes']['T']
    robot=frozen['themes']['ROBOTICS_ADVANCED_MANUFACTURING']
    assert robot['base_score'] is None and robot['status']=='SCORE_DATA_LIMITED'
    assert robot['member_symbols']==[] and robot['research_member_symbols']==['000001.SZ']
    assert all(r['mean_score'] is None for r in robot['factors'].values())
    assert app.team_research_adapter.prepare_a2_snapshot(prepared,args['upstream_symbols']).data['_TEAM_A2_SCORE_FREEZE']==frozen


def test_real_adapter_one_mock_response_reviews_both_themes_and_keeps_permission(tmp_path):
    def extra(raw,_):
        robot=deepcopy(raw['theme_reviews'][0]);robot.update(theme_id='ROBOTICS_ADVANCED_MANUFACTURING',
            stage='IGNITION',new_entry_policy='PROBE_ONLY')
        raw['theme_reviews'].append(robot)
    app,args,seen=fixture(tmp_path,extra);attach(args)
    args['snapshot']=replace(args['snapshot'],data={**args['snapshot'].data,
        'A2_MARKET_REFERENCE':{'market_trade_date':'2026-10-16'}})
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1
    themes={r['theme_id']:r for r in result.output['active_themes']}
    assert themes['ROBOTICS_ADVANCED_MANUFACTURING']['stage']=='IGNITION'
    assert themes['ROBOTICS_ADVANCED_MANUFACTURING']['theme_score'] is None
    assert themes['ROBOTICS_ADVANCED_MANUFACTURING']['stage_evidence_hash']
    reviews=result.output['independent_theme_reviews']
    assert {r['theme_id'] for r in reviews}=={'T','ROBOTICS_ADVANCED_MANUFACTURING'}
    row=next(r for p in ('focus_pool','watch_only_pool') for r in result.output[p] if r['symbol']=='000001.SZ')
    assert row['theme_id']=='T' and row['execution_permission']=='BLOCKED'


def test_missing_new_theme_review_blocks_instead_of_silently_omitting_robotics(tmp_path):
    app,args,seen=fixture(tmp_path);attach(args)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED'
    assert any('THEME_COVERAGE' in reason for reason in result.reason_codes)
    assert len(seen)<=3


def technical_inputs():
    from test_a3_theme_stage_handoff import _inputs
    snapshot,output=_inputs()
    symbol=output['focus_pool'][0]['symbol']
    context=dict(theme_id='AGRICULTURE',deterministic_status='REVIEW_CANDIDATE',
        decision_id='test-decision-001366',as_of='2026-09-28T15:10:00+08:00',
        emotion_core_eligible=True,stock_behavior_type='EMOTION',route_permission=['LEADER_INTRADAY'],
        execution_permission='BLOCKED',research_only_reason='A2_EMOTION_THEME_SELECTION_REQUIRED',
        theme_research_binding=binding(symbol,'AGRICULTURE','ROBOTICS_ADVANCED_MANUFACTURING'))
    snapshot['A2_BOTTLENECK_CONTEXT']={symbol:context}
    snapshot['A2_MARKET_REFERENCE']={'market_trade_date':'2026-09-28'}
    output['active_themes'][0]['stage']='RETREAT'
    robot={**output['active_themes'][0],'theme_id':'ROBOTICS_ADVANCED_MANUFACTURING','stage':'ACCELERATION'}
    output['active_themes'].append(robot)
    for theme in output['active_themes']:
        theme['stage_observed_as_of']='2026-09-28'
        theme['stage_evidence_hash']=content_hash(theme)
    output['independent_theme_reviews']=independent_theme_reviews(output,{symbol:context},{symbol})
    return snapshot,output,symbol


def test_a3_reviews_robot_theme_independently_and_never_changes_monthly_identity():
    from liangjian_funnel.pipeline.deterministic import screen_a3
    snapshot,output,symbol=technical_inputs()
    original=deepcopy(output)
    decision=screen_a3(snapshot,output).decisions[0]
    assert decision['status']=='REVIEW_CANDIDATE',decision['reason_codes']
    assert decision['eligibility']=='QUALIFIED'
    assert decision['theme_id'] is None  # the fixture owns primary_theme, no fabricated alias
    assert decision['execution_permission']=='BLOCKED'
    assert decision['research_only_reason']=='A3_INDEPENDENT_THEME_RESEARCH_ONLY'
    selection=decision['strategy_facts']['independent_theme_research_selection']
    assert selection['theme_id']=='ROBOTICS_ADVANCED_MANUFACTURING'
    assert selection['monthly_primary_theme_id']=='AGRICULTURE'
    checks={r['theme_id']:r for r in decision['strategy_facts']['independent_theme_research_checks']}
    assert checks['AGRICULTURE']['eligibility']=='REJECTED'
    assert checks['ROBOTICS_ADVANCED_MANUFACTURING']['eligibility']=='QUALIFIED'
    assert output==original


@pytest.mark.parametrize('defect',['not_tradable','theme_retreat','missing_review','conflicting_review','foreign_hypothesis',
    'stale_review','different_decision'])
def test_alternative_theme_research_keeps_technical_and_identity_gates(defect):
    from liangjian_funnel.pipeline.deterministic import screen_a3
    snapshot,output,symbol=technical_inputs()
    if defect=='not_tradable':snapshot['TRADABILITY_FLAGS'][symbol]['tradable']=False
    elif defect=='theme_retreat':
        output['active_themes'][1]['stage']='RETREAT'
        output['independent_theme_reviews']=independent_theme_reviews(output,snapshot['A2_BOTTLENECK_CONTEXT'],{symbol})
    elif defect=='missing_review':output['independent_theme_reviews']=[]
    elif defect=='conflicting_review':
        other=deepcopy(output['independent_theme_reviews'][1]);other['review']['stage']='RETREAT'
        other['review_id']=content_hash({k:v for k,v in other.items() if k!='review_id'})
        output['independent_theme_reviews'].append(other)
    elif defect=='stale_review':
        for review in output['independent_theme_reviews']:
            review['review']['stage_observed_as_of']='2026-09-25'
            review['review_id']=content_hash({k:v for k,v in review.items() if k!='review_id'})
    elif defect=='different_decision':
        snapshot['A2_BOTTLENECK_CONTEXT'][symbol]['decision_id']='a-different-fact-decision'
    else:
        for review in output['independent_theme_reviews']:
            review['hypothesis_ids']=['f'*64]
            review['review_id']=content_hash({k:v for k,v in review.items() if k!='review_id'})
    decision=screen_a3(snapshot,output).decisions[0]
    assert decision['status']!='REVIEW_CANDIDATE'
    assert not decision['sent_to_llm']


def test_model_cannot_launder_independent_research_selection_into_entry():
    from liangjian_funnel.pipeline.deterministic import screen_a3
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot, _with_a3_deterministic_context
    from liangjian_funnel.pipeline.research.a3 import _apply_a3_candidate_origin_policy
    snapshot,output,symbol=technical_inputs()
    snapshot['A3_CANDIDATE_ORIGIN']={symbol:'WATCH_ONLY'}
    frozen=_with_a3_deterministic_context(FrozenInputSnapshot('technical-test',snapshot),screen_a3(snapshot,output))
    response={'core_watch_pool':[{'symbol':symbol,'strategy_profile':'LEADER_INTRADAY',
        'execution_permission':'REQUIRES_A3_A4_CONFIRMATION','risk_unit':'FULL'}], 'secondary_watch_pool':[]}
    result,_=_apply_a3_candidate_origin_policy(response,frozen.data)
    row=result['core_watch_pool'][0]
    assert row['execution_permission']=='BLOCKED'
    assert row['research_only_reason']=='A3_INDEPENDENT_THEME_RESEARCH_ONLY'
    assert row['risk_unit']=='PROBE'
    assert row['research_observation_scope']=='RESEARCH_ONLY_NO_AUTOMATIC_ENTRY'


@pytest.mark.parametrize('flag',['reason','technical_evidence'])
def test_actual_publisher_cannot_reuse_monthly_permission_for_alternative_theme(tmp_path,flag):
    from types import SimpleNamespace
    from datetime import datetime
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    symbol='001366.SZ'
    raw={'symbol':symbol,'strategy_profile':'TREND_MA5','execution_permission':'BLOCKED'}
    if flag=='reason':raw['research_only_reason']='A3_INDEPENDENT_THEME_RESEARCH_ONLY'
    else:raw['deterministic_technical_evidence']={'independent_theme_research_selection':{'theme_id':'ROBOT'}}
    output={'core_watch_pool':[raw],'secondary_watch_pool':[]}
    audit=SimpleNamespace(stage='A2',output={'focus_pool':[{'symbol':symbol,
        'execution_permission':'REQUIRES_A3_A4_CONFIRMATION'}]})
    lane=SimpleNamespace(lane='lane_1',status='READY',final_output=output,stages=(audit,),audit_path=None)
    stored=[]
    def publish(rows,**kwargs):
        stored.extend(rows)
        return []
    app=SimpleNamespace(store=SimpleNamespace(list_execution_plans=lambda **kw:[],
        publish_plan_batch=publish,mark_workflow_runs_published=lambda *a:None),
        settings=SimpleNamespace(workflow_output_dir=tmp_path),trading_calendar=ExchangeTradingCalendar())
    result=WorkflowApplication._publish_plans(app,SimpleNamespace(run_id='local-research-only',lanes=(lane,)),
        'close',datetime.fromisoformat('2026-10-23T16:00:00+08:00'),snapshot_data={})
    assert stored==[] and result['created']==[] and result['activated']==[]
    assert result['blocked']==[{'lane':'lane_1','symbol':symbol,'reason':'A3_INDEPENDENT_THEME_RESEARCH_ONLY'}]
