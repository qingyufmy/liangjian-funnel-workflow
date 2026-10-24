from copy import deepcopy
import pytest
from liangjian_funnel.pipeline.a3_attention_research import evaluate_attention_a3_domain
from liangjian_funnel.pipeline.feature_store import content_hash
from test_a3_attention_research_domain import inputs,build,LEAD,NOW

def technical(tmp_path):
    from test_a3_theme_stage_handoff import _inputs
    args,audit=inputs(tmp_path);domain=build(args,audit)
    base,_=_inputs();old='001366.SZ'
    data=deepcopy(dict(args['snapshot'].data))
    for key,value in base.items():
        data[key]={LEAD:deepcopy(value[old])} if isinstance(value,dict) and old in value else deepcopy(value)
    data['g0_candidates']=[dict(symbol=LEAD,amount=100000000)]
    data['FACTOR_SNAPSHOT'][LEAD]['a3_ready']=True
    for frame in data['FACTOR_SNAPSHOT'][LEAD]['timeframes'].values():frame['latest']={'close':10.5}
    data['FACTOR_SNAPSHOT'][LEAD]['timeframes']['daily'].update(ma_alignment='BULL_STACK',ma_event='NONE',ma_bias={})
    data['PRICE_LEVELS'][LEAD]['available']=True
    data['TIER_STRUCTURE_SNAPSHOT']={LEAD:dict(available=True,score=100,ladder_height=3,
        source='HITHINK_LIMIT_UP_LADDER',source_refs=['fixture:ladder'])}
    # The actual A2 fixture returns LATENT. Positive setup explicitly has a
    # separately hashed same-day reviewed stage; no live source is implied.
    case=domain['records'][0];case['theme_review'].update(stage='ACCELERATION',
        supporting_evidence=['fixture:support'],contradicting_evidence=['fixture:risk'],source_refs=['fixture:A2'])
    case['case_id']=content_hash({k:v for k,v in case.items() if k!='case_id'})
    domain['content_hash']=content_hash({k:v for k,v in domain.items() if k!='content_hash'})
    return data,args['upstream_output'],domain

def test_outside_monitor_runs_original_technical_checks_without_formal_promotion(tmp_path):
    data,a1,domain=technical(tmp_path);before=deepcopy((data,a1,domain))
    result=evaluate_attention_a3_domain(data,a1,domain);row=result['records'][0]
    assert row['technical']['status']=='REVIEW_CANDIDATE',row['technical']['reason_codes']
    assert row['behavior']['stock_behavior_type']=='EMOTION'
    assert row['sent_to_llm'] is False and result['first_pass_model_requests']==1
    assert row['execution_permission']=='BLOCKED' and row['risk_unit']=='NO_ENTRY'
    assert row['technical']['fundamental_admission_passed'] is False
    assert (data,a1,domain)==before

@pytest.mark.parametrize('defect',['missing_factor','distribution','retreat','first_board','untradable','missing_ladder','unavailable_price'])
def test_original_negative_technical_and_evidence_gates_are_retained(tmp_path,defect):
    data,a1,domain=technical(tmp_path)
    if defect=='missing_factor':data['FACTOR_SNAPSHOT']={}
    if defect=='distribution':data['KLINE_PATTERNS'][LEAD]={'labels':['HIGH_VOLUME_DISTRIBUTION']}
    if defect=='untradable':data['TRADABILITY_FLAGS'][LEAD]['tradable']=False
    if defect=='missing_ladder':data['TIER_STRUCTURE_SNAPSHOT']={}
    if defect=='unavailable_price':data['PRICE_LEVELS'][LEAD]['available']=False
    if defect=='first_board':data['TIER_STRUCTURE_SNAPSHOT'][LEAD].update(ladder_height=1,first_board_observed=True,event_source='HITHINK_LIMIT_UP_POOL_FIRST_BOARD')
    if defect=='retreat':
        case=domain['records'][0];case['theme_review']['stage']='RETREAT'
        case['case_id']=content_hash({k:v for k,v in case.items() if k!='case_id'})
        domain['content_hash']=content_hash({k:v for k,v in domain.items() if k!='content_hash'})
    result=evaluate_attention_a3_domain(data,a1,domain);row=result['records'][0]
    assert result['first_pass_model_requests']==0,(defect,row['technical'])
    assert row['model_state']=='NOT_MODEL_REVIEWED_LOCAL_GATE'
    assert row['technical']['reason_codes'] or row['technical']['unmet_conditions'] or row['technical']['veto_conditions']
    assert row['execution_permission']=='BLOCKED'

def test_hot_rank_does_not_manufacture_behavior_role_without_ladder_or_trend_proof(tmp_path):
    data,a1,domain=technical(tmp_path);data['TIER_STRUCTURE_SNAPSHOT']={}
    result=evaluate_attention_a3_domain(data,a1,domain)
    assert result['records'][0]['behavior']['stock_behavior_type']=='UNRESOLVED'
    assert result['first_pass_model_requests']==0

def test_legacy_technical_bypass_is_not_available_to_new_research_branch(tmp_path):
    data,a1,domain=technical(tmp_path);data.pop('DETERMINISTIC_RESEARCH_V2_ENABLED')
    with pytest.raises(ValueError,match='ORDINARY_V2_TECHNICAL_REQUIRED'):
        evaluate_attention_a3_domain(data,a1,domain)


def test_real_by_symbol_factor_envelope_matches_flat_projection_without_losing_trend_evidence(tmp_path):
    data,a1,domain=technical(tmp_path)
    factor=dict(relative_strength_score=90,factors={
        'stock_trend_structure':dict(available=True,score=100,structure_confirmed=True),
        'index_chain_resonance':dict(available=True,score=75)})
    data['A2_FACTOR_SNAPSHOT']={LEAD:factor}
    flat=evaluate_attention_a3_domain(data,a1,domain)['records'][0]['behavior']
    data['A2_FACTOR_SNAPSHOT']={'schema_version':'a2-features/3.3.0','by_symbol':{LEAD:factor}}
    nested=evaluate_attention_a3_domain(data,a1,domain)['records'][0]['behavior']
    assert nested==flat
    assert nested['research_route_qualifications']['TREND_MA5']['eligible'] is True
