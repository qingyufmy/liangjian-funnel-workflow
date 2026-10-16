from copy import deepcopy
from decimal import Decimal as D
from pathlib import Path

import pytest
import yaml


def fixture():
    config=yaml.safe_load((Path(__file__).resolve().parents[1]/'config/funnel_config_v2.yaml').read_text(encoding='utf-8'))['agent_2']
    contexts={str(i):dict(theme_id='T1',a2_factor_scores={name:dict(available=True,score=value,
        source='ORIGINAL',source_refs=['row:'+str(i)]) for name in config['score_weights']})
        for i,value in enumerate((10,40,100))}
    return config,dict(A2_BOTTLENECK_CONTEXT=contexts,CAPITAL_FLOW_SNAPSHOT={'available':True}),set(contexts)


def freeze(config,data,symbols):
    from liangjian_funnel.pipeline.team_v2_a2_score import freeze_theme_scores
    return freeze_theme_scores(data,review_symbols=symbols,weights=config['score_weights'],
        policy=config['team_theme_score_policy'],source_snapshot_sha256='a'*64)


def test_full_quant_domain_mean_not_model_focus_and_original_field_bindings():
    config,data,symbols=fixture();original=deepcopy(data);body=freeze(config,data,symbols)
    theme=body['themes']['T1']
    assert theme['member_symbols']==['0','1','2'] and D(theme['base_score'])==50
    assert theme['factors']['breadth']['effective_member_count']==3
    assert theme['factors']['breadth']['members'][0]['source_refs']==['row:0']
    assert theme['factors']['breadth']['field_refs'][0]=='A2_BOTTLENECK_CONTEXT:0.a2_factor_scores.breadth'
    assert data==original and len(body['score_snapshot_sha256'])==64
    # Model-selected FOCUS is not an accepted argument and cannot alter this freeze.
    assert body==freeze(config,data,{'2','1','0'})


@pytest.mark.parametrize('missing,expected',[
    (['leader_structure'],'SCORE_RENORMALIZED_MINOR'),
    (['leader_structure','breadth'],'SCORE_DATA_LIMITED'),
    (['catalyst_freshness','weekly_confirmation'],'SCORE_RENORMALIZED_MINOR')])
def test_minor_missing_original_weight_boundary_and_no_zero_fill(missing,expected):
    config,data,symbols=fixture()
    for row in data['A2_BOTTLENECK_CONTEXT'].values():
        for name in missing:row['a2_factor_scores'][name]['available']=False
    theme=freeze(config,data,symbols)['themes']['T1']
    assert theme['status']==expected and theme['missing_factors']==sorted(missing)
    assert all(theme['factors'][name]['mean_score'] is None for name in missing)
    if expected=='SCORE_DATA_LIMITED':assert theme['base_score'] is None and theme['focus_policy']=='WATCH_ONLY'
    else:assert D(theme['base_score'])==50


def test_fewer_than_three_valid_members_is_theme_missing_not_average_of_two():
    config,data,symbols=fixture();data['A2_BOTTLENECK_CONTEXT']['2']['a2_factor_scores']['breadth']['score']=101
    row=freeze(config,data,symbols)['themes']['T1']['factors']['breadth']
    assert row['effective_member_count']==2 and row['mean_score'] is None


@pytest.mark.parametrize('value',[True,None,'NaN','Infinity',-1,101])
def test_invalid_available_value_does_not_count(value):
    config,data,symbols=fixture();data['A2_BOTTLENECK_CONTEXT']['0']['a2_factor_scores']['breadth']['score']=value
    assert freeze(config,data,symbols)['themes']['T1']['factors']['breadth']['effective_member_count']==2


def test_global_capital_unavailable_is_legacy_removal_not_minor_missing_weight():
    config,data,symbols=fixture();data['CAPITAL_FLOW_SNAPSHOT']['available']=False
    for row in data['A2_BOTTLENECK_CONTEXT'].values():row['a2_factor_scores']['leader_structure']['available']=False
    theme=freeze(config,data,symbols)['themes']['T1']
    assert theme['globally_removed_factors']==['capital_flow']
    assert D(theme['missing_weight'])==D('.15') and D(theme['available_weight'])==D('.73')
    assert theme['status']=='SCORE_RENORMALIZED_MINOR' and D(theme['base_score'])==50


def test_source_or_membership_changes_are_visible_in_frozen_hash():
    config,data,symbols=fixture();old=freeze(config,data,symbols)
    data['A2_BOTTLENECK_CONTEXT']['0']['a2_factor_scores']['breadth']['source_refs']=['other']
    assert old['score_snapshot_sha256']!=freeze(config,data,symbols)['score_snapshot_sha256']
    assert old['score_snapshot_sha256']!=freeze(config,data,{'0','1'})['score_snapshot_sha256']


def test_unknown_missing_theme_and_invalid_policy_fail_before_model():
    config,data,symbols=fixture()
    with pytest.raises(ValueError,match='DOMAIN'):freeze(config,data,symbols|{'outside'})
    data['A2_BOTTLENECK_CONTEXT']['0']['theme_id']=None
    with pytest.raises(ValueError,match='THEME'):freeze(config,data,symbols)
    config['team_theme_score_policy']['minimum_valid_members']=1
    with pytest.raises(ValueError,match='POLICY'):freeze(config,data,{'1','2'})


def test_0267_fixed_penalties_once_cap_and_original_rule_binding():
    from liangjian_funnel.pipeline.team_v2_a2_score import score_with_penalties
    config,data,symbols=fixture();body=freeze(config,data,symbols)
    rules=config['penalty_rules']
    result=score_with_penalties(body,'T1',['high_crowding','high_crowding'],rules)
    assert D(result['theme_score'])==38 and result['applied_penalty_codes']==['high_crowding']
    assert result['penalty_mode']=='MIDPOINT_FIXED_V1' and result['unresolved_penalty_codes']==[]
    result=score_with_penalties(body,'T1',['high_crowding','news_hot_capital_out','volume_up_return_down'],rules)
    assert D(result['raw_penalty_points'])==-34 and D(result['penalty_points'])==-30
    assert D(result['theme_score'])==20
    assert result['original_penalty_rules']==rules
    with pytest.raises(ValueError,match='UNKNOWN'):score_with_penalties(body,'T1',['outside'],rules)
    body['themes']['T1']['base_score']='100'
    with pytest.raises(ValueError,match='HASH'):score_with_penalties(body,'T1',[],{})


@pytest.mark.parametrize('missing,limited',[(('capital_flow',),False),(('capital_flow','breadth'),True)])
def test_0267_local_capital_missing_counts_minor_weight(missing,limited):
    config,data,symbols=fixture()
    for row in data['A2_BOTTLENECK_CONTEXT'].values():
        for name in missing:row['a2_factor_scores'][name]['available']=False
    theme=freeze(config,data,symbols)['themes']['T1']
    assert theme['globally_removed_factors']==[]
    assert D(theme['missing_weight'])==sum(D(str(config['score_weights'][n])) for n in missing)
    assert (theme['status']=='SCORE_DATA_LIMITED')==limited


def test_0267_fixed_points_and_cap_are_frozen_and_not_model_options():
    config,data,symbols=fixture()
    assert config['team_theme_score_policy']['penalty_points']==dict(high_crowding=-12,
        single_limit_up_driven=-10,laggard_spread_core_weak=-12,news_hot_capital_out=-10,
        volume_up_return_down=-12,insufficient_capacity=-10)
    config['team_theme_score_policy']['penalty_points']['high_crowding']=-5
    with pytest.raises(ValueError,match='POLICY'):freeze(config,data,symbols)
