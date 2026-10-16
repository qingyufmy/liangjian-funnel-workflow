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


def test_penalty_original_scalar_once_unknown_invalid_range_not_guessed():
    from liangjian_funnel.pipeline.team_v2_a2_score import score_with_penalties
    config,data,symbols=fixture();body=freeze(config,data,symbols)
    result=score_with_penalties(body,'T1',['one','one'],{'one':-5})
    assert D(result['theme_score'])==45 and result['applied_penalty_codes']==['one']
    with pytest.raises(ValueError,match='UNKNOWN'):score_with_penalties(body,'T1',['outside'],{'one':-5})
    result=score_with_penalties(body,'T1',['range'],{'range':[-20,-5]})
    assert result['theme_score'] is None and result['unresolved_penalty_codes']==['range']
    assert result['status']=='SCORE_DATA_LIMITED' and result['focus_policy']=='WATCH_ONLY'
    body['themes']['T1']['base_score']='100'
    with pytest.raises(ValueError,match='HASH'):score_with_penalties(body,'T1',[],{})
