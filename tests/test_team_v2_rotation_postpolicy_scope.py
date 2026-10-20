"""0387: raw focus labels cannot erase a required final-policy explanation."""
from copy import deepcopy
import json
from pathlib import Path
import pytest
from liangjian_funnel.pipeline.team_v2_rotation_scope import rotation_coverage
from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
from liangjian_funnel.pipeline.team_v2_a2_recovery import drop_focused_rotation,validation_feedback
from test_team_v2_a2_research import fixture
from test_team_v2_a2_recovery import no_focus
from test_team_v2_a2_schema import packet,validate,CATALOG,ENVELOPE,REFS


def test_only_final_focus_intersection_covers_direction_never_same_label():
    directions={'AI_COMPUTE':['300556.SZ','300839.SZ','600406.SH','601012.SH']}
    value=rotation_coverage({'603068.SH'},directions)
    assert value['covered_by_final_focus']=={}
    assert value['required_no_focus']=={'AI_COMPUTE':['300556.SZ','300839.SZ','600406.SH','601012.SH']}


def test_second_provider_no_focus_survives_when_policy_demotes_original_raw_focus(tmp_path):
    def mutate(raw,attempt):
        if attempt==2:raw['rotation_reviews']=[no_focus()]
    app,args,seen=fixture(tmp_path,mutate,rotation=True)
    for i,row in enumerate(args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values()):
        row['deterministic_score']=90-i
        for key in ('leader_structure','breadth'):row['a2_factor_scores'][key]['available']=False
    before=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==2 and result.output['focus_pool']==[]
    assert result.output['rotation_reviews'][0]['reason_codes']==['LOW_CONVICTION']
    assert result.diagnostics['server_normalization'] is None
    assert result.diagnostics['provider_validation_history']==[['A2_ROTATION_COVERAGE_MISMATCH'],[]]
    feedback=json.loads(seen[1]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))['validation_feedback']
    assert feedback['coverage_domain']['covered_by_final_focus']=={}
    assert set(feedback['required_no_focus_if_focus_unchanged']['D'])=={'000001.SZ','000002.SZ','000003.SZ'}
    assert feedback['provider_raw_focus_symbols']==['000001.SZ'] and feedback['unchanged_focus_symbols']==[]
    assert dict(args['snapshot'].data)==before


def test_schema_uses_final_policy_focus_but_keeps_all_other_raw_errors():
    raw=packet();raw['rotation_reviews']=[{**no_focus('D',['000001.SZ']),**REFS}]
    kwargs=dict(direction_symbols={'D':{'000001.SZ'}},effective_focus_symbols=set())
    result=validate(raw,**kwargs)
    assert result.valid,result.issues
    assert 'A2_ROTATION_DOMAIN_MISMATCH' not in result.issues
    assert 'A2_ROTATION_COVERAGE_MISMATCH' not in result.issues
    raw['rotation_reviews'][0]['reviewed_symbols']=['FOREIGN']
    assert 'A2_ROTATION_REVIEW_INVALID' in validate(raw,**kwargs).issues


@pytest.mark.parametrize('attempt',[0,1,2])
def test_ecc_actual_three_rounds_share_final_policy_scope_and_keep_provider_raw(attempt):
    frozen=json.loads((Path(__file__).parent/'fixtures/team_v2_a2_ecc_three_rounds_20261020.json').read_text(encoding='utf-8'))
    case=frozen['cases'][attempt];raw=deepcopy(case['provider_raw']);before=deepcopy(raw)
    directions=frozen['direction_members'];final=set(case['final_focus_symbols'])
    coverage=rotation_coverage(final,directions)
    assert set(coverage['required_no_focus'])=={'SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE','SELECTED_BOARD:INNOVATIVE_MEDICINE_HEALTHCARE'}
    assert '603068.SH' not in coverage['covered_by_final_focus'].get('SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE',[])
    projection=drop_focused_rotation(raw,frozen['catalog'],directions,effective_focus_symbols=final)
    effective=raw if projection is None else projection[0]
    if attempt==1:
        assert len(effective['rotation_reviews'])==2
        assert effective['rotation_reviews']==raw['rotation_reviews']
    domain=frozen['symbol_themes'];themes=set(domain.values())
    nominations={r['id'].removeprefix('A2:NOMINATION:') for r in frozen['catalog']['items'] if r['id'].startswith('A2:NOMINATION:')}
    checked=validate_a2_response(effective,frozen['catalog'],required_envelope=raw['envelope'],
        candidate_ids=set(domain),symbol_themes=domain,allowed_theme_ids=themes,direction_symbols=directions,
        nomination_scope=nominations,effective_focus_symbols=final,engine_emotion_cycle='LATENT')
    if attempt==1:
        assert not any(code.startswith('A2_ROTATION_') for code in checked.issues),checked.issues
        assert any('REFS_INVALID' in code for code in checked.issues),checked.issues
    else:assert 'A2_ROTATION_COVERAGE_MISMATCH' in checked.issues
    feedback=validation_feedback(effective,frozen['catalog'],checked.issues,envelope=raw['envelope'],
        symbols=set(domain),themes=themes,directions=directions,nomination_scope=nominations,
        engine_emotion_cycle='LATENT',effective_focus_symbols=final)
    assert feedback['required_no_focus_if_focus_unchanged']==coverage['required_no_focus']
    assert raw==before==case['provider_raw']
