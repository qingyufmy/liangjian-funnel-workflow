"""0384 P0-8: final explanatory coverage only, no focus/trading repair."""
from copy import deepcopy
from contextlib import closing
from pathlib import Path
import hashlib,json
import pytest

from liangjian_funnel.pipeline.team_v2_a2_normalization import synthesize_final_no_focus
from liangjian_funnel.pipeline.research import common as legacy
from test_team_v2_a2_research import fixture


def test_full_stage_final_postpolicy_coverage_synthesis_preserves_provider_and_focus(tmp_path):
    app,args,seen=fixture(tmp_path,rotation=True)
    for i,row in enumerate(args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values()):
        row['deterministic_score']=90-i
        for key in ('leader_structure','breadth'):row['a2_factor_scores'][key]['available']=False
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==3 and result.output['focus_pool']==[]
    assert len(result.output['rotation_reviews'])==1
    assert result.output['rotation_reviews'][0]['reviewed_symbols']==['000001.SZ','000002.SZ','000003.SZ']
    assert result.output['rotation_reviews'][0]['reason_codes']==['SERVER_SYNTHESIZED_NO_FOCUS']
    assert '不代表模型判断' in result.output['rotation_reviews'][0]['explanation']
    with closing(app.team_research_adapter.journal._connect()) as db:
        raw=[json.loads(r[0]) for r in db.execute('SELECT output_json FROM model_calls ORDER BY rowid')]
    assert len(raw)==3 and all(r['rotation_reviews']==[] for r in raw)
    assert all(r['focus_decisions'][0]['symbol']=='000001.SZ' for r in raw)
    audit=result.diagnostics['server_normalization']
    assert audit['authority']=='0384_REPLY_P0_8_FINAL_COVERAGE_ONLY'
    assert audit['original_issues']==['A2_ROTATION_FOCUS_COVERAGE_MISSING:D']
    with closing(app.team_research_adapter.store._connect()) as db:
        frozen=json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            (audit['audit_context_key'],)).fetchone()[0])
        before=json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            (audit['original_postpolicy_context_key'],)).fetchone()[0])
    assert before['output']['rotation_reviews']==[] and before['reason_codes']==audit['original_issues']
    assert before['output']['focus_pool']==result.output['focus_pool']
    assert frozen['projected_postpolicy_output']==result.output
    assert app._run_stage(**args).output==result.output and len(seen)==3


def real_case():
    frozen=json.loads((Path(__file__).parent/'fixtures/team_v2_a2_natural_coverage_20261020.json').read_text(encoding='utf-8'))
    contexts={r['symbol']:dict(rotation_direction_id=r['direction'],deterministic_status=r['deterministic_status'],
        trend_core_eligible=bool(r['trend_eligible']),top_rotation_theme=bool(r['top_rotation_theme']),
        deterministic_score=r['quant_score'],selected_board={'selected_for_rotation':bool(r['selected_for_rotation'])}) for r in frozen['engine_rows']}
    data={'A2_BOTTLENECK_CONTEXT':contexts};symbols=set(contexts)
    validate=lambda output:legacy._validate_a2_rotation_focus_coverage(output,data,symbols)
    return frozen,validate


def test_real_ecc_original_red_final_projection_green_with_exact_engine_ranking():
    frozen,validate=real_case();source=frozen['original_postpolicy_output_projection'];before=deepcopy(source)
    assert validate(source)==frozen['original_issues']
    raw=deepcopy(frozen['provider_raw'])
    result=synthesize_final_no_focus(source,issues=frozen['original_issues'],
        direction_members=frozen['direction_members'],engine_scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']},
        final_attempt=3,validator=validate,provider_raw=raw)
    projected,audit=result
    assert validate(projected)==[] and source==before and raw==frozen['provider_raw']
    assert projected['focus_pool']==source['focus_pool']
    actual={r['rotation_direction_id']:r['reviewed_symbols'] for r in projected['rotation_reviews']}
    assert actual['SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE']==['300556.SZ','300839.SZ','600406.SH']
    assert actual['SELECTED_BOARD:INNOVATIVE_MEDICINE_HEALTHCARE']==['688617.SH','300298.SZ','300453.SZ']
    assert audit['original_issues']==frozen['original_issues'] and audit['projected_validation_issues']==[]
    assert audit['provider_capture_unchanged'] and audit['focus_unchanged']


@pytest.mark.parametrize('attempt',[1,2,4,True])
def test_not_final_round_never_synthesizes(attempt):
    frozen,validate=real_case()
    assert synthesize_final_no_focus(frozen['original_postpolicy_output_projection'],issues=frozen['original_issues'],
        direction_members=frozen['direction_members'],engine_scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']},
        final_attempt=attempt,validator=validate,provider_raw=frozen['provider_raw']) is None


@pytest.mark.parametrize('bad',[None,True,float('nan'),'BAD_SCORE'])
def test_missing_or_bad_engine_score_cannot_fake_a_rank(bad):
    frozen,validate=real_case();scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']};scores['300556.SZ']=bad
    assert synthesize_final_no_focus(frozen['original_postpolicy_output_projection'],issues=frozen['original_issues'],
        direction_members=frozen['direction_members'],engine_scores=scores,final_attempt=3,
        validator=validate,provider_raw=frozen['provider_raw']) is None


@pytest.mark.parametrize('extra',['A2_ROTATION_DOMAIN_MISMATCH','A2_MARKET_READ_INVALID','TEAM_OPPOSING_REFS_INVALID:theme_reviews[0]',
    'A2_ROTATION_REVIEW_INVALID:FOREIGN:UNKNOWN_DIRECTION'])
def test_mixed_noncoverage_errors_are_not_repaired(extra):
    frozen,validate=real_case();issues=[*frozen['original_issues'],extra]
    assert synthesize_final_no_focus(frozen['original_postpolicy_output_projection'],issues=issues,
        direction_members=frozen['direction_members'],engine_scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']},
        final_attempt=3,validator=lambda value:[*validate(value),extra],provider_raw=frozen['provider_raw']) is None


def test_feedback_uses_qualified_member_intersection_not_same_direction_label():
    contexts={'000001.SZ':dict(rotation_direction_id='D',deterministic_status='REVIEW_CANDIDATE',
        trend_core_eligible=True,top_rotation_theme=True,selected_board={'selected_for_rotation':True}),
        '000002.SZ':dict(rotation_direction_id='D',deterministic_status='REVIEW_CANDIDATE',
        trend_core_eligible=False,top_rotation_theme=True,selected_board={'selected_for_rotation':True})}
    feedback=legacy._a2_rotation_retry_feedback({'focus_pool':[{'symbol':'000002.SZ'}],'rotation_reviews':[]},
        {'A2_BOTTLENECK_CONTEXT':contexts},set(contexts))
    assert feedback['already_focused_directions']=={}
    assert feedback['required_no_focus_if_focus_unchanged']=={'D':['000001.SZ']}
    assert feedback['validation_errors']==['A2_ROTATION_FOCUS_COVERAGE_MISSING:D']


def test_foreign_direction_or_stock_is_not_hidden_by_explanation_fill():
    frozen,validate=real_case();source=deepcopy(frozen['original_postpolicy_output_projection'])
    source['rotation_reviews']=[dict(rotation_direction_id='FOREIGN',decision='NO_FOCUS',
        reviewed_symbols=['FOREIGN'],reason_codes=['FAKE'],explanation='This invalid row must remain blocked')]
    issues=validate(source)
    assert any('UNKNOWN_DIRECTION' in code for code in issues)
    assert synthesize_final_no_focus(source,issues=issues,direction_members=frozen['direction_members'],
        engine_scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']},final_attempt=3,
        validator=validate,provider_raw=frozen['provider_raw']) is None


def test_final_validator_must_prove_green_not_assume_added_rows_work():
    frozen,validate=real_case();source=frozen['original_postpolicy_output_projection']
    def reject_projection(value):
        return validate(value) if not value['rotation_reviews'] else ['STILL_INVALID']
    assert synthesize_final_no_focus(source,issues=frozen['original_issues'],direction_members=frozen['direction_members'],
        engine_scores={r['symbol']:r['quant_score'] for r in frozen['engine_rows']},final_attempt=3,
        validator=reject_projection,provider_raw=frozen['provider_raw']) is None
