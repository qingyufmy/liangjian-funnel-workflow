"""Generic primary-identity/research-scope and non-executable A3 regressions."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.a2_theme_research import (
    independent_theme_reviews, theme_hypotheses,
)
from liangjian_funnel.pipeline.deterministic import screen_a3
from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes
from liangjian_funnel.pipeline.research import _project_a2_bottleneck_context
from test_independent_theme_research import attach, technical_inputs
from test_team_v2_a2_research import fixture


def primary_and_concept_binding(symbol, *, concept='ALTERNATIVE'):
    links = [dict(theme_id='T', node_id='T:CORE', taxonomy='INDUSTRY',
        taxonomy_code='881263.TI', taxonomy_name='农化制品',
        match_method='MATURE_THEME_REGISTRY_EXACT_NAME'),
        dict(theme_id=concept, node_id=concept+':CORE', taxonomy='CONCEPT',
        taxonomy_code='301000.TI', taxonomy_name='新材料概念',
        match_method='MATURE_THEME_REGISTRY_EXACT_NAME')]
    snapshot = {'THS_INDUSTRY_MEMBERSHIP': {'records': [dict(symbol=symbol,
        memberships=[dict(industry_thscode='881263.TI', industry_name='农化制品')])]},
        'THS_CONCEPT_MEMBERSHIP': {'records': [dict(symbol=symbol,
        memberships=[dict(concept_thscode='301000.TI', concept_name='新材料概念')])]}}
    return bind_emotion_themes({'taxonomy_links': links}, snapshot, {symbol})[symbol]


def test_real_binding_preserves_primary_but_all_proven_research_routes_reach_mandatory_scope(tmp_path):
    app, args, _ = fixture(tmp_path)
    row = attach(args)
    binding = primary_and_concept_binding('000001.SZ')
    row['theme_research_binding'] = binding
    assert binding['theme_id'] == 'T' and binding['resolved']
    assert {r['theme_id'] for r in binding['candidate_routes']} == {'T'}
    assert {r['theme_id'] for r in binding['research_candidate_routes']} == {'T', 'ALTERNATIVE'}
    contexts = args['snapshot'].data['A2_BOTTLENECK_CONTEXT']
    hypotheses = theme_hypotheses(contexts, set(contexts))['000001.SZ']
    assert {r['theme_id'] for r in hypotheses} == {'T', 'ALTERNATIVE'}
    projected = _project_a2_bottleneck_context(contexts, set(contexts))
    assert projected['_theme_review_scope']['ALTERNATIVE'] == ['000001.SZ']
    assert projected['_rotation_review_scope'] == {}
    frozen = app.team_research_adapter.prepare_a2_snapshot(args['snapshot'], args['upstream_symbols'])
    alternate = frozen.data['_TEAM_A2_SCORE_FREEZE']['themes']['ALTERNATIVE']
    assert alternate['base_score'] is None and alternate['status'] == 'SCORE_DATA_LIMITED'
    assert row['theme_id'] == 'T' and row['execution_permission'] == 'BLOCKED'


def test_real_binding_new_concept_is_required_by_actual_a2_adapter(tmp_path):
    app, args, _ = fixture(tmp_path)
    row = attach(args)
    row['theme_research_binding'] = primary_and_concept_binding('000001.SZ')
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED'
    assert any('THEME_COVERAGE' in reason for reason in result.reason_codes)


def test_historical_binding_does_not_invent_omitted_concept_route(tmp_path):
    _, args, _ = fixture(tmp_path)
    row = attach(args)
    binding = primary_and_concept_binding('000001.SZ')
    binding.pop('research_candidate_routes')
    row['theme_research_binding'] = binding
    assert {r['theme_id'] for r in theme_hypotheses({'000001.SZ': row}, {'000001.SZ'})['000001.SZ']} == {'T'}


@pytest.mark.parametrize('defect', ['foreign_evidence', 'hash', 'explicit_invalid_routes'])
def test_new_research_routes_still_require_exact_original_membership(tmp_path, defect):
    _, args, _ = fixture(tmp_path)
    row = attach(args)
    binding = primary_and_concept_binding('000001.SZ')
    row['theme_research_binding'] = binding
    if defect == 'foreign_evidence':
        route = next(r for r in binding['research_candidate_routes'] if r['theme_id'] == 'ALTERNATIVE')
        route['evidence'][0]['taxonomy_code'] = 'FOREIGN'
    elif defect == 'hash':
        binding['source_hash'] = '0' * 64
    else:
        binding['research_candidate_routes'] = None
    hypotheses = theme_hypotheses({'000001.SZ': row}, {'000001.SZ'}).get('000001.SZ', [])
    assert all(r['theme_id'] != 'ALTERNATIVE' for r in hypotheses)


def test_real_binding_to_a3_technical_scope_preserves_monthly_identity():
    from liangjian_funnel.pipeline.research.a3 import _build_a3_candidate_domain
    snapshot, output, symbol = technical_inputs()
    context = snapshot['A2_BOTTLENECK_CONTEXT'][symbol]
    context['theme_research_binding'] = primary_and_concept_binding(symbol)
    # The domain helper only consumes existing A2 research-eligible rows.
    row = {**context, 'symbol': symbol, 'primary_theme': 'T'}
    domain, origins = _build_a3_candidate_domain({'watch_only_pool': [row]})
    assert origins == {symbol: 'WATCH_ONLY'}
    assert domain['focus_pool'][0]['primary_theme'] == 'T'
    assert {h['theme_id'] for h in theme_hypotheses({symbol: context}, {symbol})[symbol]} == {'T', 'ALTERNATIVE'}


def test_alternative_watch_is_observation_not_primary_theme_rejection_or_model_request():
    snapshot, output, _ = technical_inputs()
    output['focus_pool'][0]['ladder_height'] = 1
    original = deepcopy(output)
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision['eligibility'] == 'WATCH'
    assert decision['status'] == 'LOCAL_MONITOR' and not decision['sent_to_llm']
    assert decision['research_state'] == 'PREPARATION_WATCH'
    assert decision['veto_conditions'] == []
    assert decision['reason_codes'] == ['FIRST_BOARD_OBSERVE_ONLY']
    assert decision['execution_permission'] == 'BLOCKED'
    assert decision['research_observation_scope'] == 'RESEARCH_ONLY_NO_AUTOMATIC_ENTRY'
    facts = decision['strategy_facts']
    assert facts['independent_theme_original_primary_verdict']['veto_conditions'] == ['LEADER_THEME_RETREAT_OR_CLIMAX']
    assert output == original


@pytest.mark.parametrize('defect', ['missing_review', 'stale_review', 'different_decision', 'conflicting_review'])
def test_incomplete_alternative_evidence_is_data_gap_not_validated_no_setup(defect):
    from liangjian_funnel.pipeline.feature_store import content_hash
    from liangjian_funnel.pipeline.research.a3 import _a3_decision_evidence_gaps
    snapshot, output, symbol = technical_inputs()
    if defect == 'missing_review':
        output['independent_theme_reviews'] = []
    elif defect == 'stale_review':
        for review in output['independent_theme_reviews']:
            review['review']['stage_observed_as_of'] = '2026-09-25'
            review['review_id'] = content_hash({k: v for k, v in review.items() if k != 'review_id'})
    elif defect == 'different_decision':
        snapshot['A2_BOTTLENECK_CONTEXT'][symbol]['decision_id'] = 'changed-frozen-fact'
    else:
        other = deepcopy(output['independent_theme_reviews'][1])
        other['review']['stage'] = 'RETREAT'
        other['review_id'] = content_hash({k: v for k, v in other.items() if k != 'review_id'})
        output['independent_theme_reviews'].append(other)
    gate = screen_a3(snapshot, output)
    decision = gate.decisions[0]
    assert decision['eligibility'] == 'DATA_GAP' and decision['status'] == 'DATA_GAP'
    assert decision['research_state'] == 'EVIDENCE_PENDING' and not decision['sent_to_llm']
    assert decision['execution_permission'] == 'BLOCKED'
    assert _a3_decision_evidence_gaps({'rejected_candidates': [decision]}, gate)


def test_known_primary_qualified_is_not_downgraded_by_missing_alternative_review():
    from liangjian_funnel.pipeline.feature_store import content_hash
    snapshot, output, symbol = technical_inputs()
    output['active_themes'][0]['stage'] = 'ACCELERATION'
    output['active_themes'][0]['stage_evidence_hash'] = content_hash(output['active_themes'][0])
    output['independent_theme_reviews'] = []
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision['eligibility'] == 'QUALIFIED' and decision['sent_to_llm']
    assert 'independent_theme_original_primary_verdict' not in decision['strategy_facts']


def test_incomplete_theme_guard_survives_actual_local_row_and_authority_projection():
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot, _with_a3_deterministic_context
    from liangjian_funnel.pipeline.research.common import _gate_secondary_items
    from liangjian_funnel.pipeline.research.a3 import _apply_a3_candidate_origin_policy
    snapshot, output, symbol = technical_inputs()
    snapshot['A2_BOTTLENECK_CONTEXT'][symbol]['execution_permission'] = 'REQUIRES_A3_A4_CONFIRMATION'
    output['independent_theme_reviews'] = []
    gate = screen_a3(snapshot, output)
    frozen = _with_a3_deterministic_context(FrozenInputSnapshot('local-gap', snapshot), gate)
    rows = _gate_secondary_items(gate, 'A3')
    result, _ = _apply_a3_candidate_origin_policy({'core_watch_pool': [],
        'secondary_watch_pool': [], 'rejected_candidates': rows}, frozen.data)
    decision = gate.decisions[0]
    evidence = decision['strategy_facts']['independent_theme_research_selection']
    assert evidence['selection_state'] == 'INCOMPLETE_REVIEW_NO_THEME_SELECTED'
    assert evidence['theme_id'] is None
    # Rejected/data-gap rows cannot become executable even though the monthly
    # route has a different permission. Its persisted eligibility stays a gap.
    assert result['rejected_candidates'][0]['eligibility'] == 'DATA_GAP'
    assert decision['execution_permission'] == 'BLOCKED'


@pytest.mark.parametrize('missing_review', [False, True])
def test_known_safety_veto_is_not_swallowed_by_watch_or_missing_theme(missing_review):
    snapshot, output, symbol = technical_inputs()
    snapshot['TRADABILITY_FLAGS'][symbol]['tradable'] = False
    output['focus_pool'][0]['ladder_height'] = 1
    if missing_review:
        output['independent_theme_reviews'] = []
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision['eligibility'] == 'REJECTED'
    assert 'NOT_TRADABLE' in decision['veto_conditions']
    assert not decision['sent_to_llm']
