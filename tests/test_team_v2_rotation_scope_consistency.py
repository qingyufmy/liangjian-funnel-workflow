"""A focused EMOTION stock cannot cover an independent TREND review domain."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.research.common import _validate_a2_rotation_focus_coverage
from test_team_v2_a2_schema import packet, validate, REFS


def contexts():
    return {
        '000001.SZ': dict(deterministic_status='REVIEW_CANDIDATE', trend_core_eligible=True,
            top_rotation_theme=True, rotation_direction_id='D', selected_board={'selected_for_rotation': True}),
        '000002.SZ': dict(deterministic_status='REVIEW_CANDIDATE', trend_core_eligible=False,
            top_rotation_theme=False, rotation_direction_id='D', selected_board={'selected_for_rotation': True}),
    }


def review(direction='D', symbol='000001.SZ'):
    return dict(rotation_direction_id=direction, decision='NO_FOCUS', reviewed_symbols=[symbol],
        reason_codes=['LOW_CONVICTION'], explanation='Frozen trend member lacks relative conviction', **REFS)


def test_emotion_focus_same_direction_and_trend_no_focus_pass_both_validators_without_repair():
    raw = packet(); raw['focus_decisions'][0]['symbol'] = '000002.SZ'
    raw['rotation_reviews'] = [review()]
    assert validate(raw, direction_symbols={'D': {'000001.SZ'}}).valid
    before = deepcopy(raw)
    output = dict(focus_pool=[{'symbol': '000002.SZ'}], rotation_reviews=raw['rotation_reviews'])
    assert _validate_a2_rotation_focus_coverage(output, {'A2_BOTTLENECK_CONTEXT': contexts()}, set(contexts())) == []
    assert raw == before


def test_emotion_focus_does_not_excuse_missing_required_trend_review():
    output = dict(focus_pool=[{'symbol': '000002.SZ'}], rotation_reviews=[])
    assert _validate_a2_rotation_focus_coverage(output, {'A2_BOTTLENECK_CONTEXT': contexts()}, set(contexts())) == [
        'A2_ROTATION_FOCUS_COVERAGE_MISSING:D']


def test_actual_qualified_trend_focus_still_forbids_redundant_no_focus():
    output = dict(focus_pool=[{'symbol': '000001.SZ'}], rotation_reviews=[review()])
    assert _validate_a2_rotation_focus_coverage(output, {'A2_BOTTLENECK_CONTEXT': contexts()}, set(contexts())) == [
        'A2_ROTATION_REVIEW_INVALID:D:ALREADY_FOCUSED']


@pytest.mark.parametrize('bad_review,required_reason', [
    (review('FORGED'), 'A2_ROTATION_REVIEW_INVALID:FORGED:UNKNOWN_DIRECTION'),
    (review(symbol='FOREIGN'), 'A2_ROTATION_REVIEW_REPRESENTATIVE_INVALID:D'),
    ({**review(), 'explanation': ''}, 'A2_ROTATION_REVIEW_INVALID:D:EXPLANATION_REQUIRED'),
    ({**review(), 'reason_codes': []}, 'A2_ROTATION_REVIEW_INVALID:D:REASON_CODES_REQUIRED'),
])
def test_domain_consistency_does_not_relax_any_original_review_requirement(bad_review, required_reason):
    reasons = _validate_a2_rotation_focus_coverage(dict(focus_pool=[], rotation_reviews=[bad_review]),
        {'A2_BOTTLENECK_CONTEXT': contexts()}, set(contexts()))
    assert required_reason in reasons and 'A2_ROTATION_FOCUS_COVERAGE_MISSING:D' in reasons
