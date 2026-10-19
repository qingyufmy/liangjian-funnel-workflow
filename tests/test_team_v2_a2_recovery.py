"""0361-b: feedback is not validation; normalized projections are not model raw."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_team_v2_a2_research import fixture


def no_focus(direction='D', symbols=None):
    return dict(rotation_direction_id=direction, decision='NO_FOCUS',
        reviewed_symbols=symbols or ['000001.SZ'], reason_codes=['LOW_CONVICTION'],
        explanation='Provider reports no focused candidate in this direction',
        supporting_refs=['A2:QUANT:000001.SZ'], opposing_refs=['A2:NEWS'])


def test_mixed_enum_and_rotation_gets_three_rounds_then_safe_projection(tmp_path):
    def mutate(raw, attempt):
        raw['rotation_reviews'] = [no_focus()]
        if attempt == 1:
            raw['market_read']['emotion_cycle'] = 'LATENT'
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED', result.reason_codes
    assert len(seen) == 3
    feedback = seen[1]['messages'][0]['content']
    assert 'LATENT' in feedback and 'A2_MARKET_READ_INVALID' in feedback
    assert all(value in feedback for value in (
        'IGNITION', 'CONFIRMATION', 'ACCELERATION', 'CLIMAX', 'DIVERGENCE', 'RETREAT', 'REPAIR', 'FADE'))
    assert result.diagnostics['server_normalization']['status'] == 'VALIDATED'
    assert result.output['rotation_reviews'] == []
    with closing(app.team_research_adapter.journal._connect()) as db:
        captured = [json.loads(r[0]) for r in db.execute('SELECT output_json FROM model_calls ORDER BY captured_at')]
        validated = [r[0] for r in db.execute('SELECT validated FROM model_calls ORDER BY captured_at')]
    assert captured[0]['market_read']['emotion_cycle'] == 'LATENT'
    assert all(r['rotation_reviews'] for r in captured)
    assert validated == [0, 0, 0]
    audit_key = result.diagnostics['server_normalization']['audit_context_key']
    with closing(app.team_research_adapter.store._connect()) as db:
        audit = json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            (audit_key,)).fetchone()[0])
    assert audit['status'] == 'VALIDATED' and audit['provider_capture_unchanged'] is True
    assert audit['normalized_raw']['rotation_reviews'] == []
    assert audit['normalized_validation_issues'] == audit['postpolicy_reason_codes'] == []
    assert audit['postpolicy_output'] == result.output
    assert audit['original_issues'] == ['A2_ROTATION_DOMAIN_MISMATCH']
    assert app._run_stage(**args).output == result.output and len(seen) == 3


def test_only_missing_rotation_is_synthesized_after_third_invalid_capture(tmp_path):
    app, args, seen = fixture(tmp_path, lambda raw, _: raw.update(focus_decisions=[]), rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED', result.reason_codes
    assert len(seen) == 3 and result.output['focus_pool'] == []
    row = result.output['rotation_reviews'][0]
    assert row['reviewed_symbols'] == ['000001.SZ', '000002.SZ', '000003.SZ']
    assert row['reason_codes'] == ['SERVER_NORMALIZED']
    assert 'server' in row['explanation'].lower()
    assert result.diagnostics['validation_history'] == [['A2_ROTATION_COVERAGE_MISMATCH']] * 3


def test_illegal_enum_never_normalized_even_with_rotation_error(tmp_path):
    def mutate(raw, _):
        raw['market_read']['emotion_cycle'] = 'LATENT'
        raw['rotation_reviews'] = [no_focus()]
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3
    assert 'A2_MARKET_READ_INVALID' in result.reason_codes
    assert result.diagnostics['server_normalization'] is None


@pytest.mark.parametrize('mutation', [
    lambda raw: raw['rotation_reviews'].append(no_focus('FOREIGN')),
    lambda raw: raw['rotation_reviews'].append(no_focus('D', ['FOREIGN'])),
    lambda raw: raw['rotation_reviews'].append(no_focus('D', ['000001.SZ', '000001.SZ'])),
    lambda raw: raw['rotation_reviews'].extend([no_focus(), no_focus()]),
])
def test_normalization_cannot_hide_foreign_stock_direction_or_duplicates(tmp_path, mutation):
    def mutate(raw, _):
        raw['focus_decisions'] = []
        mutation(raw)
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3


def test_real_failure_fixture_keeps_latent_invalid_until_model_reanswers():
    from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
    from liangjian_funnel.pipeline.team_v2_a2_recovery import normalize_rotation
    path = Path(__file__).parent / 'fixtures/team_v2_a2_real_failure_20261019.json'
    frozen = json.loads(path.read_text(encoding='utf-8'))
    raw = frozen['output']; before = deepcopy(raw)
    context = {key: frozen[key] for key in ('required_envelope', 'candidate_ids', 'nomination_scope',
        'symbol_themes', 'allowed_theme_ids', 'direction_symbols')}
    for key in ('candidate_ids', 'nomination_scope', 'allowed_theme_ids'):
        context[key] = set(context[key])
    context['direction_symbols'] = {key: set(value) for key, value in context['direction_symbols'].items()}
    def validate(value):
        return validate_a2_response(value, frozen['catalog'], **context)
    invalid = validate(raw)
    assert set(invalid.issues) == {'A2_MARKET_READ_INVALID', 'A2_ROTATION_DOMAIN_MISMATCH'}
    assert normalize_rotation(raw, frozen['catalog'], context['direction_symbols'], invalid.issues) is None
    # This line represents the model's corrected second reply, NOT enum repair by the server.
    answered = deepcopy(raw); answered['market_read']['emotion_cycle'] = 'CONFIRMATION'
    projected, audit = normalize_rotation(answered, frozen['catalog'], context['direction_symbols'], validate(answered).issues)
    assert validate(projected).valid and raw == before
    assert audit['actions'] and audit['original_issues'] == ['A2_ROTATION_DOMAIN_MISMATCH']


@pytest.mark.parametrize('remaining,requests', [(119, 0), (120, 1)])
def test_new_request_requires_120_seconds_global_budget(tmp_path, monkeypatch, remaining, requests):
    from liangjian_funnel.pipeline import team_v2_a2_research as research
    app, args, seen = fixture(tmp_path)
    app._deadline_started_monotonic = 100
    now = 100 + app.settings.research_close_deadline_seconds - remaining
    monkeypatch.setattr(research, 'time', SimpleNamespace(monotonic=lambda: now))
    result = app._run_stage(**args)
    assert len(seen) == requests
    assert result.status == ('VALIDATED' if requests else 'BLOCKED')


def test_short_configured_timeout_is_not_mistaken_for_remaining_global_budget(tmp_path):
    app, args, seen = fixture(tmp_path)
    app.settings = app.settings.model_copy(update={'model_timeout_seconds': 30})
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 1


def test_every_semantic_retry_gets_fresh_bounded_timeout_not_original_elapsed(tmp_path, monkeypatch):
    from liangjian_funnel.pipeline import team_v2_a2_research as research
    state = {'clock': 100.0}; budgets = []
    def mutate(raw, attempt):
        if attempt < 3:
            raw['market_read']['emotion_cycle'] = 'LATENT'
        state['clock'] += 550
    app, args, seen = fixture(tmp_path, mutate)
    app._deadline_started_monotonic = 100
    monkeypatch.setattr(research, 'time', SimpleNamespace(monotonic=lambda: state['clock']))
    original = research._response
    def capture(**kwargs):
        budgets.append(kwargs['timeout_seconds'])
        return original(**kwargs)
    monkeypatch.setattr(research, '_response', capture)
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 3
    assert budgets == [600.0, 600.0, 600.0]


def test_retry_does_not_issue_request_when_global_remaining_drops_below_120(tmp_path, monkeypatch):
    from liangjian_funnel.pipeline import team_v2_a2_research as research
    state = {'clock': 100.0}
    def mutate(raw, _):
        raw['market_read']['emotion_cycle'] = 'LATENT'
        state['clock'] += 550
    app, args, seen = fixture(tmp_path, mutate)
    app.settings = app.settings.model_copy(update={'research_close_deadline_seconds': 650})
    app._deadline_started_monotonic = 100
    monkeypatch.setattr(research, 'time', SimpleNamespace(monotonic=lambda: state['clock']))
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 1
    assert result.reason_codes == ('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT',)
    assert result.diagnostics['validation_history'] == [['A2_MARKET_READ_INVALID']]
    assert result.attempts == 1
    assert result.diagnostics['budget_stop']['remaining_seconds'] == 100


@pytest.mark.parametrize('rounds,valid', [(1, False), (2, True), (3, True), (4, False)])
def test_legacy_degraded_gate_accepts_only_old_two_or_authorized_three_rotation_rounds(rounds, valid):
    from liangjian_funnel.pipeline.review_degraded import rotation_review_failed_twice
    history = [['A2_ROTATION_COVERAGE_MISMATCH']] * rounds
    assert rotation_review_failed_twice(history) is valid
    assert not rotation_review_failed_twice([['A2_MARKET_READ_INVALID'], *history[1:]])


@pytest.mark.parametrize('field,value', [
    ('decision', 'FOCUS'), ('explanation', ''), ('reason_codes', [None]),
    ('supporting_refs', ['FOREIGN']), ('opposing_refs', []),
    ('reviewed_symbols', ['000001.SZ', 'FOREIGN']),
])
def test_deleting_focused_direction_must_not_conceal_invalid_row_fields(tmp_path, field, value):
    def mutate(raw, _):
        row = no_focus(); row[field] = value
        raw['rotation_reviews'] = [row]
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3


def test_synthesis_requires_actual_member_quant_catalog_entry(tmp_path):
    from liangjian_funnel.pipeline.team_v2_a2_recovery import normalize_rotation
    raw = dict(focus_decisions=[], rotation_reviews=[], handoff_notes={'to_a3': ['Frozen']},
        what_would_change_my_mind=['Frozen evidence changes'])
    assert normalize_rotation(raw, {'categories': ['NEWS'], 'items': [{'id': 'NEWS', 'category': 'NEWS'}]},
        {'D': {'000001.SZ'}}, ['A2_ROTATION_COVERAGE_MISMATCH']) is None
