"""0361-b: feedback is not validation; normalized projections are not model raw."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
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


def test_mixed_unbound_enum_and_rotation_retries_only_enum_after_safe_drop(tmp_path):
    def mutate(raw, attempt):
        raw['rotation_reviews'] = [no_focus()]
        if attempt == 1:
            raw['market_read']['emotion_cycle'] = 'INVENTED_STAGE'
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED', result.reason_codes
    assert len(seen) == 2
    feedback = seen[1]['messages'][0]['content']
    assert 'INVENTED_STAGE' in feedback and 'A2_MARKET_READ_INVALID' in feedback
    assert all(value in feedback for value in (
        'IGNITION', 'CONFIRMATION', 'ACCELERATION', 'CLIMAX', 'DIVERGENCE', 'RETREAT', 'REPAIR', 'FADE'))
    assert result.diagnostics['server_normalization']['status'] == 'VALIDATED'
    assert result.output['rotation_reviews'] == []
    with closing(app.team_research_adapter.journal._connect()) as db:
        captured = [json.loads(r[0]) for r in db.execute('SELECT output_json FROM model_calls ORDER BY captured_at')]
        validated = [r[0] for r in db.execute('SELECT validated FROM model_calls ORDER BY captured_at')]
    assert captured[0]['market_read']['emotion_cycle'] == 'INVENTED_STAGE'
    assert all(r['rotation_reviews'] for r in captured)
    assert validated == [0, 0]
    audit_key = result.diagnostics['server_normalization']['audit_context_key']
    with closing(app.team_research_adapter.store._connect()) as db:
        audit = json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            (audit_key,)).fetchone()[0])
    assert audit['status'] == 'VALIDATED' and audit['provider_capture_unchanged'] is True
    assert audit['normalized_raw']['rotation_reviews'] == []
    assert audit['normalized_validation_issues'] == audit['postpolicy_reason_codes'] == []
    assert audit['postpolicy_output'] == result.output
    assert audit['original_issues'] == ['A2_ROTATION_DOMAIN_MISMATCH']
    assert app._run_stage(**args).output == result.output and len(seen) == 2


def test_missing_rotation_is_only_feedback_and_never_server_synthesized(tmp_path):
    app, args, seen = fixture(tmp_path, lambda raw, _: raw.update(focus_decisions=[]), rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED', result.reason_codes
    assert len(seen) == 3 and result.output is None
    assert result.diagnostics['server_normalization'] is None
    assert result.diagnostics['validation_history'] == [['A2_ROTATION_COVERAGE_MISMATCH']] * 3
    assert 'required_no_focus_if_focus_unchanged' in seen[1]['messages'][0]['content']
    assert all(symbol in seen[1]['messages'][0]['content'] for symbol in ('000001.SZ', '000002.SZ', '000003.SZ'))


def test_illegal_enum_never_normalized_even_with_rotation_error(tmp_path):
    def mutate(raw, _):
        raw['market_read']['emotion_cycle'] = 'INVENTED_STAGE'
        raw['rotation_reviews'] = [no_focus()]
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3
    assert 'A2_MARKET_READ_INVALID' in result.reason_codes
    assert result.diagnostics['server_normalization']['status'] == 'BLOCKED'


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


@pytest.mark.parametrize('name', ['team_v2_a2_real_failure_20261019.json',
    'team_v2_a2_real_failure_rerun_20261019.json'])
def test_two_real_failures_use_exact_frozen_market_fact_and_drop_without_retry(name):
    from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
    from liangjian_funnel.pipeline.team_v2_a2_recovery import drop_focused_rotation
    path = Path(__file__).parent / 'fixtures' / name
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
    # P0-17: LATENT is now a first-class ten-stage value, not an illegal enum.
    assert set(invalid.issues) == {'A2_ROTATION_DOMAIN_MISMATCH'}
    # First capture's exact fact is independently archived in
    # FROZEN_A2_REQUEST_VALUES.json (SHA58e94020...); rerun carries its fact inline.
    fact = frozen.get('engine_emotion_snapshot', {'emotion_cycle_stage': 'LATENT'})
    assert fact['emotion_cycle_stage'] == 'LATENT'
    context['engine_emotion_cycle'] = fact['emotion_cycle_stage']
    projected, audit = drop_focused_rotation(raw, frozen['catalog'], context['direction_symbols'])
    assert validate(projected).valid and raw == before
    assert projected['market_read']['emotion_cycle'] == raw['market_read']['emotion_cycle'] == 'LATENT'
    assert audit['actions'] and all(r['action'] == 'SERVER_DROPPED_FOCUSED_DIRECTION' for r in audit['actions'])


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
            raw['market_read']['emotion_cycle'] = 'INVENTED_STAGE'
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
        raw['market_read']['emotion_cycle'] = 'INVENTED_STAGE'
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


def test_missing_coverage_never_creates_server_rows():
    from liangjian_funnel.pipeline.team_v2_a2_recovery import drop_focused_rotation
    raw = dict(focus_decisions=[], rotation_reviews=[], handoff_notes={'to_a3': ['Frozen']},
        what_would_change_my_mind=['Frozen evidence changes'])
    assert drop_focused_rotation(raw, {'categories': ['NEWS'], 'items': [{'id': 'NEWS', 'category': 'NEWS'}]},
        {'D': {'000001.SZ'}}) is None


def test_exact_engine_latent_and_redundant_bank_review_validate_in_one_call_without_raw_rewrite(tmp_path):
    def mutate(raw, _):
        raw['market_read']['emotion_cycle'] = 'LATENT'
        raw['rotation_reviews'] = [no_focus()]
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    args['snapshot'] = replace(args['snapshot'], data={**args['snapshot'].data,
        'MARKET_EMOTION_SNAPSHOT': {'emotion_cycle_stage': 'LATENT'}})
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 1
    assert result.output['market_read']['emotion_cycle'] == 'LATENT'
    assert result.diagnostics['validation_history'] == [[]]
    assert result.diagnostics['provider_validation_history'] == [['A2_ROTATION_DOMAIN_MISMATCH']]
    with closing(app.team_research_adapter.journal._connect()) as db:
        captured, validated = db.execute('SELECT output_json,validated FROM model_calls').fetchone()
    assert not validated and json.loads(captured)['rotation_reviews'] == [no_focus()]
    audit = result.diagnostics['server_normalization']
    assert audit['status'] == 'VALIDATED'
    assert [r['action'] for r in audit['actions']] == ['SERVER_DROPPED_FOCUSED_DIRECTION']
    assert app._run_stage(**args).output == result.output and len(seen) == 1


@pytest.mark.parametrize('fact,answer,valid', [
    ('LATENT', 'LATENT', True), ('STARTUP', 'STARTUP', False), ('ICE_POINT', 'ICE_POINT', True),
    ('LATENT', 'STARTUP', False), ('STARTUP', 'LATENT', True), (None, 'LATENT', True),
    ('INVENTED', 'INVENTED', False), ('LATENT', 'INVENTED', False),
    ('LATENT', 'CONFIRMATION', True),
])
def test_market_stage_extension_is_exact_frozen_fact_not_mapping_or_free_enum(fact, answer, valid):
    from test_team_v2_a2_schema import packet, validate
    raw = packet(); raw['market_read']['emotion_cycle'] = answer
    result = validate(raw, engine_emotion_cycle=fact)
    assert result.valid is valid
    assert raw['market_read']['emotion_cycle'] == answer


def test_shared_ten_stages_accept_latent_watch_but_never_latent_allow():
    from test_team_v2_a2_schema import packet, validate
    raw = packet(); raw['market_read']['emotion_cycle'] = 'LATENT'
    raw['theme_reviews'][0].update(stage='LATENT', new_entry_policy='WATCH_ONLY')
    result = validate(raw, engine_emotion_cycle='LATENT')
    assert result.valid
    raw['theme_reviews'][0]['new_entry_policy']='ALLOW'
    assert 'A2_STAGE_POLICY_MISMATCH' in validate(raw,engine_emotion_cycle='LATENT').issues


def test_redundant_focused_row_with_unknown_extra_field_is_not_deleted(tmp_path):
    def mutate(raw, _):
        row = no_focus(); row['executable'] = True
        raw['rotation_reviews'] = [row]
    app, args, seen = fixture(tmp_path, mutate, rotation=True)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3
    assert 'A2_ITEM_FIELDS_INVALID:rotation_reviews' in result.reason_codes
    assert result.diagnostics['server_normalization'] is None
