import copy
from datetime import datetime
from types import SimpleNamespace

import pytest

from liangjian_funnel.evaluation.b3_variant_replay import (
    evaluate_window, reference_outcomes, summarize_plan_windows,
)


def window(at='2026-10-15T09:46:00+08:00'):
    return {'kind': 'WINDOW', 'trade_date': at[:10], 'plan_id': 'p',
        'plan': {'symbol': '600000.SH', 'strategy_profile': 'TREND_MA5'},
        'plan_row': {'valid_from': at, 'expires_at': at[:10] + 'T15:00:00+08:00'},
        'event': {'event_id': 'e', 'minute_end': at, 'action': 'LLM_VETO', 'reason_code': 'LLM_VETO', 'effective': True},
        'payload': {'strategy': {'action': 'BUY_SIGNAL', 'reason_codes': ['PASS']},
                    'llm_veto': True, 'llm_reason_code': 'LLM_VETO'},
        'bars': (), 'market_context': {}, 'observation_time': datetime.fromisoformat(at),
        'decision_time': datetime.fromisoformat(at), 'provenance': {'event_sha256': 'event'}}


def evaluator(action='BUY_SIGNAL', reasons=None):
    def evaluate(*args, **kwargs):
        return SimpleNamespace(model_dump=lambda **kw: {'action': action, 'reason_codes': reasons or ['PASS'],
            'live_entry_price': 10.0, 'confirmation_results': {'leaf': {'met': True, 'available': True, 'kind': 'CONDITION'}}})
    return evaluate


def test_variant_buy_is_technical_candidate_not_frozen_llm_approval(monkeypatch):
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    monkeypatch.setattr(replay, 'build_evaluator', lambda variant: evaluator())
    original = window(); before = copy.deepcopy(original)
    row = evaluate_window(original, 'V1', evidence={}, history=[])
    assert row['technical_action'] == 'BUY_SIGNAL'
    assert row['actual_execution_action'] == 'LLM_VETO'
    assert row['variant_model_verdict'] == 'NOT_EVALUATED_NO_MODEL_CALL'
    assert row['variant_order_or_fill'] is None
    assert original == before


def test_pre_kernel_block_never_evaluates_variant(monkeypatch):
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    monkeypatch.setattr(replay, 'build_evaluator', lambda *args: pytest.fail('No synthetic opportunity'))
    source = {**window(), 'kind': 'PRE_KERNEL_BLOCK', 'recorded_action': 'DATA_BLOCK',
              'recorded_reason': 'QUOTE_MISSING'}
    row = evaluate_window(source, 'V5', evidence={}, history=[])
    assert row['status'] == 'PRE_KERNEL_BLOCK'
    assert row['technical_action'] is None


def test_geometry_variants_do_not_promote_post_auction_proxy(monkeypatch):
    source = window()
    row = evaluate_window(source, 'V4', evidence={'auction_status': 'POST_AUCTION_QUOTES',
        'auction_price': 10.0, 'auction_at': '2026-10-15T09:27:00+08:00'}, history=[])
    assert row['status'] == 'DATA_LIMITED'
    assert row['reason'] == 'ORIGINAL_AUCTION_TIMESTAMP_REQUIRED'
    assert row['technical_action'] is None


def test_invalidated_plan_is_right_censored_not_proof_of_later_no_signal():
    rows = [{'at': '2026-10-15T09:46:00+08:00', 'technical_action': 'START_CONFIRMATION',
             'recorded_action': 'PLAN_INVALIDATED', 'status': 'EVALUATED',
             'max_conditions_met': 2, 'failing_conditions': ['VWAP'], 'unavailable_conditions': [],
             'reference_price': None}]
    summary = summarize_plan_windows(rows, plan={'symbol': '600000.SH'},
        expires_at='2026-10-15T15:00:00+08:00', captured_at='2026-10-15T12:13:00+08:00')
    assert summary['observation_status'] == 'RIGHT_CENSORED_ORIGINAL_TERMINAL'
    assert summary['post_original_invalidation_signals'] is None
    assert summary['first_technical_signal_at'] is None


def test_reference_returns_are_mature_archived_only_and_not_fills():
    closes = {'2026-10-09': {'close': 11.0, 'closed': True}, '2026-10-13': {'close': 9.0, 'closed': True}}
    result = reference_outcomes('2026-10-08', 10.0, closes,
        sessions=['2026-10-08', '2026-10-09', '2026-10-12', '2026-10-13'],
        captured_at='2026-10-15T12:13:00+08:00', pit_proven=True)
    assert result['T+1']['return'] == pytest.approx(.1)
    assert result['T+3']['return'] == pytest.approx(-.1)
    assert result['basis'] == 'SIGNAL_REFERENCE_RETURN_NOT_EXECUTED_TRADE_OR_PIT_PROOF'
    missing = reference_outcomes('2026-10-15', 10.0, closes, sessions=['2026-10-15'],
        captured_at='2026-10-15T12:13:00+08:00')
    assert missing['T+1']['return'] is None
    assert missing['T+1']['reason'] == 'TARGET_SESSION_NOT_IN_ARCHIVE'


def test_unclosed_current_day_and_missing_reference_do_not_make_zero_return():
    result = reference_outcomes('2026-10-14', 10.0,
        {'2026-10-15': {'close': 11, 'closed': True}}, sessions=['2026-10-14', '2026-10-15'],
        captured_at='2026-10-15T12:13:00+08:00')
    assert result['T+1']['return'] is None
    assert result['T+1']['reason'] == 'TARGET_DAY_NOT_CLOSED_AS_OF_CAPTURE'


def test_missing_pit_identity_is_null_not_inferred_from_symbol_name():
    result = reference_outcomes('2026-10-08', 10.0,
        {'2026-10-09': {'close': 11, 'closed': True}}, sessions=['2026-10-08', '2026-10-09'],
        captured_at='2026-10-15T12:13:00+08:00', pit_proven=False)
    assert result['T+1']['return'] is None
    assert result['T+1']['reason'] == 'PIT_IDENTITY_UNPROVEN'


def test_v0_compares_kernel_and_monitor_while_variant_does_not_reuse_episode(monkeypatch):
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    monkeypatch.setattr(replay, 'build_evaluator', lambda variant: evaluator())
    source = window()
    row = evaluate_window(source, 'V0', evidence={}, history=[])
    assert row['kernel_equal'] is True
    assert row['monitor_equal'] is True
    assert row['replay_final_action'] == 'LLM_VETO'
    assert 'NOT_REINFERRED' in row['final_basis']
    other = evaluate_window(source, 'V1', evidence={}, history=[])
    assert 'replay_final_action' not in other


def test_runner_streams_all_six_rows_and_censors_late_source(tmp_path, monkeypatch):
    import sqlite3
    import json
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    path = tmp_path / 'isolated.sqlite3'
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE execution_plans(plan_id TEXT, symbol TEXT,valid_from TEXT, expires_at TEXT,payload_json TEXT)')
    source = window()
    conn.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?)', ('p', '600000.SH',
        source['plan_row']['valid_from'], source['plan_row']['expires_at'], json.dumps(source['plan'])))
    conn.commit(); conn.close()
    monkeypatch.setattr(replay, 'iter_frozen_windows', lambda **kwargs: iter([source]))
    monkeypatch.setattr(replay, 'build_evaluator', lambda variant: evaluator())
    result = replay.run_variants(state_db=path, market_state_root=tmp_path, output=tmp_path/'out',
        captured_at='2026-10-15T12:13:00+08:00', trade_dates=['2026-10-15'])
    assert result['V0_diff_count'] == 0
    assert len(result['days']) == 6
    rows = [json.loads(line) for line in (tmp_path/'out/plan_metrics.jsonl').read_text(encoding='utf-8').splitlines()]
    assert len(rows) == 6
    assert rows[0]['first_technical_signal_at'] is not None
    assert rows[0]['reference_outcomes']['T+1']['return'] is None
    assert all(row['first_technical_signal_at'] is None for row in rows[3:])
    assert all(row['observation_status'].startswith('RIGHT_CENSORED') for row in rows)


def test_known_permanent_invalidation_is_not_hidden_by_later_partial_capture():
    row = {'at': '2026-10-15T09:46:00+08:00', 'technical_action': 'PLAN_INVALIDATED',
           'recorded_action': 'PLAN_INVALIDATED', 'status': 'EVALUATED'}
    summary = summarize_plan_windows([row], plan={'symbol': '600000.SH'},
        expires_at='2026-10-15T15:00:00+08:00', captured_at='2026-10-15T12:13:00+08:00')
    assert summary['observation_status'] == 'VARIANT_PERMANENTLY_INVALIDATED'


@pytest.mark.parametrize('variant', ['V1', 'V2', 'V3', 'V5'])
@pytest.mark.parametrize('profile', ['MA520_SWING', 'LEADER_INTRADAY'])
def test_non_trend_is_not_counted_as_unchanged_variant_coverage(monkeypatch, variant, profile):
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    monkeypatch.setattr(replay, 'build_evaluator', lambda *args: pytest.fail('Control is not a variant evaluation'))
    row = window(); row['plan']['strategy_profile'] = profile
    result = evaluate_window(row, variant, evidence={}, history=[])
    assert result['status'] == 'NOT_APPLICABLE_CONTROL'
    assert result['technical_action'] is None


def test_complete_entry_leaves_do_not_hide_rr_gate(monkeypatch):
    import liangjian_funnel.evaluation.b3_variant_replay as replay
    from liangjian_funnel.review.observability import TREND_V2_LEAVES
    source = window(); source['plan']['trend_entry_rule_version'] = 'trend-ma5/2'
    matrix = {key: {'met': True, 'available': True, 'kind': 'CONDITION'} for key in TREND_V2_LEAVES}
    matrix['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'] = {'met': False, 'available': True, 'kind': 'CONDITION', 'reason': 'RR_TOO_LOW'}
    monkeypatch.setattr(replay, 'build_evaluator', lambda variant: lambda *a, **kw: SimpleNamespace(
        model_dump=lambda **kw: {'action': 'START_CONFIRMATION', 'reason_codes': ['RR_TOO_LOW'], 'confirmation_results': matrix}))
    result = evaluate_window(source, 'V1', evidence={}, history=[])
    assert result['max_conditions_met'] == 7
    assert result['failing_conditions'] == ['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED']
    assert result['failing_condition_reasons']['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'] == 'RR_TOO_LOW'
