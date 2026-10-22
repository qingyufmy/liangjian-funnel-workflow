"""Controlled replay safety tests; these fixtures are NOT today's originals."""
from copy import deepcopy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import socket

import pytest

from test_entry_confirmation import intraday_minutes, integrated_plan, daily, CFG, server_context


spec = importlib.util.spec_from_file_location('replay_a4_originals_0589',
    Path(__file__).resolve().parents[1] / 'scripts/replay_a4_originals_0589.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def packet():
    bars = [{**row, 'bar_end': row['bar_end'].isoformat(), 'source_id': 'CONTROLLED_TEST_MARKET',
        'evidence_kind': 'MARKET_BAR'} for row in intraday_minutes()]
    at = bars[-1]['bar_end']
    snapshot_id = 'CONTROLLED_TEST_SNAPSHOT'
    raw = module.canonical(bars)
    plan = dict(plan_id='controlled-p1', symbol='600001.SH', payload_json=module.canonical(integrated_plan()))
    payload = dict(plan_id=plan['plan_id'], symbol=plan['symbol'], minute_snapshot_id=snapshot_id,
        strategy=dict(reason_codes=['CONTROLLED_WAIT'], entry_confirmation=dict(status='NOT_CONFIRMED', conditions={})))
    event = dict(event_id='controlled-event', plan_id=plan['plan_id'], minute_end=at,
        action='NO_ACTION', reason_code='CONTROLLED_WAIT', effective=0, payload_json=module.canonical(payload))
    body = dict(minute_snapshot=dict(snapshot_id=snapshot_id, symbol=plan['symbol'], interval='1m',
        decision_as_of=at, payload_json=raw, payload_sha256=module.hashlib.sha256(raw.encode()).hexdigest()),
        config=deepcopy(CFG), daily_facts=daily(), historical_five=[])
    record = dict(event=event, event_sha256=module.digest(event), plan=plan, plan_sha256=module.digest(plan),
        frozen_inputs=dict(source_ref='CONTROLLED_TEST_ONLY', body=body, sha256=module.digest(body)))
    return dict(schema_version='a4-originals-packet/1', trade_date=at[:10],
        capture_kind='CONTROLLED_TEST_ONLY_NOT_PRODUCTION', records=[record])


def rebind(value):
    row = value['records'][0]
    row['event_sha256'] = module.digest(row['event'])
    row['plan_sha256'] = module.digest(row['plan'])
    if 'frozen_inputs' in row:
        row['frozen_inputs']['sha256'] = module.digest(row['frozen_inputs']['body'])


def test_bounded_pure_pattern_retains_controlled_label_and_does_not_claim_full_strategy(monkeypatch):
    def forbidden(*a, **kw): pytest.fail('network forbidden')
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    value = packet(); before = deepcopy(value)
    result = module.replay_packet(value)
    assert value == before
    assert result['capture_kind_declared'] == 'CONTROLLED_TEST_ONLY_NOT_PRODUCTION'
    assert result['model_http_calls'] == 0 and result['complete_strategy_count'] == 0
    row = result['records'][0]
    assert row['status'] == 'PURE_PATTERN_REPLAY'
    assert row['actual_buy_impact'] in {'UNKNOWN_WITHOUT_COMPLETE_STRATEGY_INPUTS',
        'NO_RECORDED_BUY_IN_SAMPLE;PATTERN_STILL_UNCONFIRMED;OTHER_GATES_NOT_REPLAYED'}
    assert row['missing'] == ['EXACT_MARKET_CONTEXT', 'EXACT_POSITION_OVERLAY']


def test_missing_original_inputs_preserves_recorded_reason_without_making_up_replay():
    value = packet(); value['records'][0].pop('frozen_inputs')
    result = module.replay_packet(value)
    row = result['records'][0]
    assert result['status'] == 'INCOMPLETE_ORIGINALS'
    assert row['original_reason'] == 'CONTROLLED_WAIT' and row['conditions'] == []
    assert 'candidate_pattern' not in row


@pytest.mark.parametrize('target', ['event', 'plan', 'frozen_inputs'])
def test_rejects_hash_tampering_before_any_replay(target):
    value = packet()
    if target == 'frozen_inputs': value['records'][0][target]['sha256'] = '0' * 64
    else: value['records'][0][target + '_sha256'] = '0' * 64
    with pytest.raises(ValueError): module.replay_packet(value)


@pytest.mark.parametrize('case', ['synthetic', 'risk_only', 'future', 'wrong_symbol', 'unclosed'])
def test_never_uses_synthetic_future_or_other_identity_as_today_bar(case):
    value = packet(); body = value['records'][0]['frozen_inputs']['body']; snap = body['minute_snapshot']
    bars = json.loads(snap['payload_json'])
    if case == 'synthetic': bars[0]['evidence_kind'] = 'SYNTHETIC_QUOTE'
    elif case == 'risk_only': bars[0]['source_id'] = 'QUOTE:RISK_ONLY'
    elif case == 'future': bars[-1]['bar_end'] = (module._time(snap['decision_as_of']) + timedelta(minutes=1)).isoformat()
    elif case == 'wrong_symbol': bars[0]['symbol'] = '600002.SH'
    else: bars[0]['closed'] = False
    snap['payload_json'] = module.canonical(bars)
    snap['payload_sha256'] = module.hashlib.sha256(snap['payload_json'].encode()).hexdigest()
    rebind(value)
    with pytest.raises(ValueError, match='SYNTHETIC_FUTURE_OR_WRONG_BAR'): module.replay_packet(value)


def test_rejects_minute_byte_sha_and_execution_projection_sha_mismatch():
    value = packet(); body = value['records'][0]['frozen_inputs']['body']
    body['minute_snapshot']['payload_sha256'] = '0' * 64; rebind(value)
    with pytest.raises(ValueError, match='SNAPSHOT_HASH_MISMATCH'): module.replay_packet(value)
    value = packet(); body = value['records'][0]['frozen_inputs']['body']
    body['expected_execution_input_sha256'] = '0' * 64; rebind(value)
    with pytest.raises(ValueError, match='EXECUTION_INPUT_BINDING_MISMATCH'): module.replay_packet(value)


@pytest.mark.parametrize('count', [0, 49])
def test_empty_or_unbounded_domain_cannot_claim_completed_replay(count):
    value = packet(); value['records'] = value['records'] * count
    with pytest.raises(ValueError, match='DOMAIN_MISSING_OR_OVERSIZE'): module.replay_packet(value)


def test_full_pure_strategy_requires_actual_matching_context_not_a_ready_stub():
    value = packet(); body = value['records'][0]['frozen_inputs']['body']
    body['market_context'] = {'status': 'READY'}; body['position'] = None; rebind(value)
    with pytest.raises(ValueError, match='CONTEXT_PATTERN_BINDING_CONFLICT'): module.replay_packet(value)
    body['market_context'] = server_context(module._time(body['minute_snapshot']['decision_as_of']),
        entry_confirmation_config=body['config'], entry_confirmation_daily_facts=body['daily_facts'],
        historical_5m=body['historical_five'])
    rebind(value)
    result = module.replay_packet(value)
    assert result['complete_strategy_count'] == 1 and result['records'][0]['status'] == 'PURE_STRATEGY_REPLAY'
    assert result['capture_kind_declared'] == 'CONTROLLED_TEST_ONLY_NOT_PRODUCTION'
