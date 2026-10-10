"""Local fixed fixtures only: no RuntimeStore, provider or production files."""
import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger

SH = ZoneInfo('Asia/Shanghai')


def at(minute='10:00', day='2026-10-08'):
    return datetime.fromisoformat(f'{day}T{minute}:00').replace(tzinfo=SH)


def plan(**kw):
    return dict(plan_id='p1', symbol='600001.SH', profile='TREND_MA5',
                valid_from=at('09:32').isoformat(), valid_until=at('14:00').isoformat(),
                stop_level=9.5, strategy_version='1.5.0', **kw)


def signal(**kw):
    return dict(schema='a4-shadow-signal/1', cohort='REALTIME_SHADOW',
                variant_set_version='shadow-v1', variant_id='V1', plan_id='p1',
                symbol='600001.SH', profile='TREND_MA5', minute=at().isoformat(),
                status='OK', variant_action='BUY_SIGNAL', baseline_action='WAIT',
                baseline_first_cause='ZONE', shadow_inputs_hash='a'*64,
                variant_conditions={}, effective_zone=[9.8, 10.2],
                sequence_evidence={}, elapsed_ms=1, event_kind='FIRST_TRIGGER') | kw


def evidence(**kw):
    return dict(symbol='600001.SH', trade_date='2026-10-08', security_name='普通测试',
                board='SSE_MAIN', security_type='CASH_A_SHARE', security_status='ORDINARY',
                is_st=False, limit_regime='NORMAL', listing_date='2020-01-01',
                rule_effective_from='2026-07-06', preclose=10,
                preclose_basis='EXCHANGE_DISPLAYED_PRECLOSE', observed_at=at('09:26').isoformat(),
                source_ref='fixture:exchange-snapshot', source_input_sha256='b'*64) | kw


def bar(minute='10:01', day='2026-10-08', **kw):
    return dict(symbol='600001.SH', interval='1m', bar_end=at(minute, day).isoformat(),
                captured_at=at(minute, day).isoformat(), source_ref='fixture:raw-minute',
                source_input_sha256='c'*64, complete=True, adjust_mode='none',
                volume_unit='shares', evidence_kind='MARKET_BAR', source_id='fixture-native',
                open=10, high=10.2, low=9.8, close=10, volume=1000, amount=10000,
                upper_limit=11, lower_limit=9) | kw


@pytest.fixture
def ledger(tmp_path):
    return ShadowEvidenceLedger(tmp_path/'shadow.sqlite3', tmp_path/'signals.jsonl')


def first(ledger):
    assert ledger.seal_price_limit_evidence('p1', evidence(), observed_at=at('09:26'))['ok']
    assert ledger.record_signal(signal(), plan(), observed_at=at())['ok']


def test_first_trigger_waits_for_real_next_complete_arrival(ledger):
    first(ledger)
    item = ledger.snapshot()['signals'][0]
    assert item['outcome']['fill_status'] == 'PENDING_NEXT_COMPLETE_MINUTE'
    assert item['outcome']['fill_price'] is None
    assert ledger.advance_outcomes([bar()], observed_at=at())['rejected_bar_count'] == 1
    assert ledger.snapshot()['signals'][0]['outcome']['fill_price'] is None
    assert ledger.advance_outcomes([bar()], observed_at=at('10:01'))['ok']
    item = ledger.snapshot()['signals'][0]
    assert item['outcome']['fill_price'] == 10.01
    assert item['outcome']['fill_bar_end'] == at('10:01').isoformat()
    assert item['outcome']['t1_close_return'] is None
    assert item['account_pnl'] is None


@pytest.mark.parametrize('kw', [dict(complete=False), dict(volume_unit='legacy_unspecified'),
                              dict(captured_at=at('10:02').isoformat()),
                              dict(source_input_sha256=None), dict(interval='5m')])
def test_arrival_evidence_missing_or_incomplete_cannot_fill(ledger, kw):
    first(ledger)
    result = ledger.advance_outcomes([bar(**kw)], observed_at=at('10:01'))
    assert result['rejected_bar_count'] == 1
    assert ledger.snapshot()['signals'][0]['outcome']['fill_price'] is None


def test_missing_exact_minute_not_replaced_by_later_minute(ledger):
    first(ledger)
    ledger.advance_outcomes([bar('10:02')], observed_at=at('10:02'))
    assert 'NEXT_COMPLETE_MINUTE_MISSING' in ledger.snapshot()['signals'][0]['outcome']['reason_codes']


@pytest.mark.parametrize('kw,reason', [
    (dict(preclose=None), 'EXCHANGE_PRECLOSE_UNPROVEN'),
    (dict(security_name='ST测试'), 'ORDINARY_NORMAL_LIMIT_REGIME_UNPROVEN'),
    (dict(listing_date='2026-09-30'), 'NORMAL_LISTING_PERIOD_UNPROVEN'),
    (dict(observed_at=at('09:26', '2026-09-30').isoformat()), 'SOURCE_DATE_UNPROVEN'),
    (dict(limit_regime=None), 'ORDINARY_NORMAL_LIMIT_REGIME_UNPROVEN')])
def test_identity_missing_or_special_is_unknown_not_derived(ledger, kw, reason):
    ledger.seal_price_limit_evidence('p1', evidence(**kw), observed_at=at('09:26'))
    item = ledger.snapshot()['price_limit_evidence'][0]
    assert item['limits']['status'] == 'UNKNOWN'
    assert item['reason_code'] == reason
    assert item['limits']['upper'] is None


def test_exrights_uses_displayed_reference_not_yesterday_close(ledger):
    ledger.seal_price_limit_evidence('p1', evidence(preclose=9, prior_raw_close=10), observed_at=at('09:26'))
    item = ledger.snapshot()['price_limit_evidence'][0]
    assert item['limits']['upper'] == 9.9
    assert item['annotations'] == ['EX_RIGHTS_REFERENCE_OBSERVED']
    ledger.seal_price_limit_evidence('p2', evidence(preclose=9, preclose_basis='DERIVED_RAW_CLOSE'), observed_at=at('09:26'))
    assert ledger.snapshot()['price_limit_evidence'][1]['limits']['status'] == 'UNKNOWN'


def test_review_date_not_silently_extended(ledger):
    e = evidence(trade_date='2026-10-12', observed_at=at('09:26', '2026-10-12').isoformat())
    ledger.seal_price_limit_evidence('p1', e, observed_at=at('09:26', '2026-10-12'))
    assert ledger.snapshot()['price_limit_evidence'][0]['reason_code'] == 'RULE_DATE_UNPROVEN'


def test_raw_response_hash_is_verified_not_authenticated(ledger):
    raw = b'{"preclose":10}'
    bad = ledger.seal_price_limit_evidence('p1', evidence(), observed_at=at('09:26'), raw_response=raw)
    assert not bad['ok'] and bad['error_code'] == 'SOURCE_RESPONSE_HASH_MISMATCH'
    good = evidence(source_input_sha256=hashlib.sha256(raw).hexdigest())
    assert ledger.seal_price_limit_evidence('p1', good, observed_at=at('09:26'), raw_response=raw)['ok']
    assert ledger.snapshot()['price_limit_evidence'][0]['source_authenticated'] is False


def test_explicit_conflicting_derived_prices_block(ledger):
    ledger.seal_price_limit_evidence('p1', evidence(upper_limit=12, lower_limit=9), observed_at=at('09:26'))
    assert ledger.snapshot()['price_limit_evidence'][0]['limits']['status'] == 'CONFLICT'


def test_first_trigger_state_change_and_restart_idempotence(ledger):
    first(ledger)
    assert ledger.record_signal(signal(), plan(), observed_at=at())['duplicate']
    ledger.record_signal(signal(minute=at('10:01').isoformat()), plan(), observed_at=at('10:01'))
    assert len(ledger.snapshot()['signals']) == 1
    ledger.record_signal(signal(minute=at('10:02').isoformat(), variant_action='WAIT', event_kind='STATE_CHANGE'), plan(), observed_at=at('10:02'))
    assert len(ledger.snapshot()['signals']) == 2
    other = ShadowEvidenceLedger(ledger.db_path, ledger.jsonl_path)
    assert other.record_signal(signal(minute=at('10:02').isoformat(), variant_action='WAIT', event_kind='STATE_CHANGE'), plan(), observed_at=at('10:02'))['duplicate']


def test_concurrent_reentry_only_one_trigger(ledger):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: ledger.record_signal(signal(), plan(), observed_at=at()), range(4)))
    assert all(r['ok'] for r in results)
    assert len(ledger.snapshot()['signals']) == 1


def test_data_limited_and_claimed_first_trigger_never_fill(ledger):
    ledger.record_signal(signal(status='DATA_LIMITED'), plan(), observed_at=at())
    ledger.advance_outcomes([bar()], observed_at=at('10:01'))
    assert ledger.snapshot()['signals'][0]['outcome'] is None


def test_sealed_plan_invalidation_blocks_next_fill(ledger):
    p = plan(invalidated_at=at().isoformat())
    result = ledger.record_signal(signal(), p, observed_at=at())
    assert not result['ok'] and result['error_code'] == 'SIGNAL_OUTSIDE_PLAN_VALIDITY'


def test_same_day_stop_only_risk_and_legal_t1_exit(ledger):
    first(ledger)
    ledger.advance_outcomes([bar(), bar('10:02', low=9.4)], observed_at=at('10:02'))
    item = ledger.snapshot()['signals'][0]['outcome']
    assert item['same_day_hard_exit_touched'] is True and item['stop_r'] is None
    ledger.advance_outcomes([bar('09:31', '2026-10-09', open=9.2, high=9.4, low=9, close=9.3)], observed_at=at('09:31','2026-10-09'))
    item = ledger.snapshot()['signals'][0]['outcome']
    assert item['stop_exit_bar_end'] == at('09:31', '2026-10-09').isoformat(), item
    assert item['stop_r'] is not None


def test_horizon_arrival_and_pit_labels_never_use_future(ledger):
    first(ledger)
    ledger.advance_outcomes([bar()], observed_at=at('10:01'), outcome_labels=[
        dict(symbol='600001.SH', trade_date='2026-10-09', raw_close=20,
             captured_at=at('15:00','2026-10-09').isoformat(), source_ref='fixture',source_input_sha256='d'*64)])
    assert ledger.snapshot()['signals'][0]['outcome']['t1_close_return'] is None
    ledger.advance_outcomes([bar('15:00','2026-10-09', close=10.1)], observed_at=at('15:00','2026-10-09'))
    assert ledger.snapshot()['signals'][0]['outcome']['t1_close_return'] == pytest.approx(10.1/10.01-1)


def test_minute_summary_reentry_and_budget_count(ledger):
    summary = dict(minute=at().isoformat(), evaluated_plan_count=2, evaluated_variant_count=8,
                   elapsed_ms=4999, budget_exceeded_count=1)
    assert ledger.record_minute(summary, observed_at=at())['ok']
    assert ledger.record_minute(summary, observed_at=at())['duplicate']
    assert len(ledger.snapshot()['minutes']) == 1


def test_jsonl_matches_sqlite_events_and_failed_mirror_recovers(ledger, monkeypatch):
    original = ledger._write_mirror
    monkeypatch.setattr(ledger, '_write_mirror', lambda: (_ for _ in ()).throw(OSError('secret-not-output')))
    r = ledger.record_signal(signal(), plan(), observed_at=at())
    assert r['ok'] and r['stored'] and r['mirror_status'] == 'PENDING'
    assert 'secret' not in json.dumps(r)
    monkeypatch.setattr(ledger, '_write_mirror', original)
    assert ledger.recover_mirror()['ok']
    rows = [json.loads(line) for line in ledger.jsonl_path.read_text(encoding='utf-8').splitlines()]
    assert rows == ledger.snapshot()['events']


def test_database_failure_never_changes_production_action(tmp_path):
    parent = tmp_path/'not-a-directory'
    parent.write_text('fixture')
    ledger = ShadowEvidenceLedger(parent/'shadow.sqlite3', tmp_path/'shadow.jsonl')
    production = dict(action='WAIT', reason='ORIGINAL')
    r = ledger.record_signal(signal(), plan(), observed_at=at())
    assert not r['ok'] and r['error_code'] == 'SHADOW_EVIDENCE_WRITE_FAILED'
    assert production == dict(action='WAIT', reason='ORIGINAL')


def test_existing_nonshadow_database_not_mutated(tmp_path):
    p = tmp_path/'fixture.sqlite3'
    with sqlite3.connect(p) as db:
        db.execute('CREATE TABLE unrelated(x)')
    before = p.read_bytes()
    ledger = ShadowEvidenceLedger(p, tmp_path/'signals.jsonl')
    assert not ledger.record_signal(signal(), plan(), observed_at=at())['ok']
    assert p.read_bytes() == before


def test_actual_w1_schema_and_minute_contract(ledger):
    s = signal(status='DATA_LIMITED', variant_action=None)
    s['schema_version'] = s.pop('schema')
    s['observation_time'] = at().isoformat()
    p = plan()
    p['strategy_profile'] = p.pop('profile')
    assert ledger.record_signal(s, p, observed_at=at())['ok']
    assert ledger.snapshot()['signals'][0]['outcome'] is None
    summary = dict(schema_version='a4-shadow-minute/1', cohort='REALTIME_SHADOW',
                   minute=at().isoformat(), evaluated_plan_count=2,
                   evaluated_variant_count=8, shadow_budget_exceeded_count=1, elapsed_ms=4999)
    assert ledger.record_minute(summary, observed_at=at())['ok']


def test_decision_clock_not_rewound_to_reference_minute(ledger):
    assert not ledger.record_signal(signal(observation_time=at('10:01').isoformat()), plan(), observed_at=at())['ok']


def test_signal_plan_field_and_schema_alias_conflicts_reject(ledger):
    assert not ledger.record_signal(signal(schema_version='other'), plan(), observed_at=at())['ok']
    assert not ledger.record_signal(signal(), plan(strategy_profile='MA520_SWING'), observed_at=at())['ok']


def test_name_missing_cannot_derive_ordinary_identity(ledger):
    ledger.seal_price_limit_evidence('p1', evidence(security_name=None), observed_at=at('09:26'))
    item = ledger.snapshot()['price_limit_evidence'][0]
    assert item['limits']['status'] == 'UNKNOWN'
    assert item['reason_code'] == 'EXCHANGE_NAME_UNPROVEN'


def test_duplicate_same_bar_later_capture_preserves_first_arrival(ledger):
    first(ledger)
    ledger.advance_outcomes([bar()], observed_at=at('10:01'))
    r = ledger.advance_outcomes([bar(captured_at=at('10:02').isoformat())], observed_at=at('10:02'))
    assert r['conflicting_bar_count'] == 0
    assert ledger.snapshot()['signals'][0]['outcome']['fill_status'] == 'FILLED'


def test_clock_regression_rejects_entire_outcome_update(ledger):
    first(ledger)
    ledger.advance_outcomes([bar()], observed_at=at('10:01'))
    r = ledger.advance_outcomes([], observed_at=at())
    assert not r['ok'] and r['error_code'] == 'ARRIVAL_CLOCK_REGRESSED'
    assert ledger.snapshot()['signals'][0]['outcome']['fill_status'] == 'FILLED'


def test_no_unproven_limit_fields_from_plan(ledger):
    ledger.record_signal(signal(), plan(upper_limit=11, lower_limit=9), observed_at=at())
    ledger.advance_outcomes([bar(upper_limit=None, lower_limit=None)], observed_at=at('10:01'))
    item = ledger.snapshot()['signals'][0]['outcome']
    assert item['fill_price'] is None and 'PRICE_LIMITS_UNKNOWN' in item['reason_codes']


def test_locked_and_outside_price_bar_have_no_fill(ledger):
    first(ledger)
    ledger.advance_outcomes([bar(open=11, high=11, low=11, close=11)], observed_at=at('10:01'))
    assert 'LIMIT_UP_LOCKED' in ledger.snapshot()['signals'][0]['outcome']['reason_codes']


def test_unknown_prior_day_limits_not_moved_to_t1_exit(ledger):
    first(ledger)
    ledger.advance_outcomes([bar(), bar('10:02',low=9.4)], observed_at=at('10:02'))
    ledger.advance_outcomes([bar('09:31','2026-10-09', open=9.2,high=9.4,low=9,close=9.3,
                                upper_limit=None, lower_limit=None)], observed_at=at('09:31','2026-10-09'))
    item = ledger.snapshot()['signals'][0]['outcome']
    assert item['stop_r'] is None and 'PRICE_LIMITS_UNKNOWN' in item['reason_codes']


def test_foreign_mirror_is_not_overwritten(ledger):
    ledger.jsonl_path.write_text('unrelated material\n',encoding='utf-8')
    r = ledger.record_signal(signal(),plan(),observed_at=at())
    assert r['mirror_status'] == 'PENDING'
    assert ledger.jsonl_path.read_text(encoding='utf-8') == 'unrelated material\n'


def test_torn_own_mirror_prefix_recovered_and_hash_chain_valid(ledger):
    first(ledger)
    raw = ledger.jsonl_path.read_bytes()
    ledger.jsonl_path.write_bytes(raw[:-10])
    assert ledger.recover_mirror()['ok']
    events = ledger.snapshot()['events']
    for i, event in enumerate(events):
        assert event['previous_event_sha256'] == (events[i-1]['event_sha256'] if i else None)
        plain = dict(event)
        digest = plain.pop('event_sha256')
        assert hashlib.sha256(json.dumps(plain,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()).hexdigest() == digest
    assert ledger.jsonl_path.read_bytes() == raw


def test_event_pins_implementation_and_shared_contracts(ledger):
    first(ledger)
    event = ledger.snapshot()['events'][0]
    pins = event['implementation_source_sha256']
    assert set(pins) == {'shadow_evidence.py', 'outcomes.py', 'price_limits.py', 'simulation.py', 'stock_trading_rules.py'}
    assert all(len(value) == 64 for value in pins.values())


def test_transaction_failure_has_no_partial_signal_or_claimed_success(ledger, monkeypatch):
    monkeypatch.setattr(ledger, '_event', lambda *args: (_ for _ in ()).throw(sqlite3.OperationalError('fixture')))
    receipt = ledger.record_signal(signal(),plan(),observed_at=at())
    assert not receipt['ok'] and not receipt['stored']
    assert ledger.snapshot()['signals'] == []


def test_partial_reference_minute_does_not_rewind_trigger_knowledge(ledger):
    known = at().replace(second=15)
    ledger.record_signal(signal(),plan(),observed_at=known)
    ledger.advance_outcomes([bar()],observed_at=at('10:01'))
    assert ledger.snapshot()['signals'][0]['outcome']['fill_price'] is None
    ledger.advance_outcomes([bar('10:02')],observed_at=at('10:02'))
    assert ledger.snapshot()['signals'][0]['outcome']['fill_bar_end'] == at('10:02').isoformat()


def test_real_dispatch_cutoff_and_arrival_clocks_are_distinct(ledger):
    s = signal(minute=at('13:06').isoformat(), observation_time=at('13:05').isoformat())
    known = at('13:06').replace(second=5)
    assert ledger.record_signal(s,plan(),observed_at=known)['ok']
    item = ledger.snapshot()['signals'][0]
    assert item['trigger_at'] == known.isoformat()
    assert item['outcome']['expected_bar_end'] == at('13:08').isoformat()


def test_pure_reader_mode_ro_hash_chain_and_no_schema_writes(ledger):
    from liangjian_funnel.runtime import shadow_evidence
    first(ledger)
    before = ledger.db_path.read_bytes()
    result = shadow_evidence.read_shadow_evidence(ledger.db_path, ledger.jsonl_path)
    assert result['ok'] and result['hash_chain_status'] == 'MATCHED'
    assert result['mirror_status'] == 'SYNCED'
    assert result['signals'] == ledger.snapshot()['signals']
    assert ledger.db_path.read_bytes() == before


@pytest.mark.parametrize('table', ['shadow_identity','shadow_minutes'])
def test_pure_reader_material_projection_must_match_journal(ledger, table):
    from liangjian_funnel.runtime.shadow_evidence import read_shadow_evidence
    first(ledger)
    ledger.record_minute(dict(minute=at().isoformat(), evaluated_plan_count=1,
                              evaluated_variant_count=1,budget_exceeded_count=0,elapsed_ms=1),observed_at=at())
    with sqlite3.connect(ledger.db_path) as db:
        key, raw = db.execute(f'SELECT id,record FROM {table}').fetchone()
        item = json.loads(raw)
        item['tampered'] = True
        db.execute(f'UPDATE {table} SET record=? WHERE id=?',(json.dumps(item),key))
    result = read_shadow_evidence(ledger.db_path)
    assert not result['ok'] and result['error_code'] == 'SHADOW_MATERIAL_PROJECTION_INVALID'


def test_w1_outer_baseline_summary_is_preserved_not_authenticated(ledger):
    outer = [{'plan_id':'p1','source_scope':'CALLER_SUPPLIED_NOT_REEVALUATED',
              'actual_outer_baseline':None,'outer_evidence_status':'MISSING'}]
    summary = dict(schema_version='a4-shadow-minute/2', minute=at().isoformat(),
                   evaluated_plan_count=1,evaluated_variant_count=1,elapsed_ms=1,
                   shadow_budget_exceeded_count=0,baseline_records=outer)
    ledger.record_minute(summary,observed_at=at())
    result = ledger.snapshot()
    assert result['minutes'][0]['baseline_records'] == outer
    assert result['source_authenticated'] is False
