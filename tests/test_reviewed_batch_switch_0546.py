"""One workflow transaction; synthetic reviews never stand for model evidence."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import sqlite3
import threading

import pytest

from liangjian_funnel.runtime.state import RuntimeStore, StateTransitionError, PersistenceError
from liangjian_funnel.runtime.reviewed_batch_switch import canonical, rowset_sha256, load_active_reviewed_batch

DAY = '2026-10-22'
EXP = DAY + 'T15:00:00+08:00'


@pytest.fixture(autouse=True)
def controlled_clock(monkeypatch):
    # Unit-test clock only, never an application/OS clock mutation.
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now',
        lambda: datetime.fromisoformat(DAY + 'T08:20:00+08:00'))


def plan(pid, run='old', symbol='600000.SH'):
    return dict(plan_id=pid, lane_id='lane_1', symbol=symbol, status='PENDING_MORNING_REVIEW',
        expires_at=EXP, payload=dict(symbol=symbol, source_run_id=run, target_trade_date=DAY,
            risk_unit='PROBE', trigger_low=10, trigger_high=11, stop_level=9, no_chase=12))


def bundle(scope='scope-new'):
    rows = [plan(scope + ':p1', scope), plan(scope + ':p2', scope, '600001.SH')]
    source_rows = [dict(plan_id=r['plan_id'], symbol=r['symbol'], status=r['status'],
        expires_at=r['expires_at'], payload_json=canonical(r['payload'])) for r in rows]
    source = dict(status='COMPLETE', trade_date=DAY, lane_id='lane_1', plan_rows=source_rows, observed_at=DAY + 'T08:00:00+08:00',
        plan_rowset_sha256=hashlib.sha256(canonical(source_rows).encode()).hexdigest())
    engine = dict(status='COMPLETE', trade_date=DAY, account_id='local-paper', source=source,
        observed_at=DAY + 'T08:00:00+08:00', snapshot={'controlled': True}, held_plan_bindings=[],
        capacity_slots=6, cluster_caps={'bank': 2}, baseline_range=[10, 30], engine_utilization_cap=30,
        risk_limits_sha256='d' * 64)
    risk = dict(status='READY', consecutive_failures=0, reason_codes=[], utilization_cap='30',
        envelopes=[dict(plan_id=r['plan_id'], symbol=r['symbol'], trade_date=DAY, eligible=True,
            risk_unit='PROBE', risk_budget='100', max_batches=1, max_shares_cap=100,
            blocked_reasons=[], binding_limits=[], purpose='PLANNING') for r in rows])
    engine['envelopes'] = deepcopy(risk['envelopes'])
    evidence = {role: dict(call_id=role + ':' + scope, meeting_id=role + '-meeting:' + scope,
        raw_sha256=('a' if role == 'a6' else 'b') * 64, input_sha256='c' * 64,
        journal_ref='controlled:model.db#' + role + ':' + scope, validated=True) for role in ('a6', 'pm')}
    pm = dict(status='READY', model_validated=True, decision_authority='VALIDATED_MODEL', session='PM_PREMARKET',
        trade_date=DAY, source_call_id=evidence['pm']['call_id'],
        server_projection={'raw_output_sha256': evidence['pm']['raw_sha256']},
        allocation_plan_bindings={r['plan_id']: [r['symbol'], 'bank'] for r in rows},
        slot_plan=[dict(slot=1, primary_plan_id=rows[0]['plan_id'], backup_plan_id='', cluster='bank')],
        plan_allocations=[dict(plan_id=rows[0]['plan_id'], intended_risk_unit='PROBE', max_batches=1)])
    evidence['a6'].update(risk_result_sha256=hashlib.sha256(canonical(risk).encode()).hexdigest(),
        engine_envelopes_sha256=hashlib.sha256(canonical(engine['envelopes']).encode()).hexdigest(),
        plan_rowset_sha256=source['plan_rowset_sha256'])
    evidence['a6']['meeting_record'] = dict(meeting_id=evidence['a6']['meeting_id'],
        source_call_id=evidence['a6']['call_id'], input_sha256=evidence['a6']['input_sha256'],
        result_sha256=evidence['a6']['risk_result_sha256'])
    return dict(schema_version='reviewed-batch/1', switch_mode='PRE_AUCTION', scope_id=scope, status='READY', account_id='local-paper',
        trade_date=DAY, lane_id='lane_1', run_id=scope, engine=engine, risk=risk, pm=pm, evidence=evidence, candidate_rows=rows)


def fixture(tmp_path):
    store = RuntimeStore(tmp_path / 'workflow.sqlite3')
    store.publish_plan_batch([plan(str(i)) for i in range(3)])
    old = list(store.list_execution_plans(status='PENDING_MORNING_REVIEW'))
    return store, old, dict(expected_old_plan_ids=[r['plan_id'] for r in old],
        expected_old_rows_sha256=rowset_sha256(old), expected_pointer_revision=None)


def state(store):
    with store._connect() as db:
        return {table: [tuple(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
            for table in ('execution_plans', 'plan_supersessions', 'active_reviewed_batch_scope', 'reviewed_batch_switch_audit')}


def test_stage_inert_then_one_atomic_switch_and_immutable_old_payload(tmp_path):
    store, old, cas = fixture(tmp_path)
    before = state(store)
    body = bundle()
    staged = store.stage_reviewed_batch(body)
    assert state(store) == before
    assert load_active_reviewed_batch(store.path, account_id='local-paper', trade_date=DAY) is None
    result = store.commit_reviewed_batch_switch(body['scope_id'], **cas)
    assert result['pointer_revision'] == 1 and result['bundle_sha256'] == staged['bundle_sha256']
    assert all(store.get_execution_plan(r['plan_id'])['status'] == 'SUPERSEDED' for r in old)
    assert all(store.get_execution_plan(r['plan_id'])['payload_json'] == r['payload_json'] for r in old)
    active = load_active_reviewed_batch(store.path, account_id='local-paper', trade_date=DAY)
    assert active['scope_id'] == body['scope_id'] and active['bundle']['engine'] == body['engine']
    assert len(state(store)['reviewed_batch_switch_audit']) == 1
    assert store.commit_reviewed_batch_switch(body['scope_id'], **cas)['idempotent']


@pytest.mark.parametrize('change', [
    lambda b: b.update(status='PENDING'),
    lambda b: b['risk'].update(status='DEGRADED'),
    lambda b: b['risk'].update(consecutive_failures=1),
    lambda b: b['pm'].update(model_validated=False),
    lambda b: b['pm'].update(decision_authority='SERVER_FALLBACK_NOT_MODEL_VALIDATED'),
    lambda b: b['pm'].update(fallback_observed_at=DAY + 'T08:00:00+08:00'),
    lambda b: b['evidence']['a6'].update(validated=False),
    lambda b: b['evidence']['pm'].update(raw_sha256='d' * 64),
    lambda b: b['evidence']['pm'].pop('journal_ref'),
    lambda b: b['evidence']['pm'].pop('input_sha256'),
    lambda b: b.update(candidate_rows=[]),
    lambda b: b['engine']['source'].update(plan_rowset_sha256='0' * 64),
    lambda b: b['engine']['source']['plan_rows'][0].update(payload_json='{}'),
    lambda b: b['candidate_rows'][0].update(status='ACTIVE_TODAY'),
    lambda b: b['candidate_rows'][0].update(parent_plan_id='0'),
    lambda b: b['candidate_rows'][0]['payload'].update(source_run_id='old'),
    lambda b: b['pm'].update(allocation_plan_bindings={}),
    lambda b: b['risk'].update(envelopes=[]),
])
def test_unreviewed_or_mismatched_bundle_never_changes_old_three(tmp_path, change):
    store, _, _ = fixture(tmp_path)
    before = state(store)
    body = bundle(); change(body)
    with pytest.raises((StateTransitionError, ValueError)):
        store.stage_reviewed_batch(body)
    assert state(store) == before


def test_bundle_immutable_and_same_bytes_idempotent(tmp_path):
    store, _, _ = fixture(tmp_path)
    body = bundle()
    first = store.stage_reviewed_batch(body)
    assert store.stage_reviewed_batch(body) == first
    body['engine']['snapshot']['controlled'] = False
    with pytest.raises(StateTransitionError, match='IMMUTABLE_CONFLICT'):
        store.stage_reviewed_batch(body)


@pytest.mark.parametrize('concurrent', ['extra_pending', 'changed_rows', 'pointer_revision'])
def test_cas_rejects_concurrency_without_publishing_or_retiring(tmp_path, concurrent):
    store, _, cas = fixture(tmp_path)
    body = bundle(); store.stage_reviewed_batch(body)
    if concurrent == 'extra_pending': store.publish_plan_batch([plan('concurrent')])
    elif concurrent == 'changed_rows': cas['expected_old_rows_sha256'] = '0' * 64
    elif concurrent == 'pointer_revision': cas['expected_pointer_revision'] = 1
    before = state(store)
    with pytest.raises(StateTransitionError, match='CAS_FAILED'):
        store.commit_reviewed_batch_switch(body['scope_id'], **cas)
    assert state(store) == before
    assert store.get_execution_plan(body['candidate_rows'][0]['plan_id']) is None


def test_already_active_all_three_exact_cas_can_switch_without_deleting_owner(tmp_path):
    store, old, cas = fixture(tmp_path)
    for row in old: store.activate_plan(row['plan_id'], valid_from=DAY + 'T09:26:00+08:00')
    held = plan('held-002270', run='held-prior', symbol='002270.SZ')
    store.publish_plan_batch([held]); store.activate_plan('held-002270', valid_from=DAY + 'T09:26:00+08:00')
    old = [store.get_execution_plan(row['plan_id']) for row in old]
    cas['expected_old_rows_sha256'] = rowset_sha256(old)
    held_before = store.get_execution_plan('held-002270')
    body = bundle(); store.stage_reviewed_batch(body)
    store.commit_reviewed_batch_switch(body['scope_id'], **cas)
    assert all(store.get_execution_plan(r['plan_id'])['status'] == 'SUPERSEDED' for r in old)
    assert all(store.get_execution_plan(r['plan_id'])['payload_json'] == r['payload_json'] for r in old)
    assert store.get_execution_plan('held-002270') == held_before


def test_held_active_plan_preserved_on_successful_pending_switch(tmp_path):
    store, _, cas = fixture(tmp_path)
    held = plan('held-002270', run='held-prior', symbol='002270.SZ')
    store.publish_plan_batch([held]); store.activate_plan('held-002270', valid_from=DAY + 'T09:26:00+08:00')
    held_before = store.get_execution_plan('held-002270')
    store.stage_reviewed_batch(bundle())
    store.commit_reviewed_batch_switch('scope-new', **cas)
    assert store.get_execution_plan('held-002270') == held_before


def test_exception_after_plan_insert_rolls_back_all_retirement_and_pointer(tmp_path):
    store, _, cas = fixture(tmp_path)
    store.stage_reviewed_batch(bundle())
    with store._connect() as db:
        db.execute("CREATE TRIGGER controlled_switch_failure BEFORE INSERT ON reviewed_batch_switch_audit BEGIN SELECT RAISE(ABORT,'controlled'); END")
    before = state(store)
    with pytest.raises(StateTransitionError): store.commit_reviewed_batch_switch('scope-new', **cas)
    assert state(store) == before


def test_two_racing_reviewed_batches_only_one_wins(tmp_path):
    store, _, cas = fixture(tmp_path)
    for scope in ('one', 'two'): store.stage_reviewed_batch(bundle(scope))
    ready = threading.Barrier(2)
    def commit(scope):
        own = RuntimeStore(store.path)
        ready.wait(timeout=5)
        try: own.commit_reviewed_batch_switch(scope, **cas); return scope
        except StateTransitionError: return None
    with ThreadPoolExecutor(2) as pool:
        winners = list(pool.map(commit, ('one', 'two')))
    assert len([winner for winner in winners if winner]) == 1
    assert len(state(store)['reviewed_batch_switch_audit']) == 1
    assert len(state(store)['plan_supersessions']) == 3


def test_legacy_read_missing_tables_does_not_migrate_database(tmp_path):
    path = tmp_path / 'legacy.sqlite3'
    with sqlite3.connect(path) as db: db.execute('CREATE TABLE legacy(x)')
    before = path.read_bytes()
    assert load_active_reviewed_batch(path, account_id='local-paper', trade_date=DAY) is None
    assert path.read_bytes() == before


def test_reader_detects_hash_tampering_without_silent_legacy_fallback(tmp_path):
    store, _, cas = fixture(tmp_path)
    store.stage_reviewed_batch(bundle()); store.commit_reviewed_batch_switch('scope-new', **cas)
    with store._connect() as db: db.execute("UPDATE active_reviewed_batch_scope SET bundle_sha256=?", ('0' * 64,))
    with pytest.raises(ValueError, match='POINTER_UNPROVEN'):
        load_active_reviewed_batch(store.path, account_id='local-paper', trade_date=DAY)


@pytest.mark.parametrize('scope', ['x' * 41, 'quote\'bad', 'bad\nline', '中文', 'a:b', ''])
def test_scope_identity_is_bounded_ascii_for_monitor_and_journal(tmp_path, scope):
    store, _, _ = fixture(tmp_path)
    body = bundle(); body['scope_id'] = scope
    with pytest.raises(StateTransitionError, match='IDENTITY_INVALID'): store.stage_reviewed_batch(body)


def test_run_identity_keeps_existing_64_character_contract(tmp_path):
    store, _, _ = fixture(tmp_path)
    body = bundle(); body['run_id'] = 'x' * 65
    with pytest.raises(StateTransitionError, match='IDENTITY_INVALID'): store.stage_reviewed_batch(body)


@pytest.mark.parametrize('key', ['meeting_id', 'source_call_id', 'input_sha256', 'result_sha256'])
def test_a6_true_meeting_record_must_bind_result_and_actual_request(tmp_path, key):
    store, _, _ = fixture(tmp_path)
    body = bundle(); body['evidence']['a6']['meeting_record'][key] = '0' * 64
    with pytest.raises(StateTransitionError, match='A6_RESULT_BINDING_CONFLICT'): store.stage_reviewed_batch(body)


@pytest.mark.parametrize('key', ['risk_result_sha256', 'engine_envelopes_sha256', 'plan_rowset_sha256'])
def test_a6_body_hashes_are_recomputed_not_green_labels(tmp_path, key):
    store, _, _ = fixture(tmp_path)
    body = bundle(); body['evidence']['a6'][key] = '0' * 64
    with pytest.raises(StateTransitionError, match='A6_RESULT_BINDING_CONFLICT'): store.stage_reviewed_batch(body)


@pytest.mark.parametrize('stamp', [DAY + 'T09:26:00+08:00', DAY + 'T15:30:00+08:00',
    '2026-10-21T23:59:00+08:00', '2026-10-23T08:20:00+08:00'])
def test_commit_only_actual_target_day_before_auction_without_fabricating_activation(tmp_path, monkeypatch, stamp):
    store, _, cas = fixture(tmp_path); store.stage_reviewed_batch(bundle())
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now', lambda: datetime.fromisoformat(stamp))
    before = state(store)
    with pytest.raises(StateTransitionError): store.commit_reviewed_batch_switch('scope-new', **cas)
    assert state(store) == before


def test_future_observations_cannot_be_staged(tmp_path):
    store, _, _ = fixture(tmp_path)
    body = bundle(); body['engine']['observed_at'] = DAY + 'T08:21:00+08:00'
    with pytest.raises(StateTransitionError, match='OBSERVATION_IN_FUTURE'): store.stage_reviewed_batch(body)


def test_mixed_active_pending_rows_switch_exactly_and_new_rows_remain_pending(tmp_path):
    store, old, cas = fixture(tmp_path)
    store.activate_plan(old[0]['plan_id'], valid_from=DAY + 'T09:26:00+08:00')
    old = [store.get_execution_plan(r['plan_id']) for r in old]
    cas['expected_old_rows_sha256'] = rowset_sha256(old)
    body = bundle(); store.stage_reviewed_batch(body)
    store.commit_reviewed_batch_switch('scope-new', **cas)
    assert all(store.get_execution_plan(r['plan_id'])['status'] == 'SUPERSEDED' for r in old)
    assert all(store.get_execution_plan(r['plan_id'])['status'] == 'PENDING_MORNING_REVIEW'
        and store.get_execution_plan(r['plan_id'])['valid_from'] is None for r in body['candidate_rows'])


def test_activation_after_cas_observation_rejects_without_retiring_or_publishing(tmp_path):
    store, old, cas = fixture(tmp_path); store.stage_reviewed_batch(bundle())
    store.activate_plan(old[0]['plan_id'], valid_from=DAY + 'T09:26:00+08:00')
    before = state(store)
    with pytest.raises(StateTransitionError, match='OLD_SCOPE_CAS_FAILED'):
        store.commit_reviewed_batch_switch('scope-new', **cas)
    assert state(store) == before


@pytest.mark.parametrize('change', ['omit_sibling', 'competing_pending', 'multiple_old_runs'])
def test_exact_old_batch_cas_never_omits_or_combines_batches(tmp_path, change):
    store, old, cas = fixture(tmp_path); store.stage_reviewed_batch(bundle())
    if change == 'omit_sibling':
        cas['expected_old_plan_ids'] = [r['plan_id'] for r in old[:2]]
        cas['expected_old_rows_sha256'] = rowset_sha256(old[:2])
    elif change == 'competing_pending': store.publish_plan_batch([plan('new-other-run', run='other')])
    else:
        with store._connect() as db:
            altered = json.loads(old[0]['payload_json']); altered['source_run_id'] = 'another'
            db.execute('UPDATE execution_plans SET payload_json=? WHERE plan_id=?', (canonical(altered), old[0]['plan_id']))
        cas['expected_old_rows_sha256'] = rowset_sha256([store.get_execution_plan(r['plan_id']) for r in old])
    before = state(store)
    with pytest.raises(StateTransitionError, match='OLD_SCOPE_CAS_FAILED'):
        store.commit_reviewed_batch_switch('scope-new', **cas)
    assert state(store) == before


def test_idempotence_never_masks_corrupted_published_rows(tmp_path):
    store, _, cas = fixture(tmp_path); store.stage_reviewed_batch(bundle())
    store.commit_reviewed_batch_switch('scope-new', **cas)
    with store._connect() as db: db.execute("UPDATE execution_plans SET payload_json='{}' WHERE plan_id='scope-new:p1'")
    before = state(store)
    with pytest.raises(StateTransitionError, match='IDEMPOTENT_PLAN_CONFLICT'):
        store.commit_reviewed_batch_switch('scope-new', **cas)
    assert state(store) == before


def test_reader_checks_bundle_table_digest_not_only_pointer_digest(tmp_path):
    store, _, cas = fixture(tmp_path); store.stage_reviewed_batch(bundle())
    store.commit_reviewed_batch_switch('scope-new', **cas)
    with store._connect() as db: db.execute("UPDATE reviewed_batch_bundles SET bundle_sha256=?", ('0' * 64,))
    with pytest.raises(ValueError, match='POINTER_UNPROVEN'):
        load_active_reviewed_batch(store.path, account_id='local-paper', trade_date=DAY)


@pytest.mark.parametrize('reason', ['empty_pm', 'all_a6_ineligible', 'only_backup_eligible',
    'zero_shares', 'zero_budget', 'zero_batches', 'none_risk_unit'])
def test_ready_but_no_allocatable_primary_never_replaces_old_domain(tmp_path, reason):
    store, _, _ = fixture(tmp_path)
    before = state(store)
    body = bundle()
    if reason == 'empty_pm': body['pm'].update(slot_plan=[], plan_allocations=[])
    elif reason == 'all_a6_ineligible':
        for envelope in body['risk']['envelopes']: envelope['eligible'] = False
    elif reason == 'only_backup_eligible':
        body['risk']['envelopes'][0]['eligible'] = False
        body['pm']['slot_plan'][0]['backup_plan_id'] = body['candidate_rows'][1]['plan_id']
        body['pm']['plan_allocations'].append(dict(plan_id=body['candidate_rows'][1]['plan_id'],
            intended_risk_unit='PROBE', max_batches=1))
    else:
        field, value = {'zero_shares': ('max_shares_cap', 0), 'zero_budget': ('risk_budget', '0'),
            'zero_batches': ('max_batches', 0), 'none_risk_unit': ('risk_unit', 'NONE')}[reason]
        body['risk']['envelopes'][0][field] = value
    # Explicitly rebind the controlled original result: test the business gate,
    # not an incidental stale result hash.
    digest = hashlib.sha256(canonical(body['risk']).encode()).hexdigest()
    body['evidence']['a6']['risk_result_sha256'] = digest
    body['evidence']['a6']['meeting_record']['result_sha256'] = digest
    with pytest.raises(StateTransitionError, match='NO_ALLOCATABLE_PRIMARY'):
        store.stage_reviewed_batch(body)
    assert state(store) == before


def test_one_actual_primary_and_eligible_a6_is_sufficient_not_a_trigger_claim(tmp_path):
    store, _, cas = fixture(tmp_path)
    body = bundle()
    assert len(body['pm']['slot_plan']) == 1
    store.stage_reviewed_batch(body); store.commit_reviewed_batch_switch('scope-new', **cas)
    assert all(store.get_execution_plan(row['plan_id'])['status'] == 'PENDING_MORNING_REVIEW'
        for row in body['candidate_rows'])
