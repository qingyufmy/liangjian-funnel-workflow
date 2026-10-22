"""Atomic reviewed-scope consumption, not new model or production evidence."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.team_v2_review_scope import (
    ReviewedScopeStore, ReviewScopeError, read_scope_token, assert_scope_token,
)
from liangjian_funnel.runtime.team_v2_sources import read_execution_scope
from liangjian_funnel.runtime.team_v2_store import canonical


DAY = '2026-10-22'
ACCOUNT = 'team-v2-paper'


@pytest.fixture(autouse=True)
def controlled_switch_clock(monkeypatch):
    # Only the controlled test clock; all engine observations precede it.
    monkeypatch.setattr('liangjian_funnel.runtime.reviewed_batch_switch._now',
        lambda: datetime.fromisoformat(DAY + 'T08:40:00+08:00'))


def scope_value():
    evidence = {role: dict(meeting_id='new-' + role + '-scope', input_sha256='a' * 64,
        call_id=role + ':new-scope', raw_sha256='b' * 64, journal_ref='isolated/model.db', validated=True)
        for role in ('a6', 'pm')}
    source = dict(status='COMPLETE', trade_date=DAY, lane_id='lane_1',
        observed_at=DAY + 'T08:32:00+08:00', source_trade_date='2026-10-21', plan_rows=[],
        plan_rowset_sha256=hashlib.sha256(canonical([]).encode()).hexdigest())
    body = dict(schema_version='reviewed-batch/1', scope_id='new-scope', account_id=ACCOUNT,
        trade_date=DAY, lane_id='lane_1', run_id='new-research', status='READY',
        engine=dict(status='COMPLETE', source=source, envelopes=[]),
        risk=dict(status='READY', envelopes=[], utilization_cap='35', consecutive_failures=0, reason_codes=[]),
        pm=dict(status='READY', model_validated=True, session='PM_PREMARKET', trade_date=DAY,
            source_call_id=evidence['pm']['call_id'], allocation_plan_bindings={}, slot_plan=[],
            plan_allocations=[], deployment_target=dict(baseline_range=[20, 35])),
        evidence=evidence, candidate_rows=[])
    return dict(scope_id='new-scope', bundle_sha256=hashlib.sha256(canonical(body).encode()).hexdigest(),
        pointer_revision=1, bundle=body)


def pointer_database(tmp_path, scope=None):
    path = tmp_path / 'workflow.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE active_reviewed_batch_scope(account_id TEXT,trade_date TEXT,scope_id TEXT,bundle_sha256 TEXT,revision INTEGER)')
        db.execute('CREATE TABLE execution_plans(plan_id TEXT,lane_id TEXT,symbol TEXT,status TEXT,valid_from TEXT,expires_at TEXT,payload_json TEXT)')
        if scope:
            db.execute('INSERT INTO active_reviewed_batch_scope VALUES (?,?,?,?,?)',
                (ACCOUNT, DAY, scope['scope_id'], scope['bundle_sha256'], scope['pointer_revision']))
    return path


def legacy_store():
    calls = []
    def call(name):
        return lambda *args, **kwargs: calls.append((name, args, kwargs)) or name
    return SimpleNamespace(**{name: call(name) for name in
        ('load_morning_engine', 'load_morning_risk', 'load_pm_target_upper', 'load_pm_allocation', 'arbitrary_delegated')}), calls


def test_no_pointer_preserves_legacy_reads_and_unrelated_methods(tmp_path):
    path = pointer_database(tmp_path)
    old, calls = legacy_store()
    store = ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=None)
    assert store.pm_meeting_id == 'morning-pm:' + DAY
    assert store.load_morning_engine(ACCOUNT, DAY) == 'load_morning_engine'
    assert store.load_morning_risk(ACCOUNT, DAY, source_call_id='old') == 'load_morning_risk'
    assert store.load_pm_target_upper(store.pm_meeting_id, account_id=ACCOUNT, trade_date=DAY) == 'load_pm_target_upper'
    assert store.load_pm_allocation(store.pm_meeting_id, account_id=ACCOUNT, trade_date=DAY) == 'load_pm_allocation'
    assert store.arbitrary_delegated() == 'arbitrary_delegated'
    assert len(calls) == 5


def test_scope_decodes_actual_risk_and_pm_without_reading_old_meetings(tmp_path):
    scope = scope_value()
    path = pointer_database(tmp_path, scope)
    old, calls = legacy_store()
    store = ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=scope)
    assert store.pm_meeting_id == 'new-pm-scope'
    assert store.load_morning_engine(ACCOUNT, DAY) == scope['bundle']['engine']
    risk = store.load_morning_risk(ACCOUNT, DAY, source_call_id='A6:morning:' + DAY)
    assert risk.status == 'READY' and risk.utilization_cap == Decimal(35)
    assert store.load_pm_target_upper(store.pm_meeting_id, account_id=ACCOUNT, trade_date=DAY) == Decimal(35)
    allocation = store.load_pm_allocation(store.pm_meeting_id, account_id=ACCOUNT, trade_date=DAY)
    assert allocation.meeting_id == 'new-pm-scope' and allocation.input_sha256 == 'a' * 64
    assert calls == []


def test_scope_result_is_immutable_to_caller_mutation(tmp_path):
    scope = scope_value()
    path = pointer_database(tmp_path, scope)
    old, _ = legacy_store()
    store = ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=scope)
    engine = store.load_morning_engine(ACCOUNT, DAY)
    engine['source']['trade_date'] = 'WRONG'
    scope['bundle']['risk']['utilization_cap'] = '100'
    assert store.load_morning_engine(ACCOUNT, DAY)['source']['trade_date'] == DAY
    assert store.load_morning_risk(ACCOUNT, DAY, source_call_id='A6:morning:' + DAY).utilization_cap == 35


@pytest.mark.parametrize('mode', ['hash', 'risk', 'fallback', 'evidence'])
def test_corrupt_or_unreviewed_bundle_never_falls_back_to_old_meetings(tmp_path, mode):
    scope = scope_value()
    if mode == 'hash': scope['bundle_sha256'] = 'f' * 64
    elif mode == 'risk': scope['bundle']['risk']['status'] = 'HALT'
    elif mode == 'fallback': scope['bundle']['pm']['model_validated'] = False
    else: scope['bundle']['evidence']['a6']['validated'] = False
    if mode != 'hash': scope['bundle_sha256'] = hashlib.sha256(canonical(scope['bundle']).encode()).hexdigest()
    path = pointer_database(tmp_path, scope)
    old, calls = legacy_store()
    with pytest.raises(ReviewScopeError):
        ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=scope)
    assert calls == []


def test_pointer_change_after_freeze_blocks_without_legacy_fallback(tmp_path):
    scope = scope_value()
    path = pointer_database(tmp_path, scope)
    old, calls = legacy_store()
    store = ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=scope)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE active_reviewed_batch_scope SET revision=2')
    with pytest.raises(ReviewScopeError, match='CHANGED_RETRY'):
        store.load_morning_engine(ACCOUNT, DAY)
    assert calls == []


def test_legacy_freeze_cannot_mix_with_concurrently_activated_pointer(tmp_path):
    path = pointer_database(tmp_path)
    old, calls = legacy_store()
    store = ReviewedScopeStore(old, path, account_id=ACCOUNT, trade_date=DAY, scope=None)
    scope = scope_value()
    with sqlite3.connect(path) as db:
        db.execute('INSERT INTO active_reviewed_batch_scope VALUES (?,?,?,?,?)',
            (ACCOUNT, DAY, scope['scope_id'], scope['bundle_sha256'], 1))
    with pytest.raises(ReviewScopeError, match='CHANGED_RETRY'):
        store.load_pm_allocation(store.pm_meeting_id, account_id=ACCOUNT, trade_date=DAY)
    assert calls == []


def test_execution_scope_checks_pointer_in_same_ro_transaction(tmp_path):
    scope = scope_value()
    path = pointer_database(tmp_path, scope)
    settings = SimpleNamespace(state_db_path=path, research_primary_lane_id='lane_1')
    now = datetime.fromisoformat(DAY + 'T09:40:00+08:00')
    cal = SimpleNamespace(is_trading_day=lambda day: True)
    wrong = {key: scope[key] for key in ('scope_id', 'bundle_sha256', 'pointer_revision')}
    wrong['pointer_revision'] = 2
    with pytest.raises(ReviewScopeError, match='CHANGED_RETRY'):
        read_execution_scope(settings, now=now, calendar=cal, morning_source=scope['bundle']['engine']['source'],
            positions=[], review_scope_token=wrong, review_scope_account_id=ACCOUNT)


def test_late_morning_source_gate_not_relaxed(tmp_path):
    scope = scope_value()
    path = pointer_database(tmp_path, scope)
    source = deepcopy(scope['bundle']['engine']['source'])
    source['observed_at'] = DAY + 'T09:35:00+08:00'
    with pytest.raises(ValueError, match='EXECUTION_MORNING_SOURCE_UNPROVEN'):
        read_execution_scope(SimpleNamespace(state_db_path=path, research_primary_lane_id='lane_1'),
            now=datetime.fromisoformat(DAY + 'T09:40:00+08:00'),
            calendar=SimpleNamespace(is_trading_day=lambda day: True), morning_source=source, positions=[])


def test_monitor_scope_race_is_noop_not_account_record(monkeypatch):
    import liangjian_funnel.runtime.team_v2_monitor as module
    def changed(*args, **kwargs): raise ReviewScopeError('TEAM_REVIEW_SCOPE_CHANGED_RETRY')
    monkeypatch.setattr(module, '_run_monitor_control', changed)
    result = module.run_monitor_control(object())
    assert result == dict(status='NOOP', reason_code='TEAM_REVIEW_SCOPE_CHANGED_RETRY',
        scope_retry_required=True, record_latch_changed=False)


def committed_scope(tmp_path):
    from test_reviewed_batch_switch_0546 import fixture, bundle
    workflow, old_rows, cas = fixture(tmp_path)
    body = bundle()
    source = body['engine']['source']
    source.update(observed_at=DAY + 'T08:20:00+08:00', source_trade_date='2026-10-21')
    for row in body['candidate_rows']:
        row['payload'].update(reference_price=10.5, reference_price_as_of='2026-10-21T15:00:00+08:00',
            cluster_label='bank', market_regime='ROTATION_NO_MAINLINE')
    for row, candidate in zip(source['plan_rows'], body['candidate_rows']):
        row.update(payload_json=canonical(candidate['payload']), valid_from=None, lane_id='lane_1')
    source['plan_rowset_sha256'] = hashlib.sha256(canonical(source['plan_rows']).encode()).hexdigest()
    body['risk']['envelopes'] = [dict(plan_id=row['plan_id'], symbol=row['symbol'], trade_date=DAY,
        eligible=True, risk_unit='PROBE', risk_budget='15000', max_batches=1, max_shares_cap=500,
        blocked_reasons=[], binding_limits=['MAX_POSITION_10PCT'], plan_risk_unit='PROBE',
        purpose='PLANNING') for row in body['candidate_rows']]
    body['engine']['envelopes'] = deepcopy(body['risk']['envelopes'])
    a6 = body['evidence']['a6']
    a6.update(risk_result_sha256=hashlib.sha256(canonical(body['risk']).encode()).hexdigest(),
        engine_envelopes_sha256=hashlib.sha256(canonical(body['engine']['envelopes']).encode()).hexdigest(),
        plan_rowset_sha256=source['plan_rowset_sha256'])
    a6['meeting_record']['result_sha256'] = a6['risk_result_sha256']
    body['pm'].update(slot_plan=[dict(slot=index + 1, primary_plan_id=row['plan_id'], backup_plan_id='', cluster='bank')
        for index, row in enumerate(body['candidate_rows'])],
        plan_allocations=[dict(plan_id=row['plan_id'], intended_risk_unit='PROBE', max_batches=1) for row in body['candidate_rows']],
        deployment_target=dict(baseline_range=[20, 30]))
    workflow.stage_reviewed_batch(body)
    workflow.commit_reviewed_batch_switch(body['scope_id'], **cas)
    return workflow, body


def test_real_loader_facade_committed_scope_keeps_limits_and_old_held_owner(tmp_path):
    from liangjian_funnel.runtime.team_v2_review_scope import freeze_review_scope
    workflow, body = committed_scope(tmp_path)
    settings = SimpleNamespace(state_db_path=workflow.path, research_primary_lane_id='lane_1')
    old, calls = legacy_store()
    store = freeze_review_scope(settings, old, account_id='local-paper', trade_date=DAY)
    risk = store.load_morning_risk('local-paper', DAY, source_call_id='A6:morning:' + DAY)
    assert {row.max_shares_cap for row in risk.envelopes} == {500}
    assert {row.risk_unit for row in risk.envelopes} == {'PROBE'}
    assert store.load_pm_target_upper(store.pm_meeting_id, account_id='local-paper', trade_date=DAY) == Decimal(30)
    assert calls == []
    # An older owned 002270 row is not in the new research batch. It remains
    # visible for holding protection and never joins new-entry activation.
    held_payload = dict(body['candidate_rows'][0]['payload'], symbol='002270.SZ')
    with workflow._connect() as db:
        db.execute("INSERT INTO execution_plans(plan_id,lane_id,symbol,status,plan_version,valid_from,expires_at,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            ('held-002270', 'lane_1', '002270.SZ', 'INVALIDATED', 1, '2026-10-21T10:00:00+08:00',
             '2026-10-21T15:00:00+08:00', canonical(held_payload), DAY, DAY))
        db.commit()
    result = read_execution_scope(settings, now=datetime.fromisoformat(DAY + 'T09:40:00+08:00'),
        calendar=SimpleNamespace(is_trading_day=lambda day: True), morning_source=store.load_morning_engine('local-paper', DAY)['source'],
        positions=[dict(plan_id='held-002270', symbol='002270.SZ')],
        review_scope_token=store.review_scope_token, review_scope_account_id='local-paper')
    assert result['held_plan_ids'] == {'held-002270': '002270.SZ'}
    assert any(row['plan_id'] == 'held-002270' for row in result['plans'])
    assert 'held-002270' not in result['active_plan_ids']


def test_loader_corruption_is_scope_noop_error_not_account_exception(tmp_path):
    from liangjian_funnel.runtime.team_v2_review_scope import freeze_review_scope
    workflow, body = committed_scope(tmp_path)
    with workflow._connect() as db:
        row = db.execute('SELECT body_json FROM reviewed_batch_bundles').fetchone()
        corrupted = json.loads(row[0])
        corrupted['risk']['status'] = 'HALT'
        text = canonical(corrupted)
        pin = hashlib.sha256(text.encode()).hexdigest()
        db.execute('UPDATE reviewed_batch_bundles SET body_json=?,bundle_sha256=?', (text, pin))
        db.execute('UPDATE active_reviewed_batch_scope SET bundle_sha256=?', (pin,))
        db.commit()
    with pytest.raises(ReviewScopeError, match='TEAM_REVIEW_SCOPE_UNPROVEN'):
        freeze_review_scope(SimpleNamespace(state_db_path=workflow.path, research_primary_lane_id='lane_1'),
            legacy_store()[0], account_id='local-paper', trade_date=DAY)


def test_reviewed_scope_reads_only_new_batch_after_separate_auction_activation(tmp_path):
    from liangjian_funnel.runtime.team_v2_review_scope import freeze_review_scope
    workflow, body = committed_scope(tmp_path)
    ids = [row['plan_id'] for row in body['candidate_rows']]
    # Controlled existing activation API; not a claim about real auction quotes.
    workflow.activate_pending_plan_batch(ids, valid_from=datetime.fromisoformat(DAY + 'T09:26:00+08:00'))
    settings = SimpleNamespace(state_db_path=workflow.path, research_primary_lane_id='lane_1')
    store = freeze_review_scope(settings, legacy_store()[0], account_id='local-paper', trade_date=DAY)
    result = read_execution_scope(settings, now=datetime.fromisoformat(DAY + 'T09:40:00+08:00'),
        calendar=SimpleNamespace(is_trading_day=lambda day: True),
        morning_source=store.load_morning_engine('local-paper', DAY)['source'], positions=[],
        review_scope_token=store.review_scope_token, review_scope_account_id='local-paper')
    assert result['status'] == 'COMPLETE'
    assert set(result['active_plan_ids']) == set(ids)
    assert {row['plan_id'] for row in result['plan_rows']} == set(ids)
    assert all(workflow.get_execution_plan(str(index))['status'] == 'SUPERSEDED' for index in range(3))
