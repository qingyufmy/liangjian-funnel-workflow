from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.plan_validity import visible_current_plan
from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore, StateTransitionError


TZ = ZoneInfo('Asia/Shanghai')
NOW = datetime(2026, 9, 28, 9, 32, tzinfo=TZ)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr('liangjian_funnel.runtime.state._now', lambda: NOW)
    return RuntimeStore(tmp_path / 'state.sqlite3')


def pending(store, *, expiry=NOW.replace(hour=15), start=NOW, payload=None, key='p'):
    return store.create_execution_plan(key, 'lane', '600001.SH',
        status=PlanStatus.PENDING_MORNING_REVIEW, valid_from=start,
        expires_at=expiry, payload=payload or {})


@pytest.mark.parametrize('expiry,payload,reason', [
    (NOW - timedelta(days=3), {}, 'PLAN_EXPIRED'),
    (NOW.replace(hour=15) + timedelta(days=1), {}, 'PLAN_SESSION_MISMATCH'),
    (NOW.replace(hour=15), {'target_trade_date': '2026-09-29'}, 'PLAN_SESSION_MISMATCH'),
    (None, {}, 'PLAN_EXPIRY_INVALID'),
    ('2026-09-28T15:00:00', {}, 'PLAN_EXPIRY_INVALID'),
])
def test_generic_activation_rejects_invalid_date_without_state_change(store, expiry, payload, reason):
    before = pending(store, expiry=expiry, payload=payload)
    with pytest.raises(StateTransitionError, match=reason):
        store.activate_plan('p', valid_from=NOW)
    assert store.get_execution_plan('p') == before
    assert not store.persistence_failed


@pytest.mark.parametrize('start', [None, '2026-09-28T09:32:00', 'bad-date'])
def test_omitted_activation_start_never_infers_missing_or_naive_date(store, start):
    before = pending(store, start=start)
    with pytest.raises(StateTransitionError, match='PLAN_VALID_FROM_INVALID'):
        store.activate_plan('p')
    assert store.get_execution_plan('p') == before


def test_omitted_activation_preserves_existing_start_and_validates_current_session(store):
    before = pending(store, start=NOW - timedelta(minutes=1))
    active = store.activate_plan('p')
    assert active['valid_from'] == before['valid_from']
    assert active['status'] == 'ACTIVE_TODAY'


def test_active_idempotency_preserves_start_but_does_not_bypass_expiry(store):
    pending(store)
    active = store.activate_plan('p', valid_from=NOW)
    assert store.activate_plan('p', valid_from=NOW + timedelta(minutes=1)) == active
    with pytest.raises(StateTransitionError, match='PLAN_EXPIRED'):
        store.activate_plan('p', valid_from=NOW + timedelta(days=1))
    assert store.get_execution_plan('p') == active


def active_item(key='new', *, start=NOW, expiry=NOW.replace(hour=15), payload=None):
    return {'plan_id': key, 'lane_id': 'lane', 'symbol': '600002.SH',
        'status': 'ACTIVE_TODAY', 'valid_from': start, 'expires_at': expiry,
        'payload': payload or {}}


@pytest.mark.parametrize('start,expiry,payload,reason', [
    (NOW, NOW - timedelta(days=3), {}, 'PLAN_EXPIRED'),
    (NOW, NOW.replace(hour=15) + timedelta(days=1), {}, 'PLAN_SESSION_MISMATCH'),
    (NOW, NOW.replace(hour=15), {'target_trade_date': '2026-09-29'}, 'PLAN_SESSION_MISMATCH'),
    (NOW, None, {}, 'PLAN_EXPIRY_INVALID'),
    (NOW, '2026-09-28T15:00:00', {}, 'PLAN_EXPIRY_INVALID'),
    (None, NOW.replace(hour=15), {}, 'PLAN_VALID_FROM_INVALID'),
    ('2026-09-28T09:32:00', NOW.replace(hour=15), {}, 'PLAN_VALID_FROM_INVALID'),
])
def test_direct_active_batch_rejects_bad_row_before_retirement_or_partial_publish(store, start, expiry, payload, reason):
    old = pending(store, key='old')
    old = store.activate_plan('old', valid_from=NOW)
    with pytest.raises(StateTransitionError, match=reason):
        store.publish_plan_batch([active_item('good'), active_item('bad', start=start, expiry=expiry, payload=payload)],
            expire_active_lanes=('lane',), invalidate_pending_lanes=('lane',))
    assert store.get_execution_plan('old') == old
    assert store.get_execution_plan('good') is None
    assert store.get_execution_plan('bad') is None
    assert not store.persistence_failed


def test_republication_checks_existing_expiry_not_proposed_new_dates(store):
    existing = store.create_execution_plan('new', 'lane', '600002.SH', status='ACTIVE_TODAY',
        valid_from=NOW - timedelta(days=3), expires_at=NOW - timedelta(days=3), payload={})
    with pytest.raises(StateTransitionError, match='PLAN_EXPIRED'):
        store.publish_plan_batch([active_item()], expire_active_lanes=('lane',))
    assert store.get_execution_plan('new') == existing


def test_active_dates_normalize_offset_for_existing_sql_scope(store):
    result = store.publish_plan_batch([active_item(start='2026-09-28T01:32:00+00:00',
        expiry='2026-09-28T07:00:00+00:00')])[0]
    assert result['valid_from'] == NOW.isoformat()
    assert result['expires_at'] == NOW.replace(hour=15, minute=0).isoformat()
    assert len(store.list_active_plans('lane', at=NOW)) == 1


def test_pending_next_day_remains_visible_but_not_activatable(store):
    row = pending(store, start=None, expiry=NOW.replace(hour=15) + timedelta(days=1))
    assert visible_current_plan(row, NOW)
    with pytest.raises(StateTransitionError, match='PLAN_SESSION_MISMATCH'):
        store.activate_plan('p', valid_from=NOW)


def test_expired_entry_plan_can_still_be_invalidated_for_position_lineage(store):
    row = pending(store, expiry=NOW - timedelta(days=3))
    result = store.invalidate_plan('p', status=PlanStatus.EXPIRED)
    assert result['status'] == 'EXPIRED'
    assert result['expires_at'] == row['expires_at']


def test_omitted_start_rejects_future_start_and_expired_existing_interval(store):
    pending(store, key='future', start=NOW + timedelta(minutes=1))
    with pytest.raises(StateTransitionError, match='PLAN_NOT_YET_VALID'):
        store.activate_plan('future')
    pending(store, key='expired', start=NOW - timedelta(days=3), expiry=NOW - timedelta(days=3))
    with pytest.raises(StateTransitionError, match='PLAN_EXPIRED'):
        store.activate_plan('expired')


def test_generic_activation_normalizes_aware_expiry_for_sql_scope(store):
    pending(store, expiry='2026-09-28T07:00:00+00:00')
    result = store.activate_plan('p', valid_from='2026-09-28T01:32:00+00:00')
    assert result['expires_at'] == '2026-09-28T15:00:00+08:00'
    assert len(store.list_active_plans('lane', at=NOW)) == 1


def test_future_same_session_publication_does_not_execute_before_start(store, monkeypatch):
    monkeypatch.setattr('liangjian_funnel.runtime.state._now', lambda: NOW - timedelta(minutes=6))
    store.publish_plan_batch([active_item()])
    assert not store.list_active_plans('lane', at=NOW - timedelta(minutes=6))
    assert len(store.list_active_plans('lane', at=NOW)) == 1


def test_existing_bad_interval_is_checked_before_any_batch_dml(store, monkeypatch):
    existing = store.create_execution_plan('new', 'lane', '600002.SH', status='ACTIVE_TODAY',
        valid_from=NOW - timedelta(days=3), expires_at=NOW - timedelta(days=3), payload={})
    trace = []
    connect = store._connect

    def traced_connect():
        connection = connect()
        connection.set_trace_callback(trace.append)
        return connection

    monkeypatch.setattr(store, '_connect', traced_connect)
    with pytest.raises(StateTransitionError, match='PLAN_EXPIRED'):
        store.publish_plan_batch([active_item('good'), active_item()], expire_active_lanes=('lane',))
    assert not any(sql.lstrip().upper().startswith(('INSERT ', 'UPDATE ', 'DELETE ')) for sql in trace)
    assert store.get_execution_plan('new') == existing
