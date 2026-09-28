from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from liangjian_funnel.workflow import WorkflowApplication, _plan_expiry
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus, StateTransitionError
from liangjian_funnel.runtime.plan_validity import activation_reason, visible_current_plan


def dt(value):
    return datetime.fromisoformat(value + '+08:00')


NOW = dt('2026-09-28T09:32:00')


def put(store, key, expiry):
    store.create_execution_plan(key, 'lane_1', '600001.SH',
        status=PlanStatus.PENDING_MORNING_REVIEW, expires_at=expiry,
        payload={'source_run_id': 'source', 'stop_level': 9, 'trigger_high': 11})


def test_midautumn_and_model_future_expiry_use_real_session():
    assert _plan_expiry(None, dt('2026-09-24T16:00:00'), 'close') == dt('2026-09-28T15:00:00')
    assert _plan_expiry('2026-10-30T15:00:00+08:00', dt('2026-09-24T16:00:00'), 'close') == dt('2026-09-28T15:00:00')
    assert _plan_expiry(None, dt('2026-09-28T16:00:00'), 'close') == dt('2026-09-29T15:00:00')


def test_calendar_unavailable_does_not_fallback_to_weekday(monkeypatch):
    import liangjian_funnel.workflow as w
    from liangjian_funnel.runtime.calendar import TradingCalendarError
    monkeypatch.setattr(w, 'ExchangeTradingCalendar', Mock(side_effect=TradingCalendarError()))
    with pytest.raises(TradingCalendarError):
        w._plan_expiry(None, NOW, 'close')


@pytest.mark.parametrize('expiry,reason', [('2026-09-25T15:00:00','PLAN_EXPIRED'),
    ('2026-09-29T15:00:00','PLAN_SESSION_MISMATCH')])
def test_atomic_activation_rejects_wrong_session_without_partial_write(tmp_path, expiry, reason):
    store = RuntimeStore(tmp_path/'test.sqlite3')
    put(store, 'good', NOW.replace(hour=15))
    put(store, 'bad', dt(expiry))
    with pytest.raises(StateTransitionError, match=reason):
        store.activate_pending_plan_batch(['good', 'bad'], valid_from=NOW)
    assert all(r['status']=='PENDING_MORNING_REVIEW' for r in store.list_execution_plans())


def test_morning_expires_old_plan_without_fetching_quote(tmp_path):
    store = RuntimeStore(tmp_path/'test.sqlite3')
    put(store, 'old', dt('2026-09-25T15:00:00'))
    quote = Mock(side_effect=AssertionError('Must reject date before data calls'))
    app = SimpleNamespace(store=store, brokers={'lane_1':None},
        settings=SimpleNamespace(workflow_output_dir=tmp_path),
        _ensure_trading_day=lambda _:None, market_data=SimpleNamespace(fetch_quote=quote))
    result = WorkflowApplication.review_pending_morning(app, now=NOW)
    assert result['status']=='BLOCKED'
    assert result['activated']==[]
    assert store.get_execution_plan('old')['status']=='EXPIRED'
    quote.assert_not_called()


def test_visible_scope_does_not_count_stale_active_but_keeps_tomorrow_pending():
    row={'status':'ACTIVE_TODAY','expires_at':'2026-09-25T15:00:00+08:00','valid_from':NOW.isoformat()}
    assert not visible_current_plan(row,NOW)
    row['expires_at']='2026-09-28T15:00:00+08:00'
    assert visible_current_plan(row,NOW)
    row['valid_from']='2026-09-28T09:33:00+08:00'
    assert not visible_current_plan(row,NOW)
    row.update(status='PENDING_MORNING_REVIEW', expires_at='2026-09-29T15:00:00+08:00')
    assert visible_current_plan(row,NOW)
    assert activation_reason(row,NOW)=='PLAN_SESSION_MISMATCH'


def test_recovery_cannot_activate_expired_or_explicit_future_target(tmp_path):
    store = RuntimeStore(tmp_path/'test.sqlite3')
    put(store, 'expired', dt('2026-09-25T15:00:00'))
    with pytest.raises(StateTransitionError, match='A3_PLAN_EXPIRED'):
        store.activate_latest_a3_plan_batch(['expired'], valid_from=NOW,
            as_of=NOW, session_expires_at=NOW.replace(hour=15), source_run_id='source')
    store.create_execution_plan('future', 'lane_1', '600002.SH',
        status=PlanStatus.PENDING_MORNING_REVIEW, expires_at=dt('2026-09-29T15:00:00'),
        payload={'source_run_id':'source','target_trade_date':'2026-09-29'})
    with pytest.raises(StateTransitionError, match='PLAN_SESSION_MISMATCH'):
        store.activate_latest_a3_plan_batch(['future'], valid_from=NOW,
            as_of=NOW, session_expires_at=NOW.replace(hour=15), source_run_id='source')


def test_cross_year_calendar_used(monkeypatch):
    import liangjian_funnel.workflow as w
    from datetime import date
    calendar = SimpleNamespace(next_trading_day=lambda _:date(2027,1,4), is_trading_day=lambda _:True)
    monkeypatch.setattr(w, 'ExchangeTradingCalendar', lambda:calendar)
    assert w._plan_expiry(None, dt('2026-12-31T16:00:00'), 'close') == dt('2027-01-04T15:00:00')
