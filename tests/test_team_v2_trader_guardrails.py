"""Declared live-paper hard boundaries, independent of model technical opinions."""
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.trader_guardrails import guard_trader_signal, is_closed_trader_tick
from liangjian_funnel.runtime.risk_engine import PositionRisk, RiskLimits
from test_team_v2_risk_engine import NOW, plan, account, envelope


def guard(**changes):
    args = dict(signal_id='sig', action='BUY_SIGNAL', signal_plan_id='p1', signal_symbol='000001.SZ',
        plan=plan(), envelope=envelope(), account=account(), positions=(), limits=RiskLimits(),
        now=NOW, closed_5m_end=NOW, regime='TREND_MAINLINE', live_market_ready=True,
        snapshot_ready=True, schema_valid=True, mode='control', cluster_signals_today=0,
        batches_executed=0, calendar=SimpleNamespace(is_trading_day=lambda day: day.isoformat() == '2026-10-19'))
    args.update(changes)
    return guard_trader_signal(**args)


def test_valid_control_decision_can_dispatch_but_record_never_can():
    result = guard()
    assert result.dispatch_allowed and result.risk_decision.shares == 20000
    recorded = guard(mode='record')
    assert not recorded.dispatch_allowed and recorded.risk_decision == result.risk_decision
    assert recorded.status == 'RECORD_ONLY'


@pytest.mark.parametrize('change,code', [
    ({'signal_plan_id': 'other'}, 'TRADER_PLAN_BINDING_MISMATCH'),
    ({'signal_symbol': '000002.SZ'}, 'TRADER_PLAN_BINDING_MISMATCH'),
    ({'schema_valid': False}, 'TRADER_SCHEMA_INVALID'),
    ({'live_market_ready': False}, 'LIVE_MARKET_NOT_READY'),
    ({'snapshot_ready': False}, 'TRADER_SNAPSHOT_NOT_READY'),
    ({'closed_5m_end': datetime.fromisoformat('2026-10-19T10:05:00+08:00')}, 'TRADER_TICK_UNPROVEN'),
    ({'closed_5m_end': datetime.fromisoformat('2026-10-16T10:00:00+08:00')}, 'TRADER_TICK_UNPROVEN'),
    ({'closed_5m_end': datetime.fromisoformat('2026-10-19T09:55:00+08:00')}, 'TRADER_TICK_NOT_CURRENT'),
    ({'plan': plan(status='PENDING_MORNING_REVIEW')}, 'ENVELOPE_INELIGIBLE'),
    ({'plan': plan(price=Decimal(11))}, 'PRICE_OUTSIDE_PLAN_GEOMETRY'),
    ({'plan': plan(tradable=False)}, 'NOT_TRADABLE'),
    ({'cluster_signals_today': 2}, 'CLUSTER_SIGNAL_CAP'),
    ({'batches_executed': 1}, 'TRADER_BATCH_CAP'),
    ({'batches_executed': True}, 'TRADER_BATCH_COUNT_UNPROVEN'),
])
def test_hard_boundary_counterexamples_do_not_dispatch(change, code):
    result = guard(**change)
    assert not result.dispatch_allowed and code in result.reason_codes


@pytest.mark.parametrize('time,allowed', [
    ('09:30:00', False), ('09:35:00', True), ('11:30:00', True), ('11:35:00', False),
    ('12:00:00', False), ('13:00:00', False), ('13:05:00', True), ('15:00:00', True),
    ('15:05:00', False), ('10:01:00', False), ('10:00:01', False),
])
def test_five_minute_tick_does_not_make_lunch_or_unclosed_bars(time, allowed):
    assert is_closed_trader_tick(datetime.fromisoformat('2026-10-19T'+time+'+08:00')) is allowed


def test_entry_cutoff_is_not_an_exit_cutoff_and_expired_held_plan_can_exit():
    at = datetime.fromisoformat('2026-10-19T14:35:00+08:00')
    assert 'NO_NEW_ENTRY_TIME' in guard(now=at, closed_5m_end=at).reason_codes
    held = PositionRisk('p1', '000001.SZ', 'FINANCE', 1000, 1000, Decimal(10000), Decimal(9))
    old = plan(status='INVALIDATED', expires_at=datetime.fromisoformat('2026-10-16T15:00:00+08:00'))
    result = guard(action='SELL_SIGNAL', plan=old, positions=(held,), live_market_ready=False,
                   now=at, closed_5m_end=at)
    assert result.dispatch_allowed and result.risk_decision.shares == 1000
    today = replace(held, sellable_qty=0)
    assert 'T1_LOCKED' in guard(action='SELL_SIGNAL', positions=(today,)).reason_codes


def test_add_requires_profit_and_remaining_batches_even_when_model_wants_more():
    held = PositionRisk('p1', '000001.SZ', 'FINANCE', 1000, 0, Decimal(10000), Decimal(11))
    e = replace(envelope(), max_batches=2)
    result = guard(action='ADD_SIGNAL', positions=(held,), envelope=e, batches_executed=1)
    assert not result.dispatch_allowed and 'LOSS_ADD_FORBIDDEN' in result.reason_codes


@pytest.mark.parametrize('action', ['NO_ACTION', 'START_CONFIRMATION', 'BLOCK', 'CANCEL_SIGNAL'])
def test_non_order_actions_never_gain_dispatch_authority(action):
    result = guard(action=action)
    assert not result.dispatch_allowed and result.risk_decision is None


def test_invalid_mode_is_not_implicitly_control_and_naive_clock_rejects():
    with pytest.raises(ValueError, match='TRADER_MODE_INVALID'): guard(mode='unknown')
    assert not guard(now=datetime(2026, 10, 19, 10)).dispatch_allowed


def test_no_orders_on_a_calendar_closed_day_or_during_the_lunch_gap_even_for_exits():
    assert 'NON_TRADING_DAY' in guard(calendar=SimpleNamespace(is_trading_day=lambda _: False)).reason_codes
    at = datetime.fromisoformat('2026-10-19T11:31:00+08:00')
    closed = datetime.fromisoformat('2026-10-19T11:30:00+08:00')
    held = PositionRisk('p1', '000001.SZ', 'FINANCE', 1000, 1000, Decimal(10000), Decimal(9))
    result = guard(action='SELL_SIGNAL', positions=(held,), now=at, closed_5m_end=closed)
    assert not result.dispatch_allowed and 'NO_TRADING_TIME' in result.reason_codes
