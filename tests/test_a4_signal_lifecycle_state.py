from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.state import (
    A4SignalStatus,
    MonitorAction,
    PlanStatus,
    RuntimeStore,
    StateTransitionError,
)


TZ = ZoneInfo("Asia/Shanghai")


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 3, hour, minute, tzinfo=TZ)


def _plan(store: RuntimeStore, plan_id: str = "plan-1", symbol: str = "002837.SZ") -> dict:
    return store.create_execution_plan(
        plan_id,
        "lane-a",
        symbol,
        status=PlanStatus.ACTIVE_TODAY,
        payload={
            "source_run_id": "run-20260903",
            "name": "英维克" if symbol == "002837.SZ" else symbol,
            "stock_behavior_type": "TREND",
            "strategy_profile": "TREND_MA5",
            "trigger_low": 66.158,
            "trigger_high": 66.23,
            "stop_level": 65.81,
        },
    )


def _entry_event(
    plan_id: str = "plan-1",
    symbol: str = "002837.SZ",
    event_key: str = "entry-1",
    event_id: str = "event-1",
    at: datetime | None = None,
) -> dict:
    return {
        "event_id": event_id,
        "event_key": event_key,
        "lane_id": "lane-a",
        "minute_end": at or _at(10, 0),
        "action": MonitorAction.BUY_SIGNAL.value,
        "payload": {
            "plan_id": plan_id,
            "symbol": symbol,
            "signal_price": 66.20,
            "strategy": {
                "stock_behavior_type": "TREND",
                "strategy_profile": "TREND_MA5",
                "met_conditions": ["CLOSED_5M_RECLAIM"],
            },
        },
    }


def _exit_event(
    event_key: str = "exit-1",
    action: str = MonitorAction.SELL_SIGNAL.value,
    at: datetime | None = None,
) -> dict:
    return {
        "event_id": f"{event_key}-id",
        "event_key": event_key,
        "lane_id": "lane-a",
        "minute_end": at or _at(10, 20),
        "action": action,
        "reason_code": "TREND_BREAK",
        "payload": {"plan_id": "plan-1", "symbol": "002837.SZ", "signal_price": 67.10},
    }


def test_schema_entry_is_idempotent_and_monitor_syncs_in_one_store(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    event = _entry_event()

    row, inserted = store.record_a4_entry_signal(event, plan)
    assert inserted is True
    assert row["status"] == A4SignalStatus.SIGNALLED.value
    assert row["account_id"] == "paper:lane-a"
    assert row["entry_event_key"] == "entry-1"
    same, inserted = store.record_a4_entry_signal(event, plan)
    assert inserted is False
    assert same["lifecycle_id"] == row["lifecycle_id"]
    assert len(store.list_a4_lifecycles()) == 1

    # The production path uses the event API.  Replaying it must not create a
    # second event or a second lifecycle.
    event2 = {**event, "payload": {**event["payload"], "strategy": event["payload"]["strategy"]}}
    event_row, inserted = store.record_monitor_event(
        event_key="effective-entry",
        lane_id="lane-a",
        minute_end=_at(10, 1),
        action=MonitorAction.BUY_SIGNAL,
        effective=True,
        sync_a4_lifecycle=True,
        payload=event2["payload"],
    )
    assert inserted is True
    assert event_row["event_key"] == "effective-entry"
    assert len(store.list_a4_lifecycles()) == 2


def test_fill_extrema_partial_and_full_close_are_idempotent(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    event = _entry_event()
    store.record_a4_entry_signal(event, plan)

    bought, inserted = store.apply_a4_fill(
        "entry-1",
        {
            "fill_id": "fill-entry",
            "account_id": "paper:lane-a",
            "symbol": "002837.SZ",
            "action": "BUY",
            "qty": 100,
            "price": 66.20,
            "fee": 1.0,
            "bar_end": _at(10, 1),
        },
        remaining_qty=100,
    )
    assert inserted is True
    assert bought["status"] == A4SignalStatus.OPEN.value
    assert bought["entry_qty"] == 100
    duplicate, inserted = store.apply_a4_fill(
        "entry-1",
        {
            "fill_id": "fill-entry",
            "action": "BUY",
            "qty": 100,
            "price": 66.20,
            "fee": 1.0,
            "bar_end": _at(10, 1),
        },
        remaining_qty=100,
    )
    assert inserted is False
    assert duplicate["entry_qty"] == 100

    observed = store.observe_a4_lifecycle("paper:lane-a", "002837.SZ", _at(10, 5), 67.10, 65.90, 66.80)
    assert observed is not None
    assert observed["max_price"] == pytest.approx(67.10)
    assert observed["min_price"] == pytest.approx(65.90)
    # A repeated bar is an aggregate max/min update, not a duplicated detail.
    repeated = store.observe_a4_lifecycle("paper:lane-a", "002837.SZ", _at(10, 5), 67.10, 65.90, 66.80)
    assert repeated["max_price"] == observed["max_price"]
    assert repeated["min_price"] == observed["min_price"]
    assert repeated["mfe"] > 0
    assert repeated["mae"] < 0

    exit_row, inserted = store.record_a4_exit_signal(_exit_event())
    assert inserted is True
    assert exit_row is not None and exit_row["status"] == A4SignalStatus.EXIT_PENDING.value
    exit_same, inserted = store.record_a4_exit_signal(_exit_event())
    assert inserted is False
    assert exit_same["status"] == A4SignalStatus.EXIT_PENDING.value

    partial, inserted = store.apply_a4_fill(
        "entry-1",
        {"fill_id": "fill-partial", "action": "SELL", "qty": 40, "price": 66.80, "fee": 1.0, "bar_end": _at(10, 21)},
        remaining_qty=60,
    )
    assert inserted is True
    assert partial["status"] == A4SignalStatus.PARTIALLY_CLOSED.value
    assert partial["exit_qty"] == 40
    closed, inserted = store.apply_a4_fill(
        "entry-1",
        {"fill_id": "fill-close", "action": "SELL", "qty": 60, "price": 67.10, "fee": 1.0, "bar_end": _at(10, 22)},
        remaining_qty=0,
    )
    assert inserted is True
    assert closed["status"] == A4SignalStatus.CLOSED.value
    assert closed["remaining_qty"] == 0
    assert closed["exit_qty"] == 100
    assert closed["realized_pnl"] > 0
    assert closed["net_return"] > 0
    duplicate_close, inserted = store.apply_a4_fill(
        "entry-1",
        {"fill_id": "fill-close", "action": "SELL", "qty": 60, "price": 67.10, "fee": 1.0, "bar_end": _at(10, 22)},
        remaining_qty=0,
    )
    assert inserted is False
    assert duplicate_close["exit_qty"] == 100


def test_terminal_states_and_atomic_plan_invalidation(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    event = _entry_event()
    store.record_a4_entry_signal(event, plan)
    invalidation, inserted = store.record_monitor_event(
        event_key="effective-invalidation",
        lane_id="lane-a",
        minute_end=_at(10, 30),
        action=MonitorAction.PLAN_INVALIDATED,
        reason_code="CLOSED_5M_BREAK",
        effective=True,
        terminal_plan_id="plan-1",
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-1", "symbol": "002837.SZ"},
    )
    assert inserted is True
    assert invalidation["action"] == MonitorAction.PLAN_INVALIDATED.value
    assert store.get_execution_plan("plan-1")["status"] == PlanStatus.INVALIDATED.value
    assert store.get_a4_lifecycle("entry-1")["status"] == A4SignalStatus.INVALIDATED.value

    # A terminal lifecycle cannot be changed to another terminal state.
    with pytest.raises(StateTransitionError, match="A4_TERMINAL_STATE_CONFLICT"):
        store.mark_a4_signal_terminal("entry-1", A4SignalStatus.CANCELLED)

    # Missing plan is validated before the event can become durable.
    with pytest.raises(StateTransitionError, match="PLAN_NOT_FOUND"):
        store.record_monitor_event(
            event_key="effective-missing-plan",
            lane_id="lane-a",
            minute_end=_at(10, 31),
            action=MonitorAction.PLAN_INVALIDATED,
            effective=True,
            terminal_plan_id="does-not-exist",
            sync_a4_lifecycle=True,
            payload={"plan_id": "does-not-exist", "symbol": "002837.SZ"},
        )
    assert store.list_monitor_events(lane_id="lane-a")[-1]["event_key"] == "effective-invalidation"


def test_unfilled_and_illegal_state_transitions_are_retained(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    store.record_a4_entry_signal(_entry_event(), plan)
    unfilled = store.mark_a4_signal_terminal("entry-1", A4SignalStatus.UNFILLED, reason_code="ENTRY_EXPIRED")
    assert unfilled["status"] == A4SignalStatus.UNFILLED.value
    assert unfilled["exit_reason"] == "ENTRY_EXPIRED"
    with pytest.raises(StateTransitionError, match="A4_TERMINAL_STATE_CONFLICT"):
        store.apply_a4_fill(
            "entry-1",
            {"fill_id": "late-fill", "action": "BUY", "qty": 100, "price": 66.2, "fee": 0, "bar_end": _at(10, 40)},
            remaining_qty=100,
        )
    assert [item["status"] for item in store.list_a4_lifecycles()] == [A4SignalStatus.UNFILLED.value]


def test_transient_data_block_does_not_terminate_an_open_signal(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    store.record_a4_entry_signal(_entry_event(), plan)
    store.apply_a4_fill(
        "entry-1",
        {
            "fill_id": "fill-entry",
            "action": "BUY",
            "qty": 100,
            "price": 66.20,
            "fee": 1.0,
            "bar_end": _at(10, 1),
        },
        remaining_qty=100,
    )

    store.record_monitor_event(
        event_key="transient-data-block",
        lane_id="lane-a",
        minute_end=_at(10, 5),
        action=MonitorAction.DATA_BLOCK,
        reason_code="MINUTE_DATA_GAP",
        effective=True,
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-1", "symbol": "002837.SZ"},
    )

    lifecycle = store.get_a4_lifecycle("entry-1")
    assert lifecycle["status"] == A4SignalStatus.OPEN.value
    assert lifecycle["remaining_qty"] == 100


def test_plan_invalidation_keeps_an_existing_position_exit_pending(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    plan = _plan(store)
    store.record_a4_entry_signal(_entry_event(), plan)
    store.apply_a4_fill(
        "entry-1",
        {
            "fill_id": "fill-entry",
            "action": "BUY",
            "qty": 100,
            "price": 66.20,
            "fee": 1.0,
            "bar_end": _at(10, 1),
        },
        remaining_qty=100,
    )

    store.record_monitor_event(
        event_key="invalidate-open-position-plan",
        lane_id="lane-a",
        minute_end=_at(10, 30),
        action=MonitorAction.PLAN_INVALIDATED,
        reason_code="CLOSED_5M_BREAK",
        effective=True,
        terminal_plan_id="plan-1",
        sync_a4_lifecycle=True,
        payload={"plan_id": "plan-1", "symbol": "002837.SZ"},
    )

    assert store.get_execution_plan("plan-1")["status"] == PlanStatus.INVALIDATED.value
    lifecycle = store.get_a4_lifecycle("entry-1")
    assert lifecycle["status"] == A4SignalStatus.EXIT_PENDING.value
    assert lifecycle["remaining_qty"] == 100
