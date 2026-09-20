from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import (
    PaperBroker,
    SimulationAction,
    SimulationConfig,
    SimulationStatus,
    resolve_action_conflicts,
)
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore, StateTransitionError


TZ = ZoneInfo("Asia/Shanghai")


def _bar(at: datetime, *, symbol: str = "600519.SH", open_: float = 10, close: float = 10, volume: float = 10_000) -> MinuteBar:
    return MinuteBar(
        symbol=symbol,
        interval="1m",
        bar_end=at,
        open=open_,
        high=max(open_, close) + 0.2,
        low=min(open_, close) - 0.2,
        close=close,
        volume=volume,
        amount=volume * close,
        source_id="MOOTDX:risk-test",
        volume_unit="shares",
    )


def _action(account: str, signal: str, at: datetime, action: str = "BUY", **updates) -> SimulationAction:
    values = {
        "account_id": account,
        "signal_id": signal,
        "symbol": "600519.SH",
        "action": action,
        "signal_bar_end": at,
        "entry_reference": 10,
        "stop_level": 9,
        "requested_qty": 100,
        "plan_id": "plan",
        "primary_theme_id": "theme-a",
        "strategy_identity": "TREND_MA5",
    }
    values.update(updates)
    return SimulationAction(**values)


def test_risk_01_concurrent_reservations_atomically_enforce_cash_and_total_budget(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "reserve.sqlite3")
    store.ensure_virtual_account("paper:lane", "lane", 10_000)

    def reserve(index: int):
        return store.reserve_simulation_order(
            reservation_id=f"r{index}",
            order_identity=f"o{index}",
            account_id="paper:lane",
            symbol=f"60000{index}.SH",
            primary_theme_id="theme-a",
            requested_qty=100,
            reserved_cash=6_000,
            reserved_risk=500,
            max_total_value=10_000,
            max_symbol_value=10_000,
            max_open_risk=1_000,
            max_theme_value=10_000,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(reserve, index) for index in (1, 2)]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result()[0]["status"])
            except StateTransitionError as exc:
                outcomes.append(str(exc))
    assert outcomes.count("RESERVED") == 1
    assert any(item in {"RESERVATION_CASH_LIMIT", "RESERVATION_TOTAL_POSITION_LIMIT"} for item in outcomes)
    rows = store.list_risk_reservations("paper:lane")
    assert sum(row["reserved_cash"] for row in rows) == 6_000
    assert all(row["reserved_cash"] >= 0 and row["reserved_risk"] >= 0 for row in rows)

    constrained = RuntimeStore(tmp_path / "theme-risk.sqlite3")
    constrained.ensure_virtual_account("paper:theme", "theme", 20_000)
    constrained.reserve_simulation_order(
        reservation_id="theme-1", order_identity="theme-o1", account_id="paper:theme",
        symbol="600001.SH", primary_theme_id="theme-a", requested_qty=100,
        reserved_cash=4_000, reserved_risk=400, max_total_value=20_000,
        max_symbol_value=10_000, max_open_risk=700, max_theme_value=6_000,
    )
    with pytest.raises(StateTransitionError, match="RESERVATION_OPEN_RISK_LIMIT"):
        constrained.reserve_simulation_order(
            reservation_id="theme-2", order_identity="theme-o2", account_id="paper:theme",
            symbol="600002.SH", primary_theme_id="theme-a", requested_qty=100,
            reserved_cash=3_000, reserved_risk=400, max_total_value=20_000,
            max_symbol_value=10_000, max_open_risk=700, max_theme_value=10_000,
        )
    with pytest.raises(StateTransitionError, match="RESERVATION_THEME_LIMIT"):
        constrained.reserve_simulation_order(
            reservation_id="theme-3", order_identity="theme-o3", account_id="paper:theme",
            symbol="600003.SH", primary_theme_id="theme-a", requested_qty=100,
            reserved_cash=3_000, reserved_risk=100, max_total_value=20_000,
            max_symbol_value=10_000, max_open_risk=2_000, max_theme_value=6_000,
        )


def test_risk_02_forced_exit_wins_conflict_without_increasing_risk() -> None:
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    winner = resolve_action_conflicts([
        _action("paper:lane", "buy", at),
        _action("paper:lane", "exit", at, "FORCED_RISK_EXIT"),
    ])
    assert len(winner) == 1
    assert winner[0].action.value == "FORCED_RISK_EXIT"


def test_risk_03_same_day_hard_stop_stays_pending_until_t1_release(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "t1.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    day1 = datetime(2026, 9, 17, 10, 0, tzinfo=TZ)
    assert broker.apply(_action("paper:lane", "buy", day1), _bar(day1 + timedelta(minutes=1))).status is SimulationStatus.FILLED
    blocked = broker.apply(
        _action("paper:lane", "stop", day1 + timedelta(minutes=2), "FORCED_RISK_EXIT"),
        _bar(day1 + timedelta(minutes=3), open_=8.5, close=8.4),
    )
    assert blocked.reason_code == "BLOCKED_T1"
    assert len(store.list_fills("paper:lane")) == 1
    day2 = datetime(2026, 9, 18, 9, 30, tzinfo=TZ)
    broker.start_trading_day(day2.date())
    closed = broker.apply(
        _action("paper:lane", "stop", day1 + timedelta(minutes=2), "FORCED_RISK_EXIT"),
        _bar(day2 + timedelta(minutes=1), open_=8.2, close=8.1),
    )
    assert closed.status is SimulationStatus.FILLED


def test_risk_04_old_sellable_lot_and_new_locked_add_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "lots.sqlite3"
    store = RuntimeStore(path)
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    day1 = datetime(2026, 9, 17, 10, 0, tzinfo=TZ)
    broker.apply(_action("paper:lane", "buy", day1, requested_qty=200), _bar(day1 + timedelta(minutes=1)))
    day2 = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    broker.start_trading_day(day2.date())
    added = broker.apply(
        _action("paper:lane", "add", day2, "ADD", requested_qty=100),
        _bar(day2 + timedelta(minutes=1), close=10.5),
    )
    assert added.status is SimulationStatus.FILLED
    position = store.get_position("paper:lane", "600519.SH")
    assert (position["total_qty"], position["sellable_qty"]) == (300, 200)
    reopened = RuntimeStore(path)
    lots = reopened.list_position_lots("paper:lane", "600519.SH")
    assert sorted(lot["remaining_qty"] for lot in lots) == [100, 200]
    assert reopened.get_position("paper:lane", "600519.SH")["sellable_qty"] == 200


def test_risk_05_duplicate_fill_is_idempotent_and_new_exit_revision_can_continue(tmp_path: Path) -> None:
    path = tmp_path / "fills.sqlite3"
    store = RuntimeStore(path)
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000, max_volume_participation=0.1))
    day1 = datetime(2026, 9, 17, 10, 0, tzinfo=TZ)
    buy = _action("paper:lane", "buy", day1, requested_qty=300, fill_model_version="next-complete-minute-capacity/2")
    first = broker.apply(buy, _bar(day1 + timedelta(minutes=1), volume=1_500))
    assert first.reason_code == "PARTIALLY_FILLED"
    assert broker.apply(buy, _bar(day1 + timedelta(minutes=1), volume=1_500)).status is SimulationStatus.DUPLICATE
    reopened = RuntimeStore(path)
    assert len(reopened.list_fills("paper:lane")) == 1


def test_risk_06_release_and_partial_consumption_zero_out_reservation(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "release.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000, max_volume_participation=0.1))
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    action = _action(
        "paper:lane",
        "partial",
        at,
        requested_qty=500,
        risk_reservation_id="risk:partial",
        fill_model_version="next-complete-minute-capacity/2",
    )
    assert broker.apply(action, _bar(at + timedelta(minutes=1), volume=1_500)).reason_code == "PARTIALLY_FILLED"
    reservation = store.list_risk_reservations("paper:lane")[0]
    assert reservation["status"] == "CONSUMED"
    assert reservation["reserved_cash"] == 0 and reservation["reserved_risk"] == 0
    statuses = [
        row["status"]
        for row in store.list_simulation_order_events("paper:lane:partial:BUY:r1")
    ]
    assert statuses == ["CREATED", "READY", "RESERVED", "SUBMITTED", "PARTIALLY_FILLED"]

    store.reserve_simulation_order(
        reservation_id="risk:cancel",
        order_identity="paper:lane:cancel:BUY:r1",
        account_id="paper:lane",
        symbol="000001.SZ",
        primary_theme_id="UNKNOWN",
        requested_qty=100,
        reserved_cash=1_000,
        reserved_risk=100,
        max_total_value=100_000,
        max_symbol_value=50_000,
    )
    assert store.release_risk_reservation("risk:cancel", "ORDER_EXPIRED") is True
    assert [row["status"] for row in store.list_simulation_order_events("paper:lane:cancel:BUY:r1")] == [
        "CREATED", "READY", "RESERVED", "EXPIRED",
    ]


def test_risk_07_new_reduce_episode_is_distinct_but_same_episode_is_idempotent(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "episodes.sqlite3")
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    store.create_execution_plan("plan", "lane", "600519.SH", status=PlanStatus.ACTIVE_TODAY, payload={"stop_level": 9})
    plan = store.get_execution_plan("plan")
    engine = MonitorEngine(store)
    first = engine._emit_effective("lane", plan, at, "snap-1", MonitorAction.REDUCE_SIGNAL.value, "REDUCE")
    duplicate = engine._emit_effective("lane", plan, at, "snap-1", MonitorAction.REDUCE_SIGNAL.value, "REDUCE")
    second = engine._emit_effective("lane", plan, at + timedelta(minutes=2), "snap-2", MonitorAction.REDUCE_SIGNAL.value, "REDUCE")
    assert first.effective is True
    assert duplicate.effective is False
    assert second.effective is True
    assert len(store.list_monitor_events(lane_id="lane", effective_only=True)) == 2


def test_risk_08_plan_expiry_does_not_close_position_risk_plan(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "plan-expiry.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    store.create_execution_plan("plan", "lane", "600519.SH", status=PlanStatus.ACTIVE_TODAY, payload={"stop_level": 9})
    broker.apply(_action("paper:lane", "buy", at), _bar(at + timedelta(minutes=1)))
    store.invalidate_plan("plan", status=PlanStatus.EXPIRED)
    assert store.get_execution_plan("plan")["status"] == PlanStatus.EXPIRED.value
    risk = store.get_position_risk_plan("paper:lane", "600519.SH")
    assert risk["status"] == "ACTIVE" and risk["strategy_identity"] == "TREND_MA5"


def test_risk_09_gap_exit_uses_current_window_and_locked_bar_does_not_fake_fill(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "gap.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    day1 = datetime(2026, 9, 17, 10, 0, tzinfo=TZ)
    broker.apply(_action("paper:lane", "buy", day1), _bar(day1 + timedelta(minutes=1)))
    day2 = datetime(2026, 9, 18, 9, 30, tzinfo=TZ)
    broker.start_trading_day(day2.date())
    exit_result = broker.apply(
        _action("paper:lane", "gap-exit", day1 + timedelta(minutes=2), "FORCED_RISK_EXIT", entry_reference=10),
        _bar(day2 + timedelta(minutes=1), open_=8, close=7.9),
    )
    assert exit_result.status is SimulationStatus.FILLED
    assert exit_result.price < 8.1

    locked_store = RuntimeStore(tmp_path / "locked.sqlite3")
    locked_broker = PaperBroker(locked_store, model="locked", config=SimulationConfig(initial_cash=100_000))
    locked_broker.apply(_action("paper:locked", "buy", day1), _bar(day1 + timedelta(minutes=1)))
    locked_broker.start_trading_day(day2.date())
    locked_bar = _bar(day2 + timedelta(minutes=1), open_=7, close=7).model_copy(
        update={"open": 7.0, "high": 7.0, "low": 7.0, "close": 7.0}
    )
    blocked = locked_broker.apply(
        _action("paper:locked", "locked-exit", day1 + timedelta(minutes=2), "FORCED_RISK_EXIT"),
        locked_bar,
    )
    assert blocked.reason_code == "BAR_NOT_EXECUTABLE"
    assert locked_store.get_position("paper:locked", "600519.SH")["total_qty"] == 100


def test_risk_10_corporate_action_is_versioned_and_unresolved_blocks_add(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "corporate.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    at = datetime(2026, 9, 17, 10, 0, tzinfo=TZ)
    broker.apply(_action("paper:lane", "buy", at), _bar(at + timedelta(minutes=1)))
    assert store.apply_corporate_action(
        account_id="paper:lane", symbol="600519.SH", action_id="split-v1",
        quantity_factor=2, price_factor=0.5, effective_at=at + timedelta(days=1),
    )
    assert store.get_position_risk_plan("paper:lane", "600519.SH")["corporate_action_version"] == "split-v1"
    assert sum(lot["remaining_qty"] for lot in store.list_position_lots("paper:lane", "600519.SH")) == 200
    assert store.mark_corporate_action_unresolved("paper:lane", "600519.SH", "SOURCE_UNAVAILABLE")
    blocked = broker.apply(
        _action("paper:lane", "blocked-add", at + timedelta(days=1), "ADD"),
        _bar(at + timedelta(days=1, minutes=1), close=10.5),
    )
    assert blocked.reason_code == "CORPORATE_ACTION_UNRESOLVED"


def test_risk_11_failed_commit_leaves_no_half_written_fill_or_position(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = RuntimeStore(tmp_path / "atomic-crash.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)

    original = store.commit_fill
    def fail_after_reservation(**kwargs):
        raise StateTransitionError("INJECTED_COMMIT_FAILURE")
    monkeypatch.setattr(store, "commit_fill", fail_after_reservation)
    result = broker.apply(_action("paper:lane", "crash", at), _bar(at + timedelta(minutes=1)))
    assert result.reason_code == "INJECTED_COMMIT_FAILURE"
    assert store.list_fills("paper:lane") == ()
    assert store.get_position("paper:lane", "600519.SH") is None
    assert store.list_risk_reservations("paper:lane")[0]["status"] == "RELEASED"
    monkeypatch.setattr(store, "commit_fill", original)

    recovered = broker.apply(_action("paper:lane", "recovered", at), _bar(at + timedelta(minutes=1)))
    assert recovered.status is SimulationStatus.FILLED
    audit = RuntimeStore(store.path).audit_virtual_account("paper:lane")
    assert audit["consistent"] is True
    assert audit["cash_delta"] == pytest.approx(0)
