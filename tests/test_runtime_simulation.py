from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.simulation import (
    PaperBroker,
    SimulationAction,
    SimulationActionType,
    SimulationConfig,
    SimulationStatus,
    resolve_action_conflicts,
)
from liangjian_funnel.runtime.state import RuntimeStore


TZ = ZoneInfo("Asia/Shanghai")


def bar(at: datetime, *, close: float = 10.0) -> MinuteBar:
    return MinuteBar(
        symbol="600519",
        interval="1m",
        bar_end=at,
        open=10,
        high=11,
        low=9,
        close=close,
        volume=1_000,
        amount=10_000,
        source_id="MOOTDX:127.0.0.1:7709",
    )


def action(account: str, signal: str, at: datetime, kind: str = "BUY", **updates):
    return SimulationAction(
        account_id=account,
        signal_id=signal,
        symbol="600519",
        action=kind,
        signal_bar_end=at,
        entry_reference=10,
        stop_level=9,
        **updates,
    )


def test_three_models_have_isolated_accounts_and_buy_is_full_lot_with_slippage(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    brokers = PaperBroker.for_models(store, ["model-a", "model-b", "model-c"])
    assert {broker.account_id for broker in brokers} == {"paper:model-a", "paper:model-b", "paper:model-c"}
    signal_time = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    result = brokers[0].apply(action("paper:model-a", "s1", signal_time), bar(signal_time + timedelta(minutes=1)))
    assert result.status is SimulationStatus.FILLED
    assert result.qty % 100 == 0
    assert result.price is not None and result.price > 10
    assert store.get_account("paper:model-b")["cash"] == 1_000_000
    assert store.get_position("paper:model-a", "600519.SH")["sellable_qty"] == 0


def test_t1_blocks_sell_until_next_session_and_cash_never_negative(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=20_000))
    signal_time = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    bought = broker.apply(action("paper:model-a", "s1", signal_time), bar(signal_time + timedelta(minutes=1)))
    assert bought.status is SimulationStatus.FILLED
    blocked = broker.apply(
        action("paper:model-a", "s2", signal_time + timedelta(minutes=2), "SELL"),
        bar(signal_time + timedelta(minutes=3)),
    )
    assert blocked.reason_code == "BLOCKED_T1"
    assert store.get_account("paper:model-a")["cash"] >= 0
    broker.start_trading_day()
    sold = broker.apply(
        action("paper:model-a", "s3", signal_time + timedelta(days=1), "SELL"),
        bar(signal_time + timedelta(days=1, minutes=1), close=10),
    )
    assert sold.status is SimulationStatus.FILLED
    assert store.get_position("paper:model-a", "600519.SH") is None


def test_stop_distance_position_sizing_and_idempotent_replay(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a")
    signal_time = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    first = broker.apply(action("paper:model-a", "s1", signal_time, requested_qty=137), bar(signal_time + timedelta(minutes=1)))
    assert first.qty == 100
    replay = broker.apply(action("paper:model-a", "s1", signal_time, requested_qty=137), bar(signal_time + timedelta(minutes=1)))
    assert replay.status is SimulationStatus.DUPLICATE
    assert len(store.list_fills("paper:model-a")) == 1


def test_action_priority_exits_win_over_buy_same_minute():
    at = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    actions = [
        action("paper:model-a", "buy", at, "BUY"),
        action("paper:model-a", "sell", at, "SELL"),
        action("paper:model-a", "stop", at, "FORCED_RISK_EXIT"),
    ]
    winners = resolve_action_conflicts(actions)
    assert len(winners) == 1
    assert winners[0].action is SimulationActionType.FORCED_RISK_EXIT


def test_buy_creates_risk_plan_add_requires_profit_and_has_limit(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=100_000))
    signal_time = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    bought = broker.apply(
        action("paper:model-a", "buy", signal_time, requested_qty=100, plan_id="plan-a"),
        bar(signal_time + timedelta(minutes=1), close=10),
    )
    assert bought.status is SimulationStatus.FILLED
    risk_plan = store.get_position_risk_plan("paper:model-a", "600519.SH")
    assert risk_plan["status"] == "ACTIVE"
    assert risk_plan["source_plan_id"] == "plan-a"

    losing_add = broker.apply(
        action("paper:model-a", "add-losing", signal_time + timedelta(minutes=2), "ADD", requested_qty=100),
        bar(signal_time + timedelta(minutes=3), close=9.5),
    )
    assert losing_add.reason_code == "ADD_REQUIRES_OPEN_PROFIT"
    profitable_add = broker.apply(
        action("paper:model-a", "add-profit", signal_time + timedelta(minutes=4), "ADD", requested_qty=100),
        bar(signal_time + timedelta(minutes=5), close=10.8),
    )
    assert profitable_add.status is SimulationStatus.FILLED
    maxed = broker.apply(
        action("paper:model-a", "add-maxed", signal_time + timedelta(minutes=6), "ADD", requested_qty=100),
        bar(signal_time + timedelta(minutes=7), close=11),
    )
    assert maxed.reason_code == "MAX_ADDS_REACHED"


def test_verified_corporate_action_adjusts_position_and_risk_plan_once(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=100_000))
    signal_time = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    broker.apply(
        action("paper:model-a", "buy", signal_time, requested_qty=100, plan_id="plan-a"),
        bar(signal_time + timedelta(minutes=1), close=10),
    )
    before = store.get_position("paper:model-a", "600519.SH")
    assert store.apply_corporate_action(
        account_id="paper:model-a",
        symbol="600519.SH",
        action_id="split-20260825",
        quantity_factor=2,
        price_factor=0.5,
        effective_at=datetime(2026, 8, 25, 9, 0, tzinfo=TZ),
    ) is True
    after = store.get_position("paper:model-a", "600519.SH")
    risk = store.get_position_risk_plan("paper:model-a", "600519.SH")
    assert after["total_qty"] == before["total_qty"] * 2
    assert after["avg_cost"] == before["avg_cost"] * 0.5
    assert risk["corporate_action_version"] == "split-20260825"
    assert store.apply_corporate_action(
        account_id="paper:model-a",
        symbol="600519.SH",
        action_id="split-20260825",
        quantity_factor=2,
        price_factor=0.5,
        effective_at=datetime(2026, 8, 25, 9, 0, tzinfo=TZ),
    ) is False
