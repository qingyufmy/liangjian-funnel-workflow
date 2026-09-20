from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.execution_accounting import FeeSchedule, OrderFeeLedger
from liangjian_funnel.runtime.entry_contract import freeze_entry_contract
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from liangjian_funnel.runtime.stock_trading_rules import stock_trading_rules
from liangjian_funnel.runtime.state import A4SignalStatus, MonitorAction, PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication, _quote_risk_bar


TZ = ZoneInfo("Asia/Shanghai")


def _bar(
    at: datetime,
    *,
    close: float = 10.0,
    volume: float = 10_000,
    source_id: str = "MOOTDX:test",
    evidence_kind: str = "MARKET_BAR",
) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval="1m",
        bar_end=at,
        open=10.0,
        high=max(10.5, close),
        low=min(9.5, close),
        close=close,
        volume=volume,
        amount=volume * close,
        source_id=source_id,
        volume_unit="shares",
        evidence_kind=evidence_kind,
    )


def _action(account: str, signal: str, at: datetime, **updates) -> SimulationAction:
    values = {
        "account_id": account,
        "signal_id": signal,
        "decision_id": f"decision:{signal}",
        "trigger_episode_id": f"episode:{signal}",
        "symbol": "600519.SH",
        "action": "BUY",
        "signal_bar_end": at,
        "data_available_at": at,
        "deterministic_decided_at": at,
        "review_completed_at": at,
        "order_created_at": at,
        "eligible_from": at,
        "expire_at": at + timedelta(minutes=1),
        "entry_reference": 10.0,
        "stop_level": 9.0,
        "stop_basis": "PLAN_HARD_STOP",
        "requested_qty": 100,
        "risk_reservation_id": f"risk:{signal}",
        "fee_model_version": "test-fee/1",
        "fill_model_version": "minute-capacity/2",
    }
    values.update(updates)
    return SimulationAction(**values)


def test_ex_01_fee_components_minimum_rounding_split_and_order_boundary() -> None:
    schedule = FeeSchedule(
        commission_bps=Decimal("1"),
        minimum_commission=Decimal("5"),
        sell_tax_bps=Decimal("5"),
        other_fee_bps=Decimal("0"),
        version="fixture/1",
    )
    assert OrderFeeLedger(schedule, side="SELL").charge(Decimal("1000"), final=True).total == Decimal("5.50")
    assert OrderFeeLedger(schedule, side="SELL").charge(Decimal("10000"), final=True).total == Decimal("10.00")
    assert OrderFeeLedger(schedule, side="SELL").charge(Decimal("100000"), final=True).total == Decimal("60.00")
    assert OrderFeeLedger(schedule, side="BUY").charge(Decimal("10000"), final=True).total == Decimal("5.00")

    one_order = OrderFeeLedger(schedule, side="SELL")
    first = one_order.charge(Decimal("5000"), final=False)
    second = one_order.charge(Decimal("5000"), final=True)
    assert first.commission + second.commission == Decimal("5.00")
    assert first.sell_tax + second.sell_tax == Decimal("5.00")
    assert sum((first.total, second.total), Decimal("0")) == Decimal("10.00")

    two_orders = [OrderFeeLedger(schedule, side="SELL").charge(Decimal("5000"), final=True) for _ in range(2)]
    assert sum((item.commission for item in two_orders), Decimal("0")) == Decimal("10.00")
    with pytest.raises(ValueError):
        OrderFeeLedger(schedule, side="BUY").charge(Decimal("NaN"), final=True)


def test_ex_02_future_close_does_not_change_frozen_order_quantity(tmp_path) -> None:
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    quantities = []
    for index, close in enumerate((9.7, 10.4)):
        store = RuntimeStore(tmp_path / f"future-{index}.sqlite3")
        broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
        result = broker.apply(_action("paper:lane", f"future-{index}", at), _bar(at + timedelta(minutes=1), close=close))
        quantities.append(result.qty)
    assert quantities == [100, 100]


def test_ex_03_mid_bar_review_uses_only_following_complete_bar(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "mid-bar.sqlite3")
    broker = PaperBroker(store, model="lane")
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    contract = freeze_entry_contract(
        "600519.SH",
        {"stop_level": 9, "expires_at": at.replace(hour=15)},
        {"reference_price": 10},
        at=at,
        review_completed_at=at + timedelta(seconds=25),
    )
    assert contract["eligible_bar_end"] == (at + timedelta(minutes=2)).isoformat()
    assert contract["clock_quality"] == "OBSERVED_WALL_CLOCK"
    action = _action(
        "paper:lane",
        "mid-bar",
        at,
        review_completed_at=at + timedelta(seconds=25),
        order_created_at=at + timedelta(seconds=25),
        eligible_from=at + timedelta(seconds=25),
        expire_at=at + timedelta(minutes=2),
    )
    assert broker.apply(action, _bar(at + timedelta(minutes=1))).reason_code == "ORDER_NOT_YET_ELIGIBLE"
    assert broker.apply(action, _bar(at + timedelta(minutes=2))).reason_code == "FILLED"


def test_ex_04_lunch_and_expiry_are_session_aware_and_never_backfilled(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "lunch.sqlite3")
    broker = PaperBroker(store, model="lane")
    at = datetime(2026, 9, 18, 11, 30, tzinfo=TZ)
    lunch = _action("paper:lane", "lunch", at, expire_at=datetime(2026, 9, 18, 13, 1, tzinfo=TZ))
    assert broker.apply(lunch, _bar(datetime(2026, 9, 18, 13, 1, tzinfo=TZ))).reason_code == "FILLED"

    expired = _action("paper:lane", "expired", at, expire_at=datetime(2026, 9, 18, 13, 0, tzinfo=TZ))
    assert broker.apply(expired, _bar(datetime(2026, 9, 18, 13, 1, tzinfo=TZ))).reason_code == "ORDER_EXPIRED"


def test_ex_05_capacity_caps_fill_and_records_remaining_quantity(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "capacity.sqlite3")
    broker = PaperBroker(
        store,
        model="lane",
        config=SimulationConfig(initial_cash=100_000, max_volume_participation=0.10),
    )
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    result = broker.apply(
        _action("paper:lane", "capacity", at, requested_qty=500),
        _bar(at + timedelta(minutes=1), volume=1_500),
    )
    assert result.reason_code == "PARTIALLY_FILLED"
    assert result.qty == 100
    assert result.remaining_qty == 400
    intent = store.get_simulation_intent("paper:lane:capacity:BUY")
    assert intent["status"] == "PARTIALLY_FILLED_EXPIRED"
    assert intent["reserved_cash"] == 0


def test_ex_06_security_rules_come_from_versioned_provider() -> None:
    main = stock_trading_rules("600519.SH")
    chinext = stock_trading_rules("300750.SZ")
    star = stock_trading_rules("688981.SH")
    assert main.version and chinext.version == main.version and star.version == main.version
    assert main.floor_buy(199) == 100
    assert chinext.floor_buy(199) == 100
    assert star.floor_buy(199) == 0
    assert star.floor_buy(201) == 201
    assert main.adverse_tick(10.001, buy=True) == 10.01
    with pytest.raises(ValueError, match="UNSUPPORTED_SECURITY_RULES"):
        stock_trading_rules("900001.SH")


def test_ex_07_accounting_is_atomic_and_concurrent_orders_do_not_double_spend(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "atomic.sqlite3")
    first = PaperBroker(store, account_id="paper:lane", model="lane", config=SimulationConfig(initial_cash=1_100))
    second = PaperBroker(store, account_id="paper:lane", model="lane", config=SimulationConfig(initial_cash=1_100))
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    actions = (
        (_action("paper:lane", "one", at), _bar(at + timedelta(minutes=1))),
        (
            _action("paper:lane", "two", at, symbol="000001.SZ"),
            _bar(at + timedelta(minutes=1)).model_copy(update={"symbol": "000001.SZ"}),
        ),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(broker.apply, action, bar)
            for broker, (action, bar) in zip((first, second), actions)
        ]
        results = tuple(future.result() for future in futures)
    assert sum(result.qty for result in results) <= 100
    account = store.get_account("paper:lane")
    assert account["cash"] >= 0
    fills = store.list_fills("paper:lane")
    assert len(fills) <= 1
    if fills:
        fill = fills[0]
        assert fill["fee"] == pytest.approx(fill["commission"] + fill["sell_tax"] + fill["other_fee"])
        assert account["cash"] == pytest.approx(1_100 - fill["gross"] - fill["fee"])


def test_ex_08_legacy_fill_history_is_not_overwritten_by_new_replay(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "versions.sqlite3")
    broker = PaperBroker(store, model="lane", config=SimulationConfig(initial_cash=100_000))
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    legacy = SimulationAction(
        account_id="paper:lane",
        signal_id="legacy",
        symbol="600519.SH",
        action="BUY",
        signal_bar_end=at,
        entry_reference=10,
        stop_level=9,
        requested_qty=100,
    )
    assert broker.apply(legacy, _bar(at + timedelta(minutes=1))).reason_code == "FILLED"
    legacy_fill = store.list_fills("paper:lane")[0]
    assert legacy_fill["fill_model_version"] == "legacy-next-minute/1"

    # A replay has a new account/run identity; it cannot mutate the immutable
    # historical ledger row merely because its execution model changed.
    replay = PaperBroker(store, account_id="paper:replay-2", model="lane-replay", config=SimulationConfig(initial_cash=100_000))
    new_action = _action("paper:replay-2", "legacy", at, replay_run_id="replay:s05")
    assert replay.apply(new_action, _bar(at + timedelta(minutes=1))).reason_code == "FILLED"
    assert store.list_fills("paper:lane")[0] == legacy_fill
    replay_intent = store.get_simulation_intent("paper:replay-2:legacy:BUY")
    assert "replay:s05" in replay_intent["order_metadata_json"]


def test_ex_09_synthetic_quote_cannot_supply_fill_or_capacity(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "synthetic.sqlite3")
    broker = PaperBroker(store, model="lane")
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    quote = SimpleNamespace(
        price=10.0,
        open=9.9,
        volume=1_000,
        amount=10_000,
        source_id="TENCENT_QUOTE",
        quote_time=at + timedelta(minutes=1),
    )
    synthetic = _quote_risk_bar("600519.SH", quote, at + timedelta(minutes=1))
    assert synthetic.evidence_kind == "SYNTHETIC_QUOTE"
    assert broker.apply(_action("paper:lane", "synthetic", at), synthetic).reason_code == "FILL_EVIDENCE_INVALID"


def test_ex_10_workflow_freezes_order_contract_and_rejects_risk_quote(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "workflow.sqlite3")
    at = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan",
        "lane",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        expires_at=at.replace(hour=15),
        payload={"trigger_low": 10, "trigger_high": 10.5, "stop_level": 9},
    )
    store.record_monitor_event(
        event_key="effective:workflow",
        lane_id="lane",
        minute_end=at,
        action=MonitorAction.BUY_SIGNAL,
        effective=True,
        sync_a4_lifecycle=True,
        payload={
            "plan_id": "plan",
            "symbol": "600519.SH",
            "entry_contract": {
                "status": "READY",
                "limit_price": 10,
                "stop_level": 9,
                "risk_unit": 1,
                "eligible_bar_end": (at + timedelta(minutes=1)).isoformat(),
                "version": "a4-entry/2",
            },
        },
    )
    broker = PaperBroker(store, account_id="paper:lane", model="lane")
    app = SimpleNamespace(store=store, brokers={"lane": broker})
    synthetic = _bar(
        at + timedelta(minutes=1),
        source_id="TENCENT_QUOTE:RISK_ONLY",
        evidence_kind="SYNTHETIC_QUOTE",
    )
    result = WorkflowApplication._settle_prior_signals(app, "lane", "600519.SH", synthetic)
    assert result[0]["reason_code"] == "FILL_EVIDENCE_INVALID"
    assert store.list_fills("paper:lane") == ()
    assert store.get_a4_signal_lifecycle("effective:workflow")["status"] == A4SignalStatus.SIGNALLED.value
