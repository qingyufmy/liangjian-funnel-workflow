from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.simulation import (
    PaperBroker,
    SimulationAction,
    SimulationActionType,
    SimulationConfig,
    SimulationStatus,
    resolve_action_conflicts,
)
from liangjian_funnel.runtime.state import PersistenceError, RuntimeStore


TZ = ZoneInfo("Asia/Shanghai")


def _bar(
    at: datetime,
    *,
    interval: str = "1m",
    close: float = 10.0,
    volume: float = 1_000,
    high: float = 11.0,
    low: float = 9.0,
) -> MinuteBar:
    return MinuteBar(
        symbol="600519",
        interval=interval,
        bar_end=at,
        open=10,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=10_000,
        source_id="MOOTDX:test",
    )


def _action(
    account: str,
    signal: str,
    at: datetime,
    kind: str = "BUY",
    *,
    entry: float = 10.0,
    stop: float | None = 9.0,
    requested_qty: int | None = None,
    plan_id: str | None = None,
    symbol: str = "600519",
) -> SimulationAction:
    return SimulationAction(
        account_id=account,
        signal_id=signal,
        symbol=symbol,
        action=kind,
        signal_bar_end=at,
        entry_reference=entry,
        stop_level=stop,
        requested_qty=requested_qty,
        plan_id=plan_id,
    )


def test_simulation_configuration_and_action_contracts_are_strict() -> None:
    with pytest.raises(ValueError, match="lot_size"):
        SimulationConfig(lot_size=200)
    with pytest.raises(ValueError, match="total position cap"):
        SimulationConfig(max_single_position_pct=0.8, max_total_position_pct=0.7)
    with pytest.raises(ValidationError, match="timezone-aware"):
        _action("paper:m", "s", datetime(2026, 8, 29, 9, 30))
    alias = _action("paper:m", "s", datetime(2026, 8, 29, 9, 30, tzinfo=TZ), "BUY_SIGNAL")
    assert alias.action is SimulationActionType.BUY
    with pytest.raises(ValidationError, match="positive and finite"):
        _action("paper:m", "bad-price", datetime(2026, 8, 29, 9, 30, tzinfo=TZ), entry=float("nan"))


def test_conflict_resolution_accepts_mappings_and_is_stable_on_equal_priority() -> None:
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    first = _action("paper:m", "first", at, "BUY")
    second = _action("paper:m", "second", at, "BUY")
    forced = _action("paper:m", "forced", at, "FORCED_RISK_EXIT")
    winners = resolve_action_conflicts([first.model_dump(), second.model_dump(), forced])
    assert len(winners) == 1
    assert winners[0].signal_id == "forced"
    assert resolve_action_conflicts([first, second])[0].signal_id == "first"


def test_quantity_calculation_rejects_invalid_risk_inputs_and_caps_lots(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=10_000))
    assert broker.calculate_quantity(entry_reference=10, stop_level=10)[1] == "INVALID_STOP_DISTANCE"
    assert broker.calculate_quantity(entry_reference=10, stop_level=11)[1] == "INVALID_STOP_DIRECTION"
    assert broker.calculate_quantity(entry_reference=10, stop_level=9, risk_unit=0)[1] == "INVALID_RISK_UNIT"
    assert broker.calculate_quantity(entry_reference=float("nan"), stop_level=9)[1] == "INVALID_PRICE"
    assert broker.calculate_quantity(entry_reference=10, stop_level=9, mark_price=-1)[1] == "INVALID_PRICE"
    quantity, reason = broker.calculate_quantity(
        symbol="600519.SH",
        entry_reference=10,
        stop_level=9,
        requested_qty=137,
    )
    assert reason == "OK"
    assert quantity == 100


def test_quantity_calculation_handles_missing_account_and_position_caps(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(
        store,
        model="model-a",
        config=SimulationConfig(initial_cash=10_000, max_single_position_pct=0.005),
    )

    with sqlite3.connect(store.path) as connection:
        connection.execute("DELETE FROM virtual_accounts WHERE account_id=?", ("paper:model-a",))
        connection.commit()
    assert broker.calculate_quantity(entry_reference=10, stop_level=9)[1] == "ACCOUNT_NOT_FOUND"

    capped_store = RuntimeStore(tmp_path / "capped.sqlite3")
    capped = PaperBroker(
        capped_store,
        model="model-a",
        config=SimulationConfig(initial_cash=10_000, max_single_position_pct=0.005),
    )
    quantity, reason = capped.calculate_quantity(entry_reference=10, stop_level=9)
    assert (quantity, reason) == (0, "POSITION_OR_CASH_CAP")


def test_apply_fail_closed_paths_do_not_create_fills(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a")
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    complete = _bar(at + timedelta(minutes=1))

    mismatch = broker.apply(_action("paper:other", "mismatch", at), complete)
    assert mismatch.reason_code == "ACCOUNT_LANE_MISMATCH"
    assert broker.apply(_action("paper:model-a", "cancel", at, "CANCEL"), complete).status is SimulationStatus.CANCELLED
    assert broker.apply(_action("paper:model-a", "five", at), _bar(at + timedelta(minutes=1), interval="5m")).reason_code == "BAR_INTERVAL_INVALID"
    assert broker.apply(_action("paper:model-a", "same", at), _bar(at)).reason_code == "NEXT_COMPLETE_BAR_REQUIRED"
    assert broker.apply(_action("paper:model-a", "empty", at), _bar(at + timedelta(minutes=1), volume=0)).reason_code == "BAR_NOT_EXECUTABLE"
    late = datetime(2026, 8, 29, 14, 45, tzinfo=TZ)
    assert broker.apply(_action("paper:model-a", "late", late), _bar(late + timedelta(minutes=1))).reason_code == "BUY_AFTER_CLOSE"
    outside_store = RuntimeStore(tmp_path / "outside.sqlite3")
    outside_broker = PaperBroker(outside_store, model="model-a")
    assert outside_broker.apply(_action("paper:model-a", "outside", at, entry=20, stop=19), complete).reason_code == "PRICE_OUTSIDE_BAR"
    assert broker.apply(_action("paper:model-a", "bad-stop", at, stop=None), complete).reason_code == "INVALID_STOP_DIRECTION"

    assert store.list_fills("paper:model-a") == ()
    store.mark_persistence_failed()
    assert broker.apply(_action("paper:model-a", "persistence", at), complete).reason_code == "PERSISTENCE_FAILED"


def test_apply_enforces_position_state_t1_account_status_and_forced_exit(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=100_000))
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    bought = broker.apply(_action("paper:model-a", "buy", at, requested_qty=100, plan_id="p"), _bar(at + timedelta(minutes=1)))
    assert bought.status is SimulationStatus.FILLED
    assert broker.apply(_action("paper:model-a", "duplicate-buy", at + timedelta(minutes=2)), _bar(at + timedelta(minutes=3))).reason_code == "POSITION_ALREADY_OPEN"
    assert broker.apply(_action("paper:model-a", "t1-sell", at + timedelta(minutes=2), "SELL"), _bar(at + timedelta(minutes=3))).reason_code == "BLOCKED_T1"
    assert broker.apply(_action("paper:model-a", "no-stop-add", at + timedelta(minutes=2), "ADD", stop=9), _bar(at + timedelta(minutes=3), close=8, low=7)).reason_code == "ADD_REQUIRES_OPEN_PROFIT"

    broker.start_trading_day(trade_date=datetime(2026, 8, 30, tzinfo=TZ).date())
    blocked_over_sell = broker.apply(
        _action("paper:model-a", "over-sell", at + timedelta(days=1), "SELL", requested_qty=200),
        _bar(at + timedelta(days=1, minutes=1)),
    )
    assert blocked_over_sell.reason_code == "BLOCKED_T1"
    invalid_qty = broker.apply(
        _action("paper:model-a", "invalid-sell", at + timedelta(days=1), "SELL", requested_qty=1),
        _bar(at + timedelta(days=1, minutes=1)),
    )
    assert invalid_qty.reason_code == "INVALID_SELL_QTY"
    exited = broker.apply(
        _action("paper:model-a", "forced", at + timedelta(days=1), "FORCED_RISK_EXIT"),
        _bar(at + timedelta(days=1, minutes=2)),
    )
    assert exited.status is SimulationStatus.FILLED
    assert store.get_position("paper:model-a", "600519.SH") is None

    # An inactive account is rejected after the deterministic risk checks and
    # before any market mark or ledger mutation.
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE virtual_accounts SET status='DISABLED' WHERE account_id=?", ("paper:model-a",))
        connection.commit()
    unavailable = broker.apply(_action("paper:model-a", "inactive", at + timedelta(days=2)), _bar(at + timedelta(days=2, minutes=1)))
    assert unavailable.reason_code == "ACCOUNT_UNAVAILABLE"


def test_apply_exercises_idempotency_equity_and_provider_failure_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    store = RuntimeStore(tmp_path / "apply.sqlite3")
    broker = PaperBroker(store, model="model-a", config=SimulationConfig(initial_cash=100_000))
    first_action = _action("paper:model-a", "buy-once", at, requested_qty=100)
    first = broker.apply(first_action, _bar(at + timedelta(minutes=1)))
    assert first.status is SimulationStatus.FILLED

    # Replaying the same intent is safe and returns the durable fill rather
    # than trying to open a second position.
    replay = broker.apply(first_action, _bar(at + timedelta(minutes=2)))
    assert replay.status is SimulationStatus.DUPLICATE
    assert replay.reason_code == "IDEMPOTENT_REPLAY"
    assert replay.qty == first.qty

    # A second symbol exercises equity accounting for an existing unrelated
    # position (the false arm of the same-symbol replacement branch).
    broker.start_trading_day(trade_date=datetime(2026, 8, 30, tzinfo=TZ).date())
    second = broker.apply(
        _action(
            "paper:model-a",
            "buy-other-symbol",
            at + timedelta(days=1, minutes=2),
            requested_qty=100,
            symbol="000001.SZ",
        ),
        _bar(at + timedelta(days=1, minutes=3)),
    )
    assert second.status is SimulationStatus.FILLED
    assert store.get_position("paper:model-a", "000001.SZ") is not None

    # The durable fill lookup itself is fail-closed if the local store is
    # unavailable, before any market/account mutation is attempted.
    lookup_store = RuntimeStore(tmp_path / "lookup.sqlite3")
    lookup_broker = PaperBroker(lookup_store, model="model-a")

    def fail_lookup(_intent_key):
        raise PersistenceError("lookup unavailable")

    monkeypatch.setattr(lookup_store, "get_fill_by_intent_key", fail_lookup)
    lookup_result = lookup_broker.apply(
        _action("paper:model-a", "lookup-failure", at),
        _bar(at + timedelta(minutes=1)),
    )
    assert lookup_result.reason_code == "PERSISTENCE_FAILED"

    # Commit failure is handled separately from an earlier writability check;
    # no in-memory success can escape without a durable ledger row.
    commit_store = RuntimeStore(tmp_path / "commit.sqlite3")
    commit_broker = PaperBroker(commit_store, model="model-a")

    def fail_commit(**_kwargs):
        raise PersistenceError("commit unavailable")

    monkeypatch.setattr(commit_store, "commit_fill", fail_commit)
    commit_result = commit_broker.apply(
        _action("paper:model-a", "commit-failure", at),
        _bar(at + timedelta(minutes=1)),
    )
    assert commit_result.reason_code == "PERSISTENCE_FAILED"
    assert commit_store.list_fills("paper:model-a") == ()


def test_apply_keeps_broker_guards_after_risk_and_quantity_dependencies_allow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    allow = lambda *_args: SimpleNamespace(allowed=True, reason_code="OK")

    # The broker still requires a stop even if a caller's upstream risk
    # adapter reports an otherwise-allowed action.
    stop_store = RuntimeStore(tmp_path / "stop.sqlite3")
    stop_broker = PaperBroker(stop_store, model="model-a")
    monkeypatch.setattr(stop_broker.risk_governor, "evaluate", allow)
    missing_stop = stop_broker.apply(
        _action("paper:model-a", "missing-stop", at, stop=None),
        _bar(at + timedelta(minutes=1)),
    )
    assert missing_stop.reason_code == "STOP_LEVEL_REQUIRED"

    # A requested quantity smaller than one A-share lot is not silently
    # rounded into a fill.
    zero_store = RuntimeStore(tmp_path / "zero-quantity.sqlite3")
    zero_broker = PaperBroker(zero_store, model="model-a")
    monkeypatch.setattr(zero_broker.risk_governor, "evaluate", allow)
    zero_quantity = zero_broker.apply(
        _action("paper:model-a", "zero-quantity", at, requested_qty=1),
        _bar(at + timedelta(minutes=1)),
    )
    assert zero_quantity.reason_code == "POSITION_OR_CASH_CAP"

    # ADD cannot manufacture a position.  This is checked at the broker
    # boundary even when the external risk adapter is permissive.
    add_store = RuntimeStore(tmp_path / "add.sqlite3")
    add_broker = PaperBroker(add_store, model="model-a")
    monkeypatch.setattr(add_broker.risk_governor, "evaluate", allow)
    add_without_position = add_broker.apply(
        _action("paper:model-a", "add-without-position", at, "ADD"),
        _bar(at + timedelta(minutes=1)),
    )
    assert add_without_position.reason_code == "ADD_WITHOUT_POSITION"

    # Quantity calculation normally caps to cash.  Simulate a stale/corrupt
    # calculator result to verify the final exact cash check remains active.
    cash_store = RuntimeStore(tmp_path / "cash.sqlite3")
    cash_broker = PaperBroker(cash_store, model="model-a", config=SimulationConfig(initial_cash=100))
    monkeypatch.setattr(cash_broker.risk_governor, "evaluate", allow)
    monkeypatch.setattr(cash_broker, "calculate_quantity", lambda **_kwargs: (100, "OK"))
    insufficient_cash = cash_broker.apply(
        _action("paper:model-a", "insufficient-cash", at),
        _bar(at + timedelta(minutes=1)),
    )
    assert insufficient_cash.reason_code == "INSUFFICIENT_CASH"

    # SELL on an empty account is a broker-level no-position guard.
    sell_store = RuntimeStore(tmp_path / "sell-empty.sqlite3")
    sell_broker = PaperBroker(sell_store, model="model-a")
    monkeypatch.setattr(sell_broker.risk_governor, "evaluate", allow)
    no_position = sell_broker.apply(
        _action("paper:model-a", "sell-empty", at, "SELL"),
        _bar(at + timedelta(minutes=1)),
    )
    assert no_position.reason_code == "NO_POSITION"


def test_adverse_price_rejects_corrupted_internal_reference(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "price.sqlite3")
    broker = PaperBroker(store, model="model-a")
    at = datetime(2026, 8, 29, 9, 30, tzinfo=TZ)
    invalid_action = SimpleNamespace(entry_reference=float("nan"), action=SimulationActionType.BUY)
    assert broker._adverse_price(invalid_action, _bar(at + timedelta(minutes=1))) is None
