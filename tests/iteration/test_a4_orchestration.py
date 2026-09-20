from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import time

import pytest

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.data.tencent_minute import MarketQuote, QuoteResult
from liangjian_funnel.runtime.bounded_work import BoundedWorkGate
from liangjian_funnel.runtime.monitor import MonitorBatchResult, MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 21, 10, 0, tzinfo=TZ)
BUY_AT = datetime(2026, 9, 18, 10, 0, tzinfo=TZ)


def bar(symbol: str, *, close: float = 10.0, low: float = 9.8, at: datetime = NOW) -> MinuteBar:
    return MinuteBar(
        symbol=symbol,
        interval="1m",
        bar_end=at,
        open=close,
        high=max(close * 1.01, low),
        low=low,
        close=close,
        volume=100,
        amount=1000,
        source_id="TEST",
    )


def quote(symbol: str, *, price: float, at: datetime = NOW) -> QuoteResult:
    return QuoteResult(
        symbol=symbol,
        complete=True,
        reason_code="OK",
        quote=MarketQuote(
            symbol=symbol,
            quote_time=at,
            price=price,
            previous_close=price,
            open=price,
            volume=100,
            amount=price * 100,
            source_id="TEST_QUOTE",
        ),
    )


def add_position(store: RuntimeStore, *, lane: str, symbol: str, stop: float) -> None:
    plan_id = f"plan-{symbol}"
    store.create_execution_plan(
        plan_id,
        lane,
        symbol,
        status=PlanStatus.ACTIVE_TODAY,
        valid_from=BUY_AT - timedelta(minutes=5),
        expires_at=NOW + timedelta(days=1),
        payload={"stop_level": stop, "trigger_low": stop + 1, "trigger_high": stop + 2},
    )
    broker = PaperBroker(store, account_id=f"paper:{lane}", model="TEST")
    result = broker.apply(
        SimulationAction(
            account_id=f"paper:{lane}",
            signal_id=f"buy-{symbol}",
            symbol=symbol,
            action="BUY",
            signal_bar_end=BUY_AT - timedelta(minutes=1),
            entry_reference=10,
            stop_level=stop,
            requested_qty=100,
            plan_id=plan_id,
        ),
        bar(symbol, close=10, at=BUY_AT),
    )
    assert result.status.value == "FILLED", result.reason_code


def test_a4_01_hung_auxiliary_is_bounded_and_required_lane_stays_available() -> None:
    auxiliary = BoundedWorkGate(1)
    required = BoundedWorkGate(1)

    first = auxiliary.call(lambda: time.sleep(0.5), deadline=time.monotonic() + 0.02, name="hung-archive")
    second = auxiliary.call(lambda: "late", deadline=time.monotonic() + 0.02)
    critical = required.call(lambda: "risk-ready", deadline=time.monotonic() + 0.1)

    assert first.status == "TIMED_OUT"
    assert second.status == "BACKPRESSURE"
    assert critical.status == "READY" and critical.value == "risk-ready"


def test_a4_04_position_hard_stop_does_not_need_market_or_llm(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    add_position(store, lane="lane_1", symbol="600519.SH", stop=9.0)
    model_calls = 0

    def forbidden_model(_payload):
        nonlocal model_calls
        model_calls += 1
        raise AssertionError("position hard stop must not call LLM")

    result = MonitorEngine(store, llm_veto=forbidden_model).process_position_risk(
        "lane_1",
        {"600519.SH": bar("600519.SH", close=8.9, low=8.8)},
        minute_snapshot_id="risk-only",
        now=NOW,
    )
    assert model_calls == 0
    assert [(event.action, event.reason_code) for event in result.events] == [
        (MonitorAction.FORCED_RISK_EXIT.value, "HARD_STOP")
    ]


def test_a4_11_stale_position_quote_is_data_block_not_success(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    add_position(store, lane="lane_1", symbol="600519.SH", stop=9.0)
    stale = bar("600519.SH", close=8.9, low=8.8, at=NOW - timedelta(minutes=3))
    result = MonitorEngine(store).process_position_risk(
        "lane_1",
        {"600519.SH": stale},
        minute_snapshot_id="stale-risk",
        now=NOW,
    )
    assert result.blocked
    assert result.events[0].action == MonitorAction.DATA_BLOCK.value
    assert result.events[0].reason_code != "POSITION_RISK_CLEAR"


def test_a4_04_position_protection_is_written_before_plan_activation(tmp_path: Path) -> None:
    """A stuck recovery path cannot sit in front of an existing hard stop."""

    current = NOW.replace(minute=6)
    store = RuntimeStore(tmp_path / "state.sqlite3")
    add_position(store, lane="lane_1", symbol="600519.SH", stop=9.0)
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(workflow_output_dir=tmp_path / "outputs")
    app.store = store
    app.brokers = {"lane_1": object()}
    app._ensure_trading_day = lambda _: None
    app._expire_missed_a4_entries = lambda _: None
    app._fetch_live_quote = lambda *_args, **_kwargs: quote("600519.SH", price=8.8, at=current)
    app._settle_prior_signals = lambda *_args: []

    def activation_barrier(**_kwargs):
        events = store.list_monitor_events(lane_id="lane_1", effective_only=True)
        assert any(
            event["action"] == MonitorAction.FORCED_RISK_EXIT.value
            and event["reason_code"] == "HARD_STOP"
            for event in events
        )
        raise RuntimeError("activation-barrier-observed")

    app.activate_latest_a3_for_monitor = activation_barrier
    with pytest.raises(RuntimeError, match="activation-barrier-observed"):
        app.monitor_once(now=current)


def test_a4_06_morning_recovery_cannot_mutate_after_round_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    current = NOW.replace(hour=9, minute=33)
    store = RuntimeStore(tmp_path / "state.sqlite3")
    store.create_execution_plan(
        "pending",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=current.replace(hour=15),
        payload={
            "source_run_id": "published-a3",
            "stop_level": 9.0,
            "trigger_high": 11.0,
            "no_chase": 11.2,
        },
    )
    ticks = iter((0.0, 2.0))
    monkeypatch.setattr("liangjian_funnel.workflow.time.monotonic", lambda: next(ticks))
    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": object()},
        _fetch_live_quote=lambda *_args, **_kwargs: quote("600519.SH", price=10.0, at=current),
    )
    result = WorkflowApplication.activate_latest_a3_for_monitor(
        app,
        now=current,
        deadline=1.0,
    )
    assert result["reason_code"] == "A3_SCOPE_ACTIVATION_DEADLINE_EXCEEDED"
    assert store.get_execution_plan("pending")["status"] == PlanStatus.PENDING_MORNING_REVIEW.value


def test_a4_06_late_bounded_result_cannot_replace_terminal_timeout() -> None:
    gate = BoundedWorkGate(1)
    state = {"published": None}

    def late() -> str:
        time.sleep(0.05)
        state["published"] = "late-source-value"
        return "late-source-value"

    result = gate.call(late, deadline=time.monotonic() + 0.01)
    assert result.status == "TIMED_OUT" and result.value is None
    time.sleep(0.07)
    # The dependency may finish, but the frozen terminal result object does
    # not acquire or publish that late value into the old decision.
    assert state["published"] == "late-source-value"
    assert result.status == "TIMED_OUT" and result.value is None


def test_a4_05_model_completion_after_absolute_deadline_cannot_publish_buy(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    store.create_execution_plan(
        "deadline-plan",
        "lane_1",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        valid_from=NOW - timedelta(minutes=1),
        expires_at=NOW.replace(hour=15),
        payload={"trigger_low": 9.5, "trigger_high": 10.5, "confirmation_bars": 1},
    )
    monotonic = {"value": 0.0}

    def late_model(_payload):
        monotonic["value"] = 5.0
        return False

    result = MonitorEngine(
        store,
        llm_veto=late_model,
        max_seconds=10.0,
        deadline_monotonic=5.0,
        clock=lambda: monotonic["value"],
    ).process_minute(
        "lane_1",
        {"600519.SH": bar("600519.SH", close=10.0, low=9.8)},
        minute_snapshot_id="deadline-model",
        now=NOW,
    )
    assert result.model_called is True
    assert not any(event.action == MonitorAction.BUY_SIGNAL.value for event in result.events)
    assert result.events[-1].action == MonitorAction.DATA_BLOCK.value
    assert result.events[-1].reason_code == "MONITOR_OVERRUN"


def test_a4_12_archive_sqlite_writer_cannot_block_risk_intent_store(tmp_path: Path) -> None:
    minute_store = MinuteBarStore(tmp_path / "minute")
    state_store = RuntimeStore(tmp_path / "runtime.sqlite3")
    add_position(state_store, lane="lane_1", symbol="600519.SH", stop=9.0)

    archive_writer = sqlite3.connect(minute_store.path, timeout=0.1)
    archive_writer.execute("BEGIN IMMEDIATE")
    try:
        started = time.monotonic()
        result = MonitorEngine(state_store).process_position_risk(
            "lane_1",
            {"600519.SH": bar("600519.SH", close=8.9, low=8.8)},
            minute_snapshot_id="risk-while-archive-locked",
            now=NOW,
        )
        elapsed = time.monotonic() - started
    finally:
        archive_writer.rollback()
        archive_writer.close()

    assert elapsed < 0.5
    assert result.events[0].action == MonitorAction.FORCED_RISK_EXIT.value


def test_a4_10_absolute_deadline_wrapper_preserves_valid_deterministic_output(tmp_path: Path) -> None:
    def evaluate(path: Path, *, deadline: float | None) -> list[tuple[str, str, bool]]:
        store = RuntimeStore(path)
        store.create_execution_plan(
            "same-input",
            "lane_1",
            "600519.SH",
            status=PlanStatus.ACTIVE_TODAY,
            valid_from=NOW - timedelta(minutes=1),
            expires_at=NOW.replace(hour=15),
            payload={"trigger_low": 9.5, "trigger_high": 10.5, "confirmation_bars": 1},
        )
        batch = MonitorEngine(
            store,
            llm_veto=lambda _payload: False,
            deadline_monotonic=deadline,
            clock=lambda: 1.0,
        ).process_minute(
            "lane_1",
            {"600519.SH": bar("600519.SH", close=10.0, low=9.8)},
            minute_snapshot_id="same-frozen-input",
            now=NOW,
        )
        return [(event.action, event.reason_code, event.effective) for event in batch.events]

    legacy = evaluate(tmp_path / "legacy.sqlite3", deadline=None)
    bounded = evaluate(tmp_path / "bounded.sqlite3", deadline=2.0)
    assert legacy == bounded
    assert legacy[-1][0] == MonitorAction.BUY_SIGNAL.value


def test_a4_02_monitor_never_fetches_native_5m_or_archive_only_symbol(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    current = NOW.replace(minute=6)
    symbol = "000001.SZ"
    archive_symbol = "000002.SZ"
    current_bar = bar(symbol, at=current - timedelta(minutes=1))
    store = RuntimeStore(tmp_path / "state.sqlite3")
    store.create_execution_plan(
        "active",
        "lane_1",
        symbol,
        status=PlanStatus.ACTIVE_TODAY,
        valid_from=current - timedelta(minutes=10),
        expires_at=current.replace(hour=15),
        payload={},
    )
    store.list_observable_invalidated_plans = lambda *_args, **_kwargs: ({"symbol": archive_symbol},)
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(
        workflow_output_dir=tmp_path / "outputs",
        fact_store_dir=tmp_path / "facts",
        research_primary_lane_id="lane_1",
    )
    app.store = store
    app.minute_store = MinuteBarStore(tmp_path / "minute")
    app.brokers = {"lane_1": object()}
    app.market_data = object()
    app.lark_publisher = None
    app._ensure_trading_day = lambda _: None
    app._expire_missed_a4_entries = lambda _: None
    app.activate_latest_a3_for_monitor = lambda **_: {}
    app._settle_prior_signals = lambda *_args: []
    app._record_a4_outcomes = lambda *_args: {}
    app._a4_callback = lambda *_args, **_kwargs: None
    fetched: list[tuple[str, str]] = []

    def fetch_bars(request_symbol, interval, *_args, **_kwargs):
        fetched.append((request_symbol, interval))
        assert request_symbol != archive_symbol
        assert interval == "1m"
        return FetchResult(
            symbol=request_symbol,
            interval="1m",
            requested_bars=1,
            returned_bars=1,
            bars=(current_bar,),
            reason_code="OK",
            complete=True,
        )

    app._fetch_live_bars = fetch_bars
    app._fetch_live_quote = lambda request_symbol, *_args, **_kwargs: quote(request_symbol, price=10, at=current)
    monkeypatch.setattr("liangjian_funnel.workflow._a4_required_bars", lambda *_args: 1)
    monkeypatch.setattr("liangjian_funnel.workflow.load_or_refresh_live_market_state", lambda *_args, **_kwargs: {
        "status": "READY", "entry_permission": "ALLOW", "as_of": current.isoformat(),
    })
    monkeypatch.setattr(
        "liangjian_funnel.data.publication.confirm_publications",
        lambda market, *_args, **_kwargs: {
            key: {**value, "publication": {"state": "READY", "attempts": []}}
            for key, value in market.items()
        },
    )
    monkeypatch.setattr(
        "liangjian_funnel.data.publication.classify_persistent_pending",
        lambda pack, *_args, **_kwargs: pack,
    )

    class Engine:
        def __init__(self, *_args, **_kwargs):
            pass

        def process_minute(self, lane_id, _bars, **kwargs):
            return MonitorBatchResult(lane_id=lane_id, minute_snapshot_id=kwargs["minute_snapshot_id"])

    monkeypatch.setattr("liangjian_funnel.workflow.MonitorEngine", Engine)
    result = app.monitor_once(now=current)
    assert fetched and set(fetched) == {(symbol, "1m")}
    assert result["archive_only_symbols"] == [archive_symbol]
    assert result["deferred_auxiliary"]["archive_symbols"] == [archive_symbol]
    assert result["deferred_auxiliary"]["native_5m_symbols"] == [symbol]
