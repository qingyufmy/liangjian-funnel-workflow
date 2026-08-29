"""TEST_ONLY acceptance for the non-empty plan lifecycle.

No external provider or trading account is touched.  The fixture proves the
same persisted state transitions used by morning review, A4 monitoring and
the paper broker.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationConfig
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication


TZ = ZoneInfo("Asia/Shanghai")


def _bar(at: datetime, close: float = 10.5) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval="1m",
        bar_end=at,
        open=10,
        high=10.8,
        low=9.9,
        close=close,
        volume=1_000,
        amount=10_500,
        source_id="TEST_ONLY:FIXTURE",
    )


def test_nonempty_plan_morning_a4_entry_and_next_day_exit(tmp_path) -> None:
    store = RuntimeStore(tmp_path / "acceptance.sqlite3")
    broker = PaperBroker(
        store,
        account_id="paper:lane_1",
        model="TEST_ONLY",
        config=SimulationConfig(initial_cash=100_000),
    )
    morning = datetime(2026, 8, 24, 9, 26, tzinfo=TZ)
    store.create_execution_plan(
        "test-only-plan",
        "lane_1",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=morning.replace(hour=15, minute=0),
        payload={
            "trigger_low": 10,
            "trigger_high": 11,
            "stop_level": 9,
            "confirmation_bars": 2,
        },
    )

    class Market:
        def fetch_bars(self, symbol, interval, required_bars, *, as_of):
            bars = (_bar(as_of - timedelta(minutes=1)), _bar(as_of))
            return FetchResult(
                symbol=symbol,
                interval=interval,
                requested_bars=required_bars,
                returned_bars=len(bars),
                bars=bars,
                reason_code="OK",
                complete=True,
            )

    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": broker},
        mootdx=Market(),
        minute_store=SimpleNamespace(write=lambda _bars: None),
        settings=SimpleNamespace(workflow_output_dir=tmp_path / "outputs"),
        _ensure_trading_day=lambda _current: None,
    )
    review = WorkflowApplication.review_pending_morning(app, now=morning)
    assert review["activated"] == ["test-only-plan"]
    assert store.get_execution_plan("test-only-plan")["status"] == PlanStatus.ACTIVE_TODAY.value

    engine = MonitorEngine(store, llm_veto=lambda _context: False)
    first_minute = morning.replace(hour=9, minute=32)
    engine.process_minute("lane_1", {"600519.SH": _bar(first_minute)}, minute_snapshot_id="fixture-1", now=first_minute)
    second = engine.process_minute(
        "lane_1",
        {"600519.SH": _bar(first_minute + timedelta(minutes=1))},
        minute_snapshot_id="fixture-2",
        now=first_minute + timedelta(minutes=1),
    )
    assert any(event.action == MonitorAction.BUY_SIGNAL.value and event.effective for event in second.events)

    entry = WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(first_minute + timedelta(minutes=2)),
    )
    assert any(item["action"] == "BUY" and item["status"] == "FILLED" for item in entry)
    assert store.get_position("paper:lane_1", "600519.SH") is not None

    next_day = first_minute + timedelta(days=1)
    broker.start_trading_day(next_day.date())
    store.record_monitor_event(
        event_key="test-only-exit",
        lane_id="lane_1",
        minute_end=next_day,
        action=MonitorAction.SELL_SIGNAL,
        effective=True,
        payload={"plan_id": "test-only-plan", "symbol": "600519.SH"},
    )
    exit_results = WorkflowApplication._settle_prior_signals(
        app,
        "lane_1",
        "600519.SH",
        _bar(next_day + timedelta(minutes=1), close=10.7),
    )
    assert any(item["action"] == "SELL" and item["status"] == "FILLED" for item in exit_results)
    assert store.get_position("paper:lane_1", "600519.SH") is None
