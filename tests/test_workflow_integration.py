from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
from liangjian_funnel.pipeline.research import FrozenInputSnapshot as ResearchSnapshot
from liangjian_funnel.pipeline.research import _approved_symbols, _runtime_input
from liangjian_funnel.workflow import _canonical_symbol, _compact_factor, _plan_expiry, _tightens


TZ = ZoneInfo("Asia/Shanghai")


def _bar(end: datetime) -> MinuteBar:
    return MinuteBar(
        symbol="600519.SH",
        interval="1m",
        bar_end=end,
        open=10,
        high=10.2,
        low=9.9,
        close=10.1,
        volume=1000,
        amount=10_000,
        source_id="mootdx:test",
        adjust_mode="none",
    )


def test_monitor_confirmation_survives_new_process_instance(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    start = datetime(2026, 8, 24, 10, 0, tzinfo=TZ)
    store.create_execution_plan(
        "plan-1",
        "lane_1",
        "600519.SH",
        status=PlanStatus.DRAFT_CLOSE,
        expires_at=start + timedelta(hours=1),
        payload={"trigger_low": 10, "trigger_high": 11, "stop_level": 9, "confirmation_bars": 2},
    )
    store.set_plan_pending_morning_review("plan-1")
    store.activate_plan("plan-1", valid_from=start)
    first_calls = []
    first = MonitorEngine(store, llm_veto=lambda value: first_calls.append(value) or {"vetoes": {"plan-1": False}})
    one = first.process_minute("lane_1", [_bar(start)], minute_snapshot_id="m1", now=start)
    assert any(event.action == "START_CONFIRMATION" for event in one.events)
    assert len(first_calls) == 1

    second_calls = []
    second = MonitorEngine(store, llm_veto=lambda value: second_calls.append(value) or {"vetoes": {"plan-1": False}})
    two = second.process_minute(
        "lane_1",
        [_bar(start + timedelta(minutes=1))],
        minute_snapshot_id="m2",
        now=start + timedelta(minutes=1),
    )
    assert any(event.action == "BUY_SIGNAL" for event in two.events)
    assert len(second_calls) == 1


def test_workflow_plan_helpers_are_fail_closed():
    assert _canonical_symbol("SHSE.600519") == "600519.SH"
    parent = {"payload_json": '{"trigger_low":10,"trigger_high":11,"risk_unit":"STANDARD"}'}
    assert _tightens(parent, {"trigger_low": 10.1, "trigger_high": 10.9, "risk_unit": "PROBE"})
    assert not _tightens(parent, {"trigger_low": 9.9, "trigger_high": 11.1, "risk_unit": "STANDARD"})
    expiry = _plan_expiry("not-a-time", datetime(2026, 8, 28, 15, 10, tzinfo=TZ), "close")
    assert expiry.weekday() == 0
    compact = _compact_factor({"symbol": "600519.SH", "timeframes": {"5m": {"bars": [1, 2], "latest": {"close": 10}, "moving_averages": {"ma5": 9}, "ready": True}}})
    assert "bars" not in compact["timeframes"]["5m"]


def test_research_runtime_injects_exact_model_and_only_approved_pool_flows_downstream():
    snapshot = ResearchSnapshot(
        snapshot_id="snap-1",
        snapshot_hash="a" * 64,
        data={"g0_symbols": ["600519.SH"], "MARKET_REGIME_SNAPSHOT": {"regime": "ROTATION_NO_MAINLINE"}},
    )
    runtime = _runtime_input(snapshot, "lane_1", "deepseek-v4-pro-0813", "A1", None, {"600519.SH"})
    assert runtime["required_envelope"]["model_name"] == "deepseek-v4-pro-0813"
    output = {
        "active_research_pool": [],
        "monitor_pool": [{"symbol": "SHSE.600519"}],
    }
    assert _approved_symbols(output, "A1") == set()
