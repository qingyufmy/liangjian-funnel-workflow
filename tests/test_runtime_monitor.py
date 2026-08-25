from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine, rebuild_effective_markdown
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore


TZ = ZoneInfo("Asia/Shanghai")


def bar(at: datetime, close: float = 10.5) -> MinuteBar:
    return MinuteBar(
        symbol="600519",
        interval="1m",
        bar_end=at,
        open=10,
        high=max(11, close),
        low=9,
        close=close,
        volume=1_000,
        amount=10_000,
        source_id="MOOTDX:127.0.0.1:7709",
    )


def setup_store(tmp_path: Path, lane: str = "lane-a") -> RuntimeStore:
    store = RuntimeStore(tmp_path / f"{lane}.sqlite3")
    store.create_execution_plan(
        f"p-{lane}",
        lane,
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload={"trigger_low": 10, "trigger_high": 11, "confirmation_bars": 2},
    )
    store.activate_plan(f"p-{lane}")
    return store


def test_llm_cannot_promote_non_trigger_and_can_only_veto(tmp_path):
    store = setup_store(tmp_path)
    calls = []

    def veto(context):
        calls.append(context)
        return {"action": "BUY_SIGNAL", "llm_veto": True, "thinking": "must not persist"}

    engine = MonitorEngine(store, llm_veto=veto, effective_md_path=tmp_path / "effective.md")
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    # Outside deterministic zone: no LLM call, even if it would request BUY.
    no_trigger = engine.process_minute("lane-a", {"600519.SH": bar(t0, close=12)}, minute_snapshot_id="m1", now=t0)
    assert len(calls) == 1
    assert calls[0]["plans"][0]["trigger_pass"] is False
    assert all(event.action == MonitorAction.NO_ACTION.value for event in no_trigger.events)
    first = engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m2", now=t0)
    assert first.events[0].action == MonitorAction.START_CONFIRMATION.value
    second = engine.process_minute("lane-a", {"600519.SH": bar(t0 + timedelta(minutes=1))}, minute_snapshot_id="m3", now=t0 + timedelta(minutes=1))
    assert any(call["plans"][0]["trigger_pass"] is True for call in calls)
    assert second.events[-1].action == MonitorAction.LLM_VETO.value


def test_lane_isolation_and_effective_markdown_dedup(tmp_path):
    store = setup_store(tmp_path, "lane-a")
    store.create_execution_plan("p-lane-b", "lane-b", "000001.SZ", status=PlanStatus.PENDING_MORNING_REVIEW, payload={"trigger_low": 10, "trigger_high": 11})
    store.activate_plan("p-lane-b")
    md = tmp_path / "effective.md"
    engine = MonitorEngine(store, llm_veto=lambda _context: False, effective_md_path=md)
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    result = engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    assert all(event.symbol in {None, "600519.SH"} for event in result.events)
    # Confirmation default is 2 for lane-a setup, so no effective markdown yet.
    engine.process_minute("lane-a", {"600519.SH": bar(t0 + timedelta(minutes=1))}, minute_snapshot_id="m2", now=t0 + timedelta(minutes=1))
    before = md.read_text(encoding="utf-8")
    engine.process_minute("lane-a", {"600519.SH": bar(t0 + timedelta(minutes=2))}, minute_snapshot_id="m3", now=t0 + timedelta(minutes=2))
    after = md.read_text(encoding="utf-8")
    assert after.count("BUY_SIGNAL") == 1
    assert len(store.list_monitor_events(lane_id="lane-b")) == 0
    assert "thinking" not in after


def test_effective_markdown_is_rebuilt_from_sqlite_after_corruption(tmp_path):
    store = setup_store(tmp_path)
    minute = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    store.record_monitor_event(
        event_key="effective:test",
        lane_id="lane-a",
        minute_end=minute,
        action=MonitorAction.DATA_BLOCK,
        reason_code="DATA_TEST",
        effective=True,
        payload={"symbol": "600519.SH"},
    )
    path = tmp_path / "effective.md"
    path.write_text("partial-corruption", encoding="utf-8")
    rebuild_effective_markdown(store, path)
    content = path.read_text(encoding="utf-8")
    assert "partial-corruption" not in content
    assert content.count("DATA_BLOCK") == 1


def test_one_flash_batch_per_nonempty_lane_contains_all_trigger_results(tmp_path):
    store = setup_store(tmp_path, "lane-a")
    store.create_execution_plan("p-a2", "lane-a", "000001.SZ", status=PlanStatus.PENDING_MORNING_REVIEW, payload={"trigger_low": 20, "trigger_high": 21})
    store.activate_plan("p-a2")
    store.create_execution_plan("p-b", "lane-b", "600519.SH", status=PlanStatus.PENDING_MORNING_REVIEW, payload={"trigger_low": 10, "trigger_high": 11})
    store.activate_plan("p-b")
    calls = []
    engine = MonitorEngine(store, llm_veto=lambda context: calls.append(context) or {"vetoes": {"p-lane-a": True}})
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    result = engine.process_minute("lane-a", {"600519.SH": bar(t0), "000001.SZ": bar(t0, close=30)}, minute_snapshot_id="m1", now=t0)
    assert result.model_called is True
    assert len(calls) == 1
    assert {item["plan_id"] for item in calls[0]["plans"]} == {"p-lane-a", "p-a2"}
    # The second lane has its own batch and cannot receive lane-a's veto.
    engine.process_minute("lane-b", {"600519.SH": bar(t0)}, minute_snapshot_id="m2", now=t0)
    assert len(calls) == 2
    assert calls[1]["lane_id"] == "lane-b"


def test_gap_resets_confirmation_and_no_action_is_not_markdown(tmp_path):
    store = setup_store(tmp_path)
    md = tmp_path / "effective.md"
    engine = MonitorEngine(store, llm_veto=lambda _context: False, effective_md_path=md)
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    blocked = engine.process_minute("lane-a", {"600519.SH": bar(t0 + timedelta(minutes=1))}, minute_snapshot_id="m2", now=t0 + timedelta(minutes=1), gap_detected=True)
    assert blocked.blocked is True
    assert all(event.action == MonitorAction.DATA_BLOCK.value for event in blocked.events)
    assert "NO_ACTION" not in md.read_text(encoding="utf-8")


def test_after_cutoff_buy_is_blocked_and_forced_exit_survives(tmp_path):
    store = setup_store(tmp_path)
    md = tmp_path / "effective.md"
    engine = MonitorEngine(store, llm_veto=lambda _context: False, effective_md_path=md)
    t0 = datetime(2026, 8, 24, 14, 46, tzinfo=TZ)
    result = engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    assert all(event.action != MonitorAction.BUY_SIGNAL.value for event in result.events)
