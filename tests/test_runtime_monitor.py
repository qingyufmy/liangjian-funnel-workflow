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
    assert len(calls) == 0
    assert all(event.action == MonitorAction.NO_ACTION.value for event in no_trigger.events)
    first = engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m2", now=t0)
    assert first.events[0].action == MonitorAction.START_CONFIRMATION.value
    assert len(calls) == 0
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
    first = engine.process_minute("lane-a", {"600519.SH": bar(t0), "000001.SZ": bar(t0, close=30)}, minute_snapshot_id="m1", now=t0)
    assert first.model_called is False
    result = engine.process_minute("lane-a", {"600519.SH": bar(t0 + timedelta(minutes=1)), "000001.SZ": bar(t0 + timedelta(minutes=1), close=30)}, minute_snapshot_id="m1b", now=t0 + timedelta(minutes=1))
    assert result.model_called is True
    assert len(calls) == 1
    assert {item["plan_id"] for item in calls[0]["plans"]} == {"p-lane-a", "p-a2"}
    # The second lane has its own batch and cannot receive lane-a's veto.
    engine.process_minute("lane-b", {"600519.SH": bar(t0)}, minute_snapshot_id="m2", now=t0)
    assert len(calls) == 2
    assert calls[1]["lane_id"] == "lane-b"


def test_strategy_plan_uses_closed_15m_and_5m_not_legacy_1m_zone(tmp_path):
    store = RuntimeStore(tmp_path / "strategy.sqlite3")
    store.create_execution_plan(
        "p-strategy",
        "lane-a",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload={
            "strategy_profile": "TREND_MA5",
            "eligibility": "QUALIFIED",
            # Deliberately excludes the current price.  A strategy plan must
            # not fall back to the retired one-minute trigger-zone check.
            "trigger_low": 1.0,
            "trigger_high": 2.0,
            "entry_reference_zone": {"low": 10.0, "high": 12.0},
            "invalidation_level": 8.0,
            "stop_level": 8.0,
            "daily_indicators": {
                "ma5": 11.0,
                "ma10": 10.7,
                "ma20": 10.2,
                "ma60": 9.5,
                "close": 11.3,
            },
        },
    )
    store.activate_plan("p-strategy")
    start = datetime(2026, 8, 24, 9, 31, tzinfo=TZ)
    history = tuple(
        MinuteBar(
            symbol="600519",
            interval="1m",
            bar_end=start + timedelta(minutes=index),
            open=10.3 + index * 0.01,
            high=10.7 + index * 0.01,
            low=10.3 + index * 0.01,
            close=10.5 + index * 0.01,
            volume=1_000,
            amount=(10.5 + index * 0.01) * 1_000,
            source_id="MOOTDX:127.0.0.1:7709",
        )
        for index in range(30)
    )
    calls = []
    engine = MonitorEngine(store, llm_veto=lambda context: calls.append(context) or False)
    result = engine.process_minute(
        "lane-a",
        {"600519.SH": history[-1]},
        minute_snapshot_id="strategy-1000",
        now=history[-1].bar_end,
        bar_histories={"600519.SH": history},
        market_contexts={
            "600519.SH": {
                "live_market_state": {
                    "status": "READY",
                    "entry_permission": "ALLOW",
                    "as_of": history[-1].bar_end.isoformat(),
                    "trade_date": history[-1].bar_end.date().isoformat(),
                    "source": "TEST_FULL_MARKET",
                }
            }
        },
    )
    assert result.model_called is True
    assert result.events[-1].action == MonitorAction.BUY_SIGNAL.value
    assert calls[0]["plans"][0]["strategy_profile"] == "TREND_MA5"
    persisted = store.list_monitor_events(lane_id="lane-a", effective_only=True)[-1]
    payload = __import__("json").loads(persisted["payload_json"])
    assert payload["strategy"]["closed_15m_end"].endswith("10:00:00+08:00")


def test_restart_does_not_recall_model_during_same_trigger_episode(tmp_path):
    store = setup_store(tmp_path)
    calls = []
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    first = MonitorEngine(store, llm_veto=lambda context: calls.append(context) or True)
    first.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    vetoed = first.process_minute(
        "lane-a",
        {"600519.SH": bar(t0 + timedelta(minutes=1))},
        minute_snapshot_id="m2",
        now=t0 + timedelta(minutes=1),
    )
    assert vetoed.events[-1].action == MonitorAction.LLM_VETO.value
    assert len(calls) == 1

    restarted = MonitorEngine(store, llm_veto=lambda context: calls.append(context) or False)
    continuous = restarted.process_minute(
        "lane-a",
        {"600519.SH": bar(t0 + timedelta(minutes=2))},
        minute_snapshot_id="m3",
        now=t0 + timedelta(minutes=2),
    )
    assert continuous.model_called is False
    assert continuous.events[-1].reason_code == "SIGNAL_ALREADY_EMITTED"
    assert len(calls) == 1


def test_model_failure_persists_only_stable_diagnostic_code(tmp_path):
    store = setup_store(tmp_path)
    t0 = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)

    class TimedOut(RuntimeError):
        reason_code = "MODEL_TOTAL_DEADLINE_EXCEEDED"

    def fail(_context):
        raise TimedOut("must not be persisted")

    engine = MonitorEngine(store, llm_veto=fail)
    engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    result = engine.process_minute(
        "lane-a",
        {"600519.SH": bar(t0 + timedelta(minutes=1))},
        minute_snapshot_id="m2",
        now=t0 + timedelta(minutes=1),
    )
    assert result.blocked is True
    event = store.list_monitor_events(lane_id="lane-a", effective_only=True)[-1]
    assert event["reason_code"] == "LLM_UNAVAILABLE"
    payload = __import__("json").loads(event["payload_json"])
    assert payload["diagnostic_code"] == "MODEL_TOTAL_DEADLINE_EXCEEDED"
    assert "must not be persisted" not in event["payload_json"]


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


def test_symbol_data_block_does_not_stop_healthy_plan(tmp_path):
    store = setup_store(tmp_path)
    store.create_execution_plan(
        "p-bad-data",
        "lane-a",
        "000001.SZ",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload={"trigger_low": 10, "trigger_high": 11},
    )
    store.activate_plan("p-bad-data")
    now = datetime(2026, 8, 24, 9, 32, tzinfo=TZ)
    engine = MonitorEngine(store, llm_veto=lambda _context: False)
    result = engine.process_minute(
        "lane-a",
        {"600519.SH": bar(now)},
        minute_snapshot_id="mixed-data-1",
        now=now,
        data_ok=True,
        data_errors={"000001.SZ": "MINUTE_DATA_GAP"},
        snapshot_contiguous=True,
    )
    by_symbol = {event.symbol: event for event in result.events if event.symbol}
    assert by_symbol["000001.SZ"].action == MonitorAction.DATA_BLOCK.value
    assert by_symbol["000001.SZ"].reason_code == "MINUTE_DATA_GAP"
    assert by_symbol["600519.SH"].action == MonitorAction.START_CONFIRMATION.value
    assert result.blocked is True

    system_store = setup_store(tmp_path / "system")
    system_store.create_execution_plan(
        "p-bad-system",
        "lane-a",
        "000001.SZ",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload={"trigger_low": 10, "trigger_high": 11},
    )
    system_store.activate_plan("p-bad-system")
    system_failure = MonitorEngine(system_store, llm_veto=lambda _context: False).process_minute(
        "lane-a",
        {"600519.SH": bar(now)},
        minute_snapshot_id="mixed-data-system-failure",
        now=now,
        data_ok=False,
        data_errors={"000001.SZ": "MINUTE_DATA_GAP"},
    )
    system_by_symbol = {event.symbol: event for event in system_failure.events if event.symbol}
    assert system_by_symbol["600519.SH"].action == MonitorAction.DATA_BLOCK.value
    assert system_by_symbol["000001.SZ"].action == MonitorAction.DATA_BLOCK.value


def test_after_cutoff_buy_is_blocked_and_forced_exit_survives(tmp_path):
    store = setup_store(tmp_path)
    md = tmp_path / "effective.md"
    engine = MonitorEngine(store, llm_veto=lambda _context: False, effective_md_path=md)
    t0 = datetime(2026, 8, 24, 14, 46, tzinfo=TZ)
    result = engine.process_minute("lane-a", {"600519.SH": bar(t0)}, minute_snapshot_id="m1", now=t0)
    assert all(event.action != MonitorAction.BUY_SIGNAL.value for event in result.events)
