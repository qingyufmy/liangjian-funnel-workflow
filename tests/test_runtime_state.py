from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.contracts import RunStatus, StageStatus
from liangjian_funnel.runtime.state import (
    MonitorAction,
    PersistenceBlockedError,
    PlanStatus,
    RuntimeStore,
    StateTransitionError,
)


TZ = ZoneInfo("Asia/Shanghai")


def test_sqlite_is_wal_full_and_run_stage_transitions_are_atomic(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2

    first = store.create_research_run("r1", "2026-08-24", "CLOSE_1510", "model-a")
    assert first["status"] == RunStatus.CREATED.value
    assert store.create_research_run("r1", "2026-08-24", "CLOSE_1510", "model-a")["run_id"] == "r1"
    assert store.transition_research_run("r1", RunStatus.DATA_PREPARING)["status"] == RunStatus.DATA_PREPARING.value
    with pytest.raises(StateTransitionError, match="DATA_BOUND_HASHES_REQUIRED"):
        store.transition_research_run("r1", RunStatus.DATA_BOUND)
    bound = store.transition_research_run(
        "r1",
        RunStatus.DATA_BOUND,
        snapshot_hash="s" * 16,
        prompt_hash="p" * 16,
        config_hash="c" * 16,
    )
    assert bound["status"] == RunStatus.DATA_BOUND.value
    with pytest.raises(StateTransitionError, match="IMMUTABLE_RUN_HASH"):
        store.transition_research_run("r1", RunStatus.RUNNING, snapshot_hash="x" * 16)

    store.create_lane_stage("r1", "A1")
    store.create_lane_stage("r1", "A2")
    with pytest.raises(StateTransitionError, match="STAGE_PREREQUISITE"):
        store.transition_lane_stage("r1", "A2", StageStatus.RUNNING)
    store.transition_lane_stage("r1", "A1", StageStatus.RUNNING)
    store.transition_lane_stage("r1", "A1", StageStatus.VALIDATED)
    assert store.transition_lane_stage("r1", "A2", StageStatus.RUNNING)["status"] == StageStatus.RUNNING.value


def test_plan_event_and_account_keys_are_idempotent(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    plan = store.create_execution_plan(
        "p1",
        "lane-a",
        "600519.SH",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        payload={"trigger_low": 10, "trigger_high": 11},
    )
    assert store.activate_plan("p1")["status"] == PlanStatus.ACTIVE_TODAY.value
    assert len(store.list_active_plans("lane-a")) == 1
    assert [item["plan_id"] for item in store.list_execution_plans(lane_id="lane-a", status=PlanStatus.ACTIVE_TODAY)] == ["p1"]
    event, inserted = store.record_monitor_event(
        event_key="event-1",
        lane_id="lane-a",
        minute_end=datetime(2026, 8, 24, 9, 32, tzinfo=TZ),
        action=MonitorAction.NO_ACTION,
        reason_code="TRIGGER_NOT_MET",
    )
    assert inserted is True
    same, inserted = store.record_monitor_event(
        event_key="event-1",
        lane_id="lane-a",
        minute_end=datetime(2026, 8, 24, 9, 32, tzinfo=TZ),
        action=MonitorAction.NO_ACTION,
        reason_code="TRIGGER_NOT_MET",
    )
    assert inserted is False
    assert same["event_id"] == event["event_id"]
    account = store.ensure_virtual_account("paper:model-a", "model-a", 1_000_000)
    assert account["cash"] == 1_000_000
    assert store.ensure_virtual_account("paper:model-a", "model-a", 3)["cash"] == 1_000_000


def test_notification_delivery_is_idempotent_and_rotates_colors(tmp_path):
    store = RuntimeStore(tmp_path / "notifications.sqlite3")
    first, inserted = store.record_delivery(
        delivery_key="premarket:2026-08-28:run-a:1",
        kind="PREMARKET_A3",
        source_id="run-a",
        title="盘前计划",
        status="SENT",
        payload={"symbol": "600519.SH", "reason": "A3 trend"},
        created_at=datetime(2026, 8, 28, 9, 26, tzinfo=TZ),
    )
    assert inserted is True
    assert first["status"] == "SENT"
    assert first["sent_at"] == first["created_at"]
    same, inserted = store.record_delivery(
        delivery_key="premarket:2026-08-28:run-a:1",
        kind="PREMARKET_A3",
        source_id="run-a",
        title="should not replace",
        status="FAILED",
    )
    assert inserted is False
    assert same["delivery_id"] == first["delivery_id"]
    assert same["title"] == "盘前计划"
    second, inserted = store.record_delivery(
        delivery_key="a4:event-1",
        kind="A4_EFFECTIVE",
        source_id="event-1",
        title="A4有效信号",
        status="FAILED",
        last_reason_code="LARK_HTTP_RETRYABLE",
        attempt_count=2,
        payload={"action": "BUY_SIGNAL", "condition": "trend confirmed"},
        created_at=datetime(2026, 8, 28, 9, 27, tzinfo=TZ),
    )
    assert inserted is True
    assert second["color"] != first["color"]
    assert second["sent_at"] is None
    assert store.next_notification_color() not in {first["color"], second["color"]}
    assert len(store.list_notification_deliveries(kind="A4_EFFECTIVE")) == 1
    assert store.get_delivery_by_key("a4:event-1")["status"] == "FAILED"


def test_notification_payload_rejects_raw_fields(tmp_path):
    store = RuntimeStore(tmp_path / "notifications-safe.sqlite3")
    with pytest.raises(ValueError, match="unsafe"):
        store.record_delivery(
            delivery_key="a4:unsafe",
            kind="A4_EFFECTIVE",
            source_id="event-unsafe",
            title="A4",
            payload={"raw_response": "model output"},
            status="SENT",
        )


def test_persistence_failure_blocks_new_writes(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    store.mark_persistence_failed()
    with pytest.raises(PersistenceBlockedError, match="PERSISTENCE_FAILED"):
        store.ensure_virtual_account("paper:model-a", "model-a")


def test_plan_batch_conflict_rolls_back_every_insert(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    expires = datetime(2026, 8, 24, 15, 0, tzinfo=TZ)
    batch = [
        {
            "plan_id": "same-id",
            "lane_id": "lane-a",
            "symbol": "600519.SH",
            "status": PlanStatus.ACTIVE_TODAY.value,
            "expires_at": expires,
            "payload": {"trigger_low": 10},
        },
        {
            "plan_id": "same-id",
            "lane_id": "lane-b",
            "symbol": "000001.SZ",
            "status": PlanStatus.ACTIVE_TODAY.value,
            "expires_at": expires,
            "payload": {"trigger_low": 20},
        },
    ]
    with pytest.raises(StateTransitionError, match="PLAN_ID_CONTENT_CONFLICT"):
        store.publish_plan_batch(batch)
    assert store.list_execution_plans() == ()


def test_workflow_lane_state_and_real_trading_day_are_durable(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    store.ensure_virtual_account("paper:lane-a", "model-a")
    store.record_workflow_run(
        run_id="run-1",
        lane_id="lane-a",
        trade_date="2026-08-24",
        slot="close",
        model="model-a",
        status="READY_TO_PUBLISH",
        snapshot_hash="s" * 64,
    )
    store.record_workflow_stage(run_id="run-1", lane_id="lane-a", stage="A1", status="VALIDATED")
    assert store.mark_workflow_runs_published("run-1", ["lane-a"]) == 1
    assert store.list_workflow_runs()[0]["status"] == "PUBLISHED"
    assert store.start_account_trading_day("paper:lane-a", date(2026, 8, 24)) is True
    assert store.start_account_trading_day("paper:lane-a", date(2026, 8, 24)) is False
    with pytest.raises(StateTransitionError, match="TRADING_DAY_REGRESSION"):
        store.start_account_trading_day("paper:lane-a", date(2026, 8, 23))


def test_latest_a3_activation_is_atomic_idempotent_and_overrides_session_expiry(tmp_path):
    store = RuntimeStore(tmp_path / "a3-activation.sqlite3")
    expiry = datetime(2026, 9, 2, 15, 0, tzinfo=TZ)
    source = "close-2026-08-28-a3"
    payload = {
        "source_run_id": source,
        "trigger_high": 11,
        "stop_level": 9,
    }
    store.publish_plan_batch(
        [
            {
                "plan_id": "a3-valid",
                "lane_id": "lane-a",
                "symbol": "600519.SH",
                "status": PlanStatus.PENDING_MORNING_REVIEW.value,
                "expires_at": expiry,
                "payload": payload,
            },
            {
                "plan_id": "a3-invalid",
                "lane_id": "lane-a",
                "symbol": "000001.SZ",
                "status": PlanStatus.PENDING_MORNING_REVIEW.value,
                "expires_at": expiry,
                "payload": payload,
            },
        ]
    )
    as_of = datetime(2026, 9, 1, 14, 0, tzinfo=TZ)
    session_expiry = datetime(2026, 9, 1, 15, 0, tzinfo=TZ)
    first = store.activate_latest_a3_plan_batch(
        ["a3-valid"],
        invalidated_plan_ids=["a3-invalid"],
        valid_from=datetime(2026, 9, 1, 9, 32, tzinfo=TZ),
        as_of=as_of,
        session_expires_at=session_expiry,
        source_run_id=source,
    )
    assert {row["status"] for row in first} == {
        PlanStatus.ACTIVE_TODAY.value,
        PlanStatus.INVALIDATED.value,
    }
    assert store.get_execution_plan("a3-valid")["expires_at"] == session_expiry.isoformat()
    assert store.get_execution_plan("a3-invalid")["status"] == PlanStatus.INVALIDATED.value

    # A retry sees the same terminal states, performs no second transition,
    # and remains valid only for the same immutable A3 source.
    second = store.activate_latest_a3_plan_batch(
        ["a3-valid"],
        invalidated_plan_ids=["a3-invalid"],
        valid_from=datetime(2026, 9, 1, 9, 32, tzinfo=TZ),
        as_of=as_of,
        session_expires_at=session_expiry,
        source_run_id=source,
    )
    assert [row["status"] for row in second] == [
        PlanStatus.ACTIVE_TODAY.value,
        PlanStatus.INVALIDATED.value,
    ]
    with pytest.raises(StateTransitionError, match="A3_PLAN_SOURCE_MISMATCH"):
        store.activate_latest_a3_plan_batch(
            ["a3-valid"],
            valid_from=datetime(2026, 9, 1, 9, 32, tzinfo=TZ),
            as_of=as_of,
            source_run_id="different-source",
        )
