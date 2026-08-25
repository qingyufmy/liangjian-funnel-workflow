from __future__ import annotations

import sqlite3
from datetime import datetime
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


def test_persistence_failure_blocks_new_writes(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    store.mark_persistence_failed()
    with pytest.raises(PersistenceBlockedError, match="PERSISTENCE_FAILED"):
        store.ensure_virtual_account("paper:model-a", "model-a")
