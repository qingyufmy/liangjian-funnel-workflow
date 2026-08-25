from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from liangjian_funnel.contracts import ResearchRun, RunSlot, RunStatus, SourcedValue, StageRun, StageStatus


def test_sourced_value_requires_timezone_and_fetch_before_ingest():
    now = datetime(2026, 8, 24, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
    value = SourcedValue(
        value=1,
        source_id="TEST",
        event_time=now,
        publish_time=now,
        fetch_time=now,
        ingest_time=now,
    )
    assert value.source_id == "TEST"
    with pytest.raises(ValidationError, match="timezone-aware"):
        SourcedValue(
            value=1,
            source_id="TEST",
            event_time=now.replace(tzinfo=None),
            publish_time=now,
            fetch_time=now,
            ingest_time=now,
        )


def test_run_hashes_are_immutable_after_data_bound():
    run = ResearchRun(run_id="r1", trade_date="2026-08-24", slot=RunSlot.MORNING_0925, model="m")
    run = run.prepare_data().bind_data(snapshot_hash="s" * 16, prompt_hash="p" * 16, config_hash="c" * 16)
    assert run.status is RunStatus.DATA_BOUND
    with pytest.raises(ValueError, match="snapshot_hash is immutable"):
        run.transition(RunStatus.RUNNING, snapshot_hash="x" * 16)
    running = run.transition(RunStatus.RUNNING)
    assert running.snapshot_hash == "s" * 16


def test_run_cannot_rebind_after_data_bound():
    run = ResearchRun(run_id="r1", trade_date="2026-08-24", slot=RunSlot.CLOSE_1510, model="m").prepare_data()
    run = run.bind_data(snapshot_hash="s" * 16, prompt_hash="p" * 16, config_hash="c" * 16)
    with pytest.raises(ValueError, match="DATA_PREPARING"):
        run.bind_data(snapshot_hash="x" * 16, prompt_hash="p" * 16, config_hash="c" * 16)


def test_illegal_run_and_stage_transitions_are_rejected():
    run = ResearchRun(run_id="r1", trade_date="2026-08-24", slot=RunSlot.CLOSE_1510, model="m")
    with pytest.raises(ValueError, match="illegal run transition"):
        run.transition(RunStatus.PUBLISHED)
    stage = StageRun(stage="A1")
    with pytest.raises(ValueError, match="illegal stage transition"):
        stage.transition(StageStatus.VALIDATED)
