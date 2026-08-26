from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.progress import WorkflowProgress


TZ = ZoneInfo("Asia/Shanghai")


def test_progress_is_atomic_bounded_and_does_not_retain_sensitive_payload(tmp_path):
    started = datetime(2026, 8, 26, 15, 10, tzinfo=TZ)
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-1", job="close", now=started)
    progress.set_phase("data_sync", eta_seconds=120, now=started + timedelta(seconds=5))
    progress.update_data(
        processed=25,
        total=5000,
        cache_hits=20,
        cache_misses=5,
        failures=1,
        current_symbol="600519.SH",
        eta_seconds=100,
        now=started + timedelta(seconds=20),
    )
    progress.research_event(
        {
            "lane_id": "lane_1",
            "model": "deepseek-v4-pro-0813",
            "stage": "A1",
            "status": "RUNNING",
            "completed_batches": 2,
            "total_batches": 50,
            "attempts": 3,
            "reasoning": "must never persist",
            "api_key": "sk-must-never-persist",
        },
        now=started + timedelta(seconds=30),
    )

    text = (tmp_path / "workflow_progress.json").read_text(encoding="utf-8")
    state = progress.snapshot()
    assert state["phase"] == "RESEARCH_A1"
    assert state["data"]["processed"] == 25
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["completed_batches"] == 2
    assert state["elapsed_seconds"] == 30
    assert "must never persist" not in text
    assert "sk-must-never-persist" not in text


def test_progress_finish_records_only_stable_reason_code(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-2", job="close")
    progress.finish(status="BLOCKED", phase="FAILED", reason_code="LOCAL_FACT_CACHE_NOT_READY")
    state = progress.snapshot()
    assert state["status"] == "BLOCKED"
    assert state["phase"] == "FAILED"
    assert state["reason_code"] == "LOCAL_FACT_CACHE_NOT_READY"
    assert state["eta_seconds"] == 0
