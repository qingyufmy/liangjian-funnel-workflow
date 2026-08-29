import os
import stat
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
            "processed_symbols": 3886,
            "total_symbols": 3886,
            "selected_symbols": 36,
            "monitor_symbols": 3800,
            "rejected_symbols": 50,
            "industry_count": 40,
            "monthly_decision_count": 20,
            "theme_count": 8,
            "node_count": 40,
            "mapping_count": 14,
            "reasoning": "must never persist",
            "api_key": "sk-must-never-persist",
        },
        now=started + timedelta(seconds=30),
    )
    progress.update_resources(
        {
            "rss_current_mb": 321.25,
            "rss_peak_mb": 456.75,
            "system_mem_available_mb": 1024,
            "swap_used_mb": 88,
            "disk_free_mb": 9000,
            "disk_free_ratio": 0.25,
            "open_file_descriptors": 17,
            "command": "must never persist",
        },
        now=started + timedelta(seconds=31),
    )

    text = (tmp_path / "workflow_progress.json").read_text(encoding="utf-8")
    state = progress.snapshot()
    assert state["phase"] == "RESEARCH_A1"
    assert state["data"]["processed"] == 25
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["completed_batches"] == 2
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["processed_symbols"] == 3886
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["selected_symbols"] == 36
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["monthly_decision_count"] == 20
    assert state["lanes"]["LANE_1"]["stages"]["A1"]["mapping_count"] == 14
    assert state["elapsed_seconds"] == 31
    assert state["resources"]["rss_current_mb"] == 321.25
    assert state["resources"]["open_file_descriptors"] == 17.0
    assert "must never persist" not in text
    assert "sk-must-never-persist" not in text
    if os.name != "nt":
        metadata = (tmp_path / "workflow_progress.json").stat()
        assert stat.S_IMODE(metadata.st_mode) == 0o640
        assert metadata.st_gid == tmp_path.stat().st_gid


def test_progress_finish_records_only_stable_reason_code(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-2", job="close")
    progress.finish(status="BLOCKED", phase="FAILED", reason_code="LOCAL_FACT_CACHE_NOT_READY")
    state = progress.snapshot()
    assert state["status"] == "BLOCKED"
    assert state["phase"] == "FAILED"
    assert state["reason_code"] == "LOCAL_FACT_CACHE_NOT_READY"
    assert state["eta_seconds"] == 0
