import os
import stat
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.progress import WorkflowProgress
from liangjian_funnel.pipeline.outcomes import aggregate_lane_outcome, aggregate_run_outcome, stage_outcome_from_legacy


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
        daily_updates=21,
        financial_refreshes=4,
        deferred_financial_refreshes=96,
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
    assert state["data"]["daily_updates"] == 21
    assert state["data"]["financial_refreshes"] == 4
    assert state["data"]["deferred_financial_refreshes"] == 96
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


def test_progress_diagnostics_keep_only_bounded_structural_counts(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-diagnostics", job="close")
    progress.research_event({
        "lane": "lane_1",
        "model": "fixture-model",
        "stage": "MACRO_DISCOVERY",
        "status": "FAILED",
        "diagnostics": {
            "last_invalid_output_shape": {
                "type": "object",
                "fields": ["envelope", "secret_field", "structural_themes", "structural_themes"],
                "unknown_field_count": 99_999_999,
                "envelope_unknown_field_count": -4,
                "raw_model_content": "must-not-persist",
            },
            "semantic_attempts": 99_999_999,
            "theme_count": 8,
            "node_count": 40,
            "mapping_count": 14,
            "expected_mapping_count": 20,
            "missing_mapping_codes": ["884001.TI", "", 123, "secret-code"],
            "provider_response": "must-not-persist",
        },
    })

    state = progress.snapshot()
    diagnostics = state["lanes"]["LANE_1"]["stages"]["MACRO_DISCOVERY"]["diagnostics"]
    assert diagnostics == {
        "last_invalid_output_shape": {
            "type": "object",
            "fields": ["envelope", "structural_themes"],
            "unknown_field_count": 10_000_000,
            "envelope_unknown_field_count": 0,
        },
        "semantic_attempts": 10_000_000,
        "theme_count": 8,
        "node_count": 40,
        "mapping_count": 14,
        "expected_mapping_count": 20,
        "missing_mapping_count": 2,
    }
    serialized = (tmp_path / "workflow_progress.json").read_text(encoding="utf-8")
    assert "secret_field" not in serialized
    assert "raw_model_content" not in serialized
    assert "provider_response" not in serialized
    assert "884001.TI" not in serialized


def test_progress_diagnostics_reject_unknown_shape_types_and_arbitrary_keys(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-diagnostics-unknown", job="close")
    progress.research_event({
        "lane": "lane_1",
        "stage": "MACRO_DISCOVERY",
        "status": "FAILED",
        "diagnostics": {
            "last_invalid_output_shape": {
                "type": "ProviderPrivateType",
                "fields": ["not-a-contract-field"],
            },
            "arbitrary": {"model": "private"},
        },
    })

    state = progress.snapshot()
    stage = state["lanes"]["LANE_1"]["stages"]["MACRO_DISCOVERY"]
    assert "diagnostics" not in stage
    assert "private" not in (tmp_path / "workflow_progress.json").read_text(encoding="utf-8")


def test_finish_closes_lane_states_and_uses_canonical_job_status(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-outcome", job="close")
    progress.research_event({"lane_id": "lane_1", "stage": "A1", "status": "RUNNING", "lane_status": "RUNNING"})
    progress.research_event({"lane_id": "lane_2", "stage": "A1", "status": "FAILED", "lane_status": "FAILED"})
    lane = aggregate_lane_outcome((), lane_id="lane_1", legacy_status="READY_DEGRADED")
    outcome = aggregate_run_outcome((lane,), run_id="run-outcome", expected_lane_count=1)
    progress.finish(status="BLOCKED", phase="COMPLETED", outcome=outcome.as_dict())
    state = progress.snapshot()
    assert state["job_status"] == "SUCCEEDED"
    assert state["lanes"]["LANE_1"]["status"] == "READY_DEGRADED"
    assert state["lanes"]["LANE_1"]["job_status"] == "SUCCEEDED"
    assert state["lanes"]["LANE_1"]["current_stage"] is None
    assert state["lanes"]["LANE_2"]["status"] == "FAILED"
    assert state["lanes"]["LANE_2"]["job_status"] == "FAILED"
    assert state["lanes"]["LANE_2"]["current_stage"] is None
    assert state["outcome_v3"]["schema_version"] == "research-outcome/3.0.0"


def test_finish_closes_lane_without_rewriting_stage_batch_facts(tmp_path):
    progress = WorkflowProgress(tmp_path / "workflow_progress.json", run_id="run-partial", job="close")
    progress.research_event({
        "lane_id": "lane_1",
        "stage": "A1_LLM_REVIEW",
        "status": "COMPLETED",
        "completed_batches": 10,
        "total_batches": 248,
        "lane_status": "RUNNING",
    })
    progress.finish(status="READY_DEGRADED", phase="COMPLETED")
    state = progress.snapshot()
    lane = state["lanes"]["LANE_1"]
    assert lane["status"] == "READY_DEGRADED"
    assert lane["job_status"] == "SUCCEEDED"
    assert lane["current_stage"] is None
    assert lane["stages"]["A1_LLM_REVIEW"]["status"] == "COMPLETED"
    assert lane["stages"]["A1_LLM_REVIEW"]["completed_batches"] == 10
    assert lane["stages"]["A1_LLM_REVIEW"]["total_batches"] == 248
