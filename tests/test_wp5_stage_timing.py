from datetime import datetime, timedelta
import json
import hashlib
import os
import pytest
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.progress import WorkflowProgress

NOW = datetime(2026, 10, 10, 0, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


def totals(progress, kind, phase, lane=None):
    return next(x for x in progress.snapshot()["timing"]["totals"]
                if (x["kind"], x["phase"], x["lane_id"]) == (kind, phase, lane))


def test_phase_reentry_retains_visits_not_just_latest_start(tmp_path):
    p = WorkflowProgress(tmp_path / "progress.json", run_id="r", job="close", now=NOW)
    p.set_phase("DATA_SYNC", now=NOW + timedelta(seconds=2))
    p.set_phase("CNINFO_SYNC", now=NOW + timedelta(seconds=12))
    p.set_phase("DATA_SYNC", now=NOW + timedelta(seconds=20))
    p.update_resources({}, now=NOW + timedelta(seconds=25))
    visits = [v for v in p.snapshot()["timing"]["visits"] if v["phase"] == "DATA_SYNC"]
    assert len(visits) == 2 and visits[0]["elapsed_seconds"] == 10 and visits[1]["elapsed_seconds"] == 5
    assert totals(p, "PHASE", "DATA_SYNC")["elapsed_seconds"] == 15


def test_parallel_stage_times_are_not_run_wall_total(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    for lane in ("lane_1", "lane_2"):
        p.research_event({"lane": lane, "stage": "A2", "status": "RUNNING"}, now=NOW)
    p.update_resources({}, now=NOW + timedelta(seconds=10))
    timing = p.snapshot()["timing"]
    assert timing["python_elapsed_seconds"] == 10
    assert sum(t["elapsed_seconds"] for t in timing["totals"] if t["kind"] == "RESEARCH_STAGE") == 20
    assert timing["stage_times_are_additive"] is False


def test_incomplete_batch_completion_does_not_end_stage(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "RUNNING"}, now=NOW)
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "COMPLETED",
                      "completed_batches": 1, "total_batches": 2}, now=NOW + timedelta(seconds=5))
    p.update_resources({}, now=NOW + timedelta(seconds=10))
    assert totals(p, "RESEARCH_STAGE", "A1", "LANE_1")["elapsed_seconds"] == 10
    assert p.snapshot()["timing"]["visits"][-1]["status"] == "RUNNING"


def test_failed_stage_retry_is_a_second_visit(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "RUNNING"}, now=NOW)
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "FAILED"}, now=NOW + timedelta(seconds=3))
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "RUNNING"}, now=NOW + timedelta(seconds=5))
    p.update_resources({}, now=NOW + timedelta(seconds=8))
    assert totals(p, "RESEARCH_STAGE", "A1", "LANE_1")["elapsed_seconds"] == 6
    assert totals(p, "RESEARCH_STAGE", "A1", "LANE_1")["visits_count"] == 2


def test_same_run_resume_keeps_history_and_does_not_charge_unobserved_gap(tmp_path):
    path = tmp_path / "p.json"
    p = WorkflowProgress(path, run_id="r", job="close", now=NOW)
    p.set_phase("DATA_SYNC", now=NOW)
    p.update_resources({}, now=NOW + timedelta(seconds=7))
    previous = json.loads(path.read_text())["timing"]["visits"][-1]
    assert previous["status"] == "RUNNING" and previous["ended_at"] is None
    q = WorkflowProgress(path, run_id="r", job="close", now=NOW + timedelta(seconds=100))
    assert totals(q, "PHASE", "DATA_SYNC")["elapsed_seconds"] == 7
    recovered = [v for v in q.snapshot()["timing"]["visits"] if v["phase"] == "DATA_SYNC"][0]
    assert recovered["status"] == "INTERRUPTED"
    assert recovered["ended_at"] == (NOW + timedelta(seconds=7)).isoformat()
    assert q.snapshot()["timing"]["python_elapsed_seconds"] == 7
    assert q.snapshot()["timing"]["run_wall_elapsed_seconds"] == 100
    assert totals(q, "PHASE", "DATA_SYNC")["current_invocation_elapsed_seconds"] == 0
    q.set_phase("DATA_SYNC", now=NOW + timedelta(seconds=100))
    q.update_resources({}, now=NOW + timedelta(seconds=103))
    assert totals(q, "PHASE", "DATA_SYNC")["elapsed_seconds"] == 10
    assert totals(q, "PHASE", "DATA_SYNC")["current_invocation_elapsed_seconds"] == 3
    fresh = WorkflowProgress(path, run_id="other", job="close", now=NOW + timedelta(seconds=101))
    assert len(fresh.snapshot()["timing"]["visits"]) == 1
    suffix = hashlib.sha256(b"r").hexdigest()[:16]
    assert json.loads((tmp_path / "run_timing" / f"r-{suffix}.json").read_text())["timing"]["totals"]


def test_finish_failure_does_not_mark_running_stage_completed(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    p.research_event({"lane": "lane_1", "stage": "A2", "status": "RUNNING"}, now=NOW)
    p.finish(status="FAILED", phase="FAILED", now=NOW + timedelta(seconds=11))
    assert p.snapshot()["timing"]["visits"][-1]["status"] == "INTERRUPTED"
    assert totals(p, "RESEARCH_STAGE", "A2", "LANE_1")["elapsed_seconds"] == 11


def test_real_parent_budget_uses_parent_elapsed_not_python_elapsed(tmp_path, monkeypatch):
    parent = NOW - timedelta(seconds=20)
    monkeypatch.setenv("LIANGJIAN_PARENT_JOB_BUDGET_MS", "100000")
    monkeypatch.setenv("LIANGJIAN_PARENT_JOB_STARTED_MS", str(int(parent.timestamp() * 1000)))
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    p.update_resources({}, now=NOW + timedelta(seconds=10))
    t = p.snapshot()["timing"]
    assert t["python_elapsed_seconds"] == 10 and t["parent_elapsed_seconds"] == 30
    assert t["budget_seconds"] == 100 and t["budget_used_ratio"] == .3
    assert t["budget_source"] == "NODE_TIMEOUT_FOR_JOB"


def test_invalid_or_missing_budget_is_unknown_not_default_5400(tmp_path, monkeypatch):
    monkeypatch.setenv("LIANGJIAN_PARENT_JOB_BUDGET_MS", "nan")
    monkeypatch.setenv("LIANGJIAN_PARENT_JOB_STARTED_MS", "Infinity")
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    assert p.snapshot()["timing"]["budget_seconds"] is None
    assert p.snapshot()["timing"]["budget_used_ratio"] is None


def test_backward_clock_and_late_callbacks_cannot_reopen_finished_timing(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    p.set_phase("DATA_SYNC", now=NOW)
    p.update_resources({}, now=NOW + timedelta(seconds=10))
    p.update_resources({}, now=NOW + timedelta(seconds=5))
    assert totals(p, "PHASE", "DATA_SYNC")["elapsed_seconds"] == 10
    p.finish(status="FAILED", phase="FAILED", now=NOW + timedelta(seconds=11))
    timing = p.snapshot()["timing"]
    p.research_event({"lane": "lane_1", "stage": "A1", "status": "RUNNING"}, now=NOW + timedelta(seconds=20))
    p.set_phase("DATA_SYNC", now=NOW + timedelta(seconds=21))
    p.update_resources({}, now=NOW + timedelta(seconds=22))
    assert p.snapshot()["timing"] == timing and p.snapshot()["status"] == "FAILED"
    assert p.snapshot()["elapsed_seconds"] == 11


def test_history_bound_preserves_cumulative_duration_and_recovery(tmp_path):
    path = tmp_path / "p.json"
    p = WorkflowProgress(path, run_id="r", job="close", now=NOW)
    for i in range(1, 401):
        p.set_phase("DATA_SYNC" if i % 2 else "CNINFO_SYNC", now=NOW + timedelta(seconds=i))
    before = sum(t["elapsed_seconds"] for t in p.snapshot()["timing"]["totals"])
    assert before == 400
    assert len(p.snapshot()["timing"]["visits"]) <= 384
    assert p.snapshot()["timing"]["visits_dropped_count"] > 0
    q = WorkflowProgress(path, run_id="r", job="close", now=NOW + timedelta(seconds=500))
    assert sum(t["elapsed_seconds"] for t in q.snapshot()["timing"]["totals"]) == before


def test_unknown_timing_names_cannot_copy_arbitrary_model_text(tmp_path):
    p = WorkflowProgress(tmp_path / "p.json", run_id="r", job="close", now=NOW)
    # Existing legacy state has its own contract; the NEW timing projection
    # uses fixed stage/lane tokens rather than copying these arbitrary strings.
    p.research_event({"lane": "SECRET_PROVIDER_ID", "stage": "PRIVATE_MODEL_REASONING",
                      "status": "RUNNING", "api_key": "sk-private-key"}, now=NOW)
    serialized = json.dumps(p.snapshot()["timing"])
    assert "SECRET_PROVIDER_ID" not in serialized and "PRIVATE_MODEL_REASONING" not in serialized
    assert "sk-private-key" not in serialized


@pytest.mark.parametrize("run_ids", [("a:b", "a/b"), ("r" * 190 + "a", "r" * 190 + "b")])
def test_receipt_filename_collision_cannot_overwrite_another_run(tmp_path, run_ids):
    # Exercise the old 180-character prefix on Windows too, so this red test
    # proves overwrite rather than stopping at the platform path-length limit.
    if os.name == "nt":
        tmp_path = type(tmp_path)("\\\\?\\" + str(tmp_path))
    for index, run_id in enumerate(run_ids):
        p = WorkflowProgress(tmp_path / "p.json", run_id=run_id, job="close", now=NOW + timedelta(seconds=index * 20))
        p.update_resources({}, now=NOW + timedelta(seconds=index * 20 + 7))
    paths = list((tmp_path / "run_timing").glob("*.json"))
    assert len(paths) == 2
    receipts = [json.loads(path.read_text()) for path in paths]
    assert {receipt["run_id"] for receipt in receipts} == set(run_ids)
    assert all(receipt["timing"]["python_elapsed_seconds"] == 7 for receipt in receipts)
    for path in paths:
        receipt = json.loads(path.read_text())
        expected_hash = hashlib.sha256(receipt["run_id"].encode()).hexdigest()
        assert path.name.endswith(f"-{expected_hash[:16]}.json")
        assert receipt["run_id_sha256"] == expected_hash
