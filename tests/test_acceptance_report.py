from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.generate_acceptance_report import (
    ENGINEERING_FAIL,
    ENGINEERING_PASS,
    PENDING_BUSINESS_ACCEPTANCE,
    build_acceptance_report,
    write_acceptance_report,
)


OUTCOME_VERSION = "research-outcome/3.0.0"


def _stage(name: str, selected: int, *, data_gap: bool = False) -> dict[str, object]:
    reasons = ["A2_DATA_GAP"] if data_gap else []
    quality = "DEGRADED" if data_gap else "VALIDATED"
    opportunity = "UNKNOWN" if data_gap else ("PRESENT" if selected else "ABSENT")
    actionability = "UNKNOWN" if data_gap else ("ACTIONABLE" if name == "A3" and selected else ("NO_ACTION" if not selected else "NOT_APPLICABLE"))
    return {
        "stage": name,
        "status": "VALIDATED",
        "symbols": [f"60000{i}.SH" for i in range(selected)],
        "input_count": 100 if name == "A1" else max(selected, 1),
        "outcome_v3": {
            "schema_version": OUTCOME_VERSION,
            "stage": name,
            "lifecycle_state": "TERMINAL",
            "quality_state": quality,
            "data_sufficiency_state": "INSUFFICIENT" if data_gap else "SUFFICIENT",
            "opportunity_state": opportunity,
            "actionability_state": actionability,
            "publication_state": "PUBLISHED",
            "reason_codes": reasons,
            "counts": {"input": 100, "evaluated": 100, "selected": selected},
            "data_coverage": {"required": 100, "actual": 100},
            "legacy_status": "VALIDATED",
        },
        "output": {
            "active_research_pool": [{"symbol": "600000.SH"}] if name == "A1" and selected else [],
            "focus_pool": [{"symbol": "600000.SH"}] if name == "A2" and selected else [],
            "core_watch_pool": [{"symbol": "600000.SH"}] if name == "A3" and selected else [],
        },
    }


def _write_feature_db(root: Path, run_id: str) -> None:
    path = root / "storage" / "features" / "research_feature_store.sqlite3"
    path.parent.mkdir(parents=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE feature_generations (
            generation_id TEXT PRIMARY KEY, domain TEXT, status TEXT, purpose TEXT,
            as_of TEXT, created_at TEXT, activation_eligible INTEGER
        );
        CREATE TABLE active_feature_generations (
            domain TEXT PRIMARY KEY, generation_id TEXT, activated_at TEXT,
            previous_generation_id TEXT
        );
        CREATE TABLE run_feature_bindings (
            run_id TEXT, domain TEXT, generation_id TEXT, contract_hash TEXT, bound_at TEXT
        );
        INSERT INTO feature_generations VALUES
          ('live-1','RESEARCH','SEALED','LIVE_FULL','2026-08-29T08:00:00+00:00','2026-08-29T08:00:00+00:00',1),
          ('run-1','RESEARCH','SEALED','RUN_SNAPSHOT','2026-08-29T09:00:00+00:00','2026-08-29T09:00:00+00:00',0);
        INSERT INTO active_feature_generations VALUES ('RESEARCH','live-1','2026-08-29T08:30:00+00:00',NULL);
        """,
    )
    connection.execute(
        "INSERT INTO run_feature_bindings VALUES (?, 'RESEARCH', 'run-1', 'contract', '2026-08-29T09:01:00+00:00')",
        (run_id,),
    )
    connection.commit()
    connection.close()


def _write_fixture(
    root: Path,
    *,
    replay_days: int = 10,
    gold_months: int = 4,
    plan: bool = True,
    run_status: str = "READY",
    a2_gap: bool = False,
) -> None:
    run_id = "2026-08-29-close-acceptance"
    run_dir = root / "outputs" / "runs"
    audit_dir = root / "outputs" / "research"
    run_dir.mkdir(parents=True)
    audit_dir.mkdir(parents=True)
    lane = {
        "lane": "lane_1",
        "model": "deepseek-v4-pro-0813",
        "status": run_status,
        "stages": [_stage("A1", 2), _stage("A2", 0 if a2_gap else 1, data_gap=a2_gap), _stage("A3", 0 if a2_gap else 1)],
    }
    comparison_lane = {
        "lane": "lane_2",
        "model": "moonshotai/kimi-k3-free",
        "status": "READY",
        "stages": [_stage("A1", 1), _stage("A2", 1), _stage("A3", 1)],
    }
    outcome = {
        "schema_version": OUTCOME_VERSION,
        "job_status": "SUCCEEDED" if run_status == "READY" else "FAILED",
        "lifecycle_state": "TERMINAL",
        "quality_state": "VALIDATED" if run_status == "READY" else "FAILED",
        "data_sufficiency_state": "INSUFFICIENT" if a2_gap else "SUFFICIENT",
        "research_opportunity_state": "PRESENT" if not a2_gap else "UNKNOWN",
        "focus_opportunity_state": "PRESENT" if not a2_gap else "UNKNOWN",
        "actionability_state": "ACTIONABLE" if not a2_gap else "UNKNOWN",
        "publication_state": "PUBLISHED" if run_status == "READY" else "BLOCKED",
        "reason_codes": [],
        "counts": {"expected_lanes": 1, "recorded_lanes": 1, "ready_lanes": 1, "required_lanes": 1, "ready_required_lanes": 1},
        "data_coverage": {},
        "legacy_status": run_status,
        "primary_lane_ids": ["lane_1"],
        "comparison_status": "PENDING",
        "lanes": [lane],
    }
    summary = {
        "run_id": run_id,
        "slot": "close",
        "status": run_status,
        "generated_at": "2026-08-29T09:30:00+08:00",
        "outcome_v3": outcome,
        "lanes": [lane],
        "primary_lane_ids": ["lane_1"],
        "plan_publication": {"created": [{"plan_id": "p-1", "symbol": "600000.SH"}]} if plan else {"created": []},
    }
    (run_dir / f"{run_id}.json").write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    child_run_id = f"{run_id}-comparison-1"
    child = {
        "run_id": child_run_id,
        "parent_run_id": run_id,
        "comparison_request_id": run_id,
        "run_role": "comparison",
        "slot": "close",
        "status": "READY",
        "generated_at": "2026-08-29T09:32:00+08:00",
        "outcome_v2": {
            **outcome,
            "primary_lane_ids": ["lane_2"],
            "counts": {"expected_lanes": 1, "recorded_lanes": 1, "ready_lanes": 1, "required_lanes": 1, "ready_required_lanes": 1},
            "lanes": [comparison_lane],
        },
        "plan_publication": {"publication": "COMPARISON_ONLY", "created": [], "activated": []},
    }
    (run_dir / f"{child_run_id}.json").write_text(json.dumps(child, ensure_ascii=False), encoding="utf-8")
    request_dir = root / "outputs" / "comparison_requests"
    request_dir.mkdir(parents=True)
    (request_dir / f"{run_id}.json").write_text(json.dumps({
        "schema_version": "liangjian-comparison-request/1.0.0",
        "request_id": run_id,
        "parent_run_id": run_id,
        "child_run_id": child_run_id,
        "status": "SUCCEEDED",
        "updated_at": "2026-08-29T09:33:00+08:00",
    }), encoding="utf-8")
    (audit_dir / f"research_{run_id}_lane_1.json").write_text(json.dumps(lane, ensure_ascii=False), encoding="utf-8")
    (root / "state").mkdir()
    (root / "state" / "workflow_progress.json").write_text(json.dumps({"status": "COMPLETED", "generated_at": "2026-08-29T09:31:00+08:00", "outcome_v3": outcome}), encoding="utf-8")
    _write_feature_db(root, run_id)
    evaluation = root / "outputs" / "evaluation"
    evaluation.mkdir(parents=True)
    (evaluation / "replay-window.json").write_text(json.dumps({"status": "READY" if replay_days >= 10 else "BLOCKED_REPLAY_WINDOW_INSUFFICIENT", "summary": {"independent_trading_days": replay_days, "minimum_days": 10, "terminal_days": replay_days}}), encoding="utf-8")
    gold = root / "storage" / "benchmarks" / "broker_gold"
    gold.mkdir(parents=True)
    for index in range(gold_months):
        month = f"2026-{index + 1:02d}"
        (gold / f"{month}.json").write_text(json.dumps({"records": [{"month": month, "broker": "fixture", "symbol": "600000.SH"}]}), encoding="utf-8")


def test_complete_fact_backed_report_is_engineering_pass(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    report = build_acceptance_report(tmp_path, generated_at="2026-08-29T12:00:00Z")

    assert report["verdict"] == ENGINEERING_PASS
    assert report["engineering_contract"]["status"] == "PASS"
    assert report["replay"]["independent_trading_days"] == 10
    assert report["broker_gold"]["month_count"] == 4
    assert report["nonempty_a3_plan"]["status"] == "PASS"
    assert report["feature_generation"]["isolation"] == "PASS"
    assert report["models"]["primary"][0]["model"] == "deepseek-v4-pro-0813"
    assert report["models"]["comparison"][0]["child_run_id"].endswith("comparison-1")
    assert report["run"]["run_id"] == "2026-08-29-close-acceptance"

    json_path, markdown_path = write_acceptance_report(report, tmp_path / "acceptance")
    assert json.loads(json_path.read_text(encoding="utf-8"))["report_hash"] == report["report_hash"]
    assert "ENGINEERING_PASS" in markdown_path.read_text(encoding="utf-8")


def test_missing_facts_are_explicit_and_never_pass(tmp_path: Path) -> None:
    report = build_acceptance_report(tmp_path, generated_at="2026-08-29T12:00:00Z")

    assert report["verdict"] == PENDING_BUSINESS_ACCEPTANCE
    assert report["run"]["status"] == "UNKNOWN"
    assert report["feature_generation"]["status"] == "UNKNOWN"
    assert report["storage"]["status"] in {"UNKNOWN", "PASS"}
    assert report["replay"]["status"] == "PENDING"
    assert report["broker_gold"]["status"] == "PENDING"
    assert report["nonempty_a3_plan"]["status"] == "PENDING"
    assert all(item["status"] != "PASS" for item in report["engineering_contract"]["checks"] if item["status"] == "UNKNOWN")


def test_insufficient_business_samples_are_pending_not_failed(tmp_path: Path) -> None:
    _write_fixture(tmp_path, replay_days=2, gold_months=1, plan=False)
    report = build_acceptance_report(tmp_path, generated_at="2026-08-29T12:00:00Z")

    assert report["verdict"] == PENDING_BUSINESS_ACCEPTANCE
    assert report["engineering_contract"]["status"] == "PASS"
    assert report["replay"]["status"] == "PENDING"
    assert report["broker_gold"]["status"] == "PENDING"
    assert report["nonempty_a3_plan"]["status"] == "PENDING"


def test_explicit_primary_failure_is_engineering_fail(tmp_path: Path) -> None:
    _write_fixture(tmp_path, run_status="FAILED")
    report = build_acceptance_report(tmp_path, generated_at="2026-08-29T12:00:00Z")

    assert report["verdict"] == ENGINEERING_FAIL
    assert any(item["severity"] == "HARD" for item in report["blockers"])


def test_report_hash_is_independent_of_absolute_workspace_root(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write_fixture(first)
    _write_fixture(second)

    left = build_acceptance_report(first, generated_at="2026-08-29T12:00:00Z")
    right = build_acceptance_report(second, generated_at="2026-08-29T12:00:00Z")

    assert left["root"] != right["root"]
    assert left["report_hash"] == right["report_hash"]
