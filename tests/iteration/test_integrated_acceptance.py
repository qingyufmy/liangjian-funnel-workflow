from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from liangjian_funnel.runtime.iteration_acceptance import (
    AcceptanceContractError,
    assert_shadow_namespace,
    build_evidence_package,
    build_shadow_stability_report,
    rehearse_runtime_store_migration,
    validate_evidence_package,
)
from liangjian_funnel.runtime.state import RuntimeStore


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def _evidence_files(root: Path) -> dict[str, Path]:
    files = {
        "scheduler": root / "scheduler.json",
        "minute_data": root / "minute-data.json",
        "decisions": root / "decisions.json",
        "orders": root / "orders.json",
        "a1_coverage": root / "a1-coverage.json",
        "market_reference": root / "market-reference.json",
    }
    for category, path in files.items():
        _write_json(path, {"category": category, "records": []})
    return files


def test_ops_01_evidence_package_reports_missing_environment_instead_of_passing(tmp_path: Path) -> None:
    root = tmp_path / "package"
    root.mkdir()
    files = _evidence_files(root)
    files.pop("orders").unlink()
    manifest = build_evidence_package(
        root=root,
        files=files,
        versions={"code": "abc", "config": "cfg", "rules": "rules", "model": "model", "prompt": "prompt"},
        time_range={"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"},
        generated_by="READ_ONLY_EXPLICIT_EXPORT",
        test_only=True,
    )
    assert manifest["complete"] is False
    report = validate_evidence_package(root)
    assert report["status"] == "MISSING_EVIDENCE"
    assert "orders" in report["missing_categories"]


def test_ops_02_migration_is_idempotent_and_failed_attempt_keeps_rollback_copy(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    RuntimeStore(source).ensure_virtual_account("paper:lane_1", "test", 1_000_000)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    report = rehearse_runtime_store_migration(source, tmp_path / "success")
    assert report["status"] == "PASS"
    assert report["idempotent"] is True
    assert report["source_unchanged"] is True
    assert report["ledger_checks"]["virtual_accounts"]["before"] == report["ledger_checks"]["virtual_accounts"]["after"] == 1

    def broken_migration(path: Path) -> None:
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE half_written(value INTEGER)")
        raise RuntimeError("injected interruption")

    failed = rehearse_runtime_store_migration(source, tmp_path / "failed", migrate=broken_migration)
    assert failed["status"] == "FAILED_ROLLBACK_AVAILABLE"
    assert failed["rollback_integrity"] == "ok"
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


def test_ops_03_shadow_report_detects_faults_and_does_not_call_them_stable() -> None:
    report = build_shadow_stability_report(
        {
            "test_only": False,
            "sessions": [
                {"trade_date": "2026-09-14", "terminal_rate": 1.0, "data_evaluable_rate": 0.95, "duplicate_fills": 0, "negative_cash": 0, "future_data_uses": 0, "missing_schedule_slots": 0},
                {"trade_date": "2026-09-15", "terminal_rate": 0.99, "data_evaluable_rate": 0.80, "duplicate_fills": 1, "negative_cash": 0, "future_data_uses": 0, "missing_schedule_slots": 0},
            ],
            "fault_injections": {"kill_restart": "PASS", "network": "PASS", "slow_source": "PASS", "llm_timeout": "PASS", "disk_full": "PASS", "database_lock": "PASS", "corrupt_input": "PASS"},
        }
    )
    assert report["status"] == "OPERATIONS_NOT_ACCEPTED"
    assert "DUPLICATE_FILL_DETECTED" in report["reason_codes"]
    assert "MINIMUM_5_SESSIONS_NOT_MET" in report["reason_codes"]


def test_ops_04_shadow_paths_reject_overlap_parent_child_and_symlinks(tmp_path: Path) -> None:
    official = tmp_path / "official"
    shadow = tmp_path / "shadow"
    official.mkdir()
    shadow.mkdir()
    assert_shadow_namespace(
        official_store=official / "runtime.sqlite3",
        shadow_store=shadow / "runtime.sqlite3",
        official_output=official / "outputs",
        shadow_output=shadow / "outputs",
        official_account="paper:lane_1",
        shadow_account="shadow:lane_1",
    )
    with pytest.raises(AcceptanceContractError, match="SHADOW_PATH_OVERLAP"):
        assert_shadow_namespace(
            official_store=official / "runtime.sqlite3",
            shadow_store=official / "child" / "runtime.sqlite3",
            official_output=official / "outputs",
            shadow_output=shadow / "outputs",
            official_account="paper:lane_1",
            shadow_account="shadow:lane_1",
        )
    link = tmp_path / "link"
    try:
        link.symlink_to(official, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(AcceptanceContractError, match="SHADOW_SYMLINK_REJECTED"):
        assert_shadow_namespace(
            official_store=official / "runtime.sqlite3",
            shadow_store=link / "runtime.sqlite3",
            official_output=official / "outputs",
            shadow_output=shadow / "outputs",
            official_account="paper:lane_1",
            shadow_account="shadow:lane_1",
        )


def test_ops_07_complete_package_generates_report_only_from_hashed_files(tmp_path: Path) -> None:
    root = tmp_path / "package"
    root.mkdir()
    files = _evidence_files(root)
    build_evidence_package(
        root=root,
        files=files,
        versions={"code": "abc", "config": "cfg", "rules": "rules", "model": "model", "prompt": "prompt"},
        time_range={"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"},
        generated_by="READ_ONLY_EXPLICIT_EXPORT",
        test_only=False,
    )
    report = validate_evidence_package(root)
    assert report["status"] == "EVIDENCE_VALID"
    files["orders"].write_text("tampered", encoding="utf-8")
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_HASH_MISMATCH"):
        validate_evidence_package(root)


def test_evidence_package_rejects_credentials_and_private_reasoning(tmp_path: Path) -> None:
    root = tmp_path / "secret-package"
    root.mkdir()
    secret = root / "orders.json"
    secret.write_text('{"api_key":"must-not-export"}', encoding="utf-8")
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_SECRET_OR_PRIVATE_REASONING_DETECTED"):
        build_evidence_package(
            root=root,
            files={"orders": secret},
            versions={"code": "abc", "config": "cfg", "rules": "rules", "model": "model", "prompt": "prompt"},
            time_range={"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"},
            generated_by="READ_ONLY_EXPLICIT_EXPORT",
            test_only=True,
        )


@pytest.mark.parametrize(
    ("versions", "time_range", "generated_by", "reason"),
    [
        ({"code": "", "config": "c", "rules": "r", "model": "m", "prompt": "p"}, {"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"}, "export", "EVIDENCE_VERSION_MISSING"),
        ({"code": "a", "config": "c", "rules": "r", "model": "m", "prompt": "p"}, {"start": "bad", "end": "2026-09-18T15:00:00+08:00"}, "export", "EVIDENCE_TIME_RANGE_INVALID"),
        ({"code": "a", "config": "c", "rules": "r", "model": "m", "prompt": "p"}, {"start": "2026-09-18T09:25:00", "end": "2026-09-18T15:00:00+08:00"}, "export", "EVIDENCE_TIME_RANGE_INVALID"),
        ({"code": "a", "config": "c", "rules": "r", "model": "m", "prompt": "p"}, {"start": "2026-09-18T15:00:00+08:00", "end": "2026-09-18T09:25:00+08:00"}, "export", "EVIDENCE_TIME_RANGE_INVALID"),
        ({"code": "a", "config": "c", "rules": "r", "model": "m", "prompt": "p"}, {"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"}, "", "EVIDENCE_GENERATOR_MISSING"),
    ],
)
def test_evidence_manifest_metadata_fails_closed(tmp_path: Path, versions, time_range, generated_by: str, reason: str) -> None:
    root = tmp_path / reason
    root.mkdir()
    item = root / "item.json"
    _write_json(item, {})
    with pytest.raises(AcceptanceContractError, match=reason):
        build_evidence_package(root=root, files={"orders": item}, versions=versions, time_range=time_range, generated_by=generated_by, test_only=True)


def test_evidence_paths_and_immutable_manifest_are_guarded(tmp_path: Path) -> None:
    missing_root = tmp_path / "missing"
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_ROOT_INVALID"):
        build_evidence_package(root=missing_root, files={}, versions={}, time_range={}, generated_by="x", test_only=True)
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside.json"
    _write_json(outside, {})
    versions = {"code": "a", "config": "c", "rules": "r", "model": "m", "prompt": "p"}
    window = {"start": "2026-09-18T09:25:00+08:00", "end": "2026-09-18T15:00:00+08:00"}
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_PATH_OUTSIDE_ROOT"):
        build_evidence_package(root=root, files={"orders": outside}, versions=versions, time_range=window, generated_by="x", test_only=True)
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_FILE_MISSING"):
        build_evidence_package(root=root, files={"orders": root / "missing.json"}, versions=versions, time_range=window, generated_by="x", test_only=True)
    item = root / "item.json"
    _write_json(item, {})
    build_evidence_package(root=root, files={"orders": "item.json"}, versions=versions, time_range=window, generated_by="x", test_only=True)
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_MANIFEST_EXISTS"):
        build_evidence_package(root=root, files={"orders": item}, versions=versions, time_range=window, generated_by="x", test_only=True)


def _resign(payload: dict) -> dict:
    unsigned = dict(payload)
    unsigned.pop("manifest_sha256", None)
    payload["manifest_sha256"] = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    return payload


def test_evidence_validator_rejects_missing_invalid_and_malformed_manifests(tmp_path: Path) -> None:
    root = tmp_path / "validator"
    root.mkdir()
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_MANIFEST_MISSING"):
        validate_evidence_package(root)
    (root / "manifest.json").write_text("bad", encoding="utf-8")
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_MANIFEST_INVALID"):
        validate_evidence_package(root)
    _write_json(root / "manifest.json", {"schema_version": "wrong"})
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_MANIFEST_SCHEMA_INVALID"):
        validate_evidence_package(root)
    payload = {"schema_version": "liangjian-production-evidence/1.0.0", "manifest_sha256": "bad", "files": []}
    _write_json(root / "manifest.json", payload)
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_MANIFEST_HASH_MISMATCH"):
        validate_evidence_package(root)
    payload = _resign({"schema_version": "liangjian-production-evidence/1.0.0", "files": "bad", "complete": False})
    _write_json(root / "manifest.json", payload)
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_FILE_INDEX_INVALID"):
        validate_evidence_package(root)
    payload = _resign({"schema_version": "liangjian-production-evidence/1.0.0", "files": ["bad"], "complete": False})
    _write_json(root / "manifest.json", payload)
    with pytest.raises(AcceptanceContractError, match="EVIDENCE_FILE_INDEX_INVALID"):
        validate_evidence_package(root)


def test_migration_rehearsal_rejects_corrupt_source_and_nonempty_workdir(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.sqlite3"
    corrupt.write_text("not sqlite", encoding="utf-8")
    with pytest.raises(AcceptanceContractError, match="MIGRATION_SOURCE_INVALID"):
        rehearse_runtime_store_migration(corrupt, tmp_path / "work")
    source = tmp_path / "source.sqlite3"
    RuntimeStore(source)
    work = tmp_path / "nonempty"
    work.mkdir()
    _write_json(work / "keep.json", {})
    with pytest.raises(AcceptanceContractError, match="MIGRATION_WORKDIR_NOT_EMPTY"):
        rehearse_runtime_store_migration(source, work)


def test_shadow_contract_rejects_bad_shape_same_account_and_accepts_five_clean_sessions(tmp_path: Path) -> None:
    with pytest.raises(AcceptanceContractError, match="SHADOW_SESSIONS_INVALID"):
        build_shadow_stability_report({"sessions": "bad", "fault_injections": {}})
    with pytest.raises(AcceptanceContractError, match="SHADOW_FAULT_EVIDENCE_MISSING"):
        build_shadow_stability_report({"sessions": []})
    with pytest.raises(AcceptanceContractError, match="SHADOW_SESSION_INVALID"):
        build_shadow_stability_report({"sessions": ["bad"] * 5, "fault_injections": {name: "PASS" for name in ("kill_restart", "network", "slow_source", "llm_timeout", "disk_full", "database_lock", "corrupt_input")}})
    with pytest.raises(AcceptanceContractError, match="SHADOW_ACCOUNT_NOT_ISOLATED"):
        assert_shadow_namespace(
            official_store=tmp_path / "official" / "runtime.sqlite3",
            shadow_store=tmp_path / "shadow" / "runtime.sqlite3",
            official_output=tmp_path / "official" / "outputs",
            shadow_output=tmp_path / "shadow" / "outputs",
            official_account="same",
            shadow_account="same",
        )
    session = {"terminal_rate": 1.0, "data_evaluable_rate": 1.0, "quote_coverage_rate": 1.0, "model_success_rate": 1.0, "duplicate_fills": 0, "negative_cash": 0, "future_data_uses": 0, "missing_schedule_slots": 0}
    clean = build_shadow_stability_report({
        "test_only": False,
        "sessions": [{**session, "trade_date": f"2026-09-{day:02d}"} for day in range(14, 19)],
        "fault_injections": {name: "PASS" for name in ("kill_restart", "network", "slow_source", "llm_timeout", "disk_full", "database_lock", "corrupt_input")},
    })
    assert clean["status"] == "OPERATIONS_ACCEPTED"


def test_ops_01_acceptance_entry_returns_missing_evidence_code(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [sys.executable, str(root / "scripts" / "run_iteration_acceptance.py"), "--profile", "replay", "--output-dir", str(tmp_path / "acceptance")],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 3
    summary = json.loads((tmp_path / "acceptance" / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "PENDING_EVIDENCE"


def test_ops_05_requirement_bindings_name_real_python_tests() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "scripts" / "run_iteration_acceptance.py").read_text(encoding="utf-8")
    required = (
        "OBS-01", "SRC-01", "A4-01", "A1-01", "EX-01", "RISK-01", "LLM-01",
        "UI-01", "SRC-09", "EVAL-01", "OPS-01", "OPS-02", "OPS-03", "OPS-04", "OPS-05", "OPS-06", "OPS-07",
    )
    assert all(f'"{item}"' in source for item in required)
    bindings = re.findall(r'"(tests/[^"\n]+\.py)::(test_[A-Za-z0-9_]+)', source)
    assert bindings
    for relative, function in bindings:
        path = root / relative
        assert path.is_file(), relative
        assert re.search(rf"^def {re.escape(function)}\b", path.read_text(encoding="utf-8"), re.MULTILINE), f"{relative}::{function}"


@pytest.mark.parametrize(
    "script",
    (
        "run_iteration_acceptance.py",
        "export_iteration_evidence.py",
        "rehearse_runtime_migration.py",
        "build_shadow_stability_report.py",
        "run_layered_evaluation.py",
    ),
)
def test_ops_06_documented_commands_have_working_help(script: str) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [sys.executable, str(root / "scripts" / script), "--help"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "usage:" in completed.stdout.lower()
