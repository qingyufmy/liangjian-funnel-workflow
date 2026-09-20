"""Read-only evidence, migration and shadow-acceptance helpers for S11."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .state import RuntimeStore


EVIDENCE_SCHEMA = "liangjian-production-evidence/1.0.0"
MIGRATION_SCHEMA = "liangjian-migration-rehearsal/1.0.0"
SHADOW_REPORT_SCHEMA = "liangjian-shadow-stability/1.0.0"
REQUIRED_EVIDENCE_CATEGORIES = (
    "scheduler",
    "minute_data",
    "decisions",
    "orders",
    "a1_coverage",
    "market_reference",
)
REQUIRED_FAULTS = (
    "kill_restart",
    "network",
    "slow_source",
    "llm_timeout",
    "disk_full",
    "database_lock",
    "corrupt_input",
)
LEDGER_TABLES = (
    "virtual_accounts",
    "virtual_positions",
    "simulation_intents",
    "virtual_fills",
    "risk_reservations",
    "position_lots",
    "position_risk_plans",
)
_FORBIDDEN_EVIDENCE_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        rb'"(?:api_key|secret_key|access_token|webhook_url)"\s*:\s*"(?!\[?REDACTED)',
        rb"authorization\s*:\s*bearer\s+[A-Za-z0-9._-]+",
        rb'"(?:chain_of_thought|reasoning_content|internal_thought)"\s*:',
    )
)


class AcceptanceContractError(ValueError):
    def __init__(self, reason_code: str, message: str | None = None) -> None:
        self.reason_code = reason_code
        super().__init__(message or reason_code)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _parse_time(value: Any, reason: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AcceptanceContractError(reason) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AcceptanceContractError(reason)
    return parsed


def _has_symlink(path: Path) -> bool:
    current = Path(os.path.abspath(path))
    while True:
        if current.is_symlink():
            return True
        if current.parent == current:
            return False
        current = current.parent


def _relative_regular_file(root: Path, path: Path) -> str:
    if _has_symlink(path):
        raise AcceptanceContractError("EVIDENCE_SYMLINK_REJECTED")
    resolved_root = root.resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise AcceptanceContractError("EVIDENCE_PATH_OUTSIDE_ROOT") from exc
    if not resolved.is_file():
        raise AcceptanceContractError("EVIDENCE_FILE_MISSING")
    return relative.as_posix()


def _assert_redacted(path: Path) -> None:
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sample = carry + chunk
            if any(pattern.search(sample) for pattern in _FORBIDDEN_EVIDENCE_PATTERNS):
                raise AcceptanceContractError("EVIDENCE_SECRET_OR_PRIVATE_REASONING_DETECTED")
            carry = sample[-512:]


def build_evidence_package(
    *,
    root: str | Path,
    files: Mapping[str, str | Path],
    versions: Mapping[str, Any],
    time_range: Mapping[str, Any],
    generated_by: str,
    test_only: bool,
    redactions: Sequence[str] = (),
) -> dict[str, Any]:
    """Hash explicitly supplied read-only evidence files and write a manifest."""

    package_root = Path(root)
    if _has_symlink(package_root):
        raise AcceptanceContractError("EVIDENCE_SYMLINK_REJECTED")
    package_root = package_root.resolve()
    if not package_root.is_dir():
        raise AcceptanceContractError("EVIDENCE_ROOT_INVALID")
    if (package_root / "manifest.json").exists():
        raise AcceptanceContractError("EVIDENCE_MANIFEST_EXISTS")
    required_versions = ("code", "config", "rules", "model", "prompt")
    missing_versions = [name for name in required_versions if not str(versions.get(name) or "").strip()]
    if missing_versions:
        raise AcceptanceContractError("EVIDENCE_VERSION_MISSING")
    start = _parse_time(time_range.get("start"), "EVIDENCE_TIME_RANGE_INVALID")
    end = _parse_time(time_range.get("end"), "EVIDENCE_TIME_RANGE_INVALID")
    if end < start:
        raise AcceptanceContractError("EVIDENCE_TIME_RANGE_INVALID")
    if not str(generated_by).strip():
        raise AcceptanceContractError("EVIDENCE_GENERATOR_MISSING")

    entries: list[dict[str, Any]] = []
    for category, raw in sorted(files.items()):
        path = Path(raw)
        if not path.is_absolute():
            path = package_root / path
        relative = _relative_regular_file(package_root, path)
        _assert_redacted(path)
        entries.append({
            "category": str(category),
            "path": relative,
            "size_bytes": path.stat().st_size,
            "sha256": _hash_file(path),
        })
    present = {item["category"] for item in entries}
    missing = sorted(set(REQUIRED_EVIDENCE_CATEGORIES) - present)
    payload = {
        "schema_version": EVIDENCE_SCHEMA,
        "test_only": bool(test_only),
        "versions": {name: str(versions[name]) for name in required_versions},
        "time_range": {"start": start.isoformat(), "end": end.isoformat()},
        "generated_by": str(generated_by),
        "complete": not missing,
        "missing_categories": missing,
        "redactions": sorted({str(item) for item in redactions}),
        "files": entries,
    }
    payload["manifest_sha256"] = _canonical_hash(payload)
    (package_root / "manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return payload


def validate_evidence_package(root: str | Path) -> dict[str, Any]:
    package_root = Path(root)
    if _has_symlink(package_root):
        raise AcceptanceContractError("EVIDENCE_SYMLINK_REJECTED")
    package_root = package_root.resolve()
    manifest_path = package_root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AcceptanceContractError("EVIDENCE_MANIFEST_MISSING") from exc
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AcceptanceContractError("EVIDENCE_MANIFEST_INVALID") from exc
    if not isinstance(manifest, Mapping) or manifest.get("schema_version") != EVIDENCE_SCHEMA:
        raise AcceptanceContractError("EVIDENCE_MANIFEST_SCHEMA_INVALID")
    expected_digest = str(manifest.get("manifest_sha256") or "")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    if expected_digest != _canonical_hash(unsigned):
        raise AcceptanceContractError("EVIDENCE_MANIFEST_HASH_MISMATCH")
    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise AcceptanceContractError("EVIDENCE_FILE_INDEX_INVALID")
    verified: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, Mapping):
            raise AcceptanceContractError("EVIDENCE_FILE_INDEX_INVALID")
        relative = str(item.get("path") or "")
        path = package_root / relative
        _relative_regular_file(package_root, path)
        _assert_redacted(path)
        if _hash_file(path) != item.get("sha256") or path.stat().st_size != item.get("size_bytes"):
            raise AcceptanceContractError("EVIDENCE_HASH_MISMATCH")
        verified.append(dict(item))
    present = {str(item.get("category")) for item in verified}
    missing = sorted(set(REQUIRED_EVIDENCE_CATEGORIES) - present)
    complete = manifest.get("complete") is True and not missing
    return {
        "schema_version": EVIDENCE_SCHEMA,
        "status": "EVIDENCE_VALID" if complete else "MISSING_EVIDENCE",
        "test_only": manifest.get("test_only") is True,
        "complete": complete,
        "missing_categories": missing or list(manifest.get("missing_categories") or ()),
        "versions": dict(manifest.get("versions") or {}),
        "time_range": dict(manifest.get("time_range") or {}),
        "file_count": len(verified),
        "files_by_category": {str(item["category"]): str(item["path"]) for item in verified},
        "manifest_sha256": expected_digest,
    }


def _sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True) as src:
        with sqlite3.connect(destination) as dst:
            src.backup(dst)


def _integrity(path: Path) -> str:
    try:
        with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
            rows = connection.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.DatabaseError:
        return "invalid"
    return ";".join(str(row[0]) for row in rows)


def _table_counts(path: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        names = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        return {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in LEDGER_TABLES
            if table in names
        }


def _schema_hash(path: Path) -> str:
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name"
        ).fetchall()
    return _canonical_hash([tuple(row) for row in rows])


def rehearse_runtime_store_migration(
    source: str | Path,
    workdir: str | Path,
    *,
    migrate: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    """Migrate an isolated SQLite backup twice and prove the source is untouched."""

    source_path = Path(source)
    target_root = Path(workdir)
    if _has_symlink(source_path) or _has_symlink(target_root):
        raise AcceptanceContractError("MIGRATION_SYMLINK_REJECTED")
    source_path = source_path.resolve()
    target_root = target_root.resolve()
    if not source_path.is_file() or _integrity(source_path).lower() != "ok":
        raise AcceptanceContractError("MIGRATION_SOURCE_INVALID")
    if target_root.exists() and any(target_root.iterdir()):
        raise AcceptanceContractError("MIGRATION_WORKDIR_NOT_EMPTY")
    target_root.mkdir(parents=True, exist_ok=True)
    target = target_root / "migration.sqlite3"
    rollback = target_root / "rollback.sqlite3"
    source_hash_before = _hash_file(source_path)
    _sqlite_backup(source_path, target)
    _sqlite_backup(source_path, rollback)
    before = _table_counts(target)
    runner = migrate or (lambda path: RuntimeStore(path))
    try:
        runner(target)
        after_first = _table_counts(target)
        schema_first = _schema_hash(target)
        runner(target)
        after_second = _table_counts(target)
        schema_second = _schema_hash(target)
    except Exception as exc:
        return {
            "schema_version": MIGRATION_SCHEMA,
            "status": "FAILED_ROLLBACK_AVAILABLE",
            "error": type(exc).__name__,
            "source_unchanged": _hash_file(source_path) == source_hash_before,
            "rollback_path": str(rollback),
            "rollback_integrity": _integrity(rollback),
            "production_written": False,
        }
    all_tables = sorted(set(before) | set(after_first))
    ledger_checks = {
        table: {"before": before.get(table, 0), "after": after_first.get(table, 0), "preserved": before.get(table, 0) == after_first.get(table, 0)}
        for table in all_tables
    }
    preserved = all(item["preserved"] for item in ledger_checks.values())
    idempotent = after_first == after_second and schema_first == schema_second
    source_unchanged = _hash_file(source_path) == source_hash_before
    return {
        "schema_version": MIGRATION_SCHEMA,
        "status": "PASS" if preserved and idempotent and source_unchanged and _integrity(target).lower() == "ok" else "FAIL",
        "source_unchanged": source_unchanged,
        "target_integrity": _integrity(target),
        "rollback_integrity": _integrity(rollback),
        "idempotent": idempotent,
        "ledger_checks": ledger_checks,
        "rollback_path": str(rollback),
        "production_written": False,
    }


def _is_within(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def assert_shadow_namespace(
    *,
    official_store: str | Path,
    shadow_store: str | Path,
    official_output: str | Path,
    shadow_output: str | Path,
    official_account: str,
    shadow_account: str,
) -> None:
    paths = tuple(Path(item) for item in (official_store, shadow_store, official_output, shadow_output))
    if any(_has_symlink(path) for path in paths):
        raise AcceptanceContractError("SHADOW_SYMLINK_REJECTED")
    official_roots = (Path(official_store).resolve().parent, Path(official_output).resolve())
    shadow_roots = (Path(shadow_store).resolve().parent, Path(shadow_output).resolve())
    if any(_is_within(shadow, official) or _is_within(official, shadow) for official in official_roots for shadow in shadow_roots):
        raise AcceptanceContractError("SHADOW_PATH_OVERLAP")
    if str(official_account).strip() == str(shadow_account).strip():
        raise AcceptanceContractError("SHADOW_ACCOUNT_NOT_ISOLATED")


def build_shadow_stability_report(evidence: Mapping[str, Any]) -> dict[str, Any]:
    sessions = evidence.get("sessions")
    if not isinstance(sessions, Sequence) or isinstance(sessions, (str, bytes, bytearray)):
        raise AcceptanceContractError("SHADOW_SESSIONS_INVALID")
    faults = evidence.get("fault_injections")
    if not isinstance(faults, Mapping):
        raise AcceptanceContractError("SHADOW_FAULT_EVIDENCE_MISSING")
    reasons: list[str] = []
    if len(sessions) < 5:
        reasons.append("MINIMUM_5_SESSIONS_NOT_MET")
    if evidence.get("test_only") is True:
        reasons.append("TEST_ONLY_EVIDENCE")
    terminal_rates: list[float] = []
    evaluable_rates: list[float] = []
    quote_rates: list[float] = []
    model_rates: list[float] = []
    no_opportunity_rates: list[float] = []
    stage_p95: list[float] = []
    stage_p99: list[float] = []
    max_backlog: list[int] = []
    db_wait_p99: list[float] = []
    for raw in sessions:
        if not isinstance(raw, Mapping):
            raise AcceptanceContractError("SHADOW_SESSION_INVALID")
        terminal_rates.append(float(raw.get("terminal_rate", 0.0)))
        evaluable_rates.append(float(raw.get("data_evaluable_rate", 0.0)))
        quote_rates.append(float(raw.get("quote_coverage_rate", 0.0)))
        model_rates.append(float(raw.get("model_success_rate", 0.0)))
        no_opportunity_rates.append(float(raw.get("true_no_opportunity_rate", 0.0)))
        stage_p95.append(float(raw.get("stage_latency_p95_ms", 0.0)))
        stage_p99.append(float(raw.get("stage_latency_p99_ms", 0.0)))
        max_backlog.append(int(raw.get("max_backlog", 0)))
        db_wait_p99.append(float(raw.get("db_wait_p99_ms", 0.0)))
        if int(raw.get("duplicate_fills", 0)):
            reasons.append("DUPLICATE_FILL_DETECTED")
        if int(raw.get("negative_cash", 0)):
            reasons.append("NEGATIVE_CASH_DETECTED")
        if int(raw.get("future_data_uses", 0)):
            reasons.append("FUTURE_DATA_USE_DETECTED")
        if int(raw.get("missing_schedule_slots", 0)):
            reasons.append("SCHEDULE_SLOT_MISSING")
    if terminal_rates and min(terminal_rates) < 0.995:
        reasons.append("TERMINAL_RATE_BELOW_TARGET")
    missing_faults = [name for name in REQUIRED_FAULTS if str(faults.get(name) or "").upper() != "PASS"]
    if missing_faults:
        reasons.append("FAULT_INJECTION_INCOMPLETE")
    unique_reasons = sorted(set(reasons))
    return {
        "schema_version": SHADOW_REPORT_SCHEMA,
        "status": "OPERATIONS_ACCEPTED" if not unique_reasons else "OPERATIONS_NOT_ACCEPTED",
        "session_count": len(sessions),
        "terminal_rate_min": min(terminal_rates) if terminal_rates else None,
        "data_evaluable_rate_min": min(evaluable_rates) if evaluable_rates else None,
        "quote_coverage_rate_min": min(quote_rates) if quote_rates else None,
        "model_success_rate_min": min(model_rates) if model_rates else None,
        "true_no_opportunity_rate_mean": (sum(no_opportunity_rates) / len(no_opportunity_rates)) if no_opportunity_rates else None,
        "stage_latency_p95_ms_max": max(stage_p95) if stage_p95 else None,
        "stage_latency_p99_ms_max": max(stage_p99) if stage_p99 else None,
        "max_backlog": max(max_backlog) if max_backlog else None,
        "db_wait_p99_ms_max": max(db_wait_p99) if db_wait_p99 else None,
        "faults": {name: faults.get(name) for name in REQUIRED_FAULTS},
        "reason_codes": unique_reasons,
        "strategy_profitability_claim": False,
    }


__all__ = [
    "AcceptanceContractError",
    "EVIDENCE_SCHEMA",
    "MIGRATION_SCHEMA",
    "REQUIRED_EVIDENCE_CATEGORIES",
    "REQUIRED_FAULTS",
    "SHADOW_REPORT_SCHEMA",
    "assert_shadow_namespace",
    "build_evidence_package",
    "build_shadow_stability_report",
    "rehearse_runtime_store_migration",
    "validate_evidence_package",
]
