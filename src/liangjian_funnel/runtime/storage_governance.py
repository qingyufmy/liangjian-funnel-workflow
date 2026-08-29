"""Read-only storage governance and verified SQLite backup helpers.

This module deliberately sits outside the research workflow.  It provides
operational evidence for disk pressure, SQLite health, and retention
references without deleting rows or files.  A backup is the only mutating
operation exposed here, and it writes a new backup plus its manifest; the
source database is opened read-only and is never replaced.

The retention plan is intentionally advisory.  Every candidate is returned
with the references that were checked, but no cleanup action is implemented
in this module.  This makes the dry-run boundary explicit instead of relying
on a caller to remember it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


# These values are expressed as *free* disk percentages.  They intentionally
# match the operational contract: below 25% is an alert, below 15% blocks new
# full snapshots/rebuilds, and below 10% fails closed for heavy research.
DISK_FREE_WARNING_PERCENT = 25.0
DISK_FREE_CRITICAL_PERCENT = 15.0
DISK_FREE_BLOCK_PERCENT = 10.0
DISK_FREE_FULL_REBUILD_MIN_BYTES = 5 * 1024 * 1024 * 1024

STORAGE_SCHEMA_VERSION = "liangjian-storage-governance/1.0.0"
_REFERENCE_SKIP_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
    }
)
_REFERENCE_SUFFIXES = frozenset(
    {
        ".json",
        ".jsonl",
        ".ndjson",
        ".md",
        ".txt",
        ".yaml",
        ".yml",
        ".toml",
        ".csv",
    }
)
_GENERATION_TOKEN = re.compile(
    rb"(?:generation_id|feature_generation_id|generation|featureGenerationId)"
    rb"\s*[:=]\s*[\"']?([A-Za-z0-9][A-Za-z0-9_.:+-]{2,127})",
    re.IGNORECASE,
)


class StorageGovernanceError(RuntimeError):
    """Raised for an unsafe or unverifiable storage operation."""


@dataclass(frozen=True, slots=True)
class DiskWatermark:
    """A deterministic disk free-space classification."""

    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    free_percent: float
    status: str
    full_rebuild_allowed: bool
    incremental_write_allowed: bool
    research_write_allowed: bool
    read_only_allowed: bool

    @property
    def disk_free_ratio(self) -> float:
        """Compatibility ratio used by the existing resource diagnostics."""

        return self.free_percent / 100.0

    @property
    def reason_codes(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.free_percent < DISK_FREE_WARNING_PERCENT:
            reasons.append("DISK_FREE_BELOW_25_PERCENT")
        if self.free_percent < DISK_FREE_CRITICAL_PERCENT:
            reasons.append("DISK_FREE_BELOW_15_PERCENT")
        if self.free_bytes < DISK_FREE_FULL_REBUILD_MIN_BYTES:
            reasons.append("DISK_FREE_BELOW_5_GIB")
        if self.free_percent < DISK_FREE_BLOCK_PERCENT:
            reasons.append("DISK_FREE_BELOW_10_PERCENT")
        return tuple(reasons)

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "total_bytes": self.total_bytes,
            "used_bytes": self.used_bytes,
            "free_bytes": self.free_bytes,
            "free_percent": round(self.free_percent, 4),
            "free_ratio": round(self.disk_free_ratio, 6),
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "full_rebuild_allowed": self.full_rebuild_allowed,
            "incremental_write_allowed": self.incremental_write_allowed,
            "research_write_allowed": self.research_write_allowed,
            "read_only_allowed": self.read_only_allowed,
            "thresholds": {
                "warning_free_percent": DISK_FREE_WARNING_PERCENT,
                "critical_free_percent": DISK_FREE_CRITICAL_PERCENT,
                "block_free_percent": DISK_FREE_BLOCK_PERCENT,
                "full_rebuild_min_free_bytes": DISK_FREE_FULL_REBUILD_MIN_BYTES,
            },
        }


@dataclass(frozen=True, slots=True)
class SQLiteHealth:
    """Read-only health and size evidence for one SQLite file."""

    path: str
    exists: bool
    size_bytes: int
    sha256: str | None
    integrity_check: str
    page_count: int | None
    page_size: int | None
    freelist_count: int | None
    wal_size_bytes: int
    shm_size_bytes: int
    error: str | None = None

    @property
    def healthy(self) -> bool:
        return self.exists and self.integrity_check.lower() == "ok" and self.error is None

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "exists": self.exists,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "integrity_check": self.integrity_check,
            "healthy": self.healthy,
            "page_count": self.page_count,
            "page_size": self.page_size,
            "freelist_count": self.freelist_count,
            "wal_size_bytes": self.wal_size_bytes,
            "shm_size_bytes": self.shm_size_bytes,
            "error": self.error,
        }


def _file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def evaluate_disk_watermark(
    path: str | Path,
    *,
    usage: Any | None = None,
) -> DiskWatermark:
    """Measure and classify free disk space.

    ``usage`` is an optional object with ``total``, ``used`` and ``free``
    attributes.  It exists for deterministic tests and does not alter the
    production path, which always uses :func:`shutil.disk_usage`.
    """

    target = Path(path).resolve()
    measured = usage if usage is not None else shutil.disk_usage(target)
    total = int(getattr(measured, "total"))
    used = int(getattr(measured, "used"))
    free = int(getattr(measured, "free"))
    if total <= 0 or free < 0:
        raise StorageGovernanceError("DISK_USAGE_INVALID")
    free_percent = (free / total) * 100.0
    full_rebuild_allowed = (
        free_percent >= DISK_FREE_CRITICAL_PERCENT
        and free >= DISK_FREE_FULL_REBUILD_MIN_BYTES
    )
    if free_percent < DISK_FREE_BLOCK_PERCENT:
        status = "BLOCKED"
    elif free_percent < DISK_FREE_CRITICAL_PERCENT or free < DISK_FREE_FULL_REBUILD_MIN_BYTES:
        status = "CRITICAL"
    elif free_percent < DISK_FREE_WARNING_PERCENT:
        status = "WARNING"
    else:
        status = "OK"
    return DiskWatermark(
        path=str(target),
        total_bytes=total,
        used_bytes=used,
        free_bytes=free,
        free_percent=free_percent,
        status=status,
        full_rebuild_allowed=full_rebuild_allowed,
        incremental_write_allowed=free_percent >= DISK_FREE_BLOCK_PERCENT,
        research_write_allowed=free_percent >= DISK_FREE_BLOCK_PERCENT,
        read_only_allowed=True,
    )


# A short alias makes this useful from operational scripts without forcing
# callers to know the implementation's verb choice.
disk_watermark = evaluate_disk_watermark


def _read_only_uri(path: Path) -> str:
    # ``Path.as_uri`` handles spaces and Windows drive letters correctly.  A
    # URI with mode=ro prevents an audit or backup source connection from
    # creating a WAL/shm file or changing the source database.
    return f"{path.resolve().as_uri()}?mode=ro"


def sqlite_integrity_check(path: str | Path) -> str:
    """Run SQLite's full integrity check through a read-only connection."""

    target = Path(path).resolve()
    if not target.is_file():
        return "MISSING"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(_read_only_uri(target), uri=True, timeout=30)
        rows = connection.execute("PRAGMA integrity_check").fetchall()
        values = [str(row[0]) for row in rows]
        return "ok" if len(values) == 1 and values[0].lower() == "ok" else "; ".join(values) or "EMPTY"
    except sqlite3.Error as exc:
        return f"ERROR:{type(exc).__name__}"
    finally:
        if connection is not None:
            connection.close()


def inspect_sqlite(path: str | Path) -> SQLiteHealth:
    """Collect read-only size, hash, WAL, and integrity evidence."""

    target = Path(path).resolve()
    if not target.is_file():
        return SQLiteHealth(
            path=str(target),
            exists=False,
            size_bytes=0,
            sha256=None,
            integrity_check="MISSING",
            page_count=None,
            page_size=None,
            freelist_count=None,
            wal_size_bytes=_sidecar_size(target, "-wal"),
            shm_size_bytes=_sidecar_size(target, "-shm"),
            error=None,
        )
    page_count: int | None = None
    page_size: int | None = None
    freelist_count: int | None = None
    error: str | None = None
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(_read_only_uri(target), uri=True, timeout=30)
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        error = f"{type(exc).__name__}:{str(exc)[:160]}"
    finally:
        if connection is not None:
            connection.close()
    return SQLiteHealth(
        path=str(target),
        exists=True,
        size_bytes=target.stat().st_size,
        sha256=_file_sha256(target),
        integrity_check=sqlite_integrity_check(target),
        page_count=page_count,
        page_size=page_size,
        freelist_count=freelist_count,
        wal_size_bytes=_sidecar_size(target, "-wal"),
        shm_size_bytes=_sidecar_size(target, "-shm"),
        error=error,
    )


def _sidecar_size(path: Path, suffix: str) -> int:
    sidecar = Path(f"{path}{suffix}")
    try:
        return sidecar.stat().st_size
    except OSError:
        return 0


def _restore_validate(path: Path) -> dict[str, Any]:
    """Copy a backup into a temporary directory and validate the copy."""

    result: dict[str, Any]
    with tempfile.TemporaryDirectory(prefix="liangjian-storage-restore-") as directory:
        restored = Path(directory) / path.name
        shutil.copy2(path, restored)
        integrity = sqlite_integrity_check(restored)
        connection: sqlite3.Connection | None = None
        table_count: int | None = None
        error: str | None = None
        try:
            connection = sqlite3.connect(_read_only_uri(restored), uri=True, timeout=30)
            table_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                ).fetchone()[0]
            )
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            error = f"{type(exc).__name__}:{str(exc)[:160]}"
        finally:
            if connection is not None:
                connection.close()
        result = {
            "status": "PASS" if integrity.lower() == "ok" and error is None else "FAILED",
            "integrity_check": integrity,
            "table_count": table_count,
            "error": error,
        }
    # The context manager removes its directory before this result is
    # returned, making the cleanup assertion meaningful to operators/tests.
    result["temporary_directory_removed"] = not Path(directory).exists()
    return result


def backup_sqlite(
    source: str | Path,
    destination: str | Path,
    *,
    manifest_path: str | Path | None = None,
    verify_restore: bool = True,
    pages: int = 256,
    sleep: float = 0.05,
) -> dict[str, Any]:
    """Create a consistent SQLite backup using the online backup API.

    The destination and manifest must not exist already.  This avoids silent
    overwrite of an older recovery point and makes rerunning a command safe.
    """

    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    if not source_path.is_file():
        raise StorageGovernanceError("SQLITE_SOURCE_MISSING")
    if source_path == destination_path:
        raise StorageGovernanceError("SQLITE_BACKUP_SOURCE_EQUALS_DESTINATION")
    if not verify_restore:
        raise StorageGovernanceError("SQLITE_RESTORE_VALIDATION_REQUIRED")
    if destination_path.exists():
        raise StorageGovernanceError("SQLITE_BACKUP_DESTINATION_EXISTS")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = Path(manifest_path).resolve() if manifest_path is not None else Path(f"{destination_path}.manifest.json")
    if manifest.exists():
        raise StorageGovernanceError("SQLITE_BACKUP_MANIFEST_EXISTS")
    if pages <= 0 or sleep < 0:
        raise StorageGovernanceError("SQLITE_BACKUP_OPTIONS_INVALID")

    temp_path: Path | None = None
    source_connection: sqlite3.Connection | None = None
    destination_connection: sqlite3.Connection | None = None
    try:
        # Create a uniquely named sibling so the final rename is atomic within
        # one filesystem and an interrupted backup cannot look complete.
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination_path.name}.",
            suffix=".tmp",
            dir=destination_path.parent,
            delete=False,
        ) as temporary:
            temp_path = Path(temporary.name)
        temp_path.unlink(missing_ok=True)
        source_connection = sqlite3.connect(_read_only_uri(source_path), uri=True, timeout=30)
        destination_connection = sqlite3.connect(temp_path, timeout=30)
        source_connection.backup(
            destination_connection,
            pages=int(pages),
            sleep=float(sleep),
        )
        destination_connection.commit()
        destination_connection.close()
        destination_connection = None
        source_connection.close()
        source_connection = None
        # A hard-link publish is atomic and exclusive on the same filesystem:
        # unlike os.replace(), a concurrent invocation cannot silently
        # overwrite an already-created recovery point.
        os.link(temp_path, destination_path)
        temp_path.unlink()
        temp_path = None
    except (sqlite3.Error, OSError) as exc:
        raise StorageGovernanceError(f"SQLITE_BACKUP_FAILED:{type(exc).__name__}") from exc
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            source_connection.close()
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    backup_health = inspect_sqlite(destination_path)
    restore_validation = _restore_validate(destination_path)
    if not backup_health.healthy or restore_validation["status"] != "PASS":
        # The backup file is deliberately retained for forensic inspection;
        # no cleanup is attempted by this module.
        raise StorageGovernanceError("SQLITE_BACKUP_VALIDATION_FAILED")

    payload: dict[str, Any] = {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "created_at": _utc_now(),
        "source": {
            "path": str(source_path),
            "size_bytes": source_path.stat().st_size,
            "sha256_at_start_or_after": _file_sha256(source_path),
        },
        "backup": {
            "path": str(destination_path),
            "size_bytes": destination_path.stat().st_size,
            "sha256": backup_health.sha256,
            "integrity_check": backup_health.integrity_check,
        },
        # ``sha256`` is retained as a short, script-friendly alias for the
        # canonical backup digest.
        "sha256": backup_health.sha256,
        "restore_validation": restore_validation,
        "manifest_path": str(manifest),
    }
    manifest_digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    payload["manifest_sha256"] = manifest_digest
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest_tmp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{manifest.name}.",
            suffix=".tmp",
            dir=manifest.parent,
            mode="w",
            encoding="utf-8",
            delete=False,
        ) as handle:
            manifest_tmp = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        # Publish the manifest with the same no-overwrite guarantee as the
        # backup.  A manifest is evidence for a recovery point, so silently
        # replacing it would invalidate the audit trail.
        os.link(manifest_tmp, manifest)
        manifest_tmp.unlink()
        manifest_tmp = None
    finally:
        if manifest_tmp is not None:
            manifest_tmp.unlink(missing_ok=True)
    return payload


def _connect_readonly(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(_read_only_uri(path), uri=True, timeout=30)


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(connection, table):
        return set()
    return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _rows_as_dict(connection: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    return [dict(row) for row in connection.execute(sql).fetchall()]


def _database_references(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read active, previous, run-bound, and known generation rows."""

    active: list[dict[str, Any]] = []
    previous: list[dict[str, Any]] = []
    run_bound: list[dict[str, Any]] = []
    generations: list[dict[str, Any]] = []
    if not path.is_file():
        return active, previous, run_bound, generations
    connection: sqlite3.Connection | None = None
    try:
        connection = _connect_readonly(path)
        if _table_exists(connection, "feature_generations"):
            generations = _rows_as_dict(
                connection,
                "SELECT generation_id,domain,status,purpose,as_of,created_at "
                "FROM feature_generations ORDER BY created_at,generation_id",
            )
        if _table_exists(connection, "active_feature_generations"):
            columns = _columns(connection, "active_feature_generations")
            wanted = [name for name in ("domain", "generation_id", "previous_generation_id", "activated_at") if name in columns]
            if "generation_id" in wanted:
                active = _rows_as_dict(
                    connection,
                    f"SELECT {','.join(wanted)} FROM active_feature_generations",
                )
                previous = [
                    {
                        "domain": row.get("domain"),
                        "generation_id": row.get("previous_generation_id"),
                        "active_generation_id": row.get("generation_id"),
                        "activated_at": row.get("activated_at"),
                    }
                    for row in active
                    if row.get("previous_generation_id")
                ]
        if _table_exists(connection, "run_feature_bindings"):
            columns = _columns(connection, "run_feature_bindings")
            wanted = [name for name in ("run_id", "domain", "generation_id", "bound_at") if name in columns]
            if {"run_id", "generation_id"}.issubset(wanted):
                run_bound = _rows_as_dict(
                    connection,
                    f"SELECT {','.join(wanted)} FROM run_feature_bindings",
                )
    except (sqlite3.Error, OSError):
        # The caller still gets a useful disk/database health report.  A
        # malformed/missing reference schema simply produces no candidates
        # rather than guessing that an object is safe to remove.
        active, previous, run_bound, generations = [], [], [], []
    finally:
        if connection is not None:
            connection.close()
    return active, previous, run_bound, generations


def _iter_reference_files(roots: Iterable[str | Path]) -> Iterable[Path]:
    seen: set[Path] = set()
    for raw_root in roots:
        root = Path(raw_root).resolve()
        if root.is_file():
            candidates = (root,)
        elif root.is_dir():
            candidates = root.rglob("*")
        else:
            continue
        for candidate in candidates:
            if not candidate.is_file() or candidate in seen:
                continue
            if any(part in _REFERENCE_SKIP_DIRS for part in candidate.parts):
                continue
            if candidate.suffix.lower() not in _REFERENCE_SUFFIXES:
                continue
            # SQLite and generated binary blobs can carry an incidental token;
            # they are not snapshot/reference manifests and may be very large.
            if candidate.name.endswith((".sqlite", ".sqlite3", ".db", ".wal", ".shm")):
                continue
            seen.add(candidate)
            yield candidate


def _scan_snapshot_references(
    roots: Sequence[str | Path],
    known_generation_ids: set[str],
) -> list[dict[str, Any]]:
    if not known_generation_ids:
        return []
    references: list[dict[str, Any]] = []
    # Use direct byte search first; this handles arbitrary JSON whitespace and
    # avoids loading a 200MB historical snapshot into RAM.  The regex path
    # covers files that use a conventional generation_id key.
    encoded = {item: item.encode("utf-8") for item in known_generation_ids}
    max_token = max(len(token) for token in encoded.values())
    for path in _iter_reference_files(roots):
        try:
            with path.open("rb") as handle:
                tail = b""
                found: set[str] = set()
                while True:
                    chunk = handle.read(64 * 1024)
                    if not chunk:
                        break
                    data = tail + chunk
                    for generation_id, token in encoded.items():
                        if token in data:
                            found.add(generation_id)
                    for match in _GENERATION_TOKEN.finditer(data):
                        token = match.group(1).decode("utf-8", errors="ignore")
                        if token in known_generation_ids:
                            found.add(token)
                    tail = data[-max_token:]
                for generation_id in sorted(found):
                    references.append(
                        {
                            "kind": "snapshot",
                            "generation_id": generation_id,
                            "path": str(path),
                        }
                    )
        except (OSError, UnicodeError):
            # A file that cannot be read is not evidence that its references
            # are absent.  It therefore contributes no deletion candidate.
            continue
    return references


def scan_reference_plan(
    feature_store_db: str | Path,
    *,
    snapshot_roots: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Produce an auditable, non-destructive retention/reference plan.

    The returned ``candidates`` are merely objects that have no *known*
    active, previous, run-bound, or snapshot reference.  They are explicitly
    marked ``deletion_allowed: false`` and must not be interpreted as an
    instruction to remove anything.
    """

    database_path = Path(feature_store_db).resolve()
    active, previous, run_bound, generations = _database_references(database_path)
    snapshot_refs = _scan_snapshot_references(snapshot_roots, {str(row.get("generation_id")) for row in generations if row.get("generation_id")})
    active_ids = {str(row["generation_id"]) for row in active if row.get("generation_id")}
    previous_ids = {str(row["generation_id"]) for row in previous if row.get("generation_id")}
    run_ids = {str(row["generation_id"]) for row in run_bound if row.get("generation_id")}
    snapshot_ids = {str(row["generation_id"]) for row in snapshot_refs if row.get("generation_id")}
    referenced_ids = active_ids | previous_ids | run_ids | snapshot_ids
    candidates = []
    for row in generations:
        generation_id = str(row.get("generation_id") or "")
        if not generation_id or generation_id in referenced_ids:
            continue
        candidates.append(
            {
                "kind": "feature_generation",
                "generation_id": generation_id,
                "status": row.get("status"),
                "purpose": row.get("purpose"),
                "path": str(database_path),
                "references_checked": [
                    "active_feature_generations",
                    "previous_generation_id",
                    "run_feature_bindings",
                    "snapshot_roots",
                ],
                "deletion_allowed": False,
                "action": "REVIEW_ONLY",
            }
        )
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "feature_store_db": str(database_path),
        "active": active,
        "previous": previous,
        "run_bound": run_bound,
        "snapshot_refs": snapshot_refs,
        "referenced_generation_ids": sorted(referenced_ids),
        "candidates": candidates,
        "deletion_allowed": False,
        "dry_run": True,
    }


def storage_audit(
    root: str | Path,
    *,
    database_paths: Sequence[str | Path] = (),
    feature_store_db: str | Path | None = None,
    snapshot_roots: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Return disk, SQLite, and retention evidence without changing state."""

    target_root = Path(root).resolve()
    watermark = evaluate_disk_watermark(target_root)
    paths: list[Path] = []
    for raw in database_paths:
        candidate = Path(raw).resolve()
        if candidate not in paths:
            paths.append(candidate)
    reference_db = Path(feature_store_db).resolve() if feature_store_db is not None else (paths[0] if paths else None)
    checks = [inspect_sqlite(path).as_dict() for path in paths]
    reference_plan = (
        scan_reference_plan(reference_db, snapshot_roots=snapshot_roots)
        if reference_db is not None
        else {
            "schema_version": STORAGE_SCHEMA_VERSION,
            "feature_store_db": None,
            "active": [],
            "previous": [],
            "run_bound": [],
            "snapshot_refs": [],
            "referenced_generation_ids": [],
            "candidates": [],
            "deletion_allowed": False,
            "dry_run": True,
        }
    )
    unhealthy = [item for item in checks if not item["healthy"] and item["exists"]]
    status = "BLOCKED" if watermark.status == "BLOCKED" or unhealthy else watermark.status
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "status": status,
        "disk": watermark.as_dict(),
        "databases": checks,
        "reference_plan": reference_plan,
        "cleanup": {
            "dry_run": True,
            "deletion_allowed": False,
            "candidate_count": len(reference_plan["candidates"]),
        },
    }


def storage_cleanup_plan(
    feature_store_db: str | Path,
    *,
    snapshot_roots: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Return the dry-run cleanup shape; never delete anything."""

    plan = scan_reference_plan(feature_store_db, snapshot_roots=snapshot_roots)
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "status": "DRY_RUN",
        "dry_run": True,
        "deletion_allowed": False,
        "message": "No files or database rows are deleted by this build.",
        "reference_plan": plan,
    }


__all__ = [
    "DISK_FREE_BLOCK_PERCENT",
    "DISK_FREE_CRITICAL_PERCENT",
    "DISK_FREE_FULL_REBUILD_MIN_BYTES",
    "DISK_FREE_WARNING_PERCENT",
    "DiskWatermark",
    "SQLiteHealth",
    "STORAGE_SCHEMA_VERSION",
    "StorageGovernanceError",
    "backup_sqlite",
    "disk_watermark",
    "evaluate_disk_watermark",
    "inspect_sqlite",
    "scan_reference_plan",
    "sqlite_integrity_check",
    "storage_audit",
    "storage_cleanup_plan",
]
