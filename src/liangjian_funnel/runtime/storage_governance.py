"""Storage governance, verified SQLite backups, and safe file retention.

This module deliberately sits outside the research workflow.  It provides
operational evidence for disk pressure, SQLite health, and retention
references.  SQLite sources are opened read-only and retention never mutates
SQLite rows or feature generations.  The default retention operation is also
recoverable: each selected file is gzip archived, hash-verified, and only then
removed from its source directory.

The retention plan remains a dry-run until a caller supplies an explicit
project root, policy, plan manifest, and confirmation token (or a separate
confirmation manifest).  Every candidate is bound to its path, size, mtime,
and SHA-256 so a changed file fails closed instead of being silently removed.
"""

from __future__ import annotations

import hashlib
import fnmatch
import gzip
import hmac
import json
import os
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


# These values are expressed as *free* disk percentages.  They intentionally
# match the operational contract: below 25% is an alert, below 15% blocks new
# full snapshots/rebuilds, and below 10% fails closed for heavy research.
DISK_FREE_WARNING_PERCENT = 25.0
DISK_FREE_CRITICAL_PERCENT = 15.0
DISK_FREE_BLOCK_PERCENT = 10.0
DISK_FREE_FULL_REBUILD_MIN_BYTES = 5 * 1024 * 1024 * 1024

STORAGE_SCHEMA_VERSION = "liangjian-storage-governance/1.0.0"
RETENTION_SCHEMA_VERSION = "liangjian-storage-retention/1.0.0"
RETENTION_DEFAULT_KEEP_DAYS = 30
RETENTION_RECENT_HOURS = 24
RETENTION_ALLOWED_RELATIVE_DIRS = (
    Path("outputs") / "research",
    Path("storage") / "snapshots",
    Path("storage") / "facts" / "snapshots",
)
RETENTION_ARCHIVE_RELATIVE_DIR = Path("storage") / "archive" / "retention"
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
            generation_columns = _columns(connection, "feature_generations")
            generation_fields = [
                name
                for name in (
                    "generation_id", "domain", "status", "purpose", "as_of", "created_at",
                    "metadata_json", "validation_manifest_json",
                )
                if name in generation_columns
            ]
            generations = _rows_as_dict(
                connection,
                f"SELECT {','.join(generation_fields)} "
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


def _safe_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _live_source_references(generations: Sequence[dict[str, Any]]) -> tuple[set[str], list[dict[str, Any]]]:
    ready_sources = []
    staging_references: list[dict[str, Any]] = []
    for row in generations:
        generation_id = str(row.get("generation_id") or "")
        purpose = str(row.get("purpose") or "").upper()
        status = str(row.get("status") or "").upper()
        metadata = _safe_json_object(row.get("metadata_json"))
        validation = _safe_json_object(row.get("validation_manifest_json"))
        if purpose == "LIVE_SOURCE" and status in {"SEALED", "PUBLISHED"} and validation.get("status") == "READY":
            ready_sources.append(row)
        if status in {"STAGING", "VALIDATED"}:
            source_id = str(
                metadata.get("source_generation_id")
                or metadata.get("live_source_generation_id")
                or ""
            ).strip()
            if source_id:
                staging_references.append(
                    {
                        "kind": "staging_source",
                        "generation_id": source_id,
                        "target_generation_id": generation_id,
                    }
                )
    ready_sources.sort(
        key=lambda row: (str(row.get("as_of") or ""), str(row.get("created_at") or ""), str(row.get("generation_id") or "")),
        reverse=True,
    )
    protected = {
        str(row.get("generation_id"))
        for row in ready_sources[:2]
        if row.get("generation_id")
    }
    protected.update(str(item["generation_id"]) for item in staging_references)
    return protected, staging_references


def live_source_storage_projection(feature_store_db: str | Path) -> dict[str, Any]:
    """Estimate 7/14-day LIVE_SOURCE growth without mutating the store."""

    path = Path(feature_store_db).resolve()
    if not path.is_file():
        return {"source_count": 0, "average_bytes": 0, "projected_7d_bytes": 0, "projected_14d_bytes": 0}
    connection: sqlite3.Connection | None = None
    try:
        connection = _connect_readonly(path)
        rows = connection.execute(
            """
            SELECT g.generation_id,
                   COALESCE((SELECT SUM(LENGTH(payload_json)) FROM feature_generation_members m WHERE m.generation_id=g.generation_id),0)
                 + COALESCE((SELECT SUM(LENGTH(payload_json)) FROM stock_fundamental_features f WHERE f.generation_id=g.generation_id),0)
                 + COALESCE((SELECT SUM(LENGTH(payload_json)) FROM taxonomy_membership_versions t WHERE t.generation_id=g.generation_id),0)
                 + COALESCE((SELECT SUM(LENGTH(payload_json)) FROM business_exposure_facts b WHERE b.generation_id=g.generation_id),0) AS payload_bytes
            FROM feature_generations g
            WHERE g.purpose='LIVE_SOURCE' AND g.status IN ('SEALED','PUBLISHED')
            ORDER BY g.as_of DESC, g.created_at DESC
            LIMIT 14
            """
        ).fetchall()
        sizes = [max(0, int(row[1] or 0)) for row in rows]
    except sqlite3.Error:
        sizes = []
    finally:
        if connection is not None:
            connection.close()
    average = int(sum(sizes) / len(sizes)) if sizes else 0
    return {
        "source_count": len(sizes),
        "average_bytes": average,
        "projected_7d_bytes": average * 7,
        "projected_14d_bytes": average * 14,
        "basis": "PAYLOAD_LENGTH_LAST_14_READY_SOURCES",
    }


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
    live_source_ids, staging_source_refs = _live_source_references(generations)
    referenced_ids = active_ids | previous_ids | run_ids | snapshot_ids | live_source_ids
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
                    "latest_two_live_sources",
                    "staging_source_generation_id",
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
        "staging_source_refs": staging_source_refs,
        "protected_live_source_generation_ids": sorted(live_source_ids),
        "live_source_growth": live_source_storage_projection(database_path),
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


def _retention_datetime(value: datetime | str | None) -> datetime:
    if value is None:
        current = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        current = value
    else:
        current = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if current.tzinfo is None or current.utcoffset() is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _retention_root(root: str | Path | None) -> Path:
    if root is None:
        raise StorageGovernanceError("STORAGE_RETENTION_ROOT_REQUIRED")
    raw = Path(root).expanduser()
    if raw.is_symlink():
        raise StorageGovernanceError("STORAGE_RETENTION_ROOT_SYMLINK")
    try:
        resolved = raw.resolve(strict=False)
    except OSError as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_ROOT_INVALID:{type(exc).__name__}") from exc
    if not resolved.is_dir():
        raise StorageGovernanceError("STORAGE_RETENTION_ROOT_INVALID")
    return resolved


def _retention_is_within(path: Path, parent: Path) -> bool:
    try:
        Path(path).resolve(strict=False).relative_to(Path(parent).resolve(strict=False))
    except (OSError, ValueError):
        return False
    return True


def _retention_assert_safe(path: Path, root: Path) -> None:
    raw = Path(os.path.abspath(path))
    try:
        raw_relative = raw.relative_to(root)
    except ValueError as exc:
        raise StorageGovernanceError("STORAGE_RETENTION_PATH_OUTSIDE_ROOT") from exc
    cursor = root
    for part in raw_relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")
    if not _retention_is_within(path, root):
        raise StorageGovernanceError("STORAGE_RETENTION_PATH_OUTSIDE_ROOT")
    try:
        relative = Path(path).resolve(strict=False).relative_to(root.resolve(strict=False))
    except (OSError, ValueError) as exc:
        raise StorageGovernanceError("STORAGE_RETENTION_PATH_OUTSIDE_ROOT") from exc
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")


def _retention_source_roots(root: Path) -> tuple[Path, ...]:
    return tuple(root / relative for relative in RETENTION_ALLOWED_RELATIVE_DIRS)


def _retention_files(root: Path) -> list[Path]:
    result: list[Path] = []
    for source_root in _retention_source_roots(root):
        if source_root.is_symlink():
            raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")
        if not source_root.exists():
            continue
        if not source_root.is_dir():
            raise StorageGovernanceError("STORAGE_RETENTION_SOURCE_ROOT_INVALID")
        for directory, directories, filenames in os.walk(source_root, followlinks=False):
            current = Path(directory)
            _retention_assert_safe(current, root)
            for name in directories:
                candidate = current / name
                if candidate.is_symlink():
                    raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")
            for name in filenames:
                candidate = current / name
                if candidate.is_symlink():
                    raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")
                if name == ".gitkeep" or name.endswith((".tmp", ".part", ".partial")):
                    continue
                _retention_assert_safe(candidate, root)
                if candidate.is_file():
                    result.append(candidate.resolve())
    return sorted(set(result), key=lambda item: item.as_posix().lower())


def _retention_metadata(path: Path, root: Path) -> dict[str, Any]:
    _retention_assert_safe(path, root)
    try:
        info = path.stat()
    except OSError as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_FILE_UNREADABLE:{type(exc).__name__}") from exc
    if not path.is_file():
        raise StorageGovernanceError("STORAGE_RETENTION_NON_REGULAR_FILE")
    return {
        "path": str(path),
        "relative_path": path.relative_to(root).as_posix(),
        "size_bytes": int(info.st_size),
        "mtime_ns": int(info.st_mtime_ns),
        "mtime": datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
        "sha256": _file_sha256(path),
    }


def _retention_running_run(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    if path.is_symlink():
        raise StorageGovernanceError("STORAGE_RETENTION_SYMLINK_REJECTED")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_PROGRESS_INVALID:{type(exc).__name__}") from exc
    if not isinstance(payload, Mapping):
        return None
    status = str(payload.get("status") or payload.get("job_status") or "").strip().upper()
    run_id = str(payload.get("run_id") or "").strip()
    return run_id if status == "RUNNING" and run_id else None


def _retention_matches(path: Path, patterns: Sequence[str], run_ids: Sequence[str]) -> tuple[bool, str | None]:
    relative = path.as_posix().lower()
    name = path.name.lower()
    for pattern in patterns:
        value = str(pattern or "").strip()
        if not value:
            continue
        if value.lower() in relative or fnmatch.fnmatch(relative, value.lower()) or fnmatch.fnmatch(name, value.lower()):
            return True, "PROTECTED_PATTERN"
    for run_id in run_ids:
        value = str(run_id or "").strip()
        if value and value.lower() in relative:
            return True, "PROTECTED_RUN_ID"
    return False, None


def _retention_plan_identity(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": plan.get("schema_version"),
        "root": plan.get("root"),
        "policy": plan.get("policy"),
        "cutoff_hours": plan.get("cutoff_hours"),
        "mode": plan.get("mode"),
        "protected_patterns": sorted(str(item) for item in (plan.get("protected_patterns") or ())),
        "protected_run_ids": sorted(str(item) for item in (plan.get("protected_run_ids") or ())),
        "files": [
            {
                key: item.get(key)
                for key in ("relative_path", "size_bytes", "mtime_ns", "sha256", "protected", "reason")
            }
            for item in (plan.get("files") or ())
            if isinstance(item, Mapping)
        ],
    }


def _retention_with_digest(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("manifest_sha256", None)
    result["manifest_sha256"] = hashlib.sha256(_canonical_json(result)).hexdigest()
    return result


def _retention_atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_EXISTS")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            mode="w",
            encoding="utf-8",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_EXISTS") from exc
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
        temporary.unlink()
        temporary = None
    except StorageGovernanceError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_MANIFEST_WRITE_FAILED:{type(exc).__name__}") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def storage_cleanup_plan(
    feature_store_db: str | Path | None = None,
    *,
    root: str | Path | None = None,
    snapshot_roots: Sequence[str | Path] = (),
    workflow_progress_path: str | Path | None = None,
    cutoff_hours: int | float | None = None,
    keep_days: int | float | None = None,
    policy: str | None = None,
    protected_patterns: Sequence[str] = (),
    protected_run_ids: Sequence[str] = (),
    now: datetime | str | None = None,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build a dry-run plan for the three approved file trees only."""

    database_path = Path(feature_store_db).resolve() if feature_store_db is not None else None
    target_root = _retention_root(root or (database_path.parent if database_path is not None else Path.cwd()))
    if cutoff_hours is None:
        cutoff_hours = RETENTION_DEFAULT_KEEP_DAYS * 24 if keep_days is None else float(keep_days) * 24
    try:
        cutoff = float(cutoff_hours)
    except (TypeError, ValueError):
        raise StorageGovernanceError("STORAGE_RETENTION_CUTOFF_INVALID") from None
    if cutoff < 0 or cutoff > 24 * 36_500:
        raise StorageGovernanceError("STORAGE_RETENTION_CUTOFF_INVALID")
    policy_name = str(policy or "gzip-archive-v1").strip()
    if not policy_name or len(policy_name) > 80 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:+/-]*", policy_name):
        raise StorageGovernanceError("STORAGE_RETENTION_POLICY_INVALID")
    current = _retention_datetime(now)
    recent_cutoff = current - timedelta(hours=RETENTION_RECENT_HOURS)
    age_cutoff = current - timedelta(hours=cutoff)
    patterns = tuple(dict.fromkeys(str(item).strip() for item in protected_patterns if str(item).strip()))
    run_ids = set(str(item).strip() for item in protected_run_ids if str(item).strip())
    progress = Path(workflow_progress_path) if workflow_progress_path is not None else target_root / "state" / "workflow_progress.json"
    running_run_id = _retention_running_run(progress)
    if running_run_id:
        run_ids.add(running_run_id)

    reference_roots = tuple(Path(item).resolve() for item in snapshot_roots) if snapshot_roots else _retention_source_roots(target_root)
    reference_plan = (
        scan_reference_plan(database_path, snapshot_roots=reference_roots)
        if database_path is not None
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

    entries: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for path in _retention_files(target_root):
        entry = _retention_metadata(path, target_root)
        modified = datetime.fromtimestamp(entry["mtime_ns"] / 1_000_000_000, timezone.utc)
        protected, protection_reason = _retention_matches(path, patterns, sorted(run_ids))
        reasons: list[str] = []
        if protected:
            reasons.append(str(protection_reason))
        if modified >= recent_cutoff:
            protected = True
            reasons.append("RECENT_24H")
        if modified > current:
            protected = True
            reasons.append("FUTURE_MTIME")
        if modified > age_cutoff:
            protected = True
            reasons.append("KEEP_CUTOFF")
        entry.update({
            "protected": protected,
            "deletion_allowed": not protected,
            "reason_codes": list(dict.fromkeys(reasons)),
        })
        if protected:
            entry.update({"action": "PROTECT", "reason": reasons[0] if reasons else "PROTECTED"})
        else:
            entry.update({
                "action": "ARCHIVE_GZIP",
                "reason": "OLDER_THAN_CUTOFF",
                "reason_codes": ["OLDER_THAN_CUTOFF", *reasons],
            })
            candidates.append(entry)
        entries.append(entry)

    identity = {
        "schema_version": RETENTION_SCHEMA_VERSION,
        "root": str(target_root),
        "policy": policy_name,
        "cutoff_hours": cutoff,
        "mode": "ARCHIVE_GZIP",
        "protected_patterns": list(patterns),
        "protected_run_ids": sorted(run_ids),
        "files": [
            {
                key: entry.get(key)
                for key in ("relative_path", "size_bytes", "mtime_ns", "sha256", "protected", "reason")
            }
            for entry in entries
        ],
    }
    fingerprint = hashlib.sha256(_canonical_json(identity)).hexdigest()
    plan: dict[str, Any] = {
        "schema_version": RETENTION_SCHEMA_VERSION,
        "status": "DRY_RUN",
        "dry_run": True,
        "deletion_allowed": False,
        "message": "Planning only; no source files or SQLite rows are changed.",
        "generated_at": current.isoformat().replace("+00:00", "Z"),
        "root": str(target_root),
        "policy": policy_name,
        "cutoff_hours": cutoff,
        "recent_protection_hours": RETENTION_RECENT_HOURS,
        "mode": "ARCHIVE_GZIP",
        "archive_root": str(target_root / RETENTION_ARCHIVE_RELATIVE_DIR),
        "workflow_progress_path": str(progress),
        "current_running_run_id": running_run_id,
        "protected_patterns": list(patterns),
        "protected_run_ids": sorted(run_ids),
        "files": entries,
        "candidates": candidates,
        "candidate_count": len(candidates),
        "candidate_bytes": sum(int(item["size_bytes"]) for item in candidates),
        "plan_fingerprint": fingerprint,
        "plan_id": fingerprint[:24],
        "confirmation_token": fingerprint[:24],
        "reference_plan": reference_plan,
    }
    if manifest_path is not None:
        output_raw = Path(manifest_path).expanduser()
        _retention_assert_safe(output_raw.parent, target_root)
        output_path = output_raw.resolve()
        if not _retention_is_within(output_path, target_root):
            raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_PATH_INVALID")
        plan["manifest_path"] = str(output_path)
    plan = _retention_with_digest(plan)
    if manifest_path is not None:
        _retention_atomic_json(output_path, plan)
    return plan


def _retention_load_plan(plan_or_manifest: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(plan_or_manifest, Mapping):
        return dict(plan_or_manifest)
    path = Path(plan_or_manifest).expanduser()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_MANIFEST_INVALID:{type(exc).__name__}") from exc
    if not isinstance(payload, Mapping):
        raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_INVALID")
    return dict(payload)


def _retention_validate_plan(plan: Mapping[str, Any], root: Path, policy: str | None) -> dict[str, Any]:
    if plan.get("schema_version") != RETENTION_SCHEMA_VERSION:
        raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_SCHEMA_INVALID")
    digest = str(plan.get("manifest_sha256") or "")
    if digest:
        unsigned = dict(plan)
        unsigned.pop("manifest_sha256", None)
        actual = hashlib.sha256(_canonical_json(unsigned)).hexdigest()
        if not hmac.compare_digest(digest, actual):
            raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_HASH_MISMATCH")
    if Path(str(plan.get("root") or "")).expanduser().resolve(strict=False) != root:
        raise StorageGovernanceError("STORAGE_RETENTION_ROOT_MISMATCH")
    if policy is not None and str(policy).strip() != str(plan.get("policy") or ""):
        raise StorageGovernanceError("STORAGE_RETENTION_POLICY_MISMATCH")
    fingerprint = str(plan.get("plan_fingerprint") or "")
    if not fingerprint or not hmac.compare_digest(fingerprint, hashlib.sha256(_canonical_json(_retention_plan_identity(plan))).hexdigest()):
        raise StorageGovernanceError("STORAGE_RETENTION_PLAN_HASH_MISMATCH")
    if str(plan.get("mode") or "") != "ARCHIVE_GZIP":
        raise StorageGovernanceError("STORAGE_RETENTION_MODE_INVALID")
    return dict(plan)


def _retention_candidate_path(root: Path, item: Mapping[str, Any]) -> tuple[Path, Path]:
    relative_raw = str(item.get("relative_path") or "")
    relative = Path(relative_raw)
    if not relative_raw or relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise StorageGovernanceError("STORAGE_RETENTION_PATH_TRAVERSAL")
    source = (root / relative).resolve(strict=False)
    listed = Path(str(item.get("path") or "")).expanduser().resolve(strict=False)
    if listed != source:
        raise StorageGovernanceError("STORAGE_RETENTION_PLAN_PATH_MISMATCH")
    if not any(_retention_is_within(source, allowed) for allowed in _retention_source_roots(root)):
        raise StorageGovernanceError("STORAGE_RETENTION_PATH_NOT_ALLOWED")
    _retention_assert_safe(source, root)
    return source, relative


def _retention_recheck(path: Path, item: Mapping[str, Any], root: Path) -> None:
    _retention_assert_safe(path, root)
    try:
        info = path.stat()
    except OSError as exc:
        raise StorageGovernanceError("STORAGE_RETENTION_PLAN_DRIFT") from exc
    if int(info.st_size) != int(item.get("size_bytes")) or int(info.st_mtime_ns) != int(item.get("mtime_ns")):
        raise StorageGovernanceError("STORAGE_RETENTION_PLAN_DRIFT")
    if not hmac.compare_digest(_file_sha256(path), str(item.get("sha256") or "")):
        raise StorageGovernanceError("STORAGE_RETENTION_PLAN_DRIFT")


def _retention_archive_verify(path: Path, expected_size: int, expected_sha256: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise StorageGovernanceError("STORAGE_RETENTION_ARCHIVE_MISSING")
    digest = hashlib.sha256()
    size = 0
    try:
        with gzip.open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise StorageGovernanceError("STORAGE_RETENTION_ARCHIVE_INVALID") from exc
    restored_hash = digest.hexdigest()
    if size != int(expected_size) or not hmac.compare_digest(restored_hash, str(expected_sha256)):
        raise StorageGovernanceError("STORAGE_RETENTION_ARCHIVE_HASH_MISMATCH")
    return {
        "compressed_size_bytes": int(path.stat().st_size),
        "compressed_sha256": _file_sha256(path),
        "restored_size_bytes": size,
        "restored_sha256": restored_hash,
    }


def _retention_archive_file(source: Path, destination: Path, root: Path, *, expected_size: int, expected_sha256: str) -> dict[str, Any]:
    _retention_assert_safe(destination.parent, root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _retention_assert_safe(destination.parent, root)
    if destination.exists():
        return {"already_archived": True, **_retention_archive_verify(destination, expected_size, expected_sha256)}
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, mode="wb", delete=False) as handle:
            temporary = Path(handle.name)
            # Level 1 keeps the maintenance window short.  The project JSON
            # payloads still shrink to roughly 12--22% in production samples,
            # while higher levels add CPU time without changing recoverability.
            with source.open("rb") as source_handle, gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=handle,
                compresslevel=1,
                mtime=0,
            ) as compressed:
                shutil.copyfileobj(source_handle, compressed, length=1024 * 1024)
            handle.flush()
            os.fsync(handle.fileno())
        info = _retention_archive_verify(temporary, expected_size, expected_sha256)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            info = {"already_archived": True, **_retention_archive_verify(destination, expected_size, expected_sha256)}
        else:
            temporary.unlink()
            temporary = None
            try:
                directory_fd = os.open(destination.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                # Directory fsync is not available on every Windows filesystem;
                # the file itself was already flushed and hash-verified.
                pass
        return info
    except StorageGovernanceError:
        raise
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise StorageGovernanceError(f"STORAGE_RETENTION_ARCHIVE_FAILED:{type(exc).__name__}") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def storage_cleanup_execute(
    plan_or_manifest: Mapping[str, Any] | str | Path,
    *,
    root: str | Path,
    policy: str | None = None,
    retention_policy: str | None = None,
    confirmation_token: str | None = None,
    confirm_token: str | None = None,
    confirmation_manifest: Mapping[str, Any] | str | Path | None = None,
    audit_manifest_path: str | Path | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Execute a plan after explicit plan-id confirmation."""

    target_root = _retention_root(root)
    if not isinstance(plan_or_manifest, Mapping):
        plan_raw = Path(plan_or_manifest).expanduser()
        _retention_assert_safe(plan_raw.parent, target_root)
        plan_path = plan_raw.resolve()
        if not _retention_is_within(plan_path, target_root):
            raise StorageGovernanceError("STORAGE_RETENTION_MANIFEST_PATH_INVALID")
    selected_policy = retention_policy if retention_policy is not None else policy
    plan = _retention_validate_plan(_retention_load_plan(plan_or_manifest), target_root, selected_policy)
    token = confirmation_token if confirmation_token is not None else confirm_token
    expected_token = str(plan.get("plan_id") or "")
    confirmed = bool(token and hmac.compare_digest(str(token), expected_token))
    if confirmation_manifest is not None:
        if not isinstance(confirmation_manifest, Mapping):
            confirmation_raw = Path(confirmation_manifest).expanduser()
            _retention_assert_safe(confirmation_raw.parent, target_root)
            confirmation_path = confirmation_raw.resolve()
            if not _retention_is_within(confirmation_path, target_root):
                raise StorageGovernanceError("STORAGE_RETENTION_CONFIRMATION_PATH_INVALID")
        confirmation = _retention_load_plan(confirmation_manifest)
        confirmed = confirmed or (
            str(confirmation.get("plan_id") or "") == expected_token
            and bool(confirmation.get("confirmed"))
        )
    if not confirmed:
        raise StorageGovernanceError("STORAGE_RETENTION_CONFIRMATION_REQUIRED")

    audit_path = (
        Path(audit_manifest_path).expanduser().resolve()
        if audit_manifest_path is not None
        else target_root / RETENTION_ARCHIVE_RELATIVE_DIR / expected_token / "audit.json"
    )
    if not _retention_is_within(audit_path, target_root):
        raise StorageGovernanceError("STORAGE_RETENTION_AUDIT_PATH_INVALID")
    for allowed in _retention_source_roots(target_root):
        if _retention_is_within(audit_path, allowed):
            raise StorageGovernanceError("STORAGE_RETENTION_AUDIT_PATH_INVALID")
    _retention_assert_safe(audit_path.parent, target_root)
    if audit_path.is_file():
        try:
            previous = json.loads(audit_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise StorageGovernanceError("STORAGE_RETENTION_AUDIT_INVALID") from exc
        if isinstance(previous, Mapping) and str(previous.get("plan_id") or "") == expected_token:
            return {**dict(previous), "status": "IDEMPOTENT", "idempotent": True}
        raise StorageGovernanceError("STORAGE_RETENTION_AUDIT_EXISTS")

    entries = plan.get("candidates")
    if not isinstance(entries, list):
        raise StorageGovernanceError("STORAGE_RETENTION_CANDIDATES_INVALID")
    execution_now = _retention_datetime(now)
    preflight: list[tuple[dict[str, Any], Path, Path, bool]] = []
    archive_root = target_root / RETENTION_ARCHIVE_RELATIVE_DIR / expected_token
    _retention_assert_safe(target_root / "storage", target_root)
    for raw in entries:
        if not isinstance(raw, Mapping) or bool(raw.get("protected")) or str(raw.get("action") or "") != "ARCHIVE_GZIP":
            raise StorageGovernanceError("STORAGE_RETENTION_CANDIDATE_INVALID")
        source, relative = _retention_candidate_path(target_root, raw)
        destination = archive_root.joinpath(*relative.parts).with_name(relative.name + ".gz")
        if not _retention_is_within(destination, archive_root):
            raise StorageGovernanceError("STORAGE_RETENTION_PATH_TRAVERSAL")
        _retention_assert_safe(destination.parent, target_root)
        if source.exists():
            _retention_recheck(source, raw, target_root)
            if datetime.fromtimestamp(int(raw["mtime_ns"]) / 1_000_000_000, timezone.utc) >= execution_now - timedelta(hours=RETENTION_RECENT_HOURS):
                raise StorageGovernanceError("STORAGE_RETENTION_CANDIDATE_NOW_PROTECTED")
            preflight.append((dict(raw), source, destination, False))
        elif destination.is_file():
            _retention_archive_verify(destination, int(raw["size_bytes"]), str(raw["sha256"]))
            preflight.append((dict(raw), source, destination, True))
        else:
            raise StorageGovernanceError("STORAGE_RETENTION_PLAN_DRIFT")

    items: list[dict[str, Any]] = []
    for raw, source, destination, already_archived in preflight:
        if not already_archived:
            _retention_recheck(source, raw, target_root)
            archive_info = _retention_archive_file(source, destination, target_root, expected_size=int(raw["size_bytes"]), expected_sha256=str(raw["sha256"]))
            _retention_recheck(source, raw, target_root)
            try:
                source.unlink()
            except OSError as exc:
                raise StorageGovernanceError(f"STORAGE_RETENTION_SOURCE_DELETE_FAILED:{type(exc).__name__}") from exc
        else:
            archive_info = {"already_archived": True, **_retention_archive_verify(destination, int(raw["size_bytes"]), str(raw["sha256"]))}
        items.append({
            **raw,
            "action": "ARCHIVED_GZIP",
            "archived_path": str(destination),
            "recovery_path": str(destination),
            "deleted": True,
            "raw_size_bytes": int(raw["size_bytes"]),
            "raw_sha256": str(raw["sha256"]),
            **archive_info,
        })

    audit_payload: dict[str, Any] = {
        "schema_version": RETENTION_SCHEMA_VERSION,
        "kind": "retention-execution-audit",
        "status": "EXECUTED",
        "plan_id": expected_token,
        "root": str(target_root),
        "policy": plan.get("policy"),
        "cutoff_hours": plan.get("cutoff_hours"),
        "started_at": execution_now.isoformat().replace("+00:00", "Z"),
        "completed_at": _retention_datetime(None).isoformat().replace("+00:00", "Z"),
        "items": items,
        "archive_count": len(items),
        "deleted_count": sum(1 for item in items if item.get("deleted")),
        "audit_manifest_path": str(audit_path),
    }
    audit_payload = _retention_with_digest(audit_payload)
    _retention_atomic_json(audit_path, audit_payload)
    return audit_payload


execute_storage_cleanup = storage_cleanup_execute

__all__ = [
    "DISK_FREE_BLOCK_PERCENT",
    "DISK_FREE_CRITICAL_PERCENT",
    "DISK_FREE_FULL_REBUILD_MIN_BYTES",
    "DISK_FREE_WARNING_PERCENT",
    "DiskWatermark",
    "SQLiteHealth",
    "RETENTION_ALLOWED_RELATIVE_DIRS",
    "RETENTION_ARCHIVE_RELATIVE_DIR",
    "RETENTION_DEFAULT_KEEP_DAYS",
    "RETENTION_RECENT_HOURS",
    "RETENTION_SCHEMA_VERSION",
    "STORAGE_SCHEMA_VERSION",
    "StorageGovernanceError",
    "backup_sqlite",
    "disk_watermark",
    "evaluate_disk_watermark",
    "inspect_sqlite",
    "live_source_storage_projection",
    "scan_reference_plan",
    "sqlite_integrity_check",
    "storage_audit",
    "storage_cleanup_execute",
    "storage_cleanup_plan",
    "execute_storage_cleanup",
]
