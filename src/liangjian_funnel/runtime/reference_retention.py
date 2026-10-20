"""Reference-aware retention *planning*, with no mutation or execution API.

Only declared research outputs/checkpoints are in scope. An incomplete file
or reference census keeps every candidate: absence of evidence is not evidence
of an unreferenced file. The old gzip/archive executor is deliberately unused.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

SCHEMA = "reference-retention/1"
_ALLOWED = frozenset({"outputs/research", "state/research_checkpoints"})
_AUDIT_KEY = re.compile(r"audit|publication_receipt|evidence_ref", re.I)
_FILE_KEY = re.compile(r"(?:path|file|manifest|snapshot_ref|archive_ref)$", re.I)


def plan_reference_retention(
    files: Iterable[Mapping[str, Any]], *, references: Mapping[str, Iterable[str]],
    permanent_roots: Iterable[str], now: datetime, keep_days: int = 30,
    census_complete: bool, unknowns: Iterable[str] = (),
) -> dict[str, Any]:
    """Pure graph closure; never touches a filesystem or a database."""
    if isinstance(keep_days, bool) or not isinstance(keep_days, int) or keep_days < 1:
        raise ValueError("RETENTION_KEEP_DAYS_INVALID")
    if now.tzinfo is None:
        raise ValueError("RETENTION_TIME_MUST_BE_AWARE")
    rows = [dict(row) for row in files]
    inventory = {row["path"]: row for row in rows}
    if len(inventory) != len(rows):
        raise ValueError("RETENTION_DUPLICATE_PATH")
    edges = {str(k): set(v) for k, v in references.items()}
    unknown = set(unknowns)
    protected = set(permanent_roots)
    stack = list(protected)
    while stack:
        parent = stack.pop()
        for child in edges.get(parent, ()):
            if child not in protected:
                protected.add(child)
                stack.append(child)
    # An edge to a missing file may be an external permanent original, but its
    # identity is unresolved here. It cannot authorize deleting other files.
    for source, targets in edges.items():
        for target in targets:
            if target not in inventory:
                unknown.add("UNRESOLVED_REFERENCE:" + source + "->" + target)
    complete = bool(census_complete) and not unknown
    result = []
    for row in sorted(rows, key=lambda x: x["path"]):
        stamp = datetime.fromisoformat(str(row["mtime"]).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("RETENTION_MTIME_MUST_BE_AWARE")
        age_seconds = (now - stamp).total_seconds()
        if row["path"] in protected:
            reason = "PERMANENT_PLAN_OR_AUDIT_REFERENCE"
        elif not complete:
            reason = "KEEP_REFERENCE_CENSUS_INCOMPLETE"
        elif not row.get("understood", False):
            reason = "KEEP_UNKNOWN_FORMAT"
        elif age_seconds < keep_days * 86400:
            reason = "KEEP_WITHIN_RETENTION"
        else:
            reason = "ELIGIBLE_UNREFERENCED_EXPIRED_DRY_RUN_ONLY"
        result.append({**row, "age_days": round(age_seconds / 86400, 3), "reason_code": reason})
    candidates = [x for x in result if x["reason_code"].startswith("ELIGIBLE_")]
    return {
        "schema": SCHEMA, "mode": "DRY_RUN_ONLY", "execution_available": False,
        "observed_at": now.isoformat(), "keep_days": keep_days,
        "reference_census_complete": complete, "unknowns": sorted(unknown),
        "files": result, "file_count": len(result),
        "permanently_protected_count": sum(x["path"] in protected for x in result),
        "candidate_count": len(candidates), "candidate_bytes": sum(x["bytes"] for x in candidates),
        "deleted_files": 0, "archived_files": 0,
    }


def scan_reference_retention(
    root: Path, policy: Mapping[str, Any], *, reference_records: Iterable[Any],
    reference_census_complete: bool, now: datetime,
) -> dict[str, Any]:
    """Bounded read-only collector. Callers supply existing plan/audit records.

    A skipped large/changed/unparseable JSON, symlink, unknown format, missing
    declared root or unresolved path makes the entire plan non-actionable.
    No directories are created; no environment or service/store is instantiated.
    """
    if policy.get("schema") != SCHEMA or policy.get("dry_run_only") is not True:
        raise ValueError("RETENTION_DRY_RUN_POLICY_REQUIRED")
    roots = policy.get("managed_roots")
    if not isinstance(roots, list) or not roots or set(roots) - _ALLOWED:
        raise ValueError("RETENTION_ROOT_NOT_ALLOWED")
    root = root.resolve(strict=True)
    limit = int(policy.get("max_files", 10000))
    per_file = int(policy.get("max_json_bytes", 1048576))
    budget = int(policy.get("max_total_read_bytes", 67108864))
    if not (1 <= limit <= 10000 and 1 <= per_file <= 1048576 and 1 <= budget <= 67108864):
        raise ValueError("RETENTION_READ_BUDGET_INVALID")
    unknowns, rows, bodies = [], [], {}
    aliases = defaultdict(set)
    consumed = 0
    for relative in roots:
        base = root / relative
        if not base.is_dir() or base.is_symlink():
            unknowns.append("ROOT_MISSING_OR_SYMLINK:" + relative)
            continue
        # No followed symlinks, including nested directories.
        import os
        for directory, dirs, names in os.walk(base, followlinks=False):
            for name in list(dirs):
                child = Path(directory) / name
                if child.is_symlink():
                    dirs.remove(name)
                    unknowns.append("SYMLINK:" + child.relative_to(root).as_posix())
            for name in sorted(names):
                path = Path(directory) / name
                relative_path = path.relative_to(root).as_posix()
                if len(rows) >= limit:
                    unknowns.append("FILE_LIMIT_REACHED")
                    break
                if path.is_symlink() or not path.resolve().is_relative_to(root):
                    unknowns.append("SYMLINK_OR_OUTSIDE:" + relative_path)
                    continue
                before = path.stat()
                row = {"path": relative_path, "bytes": before.st_size,
                       "mtime": datetime.fromtimestamp(before.st_mtime, timezone.utc).isoformat(),
                       "understood": False}
                rows.append(row)
                aliases[str(path)].add(relative_path)
                aliases[relative_path].add(relative_path)
                aliases[path.name].add(relative_path)
                if path.suffix != ".json" or before.st_size > per_file or consumed + before.st_size > budget:
                    unknowns.append("FORMAT_OR_READ_BUDGET:" + relative_path)
                    continue
                try:
                    data = path.read_bytes()
                    after = path.stat()
                    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                        raise ValueError("changed")
                    consumed += len(data)
                    body = json.loads(data)
                    if not isinstance(body, (dict, list)):
                        raise ValueError("not structured JSON")
                    row.update(understood=True, sha256=hashlib.sha256(data).hexdigest())
                    aliases[row["sha256"]].add(relative_path)
                    bodies[relative_path] = body
                except (OSError, UnicodeError, ValueError):
                    unknowns.append("UNREADABLE_OR_CHANGED:" + relative_path)
            if len(rows) >= limit:
                break
    edges, permanent = defaultdict(set), set()

    def walk(value: Any, source: str, *, key: str = "", audit: bool = False) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                is_audit = audit or bool(_AUDIT_KEY.search(str(k)))
                if is_audit and source in bodies:
                    permanent.add(source)
                walk(v, source, key=str(k), audit=is_audit)
        elif isinstance(value, list):
            for child in value:
                walk(child, source, key=key, audit=audit)
        elif isinstance(value, str):
            # DB payload_json and similar frozen JSON are traversed, not merely
            # searched with substring matching (which loses nested references).
            if value[:1] in ("{", "[") and len(value.encode()) <= per_file:
                try:
                    walk(json.loads(value), source, key=key, audit=audit)
                except (ValueError, RecursionError):
                    unknowns.append("UNPARSEABLE_NESTED_RECORD:" + source)
                return
            targets = aliases.get(value.replace("\\", "/"), set())
            if targets:
                edges[source].update(targets)
                if audit or source.startswith("RECORD:"):
                    permanent.update(targets)
            elif _FILE_KEY.search(key) and ("/" in value or value.endswith(".json")):
                unknowns.append("UNRESOLVED_PATH:" + source + ":" + key)

    for path, body in bodies.items():
        walk(body, path)
    for index, record in enumerate(reference_records):
        source = f"RECORD:{index}"
        permanent.add(source)
        walk(record, source, audit=True)
    result = plan_reference_retention(
        rows, references=edges, permanent_roots=permanent, now=now,
        keep_days=policy.get("keep_days", 30),
        census_complete=reference_census_complete, unknowns=unknowns,
    )
    result.update(managed_roots=roots, bytes_read=consumed,
                  policy_sha256=hashlib.sha256(json.dumps(dict(policy), sort_keys=True).encode()).hexdigest())
    return result
