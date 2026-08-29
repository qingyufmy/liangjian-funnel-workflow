#!/usr/bin/env python3
"""Generate a fact-backed engineering/business acceptance report.

The report is deliberately an *observer*.  It reads persisted progress,
research results, SQLite metadata and existing replay/benchmark evidence.  It
never calls a provider, model or trading endpoint and it only writes the two
requested report files.  Missing evidence is represented as ``UNKNOWN`` or
``PENDING``; it is never promoted to a passing check.

Typical use::

    python scripts/generate_acceptance_report.py --root . --output-dir outputs/acceptance

The optional directory arguments are useful on a deployment whose paths were
overridden through ``.env``.  They are paths to *read*, except for
``--output-dir``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ACCEPTANCE_SCHEMA_VERSION = "liangjian-acceptance/1.0.0"
ENGINEERING_PASS = "ENGINEERING_PASS"
ENGINEERING_FAIL = "ENGINEERING_FAIL"
PENDING_BUSINESS_ACCEPTANCE = "PENDING_BUSINESS_ACCEPTANCE"
VERDICTS = (ENGINEERING_PASS, ENGINEERING_FAIL, PENDING_BUSINESS_ACCEPTANCE)
STAGES = ("A1", "A2", "A3")
PRIMARY_LANE = "lane_1"
REPLAY_MINIMUM_DAYS = 10
BROKER_GOLD_MINIMUM_MONTHS = 4
_DATE_IN_ID = re.compile(r"(?<!\d)(20\d\d-\d\d-\d\d)(?!\d)")
_MONTH_FILE = re.compile(r"^(20\d\d-\d\d)(?:[-_.].*)?\.(?:json|csv)$", re.IGNORECASE)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)


class AcceptanceReportError(RuntimeError):
    """Raised when report arguments are invalid or output cannot be written."""


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _upper(value: Any) -> str:
    return (_text(value) or "").upper().replace("-", "_").replace(" ", "_")


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _sequence(value: Any) -> Sequence[Any] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return None


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat().replace("+00:00", "Z") if parsed else None


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _hash_projection(value: Any, root: Path) -> Any:
    """Remove deployment-root variance without removing evidence fields."""

    if isinstance(value, Mapping):
        return {str(key): _hash_projection(item, root) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_hash_projection(item, root) for item in value]
    if isinstance(value, str):
        root_text = str(root.resolve())
        return value.replace(root_text, "<ROOT>").replace(root_text.replace("\\", "/"), "<ROOT>")
    return value


def _read_json(path: Path) -> tuple[Mapping[str, Any] | None, str | None]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}:{str(exc)[:160]}"
    if not isinstance(raw, Mapping):
        return None, "JSON_OBJECT_REQUIRED"
    return raw, None


def _relative(root: Path, path: Path | str | None) -> str | None:
    if path is None:
        return None
    try:
        candidate = Path(path).resolve()
        return candidate.relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        # A declared external path is evidence, not an instruction.  Keep a
        # bounded display value without leaking an absolute deployment path.
        return Path(str(path)).name or None


def _existing(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.is_file():
            return path.resolve()
    return None


def _existing_dir(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.is_dir():
            return path.resolve()
    return None


def _json_files(directory: Path | None, *, suffixes: tuple[str, ...] = (".json",)) -> list[Path]:
    if directory is None or not directory.is_dir():
        return []
    return sorted(
        (path for path in directory.iterdir() if path.is_file() and path.suffix.lower() in suffixes),
        key=lambda path: path.name,
    )


def _safe_status(value: Any) -> str:
    return _upper(value) or "UNKNOWN"


def _status_is_failed(value: Any) -> bool:
    status = _safe_status(value)
    return status in {"FAILED", "FAIL", "MODEL_FAILED", "EXECUTION_FAILURE", "ERROR", "INVALID"} or status.endswith("_FAILED")


def _status_is_terminal(value: Any) -> bool:
    return _safe_status(value) in {
        "READY",
        "READY_DEGRADED",
        "SUCCEEDED",
        "SUCCESS",
        "PUBLISHED",
        "VALIDATED",
        "VALIDATED_NO_OPPORTUNITY",
        "VALIDATED_NO_ACTION",
        "VALIDATED_NO_SETUP",
        "DEGRADED_UNDERFILLED_DATA_GAP",
        "VALIDATED_UNDERFILLED_MARKET",
        "COMPLETED",
        "TERMINAL",
    }


def _reason_codes(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, str):
            values_to_add: Iterable[Any] = (value,)
        else:
            values_to_add = _sequence(value) or ()
        for item in values_to_add:
            token = _upper(item)
            if token and token not in result:
                result.append(token)
    return sorted(result)


def _has_data_gap(reasons: Sequence[str], sufficiency: str) -> bool:
    haystack = set(reasons)
    return bool(
        # UNKNOWN means the fact was not recorded at all; it is not evidence
        # of a recoverable A2 data gate.  Treating it as a gap would allow a
        # crashed A1/model run to masquerade as a degraded-but-valid result.
        sufficiency in {"INSUFFICIENT", "PARTIAL"}
        or any(
            "DATA_GAP" in item
            or "DATA_INSUFFICIENT" in item
            or "COVERAGE_INSUFFICIENT" in item
            or item in {"A2_FACTS_UNAVAILABLE", "A2_DATA_UNAVAILABLE", "UPSTREAM_DATA_INSUFFICIENT"}
            for item in haystack
        )
    )


def _pool_count(output: Mapping[str, Any], fields: Sequence[str]) -> int | None:
    for field in fields:
        value = output.get(field)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return len(value)
    return None


def _stage_summary(raw: Mapping[str, Any] | None, stage_name: str) -> dict[str, Any]:
    raw = raw or {}
    output = _mapping(raw.get("output")) or {}
    nested = _mapping(raw.get("outcome_v3")) or _mapping(raw.get("outcome_v2")) or _mapping(raw.get("outcome")) or {}
    counts = _mapping(nested.get("counts")) or _mapping(raw.get("counts")) or {}
    selected = next(
        (
            value
            for value in (
                _int(counts.get("selected")),
                _int(counts.get("output")),
                _int(raw.get("output_count")),
                _int(raw.get("selected_count")),
                _pool_count(output, {"A1": ("active_research_pool", "monitor_pool"), "A2": ("focus_pool", "watch_only_pool"), "A3": ("core_watch_pool", "secondary_watch_pool")}.get(stage_name, ())),
                len(raw.get("symbols")) if isinstance(raw.get("symbols"), list) else None,
            )
            if value is not None
        ),
        None,
    )
    input_count = next(
        (
            value
            for value in (
                _int(counts.get("input")),
                _int(counts.get("evaluated")),
                _int(raw.get("input_count")),
                _int(raw.get("evaluated_count")),
            )
            if value is not None
        ),
        None,
    )
    status = _safe_status(raw.get("status") or nested.get("legacy_status") or nested.get("quality_state"))
    quality = _upper(nested.get("quality_state"))
    if quality not in {"VALIDATED", "DEGRADED", "BLOCKED", "FAILED", "CANCELLED"}:
        quality = "UNKNOWN"
    sufficiency = _upper(nested.get("data_sufficiency_state") or raw.get("data_sufficiency_state"))
    if sufficiency not in {"SUFFICIENT", "PARTIAL", "INSUFFICIENT", "NOT_APPLICABLE", "UNKNOWN"}:
        sufficiency = "UNKNOWN"
    opportunity = _upper(nested.get("opportunity_state") or raw.get("opportunity_state"))
    if opportunity not in {"PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"}:
        opportunity = ""
    reasons = _reason_codes(nested.get("reason_codes"), raw.get("reason_codes"), (_mapping(output.get("analysis_summary")) or {}).get("reason_codes"))
    if not opportunity:
        if _has_data_gap(reasons, sufficiency):
            opportunity = "UNKNOWN"
        elif selected is not None and selected > 0:
            opportunity = "PRESENT"
        elif any(token in reasons for token in {"UPSTREAM_POOL_EMPTY", "NO_OPPORTUNITY", "NO_ACTION"}):
            opportunity = "ABSENT"
        elif stage_name == "A3" and status in {"NOT_RUN", "BLOCKED", "CANCELLED"}:
            opportunity = "NOT_APPLICABLE"
        else:
            opportunity = "UNKNOWN"
    actionability = _upper(nested.get("actionability_state") or raw.get("actionability_state"))
    if actionability not in {"ACTIONABLE", "NO_ACTION", "UNKNOWN", "NOT_APPLICABLE"}:
        actionability = "ACTIONABLE" if stage_name == "A3" and selected and selected > 0 else ("NO_ACTION" if opportunity == "ABSENT" else "UNKNOWN")
    return {
        "stage": stage_name,
        "status": status,
        "quality_state": quality,
        "data_sufficiency_state": sufficiency,
        "opportunity_state": opportunity,
        "actionability_state": actionability,
        "input_count": input_count,
        "selected_count": selected,
        "reason_codes": reasons,
        "output_pools": {field: len(output[field]) for field in ("active_research_pool", "monitor_pool", "focus_pool", "watch_only_pool", "core_watch_pool", "secondary_watch_pool", "rejected_candidates") if isinstance(output.get(field), list)},
        "source_snapshot_id": _text(raw.get("snapshot_id")),
    }


def _stage_entries(lane: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    lane = lane or {}
    result: dict[str, Mapping[str, Any]] = {}
    raw_stages = _sequence(lane.get("stages")) or ()
    for raw in raw_stages:
        mapping = _mapping(raw)
        if not mapping:
            continue
        name = _upper(mapping.get("stage"))
        if name in STAGES and name not in result:
            result[name] = mapping
    return result


def _load_progress(root: Path) -> tuple[dict[str, Any], Path | None, list[dict[str, str]]]:
    candidates = [root / "state" / "workflow_progress.json", root / "workflow_progress.json", root / "outputs" / "workflow_progress.json"]
    path = _existing(candidates)
    if path is None:
        return {"status": "UNKNOWN", "availability": "UNKNOWN"}, None, [{"code": "PROGRESS_NOT_FOUND", "detail": "workflow_progress.json is not available"}]
    raw, error = _read_json(path)
    if error or raw is None:
        return {"status": "UNKNOWN", "availability": "INVALID", "path": _relative(root, path)}, path, [{"code": "PROGRESS_INVALID", "detail": error or "unknown"}]
    raw = dict(raw)
    raw.setdefault("status", raw.get("job_status") or raw.get("state") or "UNKNOWN")
    raw["availability"] = "AVAILABLE"
    raw["path"] = _relative(root, path)
    return raw, path, []


def _record_sort_key(record: Mapping[str, Any], path: Path) -> tuple[float, float, str, str]:
    timestamp = None
    for value in (record.get("generated_at"), record.get("updated_at"), record.get("created_at"), record.get("as_of"), (_mapping(record.get("snapshot")) or {}).get("as_of")):
        timestamp = _parse_datetime(value)
        if timestamp:
            break
    # Older run summaries did not persist ``created_at``.  Their run id still
    # carries a point-in-time date, so use it as a deterministic fallback
    # instead of allowing the lexical order of ``morning``/``close`` to pick
    # an older run over a newer one.
    explicit_timestamp = timestamp is not None
    if timestamp is None:
        date_token = _trade_date(record, path)
        try:
            timestamp = datetime.combine(date.fromisoformat(date_token), datetime.min.time(), tzinfo=timezone.utc) if date_token else None
        except ValueError:
            timestamp = None
    # If two legacy summaries share only a trade date, file modification time
    # is the only persisted ordering signal (and is stable for an unchanged
    # checkout).  It is deliberately a secondary key so an explicit
    # generated/updated timestamp always wins.
    try:
        mtime = path.stat().st_mtime if not explicit_timestamp else 0.0
    except OSError:
        mtime = 0.0
    return (timestamp.timestamp() if timestamp else float("-inf"), mtime, _text(record.get("run_id") or record.get("id")) or "", path.name)


def _run_dirs(root: Path, explicit: str | None) -> list[Path]:
    if explicit:
        return [Path(explicit).expanduser().resolve()]
    result: list[Path] = []
    for path in (root / "outputs" / "runs", root / "outputs" / "results", root / "results" / "runs", root / "results"):
        if path.is_dir() and path.resolve() not in result:
            result.append(path.resolve())
    return result


def _audit_dirs(root: Path, explicit: str | None) -> list[Path]:
    if explicit:
        return [Path(explicit).expanduser().resolve()]
    result: list[Path] = []
    for path in (root / "outputs" / "research", root / "outputs" / "results", root / "results" / "research", root / "results"):
        if path.is_dir() and path.resolve() not in result:
            result.append(path.resolve())
    return result


def _is_comparison_summary(record: Mapping[str, Any]) -> bool:
    run_id = _text(record.get("run_id") or record.get("id")) or ""
    return bool(
        _upper(record.get("run_role")) == "COMPARISON"
        or _text(record.get("parent_run_id"))
        or _text(record.get("comparison_request_id"))
        or "-comparison-" in run_id.lower()
    )


def _comparison_requests(root: Path, parent_run_id: str | None) -> list[dict[str, Any]]:
    if not parent_run_id:
        return []
    directory = root / "outputs" / "comparison_requests"
    result: list[dict[str, Any]] = []
    for path in _json_files(directory):
        raw, error = _read_json(path)
        if error or raw is None or _text(raw.get("parent_run_id")) != parent_run_id:
            continue
        result.append({**dict(raw), "source": _relative(root, path)})
    return sorted(result, key=lambda item: (_text(item.get("updated_at")) or "", _text(item.get("request_id")) or ""))


def _child_lane_evidence(child: Mapping[str, Any], parent_run_id: str) -> list[dict[str, Any]]:
    child_run_id = _text(child.get("run_id") or child.get("id"))
    outcome = _mapping(child.get("outcome_v3")) or _mapping(child.get("outcome_v2")) or _mapping(child.get("outcome")) or {}
    raw_lanes = _sequence(outcome.get("lanes")) or _sequence(child.get("lanes")) or ()
    publication = dict(_mapping(child.get("plan_publication")) or {})
    result: list[dict[str, Any]] = []
    for raw in raw_lanes:
        if not isinstance(raw, Mapping):
            continue
        result.append({
            **dict(raw),
            "parent_run_id": parent_run_id,
            "child_run_id": child_run_id,
            "comparison_request_id": _text(child.get("comparison_request_id")) or parent_run_id,
            "child_run_role": _upper(child.get("run_role")) or "COMPARISON",
            "child_plan_publication": publication,
        })
    return result


def _load_runs(root: Path, runs_dir: str | None, audit_dir: str | None) -> tuple[Mapping[str, Any] | None, Path | None, list[Mapping[str, Any]], list[Path], list[dict[str, str]]]:
    records: list[tuple[Mapping[str, Any], Path]] = []
    errors: list[dict[str, str]] = []
    for directory in _run_dirs(root, runs_dir):
        for path in _json_files(directory):
            raw, error = _read_json(path)
            if error or raw is None:
                errors.append({"code": "RUN_RESULT_INVALID", "detail": f"{_relative(root, path)}:{error or 'unknown'}"})
                continue
            records.append((raw, path))
    audits: list[tuple[Mapping[str, Any], Path]] = []
    for directory in _audit_dirs(root, audit_dir):
        for path in _json_files(directory):
            if "lane" not in path.stem.lower() and not _sequence((_read_json(path)[0] or {}).get("stages")):
                continue
            raw, error = _read_json(path)
            if error or raw is None:
                continue
            if _sequence(raw.get("stages")):
                audits.append((raw, path))
    records.sort(key=lambda item: _record_sort_key(item[0], item[1]), reverse=True)
    if records:
        # A comparison child is normally newer than its parent.  It is audit
        # evidence, never the authoritative run that controls publication.
        # Prefer the newest non-comparison summary and fall back only for old
        # workspaces that contain child artifacts alone.
        summary, summary_path = next(
            (item for item in records if not _is_comparison_summary(item[0])),
            records[0],
        )
        summary = dict(summary)
        run_id = _text(summary.get("run_id") or summary.get("id"))
        matching: list[Mapping[str, Any]] = []
        audit_paths: list[Path] = []
        for audit, path in audits:
            audit_run = _text(audit.get("run_id"))
            lane = _text(audit.get("lane") or audit.get("lane_id"))
            if (run_id and (audit_run == run_id or run_id in path.stem)) or (not run_id and audit_run is None):
                matching.append(audit)
                audit_paths.append(path)
            elif run_id and lane and path.stem.endswith(f"_{lane}") and run_id in path.stem:
                matching.append(audit)
                audit_paths.append(path)
        for child, child_path in records:
            if not _is_comparison_summary(child):
                continue
            child_parent = _text(child.get("parent_run_id") or child.get("comparison_request_id"))
            if not run_id or child_parent != run_id:
                continue
            child_lanes = _child_lane_evidence(child, run_id)
            matching.extend(child_lanes)
            audit_paths.extend([child_path] * len(child_lanes))
        summary["linked_comparison_requests"] = _comparison_requests(root, run_id)
        return summary, summary_path, matching, audit_paths, errors
    if audits:
        # A lane audit can be the only durable artifact after a crash.  Group
        # by declared run id, otherwise use the common filename prefix.
        grouped: dict[str, list[tuple[Mapping[str, Any], Path]]] = defaultdict(list)
        for audit, path in audits:
            lane = _text(audit.get("lane") or audit.get("lane_id"))
            key = _text(audit.get("run_id")) or re.sub(r"_lane_[123]$", "", path.stem, flags=re.IGNORECASE)
            grouped[key].append((audit, path))
        key = sorted(grouped, key=lambda item: max(_record_sort_key(row, path) for row, path in grouped[item]), reverse=True)[0]
        group = grouped[key]
        synthetic = {"run_id": key, "status": next((_text(item.get("status")) for item, _ in group if item.get("status")), "UNKNOWN"), "slot": "UNKNOWN"}
        return synthetic, None, [item for item, _ in group], [path for _, path in group], errors
    return None, None, [], [], errors


def _extract_lanes(summary: Mapping[str, Any] | None, audits: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    summary = summary or {}
    raw_lanes: list[Mapping[str, Any]] = []
    outcome = _mapping(summary.get("outcome_v3")) or _mapping(summary.get("outcome_v2")) or _mapping(summary.get("outcome"))
    for container in (outcome, summary):
        values = _sequence(container.get("lanes")) if container else None
        if values:
            raw_lanes.extend(item for item in values if isinstance(item, Mapping))
    raw_lanes.extend(audits)
    by_lane: dict[str, dict[str, Any]] = {}
    for raw in raw_lanes:
        lane_id = _text(raw.get("lane") or raw.get("lane_id")) or "UNKNOWN"
        existing = by_lane.get(lane_id, {})
        entry = dict(existing)
        entry.update({key: value for key, value in raw.items() if key not in {"stages"} or not existing.get("stages")})
        if _sequence(raw.get("stages")):
            entry["stages"] = raw["stages"]
        by_lane[lane_id] = entry
    return [by_lane[key] for key in sorted(by_lane)]


def _run_evidence(root: Path, summary: Mapping[str, Any] | None, summary_path: Path | None, audits: Sequence[Mapping[str, Any]], audit_paths: Sequence[Path], progress: Mapping[str, Any]) -> dict[str, Any]:
    summary = summary or {}
    outcome = _mapping(summary.get("outcome_v3")) or _mapping(summary.get("outcome_v2")) or _mapping(summary.get("outcome")) or _mapping(progress.get("outcome_v3")) or {}
    primary_raw = outcome.get("primary_lane_ids") if outcome else summary.get("primary_lane_ids")
    primary_ids = tuple(str(item) for item in (_sequence(primary_raw) or ((primary_raw,) if isinstance(primary_raw, str) else (PRIMARY_LANE,))) if _text(item)) or (PRIMARY_LANE,)
    lanes = _extract_lanes(summary, audits)
    by_lane = {str(item.get("lane") or item.get("lane_id")): item for item in lanes}
    stage_source: dict[str, Mapping[str, Any]] = {}
    for lane_id in primary_ids:
        stage_source.update(_stage_entries(by_lane.get(lane_id)))
    if not stage_source and lanes:
        stage_source.update(_stage_entries(lanes[0]))
    stages = {stage: _stage_summary(stage_source.get(stage), stage) for stage in STAGES}
    lane_output: list[dict[str, Any]] = []
    for lane in lanes:
        lane_id = _text(lane.get("lane") or lane.get("lane_id")) or "UNKNOWN"
        lane_status = _safe_status(lane.get("status") or lane.get("job_status") or lane.get("quality_state"))
        lane_output.append({
            "lane_id": lane_id,
            "model": _text(lane.get("model")),
            "status": lane_status,
            "is_primary": lane_id in primary_ids,
            "parent_run_id": _text(lane.get("parent_run_id")),
            "child_run_id": _text(lane.get("child_run_id")),
            "comparison_request_id": _text(lane.get("comparison_request_id")),
            "child_run_role": _upper(lane.get("child_run_role")) or None,
            "child_plan_publication": dict(_mapping(lane.get("child_plan_publication")) or {}),
            "path": _relative(root, next((audit_paths[index] for index, audit in enumerate(audits) if (_text(audit.get("lane") or audit.get("lane_id")) == lane_id)), None)),
            "stages": {stage: _stage_summary(raw, stage) for stage, raw in _stage_entries(lane).items()},
        })
    run_status = _safe_status(summary.get("status") or outcome.get("legacy_status") or outcome.get("job_status") or progress.get("status"))
    publication = _upper(outcome.get("publication_state") or summary.get("publication_state")) or "UNKNOWN"
    quality = _upper(outcome.get("quality_state") or summary.get("quality_state")) or "UNKNOWN"
    return {
        "availability": "AVAILABLE" if summary else "UNKNOWN",
        "run_id": _text(summary.get("run_id") or summary.get("id")),
        "slot": _upper(summary.get("slot")) or "UNKNOWN",
        "status": run_status,
        "quality_state": quality,
        "publication_state": publication,
        "outcome_schema_version": _text(outcome.get("schema_version")),
        "source": _relative(root, summary_path),
        "primary_lane_ids": list(primary_ids),
        "lanes": lane_output,
        "stages": stages,
        "snapshot": dict(_mapping(summary.get("snapshot")) or {}),
        "plan_publication": dict(_mapping(summary.get("plan_publication")) or {}),
        "comparison_requests": [dict(item) for item in (_sequence(summary.get("linked_comparison_requests")) or ()) if isinstance(item, Mapping)],
        "test_only": bool(summary.get("test_only") or summary.get("mode") == "TEST_ONLY"),
    }


def _load_sqlite_rows(path: Path, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(sql, params).fetchall()]
    except (sqlite3.Error, OSError):
        return []
    finally:
        if connection is not None:
            connection.close()


def _table_names(path: Path) -> set[str]:
    return {str(row.get("name")) for row in _load_sqlite_rows(path, "SELECT name FROM sqlite_master WHERE type='table'") if row.get("name")}


def _feature_db_path(root: Path, feature_db_arg: str | None) -> Path | None:
    return Path(feature_db_arg).expanduser().resolve() if feature_db_arg else _existing((root / "storage" / "features" / "research_feature_store.sqlite3", root / "storage" / "features" / "research_feature_store.db", root / "research_feature_store.sqlite3"))


def _feature_evidence(root: Path, feature_db_arg: str | None, run_id: str | None) -> dict[str, Any]:
    path = _feature_db_path(root, feature_db_arg)
    base: dict[str, Any] = {"path": _relative(root, path), "availability": "UNKNOWN", "status": "UNKNOWN", "generation_count": None, "binding_count": None, "active": None, "run_binding": None, "isolation": "UNKNOWN"}
    if path is None or not path.is_file():
        base["reason_code"] = "FEATURE_STORE_NOT_FOUND"
        return base
    tables = _table_names(path)
    base["availability"] = "AVAILABLE"
    required = {"feature_generations", "active_feature_generations", "run_feature_bindings"}
    if not required.issubset(tables):
        base.update({"status": "UNKNOWN", "reason_code": "FEATURE_LIFECYCLE_SCHEMA_NOT_AVAILABLE", "tables": sorted(tables)})
        return base
    generations = _load_sqlite_rows(path, "SELECT generation_id,domain,status,purpose,as_of,created_at,activation_eligible FROM feature_generations ORDER BY created_at,generation_id")
    active = _load_sqlite_rows(path, "SELECT domain,generation_id,activated_at,previous_generation_id FROM active_feature_generations ORDER BY domain")
    bindings = _load_sqlite_rows(path, "SELECT run_id,domain,generation_id,contract_hash,bound_at FROM run_feature_bindings ORDER BY run_id,domain")
    generation_by_id = {str(row.get("generation_id")): row for row in generations}
    active_ids = {str(row.get("generation_id")) for row in active if row.get("generation_id")}
    orphan_active = sorted(item for item in active_ids if item not in generation_by_id)
    active_run_scoped = sorted(
        item for item in active_ids if _upper(generation_by_id.get(item, {}).get("purpose")) in {"RUN_SNAPSHOT", "HISTORICAL_REPLAY"}
    )
    selected_binding = next((row for row in bindings if run_id and str(row.get("run_id")) == run_id), None)
    bound_generation = generation_by_id.get(str(selected_binding.get("generation_id"))) if selected_binding else None
    run_scoped_binding = bool(bound_generation and _upper(bound_generation.get("purpose")) in {"RUN_SNAPSHOT", "HISTORICAL_REPLAY"})
    if orphan_active or active_run_scoped:
        isolation = "FAIL"
        status = "FAIL"
    elif selected_binding and run_scoped_binding:
        isolation = "PASS" if str(selected_binding.get("generation_id")) not in active_ids else "FAIL"
        status = "PASS" if isolation == "PASS" else "FAIL"
    elif active and generations:
        isolation = "UNKNOWN"
        status = "PASS"
    else:
        isolation = "UNKNOWN"
        status = "UNKNOWN"
    base.update({
        "status": status,
        "generation_count": len(generations),
        "binding_count": len(bindings),
        "active": active,
        "run_binding": selected_binding,
        "run_generation": bound_generation,
        "isolation": isolation,
        "orphan_active_generation_ids": orphan_active,
        "active_run_scoped_generation_ids": active_run_scoped,
        "generation_status_counts": {status_name: sum(_upper(row.get("status")) == status_name for row in generations) for status_name in ("STAGING", "VALIDATED", "SEALED", "FAILED")},
    })
    return base


def _database_paths(root: Path, feature_path: str | None) -> list[Path]:
    paths = [
        root / "state" / "workflow.sqlite3",
        Path(feature_path).expanduser().resolve() if feature_path else root / "storage" / "features" / "research_feature_store.sqlite3",
        root / "storage" / "facts" / "market_fact_cache.sqlite3",
        root / "storage" / "minute" / "minute_bars.sqlite3",
    ]
    return list(dict.fromkeys(path.resolve() for path in paths if path.is_file()))


def _storage_evidence(root: Path, feature_path: str | None) -> dict[str, Any]:
    try:
        # Running the script directly from a source checkout should behave
        # like the installed console command.  This only adjusts import
        # resolution; it does not read configuration or write package files.
        source_dir = Path(__file__).resolve().parents[1] / "src"
        if source_dir.is_dir() and str(source_dir) not in sys.path:
            sys.path.insert(0, str(source_dir))
        from liangjian_funnel.runtime.storage_governance import storage_audit

        databases = _database_paths(root, feature_path)
        resolved_feature_db = _feature_db_path(root, feature_path)
        snapshot_roots = tuple(path for path in (root / "storage" / "snapshots", root / "outputs" / "runs") if path.is_dir())
        raw = storage_audit(
            root,
            database_paths=databases,
            feature_store_db=resolved_feature_db,
            snapshot_roots=snapshot_roots,
        )
        disk = _mapping(raw.get("disk")) or {}
        dbs = _sequence(raw.get("databases")) or ()
        unhealthy = [item for item in dbs if isinstance(item, Mapping) and item.get("exists") and not item.get("healthy")]
        audit_status = _upper(raw.get("status")) or "UNKNOWN"
        status = "FAIL" if unhealthy or audit_status == "BLOCKED" else ("PASS" if audit_status in {"OK", "WARNING", "CRITICAL"} else "UNKNOWN")
        # CRITICAL explicitly means heavy/full writes are unsafe; keep it in
        # the evidence while marking this acceptance as not currently safe.
        if audit_status == "CRITICAL":
            status = "FAIL"
        return {
            "availability": "AVAILABLE",
            "status": status,
            "audit_status": audit_status,
            "disk": dict(disk),
            "databases": [
                {key: item.get(key) for key in ("path", "exists", "size_bytes", "sha256", "integrity_check", "healthy", "error")}
                for item in dbs if isinstance(item, Mapping)
            ],
            "reference": {
                "active_count": len(_sequence((_mapping(raw.get("reference_plan")) or {}).get("active")) or ()),
                "previous_count": len(_sequence((_mapping(raw.get("reference_plan")) or {}).get("previous")) or ()),
                "run_bound_count": len(_sequence((_mapping(raw.get("reference_plan")) or {}).get("run_bound")) or ()),
                "candidate_count": _int((_mapping(raw.get("cleanup")) or {}).get("candidate_count")),
            },
        }
    except Exception as exc:  # observer must still produce a useful report
        return {"availability": "UNKNOWN", "status": "UNKNOWN", "reason_code": "STORAGE_AUDIT_UNAVAILABLE", "detail": f"{type(exc).__name__}:{str(exc)[:160]}"}


def _evaluation_candidates(root: Path) -> list[Path]:
    result: list[Path] = []
    for directory in (root / "outputs" / "evaluation", root / "results" / "evaluation", root / "evaluation"):
        result.extend(_json_files(directory))
    return sorted(set(result), key=lambda path: path.name)


def _trade_date(record: Mapping[str, Any], path: Path | None = None) -> str | None:
    for value in (record.get("trade_date"), (_mapping(record.get("snapshot")) or {}).get("trade_date"), record.get("as_of"), (_mapping(record.get("snapshot")) or {}).get("as_of")):
        text = _text(value)
        if text:
            match = _DATE_IN_ID.search(text)
            if match:
                return match.group(1)
    if path:
        match = _DATE_IN_ID.search(path.name)
        if match:
            return match.group(1)
    match = _DATE_IN_ID.search(_text(record.get("run_id")) or "")
    return match.group(1) if match else None


def _replay_evidence(root: Path, summary_records: Sequence[Mapping[str, Any]], summary_paths: Sequence[Path]) -> dict[str, Any]:
    candidates = _evaluation_candidates(root)
    chosen: tuple[Mapping[str, Any], Path] | None = None
    for path in candidates:
        raw, error = _read_json(path)
        if raw is not None and error is None:
            if chosen is None or _record_sort_key(raw, path) > _record_sort_key(chosen[0], chosen[1]):
                chosen = (raw, path)
    if chosen:
        raw, path = chosen
        summary = _mapping(raw.get("summary")) or {}
        days = _int(summary.get("independent_trading_days") or raw.get("independent_trading_days"))
        raw_status = _safe_status(raw.get("status"))
        if raw_status == "READY" and days is not None and days >= REPLAY_MINIMUM_DAYS:
            status = "PASS"
        elif raw_status in {"BLOCKED_REPLAY_WINDOW_INSUFFICIENT", "BLOCKED_REPLAY_NOT_CONFIGURED", "UNKNOWN"} or (days is not None and days < REPLAY_MINIMUM_DAYS):
            status = "PENDING"
        elif raw_status.startswith("BLOCKED") or raw_status in {"FAILED", "INVALID"}:
            status = "FAIL"
        else:
            status = "UNKNOWN"
        return {"availability": "AVAILABLE", "status": status, "source": _relative(root, path), "raw_status": raw_status, "independent_trading_days": days, "required_days": REPLAY_MINIMUM_DAYS, "terminal_days": _int(summary.get("terminal_days")), "blocking_reasons": list(raw.get("blocking_reasons") or ()) if isinstance(raw.get("blocking_reasons"), list) else []}
    replay_records: list[tuple[str, Mapping[str, Any]]] = []
    for index, record in enumerate(summary_records):
        run_id = _text(record.get("run_id") or record.get("id")) or ""
        replay_flag = bool(record.get("historical_replay") or record.get("replay") or record.get("replay_window") or record.get("mode") == "REPLAY" or "replay" in run_id.lower())
        if replay_flag:
            trade_date = _trade_date(record, summary_paths[index] if index < len(summary_paths) else None)
            if trade_date:
                replay_records.append((trade_date, record))
    dates = sorted({item[0] for item in replay_records})
    failed = sorted(_text(record.get("run_id") or record.get("id")) or "UNKNOWN" for _, record in replay_records if _status_is_failed(record.get("status")))
    if not replay_records:
        return {"availability": "UNKNOWN", "status": "PENDING", "source": None, "independent_trading_days": 0, "required_days": REPLAY_MINIMUM_DAYS, "terminal_days": 0, "blocking_reasons": ["REPLAY_EVIDENCE_NOT_FOUND"]}
    if failed:
        status = "FAIL"
    elif len(dates) >= REPLAY_MINIMUM_DAYS and all(_status_is_terminal(record.get("status")) for _, record in replay_records):
        status = "PASS"
    else:
        status = "PENDING"
    return {"availability": "AVAILABLE", "status": status, "source": "run_summaries", "independent_trading_days": len(dates), "required_days": REPLAY_MINIMUM_DAYS, "terminal_days": sum(_status_is_terminal(record.get("status")) for _, record in replay_records), "trade_dates": dates, "failed_runs": failed, "blocking_reasons": [] if not failed else ["REPLAY_RUN_FAILED"]}


def _broker_dirs(root: Path, explicit: str | None) -> list[Path]:
    if explicit:
        return [Path(explicit).expanduser().resolve()]
    return [path.resolve() for path in (root / "storage" / "benchmarks" / "broker_gold", root / "broker_gold", root / "storage" / "benchmarks") if path.is_dir()]


def _broker_gold_evidence(root: Path, explicit: str | None) -> dict[str, Any]:
    files: list[Path] = []
    for directory in _broker_dirs(root, explicit):
        files.extend(path for path in directory.iterdir() if path.is_file() and path.suffix.lower() in {".json", ".csv"})
    files = sorted(set(files), key=lambda path: path.name)
    months: set[str] = set()
    invalid: list[str] = []
    record_counts: dict[str, int] = {}
    for path in files:
        match = _MONTH_FILE.match(path.name)
        month: str | None = match.group(1) if match else None
        try:
            if path.suffix.lower() == ".json":
                raw = json.loads(path.read_text(encoding="utf-8"))
                rows = raw.get("records") if isinstance(raw, Mapping) else raw
                rows = rows if isinstance(rows, list) else []
            else:
                with path.open("r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
            if not rows:
                invalid.append(f"{path.name}:EMPTY")
                continue
            if month is None:
                month = _text(rows[0].get("month")) if isinstance(rows[0], Mapping) else None
            if not month or not re.fullmatch(r"20\d\d-\d\d", month):
                invalid.append(f"{path.name}:MONTH_INVALID")
                continue
            months.add(month)
            record_counts[month] = record_counts.get(month, 0) + len(rows)
        except (OSError, UnicodeError, json.JSONDecodeError, csv.Error) as exc:
            invalid.append(f"{path.name}:{type(exc).__name__}")
    if invalid and not months:
        status = "FAIL" if files else "PENDING"
    elif len(months) >= BROKER_GOLD_MINIMUM_MONTHS:
        status = "PASS" if not invalid else "FAIL"
    else:
        status = "PENDING"
    return {"availability": "AVAILABLE" if files else "UNKNOWN", "status": status, "required_months": BROKER_GOLD_MINIMUM_MONTHS, "month_count": len(months), "months": sorted(months), "file_count": len(files), "record_counts": dict(sorted(record_counts.items())), "invalid_files": invalid, "source": [_relative(root, path) for path in files]}


def _execution_plans(root: Path, run_id: str | None) -> list[dict[str, Any]]:
    path = root / "state" / "workflow.sqlite3"
    rows = _load_sqlite_rows(path, "SELECT plan_id,lane_id,symbol,status,payload_json,created_at,updated_at FROM execution_plans ORDER BY created_at,plan_id")
    result: list[dict[str, Any]] = []
    for row in rows:
        payload: Mapping[str, Any] = {}
        try:
            decoded = json.loads(str(row.get("payload_json") or "{}"))
            payload = decoded if isinstance(decoded, Mapping) else {}
        except (TypeError, ValueError):
            pass
        if run_id and payload.get("run_id") and str(payload.get("run_id")) != run_id:
            continue
        result.append({"plan_id": row.get("plan_id"), "lane_id": row.get("lane_id"), "symbol": row.get("symbol"), "status": row.get("status"), "test_only": bool(payload.get("test_only") or payload.get("mode") == "TEST_ONLY"), "created_at": row.get("created_at")})
    return result


def _nonempty_plan_evidence(root: Path, run: Mapping[str, Any]) -> dict[str, Any]:
    plans: list[dict[str, Any]] = []
    publication = run.get("plan_publication") if isinstance(run.get("plan_publication"), Mapping) else {}
    for key in ("created", "activated"):
        values = _sequence(publication.get(key)) or ()
        for item in values:
            mapping = dict(item) if isinstance(item, Mapping) else {"plan_id": str(item)}
            mapping.setdefault("test_only", bool(run.get("test_only")))
            plans.append(mapping)
    plans.extend(_execution_plans(root, _text(run.get("run_id"))))
    natural = [item for item in plans if not bool(item.get("test_only")) and _text(item.get("symbol") or item.get("plan_id"))]
    test_only = [item for item in plans if bool(item.get("test_only"))]
    if natural:
        status = "PASS"
    else:
        status = "PENDING"
    return {"status": status, "count": len(natural), "test_only_count": len(test_only), "plans": [{key: item.get(key) for key in ("plan_id", "symbol", "lane_id", "status", "test_only", "created_at")} for item in plans], "reason_code": None if natural else ("ONLY_TEST_PLAN" if test_only else "NONEMPTY_A3_PLAN_NOT_FOUND")}


def _contract_checks(run: Mapping[str, Any], progress: Mapping[str, Any], feature: Mapping[str, Any], storage: Mapping[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    run_status = _safe_status(run.get("status"))
    primary_ids = set(str(item) for item in run.get("primary_lane_ids", ()))
    lanes = run.get("lanes") if isinstance(run.get("lanes"), list) else []
    primary_lanes = [lane for lane in lanes if isinstance(lane, Mapping) and str(lane.get("lane_id")) in primary_ids]
    if run.get("availability") != "AVAILABLE":
        checks.append({"id": "RUN_RESULT", "status": "UNKNOWN", "detail": "No persisted research result was found"})
    elif _status_is_failed(run_status) or (run_status in {"BLOCKED", "CANCELLED"} and not _has_data_gap(run.get("stages", {}).get("A2", {}).get("reason_codes", []), _text(run.get("stages", {}).get("A2", {}).get("data_sufficiency_state")) or "UNKNOWN")):
        checks.append({"id": "RUN_RESULT", "status": "FAIL", "detail": f"primary run status is {run_status}"})
    else:
        checks.append({"id": "RUN_RESULT", "status": "PASS", "detail": f"primary run status is {run_status}"})
    if not primary_lanes:
        checks.append({"id": "PRIMARY_LANE", "status": "UNKNOWN", "detail": "Primary lane evidence is absent"})
    elif any(_status_is_failed(lane.get("status")) for lane in primary_lanes):
        checks.append({"id": "PRIMARY_LANE", "status": "FAIL", "detail": "primary lane has an explicit failure"})
    else:
        checks.append({"id": "PRIMARY_LANE", "status": "PASS", "detail": f"primary lanes: {sorted(primary_ids)}"})
    stages = run.get("stages") if isinstance(run.get("stages"), Mapping) else {}
    missing = [stage for stage in STAGES if not isinstance(stages.get(stage), Mapping) or stages[stage].get("status") == "UNKNOWN"]
    a2 = stages.get("A2") if isinstance(stages.get("A2"), Mapping) else {}
    a3 = stages.get("A3") if isinstance(stages.get("A3"), Mapping) else {}
    a2_gap = _has_data_gap(a2.get("reason_codes", []), _text(a2.get("data_sufficiency_state")) or "UNKNOWN")
    a3_actionable = _upper(a3.get("actionability_state")) == "ACTIONABLE" or (_upper(a3.get("opportunity_state")) == "PRESENT" and (_int(a3.get("selected_count")) or 0) > 0)
    if a3_actionable and a2_gap:
        checks.append({"id": "STAGE_CHAIN", "status": "FAIL", "detail": "A3 is actionable while A2 data is insufficient"})
    elif missing:
        checks.append({"id": "STAGE_CHAIN", "status": "UNKNOWN", "detail": f"missing stage evidence: {','.join(missing)}"})
    else:
        checks.append({"id": "STAGE_CHAIN", "status": "PASS", "detail": "A1 -> A2 -> A3 semantics are represented"})
    schema_versions: list[str] = []
    if _text(run.get("outcome_schema_version")):
        schema_versions.append(str(run["outcome_schema_version"]))
    for container in (run, progress.get("outcome_v3"), progress.get("outcome_v2")):
        if isinstance(container, Mapping):
            value = _text(container.get("schema_version"))
            if value:
                schema_versions.append(value)
    if "research-outcome/3.0.0" in schema_versions:
        checks.append({"id": "OUTCOME_CONTRACT", "status": "PASS", "detail": "research-outcome/3.0.0 observed"})
    elif schema_versions:
        checks.append({"id": "OUTCOME_CONTRACT", "status": "UNKNOWN", "detail": f"observed versions: {sorted(set(schema_versions))}"})
    else:
        checks.append({"id": "OUTCOME_CONTRACT", "status": "UNKNOWN", "detail": "no canonical outcome schema in persisted evidence"})
    optional = [lane for lane in lanes if isinstance(lane, Mapping) and str(lane.get("lane_id")) not in primary_ids]
    requests = [item for item in run.get("comparison_requests", ()) if isinstance(item, Mapping)] if isinstance(run.get("comparison_requests"), list) else []
    bad_links: list[str] = []
    for lane in optional:
        lane_id = str(lane.get("lane_id") or "UNKNOWN")
        child_publication = _mapping(lane.get("child_plan_publication")) or {}
        created = _sequence(child_publication.get("created")) or ()
        activated = _sequence(child_publication.get("activated")) or ()
        if (
            _text(lane.get("parent_run_id")) != _text(run.get("run_id"))
            or not _text(lane.get("child_run_id"))
            or _text(lane.get("child_run_id")) == _text(run.get("run_id"))
            or bool(created)
            or bool(activated)
            or (child_publication and _upper(child_publication.get("publication")) != "COMPARISON_ONLY")
        ):
            bad_links.append(lane_id)
    if bad_links:
        comparison_status = "FAIL"
        comparison_detail = f"comparison isolation violation: {sorted(bad_links)}"
    elif optional or requests:
        comparison_status = "PASS"
        request_states = sorted({_safe_status(item.get("status")) for item in requests})
        comparison_detail = f"optional comparison is isolated; request states={request_states or ['CHILD_ONLY']}"
    else:
        comparison_status = "UNKNOWN"
        comparison_detail = "no comparison request or child-run evidence"
    checks.append({"id": "PRIMARY_COMPARISON_ISOLATION", "status": comparison_status, "detail": comparison_detail})
    checks.append({"id": "FEATURE_GENERATION_ISOLATION", "status": "FAIL" if feature.get("isolation") == "FAIL" else ("PASS" if feature.get("isolation") == "PASS" else "UNKNOWN"), "detail": str(feature.get("reason_code") or feature.get("isolation") or "feature lifecycle evidence unavailable")})
    checks.append({"id": "STORAGE_SAFETY", "status": "FAIL" if storage.get("status") == "FAIL" else ("PASS" if storage.get("status") == "PASS" else "UNKNOWN"), "detail": str(storage.get("audit_status") or storage.get("reason_code") or "storage audit unavailable")})
    return checks


def _choose_generated_at(explicit: str | None, progress: Mapping[str, Any], run: Mapping[str, Any], replay: Mapping[str, Any]) -> str:
    if explicit:
        parsed = _parse_datetime(explicit)
        if parsed is None:
            raise AcceptanceReportError("--generated-at must be a timezone-aware ISO-8601 timestamp")
        return parsed.isoformat().replace("+00:00", "Z")
    timestamps = []
    for container in (progress, run, replay):
        for key in ("generated_at", "updated_at", "created_at", "as_of", "cutoff"):
            parsed = _parse_datetime(container.get(key)) if isinstance(container, Mapping) else None
            if parsed:
                timestamps.append(parsed)
    return (max(timestamps) if timestamps else datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")


def build_acceptance_report(
    root: str | Path,
    *,
    output_dir: str | Path | None = None,
    runs_dir: str | None = None,
    audit_dir: str | None = None,
    feature_db: str | None = None,
    broker_gold_dir: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Read the workspace and return a deterministic acceptance payload."""

    root_path = Path(root).expanduser().resolve()
    if not root_path.is_dir():
        raise AcceptanceReportError(f"root directory does not exist: {root_path}")
    progress, progress_path, progress_errors = _load_progress(root_path)
    summary, summary_path, audits, audit_paths, run_errors = _load_runs(root_path, runs_dir, audit_dir)
    run = _run_evidence(root_path, summary, summary_path, audits, audit_paths, progress)
    feature = _feature_evidence(root_path, feature_db, run.get("run_id"))
    storage = _storage_evidence(root_path, feature_db)
    # Collect all run summaries for the offline replay count, not merely the
    # latest run used for the main report.
    all_records: list[Mapping[str, Any]] = []
    all_paths: list[Path] = []
    for directory in _run_dirs(root_path, runs_dir):
        for path in _json_files(directory):
            raw, error = _read_json(path)
            if raw is not None and error is None:
                all_records.append(raw)
                all_paths.append(path)
    replay = _replay_evidence(root_path, all_records, all_paths)
    gold = _broker_gold_evidence(root_path, broker_gold_dir)
    plan = _nonempty_plan_evidence(root_path, run)
    checks = _contract_checks(run, progress, feature, storage)
    hard_failures = [check for check in checks if check.get("status") == "FAIL"]
    pending_business = []
    if replay.get("status") != "PASS":
        pending_business.append("REPLAY_WINDOW_BELOW_10_DAYS" if replay.get("status") == "PENDING" else "REPLAY_ACCEPTANCE_FAILED")
    if gold.get("status") != "PASS":
        pending_business.append("BROKER_GOLD_BELOW_4_MONTHS" if gold.get("status") == "PENDING" else "BROKER_GOLD_DATA_INVALID")
    if plan.get("status") != "PASS":
        pending_business.append("NATURAL_NONEMPTY_A3_PLAN_NOT_OBSERVED")
    unknown_evidence = [check.get("id") for check in checks if check.get("status") == "UNKNOWN"]
    if hard_failures:
        verdict = ENGINEERING_FAIL
    elif pending_business or unknown_evidence or progress_errors or run_errors:
        verdict = PENDING_BUSINESS_ACCEPTANCE
    else:
        verdict = ENGINEERING_PASS
    blockers: list[dict[str, Any]] = []
    for check in hard_failures:
        blockers.append({"code": f"ENGINEERING_{check.get('id')}", "severity": "HARD", "detail": check.get("detail")})
    for code in pending_business:
        blockers.append({"code": code, "severity": "BUSINESS", "detail": "required real-world acceptance evidence is not complete"})
    for code in unknown_evidence:
        blockers.append({"code": f"EVIDENCE_{code}", "severity": "EVIDENCE", "detail": "persisted fact is unavailable; this is not treated as PASS"})
    blockers.extend({"code": item["code"], "severity": "EVIDENCE", "detail": item["detail"]} for item in [*progress_errors, *run_errors])
    report: dict[str, Any] = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "generated_at": _choose_generated_at(generated_at, progress, run, replay),
        "verdict": verdict,
        "root": str(root_path),
        "read_only": True,
        "network_used": False,
        "trading_connected": False,
        "blockers": blockers,
        "engineering_contract": {"status": "FAIL" if hard_failures else ("UNKNOWN" if unknown_evidence else "PASS"), "checks": checks},
        "run": run,
        "stages": run.get("stages", {}),
        "models": {"primary": [lane for lane in run.get("lanes", []) if lane.get("is_primary")], "comparison": [lane for lane in run.get("lanes", []) if not lane.get("is_primary")]},
        "feature_generation": feature,
        "storage": storage,
        "replay": replay,
        "broker_gold": gold,
        "nonempty_a3_plan": plan,
    }
    # Keep the payload hash independent of its own hash and of absolute paths;
    # this makes two reports from the same fixture byte-for-byte comparable
    # when --generated-at is fixed.
    hash_payload = _hash_projection(report, root_path)
    hash_payload["root"] = "<ROOT>"
    report["report_hash"] = _canonical_hash(hash_payload)
    return report


def _md_value(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return str(value)


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# 工作流自动验收报告",
        "",
        f"- schema: `{_md_value(report.get('schema_version'))}`",
        f"- generated_at: `{_md_value(report.get('generated_at'))}`",
        f"- 判定: **{_md_value(report.get('verdict'))}**",
        "- 本报告只读取已落盘事实；未连接模型、数据供应商或真实交易。",
        "",
        "## 工程合同",
        "",
        "| 检查项 | 状态 | 证据 |",
        "| --- | --- | --- |",
    ]
    contract = report.get("engineering_contract") if isinstance(report.get("engineering_contract"), Mapping) else {}
    for check in contract.get("checks", ()) if isinstance(contract.get("checks"), list) else ():
        if isinstance(check, Mapping):
            lines.append(f"| `{_md_value(check.get('id'))}` | **{_md_value(check.get('status'))}** | {_md_value(check.get('detail'))} |")
    run = report.get("run") if isinstance(report.get("run"), Mapping) else {}
    lines.extend(["", "## 运行与模型", "", f"- run_id: `{_md_value(run.get('run_id'))}`", f"- slot/status: `{_md_value(run.get('slot'))}` / `{_md_value(run.get('status'))}`", "", "| lane | 模型 | 主/对比 | 状态 | 证据文件 |", "| --- | --- | --- | --- | --- |"])
    for lane in run.get("lanes", ()) if isinstance(run.get("lanes"), list) else ():
        if isinstance(lane, Mapping):
            lines.append(f"| `{_md_value(lane.get('lane_id'))}` | `{_md_value(lane.get('model'))}` | {'主' if lane.get('is_primary') else '对比'} | {_md_value(lane.get('status'))} | `{_md_value(lane.get('path'))}` |")
    lines.extend(["", "## A1 → A2 → A3", "", "| 阶段 | 状态 | 数据充分性 | 机会语义 | 可执行性 | 输入 | 选中 | 原因 |", "| --- | --- | --- | --- | --- | ---: | ---: | --- |"])
    stages = report.get("stages") if isinstance(report.get("stages"), Mapping) else {}
    for stage in STAGES:
        item = stages.get(stage) if isinstance(stages.get(stage), Mapping) else {}
        lines.append(f"| {stage} | {_md_value(item.get('status'))} | {_md_value(item.get('data_sufficiency_state'))} | {_md_value(item.get('opportunity_state'))} | {_md_value(item.get('actionability_state'))} | {_md_value(item.get('input_count'))} | {_md_value(item.get('selected_count'))} | `{_md_value(item.get('reason_codes'))}` |")
    lines.extend(["", "## 特征代际与存储", "", f"- 特征代际：**{_md_value((report.get('feature_generation') or {}).get('isolation') if isinstance(report.get('feature_generation'), Mapping) else None)}**；active: `{_md_value((report.get('feature_generation') or {}).get('active'))}`", f"- 存储水位：**{_md_value((report.get('storage') or {}).get('status') if isinstance(report.get('storage'), Mapping) else None)}**；disk: `{_md_value((report.get('storage') or {}).get('disk') if isinstance(report.get('storage'), Mapping) else None)}`", "", "## 业务验收门", "", "| 项目 | 状态 | 事实 |", "| --- | --- | --- |"])
    replay = report.get("replay") if isinstance(report.get("replay"), Mapping) else {}
    gold = report.get("broker_gold") if isinstance(report.get("broker_gold"), Mapping) else {}
    plan = report.get("nonempty_a3_plan") if isinstance(report.get("nonempty_a3_plan"), Mapping) else {}
    lines.append(f"| 10 日点时回放 | **{_md_value(replay.get('status'))}** | {_md_value(replay.get('independent_trading_days'))} / {_md_value(replay.get('required_days'))}，来源 `{_md_value(replay.get('source'))}` |")
    lines.append(f"| 券商月度金股 | **{_md_value(gold.get('status'))}** | {_md_value(gold.get('month_count'))} / {_md_value(gold.get('required_months'))} 月，来源 `{_md_value(gold.get('source'))}` |")
    lines.append(f"| 自然非空 A3 计划 | **{_md_value(plan.get('status'))}** | {_md_value(plan.get('count'))} 个；TEST_ONLY {_md_value(plan.get('test_only_count'))} 个 |")
    lines.extend(["", "## 阻断项", ""])
    blockers = report.get("blockers") if isinstance(report.get("blockers"), list) else []
    if blockers:
        for blocker in blockers:
            if isinstance(blocker, Mapping):
                lines.append(f"- `{_md_value(blocker.get('code'))}` ({_md_value(blocker.get('severity'))})：{_md_value(blocker.get('detail'))}")
    else:
        lines.append("- 无")
    lines.extend(["", f"> report_hash: `{_md_value(report.get('report_hash'))}`", ""])
    return "\n".join(lines)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def write_acceptance_report(report: Mapping[str, Any], output_dir: str | Path) -> tuple[Path, Path]:
    """Write JSON and Markdown atomically; no other project files are changed."""

    target = Path(output_dir).expanduser().resolve()
    json_path = target / "acceptance-report.json"
    markdown_path = target / "acceptance-report.md"
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    _atomic_write(json_path, encoded)
    _atomic_write(markdown_path, render_markdown(report))
    return json_path, markdown_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="workflow project root")
    parser.add_argument("--output-dir", default="outputs/acceptance", help="directory for acceptance-report.json/.md")
    parser.add_argument("--runs-dir", help="optional run-summary directory")
    parser.add_argument("--audit-dir", help="optional lane-audit directory")
    parser.add_argument("--feature-db", help="optional feature store SQLite path")
    parser.add_argument("--broker-gold-dir", help="optional broker-gold directory")
    parser.add_argument("--generated-at", help="timezone-aware ISO timestamp for deterministic reports")
    args = parser.parse_args(argv)
    try:
        report = build_acceptance_report(args.root, output_dir=args.output_dir, runs_dir=args.runs_dir, audit_dir=args.audit_dir, feature_db=args.feature_db, broker_gold_dir=args.broker_gold_dir, generated_at=args.generated_at)
        json_path, markdown_path = write_acceptance_report(report, args.output_dir)
    except AcceptanceReportError as exc:
        print(json.dumps({"status": "ERROR", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps({"verdict": report["verdict"], "json": str(json_path), "markdown": str(markdown_path), "report_hash": report["report_hash"]}, ensure_ascii=False))
    return 0 if report["verdict"] == ENGINEERING_PASS else 2 if report["verdict"] == PENDING_BUSINESS_ACCEPTANCE else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACCEPTANCE_SCHEMA_VERSION",
    "ENGINEERING_FAIL",
    "ENGINEERING_PASS",
    "PENDING_BUSINESS_ACCEPTANCE",
    "AcceptanceReportError",
    "build_acceptance_report",
    "main",
    "render_markdown",
    "write_acceptance_report",
]
