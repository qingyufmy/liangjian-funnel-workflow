"""Offline point-in-time replay-window evaluation.

This module deliberately sits outside the runtime workflow.  It evaluates
already persisted run summaries and lane audits; it never contacts a data
provider, invokes a model, or writes a runtime state record.  The evaluator is
strict about the identity of a frozen input (``snapshot_id``, SHA-256,
``as_of`` and ``trade_date``), because a multi-day report is not useful if a
future snapshot or a duplicate day can silently enter it.

The public entry point is :func:`evaluate_replay_window`.  It accepts either a
directory containing run-summary JSON files or an in-memory sequence of run
summary mappings, and returns a JSON-serialisable report.  A separate lane
audit directory is used by default because normal workflow summaries keep
large stage decisions in the immutable audit files.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


REPLAY_SCHEMA_VERSION = "liangjian-replay-window/1.0.0"
DEFAULT_MINIMUM_DAYS = 10
PRIMARY_LANE_DEFAULT = "lane_1"
STAGES = ("A1", "A2", "A3")
SHANGHAI = ZoneInfo("Asia/Shanghai")
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")
_POOL_KEYS = {
    "A1": ("active_research_pool", "monitor_pool"),
    "A2": ("focus_pool", "watch_only_pool"),
    "A3": ("core_watch_pool", "secondary_watch_pool"),
}
_AXES = (
    "lifecycle_state",
    "quality_state",
    "opportunity_state",
    "publication_state",
)
_LIFECYCLE = frozenset({"QUEUED", "RUNNING", "TERMINAL"})
_QUALITY = frozenset({"VALIDATED", "DEGRADED", "BLOCKED", "FAILED", "CANCELLED"})
_OPPORTUNITY = frozenset({"PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"})
_PUBLICATION = frozenset({"READY", "NOT_APPLICABLE", "BLOCKED", "PUBLISHED"})
ATTRIBUTION_BOOTSTRAP_SAMPLES = 2000
ATTRIBUTION_MIN_SAMPLE = 2


class ReplayWindowContractError(ValueError):
    """A malformed replay input that must fail closed."""

    def __init__(self, reason_code: str, message: str | None = None) -> None:
        self.reason_code = reason_code
        super().__init__(message or reason_code)


# A shorter alias is useful to callers that do not need the implementation
# name, while retaining the descriptive exception for reports and tests.
ReplayContractError = ReplayWindowContractError


@dataclass(frozen=True, slots=True)
class _LoadedRun:
    summary: Mapping[str, Any]
    source: str


@dataclass(frozen=True, slots=True)
class _RunContext:
    run_id: str
    trade_date: date
    snapshot_id: str
    snapshot_hash: str
    as_of: datetime
    test_only: bool
    summary: Mapping[str, Any]
    audit: Mapping[str, Any]
    source: str
    audit_source: str


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _upper(value: Any) -> str:
    return _text(value).upper()


def _as_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() and value >= 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None
    return None


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _ratio(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _safe_component(value: str) -> str:
    return _SAFE_COMPONENT_RE.sub("_", value).strip("._-")[:200] or "unknown"


def _parse_as_of(value: Any, *, source: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ReplayWindowContractError("REPLAY_AS_OF_MISSING", f"{source}: as_of is required")
    raw = value.strip()
    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ReplayWindowContractError("REPLAY_AS_OF_INVALID", f"{source}: as_of is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayWindowContractError("REPLAY_AS_OF_TIMEZONE_REQUIRED", f"{source}: as_of must include timezone")
    return parsed.astimezone(SHANGHAI)


def _parse_trade_date(value: Any, *, source: str) -> date:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value.strip()):
        raise ReplayWindowContractError("REPLAY_TRADE_DATE_INVALID", f"{source}: trade_date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ReplayWindowContractError("REPLAY_TRADE_DATE_INVALID", f"{source}: trade_date is invalid") from exc


def _extract_snapshot(summary: Mapping[str, Any], *, source: str) -> tuple[str, str, datetime, date]:
    raw_snapshot = summary.get("snapshot")
    snapshot = raw_snapshot if isinstance(raw_snapshot, Mapping) else {}
    snapshot_id = _text(snapshot.get("snapshot_id") or summary.get("snapshot_id"))
    if not snapshot_id:
        raise ReplayWindowContractError("REPLAY_SNAPSHOT_ID_MISSING", f"{source}: snapshot_id is required")
    snapshot_hash = _text(snapshot.get("snapshot_hash") or summary.get("snapshot_hash"))
    if not _SHA256_RE.fullmatch(snapshot_hash):
        raise ReplayWindowContractError("REPLAY_SNAPSHOT_HASH_INVALID", f"{source}: snapshot_hash must be SHA-256")
    as_of = _parse_as_of(snapshot.get("as_of") or summary.get("as_of"), source=source)
    raw_trade_date = snapshot.get("trade_date") or summary.get("trade_date")
    if raw_trade_date is None:
        raise ReplayWindowContractError("REPLAY_TRADE_DATE_MISSING", f"{source}: trade_date is required")
    trade_date = _parse_trade_date(raw_trade_date, source=source)
    if as_of.date() != trade_date:
        raise ReplayWindowContractError(
            "REPLAY_AS_OF_TRADE_DATE_MISMATCH",
            f"{source}: as_of date and trade_date differ",
        )
    return snapshot_id, snapshot_hash.lower(), as_of, trade_date


def _iter_json_files(root: Path) -> tuple[Path, ...]:
    if not root.exists() or not root.is_dir():
        raise ReplayWindowContractError("REPLAY_RUNS_DIR_NOT_FOUND", f"runs directory not found: {root}")
    return tuple(sorted(path for path in root.glob("*.json") if path.is_file()))


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReplayWindowContractError("REPLAY_JSON_INVALID", f"invalid JSON: {path.name}") from exc
    if not isinstance(raw, Mapping):
        raise ReplayWindowContractError("REPLAY_JSON_SHAPE_INVALID", f"JSON object required: {path.name}")
    return raw


def _load_runs(source: str | Path | Sequence[Mapping[str, Any]]) -> tuple[_LoadedRun, ...]:
    if isinstance(source, (str, Path)):
        root = Path(source).expanduser().resolve()
        paths = _iter_json_files(root)
        result: list[_LoadedRun] = []
        for path in paths:
            try:
                summary = _read_json(path)
            except ReplayWindowContractError:
                # Keep malformed files visible to the evaluator rather than
                # aborting an otherwise useful report.  The synthetic summary
                # lets validation_errors carry the stable reason code.
                result.append(_LoadedRun({"_replay_load_error": "REPLAY_JSON_INVALID"}, str(path)))
                continue
            result.append(_LoadedRun(summary, str(path)))
        return tuple(result)
    if not isinstance(source, Sequence) or isinstance(source, (str, bytes, bytearray)):
        raise ReplayWindowContractError("REPLAY_RUNS_SOURCE_INVALID", "source must be a directory or sequence")
    result = []
    for index, item in enumerate(source, start=1):
        if not isinstance(item, Mapping):
            result.append(_LoadedRun({"_replay_load_error": "REPLAY_SUMMARY_SHAPE_INVALID"}, f"memory[{index}]"))
        else:
            result.append(_LoadedRun(item, f"memory[{index}]"))
    return tuple(result)


def _candidate_audit_paths(
    summary: Mapping[str, Any],
    *,
    audit_dir: Path | None,
    run_id: str,
    primary_lane_id: str,
) -> tuple[tuple[str, Path | None], ...]:
    """Return declared and conventional primary-audit candidates.

    A declared path is accepted only after it is resolved below ``audit_dir``
    (or the source directory when no audit directory was supplied).  Embedded
    lane audits are represented as a ``None`` path and handled separately.
    """

    candidates: list[tuple[str, Path | None]] = []
    embedded = summary.get("lane_audits")
    if isinstance(embedded, Mapping):
        value = embedded.get(primary_lane_id)
        if isinstance(value, Mapping):
            candidates.append(("embedded:lane_audits", None))
        elif value:
            candidates.append((str(value), Path(str(value))))
    for key in ("audit_path", "primary_lane_audit", "lane_audit_path"):
        value = summary.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append((value.strip(), Path(value.strip())))
    lanes = summary.get("lanes")
    if isinstance(lanes, Sequence) and not isinstance(lanes, (str, bytes, bytearray)):
        for lane in lanes:
            if not isinstance(lane, Mapping) or _text(lane.get("lane") or lane.get("lane_id")) != primary_lane_id:
                continue
            if isinstance(lane.get("audit"), Mapping):
                candidates.append(("embedded:lanes", None))
            for key in ("audit_path", "path", "file"):
                value = lane.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append((value.strip(), Path(value.strip())))
            break
    if audit_dir is not None:
        conventional = audit_dir / f"research_{_safe_component(run_id)}_{_safe_component(primary_lane_id)}.json"
        candidates.append((str(conventional), conventional))
        # Current output names use the run id literally except for path
        # separators; inspect the bounded directory and let the audit's lane
        # field decide if a glob hit is the primary lane.
        for path in sorted(audit_dir.glob(f"research_{_safe_component(run_id)}_*.json")):
            candidates.append((str(path), path))
    # Preserve declaration order but avoid duplicate reads.
    seen: set[str] = set()
    unique: list[tuple[str, Path | None]] = []
    for label, path in candidates:
        key = label if path is None else str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append((label, path))
    return tuple(unique)


def _embedded_primary_audit(summary: Mapping[str, Any], primary_lane_id: str) -> Mapping[str, Any] | None:
    value = summary.get("lane_audits")
    if isinstance(value, Mapping) and isinstance(value.get(primary_lane_id), Mapping):
        return value[primary_lane_id]  # type: ignore[return-value]
    lanes = summary.get("lanes")
    if isinstance(lanes, Sequence) and not isinstance(lanes, (str, bytes, bytearray)):
        for lane in lanes:
            if not isinstance(lane, Mapping) or _text(lane.get("lane") or lane.get("lane_id")) != primary_lane_id:
                continue
            embedded = lane.get("audit")
            if isinstance(embedded, Mapping):
                return embedded
            # A lane entry with stages is itself a valid compact audit.
            if isinstance(lane.get("stages"), Sequence):
                return lane
    return None


def _load_audit(
    summary: Mapping[str, Any],
    *,
    audit_dir: Path | None,
    run_id: str,
    primary_lane_id: str,
) -> tuple[Mapping[str, Any] | None, str | None]:
    embedded = _embedded_primary_audit(summary, primary_lane_id)
    if embedded is not None:
        return embedded, "embedded"
    for label, candidate in _candidate_audit_paths(
        summary,
        audit_dir=audit_dir,
        run_id=run_id,
        primary_lane_id=primary_lane_id,
    ):
        if candidate is None:
            continue
        if not candidate.is_absolute() and audit_dir is not None:
            candidate = audit_dir / candidate
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        if audit_dir is not None:
            try:
                resolved.relative_to(audit_dir.resolve())
            except ValueError:
                continue
        if not resolved.is_file():
            continue
        try:
            audit = _read_json(resolved)
        except ReplayWindowContractError:
            return None, label
        return audit, str(resolved)
    return None, None


def _validate_audit_identity(
    audit: Mapping[str, Any],
    *,
    lane_id: str,
    snapshot_id: str,
    snapshot_hash: str,
    as_of: datetime,
    trade_date: date,
    source: str,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    raw_lane = _text(audit.get("lane") or audit.get("lane_id"))
    if raw_lane and raw_lane != lane_id:
        errors.append({"code": "REPLAY_LANE_ID_MISMATCH", "source": source, "detail": raw_lane})
    values: dict[str, Any] = {}
    nested = audit.get("snapshot")
    if isinstance(nested, Mapping):
        values.update(nested)
    values.update({key: audit[key] for key in ("snapshot_id", "snapshot_hash", "as_of", "trade_date") if key in audit})
    if values.get("snapshot_id") is not None and _text(values.get("snapshot_id")) != snapshot_id:
        errors.append({"code": "REPLAY_SNAPSHOT_ID_MISMATCH", "source": source, "detail": "lane audit"})
    if values.get("snapshot_hash") is not None and _text(values.get("snapshot_hash")).lower() != snapshot_hash:
        errors.append({"code": "REPLAY_SNAPSHOT_HASH_MISMATCH", "source": source, "detail": "lane audit"})
    if values.get("as_of") is not None:
        try:
            audit_as_of = _parse_as_of(values.get("as_of"), source=source)
        except ReplayWindowContractError as exc:
            errors.append({"code": exc.reason_code, "source": source, "detail": "lane audit as_of"})
        else:
            if audit_as_of != as_of:
                errors.append({"code": "REPLAY_AS_OF_MISMATCH", "source": source, "detail": "lane audit"})
    if values.get("trade_date") is not None:
        try:
            audit_trade_date = _parse_trade_date(values.get("trade_date"), source=source)
        except ReplayWindowContractError as exc:
            errors.append({"code": exc.reason_code, "source": source, "detail": "lane audit trade_date"})
        else:
            if audit_trade_date != trade_date:
                errors.append({"code": "REPLAY_TRADE_DATE_MISMATCH", "source": source, "detail": "lane audit"})
    return errors


def _stage_entries(audit: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw_stages = audit.get("stages")
    result: dict[str, Mapping[str, Any]] = {}
    if isinstance(raw_stages, Sequence) and not isinstance(raw_stages, (str, bytes, bytearray)):
        for item in raw_stages:
            if isinstance(item, Mapping):
                stage = _upper(item.get("stage"))
                if stage in STAGES and stage not in result:
                    result[stage] = item
    return result


def _lane_outcome_mapping(audit: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("outcome_v3", "outcome_v2", "outcome", "lane_outcome"):
        value = audit.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _outcome_stage_mapping(audit: Mapping[str, Any], stage: str) -> Mapping[str, Any] | None:
    lane = _lane_outcome_mapping(audit)
    if lane is None:
        return None
    stages = lane.get("stages")
    if isinstance(stages, Sequence) and not isinstance(stages, (str, bytes, bytearray)):
        for item in stages:
            if isinstance(item, Mapping) and _upper(item.get("stage")) == stage:
                return item
    return None


def _normalize_axes(value: Mapping[str, Any] | None, *, status: str, stage: str) -> dict[str, str]:
    """Normalize explicit four-axis values, with a conservative legacy fallback."""

    raw = value or {}
    lifecycle = _upper(raw.get("lifecycle_state"))
    quality = _upper(raw.get("quality_state"))
    opportunity = _upper(raw.get("opportunity_state"))
    publication = _upper(raw.get("publication_state"))
    if lifecycle not in _LIFECYCLE:
        lifecycle = "TERMINAL" if status not in {"", "RUNNING", "QUEUED", "PENDING", "CREATED"} else ("RUNNING" if status == "RUNNING" else "QUEUED")
    if quality not in _QUALITY:
        if status in {"FAILED", "MODEL_FAILED", "BLOCKED_MODEL", "MODEL_CALL_FAILED"}:
            quality = "FAILED"
        elif status.startswith("BLOCKED") or status in {"CANCELLED", "CANCELED", "NOT_RUN_UPSTREAM_BLOCKED"}:
            quality = "CANCELLED" if status in {"CANCELLED", "CANCELED"} else "BLOCKED"
        elif status in {"DEGRADED", "READY_DEGRADED", "VALIDATED_UNDERFILLED_MARKET"}:
            quality = "DEGRADED"
        else:
            quality = "VALIDATED"
    if opportunity not in _OPPORTUNITY:
        if status in {"VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"}:
            opportunity = "ABSENT"
        elif quality in {"BLOCKED", "FAILED", "CANCELLED"}:
            opportunity = "UNKNOWN"
        elif status in {"READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED"}:
            opportunity = "PRESENT"
        else:
            opportunity = "NOT_APPLICABLE"
    if publication not in _PUBLICATION:
        if status in {"PUBLISHED"}:
            publication = "PUBLISHED"
        elif status in {"READY", "READY_DEGRADED", "READY_TO_PUBLISH"}:
            publication = "READY"
        elif quality in {"BLOCKED", "FAILED", "CANCELLED"}:
            publication = "BLOCKED"
        else:
            publication = "NOT_APPLICABLE"
    return {
        "lifecycle_state": lifecycle,
        "quality_state": quality,
        "opportunity_state": opportunity,
        "publication_state": publication,
    }


def _stage_selected_count(stage: Mapping[str, Any], stage_name: str) -> int | None:
    for key in ("selected_count", "output_count", "symbol_count"):
        value = _as_nonnegative_int(stage.get(key))
        if value is not None:
            return value
    symbols = stage.get("symbols")
    if isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes, bytearray)):
        return len(symbols)
    output = stage.get("output")
    if isinstance(output, Mapping):
        total = 0
        seen = False
        for key in _POOL_KEYS[stage_name]:
            values = output.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
                total += len(values)
                seen = True
        if seen:
            return total
    return None


def _stage_count(stage: Mapping[str, Any], name: str, *keys: str) -> int | None:
    for key in keys:
        value = _as_nonnegative_int(stage.get(key))
        if value is not None:
            return value
    diagnostics = stage.get("diagnostics")
    if isinstance(diagnostics, Mapping):
        for key in keys:
            value = _as_nonnegative_int(diagnostics.get(key))
            if value is not None:
                return value
        local = diagnostics.get("local_screen")
        if isinstance(local, Mapping):
            for key in keys:
                value = _as_nonnegative_int(local.get(key))
                if value is not None:
                    return value
            gate = local.get("gate")
            if isinstance(gate, Mapping):
                for key in keys:
                    value = _as_nonnegative_int(gate.get(key))
                    if value is not None:
                        return value
    return None


def _snapshot_universe_count(summary: Mapping[str, Any]) -> int | None:
    snapshot = summary.get("snapshot")
    if not isinstance(snapshot, Mapping):
        snapshot = summary
    for key in ("research_universe_count", "trade_universe_count", "full_universe_count", "universe_count", "input_count"):
        value = _as_nonnegative_int(snapshot.get(key))
        if value is not None:
            return value
    return None


def _stage_reason_codes(stage: Mapping[str, Any], outcome: Mapping[str, Any] | None) -> list[str]:
    values: list[Any] = []
    for source in (stage, outcome or {}):
        for key in ("reason_codes", "reasons", "reason_code"):
            raw = source.get(key)
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                values.extend(raw)
            elif raw:
                values.append(raw)
    result: list[str] = []
    for value in values:
        text = _upper(value)
        if text and text not in result:
            result.append(text)
    return result


def _classify_stage(
    axes: Mapping[str, str],
    selected_count: int | None,
    reasons: Sequence[str],
    *,
    status: str = "",
) -> str:
    if status in {"NOT_RUN", "NOT_RUN_UPSTREAM_BLOCKED", "UPSTREAM_STAGE_BLOCKED", "UPSTREAM_BLOCKED"} or "UPSTREAM_STAGE_BLOCKED" in reasons:
        return "NOT_APPLICABLE"
    if axes.get("quality_state") in {"FAILED", "CANCELLED"}:
        return "EXECUTION_FAILURE"
    if axes.get("quality_state") == "BLOCKED" or axes.get("opportunity_state") == "UNKNOWN":
        return "DATA_INSUFFICIENT"
    if selected_count == 0 and axes.get("opportunity_state") == "ABSENT":
        return "EMPTY_OPPORTUNITY"
    if selected_count is not None and selected_count > 0:
        return "SUCCESS"
    if any("DATA" in reason or "COVERAGE" in reason or "EVIDENCE" in reason for reason in reasons):
        return "DATA_INSUFFICIENT"
    return "UNKNOWN"


def _aggregate_lane_axes(stage_axes: Sequence[Mapping[str, str]], statuses: Sequence[str]) -> dict[str, str]:
    if not stage_axes:
        return _normalize_axes({}, status="NOT_RUN", stage="LANE")
    if any(axis.get("lifecycle_state") == "RUNNING" for axis in stage_axes):
        lifecycle = "RUNNING"
    elif all(axis.get("lifecycle_state") == "TERMINAL" for axis in stage_axes):
        lifecycle = "TERMINAL"
    else:
        lifecycle = "QUEUED"
    quality = "VALIDATED"
    for candidate in ("CANCELLED", "FAILED", "BLOCKED", "DEGRADED", "VALIDATED"):
        if any(axis.get("quality_state") == candidate for axis in stage_axes):
            quality = candidate
            break
    if any(axis.get("opportunity_state") == "PRESENT" for axis in stage_axes):
        opportunity = "PRESENT"
    elif any(axis.get("opportunity_state") == "UNKNOWN" for axis in stage_axes):
        opportunity = "UNKNOWN"
    elif any(axis.get("opportunity_state") == "ABSENT" for axis in stage_axes):
        opportunity = "ABSENT"
    else:
        opportunity = "NOT_APPLICABLE"
    publication = "READY" if lifecycle == "TERMINAL" and quality in {"VALIDATED", "DEGRADED"} else "BLOCKED"
    return {
        "lifecycle_state": lifecycle,
        "quality_state": quality,
        "opportunity_state": opportunity,
        "publication_state": publication,
    }


def _evaluate_lane(
    summary: Mapping[str, Any],
    audit: Mapping[str, Any],
    *,
    primary_lane_id: str,
) -> dict[str, Any]:
    stages = _stage_entries(audit)
    lane_outcome = _lane_outcome_mapping(audit)
    stage_reports: dict[str, dict[str, Any]] = {}
    previous_selected: int | None = _snapshot_universe_count(summary)
    lane_reasons: list[str] = []
    stage_axes: list[Mapping[str, str]] = []
    statuses: list[str] = []
    for stage_name in STAGES:
        stage = stages.get(stage_name)
        if stage is None:
            stage_reports[stage_name] = {
                "status": "NOT_RECORDED",
                "selected_count": None,
                "input_count": previous_selected,
                "evaluated_count": None,
                "conversion_rate": None,
                "four_axes": _normalize_axes({}, status="BLOCKED", stage=stage_name),
                "reason_codes": ["REPLAY_STAGE_MISSING"],
                "classification": "DATA_INSUFFICIENT",
            }
            lane_reasons.append("REPLAY_STAGE_MISSING")
            stage_axes.append(stage_reports[stage_name]["four_axes"])
            statuses.append("NOT_RECORDED")
            continue
        status = _upper(stage.get("status"))
        statuses.append(status)
        selected = _stage_selected_count(stage, stage_name)
        explicit_outcome = stage.get("outcome_v3") if isinstance(stage.get("outcome_v3"), Mapping) else None
        if explicit_outcome is None:
            explicit_outcome = stage.get("outcome_v2") if isinstance(stage.get("outcome_v2"), Mapping) else None
        if explicit_outcome is None:
            explicit_outcome = stage.get("outcome") if isinstance(stage.get("outcome"), Mapping) else None
        if explicit_outcome is None:
            explicit_outcome = _outcome_stage_mapping(audit, stage_name)
        axes = _normalize_axes(explicit_outcome, status=status, stage=stage_name)
        reasons = _stage_reason_codes(stage, explicit_outcome)
        input_count = _stage_count(stage, "input", "input_count", "source_count", "candidate_count")
        if input_count is None:
            input_count = previous_selected
        evaluated = _stage_count(stage, "evaluated", "evaluated_count", "processed_count")
        classification = _classify_stage(axes, selected, reasons, status=status)
        if classification in {"DATA_INSUFFICIENT", "EXECUTION_FAILURE"}:
            lane_reasons.extend(reasons or [classification])
        report = {
            "status": status or "UNKNOWN",
            "selected_count": selected,
            "input_count": input_count,
            "evaluated_count": evaluated,
            "conversion_rate": _ratio(selected, input_count),
            "four_axes": axes,
            "reason_codes": reasons,
            "classification": classification,
        }
        stage_reports[stage_name] = report
        stage_axes.append(axes)
        previous_selected = selected
    explicit_lane_axes: Mapping[str, Any] | None = lane_outcome
    lane_axes = _normalize_axes(explicit_lane_axes, status=_upper(audit.get("status")), stage="LANE") if explicit_lane_axes else _aggregate_lane_axes(stage_axes, statuses)
    if lane_outcome is not None:
        lane_reasons.extend(_stage_reason_codes(audit, lane_outcome))
    unique_reasons: list[str] = []
    for reason in lane_reasons:
        if reason and reason not in unique_reasons:
            unique_reasons.append(reason)
    a3_selected = stage_reports["A3"].get("selected_count")
    a3_classification = stage_reports["A3"].get("classification")
    if any(report["classification"] == "EXECUTION_FAILURE" for report in stage_reports.values()):
        classification = "EXECUTION_FAILURE"
    elif any(report["classification"] == "DATA_INSUFFICIENT" for report in stage_reports.values()):
        classification = "DATA_INSUFFICIENT"
    elif a3_selected == 0 and a3_classification == "EMPTY_OPPORTUNITY":
        classification = "EMPTY_OPPORTUNITY"
    elif all(report["classification"] in {"SUCCESS", "EMPTY_OPPORTUNITY", "NOT_APPLICABLE"} for report in stage_reports.values()):
        classification = "SUCCESS" if a3_selected and a3_selected > 0 else "EMPTY_OPPORTUNITY"
    else:
        classification = "UNKNOWN"
    terminal = all(report["four_axes"]["lifecycle_state"] == "TERMINAL" for report in stage_reports.values())
    return {
        "lane_id": _text(audit.get("lane") or audit.get("lane_id")) or primary_lane_id,
        "model": _text(audit.get("model")) or None,
        "status": _upper(audit.get("status")) or ("READY" if terminal else "BLOCKED"),
        "terminal": terminal,
        "four_axes": lane_axes,
        "reason_codes": unique_reasons,
        "classification": classification,
        "stages": stage_reports,
    }


def _embedded_broker_report(summary: Mapping[str, Any], audit: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for container in (summary, audit):
        for key in ("broker_gold", "broker_benchmark", "broker_gold_report"):
            value = container.get(key)
            if isinstance(value, Mapping):
                return value
    return None


def _a1_rows(audit: Mapping[str, Any]) -> Any:
    for stage in (_stage_entries(audit).get("A1"),):
        if not isinstance(stage, Mapping):
            continue
        output = stage.get("output")
        if isinstance(output, Mapping):
            return output
        symbols = stage.get("symbols")
        if isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes, bytearray)):
            return [{"symbol": str(value)} for value in symbols]
    return []


def _broker_metrics(
    summary: Mapping[str, Any],
    audit: Mapping[str, Any],
    *,
    as_of: datetime,
    broker_gold_dir: Path | None,
) -> dict[str, Any]:
    embedded = _embedded_broker_report(summary, audit)
    if embedded is not None:
        result: dict[str, Any] = {"status": "AVAILABLE", "source": "embedded_report"}
        for key in ("counts", "symbol_coverage", "active_coverage", "broker_consensus", "by_broker", "missing_symbols"):
            if key in embedded:
                result[key] = embedded[key]
        if "status" in embedded:
            result["benchmark_status"] = _text(embedded.get("status"))
        return result
    if broker_gold_dir is None:
        return {"status": "NOT_CONFIGURED", "reason_code": "BROKER_GOLD_NOT_CONFIGURED"}
    month = as_of.strftime("%Y-%m")
    candidates = (broker_gold_dir / f"{month}.json", broker_gold_dir / f"{month}.csv")
    candidate = next((path for path in candidates if path.is_file()), None)
    if candidate is None:
        return {"status": "NOT_CONFIGURED", "reason_code": "BROKER_GOLD_MONTH_NOT_FOUND", "month": month}
    try:
        from .broker_gold import evaluate_broker_gold, import_broker_gold

        dataset = import_broker_gold(candidate, as_of=as_of)
        report = evaluate_broker_gold(dataset, _a1_rows(audit), as_of=as_of, month=month)
    except Exception as exc:  # optional benchmark must not block replay itself
        reason = getattr(exc, "reason_code", "BROKER_GOLD_INVALID")
        return {"status": "INVALID", "reason_code": str(reason), "month": month}
    return {
        "status": "AVAILABLE" if report.get("status") != "EMPTY_BENCHMARK" else "EMPTY",
        "source": str(candidate),
        "month": month,
        "counts": report.get("counts", {}),
        "symbol_coverage": report.get("symbol_coverage", {}),
        "active_coverage": report.get("active_coverage", {}),
        "monitor_coverage": report.get("monitor_coverage", {}),
        "broker_consensus": report.get("broker_consensus", {}),
    }


def _record_validation_error(errors: list[dict[str, str]], code: str, source: str, detail: str = "") -> None:
    errors.append({"code": code, "source": source, "detail": detail})


def _context_from_loaded(
    loaded: _LoadedRun,
    *,
    audit_dir: Path | None,
    primary_lane_id: str,
    cutoff: datetime,
    errors: list[dict[str, str]],
) -> _RunContext | None:
    summary = loaded.summary
    source = loaded.source
    if summary.get("_replay_load_error"):
        _record_validation_error(errors, str(summary["_replay_load_error"]), source)
        return None
    run_id = _text(summary.get("run_id") or summary.get("id"))
    if not run_id:
        _record_validation_error(errors, "REPLAY_RUN_ID_MISSING", source)
        return None
    try:
        snapshot_id, snapshot_hash, as_of, trade_date = _extract_snapshot(summary, source=source)
    except ReplayWindowContractError as exc:
        _record_validation_error(errors, exc.reason_code, source, str(exc))
        return None
    if as_of > cutoff:
        _record_validation_error(errors, "REPLAY_FUTURE_DATA_REJECTED", source, "as_of is after evaluation cutoff")
        return None
    if trade_date > cutoff.date():
        _record_validation_error(errors, "REPLAY_FUTURE_TRADE_DATE_REJECTED", source, "trade_date is after evaluation cutoff")
        return None
    audit, audit_source = _load_audit(
        summary,
        audit_dir=audit_dir,
        run_id=run_id,
        primary_lane_id=primary_lane_id,
    )
    if audit is None:
        _record_validation_error(errors, "REPLAY_PRIMARY_LANE_AUDIT_MISSING", source, primary_lane_id)
        return None
    audit_label = audit_source or source
    errors.extend(
        _validate_audit_identity(
            audit,
            lane_id=primary_lane_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            as_of=as_of,
            trade_date=trade_date,
            source=audit_label,
        )
    )
    stages = _stage_entries(audit)
    for stage_name, stage in stages.items():
        raw_snapshot_id = stage.get("snapshot_id")
        if raw_snapshot_id is not None and _text(raw_snapshot_id) != snapshot_id:
            _record_validation_error(errors, "REPLAY_STAGE_SNAPSHOT_ID_MISMATCH", audit_label, stage_name)
    if set(stages) != set(STAGES):
        missing = ",".join(stage for stage in STAGES if stage not in stages)
        _record_validation_error(errors, "REPLAY_STAGE_SET_INVALID", audit_label, missing or "unexpected stage set")
    if errors and any(item.get("source") == audit_label for item in errors):
        # Identity errors are attached to the report but this run cannot be
        # treated as a valid day.  Returning None prevents it from inflating
        # the independent-day count.
        return None
    return _RunContext(
        run_id=run_id,
        trade_date=trade_date,
        snapshot_id=snapshot_id,
        snapshot_hash=snapshot_hash,
        as_of=as_of,
        test_only=bool(summary.get("test_only") or summary.get("mode") == "TEST_ONLY"),
        summary=summary,
        audit=audit,
        source=source,
        audit_source=audit_label,
    )


def _check_context_identity(contexts: Sequence[_RunContext], errors: list[dict[str, str]]) -> tuple[_RunContext, ...]:
    by_day: dict[date, list[_RunContext]] = defaultdict(list)
    by_snapshot: dict[str, list[_RunContext]] = defaultdict(list)
    for context in contexts:
        by_day[context.trade_date].append(context)
        by_snapshot[context.snapshot_id].append(context)
    valid: list[_RunContext] = []
    duplicate_days: set[date] = set()
    for day, values in by_day.items():
        if len(values) > 1:
            duplicate_days.add(day)
            for value in values:
                _record_validation_error(errors, "REPLAY_DUPLICATE_PRIMARY_TRADE_DATE", value.source, day.isoformat())
        else:
            valid.append(values[0])
    for snapshot_id, values in by_snapshot.items():
        pairs = {(value.snapshot_hash, value.as_of) for value in values}
        if len(pairs) > 1:
            for value in values:
                _record_validation_error(errors, "REPLAY_SNAPSHOT_ID_REUSED_WITH_DIFFERENT_CONTENT", value.source, snapshot_id)
    return tuple(sorted(valid, key=lambda value: (value.trade_date, value.run_id)))


def _summary_metrics(days: Sequence[Mapping[str, Any]], minimum_days: int) -> dict[str, Any]:
    classifications = Counter(str(day.get("classification") or "UNKNOWN") for day in days)
    terminal_count = sum(bool(day.get("terminal")) for day in days)
    success = classifications.get("SUCCESS", 0)
    empty = classifications.get("EMPTY_OPPORTUNITY", 0)
    insufficient = classifications.get("DATA_INSUFFICIENT", 0)
    failure = classifications.get("EXECUTION_FAILURE", 0)
    stage_rates: dict[str, dict[str, Any]] = {}
    stage_selected: dict[str, dict[str, Any]] = {}
    for stage_name in STAGES:
        values = [
            _as_number(day.get("stages", {}).get(stage_name, {}).get("conversion_rate"))
            for day in days
            if isinstance(day.get("stages"), Mapping)
        ]
        values = [value for value in values if value is not None]
        stage_rates[stage_name] = {
            "observations": len(values),
            "mean": round(sum(values) / len(values), 6) if values else None,
            "min": round(min(values), 6) if values else None,
            "max": round(max(values), 6) if values else None,
        }
        selected_values = [
            _as_nonnegative_int(day.get("stages", {}).get(stage_name, {}).get("selected_count"))
            for day in days
            if isinstance(day.get("stages"), Mapping)
        ]
        selected_values = [value for value in selected_values if value is not None]
        stage_selected[stage_name] = {
            "observations": len(selected_values),
            "total": sum(selected_values) if selected_values else 0,
            "mean": round(sum(selected_values) / len(selected_values), 6) if selected_values else None,
            "min": min(selected_values) if selected_values else None,
            "max": max(selected_values) if selected_values else None,
        }
    broker_available = sum(day.get("broker_gold", {}).get("status") == "AVAILABLE" for day in days if isinstance(day.get("broker_gold"), Mapping))
    return {
        "minimum_days": minimum_days,
        "independent_trading_days": len(days),
        "terminal_days": terminal_count,
        "non_terminal_days": len(days) - terminal_count,
        "classification_counts": {
            "SUCCESS": success,
            "EMPTY_OPPORTUNITY": empty,
            "DATA_INSUFFICIENT": insufficient,
            "EXECUTION_FAILURE": failure,
            "UNKNOWN": classifications.get("UNKNOWN", 0),
        },
        "success_rate": _ratio(success, terminal_count),
        "empty_opportunity_rate": _ratio(empty, terminal_count),
        "data_insufficient_rate": _ratio(insufficient, terminal_count),
        "execution_failure_rate": _ratio(failure, terminal_count),
        "stage_conversion_rates": stage_rates,
        "stage_selected_counts": stage_selected,
        "broker_gold_available_days": broker_available,
        "broker_gold_unavailable_days": len(days) - broker_available,
    }


def _day_report(context: _RunContext, *, primary_lane_id: str, broker_gold_dir: Path | None) -> dict[str, Any]:
    lane = _evaluate_lane(context.summary, context.audit, primary_lane_id=primary_lane_id)
    return {
        "trade_date": context.trade_date.isoformat(),
        "run_id": context.run_id,
        "test_only": context.test_only,
        "snapshot": {
            "snapshot_id": context.snapshot_id,
            "snapshot_hash": context.snapshot_hash,
            "as_of": context.as_of.isoformat(),
        },
        "primary_lane": lane,
        "terminal": lane["terminal"],
        "classification": lane["classification"],
        "four_axes": lane["four_axes"],
        "reason_codes": lane["reason_codes"],
        "stages": lane["stages"],
        "broker_gold": _broker_metrics(context.summary, context.audit, as_of=context.as_of, broker_gold_dir=broker_gold_dir),
        "source": {"run_summary": context.source, "lane_audit": context.audit_source},
    }


def _markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    lines = [
        "# A1-A3 点时回放验收",
        "",
        f"- schema: `{report.get('schema_version', REPLAY_SCHEMA_VERSION)}`",
        f"- 状态: **{report.get('status', 'UNKNOWN')}**",
        f"- 评估截止: `{report.get('cutoff') or '未提供'}`",
        f"- 独立交易日: **{summary.get('independent_trading_days', 0)} / {summary.get('minimum_days', DEFAULT_MINIMUM_DAYS)}**",
        "",
        "## 汇总",
        "",
        "| 指标 | 数值 |",
        "| --- | ---: |",
        f"| 终态日 | {summary.get('terminal_days', 0)} |",
        f"| 成功（非空） | {summary.get('classification_counts', {}).get('SUCCESS', 0)} |",
        f"| 已验证但无机会 | {summary.get('classification_counts', {}).get('EMPTY_OPPORTUNITY', 0)} |",
        f"| 数据不足 | {summary.get('classification_counts', {}).get('DATA_INSUFFICIENT', 0)} |",
        f"| 执行失败 | {summary.get('classification_counts', {}).get('EXECUTION_FAILURE', 0)} |",
        f"| 成功率（终态日） | {summary.get('success_rate')} |",
        f"| 券商金股可用日 | {summary.get('broker_gold_available_days', 0)} |",
        "",
        "## 阶段平均转化率",
        "",
        "| 阶段 | 样本数 | 平均 | 最低 | 最高 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for stage in STAGES:
        item = summary.get("stage_conversion_rates", {}).get(stage, {})
        lines.append(f"| {stage} | {item.get('observations', 0)} | {item.get('mean')} | {item.get('min')} | {item.get('max')} |")
    lines.extend(["", "## 阶段选中数", "", "| 阶段 | 日数 | 合计 | 平均 | 最低 | 最高 |", "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for stage in STAGES:
        item = summary.get("stage_selected_counts", {}).get(stage, {})
        lines.append(f"| {stage} | {item.get('observations', 0)} | {item.get('total', 0)} | {item.get('mean')} | {item.get('min')} | {item.get('max')} |")
    lines.extend(["", "## 逐日结果", "", "| 交易日 | 运行 | A1 | A2 | A3 | 四轴/分类 |", "| --- | --- | ---: | ---: | ---: | --- |"])
    for day in report.get("days", ()):
        stages = day.get("stages", {})
        counts = [stages.get(stage, {}).get("selected_count") for stage in STAGES]
        axes = day.get("four_axes", {})
        axis_text = "/".join(str(axes.get(axis, "UNKNOWN")) for axis in _AXES)
        lines.append(f"| {day.get('trade_date')} | `{day.get('run_id')}` | {counts[0]} | {counts[1]} | {counts[2]} | {axis_text} / {day.get('classification')} |")
    reasons = report.get("blocking_reasons")
    if isinstance(reasons, Sequence) and reasons:
        lines.extend(["", "## 阻断与校验原因", ""])
        for reason in reasons:
            if isinstance(reason, Mapping):
                lines.append(f"- `{reason.get('code')}` — {reason.get('source')}: {reason.get('detail', '')}")
    lines.extend(["", "> 本报告只读取已落盘摘要与审计文件；回放不发布计划、不调用模型、不连接外部交易。", ""])
    return "\n".join(lines)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with open(descriptor, "w", encoding="utf-8", newline="\n", closefd=True) as handle:
            handle.write(content)
            handle.flush()
        Path(temporary).replace(path)
    except BaseException:
        try:
            Path(temporary).unlink(missing_ok=True)
        except OSError:
            pass
        raise


def write_replay_report(report: Mapping[str, Any], *, json_path: str | Path, markdown_path: str | Path) -> tuple[Path, Path]:
    """Persist a replay JSON and human-readable Markdown report atomically."""

    json_target = Path(json_path).expanduser().resolve()
    markdown_target = Path(markdown_path).expanduser().resolve()
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    _atomic_write(json_target, encoded)
    _atomic_write(markdown_target, _markdown(report))
    return json_target, markdown_target


def evaluate_replay_window(
    source: str | Path | Sequence[Mapping[str, Any]],
    *,
    audit_dir: str | Path | None = None,
    broker_gold_dir: str | Path | None = None,
    minimum_days: int = DEFAULT_MINIMUM_DAYS,
    primary_lane_id: str = PRIMARY_LANE_DEFAULT,
    cutoff: datetime | str | None = None,
    output_json: str | Path | None = None,
    output_markdown: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate one primary lane over independent point-in-time run days.

    ``status`` is ``READY`` only when at least ``minimum_days`` valid unique
    days exist and every primary lane reaches a terminal state.  A short
    window is explicitly ``BLOCKED_REPLAY_WINDOW_INSUFFICIENT``; duplicate
    same-day runs and future data are validation blockers and never increase
    the day count.
    """

    if isinstance(minimum_days, bool) or not isinstance(minimum_days, int) or minimum_days <= 0:
        raise ReplayWindowContractError("REPLAY_MINIMUM_DAYS_INVALID", "minimum_days must be a positive integer")
    lane_id = _text(primary_lane_id)
    if not lane_id:
        raise ReplayWindowContractError("REPLAY_PRIMARY_LANE_ID_MISSING", "primary_lane_id is required")
    if cutoff is None:
        cutoff_dt = datetime.now(SHANGHAI)
    elif isinstance(cutoff, datetime):
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ReplayWindowContractError("REPLAY_CUTOFF_TIMEZONE_REQUIRED", "cutoff must include timezone")
        cutoff_dt = cutoff.astimezone(SHANGHAI)
    else:
        cutoff_dt = _parse_as_of(cutoff, source="cutoff")
    audit_root = Path(audit_dir).expanduser().resolve() if audit_dir is not None else None
    broker_root = Path(broker_gold_dir).expanduser().resolve() if broker_gold_dir is not None else None
    loaded = _load_runs(source)
    errors: list[dict[str, str]] = []
    contexts: list[_RunContext] = []
    for item in loaded:
        context = _context_from_loaded(
            item,
            audit_dir=audit_root,
            primary_lane_id=lane_id,
            cutoff=cutoff_dt,
            errors=errors,
        )
        if context is not None:
            contexts.append(context)
    contexts = list(_check_context_identity(contexts, errors))
    days = [_day_report(context, primary_lane_id=lane_id, broker_gold_dir=broker_root) for context in contexts]
    summary = _summary_metrics(days, minimum_days)
    blocking_reasons = list(errors)
    if any(error.get("code") in {"REPLAY_FUTURE_DATA_REJECTED", "REPLAY_FUTURE_TRADE_DATE_REJECTED"} for error in errors):
        status = "BLOCKED_REPLAY_FUTURE_DATA"
    elif any(error.get("code") == "REPLAY_DUPLICATE_PRIMARY_TRADE_DATE" for error in errors):
        status = "BLOCKED_REPLAY_DUPLICATE_PRIMARY_DAY"
    elif len(days) < minimum_days:
        status = "BLOCKED_REPLAY_WINDOW_INSUFFICIENT"
        blocking_reasons.append({
            "code": "BLOCKED_REPLAY_WINDOW_INSUFFICIENT",
            "source": "replay-window",
            "detail": f"{len(days)} independent days available; {minimum_days} required",
        })
    elif errors:
        status = "BLOCKED_REPLAY_VALIDATION"
    elif summary["non_terminal_days"]:
        status = "BLOCKED_REPLAY_INCOMPLETE"
    else:
        status = "READY"
    for day in days:
        if day.get("classification") in {"DATA_INSUFFICIENT", "EXECUTION_FAILURE", "UNKNOWN"}:
            for reason in day.get("reason_codes", ()):
                blocking_reasons.append({
                    "code": reason,
                    "source": f"{day.get('trade_date')}:{day.get('run_id')}",
                    "detail": day.get("classification", ""),
                })
    report: dict[str, Any] = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "status": status,
        "primary_lane_id": lane_id,
        "cutoff": cutoff_dt.isoformat(),
        "source": {
            "runs": str(Path(source).expanduser().resolve()) if isinstance(source, (str, Path)) else "memory",
            "audits": str(audit_root) if audit_root else None,
            "broker_gold": str(broker_root) if broker_root else None,
            "network_used": False,
            "models_called": False,
            "runtime_mutation": False,
        },
        "summary": summary,
        "blocking_reasons": blocking_reasons,
        "future_data_rejected": sum(error.get("code", "").startswith("REPLAY_FUTURE") for error in errors),
        "days": days,
    }
    report["report_hash"] = _canonical_hash({key: value for key, value in report.items() if key != "report_hash"})
    if output_json is not None or output_markdown is not None:
        if output_json is None or output_markdown is None:
            raise ReplayWindowContractError("REPLAY_OUTPUT_PAIR_REQUIRED", "output_json and output_markdown must be supplied together")
        write_replay_report(report, json_path=output_json, markdown_path=output_markdown)
    return report


# ---------------------------------------------------------------------------
# Outcome-label attribution
# ---------------------------------------------------------------------------
def _attribution_rows(labels: Any) -> tuple[dict[str, Any], ...]:
    """Read label rows from a store, a sequence, or a report mapping."""

    if hasattr(labels, "list_outcome_labels") and callable(labels.list_outcome_labels):
        labels = labels.list_outcome_labels(labeled_only=False)
    elif isinstance(labels, Mapping):
        labels = labels.get("labels", labels.get("rows", labels.get("data", ())))
    if not isinstance(labels, Sequence) or isinstance(labels, (str, bytes, bytearray)):
        return ()
    normalized: list[dict[str, Any]] = []
    for raw in labels:
        if not isinstance(raw, Mapping):
            continue
        stage = _upper(raw.get("stage"))
        if stage not in {"G0", "A1", "A2", "A3", "A4"}:
            continue
        decision = _upper(raw.get("decision"))
        if decision in {"PASS", "ACTIVE", "QUALIFIED"}:
            decision = "PASSED"
        elif decision in {"NOT_SENT", "UNSENT", "NOT_REVIEWED"}:
            decision = "NOT_SENT_TO_LLM"
        elif decision != "PASSED":
            decision = "REJECTED"
        excess = _as_number(raw.get("excess_return_5d"))
        if excess is None:
            fwd = _as_number(raw.get("fwd_return_5d"))
            benchmark = _as_number(raw.get("benchmark_return_5d"))
            if fwd is not None and benchmark is not None:
                excess = fwd - benchmark
        normalized.append(
            {
                **dict(raw),
                "stage": stage,
                "decision": decision,
                "trade_date": _text(raw.get("trade_date")),
                "symbol": _text(raw.get("symbol")).upper(),
                "excess_return_5d": excess,
            }
        )
    return tuple(normalized)


def _bootstrap_mean_ci(
    values: Sequence[float],
    *,
    seed_material: str,
    samples: int = ATTRIBUTION_BOOTSTRAP_SAMPLES,
) -> dict[str, Any] | None:
    if not values:
        return None
    if isinstance(samples, bool) or not isinstance(samples, int) or samples <= 0:
        raise ValueError("bootstrap samples must be a positive integer")
    digest = hashlib.sha256(seed_material.encode("utf-8")).digest()
    rng = __import__("random").Random(int.from_bytes(digest[:8], "big"))
    count = len(values)
    estimates = [
        sum(values[rng.randrange(count)] for _ in range(count)) / count
        for _ in range(samples)
    ]
    estimates.sort()
    low_position = (samples - 1) * 0.025
    high_position = (samples - 1) * 0.975

    def percentile(position: float) -> float:
        left = int(math.floor(position))
        right = int(math.ceil(position))
        if left == right:
            return float(estimates[left])
        fraction = position - left
        return float(estimates[left] + (estimates[right] - estimates[left]) * fraction)

    return {
        "low": percentile(low_position),
        "high": percentile(high_position),
        "confidence": 0.95,
        "samples": samples,
        "observations": count,
    }


def _bootstrap_difference_ci(
    left: Sequence[float],
    right: Sequence[float],
    *,
    seed_material: str,
    samples: int = ATTRIBUTION_BOOTSTRAP_SAMPLES,
) -> dict[str, Any] | None:
    if not left or not right:
        return None
    if isinstance(samples, bool) or not isinstance(samples, int) or samples <= 0:
        raise ValueError("bootstrap samples must be a positive integer")
    digest = hashlib.sha256(seed_material.encode("utf-8")).digest()
    rng = __import__("random").Random(int.from_bytes(digest[:8], "big"))
    left_n = len(left)
    right_n = len(right)
    estimates = [
        sum(left[rng.randrange(left_n)] for _ in range(left_n)) / left_n
        - sum(right[rng.randrange(right_n)] for _ in range(right_n)) / right_n
        for _ in range(samples)
    ]
    estimates.sort()
    low_position = (samples - 1) * 0.025
    high_position = (samples - 1) * 0.975

    def percentile(position: float) -> float:
        left_index = int(math.floor(position))
        right_index = int(math.ceil(position))
        if left_index == right_index:
            return float(estimates[left_index])
        fraction = position - left_index
        return float(estimates[left_index] + (estimates[right_index] - estimates[left_index]) * fraction)

    return {
        "low": percentile(low_position),
        "high": percentile(high_position),
        "confidence": 0.95,
        "samples": samples,
        "left_observations": left_n,
        "right_observations": right_n,
    }


def _mean(values: Sequence[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _core_distribution(rows: Sequence[Mapping[str, Any]], runs: Any) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        if row.get("stage") != "A3" or row.get("decision") != "PASSED":
            continue
        day = _text(row.get("trade_date"))
        if day:
            counts[day] = counts.get(day, 0) + 1
    if isinstance(runs, Mapping):
        runs = runs.get("runs", runs.get("days", ()))
    run_counts: dict[str, int] = {}
    if isinstance(runs, Sequence) and not isinstance(runs, (str, bytes, bytearray)):
        for run in runs:
            if not isinstance(run, Mapping):
                continue
            day = _text(run.get("trade_date"))
            if not day:
                continue
            stage = run.get("stage")
            stage_data = run.get("stages")
            if isinstance(stage_data, Mapping):
                stage_data = stage_data.get("A3")
            if not isinstance(stage_data, Mapping) and _upper(stage) == "A3":
                stage_data = run
            if isinstance(stage_data, Mapping):
                count = _as_nonnegative_int(
                    stage_data.get("core_watch_pool_count", stage_data.get("selected_count"))
                )
                if count is not None:
                    # A run summary is the authoritative count for its
                    # trading date.  Label rows may be filtered or partial
                    # (for example, only one A3 result has a forward label),
                    # so they must not undercount the core pool.
                    run_counts[day] = count
    counts.update(run_counts)
    values = sorted(counts.values())
    if not values:
        return {"status": "INSUFFICIENT_SAMPLE", "sample": 0, "counts_by_trade_date": {}}

    def quantile(fraction: float) -> float:
        position = (len(values) - 1) * fraction
        left = int(math.floor(position))
        right = int(math.ceil(position))
        if left == right:
            return float(values[left])
        weight = position - left
        return float(values[left] + (values[right] - values[left]) * weight)

    return {
        "status": "READY" if len(values) >= ATTRIBUTION_MIN_SAMPLE else "INSUFFICIENT_SAMPLE",
        "sample": len(values),
        "counts_by_trade_date": dict(sorted(counts.items())),
        "min": min(values),
        "max": max(values),
        "mean": _mean(values),
        "p25": quantile(0.25),
        "median": quantile(0.5),
        "p75": quantile(0.75),
    }


def layer_attribution(
    runs: Any,
    labels: Any,
    *,
    bootstrap_samples: int = ATTRIBUTION_BOOTSTRAP_SAMPLES,
    minimum_sample: int = ATTRIBUTION_MIN_SAMPLE,
) -> dict[str, Any]:
    """Attribute forward excess returns to each funnel layer.

    ``runs`` is used only for optional stage/count context; the calculation is
    driven by the complete label ledger.  ``NOT_SENT_TO_LLM`` remains a
    separate decision class: it contributes to the denominator but not to
    pass or rejected-loss means.  If a layer lacks enough observations, its
    result is explicitly ``INSUFFICIENT_SAMPLE`` and numeric conclusions are
    omitted.
    """

    if isinstance(minimum_sample, bool) or not isinstance(minimum_sample, int) or minimum_sample <= 0:
        raise ValueError("minimum_sample must be a positive integer")
    rows = _attribution_rows(labels)
    previous_stage = {"A1": "G0", "A2": "A1", "A3": "A2", "A4": "A3"}
    layer_reports: dict[str, dict[str, Any]] = {}
    insufficient_layers: list[str] = []
    for stage in ("G0", "A1", "A2", "A3", "A4"):
        current = [row for row in rows if row["stage"] == stage]
        passed = [row for row in current if row["decision"] == "PASSED"]
        rejected = [row for row in current if row["decision"] == "REJECTED"]
        not_sent = [row for row in current if row["decision"] == "NOT_SENT_TO_LLM"]
        passed_values = [float(row["excess_return_5d"]) for row in passed if row.get("excess_return_5d") is not None]
        rejected_values = [float(row["excess_return_5d"]) for row in rejected if row.get("excess_return_5d") is not None]
        previous_values = [
            float(row["excess_return_5d"])
            for row in rows
            if row["stage"] == previous_stage.get(stage, "")
            and row["decision"] == "PASSED"
            and row.get("excess_return_5d") is not None
        ]
        sample = len(current)
        status = "READY" if sample >= minimum_sample else "INSUFFICIENT_SAMPLE"
        if status != "READY":
            insufficient_layers.append(stage)
        # A below-minimum layer is descriptive only.  Do not expose a point
        # estimate that a caller could mistake for an actionable conclusion.
        pass_rate = len(passed) / sample if sample >= minimum_sample else None
        pass_rate_ci = (
            _bootstrap_mean_ci(
                [1.0 if row["decision"] == "PASSED" else 0.0 for row in current],
                seed_material=f"pass-rate|{stage}|{','.join(sorted(_text(row.get('trade_date')) for row in current))}",
                samples=bootstrap_samples,
            )
            if sample >= minimum_sample
            else None
        )
        gain = (
            _mean(passed_values) - _mean(previous_values)
            if status == "READY" and passed_values and previous_values
            else None
        )
        gain_ci = (
            _bootstrap_difference_ci(
                passed_values,
                previous_values,
                seed_material=f"gain|{stage}|{len(passed_values)}|{len(previous_values)}",
                samples=bootstrap_samples,
            )
            if status == "READY" and passed_values and previous_values
            else None
        )
        loss = _mean(rejected_values) if status == "READY" and rejected_values else None
        loss_ci = (
            _bootstrap_mean_ci(
                rejected_values,
                seed_material=f"loss|{stage}|{len(rejected_values)}",
                samples=bootstrap_samples,
            )
            if status == "READY" and rejected_values
            else None
        )
        report: dict[str, Any] = {
            "status": status,
            "sample": sample,
            "sample_count": sample,
            "passed_count": len(passed),
            "rejected_count": len(rejected),
            "not_sent_count": len(not_sent),
            "pass_rate": pass_rate,
            "gain": gain,
            "loss": loss,
            "gain_ci": gain_ci,
            "loss_ci": loss_ci,
            "pass_rate_ci": pass_rate_ci,
            "bootstrap_ci": {
                "gain": gain_ci,
                "loss": loss_ci,
                "pass_rate": pass_rate_ci,
            },
            "excess_sample": {
                "passed": len(passed_values),
                "rejected": len(rejected_values),
                "previous": len(previous_values),
            },
        }
        if stage == "G0":
            report["reason_code"] = "NO_PREVIOUS_LAYER"
        elif gain is None:
            report["reason_code"] = "PREVIOUS_OR_PASSED_EXCESS_INSUFFICIENT"
        layer_reports[stage] = report
    distribution = _core_distribution(rows, runs)
    overall_status = "INSUFFICIENT_SAMPLE" if insufficient_layers else "READY"
    result: dict[str, Any] = {
        "schema_version": "liangjian-layer-attribution/1.0.0",
        "status": overall_status,
        "minimum_sample": minimum_sample,
        "bootstrap_samples": bootstrap_samples,
        "insufficient_layers": insufficient_layers,
        "layers": layer_reports,
        "core_count_distribution": distribution,
        "network_used": False,
        "models_called": False,
        "runtime_mutation": False,
    }
    # Direct stage aliases keep the report convenient for CLI/JSON consumers
    # without hiding the canonical ``layers`` object.
    result.update(layer_reports)
    return result


__all__ = [
    "ATTRIBUTION_BOOTSTRAP_SAMPLES",
    "ATTRIBUTION_MIN_SAMPLE",
    "DEFAULT_MINIMUM_DAYS",
    "PRIMARY_LANE_DEFAULT",
    "REPLAY_SCHEMA_VERSION",
    "ReplayContractError",
    "ReplayWindowContractError",
    "evaluate_replay_window",
    "layer_attribution",
    "write_replay_report",
]
