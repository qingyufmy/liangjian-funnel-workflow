"""Public v3 outcome contract helpers.

``outcomes.py`` contains the reducers used by the existing pipeline.  This
module is the intentionally small boundary imported by CLI/control-plane code
and by contract tooling.  Keeping the vocabulary here makes it possible to
generate cross-language types without importing workflow orchestration.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .outcomes import (
    LEGACY_OUTCOME_SCHEMA_VERSION,
    OUTCOME_SCHEMA_VERSION,
    ActionabilityState,
    DataSufficiencyState,
    JobLifecycleState,
    LaneOutcome,
    RunOutcome,
    StageOutcome,
)


CONTRACT_NAME = "research-outcome"
CONTRACT_VERSION = OUTCOME_SCHEMA_VERSION


def contract_hash(payload: Mapping[str, Any]) -> str:
    """Return a stable digest for an already-normalized v3 payload."""

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_stage(value: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a stage or legacy stage into the v3 exchange shape."""

    return StageOutcome.from_mapping(value).as_dict()


def normalize_lane(value: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a lane or legacy lane into the v3 exchange shape."""

    return LaneOutcome.from_mapping(value).as_dict()


def normalize_run(value: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a run or legacy acceptance row into the v3 exchange shape."""

    return RunOutcome.from_mapping(value).as_dict()


def validate_contract(value: Mapping[str, Any], *, kind: str | None = None) -> tuple[str, ...]:
    """Perform deterministic structural validation without a JSON-schema lib.

    The JSON Schema remains the cross-language source of truth.  This helper
    is used by lightweight CLI/tests where installing a schema validator would
    be disproportionate.  It deliberately reports errors instead of
    accepting unknown enum values or silently inferring them.
    """

    errors: list[str] = []
    if not isinstance(value, Mapping):
        return ("root must be an object",)
    if value.get("schema_version") != CONTRACT_VERSION:
        errors.append("schema_version must be research-outcome/3.0.0")
    required = {"schema_version", "job_status", "quality_state", "data_sufficiency_state", "publication_state", "reason_codes", "counts", "data_coverage", "legacy_status"}
    required.update({"lifecycle_state", "research_opportunity_state", "focus_opportunity_state", "actionability_state"})
    missing = sorted(required.difference(value))
    errors.extend(f"missing field: {field}" for field in missing)
    if value.get("job_status") not in {item.value for item in JobLifecycleState}:
        errors.append("invalid job_status")
    if value.get("data_sufficiency_state") not in {item.value for item in DataSufficiencyState}:
        errors.append("invalid data_sufficiency_state")
    if value.get("lifecycle_state") not in {"QUEUED", "RUNNING", "TERMINAL"}:
        errors.append("invalid lifecycle_state")
    for field in ("research_opportunity_state", "focus_opportunity_state"):
        if value.get(field) not in {"PRESENT", "ABSENT", "UNKNOWN", "NOT_APPLICABLE"}:
            errors.append(f"invalid {field}")
    if value.get("actionability_state") is not None and value.get("actionability_state") not in {item.value for item in ActionabilityState}:
        errors.append("invalid actionability_state")
    if value.get("publication_state") not in {"READY", "NOT_APPLICABLE", "BLOCKED", "PUBLISHED"}:
        errors.append("invalid publication_state")
    if kind == "stage" and not value.get("stage"):
        errors.append("missing field: stage")
    if kind == "lane" and not value.get("lane_id"):
        errors.append("missing field: lane_id")
    if kind == "run" and "run_id" not in value:
        errors.append("missing field: run_id")
    return tuple(errors)


__all__ = [
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "LEGACY_OUTCOME_SCHEMA_VERSION",
    "OUTCOME_SCHEMA_VERSION",
    "ActionabilityState",
    "DataSufficiencyState",
    "JobLifecycleState",
    "contract_hash",
    "normalize_stage",
    "normalize_lane",
    "normalize_run",
    "validate_contract",
]
