"""Canonical, serializable outcome contract for the research workflow.

The historical workflow used one ``status`` string for several independent
questions: has the work finished, is the evidence good enough, did the stage
find an opportunity, and may the result be published.  This module keeps
those questions separate.  It is deliberately a pure-data module: it does
not access the database, call a provider, or make a scheduling decision.

``legacy_status`` is a compatibility projection only.  New callers should
use the four axes and ``reason_codes``.  The projection functions are kept in
one place so CLI, workflow and the control plane cannot silently grow
different meanings for ``BLOCKED`` or a zero-sized pool.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import IntEnum, StrEnum
from typing import Any, TypeVar


OUTCOME_SCHEMA_VERSION = "research-outcome/2.0.0"
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_.:-]{0,119}$")
_STAGES = ("A1", "A2", "A3")


class LifecycleState(StrEnum):
    """Whether the unit of work is waiting, executing, or terminal."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    TERMINAL = "TERMINAL"


class QualityState(StrEnum):
    """Quality of the evidence and execution, independent of opportunity."""

    VALIDATED = "VALIDATED"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class OpportunityState(StrEnum):
    """What can be concluded about an opportunity from the available data."""

    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class PublicationState(StrEnum):
    """Whether the result is eligible for publication or already published."""

    READY = "READY"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    BLOCKED = "BLOCKED"
    PUBLISHED = "PUBLISHED"


class CliExitCode(IntEnum):
    """Stable process exit semantics for a completed workflow command."""

    SUCCESS = 0
    BUSINESS_BLOCKED = 2
    TECHNICAL_FAILURE = 3
    CONTRACT_ERROR = 4
    CANCELLED = 130


_T = TypeVar("_T")


def _text(value: Any, default: str = "") -> str:
    if isinstance(value, StrEnum):
        return value.value
    if value is None:
        return default
    return str(value).strip()


def _status(value: Any) -> str:
    return _text(value).upper()


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer():
        return int(value) if value >= 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _dedupe_reason_codes(values: Sequence[Any] | None) -> tuple[str, ...]:
    """Normalize safe reason codes while preserving their first-seen order."""

    result: list[str] = []
    if values is None:
        return ()
    if isinstance(values, str):
        values = (values,)
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip().upper()
        if token and _REASON_CODE.fullmatch(token) and token not in result:
            result.append(token)
    return tuple(result)


def _normalize_counts(
    counts: Mapping[str, Any] | None = None,
    *,
    input_count: Any = None,
    evaluated_count: Any = None,
    selected_count: Any = None,
) -> dict[str, int]:
    result: dict[str, int] = {}
    if isinstance(counts, Mapping):
        for raw_key, raw_value in counts.items():
            key = str(raw_key).strip()
            parsed = _nonnegative_int(raw_value)
            if key and parsed is not None:
                result[key] = parsed
    for key, raw_value in (
        ("input", input_count),
        ("evaluated", evaluated_count),
        ("selected", selected_count),
    ):
        parsed = _nonnegative_int(raw_value)
        if parsed is not None:
            result[key] = parsed
    return result


def _normalize_coverage(value: Mapping[str, Any] | None) -> dict[str, float | int | str | None]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, float | int | str | None] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key).strip()
        if not key:
            continue
        if raw_value is None or isinstance(raw_value, str):
            result[key] = raw_value
            continue
        if isinstance(raw_value, bool):
            result[key] = int(raw_value)
            continue
        if isinstance(raw_value, (int, float)):
            result[key] = raw_value
    return result


def _coverage_sufficient(
    counts: Mapping[str, int],
    coverage: Mapping[str, float | int | str | None],
) -> bool | None:
    """Return evidence sufficiency without treating a proxy as coverage."""

    required = _number(coverage.get("required"))
    actual = _number(coverage.get("actual"))
    if required is not None and actual is not None:
        return actual >= required
    input_count = counts.get("input")
    evaluated_count = counts.get("evaluated")
    if input_count is not None and evaluated_count is not None and input_count > 0:
        return evaluated_count >= input_count
    return None


def _derived_reason(status: str, stage: str) -> str | None:
    mapping = {
        "VALIDATED_NO_OPPORTUNITY": "A2_NO_FOCUS_OPPORTUNITY" if stage == "A2" else "NO_OPPORTUNITY",
        "VALIDATED_NO_ACTION": "A3_NO_ACTION",
        "VALIDATED_NO_SETUP": "A3_NO_TECHNICAL_SETUP",
        "VALIDATED_UNDERFILLED_MARKET": "POOL_UNDERFILLED_MARKET",
        "DEGRADED_UNDERFILLED_DATA_GAP": "DATA_COVERAGE_INSUFFICIENT",
        "BLOCKED_DATA_COVERAGE": "DATA_COVERAGE_INSUFFICIENT",
        "BLOCKED_EVIDENCE_GAP": "EVIDENCE_GAP",
        "BLOCKED_MODEL": "MODEL_CALL_FAILED",
        "BLOCKED_TECHNICAL_DATA": "TECHNICAL_DATA_UNAVAILABLE",
        "NOT_RUN_UPSTREAM_BLOCKED": "UPSTREAM_STAGE_BLOCKED",
        "CANCELLED": "RUN_CANCELLED",
        "CANCELED": "RUN_CANCELLED",
    }
    return mapping.get(status)


def _quality_for_status(status: str) -> QualityState:
    if status in {"CANCELLED", "CANCELED"}:
        return QualityState.CANCELLED
    if status in {"FAILED", "BLOCKED_MODEL", "MODEL_FAILED", "MODEL_CALL_FAILED"}:
        return QualityState.FAILED
    if status in {
        "BLOCKED",
        "BLOCKED_DATA_COVERAGE",
        "BLOCKED_EVIDENCE_GAP",
        "BLOCKED_TECHNICAL_DATA",
        "NOT_RUN_UPSTREAM_BLOCKED",
        "EXPIRED",
    }:
        return QualityState.BLOCKED
    if status in {
        "DEGRADED",
        "DEGRADED_UNDERFILLED_DATA_GAP",
        "READY_DEGRADED",
        "VALIDATED_UNDERFILLED_MARKET",
    }:
        return QualityState.DEGRADED
    if status in {"PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"}:
        # Until evidence is terminally validated, DEGRADED is the honest
        # quality value; lifecycle_state carries the more precise progress.
        return QualityState.DEGRADED
    return QualityState.VALIDATED


def _lifecycle_for_status(status: str) -> LifecycleState:
    if status in {"PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"}:
        return LifecycleState.QUEUED
    if status == "RUNNING":
        return LifecycleState.RUNNING
    return LifecycleState.TERMINAL


def _opportunity_for_status(
    status: str,
    *,
    stage: str,
    counts: Mapping[str, int],
    coverage: Mapping[str, float | int | str | None],
) -> OpportunityState:
    if status in {"NOT_RUN_UPSTREAM_BLOCKED", "PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED", "RUNNING"}:
        return OpportunityState.NOT_APPLICABLE
    if status in {"BLOCKED", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_MODEL", "BLOCKED_TECHNICAL_DATA", "FAILED", "EXPIRED", "CANCELLED", "CANCELED"}:
        return OpportunityState.UNKNOWN
    if status in {"VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"}:
        return OpportunityState.ABSENT
    if status in {"VALIDATED_UNDERFILLED_MARKET", "READY", "READY_DEGRADED", "READY_TO_PUBLISH", "PUBLISHED"}:
        return OpportunityState.PRESENT
    selected = counts.get("selected")
    if selected is not None:
        if selected > 0:
            return OpportunityState.PRESENT
        if _coverage_sufficient(counts, coverage) is True:
            return OpportunityState.ABSENT
    return OpportunityState.UNKNOWN


def _publication_for_status(status: str) -> PublicationState:
    if status == "PUBLISHED":
        return PublicationState.PUBLISHED
    if status in {"READY", "READY_DEGRADED", "READY_TO_PUBLISH"}:
        return PublicationState.READY
    if status in {
        "BLOCKED",
        "BLOCKED_DATA_COVERAGE",
        "BLOCKED_EVIDENCE_GAP",
        "BLOCKED_MODEL",
        "BLOCKED_TECHNICAL_DATA",
        "FAILED",
        "EXPIRED",
        "CANCELLED",
        "CANCELED",
        "NOT_RUN_UPSTREAM_BLOCKED",
    }:
        return PublicationState.BLOCKED
    return PublicationState.NOT_APPLICABLE


@dataclass(frozen=True)
class StageOutcome:
    """Canonical result of one A1/A2/A3 stage."""

    stage: str
    lifecycle_state: LifecycleState
    quality_state: QualityState
    opportunity_state: OpportunityState
    publication_state: PublicationState
    reason_codes: tuple[str, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    data_coverage: Mapping[str, float | int | str | None] = field(default_factory=dict)
    legacy_status: str = "UNKNOWN"

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", _text(self.stage, "UNKNOWN").upper())
        object.__setattr__(self, "lifecycle_state", LifecycleState(self.lifecycle_state))
        object.__setattr__(self, "quality_state", QualityState(self.quality_state))
        object.__setattr__(self, "opportunity_state", OpportunityState(self.opportunity_state))
        object.__setattr__(self, "publication_state", PublicationState(self.publication_state))
        object.__setattr__(self, "reason_codes", _dedupe_reason_codes(self.reason_codes))
        object.__setattr__(self, "counts", _normalize_counts(self.counts))
        object.__setattr__(self, "data_coverage", _normalize_coverage(self.data_coverage))
        object.__setattr__(self, "legacy_status", _status(self.legacy_status) or "UNKNOWN")

    @classmethod
    def from_legacy(cls, status: Any, **kwargs: Any) -> "StageOutcome":
        return stage_outcome_from_legacy(status, **kwargs)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, default_stage: str = "UNKNOWN") -> "StageOutcome":
        if not isinstance(value, Mapping):
            raise TypeError("stage outcome must be a mapping")
        if all(key in value for key in ("lifecycle_state", "quality_state", "opportunity_state", "publication_state")):
            return cls(
                stage=str(value.get("stage") or default_stage),
                lifecycle_state=value["lifecycle_state"],
                quality_state=value["quality_state"],
                opportunity_state=value["opportunity_state"],
                publication_state=value["publication_state"],
                reason_codes=value.get("reason_codes") if isinstance(value.get("reason_codes"), Sequence) else (),
                counts=value.get("counts") if isinstance(value.get("counts"), Mapping) else {},
                data_coverage=value.get("data_coverage") if isinstance(value.get("data_coverage"), Mapping) else {},
                legacy_status=str(value.get("legacy_status") or value.get("status") or "UNKNOWN"),
            )
        return stage_outcome_from_legacy(
            value.get("status"),
            stage=str(value.get("stage") or default_stage),
            reason_codes=value.get("reason_codes") if isinstance(value.get("reason_codes"), Sequence) else (),
            counts=value.get("counts") if isinstance(value.get("counts"), Mapping) else {},
            data_coverage=value.get("data_coverage") if isinstance(value.get("data_coverage"), Mapping) else {},
            input_count=value.get("input_count"),
            evaluated_count=value.get("evaluated_count"),
            selected_count=value.get("selected_count", value.get("output_count")),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OUTCOME_SCHEMA_VERSION,
            "stage": self.stage,
            "lifecycle_state": self.lifecycle_state.value,
            "quality_state": self.quality_state.value,
            "opportunity_state": self.opportunity_state.value,
            "publication_state": self.publication_state.value,
            "reason_codes": list(self.reason_codes),
            "counts": dict(sorted(self.counts.items())),
            "data_coverage": dict(sorted(self.data_coverage.items())),
            "legacy_status": self.legacy_status,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stage_outcome_from_legacy(
    status: Any,
    *,
    stage: str = "UNKNOWN",
    reason_codes: Sequence[Any] = (),
    counts: Mapping[str, Any] | None = None,
    data_coverage: Mapping[str, Any] | None = None,
    input_count: Any = None,
    evaluated_count: Any = None,
    selected_count: Any = None,
) -> StageOutcome:
    """Project one historical status into the four-axis contract.

    A detailed ``VALIDATED_NO_OPPORTUNITY`` is trusted as a positive empty
    conclusion unless the supplied counts/coverage explicitly contradict it.
    This prevents old artifacts from becoming ``UNKNOWN`` merely because they
    predate the new count fields, while still refusing to call an under-covered
    zero result a real market conclusion.
    """

    text = _status(status) or "UNKNOWN"
    stage_text = _text(stage, "UNKNOWN").upper()
    normalized_counts = _normalize_counts(
        counts,
        input_count=input_count,
        evaluated_count=evaluated_count,
        selected_count=selected_count,
    )
    normalized_coverage = _normalize_coverage(data_coverage)
    derived = _derived_reason(text, stage_text)
    reasons = list(_dedupe_reason_codes(reason_codes))
    if derived and derived not in reasons:
        reasons.append(derived)

    quality = _quality_for_status(text)
    opportunity = _opportunity_for_status(
        text,
        stage=stage_text,
        counts=normalized_counts,
        coverage=normalized_coverage,
    )
    sufficient = _coverage_sufficient(normalized_counts, normalized_coverage)
    explicit_no_opportunity = text in {"VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP"}
    if explicit_no_opportunity and normalized_counts.get("input") == 0:
        # An empty input set is not evidence that the market has no
        # opportunity.  It is an unavailable/invalid upstream result.
        quality = QualityState.BLOCKED
        opportunity = OpportunityState.UNKNOWN
        if "DATA_COVERAGE_INSUFFICIENT" not in reasons:
            reasons.append("DATA_COVERAGE_INSUFFICIENT")
    elif explicit_no_opportunity and sufficient is False:
        quality = QualityState.BLOCKED
        opportunity = OpportunityState.UNKNOWN
        if "DATA_COVERAGE_INSUFFICIENT" not in reasons:
            reasons.append("DATA_COVERAGE_INSUFFICIENT")

    return StageOutcome(
        stage=stage_text,
        lifecycle_state=_lifecycle_for_status(text),
        quality_state=quality,
        opportunity_state=opportunity,
        publication_state=_publication_for_status(text),
        reason_codes=tuple(reasons),
        counts=normalized_counts,
        data_coverage=normalized_coverage,
        legacy_status=text,
    )


def project_stage_status(status: Any, **kwargs: Any) -> StageOutcome:
    """Named alias used by callers migrating from the old status strings."""

    return stage_outcome_from_legacy(status, **kwargs)


_QUALITY_PRIORITY = {
    QualityState.VALIDATED: 0,
    QualityState.DEGRADED: 1,
    QualityState.BLOCKED: 2,
    QualityState.FAILED: 3,
    QualityState.CANCELLED: 4,
}


def _as_stage_outcome(value: StageOutcome | Mapping[str, Any], *, default_stage: str) -> StageOutcome:
    if isinstance(value, StageOutcome):
        return value
    return StageOutcome.from_mapping(value, default_stage=default_stage)


def _union_reasons(values: Sequence[Sequence[str]]) -> tuple[str, ...]:
    result: list[str] = []
    for group in values:
        for value in group:
            if value not in result:
                result.append(value)
    return tuple(result)


def _aggregate_counts(stages: Sequence[StageOutcome]) -> dict[str, int]:
    if not stages:
        return {"stage_count": 0, "completed_stages": 0}
    result: dict[str, int] = {"stage_count": len(stages)}
    first = stages[0].counts
    last = stages[-1].counts
    # ``input`` is the lane's original universe; ``evaluated`` and
    # ``selected`` describe the terminal stage.  Summing these would count
    # the same symbols once per funnel stage and make the UI misleading.
    if "input" in first:
        result["input"] = first["input"]
    for key in ("evaluated", "selected"):
        if key in last:
            result[key] = last[key]
    result["completed_stages"] = sum(stage.lifecycle_state is LifecycleState.TERMINAL for stage in stages)
    return result


def _aggregate_coverage(stages: Sequence[StageOutcome]) -> dict[str, float | int | str | None]:
    result: dict[str, float | int | str | None] = {}
    for stage in stages:
        for key, value in stage.data_coverage.items():
            if key == "actual" and key in result:
                old = _number(result[key])
                new = _number(value)
                if old is not None and new is not None:
                    result[key] = min(old, new)
                    continue
            if key == "required" and key in result:
                old = _number(result[key])
                new = _number(value)
                if old is not None and new is not None:
                    result[key] = max(old, new)
                    continue
            result[key] = value
    return result


@dataclass(frozen=True)
class LaneOutcome:
    """Aggregated outcome for a model lane."""

    lane_id: str
    model: str | None
    lifecycle_state: LifecycleState
    quality_state: QualityState
    opportunity_state: OpportunityState
    publication_state: PublicationState
    reason_codes: tuple[str, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    data_coverage: Mapping[str, float | int | str | None] = field(default_factory=dict)
    legacy_status: str = "UNKNOWN"
    stages: tuple[StageOutcome, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "lane_id", _text(self.lane_id, "UNKNOWN"))
        object.__setattr__(self, "model", _text(self.model) or None)
        object.__setattr__(self, "lifecycle_state", LifecycleState(self.lifecycle_state))
        object.__setattr__(self, "quality_state", QualityState(self.quality_state))
        object.__setattr__(self, "opportunity_state", OpportunityState(self.opportunity_state))
        object.__setattr__(self, "publication_state", PublicationState(self.publication_state))
        object.__setattr__(self, "reason_codes", _dedupe_reason_codes(self.reason_codes))
        object.__setattr__(self, "counts", _normalize_counts(self.counts))
        object.__setattr__(self, "data_coverage", _normalize_coverage(self.data_coverage))
        object.__setattr__(self, "legacy_status", _status(self.legacy_status) or "UNKNOWN")
        object.__setattr__(self, "stages", tuple(self.stages))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LaneOutcome":
        if not isinstance(value, Mapping):
            raise TypeError("lane outcome must be a mapping")
        raw_stages = value.get("stages")
        stages = tuple(
            _as_stage_outcome(item, default_stage=_STAGES[index] if index < len(_STAGES) else "UNKNOWN")
            for index, item in enumerate(raw_stages)
            if isinstance(item, (StageOutcome, Mapping))
        ) if isinstance(raw_stages, Sequence) and not isinstance(raw_stages, (str, bytes, bytearray)) else ()
        if all(key in value for key in ("lifecycle_state", "quality_state", "opportunity_state", "publication_state")):
            return cls(
                lane_id=str(value.get("lane_id") or "UNKNOWN"),
                model=value.get("model") if value.get("model") is not None else None,
                lifecycle_state=value["lifecycle_state"],
                quality_state=value["quality_state"],
                opportunity_state=value["opportunity_state"],
                publication_state=value["publication_state"],
                reason_codes=value.get("reason_codes") if isinstance(value.get("reason_codes"), Sequence) else (),
                counts=value.get("counts") if isinstance(value.get("counts"), Mapping) else {},
                data_coverage=value.get("data_coverage") if isinstance(value.get("data_coverage"), Mapping) else {},
                legacy_status=str(value.get("legacy_status") or value.get("status") or "UNKNOWN"),
                stages=stages,
            )
        return aggregate_lane_outcome(
            stages,
            lane_id=str(value.get("lane_id") or "UNKNOWN"),
            model=value.get("model") if value.get("model") is not None else None,
            legacy_status=str(value.get("status") or value.get("legacy_status") or "UNKNOWN"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OUTCOME_SCHEMA_VERSION,
            "lane_id": self.lane_id,
            "model": self.model,
            "lifecycle_state": self.lifecycle_state.value,
            "quality_state": self.quality_state.value,
            "opportunity_state": self.opportunity_state.value,
            "publication_state": self.publication_state.value,
            "reason_codes": list(self.reason_codes),
            "counts": dict(sorted(self.counts.items())),
            "data_coverage": dict(sorted(self.data_coverage.items())),
            "legacy_status": self.legacy_status,
            "stages": [stage.as_dict() for stage in self.stages],
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _lane_from_axes(
    *,
    lane_id: str,
    model: str | None,
    legacy_status: str,
    stage: StageOutcome,
) -> LaneOutcome:
    publication = stage.publication_state
    if publication is PublicationState.NOT_APPLICABLE and stage.quality_state is QualityState.VALIDATED:
        # A terminal, complete lane with no explicit publication status is
        # publishable at lane level; stage-level no-op remains N/A.
        publication = PublicationState.READY
    return LaneOutcome(
        lane_id=lane_id,
        model=model,
        lifecycle_state=stage.lifecycle_state,
        quality_state=stage.quality_state,
        opportunity_state=stage.opportunity_state,
        publication_state=publication,
        reason_codes=stage.reason_codes,
        counts=stage.counts,
        data_coverage=stage.data_coverage,
        legacy_status=legacy_status,
        stages=(stage,),
    )


def aggregate_lane_outcome(
    stages: Sequence[StageOutcome | Mapping[str, Any]],
    *,
    lane_id: str,
    model: str | None = None,
    legacy_status: str | None = None,
) -> LaneOutcome:
    """Aggregate stages without accessing workflow or runtime state."""

    if not stages:
        if legacy_status:
            return _lane_from_axes(
                lane_id=lane_id,
                model=model,
                legacy_status=_status(legacy_status),
                stage=stage_outcome_from_legacy(legacy_status, stage="A3"),
            )
        return LaneOutcome(
            lane_id=lane_id,
            model=model,
            lifecycle_state=LifecycleState.QUEUED,
            quality_state=QualityState.BLOCKED,
            opportunity_state=OpportunityState.NOT_APPLICABLE,
            publication_state=PublicationState.NOT_APPLICABLE,
            reason_codes=("LANE_NOT_RUN",),
            counts={"stage_count": 0, "completed_stages": 0},
            legacy_status="NOT_RUN",
            stages=(),
        )

    normalized = tuple(
        _as_stage_outcome(item, default_stage=_STAGES[index] if index < len(_STAGES) else "UNKNOWN")
        for index, item in enumerate(stages)
    )
    lifecycle = (
        LifecycleState.RUNNING
        if any(item.lifecycle_state is LifecycleState.RUNNING for item in normalized)
        else LifecycleState.QUEUED
        if any(item.lifecycle_state is LifecycleState.QUEUED for item in normalized)
        else LifecycleState.TERMINAL
    )
    quality = max((item.quality_state for item in normalized), key=lambda item: _QUALITY_PRIORITY[item])
    if any(item.opportunity_state is OpportunityState.PRESENT for item in normalized):
        opportunity = OpportunityState.PRESENT
    elif any(item.opportunity_state is OpportunityState.UNKNOWN for item in normalized):
        opportunity = OpportunityState.UNKNOWN
    elif any(item.opportunity_state is OpportunityState.ABSENT for item in normalized):
        opportunity = OpportunityState.ABSENT
    else:
        opportunity = OpportunityState.NOT_APPLICABLE
    if any(item.publication_state is PublicationState.PUBLISHED for item in normalized):
        publication = PublicationState.PUBLISHED
    elif any(item.publication_state is PublicationState.READY for item in normalized) and quality not in {
        QualityState.BLOCKED,
        QualityState.FAILED,
        QualityState.CANCELLED,
    }:
        publication = PublicationState.READY
    elif lifecycle is LifecycleState.TERMINAL and quality in {QualityState.VALIDATED, QualityState.DEGRADED}:
        # A lane is a publishable research result even when A3 produced no
        # executable setup.  Stage-level no-op remains NOT_APPLICABLE; the
        # lane-level publication axis answers whether its complete analysis
        # can be published to the research view.
        publication = PublicationState.READY
    elif quality in {QualityState.BLOCKED, QualityState.FAILED, QualityState.CANCELLED}:
        publication = PublicationState.BLOCKED
    else:
        publication = PublicationState.NOT_APPLICABLE

    if legacy_status:
        projected_legacy = _status(legacy_status)
    elif lifecycle is not LifecycleState.TERMINAL:
        projected_legacy = lifecycle.value
    elif quality is QualityState.CANCELLED:
        projected_legacy = "CANCELLED"
    elif quality is QualityState.FAILED:
        projected_legacy = "FAILED"
    elif quality is QualityState.BLOCKED:
        projected_legacy = "BLOCKED"
    elif publication is PublicationState.PUBLISHED:
        projected_legacy = "PUBLISHED"
    elif publication is PublicationState.READY and quality is QualityState.DEGRADED:
        projected_legacy = "READY_DEGRADED"
    elif publication is PublicationState.READY:
        projected_legacy = "READY"
    elif opportunity is OpportunityState.ABSENT:
        projected_legacy = "VALIDATED_NO_OPPORTUNITY"
    else:
        projected_legacy = "VALIDATED"

    return LaneOutcome(
        lane_id=lane_id,
        model=model,
        lifecycle_state=lifecycle,
        quality_state=quality,
        opportunity_state=opportunity,
        publication_state=publication,
        reason_codes=_union_reasons([item.reason_codes for item in normalized]),
        counts=_aggregate_counts(normalized),
        data_coverage=_aggregate_coverage(normalized),
        legacy_status=projected_legacy,
        stages=normalized,
    )


def _ready(lane: LaneOutcome) -> bool:
    return lane.publication_state in {PublicationState.READY, PublicationState.PUBLISHED} and lane.quality_state in {
        QualityState.VALIDATED,
        QualityState.DEGRADED,
    }


@dataclass(frozen=True)
class RunOutcome:
    """Canonical result for a full run, with primary/optional lane context."""

    run_id: str | None
    lifecycle_state: LifecycleState
    quality_state: QualityState
    opportunity_state: OpportunityState
    publication_state: PublicationState
    reason_codes: tuple[str, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    data_coverage: Mapping[str, float | int | str | None] = field(default_factory=dict)
    legacy_status: str = "NOT_RUN"
    lanes: tuple[LaneOutcome, ...] = ()
    primary_lane_ids: tuple[str, ...] = ("lane_1",)
    comparison_status: str = "NOT_RUN"

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _text(self.run_id) or None)
        object.__setattr__(self, "lifecycle_state", LifecycleState(self.lifecycle_state))
        object.__setattr__(self, "quality_state", QualityState(self.quality_state))
        object.__setattr__(self, "opportunity_state", OpportunityState(self.opportunity_state))
        object.__setattr__(self, "publication_state", PublicationState(self.publication_state))
        object.__setattr__(self, "reason_codes", _dedupe_reason_codes(self.reason_codes))
        object.__setattr__(self, "counts", _normalize_counts(self.counts))
        object.__setattr__(self, "data_coverage", _normalize_coverage(self.data_coverage))
        object.__setattr__(self, "legacy_status", _status(self.legacy_status) or "NOT_RUN")
        object.__setattr__(self, "lanes", tuple(self.lanes))
        raw_primary_ids: Sequence[Any] = (
            (self.primary_lane_ids,)
            if isinstance(self.primary_lane_ids, str)
            else self.primary_lane_ids
        )
        object.__setattr__(self, "primary_lane_ids", tuple(_text(item) for item in raw_primary_ids if _text(item)))
        object.__setattr__(self, "comparison_status", _status(self.comparison_status) or "NOT_RUN")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RunOutcome":
        if not isinstance(value, Mapping):
            raise TypeError("run outcome must be a mapping")
        raw_lanes = value.get("lanes")
        lanes = tuple(
            item if isinstance(item, LaneOutcome) else LaneOutcome.from_mapping(item)
            for item in raw_lanes
            if isinstance(item, (LaneOutcome, Mapping))
        ) if isinstance(raw_lanes, Sequence) and not isinstance(raw_lanes, (str, bytes, bytearray)) else ()
        return cls(
            run_id=value.get("run_id"),
            lifecycle_state=value.get("lifecycle_state", LifecycleState.QUEUED),
            quality_state=value.get("quality_state", QualityState.BLOCKED),
            opportunity_state=value.get("opportunity_state", OpportunityState.NOT_APPLICABLE),
            publication_state=value.get("publication_state", PublicationState.NOT_APPLICABLE),
            reason_codes=value.get("reason_codes") if isinstance(value.get("reason_codes"), Sequence) else (),
            counts=value.get("counts") if isinstance(value.get("counts"), Mapping) else {},
            data_coverage=value.get("data_coverage") if isinstance(value.get("data_coverage"), Mapping) else {},
            legacy_status=str(value.get("legacy_status") or value.get("status") or "NOT_RUN"),
            lanes=lanes,
            primary_lane_ids=(
                (str(value.get("primary_lane_ids")),)
                if isinstance(value.get("primary_lane_ids"), str)
                else tuple(value.get("primary_lane_ids") or ("lane_1",))
            ),
            comparison_status=str(value.get("comparison_status") or "NOT_RUN"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OUTCOME_SCHEMA_VERSION,
            "run_id": self.run_id,
            "lifecycle_state": self.lifecycle_state.value,
            "quality_state": self.quality_state.value,
            "opportunity_state": self.opportunity_state.value,
            "publication_state": self.publication_state.value,
            "reason_codes": list(self.reason_codes),
            "counts": dict(sorted(self.counts.items())),
            "data_coverage": dict(sorted(self.data_coverage.items())),
            "legacy_status": self.legacy_status,
            "primary_lane_ids": list(self.primary_lane_ids),
            "comparison_status": self.comparison_status,
            "lanes": [lane.as_dict() for lane in self.lanes],
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def to_legacy_acceptance(self) -> dict[str, object]:
        """Return the exact shape used by the pre-v2 CLI status command."""

        return {
            "status": self.legacy_status,
            "run_id": self.run_id,
            "expected_lanes": self.counts.get("expected_lanes", len(self.lanes)),
            "recorded_lanes": self.counts.get("recorded_lanes", len(self.lanes)),
            "ready_lanes": self.counts.get("ready_lanes", sum(_ready(lane) for lane in self.lanes)),
            "required_lanes": self.counts.get("required_lanes", len(self.primary_lane_ids)),
            "ready_required_lanes": self.counts.get(
                "ready_required_lanes",
                sum(_ready(lane) for lane in self.lanes if lane.lane_id in self.primary_lane_ids),
            ),
        }


def aggregate_run_outcome(
    lanes: Sequence[LaneOutcome | Mapping[str, Any]],
    *,
    run_id: str | None = None,
    primary_lane_ids: Sequence[str] = ("lane_1",),
    expected_lane_count: int | None = None,
) -> RunOutcome:
    """Aggregate lanes; optional comparison lanes cannot block the primary."""

    normalized = tuple(
        item if isinstance(item, LaneOutcome) else LaneOutcome.from_mapping(item)
        for item in lanes
    )
    raw_primary_ids: Sequence[Any] = (
        (primary_lane_ids,)
        if isinstance(primary_lane_ids, str)
        else primary_lane_ids
    )
    primary_ids = tuple(_text(item) for item in raw_primary_ids if _text(item)) or ("lane_1",)
    primary = tuple(item for item in normalized if item.lane_id in primary_ids)
    missing_primary = tuple(item for item in primary_ids if item not in {lane.lane_id for lane in normalized})

    if not normalized:
        return RunOutcome(
            run_id=run_id,
            lifecycle_state=LifecycleState.QUEUED,
            quality_state=QualityState.BLOCKED,
            opportunity_state=OpportunityState.NOT_APPLICABLE,
            publication_state=PublicationState.NOT_APPLICABLE,
            reason_codes=("RUN_NOT_STARTED",),
            counts={
                "expected_lanes": expected_lane_count or 0,
                "recorded_lanes": 0,
                "ready_lanes": 0,
                "required_lanes": len(primary_ids),
                "ready_required_lanes": 0,
            },
            legacy_status="NOT_RUN",
            lanes=(),
            primary_lane_ids=primary_ids,
            comparison_status="NOT_RUN",
        )

    if any(item.lifecycle_state is LifecycleState.RUNNING for item in normalized):
        lifecycle = LifecycleState.RUNNING
    elif any(item.lifecycle_state is LifecycleState.QUEUED for item in normalized) and not primary:
        lifecycle = LifecycleState.QUEUED
    else:
        lifecycle = LifecycleState.TERMINAL

    ready_count = sum(_ready(item) for item in normalized)
    ready_primary_count = sum(_ready(item) for item in primary)
    required_complete = not missing_primary and ready_primary_count == len(primary_ids)
    primary_quality = tuple(item.quality_state for item in primary)
    if missing_primary:
        quality = QualityState.BLOCKED
        reasons = ["PRIMARY_LANE_MISSING"]
    elif any(item is QualityState.CANCELLED for item in primary_quality):
        quality = QualityState.CANCELLED
        reasons = ["PRIMARY_RUN_CANCELLED"]
    elif any(item is QualityState.FAILED for item in primary_quality):
        quality = QualityState.FAILED
        reasons = ["PRIMARY_MODEL_FAILED"]
    elif any(item is QualityState.BLOCKED for item in primary_quality):
        quality = QualityState.BLOCKED
        reasons = ["PRIMARY_LANE_BLOCKED"]
    elif any(item is QualityState.DEGRADED for item in primary_quality):
        quality = QualityState.DEGRADED
        reasons = []
    else:
        quality = QualityState.VALIDATED
        reasons = []
    reasons.extend(_union_reasons([item.reason_codes for item in primary]))

    if any(item.opportunity_state is OpportunityState.PRESENT for item in primary):
        opportunity = OpportunityState.PRESENT
    elif any(item.opportunity_state is OpportunityState.UNKNOWN for item in primary):
        opportunity = OpportunityState.UNKNOWN
    elif primary and all(item.opportunity_state is OpportunityState.ABSENT for item in primary):
        opportunity = OpportunityState.ABSENT
    else:
        opportunity = OpportunityState.NOT_APPLICABLE

    if required_complete and all(item.publication_state is PublicationState.PUBLISHED for item in primary):
        publication = PublicationState.PUBLISHED
    elif required_complete:
        publication = PublicationState.READY
    else:
        publication = PublicationState.BLOCKED

    all_recorded = expected_lane_count is not None and len(normalized) == expected_lane_count
    if required_complete and all_recorded and ready_count == expected_lane_count:
        legacy = (
            "READY_DEGRADED"
            if any(item.quality_state is QualityState.DEGRADED for item in normalized)
            else "READY"
        )
    elif required_complete and all_recorded:
        legacy = "READY_DEGRADED"
    elif ready_count:
        legacy = "PARTIAL"
    else:
        legacy = "BLOCKED"

    optional = tuple(item for item in normalized if item.lane_id not in primary_ids)
    if not optional:
        comparison = "NOT_RUN"
    elif all(_ready(item) for item in optional):
        comparison = "READY"
    elif any(_ready(item) for item in optional):
        comparison = "PARTIAL"
    else:
        comparison = "BLOCKED"
    if missing_primary and "PRIMARY_LANE_MISSING" not in reasons:
        reasons.append("PRIMARY_LANE_MISSING")

    counts = {
        "expected_lanes": expected_lane_count if expected_lane_count is not None else len(normalized),
        "recorded_lanes": len(normalized),
        "ready_lanes": ready_count,
        "required_lanes": len(primary_ids),
        "ready_required_lanes": ready_primary_count,
    }
    return RunOutcome(
        run_id=run_id,
        lifecycle_state=lifecycle,
        quality_state=quality,
        opportunity_state=opportunity,
        publication_state=publication,
        reason_codes=tuple(reasons),
        counts=counts,
        data_coverage=_aggregate_coverage([stage for lane in normalized for stage in lane.stages]),
        legacy_status=legacy,
        lanes=normalized,
        primary_lane_ids=primary_ids,
        comparison_status=comparison,
    )


def aggregate_workflow_acceptance(
    workflow_runs: Sequence[Mapping[str, Any]],
    *,
    expected_lanes: int = 3,
    required_lane_ids: Sequence[str] = ("lane_1",),
) -> RunOutcome:
    """Build a v2 run outcome from legacy ``workflow_runs`` rows.

    The newest row is authoritative for selecting the run, matching the old
    CLI behavior.  Rows belonging to older run IDs are ignored.  This function
    intentionally does not inspect model text or infer a result from an
    absent row; missing primary rows remain a blocked acceptance.
    """

    if not workflow_runs:
        return aggregate_run_outcome(
            (),
            expected_lane_count=expected_lanes,
            primary_lane_ids=required_lane_ids,
        )
    latest_run_id = _text(workflow_runs[0].get("run_id")) or None
    latest_rows = [row for row in workflow_runs if _text(row.get("run_id")) == (latest_run_id or "")]
    # The state store normally emits one row per lane.  Keeping the first row
    # for a duplicate lane avoids old/stale duplicates inflating readiness.
    by_lane: dict[str, Mapping[str, Any]] = {}
    for row in latest_rows:
        lane_id = _text(row.get("lane_id"), "UNKNOWN")
        by_lane.setdefault(lane_id, row)
    lanes: list[LaneOutcome] = []
    for lane_id, row in by_lane.items():
        raw_outcome = row.get("outcome")
        if not isinstance(raw_outcome, Mapping) and isinstance(row.get("outcome_json"), str):
            try:
                decoded = json.loads(str(row.get("outcome_json")))
            except (TypeError, ValueError):
                decoded = None
            raw_outcome = decoded if isinstance(decoded, Mapping) and decoded else None
        if isinstance(raw_outcome, Mapping):
            lane = LaneOutcome.from_mapping({**raw_outcome, "lane_id": lane_id, "model": row.get("model", raw_outcome.get("model"))})
        else:
            lane = aggregate_lane_outcome(
                (),
                lane_id=lane_id,
                model=row.get("model") if row.get("model") is not None else None,
                legacy_status=_text(row.get("status"), "UNKNOWN"),
            )
        lanes.append(lane)
    outcome = aggregate_run_outcome(
        lanes,
        run_id=latest_run_id,
        primary_lane_ids=required_lane_ids,
        expected_lane_count=expected_lanes,
    )
    # Preserve the old acceptance vocabulary exactly, including the case
    # where all optional lanes are absent but the primary is ready.
    ready_count = sum(_ready(lane) for lane in lanes)
    required = {str(item) for item in required_lane_ids if str(item)}
    ready_required_count = sum(_ready(lane) and lane.lane_id in required for lane in lanes)
    all_recorded = len(lanes) == expected_lanes
    if all_recorded and ready_count == expected_lanes:
        legacy = "READY"
    elif all_recorded and required and ready_required_count == len(required):
        legacy = "READY_DEGRADED"
    elif ready_count:
        legacy = "PARTIAL"
    else:
        legacy = "BLOCKED"
    return replace(
        outcome,
        legacy_status=legacy,
        counts={
            **outcome.counts,
            "expected_lanes": expected_lanes,
            "recorded_lanes": len(lanes),
            "ready_lanes": ready_count,
            "required_lanes": len(required),
            "ready_required_lanes": ready_required_count,
        },
    )


def project_run_status(status: Any, **kwargs: Any) -> RunOutcome:
    """Project a single legacy run status through the canonical lane reducer."""

    lane = aggregate_lane_outcome(
        (),
        lane_id=str(kwargs.pop("lane_id", "lane_1")),
        model=kwargs.pop("model", None),
        legacy_status=_status(status),
    )
    return aggregate_run_outcome(
        (lane,),
        run_id=kwargs.pop("run_id", None),
        primary_lane_ids=kwargs.pop("primary_lane_ids", (lane.lane_id,)),
        expected_lane_count=kwargs.pop("expected_lane_count", 1),
    )


def cli_exit_code(value: RunOutcome | Mapping[str, Any]) -> int:
    """Map a canonical outcome to a stable CLI exit code.

    Legacy payloads remain compatible: callers that have not yet emitted the
    four axes are intentionally left to their existing command-specific
    handling.  New payloads should include ``outcome_v2`` or the four axes.
    """

    outcome: RunOutcome
    if isinstance(value, RunOutcome):
        outcome = value
    elif isinstance(value, Mapping) and isinstance(value.get("outcome_v2"), Mapping):
        outcome = RunOutcome.from_mapping(value["outcome_v2"])
    elif isinstance(value, Mapping) and all(
        key in value for key in ("lifecycle_state", "quality_state", "opportunity_state", "publication_state")
    ):
        outcome = RunOutcome.from_mapping(value)
    else:
        return int(CliExitCode.SUCCESS if _status(value.get("status") if isinstance(value, Mapping) else "") in {"READY", "READY_DEGRADED", "PUBLISHED"} else CliExitCode.BUSINESS_BLOCKED)
    if outcome.quality_state is QualityState.CANCELLED:
        return int(CliExitCode.CANCELLED)
    if outcome.quality_state is QualityState.FAILED:
        return int(CliExitCode.TECHNICAL_FAILURE)
    if outcome.quality_state is QualityState.BLOCKED or outcome.publication_state is PublicationState.BLOCKED:
        return int(CliExitCode.BUSINESS_BLOCKED)
    return int(CliExitCode.SUCCESS)


# Short aliases make the migration discoverable without forcing every caller
# to remember the implementation-oriented function names.
aggregate_lanes = aggregate_lane_outcome
aggregate_run = aggregate_run_outcome
project_legacy_status = stage_outcome_from_legacy


__all__ = [
    "OUTCOME_SCHEMA_VERSION",
    "CliExitCode",
    "LifecycleState",
    "QualityState",
    "OpportunityState",
    "PublicationState",
    "StageOutcome",
    "LaneOutcome",
    "RunOutcome",
    "stage_outcome_from_legacy",
    "project_stage_status",
    "project_legacy_status",
    "aggregate_lane_outcome",
    "aggregate_lanes",
    "aggregate_run_outcome",
    "aggregate_run",
    "aggregate_workflow_acceptance",
    "project_run_status",
    "cli_exit_code",
]
