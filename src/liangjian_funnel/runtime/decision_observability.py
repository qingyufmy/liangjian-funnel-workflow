"""Canonical decision observability projection.

This module is not a second fact store.  It maps existing outcome enums and
runtime evidence into one bounded, redacted projection suitable for logs and
read-only UI.  Business decisions continue to belong to their existing
engines and ledgers.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from ..pipeline.outcomes import JobLifecycleState, OpportunityState


OBSERVABILITY_SCHEMA_VERSION = "decision-observability/1.0.0"
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_.:-]{0,119}$")
_SECRET_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "access_token",
        "refresh_token",
        "secret",
        "secret_key",
        "webhook",
        "password",
    }
)
_EXPECTED_SPAN_PREFIXES = (
    "scheduling_wait",
    "plan_restore",
    "market_state",
    "holding_quote",
    "required_minute",
    "auxiliary_archive",
    "publication_validation",
    "database_wait",
    "database_write",
    "deterministic_compute",
    "model_connection",
    "model_first_byte",
    "model_output_end",
    "model_total",
    "order_intent",
    "simulation_fill",
    "order_intent_and_simulation",
    "notification",
    "round_total",
)


class DataState(StrEnum):
    READY = "READY"
    MISSING = "MISSING"
    STALE = "STALE"
    CONFLICT = "CONFLICT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNSUPPORTED = "UNSUPPORTED"
    PENDING_PUBLICATION = "PENDING_PUBLICATION"
    BUDGET_DEFERRED = "BUDGET_DEFERRED"


class TradeEligibility(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"
    BLOCKED_DATA = "BLOCKED_DATA"
    BLOCKED_RISK = "BLOCKED_RISK"
    PENDING_REVIEW = "PENDING_REVIEW"


def _aware(value: datetime | None, field_name: str) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _reason_codes(values: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    for raw in values:
        token = str(raw).strip().upper()
        if token and _REASON_CODE.fullmatch(token) and token not in result:
            result.append(token)
    return tuple(result)


@dataclass(frozen=True)
class DecisionAxes:
    job_status: JobLifecycleState
    data_state: DataState
    opportunity_state: OpportunityState
    trade_eligibility: TradeEligibility
    reason_codes: tuple[str, ...] = ()
    critical_data: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_status": self.job_status.value,
            "data_state": self.data_state.value,
            "opportunity_state": self.opportunity_state.value,
            "trade_eligibility": self.trade_eligibility.value,
            "reason_codes": list(self.reason_codes),
            "critical_data": self.critical_data,
        }


def project_decision_axes(
    *,
    job_status: JobLifecycleState | str,
    data_state: DataState | str,
    opportunity_state: OpportunityState | str,
    critical_data: bool,
    risk_blocked: bool = False,
    pending_review: bool = False,
    reason_codes: Sequence[str] = (),
) -> DecisionAxes:
    """Project independent axes without hiding degradation as success."""

    job = JobLifecycleState(job_status)
    data = DataState(data_state)
    opportunity = OpportunityState(opportunity_state)
    if risk_blocked:
        eligibility = TradeEligibility.BLOCKED_RISK
    elif critical_data and data is not DataState.READY:
        eligibility = TradeEligibility.BLOCKED_DATA
    elif pending_review:
        eligibility = TradeEligibility.PENDING_REVIEW
    elif opportunity is OpportunityState.PRESENT:
        eligibility = TradeEligibility.ELIGIBLE
    else:
        eligibility = TradeEligibility.INELIGIBLE
    return DecisionAxes(
        job_status=job,
        data_state=data,
        opportunity_state=opportunity,
        trade_eligibility=eligibility,
        reason_codes=_reason_codes(reason_codes),
        critical_data=bool(critical_data),
    )


@dataclass(frozen=True)
class EvidenceMetadata:
    object_id: str
    field: str
    period: str
    provider: str
    upstream_source: str
    published_at: datetime | None
    fetched_at: datetime | None
    ingested_at: datetime | None
    processed_at: datetime | None
    effective_available_at: datetime | None
    snapshot_id: str
    raw_hash: str
    completeness: str
    applicable_uses: tuple[str, ...]
    unit: str | None = None
    currency: str | None = None
    adjustment: str | None = None
    amount_kind: str | None = None
    degradation_reason: str | None = None
    time_precision: str = "EXACT"
    time_limitation: str | None = None

    def __post_init__(self) -> None:
        times = {
            name: _aware(getattr(self, name), name)
            for name in (
                "published_at",
                "fetched_at",
                "ingested_at",
                "processed_at",
                "effective_available_at",
            )
        }
        known = [
            value
            for name, value in times.items()
            if name != "effective_available_at" and value is not None
        ]
        effective = times["effective_available_at"]
        if effective is not None and known and effective < max(known):
            raise ValueError("effective_available_at precedes actual availability")
        if effective is None and not self.time_limitation:
            raise ValueError("unknown effective availability requires time_limitation")
        if not self.snapshot_id or not self.raw_hash:
            raise ValueError("snapshot_id and raw_hash are required")


@dataclass(frozen=True)
class TimingSpan:
    name: str
    duration_ms: float
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("span name is required")
        if not math.isfinite(self.duration_ms) or self.duration_ms < 0:
            raise ValueError("duration_ms must be finite and nonnegative")
        _aware(self.started_at, "started_at")
        _aware(self.finished_at, "finished_at")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "duration_ms": round(self.duration_ms, 3),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def timing_percentiles(durations_ms: Sequence[float]) -> dict[str, Any]:
    values = sorted(
        float(value)
        for value in durations_ms
        if isinstance(value, (int, float)) and math.isfinite(float(value)) and float(value) >= 0
    )
    if not values:
        return {
            "status": "NOT_MEASURED",
            "count": 0,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
        }
    return {
        "status": "MEASURED",
        "count": len(values),
        "p50_ms": round(_percentile(values, 0.50), 3),
        "p95_ms": round(_percentile(values, 0.95), 3),
        "p99_ms": round(_percentile(values, 0.99), 3),
    }


def _without_query(value: str) -> str:
    try:
        split = urlsplit(value)
    except ValueError:
        return "[REDACTED_URL]"
    if split.scheme and split.netloc:
        return urlunsplit((split.scheme, split.netloc, split.path, "", ""))
    return value


def redact_observability(value: Any) -> Any:
    """Bounded recursive redaction for observability payloads."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)[:120]
            lowered = key.lower()
            if lowered in _SECRET_KEYS or any(token in lowered for token in ("credential", "private_key")):
                result[key] = "[REDACTED]"
            elif lowered in {"url", "uri", "endpoint"} and isinstance(raw_value, str):
                result[key] = _without_query(raw_value)[:500]
            else:
                result[key] = redact_observability(raw_value)
        return result
    if isinstance(value, (list, tuple)):
        return [redact_observability(item) for item in value[:1000]]
    if isinstance(value, str):
        if value.lower().startswith("bearer "):
            return "[REDACTED]"
        return value[:1000]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:1000]


def stable_correlation_id(prefix: str, *parts: Any) -> str:
    material = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}-{hashlib.sha256(material.encode('utf-8')).hexdigest()[:24]}"


@dataclass(frozen=True)
class DecisionObservation:
    run_id: str
    decision_id: str
    lane_id: str
    scheduled_at: datetime
    started_at: datetime
    deadline_at: datetime
    snapshot_ids: tuple[str, ...]
    required_scope: tuple[str, ...]
    ready_scope: tuple[str, ...]
    blocked_scope: tuple[str, ...]
    no_signal_scope: tuple[str, ...]
    timing_spans: tuple[TimingSpan, ...]
    source_attempts: tuple[Mapping[str, Any], ...]
    terminal_reason: str
    versions: Mapping[str, str]
    axes: DecisionAxes
    schema_version: str = field(default=OBSERVABILITY_SCHEMA_VERSION, init=False)

    def __post_init__(self) -> None:
        for name in ("scheduled_at", "started_at", "deadline_at"):
            _aware(getattr(self, name), name)
        required = set(self.required_scope)
        if not set(self.ready_scope).issubset(required):
            raise ValueError("ready_scope must be a subset of required_scope")
        if not set(self.blocked_scope).issubset(required):
            raise ValueError("blocked_scope must be a subset of required_scope")
        if not set(self.no_signal_scope).issubset(set(self.ready_scope)):
            raise ValueError("no_signal_scope must be a subset of ready_scope")

    @property
    def decision_hash(self) -> str:
        material = {
            "schema_version": self.schema_version,
            "snapshot_ids": sorted(set(self.snapshot_ids)),
            "required_scope": sorted(set(self.required_scope)),
            "ready_scope": sorted(set(self.ready_scope)),
            "blocked_scope": sorted(set(self.blocked_scope)),
            "no_signal_scope": sorted(set(self.no_signal_scope)),
            "terminal_reason": self.terminal_reason,
            "versions": dict(sorted((str(key), str(value)) for key, value in self.versions.items())),
            "axes": self.axes.to_dict(),
        }
        encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        measured_names = tuple(span.name for span in self.timing_spans)
        timing_coverage = {
            prefix: (
                "MEASURED"
                if any(name == prefix or name.startswith(f"{prefix}:") for name in measured_names)
                else "NOT_MEASURED"
            )
            for prefix in _EXPECTED_SPAN_PREFIXES
        }
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "decision_id": self.decision_id,
            "lane_id": self.lane_id,
            "scheduled_at": self.scheduled_at.isoformat(),
            "started_at": self.started_at.isoformat(),
            "deadline_at": self.deadline_at.isoformat(),
            "snapshot_ids": sorted(set(self.snapshot_ids)),
            "required_scope": sorted(set(self.required_scope)),
            "ready_scope": sorted(set(self.ready_scope)),
            "blocked_scope": sorted(set(self.blocked_scope)),
            "no_signal_scope": sorted(set(self.no_signal_scope)),
            "timing_spans": [span.to_dict() for span in self.timing_spans],
            "timing_summary": timing_percentiles([span.duration_ms for span in self.timing_spans]),
            "timing_coverage": timing_coverage,
            "source_attempts": redact_observability(self.source_attempts),
            "terminal_reason": self.terminal_reason,
            "versions": redact_observability(dict(self.versions)),
            "axes": self.axes.to_dict(),
            "decision_hash": self.decision_hash,
        }


__all__ = [
    "OBSERVABILITY_SCHEMA_VERSION",
    "DataState",
    "DecisionAxes",
    "DecisionObservation",
    "EvidenceMetadata",
    "TimingSpan",
    "TradeEligibility",
    "project_decision_axes",
    "redact_observability",
    "stable_correlation_id",
    "timing_percentiles",
]
