"""Pure, source-labelled observation evidence; no OS sampler or run gate.

The caller supplies every observation and lifecycle declaration. Content
binding does not authenticate acquisition, execution, or a host's identity.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any


_MAX_EXACT = 2**53 - 1
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$")
_MEMORY_UNITS = {"bytes": 1, "KiB": 1024, "MiB": 1024**2}
_METRICS = {
    "RSS_CURRENT": ("memory", "process", {"LINUX_PROC_STATM", "LINUX_PROC_STATUS", "WINDOWS_PSAPI"}),
    "PROCESS_RSS_LIFETIME_PEAK": ("memory", "process", {"GETRUSAGE_LINUX", "GETRUSAGE_DARWIN", "LINUX_PROC_STATUS", "WINDOWS_PSAPI"}),
    "SYSTEM_SWAP_USED": ("memory", "host", {"LINUX_PROC_MEMINFO"}),
    "PROCESS_VMSWAP": ("memory", "process", {"LINUX_PROC_STATUS"}),
    "PAGECACHE_CACHED": ("memory", "host", {"LINUX_PROC_MEMINFO"}),
    "PAGECACHE_DIRTY": ("memory", "host", {"LINUX_PROC_MEMINFO"}),
    "PAGECACHE_WRITEBACK": ("memory", "host", {"LINUX_PROC_MEMINFO"}),
    "PSI_MEMORY_SOME_TOTAL_US": ("us", "host", {"LINUX_PSI_MEMORY"}),
    "PSI_MEMORY_FULL_TOTAL_US": ("us", "host", {"LINUX_PSI_MEMORY"}),
    "CGROUP_MEMORY_CURRENT": ("memory", "cgroup", {"LINUX_CGROUP_V2"}),
    "CGROUP_MEMORY_EVENTS_OOM": ("count", "cgroup", {"LINUX_CGROUP_V2"}),
    "CGROUP_MEMORY_EVENTS_OOM_KILL": ("count", "cgroup", {"LINUX_CGROUP_V2"}),
}
_COUNTERS = {name for name, (unit, _, _) in _METRICS.items() if unit in {"count", "us"}}
_SUPPORT = {"AVAILABLE", "UNSUPPORTED", "UNAVAILABLE", "ERROR"}
_STATUSES = {"RUNNING", "SUCCEEDED", "FAILED", "TERMINATED"}
_TERMINATIONS = {"EXIT_FAILURE", "EXCEPTION", "SIGTERM", "SIGKILL", "UNKNOWN_TERMINATION"}


def _identifier(value: Any) -> None:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("IDENTITY_INVALID")


def _time(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("AWARE_TIME_REQUIRED")
    return value.astimezone(timezone.utc)


def _identity_fields(value: Any) -> None:
    for field in ("run_id", "invocation_id", "host_id"):
        _identifier(getattr(value, field))
    if type(value.pid) is not int or value.pid < 1 or value.pid > _MAX_EXACT:
        raise ValueError("PID_INVALID")
    if value.cgroup_id is not None:
        _identifier(value.cgroup_id)


@dataclass(frozen=True, slots=True)
class MetricObservation:
    metric: str
    value: int | float | None
    unit: str | None
    support: str
    source: str

    def __post_init__(self) -> None:
        if self.metric not in _METRICS or self.support not in _SUPPORT:
            raise ValueError("METRIC_OR_SUPPORT_INVALID")
        dimension, _, allowed_sources = _METRICS[self.metric]
        if self.source not in allowed_sources | {"FIXTURE", "NONE"}:
            raise ValueError("METRIC_SOURCE_INVALID")
        units = _MEMORY_UNITS if dimension == "memory" else {dimension: 1}
        if self.support != "AVAILABLE":
            if self.value is not None or self.unit not in {None, *units}:
                raise ValueError("UNSUPPORTED_VALUE_MUST_BE_NULL")
            return
        if self.source == "NONE" or self.unit not in units:
            raise ValueError("AVAILABLE_SOURCE_AND_UNIT_REQUIRED")
        if type(self.value) not in (int, float):
            raise ValueError("METRIC_NUMBER_INVALID")
        if self.value < 0 or self.value > _MAX_EXACT:
            raise ValueError("METRIC_NUMBER_OUT_OF_RANGE")
        if not math.isfinite(self.value):
            raise ValueError("METRIC_NUMBER_INVALID")
        if dimension != "memory" and self.value != int(self.value):
            raise ValueError("COUNTER_MUST_BE_INTEGRAL")
        if self.source == "GETRUSAGE_LINUX" and self.unit != "KiB":
            raise ValueError("LINUX_RUSAGE_UNIT_MUST_BE_KIB")
        if self.source in {"LINUX_PROC_STATUS", "LINUX_PROC_MEMINFO"} and self.unit != "KiB":
            raise ValueError("PROC_SOURCE_UNIT_MUST_BE_KIB")
        if self.source in {"GETRUSAGE_DARWIN", "WINDOWS_PSAPI", "LINUX_PROC_STATM", "LINUX_CGROUP_V2"} and dimension == "memory" and self.unit != "bytes":
            raise ValueError("SOURCE_UNIT_MUST_BE_BYTES")
        if self.value * units[self.unit] > _MAX_EXACT:
            raise ValueError("NORMALIZED_NUMBER_OUT_OF_RANGE")

    def normalized_value(self) -> int | float | None:
        if self.support != "AVAILABLE":
            return None
        multiplier = _MEMORY_UNITS.get(self.unit, 1)
        value = self.value * multiplier
        return int(value) if value == int(value) else value


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    run_id: str
    invocation_id: str
    pid: int
    process_started_at: datetime
    host_id: str
    observed_at: datetime
    metrics: tuple[MetricObservation, ...]
    cgroup_id: str | None = None

    def __post_init__(self) -> None:
        _identity_fields(self)
        object.__setattr__(self, "process_started_at", _time(self.process_started_at))
        object.__setattr__(self, "observed_at", _time(self.observed_at))
        if self.process_started_at > self.observed_at:
            raise ValueError("OBSERVATION_BEFORE_PROCESS_START")
        if not isinstance(self.metrics, (tuple, list)) or not all(isinstance(m, MetricObservation) for m in self.metrics):
            raise ValueError("METRIC_OBJECTS_REQUIRED")
        object.__setattr__(self, "metrics", tuple(self.metrics))
        if len({m.metric for m in self.metrics}) != len(self.metrics):
            raise ValueError("DUPLICATE_METRIC")
        if self.cgroup_id is None and any(_METRICS[m.metric][1] == "cgroup" and m.support == "AVAILABLE" for m in self.metrics):
            raise ValueError("CGROUP_ID_REQUIRED")


@dataclass(frozen=True, slots=True)
class RunResourceWindow:
    run_id: str
    invocation_id: str
    pid: int
    process_started_at: datetime
    host_id: str
    started_at: datetime
    observed_until: datetime
    status: str
    ended_at: datetime | None = None
    termination: str | None = None
    cgroup_id: str | None = None

    def __post_init__(self) -> None:
        _identity_fields(self)
        for field in ("process_started_at", "started_at", "observed_until"):
            object.__setattr__(self, field, _time(getattr(self, field)))
        if self.ended_at is not None:
            object.__setattr__(self, "ended_at", _time(self.ended_at))
        if self.status not in _STATUSES or not self.process_started_at <= self.started_at <= self.observed_until:
            raise ValueError("WINDOW_INVALID")
        if self.ended_at is not None and not self.started_at <= self.ended_at <= self.observed_until:
            raise ValueError("END_OUTSIDE_WINDOW")
        if self.status == "RUNNING" and (self.ended_at is not None or self.termination is not None):
            raise ValueError("RUNNING_CANNOT_DECLARE_END")
        if self.status == "SUCCEEDED" and (self.ended_at is None or self.termination is not None):
            raise ValueError("SUCCESS_REQUIRES_NORMAL_END")
        if self.status == "FAILED" and (self.ended_at is None or self.termination not in {"EXIT_FAILURE", "EXCEPTION"}):
            raise ValueError("FAILURE_REQUIRES_EXPLICIT_END")
        if self.status == "TERMINATED" and self.termination not in _TERMINATIONS - {"EXIT_FAILURE", "EXCEPTION"}:
            raise ValueError("TERMINATION_REQUIRED")


def evidence_hash(value: Any) -> str:
    """Hash strict JSON content; never coerce unsupported objects to text."""
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class RunResourceEvidenceBuilder:
    """Build one invocation, not an aggregate across processes or reentries."""

    def build(self, window: RunResourceWindow, observations: Sequence[ResourceObservation]) -> dict[str, Any]:
        if not isinstance(window, RunResourceWindow) or not isinstance(observations, (tuple, list)):
            raise ValueError("NORMAL_CONSTRUCTED_INPUTS_REQUIRED")
        samples = tuple(observations)
        if len(samples) > 100_000 or not all(isinstance(o, ResourceObservation) for o in samples):
            raise ValueError("OBSERVATION_SEQUENCE_INVALID")
        upper = window.ended_at or window.observed_until
        previous_time = None
        sources: dict[str, str] = {}
        previous_counters: dict[str, int | float] = {}
        rendered = []
        gaps: list[str] = []
        for index, sample in enumerate(samples):
            for field in ("run_id", "invocation_id", "pid", "process_started_at", "host_id", "cgroup_id"):
                if getattr(sample, field) != getattr(window, field):
                    raise ValueError("OBSERVATION_IDENTITY_MISMATCH")
            if not window.started_at <= sample.observed_at <= upper:
                raise ValueError("OBSERVATION_OUTSIDE_WINDOW")
            if previous_time is not None and sample.observed_at <= previous_time:
                raise ValueError("OBSERVATIONS_NOT_STRICTLY_ORDERED")
            previous_time = sample.observed_at
            row: dict[str, Any] = {}
            for metric in sample.metrics:
                number = metric.normalized_value()
                if metric.support == "AVAILABLE":
                    if metric.metric in sources and sources[metric.metric] != metric.source:
                        raise ValueError("METRIC_SOURCE_CHANGED")
                    sources[metric.metric] = metric.source
                    if metric.metric in _COUNTERS | {"PROCESS_RSS_LIFETIME_PEAK"}:
                        if metric.metric in previous_counters and number < previous_counters[metric.metric]:
                            raise ValueError("CUMULATIVE_METRIC_DECREASED")
                        previous_counters[metric.metric] = number
                else:
                    gaps.append(f"METRIC_{metric.support}:{index}:{metric.metric}")
                row[metric.metric] = {"value": metric.value, "unit": metric.unit,
                    "support": metric.support, "source": metric.source,
                    "scope": _METRICS[metric.metric][1], "normalized_value": number,
                    "normalized_unit": "bytes" if _METRICS[metric.metric][0] == "memory" else _METRICS[metric.metric][0]}
            for missing in sorted(set(_METRICS) - set(row)):
                gaps.append(f"METRIC_MISSING:{index}:{missing}")
            rss = row.get("RSS_CURRENT", {}).get("normalized_value")
            peak = row.get("PROCESS_RSS_LIFETIME_PEAK", {}).get("normalized_value")
            if rss is not None and peak is not None and rss > peak:
                raise ValueError("RSS_ABOVE_LIFETIME_HIGH_WATER_MARK")
            rendered.append({"observed_at": sample.observed_at.isoformat(), "metrics": row})
        if not samples:
            gaps.append("NO_SAMPLES")
        start = rendered[0] if samples and samples[0].observed_at == window.started_at else None
        end = rendered[-1] if samples and window.ended_at is not None and samples[-1].observed_at == window.ended_at else None
        if start is None:
            gaps.append("START_SAMPLE_MISSING")
        if window.status == "RUNNING":
            gaps.append("RUN_NOT_FINISHED")
        if window.status == "TERMINATED":
            gaps.append("RUN_TERMINATED_NOT_COMPLETED")
        if window.ended_at is None or end is None:
            gaps.append("END_SAMPLE_MISSING")

        def value(row: Mapping[str, Any] | None, name: str) -> int | float | None:
            return row["metrics"].get(name, {}).get("normalized_value") if row is not None else None

        def maximum(name: str) -> int | float | None:
            values = [value(row, name) for row in rendered if value(row, name) is not None]
            return max(values) if values else None

        swap_start, swap_end = value(start, "SYSTEM_SWAP_USED"), value(end, "SYSTEM_SWAP_USED")
        coverage_complete = bool(start is not None and end is not None and not gaps)
        result: dict[str, Any] = {
            "schema_version": "run-resource-evidence/1",
            "implementation_status": "IMPLEMENTATION_PARTIAL",
            "evidence_status": "LOCAL_OBSERVATION_BOUND" if coverage_complete else "DATA_LIMITED",
            "run_id": window.run_id, "invocation_id": window.invocation_id, "pid": window.pid,
            "process_started_at": window.process_started_at.isoformat(), "host_id": window.host_id,
            "cgroup_id": window.cgroup_id, "started_at": window.started_at.isoformat(),
            "observed_until": window.observed_until.isoformat(),
            "declared_ended_at": window.ended_at.isoformat() if window.ended_at else None,
            "declared_status": window.status, "declared_termination": window.termination,
            "observation_status": ("INTERRUPTED" if window.status == "TERMINATED" else
                "INCOMPLETE_WINDOW" if window.status == "SUCCEEDED" and not coverage_complete else window.status),
            "run_completed": window.status == "SUCCEEDED" and coverage_complete,
            "window_coverage_complete": coverage_complete,
            "acquisition_authenticated": False, "run_execution_authenticated": False,
            "eligibility_released": False, "sample_count": len(samples), "samples": rendered,
            "last_observed_at": samples[-1].observed_at.isoformat() if samples else None,
            "sampled_rss_peak_lower_bound_bytes": maximum("RSS_CURRENT"),
            "sampled_peak_is_exact_run_peak": False,
            "process_lifetime_peak_reported_max_bytes": maximum("PROCESS_RSS_LIFETIME_PEAK"),
            "lifetime_peak_is_run_peak": False,
            "system_swap": {"start_bytes": swap_start, "end_bytes": swap_end,
                "end_minus_start_bytes": swap_end - swap_start if swap_start is not None and swap_end is not None else None,
                "scope": "host", "attributable_to_this_run": False},
            "process_vmswap": {"start_bytes": value(start, "PROCESS_VMSWAP"),
                "end_bytes": value(end, "PROCESS_VMSWAP"), "sampled_max_bytes": maximum("PROCESS_VMSWAP"),
                "scope": "process"},
            "pressure_is_capacity_decision": False, "gap_codes": sorted(set(gaps)),
        }
        result["evidence_hash"] = evidence_hash(result)
        return result


def validate_run_resource_evidence(receipt: Mapping[str, Any], window: RunResourceWindow,
                                  observations: Sequence[ResourceObservation]) -> dict[str, Any]:
    """Bind to original inputs, not a receipt's self-asserted recomputed hash."""
    try:
        expected = RunResourceEvidenceBuilder().build(window, observations)
        supplied = dict(receipt)
        raw_hash = supplied.pop("evidence_hash", None)
        valid = raw_hash == evidence_hash(supplied) and evidence_hash(receipt) == evidence_hash(expected)
    except (ValueError, TypeError, AttributeError, OverflowError):
        valid = False
    return {"valid": valid, "eligibility_released": False,
            "acquisition_authenticated": False, "run_execution_authenticated": False}


__all__ = ["MetricObservation", "ResourceObservation", "RunResourceWindow",
           "RunResourceEvidenceBuilder", "evidence_hash", "validate_run_resource_evidence"]
