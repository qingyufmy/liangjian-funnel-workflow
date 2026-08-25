from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CapabilityStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"
    UNVERIFIED = "UNVERIFIED"
    PARTIAL = "PARTIAL"


class RunSlot(StrEnum):
    MORNING_0925 = "MORNING_0925"
    CLOSE_1510 = "CLOSE_1510"


class RunStatus(StrEnum):
    CREATED = "CREATED"
    DATA_PREPARING = "DATA_PREPARING"
    DATA_BOUND = "DATA_BOUND"
    RUNNING = "RUNNING"
    READY_TO_PUBLISH = "READY_TO_PUBLISH"
    PUBLISHED = "PUBLISHED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class StageStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    VALIDATED = "VALIDATED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class SourcedValue(BaseModel):
    model_config = ConfigDict(frozen=True)

    value: Any
    source_id: str
    event_time: datetime
    publish_time: datetime
    fetch_time: datetime
    ingest_time: datetime

    @field_validator("event_time", "publish_time", "fetch_time", "ingest_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("all source timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_order(self) -> "SourcedValue":
        if not self.event_time <= self.publish_time <= self.fetch_time <= self.ingest_time:
            raise ValueError("source timestamps must satisfy event <= publish <= fetch <= ingest")
        return self


class SnapshotSource(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_id: str
    record_count: int = Field(ge=0)
    checksum: str = Field(min_length=16)
    fetch_time: datetime

    @field_validator("fetch_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        return value


class SnapshotManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    as_of: datetime
    frozen: Literal[True] = True
    sources: tuple[SnapshotSource, ...]
    coverage_by_field: dict[str, float]
    stale_fields: tuple[str, ...] = ()
    snapshot_hash: str = Field(min_length=16)
    adjust_factor_version: str
    exchange_rule_snapshot_id: str

    @field_validator("as_of")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot as_of must be timezone-aware")
        return value

    @field_validator("coverage_by_field")
    @classmethod
    def coverage_range(cls, value: dict[str, float]) -> dict[str, float]:
        if any(item < 0 or item > 1 for item in value.values()):
            raise ValueError("coverage must be between 0 and 1")
        return value


class DataQualityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    as_of: datetime
    lag_by_source: dict[str, dict[str, float]] = Field(default_factory=dict)
    stale_fields: tuple[str, ...] = ()
    sla_violations: tuple[dict[str, Any], ...] = ()
    coverage_by_field: dict[str, float] = Field(default_factory=dict)
    missing_symbols: tuple[str, ...] = ()
    missing_bars: tuple[tuple[str, str], ...] = ()
    conflicts: tuple[dict[str, Any], ...] = ()
    pit_violations: tuple[str, ...] = ()
    data_quality_score: float = Field(ge=0, le=100)
    blocking: bool
    degraded_agents: tuple[str, ...] = ()

    @field_validator("as_of")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("data quality as_of must be timezone-aware")
        return value


class CapabilityCheck(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    status: CapabilityStatus
    latency_ms: int | None = Field(default=None, ge=0)
    http_status: int | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
    reason_code: str | None = None


class CapabilityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "liangjian-capability-report/1.0.0"
    provider: str
    generated_at: datetime
    overall_status: CapabilityStatus
    checks: tuple[CapabilityCheck, ...]
    secrets_redacted: bool = True

    @model_validator(mode="after")
    def pass_requires_all_checks(self) -> "CapabilityReport":
        if self.overall_status is CapabilityStatus.PASS and any(
            check.status is not CapabilityStatus.PASS for check in self.checks
        ):
            raise ValueError("PASS report requires every check to PASS")
        return self


class ResearchRun(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    trade_date: str
    slot: RunSlot
    model: str
    status: RunStatus = RunStatus.CREATED
    snapshot_hash: str | None = Field(default=None, min_length=16)
    prompt_hash: str | None = Field(default=None, min_length=16)
    config_hash: str | None = Field(default=None, min_length=16)

    def prepare_data(self) -> "ResearchRun":
        if self.status is not RunStatus.CREATED:
            raise ValueError("only CREATED run can prepare data")
        return self.model_copy(update={"status": RunStatus.DATA_PREPARING})

    def bind_data(self, *, snapshot_hash: str, prompt_hash: str, config_hash: str) -> "ResearchRun":
        if self.status is not RunStatus.DATA_PREPARING:
            raise ValueError("run data can only be bound from DATA_PREPARING")
        if not all((snapshot_hash, prompt_hash, config_hash)):
            raise ValueError("all immutable hashes are required")
        return self.model_copy(update={
            "status": RunStatus.DATA_BOUND,
            "snapshot_hash": snapshot_hash,
            "prompt_hash": prompt_hash,
            "config_hash": config_hash,
        })

    def transition(self, status: RunStatus, **updates: Any) -> "ResearchRun":
        if self.status not in {RunStatus.CREATED, RunStatus.DATA_PREPARING}:
            for field in ("snapshot_hash", "prompt_hash", "config_hash"):
                if field in updates and updates[field] != getattr(self, field):
                    raise ValueError(f"{field} is immutable after DATA_BOUND")
        allowed = {
            RunStatus.DATA_BOUND: {RunStatus.RUNNING, RunStatus.BLOCKED, RunStatus.FAILED, RunStatus.EXPIRED},
            RunStatus.RUNNING: {RunStatus.READY_TO_PUBLISH, RunStatus.BLOCKED, RunStatus.FAILED, RunStatus.EXPIRED},
            RunStatus.READY_TO_PUBLISH: {RunStatus.PUBLISHED, RunStatus.BLOCKED, RunStatus.FAILED, RunStatus.EXPIRED},
        }
        if status not in allowed.get(self.status, set()):
            raise ValueError(f"illegal run transition: {self.status.value} -> {status.value}")
        return self.model_copy(update={"status": status, **updates})


class StageRun(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage: Literal["A1", "A2", "A3"]
    status: StageStatus = StageStatus.PENDING

    def transition(self, status: StageStatus) -> "StageRun":
        allowed = {
            StageStatus.PENDING: {StageStatus.RUNNING, StageStatus.BLOCKED, StageStatus.EXPIRED},
            StageStatus.RUNNING: {StageStatus.VALIDATED, StageStatus.BLOCKED, StageStatus.FAILED, StageStatus.EXPIRED},
        }
        if status not in allowed.get(self.status, set()):
            raise ValueError(f"illegal stage transition: {self.status.value} -> {status.value}")
        return self.model_copy(update={"status": status})
