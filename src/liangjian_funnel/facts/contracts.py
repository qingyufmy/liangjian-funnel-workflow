"""Immutable contracts for normalized facts and frozen fact snapshots.

The fact layer is deliberately small.  Providers may have very different
response shapes, but the rest of the workflow receives one validated,
time-point-invariant envelope.  In particular, a missing numeric value is
kept as ``None`` in the provider payload; this module never fills it with
zero.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from enum import StrEnum
from typing import Any, ClassVar, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer, model_validator


SHANGHAI = ZoneInfo("Asia/Shanghai")
_HASH_RE = re.compile(r"^(?:sha256:)?[0-9a-fA-F]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/+\-]{0,127}$")
_URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)
_SECRET_VALUE_RE = re.compile(
    r"(?ix)"
    r"(?:\bsk-[a-z0-9][a-z0-9_-]{7,}\b)"
    r"|(?:\bbearer\s+[a-z0-9._~+/=-]{8,})"
    r"|(?:(?:api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|"
    r"client[_-]?secret|private[_-]?key|password|secret)\s*[:=]\s*[^\s,;]+)"
)
_SENSITIVE_KEY_NAMES = {
    "apikey",
    "authorization",
    "accesstoken",
    "refreshtoken",
    "clientsecret",
    "privatekey",
    "password",
    "secret",
    "token",
}


class SourceTier(StrEnum):
    """Evidence priority assigned to a provider by the source registry."""

    T0 = "T0"
    T1 = "T1"
    T2 = "T2"
    T3 = "T3"
    T4 = "T4"


class SourceHealthStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


def _secret_key_name(key: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return normalized in _SENSITIVE_KEY_NAMES or normalized.endswith("apikey")


def _reject_secrets(value: Any) -> None:
    """Reject secret-looking material before it can reach JSON serialization.

    This is intentionally a rejection policy instead of redaction.  A fact
    containing an authentication header is not an auditable fact and must be
    fixed at its source adapter.  Error text never includes the offending
    value.
    """

    if isinstance(value, Mapping):
        for key, item in value.items():
            if _secret_key_name(key):
                raise ValueError("secret-like field is not allowed in fact data")
            _reject_secrets(key)
            _reject_secrets(item)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            _reject_secrets(item)
        return
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise ValueError("secret-like value is not allowed in fact data")


def _identifier(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if not value or value != value.strip() or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{field_name} contains unsafe characters")
    _reject_secrets(value)
    return value


def _hash(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not _HASH_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a SHA-256 hex digest")
    _reject_secrets(value)
    return value.lower()


def _source_url(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _URL_RE.fullmatch(value):
        raise ValueError("source_url must be an http(s) URL")
    _reject_secrets(value)
    return value


def _shanghai(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("fact timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


def canonical_json(value: Any) -> str:
    """Return deterministic JSON used by fact and manifest hashes."""

    value = _jsonable(value)
    _reject_secrets(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def _jsonable(value: Any) -> Any:
    """Convert nested Pydantic models before handing data to ``json.dumps``."""

    if isinstance(value, BaseModel):
        return _jsonable(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_jsonable(item) for item in sorted(value, key=repr)]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _facts_hash(facts: Sequence[FactEnvelope | RealtimeFactEnvelope]) -> str:
    ordered = sorted(
        facts,
        key=lambda fact: (
            fact.fact_id,
            fact.fact_type,
            fact.symbol or "",
            canonical_json(fact),
        ),
    )
    return hashlib.sha256(canonical_json_bytes([fact for fact in ordered])).hexdigest()


class _SafeModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def reject_secret_input(cls, value: Any) -> Any:
        _reject_secrets(value)
        return value

    @model_serializer(mode="wrap")
    def serialize_without_secrets(self, handler: Any) -> Any:
        value = handler(self)
        _reject_secrets(value)
        return value


class FactEnvelope(_SafeModel):
    """A single normalized, point-in-time fact.

    ``publish_time`` is intentionally required for ordinary facts.  Data
    classes such as an intraday quote that genuinely have no provider
    publication event must use :class:`RealtimeFactEnvelope`, making the
    exception explicit in both Python and serialized data.
    """

    schema_version: str = "liangjian-fact/1.0.0"
    fact_id: str
    source_id: str
    source_tier: SourceTier | str = SourceTier.T1
    fact_type: str
    symbol: str | None = None
    event_time: datetime
    publish_time: datetime
    fetch_time: datetime
    ingest_time: datetime
    available: bool = True
    reason_code: str = "OK"
    source_url: str | None = None
    content_hash: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    _realtime_contract: ClassVar[bool] = False

    @field_validator("fact_id", "source_id", "fact_type", "reason_code")
    @classmethod
    def validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("schema_version", "source_tier")
    @classmethod
    def validate_contract_identifiers(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, field_name="symbol")

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str | None) -> str | None:
        return _source_url(value)

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str | None) -> str | None:
        return None if value is None else _hash(value, field_name="content_hash")

    @field_validator("event_time", "publish_time", "fetch_time", "ingest_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _shanghai(value)

    @model_validator(mode="after")
    def validate_fact(self) -> FactEnvelope:
        if self.publish_time is None:
            if not self._realtime_contract:
                raise ValueError("publish_time is required for ordinary facts")
        elif self.publish_time > self.fetch_time:
            raise ValueError("publish_time must be before fetch_time")
        if self.fetch_time > self.ingest_time:
            raise ValueError("fetch_time must be before ingest_time")
        if self.available and self.content_hash is None:
            raise ValueError("available facts require content_hash")
        if self.available and self.source_url is None:
            raise ValueError("available facts require source_url")
        _reject_secrets(self.payload)
        return self


class RealtimeFactEnvelope(FactEnvelope):
    """Explicit contract for facts with no meaningful publication timestamp."""

    publish_time: datetime | None = None
    realtime: Literal[True] = True
    _realtime_contract: ClassVar[bool] = True

    @field_validator("publish_time")
    @classmethod
    def normalize_optional_publish_timestamp(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _shanghai(value)

    @model_validator(mode="after")
    def validate_realtime_fact(self) -> RealtimeFactEnvelope:
        if self.publish_time is not None and self.publish_time > self.fetch_time:
            raise ValueError("publish_time must be before fetch_time")
        if self.fetch_time > self.ingest_time:
            raise ValueError("fetch_time must be before ingest_time")
        return self


class SourceHealth(_SafeModel):
    """A point-in-time health observation for one authoritative source."""

    schema_version: str = "liangjian-source-health/1.0.0"
    source_id: str
    status: SourceHealthStatus | str = SourceHealthStatus.UNKNOWN
    checked_at: datetime
    last_success_time: datetime | None = None
    reason_code: str = "UNSPECIFIED"
    coverage: float | None = Field(default=None, ge=0, le=1)
    latency_ms: int | None = Field(default=None, ge=0)
    http_status: int | None = Field(default=None, ge=100, le=599)
    available: bool = False
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_id", "reason_code")
    @classmethod
    def validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("schema_version", "status")
    @classmethod
    def validate_health_identifiers(cls, value: str, info: Any) -> str:
        return _identifier(value, field_name=info.field_name)

    @field_validator("checked_at", "last_success_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _shanghai(value)

    @model_validator(mode="after")
    def validate_health(self) -> SourceHealth:
        if self.last_success_time is not None and self.last_success_time > self.checked_at:
            raise ValueError("last_success_time must be before checked_at")
        _reject_secrets(self.details)
        return self


class FactSnapshotManifest(_SafeModel):
    """Immutable manifest binding facts to one reproducible research input."""

    schema_version: str = "liangjian-fact-snapshot/1.0.0"
    snapshot_id: str
    as_of: datetime
    frozen: Literal[True] = True
    facts: tuple[FactEnvelope | RealtimeFactEnvelope, ...] = ()
    source_health: tuple[SourceHealth, ...] = ()
    source_checksums: dict[str, str] = Field(default_factory=dict)
    coverage_by_fact_type: dict[str, float] = Field(default_factory=dict)
    facts_sha256: str | None = None

    @field_validator("snapshot_id")
    @classmethod
    def validate_snapshot_id(cls, value: str) -> str:
        return _identifier(value, field_name="snapshot_id")

    @field_validator("as_of")
    @classmethod
    def normalize_as_of(cls, value: datetime) -> datetime:
        return _shanghai(value)

    @field_validator("source_checksums")
    @classmethod
    def validate_source_checksums(cls, value: dict[str, str]) -> dict[str, str]:
        return {
            _identifier(key, field_name="source_checksums key"): _hash(item, field_name="source checksum")
            for key, item in value.items()
        }

    @field_validator("coverage_by_fact_type")
    @classmethod
    def validate_coverage(cls, value: dict[str, float]) -> dict[str, float]:
        for key, item in value.items():
            _identifier(key, field_name="coverage_by_fact_type key")
            if item < 0 or item > 1:
                raise ValueError("coverage must be between 0 and 1")
        return value

    @field_validator("facts")
    @classmethod
    def sort_facts(cls, value: tuple[FactEnvelope | RealtimeFactEnvelope, ...]) -> tuple[FactEnvelope | RealtimeFactEnvelope, ...]:
        return tuple(
            sorted(
                value,
                key=lambda fact: (
                    fact.fact_id,
                    fact.fact_type,
                    fact.symbol or "",
                    canonical_json(fact),
                ),
            )
        )

    @field_validator("source_health")
    @classmethod
    def sort_health(cls, value: tuple[SourceHealth, ...]) -> tuple[SourceHealth, ...]:
        return tuple(sorted(value, key=lambda item: (item.source_id, item.checked_at.isoformat())))

    @model_validator(mode="after")
    def bind_fact_hash(self) -> FactSnapshotManifest:
        expected = _facts_hash(self.facts)
        if self.facts_sha256 is not None and _hash(self.facts_sha256, field_name="facts_sha256") != expected:
            raise ValueError("facts_sha256 does not match canonical facts")
        object.__setattr__(self, "facts_sha256", expected)
        return self

    @property
    def manifest_hash(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self)).hexdigest()


__all__ = [
    "FactEnvelope",
    "FactSnapshotManifest",
    "RealtimeFactEnvelope",
    "SHANGHAI",
    "SourceHealth",
    "SourceHealthStatus",
    "SourceTier",
    "canonical_json",
    "canonical_json_bytes",
]
