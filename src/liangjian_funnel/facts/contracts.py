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
from typing import Any, ClassVar, Iterator, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_serializer, model_validator


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


_CANONICAL_ENCODER = json.JSONEncoder(
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
    allow_nan=False,
)
_JSON_ANY_ADAPTER = TypeAdapter(Any)


def _stream_model_items(value: BaseModel) -> tuple[tuple[str, Any], ...] | None:
    """Return the normal field view for the fact models without a model dump.

    Pydantic's ``model_dump`` is intentionally avoided for ``_SafeModel``
    instances.  A snapshot can contain thousands of nested facts, and a
    complete dump would recreate that entire tree before it can be hashed or
    written.  The fact models use the default field serializer (the
    ``_SafeModel`` wrapper only validates the serialized result), so walking
    their fields produces the same mode-json representation after the scalar
    conversions below.

    Other BaseModel implementations are delegated to Pydantic's normal
    serializer.  That keeps custom model/field serializers compatible while
    keeping the large fact snapshot path allocation-light.
    """

    safe_model = globals().get("_SafeModel")
    if safe_model is None or not isinstance(value, safe_model):
        return None

    # RootModel serializes to its root value rather than an object.  None
    # tells the caller to use the normal Pydantic dump for this uncommon case.
    if getattr(value.__class__, "__pydantic_root_model__", False):
        return None

    items: list[tuple[str, Any]] = []
    fields = getattr(value.__class__, "model_fields", {})
    for name, field in fields.items():
        # ``exclude=True`` is the only field-level exclusion that applies to
        # the default model_dump() call used by canonical_json().
        if getattr(field, "exclude", None):
            continue
        if hasattr(value, name):
            items.append((name, getattr(value, name)))

    # Computed fields are included by Pydantic's default model_dump().
    computed_fields = getattr(value.__class__, "model_computed_fields", {})
    for name, field in computed_fields.items():
        if getattr(field, "exclude", None) or any(key == name for key, _ in items):
            continue
        if hasattr(value, name):
            items.append((name, getattr(value, name)))

    # Models configured with ``extra='allow'`` retain extras in this mapping.
    # The fact contracts forbid extras, but preserving the behavior costs only
    # a shallow iteration for compatible subclasses.
    extras = getattr(value, "__pydantic_extra__", None)
    if isinstance(extras, Mapping):
        items.extend((str(key), item) for key, item in extras.items())
    return tuple(items)


def _reject_secrets_streaming(value: Any, *, model_mode: bool = False) -> None:
    """Apply the canonical secret rejection policy without a model dump."""

    if isinstance(value, BaseModel):
        items = _stream_model_items(value)
        if items is None:
            # Custom BaseModel serializers are uncommon in fact payloads.  A
            # mode-json dump preserves their existing behavior; only the
            # large _SafeModel snapshot path is required to remain streaming.
            _reject_secrets(value.model_dump(mode="json"))
            return
        for key, item in items:
            if _secret_key_name(key):
                raise ValueError("secret-like field is not allowed in fact data")
            _reject_secrets_streaming(key)
            _reject_secrets_streaming(item, model_mode=True)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = str(key)
            if _secret_key_name(normalized_key):
                raise ValueError("secret-like field is not allowed in fact data")
            _reject_secrets_streaming(normalized_key)
            _reject_secrets_streaming(item, model_mode=model_mode)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            _reject_secrets_streaming(item, model_mode=model_mode)
        return
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise ValueError("secret-like value is not allowed in fact data")
    if isinstance(value, (str, int, float, bool, type(None), datetime, date)):
        return
    if model_mode:
        converted = _JSON_ANY_ADAPTER.dump_python(value, mode="json")
        if converted is not value:
            _reject_secrets_streaming(converted, model_mode=True)


def _iter_canonical_json(value: Any, *, model_mode: bool = False) -> Iterator[bytes]:
    """Yield canonical JSON UTF-8 bytes without materializing the document."""

    if isinstance(value, BaseModel):
        items = _stream_model_items(value)
        if items is None:
            # Preserve custom Pydantic serializers for non-fact models.  This
            # fallback is intentionally limited to that model, not snapshots.
            yield from _iter_canonical_json(value.model_dump(mode="json"))
            return
        normalized = {str(key): item for key, item in items}
        yield b"{"
        for index, key in enumerate(sorted(normalized)):
            if index:
                yield b","
            yield from _iter_canonical_json(key)
            yield b":"
            yield from _iter_canonical_json(normalized[key], model_mode=True)
        yield b"}"
        return

    if isinstance(value, Mapping):
        # _jsonable() first converts every key to str and then constructs a
        # dict.  The shallow map below reproduces duplicate-key collapse while
        # avoiding any recursive copy of the values.
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            normalized[str(key)] = item
        yield b"{"
        for index, key in enumerate(sorted(normalized)):
            if index:
                yield b","
            yield from _iter_canonical_json(key)
            yield b":"
            yield from _iter_canonical_json(normalized[key], model_mode=model_mode)
        yield b"}"
        return

    if isinstance(value, (list, tuple)):
        yield b"["
        for index, item in enumerate(value):
            if index:
                yield b","
            yield from _iter_canonical_json(item, model_mode=model_mode)
        yield b"]"
        return

    if isinstance(value, (set, frozenset)):
        # Pydantic mode="json" turns sets into lists in their native
        # iteration order.  A plain set passed to canonical_json() is first
        # normalized by _jsonable(), which deliberately sorts it by repr.
        # Keep both behaviors by carrying the model serialization context.
        items = value if model_mode else sorted(value, key=repr)
        yield b"["
        for index, item in enumerate(items):
            if index:
                yield b","
            yield from _iter_canonical_json(item, model_mode=model_mode)
        yield b"]"
        return

    if isinstance(value, (datetime, date)):
        value = value.isoformat()

    if model_mode and not isinstance(value, (str, int, float, bool, type(None))):
        converted = _JSON_ANY_ADAPTER.dump_python(value, mode="json")
        if converted is not value:
            yield from _iter_canonical_json(converted, model_mode=True)
            return

    # JSONEncoder's scalar path is the source of truth for escaping, float
    # formatting, integer handling, and allow_nan=False behavior.  It emits
    # only a small scalar chunk here, never the enclosing large object.
    yield from (chunk.encode("utf-8") for chunk in _CANONICAL_ENCODER.iterencode(value))


def canonical_json_chunks(value: Any) -> Iterator[bytes]:
    """Yield the exact UTF-8 bytes produced by :func:`canonical_json_bytes`.

    Secret validation runs before the first chunk is returned, so callers can
    safely stream into a temporary file without ever publishing a partial
    document containing rejected material.
    """

    _reject_secrets_streaming(value)
    return _iter_canonical_json(value)


def canonical_json_hash(value: Any) -> str:
    """Hash canonical JSON incrementally without creating a full byte string."""

    digest = hashlib.sha256()
    for chunk in canonical_json_chunks(value):
        digest.update(chunk)
    return digest.hexdigest()


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
    digest = hashlib.sha256()
    for chunk in canonical_json_chunks(ordered):
        digest.update(chunk)
    return digest.hexdigest()


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
        return canonical_json_hash(self)


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
    "canonical_json_chunks",
    "canonical_json_hash",
]
