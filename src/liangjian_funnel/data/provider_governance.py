"""Shared provider governance for bounded, auditable external reads.

This module coordinates fetches; it does not replace the existing fact stores.
All network-specific parsing remains inside adapters and must return a typed
``ProviderEnvelope`` with the source timestamps it can actually prove.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import ipaddress
import json
from pathlib import Path
from queue import Empty, Queue
import socket
from threading import Event, Lock, Thread
import time
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlparse
import uuid

import yaml

from ..runtime.state import RuntimeStore


class ProviderStatus(StrEnum):
    OK = "OK"
    EMPTY = "EMPTY"
    NOT_PUBLISHED = "NOT_PUBLISHED"
    NETWORK_TRANSIENT = "NETWORK_TRANSIENT"
    RATE_LIMITED = "RATE_LIMITED"
    AUTH_FAILURE = "AUTH_FAILURE"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    UNAUTHORIZED = "UNAUTHORIZED"
    CONFLICT = "CONFLICT"
    TIME_UNVERIFIED = "TIME_UNVERIFIED"


RETRYABLE_STATUSES = {
    ProviderStatus.NETWORK_TRANSIENT,
    ProviderStatus.RATE_LIMITED,
}


@dataclass
class _LocalFlight:
    event: Event = field(default_factory=Event)
    result: "ProviderEnvelope | None" = None


_LOCAL_FLIGHT_LOCK = Lock()
_LOCAL_FLIGHTS: dict[str, _LocalFlight] = {}


@dataclass(frozen=True)
class ProviderCapability:
    provider: str
    capability: str
    quota_scope: str
    account_scope: str
    authorization: str
    supported_fields: tuple[str, ...]
    markets: tuple[str, ...]
    unit: str
    adjustment: str
    time_precision: str
    rate_window_seconds: float
    max_requests: int
    max_concurrency: int
    ttl_seconds: float
    stale_action: str
    priority: int
    fallbacks: tuple[str, ...]
    upstream_identity: str
    allowed_hosts: tuple[str, ...]
    max_response_bytes: int

    @property
    def authorized(self) -> bool:
        return self.authorization.upper() in {"FREE_PUBLIC", "AUTHORIZED", "LOCAL"}


class ProviderRegistry:
    def __init__(self, capabilities: Sequence[ProviderCapability]):
        self._items = {(item.provider, item.capability, item.account_scope): item for item in capabilities}
        if len(self._items) != len(capabilities):
            raise ValueError("duplicate provider capability registration")

    @classmethod
    def load(cls, path: str | Path) -> "ProviderRegistry":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        rows = raw.get("governance", {}).get("capabilities", [])
        items: list[ProviderCapability] = []
        required = {
            "provider", "capability", "quota_scope", "account_scope", "authorization",
            "supported_fields", "markets", "unit", "adjustment", "time_precision",
            "rate_window_seconds", "max_requests", "max_concurrency", "ttl_seconds",
            "stale_action", "priority", "fallbacks", "upstream_identity",
            "allowed_hosts", "max_response_bytes",
        }
        for row in rows:
            missing = sorted(required - set(row or {}))
            if missing:
                raise ValueError(f"provider capability missing fields: {','.join(missing)}")
            items.append(ProviderCapability(
                **{
                    **row,
                    "supported_fields": tuple(row["supported_fields"]),
                    "markets": tuple(row["markets"]),
                    "fallbacks": tuple(row["fallbacks"]),
                    "allowed_hosts": tuple(row["allowed_hosts"]),
                }
            ))
        return cls(items)

    def require(self, provider: str, capability: str, account_scope: str = "public") -> ProviderCapability:
        try:
            item = self._items[(provider, capability, account_scope)]
        except KeyError as exc:
            raise KeyError(f"unregistered provider capability: {provider}/{capability}/{account_scope}") from exc
        if not item.authorized:
            raise PermissionError(f"provider capability is not authorized: {provider}/{capability}")
        return item


@dataclass(frozen=True)
class ProviderRequest:
    provider: str
    capability: str
    object_id: str
    as_of: str
    account_scope: str = "public"
    fields: tuple[str, ...] = ()
    range_key: str = ""
    permission_scope: str = "public"
    semantics: Mapping[str, Any] = field(default_factory=dict)

    def key(self) -> str:
        canonical = json.dumps({
            "provider": self.provider,
            "capability": self.capability,
            "object_id": self.object_id,
            "as_of": self.as_of,
            "account_scope": self.account_scope,
            "fields": sorted(self.fields),
            "range_key": self.range_key,
            "permission_scope": self.permission_scope,
            "semantics": self.semantics,
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ProviderEnvelope:
    status: ProviderStatus
    reason_code: str
    payload: Any = None
    effective_at: str | None = None
    fetched_at: str | None = None
    ingested_at: str | None = None
    time_verified: bool = False
    retry_after_seconds: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def serializable(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "reason_code": self.reason_code,
            "payload": self.payload,
            "effective_at": self.effective_at,
            "fetched_at": self.fetched_at,
            "ingested_at": self.ingested_at,
            "time_verified": self.time_verified,
            "retry_after_seconds": self.retry_after_seconds,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ProviderEnvelope":
        return cls(
            status=ProviderStatus(str(value["status"])),
            reason_code=str(value["reason_code"]),
            payload=value.get("payload"),
            effective_at=value.get("effective_at"),
            fetched_at=value.get("fetched_at"),
            ingested_at=value.get("ingested_at"),
            time_verified=bool(value.get("time_verified", False)),
            retry_after_seconds=value.get("retry_after_seconds"),
            metadata=value.get("metadata") or {},
        )


class ProviderGovernor:
    def __init__(
        self,
        *,
        store: RuntimeStore,
        registry: ProviderRegistry,
        owner: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleeper: Callable[[float], None] = time.sleep,
        poll_seconds: float = 0.01,
    ):
        self.store = store
        self.registry = registry
        self.owner = owner or f"provider-worker-{uuid.uuid4()}"
        self.clock = clock
        self.wall_clock = wall_clock
        self.sleeper = sleeper
        self.poll_seconds = max(0.001, poll_seconds)

    def fetch(
        self,
        request: ProviderRequest,
        operation: Callable[[ProviderRequest, float], ProviderEnvelope],
        *,
        deadline_seconds: float,
        max_attempts: int = 3,
    ) -> ProviderEnvelope:
        flight_key = f"{self.store.path}:{request.key()}"
        with _LOCAL_FLIGHT_LOCK:
            flight = _LOCAL_FLIGHTS.get(flight_key)
            leader = flight is None
            if flight is None:
                flight = _LocalFlight()
                _LOCAL_FLIGHTS[flight_key] = flight
        if not leader:
            if flight.event.wait(timeout=max(0.0, deadline_seconds)) and flight.result is not None:
                # Round-trip through JSON to ensure callers cannot mutate the
                # leader's object or another waiter's nested payload.
                return ProviderEnvelope.from_mapping(json.loads(json.dumps(
                    flight.result.serializable(), ensure_ascii=False, default=str
                )))
            return ProviderEnvelope(ProviderStatus.DEADLINE_EXCEEDED, "PROVIDER_SINGLEFLIGHT_WAIT_DEADLINE")
        try:
            result = self._fetch_impl(
                request,
                operation,
                deadline_seconds=deadline_seconds,
                max_attempts=max_attempts,
            )
            flight.result = result
            return result
        finally:
            flight.event.set()
            with _LOCAL_FLIGHT_LOCK:
                if _LOCAL_FLIGHTS.get(flight_key) is flight:
                    del _LOCAL_FLIGHTS[flight_key]

    def _fetch_impl(
        self,
        request: ProviderRequest,
        operation: Callable[[ProviderRequest, float], ProviderEnvelope],
        *,
        deadline_seconds: float,
        max_attempts: int = 3,
    ) -> ProviderEnvelope:
        try:
            spec = self.registry.require(request.provider, request.capability, request.account_scope)
        except (KeyError, PermissionError) as exc:
            return ProviderEnvelope(ProviderStatus.UNAUTHORIZED, "PROVIDER_CAPABILITY_UNAUTHORIZED", metadata={"detail": str(exc)})
        if deadline_seconds <= 0 or max_attempts <= 0:
            return ProviderEnvelope(ProviderStatus.DEADLINE_EXCEEDED, "PROVIDER_DEADLINE_EXCEEDED")
        request_key = request.key()
        deadline = self.clock() + deadline_seconds
        lease_ttl = max(0.05, deadline_seconds + self.poll_seconds)
        token = self.store.acquire_provider_request(
            request_key, self.owner, now=self.wall_clock(), ttl_seconds=lease_ttl
        )
        if token is None:
            return self._wait_for_shared_result(request_key, deadline)

        final = ProviderEnvelope(ProviderStatus.NETWORK_TRANSIENT, "PROVIDER_FETCH_NOT_ATTEMPTED")
        for attempt in range(1, max_attempts + 1):
            remaining = deadline - self.clock()
            if remaining <= 0:
                final = ProviderEnvelope(ProviderStatus.DEADLINE_EXCEEDED, "PROVIDER_DEADLINE_EXCEEDED")
                break
            reserved = self.store.reserve_provider_quota(
                spec.quota_scope,
                now=self.wall_clock(),
                window_seconds=spec.rate_window_seconds,
                max_requests=spec.max_requests,
                owner=self.owner,
                probe_ttl_seconds=min(30.0, remaining),
            )
            if not reserved["allowed"]:
                status = ProviderStatus.CIRCUIT_OPEN if reserved["reason_code"] == "PROVIDER_CIRCUIT_OPEN" else ProviderStatus.QUOTA_EXHAUSTED
                final = ProviderEnvelope(status, str(reserved["reason_code"]), metadata=reserved)
                break
            started = self.wall_clock()
            attempt_id = str(uuid.uuid4())
            attempt_owner = f"{self.owner}:{request_key}:{attempt}"
            slot = self.store.acquire_provider_concurrency(
                spec.quota_scope,
                attempt_owner,
                now=started,
                ttl_seconds=max(0.05, remaining),
                max_concurrency=spec.max_concurrency,
            )
            if slot is None:
                final = ProviderEnvelope(ProviderStatus.QUOTA_EXHAUSTED, "PROVIDER_CONCURRENCY_EXHAUSTED")
                break
            try:
                final = self._call_bounded(operation, request, remaining)
            finally:
                self.store.release_provider_concurrency(spec.quota_scope, slot, attempt_owner)
            finished = self.wall_clock()
            if final.status == ProviderStatus.OK:
                try:
                    payload_size = len(json.dumps(
                        final.payload, ensure_ascii=False, default=str
                    ).encode("utf-8"))
                except (TypeError, ValueError):
                    payload_size = spec.max_response_bytes + 1
                if payload_size > spec.max_response_bytes:
                    final = ProviderEnvelope(
                        ProviderStatus.SCHEMA_ERROR,
                        "PROVIDER_RESPONSE_TOO_LARGE",
                        metadata={"response_bytes": payload_size, "limit_bytes": spec.max_response_bytes},
                    )
                elif not final.time_verified or not final.effective_at or not final.fetched_at or not final.ingested_at:
                    final = ProviderEnvelope(
                        ProviderStatus.TIME_UNVERIFIED,
                        "PROVIDER_TIME_UNVERIFIED",
                        payload=final.payload,
                        effective_at=final.effective_at,
                        fetched_at=final.fetched_at,
                        ingested_at=final.ingested_at,
                        metadata=final.metadata,
                    )
                else:
                    self.store.update_provider_health(spec.quota_scope, now=finished, success=True)
            self.store.record_provider_attempt(
                attempt_id=attempt_id,
                request_key=request_key,
                provider=request.provider,
                capability=request.capability,
                outcome=final.status.value,
                reason_code=final.reason_code,
                started_at=started,
                finished_at=finished,
                metadata={"attempt": attempt},
            )
            if final.status == ProviderStatus.OK:
                break
            if final.status not in RETRYABLE_STATUSES:
                break
            cooldown = max(0.0, float(final.retry_after_seconds or 0.0))
            self.store.update_provider_health(
                spec.quota_scope,
                now=finished,
                success=False,
                cooldown_seconds=cooldown,
                failure_threshold=max(3, max_attempts + 1),
            )
            if attempt >= max_attempts:
                break
            wait = min(cooldown or (0.01 * attempt), max(0.0, deadline - self.clock()))
            if wait > 0:
                self.sleeper(wait)

        last_good = self._last_good_payload(spec, final) if final.status == ProviderStatus.OK else None
        published = self.store.publish_provider_result(
            request_key=request_key,
            owner=self.owner,
            fencing_token=token,
            result=final.serializable(),
            now=self.wall_clock(),
            last_good=last_good,
        )
        if not published:
            return ProviderEnvelope(ProviderStatus.CONFLICT, "PROVIDER_FENCING_CONFLICT")
        return final

    def read_last_good(self, request: ProviderRequest, *, for_new_position: bool) -> ProviderEnvelope | None:
        row = self.store.get_provider_last_good(request.key())
        if row is None:
            return None
        expired = datetime.fromisoformat(str(row["expires_at"])) < self.wall_clock()
        if expired and for_new_position:
            return ProviderEnvelope(ProviderStatus.CONFLICT, "STALE_LAST_GOOD_NOT_TRADABLE", metadata={"stale": True})
        return ProviderEnvelope(
            ProviderStatus.OK,
            "LAST_GOOD_STALE_DISPLAY_ONLY" if expired else "LAST_GOOD",
            payload=row["payload"],
            effective_at=row.get("effective_at"),
            fetched_at=row.get("fetched_at"),
            ingested_at=row.get("ingested_at"),
            time_verified=bool(row.get("effective_at")),
            metadata={**row["metadata"], "stale": expired, "content_hash": row["content_hash"]},
        )

    def _wait_for_shared_result(self, request_key: str, deadline: float) -> ProviderEnvelope:
        while self.clock() < deadline:
            row = self.store.get_provider_request(request_key)
            if row and row["state"] == "COMPLETED" and row.get("result_json"):
                return ProviderEnvelope.from_mapping(json.loads(row["result_json"]))
            self.sleeper(min(self.poll_seconds, max(0.0, deadline - self.clock())))
        return ProviderEnvelope(ProviderStatus.DEADLINE_EXCEEDED, "PROVIDER_SINGLEFLIGHT_WAIT_DEADLINE")

    @staticmethod
    def _call_bounded(
        operation: Callable[[ProviderRequest, float], ProviderEnvelope],
        request: ProviderRequest,
        timeout: float,
    ) -> ProviderEnvelope:
        queue: Queue[ProviderEnvelope | BaseException] = Queue(maxsize=1)

        def invoke() -> None:
            try:
                queue.put(operation(request, timeout))
            except BaseException as exc:  # adapter errors become typed source failures
                queue.put(exc)

        thread = Thread(target=invoke, daemon=True, name="provider-bounded-fetch")
        thread.start()
        try:
            result = queue.get(timeout=max(0.001, timeout))
        except Empty:
            return ProviderEnvelope(ProviderStatus.DEADLINE_EXCEEDED, "PROVIDER_CALL_DEADLINE")
        if isinstance(result, BaseException):
            return ProviderEnvelope(ProviderStatus.NETWORK_TRANSIENT, "PROVIDER_NETWORK_EXCEPTION", metadata={"error_type": type(result).__name__})
        if not isinstance(result, ProviderEnvelope):
            return ProviderEnvelope(ProviderStatus.SCHEMA_ERROR, "PROVIDER_ENVELOPE_INVALID")
        return result

    def _last_good_payload(self, spec: ProviderCapability, result: ProviderEnvelope) -> dict[str, Any]:
        fetched = datetime.fromisoformat(str(result.fetched_at))
        expires = fetched + timedelta(seconds=spec.ttl_seconds)
        payload_text = json.dumps(result.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        return {
            "provider": spec.provider,
            "capability": spec.capability,
            "quota_scope": spec.quota_scope,
            "upstream_identity": spec.upstream_identity,
            "payload": result.payload,
            "metadata": dict(result.metadata),
            "effective_at": result.effective_at,
            "fetched_at": result.fetched_at,
            "ingested_at": result.ingested_at,
            "expires_at": expires.isoformat(),
            "content_hash": hashlib.sha256(payload_text.encode("utf-8")).hexdigest(),
        }


def validate_fallback(
    primary: ProviderCapability,
    fallback: ProviderCapability,
    *,
    primary_metadata: Mapping[str, Any],
    fallback_metadata: Mapping[str, Any],
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    for field in ("symbol", "trade_date", "interval", "unit", "adjustment"):
        if primary_metadata.get(field) != fallback_metadata.get(field):
            reasons.append(f"FALLBACK_{field.upper()}_CONFLICT")
    if not fallback_metadata.get("time_verified", False):
        reasons.append("FALLBACK_TIME_UNVERIFIED")
    if primary.upstream_identity == fallback.upstream_identity:
        reasons.append("FALLBACK_NOT_INDEPENDENT")
    if not set(primary.supported_fields).issubset(set(fallback.supported_fields)):
        reasons.append("FALLBACK_FIELD_COVERAGE_INSUFFICIENT")
    return not reasons, tuple(reasons)


def validate_external_url(url: str, *, allowed_hosts: Sequence[str]) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("EXTERNAL_URL_NOT_ALLOWED")
    host = parsed.hostname.rstrip(".").lower()
    allowed = {item.rstrip(".").lower() for item in allowed_hosts}
    if host not in allowed:
        raise ValueError("EXTERNAL_HOST_NOT_ALLOWED")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    except OSError as exc:
        raise ValueError("EXTERNAL_HOST_UNRESOLVED") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise ValueError("EXTERNAL_HOST_PRIVATE_ADDRESS")
    return url


__all__ = [
    "ProviderCapability",
    "ProviderEnvelope",
    "ProviderGovernor",
    "ProviderRegistry",
    "ProviderRequest",
    "ProviderStatus",
    "validate_external_url",
    "validate_fallback",
]
