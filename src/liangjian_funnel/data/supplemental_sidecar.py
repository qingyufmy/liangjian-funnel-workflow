"""Governed supplemental-source sidecars with zero execution authority.

The module normalizes authorized fixtures or reviewed imports into bounded
projections.  It deliberately does not scrape, persist a second fact store,
or merge shadow evidence into A1/A2/A3/A4 decisions.  Network collection, when
separately enabled and licensed, must still pass :mod:`provider_governance`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import yaml

from .provider_governance import (
    ProviderEnvelope,
    ProviderGovernor,
    ProviderRegistry,
    ProviderRequest,
    ProviderStatus,
)
from ..facts.contracts import FactEnvelope, canonical_json_hash


POLICY_SCHEMA_VERSION = "supplemental-source-policy/1.0.0"
SHADOW_SCHEMA_VERSION = "supplemental-source-shadow/1.0.0"
_IDENTITY_TYPES = frozenset({"KPL_THEME", "CATALOG_CONCEPT", "BUSINESS_DISCLOSURE"})
_VERIFIED_LICENSES = frozenset({"VERIFIED_PUBLIC", "VERIFIED_LICENSED", "VERIFIED_LOCAL"})


@dataclass(frozen=True)
class SupplementalSourcePolicy:
    source_id: str
    provider: str
    capability: str
    enabled: bool
    shadow_only: bool
    execution_authority: bool
    adapter_status: str
    live_status: str
    license_status: str
    ingestion_mode: str
    upstream_identity: str
    applicable_paths: tuple[str, ...]


def _token(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 160:
        raise ValueError(f"invalid {field_name}")
    return text


def _aware(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_supplemental_source_policies(path: str | Path) -> dict[str, SupplementalSourcePolicy]:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if raw.get("schema_version") != POLICY_SCHEMA_VERSION or not isinstance(raw.get("sources"), list):
        raise ValueError("SUPPLEMENTAL_SOURCE_POLICY_SCHEMA_INVALID")
    result: dict[str, SupplementalSourcePolicy] = {}
    for row in raw["sources"]:
        if not isinstance(row, Mapping):
            raise ValueError("SUPPLEMENTAL_SOURCE_POLICY_ROW_INVALID")
        policy = SupplementalSourcePolicy(
            source_id=_token(row.get("source_id"), "source_id"),
            provider=_token(row.get("provider"), "provider"),
            capability=_token(row.get("capability"), "capability"),
            enabled=row.get("enabled") is True,
            shadow_only=row.get("shadow_only") is True,
            execution_authority=row.get("execution_authority") is True,
            adapter_status=_token(row.get("adapter_status"), "adapter_status"),
            live_status=_token(row.get("live_status"), "live_status"),
            license_status=_token(row.get("license_status"), "license_status"),
            ingestion_mode=_token(row.get("ingestion_mode"), "ingestion_mode"),
            upstream_identity=_token(row.get("upstream_identity"), "upstream_identity"),
            applicable_paths=tuple(_token(item, "applicable_path") for item in row.get("applicable_paths", ())),
        )
        if policy.source_id in result:
            raise ValueError("SUPPLEMENTAL_SOURCE_POLICY_DUPLICATE")
        if policy.execution_authority and (policy.shadow_only or policy.license_status not in _VERIFIED_LICENSES):
            raise ValueError("SUPPLEMENTAL_SOURCE_EXECUTION_AUTHORITY_INVALID")
        result[policy.source_id] = policy
    return result


def attach_shadow_report(
    authoritative: Mapping[str, Any], report: Mapping[str, Any], *, policy: SupplementalSourcePolicy,
) -> dict[str, Any]:
    """Return parallel authoritative/shadow branches; never merge shadow rows."""

    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "authoritative": deepcopy(dict(authoritative)),
        "shadow": {
            "source_id": policy.source_id,
            "visible": True,
            "enabled": policy.enabled,
            "shadow_only": True,
            "execution_authority": False,
            "live_status": policy.live_status,
            "report": deepcopy(dict(report)),
        },
    }


def build_shadow_fact_envelope(
    policy: SupplementalSourcePolicy,
    normalized: Mapping[str, Any],
    *,
    fact_type: str,
    symbol: str | None,
    event_time: datetime,
    publish_time: datetime,
    fetch_time: datetime,
    ingest_time: datetime,
    source_url: str,
) -> FactEnvelope:
    """Validate shadow evidence with the existing immutable fact contract."""

    payload = {
        "shadow_only": True,
        "execution_authority": False,
        "policy_source_id": policy.source_id,
        "upstream_identity": policy.upstream_identity,
        "normalized": deepcopy(dict(normalized)),
    }
    content_hash = canonical_json_hash(payload)
    fact_id = f"shadow_{hashlib.sha256(f'{policy.source_id}|{fact_type}|{symbol}|{content_hash}'.encode()).hexdigest()[:24]}"
    return FactEnvelope(
        fact_id=fact_id,
        source_id=policy.source_id,
        source_tier="T4",
        fact_type=fact_type,
        symbol=symbol,
        event_time=event_time,
        publish_time=publish_time,
        fetch_time=fetch_time,
        ingest_time=ingest_time,
        available=True,
        reason_code="SHADOW_EVIDENCE_ONLY",
        source_url=source_url,
        content_hash=content_hash,
        payload=payload,
    )


def to_a1_coverage_observation(
    business_profile: Mapping[str, Any], *, attempted_at: datetime,
):
    """Bridge business sidecar diagnostics into the existing S04 ledger type."""

    from ..pipeline.a1_coverage import CoverageObservation

    raw = business_profile.get("raw_document") if isinstance(business_profile.get("raw_document"), Mapping) else {}
    coverage = business_profile.get("coverage_observation") if isinstance(business_profile.get("coverage_observation"), Mapping) else {}
    return CoverageObservation(
        requested=coverage.get("requested") is True,
        raw_found=coverage.get("raw_found") is True,
        parsed=coverage.get("parsed") is True,
        value=business_profile.get("business_segments") or None,
        announced_at=_aware(raw.get("publish_time")),
        available_at=_aware(raw.get("publish_time")),
        feature_ready=False,
        packet_ready=False,
        evidence_ref=str(coverage.get("evidence_ref") or "") or None,
        gap_reason=str(coverage.get("gap_reason") or "") or None,
        attempted_at=attempted_at,
    )


def normalize_taxonomy_record(
    row: Mapping[str, Any], *, identity_type: str, source_id: str,
) -> dict[str, Any]:
    identity = str(identity_type).strip().upper()
    if identity not in _IDENTITY_TYPES:
        raise ValueError("SUPPLEMENTAL_TAXONOMY_IDENTITY_INVALID")
    symbol = _token(row.get("symbol"), "symbol")
    name = _token(row.get("name"), "name")
    business_ratio = _finite(row.get("business_ratio")) if identity == "BUSINESS_DISCLOSURE" else None
    material = {"identity_type": identity, "symbol": symbol, "name": name, "source_id": source_id}
    return {
        **material,
        "identity_key": _hash(material),
        "first_limit_time": row.get("first_limit_time") if identity == "KPL_THEME" else None,
        "business_ratio": business_ratio,
        "interchangeable_with": [],
        "execution_authority": False,
    }


def normalize_quote_record(
    row: Mapping[str, Any], *, observed_at: datetime, source_id: str,
) -> dict[str, Any]:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("SUPPLEMENTAL_QUOTE_CUTOFF_NAIVE")
    symbol = _token(row.get("symbol"), "symbol")
    trade_date = str(row.get("trade_date") or "").strip() or None
    quote_time = _aware(row.get("quote_time"))
    price = _finite(row.get("latest_price"))
    verified = bool(
        trade_date
        and quote_time
        and quote_time.date().isoformat() == trade_date
        and trade_date == observed_at.date().isoformat()
        and quote_time <= observed_at
        and price is not None
        and price > 0
    )
    return {
        "source_id": source_id,
        "symbol": symbol,
        "status": "OK" if verified else "TIME_UNVERIFIED",
        "reason_code": "QUOTE_TIME_VERIFIED" if verified else "QUOTE_TRADING_DATE_UNPROVEN",
        "trade_date": trade_date,
        "quote_time": quote_time.isoformat() if quote_time else None,
        "latest_price": price,
        "http_file_time": row.get("http_last_modified"),
        "tradable_snapshot": verified,
        "execution_authority": False,
    }


def normalize_business_profile(
    row: Mapping[str, Any], *, source_id: str, extraction_succeeded: bool,
) -> dict[str, Any]:
    symbol = _token(row.get("symbol"), "symbol")
    raw_text = str(row.get("raw_text") or "")
    publish_time = _aware(row.get("publish_time"))
    segments: list[dict[str, Any]] = []
    if extraction_succeeded and isinstance(row.get("business_segments"), Sequence):
        for item in row.get("business_segments", ()):
            if not isinstance(item, Mapping) or not str(item.get("name") or "").strip():
                continue
            segments.append({
                "name": str(item["name"]).strip(),
                "ratio": _finite(item.get("ratio")),
                "identity_type": "BUSINESS_DISCLOSURE",
            })
    industry = str(row.get("industry") or "").strip()
    gap_reason = None if segments else "PARSE_ERROR" if raw_text else "FIELD_MISSING"
    return {
        "source_id": source_id,
        "symbol": symbol,
        "raw_document": {
            "text": raw_text,
            "publish_time": publish_time.isoformat() if publish_time else None,
            "content_hash": _hash(raw_text) if raw_text else None,
        },
        "business_segments": segments,
        "classification_fallback": (
            {"label": industry, "identity_type": "INDUSTRY_CLASSIFICATION"} if industry else None
        ),
        "claimed_business_ratio": max(
            (item["ratio"] for item in segments if item["ratio"] is not None), default=None,
        ),
        "strict_pit": bool(publish_time and segments),
        "multi_period_financials": False,
        "coverage_observation": {
            "requested": True,
            "raw_found": bool(raw_text),
            "parsed": bool(segments),
            "feature_ready": False,
            "packet_ready": False,
            "gap_reason": gap_reason,
            "evidence_ref": f"{source_id}:{symbol}:{_hash(raw_text)[:16]}" if raw_text else None,
        },
        "execution_authority": False,
    }


def normalize_opinion_records(records: Sequence[Mapping[str, Any]], *, as_of: datetime) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("SUPPLEMENTAL_OPINION_CUTOFF_NAIVE")
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in records:
        original_url = str(row.get("original_url") or "").strip()
        author = str(row.get("author") or "").strip()
        published = str(row.get("original_published_at") or "").strip()
        text = str(row.get("text") or "").strip()
        key = _hash({"original_url": original_url, "author": author, "published": published, "text": text})
        groups.setdefault(key, []).append(row)
    normalized: list[dict[str, Any]] = []
    for chain_id, copies in sorted(groups.items()):
        original = copies[0]
        expires_at = _aware(original.get("expires_at"))
        state = "EXPIRED" if expires_at is not None and expires_at < as_of else "ACTIVE_RESEARCH_LEAD"
        normalized.append({
            "source_chain_id": chain_id,
            "author": str(original.get("author") or "").strip() or None,
            "original_url": str(original.get("original_url") or "").strip() or None,
            "original_published_at": str(original.get("original_published_at") or "").strip() or None,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "text": str(original.get("text") or "").strip(),
            "testable_conditions": [
                str(item).strip()
                for item in original.get("testable_conditions", ())
                if str(item).strip()
            ] if isinstance(original.get("testable_conditions"), Sequence)
            and not isinstance(original.get("testable_conditions"), (str, bytes, bytearray)) else [],
            "repost_count": max(0, len(copies) - 1),
            "repost_urls": [str(item.get("source_url")) for item in copies if item.get("source_url")],
            "state": state,
            "research_lead_only": True,
            "execution_authority": False,
        })
    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "raw_copy_count": len(records),
        "source_chain_count": len(normalized),
        "records": normalized,
        "execution_authority": False,
    }


def fallback_eligibility(
    policy: SupplementalSourcePolicy, *, live_acceptance: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if not policy.enabled:
        reasons.append("SOURCE_DISABLED")
    if policy.shadow_only:
        reasons.append("SHADOW_ONLY")
    if not policy.execution_authority:
        reasons.append("NO_EXECUTION_AUTHORITY")
    if policy.license_status not in _VERIFIED_LICENSES:
        reasons.append("LICENSE_NOT_VERIFIED")
    if policy.live_status != "LIVE_VERIFIED":
        reasons.append("LIVE_CAPABILITY_NOT_VERIFIED")
    for key, reason in (
        ("semantic_match", "SEMANTIC_MATCH_NOT_VERIFIED"),
        ("pit_verified", "PIT_NOT_VERIFIED"),
        ("coverage_verified", "COVERAGE_NOT_VERIFIED"),
        ("independent_upstream", "UPSTREAM_INDEPENDENCE_NOT_VERIFIED"),
    ):
        if live_acceptance.get(key) is not True:
            reasons.append(reason)
    return {"eligible": not reasons, "reason_codes": reasons, "source_id": policy.source_id}


def run_shadow_collection(
    policy: SupplementalSourcePolicy,
    *,
    registry: ProviderRegistry,
    governor: ProviderGovernor | None,
    request: ProviderRequest | None,
    operation: Callable[[ProviderRequest, float], ProviderEnvelope] | None,
    deadline_seconds: float = 5.0,
) -> dict[str, Any]:
    """Run only after configuration, license and S02 governance all allow it."""

    base = {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "source_id": policy.source_id,
        "shadow_only": True,
        "execution_authority": False,
    }
    if not policy.enabled:
        return {**base, "status": "DISABLED", "reason_code": "SUPPLEMENTAL_SOURCE_DISABLED"}
    if policy.license_status not in _VERIFIED_LICENSES or policy.live_status != "LIVE_VERIFIED":
        return {**base, "status": "LIVE_UNVERIFIED", "reason_code": "SUPPLEMENTAL_SOURCE_NOT_AUTHORIZED_FOR_LIVE"}
    try:
        registry.require(policy.provider, policy.capability)
    except (KeyError, PermissionError):
        return {**base, "status": "UNSUPPORTED", "reason_code": "PROVIDER_CAPABILITY_UNAUTHORIZED"}
    if governor is None or request is None or operation is None:
        return {**base, "status": "ADAPTER_OFFLINE_TESTED", "reason_code": "LIVE_OPERATION_NOT_CONFIGURED"}
    result = governor.fetch(request, operation, deadline_seconds=deadline_seconds)
    return {
        **base,
        "status": result.status.value,
        "reason_code": result.reason_code,
        "effective_at": result.effective_at,
        "fetched_at": result.fetched_at,
        "payload": result.payload,
    }


def compare_shadow_records(
    *, primary: Sequence[Mapping[str, Any]], shadow: Sequence[Mapping[str, Any]], as_of: datetime,
) -> dict[str, Any]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("SUPPLEMENTAL_COMPARISON_CUTOFF_NAIVE")
    primary_by_id = {str(item.get("object_id")): item for item in primary if item.get("object_id")}
    shadow_by_id = {str(item.get("object_id")): item for item in shadow if item.get("object_id")}
    matched = sorted(set(primary_by_id).intersection(shadow_by_id))
    incremental = 0
    conflicts = 0
    stale = 0
    ages: list[float] = []
    latency = 0.0
    for object_id, item in shadow_by_id.items():
        effective = _aware(item.get("effective_at"))
        if effective is None:
            stale += 1
        else:
            age = max(0.0, (as_of.astimezone(timezone.utc) - effective.astimezone(timezone.utc)).total_seconds())
            ages.append(age)
            if age > 86400:
                stale += 1
        latency += max(0.0, _finite(item.get("latency_ms")) or 0.0)
        primary_values = primary_by_id.get(object_id, {}).get("values")
        primary_values = primary_values if isinstance(primary_values, Mapping) else {}
        shadow_values = item.get("values") if isinstance(item.get("values"), Mapping) else {}
        for field, value in shadow_values.items():
            old = primary_values.get(field)
            if value is not None and old is None:
                incremental += 1
            elif value is not None and old is not None and value != old:
                conflicts += 1
    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "as_of": as_of.isoformat(),
        "primary_object_count": len(primary_by_id),
        "shadow_object_count": len(shadow_by_id),
        "matched_object_count": len(matched),
        "incremental_effective_field_count": incremental,
        "conflict_count": conflicts,
        "stale_object_count": stale,
        "stale_ratio": (stale / len(shadow_by_id)) if shadow_by_id else None,
        "shadow_age_seconds_max": max(ages) if ages else None,
        "resource_cost": {"latency_ms_total": latency},
        "execution_authority": False,
    }


__all__ = [
    "SupplementalSourcePolicy",
    "attach_shadow_report",
    "build_shadow_fact_envelope",
    "compare_shadow_records",
    "fallback_eligibility",
    "load_supplemental_source_policies",
    "normalize_business_profile",
    "normalize_opinion_records",
    "normalize_quote_record",
    "normalize_taxonomy_record",
    "run_shadow_collection",
    "to_a1_coverage_observation",
]
