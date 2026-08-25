"""Normalize official policy-library metadata into auditable macro facts."""

from __future__ import annotations

import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

from ..data.gov_policy import GovPolicyFetchResult
from .contracts import (
    FactEnvelope,
    FactSnapshotManifest,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json_bytes,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def normalize_gov_policy_result(
    result: GovPolicyFetchResult,
    *,
    as_of: datetime,
    ingest_time: datetime | None = None,
    snapshot_id: str | None = None,
) -> FactSnapshotManifest:
    cutoff = _aware(as_of)
    ingested = max(_aware(ingest_time or datetime.now(SHANGHAI)), result.fetched_at)
    source_id = "gov.policy_library"
    usable = [
        item
        for item in result.documents
        if item.publish_time is not None and item.publish_time <= result.fetched_at
    ] if result.ok and result.complete else []
    missing_time = sum(item.publish_time is None for item in result.documents)
    future_time = sum(
        item.publish_time is not None and item.publish_time > result.fetched_at
        for item in result.documents
    )
    facts: list[FactEnvelope] = []
    for item in usable:
        assert item.publish_time is not None
        payload = {
            "policy_id": item.document_id,
            "title": item.title,
            "summary": item.summary,
            "issuing_body": item.issuing_body,
            "document_number": item.document_number,
            "official_category": item.category,
            "formal_document": True,
            "direct_stock_mapping_allowed": False,
            "financial_transmission_evidence": False,
            "untrusted_text": True,
            "prompt_injection_suspected": item.prompt_injection_suspected,
        }
        content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        identity = hashlib.sha256(
            canonical_json_bytes({
                "source": source_id,
                "policy_id": item.document_id,
                "url": item.url,
                "content_hash": content_hash,
            })
        ).hexdigest()
        facts.append(FactEnvelope(
            fact_id=f"sha256:{identity}",
            source_id=source_id,
            source_tier=SourceTier.T1,
            fact_type="MACRO_POLICY_EVENT",
            symbol=None,
            event_time=item.publish_time,
            publish_time=item.publish_time,
            fetch_time=result.fetched_at,
            ingest_time=ingested,
            available=True,
            reason_code="OK",
            source_url=item.url,
            content_hash=content_hash,
            payload=payload,
        ))

    query_available = bool(result.ok and result.complete)
    status = SourceHealthStatus.UNAVAILABLE
    health_reason = result.reason_code
    if query_available:
        status = SourceHealthStatus.DEGRADED if missing_time or future_time else SourceHealthStatus.HEALTHY
        health_reason = "PARTIAL_TIMESTAMP_COVERAGE" if missing_time or future_time else result.reason_code
    health = SourceHealth(
        source_id=source_id,
        status=status,
        checked_at=ingested,
        last_success_time=result.fetched_at if query_available else None,
        reason_code=health_reason,
        coverage=1.0 if query_available else 0.0,
        http_status=result.http_status,
        available=query_available,
        details={
            "query_start_date": result.start_date,
            "query_end_date": result.end_date,
            "formal_document_count": len(result.documents),
            "usable_document_count": len(usable),
            "missing_publish_time_count": missing_time,
            "future_publish_time_count": future_time,
            "pages": result.pages,
            "complete": result.complete,
            "excluded_categories": ["otherfile", "gongbao"],
        },
    )
    checksum_values = [item.model_dump(mode="json") for item in result.documents]
    checksums = {
        "GOV_POLICY_LIBRARY": hashlib.sha256(canonical_json_bytes(checksum_values)).hexdigest()
    } if query_available else {}
    timestamp_coverage = (
        len(usable) / len(result.documents)
        if result.documents
        else (1.0 if query_available else 0.0)
    )
    if facts:
        cutoff = max(cutoff, *(fact.publish_time for fact in facts))
    if snapshot_id is None:
        digest = hashlib.sha256(canonical_json_bytes({
            "as_of": cutoff,
            "start": result.start_date,
            "end": result.end_date,
            "reason": result.reason_code,
            "documents": checksum_values,
        })).hexdigest()
        snapshot_id = f"gov-policy-{digest[:24]}"
    return FactSnapshotManifest(
        snapshot_id=snapshot_id,
        as_of=cutoff,
        facts=tuple(facts),
        source_health=(health,),
        source_checksums=checksums,
        coverage_by_fact_type={
            "GOV_POLICY_QUERY": 1.0 if query_available else 0.0,
            "GOV_POLICY_PUBLISH_TIMESTAMP": timestamp_coverage,
        },
    )


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("government policy timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = ["normalize_gov_policy_result"]
