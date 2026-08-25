"""Normalize HiThink endpoint outcomes into immutable fact snapshots."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from ..pipeline.data_source import HithinkFetchResult
from .contracts import (
    FactSnapshotManifest,
    RealtimeFactEnvelope,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json_bytes,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def normalize_hithink_results(
    results: Mapping[str, HithinkFetchResult],
    *,
    base_url: str,
    as_of: datetime,
    snapshot_id: str | None = None,
    ingest_time: datetime | None = None,
) -> FactSnapshotManifest:
    """Build one deterministic manifest without treating failed data as empty."""

    effective_as_of = _aware(as_of)
    ingested = _aware(ingest_time or datetime.now(SHANGHAI))
    facts: list[RealtimeFactEnvelope] = []
    health: list[SourceHealth] = []
    checksums: dict[str, str] = {}
    coverage: dict[str, float] = {}
    for fact_type, result in sorted(results.items()):
        source_id = _source_id(result.endpoint)
        payload = {
            "endpoint": result.endpoint,
            "metadata": result.metadata,
            "record_count": len(result.items),
            "records": [row.model_dump(mode="json") for row in result.items],
        }
        available = bool(result.ok and result.complete)
        content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest() if available else None
        event_time = _event_time(result.metadata.get("timestamp"), result.fetch_time)
        fact_digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "source_id": source_id,
                    "fact_type": fact_type,
                    "event_time": event_time,
                    "content_hash": content_hash,
                    "reason_code": result.reason_code,
                }
            )
        ).hexdigest()
        facts.append(
            RealtimeFactEnvelope(
                fact_id=f"sha256:{fact_digest}",
                source_id=source_id,
                source_tier=SourceTier.T2,
                fact_type=fact_type,
                event_time=event_time,
                fetch_time=result.fetch_time,
                ingest_time=max(ingested, result.fetch_time),
                available=available,
                reason_code=result.reason_code,
                source_url=urljoin(f"{base_url.rstrip('/')}/", result.endpoint.lstrip("/")),
                content_hash=content_hash,
                payload=payload,
            )
        )
        health.append(
            SourceHealth(
                source_id=source_id,
                status=SourceHealthStatus.HEALTHY if available else SourceHealthStatus.UNAVAILABLE,
                checked_at=max(ingested, result.fetch_time),
                last_success_time=result.fetch_time if available else None,
                reason_code=result.reason_code,
                coverage=1.0 if available else 0.0,
                http_status=result.http_status,
                available=available,
                details={
                    "complete": result.complete,
                    "pages": result.pages,
                    "record_count": len(result.items),
                },
            )
        )
        if content_hash is not None:
            checksums[fact_type] = content_hash
        coverage[fact_type] = 1.0 if available else 0.0

    manifest_id = snapshot_id
    # Realtime endpoint timestamps can be generated a few seconds after the
    # caller starts collection.  The frozen fact cutoff is the latest event
    # actually included, never the earlier request-start timestamp.
    if facts:
        effective_as_of = max(effective_as_of, *(fact.event_time for fact in facts))
    if manifest_id is None:
        identity = hashlib.sha256(
            canonical_json_bytes(
                {
                    "as_of": effective_as_of,
                    "facts": facts,
                    "health": health,
                }
            )
        ).hexdigest()
        manifest_id = f"hithink-{identity[:24]}"
    return FactSnapshotManifest(
        snapshot_id=manifest_id,
        as_of=effective_as_of,
        facts=tuple(facts),
        source_health=tuple(health),
        source_checksums=checksums,
        coverage_by_fact_type=coverage,
    )


def manifest_projection(manifest: FactSnapshotManifest) -> dict[str, Any]:
    """Return the hash-bound prompt projection for a frozen input snapshot."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for fact in manifest.facts:
        payload = dict(fact.payload)
        if payload.get("prompt_injection_suspected") is True:
            # Keep the immutable fact manifest verbatim for audit, but never
            # forward suspicious provider-controlled prose to a model.  The
            # same projection is shared by CNINFO announcements and official
            # policy search results, hence the deliberately small allow-list
            # of untrusted text fields below.
            for field in ("announcement_title", "title", "summary"):
                if field in payload:
                    payload[field] = "[UNTRUSTED_TEXT_BLOCKED]"
            if "pdf_evidence_snippets" in payload:
                payload["pdf_evidence_snippets"] = []
                payload["pdf_evidence_text_blocked"] = True
        grouped.setdefault(fact.fact_type, []).append({
            "fact_id": fact.fact_id,
            "fact_type": fact.fact_type,
            "symbol": fact.symbol,
            "available": fact.available,
            "reason_code": fact.reason_code,
            "event_time": fact.event_time.isoformat(),
            "publish_time": fact.publish_time.isoformat() if fact.publish_time is not None else None,
            "fetch_time": fact.fetch_time.isoformat(),
            "source_id": fact.source_id,
            "source_url": fact.source_url,
            "content_hash": fact.content_hash,
            **payload,
        })
    facts: dict[str, Any] = {}
    for fact_type, records in grouped.items():
        if len(records) == 1:
            facts[fact_type] = records[0]
        else:
            facts[fact_type] = {
                "available": all(record.get("available") is True for record in records),
                "reason_code": "OK" if all(record.get("available") is True for record in records) else "PARTIAL_SOURCE_FAILURE",
                "record_count": len(records),
                "records": records,
            }
    return {
        "schema_version": manifest.schema_version,
        "snapshot_id": manifest.snapshot_id,
        "as_of": manifest.as_of.isoformat(),
        "manifest_hash": manifest.manifest_hash,
        "facts_sha256": manifest.facts_sha256,
        "coverage_by_fact_type": manifest.coverage_by_fact_type,
        "facts": facts,
        "fact_groups": grouped,
        "source_health": [item.model_dump(mode="json") for item in manifest.source_health],
    }


def collect_market_results(
    client: Any,
    symbols: Sequence[str],
) -> dict[str, HithinkFetchResult]:
    """Fetch the Phase-1 market facts; each endpoint retains its own status."""

    results = {
        "THS_INDUSTRY_CATALOG": client.ths_index_catalog(tag="industry"),
        "THS_CONCEPT_CATALOG": client.ths_index_catalog(tag="cn_concept"),
        "LIMIT_UP_POOL": client.limit_up_pool(),
        "LIMIT_DOWN_POOL": client.limit_down_pool(),
        "LIMIT_BREAK_POOL": client.limit_break_pool(),
        "LIMIT_UP_LADDER": client.limit_up_ladder(),
        "DRAGON_TIGER_LIST": client.dragon_tiger_list(),
        "HOT_STOCK_LIST": client.hot_stock_list(period="hour"),
    }
    if symbols:
        results["AUCTION_FINAL"] = client.auction_snapshot(symbols, stage="final")
    return results


def _source_id(endpoint: str) -> str:
    tail = endpoint.strip("/").replace("/", ".").replace("-", "_")
    return f"hithink.{tail}"[-128:]


def _event_time(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value) / 1000 if abs(float(value)) >= 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=SHANGHAI)
        except (OSError, OverflowError, ValueError):
            pass
    if isinstance(value, str) and value.strip():
        try:
            return _aware(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
        except ValueError:
            pass
    return _aware(fallback)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("fact snapshot timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = ["collect_market_results", "manifest_projection", "normalize_hithink_results"]
