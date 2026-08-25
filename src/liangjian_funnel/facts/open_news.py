"""Normalize open news results into T3, hash-bound fact snapshots.

Open news is useful context, not authoritative financial evidence.  This
module therefore keeps every title and summary marked as untrusted text,
retains independent health for every provider/stock/RSS source, and never
turns a failed request into an empty successful fact.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..data.open_news import (
    CLS_SOURCE_ID,
    EASTMONEY_7X24_SOURCE_ID,
    OpenNewsClient,
    OpenNewsFetchResult,
    OpenNewsItem,
    collect_open_news,
    collect_open_news_results,
)
from .contracts import (
    FactEnvelope,
    FactSnapshotManifest,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json_bytes,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
MARKET_NEWS_FLASH = "MARKET_NEWS_FLASH"
STOCK_NEWS_ITEM = "STOCK_NEWS_ITEM"
INDUSTRY_RSS_ITEM = "INDUSTRY_RSS_ITEM"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("open news timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _fact_type(result: OpenNewsFetchResult, item: OpenNewsItem | None = None) -> str:
    if result.source_id in {CLS_SOURCE_ID, EASTMONEY_7X24_SOURCE_ID}:
        return MARKET_NEWS_FLASH
    if result.channel == "eastmoney_stock" or (item is not None and item.symbol is not None):
        return STOCK_NEWS_ITEM
    return INDUSTRY_RSS_ITEM


def _safe_source_key(source_id: str) -> str:
    return source_id.replace(".", "_").replace("/", "_").replace(":", "_")


def _item_key(item: OpenNewsItem) -> str:
    return f"{item.url}|{item.provider_item_id or item.title}"


def normalize_open_news_results(
    results: Mapping[str, OpenNewsFetchResult] | Iterable[OpenNewsFetchResult],
    *,
    as_of: datetime,
    ingest_time: datetime | None = None,
    snapshot_id: str | None = None,
) -> FactSnapshotManifest:
    """Build one deterministic manifest from independent news outcomes.

    Successful sources are merged and reposts deduplicated.  Failed sources
    contribute health observations only, while malformed/future items are
    excluded from facts and counted in their source details.
    """

    cutoff = _aware(as_of)
    values = tuple(results.values()) if isinstance(results, Mapping) else tuple(results)
    ingested = _aware(ingest_time or datetime.now(SHANGHAI))
    usable_by_source: dict[str, tuple[OpenNewsItem, ...]] = {}
    health: list[SourceHealth] = []
    checksums: dict[str, str] = {}
    type_totals: dict[str, int] = {}
    type_available: dict[str, int] = {}
    source_for_item: dict[str, OpenNewsFetchResult] = {}
    source_names: dict[str, str] = {}
    all_items: list[OpenNewsItem] = []

    for result in sorted(values, key=lambda item: (item.source_id, item.channel, item.fetched_at.isoformat())):
        configured_name = result.metadata.get("source_name")
        if isinstance(configured_name, str) and configured_name.strip():
            source_names[result.source_id] = configured_name.strip()[:120]
        semantic_type = _fact_type(result)
        type_totals[semantic_type] = type_totals.get(semantic_type, 0) + 1
        source_available = bool(result.ok and result.complete)
        if source_available:
            type_available[semantic_type] = type_available.get(semantic_type, 0) + 1
        future_count = 0
        usable: list[OpenNewsItem] = []
        if source_available:
            for item in result.items:
                if item.publish_time > result.fetched_at or item.publish_time > ingested:
                    future_count += 1
                    continue
                usable.append(item)
                all_items.append(item)
                source_for_item[_item_key(item)] = result
            usable_by_source[result.source_id] = tuple(usable)
        raw_count = len(result.items) + result.dropped_missing_time + result.dropped_invalid_items
        rejected_count = result.dropped_missing_time + result.dropped_invalid_items + future_count
        coverage = (
            len(usable) / raw_count
            if raw_count
            else (1.0 if source_available else 0.0)
        )
        if not source_available:
            health_status = SourceHealthStatus.UNAVAILABLE
            health_reason = result.reason_code
        elif rejected_count or result.reason_code != "OK":
            health_status = SourceHealthStatus.DEGRADED
            health_reason = result.reason_code if result.reason_code != "OK" else "PARTIAL_ITEM_COVERAGE"
        else:
            health_status = SourceHealthStatus.HEALTHY
            health_reason = "OK"
        checked_at = max(ingested, result.fetched_at)
        health.append(
            SourceHealth(
                source_id=result.source_id,
                status=health_status,
                checked_at=checked_at,
                last_success_time=result.fetched_at if source_available else None,
                reason_code=health_reason,
                coverage=coverage,
                http_status=result.http_status,
                available=source_available,
                details={
                    "channel": result.channel,
                    "complete": result.complete,
                    "attempts": result.attempts,
                    "raw_item_count": raw_count,
                    "usable_item_count": len(usable),
                    "dropped_missing_time": result.dropped_missing_time,
                    "dropped_invalid_items": result.dropped_invalid_items,
                    "future_time_count": future_count,
                },
            )
        )
        if source_available:
            checksums[_safe_source_key(result.source_id)] = hashlib.sha256(
                canonical_json_bytes([item.model_dump(mode="json") for item in result.items])
            ).hexdigest()

    deduped = collect_open_news(
        {
            result.source_id: result.model_copy(update={"items": usable_by_source.get(result.source_id, ())})
            for result in values
            if result.source_id in usable_by_source
        }
    )
    facts: list[FactEnvelope] = []
    for item in deduped:
        result = source_for_item.get(_item_key(item))
        if result is None:
            # A deduplicated representative is always sourced above.  Keep
            # this guard fail-closed if a future model changes its identity.
            continue
        fact_type = _fact_type(result, item)
        payload = {
            "provider_item_id": item.provider_item_id,
            "title": item.title,
            "summary": item.summary,
            "publish_time": item.publish_time.isoformat(),
            "url": item.url,
            "symbol": item.symbol,
            "channel": item.channel,
            "source_id": item.source_id,
            "fetched_at": item.fetched_at.isoformat(),
            "original_sources": list(item.original_sources),
            "source_name": source_names.get(item.source_id),
            "original_source_names": [
                source_names[source]
                for source in item.original_sources
                if source in source_names
            ],
            "industry_hint": result.metadata.get("industry_hint"),
            "repost_count": item.repost_count,
            "untrusted_text": True,
            "prompt_injection_suspected": item.prompt_injection_suspected,
        }
        content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        identity = hashlib.sha256(
            canonical_json_bytes(
                {
                    "source_id": item.source_id,
                    "provider_item_id": item.provider_item_id,
                    "url": item.url,
                    "publish_time": item.publish_time,
                    "content_hash": content_hash,
                }
            )
        ).hexdigest()
        facts.append(
            FactEnvelope(
                fact_id=f"sha256:{identity}",
                source_id=item.source_id,
                source_tier=SourceTier.T3,
                fact_type=fact_type,
                symbol=item.symbol,
                event_time=item.publish_time,
                publish_time=item.publish_time,
                fetch_time=max(result.fetched_at, item.publish_time),
                ingest_time=max(ingested, result.fetched_at, item.publish_time),
                available=True,
                reason_code="OK",
                source_url=item.url,
                content_hash=content_hash,
                payload=payload,
            )
        )

    if facts:
        cutoff = max(cutoff, *(fact.publish_time for fact in facts))
    if snapshot_id is None:
        identity = hashlib.sha256(
            canonical_json_bytes(
                {
                    "as_of": cutoff,
                    "results": [
                        {
                            "source_id": result.source_id,
                            "reason_code": result.reason_code,
                            "fetched_at": result.fetched_at,
                            "item_count": len(result.items),
                        }
                        for result in sorted(values, key=lambda item: item.source_id)
                    ],
                    "facts": facts,
                }
            )
        ).hexdigest()
        snapshot_id = f"open-news-{identity[:24]}"

    coverage: dict[str, float] = {
        "OPEN_NEWS_QUERY": (
            sum(1 for result in values if result.ok and result.complete) / len(values)
            if values
            else 0.0
        )
    }
    for fact_type, total in sorted(type_totals.items()):
        coverage[fact_type] = type_available.get(fact_type, 0) / total if total else 0.0
    return FactSnapshotManifest(
        snapshot_id=snapshot_id,
        as_of=cutoff,
        facts=tuple(facts),
        source_health=tuple(health),
        source_checksums=checksums,
        coverage_by_fact_type=coverage,
    )


def normalize_open_news_result(
    result: OpenNewsFetchResult,
    *,
    as_of: datetime,
    ingest_time: datetime | None = None,
    snapshot_id: str | None = None,
) -> FactSnapshotManifest:
    """Singular-result convenience wrapper."""

    return normalize_open_news_results(
        {result.source_id: result},
        as_of=as_of,
        ingest_time=ingest_time,
        snapshot_id=snapshot_id,
    )


def collect_open_news_for_workflow(
    client: OpenNewsClient,
    *,
    symbols: Iterable[str] = (),
    rss_sources: Iterable[str | Mapping[str, str]] = (),
    page_size: int = 50,
) -> dict[str, OpenNewsFetchResult]:
    """Workflow-facing pure collection wrapper retaining per-source results."""

    return collect_open_news_results(
        client,
        symbols=symbols,
        rss_sources=rss_sources,
        page_size=page_size,
    )


def open_news_projection(manifest: FactSnapshotManifest) -> dict[str, Any]:
    """Return a model-facing projection with suspicious prose blocked."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for fact in manifest.facts:
        payload = dict(fact.payload)
        if payload.get("prompt_injection_suspected") is True:
            for field in ("title", "summary"):
                if field in payload:
                    payload[field] = "[UNTRUSTED_TEXT_BLOCKED]"
            payload["untrusted_text_blocked"] = True
        record = {
            "fact_id": fact.fact_id,
            "fact_type": fact.fact_type,
            "source_id": fact.source_id,
            "symbol": fact.symbol,
            "available": fact.available,
            "reason_code": fact.reason_code,
            "event_time": fact.event_time.isoformat(),
            "publish_time": fact.publish_time.isoformat(),
            "fetch_time": fact.fetch_time.isoformat(),
            "source_url": fact.source_url,
            "content_hash": fact.content_hash,
            **payload,
        }
        grouped.setdefault(fact.fact_type, []).append(record)
    return {
        "schema_version": manifest.schema_version,
        "snapshot_id": manifest.snapshot_id,
        "as_of": manifest.as_of.isoformat(),
        "manifest_hash": manifest.manifest_hash,
        "facts_sha256": manifest.facts_sha256,
        "coverage_by_fact_type": manifest.coverage_by_fact_type,
        "facts": grouped,
        "source_health": [item.model_dump(mode="json") for item in manifest.source_health],
    }


# Provider-neutral aliases for the main workflow.
normalize_news_results = normalize_open_news_results
normalize_news_result = normalize_open_news_result
collect_news_for_workflow = collect_open_news_for_workflow


__all__ = [
    "INDUSTRY_RSS_ITEM",
    "MARKET_NEWS_FLASH",
    "STOCK_NEWS_ITEM",
    "collect_news_for_workflow",
    "collect_open_news_for_workflow",
    "normalize_news_result",
    "normalize_news_results",
    "normalize_open_news_result",
    "normalize_open_news_results",
    "open_news_projection",
]
