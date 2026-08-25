"""Normalize public CNINFO announcement metadata into auditable facts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..data.cninfo import CninfoAnnouncement, CninfoFetchResult
from ..data.cninfo_pdf import CninfoPdfEvidence
from .contracts import (
    FactEnvelope,
    FactSnapshotManifest,
    SourceHealth,
    SourceHealthStatus,
    SourceTier,
    canonical_json_bytes,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
_RISK_KEYWORDS = (
    "风险", "退市", "减持", "质押", "冻结", "诉讼", "仲裁", "调查",
    "处罚", "处分", "问询", "警示", "担保", "逾期", "违约", "控制权变更",
)
_EARNINGS_KEYWORDS = ("年度报告", "半年度报告", "季度报告", "业绩预告", "业绩快报")
_ORDER_KEYWORDS = ("中标", "合同", "订单", "项目", "产能", "投资建设")
_ST_RISK = re.compile(r"(?:^|[^A-Z])(?:\*?ST|S\*ST)(?:[^A-Z]|$)", re.IGNORECASE)


def normalize_cninfo_results(
    results: Mapping[str, CninfoFetchResult],
    *,
    pdf_evidence: Mapping[tuple[str, str], CninfoPdfEvidence] | None = None,
    as_of: datetime,
    ingest_time: datetime | None = None,
    snapshot_id: str | None = None,
) -> FactSnapshotManifest:
    cutoff = _aware(as_of)
    ingested = _aware(ingest_time or datetime.now(SHANGHAI))
    facts: list[FactEnvelope] = []
    health: list[SourceHealth] = []
    checksums: dict[str, str] = {}
    successful = 0
    pdf_results = dict(pdf_evidence or {})
    pdf_requested_total = len(pdf_results)
    pdf_available_total = sum(item.available for item in pdf_results.values())
    for symbol, result in sorted(results.items()):
        source_id = f"cninfo.public.{symbol.replace('.', '_').lower()}"
        symbol_pdf = {
            identifier: item
            for (item_symbol, identifier), item in pdf_results.items()
            if item_symbol == symbol
        }
        checked_at = max(
            ingested,
            result.fetched_at,
            *(item.fetched_at for item in symbol_pdf.values()),
        )
        available = bool(result.ok and result.complete)
        if available:
            successful += 1
            query_digest = hashlib.sha256(
                canonical_json_bytes([item.model_dump(mode="json") for item in result.announcements])
            ).hexdigest()
            checksums[f"CNINFO_{symbol.replace('.', '_')}"] = query_digest
        if symbol_pdf:
            checksums[f"CNINFO_PDF_{symbol.replace('.', '_')}"] = hashlib.sha256(
                canonical_json_bytes([
                    item.model_dump(mode="json")
                    for _, item in sorted(symbol_pdf.items())
                ])
            ).hexdigest()
        pdf_available = sum(item.available for item in symbol_pdf.values())
        pdf_failed = len(symbol_pdf) - pdf_available
        health_status = SourceHealthStatus.HEALTHY if available else SourceHealthStatus.UNAVAILABLE
        health_reason = result.reason_code
        if available and pdf_failed:
            health_status = SourceHealthStatus.DEGRADED
            health_reason = "CNINFO_PDF_EVIDENCE_PARTIAL"
        health.append(
            SourceHealth(
                source_id=source_id,
                status=health_status,
                checked_at=checked_at,
                last_success_time=result.fetched_at if available else None,
                reason_code=health_reason,
                coverage=1.0 if available else 0.0,
                http_status=result.http_status,
                available=available,
                details={
                    "query_start_date": result.start_date,
                    "query_end_date": result.end_date,
                    "announcement_count": len(result.announcements),
                    "pages": result.pages,
                    "complete": result.complete,
                    "pdf_requested": len(symbol_pdf),
                    "pdf_available": pdf_available,
                    "pdf_failed": pdf_failed,
                    "pdf_evidence_coverage": pdf_available / len(symbol_pdf) if symbol_pdf else None,
                },
            )
        )
        if not available:
            continue
        for announcement in result.announcements:
            canonical_symbol = f"{announcement.sec_code}.{symbol.rsplit('.', 1)[-1]}"
            fact_type, tags = classify_cninfo_title(announcement.announcement_title)
            evidence = symbol_pdf.get(announcement.announcement_id)
            evidence_payload = _pdf_payload(evidence)
            injection_suspected = announcement.prompt_injection_suspected or bool(
                evidence and evidence.prompt_injection_suspected
            )
            payload = {
                "announcement_id": announcement.announcement_id,
                "announcement_title": announcement.announcement_title,
                "event_tags": tags,
                "sec_name": announcement.sec_name,
                "storage_time": announcement.storage_time.isoformat() if announcement.storage_time is not None else None,
                "untrusted_text": True,
                "prompt_injection_suspected": injection_suspected,
                **evidence_payload,
                "metadata_content_hash": announcement.content_hash,
            }
            content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
            identity = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "source": "cninfo",
                        "announcement_id": announcement.announcement_id,
                        "symbol": canonical_symbol,
                        "content_hash": content_hash,
                    }
                )
            ).hexdigest()
            facts.append(
                FactEnvelope(
                    fact_id=f"sha256:{identity}",
                    source_id=source_id,
                    source_tier=SourceTier.T1,
                    fact_type=fact_type,
                    symbol=canonical_symbol,
                    event_time=announcement.publish_time,
                    publish_time=announcement.publish_time,
                    fetch_time=max(
                        result.fetched_at,
                        announcement.publish_time,
                        evidence.fetched_at if evidence is not None else result.fetched_at,
                    ),
                    ingest_time=max(checked_at, announcement.publish_time),
                    available=True,
                    reason_code="OK",
                    source_url=announcement.pdf_url,
                    content_hash=content_hash,
                    payload=payload,
                )
            )
    total_sources = len(results)
    coverage = successful / total_sources if total_sources else 0.0
    if facts:
        cutoff = max(cutoff, *(fact.publish_time for fact in facts))
    if snapshot_id is None:
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "as_of": cutoff,
                    "queries": [
                        {
                            "symbol": symbol,
                            "reason_code": result.reason_code,
                            "count": len(result.announcements),
                        }
                        for symbol, result in sorted(results.items())
                    ],
                }
            )
        ).hexdigest()
        snapshot_id = f"cninfo-{digest[:24]}"
    coverage_by_fact_type = {"CNINFO_ANNOUNCEMENT_QUERY": coverage}
    if pdf_requested_total:
        coverage_by_fact_type["CNINFO_PDF_EVIDENCE"] = pdf_available_total / pdf_requested_total
    return FactSnapshotManifest(
        snapshot_id=snapshot_id,
        as_of=cutoff,
        facts=tuple(facts),
        source_health=tuple(health),
        source_checksums=checksums,
        coverage_by_fact_type=coverage_by_fact_type,
    )


def _pdf_payload(evidence: CninfoPdfEvidence | None) -> dict[str, Any]:
    if evidence is None:
        return {
            "pdf_downloaded": False,
            "pdf_evidence_available": False,
            "pdf_reason_code": "NOT_REQUESTED",
            "pdf_evidence_snippets": [],
        }
    return {
        "pdf_downloaded": evidence.pdf_sha256 is not None,
        "pdf_evidence_available": evidence.available,
        "pdf_reason_code": evidence.reason_code,
        "pdf_sha256": evidence.pdf_sha256,
        "pdf_cache_relative_path": evidence.cache_relative_path,
        "pdf_content_type": evidence.content_type,
        "pdf_byte_size": evidence.byte_size,
        "pdf_parser": evidence.parser,
        "pdf_page_count": evidence.page_count,
        "pdf_pages_scanned": evidence.pages_scanned,
        "pdf_extracted_chars": evidence.extracted_chars,
        "pdf_truncated": evidence.truncated,
        "pdf_cache_hit": evidence.cache_hit,
        "pdf_evidence_snippets": [item.model_dump(mode="json") for item in evidence.snippets],
    }


def classify_cninfo_title(title: str) -> tuple[str, list[str]]:
    normalized = title.upper()
    tags: list[str] = []
    if any(keyword.upper() in normalized for keyword in _RISK_KEYWORDS) or _ST_RISK.search(normalized):
        tags.append("RISK")
    if any(keyword.upper() in normalized for keyword in _EARNINGS_KEYWORDS):
        tags.append("EARNINGS")
    if any(keyword.upper() in normalized for keyword in _ORDER_KEYWORDS):
        tags.append("ORDER_OR_CAPACITY")
    if not tags:
        tags.append("GENERAL_DISCLOSURE")
    return ("RISK_EVENT" if "RISK" in tags else "DISCLOSURE_EVENT"), tags


def select_cninfo_pdf_candidates(
    result: CninfoFetchResult,
    *,
    limit: int,
) -> tuple[CninfoAnnouncement, ...]:
    """Choose bounded high-value disclosures for deterministic PDF enrichment."""

    if limit < 0:
        raise ValueError("CNINFO PDF candidate limit must be non-negative")
    if not result.ok or not result.complete or limit == 0:
        return ()
    ranked: list[tuple[int, float, str, CninfoAnnouncement]] = []
    for announcement in result.announcements:
        _, tags = classify_cninfo_title(announcement.announcement_title)
        tag_set = set(tags)
        if "RISK" in tag_set:
            priority = 0
        elif "ORDER_OR_CAPACITY" in tag_set:
            priority = 1
        elif "EARNINGS" in tag_set:
            priority = 2
        else:
            continue
        ranked.append(
            (
                priority,
                -announcement.publish_time.timestamp(),
                announcement.announcement_id,
                announcement,
            )
        )
    ranked.sort(key=lambda item: item[:3])
    return tuple(item[3] for item in ranked[:limit])


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("CNINFO fact timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = ["classify_cninfo_title", "normalize_cninfo_results", "select_cninfo_pdf_candidates"]
