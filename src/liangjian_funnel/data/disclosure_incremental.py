"""Compose complete historical locator evidence with a complete recent delta.

This changes neither provider facts nor their dates. A TTL alone never proves
that yesterday's announcement query covers today's disclosure window.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from .cninfo import CninfoFetchResult
from ..pipeline.local_fact_cache import canonical_json_hash


def covers_query(result: CninfoFetchResult, *, symbol: str, start: str, end: str,
                 keyword: str, now: datetime, cache_keyword: str | None = None) -> bool:
    try:
        return bool(result.ok and result.complete and result.symbol == symbol
                    and date.fromisoformat(result.start_date) <= date.fromisoformat(start) <= date.fromisoformat(end)
                    and date.fromisoformat(result.end_date) == date.fromisoformat(end) <= now.date()
                    and result.fetched_at <= now
                    and result.metadata.get("search_keyword", cache_keyword or "") == keyword)
    except (ValueError, TypeError):
        return False


def compose_disclosure_delta(base: CninfoFetchResult, recent: CninfoFetchResult, *,
                             symbol: str, start: str, end: str, keyword: str,
                             now: datetime, max_age: timedelta,
                             base_keyword: str | None = None) -> CninfoFetchResult | None:
    """Return a dated composite only when both query intervals prove coverage."""
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        base_first, base_last = date.fromisoformat(base.start_date), date.fromisoformat(base.end_date)
        recent_first = date.fromisoformat(recent.start_date)
        if (not base.ok or not base.complete or base.symbol != symbol
                or base.metadata.get("search_keyword", base_keyword or "") != keyword
                or base.metadata.get("availability_state") == "STALE_VERIFIED_FALLBACK"
                or not timedelta(0) <= now - base.fetched_at <= max_age
                or not base_first <= base_last or not first <= last
                or base_first > first or base_last > last
                or recent_first > base_last + timedelta(days=1)
                or not covers_query(recent, symbol=symbol, start=recent.start_date,
                                    end=end, keyword="", now=now)):
            return None
        # A new revision of an old ID must not be silently preferred. The
        # caller falls back to the original authoritative full query instead.
        by_id = {}
        seen = {}
        for query in (base, recent):
            for row in query.announcements:
                if (row.sec_code != symbol.split(".")[0] or row.publish_time > now
                        or not query.start_date <= row.publish_time.date().isoformat() <= query.end_date):
                    return None
                previous = seen.get(row.announcement_id)
                if previous is not None and previous.content_hash != row.content_hash:
                    return None
                seen[row.announcement_id] = row
                # CNINFO keyword matching is fuzzy (e.g. 年报工作规程 is
                # returned for 年度报告). Never reimplement it with a title
                # substring filter. Keep the complete historical locator and
                # the unfiltered recent delta; later fact/PDF selectors own
                # relevance. This is explicitly a union projection, not a
                # claim to have repeated the provider's keyword query.
                if first <= row.publish_time.date() <= last:
                    by_id[row.announcement_id] = row
        rows = tuple(sorted(by_id.values(), key=lambda r: (r.publish_time, r.announcement_id)))
        manifest = [
            {"role": role, "source_id": value.source_id, "start_date": value.start_date,
             "end_date": value.end_date, "fetched_at": value.fetched_at.isoformat(),
             "content_hash": canonical_json_hash(value.model_dump(mode="json")),
             "search_keyword": keyword if role == "historical_base" else "",
             "keyword_identity": "RESULT_METADATA" if "search_keyword" in value.metadata else "FIXED_SEMANTIC_CACHE_KEY"}
            for role, value in (("historical_base", base), ("complete_recent_delta", recent))
        ]
        return base.model_copy(update={
            "start_date": start, "end_date": end, "announcements": rows,
            "total": len(rows), "total_pages": None,
            "pages": base.pages + recent.pages,
            "attempts": base.attempts + recent.attempts,
            "fetched_at": max(base.fetched_at, recent.fetched_at),
            "source_id": "official_disclosure_incremental_projection",
            "metadata": {**base.metadata, "search_keyword": keyword,
                         "cache_status": "VERIFIED_HISTORY_WITH_COMPLETE_DELTA",
                         "result_semantics": "HISTORICAL_LOCATOR_UNION_UNFILTERED_RECENT",
                         "availability_state": "READY", "incremental_evidence": manifest},
        })
    except (ValueError, TypeError, OverflowError):
        return None
