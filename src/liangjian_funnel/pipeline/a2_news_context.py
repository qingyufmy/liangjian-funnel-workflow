"""Point-in-time news clues for A2 audit only; never a ranking or entry signal.

Exact title AND summary matching is deliberately conservative. It does not
claim semantic event clustering or independent confirmation of issuer claims.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any


def _time(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result if result.utcoffset() is not None else None
    except (ValueError, TypeError):
        return None


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def build_a2_news_shadow(
    news: Mapping[str, Any] | None,
    decisions: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
    snapshot_hash: str,
    max_events: int = 120,
) -> dict[str, Any]:
    """Build bounded evidence, preserving omissions and all decision dispositions.

Absent arrival clocks are a visible evidence gap, never inferred from publish
time. Source search association is a clue, not a verified business relationship.
    """
    if as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    if max_events < 1:
        raise ValueError("max_events must be positive")
    news = news if isinstance(news, Mapping) else {}
    rows = news.get("items", [])
    rows = rows if isinstance(rows, (list, tuple)) else []
    excluded: Counter[str] = Counter()
    groups: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for raw in rows:
        if not isinstance(raw, Mapping):
            excluded["MALFORMED"] += 1
            continue
        fact_id = raw.get("fact_id")
        published, fetched, ingested = (_time(raw.get(k)) for k in
                                        ("publish_time", "fetch_time", "ingest_time"))
        if not fact_id or not raw.get("content_hash") or not raw.get("source_url"):
            excluded["EVIDENCE_REFERENCE_MISSING"] += 1
            continue
        if published is None or fetched is None or ingested is None:
            excluded["ARRIVAL_OR_PUBLICATION_TIME_MISSING"] += 1
            continue
        if max(published, fetched, ingested) > as_of:
            excluded["NOT_KNOWN_AT_CUTOFF"] += 1
            continue
        if published > fetched or fetched > ingested:
            excluded["INVALID_TIME_ORDER"] += 1
            continue
        if published < as_of - timedelta(days=7):
            excluded["OUTSIDE_NEWS_WINDOW"] += 1
            continue
        if raw.get("prompt_injection_suspected") or any(
            "[UNTRUSTED_TEXT_BLOCKED]" in str(raw.get(k)) for k in ("title", "summary")
        ):
            excluded["UNTRUSTED_TEXT_BLOCKED"] += 1
            continue
        if str(fact_id) in seen:
            excluded["DUPLICATE_FACT_ID"] += 1
            continue
        seen.add(str(fact_id))
        title = " ".join(str(raw.get("title") or "").split())
        summary = " ".join(str(raw.get("summary") or "").split())
        if not title:
            excluded["TITLE_MISSING"] += 1
            continue
        # Include the source publication day to avoid merging repeated notices
        # across days; different summaries remain separate possible revisions.
        key = _hash([title, summary, published.astimezone(as_of.tzinfo).date().isoformat()])
        event = groups.setdefault(key, {
            "event_id": key, "title": title[:500], "summary": summary[:300],
            "grouping": "EXACT_TEXT_SAME_DAY_ONLY", "untrusted_text": True,
            "evidence_tier": "T3", "business_relationship_verified": False,
            "independent_confirmation_count": None, "references": [],
            "symbols": [], "sources": [],
        })
        event["references"].append({k: raw.get(k) for k in
            ("fact_id", "content_hash", "source_url", "source_id", "publish_time", "fetch_time", "ingest_time")})
        if raw.get("symbol"):
            event["symbols"].append(str(raw["symbol"]).upper())
        if raw.get("source_id"):
            event["sources"].append(str(raw["source_id"]))
    events = []
    for event in groups.values():
        event["symbols"] = sorted(set(event["symbols"]))
        event["sources"] = sorted(set(event["sources"]))
        event["references"].sort(key=lambda x: str(x["fact_id"]))
        event["first_received_at"] = min(_time(x["ingest_time"]) for x in event["references"]).isoformat()
        event["report_count"] = len(event["references"])
        events.append(event)
    events.sort(key=lambda e: (e["first_received_at"], e["event_id"]), reverse=True)
    retained = events[:max_events]
    by_symbol = []
    for row in sorted(decisions, key=lambda r: str(r.get("symbol"))):
        symbol = str(row.get("symbol") or "").upper()
        matches = [e["event_id"] for e in retained if symbol in e["symbols"]]
        by_symbol.append({
            "symbol": symbol, "theme_id": row.get("theme_id"),
            "rotation_rank": row.get("theme_rotation_rank"),
            "market_role": row.get("deterministic_market_role", row.get("market_role")),
            "quant_status": row.get("status"),
            "quant_reason_codes": list(row.get("reason_codes") or []),
            "event_ids": matches,
            "association": "SOURCE_SYMBOL_CLUE_NOT_VERIFIED_EXPOSURE" if matches else "NO_LINKED_NEWS",
            "explanation": "有资讯线索，经营影响待核实；量化结论不变" if matches else "未匹配有效资讯，不作为淘汰依据",
        })
    report = {
        "schema_version": "a2-news-shadow/1", "mode": "SHADOW_ONLY",
        "as_of": as_of.isoformat(), "base_snapshot_hash": snapshot_hash,
        "source_available": news.get("available") is True,
        "source_reason": news.get("reason_code", "NEWS_SNAPSHOT_MISSING"),
        "input_item_count": len(rows), "excluded_counts": dict(sorted(excluded.items())),
        "eligible_fact_count": sum(e["report_count"] for e in events),
        "event_count": len(events), "omitted_event_count": max(0, len(events) - max_events),
        "upstream_omitted_item_count": news.get("omitted_item_count"),
        "events": retained, "candidates": by_symbol,
        "unlinked_event_ids": [e["event_id"] for e in retained if not any(
            c["symbol"] in e["symbols"] for c in by_symbol)],
        "ranking_changed": False, "sentiment_inferred": False,
        "capital_flow_inferred": False, "llm_input_enabled": False,
    }
    report["report_hash"] = _hash(report)
    return report
