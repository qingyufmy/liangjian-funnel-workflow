"""Point-in-time monthly strategy context for A1 discovery.

The context combines official policy documents, published macro series,
deterministic THS industry-cycle metrics and the prior theme registry.  It is
an evidence projection, not a stock selector: no symbol is introduced here.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .macro_regime import build_macro_asset_quadrant


SHANGHAI = ZoneInfo("Asia/Shanghai")
CONTEXT_VERSION = "monthly-strategy-context/1.0.0"


def build_monthly_strategy_context(
    snapshot: Mapping[str, Any],
    *,
    as_of: datetime | str,
    prior_registry: Mapping[str, Any] | None = None,
    policy_lookback_days: int = 120,
    policy_document_limit: int = 60,
) -> dict[str, Any]:
    """Build a bounded monthly A1 evidence view without future leakage."""

    cutoff = _aware(as_of)
    lower = cutoff - timedelta(days=max(30, min(int(policy_lookback_days), 366)))
    policy = snapshot.get("MACRO_POLICY_FEED")
    policy = policy if isinstance(policy, Mapping) else {}
    documents = []
    for raw in policy.get("official_documents", ()):
        if not isinstance(raw, Mapping) or raw.get("prompt_injection_suspected") is True:
            continue
        published = _parse_time(raw.get("publish_time") or raw.get("event_time"))
        if published is None or not lower <= published <= cutoff:
            continue
        documents.append(dict(raw))
    documents.sort(
        key=lambda item: (
            str(item.get("publish_time") or item.get("event_time") or ""),
            str(item.get("fact_id") or ""),
        ),
        reverse=True,
    )
    selected_documents = _diversified_policy_documents(documents, policy_document_limit)

    macro = snapshot.get("MACRO_ECONOMIC_DATA")
    macro = macro if isinstance(macro, Mapping) else {}
    industry_profit = snapshot.get("INDUSTRY_PROFIT_DATA")
    industry_profit = industry_profit if isinstance(industry_profit, Mapping) else {}
    industry_activity = snapshot.get("INDUSTRY_ACTIVITY_DATA")
    industry_activity = industry_activity if isinstance(industry_activity, Mapping) else {}
    sector_cycle = snapshot.get("SECTOR_CYCLE_SNAPSHOT")
    sector_cycle = sector_cycle if isinstance(sector_cycle, Mapping) else {}
    history = sector_cycle.get("history_metrics")
    history = history if isinstance(history, Mapping) else {}
    rotations = history.get("monthly_rotation_candidates")
    if not isinstance(rotations, Sequence) or isinstance(rotations, (str, bytes, bytearray)):
        rotations = history.get("persistent_mainline_candidates")
    rotations = [dict(item) for item in rotations or () if isinstance(item, Mapping)]

    registry = dict(prior_registry or {})
    prior_themes = registry.get("themes") if isinstance(registry.get("themes"), list) else []
    prior_nodes = registry.get("nodes") if isinstance(registry.get("nodes"), list) else []
    pillars = {
        "official_policy": bool(selected_documents) and policy.get("available") is True,
        "macro_economy": macro.get("available") is True,
        "industry_cycle_fundamentals": (
            industry_profit.get("available") is True
            or industry_activity.get("available") is True
        ),
        "industry_cycle": sector_cycle.get("available") is True and bool(rotations),
        "prior_theme_memory": bool(prior_themes),
    }
    core_ready = sum(bool(pillars[key]) for key in ("official_policy", "macro_economy", "industry_cycle"))
    status = "READY" if core_ready == 3 else "DEGRADED" if core_ready >= 2 else "BLOCKED"
    missing = [key for key, available in pillars.items() if not available]
    months = Counter(
        str(item.get("publish_time") or item.get("event_time") or "")[:7]
        for item in selected_documents
    )
    bodies = Counter(str(item.get("issuing_body") or "UNKNOWN") for item in selected_documents)
    return {
        "version": CONTEXT_VERSION,
        "strategy_month": cutoff.strftime("%Y-%m"),
        "as_of": cutoff.isoformat(),
        "g0_symbol_count": len(snapshot.get("g0_symbols", ())) if isinstance(snapshot.get("g0_symbols"), Sequence) else 0,
        "status": status,
        "missing_pillars": missing,
        "pillar_availability": pillars,
        "policy_window": {
            "start": lower.isoformat(),
            "end": cutoff.isoformat(),
            "lookback_days": (cutoff - lower).days,
            "full_document_count": len(documents),
            "selected_document_count": len(selected_documents),
            "documents_by_month": dict(sorted(months.items())),
            "top_issuing_bodies": [
                {"issuing_body": key, "document_count": value}
                for key, value in bodies.most_common(12)
            ],
            "official_documents": selected_documents,
        },
        "macro_economic_state": _bounded_mapping(macro),
        "macro_asset_quadrant": build_macro_asset_quadrant(snapshot),
        "cross_market_leads": _bounded_mapping(
            snapshot.get("CROSS_MARKET_LEAD_SNAPSHOT")
            if isinstance(snapshot.get("CROSS_MARKET_LEAD_SNAPSHOT"), Mapping)
            else {}
        ),
        "broker_research_consensus": _bounded_mapping(
            snapshot.get("BROKER_RESEARCH_CONSENSUS")
            if isinstance(snapshot.get("BROKER_RESEARCH_CONSENSUS"), Mapping)
            else {}
        ),
        "industry_profit_state": _bounded_mapping(industry_profit),
        "industry_activity_state": _bounded_mapping(industry_activity),
        "monthly_industry_rotation": rotations[:40],
        "prior_theme_registry": {
            "available": bool(prior_themes),
            "as_of": registry.get("as_of"),
            "version_hash": registry.get("version_hash"),
            "themes": prior_themes[:24],
            "nodes": prior_nodes[:100],
        },
        "runtime_contract": {
            "stock_selection_forbidden": True,
            "hardcoded_theme_allowlist_forbidden": True,
            "monthly_cycle_and_policy_must_be_reconciled": True,
            "missing_macro_must_be_reported": True,
        },
    }


def _diversified_policy_documents(documents: Sequence[Mapping[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Avoid letting one issuing body or one recent day consume the window."""

    cap = max(1, min(int(limit), 120))
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    per_body: Counter[str] = Counter()
    # First pass gives every issuing body representation; second fills by recency.
    for body_cap in (2, 6, cap):
        for raw in documents:
            fact_id = str(raw.get("fact_id") or raw.get("source_url") or "")
            body = str(raw.get("issuing_body") or "UNKNOWN")
            if not fact_id or fact_id in seen or per_body[body] >= body_cap:
                continue
            selected.append(_bounded_policy_document(raw))
            seen.add(fact_id)
            per_body[body] += 1
            if len(selected) >= cap:
                return selected
    return selected


def _bounded_policy_document(value: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "fact_id", "title", "summary", "issuing_body", "document_number",
        "official_category", "publish_time", "event_time", "source_url",
        "formal_document", "financial_transmission_evidence",
    )
    result = {key: value.get(key) for key in keys if key in value}
    if isinstance(result.get("summary"), str):
        result["summary"] = result["summary"][:1_200]
    return result


def _bounded_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    for key, item in list(result.items()):
        if isinstance(item, list):
            result[key] = item[:36]
        elif isinstance(item, Mapping):
            result[key] = dict(list(item.items())[:36])
    return result


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI)


def _aware(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else _parse_time(value)
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("monthly strategy as_of must be timezone-aware")
    return parsed.astimezone(SHANGHAI)


__all__ = ["CONTEXT_VERSION", "build_monthly_strategy_context"]
