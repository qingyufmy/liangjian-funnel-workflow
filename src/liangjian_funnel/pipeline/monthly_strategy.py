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

from .a1_contract import A1_CONTRACT_VERSION, A1_MONTHLY_DECISION_COUNT
from .macro_regime import build_macro_asset_quadrant
from .weekly_strategy import build_weekly_strategy_context


SHANGHAI = ZoneInfo("Asia/Shanghai")
CONTEXT_VERSION = "monthly-strategy-context/1.2.0"
MONTHLY_ROTATION_DECISION_VERSION = "monthly-rotation-decision/1.0.0"
MONTHLY_ROTATION_DECISION_LIMIT = A1_MONTHLY_DECISION_COUNT


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
    rotations = _normalise_rotations(rotations)
    monthly_decisions, monthly_coverage = build_monthly_industry_decisions(
        rotations,
        minimum_appearances=_number_or_default(
            history.get("monthly_min_top10_appearances"),
            2,
        ),
        expected_count=MONTHLY_ROTATION_DECISION_LIMIT,
        default_source_ref=_cycle_source_ref(sector_cycle, history),
    )

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
    macro_asset_quadrant = build_macro_asset_quadrant(snapshot)
    weekly_strategy_context = build_weekly_strategy_context(
        snapshot,
        as_of=cutoff,
        monthly_rotations=rotations,
        policy_documents=selected_documents,
        macro_asset_quadrant=macro_asset_quadrant,
    )
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
        "macro_asset_quadrant": macro_asset_quadrant,
        "weekly_strategy_context": weekly_strategy_context,
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
        "monthly_industry_decisions": monthly_decisions,
        # This alias makes the authority boundary explicit for new packet
        # builders while retaining the historical context key for readers.
        "canonical_monthly_decisions": monthly_decisions,
        "monthly_rotation_coverage": monthly_coverage,
        "a1_contract": {
            "contract_version": A1_CONTRACT_VERSION,
            "monthly_rotation_decision_top_n": MONTHLY_ROTATION_DECISION_LIMIT,
            "model_owns_monthly_decisions": False,
        },
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
            "weekly_overlay_cannot_override_monthly_domain": True,
            "subjective_targets_and_named_recommendations_are_not_facts": True,
        },
    }


def build_monthly_industry_decisions(
    rotations: Sequence[Mapping[str, Any]],
    *,
    minimum_appearances: float = 2,
    expected_count: int = MONTHLY_ROTATION_DECISION_LIMIT,
    default_source_ref: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Create an auditable decision for every observed monthly top industry.

    The sector-cycle builder is intentionally generic and does not know about
    thematic allow-lists.  This layer therefore only uses the frozen ranking
    metrics.  Missing metrics result in ``DEFER`` rather than a fabricated
    score.  ``expected_count`` describes the requested top-N view; when fewer
    ranked industries are available the coverage object records the missing
    ranks explicitly instead of silently presenting a partial top-20.
    """

    limit = max(1, int(expected_count))
    rows: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for rank, raw in enumerate(rotations, start=1):
        if rank > limit or not isinstance(raw, Mapping):
            break
        code = str(raw.get("industry_thscode") or "").strip().upper()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        name = str(raw.get("industry_name") or "").strip() or None
        metrics = {
            key: raw.get(key)
            for key in (
                "return_5d", "return_10d", "return_20d",
                "relative_strength_percentile_20d",
                "top10_appearance_count", "top10_appearance_rate",
                "recent_turnover", "turnover_persistence_ratio",
            )
            if key in raw
        }
        supporting_refs = _source_refs(raw)
        if not supporting_refs and default_source_ref:
            supporting_refs = [default_source_ref]
        missing = [
            key for key in ("return_20d", "relative_strength_percentile_20d")
            if _number(raw.get(key)) is None
        ]
        appearances = _number(raw.get("top10_appearance_count"))
        if appearances is None:
            missing.append("top10_appearance_count")
        if not supporting_refs:
            missing.append("rotation_source_ref")

        reason_codes: list[str] = []
        contradicting_refs: list[str] = []
        structural_status = "INSUFFICIENT"
        timing_state = "UNKNOWN"
        if missing:
            decision = "DEFER"
            reason_codes.append("MONTHLY_ROTATION_DATA_INCOMPLETE")
            if not supporting_refs:
                reason_codes.append("MONTHLY_ROTATION_SOURCE_REF_MISSING")
        else:
            return_20d = _number(raw.get("return_20d"))
            relative = _percentile_0_100(raw.get("relative_strength_percentile_20d"))
            enough_appearances = appearances >= max(1.0, float(minimum_appearances))
            structural_status = "SUPPORTED" if enough_appearances and supporting_refs else "INSUFFICIENT"
            if return_20d is not None and return_20d > 0 and relative is not None and relative >= 60:
                timing_state = "ACCELERATING"
            elif return_20d is not None and return_20d > 0 and relative is not None and relative >= 40:
                timing_state = "PERSISTENT"
            elif return_20d is not None and (return_20d <= 0 or (relative is not None and relative < 40)):
                timing_state = "COOLING"
            else:
                timing_state = "MIXED"

            if structural_status == "SUPPORTED" and timing_state in {"ACCELERATING", "PERSISTENT"}:
                decision = "INCLUDE"
                reason_codes.extend((
                    "MONTHLY_STRUCTURE_SUPPORTED",
                    "MONTHLY_TIMING_POSITIVE",
                ))
            elif structural_status == "SUPPORTED" and timing_state == "COOLING":
                # A cooling 20-day window is a timing observation.  It cannot
                # erase a persistent monthly structural domain by itself.
                decision = "DEFER"
                reason_codes.extend((
                    "MONTHLY_STRUCTURE_SUPPORTED",
                    "MONTHLY_TIMING_COOLING",
                    "MONTHLY_STRUCTURE_RETAINED_WAIT_TIMING",
                ))
                contradicting_refs = list(supporting_refs)
            elif structural_status == "INSUFFICIENT" and timing_state == "COOLING":
                decision = "EXCLUDE"
                reason_codes.extend((
                    "MONTHLY_STRUCTURE_EVIDENCE_INSUFFICIENT",
                    "MONTHLY_TIMING_COOLING",
                ))
                contradicting_refs = list(supporting_refs)
            else:
                decision = "DEFER"
                reason_codes.extend((
                    "MONTHLY_STRUCTURE_EVIDENCE_INSUFFICIENT"
                    if structural_status == "INSUFFICIENT"
                    else "MONTHLY_STRUCTURE_SUPPORTED",
                    "MONTHLY_TIMING_MIXED",
                ))
                if not enough_appearances:
                    missing.append("top10_appearance_persistence")

        rows.append({
            "rank": rank,
            "industry_thscode": code,
            "industry_name": name,
            "decision": decision,
            "base_decision": decision,
            "mapped_theme_ids": [],
            "mapping_status": "UNMAPPED",
            "mapping_source": "SERVER",
            "final_decision": decision,
            "reason_codes": list(dict.fromkeys(reason_codes)),
            "base_reason_codes": list(dict.fromkeys(reason_codes)),
            "supporting_source_refs": supporting_refs,
            "base_source_refs": supporting_refs,
            "contradicting_source_refs": contradicting_refs,
            "data_gaps": list(dict.fromkeys(missing)),
            "metrics": metrics,
            "structural_status": structural_status,
            "timing_state": timing_state,
            "decision_version": MONTHLY_ROTATION_DECISION_VERSION,
        })

    observed = len(rows)
    missing_ranks = list(range(observed + 1, limit + 1))
    top10_rows = [row for row in rows if int(row["rank"]) <= 10]
    top10_missing = [rank for rank in range(1, min(10, limit) + 1) if rank > observed]
    coverage_status = "READY" if observed >= limit else "INCOMPLETE"
    return rows, {
        "contract_version": A1_CONTRACT_VERSION,
        "decision_version": MONTHLY_ROTATION_DECISION_VERSION,
        "requested_top_n": limit,
        "observed_count": observed,
        "missing_ranks": missing_ranks,
        "top10_observed_count": len(top10_rows),
        "top10_missing_ranks": top10_missing,
        "top10_complete": not top10_missing,
        "status": coverage_status,
        "decision_counts": {
            decision: sum(row["decision"] == decision for row in rows)
            for decision in ("INCLUDE", "EXCLUDE", "DEFER")
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


def _normalise_rotations(values: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Keep ranking rows stable while retaining only scalar audit metrics."""

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in values:
        code = str(raw.get("industry_thscode") or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        item = dict(raw)
        item["industry_thscode"] = code
        if item.get("industry_name") is not None:
            item["industry_name"] = str(item.get("industry_name") or "").strip()
        result.append(item)
    return result


def _cycle_source_ref(sector_cycle: Mapping[str, Any], history: Mapping[str, Any]) -> str | None:
    """Build a stable derived-fact reference for THS index-history metrics."""

    source = str(sector_cycle.get("source") or "").strip()
    version = str(history.get("algorithm_version") or sector_cycle.get("algorithm_version") or "").strip()
    as_of = str(sector_cycle.get("as_of") or "").strip()
    if not source or not version or not as_of:
        return None
    return f"derived:{source}:{version}:{as_of}"


def _source_refs(value: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    raw_values: list[Any] = []
    for key in ("source_ref", "source_url", "fact_id"):
        raw_values.append(value.get(key))
    raw_values.extend(value.get("source_refs", ()) if isinstance(value.get("source_refs"), Sequence) and not isinstance(value.get("source_refs"), (str, bytes, bytearray)) else ())
    for raw in raw_values:
        if isinstance(raw, str) and raw.strip():
            refs.append(raw.strip())
    return list(dict.fromkeys(refs))


def _number_or_default(value: Any, default: float) -> float:
    number = _number(value)
    return default if number is None else number


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def _percentile_0_100(value: Any) -> float | None:
    number = _number(value)
    if number is None:
        return None
    return number * 100.0 if 0.0 <= number <= 1.0 else number


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


__all__ = [
    "CONTEXT_VERSION",
    "MONTHLY_ROTATION_DECISION_VERSION",
    "build_monthly_industry_decisions",
    "build_monthly_strategy_context",
]
