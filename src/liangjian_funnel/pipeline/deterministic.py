"""Deterministic full-market research gates used by pipeline V2.

These gates evaluate every upstream symbol locally.  They do not predict stock
prices and they never create executable orders.  LLMs receive only the bounded
``review_symbols`` returned by a gate and may subsequently demote those rows.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .bottleneck import deterministic_bottleneck_context
from .business_exposure import extract_business_exposure_facts
from .feature_store import content_hash


PIPELINE_MODE = "deterministic_v2"
FEATURE_VERSION = "deterministic-features/2.0.0"
_TOKEN = re.compile(r"[^0-9A-Za-z\u3400-\u9fff]+")


@dataclass(frozen=True, slots=True)
class DeterministicGateResult:
    stage: str
    decisions: tuple[dict[str, Any], ...]
    review_symbols: tuple[str, ...]
    monitor_symbols: tuple[str, ...]
    rejected_symbols: tuple[str, ...]
    taxonomy_links: tuple[dict[str, Any], ...] = ()

    @property
    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = defaultdict(int)
        for decision in self.decisions:
            counts[str(decision.get("status") or "UNKNOWN")] += 1
        return {
            "stage": self.stage,
            "pipeline_mode": PIPELINE_MODE,
            "evaluated_count": len(self.decisions),
            "sent_to_llm_count": len(self.review_symbols),
            "monitor_count": len(self.monitor_symbols),
            "rejected_count": len(self.rejected_symbols),
            "status_counts": dict(sorted(counts.items())),
        }


def screen_a1(
    snapshot: Mapping[str, Any],
    discovery: Mapping[str, Any],
    *,
    local_top_n_per_node: int = 15,
    llm_top_n_per_theme: int = 8,
) -> DeterministicGateResult:
    """Evaluate G0 and build broad local coverage plus theme representatives."""

    if local_top_n_per_node < 1 or llm_top_n_per_theme < 1:
        raise ValueError("A1 Top-N values must be positive")

    symbols = _g0_symbols(snapshot)
    candidates = _candidate_map(snapshot)
    industry = _membership_map(snapshot.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
    concept = _membership_map(snapshot.get("THS_CONCEPT_MEMBERSHIP"), taxonomy="CONCEPT")
    themes = _mapping_list(discovery.get("structural_themes"))
    nodes = _mapping_list(discovery.get("industry_chain_graph"))
    theme_by_id = {str(item.get("theme_id") or ""): item for item in themes}
    node_by_id = {str(item.get("node_id") or ""): item for item in nodes}
    links = _taxonomy_links(discovery, nodes, themes, industry, concept)
    links_by_code: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for link in links:
        links_by_code[(str(link["taxonomy"]), str(link["taxonomy_code"]))].append(link)

    fundamentals = snapshot.get("COMPANY_FUNDAMENTALS")
    fundamentals = fundamentals if isinstance(fundamentals, Mapping) else {}
    business = snapshot.get("MAIN_BUSINESS_EVIDENCE")
    business = business if isinstance(business, Mapping) else {}
    structured_exposure: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fact in extract_business_exposure_facts(business):
        structured_exposure[str(fact["symbol"])].append(fact)
    risk_symbols = _hard_risk_symbols(snapshot.get("RISK_EVENTS"))
    tradability = snapshot.get("TRADABILITY_FLAGS")
    tradability = tradability if isinstance(tradability, Mapping) else {}
    weights = snapshot.get("SCORE_WEIGHTS")
    weights = weights if isinstance(weights, Mapping) else {}
    minimums = snapshot.get("A1_MINIMUMS")
    minimums = minimums if isinstance(minimums, Mapping) else {}
    minimum_score = _number(minimums.get("minimum_score")) or _number(snapshot.get("MIN_STRUCTURAL_SCORE")) or 65.0
    minimum_quality = _number(minimums.get("minimum_data_quality")) or 75.0
    targets = snapshot.get("A1_POOL_TARGETS")
    targets = targets if isinstance(targets, Mapping) else {}
    active_target = targets.get("active_research_target")
    if isinstance(active_target, Sequence) and not isinstance(active_target, (str, bytes, bytearray)):
        target_values = [int(value) for value in active_target[:2] if _number(value) is not None]
    else:
        target_values = []
    active_target_min = max(1, target_values[0] if target_values else 100)
    active_target_max = max(active_target_min, target_values[1] if len(target_values) > 1 else 250)

    decisions: list[dict[str, Any]] = []
    provisional: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_hashes = _source_hashes(snapshot)
    for symbol in symbols:
        memberships = (*industry.get(symbol, ()), *concept.get(symbol, ()))
        matched = _matched_links(memberships, links_by_code)
        candidate = candidates.get(symbol, {})
        amount = max(0.0, _number(candidate.get("amount")) or _number(candidate.get("turnover")) or 0.0)
        fundamental = fundamentals.get(symbol)
        fundamental = fundamental if isinstance(fundamental, Mapping) else {}
        evidence = business.get(symbol)
        evidence = evidence if isinstance(evidence, Mapping) else {}
        exposure_facts = structured_exposure.get(symbol, [])
        coverage = fundamental.get("dataset_coverage")
        coverage = coverage if isinstance(coverage, Mapping) else {}
        statements = fundamental.get("statements")
        statements = statements if isinstance(statements, Mapping) else {}
        core_reports = coverage.get("core_reports_complete") is True or all(
            isinstance(statements.get(dataset), Sequence)
            and not isinstance(statements.get(dataset), (str, bytes, bytearray))
            and bool(statements.get(dataset))
            for dataset in ("INCOME", "BALANCE", "CASH_FLOW")
        )
        indicators = fundamental.get("indicators")
        indicators_available = coverage.get("indicators_available") is True or (
            isinstance(indicators, Sequence)
            and not isinstance(indicators, (str, bytes, bytearray))
            and bool(indicators)
        )
        raw_evidence_available = evidence.get("available") is True and bool(_mapping_list(evidence.get("evidence")))
        structured_exposure_available = bool(exposure_facts)
        data_quality = 25.0
        data_quality += 35.0 if core_reports else 0.0
        data_quality += 15.0 if indicators_available else 0.0
        data_quality += 10.0 if raw_evidence_available else 0.0
        data_quality += 15.0 if structured_exposure_available else 0.0
        financial_quality, financial_details = _financial_quality(fundamental)
        maximum_exposure = max(
            (_number(item.get("revenue_exposure_pct")) or 0.0 for item in exposure_facts),
            default=0.0,
        )
        business_score = (
            min(100.0, 55.0 + maximum_exposure * 0.6)
            if structured_exposure_available
            else 62.0 if raw_evidence_available else 35.0
        )
        structural_score = 95.0 if matched else 0.0
        liquidity_score = _liquidity_score(amount)
        score_breakdown = _a1_breakdown(
            weights,
            structural=structural_score,
            business=business_score,
            financial=financial_quality,
            liquidity=liquidity_score,
            evidence=data_quality,
        )
        score = _weighted_score(score_breakdown, weights)
        flags = tradability.get(symbol)
        flags = flags if isinstance(flags, Mapping) else {}
        reason_codes: list[str] = []
        hard_reject = False
        if flags.get("available") is True and flags.get("tradable") is False:
            # Beijing securities are research-only, not an A1 hard reject.
            exclusions = {str(item) for item in flags.get("exclusion_reasons", ())}
            if exclusions - {"BEIJING_RESEARCH_ONLY", "EXCHANGE_NOT_SUPPORTED_FOR_SIMULATION"}:
                hard_reject = True
                reason_codes.append("A1_TRADABILITY_HARD_REJECT")
        if symbol in risk_symbols:
            hard_reject = True
            reason_codes.append("A1_RISK_EVENT_PRESENT")
        if not matched:
            status = "OUTSIDE_THEME"
            reason_codes.append("A1_OUTSIDE_DISCOVERED_THEME")
        elif hard_reject:
            status = "HARD_REJECT"
        elif not raw_evidence_available:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_MAIN_BUSINESS_EVIDENCE_MISSING")
        elif not core_reports or not indicators_available:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_FUNDAMENTAL_DATA_INCOMPLETE")
        elif data_quality < minimum_quality:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_DATA_QUALITY_BELOW_MINIMUM")
        elif score < minimum_score:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_LOCAL_SCORE_BELOW_MINIMUM")
        else:
            status = "LOCAL_CANDIDATE"
        if raw_evidence_available and not structured_exposure_available:
            reason_codes.append("A1_BUSINESS_EXPOSURE_UNSTRUCTURED")

        primary_link = matched[0] if matched else {}
        decision = {
            "symbol": symbol,
            "name": str(candidate.get("name") or candidate.get("security_name") or "") or None,
            "stage": "A1_LOCAL_SCREEN",
            "status": status,
            "score": round(score, 4),
            "data_quality_score": round(data_quality, 4),
            "financial_quality_score": round(financial_quality, 4),
            "liquidity_score": round(liquidity_score, 4),
            "theme_id": primary_link.get("theme_id"),
            "node_id": primary_link.get("node_id"),
            "taxonomy_matches": matched,
            "theme_source_refs": list(theme_by_id.get(str(primary_link.get("theme_id") or ""), {}).get("source_refs") or ()),
            "node_source_refs": list(node_by_id.get(str(primary_link.get("node_id") or ""), {}).get("source_refs") or ()),
            "score_breakdown": score_breakdown,
            "financial_features": financial_details,
            "business_exposure_facts": exposure_facts,
            "maximum_revenue_exposure_pct": maximum_exposure if exposure_facts else None,
            "amount": amount,
            "reason_codes": list(dict.fromkeys(reason_codes)),
            "sent_to_llm": False,
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        }
        decisions.append(decision)
        if status == "LOCAL_CANDIDATE":
            provisional[str(decision.get("node_id") or "UNMAPPED")].append(decision)

    local_eligible: list[dict[str, Any]] = []
    for node_id, node_decisions in provisional.items():
        node_decisions.sort(key=lambda item: (-float(item["score"]), -float(item["amount"]), str(item["symbol"])))
        for rank, item in enumerate(node_decisions, start=1):
            item["node_rank"] = rank
            if rank > local_top_n_per_node:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A1_OUTSIDE_LOCAL_TOP_N")
            elif item.get("business_exposure_facts"):
                item["status"] = "LOCAL_ACTIVE_CANDIDATE"
                local_eligible.append(item)
            else:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A1_REQUIRES_LLM_EXPOSURE_REVIEW")
                local_eligible.append(item)

    # The discovery model already owns the monthly policy/cycle thesis.  The
    # company review is a representative audit per theme, not an approval
    # quota for the entire deterministic research layer.
    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in local_eligible:
        by_theme[str(item.get("theme_id") or "UNMAPPED")].append(item)
    for values in by_theme.values():
        values.sort(key=lambda item: (
            0 if "A1_REQUIRES_LLM_EXPOSURE_REVIEW" in item.get("reason_codes", ()) else 1,
            int(item.get("node_rank") or 10**9),
            -float(item["score"]),
            str(item["symbol"]),
        ))
        for item in values[:llm_top_n_per_theme]:
            item["status"] = "REVIEW_CANDIDATE"
            item["sent_to_llm"] = True
            item["reason_codes"] = [
                code for code in item.get("reason_codes", ())
                if code != "A1_REQUIRES_LLM_EXPOSURE_REVIEW"
            ]

    # Coverage is an acceptance contract.  When enough structured evidence is
    # available, expand beyond the per-node diversity seed until the minimum
    # research layer is met.  The maximum remains a research-capacity target,
    # never a G0 scan limit; every non-selected symbol retains a decision row.
    local_active_count = sum(item.get("status") == "LOCAL_ACTIVE_CANDIDATE" for item in decisions)
    if local_active_count < active_target_min:
        expandable = sorted(
            (
                item for item in decisions
                if item.get("status") == "LOCAL_MONITOR"
                and item.get("business_exposure_facts")
                and item.get("node_id")
                and "A1_LOCAL_SCORE_BELOW_MINIMUM" not in item.get("reason_codes", ())
            ),
            key=lambda item: (int(item.get("node_rank") or 10**9), -float(item["score"]), -float(item["amount"]), str(item["symbol"])),
        )
        for item in expandable:
            if local_active_count >= min(active_target_min, active_target_max):
                break
            item["status"] = "LOCAL_ACTIVE_CANDIDATE"
            item["reason_codes"] = [
                code for code in item.get("reason_codes", ())
                if code not in {"A1_OUTSIDE_LOCAL_TOP_N", "A1_REQUIRES_LLM_EXPOSURE_REVIEW"}
            ]
            item["reason_codes"].append("A1_ADAPTIVE_COVERAGE_EXPANSION")
            local_active_count += 1

    decisions.sort(key=lambda item: str(item["symbol"]))
    review = tuple(
        str(item["symbol"])
        for item in sorted(
            (item for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
            key=lambda item: (str(item.get("node_id") or ""), int(item.get("node_rank") or 0), str(item["symbol"])),
        )
    )
    monitor = tuple(str(item["symbol"]) for item in decisions if item["status"] in {"LOCAL_MONITOR", "OUTSIDE_THEME"})
    rejected = tuple(str(item["symbol"]) for item in decisions if item["status"] == "HARD_REJECT")
    return DeterministicGateResult(
        stage="A1_LOCAL_SCREEN",
        decisions=tuple(decisions),
        review_symbols=review,
        monitor_symbols=monitor,
        rejected_symbols=rejected,
        taxonomy_links=tuple(links),
    )


def screen_a2(
    snapshot: Mapping[str, Any],
    a1_output: Mapping[str, Any],
    *,
    minimum_identifiability_score: float = 60.0,
    llm_top_n_per_theme: int = 5,
) -> DeterministicGateResult:
    """Rank A1 ACTIVE rows by deterministic liquidity and relative strength."""

    rows = _mapping_list(a1_output.get("active_research_pool"))
    candidates = _candidate_map(snapshot)
    factors = snapshot.get("FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else {}
    recent_bars = snapshot.get("RECENT_DAILY_BARS")
    recent_bars = recent_bars if isinstance(recent_bars, Mapping) else {}
    industry_membership = _membership_map(snapshot.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
    cycle_metrics = snapshot.get("SECTOR_CYCLE_SNAPSHOT")
    cycle_metrics = cycle_metrics if isinstance(cycle_metrics, Mapping) else {}
    history_metrics = cycle_metrics.get("history_metrics")
    history_metrics = history_metrics if isinstance(history_metrics, Mapping) else {}
    raw_rotations = history_metrics.get("monthly_rotation_candidates")
    if not isinstance(raw_rotations, Sequence) or isinstance(raw_rotations, (str, bytes, bytearray)):
        raw_rotations = history_metrics.get("persistent_mainline_candidates")
    rotation_by_code = {
        str(item.get("industry_thscode") or ""): item
        for item in raw_rotations or ()
        if isinstance(item, Mapping) and str(item.get("industry_thscode") or "")
    }
    bar_returns = {
        symbol: value
        for item in rows
        if (symbol := _symbol(item.get("symbol")))
        if (value := _daily_return(recent_bars.get(symbol))) is not None
    }
    return_distribution = sorted(bar_returns.values())
    attention = _attention_symbols(snapshot.get("MARKET_ATTENTION_SNAPSHOT"))
    dragon = _event_symbols(snapshot.get("DRAGON_TIGER_SNAPSHOT"))
    source_hashes = _source_hashes(snapshot)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    decisions: list[dict[str, Any]] = []
    for item in rows:
        symbol = _symbol(item.get("symbol"))
        if not symbol:
            continue
        theme_id = str(item.get("primary_theme") or item.get("theme_id") or "UNMAPPED")
        candidate = candidates.get(symbol, {})
        amount = max(0.0, _number(candidate.get("amount")) or 0.0)
        factor = factors.get(symbol)
        factor = factor if isinstance(factor, Mapping) else {}
        explicit_relative = _relative_strength_score(factor, default=None)
        relative = explicit_relative if explicit_relative is not None else _percentile_score(
            bar_returns.get(symbol), return_distribution
        )
        liquidity = _liquidity_score(amount)
        attention_score = 100.0 if symbol in attention else 50.0
        dragon_score = 100.0 if symbol in dragon else 45.0
        business = _number(item.get("structural_score")) or 50.0
        rotations = [
            rotation_by_code.get(str(membership.get("taxonomy_code") or ""))
            for membership in industry_membership.get(symbol, ())
        ]
        rotations = [value for value in rotations if isinstance(value, Mapping)]
        cycle_score = max((_cycle_rotation_score(value) for value in rotations), default=35.0)
        bottleneck_context = deterministic_bottleneck_context(
            item,
            demand_score_0_100=cycle_score,
            timing_score_0_100=max(relative, cycle_score),
        )
        bottleneck_readiness = _number(bottleneck_context.get("evidence_readiness_score")) or 0.0
        score = (
            0.20 * relative
            + 0.20 * liquidity
            + 0.20 * cycle_score
            + 0.08 * attention_score
            + 0.05 * dragon_score
            + 0.12 * business
            + 0.15 * bottleneck_readiness
        )
        reasons: list[str] = []
        status = "LOCAL_FOCUS_CANDIDATE" if score >= minimum_identifiability_score else "LOCAL_MONITOR"
        if status == "LOCAL_MONITOR":
            reasons.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
        decision = {
            "symbol": symbol,
            "name": item.get("company_name") or candidate.get("name"),
            "stage": "A2_LOCAL_ROLE",
            "status": status,
            "score": round(score, 4),
            "theme_id": theme_id,
            "node_id": item.get("industry_chain_node"),
            "role": _role(score, liquidity, relative),
            "role_breakdown": {
                "relative_strength": round(relative, 4),
                "liquidity_capacity": round(liquidity, 4),
                "market_attention": round(attention_score, 4),
                "dragon_tiger": round(dragon_score, 4),
                "monthly_cycle_rotation": round(cycle_score, 4),
                "business_purity": round(business, 4),
                "bottleneck_evidence_readiness": round(bottleneck_readiness, 4),
                "relative_strength_source": "FACTOR_SNAPSHOT" if explicit_relative is not None else "RECENT_DAILY_BARS",
            },
            "bottleneck_context": bottleneck_context,
            "reason_codes": reasons,
            "sent_to_llm": False,
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        }
        decisions.append(decision)
        if status == "LOCAL_FOCUS_CANDIDATE":
            grouped[theme_id].append(decision)
    for theme_id, values in grouped.items():
        values.sort(key=lambda item: (-float(item["score"]), str(item["symbol"])))
        for rank, item in enumerate(values, start=1):
            item["node_rank"] = rank
            if rank <= llm_top_n_per_theme:
                item["status"] = "REVIEW_CANDIDATE"
                item["sent_to_llm"] = True
            else:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A2_NOT_SENT_TO_LLM")
    decisions.sort(key=lambda item: str(item["symbol"]))
    return DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=tuple(decisions),
        review_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
        monitor_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "LOCAL_MONITOR"),
        rejected_symbols=(),
    )


def screen_a3(snapshot: Mapping[str, Any], a2_output: Mapping[str, Any]) -> DeterministicGateResult:
    """Fail closed on missing server-computed technical contracts before A3."""

    rows = _mapping_list(a2_output.get("focus_pool"))
    factors = snapshot.get("FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else {}
    levels = snapshot.get("PRICE_LEVELS")
    levels = levels if isinstance(levels, Mapping) else {}
    tradability = snapshot.get("TRADABILITY_FLAGS")
    tradability = tradability if isinstance(tradability, Mapping) else {}
    source_hashes = _source_hashes(snapshot)
    decisions: list[dict[str, Any]] = []
    for item in rows:
        symbol = _symbol(item.get("symbol"))
        if not symbol:
            continue
        factor = factors.get(symbol)
        factor = factor if isinstance(factor, Mapping) else {}
        price_level = levels.get(symbol)
        price_level = price_level if isinstance(price_level, Mapping) else {}
        flags = tradability.get(symbol)
        flags = flags if isinstance(flags, Mapping) else {}
        reasons: list[str] = []
        if factor.get("ready") is not True:
            reasons.append("A3_TECHNICAL_FACTORS_NOT_READY")
        if price_level.get("available") is False or not price_level:
            reasons.append("A3_PRICE_LEVELS_NOT_READY")
        if flags.get("tradable") is not True:
            reasons.append("A3_SYMBOL_NOT_TRADABLE")
        score = _technical_readiness_score(factor, price_level, flags)
        status = "REVIEW_CANDIDATE" if not reasons else "HARD_REJECT"
        decisions.append({
            "symbol": symbol,
            "name": item.get("company_name") or item.get("name"),
            "stage": "A3_LOCAL_TECHNICAL",
            "status": status,
            "score": round(score, 4),
            "theme_id": item.get("theme_id"),
            "node_id": item.get("industry_chain_node"),
            "reason_codes": reasons,
            "sent_to_llm": status == "REVIEW_CANDIDATE",
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        })
    decisions.sort(key=lambda item: str(item["symbol"]))
    return DeterministicGateResult(
        stage="A3_LOCAL_TECHNICAL",
        decisions=tuple(decisions),
        review_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
        monitor_symbols=(),
        rejected_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "HARD_REJECT"),
    )


def local_monitor_items(result: DeterministicGateResult) -> list[dict[str, Any]]:
    return [
        {
            "symbol": item["symbol"],
            "company_name": item.get("name"),
            "primary_theme": item.get("theme_id"),
            "industry_chain_node": item.get("node_id"),
            "structural_score": item.get("score"),
            "data_quality_score": item.get("data_quality_score"),
            "evidence_confidence": 0.0,
            "status": "MONITOR",
            "reason_codes": item.get("reason_codes", []),
            "local_decision": True,
            "sent_to_llm": False,
            "source_refs": [],
        }
        for item in result.decisions
        if item.get("status") in {"LOCAL_MONITOR", "OUTSIDE_THEME"}
    ]


def local_active_items(result: DeterministicGateResult) -> list[dict[str, Any]]:
    """Project locally verified A1 rows into the canonical research schema."""

    projected: list[dict[str, Any]] = []
    for item in result.decisions:
        if item.get("status") != "LOCAL_ACTIVE_CANDIDATE":
            continue
        facts = [fact for fact in item.get("business_exposure_facts", ()) if isinstance(fact, Mapping)]
        if not facts:
            continue
        exposure = max(facts, key=lambda fact: float(fact.get("revenue_exposure_pct") or 0.0))
        source_ref = str(exposure.get("evidence_ref") or "")
        source_refs = list(dict.fromkeys([
            *[str(value) for value in item.get("theme_source_refs", ()) if str(value)],
            *[str(value) for value in item.get("node_source_refs", ()) if str(value)],
            source_ref,
        ]))
        projected.append({
            "symbol": item["symbol"],
            "candidate_id": f"a1-local:{item['symbol']}",
            "company_name": item.get("name"),
            "primary_theme": item.get("theme_id"),
            "secondary_themes": [],
            "industry_chain_node": item.get("node_id"),
            "core_thesis": "MONTHLY_THEME_AND_DISCLOSED_BUSINESS_MAPPING_CONFIRMED",
            "bear_case": "MONTHLY_THEME_WEAKENS_OR_DISCLOSED_BUSINESS_TRANSMISSION_FAILS",
            "structural_score": item.get("score"),
            "data_quality_score": item.get("data_quality_score"),
            "evidence_confidence": min(
                float(exposure.get("confidence") or 0.0),
                float(item.get("data_quality_score") or 0.0) / 100.0,
            ),
            "status": "ACTIVE",
            "source_refs": source_refs,
            "business_exposure": {
                "business_name": exposure.get("business_name"),
                "revenue_exposure_pct": exposure.get("revenue_exposure_pct"),
                "source_ref": source_ref,
                "page_number": exposure.get("page_number"),
                "report_period": exposure.get("report_period"),
                "extraction_method": exposure.get("extraction_method"),
            },
            "score_breakdown": dict(item.get("score_breakdown") or {}),
            "reason_codes": ["A1_DETERMINISTIC_MONTHLY_RESEARCH_ELIGIBLE"],
            "local_decision": True,
            "sent_to_llm": False,
        })
    return projected


def local_rejected_items(result: DeterministicGateResult) -> list[dict[str, Any]]:
    return [
        {
            "symbol": item["symbol"],
            "reason_codes": item.get("reason_codes", []),
            "evidence": "DETERMINISTIC_LOCAL_GATE",
            "local_decision": True,
        }
        for item in result.decisions
        if item.get("status") == "HARD_REJECT"
    ]


def _g0_symbols(snapshot: Mapping[str, Any]) -> tuple[str, ...]:
    values = snapshot.get("g0_symbols", snapshot.get("g0", ()))
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return ()
    return tuple(sorted({_symbol(item) for item in values if _symbol(item)}))


def _candidate_map(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    values = snapshot.get("g0_candidates", snapshot.get("universe_candidates", ()))
    result: dict[str, dict[str, Any]] = {}
    for item in values if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)) else ():
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol(item.get("symbol"))
        if symbol:
            result[symbol] = dict(item)
    return result


def _membership_map(value: Any, *, taxonomy: str) -> dict[str, tuple[dict[str, Any], ...]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _fact_records(value):
        symbol = _symbol(row.get("thscode") or row.get("symbol"))
        memberships = row.get("memberships")
        if not symbol or not isinstance(memberships, Sequence):
            continue
        for membership in memberships:
            if not isinstance(membership, Mapping):
                continue
            code = str(
                membership.get("taxonomy_code")
                or membership.get("industry_thscode")
                or membership.get("concept_thscode")
                or ""
            ).strip().upper()
            name = str(
                membership.get("taxonomy_name")
                or membership.get("industry_name")
                or membership.get("concept_name")
                or ""
            ).strip()
            if code:
                result[symbol].append({"taxonomy": taxonomy, "taxonomy_code": code, "taxonomy_name": name})
    return {symbol: tuple(items) for symbol, items in result.items()}


def _taxonomy_links(
    discovery: Mapping[str, Any],
    nodes: Sequence[Mapping[str, Any]],
    themes: Sequence[Mapping[str, Any]],
    industry: Mapping[str, Sequence[Mapping[str, Any]]],
    concept: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    explicit = _mapping_list(discovery.get("taxonomy_links"))
    node_ids = {str(node.get("node_id") or "") for node in nodes}
    theme_by_id = {str(item.get("theme_id") or ""): item for item in themes}
    universe = {
        (str(item["taxonomy"]), str(item["taxonomy_code"])): str(item.get("taxonomy_name") or "")
        for values in (*industry.values(), *concept.values())
        for item in values
    }
    links: list[dict[str, Any]] = []
    for raw in explicit:
        node_id = str(raw.get("node_id") or "")
        if node_id not in node_ids:
            continue
        for taxonomy, keys in (
            ("INDUSTRY", ("industry_thscodes", "industry_codes")),
            ("CONCEPT", ("concept_thscodes", "concept_codes")),
        ):
            values: Any = ()
            for key in keys:
                if isinstance(raw.get(key), Sequence) and not isinstance(raw.get(key), (str, bytes, bytearray)):
                    values = raw[key]
                    break
            for code_value in values:
                code = str(code_value).strip().upper()
                name = universe.get((taxonomy, code))
                if name is None:
                    continue
                links.append({
                    "node_id": node_id,
                    "theme_id": _first_theme_id(nodes, node_id),
                    "taxonomy": taxonomy,
                    "taxonomy_code": code,
                    "taxonomy_name": name,
                    "match_method": "MODEL_SELECTED_VALIDATED_CODE",
                    "confidence": 1.0,
                })
    if links:
        return _dedupe_links(links)

    # Compatibility fallback for old discovery outputs: match exact taxonomy
    # names contained in node/theme text.  The match remains local and
    # auditable; if nothing matches the full universe is retained as monitor.
    for node in nodes:
        node_id = str(node.get("node_id") or "")
        theme_ids = [str(item) for item in node.get("theme_ids", ()) if str(item)]
        texts = [node_id, str(node.get("display_name") or ""), str(node.get("demand_driver") or "")]
        texts.extend(str(theme_by_id.get(theme_id, {}).get("display_name") or theme_id) for theme_id in theme_ids)
        normalized = _normalize(" ".join(texts))
        for (taxonomy, code), name in universe.items():
            normalized_name = _normalize(name)
            if len(normalized_name) >= 2 and normalized_name in normalized:
                links.append({
                    "node_id": node_id,
                    "theme_id": theme_ids[0] if theme_ids else None,
                    "taxonomy": taxonomy,
                    "taxonomy_code": code,
                    "taxonomy_name": name,
                    "match_method": "EXACT_NAME_IN_DISCOVERY_TEXT",
                    "confidence": 0.8,
                })
    return _dedupe_links(links)


def _matched_links(
    memberships: Sequence[Mapping[str, Any]],
    links_by_code: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for membership in memberships:
        key = (str(membership.get("taxonomy") or ""), str(membership.get("taxonomy_code") or ""))
        matched.extend(dict(item) for item in links_by_code.get(key, ()))
    matched.sort(key=lambda item: (-float(item.get("confidence") or 0.0), str(item.get("node_id") or "")))
    return _dedupe_links(matched)


def _financial_quality(value: Mapping[str, Any]) -> tuple[float, dict[str, float | None]]:
    indicators = value.get("indicators")
    indicator_map: dict[str, float] = {}
    if isinstance(indicators, Sequence):
        for row in indicators:
            if not isinstance(row, Mapping):
                continue
            key = _normalize(row.get("index_id") or row.get("name"))
            number = _number(row.get("value"))
            if key and number is not None:
                indicator_map[key] = number

    def pick(*aliases: str) -> float | None:
        for alias in aliases:
            key = _normalize(alias)
            if key in indicator_map:
                return indicator_map[key]
        for alias in aliases:
            key = _normalize(alias)
            for actual, number in indicator_map.items():
                if key and key in actual:
                    return number
        return None

    features = {
        "roe": pick("roe", "净资产收益率"),
        "gross_margin": pick("sale_gross_margin", "gross_margin", "销售毛利率"),
        "net_margin": pick("net_profit_margin", "销售净利率"),
        "revenue_growth": pick("operating_income_yoy", "revenue_yoy", "营业收入同比增长率"),
        "profit_growth": pick("net_profit_yoy", "归母净利润同比增长率"),
        "cashflow_quality": pick("cashflow_net_income_ratio", "经营现金流净利润比"),
        "debt_ratio": pick("debt_to_assets", "资产负债率"),
    }
    available = [value for value in features.values() if value is not None]
    if not available:
        coverage = value.get("dataset_coverage")
        coverage = coverage if isinstance(coverage, Mapping) else {}
        fallback = 60.0 if coverage.get("core_reports_complete") is True else 35.0
        return fallback, features
    scores: list[float] = []
    if features["roe"] is not None:
        scores.append(_scale(features["roe"], 0, 20))
    if features["gross_margin"] is not None:
        scores.append(_scale(features["gross_margin"], 5, 50))
    if features["net_margin"] is not None:
        scores.append(_scale(features["net_margin"], 0, 20))
    for key in ("revenue_growth", "profit_growth"):
        if features[key] is not None:
            scores.append(_scale(features[key], -20, 30))
    if features["cashflow_quality"] is not None:
        scores.append(_scale(features["cashflow_quality"], 0, 1.2))
    if features["debt_ratio"] is not None:
        scores.append(100.0 - _scale(features["debt_ratio"], 20, 80))
    return sum(scores) / len(scores), features


def _a1_breakdown(
    weights: Mapping[str, Any],
    *,
    structural: float,
    business: float,
    financial: float,
    liquidity: float,
    evidence: float,
) -> dict[str, float]:
    if not weights:
        weights = {
            "structural_theme": 0.20,
            "business_mapping": 0.20,
            "barrier_and_bottleneck": 0.15,
            "financial_quality": 0.20,
            "cash_flow_quality": 0.10,
            "evidence_quality": 0.15,
        }
    result: dict[str, float] = {}
    for key in weights:
        normalized = _normalize(key)
        if "structural" in normalized or "theme" in normalized:
            value = structural
        elif "business" in normalized or "mapping" in normalized:
            value = business
        elif "financial" in normalized or "cashflow" in normalized or "profit" in normalized:
            value = financial
        elif "liquidity" in normalized or "capacity" in normalized:
            value = liquidity
        elif "evidence" in normalized or "dataquality" in normalized:
            value = evidence
        elif "barrier" in normalized or "bottleneck" in normalized:
            value = (structural + evidence) / 2.0
        else:
            value = (financial + evidence) / 2.0
        result[str(key)] = round(max(0.0, min(100.0, value)), 4)
    return result


def _weighted_score(breakdown: Mapping[str, float], weights: Mapping[str, Any]) -> float:
    parsed = {key: _number(weights.get(key)) for key in breakdown}
    total_weight = sum(value for value in parsed.values() if value is not None and value > 0)
    if total_weight <= 0:
        return sum(breakdown.values()) / max(1, len(breakdown))
    return sum(breakdown[key] * float(value or 0.0) for key, value in parsed.items()) / total_weight


def _relative_strength_score(factor: Mapping[str, Any], *, default: float | None = 45.0) -> float | None:
    summary = factor.get("technical_summary")
    summary = summary if isinstance(summary, Mapping) else factor
    for key in ("relative_strength_score", "relative_strength", "rs_score"):
        value = _number(summary.get(key))
        if value is not None:
            return max(0.0, min(100.0, value))
    frames = factor.get("timeframes")
    if isinstance(frames, Mapping):
        daily = frames.get("daily")
        if isinstance(daily, Mapping):
            alignment = str(daily.get("ma_alignment") or "")
            return {"BULL_STACK": 90.0, "BULL_PARTIAL": 72.0, "ENTANGLED": 50.0, "BEAR_PARTIAL": 30.0, "BEAR_STACK": 10.0}.get(alignment, 45.0)
    return default


def _daily_return(value: Any) -> float | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    closes = [
        number
        for item in value
        if isinstance(item, Mapping)
        if (number := _number(item.get("close"))) is not None and number > 0
    ]
    if len(closes) < 2:
        return None
    return closes[-1] / closes[0] - 1.0


def _percentile_score(value: float | None, distribution: Sequence[float]) -> float:
    if value is None or not distribution:
        return 45.0
    below = sum(item < value for item in distribution)
    equal = sum(item == value for item in distribution)
    percentile = (below + 0.5 * equal) / len(distribution)
    return max(0.0, min(100.0, percentile * 100.0))


def _technical_readiness_score(factor: Mapping[str, Any], levels: Mapping[str, Any], flags: Mapping[str, Any]) -> float:
    score = 0.0
    score += 50.0 if factor.get("ready") is True else 0.0
    score += 30.0 if levels and levels.get("available") is not False else 0.0
    score += 20.0 if flags.get("tradable") is True else 0.0
    return score


def _liquidity_score(amount: float) -> float:
    if amount <= 0:
        return 0.0
    # 50m maps near 50 and 5bn near 100; clipping prevents extreme turnover
    # from dominating fundamental quality.
    return max(0.0, min(100.0, 50.0 + 25.0 * math.log10(max(amount, 50_000_000.0) / 50_000_000.0)))


def _role(score: float, liquidity: float, relative: float) -> str:
    if score >= 85 and relative >= 80:
        return "LEADER"
    if score >= 72 and liquidity >= 70:
        return "CORE_ARMY"
    if score >= 65:
        return "TREND_CORE"
    return "LOW_IDENTITY"


def _cycle_rotation_score(value: Mapping[str, Any]) -> float:
    explicit = _number(value.get("relative_strength_percentile_20d"))
    if explicit is not None:
        score = explicit * 100.0 if 0.0 <= explicit <= 1.0 else explicit
        return max(0.0, min(100.0, score))
    appearances = _number(value.get("top10_appearance_count")) or _number(value.get("top3_appearance_count")) or 0.0
    lookback_return = (
        _number(value.get("return_20d"))
        or _number(value.get("lookback_return"))
        or 0.0
    )
    return max(0.0, min(100.0, 45.0 + appearances * 6.0 + lookback_return * 200.0))


def _source_hashes(snapshot: Mapping[str, Any]) -> dict[str, str]:
    manifest = snapshot.get("snapshot_manifest")
    manifest = manifest if isinstance(manifest, Mapping) else {}
    checksums = manifest.get("source_checksums")
    result = dict(checksums) if isinstance(checksums, Mapping) else {}
    result["config_hash"] = str(snapshot.get("config_hash") or "")
    result["snapshot_hash"] = content_hash({"g0": snapshot.get("g0_symbols"), "as_of": manifest.get("as_of")})
    return {str(key): str(value) for key, value in result.items() if str(value)}


def _event_symbols(value: Any) -> set[str]:
    result: set[str] = set()
    for row in _fact_records(value):
        symbol = _symbol(row.get("symbol") or row.get("thscode"))
        if symbol:
            result.add(symbol)
    return result


def _hard_risk_symbols(value: Any) -> set[str]:
    result: set[str] = set()
    hard_tokens = (
        "退市风险",
        "财务造假",
        "否定意见",
        "无法表示意见",
        "长期停牌",
        "DELIST",
        "FRAUD",
        "ADVERSE_AUDIT",
        "DISCLAIMER_OF_OPINION",
    )
    for row in _fact_records(value):
        severity = str(row.get("severity") or row.get("risk_level") or "").upper()
        text = " ".join(
            str(row.get(key) or "")
            for key in ("event_type", "reason_code", "title", "announcement_title", "summary")
        ).upper()
        if severity not in {"HIGH", "CRITICAL"} and not any(token.upper() in text for token in hard_tokens):
            continue
        symbol = _symbol(row.get("symbol") or row.get("thscode"))
        if symbol:
            result.add(symbol)
    return result


def _attention_symbols(value: Any) -> set[str]:
    return _event_symbols(value)


def _fact_records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping) or value.get("available") is False:
        return []
    records = value.get("records")
    if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
        return [item for item in records if isinstance(item, Mapping)]
    payload = value.get("payload")
    if isinstance(payload, Mapping):
        return _fact_records(payload)
    return []


def _mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _dedupe_links(values: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in values:
        key = (
            str(raw.get("node_id") or ""),
            str(raw.get("taxonomy") or ""),
            str(raw.get("taxonomy_code") or ""),
        )
        if all(key):
            unique[key] = dict(raw)
    return [unique[key] for key in sorted(unique)]


def _first_theme_id(nodes: Sequence[Mapping[str, Any]], node_id: str) -> str | None:
    for node in nodes:
        if str(node.get("node_id") or "") != node_id:
            continue
        values = node.get("theme_ids")
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            return next((str(value) for value in values if str(value)), None)
    return None


def _top_n_tie(values: Sequence[Mapping[str, Any]], limit: int) -> bool:
    if limit < 1 or len(values) <= limit:
        return False
    return abs(float(values[limit - 1].get("score") or 0.0) - float(values[limit].get("score") or 0.0)) < 1e-9


def _normalize(value: Any) -> str:
    return _TOKEN.sub("", str(value or "")).casefold()


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _scale(value: float, low: float, high: float) -> float:
    if high <= low:
        return 50.0
    return max(0.0, min(100.0, (value - low) * 100.0 / (high - low)))


def _symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    prefixes = {"SHSE.": ".SH", "SZSE.": ".SZ", "BJSE.": ".BJ"}
    for prefix, suffix in prefixes.items():
        if text.startswith(prefix):
            return text[len(prefix):] + suffix
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", text):
        return text
    return ""


__all__ = [
    "FEATURE_VERSION",
    "PIPELINE_MODE",
    "DeterministicGateResult",
    "local_active_items",
    "local_monitor_items",
    "local_rejected_items",
    "screen_a1",
    "screen_a2",
    "screen_a3",
]
