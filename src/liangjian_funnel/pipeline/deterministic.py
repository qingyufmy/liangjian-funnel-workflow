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

from .bottleneck import (
    MARKET_CORE_ROUTE,
    SUPPLY_CHAIN_ALPHA_ROUTE,
    canonicalize_model_scorecard,
    deterministic_bottleneck_context,
)
from .business_exposure import extract_business_exposure_facts
from .feature_store import content_hash


PIPELINE_MODE = "deterministic_v2"
FEATURE_VERSION = "deterministic-features/2.1.0"
_A1_DEFAULT_WEIGHTS: dict[str, float] = {
    "structural_theme": 0.20,
    "business_mapping": 0.20,
    "barrier_and_bottleneck": 0.15,
    "financial_quality": 0.20,
    "catalyst_confirmation": 0.15,
    "valuation_expectation_gap": 0.10,
}
_TOKEN = re.compile(r"[^0-9A-Za-z\u3400-\u9fff]+")
A2_THEME_FACTORS: tuple[str, ...] = (
    "breadth",
    "turnover_share",
    "capital_flow",
    "leader_structure",
    "tier_structure",
    "profit_effect",
    "catalyst_freshness",
    "index_chain_resonance",
    "agent_1_quality",
)
A2_FACTOR_COVERAGE_MINIMUM = 0.65


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
    weights = _resolve_a1_weights(weights)
    minimums = snapshot.get("A1_MINIMUMS")
    minimums = minimums if isinstance(minimums, Mapping) else {}
    minimum_score = _number(minimums.get("minimum_score")) or _number(snapshot.get("MIN_STRUCTURAL_SCORE")) or 65.0
    minimum_quality = _number(minimums.get("minimum_data_quality")) or 75.0
    minimum_available_weight = _number(
        minimums.get("minimum_available_weight", snapshot.get("A1_MINIMUM_AVAILABLE_WEIGHT", 0.70))
    )
    if minimum_available_weight is None or minimum_available_weight <= 0 or minimum_available_weight > 1:
        minimum_available_weight = 0.70
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
        primary_link = matched[0] if matched else {}
        primary_theme = theme_by_id.get(str(primary_link.get("theme_id") or ""), {}) if matched else {}
        primary_node = node_by_id.get(str(primary_link.get("node_id") or ""), {}) if matched else {}
        factor_details = _a1_factor_details(
            snapshot,
            symbol=symbol,
            matched=matched,
            theme=primary_theme,
            node=primary_node,
            raw_evidence_available=raw_evidence_available,
            structured_exposure=exposure_facts,
            maximum_revenue_exposure_pct=maximum_exposure,
            financial_quality=financial_quality,
            financial_details=financial_details,
            data_quality=data_quality,
            as_of=_snapshot_as_of(snapshot),
        )
        score_breakdown = _a1_breakdown(weights, factor_details)
        score = _weighted_score(score_breakdown, weights)
        available_weight = _a1_available_weight(factor_details, weights)
        liquidity_score = _liquidity_score(amount)
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
        elif available_weight < minimum_available_weight:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_FACTOR_COVERAGE_BELOW_MINIMUM")
        elif score < minimum_score:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_LOCAL_SCORE_BELOW_MINIMUM")
        else:
            status = "LOCAL_CANDIDATE"
        if matched and not hard_reject and available_weight < minimum_available_weight:
            # Preserve every independent data-gap reason even when an earlier
            # fail-closed branch (for example missing business evidence) has
            # already selected LOCAL_MONITOR.
            reason_codes.append("A1_FACTOR_COVERAGE_BELOW_MINIMUM")
        if raw_evidence_available and not structured_exposure_available:
            reason_codes.append("A1_BUSINESS_EXPOSURE_UNSTRUCTURED")

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
            "factor_details": factor_details,
            "available_weight": round(available_weight, 6),
            "available_weight_pct": round(available_weight * 100.0, 4),
            "minimum_available_weight": round(minimum_available_weight, 6),
            "missing_factors": [
                key for key, value in factor_details.items()
                if not isinstance(value, Mapping) or value.get("available") is not True
            ],
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
                and not set(item.get("reason_codes", ())).intersection({
                    "A1_LOCAL_SCORE_BELOW_MINIMUM",
                    "A1_FACTOR_COVERAGE_BELOW_MINIMUM",
                    "A1_FUNDAMENTAL_DATA_INCOMPLETE",
                    "A1_MAIN_BUSINESS_EVIDENCE_MISSING",
                    "A1_DATA_QUALITY_BELOW_MINIMUM",
                })
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
    """Build the A2 market-core and supply-chain review routes locally.

    A2 is intentionally a scorer, not an LLM-sized second full-market scan.
    Every row comes from A1 ``active_research_pool`` and carries its A1 theme,
    chain node and business evidence forward.  Scores are calculated from the
    configured semantic factors.  A missing capital-flow source removes that
    dimension from the denominator; turnover is never used as a capital-flow
    substitute.
    """

    if llm_top_n_per_theme < 1:
        raise ValueError("A2 Top-N value must be positive")

    rows = _mapping_list(a1_output.get("active_research_pool"))
    candidates = _candidate_map(snapshot)
    factors = snapshot.get("FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else {}
    recent_bars = snapshot.get("RECENT_DAILY_BARS")
    recent_bars = recent_bars if isinstance(recent_bars, Mapping) else {}
    industry_membership = _membership_map(snapshot.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
    local_market_factors = _build_a2_local_market_factors(
        snapshot,
        candidates=candidates,
        industry_membership=industry_membership,
    )
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
    weights, weight_source, configured_weights = _a2_weights(snapshot)
    enforce_coverage = configured_weights or "CAPITAL_FLOW_SNAPSHOT" in snapshot
    coverage_minimum = _a2_coverage_minimum(snapshot)
    source_hashes = _source_hashes(snapshot)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    decisions: list[dict[str, Any]] = []
    for item in rows:
        symbol = _symbol(item.get("symbol"))
        if not symbol:
            continue
        theme_id = str(item.get("primary_theme") or item.get("theme_id") or "UNMAPPED")
        candidate = candidates.get(symbol, {})
        amount = max(0.0, _number(candidate.get("amount")) or _number(candidate.get("turnover")) or 0.0)
        factor = factors.get(symbol)
        factor = factor if isinstance(factor, Mapping) else {}
        explicit_relative = _relative_strength_score(factor, default=None)
        relative = explicit_relative if explicit_relative is not None else _percentile_score(
            bar_returns.get(symbol), return_distribution
        )
        liquidity = _liquidity_score(amount)
        rotations = [
            rotation_by_code.get(str(membership.get("taxonomy_code") or ""))
            for membership in industry_membership.get(symbol, ())
        ]
        rotations = [value for value in rotations if isinstance(value, Mapping)]
        cycle_score = max((_cycle_rotation_score(value) for value in rotations), default=None)
        bottleneck_context = deterministic_bottleneck_context(
            item,
            demand_score_0_100=cycle_score if cycle_score is not None else 0.0,
            timing_score_0_100=max(relative, cycle_score or 0.0),
            factor_weights=item.get("bottleneck_factor_weights")
            if isinstance(item.get("bottleneck_factor_weights"), Mapping)
            else None,
        )
        factor_scores = _a2_factor_scores(
            item=item,
            symbol=symbol,
            theme_id=theme_id,
            snapshot=snapshot,
            factor=factor,
            relative=relative,
            liquidity=liquidity,
            cycle_score=cycle_score,
            attention=symbol in attention,
            dragon=symbol in dragon,
            local_market_factors=local_market_factors.get(symbol, {}),
        )
        score, coverage = _available_weighted_score(factor_scores, weights)
        identifiability, identity_breakdown = _a2_identifiability(
            item=item,
            relative=relative,
            liquidity=liquidity,
            factor_scores=factor_scores,
            attention=symbol in attention,
            dragon=symbol in dragon,
        )
        eligible_routes = _a2_route_eligibility(
            item=item,
            identifiability=identifiability,
            minimum_identifiability_score=minimum_identifiability_score,
            factor_scores=factor_scores,
            bottleneck_context=bottleneck_context,
        )
        reasons: list[str] = []
        low_identity = identifiability < minimum_identifiability_score
        if low_identity:
            reasons.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
        if enforce_coverage and coverage["ratio"] < coverage_minimum:
            reasons.append("A2_FACTOR_COVERAGE_BELOW_MINIMUM")
        capital_flow = factor_scores.get("capital_flow", {})
        if capital_flow.get("available") is not True:
            reasons.append("A2_CAPITAL_FLOW_UNAVAILABLE")
        status = "REVIEW_CANDIDATE"
        if low_identity:
            status = "HARD_REJECT"
            reasons.append("A2_LOW_IDENTITY_EXCLUDED")
        elif enforce_coverage and coverage["ratio"] < coverage_minimum:
            status = "LOCAL_MONITOR"
        elif not eligible_routes and enforce_coverage:
            status = "LOCAL_MONITOR"
            reasons.append("A2_NO_ROUTE_READY")
        elif not configured_weights:
            reasons.append("A2_SCORE_WEIGHTS_FALLBACK")
        decision = {
            "symbol": symbol,
            "name": item.get("company_name") or item.get("name") or candidate.get("name"),
            "stage": "A2_LOCAL_ROLE",
            "status": status,
            "score": round(score, 4),
            "identifiability_score": round(identifiability, 4),
            "theme_id": theme_id,
            "primary_theme": theme_id,
            "node_id": item.get("industry_chain_node") or item.get("node_id"),
            "industry_chain_node": item.get("industry_chain_node") or item.get("node_id"),
            "upstream_candidate_id": item.get("candidate_id") or item.get("upstream_candidate_id"),
            "business_exposure": item.get("business_exposure"),
            "business_exposure_facts": item.get("business_exposure_facts", []),
            "source_refs": list(item.get("source_refs") or ()) if isinstance(item.get("source_refs"), Sequence) and not isinstance(item.get("source_refs"), (str, bytes, bytearray)) else [],
            "role": _role(identifiability, liquidity, relative),
            "route": eligible_routes[0] if eligible_routes else None,
            "eligible_routes": eligible_routes,
            "route_eligibility": {
                MARKET_CORE_ROUTE: _market_core_route_result(item, identifiability, minimum_identifiability_score, factor_scores, coverage, enforce_coverage, coverage_minimum),
                SUPPLY_CHAIN_ALPHA_ROUTE: _supply_chain_route_result(item, bottleneck_context),
            },
            "role_breakdown": {
                "relative_strength": round(relative, 4),
                "liquidity_capacity": round(liquidity, 4),
                "monthly_cycle_rotation": round(cycle_score, 4) if cycle_score is not None else None,
                "bottleneck_evidence_readiness": _number(bottleneck_context.get("evidence_readiness_score")),
                "relative_strength_source": "FACTOR_SNAPSHOT" if explicit_relative is not None else "RECENT_DAILY_BARS",
                "identifiability": identity_breakdown,
            },
            "a2_factor_scores": factor_scores,
            "factor_coverage": coverage,
            "score_weight_source": weight_source,
            "bottleneck_context": bottleneck_context,
            "bottleneck_status": "NOT_REQUIRED_FOR_MARKET_CORE" if MARKET_CORE_ROUTE in eligible_routes else "UNPROVEN",
            "reason_codes": list(dict.fromkeys(reasons)),
            "sent_to_llm": False,
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        }
        decisions.append(decision)
        if status == "REVIEW_CANDIDATE":
            grouped[theme_id].append(decision)
    for theme_id, values in grouped.items():
        values.sort(key=lambda item: (-float(item["score"]), -float(item["identifiability_score"]), str(item["symbol"])))
        for rank, item in enumerate(values, start=1):
            item["theme_rank"] = rank
            if rank > llm_top_n_per_theme:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A2_NOT_SENT_TO_LLM")
            else:
                item["sent_to_llm"] = True
    decisions.sort(key=lambda item: str(item["symbol"]))
    return DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=tuple(decisions),
        review_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
        monitor_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "LOCAL_MONITOR"),
        rejected_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "HARD_REJECT"),
    )


def _a2_weights(snapshot: Mapping[str, Any]) -> tuple[dict[str, float], str, bool]:
    """Read A2 weights from the frozen configuration and normalize them."""

    raw = snapshot.get("A2_SCORE_WEIGHTS")
    if not isinstance(raw, Mapping) or not raw:
        raw = snapshot.get("THEME_SCORE_WEIGHTS")
    parsed: dict[str, float] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            number = _number(value)
            if number is not None and number > 0:
                parsed[str(key)] = number
    if not parsed:
        # Legacy unit snapshots do not carry prompt parameters.  This fallback
        # is equal-weighted at runtime and is explicitly marked as such; it is
        # not a copy of the production configuration or a proxy weight set.
        parsed = {key: 1.0 for key in A2_THEME_FACTORS}
        return (
            {key: 1.0 / len(parsed) for key in parsed},
            "EQUAL_RUNTIME_FALLBACK",
            False,
        )
    total = sum(parsed.values())
    if total <= 0:
        parsed = {key: 1.0 for key in A2_THEME_FACTORS}
        return (
            {key: 1.0 / len(parsed) for key in parsed},
            "EQUAL_RUNTIME_FALLBACK",
            False,
        )
    return ({key: value / total for key, value in parsed.items()}, "CONFIGURED", True)


def _a2_coverage_minimum(snapshot: Mapping[str, Any]) -> float:
    for key in (
        "A2_FACTOR_COVERAGE_MINIMUM",
        "A2_MIN_FACTOR_COVERAGE",
        "MIN_A2_FACTOR_COVERAGE",
    ):
        value = _number(snapshot.get(key))
        if value is not None:
            # Configuration is expressed as a ratio, but accepting 65 keeps
            # hand-authored snapshots unambiguous and is recorded downstream.
            return max(0.0, min(1.0, value / 100.0 if value > 1.0 else value))
    return A2_FACTOR_COVERAGE_MINIMUM


def _a2_factor_scores(
    *,
    item: Mapping[str, Any],
    symbol: str,
    theme_id: str,
    snapshot: Mapping[str, Any],
    factor: Mapping[str, Any],
    relative: float,
    liquidity: float,
    cycle_score: float | None,
    attention: bool,
    dragon: bool,
    local_market_factors: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    aliases: dict[str, tuple[str, ...]] = {
        "breadth": ("breadth_score", "sector_breadth_score", "sector_breadth", "breadth"),
        "turnover_share": ("turnover_share_score", "turnover_share_pct", "turnover_share", "volume_share"),
        "capital_flow": ("capital_flow_score", "net_inflow_score", "capital_flow", "fund_flow_score"),
        "leader_structure": ("leader_structure_score", "leader_score", "leadership_score"),
        "tier_structure": ("tier_structure_score", "ladder_score", "tier_score"),
        "profit_effect": ("profit_effect_score", "profit_effect", "earning_effect_score"),
        "catalyst_freshness": ("catalyst_freshness_score", "catalyst_score", "event_freshness_score"),
        "index_chain_resonance": ("index_chain_resonance_score", "chain_resonance_score", "relative_strength_score"),
        "agent_1_quality": ("agent_1_quality_score", "a1_quality_score", "data_quality_score"),
    }
    for name in A2_THEME_FACTORS:
        if name == "capital_flow" and not _capital_flow_available(snapshot):
            # A model/A1 row cannot authorize a capital-flow score.  The source
            # availability flag is the sole authority for this dimension.
            value = _capital_flow_unavailable(snapshot)
        else:
            value = _read_item_factor(item, name, aliases[name])
            if value is None:
                value = _read_snapshot_factor(snapshot, symbol, theme_id, name, aliases[name])
            if value is None and isinstance(local_market_factors.get(name), Mapping):
                value = dict(local_market_factors[name])
            if value is None and name == "capital_flow":
                value = _capital_flow_unavailable(snapshot)
        if value is None and name == "index_chain_resonance" and cycle_score is not None:
            value = _factor_result(cycle_score, "SECTOR_CYCLE_SNAPSHOT", (), "OK")
        if value is None and name == "leader_structure" and (attention or dragon):
            value = _factor_result(100.0 if dragon else 75.0, "DRAGON_TIGER_SNAPSHOT" if dragon else "MARKET_ATTENTION_SNAPSHOT", (), "OK")
        if value is None and name == "agent_1_quality":
            quality = _number(item.get("data_quality_score"))
            if quality is None:
                confidence = _number(item.get("evidence_confidence"))
                quality = confidence * 100.0 if confidence is not None else _number(item.get("structural_score"))
            if quality is not None:
                value = _factor_result(quality, "A1_ACTIVE_RESEARCH_POOL", _item_source_refs(item), "OK")
        result[name] = value or _factor_result(None, "UNAVAILABLE", (), "A2_FACTOR_UNAVAILABLE")
    return result


def _build_a2_local_market_factors(
    snapshot: Mapping[str, Any],
    *,
    candidates: Mapping[str, Mapping[str, Any]],
    industry_membership: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Derive auditable A2 market factors from the frozen G0 snapshot.

    These are deliberately narrow definitions: member breadth, sector share
    of G0 turnover, member profit effect, stock relative-strength/liquidity,
    and source-backed disclosure freshness.  None of them is labelled or used
    as capital flow.
    """

    by_industry: dict[str, set[str]] = defaultdict(set)
    for symbol, memberships in industry_membership.items():
        for membership in memberships:
            code = str(membership.get("taxonomy_code") or "").strip()
            if code:
                by_industry[code].add(symbol)

    total_amount = sum(max(0.0, _number(item.get("amount")) or _number(item.get("turnover")) or 0.0) for item in candidates.values())
    sector_rows: dict[str, dict[str, float]] = {}
    for code, symbols in by_industry.items():
        amounts = [
            max(0.0, _number(candidates.get(symbol, {}).get("amount")) or _number(candidates.get(symbol, {}).get("turnover")) or 0.0)
            for symbol in symbols
        ]
        changes = [
            value
            for symbol in symbols
            if (value := _number(candidates.get(symbol, {}).get("change_ratio_pct"))) is not None
        ]
        observed = len(changes)
        breadth = sum(value > 0 for value in changes) / observed if observed else None
        mean_change = sum(changes) / observed if observed else None
        sector_rows[code] = {
            "turnover_share": (sum(amounts) / total_amount) if total_amount > 0 else 0.0,
            "breadth": breadth if breadth is not None else -1.0,
            "mean_change": mean_change if mean_change is not None else float("nan"),
        }
    turnover_distribution = sorted(row["turnover_share"] for row in sector_rows.values())

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for symbol, candidate in candidates.items():
        codes = [
            str(item.get("taxonomy_code") or "")
            for item in industry_membership.get(symbol, ())
            if str(item.get("taxonomy_code") or "") in sector_rows
        ]
        if not codes:
            codes = []
        primary_code = max(codes, key=lambda code: sector_rows[code]["turnover_share"], default=None)
        symbol_factors: dict[str, dict[str, Any]] = {}
        if primary_code is not None:
            sector = sector_rows[primary_code]
            if sector["breadth"] >= 0:
                symbol_factors["breadth"] = _factor_result(
                    sector["breadth"] * 100.0,
                    "FROZEN_G0_INDUSTRY_BREADTH",
                    (),
                    "OK",
                )
            symbol_factors["turnover_share"] = _factor_result(
                _percentile_score(sector["turnover_share"], turnover_distribution),
                "FROZEN_G0_INDUSTRY_TURNOVER_SHARE",
                (),
                "OK",
            )
            if math.isfinite(sector["mean_change"]) and sector["breadth"] >= 0:
                return_component = _scale(sector["mean_change"], -2.0, 2.0)
                symbol_factors["profit_effect"] = _factor_result(
                    0.60 * sector["breadth"] * 100.0 + 0.40 * return_component,
                    "FROZEN_G0_INDUSTRY_MEMBER_RETURNS",
                    (),
                    "OK",
                )

        relative = _relative_strength_score(
            snapshot.get("FACTOR_SNAPSHOT", {}).get(symbol, {})
            if isinstance(snapshot.get("FACTOR_SNAPSHOT"), Mapping)
            else {},
            default=None,
        )
        amount = max(0.0, _number(candidate.get("amount")) or _number(candidate.get("turnover")) or 0.0)
        if relative is not None and amount > 0:
            symbol_factors["leader_structure"] = _factor_result(
                0.60 * relative + 0.40 * _liquidity_score(amount),
                "FROZEN_RELATIVE_STRENGTH_AND_LIQUIDITY",
                (),
                "OK",
            )
        catalyst_score, catalyst_available, catalyst_refs, _reason = _catalyst_factor(
            snapshot.get("DISCLOSURE_EVENTS"), symbol
        )
        if catalyst_available:
            symbol_factors["catalyst_freshness"] = _factor_result(
                catalyst_score,
                "DISCLOSURE_EVENTS",
                catalyst_refs,
                "OK",
            )
        result[symbol] = symbol_factors
    return result


def _a2_identifiability(
    *,
    item: Mapping[str, Any],
    relative: float,
    liquidity: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    attention: bool,
    dragon: bool,
) -> tuple[float, dict[str, float | None]]:
    """Average independently available identity evidence without proxy weights."""

    business = _business_purity_score(item)
    a1_quality = factor_scores.get("agent_1_quality", {})
    market = factor_scores.get("leader_structure", {})
    tier = factor_scores.get("tier_structure", {})
    market_value = None
    if market.get("available") is True:
        market_value = _number(market.get("score"))
    elif tier.get("available") is True:
        market_value = _number(tier.get("score"))
    elif dragon or attention:
        market_value = 100.0 if dragon else 75.0
    breakdown: dict[str, float | None] = {
        "relative_strength": round(relative, 4),
        "liquidity_capacity": round(liquidity, 4),
        "business_purity": round(business, 4) if business is not None else None,
        "market_confirmation": round(market_value, 4) if market_value is not None else None,
        "agent_1_quality": round(_number(a1_quality.get("score")), 4)
        if a1_quality.get("available") is True and _number(a1_quality.get("score")) is not None
        else None,
    }
    values = [value for value in breakdown.values() if value is not None]
    return (sum(values) / len(values) if values else 0.0), breakdown


def _a2_route_eligibility(
    *,
    item: Mapping[str, Any],
    identifiability: float,
    minimum_identifiability_score: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    bottleneck_context: Mapping[str, Any],
) -> tuple[str, ...]:
    if identifiability < minimum_identifiability_score:
        return ()
    market = _market_core_route_result(
        item,
        identifiability,
        minimum_identifiability_score,
        factor_scores,
        {"ratio": _factor_coverage_ratio(factor_scores)},
        False,
        A2_FACTOR_COVERAGE_MINIMUM,
    )
    supply = _supply_chain_route_result(item, bottleneck_context)
    return tuple(
        route
        for route, result in (
            (MARKET_CORE_ROUTE, market),
            (SUPPLY_CHAIN_ALPHA_ROUTE, supply),
        )
        if result.get("eligible") is True
    )


def _market_core_route_result(
    item: Mapping[str, Any],
    identifiability: float,
    minimum_identifiability_score: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    coverage: Mapping[str, Any],
    enforce_coverage: bool,
    coverage_minimum: float,
) -> dict[str, Any]:
    missing: list[str] = []
    if identifiability < minimum_identifiability_score:
        missing.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
    if not str(item.get("primary_theme") or item.get("theme_id") or "").strip():
        missing.append("A1_THEME_MISSING")
    if not str(item.get("industry_chain_node") or item.get("node_id") or "").strip():
        missing.append("A1_CHAIN_NODE_MISSING")
    if not _has_business_evidence(item):
        missing.append("A1_BUSINESS_EVIDENCE_MISSING")
    market_factor_names = ("breadth", "turnover_share", "leader_structure", "tier_structure", "index_chain_resonance")
    market_fact_count = sum(
        factor_scores.get(name, {}).get("available") is True
        for name in market_factor_names
    )
    if enforce_coverage and _safe_float(coverage.get("ratio")) < coverage_minimum:
        missing.append("A2_FACTOR_COVERAGE_BELOW_MINIMUM")
    if enforce_coverage and market_fact_count < 2:
        missing.append("A2_MARKET_FACTS_INSUFFICIENT")
    return {
        "eligible": not missing,
        "route": MARKET_CORE_ROUTE,
        "missing_reason_codes": list(dict.fromkeys(missing)),
        "bottleneck_status": "NOT_REQUIRED_FOR_MARKET_CORE",
        "market_fact_count": market_fact_count,
    }


def _supply_chain_route_result(item: Mapping[str, Any], bottleneck_context: Mapping[str, Any]) -> dict[str, Any]:
    missing: list[str] = []
    role = str(item.get("supply_chain_role") or "").strip()
    if role not in {"CONTROLS_SCARCE_LAYER", "SUPPLIES_SCARCE_LAYER", "BENEFITS_WITH_LIMITED_CONTROL"}:
        missing.append("A2_SUPPLY_CHAIN_ROLE_NOT_FOCUS_ELIGIBLE")
    if not str(item.get("scarce_layer") or "").strip():
        missing.append("A2_SCARCE_LAYER_MISSING")
    if not str(item.get("value_chain_position") or "").strip():
        missing.append("A2_VALUE_CHAIN_POSITION_MISSING")
    scorecard, reasons = canonicalize_model_scorecard(item.get("bottleneck_scorecard"))
    missing.extend(reasons)
    evidence = item.get("bottleneck_evidence")
    evidence_rows = evidence if isinstance(evidence, list) else []
    valid_evidence = [row for row in evidence_rows if isinstance(row, Mapping) and str(row.get("claim") or "").strip() and str(row.get("source_ref") or "").strip()]
    stronger = [row for row in valid_evidence if str(row.get("strength") or "").upper() in {"STRONG", "MEDIUM"}]
    if len(valid_evidence) < 2:
        missing.append("A2_BOTTLENECK_EVIDENCE_INSUFFICIENT")
    if not stronger:
        missing.append("A2_BOTTLENECK_STRONG_EVIDENCE_MISSING")
    if not str(item.get("missing_proof") or "").strip():
        missing.append("A2_BOTTLENECK_MISSING_PROOF_UNDECLARED")
    kill_switches = item.get("kill_switches")
    if not isinstance(kill_switches, list) or not any(str(value).strip() for value in kill_switches):
        missing.append("A2_BOTTLENECK_KILL_SWITCH_MISSING")
    if isinstance(bottleneck_context, Mapping) and bottleneck_context.get("scarcity_claim_allowed") is True:
        # A deterministic context cannot authorize an unsupported scarcity
        # claim.  This is defensive for hand-authored snapshots.
        missing.append("A2_SCARCITY_AUTHORIZATION_INVALID")
    return {
        "eligible": not missing and scorecard is not None,
        "route": SUPPLY_CHAIN_ALPHA_ROUTE,
        "missing_reason_codes": list(dict.fromkeys(missing)),
        "bottleneck_status": "SOURCE_BACKED" if not missing and scorecard is not None else "UNPROVEN",
        "evidence_count": len(valid_evidence),
    }


def _available_weighted_score(
    factor_scores: Mapping[str, Mapping[str, Any]],
    weights: Mapping[str, float],
) -> tuple[float, dict[str, Any]]:
    available_names = [
        name for name in weights
        if isinstance(factor_scores.get(name), Mapping)
        and factor_scores[name].get("available") is True
        and _number(factor_scores[name].get("score")) is not None
    ]
    available_weight = sum(weights[name] for name in available_names)
    total_weight = sum(weights.values())
    score = (
        sum(_safe_float(factor_scores[name].get("score")) * weights[name] for name in available_names) / available_weight
        if available_weight > 0
        else 0.0
    )
    return round(score, 4), {
        "available_weight": round(available_weight, 6),
        "total_weight": round(total_weight, 6),
        "ratio": round(available_weight / total_weight, 6) if total_weight else 0.0,
        "percent": round(available_weight / total_weight * 100.0, 4) if total_weight else 0.0,
        "available_factors": available_names,
        "missing_factors": [name for name in weights if name not in available_names],
    }


def _factor_coverage_ratio(factor_scores: Mapping[str, Mapping[str, Any]]) -> float:
    available = sum(
        value.get("available") is True
        for value in factor_scores.values()
        if isinstance(value, Mapping)
    )
    total = len(factor_scores)
    return available / total if total else 0.0


def _read_item_factor(item: Mapping[str, Any], name: str, aliases: Sequence[str]) -> dict[str, Any] | None:
    for key in ("a2_factor_scores", "market_factor_scores", "factor_scores"):
        container = item.get(key)
        if not isinstance(container, Mapping):
            continue
        payload = container.get(name)
        result = _read_metric_payload(payload, source="A1_ACTIVE_RESEARCH_POOL", source_refs=_item_source_refs(item), ratio_hint=False)
        if result is not None:
            return result
        result = _read_metric_fields(container, aliases, source="A1_ACTIVE_RESEARCH_POOL", source_refs=_item_source_refs(item), ratio_hint=False)
        if result is not None:
            return result
    return _read_metric_fields(item, aliases, source="A1_ACTIVE_RESEARCH_POOL", source_refs=_item_source_refs(item), ratio_hint=False)


def _read_snapshot_factor(
    snapshot: Mapping[str, Any],
    symbol: str,
    theme_id: str,
    name: str,
    aliases: Sequence[str],
) -> dict[str, Any] | None:
    source_names = {
        "capital_flow": ("CAPITAL_FLOW_SNAPSHOT",),
        "tier_structure": ("TIER_STRUCTURE_SNAPSHOT", "MARKET_EMOTION_SNAPSHOT"),
        "breadth": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "SECTOR_CYCLE_SNAPSHOT"),
        "turnover_share": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "SECTOR_CYCLE_SNAPSHOT"),
        "leader_structure": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "DRAGON_TIGER_SNAPSHOT", "MARKET_ATTENTION_SNAPSHOT"),
        "profit_effect": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "MARKET_EMOTION_SNAPSHOT"),
        "catalyst_freshness": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "DISCLOSURE_EVENTS", "NEWS_HEAT_SNAPSHOT"),
        "index_chain_resonance": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "SECTOR_CYCLE_SNAPSHOT"),
        "agent_1_quality": ("A2_FACTOR_SNAPSHOT",),
    }
    for source_name in source_names.get(name, ("A2_FACTOR_SNAPSHOT",)):
        root = snapshot.get(source_name)
        if name == "capital_flow" and (not isinstance(root, Mapping) or root.get("available") is not True):
            continue
        for payload in _scoped_payloads(root, symbol, theme_id):
            result = _read_metric_fields(payload, aliases, source=source_name, source_refs=_payload_source_refs(payload), ratio_hint=name in {"breadth", "turnover_share"})
            if result is not None:
                return result
            # A factor may be represented as a nested object keyed by its
            # semantic name rather than flattened into the record.
            nested = payload.get(name) if isinstance(payload, Mapping) else None
            result = _read_metric_payload(nested, source=source_name, source_refs=_payload_source_refs(payload), ratio_hint=name in {"breadth", "turnover_share"})
            if result is not None:
                return result
    return None


def _capital_flow_unavailable(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    source = snapshot.get("CAPITAL_FLOW_SNAPSHOT")
    reason = "SOURCE_NOT_CONFIGURED"
    if isinstance(source, Mapping):
        reason = str(source.get("reason_code") or "SOURCE_UNAVAILABLE")
    return _factor_result(
        None,
        "CAPITAL_FLOW_SNAPSHOT",
        (),
        reason,
        reason_code_override="A2_CAPITAL_FLOW_UNAVAILABLE",
    )


def _capital_flow_available(snapshot: Mapping[str, Any]) -> bool:
    source = snapshot.get("CAPITAL_FLOW_SNAPSHOT")
    return isinstance(source, Mapping) and source.get("available") is True


def _read_metric_fields(
    payload: Mapping[str, Any],
    aliases: Sequence[str],
    *,
    source: str,
    source_refs: Sequence[str],
    ratio_hint: bool,
) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    for alias in aliases:
        if alias not in payload:
            continue
        result = _read_metric_payload(
            payload.get(alias),
            source=source,
            source_refs=source_refs,
            ratio_hint=ratio_hint or "ratio" in alias or "share" in alias or "pct" in alias,
        )
        if result is not None:
            return result
    return None


def _read_metric_payload(
    raw: Any,
    *,
    source: str,
    source_refs: Sequence[str],
    ratio_hint: bool,
) -> dict[str, Any] | None:
    available = True
    if isinstance(raw, Mapping):
        if raw.get("available") is False:
            return _factor_result(None, source, source_refs, "A2_FACTOR_UNAVAILABLE")
        available = raw.get("available") is not False
        for key in ("score", "normalized_score", "percentile", "value", "raw_value"):
            if key in raw:
                number = _number(raw.get(key))
                if number is not None:
                    ratio = ratio_hint or key in {"percentile", "ratio"}
                    return _factor_result(number * 100.0 if ratio and 0.0 <= number <= 1.0 else number, source, _payload_source_refs(raw) or source_refs, "OK")
        return _factor_result(None, source, _payload_source_refs(raw) or source_refs, "A2_FACTOR_VALUE_MISSING")
    number = _number(raw)
    if number is None:
        return None
    if ratio_hint and 0.0 <= number <= 1.0:
        number *= 100.0
    if number < 0.0 or number > 100.0:
        return _factor_result(None, source, source_refs, "A2_FACTOR_VALUE_INVALID")
    return _factor_result(number, source, source_refs, "OK") if available else _factor_result(None, source, source_refs, "A2_FACTOR_UNAVAILABLE")


def _factor_result(
    score: float | None,
    source: str,
    source_refs: Sequence[str],
    reason_code: str,
    *,
    reason_code_override: str | None = None,
) -> dict[str, Any]:
    return {
        "score": round(score, 4) if score is not None else None,
        "available": score is not None,
        "source": source,
        "source_refs": list(dict.fromkeys(str(value) for value in source_refs if str(value))),
        "reason_code": reason_code_override or reason_code,
    }


def _scoped_payloads(value: Any, symbol: str, theme_id: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    result: list[Mapping[str, Any]] = []

    def add_mapping(mapping: Any) -> None:
        if isinstance(mapping, Mapping):
            result.append(mapping)

    add_mapping(value.get(symbol))
    add_mapping(value.get(theme_id))
    for key in ("by_symbol", "symbols", "by_theme", "themes", "theme_metrics", "metrics", "records", "items", "payload", "data"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            add_mapping(nested.get(symbol))
            add_mapping(nested.get(theme_id))
        elif isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
            for row in nested:
                if not isinstance(row, Mapping):
                    continue
                row_symbol = _symbol(row.get("symbol") or row.get("thscode"))
                row_theme = str(row.get("theme_id") or row.get("primary_theme") or "")
                row_industry = str(row.get("industry_thscode") or row.get("industry_code") or "")
                if row_symbol == symbol or row_theme == theme_id or row_industry == theme_id:
                    add_mapping(row)
    return result


def _payload_source_refs(value: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    raw = value.get("source_refs")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        refs.extend(str(item) for item in raw if str(item).strip())
    for key in ("source_ref", "fact_id", "source_url"):
        if str(value.get(key) or "").strip():
            refs.append(str(value[key]).strip())
    return list(dict.fromkeys(refs))


def _item_source_refs(item: Mapping[str, Any]) -> list[str]:
    raw = item.get("source_refs")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return list(dict.fromkeys(str(value) for value in raw if str(value).strip()))
    return []


def _business_purity_score(item: Mapping[str, Any]) -> float | None:
    facts = item.get("business_exposure_facts")
    if isinstance(facts, Sequence) and not isinstance(facts, (str, bytes, bytearray)):
        exposures = [
            _number(row.get("revenue_exposure_pct"))
            for row in facts
            if isinstance(row, Mapping) and _number(row.get("revenue_exposure_pct")) is not None
        ]
        if exposures:
            return max(0.0, min(100.0, max(exposures)))
    business = item.get("business_exposure")
    if isinstance(business, Mapping):
        exposure = _number(business.get("revenue_exposure_pct"))
        if exposure is not None:
            return max(0.0, min(100.0, exposure))
    breakdown = item.get("score_breakdown")
    if isinstance(breakdown, Mapping):
        for key in ("business_mapping", "business_purity", "business_exposure"):
            value = _number(breakdown.get(key))
            if value is not None:
                return max(0.0, min(100.0, value))
    return None


def _has_business_evidence(item: Mapping[str, Any]) -> bool:
    facts = item.get("business_exposure_facts")
    if isinstance(facts, Sequence) and not isinstance(facts, (str, bytes, bytearray)):
        return any(isinstance(row, Mapping) and str(row.get("evidence_ref") or row.get("source_ref") or "").strip() for row in facts)
    business = item.get("business_exposure")
    if isinstance(business, Mapping):
        return bool(str(business.get("source_ref") or business.get("evidence_ref") or "").strip())
    return False


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
            "factor_details": item.get("factor_details", {}),
            "available_weight": item.get("available_weight"),
            "available_weight_pct": item.get("available_weight_pct"),
            "missing_factors": item.get("missing_factors", []),
            "evidence_confidence": 0.0,
            "status": "MONITOR",
            "reason_codes": item.get("reason_codes", []),
            "local_decision": True,
            "sent_to_llm": False,
            "source_refs": list(dict.fromkeys(
                str(ref)
                for factor in (item.get("factor_details") or {}).values()
                if isinstance(factor, Mapping)
                for ref in (factor.get("source_refs") or ())
                if str(ref)
            )),
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
            *[
                str(ref)
                for factor in (item.get("factor_details") or {}).values()
                if isinstance(factor, Mapping)
                for ref in (factor.get("source_refs") or ())
                if str(ref)
            ],
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
            "factor_details": dict(item.get("factor_details") or {}),
            "available_weight": item.get("available_weight"),
            "available_weight_pct": item.get("available_weight_pct"),
            "missing_factors": list(item.get("missing_factors") or ()),
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
        "cashflow_quality": pick(
            "cashflow_net_income_ratio",
            "operating_cash_flow_net_divide_income",
            "net_profit_cash_content",
            "经营现金流净利润比",
        ),
        "debt_ratio": pick("debt_to_assets", "资产负债率"),
    }
    available = [value for value in features.values() if value is not None]
    if not available:
        return 0.0, features
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


def _a1_factor_details(
    snapshot: Mapping[str, Any],
    *,
    symbol: str,
    matched: Sequence[Mapping[str, Any]],
    theme: Mapping[str, Any],
    node: Mapping[str, Any],
    raw_evidence_available: bool,
    structured_exposure: Sequence[Mapping[str, Any]],
    maximum_revenue_exposure_pct: float,
    financial_quality: float,
    financial_details: Mapping[str, Any],
    data_quality: float,
    as_of: str | None,
) -> dict[str, dict[str, Any]]:
    """Build independent A1 factor records without filling missing factors.

    ``score_breakdown`` remains a numeric compatibility view for the existing
    model contract.  The richer records here carry availability and evidence,
    allowing the caller to distinguish a real zero from an unavailable factor.
    """

    structural_refs = _source_refs_from_values(
        theme.get("source_refs"),
        node.get("source_refs"),
    )
    structural_available = bool(matched)
    structural_score = 95.0 if structural_available else 0.0

    business_refs = _source_refs_from_values(
        *(fact.get("evidence_ref") for fact in structured_exposure if isinstance(fact, Mapping))
    )
    business_available = bool(structured_exposure)
    business_score = (
        min(100.0, 55.0 + maximum_revenue_exposure_pct * 0.6)
        if business_available
        else 0.0
    )

    barrier_score, barrier_available, barrier_refs, barrier_reason = _barrier_factor(node)
    financial_refs = _source_refs_from_mapping(snapshot.get("COMPANY_FUNDAMENTALS"), symbol)
    financial_available = bool(
        any(value is not None for value in financial_details.values())
    )
    cashflow_value = _number(financial_details.get("cashflow_quality"))
    cashflow_score = _scale(cashflow_value, 0.0, 1.2) if cashflow_value is not None else 0.0
    cashflow_available = cashflow_value is not None
    evidence_available = bool(raw_evidence_available or financial_available)
    evidence_refs = _source_refs_from_values(
        _source_refs_from_mapping(snapshot.get("MAIN_BUSINESS_EVIDENCE"), symbol),
        _source_refs_from_mapping(snapshot.get("COMPANY_FUNDAMENTALS"), symbol),
    )
    catalyst_score, catalyst_available, catalyst_refs, catalyst_reason = _catalyst_factor(
        snapshot.get("DISCLOSURE_EVENTS"), symbol
    )
    valuation_score, valuation_available, valuation_refs, valuation_reason = _valuation_factor(
        snapshot.get("COMPANY_FUNDAMENTALS"), symbol,
        snapshot.get("RESEARCH_CONSENSUS"),
    )

    return {
        "structural_theme": _factor_record(
            structural_score,
            available=structural_available,
            source_refs=structural_refs,
            as_of=as_of,
            missing_reason=None if structural_available else "A1_STRUCTURAL_THEME_NOT_MAPPED",
        ),
        "business_mapping": _factor_record(
            business_score,
            available=business_available,
            source_refs=business_refs,
            as_of=as_of,
            missing_reason=None if business_available else "A1_EXPLICIT_REVENUE_EXPOSURE_MISSING",
        ),
        "barrier_and_bottleneck": _factor_record(
            barrier_score,
            available=barrier_available,
            source_refs=barrier_refs,
            as_of=as_of,
            missing_reason=barrier_reason,
        ),
        "financial_quality": _factor_record(
            financial_quality if financial_available else 0.0,
            available=financial_available,
            source_refs=financial_refs,
            as_of=as_of,
            missing_reason=None if financial_available else "A1_FINANCIAL_INDICATORS_MISSING",
        ),
        "cash_flow_quality": _factor_record(
            cashflow_score,
            available=cashflow_available,
            source_refs=financial_refs,
            as_of=as_of,
            missing_reason=None if cashflow_available else "A1_CASH_FLOW_QUALITY_MISSING",
        ),
        "evidence_quality": _factor_record(
            data_quality if evidence_available else 0.0,
            available=evidence_available,
            source_refs=evidence_refs,
            as_of=as_of,
            missing_reason=None if evidence_available else "A1_EVIDENCE_SOURCE_MISSING",
        ),
        "catalyst_confirmation": _factor_record(
            catalyst_score,
            available=catalyst_available,
            source_refs=catalyst_refs,
            as_of=as_of,
            missing_reason=catalyst_reason,
        ),
        "valuation_expectation_gap": _factor_record(
            valuation_score,
            available=valuation_available,
            source_refs=valuation_refs,
            as_of=as_of,
            missing_reason=valuation_reason,
        ),
    }


def _factor_record(
    score: float,
    *,
    available: bool,
    source_refs: Sequence[str],
    as_of: str | None,
    missing_reason: str | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "score": round(max(0.0, min(100.0, float(score))), 4),
        "available": bool(available),
        "source_refs": list(dict.fromkeys(str(value) for value in source_refs if str(value))),
        "as_of": as_of,
    }
    if missing_reason:
        result["missing_reason"] = missing_reason
    return result


def _a1_breakdown(
    weights: Mapping[str, Any],
    factors: Mapping[str, Mapping[str, Any]],
) -> dict[str, float]:
    """Project each configured weight to its own factor score.

    Unknown configured factors are explicit zeroes.  They are never replaced by
    a cross-factor average, which would turn missing evidence into a positive
    signal.
    """

    resolved = {
        str(key): _number(value)
        for key, value in (weights or _A1_DEFAULT_WEIGHTS).items()
        if _number(value) is not None and _number(value) > 0
    }
    result: dict[str, float] = {}
    for key in resolved:
        canonical = _canonical_a1_factor_name(key)
        factor = factors.get(canonical)
        value = _number(factor.get("score")) if isinstance(factor, Mapping) else None
        result[key] = round(max(0.0, min(100.0, value or 0.0)), 4)
    return result


def _canonical_a1_factor_name(value: str) -> str:
    normalized = _normalize(value)
    if "structural" in normalized or "theme" in normalized:
        return "structural_theme"
    if "business" in normalized or "mapping" in normalized:
        return "business_mapping"
    if "barrier" in normalized or "bottleneck" in normalized:
        return "barrier_and_bottleneck"
    if "catalyst" in normalized:
        return "catalyst_confirmation"
    if "valuation" in normalized or "expectationgap" in normalized:
        return "valuation_expectation_gap"
    if "cashflow" in normalized:
        return "cash_flow_quality"
    if "evidence" in normalized or "dataquality" in normalized:
        return "evidence_quality"
    if "financial" in normalized or "profit" in normalized:
        return "financial_quality"
    return normalized


def _a1_available_weight(
    factors: Mapping[str, Mapping[str, Any]],
    weights: Mapping[str, Any],
) -> float:
    resolved = {
        str(key): _number(value)
        for key, value in (weights or _A1_DEFAULT_WEIGHTS).items()
        if _number(value) is not None and _number(value) > 0
    }
    total = sum(value for value in resolved.values() if value is not None)
    if total <= 0:
        return 0.0
    available = sum(
        float(weight)
        for key, weight in resolved.items()
        if isinstance(factors.get(_canonical_a1_factor_name(key)), Mapping)
        and factors[_canonical_a1_factor_name(key)].get("available") is True
    )
    return available / total


def _resolve_a1_weights(value: Mapping[str, Any]) -> dict[str, float]:
    parsed = {
        str(key): float(number)
        for key, raw in value.items()
        if (number := _number(raw)) is not None and number > 0
    }
    if not parsed or sum(parsed.values()) <= 0:
        return dict(_A1_DEFAULT_WEIGHTS)
    return parsed


def _snapshot_as_of(snapshot: Mapping[str, Any]) -> str | None:
    manifest = snapshot.get("snapshot_manifest")
    if not isinstance(manifest, Mapping):
        return None
    value = manifest.get("as_of")
    return str(value) if value else None


def _factor_source_refs(value: Any) -> list[str]:
    refs: list[str] = []

    def visit(raw: Any) -> None:
        if isinstance(raw, str):
            text = raw.strip()
            if text:
                refs.append(text)
            return
        if isinstance(raw, Mapping):
            for key in ("source_ref", "source_url", "fact_id", "announcement_id"):
                visit(raw.get(key))
            for key in ("source_refs", "supporting_source_refs"):
                values = raw.get(key)
                if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
                    for item in values:
                        visit(item)
            return
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            for item in raw:
                visit(item)

    visit(value)
    return list(dict.fromkeys(refs))


def _source_refs_from_values(*values: Any) -> list[str]:
    refs: list[str] = []
    for value in values:
        refs.extend(_factor_source_refs(value))
    return list(dict.fromkeys(refs))


def _source_refs_from_mapping(value: Any, symbol: str) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    return _source_refs_from_values(value.get(symbol), value.get("by_symbol", {}).get(symbol) if isinstance(value.get("by_symbol"), Mapping) else None)


def _barrier_factor(node: Mapping[str, Any]) -> tuple[float, bool, list[str], str]:
    status = str(node.get("bottleneck_status") or "").strip().upper()
    barrier_type = str(node.get("barrier_type") or "").strip().upper()
    classes = node.get("bottleneck_evidence_classes")
    classes = classes if isinstance(classes, Sequence) and not isinstance(classes, (str, bytes, bytearray)) else []
    refs = _source_refs_from_values(node)
    if status == "CONFIRMED" and barrier_type and classes and refs:
        score = min(100.0, 65.0 + 10.0 * min(3, len(classes)))
        return score, True, refs, ""
    if status == "PARTIAL" and (barrier_type or classes) and refs:
        return 50.0, True, refs, ""
    if not node:
        return 0.0, False, [], "A1_BARRIER_EVIDENCE_MISSING"
    if not refs:
        return 0.0, False, [], "A1_BARRIER_SOURCE_REF_MISSING"
    return 0.0, False, refs, "A1_BARRIER_STATUS_UNPROVEN"


def _symbol_records(value: Any, symbol: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    by_symbol = value.get("by_symbol")
    if isinstance(by_symbol, Mapping):
        records = by_symbol.get(symbol, ())
    else:
        records = value.get(symbol, ())
    if isinstance(records, Mapping):
        records = records.get("records", ())
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        return []
    return [item for item in records if isinstance(item, Mapping)]


def _catalyst_factor(value: Any, symbol: str) -> tuple[float, bool, list[str], str]:
    records = _symbol_records(value, symbol)
    if not records:
        return 0.0, False, [], "A1_CATALYST_CONFIRMATION_MISSING"
    strong_terms = (
        "订单", "招标", "合同", "产能", "投产", "客户认证", "认证", "业绩", "业绩预告", "业绩快报",
        "盈利预期", "guidance", "order", "tender", "capacity", "certification", "customer",
        "contract", "ramp", "earnings",
    )
    strong: list[Mapping[str, Any]] = []
    for record in records:
        if record.get("prompt_injection_suspected") is True:
            continue
        tags = " ".join(str(item) for item in record.get("event_tags", ()) if item) if isinstance(record.get("event_tags"), list) else ""
        text = " ".join(
            str(record.get(key) or "")
            for key in ("announcement_title", "event_type", "summary", "title", "event_tags")
        )
        if any(term.casefold() in (tags + " " + text).casefold() for term in strong_terms):
            strong.append(record)
    refs = _source_refs_from_values(strong)
    if not strong or not refs:
        return 0.0, False, refs, "A1_CATALYST_CONFIRMATION_NOT_FOUND"
    return min(100.0, 60.0 + 10.0 * min(4, len(strong))), True, refs, ""


def _valuation_factor(
    fundamentals: Any,
    symbol: str,
    consensus: Any,
) -> tuple[float, bool, list[str], str]:
    payload = fundamentals.get(symbol) if isinstance(fundamentals, Mapping) else None
    indicator_map = _indicator_map(payload)
    consensus_payload = consensus.get(symbol) if isinstance(consensus, Mapping) else None
    if isinstance(consensus_payload, Mapping):
        indicator_map.update(_indicator_map(consensus_payload))
    refs = _source_refs_from_values(payload, consensus_payload)
    percentile = _pick_number(indicator_map, "valuationpercentile", "pepercentile", "pbpercentile", "估值分位")
    if percentile is not None:
        normalized = percentile * 100.0 if 0.0 <= percentile <= 1.0 else percentile
        return max(0.0, min(100.0, 100.0 - normalized)), True, refs, ""
    peg = _pick_number(indicator_map, "peg", "市盈增长比")
    if peg is not None and peg >= 0:
        return max(0.0, min(100.0, 100.0 - peg * 40.0)), True, refs, ""
    pe = _pick_number(indicator_map, "pettm", "pe", "priceearningsratio", "市盈率")
    expected_growth = _pick_number(
        indicator_map,
        "expectedgrowth", "profitgrowth", "netprofitgrowth", "earningsgrowth", "一致预期净利润增速",
    )
    if pe is not None and pe > 0:
        if expected_growth is not None:
            growth = abs(expected_growth)
            return max(0.0, min(100.0, 100.0 - pe / max(1.0, growth) * 2.0)), True, refs, ""
        return max(0.0, min(100.0, 100.0 - pe)), True, refs, ""
    return 0.0, False, refs, "A1_VALUATION_DATA_MISSING"


def _indicator_map(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    raw = value.get("indicators")
    if isinstance(raw, Mapping):
        rows = ({"index_id": key, "value": item} for key, item in raw.items())
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        rows = iter(raw)
    else:
        rows = iter(())
    result: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = _normalize(row.get("index_id") or row.get("name"))
        number = _number(row.get("value"))
        if key and number is not None:
            result[key] = number
    return result


def _pick_number(values: Mapping[str, float], *aliases: str) -> float | None:
    for alias in aliases:
        key = _normalize(alias)
        if key in values:
            return values[key]
        for actual, number in values.items():
            if key and key in actual:
                return number
    return None


def _weighted_score(breakdown: Mapping[str, float], weights: Mapping[str, Any]) -> float:
    parsed = {key: _number(weights.get(key)) for key in breakdown}
    total_weight = sum(value for value in parsed.values() if value is not None and value > 0)
    if total_weight <= 0:
        return 0.0
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


def _safe_float(value: Any) -> float:
    number = _number(value)
    return number if number is not None else 0.0


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
    "A2_FACTOR_COVERAGE_MINIMUM",
    "A2_THEME_FACTORS",
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
