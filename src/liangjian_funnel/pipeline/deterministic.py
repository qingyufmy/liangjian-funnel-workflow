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
from .a3_strategy import Eligibility, evaluate_a3_strategy


PIPELINE_MODE = "deterministic_v2"
FEATURE_VERSION = "deterministic-features/2.2.0"
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
    "weekly_confirmation",
    "agent_1_quality",
)
A2_FACTOR_COVERAGE_MINIMUM = 0.65
_A2_CRITICAL_FACTORS: tuple[str, ...] = (
    "breadth",
    "turnover_share",
    "leader_structure",
    "tier_structure",
    "index_chain_resonance",
)
_A2_CRITICAL_MIN_AVAILABLE = 2
# MARKET_CORE is primarily a theme/emotion plus role decision. These are the
# minimum local market-structure facts used to prove a live, liquid direction.
# Other feeds remain valuable enrichments and are surfaced as degradation when
# unavailable instead of becoming an implicit veto.
_A2_MARKET_FACTORS: tuple[str, ...] = (
    "breadth",
    "turnover_share",
    "leader_structure",
)
_A2_MARKET_OPTIONAL_FACTORS: tuple[str, ...] = (
    "capital_flow",
    "tier_structure",
    "index_chain_resonance",
)
# A2 roles are a single server-owned vocabulary.  These labels are emitted by
# the deterministic identity projection and are all MARKET_CORE sub-roles;
# they must not drift between route eligibility and the strict output policy.
A2_FOCUS_ROLES: frozenset[str] = frozenset({
    "LEADER",
    "CORE_ARMY",
    "TREND_CORE",
    "CHAIN_RESONANCE",
    "FIRST_MOVER",
    "EMOTION_LEADER",
    "TREND_LEADER",
    "INSTITUTIONAL_CORE",
    "CAPACITY_CORE",
})
_A2_ROUTE_DATA_GAP_REASONS: frozenset[str] = frozenset({
    "A1_THEME_MISSING",
    "A1_CHAIN_NODE_MISSING",
    "A1_BUSINESS_EVIDENCE_MISSING",
    "A2_MARKET_FACTS_INSUFFICIENT",
    "A2_FACTOR_COVERAGE_BELOW_MINIMUM",
    "A2_SUPPLY_CHAIN_ROLE_NOT_FOCUS_ELIGIBLE",
    "A2_SCARCE_LAYER_MISSING",
    "A2_VALUE_CHAIN_POSITION_MISSING",
    "A2_BOTTLENECK_FACTORS_INVALID",
    "A2_BOTTLENECK_SCORECARD_MISSING",
    "A2_BOTTLENECK_EVIDENCE_INSUFFICIENT",
    "A2_BOTTLENECK_STRONG_EVIDENCE_MISSING",
    "A2_BOTTLENECK_MISSING_PROOF_UNDECLARED",
    "A2_BOTTLENECK_KILL_SWITCH_MISSING",
    "A2_SCARCITY_AUTHORIZATION_INVALID",
})


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
        result: dict[str, Any] = {
            "stage": self.stage,
            "pipeline_mode": PIPELINE_MODE,
            "evaluated_count": len(self.decisions),
            "sent_to_llm_count": len(self.review_symbols),
            "monitor_count": len(self.monitor_symbols),
            "rejected_count": len(self.rejected_symbols),
            "status_counts": dict(sorted(counts.items())),
        }
        if self.stage == "A2_LOCAL_ROLE":
            total = len(self.decisions)
            coverage: dict[str, float] = {}
            for name in _A2_CRITICAL_FACTORS:
                observed = sum(
                    isinstance(decision.get("a2_factor_scores"), Mapping)
                    and isinstance(decision["a2_factor_scores"].get(name), Mapping)
                    and decision["a2_factor_scores"][name].get("available") is True
                    for decision in self.decisions
                )
                coverage[name] = round(observed / total, 6) if total else 0.0
            route_counts: dict[str, int] = {}
            route_coverage: dict[str, float] = {}
            for route in (MARKET_CORE_ROUTE, SUPPLY_CHAIN_ALPHA_ROUTE):
                observed = sum(
                    isinstance(decision.get("route_eligibility"), Mapping)
                    and isinstance(decision["route_eligibility"].get(route), Mapping)
                    and decision["route_eligibility"][route].get("eligible") is True
                    for decision in self.decisions
                )
                route_counts[route] = observed
                route_coverage[route] = round(observed / total, 6) if total else 0.0
            route_ready = any(route_counts.values())
            has_data_gap = counts.get("DATA_GAP", 0) > 0 or any(
                _decision_has_route_data_gap(decision)
                for decision in self.decisions
            )
            has_degraded = any(
                str(decision.get("data_sufficiency_state") or "").upper() == "DEGRADED"
                for decision in self.decisions
            )
            if not total or not route_ready:
                # A completely unavailable route is an evidence gap only when
                # rows explicitly carry missing facts.  Low identity and
                # other deterministic exclusions are not silently relabelled
                # as a market-wide data failure.
                sufficiency_state = "INSUFFICIENT" if has_data_gap or not total else "SUFFICIENT"
            else:
                sufficiency_state = "DEGRADED" if has_data_gap or has_degraded else "SUFFICIENT"
            result.update({
                "data_gap_count": counts.get("DATA_GAP", 0),
                "critical_factor_coverage": coverage,
                "minimum_critical_factor_coverage": 0.90,
                "data_sufficiency_state": sufficiency_state,
                "route_sufficiency": {
                    route: {
                        "eligible_count": route_counts[route],
                        "coverage": route_coverage[route],
                        "available": route_counts[route] > 0,
                    }
                    for route in (MARKET_CORE_ROUTE, SUPPLY_CHAIN_ALPHA_ROUTE)
                },
            })
        return result


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
    review_all_eligible: bool = False,
) -> DeterministicGateResult:
    """Build the A2 market-core and supply-chain review routes locally.

    A2 is intentionally a scorer, not an LLM-sized second full-market scan.
    Every row comes from A1 ``active_research_pool`` and carries its A1 theme,
    chain node and business evidence forward.  Scores are calculated from the
    configured semantic factors.  A missing capital-flow source removes that
    dimension from the denominator; turnover is never used as a capital-flow
    substitute. ``review_all_eligible`` is the production wide-entry switch:
    it keeps every route-eligible candidate in the model review set while
    preserving ``theme_rank`` for audit and downstream ordering. The default
    remains ``False`` so legacy callers retain the historical Top-N behavior.
    """

    if llm_top_n_per_theme < 1:
        raise ValueError("A2 Top-N value must be positive")

    rows = _mapping_list(a1_output.get("active_research_pool"))
    candidates = _candidate_map(snapshot)
    # A2 is fed by its own materialized feature contract.  The legacy
    # FACTOR_SNAPSHOT is the technical/A3 projection and is only a fallback
    # for older replay fixtures; never let it shadow a symbol-scoped A2 row.
    factors = snapshot.get("A2_FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else snapshot.get("FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else {}
    tier_snapshot = snapshot.get("TIER_STRUCTURE_SNAPSHOT")
    tier_snapshot = tier_snapshot if isinstance(tier_snapshot, Mapping) else {}
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
        factor = _symbol_scoped_row(factors, symbol)
        # Older snapshots may carry only the technical symbol map.  Keep this
        # fallback symbol-scoped as well; never consume a theme-level value as
        # an individual stock factor.
        technical_factor = _symbol_scoped_row(snapshot.get("FACTOR_SNAPSHOT"), symbol)
        if not factor:
            factor = technical_factor
        factor, taxonomy_binding = _bind_a2_factor_to_a1_lineage(
            snapshot,
            a1_output,
            item,
            symbol,
            factor,
        )
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
            tier_snapshot=tier_snapshot,
            relative=relative,
            liquidity=liquidity,
            cycle_score=cycle_score,
            attention=symbol in attention,
            dragon=symbol in dragon,
            local_market_factors=local_market_factors.get(symbol, {}),
        )
        score, coverage = _available_weighted_score(factor_scores, weights)
        critical_coverage = _critical_factor_coverage(factor_scores)
        identifiability, identity_breakdown = _a2_identifiability(
            item=item,
            relative=relative,
            liquidity=liquidity,
            factor_scores=factor_scores,
            attention=symbol in attention,
            dragon=symbol in dragon,
        )
        role = _specialize_market_role(
            _role(identifiability, liquidity, relative),
            factor_scores,
        )
        route_results = _a2_route_results(
            item=item,
            identifiability=identifiability,
            minimum_identifiability_score=minimum_identifiability_score,
            factor_scores=factor_scores,
            bottleneck_context=bottleneck_context,
            role=role,
            coverage=coverage,
            enforce_coverage=enforce_coverage,
            coverage_minimum=coverage_minimum,
            weights=weights,
        )
        eligible_routes = tuple(
            route for route, route_result in route_results.items()
            if route_result.get("eligible") is True
        )
        # A route that was not selected is not evidence against the route that
        # was selected.  In particular, MARKET_CORE must remain usable when
        # the optional SUPPLY_CHAIN_ALPHA scorecard is unavailable.  Only when
        # no route is eligible do the route failures form a data-gap reason
        # set for this symbol.
        route_data_gap_reasons = _selected_route_data_gap_reasons(
            route_results,
            eligible_routes,
        )
        has_route = bool(eligible_routes)
        reasons: list[str] = []
        low_identity = identifiability < minimum_identifiability_score
        if low_identity:
            reasons.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
        market_result = route_results[MARKET_CORE_ROUTE]
        core_route_data_gap_reasons = set()
        if not eligible_routes or MARKET_CORE_ROUTE in eligible_routes:
            core_route_data_gap_reasons = {
                str(reason)
                for reason in market_result.get("missing_reason_codes", ())
                if str(reason) in _A2_ROUTE_DATA_GAP_REASONS
            }
        route_coverage_below_minimum = (
            (not eligible_routes or MARKET_CORE_ROUTE in eligible_routes)
            and "A2_FACTOR_COVERAGE_BELOW_MINIMUM" in market_result.get("missing_reason_codes", ())
        )
        if route_coverage_below_minimum:
            reasons.append("A2_FACTOR_COVERAGE_BELOW_MINIMUM")
        capital_flow = factor_scores.get("capital_flow", {})
        if capital_flow.get("available") is not True:
            reasons.append("A2_CAPITAL_FLOW_UNAVAILABLE")
        missing_optional_factors = [
            name for name in _A2_MARKET_OPTIONAL_FACTORS
            if not (
                isinstance(factor_scores.get(name), Mapping)
                and factor_scores[name].get("available") is True
                and _number(factor_scores[name].get("score")) is not None
            )
        ]
        if missing_optional_factors and has_route:
            reasons.append("A2_OPTIONAL_FACTS_DEGRADED")
        if route_data_gap_reasons and not has_route:
            reasons.extend(("A2_CRITICAL_DATA_INSUFFICIENT", "A2_DATA_GAP"))
            reasons.extend(sorted(route_data_gap_reasons))
        if not has_route:
            reasons.append("A2_NO_ROUTE_READY")
        data_sufficiency_state = (
            "DEGRADED" if missing_optional_factors or route_coverage_below_minimum or route_data_gap_reasons
            else "SUFFICIENT"
        )
        status = "REVIEW_CANDIDATE"
        if low_identity and not core_route_data_gap_reasons:
            status = "HARD_REJECT"
            reasons.append("A2_LOW_IDENTITY_EXCLUDED")
        elif not has_route and route_data_gap_reasons:
            # A missing required fact is not negative evidence.  Preserve the
            # symbol in an explicit data-gap partition instead of rejecting it
            # or claiming the market contains no opportunity.
            status = "DATA_GAP"
            data_sufficiency_state = "INSUFFICIENT"
        elif low_identity:
            status = "HARD_REJECT"
            reasons.append("A2_LOW_IDENTITY_EXCLUDED")
        elif route_coverage_below_minimum:
            status = "LOCAL_MONITOR"
        elif not has_route:
            status = "LOCAL_MONITOR"
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
            "role": role,
            "route": eligible_routes[0] if eligible_routes else None,
            "eligible_routes": eligible_routes,
            "route_eligibility": route_results,
            "role_breakdown": {
                "relative_strength": round(relative, 4),
                "liquidity_capacity": round(liquidity, 4),
                "monthly_cycle_rotation": round(cycle_score, 4) if cycle_score is not None else None,
                "bottleneck_evidence_readiness": _number(bottleneck_context.get("evidence_readiness_score")),
                "relative_strength_source": "FACTOR_SNAPSHOT" if explicit_relative is not None else "RECENT_DAILY_BARS",
                "identifiability": identity_breakdown,
            },
            "a2_factor_scores": factor_scores,
            "a2_taxonomy_binding": taxonomy_binding,
            "factor_coverage": coverage,
            "critical_factor_coverage": critical_coverage,
            "data_sufficiency_state": data_sufficiency_state,
            "missing_optional_factors": missing_optional_factors,
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
            if not review_all_eligible and rank > llm_top_n_per_theme:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A2_NOT_SENT_TO_LLM")
            else:
                item["sent_to_llm"] = True
    decisions.sort(key=lambda item: str(item["symbol"]))
    return DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=tuple(decisions),
        review_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
        monitor_symbols=tuple(
            str(item["symbol"])
            for item in decisions
            if item["status"] in {"LOCAL_MONITOR", "DATA_GAP"}
        ),
        rejected_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "HARD_REJECT"),
    )


def _bind_a2_factor_to_a1_lineage(
    snapshot: Mapping[str, Any],
    a1_output: Mapping[str, Any],
    item: Mapping[str, Any],
    symbol: str,
    factor: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bind taxonomy aggregates to the A1 theme/node selected for a stock.

    ``A2_FACTOR_SNAPSHOT`` contains every industry/concept membership and its
    historical strongest group for general exploration.  Once A1 has selected
    a concrete industry-chain node, A2 must not borrow a stronger unrelated
    concept.  This projection replaces only taxonomy-derived factors; stock
    flow, ladder and leader facts remain symbol-scoped.
    """

    node_id = str(item.get("industry_chain_node") or item.get("node_id") or "").strip()
    if not node_id:
        return dict(factor), {"status": "UNBOUND_LEGACY", "reason_code": "A1_CHAIN_NODE_MISSING"}
    allowed: set[str] = set()
    for link in _mapping_list(a1_output.get("taxonomy_links")):
        if str(link.get("node_id") or "").strip() != node_id:
            continue
        taxonomy = str(link.get("taxonomy") or "").strip().upper()
        code = str(link.get("taxonomy_code") or "").strip().upper()
        if taxonomy in {"INDUSTRY", "CONCEPT"} and code:
            allowed.add(f"{taxonomy}:{code}")
        for key, label in (("industry_thscodes", "INDUSTRY"), ("concept_thscodes", "CONCEPT")):
            values = link.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
                allowed.update(f"{label}:{str(value).strip().upper()}" for value in values if str(value).strip())
    if not allowed:
        return dict(factor), {"status": "UNBOUND_LEGACY", "reason_code": "A1_TAXONOMY_LINK_MISSING"}

    memberships: set[str] = set()
    for key, taxonomy in (("THS_INDUSTRY_MEMBERSHIP", "INDUSTRY"), ("THS_CONCEPT_MEMBERSHIP", "CONCEPT")):
        for membership in _membership_map(snapshot.get(key), taxonomy=taxonomy).get(symbol, ()):
            code = str(membership.get("taxonomy_code") or "").strip().upper()
            if code:
                memberships.add(f"{taxonomy}:{code}")
    matched = sorted(allowed.intersection(memberships))
    metrics_contract = snapshot.get("A2_THEME_METRICS")
    metrics = metrics_contract.get("theme_metrics") if isinstance(metrics_contract, Mapping) else None
    metrics = metrics if isinstance(metrics, Mapping) else {}
    rows = [
        metrics[key]
        for key in matched
        if isinstance(metrics.get(key), Mapping) and metrics[key].get("available") is True
    ]
    best = max(rows, key=lambda row: float(_number(row.get("score")) or 0.0), default=None)
    result = dict(factor)
    raw_factors = result.get("factors")
    bound_factors = dict(raw_factors) if isinstance(raw_factors, Mapping) else {}
    if best is None:
        for name in ("breadth", "turnover_share", "index_chain_resonance", "weekly_confirmation"):
            bound_factors[name] = _factor_result(
                None,
                "A1_BOUND_TAXONOMY_AGGREGATE",
                (),
                "A2_A1_TAXONOMY_METRIC_MISSING",
            )
        result["factors"] = bound_factors
        result.update(bound_factors)
        return result, {
            "status": "MISSING",
            "reason_code": "A2_A1_TAXONOMY_METRIC_MISSING",
            "node_id": node_id,
            "allowed_taxonomies": sorted(allowed),
            "matched_taxonomies": matched,
        }

    taxonomy_code = str(best.get("taxonomy_code") or "")
    source_refs = tuple(str(value) for value in best.get("source_refs", ()) if str(value))
    values = {
        "breadth": (_number(best.get("breadth")) * 100.0 if _number(best.get("breadth")) is not None else None),
        "turnover_share": (_number(best.get("turnover_share")) * 100.0 if _number(best.get("turnover_share")) is not None else None),
        "index_chain_resonance": _number(best.get("score")),
        "weekly_confirmation": _number(best.get("weekly_confirmation_score")),
    }
    for name, value in values.items():
        row = _factor_result(
            value,
            "A1_BOUND_TAXONOMY_AGGREGATE",
            source_refs,
            "OK" if value is not None else "A2_A1_TAXONOMY_METRIC_MISSING",
        )
        row.update({
            "taxonomy": best.get("taxonomy"),
            "taxonomy_code": taxonomy_code,
            "taxonomy_name": best.get("taxonomy_name"),
            "a1_node_id": node_id,
        })
        bound_factors[name] = row
    result["factors"] = bound_factors
    result.update(bound_factors)
    return result, {
        "status": "BOUND",
        "reason_code": "OK",
        "node_id": node_id,
        "taxonomy": best.get("taxonomy"),
        "taxonomy_code": taxonomy_code,
        "taxonomy_name": best.get("taxonomy_name"),
        "matched_taxonomies": matched,
    }


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
    tier_snapshot: Mapping[str, Any],
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
        "weekly_confirmation": ("weekly_confirmation_score", "weekly_score", "weekly_rotation_score"),
        "agent_1_quality": ("agent_1_quality_score", "a1_quality_score", "data_quality_score"),
    }
    for name in A2_THEME_FACTORS:
        if name == "capital_flow" and not _capital_flow_available(snapshot):
            # A model/A1 row cannot authorize a capital-flow score.  The source
            # availability flag is the sole authority for this dimension.
            value = _capital_flow_unavailable(snapshot)
        else:
            value = _read_item_factor(item, name, aliases[name])
            if value is None and name == "tier_structure":
                # TIER_STRUCTURE_SNAPSHOT is the authoritative point-in-time
                # ladder projection.  It is deliberately read before the
                # broader A2 bundle so an old/technical row cannot overwrite
                # it.
                value = _read_snapshot_factor(tier_snapshot, symbol, theme_id, name, aliases[name])
            if value is None and _is_a2_factor_row(factor):
                value = _read_factor_row(
                    factor,
                    name,
                    aliases[name],
                    source="A2_FACTOR_SNAPSHOT",
                    source_refs=_payload_source_refs(factor),
                )
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


def _is_a2_factor_row(value: Mapping[str, Any]) -> bool:
    return isinstance(value, Mapping) and any(
        key in value
        for key in (
            "factors",
            "factor_scores",
            "tier_structure",
            "leader_structure",
            "trend_strength_proxy",
            "index_chain_resonance",
        )
    )


def _read_factor_row(
    row: Mapping[str, Any],
    name: str,
    aliases: Sequence[str],
    *,
    source: str,
    source_refs: Sequence[str],
) -> dict[str, Any] | None:
    if "score" in row and not any(key in row for key in ("factors", "factor_scores", "metrics")):
        result = _read_metric_payload(
            row,
            source=source,
            source_refs=source_refs,
            ratio_hint=name in {"breadth", "turnover_share"},
        )
        if result is not None:
            return result
    result = _read_metric_fields(row, aliases, source=source, source_refs=source_refs, ratio_hint=name in {"breadth", "turnover_share"})
    if result is not None:
        return result
    for container_name in ("factors", "factor_scores", "metrics"):
        container = row.get(container_name)
        if not isinstance(container, Mapping):
            continue
        result = _read_metric_payload(
            container.get(name),
            source=source,
            source_refs=source_refs,
            ratio_hint=name in {"breadth", "turnover_share"},
        )
        if result is not None:
            return result
        result = _read_metric_fields(
            container,
            aliases,
            source=source,
            source_refs=source_refs,
            ratio_hint=name in {"breadth", "turnover_share"},
        )
        if result is not None:
            return result
    result = _read_metric_payload(
        row.get(name),
        source=source,
        source_refs=source_refs,
        ratio_hint=name in {"breadth", "turnover_share"},
    )
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

        a2_factor_row = _symbol_scoped_row(snapshot.get("A2_FACTOR_SNAPSHOT"), symbol)
        technical_factor_row = _symbol_scoped_row(snapshot.get("FACTOR_SNAPSHOT"), symbol)
        relative = _relative_strength_score(a2_factor_row, default=None)
        if relative is None:
            relative = _relative_strength_score(technical_factor_row, default=None)
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
    role: str | None = None,
    coverage: Mapping[str, Any] | None = None,
    enforce_coverage: bool = False,
    coverage_minimum: float = A2_FACTOR_COVERAGE_MINIMUM,
    weights: Mapping[str, float] | None = None,
) -> tuple[str, ...]:
    results = _a2_route_results(
        item=item,
        identifiability=identifiability,
        minimum_identifiability_score=minimum_identifiability_score,
        factor_scores=factor_scores,
        bottleneck_context=bottleneck_context,
        role=role,
        coverage=coverage,
        enforce_coverage=enforce_coverage,
        coverage_minimum=coverage_minimum,
        weights=weights,
    )
    return tuple(
        route
        for route, result in results.items()
        if result.get("eligible") is True
    )


def _a2_route_results(
    *,
    item: Mapping[str, Any],
    identifiability: float,
    minimum_identifiability_score: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    bottleneck_context: Mapping[str, Any],
    role: str | None = None,
    coverage: Mapping[str, Any] | None = None,
    enforce_coverage: bool = False,
    coverage_minimum: float = A2_FACTOR_COVERAGE_MINIMUM,
    weights: Mapping[str, float] | None = None,
) -> dict[str, dict[str, Any]]:
    route_coverage = coverage or {"ratio": _factor_coverage_ratio(factor_scores)}
    market = _market_core_route_result(
        item,
        identifiability,
        minimum_identifiability_score,
        factor_scores,
        route_coverage,
        enforce_coverage,
        coverage_minimum,
        market_role=role,
        weights=weights,
    )
    supply = _supply_chain_route_result(item, bottleneck_context)
    return {
        MARKET_CORE_ROUTE: market,
        SUPPLY_CHAIN_ALPHA_ROUTE: supply,
    }


def _market_core_route_result(
    item: Mapping[str, Any],
    identifiability: float,
    minimum_identifiability_score: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    coverage: Mapping[str, Any],
    enforce_coverage: bool,
    coverage_minimum: float,
    *,
    market_role: str | None = None,
    weights: Mapping[str, float] | None = None,
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
    # ``market_role`` is derived by the deterministic identity scorer when
    # the A1 row does not carry a role.  It is descriptive output, not an
    # upstream assertion.  Treating the derived LOW_IDENTITY label as an
    # explicit route veto made otherwise valid rows fail closed whenever a
    # test/configuration intentionally lowered the identity threshold.  Only
    # a role explicitly supplied by the upstream row is a route contract.
    explicit_role = str(
        item.get("market_role")
        or item.get("role")
        or item.get("a2_role")
        or ""
    ).strip().upper()
    if explicit_role and explicit_role not in A2_FOCUS_ROLES:
        missing.append("A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE")
    market_factor_names = _A2_MARKET_FACTORS
    market_fact_count = sum(
        isinstance(factor_scores.get(name), Mapping)
        and factor_scores.get(name, {}).get("available") is True
        and _number(factor_scores.get(name, {}).get("score")) is not None
        for name in market_factor_names
    )
    # MARKET_CORE coverage is deliberately scoped to the hard three market
    # facts. The global weighted denominator contains additional optional
    # families, but an unavailable capital-flow/ladder/resonance feed must not
    # veto an otherwise valid theme/emotion + role candidate. Missing hard
    # market evidence still remains a deterministic data gap.
    # The route contract is a fact-count contract (at least two of the hard
    # three), not a weighted-score contract.  Using configured theme weights
    # here would let one unusually large optional weight make two observed
    # hard facts look insufficient.  Keep the ratio in the audit payload so
    # callers can still see the exact hard-fact coverage.
    route_ratio = (
        market_fact_count / len(_A2_MARKET_FACTORS)
        if _A2_MARKET_FACTORS
        else 0.0
    )
    if enforce_coverage and route_ratio < coverage_minimum:
        missing.append("A2_FACTOR_COVERAGE_BELOW_MINIMUM")
    if market_fact_count < 2:
        missing.append("A2_MARKET_FACTS_INSUFFICIENT")
    sufficiency = "SUFFICIENT" if not missing else "DATA_GAP"
    missing_optional = [
        name for name in _A2_MARKET_OPTIONAL_FACTORS
        if not (
            isinstance(factor_scores.get(name), Mapping)
            and factor_scores[name].get("available") is True
            and _number(factor_scores[name].get("score")) is not None
        )
    ]
    if not missing and missing_optional:
        sufficiency = "DEGRADED"
    return {
        "eligible": not missing,
        "route": MARKET_CORE_ROUTE,
        "missing_reason_codes": list(dict.fromkeys(missing)),
        "missing_optional_factors": missing_optional,
        "data_sufficiency_state": sufficiency,
        "bottleneck_status": "NOT_REQUIRED_FOR_MARKET_CORE",
        "market_fact_count": market_fact_count,
        "route_coverage_ratio": round(route_ratio, 6),
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
        "missing_optional_factors": [],
        "data_sufficiency_state": "SUFFICIENT" if not missing and scorecard is not None else "DATA_GAP",
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


def _critical_factor_coverage(
    factor_scores: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    states = {
        name: bool(
            isinstance(factor_scores.get(name), Mapping)
            and factor_scores[name].get("available") is True
            and _number(factor_scores[name].get("score")) is not None
        )
        for name in _A2_CRITICAL_FACTORS
    }
    available = sum(states.values())
    total = len(_A2_CRITICAL_FACTORS)
    return {
        "candidate_factors": list(_A2_CRITICAL_FACTORS),
        "available_factors": [name for name, observed in states.items() if observed],
        "missing_factors": [name for name, observed in states.items() if not observed],
        "available_count": available,
        "required_count": _A2_CRITICAL_MIN_AVAILABLE,
        "candidate_count": total,
        "ratio": round(available / total, 6) if total else 0.0,
        "sufficient": available >= _A2_CRITICAL_MIN_AVAILABLE,
    }


def _route_required_coverage(
    factor_scores: Mapping[str, Mapping[str, Any]],
    coverage: Mapping[str, Any],
    *,
    optional: Sequence[str],
    weights: Mapping[str, float] | None,
    required_names: Sequence[str] | None = None,
) -> float:
    """Return coverage for a route after removing explicitly optional facts.

    ``coverage`` is retained for legacy snapshots, but it cannot identify the
    weight of one missing factor.  When the frozen weights are available,
    recompute the route denominator.  Otherwise fall back to the aggregate
    ratio, except that an otherwise complete route is not penalised solely by
    an unavailable optional source.
    """

    optional_names = {str(name) for name in optional}
    scoped_names = {
        str(name)
        for name in (required_names or factor_scores.keys())
        if str(name) not in optional_names
    }
    if isinstance(weights, Mapping) and weights:
        required = {
            str(name): _number(value)
            for name, value in weights.items()
            if str(name) in scoped_names and (_number(value) or 0.0) > 0
        }
        # A configured weight file may omit a newly introduced route factor;
        # retain that factor in the route denominator at an equal weight rather
        # than silently making the route easier to satisfy.
        for name in scoped_names:
            if name not in required:
                required[name] = 1.0
        total = sum(float(value or 0.0) for value in required.values())
        available = sum(
            float(value or 0.0)
            for name, value in required.items()
            if isinstance(factor_scores.get(name), Mapping)
            and factor_scores[name].get("available") is True
            and _number(factor_scores[name].get("score")) is not None
        )
        if total > 0:
            return available / total
    if scoped_names:
        observed = sum(
            isinstance(factor_scores.get(name), Mapping)
            and factor_scores[name].get("available") is True
            and _number(factor_scores[name].get("score")) is not None
            for name in scoped_names
        )
        return observed / len(scoped_names)
    ratio = _safe_float(coverage.get("ratio"))
    if ratio < 1.0:
        missing_required = [
            name for name, value in factor_scores.items()
            if name in scoped_names
            and not (
                isinstance(value, Mapping)
                and value.get("available") is True
                and _number(value.get("score")) is not None
            )
        ]
        if not missing_required:
            return 1.0
    return ratio


def _decision_has_route_data_gap(decision: Mapping[str, Any]) -> bool:
    routes = decision.get("route_eligibility")
    if not isinstance(routes, Mapping):
        return str(decision.get("status") or "") == "DATA_GAP"
    eligible = tuple(
        str(route).strip().upper()
        for route in (decision.get("eligible_routes") or ())
        if str(route).strip()
    ) if isinstance(decision.get("eligible_routes"), Sequence) and not isinstance(
        decision.get("eligible_routes"), (str, bytes, bytearray)
    ) else ()
    return bool(_selected_route_data_gap_reasons(routes, eligible))


def _selected_route_data_gap_reasons(
    route_results: Mapping[str, Any],
    eligible_routes: Sequence[str] = (),
) -> set[str]:
    """Return gaps for the actually selected A2 route only.

    MARKET_CORE is the normal route.  SUPPLY_CHAIN_ALPHA is an independent
    optional route and its missing scorecard/evidence is not negative evidence
    for a MARKET_CORE candidate.  If no route is eligible, retaining all route
    gap codes is useful because the symbol genuinely has no usable path.
    """

    selected = {
        str(route).strip().upper()
        for route in eligible_routes
        if str(route).strip()
    }
    reasons: set[str] = set()
    selected_results = [
        route_results.get(route)
        for route in selected
    ] if selected else list(route_results.values())
    for result in selected_results:
        if not isinstance(result, Mapping):
            continue
        reasons.update(
            str(reason)
            for reason in result.get("missing_reason_codes", ())
            if str(reason) in _A2_ROUTE_DATA_GAP_REASONS
        )
    return reasons


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


def _symbol_scoped_row(value: Any, symbol: str) -> Mapping[str, Any]:
    """Return only the row belonging to ``symbol`` from a snapshot map."""

    payloads = _scoped_payloads(value, symbol, "", include_theme=False)
    return payloads[0] if payloads else {}


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
        "weekly_confirmation": ("A2_FACTOR_SNAPSHOT", "A2_THEME_METRICS", "SECTOR_CYCLE_SNAPSHOT"),
        "agent_1_quality": ("A2_FACTOR_SNAPSHOT",),
    }
    symbol_only = name in {"capital_flow", "tier_structure", "leader_structure", "agent_1_quality"}
    for source_name in source_names.get(name, ("A2_FACTOR_SNAPSHOT",)):
        root = snapshot.get(source_name)
        if name == "capital_flow" and (not isinstance(root, Mapping) or root.get("available") is not True):
            continue
        if name == "tier_structure" and (not isinstance(root, Mapping) or root.get("available") is False):
            continue
        for payload in _scoped_payloads(root, symbol, theme_id, include_theme=not symbol_only):
            if symbol_only and "score" in payload:
                result = _read_metric_payload(
                    payload,
                    source=source_name,
                    source_refs=_payload_source_refs(payload),
                    ratio_hint=name in {"breadth", "turnover_share"},
                )
                if result is not None:
                    return result
            result = _read_metric_fields(payload, aliases, source=source_name, source_refs=_payload_source_refs(payload), ratio_hint=name in {"breadth", "turnover_share"})
            if result is not None:
                return result
            # A factor may be represented as a nested object keyed by its
            # semantic name rather than flattened into the record.
            nested = payload.get(name) if isinstance(payload, Mapping) else None
            result = _read_metric_payload(nested, source=source_name, source_refs=_payload_source_refs(payload), ratio_hint=name in {"breadth", "turnover_share"})
            if result is not None:
                return result
            for container_name in ("factors", "factor_scores", "metrics"):
                container = payload.get(container_name) if isinstance(payload, Mapping) else None
                if not isinstance(container, Mapping):
                    continue
                result = _read_metric_payload(
                    container.get(name),
                    source=source_name,
                    source_refs=_payload_source_refs(payload),
                    ratio_hint=name in {"breadth", "turnover_share"},
                )
                if result is not None:
                    return result
    return None


def _capital_flow_unavailable(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    source = snapshot.get("CAPITAL_FLOW_SNAPSHOT")
    reason = "SOURCE_NOT_CONFIGURED"
    if isinstance(source, Mapping):
        reason = str(source.get("reason_code") or "SOURCE_UNAVAILABLE")
    board_source = snapshot.get("BOARD_CAPITAL_FLOW_SNAPSHOT")
    if isinstance(board_source, Mapping) and board_source.get("reason_code"):
        reason = f"SYMBOL:{reason};SECTOR:{board_source.get('reason_code')}"
    return _factor_result(
        None,
        "CAPITAL_FLOW_SNAPSHOT",
        (),
        reason,
        reason_code_override="A2_CAPITAL_FLOW_UNAVAILABLE",
    )


def _capital_flow_available(snapshot: Mapping[str, Any]) -> bool:
    source = snapshot.get("CAPITAL_FLOW_SNAPSHOT")
    if isinstance(source, Mapping) and source.get("available") is True:
        return True
    board_source = snapshot.get("BOARD_CAPITAL_FLOW_SNAPSHOT")
    return isinstance(board_source, Mapping) and board_source.get("available") is True


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
        resolved_source = str(raw.get("source") or source)
        if raw.get("available") is False:
            return _with_factor_metadata(
                _factor_result(
                    None,
                    resolved_source,
                    _payload_source_refs(raw) or source_refs,
                    str(raw.get("reason_code") or "A2_FACTOR_UNAVAILABLE"),
                ),
                raw,
            )
        available = raw.get("available") is not False
        for key in ("score", "normalized_score", "percentile", "value", "raw_value"):
            if key in raw:
                number = _number(raw.get(key))
                if number is not None:
                    ratio = ratio_hint or key in {"percentile", "ratio"}
                    result = _factor_result(number * 100.0 if ratio and 0.0 <= number <= 1.0 else number, resolved_source, _payload_source_refs(raw) or source_refs, "OK")
                    return _with_factor_metadata(result, raw)
        return _with_factor_metadata(
            _factor_result(None, resolved_source, _payload_source_refs(raw) or source_refs, "A2_FACTOR_VALUE_MISSING"),
            raw,
        )
    number = _number(raw)
    if number is None:
        return None
    if ratio_hint and 0.0 <= number <= 1.0:
        number *= 100.0
    if number < 0.0 or number > 100.0:
        return _factor_result(None, source, source_refs, "A2_FACTOR_VALUE_INVALID")
    return _factor_result(number, source, source_refs, "OK") if available else _factor_result(None, source, source_refs, "A2_FACTOR_UNAVAILABLE")


def _with_factor_metadata(result: dict[str, Any], raw: Mapping[str, Any]) -> dict[str, Any]:
    """Retain bounded deterministic context needed to explain one factor."""

    for key in (
        "weekly_momentum_state",
        "taxonomy_code",
        "breadth_5d",
        "relative_strength_mean_5d",
        "availability_state",
        "ladder_height",
        "tier",
        "sector_ladder_support",
        "sector_ladder_groups",
        "role",
        "tier_confirmation_mode",
    ):
        if key in raw:
            result[key] = raw.get(key)
    return result


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


def _scoped_payloads(
    value: Any,
    symbol: str,
    theme_id: str,
    *,
    include_theme: bool = True,
) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    result: list[Mapping[str, Any]] = []

    seen: set[int] = set()

    def add_mapping(mapping: Any) -> None:
        if isinstance(mapping, Mapping):
            identity = id(mapping)
            if identity in seen:
                return
            seen.add(identity)
            result.append(mapping)

    def add_keyed(mapping: Mapping[Any, Any], key: str) -> None:
        add_mapping(mapping.get(key))
        # Providers occasionally return bare six-digit keys.  Normalising the
        # key here keeps the lookup symbol-scoped without weakening source
        # availability semantics.
        for actual, payload in mapping.items():
            if _symbol(actual) == symbol:
                add_mapping(payload)

    add_keyed(value, symbol)
    if include_theme:
        add_mapping(value.get(theme_id))
    for key in ("by_symbol", "symbols", "by_theme", "themes", "theme_metrics", "metrics", "records", "items", "payload", "data"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            add_keyed(nested, symbol)
            if include_theme:
                add_mapping(nested.get(theme_id))
        elif isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
            for row in nested:
                if not isinstance(row, Mapping):
                    continue
                row_symbol = _symbol(row.get("symbol") or row.get("thscode"))
                row_theme = str(row.get("theme_id") or row.get("primary_theme") or "")
                row_industry = str(row.get("industry_thscode") or row.get("industry_code") or "")
                if row_symbol == symbol or (include_theme and (row_theme == theme_id or row_industry == theme_id)):
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
    """Route A2 candidates by explicit daily strategy conditions.

    A3 has no weighted technical score.  Only a ``QUALIFIED`` deterministic
    route is sent to the veto-only model review; WATCH/REJECTED/DATA_GAP rows
    remain visible without entering the executable plan path.
    """

    rows = _mapping_list(a2_output.get("focus_pool"))
    if snapshot.get("DETERMINISTIC_RESEARCH_V2_ENABLED") is not True:
        # Historical/legacy replay fixtures predate the A3 strategy contract.
        # Keep their model-review scope intact, but do not invent a strategy,
        # eligibility or executable plan.  Production deterministic_v2 always
        # takes the explicit route below.
        legacy = tuple({
            "symbol": symbol,
            "stage": "A3_LEGACY_MODEL_REVIEW",
            "status": "REVIEW_CANDIDATE",
            "sent_to_llm": True,
            "reason_codes": ["LEGACY_A3_STRATEGY_CONTEXT_UNAVAILABLE"],
        } for item in rows if (symbol := _symbol(item.get("symbol"))))
        return DeterministicGateResult(
            stage="A3_LEGACY_MODEL_REVIEW",
            decisions=legacy,
            review_symbols=tuple(str(item["symbol"]) for item in legacy),
        )
    factors = snapshot.get("FACTOR_SNAPSHOT")
    factors = factors if isinstance(factors, Mapping) else {}
    levels = snapshot.get("PRICE_LEVELS")
    levels = levels if isinstance(levels, Mapping) else {}
    tradability = snapshot.get("TRADABILITY_FLAGS")
    tradability = tradability if isinstance(tradability, Mapping) else {}
    kline_patterns = snapshot.get("KLINE_PATTERNS")
    kline_patterns = kline_patterns if isinstance(kline_patterns, Mapping) else {}
    a2_context = snapshot.get("A2_BOTTLENECK_CONTEXT")
    a2_context = a2_context if isinstance(a2_context, Mapping) else {}
    raw_regime = snapshot.get("MARKET_REGIME_SNAPSHOT")
    market_regime = (
        str(raw_regime.get("regime") or raw_regime.get("state") or raw_regime.get("market_regime") or "")
        if isinstance(raw_regime, Mapping)
        else str(raw_regime or "")
    )
    raw_permissions = snapshot.get("SECTOR_PERMISSIONS")
    permissions = raw_permissions if isinstance(raw_permissions, Mapping) else {}
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
        kline = kline_patterns.get(symbol)
        kline = kline if isinstance(kline, Mapping) else {}
        context = a2_context.get(symbol)
        context = context if isinstance(context, Mapping) else {}
        theme = str(item.get("theme_id") or item.get("primary_theme") or "")
        raw_permission = permissions.get(symbol) or permissions.get(theme)
        sector_permission = (
            str(raw_permission.get("permission") or raw_permission.get("state") or "")
            if isinstance(raw_permission, Mapping)
            else str(raw_permission or "")
        )
        strategy = evaluate_a3_strategy(
            item,
            factor=factor,
            price_levels=price_level,
            tradability=flags,
            kline=kline,
            a2_context=context,
            market_regime=market_regime or None,
            sector_permission=sector_permission or None,
        ).model_dump(mode="json")
        eligibility = str(strategy["eligibility"])
        minimum_reward_risk = _number(snapshot.get("MIN_REWARD_RISK")) or 2.0
        maximum_stop_distance = _number(snapshot.get("MAX_STOP_DISTANCE")) or 0.06
        reward_risk = _number(price_level.get("reward_risk"))
        stop_distance = _number(price_level.get("stop_distance_pct"))
        risk_reasons: list[str] = []
        if eligibility == Eligibility.QUALIFIED.value:
            if reward_risk is None or reward_risk < minimum_reward_risk:
                risk_reasons.append("A3_REWARD_RISK_BELOW_MINIMUM")
            if stop_distance is None or stop_distance <= 0 or stop_distance > maximum_stop_distance:
                risk_reasons.append("A3_STOP_DISTANCE_OUTSIDE_LIMIT")
        if risk_reasons:
            eligibility = Eligibility.REJECTED.value
            strategy["eligibility"] = eligibility
            strategy["strategy_profile"] = "NO_NEXT_DAY_PLAN"
            strategy["reason_codes"] = list(dict.fromkeys([
                *strategy.get("reason_codes", []),
                *risk_reasons,
            ]))
            strategy["veto_conditions"] = list(dict.fromkeys([
                *strategy.get("veto_conditions", []),
                *risk_reasons,
            ]))
        status = {
            Eligibility.QUALIFIED.value: "REVIEW_CANDIDATE",
            Eligibility.WATCH.value: "LOCAL_MONITOR",
            Eligibility.DATA_GAP.value: "DATA_GAP",
            Eligibility.REJECTED.value: "HARD_REJECT",
        }[eligibility]
        decisions.append({
            **strategy,
            "symbol": symbol,
            "name": item.get("company_name") or item.get("name"),
            "candidate_origin": item.get("candidate_origin") or "FOCUS",
            "stage": "A3_LOCAL_TECHNICAL",
            "status": status,
            "reward_risk": reward_risk,
            "stop_distance_pct": stop_distance,
            "minimum_reward_risk": minimum_reward_risk,
            "maximum_stop_distance_pct": maximum_stop_distance,
            "price_levels_hash": content_hash(price_level) if price_level else None,
            "factor_snapshot_hash": content_hash(factor) if factor else None,
            "theme_id": item.get("theme_id"),
            "node_id": item.get("industry_chain_node"),
            "sent_to_llm": status == "REVIEW_CANDIDATE",
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        })
    decisions.sort(key=lambda item: str(item["symbol"]))
    return DeterministicGateResult(
        stage="A3_LOCAL_TECHNICAL",
        decisions=tuple(decisions),
        review_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
        monitor_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] == "LOCAL_MONITOR"),
        rejected_symbols=tuple(str(item["symbol"]) for item in decisions if item["status"] in {"HARD_REJECT", "DATA_GAP"}),
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
    if not isinstance(factor, Mapping):
        return default
    candidates: list[Mapping[str, Any]] = [factor]
    for key in ("technical_summary", "trend_strength_proxy", "leader_structure", "factors", "factor_scores"):
        nested = factor.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
            if key in {"factors", "factor_scores"}:
                candidates.extend(
                    value for value in nested.values()
                    if isinstance(value, Mapping)
                )
    for summary in candidates:
        for key in ("relative_strength_score", "relative_strength", "rs_score", "relative_strength_percentile"):
            value = _number(summary.get(key))
            if value is not None:
                # Percentile fields are commonly persisted as a 0..1 ratio.
                if "percentile" in key and 0.0 <= value <= 1.0:
                    value *= 100.0
                return max(0.0, min(100.0, value))
        frames = summary.get("timeframes")
        if isinstance(frames, Mapping):
            daily = frames.get("daily")
            if isinstance(daily, Mapping):
                alignment = str(daily.get("ma_alignment") or "")
                return {"BULL_STACK": 90.0, "BULL_PARTIAL": 72.0, "ENTANGLED": 50.0, "BEAR_PARTIAL": 30.0, "BEAR_STACK": 10.0}.get(alignment, 45.0)
        score = _number(summary.get("score"))
        source = str(summary.get("source") or "")
        if score is not None and any(token in source.lower() for token in ("trend", "relative", "daily_bars")):
            return max(0.0, min(100.0, score))
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


def _specialize_market_role(
    role: str,
    factor_scores: Mapping[str, Mapping[str, Any]],
) -> str:
    """Separate an emotion leader from a cross-sectional trend leader.

    ``LEADER`` used to mean only "high identifiability plus high relative
    strength" in A2, while A3 interpreted the same token as a confirmed
    limit-up/ladder leader.  That vocabulary collision sent every strong trend
    stock through the intraday-leader route and then failed it for a missing
    board height.  Only a source-observed two-plus board is an emotion leader;
    a strong stock without that fact remains a trend leader.
    """

    if role != "LEADER":
        return role
    tier = factor_scores.get("tier_structure")
    tier = tier if isinstance(tier, Mapping) else {}
    height = _number(tier.get("ladder_height"))
    if tier.get("available") is True and height is not None and height >= 2:
        return "EMOTION_LEADER"
    # OBSERVED_ABSENT is real data, not a ladder data gap.  SOURCE_FAILED and
    # other unknown states remain visible in the tier factor itself, but they
    # cannot manufacture an emotion-leader identity.
    return "TREND_LEADER"


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
