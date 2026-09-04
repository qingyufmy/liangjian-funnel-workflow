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
from datetime import datetime, timezone
from typing import Any

from .bottleneck import (
    MARKET_CORE_ROUTE,
    SUPPLY_CHAIN_ALPHA_ROUTE,
    canonicalize_model_scorecard,
    deterministic_bottleneck_context,
)
from .business_exposure import extract_business_exposure_facts
from .feature_store import content_hash
from .a1_selection_logic import FUNDAMENTAL, build_a1_selection_evidence
from .a2_role_logic import UNRESOLVED as A2_BEHAVIOR_UNRESOLVED, classify_a2_stock
from .a3_strategy import Eligibility, evaluate_a3_strategy


PIPELINE_MODE = "deterministic_v2"
FEATURE_VERSION = "deterministic-features/2.3.0"
_A1_DEFAULT_WEIGHTS: dict[str, float] = {
    "structural_theme": 0.20,
    "business_mapping": 0.20,
    "barrier_and_bottleneck": 0.15,
    "financial_quality": 0.20,
    "catalyst_confirmation": 0.15,
    "valuation_expectation_gap": 0.10,
}
_A1_SELECTION_BASES: tuple[str, ...] = (
    "LLM_REVIEWED",
    "DETERMINISTIC_SCORE",
    "QUOTA_FILL",
    "BROKER_GOLD_DIRECT",
    "FUNDAMENTAL_BASELINE",
    "HALF_YEAR_FUNDAMENTAL",
)
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
# These are the named A2 decisions that the runtime can expose to operators.
# The list is deliberately broader than the currently implemented route gate:
# an unavailable/non-implemented check is recorded as ``available=False`` (or
# ``applied=False``) instead of being silently treated as a pass or a veto.
_A2_ATTRIBUTION_GATES: tuple[str, ...] = (
    "LOCAL_DATA_SUFFICIENCY",
    "LOCAL_ELIGIBILITY",
    "THEME_SCORE_MIN",
    "IDENTIFIABILITY_MIN",
    "BEHAVIOR_TYPE_RESOLVED",
    "LEADER_MIN_CRITERIA",
    "MAX_LEADERS_PER_THEME",
    "TIER_STRUCTURE",
    "ROUTE_REQUIREMENT",
    "SENT_TO_LLM",
    "FREE_FLOAT_CAP",
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
        if self.stage == "A1_LOCAL_SCREEN":
            # This is intentionally scoped to the research layer that can be
            # handed downstream.  Monitor/rejected rows still carry a
            # selection_basis on their individual decision records, but
            # including them here would make this aggregate look like an
            # ACTIVE-source mix and would not reconcile with the A1 pool count.
            active_statuses = {"LOCAL_ACTIVE_CANDIDATE", "REVIEW_CANDIDATE"}
            basis_counts = {basis: 0 for basis in _A1_SELECTION_BASES}
            for decision in self.decisions:
                if decision.get("status") not in active_statuses:
                    continue
                basis = str(decision.get("selection_basis") or "")
                if basis in basis_counts:
                    basis_counts[basis] += 1
            result["selection_basis_counts"] = basis_counts
            result["selection_basis_total"] = sum(basis_counts.values())
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
            surviving_scope = tuple(
                decision
                for decision in self.decisions
                if str(decision.get("status") or "").upper() != "HARD_REJECT"
            )
            has_data_gap = counts.get("DATA_GAP", 0) > 0 or any(
                _decision_has_route_data_gap(decision)
                for decision in surviving_scope
            )
            has_degraded = any(
                str(decision.get("data_sufficiency_state") or "").upper() == "DEGRADED"
                for decision in surviving_scope
            )
            gate_block_counts = {
                gate_name: sum(
                    isinstance(decision.get("gate_results"), Mapping)
                    and isinstance(decision["gate_results"].get(gate_name), Mapping)
                    and decision["gate_results"][gate_name].get("available") is True
                    and decision["gate_results"][gate_name].get("passed") is False
                    and decision["gate_results"][gate_name].get("blocks_decision") is True
                    for decision in self.decisions
                )
                for gate_name in _A2_ATTRIBUTION_GATES
            }
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
                "gate_block_counts": gate_block_counts,
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
    hard_risk_events = _hard_risk_events(snapshot.get("RISK_EVENTS"))
    risk_symbols = set(hard_risk_events)
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
    monthly_chain_only = targets.get("monthly_chain_only") is True
    active_target = targets.get("active_research_target")
    if isinstance(active_target, Sequence) and not isinstance(active_target, (str, bytes, bytearray)):
        target_values = [int(value) for value in active_target[:2] if _number(value) is not None]
    else:
        target_values = []
    active_target_min = max(1, target_values[0] if target_values else 200)
    active_target_max = max(active_target_min, target_values[1] if len(target_values) > 1 else 800)
    # Frozen snapshots created before the flag existed preserve the historical
    # expansion behavior.  New runs receive the explicit versioned setting.
    quota_fill_enabled = targets.get("quota_fill_enabled", True) is True
    # The fundamental baseline is a versioned replacement for the old quota
    # expansion path.  Older frozen snapshots do not have this contract, so
    # the safe compatibility default is disabled; current production snapshots
    # receive the explicit mapping from ``funnel_config_v2.yaml``.
    raw_baseline = targets.get("fundamental_baseline")
    baseline_config = raw_baseline if isinstance(raw_baseline, Mapping) else {}
    baseline_enabled = baseline_config.get("enabled") is True
    baseline_minimum_quality = _number(
        baseline_config.get("minimum_data_quality", minimum_quality)
    )
    if baseline_minimum_quality is None or baseline_minimum_quality < 0:
        baseline_minimum_quality = minimum_quality
    baseline_minimum_financial_quality = _number(
        baseline_config.get("minimum_financial_quality", 60.0)
    )
    if baseline_minimum_financial_quality is None or baseline_minimum_financial_quality < 0:
        baseline_minimum_financial_quality = 60.0
    baseline_minimum_liquidity = _number(
        baseline_config.get("minimum_liquidity_score", 50.0)
    )
    if baseline_minimum_liquidity is None or baseline_minimum_liquidity < 0:
        baseline_minimum_liquidity = 50.0
    baseline_maximum_per_industry = _number(
        baseline_config.get("maximum_per_industry", 12)
    )
    if baseline_maximum_per_industry is None or baseline_maximum_per_industry < 1:
        baseline_maximum_per_industry = 12.0
    institutional_contract = snapshot.get("BROKER_GOLD_COVERAGE_POOL")
    institutional_contract = institutional_contract if isinstance(institutional_contract, Mapping) else {}
    institutional_symbols = institutional_contract.get("symbols")
    institutional_symbols = institutional_symbols if isinstance(institutional_symbols, Mapping) else {}
    market_regime = snapshot.get("MARKET_REGIME_SNAPSHOT")
    market_regime = market_regime if isinstance(market_regime, Mapping) else {}
    pullback_evidence = snapshot.get("A1_PULLBACK_EVIDENCE")
    pullback_evidence = pullback_evidence if isinstance(pullback_evidence, Mapping) else {}
    minimum_financial_coverage = _number(minimums.get("minimum_financial_coverage", 0.60))
    if minimum_financial_coverage is None or not 0 < minimum_financial_coverage <= 1:
        minimum_financial_coverage = 0.60
    minimum_financial_quality = _number(minimums.get("minimum_financial_quality", 60.0))
    if minimum_financial_quality is None or minimum_financial_quality < 0:
        minimum_financial_quality = 60.0

    decisions: list[dict[str, Any]] = []
    provisional: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_hashes = _source_hashes(snapshot)
    snapshot_as_of = _snapshot_as_of(snapshot)
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
        half_year_support = _latest_half_year_support(
            fundamental,
            as_of=snapshot_as_of,
        )
        maximum_exposure = max(
            (_number(item.get("revenue_exposure_pct")) or 0.0 for item in exposure_facts),
            default=0.0,
        )
        primary_link = matched[0] if matched else {}
        primary_theme = theme_by_id.get(str(primary_link.get("theme_id") or ""), {}) if matched else {}
        primary_node = node_by_id.get(str(primary_link.get("node_id") or ""), {}) if matched else {}
        monthly_direction_matches = [
            {
                "monthly_direction_id": link.get("theme_id"),
                "monthly_direction_name": (
                    theme_by_id.get(str(link.get("theme_id") or ""), {}).get("display_name")
                    or link.get("theme_id")
                ),
                "industry_chain_node_id": link.get("node_id"),
                "industry_chain_node_name": (
                    node_by_id.get(str(link.get("node_id") or ""), {}).get("display_name")
                    or link.get("node_id")
                ),
                "sector_index_taxonomy": link.get("taxonomy"),
                "sector_index_code": link.get("taxonomy_code"),
                "sector_index_name": link.get("taxonomy_name"),
                "match_method": link.get("match_method"),
                "confidence": link.get("confidence"),
            }
            for link in matched
        ]
        a1_selection_evidence = build_a1_selection_evidence(
            market_regime=market_regime,
            company={
                "industry": candidate.get("industry") or candidate.get("industry_name"),
                "sector": primary_theme.get("display_name") or primary_theme.get("theme_id"),
                "theme": primary_node.get("display_name") or primary_node.get("node_id"),
                "style": candidate.get("style"),
            },
            pullback=(
                pullback_evidence.get(symbol)
                if isinstance(pullback_evidence.get(symbol), Mapping)
                else None
            ),
            financial_metrics=financial_details,
            required_financial_metrics=(
                "revenue_growth",
                "profit_growth",
                "cashflow_quality",
                "roe",
                "debt_ratio",
            ),
        )
        financial_coverage = _number(
            a1_selection_evidence.get("financial_quality", {}).get("coverage_ratio")
            if isinstance(a1_selection_evidence.get("financial_quality"), Mapping)
            else None
        ) or 0.0
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
        institutional = institutional_symbols.get(symbol)
        institutional = institutional if isinstance(institutional, Mapping) else {}
        institutional_seed = bool(institutional)
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
        if (
            isinstance(a1_selection_evidence.get("pullback"), Mapping)
            and a1_selection_evidence["pullback"].get("classification") == FUNDAMENTAL
        ):
            hard_reject = True
            reason_codes.append("A1_FUNDAMENTAL_PULLBACK_HARD_REJECT")
        if hard_reject:
            status = "HARD_REJECT"
        elif not matched:
            status = "OUTSIDE_THEME"
            reason_codes.append("A1_OUTSIDE_DISCOVERED_THEME")
        elif not raw_evidence_available:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_MAIN_BUSINESS_EVIDENCE_MISSING")
        elif monthly_chain_only and half_year_support.get("supported") is True:
            # A1 is a monthly research universe, not an executable buy list.
            # A constituent whose disclosed half-year revenue and attributable
            # profit are both positive and growing already has the auditable
            # fundamental support requested by the strategy.  It must not be
            # discarded merely because optional six-factor enrichments are
            # sparse or because the revenue-composition parser could not turn
            # a valid CNINFO page into an exact percentage.
            status = "LOCAL_ACTIVE_CANDIDATE"
            reason_codes.append("A1_HALF_YEAR_FUNDAMENTAL_CONFIRMED")
        elif not core_reports or not indicators_available:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_FUNDAMENTAL_DATA_INCOMPLETE")
        elif monthly_chain_only and financial_coverage < minimum_financial_coverage:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_FINANCIAL_COVERAGE_BELOW_MINIMUM")
        elif monthly_chain_only and financial_quality < minimum_financial_quality:
            status = "LOCAL_MONITOR"
            reason_codes.append("A1_FINANCIAL_QUALITY_BELOW_MINIMUM")
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
        autonomous_status = status
        if institutional_seed and not monthly_chain_only:
            # A current-month broker-gold row is a first-class A1 research
            # route once it is inside G0.  The local risk/tradability result is
            # retained in ``autonomous_status`` and reason codes; it only
            # controls whether the row can flow into the trading stages.
            if status == "OUTSIDE_THEME":
                reason_codes.append("A1_INSTITUTIONAL_THEME_MAPPING_REQUIRED")
            status = "LOCAL_ACTIVE_CANDIDATE"
            reason_codes.append("A1_INSTITUTIONAL_DIRECT_ENTRY")
        if matched and not hard_reject and available_weight < minimum_available_weight:
            # Preserve every independent data-gap reason even when an earlier
            # fail-closed branch (for example missing business evidence) has
            # already selected LOCAL_MONITOR.
            reason_codes.append("A1_FACTOR_COVERAGE_BELOW_MINIMUM")
        if raw_evidence_available and not structured_exposure_available:
            reason_codes.append("A1_BUSINESS_EXPOSURE_UNSTRUCTURED")
        if monthly_chain_only and financial_quality < minimum_financial_quality:
            reason_codes.append("A1_FINANCIAL_QUALITY_BELOW_MINIMUM")
        if monthly_chain_only and financial_coverage < minimum_financial_coverage:
            reason_codes.append("A1_FINANCIAL_COVERAGE_BELOW_MINIMUM")
        if institutional_seed:
            reason_codes.append("A1_INSTITUTIONAL_COVERAGE_SEED")
        if financial_coverage < minimum_financial_coverage:
            # The existing A1 factor-coverage gate remains authoritative.
            # Subfactor coverage is preserved as a degradation signal instead
            # of introducing a second overlapping veto that would shrink the
            # research pool before the archetype-specific profiles are fully
            # populated by the data layer.
            reason_codes.append("A1_FINANCIAL_SUBFACTOR_COVERAGE_DEGRADED")
        reason_codes.extend(
            str(code)
            for code in a1_selection_evidence.get("reason_codes", ())
            if str(code)
        )

        decision = {
            "decision_id": content_hash({
                "stage": "A1_LOCAL_SCREEN",
                "symbol": symbol,
                "as_of": snapshot_as_of,
                "feature_version": FEATURE_VERSION,
                "source_hashes": source_hashes,
            })[:24],
            "symbol": symbol,
            "name": str(candidate.get("name") or candidate.get("security_name") or "") or None,
            "stage": "A1_LOCAL_SCREEN",
            "status": status,
            # Every deterministic row has an explicit provenance.  The value
            # is changed below only when the row is actually sent to the LLM
            # or promoted by the temporary coverage mechanism.
            "selection_basis": (
                "BROKER_GOLD_DIRECT"
                if institutional_seed and not monthly_chain_only
                else "HALF_YEAR_FUNDAMENTAL"
                if monthly_chain_only and half_year_support.get("supported") is True and bool(matched) and raw_evidence_available and not hard_reject
                else "DETERMINISTIC_SCORE"
            ),
            "score": round(score, 4),
            "data_quality_score": round(data_quality, 4),
            "financial_quality_score": round(financial_quality, 4),
            "liquidity_score": round(liquidity_score, 4),
            "theme_id": primary_link.get("theme_id"),
            "node_id": primary_link.get("node_id"),
            "taxonomy_matches": matched,
            "monthly_direction_id": primary_link.get("theme_id"),
            "monthly_direction_name": primary_theme.get("display_name") or primary_link.get("theme_id"),
            "monthly_direction_matches": monthly_direction_matches,
            "sector_index_taxonomy": primary_link.get("taxonomy"),
            "sector_index_code": primary_link.get("taxonomy_code"),
            "sector_index_name": primary_link.get("taxonomy_name"),
            "sector_constituent_confirmed": bool(matched),
            "theme_source_refs": list(theme_by_id.get(str(primary_link.get("theme_id") or ""), {}).get("source_refs") or ()),
            "node_source_refs": list(node_by_id.get(str(primary_link.get("node_id") or ""), {}).get("source_refs") or ()),
            "source_refs": _source_refs_from_values(
                institutional if institutional_seed else None,
                evidence if raw_evidence_available else None,
                factor_details,
            ),
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
            "fundamental_support": {
                "score": round(financial_quality, 4),
                "minimum_score": round(minimum_financial_quality, 4),
                "supported": financial_quality >= minimum_financial_quality,
                "coverage_ratio": round(financial_coverage, 6),
                "features": financial_details,
                "latest_half_year": half_year_support,
            },
            "half_year_support": half_year_support,
            "financial_subfactor_coverage": round(financial_coverage, 6),
            "minimum_financial_subfactor_coverage": round(minimum_financial_coverage, 6),
            "company_archetype": (
                a1_selection_evidence.get("archetype", {}).get("classification")
                if isinstance(a1_selection_evidence.get("archetype"), Mapping)
                else "UNCLASSIFIED"
            ),
            "pullback_cause": (
                a1_selection_evidence.get("pullback", {}).get("classification")
                if isinstance(a1_selection_evidence.get("pullback"), Mapping)
                else "UNKNOWN"
            ),
            "hard_risk_events": hard_risk_events.get(symbol, []),
            "a1_selection_evidence": a1_selection_evidence,
            "business_exposure_facts": exposure_facts,
            "disclosed_business_match": {
                "raw_disclosure_available": raw_evidence_available,
                "structured_match_confirmed": structured_exposure_available,
                "maximum_revenue_exposure_pct": maximum_exposure if exposure_facts else None,
            },
            "maximum_revenue_exposure_pct": maximum_exposure if exposure_facts else None,
            "amount": amount,
            "reason_codes": list(dict.fromkeys(reason_codes)),
            "coverage_origin": "BROKER_GOLD_T2" if institutional_seed else "AUTONOMOUS_RESEARCH",
            "autonomous_status": autonomous_status,
            "institutional_coverage": dict(institutional) if institutional_seed else None,
            "research_route": (
                "BROKER_GOLD_DIRECT"
                if institutional_seed and not monthly_chain_only
                else "HALF_YEAR_FUNDAMENTAL"
                if monthly_chain_only and half_year_support.get("supported") is True and bool(matched) and raw_evidence_available and not hard_reject
                else ("MONTHLY_THEME" if matched else None)
            ),
            "downstream_trade_eligible": not hard_reject,
            "sent_to_llm": False,
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
            "as_of": snapshot_as_of,
        }
        decisions.append(decision)
        if status == "LOCAL_CANDIDATE":
            if monthly_chain_only:
                sector_keys = {
                    f"{str(link.get('taxonomy') or '').strip().upper()}:{str(link.get('taxonomy_code') or '').strip().upper()}"
                    for link in matched
                    if str(link.get("taxonomy") or "").strip() and str(link.get("taxonomy_code") or "").strip()
                }
                for sector_key in sorted(sector_keys):
                    provisional[sector_key].append(decision)
            else:
                provisional[str(decision.get("node_id") or "UNMAPPED")].append(decision)

    # A verified current-month broker-gold row is an A1 research obligation
    # even when it is outside today's G0.  It remains explicitly research-only
    # so A2 blocks it before model review; this widens research coverage without
    # widening the executable universe.
    g0_set = set(symbols)
    for symbol, raw_institutional in institutional_symbols.items():
        normalized = _symbol(symbol)
        if not normalized or normalized in g0_set or not isinstance(raw_institutional, Mapping):
            continue
        decisions.append({
            "symbol": normalized,
            "name": raw_institutional.get("name"),
            "stage": "A1_LOCAL_SCREEN",
            "status": "OUTSIDE_G0" if monthly_chain_only else "LOCAL_ACTIVE_CANDIDATE",
            "selection_basis": "BROKER_GOLD_DIRECT",
            "score": 0.0,
            "data_quality_score": 0.0,
            "financial_quality_score": 0.0,
            "liquidity_score": 0.0,
            "theme_id": None,
            "node_id": None,
            "taxonomy_matches": [],
            "theme_source_refs": [],
            "node_source_refs": [],
            "source_refs": _source_refs_from_values(raw_institutional),
            "score_breakdown": {},
            "factor_details": {},
            "available_weight": 0.0,
            "available_weight_pct": 0.0,
            "minimum_available_weight": round(minimum_available_weight, 6),
            "missing_factors": [],
            "financial_features": {},
            "business_exposure_facts": [],
            "maximum_revenue_exposure_pct": None,
            "amount": 0.0,
            "reason_codes": [
                "A1_INSTITUTIONAL_COVERAGE_SEED",
                "A1_INSTITUTIONAL_DIRECT_ENTRY",
                "A1_INSTITUTIONAL_OUTSIDE_G0",
            ],
            "coverage_origin": "BROKER_GOLD_T2",
            "autonomous_status": "OUTSIDE_G0",
            "institutional_coverage": dict(raw_institutional),
            "research_route": "BROKER_GOLD_DIRECT",
            "downstream_trade_eligible": False,
            "sent_to_llm": False,
            "feature_version": FEATURE_VERSION,
            "source_hashes": source_hashes,
        })

    local_eligible: list[dict[str, Any]] = []
    if monthly_chain_only:
        # A monthly direction may map to multiple THS sector indices.  Rank on
        # each concrete index and retain the union, so one broad theme cannot
        # consume the review budget of another index or hide a strong member.
        for sector_key, sector_decisions in provisional.items():
            sector_decisions.sort(key=lambda item: (
                -float(item["score"]),
                -float(item["financial_quality_score"]),
                -float(item["amount"]),
                str(item["symbol"]),
            ))
            for rank, item in enumerate(sector_decisions, start=1):
                ranks = item.setdefault("sector_index_ranks", {})
                ranks[sector_key] = rank
        for item in decisions:
            if item.get("status") != "LOCAL_CANDIDATE":
                continue
            qualifying = {
                key: int(rank)
                for key, rank in (item.get("sector_index_ranks") or {}).items()
                if int(rank) <= local_top_n_per_node
            }
            if not qualifying:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A1_OUTSIDE_SECTOR_INDEX_TOP_N")
                continue
            item["sector_index_qualifying_ranks"] = qualifying
            if item.get("business_exposure_facts"):
                item["status"] = "LOCAL_ACTIVE_CANDIDATE"
            else:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A1_REQUIRES_LLM_EXPOSURE_REVIEW")
            local_eligible.append(item)

        review_symbols: set[str] = set()
        for sector_key in sorted(provisional):
            values = [
                item for item in local_eligible
                if sector_key in (item.get("sector_index_qualifying_ranks") or {})
                and "A1_REQUIRES_LLM_EXPOSURE_REVIEW" in item.get("reason_codes", ())
            ]
            values.sort(key=lambda item: (
                int((item.get("sector_index_ranks") or {}).get(sector_key, 10**9)),
                -float(item["score"]),
                str(item["symbol"]),
            ))
            review_symbols.update(str(item["symbol"]) for item in values[:llm_top_n_per_theme])
        for item in local_eligible:
            if str(item["symbol"]) not in review_symbols:
                continue
            item["status"] = "REVIEW_CANDIDATE"
            item["selection_basis"] = "LLM_REVIEWED"
            item["sent_to_llm"] = True
            item["reason_codes"] = [
                code for code in item.get("reason_codes", ())
                if code != "A1_REQUIRES_LLM_EXPOSURE_REVIEW"
            ]
    else:
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
                item["selection_basis"] = "LLM_REVIEWED"
                item["sent_to_llm"] = True
                item["reason_codes"] = [
                    code for code in item.get("reason_codes", ())
                    if code != "A1_REQUIRES_LLM_EXPOSURE_REVIEW"
                ]

    # The fundamental baseline is a separate, auditable research route.  It
    # widens A1 beyond the monthly theme allow-list without weakening G0 or
    # silently turning missing evidence into a positive signal.  Only rows
    # which are still outside the active/review partitions can enter it.
    local_active_count = sum(item.get("status") == "LOCAL_ACTIVE_CANDIDATE" for item in decisions)
    if baseline_enabled and not monthly_chain_only and local_active_count < active_target_min:
        baseline_candidates: list[tuple[dict[str, Any], str, str]] = []
        for item in decisions:
            if item.get("status") not in {"LOCAL_MONITOR", "OUTSIDE_THEME"}:
                continue
            if item.get("coverage_origin") == "BROKER_GOLD_T2":
                continue
            if item.get("status") in {"HARD_REJECT", "OUTSIDE_G0"}:
                continue
            symbol = str(item.get("symbol") or "")
            taxonomy_code, taxonomy_name = _baseline_industry_binding(
                symbol,
                industry,
                candidates.get(symbol, {}),
            )
            if not taxonomy_code:
                item["reason_codes"] = list(dict.fromkeys([
                    *[str(code) for code in item.get("reason_codes", ()) if str(code)],
                    "A1_FUNDAMENTAL_BASELINE_INDUSTRY_MISSING",
                ]))
                continue
            if _safe_float(item.get("data_quality_score")) < baseline_minimum_quality:
                continue
            if _safe_float(item.get("financial_quality_score")) < baseline_minimum_financial_quality:
                continue
            if _safe_float(item.get("liquidity_score")) < baseline_minimum_liquidity:
                continue
            rank_score = _fundamental_baseline_score(item)
            baseline_candidates.append((item, taxonomy_code, taxonomy_name))
            item["baseline_rank_score"] = rank_score

        baseline_candidates.sort(
            key=lambda value: (
                -_safe_float(value[0].get("baseline_rank_score")),
                -_safe_float(value[0].get("financial_quality_score")),
                -_safe_float(value[0].get("data_quality_score")),
                -_safe_float(value[0].get("liquidity_score")),
                -_safe_float(value[0].get("amount")),
                str(value[0].get("symbol") or ""),
            )
        )
        industry_counts: dict[str, int] = defaultdict(int)
        for item, taxonomy_code, taxonomy_name in baseline_candidates:
            if local_active_count >= active_target_max or local_active_count >= active_target_min:
                break
            if industry_counts[taxonomy_code] >= int(baseline_maximum_per_industry):
                continue
            if not item.get("taxonomy_matches"):
                item["theme_id"] = f"INDUSTRY:{taxonomy_code}"
                item["node_id"] = f"BASELINE:{taxonomy_code}"
                item["theme_source_refs"] = []
                item["node_source_refs"] = []
                item["taxonomy_matches"] = [{
                    "taxonomy": "INDUSTRY",
                    "taxonomy_code": taxonomy_code,
                    "taxonomy_name": taxonomy_name,
                    "match_method": "FUNDAMENTAL_BASELINE_INDUSTRY",
                    "confidence": 1.0,
                }]
            item["status"] = "LOCAL_ACTIVE_CANDIDATE"
            item["selection_basis"] = "FUNDAMENTAL_BASELINE"
            item["research_route"] = "FUNDAMENTAL_BASELINE"
            item["downstream_trade_eligible"] = True
            item["reason_codes"] = list(dict.fromkeys([
                *[str(code) for code in item.get("reason_codes", ()) if str(code)],
                "A1_FUNDAMENTAL_BASELINE_ENTRY",
            ]))
            industry_counts[taxonomy_code] += 1
            local_active_count += 1

    # Compatibility path for old snapshots without the explicit baseline
    # contract.  New production snapshots use the baseline route above; this
    # path remains bounded and retains the historical provenance label.
    if quota_fill_enabled and not monthly_chain_only and not baseline_enabled and local_active_count < active_target_min:
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
            item["selection_basis"] = "QUOTA_FILL"
            item["reason_codes"] = [
                code for code in item.get("reason_codes", ())
                if code not in {"A1_OUTSIDE_LOCAL_TOP_N", "A1_REQUIRES_LLM_EXPOSURE_REVIEW"}
            ]
            item["reason_codes"].append("A1_ADAPTIVE_COVERAGE_EXPANSION")
            local_active_count += 1

    for item in decisions:
        if (
            item.get("autonomous_status") == "LOCAL_CANDIDATE"
            and item.get("selection_basis") != "BROKER_GOLD_DIRECT"
        ):
            item["autonomous_status"] = item.get("status")

    decisions.sort(key=lambda item: str(item["symbol"]))
    review = tuple(
        str(item["symbol"])
        for item in sorted(
            (item for item in decisions if item["status"] == "REVIEW_CANDIDATE"),
            key=lambda item: (str(item.get("node_id") or ""), int(item.get("node_rank") or 0), str(item["symbol"])),
        )
    )
    monitor = tuple(
        str(item["symbol"])
        for item in decisions
        if item["status"] in {"LOCAL_MONITOR", "OUTSIDE_THEME", "OUTSIDE_G0"}
    )
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
    rotation_theme_count: int = 5,
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

    if llm_top_n_per_theme < 1 or rotation_theme_count < 1:
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
    snapshot_as_of = _snapshot_as_of(snapshot)
    raw_market_emotion = snapshot.get("MARKET_EMOTION_SNAPSHOT")
    market_emotion = raw_market_emotion if isinstance(raw_market_emotion, Mapping) else {}
    raw_market_funding = snapshot.get("MARKET_FUNDING_SNAPSHOT")
    market_funding = raw_market_funding if isinstance(raw_market_funding, Mapping) else {}
    raw_hot100 = snapshot.get("EASTMONEY_HOT100_SNAPSHOT")
    hot100 = raw_hot100 if isinstance(raw_hot100, Mapping) else {}
    hot100_available = hot100.get("available") is True
    hot100_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): dict(row)
        for row in hot100.get("records", ())
        if hot100_available
        and isinstance(row, Mapping)
        and str(row.get("symbol") or "").strip()
    }
    hard_risk_by_symbol = _hard_risk_events(snapshot.get("RISK_EVENTS"))
    selected_board_field_present = "SELECTED_BOARD_SNAPSHOT" in snapshot
    raw_selected_boards = snapshot.get("SELECTED_BOARD_SNAPSHOT")
    selected_boards = raw_selected_boards if isinstance(raw_selected_boards, Mapping) else {}
    selected_board_source_available = (
        selected_board_field_present
        and selected_boards.get("available") is True
    )
    dual_channel_contract = (
        "EASTMONEY_HOT100_SNAPSHOT" in snapshot
        or selected_board_field_present
    )
    selected_board_by_symbol = (
        selected_boards.get("by_symbol")
        if selected_board_source_available
        and isinstance(selected_boards.get("by_symbol"), Mapping)
        else {}
    )
    # The materialized SELECTED_BOARD_SNAPSHOT is the production rotation
    # contract.  Once the field is present, its versioned fixed-theme
    # selection is the *only* trend top-five entry path: A2_THEME_METRICS and
    # sector health remain factor/verification evidence and cannot replace or
    # reorder it.  An explicitly unavailable field therefore fails closed for
    # trend rather than silently substituting a transient taxonomy ranking.
    # Full-market/legacy fallback is retained solely for historical fixtures
    # that predate the field altogether.
    full_market_rotation_available = (
        not selected_board_field_present
        and _a2_full_market_rotation_available(
            snapshot,
            a1_output=a1_output,
        )
    )
    full_market_rotation_directions = (
        _a2_ranked_fallback_directions(
            snapshot,
            a1_output=a1_output,
            limit=None,
        )
        if full_market_rotation_available
        else {}
    )
    compatibility_fallback_directions = (
        _a2_ranked_fallback_directions(
            snapshot,
            a1_output=a1_output,
            limit=rotation_theme_count,
        )
        if not selected_board_field_present
        else {}
    )
    preferred_rotation_directions = (
        full_market_rotation_directions
        if full_market_rotation_available
        else compatibility_fallback_directions
    )
    taxonomy_theme_map = _a2_taxonomy_theme_map(a1_output)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    market_grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    decisions: list[dict[str, Any]] = []
    for item in rows:
        symbol = _symbol(item.get("symbol"))
        if not symbol:
            continue
        # A1 has two deliberately different identities.  A monthly member is
        # eligible for the medium-term/trend channel; a validated Eastmoney
        # top-100 overlay is a daily member for the emotion channel.  The
        # overlay must never be allowed to masquerade as a monthly member, but
        # it is still a real A1 row for the current day's emotion review.
        selection_basis = str(item.get("selection_basis") or "").strip().upper()
        research_route = str(item.get("research_route") or "").strip().upper()
        daily_emotion_overlay = (
            selection_basis == "DAILY_EMOTION_OVERLAY"
            or research_route == "DAILY_EMOTION_OVERLAY"
        )
        # ``active_research_pool`` is the upstream A1 partition.  Preserve
        # monthly identity independently from today's eligibility so a risked
        # monthly row remains auditable without receiving a downstream route.
        monthly_a1_member = not daily_emotion_overlay
        upstream_research_route = str(item.get("research_route") or "").strip().upper()
        upstream_research_only = item.get("downstream_trade_eligible") is False
        item_hard_risk_events = item.get("hard_risk_events")
        item_hard_risk = bool(
            isinstance(item_hard_risk_events, Mapping)
            or (
                isinstance(item_hard_risk_events, Sequence)
                and not isinstance(item_hard_risk_events, (str, bytes, bytearray))
                and any(isinstance(event, Mapping) or str(event).strip() for event in item_hard_risk_events)
            )
        )
        hard_risk_present = bool(hard_risk_by_symbol.get(symbol)) or item_hard_risk
        candidate = candidates.get(symbol, {})
        amount = max(0.0, _number(candidate.get("amount")) or _number(candidate.get("turnover")) or 0.0)
        factor = _symbol_scoped_row(factors, symbol)
        # Older snapshots may carry only the technical symbol map.  Keep this
        # fallback symbol-scoped as well; never consume a theme-level value as
        # an individual stock factor.
        technical_factor = _symbol_scoped_row(snapshot.get("FACTOR_SNAPSHOT"), symbol)
        if not factor:
            factor = technical_factor
        source_theme_id = _a2_rotation_theme_id(item, factor)
        factor, taxonomy_binding = _bind_a2_factor_to_a1_lineage(
            snapshot,
            a1_output,
            item,
            symbol,
            factor,
            preferred_taxonomies=set(preferred_rotation_directions),
        )
        rotation_direction_id = _a2_rotation_direction_id(
            item,
            factor,
            taxonomy_theme_map,
            fallback=source_theme_id,
        )
        if taxonomy_binding.get("status") == "BOUND":
            bound_taxonomy = str(taxonomy_binding.get("taxonomy") or "").strip().upper()
            bound_code = str(taxonomy_binding.get("taxonomy_code") or "").strip().upper()
            if bound_taxonomy and bound_code:
                # Rotation is ranked on the concrete sector index, not on a
                # broad A1 narrative theme or an aggregate of constituent
                # stock scores.
                rotation_direction_id = f"{bound_taxonomy}:{bound_code}"
        a1_rotation_direction_id = rotation_direction_id
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
            theme_id=rotation_direction_id,
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
        legacy_role = _specialize_market_role(
            _role(identifiability, liquidity, relative),
            factor_scores,
        )
        behavior_decision = classify_a2_stock(
            symbol=symbol,
            name=item.get("company_name") or item.get("name") or candidate.get("name"),
            as_of=_snapshot_as_of(snapshot),
            evidence=_a2_behavior_evidence(
                item=item,
                factor_scores=factor_scores,
                identifiability=identifiability,
                minimum_identifiability_score=minimum_identifiability_score,
                relative=relative,
                liquidity=liquidity,
                legacy_role=legacy_role,
                as_of=_snapshot_as_of(snapshot),
            ),
        )
        role = str(behavior_decision.get("market_role") or A2_BEHAVIOR_UNRESOLVED)
        hot100_row = hot100_by_symbol.get(symbol)
        upstream_status = str(item.get("status") or "ACTIVE").strip().upper()
        explicitly_inactive = upstream_status in {"REJECTED", "HARD_REJECT", "INACTIVE", "DISABLED"}
        daily_member_reasons: list[str] = []
        if upstream_research_only:
            daily_member_reasons.append("A2_UPSTREAM_RESEARCH_ONLY")
        if hard_risk_present:
            daily_member_reasons.append("A2_HARD_RISK_PRESENT")
        if explicitly_inactive:
            daily_member_reasons.append("A2_UPSTREAM_A1_ROW_INACTIVE")
        if daily_emotion_overlay:
            if not hot100_available:
                daily_member_reasons.append("A2_EMOTION_HOT100_UNAVAILABLE")
            elif hot100_row is None:
                daily_member_reasons.append("A2_EMOTION_NOT_IN_EASTMONEY_HOT100")
            if item.get("emotion_attention_eligible") is not True:
                daily_member_reasons.append("A2_EMOTION_OVERLAY_NOT_VALIDATED")
        daily_a1_member = not daily_member_reasons
        if daily_a1_member:
            daily_member_reasons.append("A2_DAILY_A1_MEMBER")
        # Keep the legacy field as the monthly identity contract.  New callers
        # must use the explicit fields below instead of inferring today's
        # membership from ``a1_formal_member``.
        formal_a1_member = monthly_a1_member
        selected_board_rows = selected_board_by_symbol.get(symbol, ())
        if not isinstance(selected_board_rows, Sequence) or isinstance(selected_board_rows, (str, bytes, bytearray)):
            selected_board_rows = ()
        selected_board_matches = [
            dict(row)
            for row in selected_board_rows
            if selected_board_source_available
            and isinstance(row, Mapping)
            and row.get("selected_for_rotation") is True
            and str(row.get("board_code") or row.get("theme_id") or "").strip()
            and 1 <= (_number(row.get("primary_rank")) or 0) <= rotation_theme_count
            and (_number(row.get("main_net_inflow_cny")) or 0.0) > 0
        ]
        selected_board_match = min(
            selected_board_matches,
            key=lambda row: (
                _number(row.get("primary_rank")) or 10**9,
                -(_number(row.get("strength")) or 0.0),
                -(_number(row.get("main_net_inflow_cny")) or 0.0),
                str(row.get("board_code") or row.get("theme_id") or ""),
            ),
            default=None,
        )
        rotation_fallback = (
            _a2_rotation_fallback_evidence(
                snapshot,
                taxonomy_binding=taxonomy_binding,
                ranked_directions=compatibility_fallback_directions,
            )
            if not selected_board_field_present and not full_market_rotation_available
            else None
        )
        full_market_rotation_match = None
        if full_market_rotation_available:
            # A symbol can belong to several A1-linked concrete taxonomies.
            # Choose the best *ranked* full-market direction, not merely the
            # strongest taxonomy metric used by the factor binding.  This
            # prevents a non-top direction from hiding a top-five related
            # sector for the same stock.
            matched_taxonomies = {
                str(value).strip().upper()
                for value in (taxonomy_binding.get("matched_taxonomies") or ())
                if str(value).strip()
            }
            full_market_candidates = [
                (key, value)
                for key, value in full_market_rotation_directions.items()
                if key.upper() in matched_taxonomies
            ]
            if full_market_candidates:
                matched_key, full_market_rotation_match = min(
                    full_market_candidates,
                    key=lambda pair: (
                        int(_number(pair[1].get("primary_rank")) or 10**9),
                        str(pair[0]),
                    ),
                )
                rotation_direction_id = str(
                    full_market_rotation_match.get("rotation_direction_id")
                    or matched_key
                )
        if selected_board_source_available and selected_board_match is not None:
            rotation_direction_id = (
                "SELECTED_BOARD:"
                f"{selected_board_match.get('board_code') or selected_board_match.get('theme_id')}"
            )
        if full_market_rotation_available and full_market_rotation_match is None:
            # Keep the A1-bound identifier for audit, but do not mistake a
            # missing/invalid full-market join for a qualifying direction.
            rotation_direction_id = a1_rotation_direction_id
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
        behavior_type = str(behavior_decision.get("stock_behavior_type") or A2_BEHAVIOR_UNRESOLVED)
        cycle_stage = str(market_emotion.get("emotion_cycle_stage") or "MIXED").upper()
        emotion_cycle_allowed = cycle_stage in {"STARTUP", "IGNITION", "CONFIRMATION", "ACCELERATION"}
        # Daily emotion rows intentionally do not carry monthly business-line
        # evidence.  Once the independent hot-100, ladder and cycle gates are
        # satisfied, allow the market-core route to reach LLM review while
        # preserving all hard market-fact/identity/coverage failures.  This is
        # a route-specific evidence exemption, never a blanket overlay pass.
        emotion_overlay_route_candidate = (
            daily_emotion_overlay
            and daily_a1_member
            and behavior_type == "EMOTION"
            and hot100_available
            and hot100_row is not None
            and emotion_cycle_allowed
        )
        if emotion_overlay_route_candidate:
            market_route = route_results.get(MARKET_CORE_ROUTE)
            if isinstance(market_route, Mapping):
                missing = {
                    str(reason)
                    for reason in market_route.get("missing_reason_codes", ())
                    if str(reason)
                }
                if missing.issubset({"A1_BUSINESS_EVIDENCE_MISSING"}):
                    adjusted_market_route = dict(market_route)
                    adjusted_market_route["eligible"] = True
                    adjusted_market_route["missing_reason_codes"] = []
                    adjusted_market_route["diagnostic_reason_codes"] = list(dict.fromkeys([
                        *[str(reason) for reason in adjusted_market_route.get("diagnostic_reason_codes", ()) if str(reason)],
                        "A2_DAILY_EMOTION_BUSINESS_EVIDENCE_NOT_REQUIRED",
                    ]))
                    adjusted_market_route["data_sufficiency_state"] = (
                        "DEGRADED"
                        if adjusted_market_route.get("missing_optional_factors")
                        else "SUFFICIENT"
                    )
                    route_results[MARKET_CORE_ROUTE] = adjusted_market_route
        if upstream_research_only or hard_risk_present or explicitly_inactive:
            # A1 may retain a row with a known hard risk for research
            # traceability.  It must not receive an A2 route or be sent to an
            # LLM, even if its market facts happen to be complete.
            blocked_routes: dict[str, dict[str, Any]] = {}
            for route_name, raw_route in route_results.items():
                route = dict(raw_route) if isinstance(raw_route, Mapping) else {}
                route["eligible"] = False
                route["blocked_by_upstream"] = upstream_research_only
                route["blocked_by_risk"] = hard_risk_present
                route["blocked_by_inactive_a1_row"] = explicitly_inactive
                blocking_reasons = []
                if upstream_research_only:
                    blocking_reasons.append("A2_UPSTREAM_RESEARCH_ONLY")
                if hard_risk_present:
                    blocking_reasons.append("A2_HARD_RISK_PRESENT")
                if explicitly_inactive:
                    blocking_reasons.append("A2_UPSTREAM_A1_ROW_INACTIVE")
                route["missing_reason_codes"] = list(dict.fromkeys([
                    *[str(code) for code in route.get("missing_reason_codes", ()) if str(code)],
                    *blocking_reasons,
                ]))
                route["data_sufficiency_state"] = "BLOCKED_UPSTREAM"
                blocked_routes[route_name] = route
            route_results = blocked_routes
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
        if upstream_research_route in {"BROKER_GOLD_DIRECT", "FUNDAMENTAL_BASELINE"} and not _has_business_evidence(item):
            reasons.append("A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE")
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
        if not has_route and not (upstream_research_only or hard_risk_present or explicitly_inactive):
            reasons.append("A2_NO_ROUTE_READY")
        reasons.extend(str(code) for code in behavior_decision.get("reason_codes", ()) if str(code))
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
        if upstream_research_only or hard_risk_present or explicitly_inactive:
            status = "HARD_REJECT"
            data_sufficiency_state = "SUFFICIENT"
            if upstream_research_only:
                reasons.append("A2_UPSTREAM_RESEARCH_ONLY")
            if hard_risk_present:
                reasons.append("A2_HARD_RISK_PRESENT")
            if explicitly_inactive:
                reasons.append("A2_UPSTREAM_A1_ROW_INACTIVE")
        emotion_core_eligible = (
            daily_a1_member
            and hot100_row is not None
            and behavior_type == "EMOTION"
            and emotion_cycle_allowed
        )
        trend_core_eligible = (
            monthly_a1_member
            and not upstream_research_only
            and not hard_risk_present
            and not explicitly_inactive
            and behavior_type == "TREND"
            and (
                (
                    selected_board_source_available
                    and selected_board_match is not None
                )
                if selected_board_field_present
                else (
                    full_market_rotation_match is not None
                    if full_market_rotation_available
                    else rotation_fallback is not None
                )
            )
        )
        pool_channel = (
            "EMOTION" if emotion_core_eligible
            else "TREND" if trend_core_eligible
            else "NONE" if dual_channel_contract
            else "LEGACY"
        )
        if daily_emotion_overlay and status == "REVIEW_CANDIDATE" and not emotion_core_eligible:
            # An overlay-only row has no legacy/trend escape hatch.  It can be
            # reviewed only through the explicit emotion channel; otherwise it
            # remains visible as a local monitor with the failed evidence.
            status = "LOCAL_MONITOR"
            reasons.extend(daily_member_reasons if not daily_a1_member else ())
            if behavior_type != "EMOTION":
                reasons.append("A2_DAILY_EMOTION_OVERLAY_NOT_TREND_ELIGIBLE")
            elif hot100_row is None:
                reasons.append("A2_EMOTION_NOT_IN_EASTMONEY_HOT100")
            elif not emotion_cycle_allowed:
                reasons.append("A2_EMOTION_CYCLE_NO_NEW_ENTRY")
            else:
                reasons.append("A2_EMOTION_EVIDENCE_NOT_ROUTEABLE")
        if dual_channel_contract and status == "REVIEW_CANDIDATE" and pool_channel == "NONE":
            status = "LOCAL_MONITOR"
            if not monthly_a1_member and behavior_type == "TREND":
                reasons.append("A2_DAILY_EMOTION_OVERLAY_NOT_TREND_ELIGIBLE")
            elif not daily_a1_member:
                reasons.extend(daily_member_reasons)
            elif not monthly_a1_member:
                reasons.append("A2_OUTSIDE_FORMAL_A1_POOL")
            elif behavior_type == "EMOTION" and hot100_row is None:
                reasons.append("A2_EMOTION_NOT_IN_EASTMONEY_HOT100")
            elif behavior_type == "EMOTION" and not emotion_cycle_allowed:
                reasons.append("A2_EMOTION_CYCLE_NO_NEW_ENTRY")
            elif behavior_type == "TREND" and selected_board_field_present and not selected_board_source_available:
                reasons.append("A2_SELECTED_BOARD_SOURCE_UNAVAILABLE")
            elif behavior_type == "TREND" and selected_board_field_present:
                # Preserve the historical reason code while adding an
                # explicit top-five contract label for new audit consumers.
                reasons.append("A2_TREND_OUTSIDE_SELECTED_BOARD_TOP5")
                reasons.append("A2_TREND_OUTSIDE_POSITIVE_FLOW_TOP3_BOARD")
            elif behavior_type == "TREND" and full_market_rotation_available:
                reasons.append("A2_NO_POSITIVE_FLOW_ROTATION_DIRECTION")
            elif behavior_type == "TREND":
                # Keep the historical reason-code spelling for downstream
                # compatibility when using the historical fallback path.
                reasons.append("A2_TREND_OUTSIDE_POSITIVE_FLOW_TOP3_BOARD")
            else:
                reasons.append("A2_BEHAVIOR_UNRESOLVED")
        decision = {
            "decision_id": content_hash({
                "stage": "A2_LOCAL_ROLE",
                "symbol": symbol,
                "as_of": snapshot_as_of,
                "feature_version": FEATURE_VERSION,
                "source_hashes": source_hashes,
            })[:24],
            "symbol": symbol,
            "name": item.get("company_name") or item.get("name") or candidate.get("name"),
            "stage": "A2_LOCAL_ROLE",
            "status": status,
            # Keep the pre-ranking deterministic outcome so transport
            # attribution can distinguish a locally ineligible row from a
            # review candidate that was later clipped by theme/rank budget.
            "local_eligibility_status": status,
            "local_eligible_for_review": status == "REVIEW_CANDIDATE",
            "score": round(score, 4),
            "identifiability_score": round(identifiability, 4),
            "theme_id": source_theme_id,
            "primary_theme": item.get("primary_theme") or source_theme_id,
            "rotation_direction_id": rotation_direction_id,
            "node_id": item.get("industry_chain_node") or item.get("node_id"),
            "industry_chain_node": item.get("industry_chain_node") or item.get("node_id"),
            "upstream_candidate_id": item.get("candidate_id") or item.get("upstream_candidate_id"),
            # ``a1_formal_member`` is retained as a compatibility alias for
            # the frozen monthly identity.  Daily overlay membership is
            # intentionally exposed separately so consumers cannot route a
            # daily emotion row into the monthly trend channel by accident.
            "a1_formal_member": formal_a1_member,
            "monthly_a1_member": monthly_a1_member,
            "daily_a1_member": daily_a1_member,
            "daily_a1_member_reason_codes": list(dict.fromkeys(daily_member_reasons)),
            "daily_emotion_overlay": daily_emotion_overlay,
            "upstream_a1_status": upstream_status,
            "upstream_selection_basis": selection_basis or None,
            "upstream_coverage_origin": item.get("coverage_origin"),
            "business_exposure": item.get("business_exposure"),
            "business_exposure_facts": item.get("business_exposure_facts", []),
            "research_route": upstream_research_route or None,
            "downstream_trade_eligible": item.get("downstream_trade_eligible", True) is True,
            "hard_risk_events": [
                *(
                    [dict(event) for event in hard_risk_by_symbol.get(symbol, ()) if isinstance(event, Mapping)]
                    if hard_risk_by_symbol.get(symbol)
                    else []
                ),
                *(
                    [dict(item_hard_risk_events)]
                    if isinstance(item_hard_risk_events, Mapping)
                    else [dict(event) for event in item_hard_risk_events if isinstance(event, Mapping)]
                    if isinstance(item_hard_risk_events, Sequence)
                    and not isinstance(item_hard_risk_events, (str, bytes, bytearray))
                    else []
                ),
            ],
            "source_refs": list(item.get("source_refs") or ()) if isinstance(item.get("source_refs"), Sequence) and not isinstance(item.get("source_refs"), (str, bytes, bytearray)) else [],
            "role": role,
            "legacy_market_role": legacy_role,
            "stock_behavior_type": behavior_decision.get("stock_behavior_type"),
            "a2_pool_channel": pool_channel,
            "emotion_core_eligible": emotion_core_eligible,
            "trend_core_eligible": trend_core_eligible,
            "eastmoney_hot100": dict(hot100_row) if hot100_row is not None else None,
            "selected_board": dict(selected_board_match) if selected_board_match is not None else None,
            "rotation_fallback": dict(rotation_fallback) if rotation_fallback is not None else None,
            "rotation_input_source": (
                "SELECTED_BOARD_SNAPSHOT"
                if selected_board_field_present and selected_board_source_available
                else "SELECTED_BOARD_SNAPSHOT_UNAVAILABLE"
                if selected_board_field_present
                else "FULL_MARKET_ROTATION_FALLBACK"
                if full_market_rotation_available
                else "LEGACY_ROTATION_FALLBACK"
            ),
            "full_market_rotation": (
                dict(full_market_rotation_match)
                if isinstance(full_market_rotation_match, Mapping)
                else None
            ),
            "route_permission": list(behavior_decision.get("route_permission") or ()),
            "behavior_type_decision": behavior_decision,
            "market_emotion_cycle": {
                "available": market_emotion.get("available") is True,
                "stage": market_emotion.get("emotion_cycle_stage"),
                "new_long_permission": market_emotion.get("new_long_permission"),
                "as_of": market_emotion.get("as_of"),
                "reason_codes": list(market_emotion.get("emotion_cycle_reason_codes") or ()),
            },
            "market_funding": {
                "available": market_funding.get("available") is True,
                "state": market_funding.get("state") or "UNRESOLVED",
                "amount_ratio": market_funding.get("amount_ratio"),
                "coverage": market_funding.get("coverage"),
                "latest_trade_date": market_funding.get("latest_trade_date"),
                "reason_codes": list(market_funding.get("reason_codes") or ()),
                "turnover_is_capital_flow": False,
                "execution_context_only": True,
            },
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
            "as_of": snapshot_as_of,
        }
        decisions.append(decision)
        # Rank rotation directions from the full A1 market cross-section.
        # Stock behaviour is applied only after the strongest directions are
        # known; otherwise an entire strong board can disappear merely
        # because its constituents were classified UNRESOLVED at stock level.
        if monthly_a1_member and not (upstream_research_only or hard_risk_present or explicitly_inactive) and (
            full_market_rotation_match is not None
            or (
                selected_board_source_available
                and selected_board_match is not None
            )
            or (
                not selected_board_field_present
                and not full_market_rotation_available
                and rotation_fallback is not None
            )
            or (
                not selected_board_field_present
                and not full_market_rotation_available
                and not dual_channel_contract
                and _number(taxonomy_binding.get("rotation_strength_score")) is not None
            )
        ):
            market_grouped[rotation_direction_id].append(decision)
        if (
            status == "REVIEW_CANDIDATE"
            and monthly_a1_member
            and not (upstream_research_only or hard_risk_present or explicitly_inactive)
            and (trend_core_eligible or not dual_channel_contract)
        ):
            grouped[rotation_direction_id].append(decision)
    theme_strength: dict[str, float] = {}
    theme_strength_source: dict[str, str] = {}
    ranking_groups = market_grouped if market_grouped else grouped
    for theme_id, values in ranking_groups.items():
        values.sort(key=lambda item: (-float(item["score"]), -float(item["identifiability_score"]), str(item["symbol"])))
        full_market_strengths = [
            float(score)
            for item in values
            if isinstance(item.get("full_market_rotation"), Mapping)
            and (score := _number(item["full_market_rotation"].get("strength"))) is not None
        ]
        selected_strengths = [
            float(score)
            for item in values
            if isinstance(item.get("selected_board"), Mapping)
            and (score := _number(item["selected_board"].get("strength"))) is not None
        ]
        market_scores = [
            float(score)
            for item in values
            if (score := _number(
                (item.get("a2_taxonomy_binding") or {}).get("rotation_strength_score")
                if isinstance(item.get("a2_taxonomy_binding"), Mapping)
                else None
            )) is not None
        ]
        if full_market_rotation_available and full_market_strengths:
            theme_strength[theme_id] = round(max(full_market_strengths), 4)
            theme_strength_source[theme_id] = "A2_THEME_METRICS"
        elif selected_strengths:
            theme_strength[theme_id] = round(max(selected_strengths), 4)
            theme_strength_source[theme_id] = (
                "LIANGJIAN_FREE_ROTATION_STRENGTH"
                if str(selected_boards.get("source_id") or "").strip().upper()
                == "LIANGJIAN_FREE_ROTATION_V1"
                else "LEGACY_SELECTED_BOARD_STRENGTH"
            )
        elif market_scores:
            theme_strength[theme_id] = round(max(market_scores), 4)
            theme_strength_source[theme_id] = "A2_THEME_METRICS"
        else:
            # Frozen legacy fixtures may predate the sector-strength contract.
            # Retain deterministic replayability and label the fallback; new
            # production snapshots always use A2_THEME_METRICS.
            leaders = values[:5]
            theme_strength[theme_id] = round(
                sum(float(item["score"]) for item in leaders) / 5.0,
                4,
            )
            theme_strength_source[theme_id] = "LEGACY_CONSTITUENT_SCORE_FALLBACK"
    ranked_themes = sorted(
        theme_strength,
        key=lambda theme_id: (-theme_strength[theme_id], -len(ranking_groups[theme_id]), theme_id),
    )
    if full_market_rotation_available:
        # The local full-market facts are the authoritative source.  Keep all
        # ranks for audit, while only the deterministic top-N directions open
        # the trend channel.
        top_theme_ids = set(ranked_themes[:rotation_theme_count])
        theme_rotation_rank = {
            theme_id: rank for rank, theme_id in enumerate(ranked_themes, start=1)
        }
    elif selected_board_field_present and selected_board_source_available:
        # ``selected_for_rotation`` and ``primary_rank`` were materialized by
        # the versioned fixed-theme collector.  Only those rows (already
        # constrained to the requested top five above) can open the trend
        # channel. Child boards inherit the parent rank and therefore do not
        # consume another primary slot.
        top_theme_ids = set(ranked_themes)
        theme_rotation_rank = {}
        for rank, theme_id in enumerate(ranked_themes, start=1):
            primary_ranks = [
                int(_number(item.get("selected_board", {}).get("primary_rank")) or 999)
                for item in ranking_groups[theme_id]
                if isinstance(item.get("selected_board"), Mapping)
            ]
            theme_rotation_rank[theme_id] = min(primary_ranks) if primary_ranks else rank
    else:
        top_theme_ids = set(ranked_themes[:rotation_theme_count])
        theme_rotation_rank = {theme_id: rank for rank, theme_id in enumerate(ranked_themes, start=1)}

    for theme_id, values in ranking_groups.items():
        for item in values:
            item["theme_rotation_rank"] = theme_rotation_rank[theme_id]
            item["theme_rotation_score"] = theme_strength[theme_id]
            item["rotation_strength_source"] = theme_strength_source[theme_id]
            item["top_rotation_theme"] = theme_id in top_theme_ids
        eligible_values = grouped.get(theme_id, [])
        eligible_values.sort(key=lambda item: (-float(item["score"]), -float(item["identifiability_score"]), str(item["symbol"])))
        for rank, item in enumerate(eligible_values, start=1):
            item["theme_rank"] = rank
            if theme_id not in top_theme_ids:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A2_OUTSIDE_ROTATION_TOP_THEMES")
            elif not review_all_eligible and rank > llm_top_n_per_theme:
                item["status"] = "LOCAL_MONITOR"
                item["reason_codes"].append("A2_NOT_SENT_TO_LLM")
            else:
                item["sent_to_llm"] = True

    # The emotion channel is independently sourced from Eastmoney top 100 and
    # the six-stage cycle. It must not be demoted merely because its emerging
    # theme has not yet entered the selected-board strength top five.
    for item in decisions:
        if item.get("status") == "REVIEW_CANDIDATE" and item.get("emotion_core_eligible") is True:
            item["top_rotation_theme"] = None
            item["theme_rotation_rank"] = None
            item["theme_rotation_score"] = None
            item["rotation_strength_source"] = "EASTMONEY_HOT100_EMOTION_CHANNEL"
            item["theme_rank"] = item.get("eastmoney_hot100", {}).get("rank") if isinstance(item.get("eastmoney_hot100"), Mapping) else None
            item["sent_to_llm"] = True

    if market_grouped:
        for item in decisions:
            if (
                item.get("status") != "REVIEW_CANDIDATE"
                or item.get("top_rotation_theme") is True
                or item.get("emotion_core_eligible") is True
            ):
                continue
            item["status"] = "LOCAL_MONITOR"
            item["sent_to_llm"] = False
            item["reason_codes"] = list(dict.fromkeys([
                *[str(code) for code in item.get("reason_codes", ()) if str(code)],
                "A2_ROTATION_STRENGTH_UNAVAILABLE",
            ]))

    # Attribution is deliberately computed after theme ranking and transport
    # selection.  This records the final ``SENT_TO_LLM`` state while leaving
    # every status/partition decision above untouched.
    for item in decisions:
        gate_results = _a2_attribution_gates(
            snapshot,
            item=item,
            candidate=candidates.get(str(item.get("symbol") or ""), {}),
            theme_rotation_score=item.get("theme_rotation_score"),
            identifiability=float(item.get("identifiability_score") or 0.0),
            minimum_identifiability_score=minimum_identifiability_score,
            factor_scores=item.get("a2_factor_scores") if isinstance(item.get("a2_factor_scores"), Mapping) else {},
            eligible_routes=item.get("eligible_routes") if isinstance(item.get("eligible_routes"), Sequence) and not isinstance(item.get("eligible_routes"), (str, bytes, bytearray)) else (),
            sent_to_llm=item.get("sent_to_llm") is True,
        )
        item["gate_results"] = gate_results
        failures = _a2_gate_failures(gate_results)
        item["all_failed_gates"] = failures
        item["first_blocking_gate"] = failures[0] if failures else None
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


def _a2_rotation_fallback_evidence(
    snapshot: Mapping[str, Any],
    *,
    taxonomy_binding: Mapping[str, Any],
    ranked_directions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Return an audited trend fallback when the rotation snapshot is absent.

    The fallback never pretends that vendor board identifiers are
    interchangeable. It uses the already-bound legacy taxonomy for strength
    and only confirms the direction when independently joined Eastmoney board
    flow reports a strictly positive current-day main-net amount. The normal
    top-N ranking remains downstream of this function.
    """

    taxonomy = str(taxonomy_binding.get("taxonomy") or "").strip().upper()
    taxonomy_code = str(taxonomy_binding.get("taxonomy_code") or "").strip().upper()
    key = f"{taxonomy}:{taxonomy_code}"
    evidence = ranked_directions.get(key)
    return dict(evidence) if isinstance(evidence, Mapping) else None


def _a2_full_market_rotation_available(
    snapshot: Mapping[str, Any],
    *,
    a1_output: Mapping[str, Any],
) -> bool:
    """Return whether historical full-market fallback facts are usable.

    A2 must distinguish two different states: a source that is unavailable and
    an available source which simply has no positive-flow direction today.  A
    materialized selected-board snapshot is the production contract and is
    handled by :func:`screen_a2` before this compatibility helper.  This
    helper is deliberately valid only for historical snapshots which have no
    ``SELECTED_BOARD_SNAPSHOT`` field at all; an explicitly unavailable field
    must fail closed instead of falling back to a transient taxonomy ranking.
    It therefore requires the materialized theme metrics, sector-health rows,
    and local taxonomy membership contracts needed by A1's lineage.  The
    actual positive-flow/lineage join is deliberately evaluated separately by
    :func:`_a2_ranked_fallback_directions`; an empty result is a valid
    ``NO_QUALIFYING_DIRECTION`` state, not a provider failure.
    """

    if "SELECTED_BOARD_SNAPSHOT" in snapshot:
        return False

    raw_metrics = snapshot.get("A2_THEME_METRICS")
    metrics = raw_metrics.get("theme_metrics") if isinstance(raw_metrics, Mapping) else None
    if not isinstance(metrics, Mapping) or not metrics:
        return False
    if isinstance(raw_metrics, Mapping) and raw_metrics.get("available") is False:
        return False

    raw_health = snapshot.get("A2_SECTOR_HEALTH_SNAPSHOT")
    health = raw_health if isinstance(raw_health, Mapping) else {}
    if health.get("available") is False:
        return False
    by_taxonomy = health.get("by_taxonomy")
    if not isinstance(by_taxonomy, Mapping):
        return False
    # An explicitly available health contract with empty sector arrays still
    # represents a valid observation of no qualifying direction.  We only
    # treat malformed/missing sections as source unavailability.
    health_taxonomies = {
        str(taxonomy).strip().upper()
        for taxonomy, section in by_taxonomy.items()
        if isinstance(section, Mapping)
        and isinstance(section.get("sectors"), Sequence)
        and not isinstance(section.get("sectors"), (str, bytes, bytearray))
    }
    if not health_taxonomies:
        return False

    # Check only taxonomies represented by A1's explicit lineage.  This keeps
    # a complete industry contract usable when an old replay fixture has no
    # concept catalog, while still failing closed for a missing contract that
    # is required to resolve the current A1 direction.
    linked_taxonomies: set[str] = set()
    for link in _mapping_list(a1_output.get("taxonomy_links")):
        direct_taxonomy = str(link.get("taxonomy") or "").strip().upper()
        direct_code = str(link.get("taxonomy_code") or "").strip()
        if direct_taxonomy in {"INDUSTRY", "CONCEPT"} and direct_code:
            linked_taxonomies.add(direct_taxonomy)
        for taxonomy, fields in (
            ("INDUSTRY", ("industry_thscodes", "industry_codes")),
            ("CONCEPT", ("concept_thscodes", "concept_codes")),
        ):
            if any(
                isinstance(link.get(field), Sequence)
                and not isinstance(link.get(field), (str, bytes, bytearray))
                and any(str(value).strip() for value in link.get(field, ()))
                for field in fields
            ):
                linked_taxonomies.add(taxonomy)
    metric_taxonomies = {
        str(key).split(":", 1)[0].strip().upper()
        for key in metrics
        if ":" in str(key)
        and str(key).split(":", 1)[0].strip().upper() in {"INDUSTRY", "CONCEPT"}
    }
    required_taxonomies = linked_taxonomies.intersection(metric_taxonomies) or metric_taxonomies
    if not required_taxonomies:
        return False
    for taxonomy in required_taxonomies:
        if taxonomy not in health_taxonomies:
            return False
        contract_key = "THS_{}_MEMBERSHIP".format(taxonomy)
        membership = snapshot.get(contract_key)
        if (
            not isinstance(membership, Mapping)
            or membership.get("available") is False
            or membership.get("complete") is False
        ):
            return False
        if not _fact_records(membership):
            return False
    return True


def _a2_ranked_fallback_directions(
    snapshot: Mapping[str, Any],
    *,
    a1_output: Mapping[str, Any],
    limit: int | None,
) -> dict[str, dict[str, Any]]:
    """Rank positive-flow A1 directions before classifying individual stocks.

    ``limit=None`` returns the complete ranked set of independent A1 monthly
    directions, represented by each direction's strongest concrete board.
    The screen uses that form so every independent direction receives an
    auditable rank even when only the top five are admitted to the trend
    channel.  The historical integer form remains available for replay
    compatibility.
    """

    links_by_node: dict[str, list[tuple[str, str]]] = defaultdict(list)
    theme_by_node: dict[str, str] = {}
    for link in _mapping_list(a1_output.get("taxonomy_links")):
        node_id = str(link.get("node_id") or "").strip()
        if not node_id:
            continue
        theme_by_node[node_id] = str(link.get("theme_id") or node_id).strip().upper()
        direct_taxonomy = str(link.get("taxonomy") or "").strip().upper()
        direct_code = str(link.get("taxonomy_code") or "").strip().upper()
        if direct_taxonomy in {"INDUSTRY", "CONCEPT"} and direct_code:
            links_by_node[node_id].append((direct_taxonomy, direct_code))
        for taxonomy, fields in (
            ("INDUSTRY", ("industry_thscodes", "industry_codes")),
            ("CONCEPT", ("concept_thscodes", "concept_codes")),
        ):
            for field in fields:
                values = link.get(field)
                if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                    continue
                links_by_node[node_id].extend(
                    (taxonomy, str(value).strip().upper())
                    for value in values
                    if str(value).strip()
                )
        links_by_node[node_id] = list(dict.fromkeys(links_by_node[node_id]))
    industry_members = _membership_map(snapshot.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
    concept_members = _membership_map(snapshot.get("THS_CONCEPT_MEMBERSHIP"), taxonomy="CONCEPT")
    hard_risk_symbols = _hard_risk_symbols(snapshot.get("RISK_EVENTS"))
    themes_by_key: dict[str, set[str]] = defaultdict(set)
    for item in _mapping_list(a1_output.get("active_research_pool")):
        if (
            str(item.get("selection_basis") or "").strip().upper() == "DAILY_EMOTION_OVERLAY"
            or str(item.get("research_route") or "").strip().upper() == "DAILY_EMOTION_OVERLAY"
        ):
            continue
        symbol = _symbol(item.get("symbol"))
        node_id = str(item.get("industry_chain_node") or item.get("node_id") or "").strip()
        if not symbol or not node_id:
            continue
        if item.get("downstream_trade_eligible") is False or symbol in hard_risk_symbols:
            continue
        item_risks = item.get("hard_risk_events")
        if isinstance(item_risks, Mapping) or (
            isinstance(item_risks, Sequence)
            and not isinstance(item_risks, (str, bytes, bytearray))
            and any(isinstance(event, Mapping) or str(event).strip() for event in item_risks)
        ):
            continue
        memberships = {
            f"INDUSTRY:{str(row.get('taxonomy_code') or '').strip().upper()}"
            for row in industry_members.get(symbol, ())
            if str(row.get("taxonomy_code") or "").strip()
        }
        memberships.update({
            f"CONCEPT:{str(row.get('taxonomy_code') or '').strip().upper()}"
            for row in concept_members.get(symbol, ())
            if str(row.get("taxonomy_code") or "").strip()
        })
        for taxonomy, code in links_by_node.get(node_id, ()):
            key = f"{taxonomy}:{code}"
            if key in memberships:
                themes_by_key[key].add(theme_by_node.get(node_id, node_id.upper()))
    raw_metrics = snapshot.get("A2_THEME_METRICS")
    metrics = raw_metrics.get("theme_metrics") if isinstance(raw_metrics, Mapping) else None
    metrics = metrics if isinstance(metrics, Mapping) else {}
    candidates: list[dict[str, Any]] = []
    for key, theme_ids in themes_by_key.items():
        metric = metrics.get(key)
        if not isinstance(metric, Mapping) or metric.get("available") is not True:
            continue
        strength = _number(metric.get("score"))
        if strength is None:
            continue
        taxonomy, code = key.split(":", 1)
        flow_evidence = _a2_positive_sector_flow(snapshot, taxonomy=taxonomy, taxonomy_code=code)
        if flow_evidence is None:
            continue
        candidates.append({
            **flow_evidence,
            "taxonomy": taxonomy,
            "taxonomy_code": code,
            "taxonomy_name": metric.get("taxonomy_name") or flow_evidence.get("taxonomy_name"),
            "strength": strength,
            "theme_ids": sorted(theme_ids),
        })
    candidates.sort(key=lambda row: (
        -float(row["strength"]),
        -float(row["main_net_inflow_cny"]),
        str(row["taxonomy_code"]),
    ))
    representatives: list[dict[str, Any]] = []
    used_theme_ids: set[str] = set()
    for row in candidates:
        # Several concrete THS boards can describe the same A1 monthly
        # direction.  Counting every synonym would let one structural theme
        # consume all five rotation slots, so retain only its strongest
        # positive-flow concrete representative.
        row_theme_ids = set(row["theme_ids"])
        if row_theme_ids.intersection(used_theme_ids):
            continue
        representatives.append(row)
        used_theme_ids.update(row_theme_ids)
        if limit is not None and len(representatives) >= max(1, int(limit)):
            break
    # Every concrete child board that belongs to a selected A1 direction must
    # remain usable for stock membership matching.  It inherits the rank and
    # strength of the strongest representative, so the child does not consume
    # another top-N slot and all related constituents still reach the same
    # downstream LLM review bucket.
    expanded: dict[str, dict[str, Any]] = {}
    ranked_representatives = list(enumerate(representatives, start=1))
    for row in candidates:
        row_theme_ids = set(row["theme_ids"])
        matched = next(
            (
                (rank, representative)
                for rank, representative in ranked_representatives
                if row_theme_ids.intersection(representative["theme_ids"])
            ),
            None,
        )
        if matched is None:
            continue
        rank, representative = matched
        representative_id = (
            f"{representative['taxonomy']}:{representative['taxonomy_code']}"
        )
        expanded[f"{row['taxonomy']}:{row['taxonomy_code']}"] = {
            **representative,
            "primary_rank": rank,
            "rotation_direction_id": representative_id,
            "matched_taxonomy": row["taxonomy"],
            "matched_taxonomy_code": row["taxonomy_code"],
            "matched_taxonomy_name": row.get("taxonomy_name"),
            "matched_board_strength": row["strength"],
            "matched_board_main_net_inflow_cny": row["main_net_inflow_cny"],
            "source_scope": "SECTOR",
            "reason_code": "A2_SELECTED_BOARD_FALLBACK_SECTOR_EVIDENCE",
        }
    return expanded


def _a2_positive_sector_flow(
    snapshot: Mapping[str, Any],
    *,
    taxonomy: str,
    taxonomy_code: str,
) -> dict[str, Any] | None:
    raw_health = snapshot.get("A2_SECTOR_HEALTH_SNAPSHOT")
    health = raw_health if isinstance(raw_health, Mapping) else {}
    by_taxonomy = health.get("by_taxonomy")
    by_taxonomy = by_taxonomy if isinstance(by_taxonomy, Mapping) else {}
    section = by_taxonomy.get(taxonomy.lower())
    rows = section.get("sectors") if isinstance(section, Mapping) else ()
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("taxonomy_code") or "").strip().upper() != taxonomy_code:
            continue
        flow = row.get("capital_flow")
        if not isinstance(flow, Mapping) or flow.get("available") is not True:
            return None
        windows = flow.get("windows")
        today = windows.get("today") if isinstance(windows, Mapping) else None
        main_net_cny = _number(today.get("main_net_cny")) if isinstance(today, Mapping) else None
        if main_net_cny is None or main_net_cny <= 0:
            return None
        return {
            "taxonomy_name": row.get("taxonomy_name"),
            "main_net_inflow_cny": main_net_cny,
            "change_pct": _number(today.get("change_pct")),
            "source": flow.get("source") or "EASTMONEY_BOARD_CAPITAL_FLOW",
        }
    return None


def _a2_behavior_evidence(
    *,
    item: Mapping[str, Any],
    factor_scores: Mapping[str, Mapping[str, Any]],
    identifiability: float,
    minimum_identifiability_score: float,
    relative: float,
    liquidity: float,
    legacy_role: str,
    as_of: str | None,
) -> dict[str, Any]:
    """Adapt frozen A2 facts to the strict emotion/trend contract.

    Thresholds here only translate existing deterministic observations into a
    boolean fact.  They do not create another aggregate score.  Every adapter
    keeps source, value and availability so a missing feed cannot become a
    bearish observation.
    """

    def factor_fact(
        name: str,
        *,
        threshold: float,
        value: Any = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        factor = factor_scores.get(name)
        factor = factor if isinstance(factor, Mapping) else {}
        score = _number(factor.get("score"))
        available = factor.get("available") is True and score is not None
        refs = _payload_source_refs(factor)
        source = str(factor.get("source") or "").strip()
        if source:
            refs.append(source)
        return {
            "available": available,
            "met": score >= threshold if available else None,
            "value": value if value is not None else {"score": score, "threshold": threshold},
            "source_refs": list(dict.fromkeys(refs)),
            "as_of": as_of,
            "reason": reason or (str(factor.get("reason_code") or "A2_FACTOR_OBSERVED") if available else str(factor.get("reason_code") or "A2_FACTOR_UNAVAILABLE")),
        }

    node_id = item.get("industry_chain_node") or item.get("node_id")
    theme_id = item.get("primary_theme") or item.get("theme_id")
    business_facts = [
        fact for fact in item.get("business_exposure_facts", ())
        if isinstance(fact, Mapping)
    ]
    # A1's production projection historically persisted the normalized
    # ``business_exposure`` object but omitted the additive
    # ``business_exposure_facts`` list.  That object is still a server-owned,
    # point-in-time business-lineage fact (including its source_ref), so it
    # must be accepted as the equivalent single fact here.  Treating the
    # missing convenience list as a data gap made otherwise confirmed trend
    # candidates fail the A2 industry facet and become UNRESOLVED with no
    # route.  Do not infer a positive fact from arbitrary text: only the
    # normalized mapping is eligible for this compatibility projection.
    if not business_facts:
        exposure = item.get("business_exposure")
        # ``evidence_basis`` is intentionally part of this compatibility
        # contract.  A theme/node pair alone proves no company exposure, and
        # a source URL without a declared business-disclosure basis is not
        # enough to promote a trend route.  Keep the accepted values aligned
        # with the A1 prompt enum; ``-`` is an explicit absence of evidence.
        evidence_basis = str(exposure.get("evidence_basis") or "").strip().upper() if isinstance(exposure, Mapping) else ""
        extraction_method = str(exposure.get("extraction_method") or "").strip().upper() if isinstance(exposure, Mapping) else ""
        source_ref = str(exposure.get("source_ref") or exposure.get("evidence_ref") or "").strip() if isinstance(exposure, Mapping) else ""
        if (
            isinstance(exposure, Mapping)
            and (
                evidence_basis in {"MAIN_BUSINESS_BREAKDOWN", "COMPANY_DISCLOSURE"}
                or extraction_method in {
                    "REVENUE_COMPOSITION_TABLE_分行业".upper(),
                    "REVENUE_COMPOSITION_TABLE_分产品".upper(),
                }
            )
            and source_ref
            and (
                _number(exposure.get("revenue_exposure_pct")) is not None
                or _number(exposure.get("gross_profit_exposure_pct")) is not None
            )
        ):
            business_facts = [exposure]
    supply_available = bool(node_id and theme_id and business_facts)
    supply_refs = list(dict.fromkeys(
        str(fact.get("evidence_ref") or fact.get("source_ref") or "")
        for fact in business_facts
        if str(fact.get("evidence_ref") or fact.get("source_ref") or "")
    ))
    supply = {
        "available": supply_available,
        "met": True if supply_available else None,
        "value": {
            "theme_id": theme_id,
            "node_id": node_id,
            "industry_logic": True if supply_available else None,
        },
        "source_refs": supply_refs,
        "as_of": as_of,
        "reason": "A2_A1_CHAIN_AND_BUSINESS_LINEAGE_CONFIRMED" if supply_available else "A2_A1_CHAIN_OR_BUSINESS_LINEAGE_MISSING",
    }

    tier = factor_scores.get("tier_structure")
    tier = tier if isinstance(tier, Mapping) else {}
    ladder_height = _number(tier.get("ladder_height"))
    first_board_observed = (
        tier.get("first_board_observed") is True
        and ladder_height == 1
        and str(tier.get("event_source") or tier.get("source") or "").strip().upper()
        in {"HITHINK_LIMIT_UP_POOL", "HITHINK_LIMIT_UP_POOL_FIRST_BOARD", "HITHINK_LIMIT_UP_LADDER"}
    )
    ladder_available = tier.get("available") is True and (
        ladder_height is not None or str(tier.get("availability_state") or "").upper() == "OBSERVED_ABSENT"
    )
    ladder_refs = _payload_source_refs(tier)
    tier_source = str(tier.get("source") or "").strip()
    if tier_source:
        ladder_refs.append(tier_source)
    ladder = {
        "available": ladder_available,
        "met": (
            (ladder_height is not None and ladder_height >= 2) or first_board_observed
        ) if ladder_available else None,
        "value": {
            "ladder_height": ladder_height,
            "tier": tier.get("tier"),
            "market_role": legacy_role,
            "first_board_observed": first_board_observed,
            "continuation_confirmed": tier.get("continuation_confirmed") is True,
            "event_source": tier.get("event_source") or tier.get("source"),
        },
        "source_refs": list(dict.fromkeys(ladder_refs)),
        "as_of": as_of,
        "reason": "A2_LADDER_OBSERVED" if ladder_available else str(tier.get("reason_code") or "A2_LADDER_UNAVAILABLE"),
    }

    weekly = factor_scores.get("weekly_confirmation")
    weekly = weekly if isinstance(weekly, Mapping) else {}
    trend_proxy = factor_scores.get("trend_strength_proxy")
    trend_proxy = trend_proxy if isinstance(trend_proxy, Mapping) else {}
    medium_source = weekly if weekly.get("available") is True else trend_proxy
    medium_name = "weekly_confirmation" if medium_source is weekly else "trend_strength_proxy"
    medium_score = _number(medium_source.get("score"))
    medium_available = medium_source.get("available") is True and medium_score is not None
    medium_refs = _payload_source_refs(medium_source)
    if str(medium_source.get("source") or ""):
        medium_refs.append(str(medium_source.get("source")))

    resonance = factor_scores.get("index_chain_resonance")
    resonance = resonance if isinstance(resonance, Mapping) else {}
    resonance_score = _number(resonance.get("score"))
    resonance_available = resonance.get("available") is True and resonance_score is not None
    resonance_refs = _payload_source_refs(resonance)
    resonance_source = str(resonance.get("source") or "").strip()
    if resonance_source:
        resonance_refs.append(resonance_source)
    industry_logic = {
        "available": resonance_available,
        "met": resonance_score >= 50.0 if resonance_available else None,
        "value": {
            "score": resonance_score,
            "threshold": 50.0,
            "taxonomy": resonance.get("taxonomy"),
            "taxonomy_code": resonance.get("taxonomy_code"),
            "taxonomy_name": resonance.get("taxonomy_name"),
        },
        "source_refs": list(dict.fromkeys(resonance_refs)),
        "as_of": as_of,
        "reason": (
            str(resonance.get("reason_code") or "A2_TAXONOMY_RESONANCE_OBSERVED")
            if resonance_available
            else str(resonance.get("reason_code") or "A2_INDUSTRY_LOGIC_UNAVAILABLE")
        ),
    }

    return {
        "supply_chain_position": supply,
        "capital_flow": factor_fact("capital_flow", threshold=50.0),
        "ladder_structure": ladder,
        # No authoritative crowding feed is currently frozen.  Keep the gap
        # explicit rather than reusing turnover or capital flow as a proxy.
        "crowding": {
            "available": False,
            "met": None,
            "value": None,
            "source_refs": [],
            "as_of": as_of,
            "reason": "A2_CROWDING_SOURCE_UNAVAILABLE",
        },
        "index_chain_resonance": factor_fact("index_chain_resonance", threshold=50.0),
        "identifiability_liquidity": {
            "available": True,
            "met": identifiability >= minimum_identifiability_score and liquidity > 0,
            "value": {
                "identifiability": round(identifiability, 4),
                "threshold": round(minimum_identifiability_score, 4),
                "liquidity": round(liquidity, 4),
            },
            "source_refs": ["A2_DETERMINISTIC_IDENTITY_AND_LIQUIDITY"],
            "as_of": as_of,
            "reason": "A2_IDENTIFIABILITY_AND_LIQUIDITY_OBSERVED",
        },
        "medium_term_trend": {
            "available": medium_available,
            "met": medium_score >= 50.0 if medium_available else None,
            "value": {"score": medium_score, "threshold": 50.0, "source_factor": medium_name},
            "source_refs": list(dict.fromkeys(medium_refs)),
            "as_of": as_of,
            "reason": str(medium_source.get("reason_code") or "A2_MEDIUM_TERM_TREND_OBSERVED") if medium_available else "A2_MEDIUM_TERM_TREND_UNAVAILABLE",
        },
        "relative_strength": {
            "available": True,
            "met": relative >= 60.0,
            "value": {"score": round(relative, 4), "threshold": 60.0},
            "source_refs": ["A2_FROZEN_RELATIVE_STRENGTH"],
            "as_of": as_of,
            "reason": "A2_RELATIVE_STRENGTH_OBSERVED",
        },
        # Industry/theme resonance is a market-structure fact and must remain
        # independent from the stricter company-level supply-chain proof.
        # This lets MARKET_CORE classify a real trend while
        # SUPPLY_CHAIN_ALPHA still requires explicit business exposure.
        "industry_logic": industry_logic,
    }


def _a2_attribution_record(
    *,
    available: bool,
    value: Any,
    threshold: Any,
    passed: bool | None,
    applied: bool,
    blocks_decision: bool,
    reason_code: str,
    **extra: Any,
) -> dict[str, Any]:
    """Build one auditable A2 gate record.

    ``available`` describes whether the frozen input contains enough facts to
    evaluate the check.  A source that is unavailable, or a check that is only
    configured but not implemented by this deterministic gate, uses
    ``passed=None`` and ``blocks_decision=False``.  This prevents an absent
    feed from becoming either an implicit pass or an invented rejection.
    """

    result: dict[str, Any] = {
        "available": bool(available),
        "value": value if available else None,
        "threshold": threshold,
        "passed": passed if available else None,
        "applied": bool(applied),
        "blocks_decision": bool(blocks_decision) if available else False,
        "reason_code": str(reason_code),
    }
    result.update(extra)
    return result


def _a2_attribution_gates(
    snapshot: Mapping[str, Any],
    *,
    item: Mapping[str, Any],
    candidate: Mapping[str, Any],
    theme_rotation_score: Any,
    identifiability: float,
    minimum_identifiability_score: float,
    factor_scores: Mapping[str, Mapping[str, Any]],
    eligible_routes: Sequence[str],
    sent_to_llm: bool,
) -> dict[str, dict[str, Any]]:
    """Return the complete A2 attribution contract without changing routing.

    Some names are part of the wider A2/model contract but are not gates in the
    current deterministic implementation (for example leader criteria and
    free-float cap).  Those checks are deliberately represented as unavailable
    unless their actual per-symbol facts and an applied rule exist.  The
    distinction is important: operators must be able to tell a missing
    implementation/data source from a failed market condition.
    """

    theme_threshold = _number(snapshot.get("MIN_THEME_SCORE"))
    theme_value = _number(theme_rotation_score)
    theme_available = theme_threshold is not None and theme_value is not None

    leader_threshold = _number(snapshot.get("LEADER_MIN_CRITERIA"))
    max_leaders_threshold = _number(snapshot.get("MAX_LEADERS_PER_THEME"))
    free_float_threshold = _number(snapshot.get("MIN_FREE_FLOAT_CAP"))

    tier = factor_scores.get("tier_structure")
    tier_score = _number(tier.get("score")) if isinstance(tier, Mapping) else None
    tier_available = (
        isinstance(tier, Mapping)
        and tier.get("available") is True
        and tier_score is not None
    )

    # The existing deterministic implementation does not apply these three
    # configuration checks.  Preserve any raw input value for inspection when
    # it is present, but never label it as an effective blocking gate.
    leader_value = _first_number(
        item,
        "leader_criteria_count",
        "leader_criteria_met",
        "leader_min_criteria_value",
    )
    max_leaders_value = _first_number(
        item,
        "leaders_in_theme",
        "leader_count_in_theme",
    )
    free_float_value = _first_number(
        item,
        "free_float_cap_cny",
        "free_float_market_cap_cny",
        "free_float_cap",
    )
    if free_float_value is None:
        free_float_value = _first_number(
            candidate,
            "free_float_cap_cny",
            "free_float_market_cap_cny",
            "free_float_cap",
        )

    data_sufficiency_state = str(item.get("data_sufficiency_state") or "").upper()
    data_sufficiency_available = data_sufficiency_state in {
        "SUFFICIENT",
        "DEGRADED",
        "INSUFFICIENT",
    }
    data_sufficiency_passed = data_sufficiency_state in {"SUFFICIENT", "DEGRADED"}
    local_eligibility_status = str(item.get("local_eligibility_status") or "").upper()
    local_eligibility_available = local_eligibility_status in {
        "REVIEW_CANDIDATE",
        "LOCAL_MONITOR",
        "DATA_GAP",
        "HARD_REJECT",
    }
    local_eligible_for_review = item.get("local_eligible_for_review") is True

    gates: dict[str, dict[str, Any]] = {
        "LOCAL_DATA_SUFFICIENCY": _a2_attribution_record(
            available=data_sufficiency_available,
            value=data_sufficiency_state or None,
            threshold=["SUFFICIENT", "DEGRADED"],
            passed=data_sufficiency_passed if data_sufficiency_available else None,
            applied=data_sufficiency_available,
            blocks_decision=data_sufficiency_state == "INSUFFICIENT",
            reason_code=(
                "A2_LOCAL_DATA_SUFFICIENT"
                if data_sufficiency_state == "SUFFICIENT"
                else "A2_LOCAL_DATA_DEGRADED"
                if data_sufficiency_state == "DEGRADED"
                else "A2_LOCAL_DATA_INSUFFICIENT"
                if data_sufficiency_state == "INSUFFICIENT"
                else "A2_LOCAL_DATA_SUFFICIENCY_UNAVAILABLE"
            ),
            accepted_states=["SUFFICIENT", "DEGRADED"],
        ),
        "LOCAL_ELIGIBILITY": _a2_attribution_record(
            available=local_eligibility_available,
            value=local_eligibility_status or None,
            threshold="REVIEW_CANDIDATE",
            passed=local_eligible_for_review if local_eligibility_available else None,
            applied=local_eligibility_available,
            blocks_decision=local_eligibility_available and not local_eligible_for_review,
            reason_code=(
                "A2_LOCAL_ELIGIBLE_FOR_REVIEW"
                if local_eligible_for_review
                else f"A2_LOCAL_NOT_ELIGIBLE_{local_eligibility_status or 'UNKNOWN'}"
            ),
            eligible_for_review=local_eligible_for_review,
        ),
        "THEME_SCORE_MIN": _a2_attribution_record(
            available=theme_available,
            value=round(theme_value, 4) if theme_value is not None else None,
            threshold=round(theme_threshold, 4) if theme_threshold is not None else None,
            passed=theme_value >= theme_threshold if theme_available else None,
            applied=False,
            blocks_decision=False,
            reason_code=(
                "A2_THEME_SCORE_OBSERVED_NOT_APPLIED"
                if theme_available
                else "A2_THEME_SCORE_UNAVAILABLE"
            ),
            evaluation="OBSERVATION_ONLY_NOT_A_DETERMINISTIC_GATE",
        ),
        "IDENTIFIABILITY_MIN": _a2_attribution_record(
            available=True,
            value=round(identifiability, 4),
            threshold=round(minimum_identifiability_score, 4),
            passed=identifiability >= minimum_identifiability_score,
            applied=True,
            blocks_decision=identifiability < minimum_identifiability_score,
            reason_code=(
                "A2_IDENTIFIABILITY_MEETS_MINIMUM"
                if identifiability >= minimum_identifiability_score
                else "A2_IDENTIFIABILITY_BELOW_MINIMUM"
            ),
        ),
        "BEHAVIOR_TYPE_RESOLVED": _a2_attribution_record(
            available=bool(item.get("stock_behavior_type")),
            value=item.get("stock_behavior_type"),
            threshold=["EMOTION", "TREND"],
            passed=item.get("stock_behavior_type") in {"EMOTION", "TREND"},
            applied=True,
            # A2 remains a broad research funnel.  Unresolved type is visible
            # here and becomes a hard publication gate in A3; it must not
            # erase the candidate before richer technical evidence is read.
            blocks_decision=False,
            reason_code=(
                "A2_BEHAVIOR_TYPE_RESOLVED"
                if item.get("stock_behavior_type") in {"EMOTION", "TREND"}
                else "A2_BEHAVIOR_TYPE_UNRESOLVED"
            ),
            route_permission=list(item.get("route_permission") or ()),
        ),
        "LEADER_MIN_CRITERIA": _a2_attribution_record(
            available=leader_value is not None and leader_threshold is not None,
            value=leader_value,
            threshold=leader_threshold,
            passed=leader_value >= leader_threshold if leader_value is not None and leader_threshold is not None else None,
            applied=False,
            blocks_decision=False,
            reason_code=(
                "A2_LEADER_CRITERIA_OBSERVED_NOT_APPLIED"
                if leader_value is not None and leader_threshold is not None
                else "A2_LEADER_CRITERIA_UNAVAILABLE"
            ),
            evaluation="NO_DETERMINISTIC_LEADER_CRITERIA_GATE",
        ),
        "MAX_LEADERS_PER_THEME": _a2_attribution_record(
            available=max_leaders_value is not None and max_leaders_threshold is not None,
            value=max_leaders_value,
            threshold=max_leaders_threshold,
            passed=max_leaders_value <= max_leaders_threshold if max_leaders_value is not None and max_leaders_threshold is not None else None,
            applied=False,
            blocks_decision=False,
            reason_code=(
                "A2_MAX_LEADERS_OBSERVED_NOT_APPLIED"
                if max_leaders_value is not None and max_leaders_threshold is not None
                else "A2_MAX_LEADERS_UNAVAILABLE"
            ),
            evaluation="NO_DETERMINISTIC_MAX_LEADERS_GATE",
        ),
        "TIER_STRUCTURE": _a2_attribution_record(
            available=tier_available,
            value=round(tier_score, 4) if tier_score is not None else None,
            threshold=None,
            passed=None,
            applied=False,
            blocks_decision=False,
            reason_code=(
                "A2_TIER_STRUCTURE_OBSERVATION_ONLY"
                if tier_available
                else "A2_TIER_STRUCTURE_UNAVAILABLE"
            ),
            evaluation="OPTIONAL_FACTOR_NOT_A_MARKET_CORE_VETO",
            availability_state=(tier.get("availability_state") if isinstance(tier, Mapping) else None),
        ),
        "ROUTE_REQUIREMENT": _a2_attribution_record(
            available=True,
            value=len(eligible_routes),
            threshold=1,
            passed=bool(eligible_routes),
            applied=True,
            blocks_decision=not eligible_routes,
            reason_code=(
                "A2_ROUTE_REQUIREMENT_SATISFIED"
                if eligible_routes
                else "A2_ROUTE_REQUIREMENT_UNSATISFIED"
            ),
            eligible_routes=list(eligible_routes),
        ),
        "SENT_TO_LLM": _a2_attribution_record(
            available=True,
            value=bool(sent_to_llm),
            threshold=True,
            # A row that never qualified for local review does not fail a
            # transport gate.  Only a locally eligible review row clipped by
            # theme/rank selection is a real NOT_SENT_TO_LLM failure.
            passed=bool(sent_to_llm) if local_eligible_for_review else None,
            applied=local_eligible_for_review,
            blocks_decision=local_eligible_for_review and not sent_to_llm,
            reason_code=(
                "A2_SENT_TO_LLM"
                if sent_to_llm
                else "A2_NOT_SENT_TO_LLM"
                if local_eligible_for_review
                else "A2_NOT_SENT_NOT_REQUIRED"
            ),
            applicable=local_eligible_for_review,
        ),
        "FREE_FLOAT_CAP": _a2_attribution_record(
            available=free_float_value is not None and free_float_threshold is not None,
            value=free_float_value,
            threshold=free_float_threshold,
            passed=free_float_value >= free_float_threshold if free_float_value is not None and free_float_threshold is not None else None,
            applied=False,
            blocks_decision=False,
            reason_code=(
                "A2_FREE_FLOAT_OBSERVED_NOT_APPLIED"
                if free_float_value is not None and free_float_threshold is not None
                else "A2_FREE_FLOAT_UNAVAILABLE"
            ),
            evaluation="NO_DETERMINISTIC_FREE_FLOAT_GATE",
        ),
    }
    return gates


def _a2_gate_failures(
    gate_results: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Return only effective, available failures in canonical gate order."""

    return [
        gate_name
        for gate_name in _A2_ATTRIBUTION_GATES
        if isinstance(gate_results.get(gate_name), Mapping)
        and gate_results[gate_name].get("available") is True
        and gate_results[gate_name].get("passed") is False
        and gate_results[gate_name].get("blocks_decision") is True
    ]


def _bind_a2_factor_to_a1_lineage(
    snapshot: Mapping[str, Any],
    a1_output: Mapping[str, Any],
    item: Mapping[str, Any],
    symbol: str,
    factor: Mapping[str, Any],
    preferred_taxonomies: set[str] | None = None,
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
    preferred = set(preferred_taxonomies or ())
    preferred_rows = [
        metrics[key]
        for key in matched
        if key in preferred
        and isinstance(metrics.get(key), Mapping)
        and metrics[key].get("available") is True
    ]
    if preferred_rows:
        rows = preferred_rows
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
        "rotation_strength_score": _number(best.get("score")),
        "rotation_strength_available": best.get("available") is True,
        "reference_member_count": best.get("reference_member_count", best.get("member_count")),
        "candidate_member_count": best.get("candidate_member_count"),
        "return_coverage": best.get("return_coverage"),
        "source_refs": list(source_refs),
        "matched_taxonomies": matched,
    }


def _a2_rotation_theme_id(
    item: Mapping[str, Any],
    factor: Mapping[str, Any],
) -> str:
    """Return an A1 theme or a source-backed market taxonomy fallback.

    Broker-gold direct rows can legitimately enter A1 before a monthly
    structural theme is mapped.  A2 still has a real THS taxonomy aggregate
    for the symbol.  Use that exact taxonomy as the rotation bucket instead
    of collapsing every such row into ``UNMAPPED``.  No name/text inference is
    allowed here.
    """

    explicit = str(item.get("primary_theme") or item.get("theme_id") or "").strip()
    if explicit and explicit.upper() != "UNMAPPED":
        return explicit

    raw_factors = factor.get("factors")
    factors = raw_factors if isinstance(raw_factors, Mapping) else factor
    resonance = factors.get("index_chain_resonance")
    resonance = resonance if isinstance(resonance, Mapping) else {}
    taxonomy = str(resonance.get("taxonomy") or "").strip().upper()
    code = str(resonance.get("taxonomy_code") or "").strip().upper()
    if taxonomy in {"INDUSTRY", "CONCEPT"} and code:
        return f"{taxonomy}:{code}"
    return "UNMAPPED"


def _a2_taxonomy_theme_map(a1_output: Mapping[str, Any]) -> dict[str, str]:
    """Return source-backed taxonomy-to-A1-theme mappings for A2 rotation.

    A1 can admit institutional direct-research rows before assigning a
    ``primary_theme``.  Those rows still carry an audited THS taxonomy.  A2
    must aggregate them into the matching A1 structural direction instead of
    treating every raw industry/concept code as an independent rotation.

    Conflicting mappings are resolved deterministically by confidence and
    then by theme id.  No name matching or semantic inference is performed.
    """

    ranked: dict[str, tuple[float, str]] = {}

    def retain(code: Any, theme: Any, confidence: Any) -> None:
        taxonomy_code = str(code or "").strip().upper()
        theme_id = str(theme or "").strip()
        if not taxonomy_code or not theme_id or theme_id.upper() == "UNMAPPED":
            return
        score = _number(confidence)
        candidate = (score if score is not None else 0.0, theme_id)
        current = ranked.get(taxonomy_code)
        if current is None or candidate[0] > current[0] or (
            candidate[0] == current[0] and candidate[1] < current[1]
        ):
            ranked[taxonomy_code] = candidate

    raw_links = a1_output.get("taxonomy_links")
    if isinstance(raw_links, Sequence) and not isinstance(raw_links, (str, bytes, bytearray)):
        for raw in raw_links:
            if not isinstance(raw, Mapping):
                continue
            retain(raw.get("taxonomy_code"), raw.get("theme_id"), raw.get("confidence"))

    raw_mappings = a1_output.get("industry_theme_mappings")
    if isinstance(raw_mappings, Sequence) and not isinstance(raw_mappings, (str, bytes, bytearray)):
        for raw in raw_mappings:
            if not isinstance(raw, Mapping):
                continue
            themes = raw.get("mapped_theme_ids")
            if not isinstance(themes, Sequence) or isinstance(themes, (str, bytes, bytearray)):
                continue
            for theme_id in themes:
                retain(raw.get("industry_thscode"), theme_id, raw.get("confidence"))

    return {code: value[1] for code, value in ranked.items()}


def _a2_rotation_direction_id(
    item: Mapping[str, Any],
    factor: Mapping[str, Any],
    taxonomy_theme_map: Mapping[str, str],
    *,
    fallback: str,
) -> str:
    """Resolve the canonical direction used only for rotation aggregation."""

    explicit = str(item.get("primary_theme") or item.get("theme_id") or "").strip()
    if explicit and explicit.upper() != "UNMAPPED":
        return explicit

    raw_factors = factor.get("factors")
    factors = raw_factors if isinstance(raw_factors, Mapping) else factor
    resonance = factors.get("index_chain_resonance")
    resonance = resonance if isinstance(resonance, Mapping) else {}
    code = str(resonance.get("taxonomy_code") or "").strip().upper()
    mapped = str(taxonomy_theme_map.get(code) or "").strip()
    if mapped:
        return mapped
    return fallback


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
    diagnostics: list[str] = []
    research_route = str(item.get("research_route") or "").strip().upper()
    research_route_qualified = research_route in {"BROKER_GOLD_DIRECT", "FUNDAMENTAL_BASELINE"}
    if identifiability < minimum_identifiability_score:
        missing.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
    if not str(item.get("primary_theme") or item.get("theme_id") or "").strip():
        if research_route_qualified:
            diagnostics.append("A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_STRUCTURAL_MAPPING")
        else:
            missing.append("A1_THEME_MISSING")
    if not str(item.get("industry_chain_node") or item.get("node_id") or "").strip():
        if research_route_qualified:
            if "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_STRUCTURAL_MAPPING" not in diagnostics:
                diagnostics.append("A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_STRUCTURAL_MAPPING")
        else:
            missing.append("A1_CHAIN_NODE_MISSING")
    if not _has_business_evidence(item):
        if research_route_qualified:
            # These A1 routes are allowed to establish a broad market/emotion
            # candidate without pretending that a revenue split proves a
            # supply-chain position.  A2 keeps the gap as a diagnostic; the
            # stricter SUPPLY_CHAIN_ALPHA route still requires its own facts.
            diagnostics.append("A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE")
        else:
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
        "diagnostic_reason_codes": list(dict.fromkeys(diagnostics)),
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
        "first_board_observed",
        "continuation_confirmed",
        "event_source",
        "trade_date",
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
    if str(item.get("research_route") or "").strip().upper() == "HALF_YEAR_FUNDAMENTAL":
        disclosed = item.get("disclosed_business_match")
        half_year = item.get("half_year_support")
        refs = item.get("source_refs")
        return bool(
            isinstance(disclosed, Mapping)
            and disclosed.get("raw_disclosure_available") is True
            and isinstance(half_year, Mapping)
            and half_year.get("supported") is True
            and isinstance(refs, Sequence)
            and not isinstance(refs, (str, bytes, bytearray))
            and any(str(ref).strip() for ref in refs)
        )
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
    raw_market_emotion = snapshot.get("MARKET_EMOTION_SNAPSHOT")
    market_emotion = (
        raw_market_emotion
        if isinstance(raw_market_emotion, Mapping)
        else {
            "available": False,
            "reason_code": "MARKET_EMOTION_SNAPSHOT_MISSING",
        }
    )
    raw_market_funding = snapshot.get("MARKET_FUNDING_SNAPSHOT")
    market_funding = (
        raw_market_funding
        if isinstance(raw_market_funding, Mapping)
        else {
            "available": False,
            "state": "UNRESOLVED",
            "reason_codes": ["MARKET_FUNDING_SNAPSHOT_MISSING"],
        }
    )
    source_hashes = _source_hashes(snapshot)
    market_reference = snapshot.get("A2_MARKET_REFERENCE")
    market_trade_date = (
        str(market_reference.get("market_trade_date") or "").strip()
        if isinstance(market_reference, Mapping)
        else ""
    )
    reference_close_as_of = (
        f"{market_trade_date}T15:00:00+08:00"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", market_trade_date)
        else None
    )
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
            market_emotion=market_emotion,
            market_funding=market_funding,
            as_of=reference_close_as_of,
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
            strategy["route_permission"] = "BLOCKED"
            strategy["publication_state"] = "BLOCKED"
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
            "decision_id": content_hash({
                "stage": "A3_LOCAL_TECHNICAL",
                "symbol": symbol,
                "as_of": _snapshot_as_of(snapshot),
                "strategy_version": strategy.get("strategy_version"),
                "source_hashes": source_hashes,
            })[:24],
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
            "as_of": _snapshot_as_of(snapshot),
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
            "decision_id": item.get("decision_id"),
            "company_archetype": item.get("company_archetype"),
            "pullback_cause": item.get("pullback_cause"),
            "a1_selection_evidence": dict(item.get("a1_selection_evidence") or {}),
            "available_weight": item.get("available_weight"),
            "available_weight_pct": item.get("available_weight_pct"),
            "missing_factors": item.get("missing_factors", []),
            "evidence_confidence": 0.0,
            "status": "MONITOR",
            "selection_basis": item.get("selection_basis") or "DETERMINISTIC_SCORE",
            # Preserve the server-owned route and execution boundary for
            # research-only rows (notably broker-gold symbols outside G0).
            # These fields are copied, never inferred from a monitor status or
            # from fabricated theme/business evidence.
            "research_route": item.get("research_route"),
            "downstream_trade_eligible": item.get("downstream_trade_eligible", True) is True,
            "reason_codes": item.get("reason_codes", []),
            "coverage_origin": item.get("coverage_origin"),
            "autonomous_status": item.get("autonomous_status"),
            "institutional_coverage": item.get("institutional_coverage"),
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
        if item.get("status") in {"LOCAL_MONITOR", "OUTSIDE_THEME", "OUTSIDE_G0"}
    ]


def local_active_items(result: DeterministicGateResult) -> list[dict[str, Any]]:
    """Project locally verified A1 rows into the canonical research schema."""

    projected: list[dict[str, Any]] = []
    for item in result.decisions:
        if item.get("status") != "LOCAL_ACTIVE_CANDIDATE":
            continue
        facts = [fact for fact in item.get("business_exposure_facts", ()) if isinstance(fact, Mapping)]
        research_route = str(item.get("research_route") or "").strip().upper()
        research_only_route = research_route in {
            "BROKER_GOLD_DIRECT",
            "FUNDAMENTAL_BASELINE",
            "HALF_YEAR_FUNDAMENTAL",
        }
        if not facts and not research_only_route:
            continue
        exposure = (
            max(facts, key=lambda fact: float(fact.get("revenue_exposure_pct") or 0.0))
            if facts
            else None
        )
        source_ref = str((exposure or {}).get("evidence_ref") or "")
        institutional_refs = (
            _source_refs_from_values(item.get("institutional_coverage"))
            if isinstance(item.get("institutional_coverage"), Mapping)
            else []
        )
        source_refs = [ref for ref in dict.fromkeys([
            *[str(value) for value in item.get("source_refs", ()) if str(value)],
            *[str(value) for value in item.get("theme_source_refs", ()) if str(value)],
            *[str(value) for value in item.get("node_source_refs", ()) if str(value)],
            *institutional_refs,
            source_ref,
            *[
                str(ref)
                for factor in (item.get("factor_details") or {}).values()
                if isinstance(factor, Mapping)
                for ref in (factor.get("source_refs") or ())
                if str(ref)
            ],
        ]) if ref]
        projected.append({
            "symbol": item["symbol"],
            "candidate_id": f"a1-local:{item['symbol']}",
            "decision_id": item.get("decision_id"),
            "company_name": item.get("name"),
            "primary_theme": item.get("theme_id"),
            "monthly_direction_id": item.get("monthly_direction_id"),
            "monthly_direction_name": item.get("monthly_direction_name"),
            "monthly_direction_matches": list(item.get("monthly_direction_matches") or ()),
            "secondary_themes": [],
            "industry_chain_node": item.get("node_id"),
            "sector_index_taxonomy": item.get("sector_index_taxonomy"),
            "sector_index_code": item.get("sector_index_code"),
            "sector_index_name": item.get("sector_index_name"),
            "sector_constituent_confirmed": item.get("sector_constituent_confirmed") is True,
            "taxonomy_matches": list(item.get("taxonomy_matches") or ()),
            "core_thesis": (
                "BROKER_GOLD_MONTHLY_RESEARCH_COVERAGE"
                if research_route == "BROKER_GOLD_DIRECT"
                else "FUNDAMENTAL_BASELINE_RESEARCH_COVERAGE"
                if research_route == "FUNDAMENTAL_BASELINE"
                else "MONTHLY_THEME_AND_HALF_YEAR_GROWTH_CONFIRMED"
                if research_route == "HALF_YEAR_FUNDAMENTAL"
                else "MONTHLY_THEME_AND_DISCLOSED_BUSINESS_MAPPING_CONFIRMED"
            ),
            "bear_case": (
                "BROKER_GOLD_RESEARCH_THESIS_INVALIDATED_OR_UPSTREAM_RISK_CONFIRMED"
                if research_route == "BROKER_GOLD_DIRECT"
                else "FUNDAMENTAL_BASELINE_FINANCIAL_OR_LIQUIDITY_FACTS_DETERIORATE"
                if research_route == "FUNDAMENTAL_BASELINE"
                else "HALF_YEAR_GROWTH_REVERSES_OR_MONTHLY_THEME_WEAKENS"
                if research_route == "HALF_YEAR_FUNDAMENTAL"
                else "MONTHLY_THEME_WEAKENS_OR_DISCLOSED_BUSINESS_TRANSMISSION_FAILS"
            ),
            "structural_score": item.get("score"),
            "data_quality_score": item.get("data_quality_score"),
            "evidence_confidence": (
                min(0.90, float(item.get("data_quality_score") or 0.0) / 100.0)
                if research_route == "HALF_YEAR_FUNDAMENTAL"
                else min(
                    float((exposure or {}).get("confidence") or 0.0) if exposure else 0.0,
                    float(item.get("data_quality_score") or 0.0) / 100.0,
                )
            ),
            "status": "ACTIVE",
            "selection_basis": item.get("selection_basis") or "DETERMINISTIC_SCORE",
            "source_refs": source_refs,
            # A research-only route may have no revenue-split evidence.  Keep
            # it explicitly absent; never manufacture a percentage from the
            # route or from a factor score.
            "business_exposure": (
                {
                    "business_name": exposure.get("business_name"),
                    "revenue_exposure_pct": exposure.get("revenue_exposure_pct"),
                    "source_ref": source_ref,
                    "page_number": exposure.get("page_number"),
                    "report_period": exposure.get("report_period"),
                    "extraction_method": exposure.get("extraction_method"),
                }
                if exposure
                else None
            ),
            "score_breakdown": dict(item.get("score_breakdown") or {}),
            "factor_details": dict(item.get("factor_details") or {}),
            "company_archetype": item.get("company_archetype"),
            "pullback_cause": item.get("pullback_cause"),
            "a1_selection_evidence": dict(item.get("a1_selection_evidence") or {}),
            "fundamental_support": dict(item.get("fundamental_support") or {}),
            "half_year_support": dict(item.get("half_year_support") or {}),
            "financial_quality_score": item.get("financial_quality_score"),
            "disclosed_business_match": dict(item.get("disclosed_business_match") or {}),
            "financial_subfactor_coverage": item.get("financial_subfactor_coverage"),
            "available_weight": item.get("available_weight"),
            "available_weight_pct": item.get("available_weight_pct"),
            "missing_factors": list(item.get("missing_factors") or ()),
            "reason_codes": list(dict.fromkeys([
                *[str(code) for code in item.get("reason_codes", ()) if str(code)],
                "A1_DETERMINISTIC_MONTHLY_RESEARCH_ELIGIBLE",
            ])),
            "coverage_origin": item.get("coverage_origin"),
            "autonomous_status": item.get("autonomous_status"),
            "institutional_coverage": item.get("institutional_coverage"),
            "research_route": research_route or None,
            "downstream_trade_eligible": item.get("downstream_trade_eligible", True) is True,
            "local_decision": True,
            "sent_to_llm": False,
        })
    return projected


def local_rejected_items(result: DeterministicGateResult) -> list[dict[str, Any]]:
    return [
        {
            "symbol": item["symbol"],
            "selection_basis": item.get("selection_basis") or "DETERMINISTIC_SCORE",
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


def _baseline_industry_binding(
    symbol: str,
    industry: Mapping[str, Sequence[Mapping[str, Any]]],
    candidate: Mapping[str, Any],
) -> tuple[str, str]:
    """Return one stable 同花顺 industry identity for baseline dispersion.

    Industry memberships are an input fact, not a model-generated theme.  A
    symbol can have several memberships, so the lexical first code/name pair
    is used as the deterministic primary bucket.  The candidate fallback keeps
    hand-authored/replay snapshots usable when only a symbol-level industry
    field was frozen.
    """

    memberships = [
        item for item in industry.get(symbol, ())
        if isinstance(item, Mapping) and str(item.get("taxonomy_code") or "").strip()
    ]
    if memberships:
        primary = sorted(
            memberships,
            key=lambda item: (
                str(item.get("taxonomy_code") or "").strip().upper(),
                str(item.get("taxonomy_name") or "").strip(),
            ),
        )[0]
        return (
            str(primary.get("taxonomy_code") or "").strip().upper(),
            str(primary.get("taxonomy_name") or "").strip(),
        )
    code = str(
        candidate.get("industry_taxonomy_code")
        or candidate.get("industry_thscode")
        or candidate.get("industry_code")
        or ""
    ).strip().upper()
    name = str(candidate.get("industry_name") or candidate.get("industry") or "").strip()
    return code, name


def _fundamental_baseline_score(item: Mapping[str, Any]) -> float:
    """Build the auditable baseline rank from already-frozen A1 facts."""

    return round(
        0.55 * _safe_float(item.get("financial_quality_score"))
        + 0.30 * _safe_float(item.get("data_quality_score"))
        + 0.15 * _safe_float(item.get("liquidity_score")),
        6,
    )


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
        # The long-lived mature-theme registry already emits one normalized
        # taxonomy row per exact catalog resolution.  Accept that canonical
        # representation alongside the model contract's code-array form.
        direct_taxonomy = str(raw.get("taxonomy") or "").strip().upper()
        direct_code = str(raw.get("taxonomy_code") or "").strip().upper()
        if direct_taxonomy in {"INDUSTRY", "CONCEPT"} and direct_code:
            name = universe.get((direct_taxonomy, direct_code))
            if name is not None:
                links.append({
                    "node_id": node_id,
                    "theme_id": str(raw.get("theme_id") or _first_theme_id(nodes, node_id) or "") or None,
                    "taxonomy": direct_taxonomy,
                    "taxonomy_code": direct_code,
                    "taxonomy_name": name,
                    "match_method": str(raw.get("match_method") or "MATURE_THEME_REGISTRY_EXACT_NAME"),
                    "confidence": _number(raw.get("confidence")) or 1.0,
                })
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
    matched.sort(key=lambda item: (
        -float(item.get("confidence") or 0.0),
        0 if str(item.get("taxonomy") or "").upper() == "INDUSTRY" else 1,
        str(item.get("taxonomy_name") or ""),
        str(item.get("node_id") or ""),
    ))
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
        "net_margin": pick("sale_net_interest_ratio", "net_profit_margin", "销售净利率"),
        "revenue_growth": pick(
            "calculate_operating_income_yoy_growth_ratio",
            "operating_income_yoy",
            "revenue_yoy",
            "营业收入同比增长率",
        ),
        "profit_growth": pick(
            "calculate_parent_holder_net_profit_yoy_growth_ratio",
            "net_profit_yoy",
            "归母净利润同比增长率",
        ),
        "cashflow_quality": pick(
            "net_profit_cash_content",
            "cashflow_net_income_ratio",
            "operating_cash_flow_net_divide_income",
            "经营现金流净利润比",
        ),
        "debt_ratio": pick("assets_debt_ratio", "debt_to_assets", "资产负债率"),
    }
    # The provider publishes the cash-content indicator as a percentage
    # (for example 139.27 means 1.3927x).  Older fixtures and compatible
    # sources may already provide a ratio, so normalize only clear percentages.
    if features["cashflow_quality"] is not None and abs(features["cashflow_quality"]) > 5:
        features["cashflow_quality"] = features["cashflow_quality"] / 100.0
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


def _latest_half_year_support(
    fundamental: Mapping[str, Any],
    *,
    as_of: str | None,
) -> dict[str, Any]:
    """Verify the latest disclosed H1 result from frozen income statements.

    This deliberately bypasses the generic indicator projection: that feed
    does not carry a reporting period and may repeat the same latest value.
    The comparison therefore uses one deduplicated Q2 income row for the
    latest fiscal year and the corresponding Q2 row for the prior year.
    """

    statements = fundamental.get("statements")
    income = statements.get("INCOME") if isinstance(statements, Mapping) else None
    if not isinstance(income, Sequence) or isinstance(income, (str, bytes, bytearray)):
        return {"supported": False, "reason_code": "A1_HALF_YEAR_INCOME_MISSING"}

    cutoff_ms: int | None = None
    if as_of:
        try:
            parsed = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            cutoff_ms = int(parsed.timestamp() * 1000)
        except ValueError:
            cutoff_ms = None

    by_year: dict[int, Mapping[str, Any]] = {}
    for row in income:
        if not isinstance(row, Mapping) or str(row.get("fiscal_period") or "").upper() != "Q2":
            continue
        year_value = _number(row.get("fiscal_year"))
        if year_value is None:
            continue
        year = int(year_value)
        report_ms = _number(row.get("report_date_ms"))
        if cutoff_ms is not None and report_ms is not None and int(report_ms) > cutoff_ms:
            continue
        current = by_year.get(year)
        if current is None or (_number(current.get("report_date_ms")) or 0.0) < (report_ms or 0.0):
            by_year[year] = row
    if not by_year:
        return {"supported": False, "reason_code": "A1_HALF_YEAR_NOT_DISCLOSED"}

    fiscal_year = max(by_year)
    current = by_year[fiscal_year]
    previous = by_year.get(fiscal_year - 1)
    if previous is None:
        return {
            "supported": False,
            "fiscal_year": fiscal_year,
            "reason_code": "A1_HALF_YEAR_COMPARATIVE_MISSING",
        }
    revenue = _number(current.get("operating_income"))
    profit = _number(current.get("parent_holder_net_profit"))
    previous_revenue = _number(previous.get("operating_income"))
    previous_profit = _number(previous.get("parent_holder_net_profit"))
    if None in {revenue, profit, previous_revenue, previous_profit}:
        return {
            "supported": False,
            "fiscal_year": fiscal_year,
            "reason_code": "A1_HALF_YEAR_CORE_METRICS_MISSING",
        }

    revenue_growth = _year_over_year_growth(revenue, previous_revenue)
    profit_growth = _year_over_year_growth(profit, previous_profit)
    supported = bool(
        revenue > 0
        and profit > 0
        and revenue >= previous_revenue
        and profit >= previous_profit
    )
    return {
        "supported": supported,
        "fiscal_year": fiscal_year,
        "fiscal_period": "Q2",
        "report_date_ms": int(_number(current.get("report_date_ms")) or 0) or None,
        "operating_income": revenue,
        "parent_holder_net_profit": profit,
        "prior_operating_income": previous_revenue,
        "prior_parent_holder_net_profit": previous_profit,
        "operating_income_yoy_pct": revenue_growth,
        "parent_holder_net_profit_yoy_pct": profit_growth,
        "reason_code": (
            "A1_HALF_YEAR_REVENUE_AND_PROFIT_GROWTH_CONFIRMED"
            if supported
            else "A1_HALF_YEAR_GROWTH_NOT_CONFIRMED"
        ),
        "source": "COMPANY_FUNDAMENTALS.INCOME",
    }


def _year_over_year_growth(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100.0, 6)


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
            # Negative or zero expected growth is not a growth-adjusted
            # valuation advantage.  Preserving the sign prevents a shrinking
            # company from receiving the same PEG-like treatment as a growing
            # one merely because their absolute percentages match.
            if expected_growth <= 0:
                return max(0.0, min(100.0, 50.0 - pe)), True, refs, "A1_EXPECTED_GROWTH_NON_POSITIVE"
            return max(0.0, min(100.0, 100.0 - pe / max(1.0, expected_growth) * 2.0)), True, refs, ""
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
    return set(_hard_risk_events(value))


def _hard_risk_events(value: Any) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
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
        "大幅减持",
        "重大减持",
        "MAJOR_SHAREHOLDER_REDUCTION",
        "MATERIAL_REDUCTION",
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
            result[symbol].append({
                "event_type": row.get("event_type"),
                "reason_code": row.get("reason_code"),
                "severity": row.get("severity") or row.get("risk_level"),
                "title": row.get("title") or row.get("announcement_title"),
                "publish_time": row.get("publish_time") or row.get("event_time"),
                "source_id": row.get("source_id"),
                "source_url": row.get("source_url"),
                "fact_id": row.get("fact_id"),
            })
    return result


def _attention_symbols(value: Any) -> set[str]:
    return _event_symbols(value)


def _fact_records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping) or value.get("available") is False:
        return []
    records = value.get("records")
    if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
        return [item for item in records if isinstance(item, Mapping)]
    by_symbol = value.get("by_symbol")
    if isinstance(by_symbol, Mapping):
        flattened: list[Mapping[str, Any]] = []
        for symbol, rows in by_symbol.items():
            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                continue
            for raw in rows:
                if not isinstance(raw, Mapping):
                    continue
                item = dict(raw)
                item.setdefault("symbol", str(symbol))
                flattened.append(item)
        return flattened
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


def _first_number(value: Mapping[str, Any], *keys: str) -> float | None:
    """Read the first finite numeric value from a symbol-scoped record."""

    for key in keys:
        number = _number(value.get(key))
        if number is not None:
            return number
    return None


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
