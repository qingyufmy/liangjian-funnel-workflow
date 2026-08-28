"""Evidence-aware A2 supply-chain bottleneck scorecards.

The methodology is adapted from the MIT-licensed ``muxuuu/serenity-skill``
project, but the runtime contract is deliberately stricter: deterministic
inputs only score factors that can be derived from the frozen snapshot.
Unknown scarcity, supplier concentration, expansion, valuation, or catalyst
facts remain ``None`` and must never be silently converted to zero.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


METHODOLOGY_VERSION = "liangjian-serenity-a2/1.0.0"
FACTOR_WEIGHTS: dict[str, float] = {
    "demand_inflection": 15.0,
    "architecture_coupling": 10.0,
    "chokepoint_severity": 15.0,
    "supplier_concentration": 12.0,
    "expansion_difficulty": 12.0,
    "evidence_quality": 15.0,
    "valuation_disconnect": 11.0,
    "catalyst_timing": 10.0,
}
FACTORS: tuple[str, ...] = tuple(FACTOR_WEIGHTS)
A2_ROUTES: frozenset[str] = frozenset({"MARKET_CORE", "SUPPLY_CHAIN_ALPHA"})
MARKET_CORE_ROUTE = "MARKET_CORE"
SUPPLY_CHAIN_ALPHA_ROUTE = "SUPPLY_CHAIN_ALPHA"
PENALTY_FIELDS: tuple[str, ...] = (
    "dilution_financing",
    "governance",
    "geopolitics",
    "liquidity",
    "hype_risk",
    "accounting_quality",
    "cyclicality",
    "alternative_design_risk",
)
SUPPLY_CHAIN_ROLES: frozenset[str] = frozenset({
    "CONTROLS_SCARCE_LAYER",
    "SUPPLIES_SCARCE_LAYER",
    "BENEFITS_WITH_LIMITED_CONTROL",
    "STORY_ONLY",
})
EVIDENCE_STRENGTHS: frozenset[str] = frozenset({"STRONG", "MEDIUM", "WEAK", "NEEDS_CHECKING"})


def deterministic_bottleneck_context(
    item: Mapping[str, Any],
    *,
    demand_score_0_100: float,
    timing_score_0_100: float,
    factor_weights: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the non-speculative part of a bottleneck scorecard.

    A1 structural/business evidence can establish demand alignment,
    architecture coupling and evidence readiness.  It cannot, by itself,
    establish supplier concentration, expansion difficulty, valuation gaps or
    a true chokepoint.  Those factors stay unknown pending source-backed A2
    research.
    """

    breakdown = item.get("score_breakdown")
    breakdown = breakdown if isinstance(breakdown, Mapping) else {}
    business = item.get("business_exposure")
    business = business if isinstance(business, Mapping) else {}
    confidence = _bounded(_number(item.get("evidence_confidence")) * 100.0, 0.0, 100.0)
    business_score = _first_number(
        breakdown,
        ("business_mapping", "business_purity", "business_exposure"),
        default=min(100.0, 40.0 + (_number(business.get("revenue_exposure_pct")) * 0.6)),
    )
    known = {
        "demand_inflection": _to_five(demand_score_0_100),
        "architecture_coupling": _to_five(business_score),
        "evidence_quality": _to_five(confidence),
        "catalyst_timing": _to_five(timing_score_0_100),
    }
    weights = _validated_factor_weights(factor_weights) or FACTOR_WEIGHTS
    unknown = [factor for factor in weights if factor not in known]
    known_weight = sum(weights[factor] for factor in known)
    weighted_points = sum(
        known[factor] / 5.0 * weights[factor]
        for factor in known
    )
    total_weight = sum(weights.values())
    readiness = weighted_points / known_weight * 100.0 if known_weight else 0.0
    source_refs = [
        str(value)
        for value in item.get("source_refs", ())
        if isinstance(value, str) and value.strip()
    ] if isinstance(item.get("source_refs"), Sequence) and not isinstance(
        item.get("source_refs"), (str, bytes, bytearray)
    ) else []
    return {
        "methodology_version": METHODOLOGY_VERSION,
        "known_factor_ratings_0_5": {key: round(value, 4) for key, value in known.items()},
        "unknown_factor_names": unknown,
        "known_weight_pct": round(known_weight, 4),
        "factor_coverage_pct": round(known_weight / total_weight * 100.0, 4) if total_weight else 0.0,
        "factor_weights": {key: round(value, 4) for key, value in weights.items()},
        "evidence_readiness_score": round(readiness, 4),
        "scarcity_claim_allowed": False,
        "source_refs": list(dict.fromkeys(source_refs)),
        "required_research": [
            "rank_value_chain_layers_before_companies",
            "verify_supplier_concentration",
            "verify_expansion_or_qualification_difficulty",
            "verify_orders_capacity_customers_or_margin_transmission",
            "state_missing_proof_and_kill_switches",
        ],
    }


def canonicalize_model_scorecard(
    scorecard: Any,
    *,
    factor_weights: Mapping[str, Any] | None = None,
    penalty_multiplier: float = 2.0,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate factor ranges and recompute the 0-100 score server-side."""

    if not isinstance(scorecard, Mapping):
        return None, ["A2_BOTTLENECK_SCORECARD_MISSING"]
    weights = _validated_factor_weights(factor_weights) or FACTOR_WEIGHTS
    factors = scorecard.get("factors")
    if not isinstance(factors, Mapping) or set(factors) != set(weights):
        return None, ["A2_BOTTLENECK_FACTORS_INVALID"]
    normalized_factors: dict[str, float] = {}
    for name in weights:
        raw = factors.get(name)
        if isinstance(raw, bool) or raw is None:
            return None, ["A2_BOTTLENECK_FACTORS_INVALID"]
        value = _number(raw)
        if value < 0.0 or value > 5.0:
            return None, ["A2_BOTTLENECK_FACTORS_INVALID"]
        normalized_factors[name] = value
    penalties = scorecard.get("penalties")
    if not isinstance(penalties, Mapping):
        return None, ["A2_BOTTLENECK_PENALTIES_INVALID"]
    normalized_penalties: dict[str, float] = {}
    for name in PENALTY_FIELDS:
        raw = penalties.get(name, 0)
        if isinstance(raw, bool) or raw is None:
            return None, ["A2_BOTTLENECK_PENALTIES_INVALID"]
        value = _number(raw)
        if value < 0.0 or value > 5.0:
            return None, ["A2_BOTTLENECK_PENALTIES_INVALID"]
        normalized_penalties[name] = value
    raw_points = sum(
        normalized_factors[name] / 5.0 * weight
        for name, weight in weights.items()
    )
    try:
        parsed_multiplier = float(penalty_multiplier)
    except (TypeError, ValueError):
        parsed_multiplier = 2.0
    if parsed_multiplier < 0.0:
        return None, ["A2_BOTTLENECK_PENALTIES_INVALID"]
    penalty_points = sum(normalized_penalties.values()) * parsed_multiplier
    final_score = max(0.0, min(100.0, raw_points - penalty_points))
    return {
        **dict(scorecard),
        "methodology_version": METHODOLOGY_VERSION,
        "factors": {name: round(value, 4) for name, value in normalized_factors.items()},
        "penalties": {name: round(value, 4) for name, value in normalized_penalties.items()},
        "factor_weights": {name: round(value, 4) for name, value in weights.items()},
        "penalty_multiplier": parsed_multiplier,
        "raw_factor_points": round(raw_points, 2),
        "penalty_points": round(penalty_points, 2),
        "final_score": round(final_score, 2),
        "server_recomputed": True,
    }, []


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number


def _validated_factor_weights(value: Mapping[str, Any] | None) -> dict[str, float] | None:
    """Return positive configured weights without silently changing factor names.

    The scorecard schema is intentionally strict about the eight factor names,
    while callers may supply a versioned configuration with different numeric
    weights.  We keep the configured scale (the default sums to 100) so the
    server-side score remains auditable; only a non-positive/invalid mapping is
    rejected and the methodology default is used by the public helpers.
    """

    if not isinstance(value, Mapping) or not value:
        return None
    parsed: dict[str, float] = {}
    for name, raw in value.items():
        if isinstance(raw, bool):
            return None
        try:
            number = float(raw)
        except (TypeError, ValueError):
            return None
        if number <= 0.0:
            return None
        parsed[str(name)] = number
    if set(parsed) != set(FACTOR_WEIGHTS):
        return None
    return parsed


def _bounded(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _to_five(value: float) -> float:
    return _bounded(value, 0.0, 100.0) / 20.0


def _first_number(
    values: Mapping[str, Any],
    names: Sequence[str],
    *,
    default: float,
) -> float:
    for name in names:
        if name in values and values.get(name) is not None and not isinstance(values.get(name), bool):
            return _bounded(_number(values.get(name)), 0.0, 100.0)
    return _bounded(default, 0.0, 100.0)


__all__ = [
    "A2_ROUTES",
    "EVIDENCE_STRENGTHS",
    "FACTORS",
    "FACTOR_WEIGHTS",
    "MARKET_CORE_ROUTE",
    "METHODOLOGY_VERSION",
    "PENALTY_FIELDS",
    "SUPPLY_CHAIN_ALPHA_ROUTE",
    "SUPPLY_CHAIN_ROLES",
    "canonicalize_model_scorecard",
    "deterministic_bottleneck_context",
]
