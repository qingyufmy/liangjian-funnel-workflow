"""Deterministic, audit-safe explanation projection for A4 decisions.

The projection deliberately contains only facts already frozen in the A3 plan,
the current deterministic strategy result and the entry contract.  It is saved
with the effective event so a later simulated fill cannot be explained with
mutated plan data or model-authored prose.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "" and value != [] and value != {}:
            return value
    return None


def _items(value: Any, *, limit: int = 6) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [str(item) for item in value[:limit] if str(item or "").strip()]


def build_a4_decision_context(
    plan: Mapping[str, Any] | None,
    strategy: Mapping[str, Any] | None,
    entry_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the compact facts used by both signal and fill notifications."""

    plan_data = _mapping(plan)
    strategy_data = _mapping(strategy)
    contract = _mapping(entry_contract)
    cycle = _mapping(plan_data.get("cycle_alignment"))
    cycle_funding = _mapping(cycle.get("market_funding"))
    plan_funding = _mapping(
        _first(plan_data.get("market_funding"), plan_data.get("market_funding_snapshot"))
    )
    funding = cycle_funding or plan_funding
    relative_strength = _mapping(plan_data.get("relative_strength"))
    market_gate = _mapping(strategy_data.get("market_gate"))

    theme_id = _first(
        plan_data.get("theme_id"),
        plan_data.get("primary_theme"),
        plan_data.get("monthly_direction_id"),
    )
    theme_name = _first(
        plan_data.get("theme_name"),
        plan_data.get("primary_theme_name"),
        plan_data.get("monthly_direction_name"),
        theme_id,
        plan_data.get("theme"),
    )
    funding_state = _first(
        funding.get("state"),
        plan_data.get("market_funding_state"),
    )
    return {
        "schema_version": "liangjian-a4-decision-context/1.0.0",
        "environment": {
            "live_status": _first(
                market_gate.get("status"), strategy_data.get("live_market_state_status")
            ),
            "live_decision": _first(
                market_gate.get("decision"), strategy_data.get("live_market_state_decision")
            ),
            "live_as_of": market_gate.get("as_of"),
            "market_environment": _first(
                plan_data.get("market_environment"), cycle.get("market_environment")
            ),
            "market_regime": plan_data.get("market_regime"),
            "emotion_cycle_stage": _first(
                plan_data.get("emotion_cycle_stage"),
                _mapping(cycle.get("emotion_cycle")).get("stage"),
            ),
        },
        "sector": {
            "theme_id": theme_id,
            "theme_name": theme_name,
            "market_role": _first(
                plan_data.get("market_role"), plan_data.get("a2_role"), plan_data.get("role")
            ),
            "theme_stage": _first(
                plan_data.get("theme_stage"), plan_data.get("sector_stage")
            ),
            "relative_strength_percentile": _first(
                relative_strength.get("percentile"),
                plan_data.get("relative_strength_percentile_20d"),
            ),
            "selection_route": _first(
                plan_data.get("a2_route"),
                plan_data.get("selection_route"),
                plan_data.get("route"),
            ),
        },
        "capital": {
            "state": funding_state,
            "available": funding.get("available"),
            "amount_ratio": funding.get("amount_ratio"),
            "coverage": funding.get("coverage"),
            "source": _first(funding.get("source"), funding.get("source_id")),
            # A sector-flow score is intentionally absent unless an upstream
            # producer supplies an explicit value with provenance.
            "sector_flow_score": plan_data.get("sector_flow_score"),
            "sector_flow_source": plan_data.get("sector_flow_source"),
        },
        "strategy": {
            "profile": _first(
                plan_data.get("strategy_profile"), strategy_data.get("strategy_profile")
            ),
            "setup_pattern": _first(
                plan_data.get("setup_pattern"), plan_data.get("setup_type")
            ),
            "stock_behavior_type": plan_data.get("stock_behavior_type"),
            "plan_mode": plan_data.get("plan_mode"),
            "met_conditions": _items(
                _first(strategy_data.get("met_conditions"), plan_data.get("met_conditions"))
            ),
            "unmet_conditions": _items(strategy_data.get("unmet_conditions")),
            "veto_conditions": _items(strategy_data.get("veto_conditions")),
            "closed_5m_end": strategy_data.get("closed_5m_end"),
            "closed_15m_end": strategy_data.get("closed_15m_end"),
        },
        "price": {
            "live_entry_price": strategy_data.get("live_entry_price"),
            "signal_reference": contract.get("signal_reference"),
            "limit_price": contract.get("limit_price"),
            "trigger_low": plan_data.get("trigger_low"),
            "trigger_high": plan_data.get("trigger_high"),
            "stop_level": _first(contract.get("stop_level"), plan_data.get("stop_level")),
            "no_chase_price": _first(
                strategy_data.get("live_no_chase_price"),
                plan_data.get("no_chase_price"),
                plan_data.get("no_chase"),
            ),
            "target_price": _first(
                strategy_data.get("live_target_price"),
                plan_data.get("first_resistance"),
                plan_data.get("pressure_reduce_price"),
            ),
            "live_reward_risk": strategy_data.get("live_reward_risk"),
            "minimum_reward_risk": _first(
                strategy_data.get("minimum_reward_risk"),
                plan_data.get("minimum_reward_risk"),
            ),
        },
    }
