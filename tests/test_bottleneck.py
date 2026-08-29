from __future__ import annotations

import pytest

from liangjian_funnel.pipeline.bottleneck import (
    FACTOR_WEIGHTS,
    canonicalize_model_scorecard,
    deterministic_bottleneck_context,
)
from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.research import _a2_bottleneck_reasons, _apply_a2_lineage_policy


def _scorecard() -> dict:
    return {
        "factors": {name: 4 for name in FACTOR_WEIGHTS},
        "penalties": {
            "dilution_financing": 1,
            "governance": 0,
            "geopolitics": 0,
            "liquidity": 0,
            "hype_risk": 1,
            "accounting_quality": 0,
            "cyclicality": 0,
            "alternative_design_risk": 0,
        },
    }


def test_deterministic_context_keeps_unproven_scarcity_unknown():
    context = deterministic_bottleneck_context(
        {
            "score_breakdown": {"business_mapping": 80},
            "evidence_confidence": 0.9,
            "source_refs": ["cninfo:600000.SH:page:1"],
        },
        demand_score_0_100=70,
        timing_score_0_100=60,
    )

    assert context["scarcity_claim_allowed"] is False
    assert context["known_factor_ratings_0_5"]["architecture_coupling"] == 4
    assert "chokepoint_severity" in context["unknown_factor_names"]
    assert "supplier_concentration" in context["unknown_factor_names"]
    assert "expansion_difficulty" in context["unknown_factor_names"]


def test_scorecard_is_recomputed_and_penalized_server_side():
    scorecard, reasons = canonicalize_model_scorecard(_scorecard())

    assert reasons == []
    assert scorecard is not None
    assert scorecard["raw_factor_points"] == 80
    assert scorecard["penalty_points"] == 4
    assert scorecard["final_score"] == 76
    assert scorecard["server_recomputed"] is True


def test_scorecard_rejects_missing_or_out_of_range_factor():
    missing = _scorecard()
    missing["factors"].pop("supplier_concentration")
    assert canonicalize_model_scorecard(missing)[1] == ["A2_BOTTLENECK_FACTORS_INVALID"]

    out_of_range = _scorecard()
    out_of_range["factors"]["chokepoint_severity"] = 6
    assert canonicalize_model_scorecard(out_of_range)[1] == ["A2_BOTTLENECK_FACTORS_INVALID"]


def test_a2_gate_exposes_bottleneck_context_without_expanding_pool():
    symbol = "600000.SH"
    snapshot = {
        "g0_symbols": [symbol],
        "g0_candidates": [{"symbol": symbol, "name": "测试公司", "amount": 2_000_000_000}],
        "FACTOR_SNAPSHOT": {symbol: {"technical_summary": {"relative_strength_score": 75}}},
        "RECENT_DAILY_BARS": {},
        "THS_INDUSTRY_MEMBERSHIP": {"records": []},
        "SECTOR_CYCLE_SNAPSHOT": {},
    }
    a1_output = {
        "active_research_pool": [{
            "symbol": symbol,
            "primary_theme": "theme-test",
            "structural_score": 80,
            "score_breakdown": {"business_mapping": 85},
            "evidence_confidence": 0.9,
            "source_refs": ["cninfo:600000.SH:page:1"],
        }]
    }

    result = screen_a2(snapshot, a1_output, minimum_identifiability_score=0, llm_top_n_per_theme=1)

    assert result.review_symbols == ()
    assert result.monitor_symbols == (symbol,)
    assert result.decisions[0]["status"] == "DATA_GAP"
    context = result.decisions[0]["bottleneck_context"]
    assert context["methodology_version"] == "liangjian-serenity-a2/1.0.0"
    assert context["source_refs"] == ["cninfo:600000.SH:page:1"]


def test_new_a2_context_demotes_focus_without_source_backed_bottleneck_fields():
    symbol = "600000.SH"
    theme = {
        "theme_id": "theme-test",
        "stage": "CONFIRMATION",
        "new_entry_policy": "ALLOW",
        "supporting_evidence": ["breadth"],
        "contradicting_evidence": ["crowding"],
    }
    base = {
        "symbol": symbol,
        "theme_id": "theme-test",
        "market_role": "CORE_ARMY",
        "identifiability_score": 80,
        "theme_score": 70,
    }
    snapshot = {
        "MIN_IDENTIFIABILITY_SCORE": 60,
        "A2_BOTTLENECK_CONTEXT": {
            symbol: {"source_refs": ["cninfo:600000.SH:page:1"]},
        },
    }

    output, changed = _apply_a2_lineage_policy(
        {"active_themes": [theme], "focus_pool": [base], "watch_only_pool": []},
        {"structural_themes": [{"theme_id": "theme-test"}]},
        snapshot,
    )

    assert changed == 1
    assert output["focus_pool"] == []
    assert "A2_BOTTLENECK_SCORECARD_MISSING" in output["watch_only_pool"][0]["reason_codes"]


def test_new_a2_context_keeps_a_source_backed_bottleneck_focus_item():
    symbol = "600000.SH"
    theme = {
        "theme_id": "theme-test",
        "stage": "CONFIRMATION",
        "new_entry_policy": "ALLOW",
        "supporting_evidence": ["breadth"],
        "contradicting_evidence": ["crowding"],
        "theme_score": 70,
    }
    item = {
        "symbol": symbol,
        "theme_id": "theme-test",
        "market_role": "CORE_ARMY",
        "identifiability_score": 80,
        "theme_score": 70,
        "supply_chain_role": "CONTROLS_SCARCE_LAYER",
        "scarce_layer": "关键设备",
        "value_chain_position": "上游设备",
        "bottleneck_scorecard": _scorecard(),
        "bottleneck_evidence": [
            {"claim": "验证周期长", "source_ref": "cninfo:600000.SH:page:1", "strength": "STRONG"},
            {"claim": "订单增长", "source_ref": "cninfo:600000.SH:page:2", "strength": "MEDIUM"},
        ],
        "missing_proof": "客户集中度仍需复核",
        "kill_switches": ["重复订单下降"],
        "source_refs": ["cninfo:600000.SH:page:1", "cninfo:600000.SH:page:2"],
    }
    snapshot = {
        "MIN_IDENTIFIABILITY_SCORE": 60,
        "A2_BOTTLENECK_CONTEXT": {
            symbol: {"source_refs": ["cninfo:600000.SH:page:1", "cninfo:600000.SH:page:2"]},
        },
    }

    assert _a2_bottleneck_reasons(item, snapshot) == []

    output, changed = _apply_a2_lineage_policy(
        {"active_themes": [theme], "focus_pool": [item], "watch_only_pool": []},
        {"structural_themes": [{"theme_id": "theme-test"}]},
        snapshot,
    )

    assert changed == 0, output
    assert [row["symbol"] for row in output["focus_pool"]] == [symbol]


def _configured_a2_case(*, capital_flow_available: bool = False) -> tuple[dict, dict, str]:
    symbol = "600001.SH"
    factor_scores = {
        name: {"score": 80, "available": True, "source_ref": f"fact:{name}"}
        for name in (
            "breadth", "turnover_share", "leader_structure", "tier_structure",
            "profit_effect", "catalyst_freshness", "index_chain_resonance",
            "agent_1_quality",
        )
    }
    weights = {name: 1 / 9 for name in (
        "breadth", "turnover_share", "capital_flow", "leader_structure",
        "tier_structure", "profit_effect", "catalyst_freshness",
        "index_chain_resonance", "agent_1_quality",
    )}
    snapshot = {
        "g0_symbols": [symbol],
        "g0_candidates": [{"symbol": symbol, "name": "核心公司", "amount": 2_000_000_000}],
        "FACTOR_SNAPSHOT": {symbol: {"technical_summary": {"relative_strength_score": 80}}},
        "THEME_SCORE_WEIGHTS": weights,
        "A2_FACTOR_COVERAGE_MINIMUM": 0.65,
        "CAPITAL_FLOW_SNAPSHOT": {
            "available": capital_flow_available,
            "reason_code": "OK" if capital_flow_available else "SOURCE_NOT_CONFIGURED",
            "by_symbol": {symbol: {"capital_flow_score": 80}} if capital_flow_available else {},
        },
    }
    item = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-cycle",
        "industry_chain_node": "node-cycle",
        "structural_score": 85,
        "data_quality_score": 90,
        "business_exposure": {"revenue_exposure_pct": 70, "source_ref": "cninfo:600001.SH:page:1"},
        "business_exposure_facts": [{"revenue_exposure_pct": 70, "evidence_ref": "cninfo:600001.SH:page:1"}],
        "source_refs": ["cninfo:600001.SH:page:1"],
        "a2_factor_scores": factor_scores,
    }
    return snapshot, {"active_research_pool": [item]}, symbol


def test_a2_market_core_route_allows_missing_bottleneck_card_and_normalizes_flow_gap():
    snapshot, upstream, symbol = _configured_a2_case()
    result = screen_a2(snapshot, upstream, minimum_identifiability_score=60, llm_top_n_per_theme=1)

    decision = result.decisions[0]
    assert result.review_symbols == ()
    assert result.monitor_symbols == (symbol,)
    assert decision["status"] == "DATA_GAP"
    assert decision["route"] == "MARKET_CORE"
    assert decision["eligible_routes"] == ("MARKET_CORE",)
    assert decision["bottleneck_status"] == "NOT_REQUIRED_FOR_MARKET_CORE"
    assert decision["a2_factor_scores"]["capital_flow"]["available"] is False
    assert decision["factor_coverage"]["ratio"] == pytest.approx(8 / 9)
    assert decision["critical_factor_coverage"]["sufficient"] is False


def test_a2_supply_chain_alpha_requires_complete_source_backed_scorecard():
    snapshot, upstream, symbol = _configured_a2_case(capital_flow_available=True)
    upstream["active_research_pool"][0].update({
        "supply_chain_role": "SUPPLIES_SCARCE_LAYER",
        "scarce_layer": "关键设备",
        "value_chain_position": "上游设备",
        "bottleneck_scorecard": _scorecard(),
        "bottleneck_evidence": [
            {"claim": "认证周期长", "source_ref": "cninfo:600001.SH:page:1", "strength": "STRONG"},
            {"claim": "订单兑现", "source_ref": "cninfo:600001.SH:page:2", "strength": "MEDIUM"},
        ],
        "missing_proof": "客户集中度待更新",
        "kill_switches": ["订单下降"],
        "source_refs": ["cninfo:600001.SH:page:1", "cninfo:600001.SH:page:2"],
    })
    result = screen_a2(snapshot, upstream, minimum_identifiability_score=60, llm_top_n_per_theme=1)

    decision = result.decisions[0]
    assert set(decision["eligible_routes"]) == {"MARKET_CORE", "SUPPLY_CHAIN_ALPHA"}
    assert decision["route_eligibility"]["SUPPLY_CHAIN_ALPHA"]["eligible"] is True


def test_a2_factor_coverage_below_65_percent_is_watch_only_even_when_score_is_high():
    snapshot, upstream, symbol = _configured_a2_case()
    snapshot["THEME_SCORE_WEIGHTS"] = {
        "breadth": 0.0625,
        "turnover_share": 0.0625,
        "capital_flow": 0.50,
        "leader_structure": 0.0625,
        "tier_structure": 0.0625,
        "profit_effect": 0.0625,
        "catalyst_freshness": 0.0625,
        "index_chain_resonance": 0.0625,
        "agent_1_quality": 0.0625,
    }
    result = screen_a2(snapshot, upstream, minimum_identifiability_score=60, llm_top_n_per_theme=1)

    assert result.review_symbols == ()
    assert result.monitor_symbols == (symbol,)
    assert "A2_FACTOR_COVERAGE_BELOW_MINIMUM" in result.decisions[0]["reason_codes"]


def test_a2_invented_capital_flow_demotes_theme_and_focus_when_source_is_unavailable():
    weights = {
        "breadth": 1 / 9,
        "turnover_share": 1 / 9,
        "capital_flow": 1 / 9,
        "leader_structure": 1 / 9,
        "tier_structure": 1 / 9,
        "profit_effect": 1 / 9,
        "catalyst_freshness": 1 / 9,
        "index_chain_resonance": 1 / 9,
        "agent_1_quality": 1 / 9,
    }
    theme = {
        "theme_id": "theme-cycle",
        "stage": "CONFIRMATION",
        "new_entry_policy": "ALLOW",
        "supporting_evidence": ["industry breadth"],
        "contradicting_evidence": ["capital flow source unavailable"],
        "score_breakdown": {name: 70 for name in weights},
        "theme_score": 70,
    }
    focus = {
        "symbol": "600001.SH",
        "theme_id": "theme-cycle",
        "market_role": "CORE_ARMY",
        "identifiability_score": 80,
        "theme_score": 70,
    }

    output, changed = _apply_a2_lineage_policy(
        {"active_themes": [theme], "focus_pool": [focus], "watch_only_pool": []},
        {"structural_themes": [{"theme_id": "theme-cycle"}]},
        {
            "MIN_IDENTIFIABILITY_SCORE": 60,
            "THEME_SCORE_WEIGHTS": weights,
            "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        },
    )

    assert changed == 1
    assert output["focus_pool"] == []
    assert "A2_CAPITAL_FLOW_SCORE_INVENTED" in output["active_themes"][0]["reason_codes"]
    assert "A2_THEME_LINEAGE_INVALID" in output["watch_only_pool"][0]["reason_codes"]
