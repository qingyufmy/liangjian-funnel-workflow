from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.a2_features import build_a2_feature_snapshot
from liangjian_funnel.pipeline.a2_features import _percentiles
from liangjian_funnel.pipeline.deterministic import _a2_behavior_evidence, screen_a2


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _bars(growth: float) -> list[dict]:
    return [
        {
            "date_ms": int((NOW - timedelta(days=30 - index)).timestamp() * 1000),
            "close_price": 10 * (1 + growth) ** index,
        }
        for index in range(31)
    ]


def _membership(kind: str) -> dict:
    code_key = "industry_thscode" if kind == "industry" else "concept_thscode"
    name_key = "industry_name" if kind == "industry" else "concept_name"
    return {
        "available": True,
        "records": [
            {"thscode": symbol, "memberships": [{code_key: "881001.TI", name_key: "主线"}]}
            for symbol in ("600001.SH", "000002.SZ", "300003.SZ")
        ],
    }


def test_a2_features_materialize_tier_leader_and_chain_without_heat_as_leader() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[
            {"symbol": "600001.SH", "amount": 1000},
            {"symbol": "000002.SZ", "amount": 500},
            {"symbol": "300003.SZ", "amount": 100},
        ],
        daily_bars={"600001.SH": _bars(0.02), "000002.SZ": _bars(0.01), "300003.SZ": _bars(-0.01)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={
            "records": [{
                "date": "2026-08-29",
                "boards": {"two_board": [{"thscode": "600001.SH", "board_num": 2}]},
            }],
        },
        dragon_tiger_snapshot={"records": [{"thscode": "600001.SH"}]},
        attention_snapshot={"records": [{"thscode": "000002.SZ"}]},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={
            "available": True,
            "source_id": "TEST_VENDOR",
            "provider_method": "VENDOR_DERIVED",
            "by_symbol": {
                "600001.SH": {"available": True, "capital_flow_score": 90, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
                "000002.SZ": {"available": True, "capital_flow_score": 50, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
                "300003.SZ": {"available": True, "capital_flow_score": 10, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
            },
        },
        as_of=NOW,
    )

    assert snapshot["available"] is True
    first = snapshot["by_symbol"]["600001.SH"]
    assert first["tier"] == "T2"
    assert first["leader_role"] == "EMOTION_LEADER"
    assert first["factors"]["index_chain_resonance"]["available"] is True
    # Attention is only a bounded confirmation.  It cannot overwrite the
    # cross-sectional role with an artificial leader label.
    assert snapshot["by_symbol"]["000002.SZ"]["leader_role"] != "EMOTION_LEADER"
    weekly = snapshot["theme_metrics"]["INDUSTRY:881001.TI"]
    assert weekly["weekly_momentum_state"] == "PERSISTENT"
    assert weekly["weekly_confirmation_score"] is not None
    assert first["factors"]["weekly_confirmation"]["available"] is True


def test_missing_capital_flow_stays_unavailable_while_other_factors_remain_auditable() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.01)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )

    row = snapshot["by_symbol"]["600001.SH"]
    assert row["factors"]["capital_flow"]["available"] is False
    assert row["factors"]["capital_flow"]["score"] is None
    assert row["factors"]["tier_structure"]["available"] is True
    assert row["factors"]["tier_structure"]["score"] == 0
    assert row["factors"]["tier_structure"]["tier"] == "NONE"
    assert row["factors"]["trend_strength_proxy"]["available"] is True
    assert row["factors"]["tier_structure"]["source"] != row["factors"]["trend_strength_proxy"]["source"]
    assert snapshot["capital_flow_available"] is False
    assert snapshot["data_sufficiency_state"] == "DEGRADED"
    assert snapshot["route_sufficiency"]["MARKET_CORE"]["available"] is True
    assert "capital_flow" in snapshot["optional_missing_factors"]


def test_sector_board_flow_enriches_a2_without_claiming_individual_flow() -> None:
    sector_cycle = {
        "sector_health_snapshot": {
            "by_taxonomy": {
                "industry": {
                    "sectors": [{
                        "taxonomy_code": "881001.TI",
                        "taxonomy_name": "主线",
                        "capital_flow": {
                            "available": True,
                            "availability_state": "OBSERVED_VALUE",
                            "reason_code": "OK",
                            "score": 82.5,
                            "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
                            "source_scope": "SECTOR",
                            "provider_method": "VENDOR_DERIVED_RANK_PERCENTILE",
                        },
                    }],
                },
                "concept": {"sectors": []},
            },
        },
    }
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.01)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=sector_cycle,
        capital_flow_snapshot={"available": False, "reason_code": "HISTORICAL_SYMBOL_FLOW_CACHE_MISSING"},
        as_of=NOW,
    )

    factor = snapshot["by_symbol"]["600001.SH"]["factors"]["capital_flow"]
    assert factor["available"] is True
    assert factor["score"] == 82.5
    assert factor["source"] == "EASTMONEY_BOARD_CAPITAL_FLOW"
    assert factor["source_scope"] == "SECTOR"
    assert snapshot["capital_flow_available"] is True
    assert snapshot["capital_flow_method"] == "VENDOR_DERIVED_SECTOR_RANK_PERCENTILE"


def test_failed_ladder_source_is_unknown_even_when_trend_is_available() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.02)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot=None,
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )

    row = snapshot["by_symbol"]["600001.SH"]
    assert row["factors"]["tier_structure"]["available"] is False
    assert row["factors"]["tier_structure"]["score"] is None
    assert row["factors"]["tier_structure"]["tier"] == "UNKNOWN"
    assert row["factors"]["trend_strength_proxy"]["available"] is True
    assert snapshot["ladder_dataset_state"] == "NOT_CONFIGURED"


def test_cross_section_percentiles_average_ties() -> None:
    scores = _percentiles({"600001.SH": 1.0, "000002.SZ": 1.0, "300003.SZ": 2.0})

    assert scores["600001.SH"] == scores["000002.SZ"] == 25.0
    assert scores["300003.SZ"] == 100.0


def test_explicit_reference_universe_drives_market_denominators() -> None:
    """A2 candidates are scored against the full reference cross-section."""

    reference_symbols = ("600001.SH", "000002.SZ", "300003.SZ")
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": reference_symbols[0], "amount": 300}],
        daily_bars={reference_symbols[0]: _bars(0.02)},
        reference_candidates=[
            {"symbol": reference_symbols[0], "amount": 300},
            {"symbol": reference_symbols[1], "amount": 200},
            {"symbol": reference_symbols[2], "amount": 100},
        ],
        reference_daily_bars={
            symbol: _bars(growth)
            for symbol, growth in zip(reference_symbols, (0.02, 0.01, -0.01), strict=True)
        },
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"available": True, "records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )

    assert snapshot["symbol_count"] == snapshot["candidate_symbol_count"] == 1
    assert snapshot["reference_symbol_count"] == 3
    assert snapshot["denominator_scope"] == "FULL_MARKET_REFERENCE"
    # The strongest candidate ranks first among all three reference symbols;
    # a one-symbol candidate-only denominator would incorrectly return 50.
    candidate = snapshot["by_symbol"][reference_symbols[0]]
    assert candidate["factors"]["trend_strength_proxy"]["trend_percentile"] == 100.0
    assert candidate["factors"]["leader_structure"]["liquidity_percentile"] == 100.0
    metric = snapshot["theme_metrics"]["INDUSTRY:881001.TI"]
    assert metric["member_count"] == metric["reference_member_count"] == 3
    assert metric["candidate_member_count"] == 1


def test_explicit_reference_coverage_cannot_claim_sufficient() -> None:
    reference_symbols = ("600001.SH", "000002.SZ", "300003.SZ")
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": reference_symbols[0], "amount": 300}],
        daily_bars={reference_symbols[0]: _bars(0.02)},
        reference_candidates=[
            {"symbol": symbol, "amount": 100}
            for symbol in reference_symbols
        ],
        # Only one of three reference bars is available.
        reference_daily_bars={reference_symbols[0]: _bars(0.02)},
        # Only one of three reference identities is available.
        industry_membership={
            "available": True,
            "records": [{"thscode": reference_symbols[0], "memberships": [{"industry_thscode": "881001.TI", "industry_name": "主线"}]}],
        },
        concept_membership=None,
        ladder_snapshot={"available": True, "records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )

    assert snapshot["reference_daily_bar_coverage"] == pytest.approx(1 / 3, rel=1e-6)
    assert snapshot["reference_identity_coverage"] == pytest.approx(1 / 3, rel=1e-6)
    assert snapshot["data_sufficiency_state"] in {"DEGRADED", "INSUFFICIENT"}
    assert snapshot["data_sufficiency_state"] != "SUFFICIENT"
    assert snapshot["available"] is False


def test_20260828_grain_agriculture_chemical_fixture_keeps_sector_core_candidates_without_capital_flow() -> None:
    """A point-in-time sector ladder can support a non-limit-up core/army row.

    This is deliberately a small replay fixture for the A2 contract, not a
    synthetic capital-flow score: the only unavailable source is capital flow,
    while bars, taxonomy, turnover/breadth and the latest ladder are observed.
    """

    as_of = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)
    symbols = ("600001.SH", "600002.SH", "000001.SZ", "300001.SZ")
    bars = {
        symbol: [
            {
                "date_ms": int((as_of - timedelta(days=30 - index)).timestamp() * 1000),
                "close_price": 10 * (1 + growth) ** index,
            }
            for index in range(31)
        ]
        for symbol, growth in zip(symbols, (0.10, 0.08, 0.06, 0.04), strict=True)
    }
    memberships = {
        "available": True,
        "records": [
            {
                "thscode": symbols[0],
                "memberships": [{"industry_thscode": "I-GRAIN", "industry_name": "粮食农业"}],
            },
            {
                "thscode": symbols[1],
                "memberships": [{"industry_thscode": "I-GRAIN", "industry_name": "粮食农业"}],
            },
            {
                "thscode": symbols[2],
                "memberships": [{"industry_thscode": "I-CHEM", "industry_name": "化工"}],
            },
            {
                "thscode": symbols[3],
                "memberships": [{"industry_thscode": "I-CHEM", "industry_name": "化工"}],
            },
        ],
    }
    features = build_a2_feature_snapshot(
        candidates=[
            {"symbol": symbols[0], "amount": 700},
            {"symbol": symbols[1], "amount": 1_000},
            {"symbol": symbols[2], "amount": 500},
            {"symbol": symbols[3], "amount": 300},
        ],
        daily_bars=bars,
        industry_membership=memberships,
        concept_membership=None,
        ladder_snapshot={
            "available": True,
            "records": [{
                "date": "2026-08-28",
                "boards": {
                    "two_board": [{"thscode": symbols[0], "board_num": 2}],
                    "one_board": [{"thscode": symbols[2], "board_num": 1}],
                },
            }],
        },
        dragon_tiger_snapshot={"available": True, "records": []},
        attention_snapshot={"available": True, "records": []},
        sector_cycle_snapshot={
            "history_metrics": {
                "monthly_rotation_candidates": [
                    {"industry_thscode": "I-GRAIN", "rotation_score": 82},
                    {"industry_thscode": "I-CHEM", "rotation_score": 78},
                ],
            },
        },
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=as_of,
    )

    assert features["data_sufficiency_state"] == "DEGRADED"
    assert features["route_sufficiency"]["MARKET_CORE"]["available"] is True
    assert features["optional_missing_factors"] == ["capital_flow"]
    agriculture_core = features["by_symbol"][symbols[1]]
    assert agriculture_core["tier"] == "NONE"
    assert agriculture_core["tier_structure"]["availability_state"] == "OBSERVED_ABSENT"
    assert agriculture_core["leader_role"] == "CORE_ARMY"
    assert agriculture_core["leader_structure"]["tier_confirmation_mode"] == "SECTOR_LADDER_RELATIVE_LIQUIDITY"
    assert agriculture_core["leader_structure"]["sector_ladder_support"] is True

    a1_rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-20260828-cycle",
            "industry_chain_node": "node-grain-or-chem",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"fixture:business:{symbol}"},
            "business_exposure_facts": [{"revenue_exposure_pct": 65, "evidence_ref": f"fixture:business:{symbol}"}],
            "source_refs": [f"fixture:a1:{symbol}"],
        }
        for symbol in symbols
    ]
    deterministic_snapshot = {
        "g0_candidates": [
            {"symbol": symbol, "amount": amount}
            for symbol, amount in zip(symbols, (700, 1_000, 500, 300), strict=True)
        ],
        "A2_FACTOR_SNAPSHOT": features,
        "TIER_STRUCTURE_SNAPSHOT": {
            "available": True,
            "by_symbol": {
                symbol: features["by_symbol"][symbol]["factors"]["tier_structure"]
                for symbol in symbols
            },
        },
        "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        "THS_INDUSTRY_MEMBERSHIP": memberships,
        "A2_THEME_METRICS": features["theme_metrics"],
        "SECTOR_CYCLE_SNAPSHOT": features.get("sector_cycle_snapshot", {}),
    }
    result = screen_a2(
        deterministic_snapshot,
        {"active_research_pool": a1_rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=4,
    )
    by_symbol = {decision["symbol"]: decision for decision in result.decisions}
    assert set(by_symbol) == set(symbols)
    assert by_symbol[symbols[1]]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[symbols[1]]["route"] == "MARKET_CORE"
    assert by_symbol[symbols[1]]["data_sufficiency_state"] == "DEGRADED"
    assert "capital_flow" in by_symbol[symbols[1]]["missing_optional_factors"]
    assert by_symbol[symbols[1]]["a2_factor_scores"]["capital_flow"]["score"] is None


def test_a2_all_market_sources_missing_is_data_gap_not_no_opportunity() -> None:
    """No observed market fact must remain a fail-closed DATA_GAP."""

    symbol = "600001.SH"
    row = {
        "symbol": symbol,
        "candidate_id": "a1:missing-market-facts",
        "primary_theme": "theme-missing-facts",
        "industry_chain_node": "node-missing-facts",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": "fixture:business"},
        "business_exposure_facts": [{"revenue_exposure_pct": 65, "evidence_ref": "fixture:business"}],
    }
    result = screen_a2(
        {
            "g0_candidates": [{"symbol": symbol, "amount": 500}],
            "A2_FACTOR_SNAPSHOT": {"available": False, "reason_code": "SOURCE_FAILED"},
            "TIER_STRUCTURE_SNAPSHOT": {"available": False, "reason_code": "SOURCE_FAILED"},
            "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_FAILED"},
        },
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
    )
    decision = result.decisions[0]
    assert decision["status"] == "DATA_GAP"
    assert result.review_symbols == ()
    assert "A2_MARKET_FACTS_INSUFFICIENT" in decision["reason_codes"]
    assert "A2_NO_ROUTE_READY" in decision["reason_codes"]
    assert result.summary["data_sufficiency_state"] == "INSUFFICIENT"


def test_deterministic_a2_consumes_materialized_real_factor_projection() -> None:
    capital = {
        "available": True,
        "source_id": "TEST_VENDOR_DERIVED",
        "provider_method": "VENDOR_DERIVED",
        "by_symbol": {
            "600001.SH": {
                "available": True,
                "capital_flow_score": 90,
                "availability_state": "OBSERVED_VALUE",
                "reason_code": "OK",
            },
        },
    }
    factors = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.02)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot=capital,
        as_of=NOW,
    )
    snapshot = {
        "CAPITAL_FLOW_SNAPSHOT": capital,
        "A2_FACTOR_SNAPSHOT": factors,
        "TIER_STRUCTURE_SNAPSHOT": factors,
        "A2_THEME_METRICS": factors["theme_metrics"],
        "g0_candidates": [{"symbol": "600001.SH", "amount": 1000, "change_ratio_pct": 2}],
    }
    a1 = {
        "active_research_pool": [{
            "symbol": "600001.SH",
            "candidate_id": "a1:600001.SH",
            "primary_theme": "theme-main",
            "industry_chain_node": "node-main",
            "structural_score": 85,
            "data_quality_score": 90,
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": "fixture:business"},
            "business_exposure_facts": [{"revenue_exposure_pct": 65, "evidence_ref": "fixture:business"}],
            "source_refs": ["fixture:a1"],
        }],
    }

    decision = screen_a2(snapshot, a1, minimum_identifiability_score=0, llm_top_n_per_theme=1).decisions[0]
    assert decision["a2_factor_scores"]["capital_flow"]["available"] is True
    assert decision["a2_factor_scores"]["capital_flow"]["score"] == 90
    assert decision["a2_factor_scores"]["tier_structure"]["available"] is True
    assert decision["a2_factor_scores"]["leader_structure"]["available"] is True
    assert decision["a2_factor_scores"]["weekly_confirmation"]["available"] is True
    assert decision["a2_factor_scores"]["weekly_confirmation"]["weekly_momentum_state"] == "PERSISTENT"


def test_deterministic_a2_keeps_missing_capital_as_degraded_optional_fact() -> None:
    capital = {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED", "by_symbol": {}}
    factors = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.02)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot=capital,
        as_of=NOW,
    )
    snapshot = {
        "CAPITAL_FLOW_SNAPSHOT": capital,
        "A2_FACTOR_SNAPSHOT": factors,
        "TIER_STRUCTURE_SNAPSHOT": factors,
        "A2_THEME_METRICS": factors["theme_metrics"],
        "g0_candidates": [{"symbol": "600001.SH", "amount": 1000, "change_ratio_pct": 2}],
    }
    a1 = {
        "active_research_pool": [{
            "symbol": "600001.SH",
            "candidate_id": "a1:600001.SH",
            "primary_theme": "theme-main",
            "industry_chain_node": "node-main",
            "structural_score": 85,
            "data_quality_score": 90,
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": "fixture:business"},
            "business_exposure_facts": [{"revenue_exposure_pct": 65, "evidence_ref": "fixture:business"}],
            "source_refs": ["fixture:a1"],
        }],
    }

    result = screen_a2(snapshot, a1, minimum_identifiability_score=0, llm_top_n_per_theme=1)
    decision = result.decisions[0]
    assert decision["status"] == "REVIEW_CANDIDATE"
    assert decision["sent_to_llm"] is True
    assert decision["data_sufficiency_state"] == "DEGRADED"
    assert "capital_flow" not in decision["critical_factor_coverage"]["candidate_factors"]
    assert decision["critical_factor_coverage"]["sufficient"] is True
    assert "capital_flow" in decision["missing_optional_factors"]
    assert "A2_OPTIONAL_FACTS_DEGRADED" in decision["reason_codes"]
    assert result.summary["data_sufficiency_state"] == "DEGRADED"
    assert result.monitor_symbols == ()


@pytest.mark.parametrize(
    "exposure_provenance",
    [
        {"evidence_basis": "MAIN_BUSINESS_BREAKDOWN"},
        {"extraction_method": "REVENUE_COMPOSITION_TABLE_分行业"},
        {"extraction_method": "REVENUE_COMPOSITION_TABLE_分产品"},
    ],
    ids=("evidence-basis", "cninfo-industry-table", "cninfo-product-table"),
)
def test_a2_trend_route_accepts_canonical_business_exposure_without_fact_list(
    exposure_provenance: dict[str, str],
) -> None:
    """The production A1 projection may carry only canonical exposure data.

    ``business_exposure_facts`` is an additive storage projection, not a
    prerequisite for a valid A1 business disclosure.  A source-backed
    ``business_exposure`` with an allowed ``evidence_basis`` must therefore
    supply A2's industry-logic facet and let the medium-term/right-strength
    evidence resolve the stock as TREND.
    """

    symbols = ("600001.SH", "000002.SZ", "300003.SZ", "600004.SH")
    growth = (0.10, 0.08, 0.06, 0.04)
    bars = {
        symbol: [
            {
                "date_ms": int((NOW - timedelta(days=30 - index)).timestamp() * 1000),
                "close_price": 10 * (1 + rate) ** index,
            }
            for index in range(31)
        ]
        for symbol, rate in zip(symbols, growth, strict=True)
    }
    membership = {
        "available": True,
        "records": [
            {
                "thscode": symbol,
                "memberships": [{"industry_thscode": "881001.TI", "industry_name": "主线"}],
            }
            for symbol in symbols
        ],
    }
    features = build_a2_feature_snapshot(
        candidates=[
            {"symbol": symbol, "amount": 100_000_000 * index}
            for index, symbol in enumerate(symbols, start=1)
        ],
        daily_bars=bars,
        industry_membership=membership,
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )
    snapshot = {
        "g0_candidates": [
            {"symbol": symbol, "amount": 100_000_000 * index}
            for index, symbol in enumerate(symbols, start=1)
        ],
        "A2_FACTOR_SNAPSHOT": features,
        "TIER_STRUCTURE_SNAPSHOT": features,
        "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        "THS_INDUSTRY_MEMBERSHIP": membership,
        "A2_THEME_METRICS": features["theme_metrics"],
        "SECTOR_CYCLE_SNAPSHOT": features.get("sector_cycle_snapshot", {}),
    }
    a1 = {
        "active_research_pool": [
            {
                "symbol": symbol,
                "candidate_id": f"a1:{symbol}",
                "primary_theme": "theme-main",
                "industry_chain_node": "node-main",
                "business_exposure": {
                    "business_name": "主营业务",
                    "revenue_exposure_pct": 80,
                    "source_ref": f"fixture:business:{symbol}",
                    **exposure_provenance,
                },
                "source_refs": [f"fixture:a1:{symbol}"],
            }
            for symbol in symbols
        ],
    }

    result = screen_a2(
        snapshot,
        a1,
        minimum_identifiability_score=0,
        llm_top_n_per_theme=len(symbols),
    )
    by_symbol = {decision["symbol"]: decision for decision in result.decisions}
    trend = by_symbol["600001.SH"]

    assert trend["status"] == "REVIEW_CANDIDATE"
    assert trend["stock_behavior_type"] == "TREND"
    assert trend["route_permission"] == ["TREND_MA5", "MA520_SWING"]
    assert trend["behavior_type_decision"]["decision_basis"]["trend_qualified"] is True
    assert "A2_DATA_GAP_INDUSTRY_LOGIC" not in trend["behavior_type_decision"]["data_gaps"]


def test_a2_unmapped_broker_gold_uses_symbol_taxonomy_as_rotation_theme() -> None:
    symbol = "600001.SH"
    membership = {
        "available": True,
        "records": [{
            "thscode": symbol,
            "memberships": [{"industry_thscode": "881001.TI", "industry_name": "真实行业"}],
        }],
    }
    features = build_a2_feature_snapshot(
        candidates=[{"symbol": symbol, "amount": 1_000_000_000}],
        daily_bars={symbol: _bars(0.03)},
        industry_membership=membership,
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )
    result = screen_a2(
        {
            "g0_candidates": [{"symbol": symbol, "amount": 1_000_000_000}],
            "A2_FACTOR_SNAPSHOT": features,
            "TIER_STRUCTURE_SNAPSHOT": features,
            "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
            "THS_INDUSTRY_MEMBERSHIP": membership,
            "A2_THEME_METRICS": features["theme_metrics"],
        },
        {
            "active_research_pool": [{
                "symbol": symbol,
                "candidate_id": f"a1:{symbol}",
                "primary_theme": "UNMAPPED",
                "research_route": "BROKER_GOLD_DIRECT",
                "downstream_trade_eligible": True,
            }],
        },
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
    )

    decision = result.decisions[0]
    assert decision["theme_id"] == "INDUSTRY:881001.TI"
    assert decision["top_rotation_theme"] is True
    assert decision["sent_to_llm"] is True


def test_a2_rejects_unproven_business_text_but_keeps_market_industry_fact_independent() -> None:
    evidence = _a2_behavior_evidence(
        item={
            "primary_theme": "TH_AI",
            "industry_chain_node": "node-ai",
            "business_exposure": {
                "business_name": "模型生成描述",
                "revenue_exposure_pct": 90,
                "extraction_method": "MODEL_NARRATIVE",
            },
        },
        factor_scores={
            "index_chain_resonance": {
                "available": True,
                "score": 72,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": "881001.TI",
                "source": "POINT_IN_TIME_TAXONOMY_AGGREGATE",
            },
            "weekly_confirmation": {
                "available": True,
                "score": 65,
                "source": "POINT_IN_TIME_WEEKLY_TAXONOMY_AGGREGATE",
            },
            "tier_structure": {"available": True, "availability_state": "OBSERVED_ABSENT", "ladder_height": 0},
        },
        identifiability=80,
        minimum_identifiability_score=60,
        relative=75,
        liquidity=80,
        legacy_role="TREND_LEADER",
        as_of=NOW.isoformat(),
    )

    assert evidence["supply_chain_position"]["available"] is False
    assert evidence["industry_logic"]["available"] is True
    assert evidence["industry_logic"]["met"] is True
    assert evidence["industry_logic"]["value"]["taxonomy_code"] == "881001.TI"
