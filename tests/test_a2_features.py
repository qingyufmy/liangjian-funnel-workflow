from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.a2_features import build_a2_feature_snapshot
from liangjian_funnel.pipeline.a2_features import _percentiles
from liangjian_funnel.pipeline.deterministic import screen_a2


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
