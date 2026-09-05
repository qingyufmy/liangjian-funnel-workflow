from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from liangjian_funnel.pipeline.deterministic import (
    _financial_quality,
    _has_business_evidence,
    _read_metric_payload,
    _specialize_market_role,
    _valuation_factor,
    local_active_items,
    local_monitor_items,
    screen_a1,
    screen_a2,
    screen_a3,
)
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore
from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 27, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


def test_a1_financial_quality_recognizes_production_indicator_names_and_percent_scale() -> None:
    score, features = _financial_quality({
        "indicators": [
            {"index_id": "sale_net_interest_ratio", "value": "12.5"},
            {"index_id": "assets_debt_ratio", "value": "38.0"},
            {"index_id": "net_profit_cash_content", "value": "139.27"},
            {"index_id": "calculate_operating_income_yoy_growth_ratio", "value": "10.2"},
            {"index_id": "calculate_parent_holder_net_profit_yoy_growth_ratio", "value": "37.2"},
        ],
    })

    assert score > 0
    assert features["net_margin"] == 12.5
    assert features["debt_ratio"] == 38.0
    assert features["cashflow_quality"] == pytest.approx(1.3927)
    assert features["revenue_growth"] == 10.2
    assert features["profit_growth"] == 37.2


def test_a2_recognizes_audited_h1_main_business_route_without_inventing_exposure() -> None:
    assert _has_business_evidence({
        "research_route": "HALF_YEAR_FUNDAMENTAL",
        "disclosed_business_match": {"raw_disclosure_available": True},
        "half_year_support": {"supported": True},
        "source_refs": ["cninfo:600000:2026h1:page:12"],
        "business_exposure": None,
    }) is True


def test_a1_negative_expected_growth_does_not_receive_positive_growth_valuation_credit() -> None:
    fundamentals = {
        "600001.SH": {
            "indicators": [
                {"index_id": "pe_ttm", "value": 20},
                {"index_id": "expected_growth", "value": -25},
            ],
            "source_refs": ["cninfo:600001:2026q2"],
        }
    }

    score, available, _, reason = _valuation_factor(fundamentals, "600001.SH", {})

    assert available is True
    assert score == 30.0
    assert reason == "A1_EXPECTED_GROWTH_NON_POSITIVE"


def test_a2_ladder_fact_metadata_survives_deterministic_projection() -> None:
    projected = _read_metric_payload(
        {
            "available": True,
            "score": 0,
            "source": "HITHINK_LIMIT_UP_LADDER",
            "availability_state": "OBSERVED_ABSENT",
            "ladder_height": 0,
            "tier": "NONE",
        },
        source="TIER_STRUCTURE_SNAPSHOT",
        source_refs=(),
        ratio_hint=False,
    )

    assert projected is not None
    assert projected["available"] is True
    assert projected["ladder_height"] == 0
    assert projected["availability_state"] == "OBSERVED_ABSENT"
    assert _specialize_market_role("LEADER", {"tier_structure": projected}) == "TREND_LEADER"


def test_a2_observed_two_board_specializes_to_emotion_leader() -> None:
    assert _specialize_market_role(
        "LEADER",
        {
            "tier_structure": {
                "available": True,
                "availability_state": "OBSERVED_VALUE",
                "ladder_height": 2,
            }
        },
    ) == "EMOTION_LEADER"


def _frame(*, daily: bool = False) -> dict:
    return {
        "ready": True,
        "latest": {"close": 10.4, "low": 10.0, "closed": True},
        "ma_alignment": "BULL_STACK",
        "ma_event": "GOLDEN_CROSS" if daily else "NONE",
        "ma_bias": {"close_vs_ma20_pct": 0.02},
        "moving_averages": {"ma5": 10.3, "ma10": 10.2, "ma20": 10.0, "ma60": 9.5},
        "previous_moving_averages": {"ma5": 10.2, "ma10": 10.1, "ma20": 9.9, "ma60": 9.4},
        "ma_slopes": {"ma5": 0.01, "ma10": 0.01, "ma20": 0.01, "ma60": 0.01},
    }


def _snapshot(symbol_count: int = 8) -> dict:
    symbols = [f"{600000 + index:06d}.SH" for index in range(symbol_count)]
    industry_rows = []
    candidates = []
    fundamentals = {}
    evidence = {}
    factors = {}
    levels = {}
    flags = {}
    for index, symbol in enumerate(symbols):
        code = "884001.TI" if index < symbol_count - 1 else "884999.TI"
        name = "算力设备" if code == "884001.TI" else "食品加工"
        industry_rows.append({
            "thscode": symbol,
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": code, "industry_name": name}],
        })
        candidates.append({"symbol": symbol, "name": f"公司{index}", "amount": 5_000_000_000 - index * 100_000_000})
        fundamentals[symbol] = {
            "dataset_coverage": {"core_reports_complete": True, "indicators_available": True},
            "indicators": [
                {"index_id": "index_weighted_avg_roe", "value": 18 - index * 0.1},
                {"index_id": "sale_gross_margin", "value": 42 - index * 0.1},
                {"index_id": "net_profit_cash_content", "value": 1.1},
            ],
        }
        evidence[symbol] = {
            "available": index != symbol_count - 2,
            "evidence": ([{
                "source_ref": f"cninfo:{symbol}:page:1",
                "page_number": 1,
                "publish_time": "2026-04-30",
                "text": "算力设备业务占公司营业收入65.5%",
            }] if index != symbol_count - 2 else []),
        }
        factors[symbol] = {
            "ready": True,
            "a3_ready": True,
            "a3_reasons": [],
            "timeframes": {
                "monthly": _frame(),
                "weekly": _frame(),
                "daily": _frame(daily=True),
                "120m": _frame(),
                "15m": _frame(),
                "5m": _frame(),
            },
            "technical_summary": {"relative_strength_score": 80 - index},
        }
        levels[symbol] = {
            "available": True,
            "trigger_zone": {"low": 10, "high": 10.5},
            "invalidation": 9.5,
            "stop_distance_pct": 0.05,
            "first_resistance": 12,
            "reward_risk": 3.0,
            "no_chase_price": 10.8,
        }
        flags[symbol] = {"available": True, "tradable": True, "exclusion_reasons": []}
    return {
        "DETERMINISTIC_RESEARCH_V2_ENABLED": True,
        "g0_symbols": symbols,
        "g0_candidates": candidates,
        "snapshot_manifest": {"as_of": NOW.isoformat(), "source_checksums": {"daily": "d" * 64}},
        "THS_INDUSTRY_CATALOG": {
            "available": True,
            "records": [
                {"thscode": "884001.TI", "name": "算力设备"},
                {"thscode": "884999.TI", "name": "食品加工"},
            ],
        },
        "THS_CONCEPT_CATALOG": {"available": True, "records": []},
        "THS_INDUSTRY_MEMBERSHIP": {"available": True, "records": industry_rows},
        "THS_CONCEPT_MEMBERSHIP": {"available": True, "records": []},
        "COMPANY_FUNDAMENTALS": fundamentals,
        "MAIN_BUSINESS_EVIDENCE": evidence,
        "RISK_EVENTS": {"available": True, "records": []},
        "TRADABILITY_FLAGS": flags,
        "FACTOR_SNAPSHOT": factors,
        "PRICE_LEVELS": levels,
        "MARKET_ATTENTION_SNAPSHOT": {"available": True, "records": []},
        "DRAGON_TIGER_SNAPSHOT": {"available": True, "records": []},
        "MACRO_POLICY_FEED": {"official_documents": [{"fact_id": "policy-ref"}]},
        "SCORE_WEIGHTS": {
            "structural_theme": 0.2,
            "business_mapping": 0.2,
            "barrier_and_bottleneck": 0.15,
            "financial_quality": 0.2,
            "cash_flow_quality": 0.1,
            "evidence_quality": 0.15,
        },
        "A1_MINIMUMS": {"minimum_score": 65, "minimum_data_quality": 75},
        "STRICT_AGENT_RULES": False,
        "config_hash": "c" * 64,
    }


def _discovery() -> dict:
    return {
        "structural_themes": [{"theme_id": "theme-compute", "display_name": "算力", "source_refs": ["policy-ref"]}],
        "industry_chain_graph": [{
            "node_id": "node-compute-device",
            "theme_ids": ["theme-compute"],
            "demand_driver": "算力设备",
            "source_refs": ["policy-ref"],
        }],
        "taxonomy_links": [{
            "node_id": "node-compute-device",
            "industry_thscodes": ["884001.TI"],
            "concept_thscodes": [],
        }],
    }


def test_a1_evaluates_every_g0_and_keeps_local_research_coverage():
    snapshot = _snapshot()
    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=5, llm_top_n_per_theme=2)

    assert result.summary["evaluated_count"] == len(snapshot["g0_symbols"])
    assert len(result.review_symbols) == 2
    assert len({item["symbol"] for item in result.decisions}) == len(snapshot["g0_symbols"])
    assert next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][-1])["status"] == "OUTSIDE_THEME"
    assert next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][-2])["status"] == "LOCAL_MONITOR"
    assert all(item["sent_to_llm"] is (item["symbol"] in result.review_symbols) for item in result.decisions)
    # The two per-theme representatives are sent to the model; the remaining
    # four rows with exact disclosed business exposure stay in the local A1
    # research layer instead of being discarded by the review budget.
    assert len(local_active_items(result)) == 4


def test_a1_accepts_normalized_links_from_the_stable_theme_registry():
    snapshot = _snapshot(2)
    discovery = _discovery()
    discovery["taxonomy_links"] = [{
        "node_id": "node-compute-device",
        "theme_id": "theme-compute",
        "taxonomy": "INDUSTRY",
        "taxonomy_code": "884001.TI",
        "taxonomy_name": "算力设备",
        "match_method": "MATURE_THEME_REGISTRY_EXACT_NAME",
        "confidence": 1.0,
    }]

    result = screen_a1(snapshot, discovery, local_top_n_per_node=2, llm_top_n_per_theme=1)

    assert any(
        item.get("taxonomy_matches")
        for item in result.decisions
        if item["symbol"] == snapshot["g0_symbols"][0]
    )


def test_a1_selection_basis_is_explicit_and_does_not_change_active_symbols():
    snapshot = _snapshot(8)

    first = screen_a1(snapshot, _discovery(), local_top_n_per_node=2, llm_top_n_per_theme=1)
    second = screen_a1(snapshot, _discovery(), local_top_n_per_node=2, llm_top_n_per_theme=1)

    active_statuses = {"LOCAL_ACTIVE_CANDIDATE", "REVIEW_CANDIDATE"}
    first_active = {
        item["symbol"] for item in first.decisions if item.get("status") in active_statuses
    }
    second_active = {
        item["symbol"] for item in second.decisions if item.get("status") in active_statuses
    }
    assert first_active == second_active

    active = [item for item in first.decisions if item.get("status") in active_statuses]
    assert {item["selection_basis"] for item in active} == {
        "LLM_REVIEWED",
        "DETERMINISTIC_SCORE",
        "QUOTA_FILL",
    }
    assert first.summary["selection_basis_total"] == len(active)
    assert sum(first.summary["selection_basis_counts"].values()) == len(active)
    assert first.summary["selection_basis_counts"] == {
        "LLM_REVIEWED": 1,
        "DETERMINISTIC_SCORE": 1,
        "QUOTA_FILL": 4,
            "BROKER_GOLD_DIRECT": 0,
            "FUNDAMENTAL_BASELINE": 0,
            "HALF_YEAR_FUNDAMENTAL": 0,
        }

    local_active = local_active_items(first)
    assert all(item["selection_basis"] in {"DETERMINISTIC_SCORE", "QUOTA_FILL"} for item in local_active)
    assert any(item["selection_basis"] == "QUOTA_FILL" for item in local_active)


def test_a1_strict_monthly_chain_accepts_disclosed_h1_double_growth_without_quota_fill():
    snapshot = _snapshot(2)
    symbol = snapshot["g0_symbols"][0]
    snapshot["A1_POOL_TARGETS"] = {
        "monthly_chain_only": True,
        "quota_fill_enabled": False,
        "active_research_target": [1, 10],
    }
    snapshot["MAIN_BUSINESS_EVIDENCE"][symbol] = {
        "available": True,
        "evidence": [{
            "source_ref": f"cninfo:{symbol}:2026h1:page:12",
            "page_number": 12,
            "publish_time": "2026-08-20",
            # Valid disclosed main-business page, but deliberately no exact
            # revenue percentage for the legacy exposure parser.
            "text": "公司半年度报告主营业务分行业包括算力设备及配套服务。",
        }],
    }
    snapshot["COMPANY_FUNDAMENTALS"][symbol]["statements"] = {
        "INCOME": [
            {
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "report_date_ms": 1787155200000,
                "operating_income": 130.0,
                "parent_holder_net_profit": 18.0,
            },
            {
                "fiscal_year": 2025,
                "fiscal_period": "Q2",
                "report_date_ms": 1755619200000,
                "operating_income": 100.0,
                "parent_holder_net_profit": 10.0,
            },
        ],
        "BALANCE": [{}],
        "CASH_FLOW": [{}],
    }

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    decision = next(item for item in result.decisions if item["symbol"] == symbol)

    assert decision["status"] == "LOCAL_ACTIVE_CANDIDATE"
    assert decision["selection_basis"] == "HALF_YEAR_FUNDAMENTAL"
    assert decision["research_route"] == "HALF_YEAR_FUNDAMENTAL"
    assert decision["half_year_support"]["supported"] is True
    assert decision["business_exposure_facts"] == []
    projected = local_active_items(result)
    assert [item["symbol"] for item in projected] == [symbol]
    assert projected[0]["evidence_confidence"] >= 0.70


def test_a1_strict_monthly_chain_disables_quota_and_baseline_activation():
    config_path = Path(__file__).parents[1] / "config" / "funnel_config_v2.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    agent_1 = config["agent_1"]

    assert "quota_forbidden" not in agent_1
    assert agent_1["monthly_chain_only"] is True
    assert agent_1["quota_fill_enabled"] is False
    assert agent_1["quota_fill_observation"] == "COHORT_OBSERVATION_ONLY"
    assert agent_1["fundamental_baseline"]["enabled"] is False
    assert agent_1["minimum_financial_quality"] == 60


def test_a1_strict_monthly_chain_requires_sector_business_and_financial_support() -> None:
    snapshot = _snapshot(3)
    snapshot["A1_POOL_TARGETS"] = {
        "monthly_chain_only": True,
        "active_research_target": [2, 4],
        "quota_fill_enabled": True,
        "fundamental_baseline": {"enabled": True},
    }
    snapshot["A1_MINIMUMS"]["minimum_financial_quality"] = 60
    snapshot["A1_MINIMUMS"]["minimum_score"] = 1
    snapshot["A1_MINIMUMS"]["minimum_data_quality"] = 1
    snapshot["A1_MINIMUMS"]["minimum_available_weight"] = 0.10
    snapshot["A1_MINIMUMS"]["minimum_financial_coverage"] = 0.20
    broker_symbol, weak_symbol, outside_symbol = snapshot["g0_symbols"]
    snapshot["BROKER_GOLD_COVERAGE_POOL"] = {
        "available": True,
        "symbols": {
            broker_symbol: {"symbol": broker_symbol, "source_refs": ["broker:strict"]},
            outside_symbol: {"symbol": outside_symbol, "source_refs": ["broker:outside"]},
        },
    }
    snapshot["COMPANY_FUNDAMENTALS"][weak_symbol]["indicators"] = [
        {"index_id": "index_weighted_avg_roe", "value": -10},
        {"index_id": "sale_gross_margin", "value": 5},
        {"index_id": "net_profit_cash_content", "value": 0},
    ]

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=3, llm_top_n_per_theme=3)
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[broker_symbol]["selection_basis"] != "BROKER_GOLD_DIRECT"
    assert by_symbol[broker_symbol]["research_route"] == "MONTHLY_THEME"
    assert by_symbol[broker_symbol]["sector_constituent_confirmed"] is True
    assert by_symbol[broker_symbol]["sector_index_code"] == "884001.TI"
    assert by_symbol[broker_symbol]["fundamental_support"]["supported"] is True
    assert by_symbol[broker_symbol]["status"] == "LOCAL_ACTIVE_CANDIDATE"
    assert by_symbol[broker_symbol]["sent_to_llm"] is False
    assert by_symbol[weak_symbol]["status"] == "LOCAL_MONITOR"
    assert "A1_FINANCIAL_QUALITY_BELOW_MINIMUM" in by_symbol[weak_symbol]["reason_codes"]
    assert by_symbol[outside_symbol]["status"] == "OUTSIDE_THEME"
    assert not any(item["selection_basis"] == "FUNDAMENTAL_BASELINE" for item in result.decisions)


def test_a1_strict_monthly_chain_projects_outside_g0_broker_gold_to_monitor_only() -> None:
    snapshot = _snapshot(2)
    outside_symbol = "002293.SZ"
    snapshot["A1_POOL_TARGETS"] = {
        "monthly_chain_only": True,
        "active_research_target": [1, 2],
        "quota_fill_enabled": False,
        "fundamental_baseline": {"enabled": False},
    }
    snapshot["BROKER_GOLD_COVERAGE_POOL"] = {
        "available": True,
        "month": "2026-09",
        "symbols": {
            outside_symbol: {
                "symbol": outside_symbol,
                "name": "池外金股",
                "brokers": ["券商甲"],
                "source_refs": ["broker:strict-outside"],
                "evidence_tier": "T2",
                "direct_research_entry": True,
            },
        },
    }

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[outside_symbol]["status"] == "OUTSIDE_G0"
    assert outside_symbol in result.monitor_symbols
    assert outside_symbol not in {
        item["symbol"]
        for item in result.decisions
        if item["status"] in {"LOCAL_ACTIVE_CANDIDATE", "REVIEW_CANDIDATE"}
    }
    monitor = next(item for item in local_monitor_items(result) if item["symbol"] == outside_symbol)
    assert monitor["selection_basis"] == "BROKER_GOLD_DIRECT"
    assert monitor["research_route"] == "BROKER_GOLD_DIRECT"
    assert monitor["downstream_trade_eligible"] is False
    assert monitor["primary_theme"] is None
    assert monitor["industry_chain_node"] is None


def test_a1_missing_factor_weight_stays_monitor_and_zero_without_proxy():
    snapshot = _snapshot(2)
    snapshot["SCORE_WEIGHTS"] = {
        "structural_theme": 0.20,
        "business_mapping": 0.20,
        "barrier_and_bottleneck": 0.15,
        "financial_quality": 0.20,
        "catalyst_confirmation": 0.15,
        "valuation_expectation_gap": 0.10,
    }
    snapshot["A1_MINIMUMS"]["minimum_available_weight"] = 0.70

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    item = next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][0])

    assert item["status"] == "LOCAL_MONITOR"
    # This fixture deliberately makes the only themed symbol lack disclosed
    # business evidence.  Only structural theme and financial quality remain
    # available; missing business mapping must not be counted as coverage.
    assert item["available_weight"] == 0.40
    assert "A1_FACTOR_COVERAGE_BELOW_MINIMUM" in item["reason_codes"]
    for factor_name in (
        "barrier_and_bottleneck",
        "catalyst_confirmation",
        "valuation_expectation_gap",
    ):
        assert item["factor_details"][factor_name]["available"] is False
        assert item["factor_details"][factor_name]["score"] == 0.0
    assert local_active_items(result) == []


def test_a2_and_a3_never_expand_the_upstream_pool():
    snapshot = _snapshot(4)
    a1_output = {
        "active_research_pool": [
            {"symbol": symbol, "candidate_id": f"a1:{symbol}", "primary_theme": "theme-compute", "structural_score": 80}
            for symbol in snapshot["g0_symbols"][:3]
        ]
    }
    a2 = screen_a2(snapshot, a1_output, minimum_identifiability_score=40, llm_top_n_per_theme=2)
    assert set(a2.review_symbols).issubset(set(snapshot["g0_symbols"][:3]))
    a2_output = {"focus_pool": [{"symbol": symbol, "theme_id": "theme-compute"} for symbol in a2.review_symbols]}
    a3 = screen_a3(snapshot, a2_output)
    assert set(a3.review_symbols).issubset(set(a2.review_symbols))


def test_a3_server_strategy_defers_reference_risk_floor_to_a4_live_entry():
    snapshot = _snapshot(2)
    snapshot["TECHNICAL_SCORE_WEIGHTS"] = {
        "higher_timeframe_trend": 0.20,
        "structure_quality": 0.20,
        "volume_price": 0.15,
        "relative_strength": 0.10,
        "location_and_extension": 0.15,
        "room_and_reward_risk": 0.15,
        "liquidity": 0.05,
    }
    snapshot["MIN_TECHNICAL_SCORE"] = 70
    snapshot["MIN_REWARD_RISK"] = 2.5
    snapshot["MAX_STOP_DISTANCE"] = 0.06
    snapshot["KLINE_PATTERNS"] = {
        symbol: {
            "available": True,
            "direction": "BULLISH",
            "labels": ["BULLISH_ENGULFING"],
            "volume_percentile_60": 0.90,
            "breakout_20": {"up": True},
        }
        for symbol in snapshot["g0_symbols"]
    }
    snapshot["A2_BOTTLENECK_CONTEXT"] = {
        symbol: {
            "a2_factor_scores": {"relative_strength": {"score": 90}},
            "role_breakdown": {"relative_strength": 90, "liquidity_capacity": 90},
        }
        for symbol in snapshot["g0_symbols"]
    }
    weak_symbol = snapshot["g0_symbols"][1]
    snapshot["PRICE_LEVELS"][weak_symbol]["reward_risk"] = 2.4
    a2_output = {
        "focus_pool": [
            {"symbol": symbol, "theme_id": "theme-compute"}
            for symbol in snapshot["g0_symbols"]
        ],
    }

    result = screen_a3(snapshot, a2_output)
    ready = next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][0])
    weak = next(item for item in result.decisions if item["symbol"] == weak_symbol)

    assert ready["status"] == "REVIEW_CANDIDATE"
    assert ready["strategy_profile"] == "TREND_MA5"
    assert ready["eligibility"] == "QUALIFIED"
    assert "technical_score" not in ready
    assert weak["status"] == "REVIEW_CANDIDATE"
    assert weak["eligibility"] == "QUALIFIED"
    assert "A3_REWARD_RISK_BELOW_MINIMUM" in weak["reason_codes"]
    assert "A3_REWARD_RISK_BELOW_MINIMUM" in weak["a4_deferred_conditions"]
    assert "A3_REWARD_RISK_BELOW_MINIMUM" not in weak["veto_conditions"]


def test_a2_market_core_uses_real_local_market_factors_without_inventing_capital_flow():
    snapshot = _snapshot(4)
    for index, candidate in enumerate(snapshot["g0_candidates"]):
        candidate["change_ratio_pct"] = 2.0 - index * 0.5
    snapshot["THEME_SCORE_WEIGHTS"] = {
        "breadth": 0.15,
        "turnover_share": 0.12,
        "capital_flow": 0.13,
        "leader_structure": 0.15,
        "tier_structure": 0.15,
        "profit_effect": 0.10,
        "catalyst_freshness": 0.08,
        "index_chain_resonance": 0.07,
        "agent_1_quality": 0.05,
    }
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": False,
        "reason_code": "SOURCE_NOT_CONFIGURED",
        "turnover_is_capital_flow": False,
    }
    snapshot["SECTOR_CYCLE_SNAPSHOT"] = {
        "available": True,
        "history_metrics": {
            "monthly_rotation_candidates": [{
                "industry_thscode": "884001.TI",
                "relative_strength_percentile_20d": 0.9,
                "top10_appearance_count": 8,
            }],
        },
    }
    symbol = snapshot["g0_symbols"][0]
    snapshot["DISCLOSURE_EVENTS"] = {
        symbol: [{
            "announcement_title": "重大订单合同落地",
            "source_ref": f"cninfo:{symbol}:notice:1",
        }],
    }
    a1_output = {
        "active_research_pool": [{
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-compute",
            "industry_chain_node": "node-compute-device",
            "structural_score": 85,
            "data_quality_score": 90,
            "business_exposure": {
                "revenue_exposure_pct": 65.5,
                "source_ref": f"cninfo:{symbol}:page:1",
            },
            "source_refs": [f"cninfo:{symbol}:page:1"],
        }],
    }

    result = screen_a2(snapshot, a1_output, minimum_identifiability_score=60, llm_top_n_per_theme=2)
    item = result.decisions[0]

    assert item["status"] == "REVIEW_CANDIDATE"
    assert result.review_symbols == (symbol,)
    assert result.monitor_symbols == ()
    assert "MARKET_CORE" in item["eligible_routes"]
    # The unavailable optional supply-chain scorecard must not turn an
    # otherwise usable MARKET_CORE route into a market-wide data gap.
    assert "A2_DATA_GAP" not in item["reason_codes"]
    assert "A2_CRITICAL_DATA_INSUFFICIENT" not in item["reason_codes"]
    assert item["factor_coverage"]["ratio"] >= 0.65
    assert item["critical_factor_coverage"]["sufficient"] is True
    assert item["data_sufficiency_state"] == "DEGRADED"
    assert item["a2_factor_scores"]["capital_flow"]["available"] is False
    assert item["a2_factor_scores"]["capital_flow"]["source"] == "CAPITAL_FLOW_SNAPSHOT"
    assert item["a2_factor_scores"]["turnover_share"]["source"] == "FROZEN_G0_INDUSTRY_TURNOVER_SHARE"


def test_a2_taxonomy_aggregates_are_bound_to_the_a1_chain_node():
    snapshot = _snapshot(2)
    symbol = snapshot["g0_symbols"][0]
    snapshot["A2_FACTOR_SNAPSHOT"] = {
        symbol: {
            "factors": {
                name: {"available": True, "score": 99, "source": "UNRELATED_STRONGEST_CONCEPT"}
                for name in ("breadth", "turnover_share", "index_chain_resonance", "weekly_confirmation")
            },
        },
    }
    snapshot["A2_THEME_METRICS"] = {
        "theme_metrics": {
            "INDUSTRY:884001.TI": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": "884001.TI",
                "taxonomy_name": "算力设备",
                "score": 55,
                "breadth": 0.40,
                "turnover_share": 0.10,
                "weekly_confirmation_score": 50,
            },
        },
    }
    a1_output = {
        "taxonomy_links": [{
            "node_id": "node-compute-device",
            "industry_thscodes": ["884001.TI"],
            "concept_thscodes": [],
        }],
        "active_research_pool": [{
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-compute",
            "industry_chain_node": "node-compute-device",
            "structural_score": 80,
        }],
    }

    result = screen_a2(snapshot, a1_output, minimum_identifiability_score=40, llm_top_n_per_theme=2)
    item = result.decisions[0]

    assert item["a2_taxonomy_binding"]["status"] == "BOUND"
    assert item["a2_taxonomy_binding"]["taxonomy_code"] == "884001.TI"
    assert item["a2_factor_scores"]["breadth"]["score"] == 40.0
    assert item["a2_factor_scores"]["breadth"]["source"] == "A1_BOUND_TAXONOMY_AGGREGATE"
    assert item["a2_factor_scores"]["index_chain_resonance"]["score"] == 55.0


def test_feature_store_replaces_one_lane_stage_atomically(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    gate = screen_a1(_snapshot(5), _discovery(), local_top_n_per_node=4, llm_top_n_per_theme=2)
    assert store.replace_stage_decisions(
        run_id="run-v2",
        lane_id="lane_1",
        stage=gate.stage,
        decisions=gate.decisions,
        updated_at=NOW,
    ) == 5
    summary = store.stage_summary("run-v2", "lane_1", gate.stage)
    assert summary["evaluated_count"] == 5
    assert summary["sent_to_llm_count"] == 2
    assert len(store.stage_decisions("run-v2", "lane_1", gate.stage)) == 5

    snapshot = _snapshot(5)
    assert store.replace_taxonomy_memberships(
        taxonomy="INDUSTRY",
        snapshot=snapshot["THS_INDUSTRY_MEMBERSHIP"],
        as_of=NOW,
    ) == 5
    assert store.record_fundamental_features(as_of=NOW, decisions=gate.decisions) == 5

    a1_output = {
        "active_research_pool": [
            {"symbol": symbol, "primary_theme": "theme-compute", "structural_score": 80}
            for symbol in gate.review_symbols
        ],
    }
    a2 = screen_a2(snapshot, a1_output, minimum_identifiability_score=40, llm_top_n_per_theme=2)
    assert store.record_market_role_features(
        run_id="run-v2",
        lane_id="lane_1",
        decisions=a2.decisions,
    ) == len(a2.decisions)

    store.mark_dirty(
        entity_type="SYMBOL",
        entity_id="600000.SH",
        reason_code="DAILY_BAR_CHANGED",
        source_version="v2",
        created_at=NOW,
    )
    assert store.resolve_dirty(entity_type="SYMBOL", entity_id="600000.SH", resolved_at=NOW) == 1


def _prompt_dir(tmp_path: Path) -> Path:
    path = tmp_path / "prompts"
    path.mkdir()
    for filename in PROMPT_FILENAMES:
        path.joinpath(filename).write_text("prompt " + filename, encoding="utf-8")
    return path


def _envelope(model: str, stage: str, snapshot_id: str) -> dict:
    return {
        "schema_version": "test/2",
        "stage_id": {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}[stage],
        "status": "OK",
        "input_snapshot_ids": [snapshot_id],
        "model_name": model,
        "config_version": "test-v2",
        "prompt_version": "test-v2",
        "market_regime": "REPAIR",
    }


class V2Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []

    def complete(self, model: str, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        symbols = tuple(runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"])
        self.calls.append((model, stage, symbols))
        output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
        if stage == "A1" and not symbols:
            output.update(_discovery())
            output.update(active_research_pool=[], monitor_pool=[], rejected_candidates=[])
        elif stage == "A1":
            output.update(
                structural_themes=_discovery()["structural_themes"],
                industry_chain_graph=_discovery()["industry_chain_graph"],
                taxonomy_links=_discovery()["taxonomy_links"],
                active_research_pool=[
                    {
                        "symbol": symbol,
                        "candidate_id": f"{model}:a1:{symbol}",
                        "primary_theme": "theme-compute",
                        "industry_chain_node": "node-compute-device",
                        "structural_score": 80,
                        "score_breakdown": {"structural_theme": 80, "business_mapping": 80, "barrier_and_bottleneck": 80, "financial_quality": 80, "cash_flow_quality": 80, "evidence_quality": 80},
                        "data_quality_score": 90,
                        "evidence_confidence": 0.9,
                        "business_exposure": {"revenue_exposure_pct": 50, "source_ref": f"cninfo:{symbol}:page:1"},
                        "source_refs": ["policy-ref", f"cninfo:{symbol}:page:1"],
                    }
                    for symbol in symbols
                ],
                monitor_pool=[],
                rejected_candidates=[],
            )
        elif stage == "A2":
            output.update(
                active_themes=[],
                focus_pool=[
                    {
                        "symbol": symbol,
                        "upstream_candidate_id": f"{model}:a1:{symbol}",
                        "theme_id": "theme-compute",
                        "theme_score": 70,
                        "identifiability_score": 75,
                    }
                    for symbol in symbols
                ],
                watch_only_pool=[],
            )
        else:
            output.update(
                core_watch_pool=[
                    {
                        "symbol": symbol,
                        "parent_candidate_id": f"{model}:a2:{symbol}",
                        "technical_score": 80,
                        "score_breakdown": {
                            "higher_timeframe_trend": 80,
                            "structure_quality": 80,
                            "volume_price": 80,
                            "relative_strength": 80,
                            "location_and_extension": 80,
                            "room_and_reward_risk": 80,
                            "liquidity": 80,
                        },
                        "reward_risk": 3.0,
                        "stop_distance_pct": 0.05,
                        "risk_unit": "STANDARD",
                        "setup_type": "TREND_PULLBACK",
                        "confirmation_conditions": ["FIVE_MIN_HIGHER_LOW"],
                        "scenarios": {
                            "normal_open_plan": {},
                            "weak_open_plan": {},
                            "high_gap_no_chase_plan": {},
                            "invalidation_plan": {},
                        },
                        "plan_expiry": "2026-08-25T15:00:00+08:00",
                    }
                    for symbol in symbols
                ],
                secondary_watch_pool=[],
                rejected_candidates=[],
            )
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=1,
            attempts=1,
            thinking_variant="thinking_object",
        )


def test_v2_pipeline_does_not_send_the_full_g0_to_a1(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "test",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "deterministic_v2",
            "LIANGJIAN_A1_LOCAL_TOP_N_PER_NODE": "5",
            "LIANGJIAN_A1_LLM_TOP_N_PER_NODE": "2",
            "LIANGJIAN_A1_LLM_REPRESENTATIVES_PER_THEME": "2",
            "LIANGJIAN_A2_LLM_TOP_N_PER_THEME": "2",
        },
        root=tmp_path,
    ).model_copy(update={"research_models": MODELS})
    client = V2Client()
    progress_events: list[dict] = []
    runtime_store = RuntimeStore(settings.state_db_path)
    snapshot_data = _snapshot(8)
    snapshot_data["A1_DRIVER_LINEAGE_REQUIRED"] = True
    snapshot = FrozenInputSnapshot("snapshot-v2", snapshot_data, as_of=NOW)
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        output_dir=tmp_path / "outputs",
        progress_callback=progress_events.append,
        now=lambda: NOW,
        runtime_store=runtime_store,
    ).run(snapshot, run_id="run-v2", generated_at=NOW)

    assert result.status == "READY_DEGRADED"
    assert all(lane.status == "READY_DEGRADED" for lane in result.lanes)
    for model in MODELS:
        a1_calls = [symbols for called_model, stage, symbols in client.calls if called_model == model and stage == "A1"]
        assert a1_calls[0] == ()
        assert max(map(len, a1_calls[1:])) <= 2
        assert all(len(symbols) < len(snapshot_data["g0_symbols"]) for symbols in a1_calls)
    assert all(lane.stages[0].diagnostics["local_screen"]["evaluated_count"] == 8 for lane in result.lanes)
    for lane_id in ("lane_1", "lane_2", "lane_3"):
        a1_labels = runtime_store.list_outcome_labels(
            run_id="run-v2",
            lane_id=lane_id,
            stage="A1",
        )
        assert len(a1_labels) == 8
        assert {row["decision"] for row in a1_labels}.issubset({"PASSED", "REJECTED"})
        assert all(row["snapshot_id"] == "snapshot-v2" for row in a1_labels)
    for lane_id in ("lane_1", "lane_2", "lane_3"):
        discovery_events = [
            event
            for event in progress_events
            if event["lane"] == lane_id and event["stage"] == "MACRO_DISCOVERY"
        ]
        assert discovery_events[0]["status"] == "RUNNING"
        assert discovery_events[0]["completed"] == 0
        assert discovery_events[-1]["status"] == "COMPLETED"
        assert discovery_events[-1]["completed"] == 1
        review_events = [
            event
            for event in progress_events
            if event["lane"] == lane_id and event["stage"] == "A1_LLM_REVIEW"
        ]
        assert review_events[0]["status"] == "RUNNING"
        assert review_events[-1]["completed"] == review_events[-1]["total"]
        assert not any(
            event["lane"] == lane_id and event["stage"] == "A1"
            for event in progress_events
        )


def test_empty_a3_stage_emits_terminal_no_action_progress(tmp_path: Path):
    events: list[dict] = []
    settings = Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "test"}, root=tmp_path)
    snapshot = FrozenInputSnapshot("snapshot-empty-a3", _snapshot(1), as_of=NOW)
    pipeline = ResearchPipeline(
        settings,
        progress_callback=events.append,
        now=lambda: NOW,
    )

    audit = pipeline._empty_stage(
        run_id="run-empty-a3",
        lane_id="lane_1",
        model="deepseek-v4-pro-0813",
        stage="A3",
        snapshot=snapshot,
        upstream_output={"focus_pool": []},
        status="VALIDATED_NO_ACTION",
        outcome="NO_ACTION_UPSTREAM_NO_OPPORTUNITY",
    )

    assert audit.status == "VALIDATED_NO_ACTION"
    assert events == [
        {
            "run_id": "run-empty-a3",
            "lane": "lane_1",
            "stage": "A3_LLM_REVIEW",
            "batch": {"completed": 0, "total": 0},
            "completed": 0,
            "total": 0,
            "batch_completed": 0,
            "batch_total": 0,
            "completed_batches": 0,
            "total_batches": 0,
            "status": "VALIDATED_NO_ACTION",
            "attempts": 0,
            "model": "deepseek-v4-pro-0813",
            "outcome": "VALIDATED_NO_ACTION",
            "processed_symbols": 0,
            "total_symbols": 0,
            "selected_symbols": 0,
        }
    ]


def _complete_a2_factor_scores(score: float) -> dict[str, dict[str, float]]:
    """Build explicit observed factors for deterministic gate boundary tests."""

    return {
        name: {"score": score}
        for name in (
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
    }


def _trend_a2_factor_scores(score: float) -> dict[str, dict[str, float]]:
    factors = _complete_a2_factor_scores(score)
    factors["tier_structure"] = {
        "score": 20,
        "available": True,
        "availability_state": "OBSERVED_ABSENT",
        "ladder_height": 0,
        "first_board_observed": False,
        "event_source": "HITHINK_LIMIT_UP_LADDER",
    }
    factors["weekly_confirmation"] = {"score": score, "available": True}
    return factors


def test_a2_dual_core_pool_keeps_hot100_emotion_and_selected_board_trend_together() -> None:
    snapshot = _snapshot(3)
    emotion_symbol, trend_symbol, outside_symbol = snapshot["g0_symbols"]
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    snapshot["MARKET_EMOTION_SNAPSHOT"] = {
        "available": True,
        "emotion_cycle_stage": "STARTUP",
        "new_long_permission": "PROBE_ONLY",
    }
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = {
        "available": True,
        "trade_date": "2026-08-27",
        "record_count": 100,
        "records": [{"symbol": emotion_symbol, "name": "情绪龙头", "rank": 5}],
    }
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            trend_symbol: [{
                "board_code": "801807",
                "strategy_theme_id": "theme-core",
                "board_name": "算力",
                "strength": 3238,
                "main_net_inflow_cny": 2_467_000_000,
                "selected_for_rotation": True,
                "primary_rank": 2,
            }],
        },
    }
    rows = []
    for symbol in snapshot["g0_symbols"]:
        factor_scores = _complete_a2_factor_scores(90)
        factor_scores["tier_structure"] = {
            "score": 90 if symbol == emotion_symbol else 20,
            "available": True,
            "availability_state": "OBSERVED_VALUE" if symbol == emotion_symbol else "OBSERVED_ABSENT",
            "ladder_height": 1 if symbol == emotion_symbol else 0,
            "first_board_observed": symbol == emotion_symbol,
            "event_source": "HITHINK_LIMIT_UP_POOL" if symbol == emotion_symbol else "HITHINK_LIMIT_UP_LADDER",
        }
        factor_scores["weekly_confirmation"] = {"score": 90, "available": True}
        rows.append({
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": factor_scores,
            "data_quality_score": 90,
        })

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    by_symbol = {row["symbol"]: row for row in result.decisions}
    assert by_symbol[emotion_symbol]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[emotion_symbol]["a2_pool_channel"] == "EMOTION"
    assert by_symbol[emotion_symbol]["top_rotation_theme"] is None
    assert by_symbol[trend_symbol]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[trend_symbol]["a2_pool_channel"] == "TREND"
    assert by_symbol[trend_symbol]["top_rotation_theme"] is True
    assert by_symbol[trend_symbol]["selected_board"]["board_code"] == "801807"
    assert by_symbol[outside_symbol]["status"] == "LOCAL_MONITOR"
    assert "A2_TREND_OUTSIDE_POSITIVE_FLOW_TOP3_BOARD" in by_symbol[outside_symbol]["reason_codes"]
    assert set(result.review_symbols) == {emotion_symbol, trend_symbol}

    overlay_rows = [dict(row) for row in rows]
    overlay_rows[0].update({
        "status": "ACTIVE",
        "selection_basis": "DAILY_EMOTION_OVERLAY",
        "research_route": "DAILY_EMOTION_OVERLAY",
        "emotion_attention_eligible": True,
        "downstream_trade_eligible": True,
        # A real daily overlay has no monthly revenue-line proof.  The
        # emotion route must still reach review only after its own hot-100,
        # ladder and market-fact gates pass.
        "business_exposure": "情绪票按题材与接力事实判断",
        "business_exposure_facts": [],
    })
    overlay_result = screen_a2(
        snapshot,
        {"active_research_pool": overlay_rows},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    overlay_by_symbol = {row["symbol"]: row for row in overlay_result.decisions}
    assert overlay_by_symbol[emotion_symbol]["a1_formal_member"] is False
    assert overlay_by_symbol[emotion_symbol]["monthly_a1_member"] is False
    assert overlay_by_symbol[emotion_symbol]["daily_a1_member"] is True
    assert overlay_by_symbol[emotion_symbol]["status"] == "REVIEW_CANDIDATE"
    assert overlay_by_symbol[emotion_symbol]["emotion_core_eligible"] is True
    assert overlay_by_symbol[emotion_symbol]["trend_core_eligible"] is False
    assert overlay_by_symbol[emotion_symbol]["a2_pool_channel"] == "EMOTION"
    assert overlay_by_symbol[emotion_symbol]["top_rotation_theme"] is None
    assert emotion_symbol in overlay_result.review_symbols
    daily_a1_symbols = {
        item["symbol"] for item in overlay_result.decisions if item["daily_a1_member"] is True
    }
    monthly_a1_symbols = {
        item["symbol"] for item in overlay_result.decisions if item["monthly_a1_member"] is True
    }
    emotion_review_symbols = {
        item["symbol"]
        for item in overlay_result.decisions
        if item["status"] == "REVIEW_CANDIDATE" and item["a2_pool_channel"] == "EMOTION"
    }
    trend_review_symbols = {
        item["symbol"]
        for item in overlay_result.decisions
        if item["status"] == "REVIEW_CANDIDATE" and item["a2_pool_channel"] == "TREND"
    }
    assert emotion_review_symbols <= daily_a1_symbols
    assert trend_review_symbols <= monthly_a1_symbols
    assert emotion_symbol not in trend_review_symbols

    missing_board_snapshot = {
        **snapshot,
        "SELECTED_BOARD_SNAPSHOT": {
            "available": False,
            "reason_code": "SELECTED_BOARD_SNAPSHOT_MISSING",
            "by_symbol": {},
        },
    }
    missing_board = screen_a2(
        missing_board_snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    missing_by_symbol = {row["symbol"]: row for row in missing_board.decisions}
    assert missing_by_symbol[emotion_symbol]["status"] == "REVIEW_CANDIDATE"
    assert missing_by_symbol[emotion_symbol]["a2_pool_channel"] == "EMOTION"
    assert missing_by_symbol[emotion_symbol]["emotion_core_eligible"] is True
    assert missing_by_symbol[emotion_symbol]["rotation_input_source"] == "SELECTED_BOARD_SNAPSHOT_UNAVAILABLE"
    assert missing_by_symbol[trend_symbol]["status"] == "LOCAL_MONITOR"
    assert missing_by_symbol[trend_symbol]["trend_core_eligible"] is False
    assert "A2_SELECTED_BOARD_SOURCE_UNAVAILABLE" in missing_by_symbol[trend_symbol]["reason_codes"]


def test_a2_top5_monthly_candidate_with_identity_below_reference_reaches_llm() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbol: [{
                "board_code": "801807",
                "strength": 90,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
        },
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-core",
        "industry_chain_node": "node-core",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
        "monthly_direction_matches": [{
            "monthly_direction_id": "theme-core",
            "sector_index_code": "801807",
            "sector_index_name": "算力",
        }],
        "a2_factor_scores": _complete_a2_factor_scores(80),
        "data_quality_score": 90,
    }
    row["a2_factor_scores"]["weekly_confirmation"] = {"score": 80, "available": True}

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=101,
        review_all_eligible=True,
    )
    item = result.decisions[0]

    assert item["identifiability_score"] < 101
    assert item["status"] == "REVIEW_CANDIDATE"
    assert item["a2_pool_channel"] == "TREND"
    assert item["sent_to_llm"] is True
    assert item["selected_board_theme_match"] is True
    assert item["gate_results"]["IDENTIFIABILITY_MIN"]["blocks_decision"] is False


def test_a2_selected_board_without_a1_theme_binding_cannot_open_trend_route() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbol: [{
                "board_code": "801807",
                "strategy_theme_id": "unrelated-theme",
                "strength": 90,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
        },
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-core",
        "industry_chain_node": "node-core",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
        "a2_factor_scores": _complete_a2_factor_scores(80),
        "data_quality_score": 90,
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    item = result.decisions[0]

    assert item["selected_board"] is None
    assert item["trend_core_eligible"] is False
    assert item["a2_pool_channel"] == "NONE"
    assert "A2_SELECTED_BOARD_THEME_MISMATCH" in item["reason_codes"]
    assert result.review_symbols == ()


def test_a2_liangjian_full_market_membership_can_complement_a1_primary_theme() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "schema_version": "liangjian-rotation-theme/1.0.0",
        "source_id": "LIANGJIAN_FREE_ROTATION_V1",
        "available": True,
        "taxonomy_substitution_forbidden": False,
        "by_symbol": {
            symbol: [{
                "board_code": "AI_LIQUID_COOLING",
                "strategy_theme_id": "AI_COMPUTE_INFRASTRUCTURE",
                "strength": 90,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
        },
    }
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90},
        },
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        # A company can be selected by a different monthly primary theme and
        # still be a verified member of today's liquid-cooling board.
        "primary_theme": "AI_APPLICATIONS_DIGITAL_ECONOMY",
        "industry_chain_node": "software-platform",
        "business_exposure": {
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        },
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
        "a2_factor_scores": _trend_a2_factor_scores(80),
        "data_quality_score": 90,
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    item = result.decisions[0]

    assert item["selected_board"]["board_code"] == "AI_LIQUID_COOLING"
    assert item["selected_board_theme_match"] is False
    assert item["selected_board_binding"] == "STABLE_FULL_MARKET_SYMBOL_MEMBERSHIP"
    assert item["trend_core_eligible"] is True
    assert item["a2_pool_channel"] == "TREND"
    assert item["status"] == "REVIEW_CANDIDATE"
    assert result.review_symbols == (symbol,)


def test_a2_replays_explicit_primary_board_strategy_binding_when_reverse_row_is_legacy() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "selected_primary_boards": [{
            "board_code": "FINANCIAL_INSURANCE",
            "theme_id": "FINANCIAL_INSURANCE",
            "strategy_theme_id": "FINANCIAL_HIGH_DIVIDEND",
            "rank": 4,
        }],
        "by_symbol": {
            symbol: [{
                # Frozen pre-fix snapshots omitted strategy_theme_id here.
                "board_code": "FINANCIAL_INSURANCE",
                "theme_id": "FINANCIAL_INSURANCE",
                "strength": 76,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 4,
            }],
        },
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "FINANCIAL_HIGH_DIVIDEND",
        "industry_chain_node": "insurance",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
        "a2_factor_scores": _complete_a2_factor_scores(80),
        "data_quality_score": 90,
    }
    row["a2_factor_scores"]["weekly_confirmation"] = {"score": 80, "available": True}

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
        rotation_theme_count=5,
    )
    item = result.decisions[0]

    assert item["selected_board"]["strategy_theme_id"] == "FINANCIAL_HIGH_DIVIDEND"
    assert item["a2_pool_channel"] == "TREND"
    assert item["sent_to_llm"] is True


def test_a2_daily_emotion_overlay_risk_and_trade_boundaries_stay_closed() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {symbol: {"available": True, "capital_flow_score": 90}},
    }
    snapshot["MARKET_EMOTION_SNAPSHOT"] = {
        "available": True,
        "emotion_cycle_stage": "STARTUP",
        "new_long_permission": "PROBE_ONLY",
    }
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = {
        "available": True,
        "trade_date": "2026-08-27",
        "record_count": 100,
        "records": [{"symbol": symbol, "name": "情绪龙头", "rank": 1}],
    }
    factors = _complete_a2_factor_scores(90)
    factors["tier_structure"] = {
        "score": 90,
        "available": True,
        "availability_state": "OBSERVED_VALUE",
        "ladder_height": 1,
        "first_board_observed": True,
        "event_source": "HITHINK_LIMIT_UP_POOL",
    }
    factors["weekly_confirmation"] = {"score": 90, "available": True}
    base_overlay = {
        "symbol": symbol,
        "candidate_id": f"A1-EMOTION-{symbol}",
        "status": "ACTIVE",
        "selection_basis": "DAILY_EMOTION_OVERLAY",
        "research_route": "DAILY_EMOTION_OVERLAY",
        "emotion_attention_eligible": True,
        "primary_theme": "当日情绪热度候选",
        "industry_chain_node": "情绪票日度观察层",
        "business_exposure": "情绪票按题材与接力事实判断",
        "business_exposure_facts": [],
        "a2_factor_scores": factors,
        "data_quality_score": 90,
        "downstream_trade_eligible": True,
    }

    healthy = screen_a2(
        snapshot,
        {"active_research_pool": [base_overlay]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    healthy_item = healthy.decisions[0]
    assert healthy_item["daily_a1_member"] is True
    assert healthy_item["monthly_a1_member"] is False
    assert healthy_item["a2_pool_channel"] == "EMOTION"
    assert healthy_item["status"] == "REVIEW_CANDIDATE"
    assert "A2_DAILY_EMOTION_BUSINESS_EVIDENCE_NOT_REQUIRED" in healthy_item["route_eligibility"]["MARKET_CORE"]["diagnostic_reason_codes"]

    trade_blocked = dict(base_overlay)
    trade_blocked["downstream_trade_eligible"] = False
    blocked = screen_a2(
        snapshot,
        {"active_research_pool": [trade_blocked]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    blocked_item = blocked.decisions[0]
    assert blocked_item["status"] == "HARD_REJECT"
    assert blocked_item["daily_a1_member"] is False
    assert blocked_item["emotion_core_eligible"] is False
    assert blocked_item["trend_core_eligible"] is False
    assert blocked.review_symbols == ()
    assert "A2_UPSTREAM_RESEARCH_ONLY" in blocked_item["reason_codes"]

    risk_snapshot = {**snapshot, "RISK_EVENTS": {
        "available": True,
        "records": [{"symbol": symbol, "severity": "HIGH", "event_type": "FRAUD"}],
    }}
    risk = screen_a2(
        risk_snapshot,
        {"active_research_pool": [base_overlay]},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    risk_item = risk.decisions[0]
    assert risk_item["status"] == "HARD_REJECT"
    assert risk_item["daily_a1_member"] is False
    assert risk_item["emotion_core_eligible"] is False
    assert risk_item["trend_core_eligible"] is False
    assert risk.review_symbols == ()
    assert risk_item["hard_risk_events"][0]["event_type"] == "FRAUD"
    assert "A2_HARD_RISK_PRESENT" in risk_item["reason_codes"]

def test_a2_missing_selected_board_field_uses_available_full_market_rotation() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"][0]["memberships"] = [{
        "industry_thscode": "884001.TI",
        "industry_name": "算力设备",
    }]
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {symbol: {"available": True, "capital_flow_score": 90}},
    }
    snapshot["A2_THEME_METRICS"] = {
        "theme_metrics": {
            "INDUSTRY:884001.TI": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": "884001.TI",
                "taxonomy_name": "算力设备",
                "score": 88,
                "breadth": 0.70,
                "turnover_share": 0.08,
                "weekly_confirmation_score": 82,
            },
        },
    }
    snapshot["A2_SECTOR_HEALTH_SNAPSHOT"] = {
        "by_taxonomy": {
            "industry": {
                "sectors": [{
                    "taxonomy_code": "884001.TI",
                    "taxonomy_name": "算力设备",
                    "capital_flow": {
                        "available": True,
                        "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
                        "windows": {"today": {"main_net_cny": 2_467_000_000, "change_pct": 1.2}},
                    },
                }],
            },
        },
    }
    factor_scores = _complete_a2_factor_scores(90)
    factor_scores["tier_structure"] = {
        "score": 20,
        "available": True,
        "availability_state": "OBSERVED_ABSENT",
        "ladder_height": 0,
        "first_board_observed": False,
        "event_source": "HITHINK_LIMIT_UP_LADDER",
    }
    factor_scores["weekly_confirmation"] = {"score": 90, "available": True}
    a1_output = {
        "taxonomy_links": [{
            "node_id": "node-compute-device",
            "taxonomy": "INDUSTRY",
            "taxonomy_code": "884001.TI",
        }],
        "active_research_pool": [{
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-compute",
            "industry_chain_node": "node-compute-device",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": factor_scores,
            "data_quality_score": 90,
        }],
    }

    result = screen_a2(
        snapshot,
        a1_output,
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    item = result.decisions[0]

    assert item["status"] == "REVIEW_CANDIDATE", (
        item.get("rotation_fallback"), item.get("a2_taxonomy_binding")
    )
    assert item["a2_pool_channel"] == "TREND"
    assert item["trend_core_eligible"] is True
    assert item["rotation_fallback"] is None
    assert item["full_market_rotation"]["taxonomy_code"] == "884001.TI"
    assert item["rotation_input_source"] == "FULL_MARKET_ROTATION_FALLBACK"
    assert item["top_rotation_theme"] is True
    assert item["rotation_strength_source"] == "A2_THEME_METRICS"


def test_screen_a2_partitions_no_route_low_identity_and_llm_rank_overflow() -> None:
    """A2 must preserve every A1 row while distinguishing actionable routes."""

    snapshot = _snapshot(4)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "source_id": "TEST_CAPITAL_FLOW",
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    symbols = snapshot["g0_symbols"][:3]
    rows = [
        {
            "symbol": symbols[0],
            "candidate_id": "a1:high",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": "cninfo:high"},
            "a2_factor_scores": _complete_a2_factor_scores(95),
            "data_quality_score": 95,
        },
        {
            "symbol": symbols[1],
            "candidate_id": "a1:low",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 60, "source_ref": "cninfo:low"},
            "a2_factor_scores": _complete_a2_factor_scores(65),
            "data_quality_score": 80,
        },
        {
            # Complete evidence can still be held locally when the upstream
            # row has no theme/chain/evidence route contract.
            "symbol": symbols[2],
            "candidate_id": "a1:unmapped",
            "a2_factor_scores": _complete_a2_factor_scores(80),
            "data_quality_score": 80,
        },
        {"symbol": ""},  # malformed upstream row must not become a decision
    ]

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
    )
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert set(by_symbol) == set(symbols)
    assert by_symbol[symbols[0]]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[symbols[0]]["sent_to_llm"] is True
    assert by_symbol[symbols[0]]["theme_rank"] == 1
    assert by_symbol[symbols[1]]["status"] == "LOCAL_MONITOR"
    assert by_symbol[symbols[1]]["sent_to_llm"] is False
    assert "A2_NOT_SENT_TO_LLM" in by_symbol[symbols[1]]["reason_codes"]
    assert by_symbol[symbols[2]]["status"] == "DATA_GAP"
    assert "A2_NO_ROUTE_READY" in by_symbol[symbols[2]]["reason_codes"]

    # A high identity reference remains auditable but is not a deterministic
    # publication veto when the A1/market facts are otherwise sufficient.
    rejected = screen_a2(
        snapshot,
        {"active_research_pool": [rows[0]]},
        minimum_identifiability_score=101,
        llm_top_n_per_theme=1,
    ).decisions[0]
    assert rejected["status"] == "REVIEW_CANDIDATE"
    assert "A2_IDENTIFIABILITY_BELOW_MINIMUM" in rejected["reason_codes"]
    assert rejected["gate_results"]["IDENTIFIABILITY_MIN"]["blocks_decision"] is False


def test_screen_a2_review_all_eligible_bypasses_legacy_theme_top_n() -> None:
    snapshot = _snapshot(4)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "source_id": "TEST_CAPITAL_FLOW",
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    symbols = snapshot["g0_symbols"][:3]
    rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _complete_a2_factor_scores(80),
            "data_quality_score": 90,
        }
        for symbol in symbols
    ]

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
    )

    assert result.review_symbols == tuple(sorted(symbols))
    assert all(item["status"] == "REVIEW_CANDIDATE" for item in result.decisions)
    assert [item["theme_rank"] for item in result.decisions] == [1, 2, 3]
    assert all("A2_NOT_SENT_TO_LLM" not in item["reason_codes"] for item in result.decisions)


def test_a1_broker_gold_direct_research_entry_preserves_downstream_risk_boundary() -> None:
    snapshot = _snapshot(2)
    in_g0 = snapshot["g0_symbols"][-1]
    snapshot["BROKER_GOLD_COVERAGE_POOL"] = {
        "available": True,
        "month": "2026-09",
        "symbols": {
            in_g0: {
                "symbol": in_g0,
                "name": "食品金股",
                "brokers": ["券商甲"],
                "source_refs": ["broker:甲"],
            },
            "000001.SZ": {
                "symbol": "000001.SZ",
                "name": "池外金股",
                "brokers": ["券商乙"],
                "source_refs": ["broker:乙"],
            },
        },
    }
    snapshot["MAIN_BUSINESS_EVIDENCE"][in_g0] = {"available": False, "evidence": []}

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=2, llm_top_n_per_theme=1)
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[in_g0]["status"] == "LOCAL_ACTIVE_CANDIDATE"
    assert by_symbol[in_g0]["selection_basis"] == "BROKER_GOLD_DIRECT"
    assert by_symbol[in_g0]["research_route"] == "BROKER_GOLD_DIRECT"
    assert by_symbol[in_g0]["downstream_trade_eligible"] is True
    projected_by_symbol = {item["symbol"]: item for item in local_active_items(result)}
    assert projected_by_symbol[in_g0]["selection_basis"] == "BROKER_GOLD_DIRECT"
    assert projected_by_symbol[in_g0]["source_refs"] == ["broker:甲"]
    assert projected_by_symbol[in_g0]["business_exposure"] is None
    assert "A1_INSTITUTIONAL_DIRECT_ENTRY" in by_symbol[in_g0]["reason_codes"]
    assert by_symbol[in_g0]["coverage_origin"] == "BROKER_GOLD_T2"
    assert by_symbol["000001.SZ"]["status"] == "LOCAL_ACTIVE_CANDIDATE"
    assert by_symbol["000001.SZ"]["selection_basis"] == "BROKER_GOLD_DIRECT"
    assert by_symbol["000001.SZ"]["downstream_trade_eligible"] is False
    assert "A1_INSTITUTIONAL_OUTSIDE_G0" in by_symbol["000001.SZ"]["reason_codes"]
    assert "000001.SZ" not in result.monitor_symbols
    assert in_g0 not in result.monitor_symbols


def test_a1_broker_gold_hard_risk_still_enters_research_but_not_downstream() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["BROKER_GOLD_COVERAGE_POOL"] = {
        "available": True,
        "month": "2026-09",
        "symbols": {
            symbol: {
                "symbol": symbol,
                "name": "风险金股",
                "brokers": ["券商甲"],
                "source_refs": ["broker:risk"],
            },
        },
    }
    snapshot["MAIN_BUSINESS_EVIDENCE"][symbol] = {"available": False, "evidence": []}
    snapshot["RISK_EVENTS"] = {
        "available": True,
        "records": [{"symbol": symbol, "severity": "HIGH", "event_type": "FRAUD"}],
    }

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    item = result.decisions[0]

    assert item["status"] == "LOCAL_ACTIVE_CANDIDATE"
    assert item["autonomous_status"] == "HARD_REJECT"
    assert item["selection_basis"] == "BROKER_GOLD_DIRECT"
    assert item["downstream_trade_eligible"] is False
    assert "A1_RISK_EVENT_PRESENT" in item["reason_codes"]
    assert "A1_INSTITUTIONAL_DIRECT_ENTRY" in item["reason_codes"]
    assert local_active_items(result)[0]["business_exposure"] is None


def test_a1_fundamental_baseline_fills_minimum_with_industry_dispersion() -> None:
    snapshot = _snapshot(10)
    symbols = snapshot["g0_symbols"]
    snapshot["A1_POOL_TARGETS"] = {
        "active_research_target": [5, 6],
        "quota_fill_enabled": True,
        "fundamental_baseline": {
            "enabled": True,
            "minimum_data_quality": 75,
            "minimum_financial_quality": 60,
            "minimum_liquidity_score": 50,
            "maximum_per_industry": 1,
        },
    }
    # Keep every row outside the monthly discovery mapping so the test proves
    # that the baseline is not a disguised theme route.
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"] = [
        {
            "thscode": symbol,
            "mapping_status": "MAPPED",
            "memberships": [{
                "industry_thscode": f"IND{index:03d}.TI",
                "industry_name": f"行业{index}",
            }],
        }
        for index, symbol in enumerate(symbols)
    ]
    # Financial statements and indicators remain available, while business
    # extraction is intentionally absent.  Baseline eligibility must not
    # invent a revenue percentage to compensate.
    snapshot["MAIN_BUSINESS_EVIDENCE"] = {
        symbol: {"available": False, "evidence": []}
        for symbol in symbols
    }
    snapshot["BROKER_GOLD_COVERAGE_POOL"] = {
        "available": True,
        "month": "2026-09",
        "symbols": {
            symbols[-1]: {
                "symbol": symbols[-1],
                "name": "机构金股",
                "brokers": ["券商甲"],
                "source_refs": ["broker:direct"],
            },
        },
    }
    snapshot["RISK_EVENTS"] = {
        "available": True,
        "records": [{"symbol": symbols[1], "severity": "HIGH", "event_type": "FRAUD"}],
    }

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    active = [item for item in result.decisions if item["status"] == "LOCAL_ACTIVE_CANDIDATE"]
    projected = local_active_items(result)
    baseline = [item for item in active if item["selection_basis"] == "FUNDAMENTAL_BASELINE"]
    direct = next(item for item in active if item["selection_basis"] == "BROKER_GOLD_DIRECT")

    assert len(active) == 5
    assert len(projected) == 5
    assert len(baseline) == 4
    assert direct["downstream_trade_eligible"] is True
    assert all(item["research_route"] == "FUNDAMENTAL_BASELINE" for item in baseline)
    assert all(item["theme_id"].startswith("INDUSTRY:") for item in baseline)
    assert all(item["node_id"].startswith("BASELINE:") for item in baseline)
    assert all(item["business_exposure"] is None for item in projected if item["selection_basis"] == "FUNDAMENTAL_BASELINE")
    assert len({item["node_id"] for item in baseline}) == len(baseline)
    assert symbols[1] not in {item["symbol"] for item in active}
    assert result.summary["selection_basis_counts"] == {
        "LLM_REVIEWED": 0,
        "DETERMINISTIC_SCORE": 0,
        "QUOTA_FILL": 0,
        "BROKER_GOLD_DIRECT": 1,
        "FUNDAMENTAL_BASELINE": 4,
        "HALF_YEAR_FUNDAMENTAL": 0,
    }


def test_a2_upstream_research_only_is_hard_rejected_without_llm() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    row = {
        "symbol": symbol,
        "candidate_id": "a1:research-only",
        "primary_theme": "theme-compute",
        "industry_chain_node": "node-compute-device",
        "research_route": "BROKER_GOLD_DIRECT",
        "downstream_trade_eligible": False,
        "a2_factor_scores": _complete_a2_factor_scores(90),
        "data_quality_score": 95,
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
    )
    decision = result.decisions[0]

    assert decision["status"] == "HARD_REJECT"
    assert result.review_symbols == ()
    assert decision["downstream_trade_eligible"] is False
    assert "A2_UPSTREAM_RESEARCH_ONLY" in decision["reason_codes"]
    assert "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE" in decision["reason_codes"]
    assert all(not route["eligible"] for route in decision["route_eligibility"].values())
    assert all(route.get("blocked_by_upstream") is True for route in decision["route_eligibility"].values())


def test_a2_market_core_allows_research_route_without_business_exposure() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    row = {
        "symbol": symbol,
        "candidate_id": "a1:baseline",
        "primary_theme": "INDUSTRY:884001.TI",
        "industry_chain_node": "BASELINE:884001.TI",
        "research_route": "FUNDAMENTAL_BASELINE",
        "downstream_trade_eligible": True,
        "a2_factor_scores": _complete_a2_factor_scores(90),
        "data_quality_score": 95,
        "source_refs": ["cninfo:baseline:2026q2"],
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
    )
    decision = result.decisions[0]

    assert decision["status"] == "REVIEW_CANDIDATE"
    assert "MARKET_CORE" in decision["eligible_routes"]
    assert "A1_BUSINESS_EVIDENCE_MISSING" not in decision["route_eligibility"]["MARKET_CORE"]["missing_reason_codes"]
    assert "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE" in decision["reason_codes"]
    assert "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE" in decision["route_eligibility"]["MARKET_CORE"]["diagnostic_reason_codes"]


def test_a2_market_core_uses_research_route_when_direct_entry_has_no_theme_mapping() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    row = {
        "symbol": symbol,
        "candidate_id": "a1:broker-direct-no-theme",
        "research_route": "BROKER_GOLD_DIRECT",
        "downstream_trade_eligible": True,
        "a2_factor_scores": _complete_a2_factor_scores(90),
        "data_quality_score": 95,
        "source_refs": ["broker:direct:2026-09"],
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
    )
    decision = result.decisions[0]
    market = decision["route_eligibility"]["MARKET_CORE"]
    supply = decision["route_eligibility"]["SUPPLY_CHAIN_ALPHA"]

    assert decision["status"] == "REVIEW_CANDIDATE"
    assert "MARKET_CORE" in decision["eligible_routes"]
    assert "A1_THEME_MISSING" not in market["missing_reason_codes"]
    assert "A1_CHAIN_NODE_MISSING" not in market["missing_reason_codes"]
    assert "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_STRUCTURAL_MAPPING" in market["diagnostic_reason_codes"]
    assert "A2_UPSTREAM_RESEARCH_ROUTE_WITHOUT_BUSINESS_EXPOSURE" in market["diagnostic_reason_codes"]
    assert supply["eligible"] is False
    assert "A2_BOTTLENECK_EVIDENCE_INSUFFICIENT" in supply["missing_reason_codes"]


def test_screen_a2_only_sends_market_strength_top_three_themes_to_review() -> None:
    snapshot = _snapshot(8)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "source_id": "TEST_CAPITAL_FLOW",
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    theme_scores = {
        "theme-first": 95,
        "theme-second": 85,
        "theme-third": 75,
        "theme-fourth": 65,
    }
    rows = []
    for index, symbol in enumerate(snapshot["g0_symbols"]):
        theme = list(theme_scores)[index // 2]
        rows.append({
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": theme,
            "industry_chain_node": f"node:{theme}",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _complete_a2_factor_scores(theme_scores[theme]),
            "data_quality_score": 90,
        })

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
        rotation_theme_count=3,
    )
    by_theme: dict[str, list[dict]] = {}
    for item in result.decisions:
        by_theme.setdefault(item["theme_id"], []).append(item)

    assert {item["theme_id"] for item in result.decisions if item["top_rotation_theme"]} == {
        "theme-first",
        "theme-second",
        "theme-third",
    }
    assert all(item["sent_to_llm"] for theme in ("theme-first", "theme-second", "theme-third") for item in by_theme[theme])
    assert all(item["status"] == "LOCAL_MONITOR" for item in by_theme["theme-fourth"])
    assert all("A2_OUTSIDE_ROTATION_TOP_THEMES" in item["reason_codes"] for item in by_theme["theme-fourth"])
    assert {item["theme_rotation_rank"] for item in by_theme["theme-fourth"]} == {4}


def test_screen_a2_ranks_concrete_sector_indices_from_frozen_market_strength() -> None:
    snapshot = _snapshot(4)
    symbols = snapshot["g0_symbols"]
    codes = [f"I{index:03d}.TI" for index in range(4)]
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"] = [
        {
            "thscode": symbol,
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": code, "industry_name": f"板块{index}"}],
        }
        for index, (symbol, code) in enumerate(zip(symbols, codes))
    ]
    snapshot["A2_THEME_METRICS"] = {
        "theme_metrics": {
            f"INDUSTRY:{code}": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": code,
                "taxonomy_name": f"板块{index}",
                "score": strength,
                "member_count": 20,
                "return_coverage": 1.0,
            }
            for index, (code, strength) in enumerate(zip(codes, (40, 95, 85, 75)))
        }
    }
    rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "one-broad-monthly-theme",
            "industry_chain_node": f"node-{index}",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _complete_a2_factor_scores(90 - index),
            "data_quality_score": 90,
        }
        for index, symbol in enumerate(symbols)
    ]
    taxonomy_links = [
        {
            "node_id": f"node-{index}",
            "theme_id": "one-broad-monthly-theme",
            "taxonomy": "INDUSTRY",
            "taxonomy_code": code,
            "confidence": 1.0,
        }
        for index, code in enumerate(codes)
    ]

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows, "taxonomy_links": taxonomy_links},
        minimum_identifiability_score=0,
        review_all_eligible=True,
        rotation_theme_count=3,
    )
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[symbols[0]]["top_rotation_theme"] is False
    assert by_symbol[symbols[0]]["theme_rotation_rank"] == 4
    assert by_symbol[symbols[0]]["rotation_strength_source"] == "A2_THEME_METRICS"
    assert {item["rotation_direction_id"] for item in result.decisions if item["top_rotation_theme"]} == {
        f"INDUSTRY:{codes[1]}",
        f"INDUSTRY:{codes[2]}",
        f"INDUSTRY:{codes[3]}",
    }


def test_a2_full_market_top5_promotes_partial_trend_for_complete_llm_review() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    code = "I900.TI"
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = {
        "available": True,
        "trade_date": "2026-09-03",
        "records": [],
    }
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"] = [{
        "thscode": symbol,
        "mapping_status": "MAPPED",
        "memberships": [{"industry_thscode": code, "industry_name": "测试强板块"}],
    }]
    snapshot["A2_THEME_METRICS"] = {
        "available": True,
        "theme_metrics": {
            f"INDUSTRY:{code}": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": code,
                "taxonomy_name": "测试强板块",
                "score": 90,
                "breadth": 80,
                "turnover_share": 10,
                "weekly_confirmation_score": 80,
            }
        },
    }
    snapshot["A2_SECTOR_HEALTH_SNAPSHOT"] = {
        "available": True,
        "by_taxonomy": {
            "industry": {
                "sectors": [{
                    "taxonomy_code": code,
                    "taxonomy_name": "测试强板块",
                    "capital_flow": {
                        "available": True,
                        "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
                        "windows": {"today": {"main_net_cny": 1_000_000}},
                    },
                }]
            }
        },
    }
    snapshot["A2_FACTOR_SNAPSHOT"] = {
        symbol: {
            "relative_strength_score": 55,
            "a2_factor_scores": _trend_a2_factor_scores(80),
        }
    }
    snapshot["A2_SCORE_WEIGHTS"] = {
        name: 1.0 for name in _complete_a2_factor_scores(80)
    }
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {symbol: {"available": True, "capital_flow_score": 80}},
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-monthly",
        "industry_chain_node": "node-monthly",
        "business_exposure": {
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
            "evidence_basis": "COMPANY_DISCLOSURE",
        },
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
        "a2_factor_scores": _trend_a2_factor_scores(80),
        "data_quality_score": 90,
    }
    taxonomy_links = [{
        "node_id": "node-monthly",
        "theme_id": "theme-monthly",
        "taxonomy": "INDUSTRY",
        "taxonomy_code": code,
        "confidence": 1.0,
    }]

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row], "taxonomy_links": taxonomy_links},
        minimum_identifiability_score=0,
        review_all_eligible=True,
        rotation_theme_count=5,
    )
    decision = result.decisions[0]

    assert decision["rotation_input_source"] == "FULL_MARKET_ROTATION_FALLBACK"
    assert decision["top_rotation_theme"] is True
    assert decision["stock_behavior_type"] == "TREND"
    assert decision["trend_core_eligible"] is True
    assert decision["sent_to_llm"] is True
    assert result.review_symbols == (symbol,)


def test_screen_a2_available_selected_board_is_authoritative_over_conflicting_metrics() -> None:
    snapshot = _snapshot(3)
    symbols = snapshot["g0_symbols"]
    codes = [f"FULL{index:03d}.TI" for index in range(3)]
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"] = [
        {
            "thscode": symbol,
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": code, "industry_name": f"全市场板块{index}"}],
        }
        for index, (symbol, code) in enumerate(zip(symbols, codes))
    ]
    snapshot["A2_THEME_METRICS"] = {
        "available": True,
        "theme_metrics": {
            f"INDUSTRY:{code}": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": code,
                "taxonomy_name": f"全市场板块{index}",
                "score": strength,
                "breadth": 0.75,
                "turnover_share": 0.10,
                "weekly_confirmation_score": 80,
            }
            for index, (code, strength) in enumerate(zip(codes, (95, 85, 75)))
        },
    }
    snapshot["A2_SECTOR_HEALTH_SNAPSHOT"] = {
        "available": True,
        "by_taxonomy": {
            "industry": {
                "sectors": [
                    {
                        "taxonomy_code": code,
                        "taxonomy_name": f"全市场板块{index}",
                        "capital_flow": {
                            "available": True,
                            "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
                            "windows": {"today": {"main_net_cny": flow}},
                        },
                    }
                    for index, (code, flow) in enumerate(zip(codes, (3_000_000, 2_000_000, 1_000_000)))
                ],
            },
        },
    }
    # The versioned selected-board snapshot deliberately claims the weakest
    # full-market direction is the primary board for the first two A1 rows.
    # It is the production fixed-theme top-five contract and must win over
    # conflicting A2_THEME_METRICS/sector-health rankings.
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbols[0]: [{
                "board_code": "CURATED-WEAK",
                "strategy_theme_id": "theme-0",
                "board_name": "精选固定主题",
                "strength": 10,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
            symbols[1]: [{
                "board_code": "CURATED-WEAK",
                "strategy_theme_id": "theme-0",
                "board_name": "精选固定主题",
                "strength": 10,
                "main_net_inflow_cny": 900_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
                "is_child_board": True,
            }],
            symbols[2]: [{
                "board_code": "CURATED-OUTSIDE",
                "board_name": "精选非前五主题",
                "strength": 9999,
                "main_net_inflow_cny": 9_999_999,
                "selected_for_rotation": True,
                "primary_rank": 6,
            }],
        },
    }
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {symbol: {"available": True, "capital_flow_score": 90} for symbol in symbols},
    }
    rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-0" if index < 2 else f"theme-{index}",
            "industry_chain_node": f"node-{index}",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _trend_a2_factor_scores(90),
            "data_quality_score": 90,
            "business_exposure_facts": [{
                "revenue_exposure_pct": 65,
                "source_ref": f"cninfo:{symbol}",
            }],
        }
        for index, symbol in enumerate(symbols)
    ]
    taxonomy_links = [
        {
            "node_id": f"node-{index}",
            "theme_id": "theme-0" if index < 2 else f"theme-{index}",
            "taxonomy": "INDUSTRY",
            "taxonomy_code": code,
            "confidence": 1.0,
        }
        for index, code in enumerate(codes)
    ]

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows, "taxonomy_links": taxonomy_links},
        minimum_identifiability_score=0,
        review_all_eligible=True,
        rotation_theme_count=1,
    )
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[symbols[0]]["rotation_direction_id"] == "SELECTED_BOARD:CURATED-WEAK"
    assert by_symbol[symbols[0]]["top_rotation_theme"] is True
    assert by_symbol[symbols[0]]["theme_rotation_rank"] == 1
    assert by_symbol[symbols[0]]["rotation_strength_source"] == "LEGACY_SELECTED_BOARD_STRENGTH"
    assert by_symbol[symbols[0]]["rotation_input_source"] == "SELECTED_BOARD_SNAPSHOT"
    assert by_symbol[symbols[0]]["full_market_rotation"] is None
    assert by_symbol[symbols[1]]["rotation_direction_id"] == "SELECTED_BOARD:CURATED-WEAK"
    assert by_symbol[symbols[1]]["top_rotation_theme"] is True
    assert by_symbol[symbols[1]]["theme_rotation_rank"] == 1
    assert by_symbol[symbols[1]]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[symbols[2]]["status"] == "LOCAL_MONITOR"
    assert by_symbol[symbols[2]]["trend_core_eligible"] is False
    assert by_symbol[symbols[2]]["selected_board"] is None
    assert "A2_TREND_OUTSIDE_SELECTED_BOARD_TOP5" in by_symbol[symbols[2]]["reason_codes"]
    assert set(result.review_symbols) == set(symbols[:2])


def test_screen_a2_available_selected_board_opens_only_its_top_five_rows() -> None:
    snapshot = _snapshot(2)
    symbols = snapshot["g0_symbols"]
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {
            symbols[1]: [{
                "board_code": "CURATED-ONLY",
                "strategy_theme_id": "theme-monthly",
                "board_name": "精选兼容板块",
                "strength": 100,
                "main_net_inflow_cny": 1_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
        },
    }
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "by_symbol": {symbol: {"available": True, "capital_flow_score": 90} for symbol in symbols},
    }
    rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-monthly",
            "industry_chain_node": "node-monthly",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _trend_a2_factor_scores(90),
            "data_quality_score": 90,
            "business_exposure_facts": [{
                "revenue_exposure_pct": 65,
                "source_ref": f"cninfo:{symbol}",
            }],
        }
        for symbol in symbols
    ]
    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert by_symbol[symbols[1]]["rotation_direction_id"] == "SELECTED_BOARD:CURATED-ONLY"
    assert by_symbol[symbols[1]]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[symbols[1]]["trend_core_eligible"] is True
    assert by_symbol[symbols[1]]["rotation_input_source"] == "SELECTED_BOARD_SNAPSHOT"
    assert symbols[1] in result.review_symbols
    assert by_symbol[symbols[0]]["status"] == "LOCAL_MONITOR"
    assert by_symbol[symbols[0]]["trend_core_eligible"] is False
    assert "A2_TREND_OUTSIDE_SELECTED_BOARD_TOP5" in by_symbol[symbols[0]]["reason_codes"]


def test_screen_a2_selected_board_unavailable_fails_closed_even_with_metrics() -> None:
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    code = "NOFLOW.TI"
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"][0]["memberships"] = [{
        "industry_thscode": code,
        "industry_name": "无正流入板块",
    }]
    snapshot["A2_THEME_METRICS"] = {
        "available": True,
        "theme_metrics": {
            f"INDUSTRY:{code}": {
                "available": True,
                "taxonomy": "INDUSTRY",
                "taxonomy_code": code,
                "taxonomy_name": "无正流入板块",
                "score": 95,
            },
        },
    }
    snapshot["A2_SECTOR_HEALTH_SNAPSHOT"] = {
        "available": True,
        "by_taxonomy": {
            "industry": {
                "sectors": [{
                    "taxonomy_code": code,
                    "taxonomy_name": "无正流入板块",
                    "capital_flow": {
                        "available": True,
                        "windows": {"today": {"main_net_cny": 0}},
                    },
                }],
            },
        },
    }
    # An explicit unavailable production snapshot must fail closed for trend;
    # complete A2 metrics/health facts are not a fallback once the field exists.
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {
        "available": False,
        "reason_code": "SELECTED_BOARD_SNAPSHOT_SOURCE_UNAVAILABLE",
        "by_symbol": {
            symbol: [{
                "board_code": "SHOULD-NOT-BYPASS",
                "strength": 999,
                "main_net_inflow_cny": 2_000_000,
                "selected_for_rotation": True,
                "primary_rank": 1,
            }],
        },
    }
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-monthly",
        "industry_chain_node": "node-monthly",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
        "a2_factor_scores": _trend_a2_factor_scores(90),
        "data_quality_score": 90,
        "business_exposure_facts": [{
            "revenue_exposure_pct": 65,
            "source_ref": f"cninfo:{symbol}",
        }],
    }
    result = screen_a2(
        snapshot,
        {
            "active_research_pool": [row],
            "taxonomy_links": [{
                "node_id": "node-monthly",
                "theme_id": "theme-monthly",
                "taxonomy": "INDUSTRY",
                "taxonomy_code": code,
                "confidence": 1.0,
            }],
        },
        minimum_identifiability_score=0,
        review_all_eligible=True,
    )
    item = result.decisions[0]

    assert item["status"] == "LOCAL_MONITOR"
    assert item["trend_core_eligible"] is False
    assert item["full_market_rotation"] is None
    assert item["rotation_fallback"] is None
    assert item["rotation_input_source"] == "SELECTED_BOARD_SNAPSHOT_UNAVAILABLE"
    assert "A2_SELECTED_BOARD_SOURCE_UNAVAILABLE" in item["reason_codes"]
    assert result.review_symbols == ()


def test_screen_a2_market_core_needs_only_two_hard_facts_when_optional_facts_missing() -> None:
    snapshot = _snapshot(1)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {"available": False, "reason_code": "SOURCE_FAILED"}
    symbol = snapshot["g0_symbols"][0]
    # Remove the fixture's technical relative-strength proxy so this boundary
    # exercises exactly two hard facts: breadth and turnover share.
    snapshot["FACTOR_SNAPSHOT"] = {symbol: {}}
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        "primary_theme": "theme-core",
        "industry_chain_node": "node-core",
        "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
        "a2_factor_scores": {
            "breadth": {"score": 80},
            "turnover_share": {"score": 80},
        },
        "data_quality_score": 90,
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
    )
    decision = result.decisions[0]

    assert decision["route"] == "MARKET_CORE"
    assert decision["status"] == "REVIEW_CANDIDATE"
    assert result.review_symbols == (symbol,)
    assert decision["critical_factor_coverage"]["sufficient"] is True
    assert "capital_flow" in decision["missing_optional_factors"]
    assert "tier_structure" in decision["missing_optional_factors"]
    assert "index_chain_resonance" in decision["missing_optional_factors"]


def test_screen_a2_records_all_effective_gate_failures_without_short_circuiting():
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["FACTOR_SNAPSHOT"] = {symbol: {}}
    snapshot["A2_FACTOR_SNAPSHOT"] = {symbol: {}}
    snapshot["MIN_IDENTIFIABILITY_SCORE"] = 101
    snapshot["MIN_THEME_SCORE"] = 60
    snapshot["LEADER_MIN_CRITERIA"] = 4
    snapshot["MAX_LEADERS_PER_THEME"] = 2
    snapshot["MIN_FREE_FLOAT_CAP"] = 3_000_000_000
    row = {
        "symbol": symbol,
        "candidate_id": f"a1:{symbol}",
        # Missing A1 lineage and market facts intentionally exercise several
        # independent failures in the same row.
    }

    result = screen_a2(
        snapshot,
        {"active_research_pool": [row]},
        minimum_identifiability_score=101,
        llm_top_n_per_theme=1,
        review_all_eligible=True,
    )
    decision = result.decisions[0]
    expected_gates = {
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
    }

    assert set(decision["gate_results"]) == expected_gates
    assert {
        "LOCAL_DATA_SUFFICIENCY",
        "LOCAL_ELIGIBILITY",
        "ROUTE_REQUIREMENT",
    }.issubset(
        decision["all_failed_gates"]
    )
    assert "SENT_TO_LLM" not in decision["all_failed_gates"]
    assert decision["first_blocking_gate"] == "LOCAL_DATA_SUFFICIENCY"
    data_gate = decision["gate_results"]["LOCAL_DATA_SUFFICIENCY"]
    assert data_gate["value"] == "INSUFFICIENT"
    assert data_gate["passed"] is False
    assert data_gate["blocks_decision"] is True
    eligibility_gate = decision["gate_results"]["LOCAL_ELIGIBILITY"]
    assert eligibility_gate["value"] == "DATA_GAP"
    assert eligibility_gate["passed"] is False
    assert eligibility_gate["blocks_decision"] is True
    identity_gate = decision["gate_results"]["IDENTIFIABILITY_MIN"]
    assert identity_gate["available"] is True
    assert identity_gate["value"] < identity_gate["threshold"] == 101.0
    assert identity_gate["passed"] is False
    assert identity_gate["applied"] is False
    assert identity_gate["blocks_decision"] is False
    assert identity_gate["reason_code"] == "A2_IDENTIFIABILITY_BELOW_MINIMUM_REVIEW_ONLY"
    # Missing/unimplemented facts remain explicit but cannot become a hidden
    # veto or a false positive.
    assert decision["gate_results"]["TIER_STRUCTURE"]["available"] is False
    assert decision["gate_results"]["TIER_STRUCTURE"]["passed"] is None
    assert "TIER_STRUCTURE" not in decision["all_failed_gates"]
    assert decision["gate_results"]["LEADER_MIN_CRITERIA"]["available"] is False
    assert decision["gate_results"]["FREE_FLOAT_CAP"]["available"] is False

    counts = result.summary["gate_block_counts"]
    assert set(counts) == expected_gates
    assert all(
        counts[name] >= 1
        for name in (
            "LOCAL_DATA_SUFFICIENCY",
            "LOCAL_ELIGIBILITY",
            "ROUTE_REQUIREMENT",
        )
    )
    assert counts["SENT_TO_LLM"] == 0
    assert sum(counts.values()) >= result.summary["rejected_count"]


def test_screen_a2_gate_attribution_preserves_review_and_monitor_symbol_sets():
    snapshot = _snapshot(4)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "source_id": "TEST_CAPITAL_FLOW",
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    rows = [
        {
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": _complete_a2_factor_scores(80 - index),
            "data_quality_score": 90,
        }
        for index, symbol in enumerate(snapshot["g0_symbols"][:3])
    ]

    before = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=False,
    )
    after = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
        review_all_eligible=False,
    )

    assert set(before.review_symbols) == set(after.review_symbols)
    assert set(before.monitor_symbols) == set(after.monitor_symbols)
    assert set(before.rejected_symbols) == set(after.rejected_symbols)

    clipped = [
        item for item in after.decisions
        if item.get("status") == "LOCAL_MONITOR"
        and item.get("gate_results", {}).get("SENT_TO_LLM", {}).get("reason_code")
        == "A2_NOT_SENT_TO_LLM"
    ]
    assert clipped
    for item in clipped:
        assert item["gate_results"]["LOCAL_ELIGIBILITY"]["passed"] is True
        assert item["gate_results"]["SENT_TO_LLM"]["blocks_decision"] is True
        assert "SENT_TO_LLM" in item["all_failed_gates"]


def test_screen_a2_rejects_non_positive_review_budget() -> None:
    with pytest.raises(ValueError, match="Top-N value must be positive"):
        screen_a2(_snapshot(1), {"active_research_pool": []}, llm_top_n_per_theme=0)


def test_material_shareholder_reduction_is_an_a1_hard_risk_fact() -> None:
    from liangjian_funnel.pipeline.deterministic import _hard_risk_events, _hard_risk_symbols

    risks = {
        "available": True,
        "source": "CNINFO_PUBLIC_ANNOUNCEMENTS",
        "by_symbol": {
            "600001.SH": [{
                "symbol": "600001.SH",
                "event_type": "股东大幅减持",
                "event_time": "2026-09-01T15:00:00+08:00",
            }],
            "600002.SH": [{
                "symbol": "600002.SH",
                "event_type": "常规减持进展",
                "event_time": "2026-09-01T15:00:00+08:00",
            }],
        },
    }

    assert _hard_risk_symbols(risks) == {"600001.SH"}
    assert _hard_risk_events(risks)["600001.SH"][0]["publish_time"] == "2026-09-01T15:00:00+08:00"
