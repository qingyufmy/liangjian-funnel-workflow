from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.monthly_strategy import (
    build_monthly_industry_decisions,
    build_monthly_strategy_context,
)
from liangjian_funnel.pipeline.a1_contract import A1_THEME_TARGET
from liangjian_funnel.pipeline.research import _monthly_discovery_reasons


AS_OF = datetime(2026, 8, 27, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_monthly_strategy_filters_future_and_keeps_cycle_and_prior_registry():
    context = build_monthly_strategy_context(
        {
            "MACRO_POLICY_FEED": {
                "available": True,
                "official_documents": [
                    {"fact_id": "old", "publish_time": "2026-04-01T10:00:00+08:00", "issuing_body": "A"},
                    {"fact_id": "current", "publish_time": "2026-08-20T10:00:00+08:00", "issuing_body": "A"},
                    {"fact_id": "future", "publish_time": "2026-08-28T10:00:00+08:00", "issuing_body": "B"},
                ],
            },
            "MACRO_ECONOMIC_DATA": {"available": True, "series": [{"id": "PMI", "value": 50.2}]},
            "INDUSTRY_PROFIT_DATA": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
            "INDUSTRY_ACTIVITY_DATA": {
                "available": True,
                "metric_scope": "INDUSTRIAL_VALUE_ADDED_GROWTH_NOT_PROFIT",
                "items": [{"industry": "煤炭开采和洗选业", "yoy": 4.2}],
            },
            "BROKER_RESEARCH_CONSENSUS": {
                "available": True,
                "evidence_tier": "T2",
                "primary_evidence": False,
                "viewpoint_only": True,
                "documents": [{"document_id": "sept-view", "effective_month": "2026-09"}],
            },
            "SECTOR_CYCLE_SNAPSHOT": {
                "available": True,
                "history_metrics": {
                    "monthly_rotation_candidates": [
                        {"industry_thscode": "881168.TI", "industry_name": "工业金属", "return_20d": 0.08}
                    ]
                },
            },
        },
        as_of=AS_OF,
        prior_registry={
            "as_of": "2026-07-31T15:10:00+08:00",
            "version_hash": "abc",
            "themes": [{"theme_id": "prior-tech"}],
            "nodes": [{"node_id": "prior-node"}],
        },
        policy_lookback_days=120,
    )

    assert context["status"] == "READY"
    assert [item["fact_id"] for item in context["policy_window"]["official_documents"]] == ["current"]
    assert context["monthly_industry_rotation"][0]["industry_name"] == "工业金属"
    assert context["prior_theme_registry"]["themes"][0]["theme_id"] == "prior-tech"
    assert context["pillar_availability"]["industry_cycle_fundamentals"] is True
    assert context["industry_activity_state"]["metric_scope"] == "INDUSTRIAL_VALUE_ADDED_GROWTH_NOT_PROFIT"
    assert context["runtime_contract"]["stock_selection_forbidden"] is True
    assert context["weekly_strategy_context"]["status"] == "DEGRADED"
    assert context["weekly_strategy_context"]["industry_rotation"][0]["weekly_state"] == "UNKNOWN"
    assert context["runtime_contract"]["weekly_overlay_cannot_override_monthly_domain"] is True
    assert context["broker_research_consensus"]["documents"][0]["document_id"] == "sept-view"
    assert context["broker_research_consensus"]["primary_evidence"] is False


def test_monthly_strategy_reports_missing_macro_instead_of_substituting_news():
    context = build_monthly_strategy_context(
        {
            "MACRO_POLICY_FEED": {"available": True, "official_documents": []},
            "MACRO_ECONOMIC_DATA": {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
            "SECTOR_CYCLE_SNAPSHOT": {"available": False},
        },
        as_of=AS_OF,
    )
    assert context["status"] == "BLOCKED"
    assert "macro_economy" in context["missing_pillars"]
    assert context["macro_economic_state"]["available"] is False


def test_monthly_coverage_counts_taxonomy_links_nested_on_chain_nodes():
    rotations = [
        {"industry_thscode": f"88400{index}.TI"}
        for index in range(1, 6)
    ]
    nodes = [
        {
            "node_id": f"node-{index}",
            "theme_ids": [f"theme-{index % 6}"],
            "taxonomy_links": {"industry_thscodes": [f"88400{(index % 5) + 1}.TI"]},
        }
        for index in range(12)
    ]
    output = {
        "structural_themes": [{"theme_id": f"theme-{index}"} for index in range(6)],
        "industry_chain_graph": nodes,
        "taxonomy_links": [],
    }
    context = {
        "g0_symbol_count": 3886,
        "status": "READY",
        "monthly_industry_rotation": rotations,
    }

    assert _monthly_discovery_reasons(output, context) == ()


def test_monthly_rotation_decisions_cover_top20_without_silent_omission():
    rotations = [
        {
            "industry_thscode": f"884{index:03d}.TI",
            "industry_name": f"行业{index}",
            "return_20d": 0.12 if index == 1 else (-0.01 if index == 2 else 0.04),
            "relative_strength_percentile_20d": 0.82 if index == 1 else (0.25 if index == 2 else 0.55),
            "top10_appearance_count": 3 if index != 3 else 1,
            "source_ref": f"ths:sector:{index}",
        }
        for index in range(1, 21)
    ]
    rotations[2].pop("return_20d")
    decisions, coverage = build_monthly_industry_decisions(rotations)

    assert len(decisions) == 20
    assert [item["rank"] for item in decisions] == list(range(1, 21))
    assert {item["decision"] for item in decisions} == {"INCLUDE", "DEFER"}
    assert coverage["status"] == "READY"
    assert coverage["top10_complete"] is True
    assert decisions[0]["decision"] == "INCLUDE"
    assert decisions[1]["decision"] == "DEFER"
    assert decisions[1]["structural_status"] == "SUPPORTED"
    assert decisions[1]["timing_state"] == "COOLING"
    assert "MONTHLY_STRUCTURE_RETAINED_WAIT_TIMING" in decisions[1]["reason_codes"]
    assert decisions[2]["decision"] == "DEFER"
    assert "return_20d" in decisions[2]["data_gaps"]


def test_monthly_rotation_excludes_only_when_structure_is_unproven_and_timing_is_cooling():
    decisions, _ = build_monthly_industry_decisions([
        {
            "industry_thscode": "884999.TI",
            "industry_name": "弱结构行业",
            "return_20d": -0.08,
            "relative_strength_percentile_20d": 0.22,
            "top10_appearance_count": 1,
            "source_ref": "ths:sector:weak",
        }
    ])

    assert decisions[0]["decision"] == "EXCLUDE"
    assert decisions[0]["structural_status"] == "INSUFFICIENT"
    assert decisions[0]["timing_state"] == "COOLING"


def test_monthly_discovery_requires_one_decision_per_frozen_rotation_row():
    codes = [f"88400{index}.TI" for index in range(1, 4)]
    themes = [{"theme_id": f"theme-{index}"} for index in range(6)]
    nodes = [
        {
            "node_id": f"node-{index}",
            "theme_ids": [f"theme-{index % 6}"],
            "taxonomy_links": {"industry_thscodes": [codes[index % len(codes)]]},
        }
        for index in range(12)
    ]
    context = {
        "g0_symbol_count": 3886,
        "status": "READY",
        "monthly_industry_rotation": [
            {"industry_thscode": code} for code in codes
        ],
        "monthly_industry_decisions": [
            {"rank": rank, "industry_thscode": code}
            for rank, code in enumerate(codes, start=1)
        ],
        "monthly_rotation_coverage": {"decision_version": "test", "status": "INCOMPLETE"},
    }
    base = {
        "structural_themes": themes,
        "industry_chain_graph": nodes,
        "taxonomy_links": [],
    }

    assert "A1_MONTHLY_ROTATION_DECISIONS_MISSING" in _monthly_discovery_reasons(base, context)

    complete = {
        **base,
        "monthly_industry_decisions": [
            {
                "rank": 1,
                "industry_thscode": codes[0],
                "decision": "INCLUDE",
                "mapped_theme_ids": ["theme-0"],
            },
            {
                "rank": 2,
                "industry_thscode": codes[1],
                "decision": "EXCLUDE",
                "mapped_theme_ids": [],
            },
            {
                "rank": 3,
                "industry_thscode": codes[2],
                "decision": "DEFER",
                "mapped_theme_ids": [],
            },
        ],
    }
    assert _monthly_discovery_reasons(complete, context) == ()


def test_contract_v3_uses_explicit_industry_mappings_instead_of_legacy_node_codes():
    codes = [f"884{index:03d}.TI" for index in range(1, 21)]
    theme_count = A1_THEME_TARGET[0]
    themes = [{"theme_id": f"theme-{index}"} for index in range(theme_count)]
    nodes = [
        {
            "node_id": f"node-{index}",
            "theme_ids": [f"theme-{index % theme_count}"],
        }
        for index in range(40)
    ]
    decisions = [
        {
            "rank": rank,
            "industry_thscode": code,
            "decision": "INCLUDE",
            "reason_codes": ["MONTHLY_RELATIVE_STRENGTH"],
            "supporting_source_refs": [f"ths:sector:{rank}"],
        }
        for rank, code in enumerate(codes, start=1)
    ]
    output = {
        "structural_themes": themes,
        "industry_chain_graph": nodes,
        "taxonomy_links": [],
        "industry_theme_mappings": [
            {
                "industry_thscode": code,
                "mapped_theme_ids": [f"theme-{(rank - 1) % theme_count}"],
                "mapping_status": "MAPPED",
                "supporting_source_refs": [f"ths:sector:{rank}"],
                "confidence": 0.8,
            }
            for rank, code in enumerate(codes, start=1)
        ],
    }
    context = {
        "g0_symbol_count": 3886,
        "status": "READY",
        "monthly_industry_rotation": [
            {"industry_thscode": code} for code in codes
        ],
        "monthly_industry_decisions": decisions,
        "monthly_rotation_coverage": {
            "requested_top_n": 20,
            "observed_count": 20,
            "status": "READY",
        },
    }

    assert _monthly_discovery_reasons(output, context) == ()


def test_contract_v3_rejects_partial_server_canonical_decision_set():
    codes = [f"884{index:03d}.TI" for index in range(1, 4)]
    theme_count = A1_THEME_TARGET[0]
    themes = [{"theme_id": f"theme-{index}"} for index in range(theme_count)]
    nodes = [
        {"node_id": f"node-{index}", "theme_ids": [f"theme-{index % theme_count}"]}
        for index in range(40)
    ]
    output = {
        "structural_themes": themes,
        "industry_chain_graph": nodes,
        "taxonomy_links": [],
        "industry_theme_mappings": [
            {
                "industry_thscode": code,
                "mapped_theme_ids": ["theme-0"],
                "mapping_status": "MAPPED",
            }
            for code in codes
        ],
    }
    context = {
        "g0_symbol_count": 3886,
        "status": "READY",
        "monthly_industry_decisions": [
            {"rank": rank, "industry_thscode": code, "decision": "EXCLUDE"}
            for rank, code in enumerate(codes, start=1)
        ],
        "monthly_rotation_coverage": {
            "requested_top_n": 20,
            "observed_count": 3,
            "status": "INCOMPLETE",
        },
    }

    assert "A1_CANONICAL_MONTHLY_DECISIONS_INCOMPLETE" in _monthly_discovery_reasons(output, context)
