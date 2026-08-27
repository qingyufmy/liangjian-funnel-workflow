from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.monthly_strategy import build_monthly_strategy_context
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
