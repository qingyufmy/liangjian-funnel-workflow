from __future__ import annotations

import json

import pytest

from liangjian_funnel.pipeline.chinese_export import (
    build_chinese_export_rows,
    validate_stage_lineage,
)


def _outputs() -> tuple[dict, dict, dict]:
    a1 = {
        "active_research_pool": [
            {"symbol": "600001.SH", "company_name": "情绪甲", "selection_basis": "DAILY_EMOTION_OVERLAY", "eastmoney_hot_rank": 8, "primary_theme": "军工"},
            {"symbol": "600002.SH", "company_name": "趋势乙", "selection_basis": "MONTHLY_THEME", "primary_theme": "AI_COMPUTE", "monthly_direction_name": "算力"},
            {"symbol": "600003.SH", "company_name": "波段丙", "selection_basis": "MONTHLY_THEME", "primary_theme": "消费电子"},
        ]
    }
    a2 = {
        "focus_pool": [
            {"symbol": "600001.SH", "company_name": "情绪甲", "a2_pool_channel": "EMOTION", "eastmoney_hot_rank": 8, "emotion_cycle_stage": "STARTUP", "ladder_height": 1, "primary_theme": "军工"},
            {"symbol": "600002.SH", "company_name": "趋势乙", "a2_pool_channel": "TREND", "selected_board": {"board_name": "算力", "primary_rank": 2, "main_net_inflow_cny": 2_467_000_000}},
        ],
        "watch_only_pool": [
            {"symbol": "600003.SH", "company_name": "波段丙", "a2_pool_channel": "TREND", "selected_board": {"board_name": "消费电子", "primary_rank": 3, "main_net_inflow_cny": 32_860_000}},
        ],
    }
    a3 = {
        "core_watch_pool": [
            {"symbol": "600001.SH", "company_name": "情绪甲", "strategy_profile": "LEADER_INTRADAY", "plan_mode": "PROBE", "primary_theme": "军工"},
            {"symbol": "600002.SH", "company_name": "趋势乙", "strategy_profile": "TREND_MA5", "selected_board": {"board_name": "算力"}},
        ],
        "secondary_watch_pool": [
            {"symbol": "600003.SH", "company_name": "波段丙", "strategy_profile": "MA520_SWING", "selected_board": {"board_name": "消费电子"}},
        ],
    }
    return a1, a2, a3


def test_export_projection_is_chinese_and_preserves_three_a3_routes() -> None:
    result = build_chinese_export_rows(*_outputs())

    assert result["自查"] == {
        "通过": True,
        "A1数量": 3,
        "A2数量": 3,
        "A3数量": 3,
        "A2未包含于A1": [],
        "A3未包含于A2": [],
    }
    assert {row["类别"] for row in result["A2"]} == {"情绪票", "趋势票"}
    assert result["A1"][1]["板块"] == "算力"
    assert {row["策略"] for row in result["A3"]} == {
        "情绪龙头",
        "五日与二十日均线波段",
        "五日线趋势",
    }
    rendered = json.dumps(result, ensure_ascii=False)
    for token in ("DAILY_EMOTION_OVERLAY", "LEADER_INTRADAY", "TREND_MA5", "MA520_SWING"):
        assert token not in rendered


def test_export_refuses_broken_a1_a2_a3_lineage() -> None:
    a1, a2, a3 = _outputs()
    a3["core_watch_pool"].append({"symbol": "600999.SH"})

    with pytest.raises(ValueError, match="A3存在未进入A2"):
        validate_stage_lineage(a1, a2, a3)


def test_export_does_not_describe_unresolved_a2_row_as_trend() -> None:
    a1, a2, a3 = _outputs()
    a1["active_research_pool"].append(
        {"symbol": "600004.SH", "company_name": "未决丁", "primary_theme": "金融"}
    )
    a2["watch_only_pool"].append(
        {
            "symbol": "600004.SH",
            "company_name": "未决丁",
            "a2_pool_channel": "NONE",
            "stock_behavior_type": "UNRESOLVED",
            "selected_board": {"board_name": "金融保险"},
        }
    )

    result = build_chinese_export_rows(a1, a2, a3)
    row = next(item for item in result["A2"] if item["代码"] == "600004")

    assert row["类别"] == "待分类"
    assert "尚未确认" in row["入选理由"]
    assert "识别为趋势票" not in row["入选理由"]
