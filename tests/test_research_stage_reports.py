from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from liangjian_funnel.pipeline.research_reports import write_stage_markdown_reports


def _stage(name: str, output: dict) -> SimpleNamespace:
    return SimpleNamespace(
        stage=name,
        status="VALIDATED",
        snapshot_id="snapshot-1",
        reason_codes=(),
        output=output,
    )


def test_stage_reports_keep_selected_rows_and_bound_whole_market_monitor(tmp_path: Path) -> None:
    monitor = [
        {"symbol": f"{index:06d}.SZ", "reason_codes": ["A1_OUTSIDE_DISCOVERED_THEME"]}
        for index in range(200)
    ]
    result = SimpleNamespace(
        run_id="run-1",
        status="READY",
        lanes=(
            SimpleNamespace(
                lane="lane_1",
                model="deepseek-v4-pro-0813",
                stages=(
                    _stage(
                        "A1",
                        {
                            "active_research_pool": [
                                {
                                    "symbol": "600001.SH",
                                    "company_name": "甲公司",
                                    "primary_theme": "T1",
                                    "industry_chain_node": "N1",
                                    "structural_score": 88,
                                    "reason_codes": ["SOURCE_VERIFIED"],
                                }
                            ],
                            "monitor_pool": monitor,
                            "rejected_candidates": [],
                        },
                    ),
                    _stage(
                        "A2",
                        {
                            "focus_pool": [
                                {
                                    "symbol": "600001.SH",
                                    "company_name": "甲公司",
                                    "theme_id": "T1",
                                    "selection_route": "MARKET_CORE",
                                }
                            ],
                            "watch_only_pool": [],
                            "rejected_candidates": [],
                        },
                    ),
                    _stage("A3", {"core_watch_pool": [], "secondary_watch_pool": [], "rejected_candidates": []}),
                ),
            ),
        ),
    )

    paths = write_stage_markdown_reports(result, tmp_path)

    a1 = Path(paths["A1"]).read_text(encoding="utf-8")
    assert "600001.SH" in a1
    assert "A1_OUTSIDE_DISCOVERED_THEME" in a1
    assert "200" in a1
    assert "000199.SZ" not in a1
    a2 = Path(paths["A2"]).read_text(encoding="utf-8")
    assert "MARKET_CORE" in a2
    assert "600001.SH" in a2
    assert "内部模拟、非投资建议" in a2


def test_a3_rejected_report_separates_blocker_background_and_a4_wait(tmp_path: Path) -> None:
    result = SimpleNamespace(
        run_id="run-a3-display",
        status="READY_DEGRADED",
        lanes=(SimpleNamespace(
            lane="lane_1",
            model="deepseek-v4-pro-0813",
            stages=(_stage("A3", {
                "core_watch_pool": [],
                "secondary_watch_pool": [],
                "rejected_candidates": [{
                    "symbol": "600001.SH",
                    "company_name": "甲公司",
                    "theme_id": "AI",
                    "strategy_profile": "LEADER_INTRADAY",
                    "eligibility": "REJECTED",
                    "reason_codes": [
                        "LEADER_THEME_RETREAT_OR_CLIMAX",
                        "MARKET_RISK_OFF",
                        "A4_RETEST_CONFIRMATION_REQUIRED",
                    ],
                    "veto_conditions": ["LEADER_THEME_RETREAT_OR_CLIMAX"],
                }],
            }),),
        ),),
    )

    paths = write_stage_markdown_reports(result, tmp_path)
    report = Path(paths["A3"]).read_text(encoding="utf-8")
    assert "| 状态 | 主要阻断 | 其他条件 | 背景风险 | A4待确认 |" in report
    assert "题材处于高潮、分化或退潮阶段" in report
    assert "市场处于防守背景" in report
    assert "由A4等待盘中回踩确认" in report
