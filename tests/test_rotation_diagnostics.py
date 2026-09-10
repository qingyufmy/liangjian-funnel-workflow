from liangjian_funnel.pipeline.rotation_diagnostics import rotation_coverage
from liangjian_funnel.pipeline.research import _annotate_a2_pool_target


def test_empty_a1_board_is_not_hidden_or_filled_from_other_themes():
    snapshot = {"A2_ROTATION_THEME_COUNT": 5, "SELECTED_BOARD_SNAPSHOT": {
        "available": True, "selected_primary_boards": [{"board_code": "BANK", "rank": 1},
            {"board_code": "TECH", "rank": 2}],
        "by_symbol": {"600000.SH": [{"board_code": "BANK"}], "000001.SZ": [{"board_code": "TECH"}]}}}
    decisions = [{"symbol": "000001.SZ", "status": "REVIEW_CANDIDATE", "stock_behavior_type": "TREND"}]
    output = {"focus_pool": [], "watch_only_pool": [{"symbol": "000001.SZ"}]}
    report = rotation_coverage(snapshot, decisions, output)
    assert report["selected_board_count"] == 2
    assert report["boards"][0]["reason"] == "A1_HAS_NO_BOARD_MEMBERS"
    assert report["boards"][0]["mapped_market_member_count"] == 1
    assert report["boards"][1]["quant_review_count"] == report["boards"][1]["watch_count"] == 1
    assert output["focus_pool"] == []


def test_final_a2_summary_recounts_appended_rows():
    output = {"analysis_summary": {"effective_research_pool_count": 1}, "focus_pool": [{"symbol": "000001.SZ"}],
        "watch_only_pool": [{"symbol": "000002.SZ", "stock_behavior_type": "TREND"}]}
    summary = _annotate_a2_pool_target(output, {})["analysis_summary"]
    assert summary["effective_research_pool_count"] == 2
    assert summary["watch_only_pool_count"] == 1
