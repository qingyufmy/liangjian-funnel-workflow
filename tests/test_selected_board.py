from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.selected_board import normalize_selected_board_snapshot


TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 2)


def _payload(boards):
    return {
        "trade_date": DAY.isoformat(),
        "captured_at": "2026-09-02T15:10:00+08:00",
        "source_url": "https://example.test/selected-board",
        "boards": boards,
    }


def _board(code, name, strength, inflow, members, parent=None):
    return {
        "board_code": code,
        "board_name": name,
        "strength": strength,
        "main_net_inflow_cny": inflow,
        "parent_board_code": parent,
        "constituents": members,
    }


def test_positive_flow_top5_use_primary_boards_and_include_child():
    payload = _payload([
            _board("801062", "军工", 3636, 3_787_000_000, ["600001"]),
            _board("801235", "化工", 3397, -879_000_000, ["600002"]),
            _board("801807", "算力", 3238, 2_467_000_000, ["600003"]),
            _board("801730", "液冷", 2174, 2_315_000_000, ["600004"], "801807"),
            _board("801328", "消费电子", 1471, 32_860_000, ["600005"]),
            _board("801999", "其它", 1200, 10_000_000, ["600006"]),
        ])
    result = normalize_selected_board_snapshot(
        payload,
        as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
        expected_trade_date=DAY,
    )
    selected = {
        board["board_name"]
        for board in result["boards"]
        if board["selected_for_rotation"]
    }
    assert selected == {"军工", "算力", "液冷", "消费电子", "其它"}
    assert [row["board_name"] for row in result["selected_primary_boards"]] == ["军工", "算力", "消费电子", "其它"]
    assert result["by_symbol"]["600004.SH"][0]["primary_rank"] == 2


def test_positive_flow_top5_excludes_sixth_primary_but_keeps_positive_child_outside_slot():
    payload = _payload([
        _board("801001", "第一主板块", 600, 600, ["600001"]),
        _board("801002", "第二主板块", 500, 500, ["600002"]),
        _board("801003", "第三主板块", 400, 400, ["600003"]),
        _board("801004", "第四主板块", 300, 300, ["600004"]),
        _board("801005", "第五主板块", 200, 200, ["600005"]),
        _board("801006", "第六主板块", 100, 100, ["600006"]),
        _board("801730", "第五主板块子板", 700, 50, ["600007"], "801005"),
        _board("801235", "净流入为负", 900, -1, ["600008"]),
    ])

    result = normalize_selected_board_snapshot(
        payload,
        as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
        expected_trade_date=DAY,
    )

    assert [row["board_name"] for row in result["selected_primary_boards"]] == [
        "第一主板块",
        "第二主板块",
        "第三主板块",
        "第四主板块",
        "第五主板块",
    ]
    selected_names = {
        row["board_name"]
        for row in result["boards"]
        if row["selected_for_rotation"]
    }
    assert selected_names == {
        "第一主板块",
        "第二主板块",
        "第三主板块",
        "第四主板块",
        "第五主板块",
        "第五主板块子板",
    }
    assert result["selected_board_count"] == 6
    assert result["by_symbol"]["600007.SH"][0]["primary_rank"] == 5
    assert result["by_symbol"]["600006.SH"][0]["selected_for_rotation"] is False


def test_rotation_theme_count_is_configurable_without_child_consuming_a_primary_slot():
    payload = _payload([
        _board("801001", "第一主板块", 600, 600, ["600001"]),
        _board("801002", "第二主板块", 500, 500, ["600002"]),
        _board("801003", "第三主板块", 400, 400, ["600003"]),
        _board("801730", "第二主板块子板", 700, 50, ["600004"], "801002"),
    ])

    result = normalize_selected_board_snapshot(
        payload,
        as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
        expected_trade_date=DAY,
        rotation_theme_count=2,
    )

    assert result["rotation_theme_count"] == 2
    assert [row["board_name"] for row in result["selected_primary_boards"]] == [
        "第一主板块",
        "第二主板块",
    ]
    assert result["by_symbol"]["600004.SH"][0]["selected_for_rotation"] is True
    assert result["by_symbol"]["600003.SH"][0]["selected_for_rotation"] is False


def test_constituents_are_mandatory():
    with pytest.raises(ValueError, match="CONSTITUENTS_MISSING"):
        normalize_selected_board_snapshot(
            _payload([_board("801062", "军工", 1, 1, [])]),
            as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
            expected_trade_date=DAY,
        )


def test_non_selected_rank_rows_may_omit_members_and_provider_k_suffix_is_normalized():
    payload = _payload([
            _board("801062k", "军工", 300, 30, ["600001"]),
            _board("801807k", "算力", 200, 20, ["600002"]),
            _board("801328k", "消费电子", 100, 10, ["600003"]),
            _board("801999k", "未入选板块", 50, -1, []),
        ])
    result = normalize_selected_board_snapshot(
        payload,
        as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
        expected_trade_date=DAY,
    )

    assert result["ranking_board_count"] == 4
    assert [row["board_code"] for row in result["selected_primary_boards"]] == [
        "801062",
        "801807",
        "801328",
    ]
