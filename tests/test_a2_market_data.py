from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.a2_market import (
    build_capital_flow_snapshot,
    collect_eastmoney_capital_flow,
    load_capital_flow_snapshot,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _rows(prefix: str) -> list[dict]:
    return [
        {
            "代码": "600001",
            f"{prefix}主力净流入-净额": 100,
            f"{prefix}主力净流入-净占比": 10,
            f"{prefix}大单净流入-净占比": 5,
            f"{prefix}超大单净流入-净占比": 3,
        },
        {
            "代码": "000002",
            f"{prefix}主力净流入-净额": -50,
            f"{prefix}主力净流入-净占比": -5,
            f"{prefix}大单净流入-净占比": -2,
            f"{prefix}超大单净流入-净占比": -1,
        },
    ]


def test_build_capital_flow_snapshot_is_vendor_derived_and_never_uses_turnover() -> None:
    snapshot = build_capital_flow_snapshot(
        {"today": _rows("今日"), "3d": _rows("3日"), "5d": _rows("5日"), "10d": _rows("10日")},
        as_of=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
    )

    assert snapshot["available"] is True
    assert snapshot["provider_method"] == "VENDOR_DERIVED"
    assert snapshot["turnover_is_capital_flow"] is False
    assert snapshot["by_symbol"]["600001.SH"]["capital_flow_score"] == 100
    assert snapshot["by_symbol"]["000002.SZ"]["capital_flow_score"] == 0
    assert snapshot["coverage_by_window"]["today"]["coverage_ratio"] == 1


def test_missing_expected_symbol_is_not_a_zero_flow_observation() -> None:
    snapshot = build_capital_flow_snapshot(
        {"today": _rows("今日")[:1]},
        as_of=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
        minimum_coverage=0.90,
    )

    assert snapshot["available"] is False
    missing = snapshot["by_symbol"]["000002.SZ"]
    assert missing["available"] is False
    assert missing["capital_flow_score"] is None
    assert missing["availability_state"] == "SOURCE_FAILED"
    assert missing["reason_code"] == "SYMBOL_MISSING_FROM_PROVIDER_CROSS_SECTION"


def test_malformed_symbol_is_rejected_instead_of_becoming_zero_code() -> None:
    rows = _rows("今日")
    rows.append({"代码": "INVALID", "今日主力净流入-净占比": 99})
    snapshot = build_capital_flow_snapshot(
        {"today": rows},
        as_of=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
    )

    assert "000000.SZ" not in snapshot["by_symbol"]
    assert snapshot["coverage_by_window"]["today"]["invalid_row_count"] == 1


def test_historical_collection_never_relabels_current_provider_data(tmp_path) -> None:
    called = False

    def fetch(_indicator: str):
        nonlocal called
        called = True
        return _rows("今日")

    result = collect_eastmoney_capital_flow(
        as_of=NOW - timedelta(days=1),
        now=NOW,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_rank=fetch,
    )

    assert called is False
    assert result["available"] is False
    assert result["reason_code"] == "HISTORICAL_CAPITAL_FLOW_CACHE_MISSING"


def test_current_collection_persists_hash_bound_point_in_time_cache(tmp_path) -> None:
    prefixes = {"今日": "今日", "3日": "3日", "5日": "5日", "10日": "10日"}

    def fetch(indicator: str):
        return _rows(prefixes[indicator])

    first = collect_eastmoney_capital_flow(
        as_of=NOW,
        now=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
        cache_dir=tmp_path,
        fetch_rank=fetch,
    )
    second = load_capital_flow_snapshot(tmp_path, "2026-08-29")

    assert second == first
    assert second is not None and second["content_hash"] == first["content_hash"]
