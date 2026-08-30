from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import liangjian_funnel.data.a2_market as a2_market
from liangjian_funnel.data.a2_market import (
    build_capital_flow_snapshot,
    collect_eastmoney_capital_flow,
    collect_tencent_capital_flow,
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


def test_historical_collection_recovers_only_with_exact_provider_trade_date(tmp_path) -> None:
    target = NOW - timedelta(days=1)

    def fetch(indicator: str):
        prefix = {"今日": "今日", "3日": "3日", "5日": "5日", "10日": "10日"}[indicator]
        return [
            {**row, "provider_timestamp": int(target.timestamp())}
            for row in _rows(prefix)
        ]

    result = collect_eastmoney_capital_flow(
        as_of=target,
        now=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
        cache_dir=tmp_path,
        fetch_rank=fetch,
        allow_historical_recovery=True,
    )

    assert result["available"] is True
    assert result["historical_recovery"] is True
    assert result["provider_trade_date_verified"] is True
    assert result["provider_trade_dates"]["today"] == [target.date().isoformat()]
    assert load_capital_flow_snapshot(tmp_path, target.date().isoformat()) == result


def test_historical_collection_rejects_mismatched_provider_trade_date(tmp_path) -> None:
    target = NOW - timedelta(days=2)

    def fetch(indicator: str):
        prefix = {"今日": "今日", "3日": "3日", "5日": "5日", "10日": "10日"}[indicator]
        return [
            {**row, "provider_timestamp": int(NOW.timestamp())}
            for row in _rows(prefix)
        ]

    result = collect_eastmoney_capital_flow(
        as_of=target,
        now=NOW,
        expected_symbols=["600001.SH", "000002.SZ"],
        cache_dir=tmp_path,
        fetch_rank=fetch,
        allow_historical_recovery=True,
    )

    assert result["available"] is False
    assert result["reason_code"] == "HISTORICAL_CAPITAL_FLOW_PROVIDER_DATE_MISMATCH"
    assert load_capital_flow_snapshot(tmp_path, target.date().isoformat()) is None


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


def test_tencent_collection_uses_order_size_fields_and_declares_window_capabilities(tmp_path) -> None:
    rows = {
        "600001.SH": {
            "symbol": "600001.SH",
            "net_inflow_amount": 100,
            "net_inflow_ratio": 10,
            "large_inflow_ratio": 4,
            "super_inflow_ratio": 2,
        },
        "000002.SZ": {
            "symbol": "000002.SZ",
            "net_inflow_amount": -50,
            "net_inflow_ratio": -5,
            "large_inflow_ratio": -2,
            "super_inflow_ratio": -1,
        },
    }

    result = collect_tencent_capital_flow(
        as_of=NOW,
        now=NOW,
        expected_symbols=list(rows),
        cache_dir=tmp_path,
        fetch_symbol=rows.__getitem__,
        fetch_trade_timestamp=lambda: "20260829151000",
        workers=2,
    )

    assert result["available"] is True
    assert result["availability_state"] == "OBSERVED_VALUE"
    assert result["source_id"] == "TENCENT_QQ_FINANCE_FUND_FLOW"
    assert result["provider_trade_date_verified"] is True
    assert result["provider_capabilities"] == {
        "today": True,
        "3d": False,
        "5d": False,
        "10d": False,
    }
    assert result["coverage_by_window"]["today"]["coverage_ratio"] == 1
    assert result["by_symbol"]["600001.SH"]["capital_flow_score"] == 100
    assert result["by_symbol"]["000002.SZ"]["capital_flow_score"] == 0


def test_tencent_historical_recovery_requires_exact_quote_date(tmp_path) -> None:
    target = NOW - timedelta(days=1)
    called = False

    def fetch_symbol(_symbol: str):
        nonlocal called
        called = True
        return {}

    result = collect_tencent_capital_flow(
        as_of=target,
        now=NOW,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_symbol=fetch_symbol,
        fetch_trade_timestamp=lambda: "20260829151000",
        allow_historical_recovery=True,
    )

    assert called is False
    assert result["available"] is False
    assert result["source_id"] == "TENCENT_QQ_FINANCE_FUND_FLOW"
    assert result["reason_code"] == "HISTORICAL_CAPITAL_FLOW_PROVIDER_DATE_MISMATCH"


def test_tencent_current_collection_rejects_stale_provider_date(tmp_path) -> None:
    result = collect_tencent_capital_flow(
        as_of=NOW,
        now=NOW,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_symbol=lambda _symbol: {},
        fetch_trade_timestamp=lambda: "20260828151000",
    )

    assert result["available"] is False
    assert result["reason_code"] == "CAPITAL_FLOW_PROVIDER_DATE_MISMATCH"


def test_tencent_payload_parser_uses_vendor_order_buckets_not_turnover() -> None:
    parsed = a2_market._parse_tencent_flow_payload(
        {
            "data": {
                "todayFundFlow": {
                    "stockCode": "sh600001",
                    "mainNetIn": "100",
                    "mainIn": "300",
                    "mainOut": "200",
                    "retailIn": "250",
                    "retailOut": "250",
                    "superFlow": "40",
                    "bigFlow": "60",
                }
            }
        },
        expected_symbol="600001.SH",
    )

    assert parsed["net_inflow_amount"] == 100
    assert parsed["net_inflow_ratio"] == 10
    assert parsed["large_inflow_ratio"] == 6
    assert parsed["super_inflow_ratio"] == 4
