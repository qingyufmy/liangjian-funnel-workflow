from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.market_aggregates import (
    build_crowding_snapshot,
    build_market_emotion,
    build_news_heat_snapshot,
    build_sector_cycle_and_permissions,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 25, 15, 10, tzinfo=TZ)


def _fact(records: list[dict], *, when: datetime = NOW) -> dict:
    return {
        "available": True,
        "reason_code": "OK",
        "event_time": when.isoformat(),
        "fetch_time": when.isoformat(),
        "record_count": len(records),
        "records": records,
    }


def _facts() -> dict:
    return {
        "LIMIT_UP_POOL": _fact([{"thscode": f"600{i:03d}.SH"} for i in range(50)]),
        "LIMIT_DOWN_POOL": _fact([{"thscode": "000001.SZ"}]),
        "LIMIT_BREAK_POOL": _fact([{"thscode": f"300{i:03d}.SZ"} for i in range(10)]),
        "LIMIT_UP_LADDER": _fact([
            {"date": "2026-08-25", "boards": {"five_board": [{"board_num": 5, "seal_nextday": None}]}},
            {"date": "2026-08-24", "boards": {"two_board": [
                {"board_num": 2, "seal_nextday": True},
                {"board_num": 2, "seal_nextday": False},
            ]}},
        ]),
        "DRAGON_TIGER_LIST": _fact([{"thscode": "600519.SH", "net_value": 10}]),
        "HOT_STOCK_LIST": _fact([{"thscode": "600519.SH", "rank": 1}]),
        "THS_INDUSTRY_CATALOG": _fact([{"thscode": "881101.TI", "name": "行业"}]),
        "THS_INDUSTRY_MEMBERSHIP": _fact([{
            "thscode": "600519.SH",
            "taxonomy": "THS",
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": "881101.TI", "industry_name": "行业"}],
        }]),
    }


def test_market_emotion_is_complete_and_deterministic() -> None:
    records = [{"change_ratio_pct": 1.0}] * 60 + [{"change_ratio_pct": -1.0}] * 40

    first = build_market_emotion(records, _facts(), as_of=NOW)
    second = build_market_emotion(records, _facts(), as_of=NOW)

    assert first == second
    assert first["available"] is True
    assert first["breadth"] == pytest.approx(0.6)
    assert first["limit_up_count"] == 50
    assert first["break_rate"] == pytest.approx(1 / 6)
    assert first["ladder_height"] == 5
    assert first["previous_day_promotion_rate"] == pytest.approx(0.5)
    assert first["temperature"] == "STRONG"


def test_market_emotion_rejects_future_and_low_breadth_coverage() -> None:
    facts = _facts()
    facts["LIMIT_UP_POOL"] = _fact([], when=NOW + timedelta(minutes=1))
    assert build_market_emotion([{"change_ratio_pct": 1}], facts, as_of=NOW)["reason_code"] == "FUTURE_FACT_DETECTED"

    records = [{"change_ratio_pct": 1.0}] * 8 + [{"change_ratio_pct": None}] * 2
    result = build_market_emotion(records, _facts(), as_of=NOW)
    assert result["available"] is False
    assert result["reason_code"] == "BREADTH_COVERAGE_INSUFFICIENT"


def test_partial_crowding_does_not_claim_full_availability() -> None:
    result = build_crowding_snapshot(_facts(), ["600519.SH", "000001.SZ"], as_of=NOW)

    assert result["available"] is False
    assert result["reason_code"] == "PARTIAL_PROXY_ONLY"
    assert result["dragon_tiger_component"]["records"][0]["thscode"] == "600519.SH"
    assert "FUND_HOLDINGS" in result["missing_components"]


def test_ths_membership_is_primary_but_does_not_invent_sector_cycle() -> None:
    cycle, permissions = build_sector_cycle_and_permissions(_facts(), ["600519.SH"], as_of=NOW)

    assert cycle["available"] is False
    assert cycle["source"] == "THS_PRIMARY_TAXONOMY"
    assert cycle["membership_coverage"] == 1.0
    assert cycle["missing_components"] == ["INDEX_HISTORY", "SECTOR_CAPITAL_FLOW"]
    assert permissions["available"] is True
    assert permissions["by_symbol"] == {"600519.SH": "PROBE_ONLY"}


def test_sector_history_proves_persistent_mainline_without_calling_turnover_capital_flow() -> None:
    facts = _facts()
    facts["THS_INDUSTRY_CATALOG"] = _fact([
        {"thscode": "881101.TI", "name": "行业A"},
        {"thscode": "881102.TI", "name": "行业B"},
        {"thscode": "881103.TI", "name": "行业C"},
    ])
    histories = []
    for code, name, closes in (
        ("881101.TI", "行业A", [10, 11, 12, 13, 14, 15]),
        ("881102.TI", "行业B", [10, 10, 10, 10, 10, 10]),
        ("881103.TI", "行业C", [10, 9, 8, 7, 6, 5]),
    ):
        histories.append({
            "industry_thscode": code,
            "industry_name": name,
            "bars": [
                {"date_ms": day, "close_price": close, "turnover": 1000 + day}
                for day, close in enumerate(closes, start=1)
            ],
        })
    facts["THS_INDUSTRY_HISTORY"] = _fact(histories)

    cycle, permissions = build_sector_cycle_and_permissions(facts, ["600519.SH"], as_of=NOW)

    assert cycle["available"] is True
    assert cycle["capital_flow_available"] is False
    assert cycle["turnover_is_capital_flow"] is False
    assert cycle["history_metrics"]["top3_daily_overlap"] == 1.0
    assert cycle["history_metrics"]["persistent_mainline_candidates"][0]["industry_thscode"] == "881101.TI"
    assert permissions["by_symbol"] == {"600519.SH": "STANDARD"}


def test_monthly_rotation_keeps_persistent_sector_and_rejects_one_day_pulse() -> None:
    facts = _facts()
    histories = []
    # Twelve synthetic broad sectors leave two sectors outside the daily
    # Top-10.  One sector compounds steadily; the pulse sector only jumps on
    # the final day and therefore cannot pass the monthly appearance gate.
    for index in range(10):
        code = f"88110{index + 1}.TI"
        histories.append({
            "industry_thscode": code,
            "industry_name": f"基准行业{index}",
            "bars": [
                {
                    "date_ms": int((NOW - timedelta(days=21 - day)).timestamp() * 1000),
                    "close_price": 100.0 * (1.001 ** day),
                    "turnover": 1_000.0 + day,
                }
                for day in range(22)
            ],
        })
    histories.append({
        "industry_thscode": "881199.TI",
        "industry_name": "持续轮动行业",
        "bars": [
            {
                "date_ms": int((NOW - timedelta(days=21 - day)).timestamp() * 1000),
                "close_price": 100.0 * (1.003 ** day),
                "turnover": 2_000.0 + day * 10,
            }
            for day in range(22)
        ],
    })
    histories.append({
        "industry_thscode": "881198.TI",
        "industry_name": "单日脉冲行业",
        "bars": [
            {
                "date_ms": int((NOW - timedelta(days=21 - day)).timestamp() * 1000),
                "close_price": 100.0 if day < 21 else 120.0,
                "turnover": 900.0 if day < 21 else 9_000.0,
            }
            for day in range(22)
        ],
    })
    facts["THS_INDUSTRY_CATALOG"] = _fact([
        {"thscode": row["industry_thscode"], "name": row["industry_name"]}
        for row in histories
    ])
    facts["THS_INDUSTRY_HISTORY"] = _fact(histories)

    cycle, _permissions = build_sector_cycle_and_permissions(
        facts,
        ["600519.SH"],
        as_of=NOW,
    )
    metrics = cycle["history_metrics"]
    candidates = metrics["monthly_rotation_candidates"]
    persistent = next(item for item in candidates if item["industry_thscode"] == "881199.TI")

    assert metrics["monthly_lookback_trading_days"] == 20
    assert persistent["top10_appearance_count"] >= 2
    assert persistent["return_5d"] is not None
    assert persistent["return_10d"] is not None
    assert persistent["return_20d"] is not None
    assert persistent["relative_strength_percentile_20d"] > 50
    assert persistent["recent_turnover"] is not None
    assert persistent["turnover_persistence_ratio"] is not None
    assert not any(item["industry_thscode"] == "881198.TI" for item in candidates)


def test_monthly_rotation_drops_future_bars_and_degrades_missing_fields() -> None:
    facts = _facts()
    histories = []
    for index in range(3):
        code = f"88120{index + 1}.TI"
        bars = [
            {
                "date_ms": int((NOW - timedelta(days=21 - day)).timestamp() * 1000),
                "close_price": 100.0 + day + index,
                "turnover": 1_000.0 + day,
            }
            for day in range(22)
        ]
        histories.append({
            "industry_thscode": code,
            "industry_name": f"缺数据行业{index}",
            "bars": bars,
        })
    # Missing close removes one observation and therefore makes the 20d
    # return unavailable for this sector.  Missing turnover only affects
    # turnover-derived fields and must not remove the price observation.
    histories[0]["bars"][5].pop("close_price")
    for bar in histories[1]["bars"][-5:]:
        bar.pop("turnover")
    histories[0]["bars"].append({
        "date_ms": int((NOW + timedelta(days=1)).timestamp() * 1000),
        "close_price": 999.0,
        "turnover": 999.0,
    })
    facts["THS_INDUSTRY_CATALOG"] = _fact([
        {"thscode": row["industry_thscode"], "name": row["industry_name"]}
        for row in histories
    ])
    facts["THS_INDUSTRY_HISTORY"] = _fact(histories)

    cycle, _permissions = build_sector_cycle_and_permissions(
        facts,
        ["600519.SH"],
        as_of=NOW,
    )
    metrics = cycle["history_metrics"]

    assert cycle["available"] is True
    assert metrics["future_bars_dropped"] == 1
    first = next(item for item in metrics["monthly_rotation_candidates"] if item["industry_thscode"] == "881201.TI")
    second = next(item for item in metrics["monthly_rotation_candidates"] if item["industry_thscode"] == "881202.TI")
    assert first["return_20d"] is None
    assert second["return_20d"] is not None
    assert second["recent_turnover"] is None
    assert second["turnover_persistence_ratio"] is None


def test_news_heat_uses_only_frozen_deduped_t3_items() -> None:
    payload = {
        "fact_groups": {
            "MARKET_NEWS_FLASH": [{
                "fact_id": "news-1",
                "source_id": "open_news.cls",
                "source_url": "https://example.test/1",
                "publish_time": (NOW - timedelta(hours=1)).isoformat(),
                "channel": "CLS_FLASH",
                "title": "市场快讯",
                "summary": "摘要",
                "repost_count": 2,
            }, {
                "fact_id": "future",
                "source_id": "open_news.cls",
                "source_url": "https://example.test/future",
                "publish_time": (NOW + timedelta(minutes=1)).isoformat(),
                "channel": "CLS_FLASH",
                "title": "未来数据",
            }],
            "STOCK_NEWS_ITEM": [{
                "fact_id": "news-2",
                "source_id": "open_news.eastmoney_stock.600519_sh",
                "source_url": "https://example.test/2",
                "publish_time": (NOW - timedelta(hours=2)).isoformat(),
                "channel": "EASTMONEY_STOCK",
                "symbol": "600519.SH",
                "title": "个股资讯",
            }, {
                "fact_id": "outside",
                "source_id": "open_news.eastmoney_stock.000001_sz",
                "source_url": "https://example.test/3",
                "publish_time": (NOW - timedelta(hours=2)).isoformat(),
                "channel": "EASTMONEY_STOCK",
                "symbol": "000001.SZ",
                "title": "池外资讯",
            }],
        },
        "source_health": [
            {"source_id": "open_news.cls", "available": True},
            {"source_id": "open_news.eastmoney_stock.600519_sh", "available": True},
        ],
    }

    result = build_news_heat_snapshot(payload, ["600519.SH"], as_of=NOW)

    assert result["available"] is True
    assert result["deduped_item_count"] == 2
    assert result["repost_count_total"] == 2
    assert result["by_symbol"] == {"600519.SH": 1}
    assert result["future_items_dropped"] == 1
    assert result["sentiment_available"] is False
