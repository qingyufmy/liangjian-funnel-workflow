from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.market_aggregates import (
    build_a2_sector_health_snapshot,
    build_crowding_snapshot,
    build_market_emotion,
    build_news_heat_snapshot,
    build_sector_health_snapshot,
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
    assert first["emotion_cycle_stage"] == "ACCELERATION"
    assert first["new_long_permission"] == "ALLOW_CORE"
    assert first["emotion_cycle_evidence"]["scoring_used"] is False


def test_market_emotion_blocks_new_leader_plans_at_climax_and_retreat() -> None:
    climax_records = [{"change_ratio_pct": 1.0}] * 80 + [{"change_ratio_pct": -1.0}] * 20
    climax_facts = _facts()
    climax_facts["LIMIT_UP_POOL"] = _fact([{"thscode": f"600{i:03d}.SH"} for i in range(80)])
    climax_facts["LIMIT_BREAK_POOL"] = _fact([])
    climax = build_market_emotion(climax_records, climax_facts, as_of=NOW)

    assert climax["temperature"] == "OVERHEATED"
    assert climax["emotion_cycle_stage"] == "CLIMAX"
    assert climax["new_long_permission"] == "NO_NEW_ENTRY"

    retreat_records = [{"change_ratio_pct": 1.0}] * 30 + [{"change_ratio_pct": -1.0}] * 70
    retreat = build_market_emotion(retreat_records, _facts(), as_of=NOW)

    assert retreat["emotion_cycle_stage"] == "ICE_POINT"
    assert retreat["new_long_permission"] == "NO_NEW_ENTRY"


def test_market_emotion_exposes_only_the_six_confirmed_cycle_stages() -> None:
    scenarios = [
        ([{"change_ratio_pct": 1.0}] * 20 + [{"change_ratio_pct": -1.0}] * 80, "ICE_POINT"),
        ([{"change_ratio_pct": 1.0}] * 40 + [{"change_ratio_pct": -1.0}] * 60, "LATENT"),
        ([{"change_ratio_pct": 1.0}] * 50 + [{"change_ratio_pct": -1.0}] * 50, "STARTUP"),
        ([{"change_ratio_pct": 1.0}] * 60 + [{"change_ratio_pct": -1.0}] * 40, "ACCELERATION"),
        ([{"change_ratio_pct": 1.0}] * 80 + [{"change_ratio_pct": -1.0}] * 20, "CLIMAX"),
    ]
    for records, expected in scenarios:
        facts = _facts()
        if expected == "STARTUP":
            facts["LIMIT_UP_POOL"] = _fact([{"thscode": f"600{i:03d}.SH"} for i in range(20)])
            facts["LIMIT_BREAK_POOL"] = _fact([{"thscode": "300001.SZ"}])
            facts["LIMIT_UP_LADDER"] = _fact([
                {"date": "2026-08-25", "boards": {"first_board": [{"board_num": 1}]}}
            ])
        elif expected == "LATENT":
            facts["LIMIT_UP_POOL"] = _fact([{"thscode": f"600{i:03d}.SH"} for i in range(10)])
            facts["LIMIT_BREAK_POOL"] = _fact([{"thscode": f"300{i:03d}.SZ"} for i in range(2)])
            facts["LIMIT_UP_LADDER"] = _fact([])
        elif expected == "CLIMAX":
            facts["LIMIT_UP_POOL"] = _fact([{"thscode": f"600{i:03d}.SH"} for i in range(80)])
            facts["LIMIT_BREAK_POOL"] = _fact([])
        stage = build_market_emotion(records, facts, as_of=NOW)["emotion_cycle_stage"]
        assert stage == expected

    divergence_facts = _facts()
    divergence_facts["LIMIT_UP_POOL"] = _fact([{"thscode": f"600{i:03d}.SH"} for i in range(50)])
    divergence_facts["LIMIT_BREAK_POOL"] = _fact([{"thscode": f"300{i:03d}.SZ"} for i in range(30)])
    divergence = build_market_emotion(
        [{"change_ratio_pct": 1.0}] * 55 + [{"change_ratio_pct": -1.0}] * 45,
        divergence_facts,
        as_of=NOW,
    )
    assert divergence["emotion_cycle_stage"] == "DIVERGENCE"

    observed = {expected for _, expected in scenarios} | {divergence["emotion_cycle_stage"]}
    assert observed == {"LATENT", "STARTUP", "ACCELERATION", "CLIMAX", "DIVERGENCE", "ICE_POINT"}


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


def test_sector_health_aggregates_industry_and_concept_point_in_time_evidence() -> None:
    as_of = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)
    fact = lambda records: _fact(records, when=as_of)
    facts = _facts()
    facts["THS_INDUSTRY_CATALOG"] = fact([
        {"thscode": "881001.TI", "name": "化工"},
        {"thscode": "881002.TI", "name": "农业种植"},
    ])
    facts["THS_CONCEPT_CATALOG"] = fact([
        {"taxonomy_code": "885001.TI", "taxonomy_name": "粮食概念"},
        {"taxonomy_code": "885002.TI", "taxonomy_name": "化工新材料"},
    ])
    facts["THS_INDUSTRY_MEMBERSHIP"] = fact([
        {
            "thscode": "600001.SH",
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": "881001.TI", "industry_name": "化工"}],
        },
        {
            "thscode": "600002.SH",
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": "881002.TI", "industry_name": "农业种植"}],
        },
        {
            "thscode": "600003.SH",
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": "881002.TI", "industry_name": "农业种植"}],
        },
    ])
    facts["THS_CONCEPT_MEMBERSHIP"] = fact([
        {
            "symbol": "600001.SH",
            "memberships": [{"concept_thscode": "885002.TI", "concept_name": "化工新材料"}],
        },
        {
            "symbol": "600002.SH",
            "memberships": [{"concept_thscode": "885001.TI", "concept_name": "粮食概念"}],
        },
        {
            "symbol": "600003.SH",
            "memberships": [{"concept_thscode": "885001.TI", "concept_name": "粮食概念"}],
        },
    ])
    facts["THS_INDUSTRY_HISTORY"] = fact([
        {
            "industry_thscode": "881001.TI",
            "industry_name": "化工",
            "bars": [
                {"date_ms": int((as_of - timedelta(days=4 - index)).timestamp() * 1000), "close_price": close}
                for index, close in enumerate((10.0, 9.0, 9.0, 10.0, 11.0))
            ]
            + [{"date_ms": int((as_of + timedelta(days=1)).timestamp() * 1000), "close_price": 99.0}],
        },
        {
            "industry_thscode": "881002.TI",
            "industry_name": "农业种植",
            "bars": [
                {"date_ms": int((as_of - timedelta(days=4 - index)).timestamp() * 1000), "close_price": 10.0 + index}
                for index in range(5)
            ],
        },
    ])
    facts["LIMIT_UP_POOL"] = fact([
        {"thscode": "600001.SH"},
        {"thscode": "600002.SH"},
    ])
    facts["LIMIT_UP_LADDER"] = fact([
        {
            "date": as_of.date().isoformat(),
            "boards": {
                "three_board": [{"thscode": "600001.SH", "board_num": 3}],
                "two_board": [{"thscode": "600002.SH", "board_num": 2}],
            },
        },
        {
            "date": (as_of - timedelta(days=1)).date().isoformat(),
            "boards": {"one_board": [{"thscode": "600003.SH", "board_num": 1}]},
        },
    ])

    snapshot = build_a2_sector_health_snapshot(
        facts,
        [
            {"symbol": "600001.SH", "change_ratio_pct": 3.0, "amount": 100.0},
            {"symbol": "600002.SH", "change_ratio_pct": 2.0, "amount": 200.0},
            {"symbol": "600003.SH", "change_ratio_pct": -1.0, "amount": 150.0},
        ],
        symbols=["600001.SH", "600002.SH", "600003.SH"],
        as_of=as_of,
    )

    assert snapshot["available"] is True
    assert snapshot["data_sufficiency_state"] == "SUFFICIENT"
    assert snapshot["capital_flow_available"] is False
    assert snapshot["turnover_is_capital_flow"] is False
    assert snapshot["history"]["future_bars_dropped"] == 1
    assert snapshot["limit_up_ladder"]["latest_date"] == as_of.date().isoformat()

    industry = snapshot["by_taxonomy"]["industry"]["sectors"]
    agriculture = next(item for item in industry if item["taxonomy_code"] == "881002.TI")
    chemical = next(item for item in industry if item["taxonomy_code"] == "881001.TI")
    assert agriculture["health_state"] == "HEALTHY"
    assert agriculture["breadth"] == pytest.approx(0.5)
    assert agriculture["limit_up_count"] == 1
    assert agriculture["ladder_count"] == 1
    assert agriculture["max_board"] == 2
    assert chemical["return_flow_state"] == "WEAK_TO_STRONG"
    assert chemical["max_board"] == 3

    concept = snapshot["by_taxonomy"]["concept"]["sectors"]
    grain = next(item for item in concept if item["taxonomy_code"] == "885001.TI")
    assert grain["health_state"] == "HEALTHY"
    assert grain["return_flow_state"] == "UNKNOWN"
    assert grain["relative_strength_percentile"] is not None


def test_sector_health_never_infers_reflow_from_a_single_current_quote() -> None:
    facts = _facts()
    facts["THS_INDUSTRY_CATALOG"] = _fact([{"thscode": "881001.TI", "name": "化工"}])
    facts["THS_INDUSTRY_MEMBERSHIP"] = _fact([{
        "thscode": "600001.SH",
        "memberships": [{"industry_thscode": "881001.TI", "industry_name": "化工"}],
    }])
    result = build_sector_health_snapshot(
        facts,
        [{"symbol": "600001.SH", "change_ratio_pct": 4.0}],
        symbols=["600001.SH"],
        as_of=NOW,
    )
    sector = result["industry"]["sectors"][0]
    assert sector["return_flow_state"] == "UNKNOWN"
    assert sector["persistence"]["state"] == "UNKNOWN"


def test_sector_health_joins_real_board_flow_by_exact_cross_vendor_name() -> None:
    facts = _facts()
    facts["THS_INDUSTRY_CATALOG"] = _fact([
        {"thscode": "881001.TI", "name": "化工"},
        {"thscode": "881002.TI", "name": "农业种植"},
    ])
    facts["THS_INDUSTRY_MEMBERSHIP"] = _fact([
        {
            "thscode": "600001.SH",
            "memberships": [{"industry_thscode": "881001.TI", "industry_name": "化工"}],
        },
        {
            "thscode": "600002.SH",
            "memberships": [{"industry_thscode": "881002.TI", "industry_name": "农业种植"}],
        },
    ])
    board_flow = {
        "available": True,
        "reason_code": "OK",
        "by_taxonomy": {
            "industry": {
                "today": {
                    "available": True,
                    "content_hash": "today-hash",
                    "records": [
                        {"rank": 1, "code": "BK0477", "name": "化工", "main_net_cny": 8_000_000, "main_pct": 3.2},
                        {"rank": 2, "code": "BK0910", "name": "农业种植", "main_net_cny": 3_000_000, "main_pct": 1.4},
                    ],
                },
                "5d": {
                    "available": True,
                    "content_hash": "5d-hash",
                    "records": [
                        {"rank": 1, "code": "BK0910", "name": "农业种植", "main_net_cny": 20_000_000, "main_pct": 5.0},
                        {"rank": 2, "code": "BK0477", "name": "化工", "main_net_cny": 10_000_000, "main_pct": 2.0},
                    ],
                },
            },
            "concept": {},
        },
    }

    result = build_sector_health_snapshot(
        facts,
        [
            {"symbol": "600001.SH", "change_ratio_pct": 2.0, "amount": 100.0},
            {"symbol": "600002.SH", "change_ratio_pct": 1.0, "amount": 80.0},
        ],
        symbols=["600001.SH", "600002.SH"],
        as_of=NOW,
        board_capital_flow_snapshot=board_flow,
    )

    assert result["capital_flow_available"] is True
    assert result["capital_flow_mapped_sector_count"] == 2
    chemical = next(row for row in result["industry"]["sectors"] if row["taxonomy_name"] == "化工")
    agriculture = next(row for row in result["industry"]["sectors"] if row["taxonomy_name"] == "农业种植")
    assert chemical["capital_flow"]["source_scope"] == "SECTOR"
    assert chemical["capital_flow"]["windows"]["today"]["main_net_cny"] == 8_000_000
    assert agriculture["capital_flow"]["windows"]["5d"]["main_pct"] == 5.0
    assert chemical["capital_flow"]["score"] != agriculture["capital_flow"]["score"]
    assert result["turnover_is_capital_flow"] is False


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
