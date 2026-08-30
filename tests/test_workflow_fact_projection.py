from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot, UniverseSnapshot
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, _compact_fundamental_rows


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 25, 9, 26, tzinfo=TZ)


def test_compact_fundamentals_expose_deterministic_dataset_coverage() -> None:
    compact = _compact_fundamental_rows(
        [
            {"_dataset": "INCOME", "report_date_ms": 3},
            {"_dataset": "BALANCE", "report_date_ms": 2},
            {"_dataset": "CASH_FLOW", "report_date_ms": 1},
        ]
    )

    assert compact["dataset_coverage"] == {
        "core_reports_complete": True,
        "indicators_available": False,
        "missing_datasets": ["INDICATORS"],
    }
    assert compact["indicators"] == []


def _fact(count: int) -> dict:
    return {
        "available": True,
        "reason_code": "OK",
        "event_time": NOW.isoformat(),
        "fetch_time": NOW.isoformat(),
        "record_count": count,
        "records": [{"value": index} for index in range(count)],
    }


def test_research_input_projects_phase_one_facts_without_semantic_substitution(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_dir.joinpath("exchange_rules.yaml").write_text(
        "schema_version: liangjian-exchange-rules/1.0.0\n"
        "snapshot_id: CN-A-SIMULATION-20260706\neffective_from: '2026-07-06'\n"
        "simulation_only: true\nexternal_orders: false\nt_plus_one: true\nlot_size: 100\n"
        "sources: {sse: {}, szse: {}, bse: {}}\n",
        encoding="utf-8",
    )
    universe = UniverseSnapshot.from_records(
        [
            {"thscode": "600519.SH", "name": "A"},
            {"thscode": "000001.SZ", "name": "B"},
        ],
        [
            {"thscode": "600519.SH", "last_price": 11, "prev_price": 10, "volume": 1, "turnover": 2},
            {"thscode": "000001.SZ", "last_price": 9, "prev_price": 10, "volume": 1, "turnover": 1},
        ],
        as_of=NOW,
    )
    facts = {
        "LIMIT_UP_POOL": _fact(47),
        "LIMIT_DOWN_POOL": _fact(3),
        "LIMIT_BREAK_POOL": _fact(19),
        "LIMIT_UP_LADDER": _fact(30),
        "AUCTION_FINAL": _fact(2),
        "DRAGON_TIGER_LIST": _fact(64),
        "HOT_STOCK_LIST": _fact(30),
        "THS_INDUSTRY_CATALOG": {
            **_fact(3),
            "records": [
                {"thscode": "881101.TI", "name": "行业A"},
                {"thscode": "881102.TI", "name": "行业B"},
                {"thscode": "881103.TI", "name": "行业C"},
            ],
        },
        "THS_INDUSTRY_MEMBERSHIP": {
            **_fact(2),
            "records": [
                {
                    "thscode": "600519.SH",
                    "mapping_status": "MAPPED",
                    "memberships": [{"industry_thscode": "881101.TI", "industry_name": "行业A"}],
                },
                {
                    "thscode": "000001.SZ",
                    "mapping_status": "MAPPED",
                    "memberships": [{"industry_thscode": "881102.TI", "industry_name": "行业B"}],
                },
            ],
        },
        "THS_INDUSTRY_HISTORY": {
            **_fact(3),
            "records": [
                {
                    "industry_thscode": code,
                    "industry_name": name,
                    "bars": [
                        {"date_ms": day, "close_price": close, "turnover": 1000 + day}
                        for day, close in enumerate(closes, start=1)
                    ],
                }
                for code, name, closes in (
                    ("881101.TI", "行业A", [10, 11, 12, 13, 14, 15]),
                    ("881102.TI", "行业B", [10, 10, 10, 10, 10, 10]),
                    ("881103.TI", "行业C", [10, 9, 8, 7, 6, 5]),
                )
            ],
        },
    }
    frozen = FrozenInputSnapshot.freeze(
        universe,
        as_of=NOW,
        daily_payload={symbol: [{"close": 10}] for symbol in ("600519.SH", "000001.SZ")},
        fundamental_payload={symbol: [{"_dataset": "INCOME"}] for symbol in ("600519.SH", "000001.SZ")},
        technical_payload={
            symbol: {
                "ready": True,
                "kline_patterns": {"available": True, "labels": ["DOJI"]},
                "price_levels": {"available": True, "trigger_zone": {"low": 9.9, "high": 10.0}},
            }
            for symbol in ("600519.SH", "000001.SZ")
        },
        fact_payload={
            "snapshot_id": "facts-1",
            "manifest_hash": "a" * 64,
            "open_macro_bundle": {
                "schema_version": "open-macro-contract/1.0.0",
                "content_hash": "b" * 64,
                "cache_status": "LIVE",
                "MACRO_ECONOMIC_DATA": {
                    "contract": "MACRO_ECONOMIC_DATA",
                    "available": True,
                    "reason_code": "OK",
                    "values": {"PMI": 49.2, "m1_m2_gap": -2.1},
                },
                "ASSET_ROTATION_SNAPSHOT": {
                    "contract": "ASSET_ROTATION_SNAPSHOT",
                    "available": True,
                    "reason_code": "OK",
                    "assets": {"EQUITY": {"momentum_20d_percentile": 75}},
                },
            },
            "facts": facts,
            "fact_groups": {
                "DISCLOSURE_EVENT": [{
                    "fact_id": "fact-1",
                    "symbol": "600519.SH",
                    "publish_time": NOW.isoformat(),
                    "announcement_title": "定期报告",
                }],
                "MACRO_POLICY_EVENT": [{
                    "fact_id": "policy-1",
                    "symbol": None,
                    "publish_time": NOW.isoformat(),
                    "title": "正式政策",
                    "direct_stock_mapping_allowed": False,
                }],
                "MARKET_NEWS_FLASH": [{
                    "fact_id": "news-market-1",
                    "source_id": "open_news.cls_roll",
                    "source_url": "https://www.cls.cn/detail/1",
                    "publish_time": (NOW - timedelta(minutes=5)).isoformat(),
                    "channel": "cls_roll",
                    "title": "市场快讯",
                    "summary": "仅作线索",
                    "repost_count": 1,
                }],
                "STOCK_NEWS_ITEM": [{
                    "fact_id": "news-stock-1",
                    "source_id": "open_news.eastmoney_stock.600519_SH",
                    "source_url": "https://finance.eastmoney.com/a/1.html",
                    "symbol": "600519.SH",
                    "publish_time": (NOW - timedelta(minutes=10)).isoformat(),
                    "channel": "eastmoney_stock",
                    "title": "个股资讯",
                    "summary": "等待公告核实",
                    "repost_count": 1,
                }],
                "INDUSTRY_RSS_ITEM": [{
                    "fact_id": "news-rss-1",
                    "source_id": "open_news.rss.ai.example",
                    "source_url": "https://example.com/feed/item",
                    "publish_time": (NOW - timedelta(hours=1)).isoformat(),
                    "channel": "ai",
                    "title": "行业资讯",
                    "summary": "等待正式事实源核实",
                    "repost_count": 2,
                }],
            },
            "source_health": [
                {"source_id": "cninfo.public.600519_sh", "available": True},
                {"source_id": "cninfo.public.000001_sz", "available": True},
                {"source_id": "gov.policy_library", "available": True, "reason_code": "OK"},
                {"source_id": "open_news.cls_roll", "available": True, "reason_code": "OK"},
                {"source_id": "open_news.eastmoney_stock.600519_SH", "available": True, "reason_code": "OK"},
                {"source_id": "open_news.rss.ai.example", "available": True, "reason_code": "OK"},
            ],
        },
        max_candidates=2,
    )
    app = SimpleNamespace(settings=Settings.from_env({}, root=tmp_path))

    result = WorkflowApplication._research_input(
        app,
        frozen=frozen,
        universe=universe,
        technical=frozen.technical_payload,
        g0_symbols=["600519.SH", "000001.SZ"],
        source_failures={},
        raw_snapshot_path=tmp_path / "raw.json",
        as_of=NOW,
    )

    emotion = result["MARKET_EMOTION_SNAPSHOT"]
    assert emotion["advances"] == 1
    assert emotion["declines"] == 1
    assert emotion["limit_up_count"] == 47
    assert result["AUCTION_SNAPSHOT"]["available"] is True
    assert result["CROWDING_SNAPSHOT"]["available"] is False
    assert result["CROWDING_SNAPSHOT"]["reason_code"] == "PARTIAL_PROXY_ONLY"
    assert result["NEWS_HEAT_SNAPSHOT"]["available"] is True
    assert result["NEWS_HEAT_SNAPSHOT"]["deduped_item_count"] == 3
    assert result["NEWS_HEAT_SNAPSHOT"]["sentiment_available"] is False
    assert len(result["INDUSTRY_NEWS_FEED"]["items"]) == 1
    assert result["KLINE_PATTERNS"]["600519.SH"]["labels"] == ["DOJI"]
    assert result["PRICE_LEVELS"]["600519.SH"]["trigger_zone"]["low"] == 9.9
    assert "kline_patterns" not in result["FACTOR_SNAPSHOT"]["600519.SH"]
    assert result["DISCLOSURE_EVENTS"]["available"] is True
    assert result["DISCLOSURE_EVENTS"]["by_symbol"]["600519.SH"][0]["announcement_title"] == "定期报告"
    assert result["MACRO_POLICY_FEED"]["available"] is True
    assert result["MACRO_POLICY_FEED"]["document_count"] == 1
    assert result["MACRO_POLICY_FEED"]["direct_stock_mapping_allowed"] is False
    assert result["MACRO_ECONOMIC_DATA"]["values"]["PMI"] == 49.2
    assert result["ASSET_ROTATION_SNAPSHOT"]["assets"]["EQUITY"]["momentum_20d_percentile"] == 75
    assert result["snapshot_manifest"]["open_macro"]["content_hash"] == "b" * 64
    assert result["A2_SECTOR_HEALTH_SNAPSHOT"]["available"] is True
    assert result["A2_SECTOR_HEALTH_SNAPSHOT"]["data_sufficiency_state"] == "PARTIAL"
    assert result["SECTOR_CYCLE_SNAPSHOT"]["sector_health_snapshot"] == result["A2_SECTOR_HEALTH_SNAPSHOT"]
