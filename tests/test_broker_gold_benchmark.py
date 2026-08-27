from __future__ import annotations

import io
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.evaluation.broker_gold import (
    BrokerGoldContractError,
    evaluate_broker_gold,
    import_broker_gold,
    load_broker_gold_csv,
    load_broker_gold_json,
)


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 20, 15, 10, tzinfo=TZ)


def _row(
    symbol: str,
    broker: str = "甲券商",
    *,
    month: str = "2026-08",
    name: str | None = None,
    publish_time: str | None = "2026-08-01T09:00:00+08:00",
    source_ref: str | None = None,
) -> dict[str, str]:
    result: dict[str, str] = {
        "month": month,
        "broker": broker,
        "symbol": symbol,
        "source_ref": source_ref or f"https://example.test/{broker}/{symbol}",
    }
    if name is not None:
        result["name"] = name
    if publish_time is not None:
        result["publish_time"] = publish_time
    return result


def test_multi_broker_consensus_deduplicates_and_splits_active_monitor() -> None:
    rows = [
        _row("600001.SH", name="甲公司"),
        _row("600001.SH", broker="乙券商", name="甲公司"),
        _row("600002.SZ", name="乙公司"),
        # Same broker/month/symbol: must count once, even with a second ref.
        _row("600002.SZ", source_ref="https://example.test/revised/600002"),
        _row("600003.SZ", broker="乙券商", name="丙公司"),
    ]
    dataset = load_broker_gold_json(rows, as_of=AS_OF)
    report = evaluate_broker_gold(
        dataset,
        {
            "active_research_pool": [
                {"symbol": "600001.SH", "theme_id": "theme-policy", "node_id": "node-device", "rank": 1},
            ],
            "monitor_pool": [
                {"symbol": "600002.SZ", "theme_id": "theme-policy", "node_id": "node-material", "rank": 2},
            ],
        },
    )

    assert report["benchmark_not_runtime_input"] is True
    assert report["dataset"]["eligible_record_count"] == 4
    assert report["dataset"]["duplicate_count"] == 1
    assert report["counts"]["gold_symbols"] == 3
    assert report["counts"]["a1_active_gold_symbols"] == 1
    assert report["counts"]["a1_monitor_gold_symbols"] == 1
    assert report["symbol_coverage"]["covered_count"] == 2
    assert report["active_coverage"]["coverage"] == pytest.approx(1 / 3)
    assert report["broker_consensus"]["600001.SH"]["broker_count"] == 2
    assert report["by_broker"]["甲券商"]["symbol_coverage"]["coverage"] == 1.0
    assert report["by_broker"]["乙券商"]["active_coverage"]["coverage"] == 0.5


def test_future_publish_time_and_future_month_are_excluded_before_deduplication() -> None:
    rows = [
        _row("600001.SH", publish_time="2026-08-21T09:00:00+08:00"),
        _row("600002.SZ", month="2026-09", publish_time="2026-08-01T09:00:00+08:00"),
        _row("600003.SZ", publish_time="2026-08-20T15:10:00+08:00"),
        _row("600004.SZ", publish_time="2026-08-20T15:10:01+08:00"),
        _row("600005.SZ", publish_time=None),
    ]
    dataset = load_broker_gold_json(rows, as_of=AS_OF)

    assert [record.symbol for record in dataset.records] == ["600003.SZ", "600005.SZ"]
    assert {record.symbol for record in dataset.excluded_future} == {
        "600001.SH",
        "600002.SZ",
        "600004.SZ",
    }
    # The publication timestamp exactly at as_of is available at the cutoff.
    assert {record.symbol for record in dataset.records + dataset.excluded_future} >= {"600003.SZ"}

    report = evaluate_broker_gold(dataset, [], as_of=AS_OF)
    assert report["dataset"]["excluded_future_count"] == 3
    assert report["counts"]["gold_symbols"] == 2
    assert {item["symbol"] for item in report["missing_symbols"]} == {"600003.SZ", "600005.SZ"}


def test_theme_and_node_explainability_and_rank_percentile_are_reported() -> None:
    dataset = load_broker_gold_json(
        [_row(f"60000{index}.SH", name=f"公司{index}") for index in range(1, 4)],
        as_of=AS_OF,
    )
    report = evaluate_broker_gold(
        dataset,
        [
            {"symbol": "600001.SH", "status": "ACTIVE", "theme_id": "theme-a", "node_id": "node-a", "score": 95},
            {"symbol": "600002.SH", "status": "MONITOR", "theme_id": "theme-a", "score": 85},
            {"symbol": "600003.SH", "status": "LOCAL_MONITOR", "node_id": "node-b", "score": 75},
        ],
    )

    assert report["explainability"]["theme"]["covered_count"] == 2
    assert report["explainability"]["node"]["covered_count"] == 2
    assert report["rank_percentile"]["by_symbol"]["600001.SH"] == 100.0
    assert report["rank_percentile"]["by_symbol"]["600003.SH"] == 0.0
    assert report["rank_percentile"]["gold_symbols_ranked"] == 3


def test_missing_symbols_include_outside_status_and_reasons() -> None:
    dataset = load_broker_gold_json([_row("600001.SH", name="甲公司")], as_of=AS_OF)
    report = evaluate_broker_gold(
        dataset,
        [{"symbol": "600001.SH", "status": "REJECTED", "reason_codes": ["A1_RISK_EVENT_PRESENT"]}],
    )

    assert report["symbol_coverage"]["coverage"] == 0.0
    assert report["missing_symbols"] == [{
        "symbol": "600001.SH",
        "name": "甲公司",
        "brokers": ["甲券商"],
        "reasons": ["PRESENT_OUTSIDE_ACTIVE_MONITOR", "A1_RISK_EVENT_PRESENT"],
    }]
    assert report["missing_active_symbols"][0]["reasons"] == [
        "NOT_IN_A1_ACTIVE_POOL",
        "A1_RISK_EVENT_PRESENT",
    ]


def test_empty_benchmark_is_valid_and_strict_csv_json_contract_is_enforced() -> None:
    report = evaluate_broker_gold([], [])
    assert report["status"] == "EMPTY_BENCHMARK"
    assert report["symbol_coverage"]["coverage"] == 0.0
    assert report["missing_symbols"] == []
    assert report["benchmark_not_runtime_input"] is True

    csv_text = "month,broker,symbol,name,publish_time,source_ref\n2026-08,甲券商,600001.SH,甲公司,2026-08-01,ref-1\n"
    csv_dataset = load_broker_gold_csv(io.StringIO(csv_text), as_of=AS_OF)
    assert csv_dataset.records[0].symbol == "600001.SH"
    json_dataset = load_broker_gold_json(
        io.StringIO(json.dumps([_row("600002.SZ")], ensure_ascii=False)),
        as_of=AS_OF,
    )
    assert json_dataset.records[0].symbol == "600002.SZ"

    with pytest.raises(BrokerGoldContractError) as error:
        load_broker_gold_json([_row("600003.SZ") | {"unexpected": "reject"}])
    assert error.value.reason_code == "BROKER_GOLD_UNKNOWN_FIELDS"


def test_import_requires_explicit_format_for_file_like_sources() -> None:
    with pytest.raises(BrokerGoldContractError) as error:
        import_broker_gold(io.StringIO("[]"))
    assert error.value.reason_code == "BROKER_GOLD_FORMAT_REQUIRED"
