from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.institutional_coverage import load_broker_gold_coverage


AS_OF = datetime(2026, 9, 1, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_current_month_broker_gold_is_projected_as_t2_coverage(tmp_path) -> None:
    payload = [
        {
            "month": "2026-09",
            "broker": "券商甲",
            "symbol": "600000.SH",
            "name": "浦发银行",
            "publish_time": "2026-08-31T18:00:00+08:00",
            "source_ref": "https://example.test/a",
        },
        {
            "month": "2026-09",
            "broker": "券商乙",
            "symbol": "600000.SH",
            "name": "浦发银行",
            "publish_time": "2026-09-01T07:00:00+08:00",
            "source_ref": "https://example.test/b",
        },
        {
            "month": "2026-09",
            "broker": "未来券商",
            "symbol": "000001.SZ",
            "name": "平安银行",
            "publish_time": "2026-09-01T09:00:00+08:00",
            "source_ref": "https://example.test/future",
        },
    ]
    (tmp_path / "2026-09.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = load_broker_gold_coverage(tmp_path, as_of=AS_OF)

    assert result["available"] is True
    assert result["record_count"] == 2
    assert result["broker_count"] == 2
    assert result["source_count"] == 2
    assert result["brokers"] == ["券商乙", "券商甲"]
    assert result["coverage_scope"] == "VERIFIED_INPUT_ROWS"
    assert result["symbol_count"] == 1
    assert result["excluded_future_count"] == 1
    assert result["symbols"]["600000.SH"]["broker_count"] == 2
    assert result["symbols"]["600000.SH"]["direct_approval_forbidden"] is True
    assert result["benchmark_evaluation_remains_independent"] is True


def test_missing_month_is_observable_but_does_not_block_a1(tmp_path) -> None:
    result = load_broker_gold_coverage(tmp_path, as_of=AS_OF)

    assert result["available"] is False
    assert result["reason_code"] == "BROKER_GOLD_COVERAGE_NOT_CONFIGURED"
    assert result["broker_count"] == 0
    assert result["source_count"] == 0
    assert result["symbols"] == {}
