from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot, UniverseSnapshot
from liangjian_funnel.settings import Settings


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 24, 15, 10, tzinfo=TZ)


def settings(tmp_path: Path) -> Settings:
    return Settings.from_env({"HITHINK_FINANCE_API_KEY": "unit-secret", "ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS": "0"}, root=tmp_path)


def test_hithink_catalog_paginates_and_keeps_request_shape(tmp_path: Path):
    calls: list[tuple[int, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = dict(request.url.params)
        calls.append((int(query["offset"]), int(query["limit"])))
        offset = int(query["offset"])
        rows = [
            {"thscode": "600519.SH", "name": "A"},
            {"thscode": "000001.SZ", "name": "B"},
        ][offset : offset + int(query["limit"])]
        return httpx.Response(200, json={"code": 0, "data": {"item": rows, "total": 2}}, request=request)

    client = HithinkClient(settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.ticker_catalog(limit=1)
    client.close()
    assert result.ok and result.complete
    assert [row.model_dump()["thscode"] for row in result.items] == ["600519.SH", "000001.SZ"]
    assert calls == [(0, 1), (1, 1)]


def test_hithink_business_error_and_empty_page_are_structured(tmp_path: Path):
    def business_error(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 1002, "data": None}, request=request)

    client = HithinkClient(settings(tmp_path), transport=httpx.MockTransport(business_error), sleep=lambda _: None)
    result = client.income_statements("600519.SH")
    client.close()
    assert not result.ok and result.reason_code == "BUSINESS_ERROR"
    assert "unit-secret" not in result.model_dump_json()

    empty = HithinkClient(
        settings(tmp_path),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"code": 0, "data": {"item": []}}, request=request)),
        sleep=lambda _: None,
    )
    result = empty.income_statements("600519.SH")
    empty.close()
    assert not result.ok and result.reason_code == "EMPTY_DATA"


def test_financial_indicators_flatten_nested_abilities(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["report"] == "2025-4"
        return httpx.Response(
            200,
            json={"code": 0, "data": {"thscode": "600519.SH", "report": "2025-4", "abilities": [{"ability": "growth", "indicators": [{"index_id": "roe", "value": "1.2"}]}]}},
            request=request,
        )

    client = HithinkClient(settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.financial_indicators("600519.SH", report="2025-4")
    client.close()
    assert result.ok and result.complete
    assert result.items[0].model_dump() == {"ability": "growth", "index_id": "roe", "value": "1.2"}


def _universe() -> UniverseSnapshot:
    catalog = [
        {"thscode": "600519.SH", "name": "A"},
        {"thscode": "000001.SZ", "name": "B"},
        {"thscode": "830001.BJ", "name": "C"},
    ]
    snapshot = [
        {"thscode": "600519.SH", "last_price": 10, "volume": 1, "turnover": 300},
        {"thscode": "000001.SZ", "last_price": 11, "volume": 1, "turnover": 200},
        {"thscode": "830001.BJ", "last_price": 12, "volume": 1, "turnover": 100},
    ]
    return UniverseSnapshot.from_records(catalog, snapshot, as_of=NOW)


def test_universe_lineage_preselect_and_bj_research_only():
    universe = _universe()
    assert universe.ready
    assert universe.lineage.total_record_count == 3
    assert [record.symbol for record in universe.research_candidates] == ["600519.SH", "000001.SZ", "830001.BJ"]
    assert [record.symbol for record in universe.trade_candidates] == ["600519.SH", "000001.SZ"]
    assert [record.symbol for record in universe.deterministic_preselect(1)] == ["600519.SH"]
    assert universe.lineage.excluded_by_reason["BJ_RESEARCH_ONLY"] == 1


def test_invalid_market_row_blocks_every_downstream_candidate():
    universe = UniverseSnapshot.from_records(
        [{"thscode": "600519.SH", "name": "A"}],
        [{"thscode": "600519.SH", "last_price": -1, "volume": 1, "turnover": 1}],
        as_of=NOW,
    )
    assert not universe.ready
    assert universe.trade_candidates == ()
    assert "INVALID_PRICE" in universe.records[0].exclusion_reasons


def test_universe_preserves_market_change_for_breadth() -> None:
    universe = UniverseSnapshot.from_records(
        [{"thscode": "600519.SH", "name": "A"}],
        [{
            "thscode": "600519.SH",
            "last_price": 11,
            "prev_price": 10,
            "volume": 1,
            "turnover": 1,
        }],
        as_of=NOW,
    )

    assert universe.records[0].change_ratio_pct == pytest.approx(10.0)


def test_frozen_snapshot_hash_replay_and_candidate_failure(tmp_path: Path):
    universe = _universe()
    frozen = FrozenInputSnapshot.freeze(
        universe,
        as_of=NOW,
        daily_payload={"600519.SH": [{"date": "2026-08-23", "close": 10}]},
        fundamental_payload={"600519.SH": [{"period": "2026Q2"}]},
        fact_payload={"manifest_hash": "a" * 64, "facts": {"LIMIT_UP_POOL": {"available": True}}},
        max_candidates=2,
    )
    assert frozen.verify_hash()
    path = frozen.write_json(tmp_path / "snapshot.json")
    restored = FrozenInputSnapshot.read_json(path)
    assert restored.snapshot_hash == frozen.snapshot_hash
    assert restored.fact_payload == frozen.fact_payload
    assert [failure.symbol for failure in restored.candidate_failures] == ["000001.SZ"]
