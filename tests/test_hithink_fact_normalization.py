from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.facts import collect_market_results, manifest_projection, normalize_hithink_results
from liangjian_funnel.facts import FactSnapshotManifest
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.workflow import (
    _advance_live_market_cutoff,
    _bind_reference_fact_event_time,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 25, 9, 26, tzinfo=TZ)


def _result(*, ok: bool = True, reason: str = "OK") -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint="/api/a-share/special-data/limit-up-pool",
        ok=ok,
        complete=ok,
        reason_code=reason,
        items=(HithinkRow.model_validate({"thscode": "600519.SH", "last_price": 100}),) if ok else (),
        pages=1 if ok else 0,
        fetch_time=NOW,
        http_status=200 if ok else 429,
        metadata={"timestamp": int(NOW.timestamp() * 1000)},
    )


def test_hithink_result_is_normalized_and_hash_bound() -> None:
    manifest = normalize_hithink_results(
        {"LIMIT_UP_POOL": _result()},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(seconds=1),
    )
    fact = manifest.facts[0]

    assert fact.available is True
    assert fact.source_tier == "T2"
    assert fact.source_url == "https://fuyao.aicubes.cn/api/a-share/special-data/limit-up-pool"
    assert fact.payload["record_count"] == 1
    assert manifest.source_checksums["LIMIT_UP_POOL"] == fact.content_hash
    assert manifest_projection(manifest)["manifest_hash"] == manifest.manifest_hash


def test_failed_endpoint_stays_unavailable_instead_of_empty_success() -> None:
    manifest = normalize_hithink_results(
        {"LIMIT_UP_POOL": _result(ok=False, reason="HTTP_429")},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW,
    )
    fact = manifest.facts[0]

    assert fact.available is False
    assert fact.reason_code == "HTTP_429"
    assert fact.content_hash is None
    assert manifest.coverage_by_fact_type["LIMIT_UP_POOL"] == 0.0
    assert manifest.source_health[0].http_status == 429


def test_manifest_is_deterministic_for_same_inputs() -> None:
    kwargs = {
        "base_url": "https://fuyao.aicubes.cn",
        "as_of": NOW,
        "ingest_time": NOW + timedelta(seconds=1),
    }
    first = normalize_hithink_results({"LIMIT_UP_POOL": _result()}, **kwargs)
    second = normalize_hithink_results({"LIMIT_UP_POOL": _result()}, **kwargs)

    assert first.snapshot_id == second.snapshot_id
    assert first.manifest_hash == second.manifest_hash


def test_realtime_manifest_cutoff_includes_latest_event_time() -> None:
    result = _result().model_copy(
        update={"metadata": {"timestamp": int((NOW + timedelta(seconds=5)).timestamp() * 1000)}}
    )

    manifest = normalize_hithink_results(
        {"LIMIT_UP_POOL": result},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(seconds=6),
    )

    assert manifest.as_of == NOW + timedelta(seconds=5)
    assert manifest_projection(manifest)["as_of"] == manifest.as_of.isoformat()


def test_historical_fact_keeps_cutoff_event_time_when_fetch_finishes_later() -> None:
    result = _result().model_copy(
        update={
            "endpoint": "/api/a-share-index/prices/historical",
            "fetch_time": NOW + timedelta(minutes=10),
            "metadata": {"timestamp": NOW.isoformat(), "cache_hit": True},
        }
    )

    manifest = normalize_hithink_results(
        {"THS_INDUSTRY_HISTORY": result},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(minutes=11),
    )

    assert manifest.facts[0].event_time == NOW
    assert manifest.facts[0].fetch_time == NOW + timedelta(minutes=10)
    assert manifest.as_of == NOW


def test_reference_fact_keeps_cutoff_event_time_when_request_finishes_later() -> None:
    result = _bind_reference_fact_event_time(
        _result().model_copy(update={"fetch_time": NOW + timedelta(seconds=5)}),
        as_of=NOW,
    )

    manifest = normalize_hithink_results(
        {"THS_INDUSTRY_CATALOG": result},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(seconds=6),
    )

    fact = manifest.facts[0]
    assert fact.event_time == NOW
    assert fact.fetch_time == NOW + timedelta(seconds=5)
    assert manifest.as_of == NOW


def test_live_market_cutoff_advances_to_latest_included_fact() -> None:
    assert _advance_live_market_cutoff(
        market_as_of=NOW,
        research_as_of=NOW,
        included_fact_as_of=NOW + timedelta(seconds=5),
    ) == NOW + timedelta(seconds=5)


def test_historical_market_cutoff_does_not_advance_to_research_day() -> None:
    prior_session = NOW - timedelta(days=1)
    assert _advance_live_market_cutoff(
        market_as_of=prior_session,
        research_as_of=NOW,
        included_fact_as_of=NOW + timedelta(seconds=5),
    ) == prior_session


def test_manifest_projection_preserves_duplicate_fact_types() -> None:
    first = normalize_hithink_results(
        {"DUPLICATE_TYPE": _result()},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(seconds=1),
    ).facts[0]
    second = first.model_copy(update={"fact_id": f"sha256:{'b' * 64}"})
    manifest = FactSnapshotManifest(snapshot_id="duplicates", as_of=NOW, facts=(first, second))

    projection = manifest_projection(manifest)

    assert projection["facts"]["DUPLICATE_TYPE"]["record_count"] == 2
    assert len(projection["fact_groups"]["DUPLICATE_TYPE"]) == 2


def test_manifest_projection_does_not_duplicate_large_fact_groups() -> None:
    first = normalize_hithink_results(
        {"LARGE_TYPE": _result()},
        base_url="https://fuyao.aicubes.cn",
        as_of=NOW,
        ingest_time=NOW + timedelta(seconds=1),
    ).facts[0]
    facts = tuple(
        first.model_copy(update={"fact_id": f"sha256:{index:064x}"})
        for index in range(257)
    )
    manifest = FactSnapshotManifest(snapshot_id="large-group", as_of=NOW, facts=facts)

    projection = manifest_projection(manifest)

    assert projection["facts"]["LARGE_TYPE"] == {
        "available": True,
        "reason_code": "OK",
        "record_count": 257,
    }
    assert len(projection["fact_groups"]["LARGE_TYPE"]) == 257


def test_market_fact_auction_requests_are_batched_and_merged_without_loss() -> None:
    symbols = tuple(f"{600000 + index:06d}.SH" for index in range(205))

    class Client:
        def __init__(self) -> None:
            self.auction_batches: list[tuple[str, ...]] = []

        def ths_index_catalog(self, *, tag: str) -> HithinkFetchResult:
            return _result()

        def limit_up_pool(self) -> HithinkFetchResult:
            return _result()

        def limit_down_pool(self) -> HithinkFetchResult:
            return _result()

        def limit_break_pool(self) -> HithinkFetchResult:
            return _result()

        def limit_up_ladder(self) -> HithinkFetchResult:
            return _result()

        def dragon_tiger_list(self) -> HithinkFetchResult:
            return _result()

        def hot_stock_list(self, *, period: str) -> HithinkFetchResult:
            return _result()

        def auction_snapshot(self, batch: tuple[str, ...], *, stage: str) -> HithinkFetchResult:
            self.auction_batches.append(batch)
            return HithinkFetchResult(
                endpoint="/api/a-share/auction/snapshot",
                ok=True,
                complete=True,
                reason_code="OK",
                items=tuple(HithinkRow.model_validate({"thscode": symbol, "auction_price": 10}) for symbol in batch),
                pages=1,
                total=len(batch),
                fetch_time=NOW,
                http_status=200,
            )

    client = Client()
    results = collect_market_results(client, symbols)
    auction = results["AUCTION_FINAL"]

    assert [len(batch) for batch in client.auction_batches] == [100, 100, 5]
    assert auction.ok and auction.complete
    assert {row.model_dump()["thscode"] for row in auction.items} == set(symbols)
    assert auction.metadata["batch_count"] == 3
    assert auction.metadata["missing_symbol_count"] == 0


def test_market_facts_bind_pools_and_dragon_tiger_to_closed_trade_date() -> None:
    requested: dict[str, object] = {}

    class Client:
        def ths_index_catalog(self, *, tag: str) -> HithinkFetchResult:
            return _result()

        def limit_up_pool(self, **kwargs: object) -> HithinkFetchResult:
            requested["limit_up"] = kwargs
            return _result()

        def limit_down_pool(self, **kwargs: object) -> HithinkFetchResult:
            requested["limit_down"] = kwargs
            return _result()

        def limit_break_pool(self, **kwargs: object) -> HithinkFetchResult:
            requested["limit_break"] = kwargs
            return _result()

        def limit_up_ladder(self) -> HithinkFetchResult:
            return _result()

        def dragon_tiger_list(self, **kwargs: object) -> HithinkFetchResult:
            requested["dragon_tiger"] = kwargs
            return _result()

        def hot_stock_list(self, *, period: str) -> HithinkFetchResult:
            return _result()

    collect_market_results(Client(), (), market_trade_date=datetime(2026, 8, 31).date())

    expected_ms = int(datetime(2026, 8, 31, tzinfo=TZ).timestamp() * 1000)
    assert requested["limit_up"] == {"date_ms": expected_ms}
    assert requested["limit_down"] == {"date_ms": expected_ms}
    assert requested["limit_break"] == {"date_ms": expected_ms}
    assert requested["dragon_tiger"] == {"date": "2026-08-31"}
