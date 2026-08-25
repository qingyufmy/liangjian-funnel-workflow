from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.facts import manifest_projection, normalize_hithink_results
from liangjian_funnel.facts import FactSnapshotManifest
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow


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
