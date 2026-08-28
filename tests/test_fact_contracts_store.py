from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from liangjian_funnel.facts import (
    FactEnvelope,
    FactSnapshotManifest,
    FactStore,
    RealtimeFactEnvelope,
    SourceHealth,
    merge_fact_manifests,
)
from liangjian_funnel.facts import contracts as fact_contracts
from liangjian_funnel.facts.contracts import canonical_json_chunks, canonical_json_hash


SHANGHAI = ZoneInfo("Asia/Shanghai")
T0 = datetime(2026, 8, 25, 8, 0, tzinfo=SHANGHAI)
CONTENT_HASH = hashlib.sha256(b"source-content").hexdigest()


def make_fact(fact_id: str = "fact-001", *, symbol: str | None = "600519.SH") -> FactEnvelope:
    return FactEnvelope(
        fact_id=fact_id,
        source_id="cninfo_public",
        source_tier="T1",
        fact_type="DISCLOSURE_EVENT",
        symbol=symbol,
        event_time=T0,
        publish_time=T0 + timedelta(minutes=1),
        fetch_time=T0 + timedelta(minutes=2),
        ingest_time=T0 + timedelta(minutes=3),
        reason_code="OK",
        source_url="https://example.test/disclosure/1",
        content_hash=CONTENT_HASH,
        payload={"amount": None, "title": "contract"},
    )


def test_fact_normalizes_utc_and_keeps_missing_numeric_as_none() -> None:
    fact = FactEnvelope(
        fact_id="fact-utc",
        source_id="hithink",
        fact_type="QUOTE",
        event_time=datetime(2026, 8, 25, 0, 0, tzinfo=timezone.utc),
        publish_time=datetime(2026, 8, 25, 0, 1, tzinfo=timezone.utc),
        fetch_time=datetime(2026, 8, 25, 0, 2, tzinfo=timezone.utc),
        ingest_time=datetime(2026, 8, 25, 0, 3, tzinfo=timezone.utc),
        source_url="https://example.test/quote",
        content_hash=CONTENT_HASH,
        payload={"volume": None},
    )
    assert fact.event_time.tzinfo == SHANGHAI
    assert fact.event_time.hour == 8
    assert fact.payload["volume"] is None


def test_missing_publish_time_requires_explicit_realtime_contract() -> None:
    values = make_fact().model_dump()
    values["publish_time"] = None
    with pytest.raises(ValidationError, match="publish_time"):
        FactEnvelope.model_validate(values)

    realtime = RealtimeFactEnvelope.model_validate(values | {"realtime": True})
    assert realtime.publish_time is None
    assert realtime.realtime is True


def test_naive_time_is_rejected() -> None:
    values = make_fact().model_dump()
    values["event_time"] = datetime(2026, 8, 25, 8, 0)
    with pytest.raises(ValidationError, match="timezone-aware"):
        FactEnvelope.model_validate(values)


def test_secret_like_fields_are_rejected_and_never_dumped() -> None:
    values = make_fact().model_dump()
    values["payload"] = {"api_key": "sk-example-123456789"}
    with pytest.raises(ValidationError, match="secret-like"):
        FactEnvelope.model_validate(values)

    values = make_fact().model_dump()
    values["source_url"] = "https://example.test/data?api_key=sk-example-123456789"
    with pytest.raises(ValidationError, match="secret-like"):
        FactEnvelope.model_validate(values)


def test_manifest_hash_is_stable_independent_of_fact_order() -> None:
    first = make_fact("fact-a")
    second = make_fact("fact-b")
    left = FactSnapshotManifest(snapshot_id="snapshot-1", as_of=T0, facts=(first, second))
    right = FactSnapshotManifest(snapshot_id="snapshot-1", as_of=T0, facts=(second, first))
    assert left.facts_sha256 == right.facts_sha256
    assert left.facts == right.facts


def test_streaming_canonical_json_matches_legacy_bytes_for_nested_values() -> None:
    fact = make_fact().model_copy(
        update={
            "payload": {
                "unicode": "中文\nquote\"",
                "nested": {"tuple": (None, 1, -0.0)},
                "plain_set": {"b", "a"},
                "timestamp": T0,
            }
        }
    )
    values = [
        {
            "mapping": {2: "numeric key", "nested": [{"value": None}]},
            "tuple": (1, 2.5),
            "set": {"b", "a"},
            "date": T0.date(),
            "fact": fact,
        },
        fact,
        FactSnapshotManifest(snapshot_id="streaming", as_of=T0, facts=(fact,)),
    ]

    for value in values:
        expected = fact_contracts.canonical_json_bytes(value)
        actual = b"".join(canonical_json_chunks(value))
        assert actual == expected
        assert canonical_json_hash(value) == hashlib.sha256(expected).hexdigest()


def test_fact_hash_uses_incremental_canonical_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = (make_fact("fact-a"), make_fact("fact-b"))
    expected = hashlib.sha256(fact_contracts.canonical_json_bytes(list(facts))).hexdigest()

    def fail_canonical_bytes(_: object) -> bytes:
        raise AssertionError("canonical_json_bytes must not be used for fact hashing")

    monkeypatch.setattr(fact_contracts, "canonical_json_bytes", fail_canonical_bytes)
    assert fact_contracts._facts_hash(facts) == expected


def test_manifest_rejects_tampered_fact_hash() -> None:
    fact = make_fact()
    with pytest.raises(ValidationError, match="facts_sha256"):
        FactSnapshotManifest(
            snapshot_id="snapshot-1",
            as_of=T0,
            facts=(fact,),
            facts_sha256="0" * 64,
        )


def test_store_round_trip_is_atomic_and_hash_checked(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    fact = make_fact()
    path = store.write_fact(fact, "normalized/fact.json")
    assert path.exists()
    assert not list(path.parent.glob("*.tmp"))
    assert store.read_fact(path) == fact
    assert store.content_hash(path) == hashlib.sha256(path.read_bytes()).hexdigest()


def test_store_detects_tampering_on_readback(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    path = store.write_fact(make_fact(), "normalized/fact.json")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["payload"]["title"] = "tampered"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="content hash mismatch"):
        store.read_fact(path)


def test_store_requires_checksum_sidecar(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    path = store.write_fact(make_fact())
    Path(f"{path}.sha256").unlink()

    with pytest.raises(ValueError, match="sidecar is missing"):
        store.read_fact(path)


def test_store_uses_windows_safe_default_filename(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    fact = make_fact(f"sha256:{'a' * 64}")

    path = store.write_fact(fact)

    assert ":" not in path.name
    assert store.read_fact(path) == fact


def test_store_rejects_path_traversal(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    with pytest.raises(ValueError, match="escapes root"):
        store.write_json("../escape.json", {"ok": True})
    with pytest.raises(ValueError, match="escapes root"):
        store.read_json(tmp_path / "outside.json")


def test_source_health_normalizes_time_and_has_no_numeric_zero_defaults() -> None:
    health = SourceHealth(
        source_id="cninfo_public",
        checked_at=datetime(2026, 8, 25, 0, 0, tzinfo=timezone.utc),
    )
    assert health.checked_at.tzinfo == SHANGHAI
    assert health.coverage is None
    assert health.latency_ms is None
    assert health.http_status is None


def test_future_effective_event_is_valid() -> None:
    values = make_fact().model_dump()
    values["event_time"] = datetime(2026, 9, 1, 0, 0, tzinfo=SHANGHAI)
    fact = FactEnvelope.model_validate(values)

    assert fact.event_time > fact.publish_time


def test_manifest_store_round_trip_revalidates_canonical_hash(tmp_path: Path) -> None:
    store = FactStore(tmp_path / "facts")
    manifest = FactSnapshotManifest(snapshot_id="snapshot-1", as_of=T0, facts=(make_fact(),))
    path = store.write_manifest(manifest)
    loaded = store.read_manifest(path)
    assert loaded.facts_sha256 == manifest.facts_sha256
    assert loaded.manifest_hash == manifest.manifest_hash


def test_large_manifest_stream_path_avoids_full_dump_and_json_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = tuple(make_fact(f"fact-{index:04d}") for index in range(128))
    manifest = FactSnapshotManifest(snapshot_id="large-streaming", as_of=T0, facts=facts)
    expected = fact_contracts.canonical_json_bytes(manifest)
    expected_hash = hashlib.sha256(expected).hexdigest()

    def fail_json_dump(*_: object, **__: object) -> str:
        raise AssertionError("streaming path must not call json.dumps")

    def fail_model_dump(*_: object, **__: object) -> object:
        raise AssertionError("streaming path must not call model_dump")

    monkeypatch.setattr(fact_contracts.json, "dumps", fail_json_dump)
    monkeypatch.setattr(fact_contracts.FactSnapshotManifest, "model_dump", fail_model_dump)
    monkeypatch.setattr(fact_contracts.FactEnvelope, "model_dump", fail_model_dump)

    assert manifest.manifest_hash == expected_hash
    store = FactStore(tmp_path / "facts")
    path = store.write_manifest(manifest)
    assert path.read_bytes() == expected
    assert path.with_name(f"{path.name}.sha256").read_text(encoding="ascii").strip() == expected_hash


def test_manifest_merge_is_stable_and_uses_latest_cutoff() -> None:
    first = FactSnapshotManifest(
        snapshot_id="source-a",
        as_of=T0,
        facts=(make_fact("fact-a"),),
        source_checksums={"SOURCE_A": "a" * 64},
        coverage_by_fact_type={"DISCLOSURE_EVENT": 1.0},
    )
    second_fact = make_fact("fact-b").model_copy(
        update={
            "source_id": "source_b",
            "event_time": T0 + timedelta(minutes=1),
            "publish_time": T0 + timedelta(minutes=2),
            "fetch_time": T0 + timedelta(minutes=3),
            "ingest_time": T0 + timedelta(minutes=4),
        }
    )
    second = FactSnapshotManifest(
        snapshot_id="source-b",
        as_of=T0 + timedelta(minutes=5),
        facts=(second_fact,),
        source_checksums={"SOURCE_B": "b" * 64},
        coverage_by_fact_type={"DISCLOSURE_EVENT": 0.5},
    )

    merged = merge_fact_manifests((first, second))
    replay = merge_fact_manifests((first, second))

    assert merged.manifest_hash == replay.manifest_hash
    assert merged.as_of == second.as_of
    assert merged.coverage_by_fact_type["DISCLOSURE_EVENT"] == 0.5
