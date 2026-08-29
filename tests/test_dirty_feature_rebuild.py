from datetime import datetime, timedelta, timezone
from pathlib import Path

from liangjian_funnel.pipeline.feature_rebuild import FeatureRebuildCoordinator
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


NOW = datetime(2026, 8, 29, 9, 30, tzinfo=timezone.utc)


def _mark(store: ResearchFeatureStore, entity_id: str, *, reason: str = "FACT_CHANGED", priority: int = 0):
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=entity_id,
        reason_code=reason,
        source_version="v1",
        created_at=NOW,
        priority=priority,
        max_attempts=2,
    )


def test_dirty_queue_lifecycle_lease_retry_backoff_and_dead(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _mark(store, "600000.SH", priority=5)

    claimed = store.claim_dirty(worker_id="worker-a", now=NOW, lease_seconds=60)
    assert len(claimed) == 1
    assert claimed[0]["status"] == "LEASED"
    assert claimed[0]["attempts"] == 1
    assert claimed[0]["lease_owner"] == "worker-a"

    retried = store.retry_dirty(
        entity_type="STOCK",
        entity_id="600000.SH",
        reason_code="FACT_CHANGED",
        source_version="v1",
        error_code="SOURCE_503",
        worker_id="worker-a",
        now=NOW,
        base_delay_seconds=10,
    )
    assert retried is not None
    assert retried["status"] == "RETRY"
    assert retried["attempts"] == 1
    assert retried["next_retry_at"] == (NOW + timedelta(seconds=10)).isoformat()
    assert retried["last_error_code"] == "SOURCE_503"

    # The item is not claimable until the persisted backoff time.
    assert store.claim_dirty(worker_id="worker-b", now=NOW, lease_seconds=60) == []
    second = store.claim_dirty(
        worker_id="worker-b",
        now=NOW + timedelta(seconds=10),
        lease_seconds=60,
    )
    assert second[0]["attempts"] == 2
    dead = store.retry_dirty(
        entity_type="STOCK",
        entity_id="600000.SH",
        reason_code="FACT_CHANGED",
        source_version="v1",
        error_code="PERMANENT_BAD_PAYLOAD",
        worker_id="worker-b",
        now=NOW + timedelta(seconds=10),
    )
    assert dead is not None
    assert dead["status"] == "DEAD"
    assert store.dirty_stats()["DEAD"] == 1

    # Explicitly marking the same source version reopens a dead item without
    # deleting its row or creating a duplicate primary key.
    _mark(store, "600000.SH")
    current = store.list_dirty(include_resolved=True)
    assert len(current) == 1
    assert current[0]["status"] == "PENDING"
    assert current[0]["attempts"] == 0


def test_expired_lease_is_recoverable_and_exact_completion_is_owner_bound(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _mark(store, "000001.SZ")
    claimed = store.claim_dirty(worker_id="crashed", now=NOW, lease_seconds=5)
    assert claimed[0]["status"] == "LEASED"

    recovered = store.claim_dirty(
        worker_id="replacement",
        now=NOW + timedelta(seconds=6),
        lease_seconds=60,
    )
    assert recovered[0]["lease_owner"] == "replacement"
    assert recovered[0]["attempts"] == 2
    assert not store.complete_dirty(
        entity_type="STOCK",
        entity_id="000001.SZ",
        reason_code="FACT_CHANGED",
        source_version="v1",
        resolved_at=NOW,
        worker_id="crashed",
    )
    assert store.complete_dirty(
        entity_type="STOCK",
        entity_id="000001.SZ",
        reason_code="FACT_CHANGED",
        source_version="v1",
        resolved_at=NOW + timedelta(seconds=6),
        worker_id="replacement",
    )
    assert store.dirty_stats() == {
        "PENDING": 0,
        "LEASED": 0,
        "RETRY": 0,
        "DEAD": 0,
        "RESOLVED": 1,
    }


def test_dependency_expansion_is_cycle_safe_and_persistent(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _mark(store, "THEME-1")
    store.register_dirty_dependency(
        entity_type="STOCK", entity_id="THEME-1",
        dependency_type="CHAIN_NODE", dependency_id="NODE-1",
        created_at=NOW,
    )
    store.register_dirty_dependency(
        entity_type="CHAIN_NODE", entity_id="NODE-1",
        dependency_type="STOCK", dependency_id="THEME-1",
        created_at=NOW,
    )
    root = store.claim_dirty(worker_id="worker", now=NOW)
    expanded = store.expand_dirty_dependencies(root)
    assert [(item["entity_type"], item["entity_id"]) for item in expanded] == [
        ("STOCK", "THEME-1"),
        ("CHAIN_NODE", "NODE-1"),
    ]
    assert expanded[1]["dependency"] is True


def test_incremental_rebuild_clones_active_and_publishes_atomically(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    initial = store.create_feature_generation(
        generation_id="baseline",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="baseline",
        created_at=NOW,
    )
    store.record_feature_generation_members(
        generation_id=initial,
        members=[
            {"entity_type": "STOCK", "entity_id": "600000.SH", "payload": {"score": 1}},
            {"entity_type": "STOCK", "entity_id": "000001.SZ", "payload": {"score": 2}},
        ],
    )
    store.validate_feature_generation(initial, validated_at=NOW)
    store.publish_feature_generation(initial, activated_at=NOW)
    _mark(store, "600000.SH")

    def build(entity, generation_id, feature_store):
        feature_store.record_feature_generation_members(
            generation_id=generation_id,
            members=[
                {
                    "entity_type": entity["entity_type"],
                    "entity_id": entity["entity_id"],
                    "payload": {"score": 99},
                }
            ],
        )

    result = FeatureRebuildCoordinator(store, build).run_incremental(
        as_of=NOW,
        worker_id="maintenance",
        now=NOW,
        lease_seconds=60,
    )
    assert result.status == "PUBLISHED"
    assert result.processed_count == 1
    assert result.resolved_count == 1
    active = store.get_active_feature_generation()
    assert active["generation_id"] == result.generation_id
    rows = store.feature_generation_members(result.generation_id, strict=True)
    assert {row["entity_id"] for row in rows} == {"600000.SH", "000001.SZ"}
    assert next(row for row in rows if row["entity_id"] == "600000.SH")["payload"]["score"] == 99
    assert next(row for row in rows if row["entity_id"] == "000001.SZ")["payload"]["score"] == 2


def test_incremental_failure_marks_generation_failed_and_keeps_active(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    initial = store.create_feature_generation(
        generation_id="baseline",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="baseline",
        created_at=NOW,
    )
    store.record_feature_generation_members(
        generation_id=initial,
        members=[{"entity_type": "STOCK", "entity_id": "600000.SH", "payload": {"score": 1}}],
    )
    store.validate_feature_generation(initial, validated_at=NOW)
    store.publish_feature_generation(initial, activated_at=NOW)
    _mark(store, "600000.SH")

    def fail(_entity, _generation_id, _store):
        raise RuntimeError("builder failed")

    result = FeatureRebuildCoordinator(store, fail).run_incremental(
        as_of=NOW,
        worker_id="maintenance",
        now=NOW,
        lease_seconds=60,
    )
    assert result.status == "FAILED"
    assert result.retry_count == 1
    assert store.get_active_feature_generation()["generation_id"] == initial
    assert store.get_feature_generation(result.generation_id)["status"] == "FAILED"
    assert store.list_dirty(statuses=["RETRY"])[0]["last_error_code"] == "RUNTIMEERROR"
