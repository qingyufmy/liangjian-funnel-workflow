import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from liangjian_funnel.pipeline.feature_rebuild import FeatureRebuildCoordinator
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


NOW = datetime(2026, 8, 29, 21, 0, tzinfo=timezone.utc)


def _builder(entity, generation_id, store):
    store.record_feature_generation_members(
        generation_id=generation_id,
        members=[
            {
                "entity_type": entity["entity_type"],
                "entity_id": entity["entity_id"],
                "partition_name": "weekly",
                "payload": {"version": entity.get("version", 1)},
            }
        ],
    )


def test_full_rebuild_stages_validates_and_replaces_active_generation(tmp_path: Path):
    path = tmp_path / "features.sqlite3"
    store = ResearchFeatureStore(path)
    baseline = store.create_feature_generation(
        generation_id="baseline",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="baseline",
        created_at=NOW,
    )
    store.record_feature_generation_members(
        generation_id=baseline,
        members=[{"entity_type": "STOCK", "entity_id": "OLD", "payload": {"old": True}}],
    )
    store.validate_feature_generation(baseline, validated_at=NOW)
    store.publish_feature_generation(baseline, activated_at=NOW)

    result = FeatureRebuildCoordinator(store, _builder).run_full(
        entities=[
            {"entity_type": "STOCK", "entity_id": "600000.SH", "version": 2},
            {"entity_type": "STOCK", "entity_id": "000001.SZ", "version": 2},
        ],
        as_of=NOW,
        worker_id="weekly",
    )
    assert result.status == "PUBLISHED"
    assert result.previous_generation_id == baseline
    assert result.processed_count == 2
    assert store.get_active_feature_generation()["generation_id"] == result.generation_id
    rows = store.feature_generation_members(result.generation_id, strict=True)
    assert {row["entity_id"] for row in rows} == {"600000.SH", "000001.SZ"}
    assert store.feature_generation_members(baseline, strict=True)[0]["entity_id"] == "OLD"


def test_full_rebuild_failure_never_contaminates_active_or_raw_facts(tmp_path: Path):
    path = tmp_path / "features.sqlite3"
    store = ResearchFeatureStore(path)
    baseline = store.create_feature_generation(
        generation_id="baseline",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="baseline",
        created_at=NOW,
    )
    store.record_feature_generation_members(
        generation_id=baseline,
        members=[{"entity_type": "STOCK", "entity_id": "OLD", "payload": {"old": True}}],
    )
    store.validate_feature_generation(baseline, validated_at=NOW)
    store.publish_feature_generation(baseline, activated_at=NOW)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE raw_fact_sentinel(value TEXT NOT NULL)"
        )
        connection.execute("INSERT INTO raw_fact_sentinel VALUES ('keep-me')")

    def fail_on_second(entity, generation_id, feature_store):
        _builder(entity, generation_id, feature_store)
        if entity["entity_id"] == "BROKEN":
            raise ValueError("invalid source revision")

    result = FeatureRebuildCoordinator(store, fail_on_second).run_full(
        entities=[
            {"entity_type": "STOCK", "entity_id": "GOOD"},
            {"entity_type": "STOCK", "entity_id": "BROKEN"},
        ],
        as_of=NOW,
        worker_id="weekly",
    )
    assert result.status == "FAILED"
    assert store.get_active_feature_generation()["generation_id"] == baseline
    assert store.feature_generation_members(baseline, strict=True)[0]["entity_id"] == "OLD"
    failed = store.get_feature_generation(result.generation_id)
    assert failed["status"] == "FAILED"
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT value FROM raw_fact_sentinel").fetchone()[0] == "keep-me"


def test_failed_weekly_generation_is_not_publishable(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")

    def invalid(_entity, _generation_id, _store):
        raise RuntimeError("source unavailable")

    result = FeatureRebuildCoordinator(store, invalid).run_full(
        entities=[("STOCK", "600000.SH")],
        as_of=NOW,
    )
    assert result.status == "FAILED"
    assert store.get_active_feature_generation() is None
    assert store.get_feature_generation(result.generation_id)["status"] == "FAILED"
