import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.feature_rebuild import (
    FeatureRebuildCoordinator,
    copy_live_source_entities,
    copy_live_source_generation,
)
from liangjian_funnel.pipeline.feature_store import (
    FeatureGenerationError,
    ResearchFeatureStore,
)


NOW = datetime(2026, 8, 31, 3, 30, tzinfo=timezone.utc)


def _live_source(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    snapshot_hash: str = "snapshot-a",
    source_hash: str = "source-a",
    as_of: datetime = NOW,
    symbols: tuple[str, ...] = ("600000.SH", "000001.SZ"),
) -> dict:
    source = store.create_or_get_live_source(
        generation_id=generation_id,
        snapshot_hash=snapshot_hash,
        source_manifest_hash=source_hash,
        as_of=as_of,
        contract_version="test",
        algorithm_version="test",
        metadata={"market_trade_date": "2026-08-28"},
    )
    store.record_feature_generation_members_batched(
        generation_id=generation_id,
        members=(
            {
                "entity_type": "STOCK",
                "entity_id": symbol,
                "payload": {"symbol": symbol, "snapshot": snapshot_hash},
            }
            for symbol in symbols
        ),
        batch_size=1,
    )
    store.record_fundamental_features(
        as_of=as_of,
        generation_id=generation_id,
        decisions=[
            {
                "symbol": symbol,
                "financial_features": {"roe": 10},
                "data_quality_score": 1,
                "source_hashes": {"source": source_hash},
            }
            for symbol in symbols
        ],
    )
    store.validate_feature_generation(
        generation_id,
        validated_at=as_of,
        validation={"status": "READY", "failures": []},
    )
    return store.seal_generation(
        generation_id,
        purpose="LIVE_SOURCE",
        activation_eligible=False,
        sealed_at=as_of,
    )


def _target(store: ResearchFeatureStore, generation_id: str = "target") -> str:
    return store.create_feature_generation(
        generation_id=generation_id,
        as_of=NOW + timedelta(minutes=1),
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="target-source",
        created_at=NOW + timedelta(minutes=1),
        purpose="LIVE_FULL",
        activation_eligible=True,
    )


def test_live_source_identity_is_idempotent_and_never_activatable(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    first = store.create_or_get_live_source(
        snapshot_hash="snap-1",
        source_hash="manifest-1",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
    )
    repeat = store.create_or_get_live_source(
        snapshot_hash="snap-1",
        source_manifest_hash="manifest-1",
        as_of=NOW,
        contract_version="different",
        algorithm_version="different",
    )
    assert repeat["generation_id"] == first["generation_id"]
    assert repeat["purpose"] == "LIVE_SOURCE"
    assert repeat["activation_eligible"] is False

    with pytest.raises(FeatureGenerationError, match="LIVE_SOURCE_AMBIGUOUS"):
        store.create_or_get_live_source(
            snapshot_hash="snap-1",
            source_hash="manifest-2",
            as_of=NOW,
            contract_version="test",
            algorithm_version="test",
        )

    _live_source(store, "source-sealed", snapshot_hash="snap-2", source_hash="manifest-2")
    with pytest.raises(FeatureGenerationError, match="PURPOSE_NOT_ACTIVATABLE"):
        store.activate_generation("source-sealed", None, "must-not-activate")


def test_select_latest_live_source_skips_unsealed_and_blocks_hash_ties(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    store.create_or_get_live_source(
        generation_id="staging-source",
        snapshot_hash="staging-snap",
        source_hash="staging-hash",
        as_of=NOW + timedelta(hours=2),
        contract_version="test",
        algorithm_version="test",
    )
    _live_source(store, "source-old", as_of=NOW - timedelta(hours=1), snapshot_hash="old")
    _live_source(store, "source-new", as_of=NOW, snapshot_hash="new")
    selected = store.select_latest_live_source(market_trade_date="2026-08-28")
    assert selected is not None
    assert selected["generation_id"] == "source-new"
    assert selected["snapshot_hash"] == "new"

    _live_source(
        store,
        "source-tie",
        as_of=NOW,
        snapshot_hash="tie",
        source_hash="different-hash",
    )
    with pytest.raises(FeatureGenerationError, match="LIVE_SOURCE_AMBIGUOUS_MAX_AS_OF"):
        store.select_latest_live_source()


def test_unready_live_source_is_not_selectable_or_copyable(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    source = store.create_or_get_live_source(
        generation_id="unready-source",
        snapshot_hash="unready-snap",
        source_hash="unready-hash",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
    )
    store.validate_feature_generation(
        source["generation_id"],
        validated_at=NOW,
        validation={"status": "INCOMPLETE", "failures": ["FUNDAMENTAL"]},
    )
    store.seal_generation(
        source["generation_id"],
        purpose="LIVE_SOURCE",
        activation_eligible=False,
        sealed_at=NOW,
    )
    assert store.select_latest_live_source() is None
    target = _target(store)
    with pytest.raises(FeatureGenerationError, match="LIVE_SOURCE_NOT_READY"):
        copy_live_source_generation(store, source["generation_id"], target)


def test_member_batches_stream_and_validate_each_write_boundary(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    source = store.create_or_get_live_source(
        generation_id="source",
        snapshot_hash="snap",
        source_hash="source",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
    )
    members = (
        {
            "entity_type": "STOCK",
            "entity_id": f"{index:06d}.SH",
            "payload": {"index": index},
        }
        for index in range(7)
    )
    assert store.record_feature_generation_members_batched(
        generation_id=source["generation_id"], members=members, batch_size=2
    ) == 7
    assert len(store.feature_generation_members(source["generation_id"])) == 7
    with pytest.raises(ValueError, match="positive integer"):
        store.record_feature_generation_members_batched(
            generation_id=source["generation_id"], members=(), batch_size=0
        )


def test_sql_copy_replaces_selected_entities_without_payload_round_trip(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _live_source(store, "source")
    target = _target(store)
    # This stale row must disappear when source has no corresponding row.
    store.record_feature_generation_members(
        generation_id=target,
        members=[
            {"entity_type": "STOCK", "entity_id": "600000.SH", "payload": {"old": 1}},
            {"entity_type": "STOCK", "entity_id": "999999.SH", "payload": {"stale": 1}},
        ],
    )
    counts = copy_live_source_entities(
        store,
        "source",
        target,
        (item for item in (("STOCK", "600000.SH"), ("STOCK", "999999.SH"))),
        batch_size=1,
    )
    assert counts["feature_generation_members"] == 1
    rows = store.feature_generation_members(target)
    assert {row["entity_id"] for row in rows} == {"600000.SH"}

    # Full copy is SQL-only and includes all maintenance projections.
    target_full = _target(store, "target-full")
    full_counts = copy_live_source_generation(store, "source", target_full)
    assert full_counts["feature_generation_members"] == 2
    assert full_counts["stock_fundamental_features"] == 2
    assert len(store.get_fundamental_features(generation_id=target_full, strict=False)) == 2

    store.validate_feature_generation(target_full, validated_at=NOW)
    store.seal_generation(target_full, purpose="LIVE_FULL", activation_eligible=True, sealed_at=NOW)
    activated = store.activate_generation(target_full, None, "test-copy")
    assert activated["generation_id"] == target_full


def test_claim_then_source_select_and_missing_source_retries_dirty_items(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    store.mark_dirty(
        entity_type="STOCK",
        entity_id="600000.SH",
        reason_code="FUNDAMENTAL_CHANGED",
        source_version="v1",
        created_at=NOW,
        max_attempts=3,
    )
    observed: list[str] = []

    def selector():
        observed.append(str(store.list_dirty(statuses="LEASED")[0]["status"]))
        return None

    result = FeatureRebuildCoordinator(store, lambda *_args: None).run_incremental_from_live_source(
        as_of=NOW,
        worker_id="maintenance-test",
        source_selector=selector,
        now=NOW,
    )
    assert result.status == "FAILED"
    assert observed == ["LEASED"]
    assert result.reason_code == "LIVE_SOURCE_NOT_AVAILABLE"
    assert store.list_dirty(statuses="RETRY")[0]["last_error_code"] == "LIVE_SOURCE_NOT_AVAILABLE"


def test_incremental_missing_active_generation_never_upgrades_to_full(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _live_source(store, "source")
    store.mark_dirty(
        entity_type="STOCK",
        entity_id="600000.SH",
        reason_code="FUNDAMENTAL_CHANGED",
        source_version="v1",
        created_at=NOW,
        max_attempts=3,
    )
    result = FeatureRebuildCoordinator(store, lambda *_args: None).run_incremental_from_live_source(
        as_of=NOW,
        worker_id="maintenance-test",
        source_generation_id="source",
        now=NOW,
    )
    assert result.status == "FAILED"
    assert result.reason_code == "FEATURE_ACTIVE_GENERATION_MISSING"
    assert result.generation_id is None
    assert store.get_active_feature_generation() is None
    assert store.list_dirty(statuses="RETRY")[0]["status"] == "RETRY"


def test_incremental_source_compatibility_validator_can_fail_closed(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _live_source(store, "source")
    store.mark_dirty(
        entity_type="STOCK",
        entity_id="600000.SH",
        reason_code="FUNDAMENTAL_CHANGED",
        source_version="v1",
        created_at=NOW,
        max_attempts=3,
    )
    observed: list[tuple[str, str]] = []

    def compatibility(batch, source):
        observed.append((batch.worker_id, source["generation_id"]))
        return False

    result = FeatureRebuildCoordinator(store, lambda *_args: None).run_incremental_from_live_source(
        as_of=NOW,
        worker_id="maintenance-test",
        source_generation_id="source",
        source_compatibility_validator=compatibility,
        now=NOW,
    )
    assert result.status == "FAILED"
    assert result.reason_code == "LIVE_SOURCE_INCOMPATIBLE"
    assert observed == [("maintenance-test", "source")]
    assert store.list_dirty(statuses="RETRY")[0]["status"] == "RETRY"


def test_full_from_live_source_uses_sql_copy_and_publishes_without_entities(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    _live_source(store, "source")
    result = FeatureRebuildCoordinator(store, lambda *_args: None).run_full_from_live_source(
        as_of=NOW + timedelta(minutes=1),
        worker_id="weekly-source",
        source_generation_id="source",
    )
    assert result.status == "PUBLISHED"
    assert result.generation_id is not None
    assert result.processed_count == 2
    active = store.get_active_feature_generation()
    assert active is not None
    assert active["generation_id"] == result.generation_id
    generation = store.get_feature_generation(result.generation_id)
    assert generation["metadata"]["source_generation_id"] == "source"
    assert len(store.feature_generation_members(result.generation_id, strict=True)) == 2


def test_full_from_live_source_missing_source_returns_stable_reason(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    result = FeatureRebuildCoordinator(store, lambda *_args: None).run_full_from_live_source(
        as_of=NOW,
        worker_id="weekly-source",
        source_generation_id="missing-source",
    )
    assert result.status == "FAILED"
    assert result.reason_code == "LIVE_SOURCE_NOT_AVAILABLE"
    assert result.generation_id is None


def test_old_generation_check_constraint_is_migrated_to_live_source(tmp_path: Path):
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE feature_generations(
                generation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                as_of TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('STAGING','VALIDATED','SEALED','PUBLISHED','FAILED','LEGACY')),
                created_at TEXT NOT NULL,
                validated_at TEXT,
                published_at TEXT,
                failed_at TEXT,
                failure_reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                purpose TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK(purpose IN ('LIVE_FULL','LIVE_INCREMENTAL','RUN_SNAPSHOT','HISTORICAL_REPLAY','TEST_FIXTURE','UNKNOWN')),
                activation_eligible INTEGER NOT NULL DEFAULT 0 CHECK(activation_eligible IN (0,1)),
                sealed_at TEXT,
                validation_manifest_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        connection.execute("PRAGMA user_version=2")
    store = ResearchFeatureStore(path)
    source = store.create_or_get_live_source(
        snapshot_hash="migrated",
        source_hash="manifest",
        as_of=NOW,
        contract_version="test",
        algorithm_version="test",
    )
    assert source["purpose"] == "LIVE_SOURCE"
    with sqlite3.connect(path) as connection:
        sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='feature_generations'"
        ).fetchone()[0]
    assert "LIVE_SOURCE" in sql
