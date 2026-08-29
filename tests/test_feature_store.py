import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.feature_store import (
    FEATURE_SCHEMA,
    LEGACY_GENERATION_ID,
    FeatureGenerationError,
    ResearchFeatureStore,
)


NOW = datetime(2026, 8, 29, 9, 30, tzinfo=timezone.utc)


def _create_generation(store: ResearchFeatureStore, generation_id: str) -> str:
    return store.create_feature_generation(
        generation_id=generation_id,
        domain="RESEARCH",
        as_of=NOW,
        contract_version="research-outcome/2.0.0",
        algorithm_version="a2-features/2.0.0",
        source_manifest_hash=f"manifest-{generation_id}",
        created_at=NOW,
    )


def _publish(store: ResearchFeatureStore, generation_id: str) -> None:
    store.validate_feature_generation(generation_id, validated_at=NOW)
    store.publish_feature_generation(generation_id, activated_at=NOW)


def _decision(symbol: str, score: float) -> dict:
    return {
        "symbol": symbol,
        "status": "REVIEW_CANDIDATE",
        "score": score,
        "sent_to_llm": True,
        "reason_codes": ["TEST"],
        "source_hashes": {"fixture": "v2"},
    }


def test_fresh_store_is_v2_and_legacy_generation_is_read_disabled(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")

    assert store.get_feature_generation(LEGACY_GENERATION_ID)["status"] == "LEGACY"
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert connection.execute("SELECT value FROM feature_store_meta WHERE key='schema'").fetchone()[0] == FEATURE_SCHEMA
        assert "generation_id" in {
            row[1] for row in connection.execute("PRAGMA table_info(deterministic_stage_decisions)")
        }

    with pytest.raises(FeatureGenerationError, match="LEGACY_READ_DISABLED"):
        store.strict_stage_decisions("run", "lane_1", "A1", generation_id=LEGACY_GENERATION_ID)


def test_v1_migration_keeps_audit_table_and_isolates_legacy_rows(tmp_path: Path):
    path = tmp_path / "v1.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE feature_store_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO feature_store_meta VALUES('schema', 'liangjian-research-feature-store/1.0.0');
            CREATE TABLE deterministic_stage_decisions(
                run_id TEXT NOT NULL, lane_id TEXT NOT NULL, stage TEXT NOT NULL,
                symbol TEXT NOT NULL, status TEXT NOT NULL, score REAL,
                node_id TEXT, theme_id TEXT, node_rank INTEGER, sent_to_llm INTEGER NOT NULL,
                reason_codes_json TEXT NOT NULL, source_hashes_json TEXT NOT NULL,
                payload_json TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY(run_id, lane_id, stage, symbol)
            );
            INSERT INTO deterministic_stage_decisions VALUES(
                'old-run','lane_1','A1','600000.SH','OLD',1,NULL,NULL,NULL,0,'[]','{}',
                '{"symbol":"600000.SH","status":"OLD"}','2026-08-29'
            );
            PRAGMA user_version=1;
            """
        )

    store = ResearchFeatureStore(path)
    old_rows = store.stage_decisions("old-run", "lane_1", "A1")
    assert old_rows == [{"symbol": "600000.SH", "status": "OLD"}]
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM deterministic_stage_decisions_legacy_v1"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM deterministic_stage_decisions WHERE generation_id=?",
            (LEGACY_GENERATION_ID,),
        ).fetchone()[0] == 1

    generation = _create_generation(store, "current")
    store.replace_stage_decisions(
        run_id="old-run",
        lane_id="lane_1",
        stage="A1",
        decisions=[_decision("600000.SH", 90)],
        updated_at=NOW,
        generation_id=generation,
    )
    assert store.stage_decisions("old-run", "lane_1", "A1", generation_id=generation) == [
        {**_decision("600000.SH", 90)}
    ]
    assert store.stage_decisions(
        "old-run", "lane_1", "A1", generation_id=LEGACY_GENERATION_ID
    ) == [{"symbol": "600000.SH", "status": "OLD"}]


def test_generation_failure_does_not_change_active_generation(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    first = _create_generation(store, "g1")
    _publish(store, first)
    second = _create_generation(store, "g2")
    store.replace_stage_decisions(
        run_id="run-2",
        lane_id="lane_1",
        stage="A1",
        decisions=[_decision("600001.SH", 70)],
        updated_at=NOW,
        generation_id=second,
    )

    store.fail_feature_generation(second, reason="CHECKSUM_MISMATCH", failed_at=NOW)
    assert store.get_active_feature_generation()["generation_id"] == first
    assert store.get_feature_generation(second)["status"] == "FAILED"
    with pytest.raises(FeatureGenerationError, match="FAILED_NOT_PUBLISHABLE"):
        store.publish_feature_generation(second)
    assert store.strict_stage_decisions(
        "run-2", "lane_1", "A1", generation_id=first
    ) == []


def test_publish_switch_is_atomic_and_run_binding_cannot_drift(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    first = _create_generation(store, "g1")
    second = _create_generation(store, "g2")
    _publish(store, first)

    binding = store.bind_run_feature_generation(
        run_id="run-1", generation_id=first, contract_hash="contract-a", bound_at=NOW
    )
    assert binding["generation_id"] == first
    with pytest.raises(FeatureGenerationError, match="CONTRACT_MISMATCH"):
        store.bind_run_feature_generation(
            run_id="run-1", generation_id=first, contract_hash="contract-b", bound_at=NOW
        )
    _publish(store, second)
    with pytest.raises(FeatureGenerationError, match="ALREADY_BOUND"):
        store.bind_run_feature_generation(
            run_id="run-1", generation_id=second, contract_hash="contract-a", bound_at=NOW
        )

    store.replace_stage_decisions(
        run_id="run-1",
        lane_id="lane_1",
        stage="A1",
        decisions=[_decision("600002.SH", 80)],
        updated_at=NOW,
    )
    # The run remains on g1 even after the active pointer moves to g2.
    assert store.strict_stage_decisions("run-1", "lane_1", "A1")
    assert store.get_run_feature_binding(run_id="run-1", strict=True)["generation_id"] == first
    assert store.get_active_feature_generation()["generation_id"] == second


def test_published_source_generation_is_immutable_but_bound_run_can_append_decisions(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _create_generation(store, "immutable-source")
    store.replace_taxonomy_memberships(
        taxonomy="INDUSTRY",
        snapshot={"records": []},
        as_of=NOW,
        generation_id=generation,
    )
    _publish(store, generation)

    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_IMMUTABLE"):
        store.replace_taxonomy_memberships(
            taxonomy="INDUSTRY",
            snapshot={"records": []},
            as_of=NOW,
            generation_id=generation,
        )

    store.bind_run_feature_generation(
        run_id="run-bound",
        generation_id=generation,
        contract_hash="contract-a",
        bound_at=NOW,
    )
    store.replace_stage_decisions(
        run_id="run-bound",
        lane_id="lane_1",
        stage="A1",
        decisions=[_decision("600000.SH", 88)],
        updated_at=NOW,
    )
    assert store.strict_stage_decisions("run-bound", "lane_1", "A1") == [
        _decision("600000.SH", 88)
    ]


def test_concurrent_generation_creation_and_publication_does_not_cross_contaminate(tmp_path: Path):
    path = tmp_path / "features.sqlite3"

    def create_and_publish(index: int) -> str:
        store = ResearchFeatureStore(path)
        generation = _create_generation(store, f"parallel-{index}")
        _publish(store, generation)
        return generation

    with ThreadPoolExecutor(max_workers=2) as executor:
        generations = list(executor.map(create_and_publish, (1, 2)))

    store = ResearchFeatureStore(path)
    active = store.get_active_feature_generation()
    assert active["generation_id"] in generations
    assert {item["generation_id"] for item in store.list_feature_generations(domain="RESEARCH")} >= set(generations)
    audits = store.list_generation_activation_audit(domain="RESEARCH", limit=1000)
    assert {item["generation_id"] for item in audits} >= set(generations)
