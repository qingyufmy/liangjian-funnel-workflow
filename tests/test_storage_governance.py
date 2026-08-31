from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from liangjian_funnel.cli import main
from liangjian_funnel.runtime.storage_governance import (
    StorageGovernanceError,
    backup_sqlite,
    evaluate_disk_watermark,
    inspect_sqlite,
    live_source_storage_projection,
    scan_reference_plan,
    storage_cleanup_plan,
)
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore
from liangjian_funnel.settings import Settings


def _usage(free_percent: float) -> SimpleNamespace:
    total = 100 * 1024 * 1024 * 1024
    free = int(total * free_percent / 100)
    return SimpleNamespace(total=total, used=total - free, free=free)


@pytest.mark.parametrize(
    ("free_percent", "status", "full", "incremental", "research"),
    (
        (40.0, "OK", True, True, True),
        (25.0, "OK", True, True, True),
        (20.0, "WARNING", True, True, True),
        (15.0, "WARNING", True, True, True),
        (12.0, "CRITICAL", False, True, True),
        (10.0, "CRITICAL", False, True, True),
        (9.0, "BLOCKED", False, False, False),
    ),
)
def test_disk_watermark_contract(
    tmp_path: Path,
    free_percent: float,
    status: str,
    full: bool,
    incremental: bool,
    research: bool,
) -> None:
    result = evaluate_disk_watermark(tmp_path, usage=_usage(free_percent))

    assert result.status == status
    assert result.full_rebuild_allowed is full
    assert result.incremental_write_allowed is incremental
    assert result.research_write_allowed is research
    assert result.read_only_allowed is True
    assert result.as_dict()["thresholds"] == {
        "warning_free_percent": 25.0,
        "critical_free_percent": 15.0,
        "block_free_percent": 10.0,
        "full_rebuild_min_free_bytes": 5 * 1024 * 1024 * 1024,
    }


def test_full_rebuild_is_blocked_below_five_gib_even_when_percentage_is_high(tmp_path: Path) -> None:
    gib = 1024 * 1024 * 1024
    result = evaluate_disk_watermark(
        tmp_path,
        usage=SimpleNamespace(total=10 * gib, used=6 * gib, free=4 * gib),
    )

    assert result.status == "CRITICAL"
    assert result.full_rebuild_allowed is False
    assert result.incremental_write_allowed is True
    assert "DISK_FREE_BELOW_5_GIB" in result.reason_codes


def _make_feature_db(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE feature_generations(
                generation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                status TEXT NOT NULL,
                purpose TEXT NOT NULL,
                as_of TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE active_feature_generations(
                domain TEXT PRIMARY KEY,
                generation_id TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                previous_generation_id TEXT
            );
            CREATE TABLE run_feature_bindings(
                run_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                generation_id TEXT NOT NULL,
                bound_at TEXT NOT NULL,
                PRIMARY KEY(run_id, domain)
            );
            INSERT INTO feature_generations VALUES
                ('g-active','RESEARCH','SEALED','LIVE_FULL','2026-08-29T09:00:00+08:00','2026-08-29T09:00:00+08:00'),
                ('g-previous','RESEARCH','SEALED','LIVE_FULL','2026-08-28T09:00:00+08:00','2026-08-28T09:00:00+08:00'),
                ('g-bound','RESEARCH','SEALED','HISTORICAL_REPLAY','2026-08-27T09:00:00+08:00','2026-08-27T09:00:00+08:00'),
                ('g-free','RESEARCH','FAILED','LIVE_FULL','2026-08-26T09:00:00+08:00','2026-08-26T09:00:00+08:00');
            INSERT INTO active_feature_generations VALUES
                ('RESEARCH','g-active','2026-08-29T09:00:00+08:00','g-previous');
            INSERT INTO run_feature_bindings VALUES
                ('replay-1','RESEARCH','g-bound','2026-08-29T09:01:00+08:00');
            """
        )


def test_reference_scan_proves_all_protection_classes_without_deleting(tmp_path: Path) -> None:
    database = tmp_path / "features.sqlite3"
    _make_feature_db(database)
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    snapshot = snapshots / "replay.json"
    snapshot.write_text(
        json.dumps({"snapshot_id": "s-1", "feature_generation_id": "g-bound"}),
        encoding="utf-8",
    )

    before = database.read_bytes()
    plan = scan_reference_plan(database, snapshot_roots=(snapshots,))

    assert plan["referenced_generation_ids"] == [
        "g-active",
        "g-bound",
        "g-previous",
    ]
    assert {item["generation_id"] for item in plan["active"]} == {"g-active"}
    assert {item["generation_id"] for item in plan["previous"]} == {"g-previous"}
    assert {item["generation_id"] for item in plan["run_bound"]} == {"g-bound"}
    assert {item["generation_id"] for item in plan["snapshot_refs"]} == {"g-bound"}
    assert [item["generation_id"] for item in plan["candidates"]] == ["g-free"]
    assert plan["deletion_allowed"] is False
    assert plan["dry_run"] is True
    assert database.read_bytes() == before


def test_online_backup_manifest_integrity_and_temporary_restore(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE facts(symbol TEXT PRIMARY KEY, value INTEGER)")
        connection.execute("INSERT INTO facts VALUES (?, ?)", ("600000.SH", 42))
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    destination = tmp_path / "backups" / "source.sqlite3"

    payload = backup_sqlite(source, destination)

    assert destination.is_file()
    manifest_path = Path(payload["manifest_path"])
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"].startswith("liangjian-storage-governance/")
    assert manifest["backup"]["sha256"] == hashlib.sha256(destination.read_bytes()).hexdigest()
    assert manifest["sha256"] == manifest["backup"]["sha256"]
    manifest_without_digest = dict(manifest)
    manifest_digest = manifest_without_digest.pop("manifest_sha256")
    canonical = json.dumps(
        manifest_without_digest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    assert manifest_digest == hashlib.sha256(canonical).hexdigest()
    assert manifest["restore_validation"]["status"] == "PASS"
    assert manifest["restore_validation"]["temporary_directory_removed"] is True
    assert manifest["source"]["sha256_at_start_or_after"] == source_hash
    with sqlite3.connect(destination) as connection:
        assert connection.execute("SELECT value FROM facts WHERE symbol='600000.SH'").fetchone()[0] == 42
    assert inspect_sqlite(destination).healthy is True

    with pytest.raises(StorageGovernanceError, match="DESTINATION_EXISTS"):
        backup_sqlite(source, destination)


def test_cleanup_plan_is_always_dry_run_and_does_not_change_database(tmp_path: Path) -> None:
    database = tmp_path / "features.sqlite3"
    _make_feature_db(database)
    before = database.read_bytes()

    payload = storage_cleanup_plan(database)

    assert payload["status"] == "DRY_RUN"
    assert payload["dry_run"] is True
    assert payload["deletion_allowed"] is False
    assert payload["reference_plan"]["candidates"][0]["action"] == "REVIEW_ONLY"
    assert database.read_bytes() == before


def test_retention_protects_latest_two_live_sources_and_staging_reference(tmp_path: Path) -> None:
    database = tmp_path / "features.sqlite3"
    store = ResearchFeatureStore(database)
    source_ids: list[str] = []
    for day in range(1, 5):
        row = store.create_or_get_live_source(
            snapshot_hash=f"snapshot-{day}",
            source_manifest_hash=f"source-{day}",
            as_of=f"2026-08-0{day}T15:10:00+08:00",
            contract_version="live-source-generation/1.0.0",
            algorithm_version="test",
            metadata={"market_trade_date": f"2026-08-0{day}"},
        )
        generation_id = str(row["generation_id"])
        store.record_feature_generation_members_batched(
            generation_id=generation_id,
            members=[{
                "entity_type": "STOCK",
                "entity_id": "600000.SH",
                "partition_name": "snapshot-inputs",
                "payload": {"symbol": "600000.SH", "day": day},
            }],
        )
        validation = {"status": "READY", "activation_eligible": False}
        store.validate_feature_generation(generation_id, validation=validation)
        store.seal_generation(
            generation_id,
            validation_manifest=validation,
            purpose="LIVE_SOURCE",
            activation_eligible=False,
        )
        source_ids.append(generation_id)
    target = store.create_feature_generation(
        as_of="2026-08-05T03:30:00+08:00",
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="target-source",
        metadata={"source_generation_id": source_ids[0]},
        purpose="LIVE_FULL",
        activation_eligible=True,
    )

    plan = scan_reference_plan(database)

    assert set(plan["protected_live_source_generation_ids"]) == {
        source_ids[0], source_ids[2], source_ids[3],
    }
    assert plan["staging_source_refs"] == [{
        "kind": "staging_source",
        "generation_id": source_ids[0],
        "target_generation_id": target,
    }]
    assert source_ids[1] in {item["generation_id"] for item in plan["candidates"]}
    projection = live_source_storage_projection(database)
    assert projection["source_count"] == 4
    assert projection["average_bytes"] > 0
    assert projection["projected_14d_bytes"] == projection["average_bytes"] * 14


def test_cli_storage_cleanup_refuses_execute_and_storage_backup_is_explicit(tmp_path: Path, capsys) -> None:
    database = tmp_path / "features.sqlite3"
    _make_feature_db(database)
    settings = Settings.from_env({}, root=tmp_path)

    assert main(
        ["storage-cleanup", "--feature-db", str(database)],
        settings=settings,
    ) == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["dry_run"] is True
    assert dry_run["deletion_allowed"] is False

    assert main(
        ["storage-cleanup", "--feature-db", str(database), "--execute"],
        settings=settings,
    ) == 2
    refused = json.loads(capsys.readouterr().out)
    assert refused["reason_code"] == "STORAGE_CLEANUP_EXECUTION_NOT_IMPLEMENTED"
    assert database.is_file()

    destination = tmp_path / "backup.sqlite3"
    assert main(
        ["storage-backup", str(database), "--destination", str(destination)],
        settings=settings,
    ) == 0
    backup_output = json.loads(capsys.readouterr().out)
    assert Path(backup_output["manifest_path"]).is_file()
    assert destination.is_file()
