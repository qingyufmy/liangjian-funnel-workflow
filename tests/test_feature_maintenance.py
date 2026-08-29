import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.feature_maintenance import (
    FeatureMaintenanceError,
    _canonical_hash,
    run_feature_maintenance,
)
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


TZ = ZoneInfo("Asia/Shanghai")
SYMBOLS = ("600000.SH", "000001.SZ", "830001.BJ")


def _write_snapshot(root: Path, *, snapshot_id: str = "snapshot-20260829T030000+0800-fixture") -> Path:
    data = {
        "G0_SCOPE_CONTRACT": "CONFIGURED_RESEARCH_UNIVERSE_V1",
        "g0_symbols": list(SYMBOLS),
        "g0_candidates": [{"symbol": symbol, "name": f"name-{symbol}"} for symbol in SYMBOLS],
        "RECENT_DAILY_BARS": {symbol: [{"close_price": 10, "volume": 100}] for symbol in SYMBOLS},
        "COMPANY_FUNDAMENTALS": {symbol: {"roe": 12} for symbol in SYMBOLS},
        "FACTOR_SNAPSHOT": {symbol: {"momentum": 0.8} for symbol in SYMBOLS},
        "A2_FACTOR_SNAPSHOT": {symbol: {"tier": "LEADER"} for symbol in SYMBOLS},
        "LIQUIDITY_SNAPSHOT": {symbol: {"amount": 1_000_000} for symbol in SYMBOLS},
        "TRADABILITY_FLAGS": {symbol: {"tradable": True} for symbol in SYMBOLS},
        "THS_INDUSTRY_MEMBERSHIP": {
            "records": [
                {
                    "thscode": symbol,
                    "memberships": [{"industry_thscode": "881155.TI", "industry_name": "industry"}],
                }
                for symbol in SYMBOLS
            ]
        },
        "THS_CONCEPT_MEMBERSHIP": {
            "records": [
                {
                    "thscode": symbol,
                    "memberships": [{"taxonomy_code": "885338.TI", "taxonomy_name": "concept"}],
                }
                for symbol in SYMBOLS
            ]
        },
        "MAIN_BUSINESS_EVIDENCE": {
            symbol: {
                "available": True,
                "evidence": [
                    {
                        "source_ref": f"cninfo:{symbol}",
                        "announcement_title": "annual report",
                        "publish_time": "2026-08-28",
                        "page_number": 1,
                    }
                ],
            }
            for symbol in SYMBOLS
        },
    }
    payload = {
        "snapshot_id": snapshot_id,
        "snapshot_hash": _canonical_hash(data),
        "as_of": "2026-08-29T03:00:00+08:00",
        "data": data,
    }
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{snapshot_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _settings(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        snapshot_dir=tmp_path / "snapshots",
        feature_store_db_path=tmp_path / "features.sqlite3",
    )


def test_feature_maintenance_rejects_snapshot_hash_mismatch(tmp_path: Path):
    settings = _settings(tmp_path)
    path = _write_snapshot(settings.snapshot_dir)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["snapshot_hash"] = "bad"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(FeatureMaintenanceError, match="FEATURE_SNAPSHOT_HASH_MISMATCH"):
        run_feature_maintenance(settings, full=True, now=datetime(2026, 8, 29, 4, tzinfo=TZ))


def test_full_maintenance_fails_before_writes_when_storage_watermark_blocks(tmp_path: Path, monkeypatch):
    settings = _settings(tmp_path)
    _write_snapshot(settings.snapshot_dir)
    monkeypatch.setattr(
        "liangjian_funnel.pipeline.feature_maintenance.evaluate_disk_watermark",
        lambda _path: SimpleNamespace(
            full_rebuild_allowed=False,
            incremental_write_allowed=True,
            as_dict=lambda: {"status": "CRITICAL"},
        ),
    )

    with pytest.raises(FeatureMaintenanceError, match="FEATURE_FULL_REBUILD_STORAGE_WATERMARK_BLOCKED"):
        run_feature_maintenance(settings, full=True, now=datetime(2026, 8, 29, 4, tzinfo=TZ))
    assert not settings.feature_store_db_path.exists()


def test_full_maintenance_writes_stock_members_and_shared_taxonomy_business_once(tmp_path: Path, monkeypatch):
    settings = _settings(tmp_path)
    _write_snapshot(settings.snapshot_dir)

    store = ResearchFeatureStore(settings.feature_store_db_path)
    member_calls = []
    original_record_members = store.record_feature_generation_members

    def record_members(*, generation_id, members):
        member_calls.append(len(members))
        return original_record_members(generation_id=generation_id, members=members)

    monkeypatch.setattr(
        "liangjian_funnel.pipeline.feature_maintenance.ResearchFeatureStore",
        lambda _path: store,
    )
    # The callback is invoked once per G0 stock by the coordinator, but the
    # underlying generation-member write must remain one full-universe batch.
    monkeypatch.setattr(store, "record_feature_generation_members", record_members)

    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )

    assert result["status"] == "PUBLISHED"
    assert result["mode"] == "FULL"
    assert result["processed_count"] == len(SYMBOLS)
    generation_id = result["generation_id"]
    assert member_calls == [len(SYMBOLS)]
    assert len(store.feature_generation_members(generation_id, strict=True)) == len(SYMBOLS)
    fundamentals = store.get_fundamental_features(generation_id=generation_id, strict=True)
    assert {row["symbol"] for row in fundamentals} == set(SYMBOLS)
    assert all(row["available"] == 1 for row in fundamentals)
    assert all(row["financial_features"] for row in fundamentals)
    validation = result["validation"]
    assert validation["status"] == "READY"
    assert validation["activation_eligible"] is True
    assert validation["coverage"]["members"]["status"] == "READY"
    assert validation["coverage"]["fundamental"]["status"] == "READY"
    assert validation["coverage"]["taxonomy"]["status"] == "READY"
    assert validation["coverage"]["business"]["status"] == "READY"
    assert all(
        item["status"] == "RUN_SCOPED" and item["rows"] == 0
        for item in validation["runtime_projections"].values()
    )
    with store._connect() as connection:  # noqa: SLF001
        assert connection.execute(
            "SELECT COUNT(*) FROM taxonomy_membership_versions WHERE generation_id=?",
            (generation_id,),
        ).fetchone()[0] == len(SYMBOLS) * 2
        assert connection.execute(
            "SELECT COUNT(*) FROM business_exposure_facts WHERE generation_id=?",
            (generation_id,),
        ).fetchone()[0] == len(SYMBOLS)


def test_missing_required_fundamental_cannot_activate_and_keeps_old_active(tmp_path: Path):
    settings = _settings(tmp_path)
    snapshot_path = _write_snapshot(settings.snapshot_dir)
    baseline = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["data"]["COMPANY_FUNDAMENTALS"].pop(SYMBOLS[-1])
    payload["snapshot_hash"] = _canonical_hash(payload["data"])
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 5, tzinfo=TZ),
    )

    assert result["status"] == "FAILED"
    assert "REQUIRED_COVERAGE" in (result["error"] or "")
    store = ResearchFeatureStore(settings.feature_store_db_path)
    assert store.get_active_feature_generation()["generation_id"] == baseline["generation_id"]
    failed = store.get_feature_generation(result["generation_id"])
    assert failed["status"] == "FAILED"


def test_incremental_replaces_one_symbol_fundamental_and_preserves_other_rows(tmp_path: Path):
    settings = _settings(tmp_path)
    snapshot_path = _write_snapshot(settings.snapshot_dir)
    full = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    store = ResearchFeatureStore(settings.feature_store_db_path)
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=SYMBOLS[0],
        reason_code="FUNDAMENTAL_CHANGED",
        source_version="v2",
        created_at=datetime(2026, 8, 31, 3, tzinfo=TZ),
    )
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["data"]["COMPANY_FUNDAMENTALS"][SYMBOLS[0]]["roe"] = 99
    payload["snapshot_hash"] = _canonical_hash(payload["data"])
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    incremental = run_feature_maintenance(
        settings,
        now=datetime(2026, 8, 31, 4, tzinfo=TZ),
    )

    assert incremental["status"] == "PUBLISHED"
    rows = store.get_fundamental_features(
        generation_id=incremental["generation_id"],
        strict=True,
    )
    assert len(rows) == len(SYMBOLS)
    changed = next(row for row in rows if row["symbol"] == SYMBOLS[0])
    assert changed["financial_features"]["roe"] == 99
    assert sum(row["symbol"] == SYMBOLS[0] for row in rows) == 1
    # The old generation remains immutable and retains its original value.
    old = store.get_fundamental_features(generation_id=full["generation_id"], strict=True)
    original = next(row for row in old if row["symbol"] == SYMBOLS[0])
    assert original["financial_features"]["roe"] == 12


def test_incremental_maintenance_only_rebuilds_dirty_stock_and_sunday_is_noop(tmp_path: Path):
    settings = _settings(tmp_path)
    _write_snapshot(settings.snapshot_dir)
    full = run_feature_maintenance(settings, full=True, now=datetime(2026, 8, 29, 4, tzinfo=TZ))
    store = ResearchFeatureStore(settings.feature_store_db_path)
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=SYMBOLS[0],
        reason_code="FACT_UPDATE",
        source_version="sync-1",
        created_at=datetime(2026, 8, 31, 3, tzinfo=TZ),
    )
    incremental = run_feature_maintenance(
        settings,
        now=datetime(2026, 8, 31, 4, tzinfo=TZ),
    )
    assert incremental["status"] == "PUBLISHED"
    assert incremental["mode"] == "INCREMENTAL"
    assert incremental["processed_count"] == 1
    assert incremental["previous_generation_id"] == full["generation_id"]
    sunday = run_feature_maintenance(settings, now=datetime(2026, 8, 30, 4, tzinfo=TZ))
    assert sunday["status"] == "NOOP"
    assert sunday["reason_code"] == "NON_MAINTENANCE_DAY"
