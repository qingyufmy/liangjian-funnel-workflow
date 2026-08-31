import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.feature_maintenance import (
    FeatureMaintenanceError,
    _canonical_hash,
    _maintenance_lock,
    _source_supports_dirty,
    load_latest_verified_snapshot,
    materialize_live_source,
    run_feature_maintenance,
)
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


TZ = ZoneInfo("Asia/Shanghai")
SYMBOLS = ("600000.SH", "000001.SZ", "830001.BJ")


def _data(*, trade_date: str = "2026-08-28", first_roe: int = 12) -> dict:
    fundamentals = {symbol: {"roe": 12} for symbol in SYMBOLS}
    fundamentals[SYMBOLS[0]]["roe"] = first_roe
    return {
        "G0_SCOPE_CONTRACT": "CONFIGURED_RESEARCH_UNIVERSE_V1",
        "g0_symbols": list(SYMBOLS),
        "g0_candidates": [
            {"symbol": symbol, "name": f"name-{symbol}"} for symbol in SYMBOLS
        ],
        "RECENT_DAILY_BARS": {
            symbol: [
                {
                    "trade_date": trade_date,
                    "close_price": 10,
                    "volume": 100,
                }
            ]
            for symbol in SYMBOLS
        },
        "COMPANY_FUNDAMENTALS": fundamentals,
        "FACTOR_SNAPSHOT": {
            symbol: {"momentum": 0.8} for symbol in SYMBOLS
        },
        "A2_FACTOR_SNAPSHOT": {
            symbol: {"tier": "LEADER"} for symbol in SYMBOLS
        },
        "LIQUIDITY_SNAPSHOT": {
            symbol: {"amount": 1_000_000} for symbol in SYMBOLS
        },
        "TRADABILITY_FLAGS": {
            symbol: {"tradable": True} for symbol in SYMBOLS
        },
        "THS_INDUSTRY_MEMBERSHIP": {
            "records": [
                {
                    "thscode": symbol,
                    "memberships": [
                        {
                            "industry_thscode": "881155.TI",
                            "industry_name": "industry",
                        }
                    ],
                }
                for symbol in SYMBOLS
            ]
        },
        "THS_CONCEPT_MEMBERSHIP": {
            "records": [
                {
                    "thscode": symbol,
                    "memberships": [
                        {
                            "taxonomy_code": "885338.TI",
                            "taxonomy_name": "concept",
                        }
                    ],
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
                        "publish_time": trade_date,
                        "page_number": 1,
                    }
                ],
            }
            for symbol in SYMBOLS
        },
    }


def _settings(tmp_path: Path, *, enabled: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        root=tmp_path,
        snapshot_dir=tmp_path / "snapshots",
        feature_store_db_path=tmp_path / "features.sqlite3",
        workflow_progress_path=tmp_path / "state" / "workflow_progress.json",
        feature_maintenance_enabled=enabled,
        feature_source_batch_size=25,
    )


def _materialize(
    settings: SimpleNamespace,
    *,
    data: dict | None = None,
    trade_date: str = "2026-08-28",
    as_of: datetime | None = None,
) -> tuple[ResearchFeatureStore, dict]:
    store = ResearchFeatureStore(settings.feature_store_db_path)
    source_data = data or _data(trade_date=trade_date)
    snapshot_hash = _canonical_hash(source_data)
    result = materialize_live_source(
        store,
        snapshot_id=f"snapshot-{trade_date}-{snapshot_hash[:12]}",
        snapshot_hash=snapshot_hash,
        as_of=as_of or datetime.fromisoformat(f"{trade_date}T15:10:00+08:00"),
        market_trade_date=trade_date,
        data=source_data,
        batch_size=25,
    )
    assert result.status == "READY"
    return store, source_data


def test_historical_snapshot_loader_still_rejects_hash_mismatch(tmp_path: Path):
    settings = _settings(tmp_path)
    payload = {
        "snapshot_id": "snapshot-hash-fixture",
        "snapshot_hash": "bad",
        "as_of": "2026-08-28T15:10:00+08:00",
        "data": _data(),
    }
    settings.snapshot_dir.mkdir(parents=True)
    (settings.snapshot_dir / "snapshot-hash-fixture.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )

    try:
        load_latest_verified_snapshot(settings.snapshot_dir)
    except FeatureMaintenanceError as exc:
        assert exc.reason_code == "FEATURE_SNAPSHOT_HASH_MISMATCH"
    else:  # pragma: no cover
        raise AssertionError("hash mismatch must be rejected")


def test_disabled_maintenance_returns_before_opening_store_or_snapshot(tmp_path: Path):
    settings = _settings(tmp_path, enabled=False)
    result = run_feature_maintenance(
        settings, now=datetime(2026, 8, 31, 3, 30, tzinfo=TZ)
    )
    assert result["status"] == "NOOP"
    assert result["reason_code"] == "FEATURE_MAINTENANCE_DISABLED"
    assert not settings.feature_store_db_path.exists()


def test_sunday_returns_before_opening_store_or_snapshot(tmp_path: Path):
    settings = _settings(tmp_path)
    result = run_feature_maintenance(
        settings, now=datetime(2026, 8, 30, 3, 30, tzinfo=TZ)
    )
    assert result["status"] == "NOOP"
    assert result["reason_code"] == "NON_MAINTENANCE_DAY"
    assert not settings.feature_store_db_path.exists()


def test_empty_incremental_queue_is_noop_without_live_source(tmp_path: Path):
    settings = _settings(tmp_path)
    result = run_feature_maintenance(
        settings, now=datetime(2026, 8, 31, 3, 30, tzinfo=TZ)
    )
    assert result["status"] == "NOOP"
    assert result["reason_code"] == "NOOP_NO_DIRTY"
    progress = json.loads(settings.workflow_progress_path.read_text(encoding="utf-8"))
    assert progress["status"] == "SUCCEEDED"
    assert progress["phase"] == "FEATURE_MAINTENANCE_NOOP"
    assert progress["reason_code"] == "NOOP_NO_DIRTY"


def test_concurrent_maintenance_is_host_locked_without_opening_store(tmp_path: Path):
    settings = _settings(tmp_path)
    lock_path = settings.feature_store_db_path.with_suffix(".maintenance.lock")
    with _maintenance_lock(lock_path) as acquired:
        assert acquired is True
        result = run_feature_maintenance(
            settings, now=datetime(2026, 8, 31, 3, 30, tzinfo=TZ)
        )
        assert result["status"] == "NOOP"
        assert result["reason_code"] == "FEATURE_MAINTENANCE_BUSY"
        assert not settings.feature_store_db_path.exists()
    assert not lock_path.exists()


def test_dirty_version_compatibility_is_entity_scoped():
    source = {
        "validation_manifest": {
            "source_versions_by_entity": {SYMBOLS[1]: ["v2"]},
            "dependency_hashes_by_entity": {},
        }
    }
    batch = SimpleNamespace(
        all_claimed=(
            {
                "entity_id": SYMBOLS[0],
                "source_version": "v2",
                "dependency_hash": "",
            },
        ),
        claimed=(),
    )
    assert _source_supports_dirty(batch, source) is False


def test_full_maintenance_fails_before_generation_write_when_watermark_blocks(
    tmp_path: Path, monkeypatch
):
    settings = _settings(tmp_path)
    monkeypatch.setattr(
        "liangjian_funnel.pipeline.feature_maintenance.evaluate_disk_watermark",
        lambda _path: SimpleNamespace(
            full_rebuild_allowed=False,
            incremental_write_allowed=True,
            as_dict=lambda: {"status": "CRITICAL"},
        ),
    )
    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    assert result["status"] == "FAILED_RESOURCE"
    assert result["reason_code"] == "FEATURE_FULL_REBUILD_STORAGE_WATERMARK_BLOCKED"
    assert not settings.feature_store_db_path.exists()


def test_full_maintenance_publishes_from_live_source_without_snapshot_file(
    tmp_path: Path,
):
    settings = _settings(tmp_path)
    store, _ = _materialize(settings)
    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    assert result["status"] == "PUBLISHED"
    assert result["mode"] == "FULL"
    assert result["processed_count"] == len(SYMBOLS)
    assert result["g0_count"] == len(SYMBOLS)
    assert not settings.snapshot_dir.exists()
    generation_id = result["generation_id"]
    assert len(store.feature_generation_members(generation_id, strict=True)) == len(SYMBOLS)
    validation = result["validation"]
    assert validation["status"] == "READY"
    assert validation["activation_eligible"] is True
    assert validation["coverage"]["fundamental"]["status"] == "READY"
    assert validation["source_equivalence"]["status"] == "READY"
    assert validation["source_equivalence"]["counts"] == {
        "members": len(SYMBOLS),
        "fundamental": len(SYMBOLS),
        "taxonomy": len(SYMBOLS) * 2,
        "business": len(SYMBOLS),
    }


def test_full_maintenance_missing_source_is_explicitly_blocked(tmp_path: Path):
    settings = _settings(tmp_path)
    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    assert result["status"] == "BLOCKED_SOURCE_GENERATION"
    assert result["reason_code"] == "LIVE_SOURCE_NOT_AVAILABLE"


def test_newer_stale_source_blocks_fallback_to_older_ready_source(tmp_path: Path):
    settings = _settings(tmp_path)
    store, _ = _materialize(settings)
    stale_data = _data(trade_date="2026-08-28", first_roe=13)
    stale_hash = _canonical_hash(stale_data)
    stale = materialize_live_source(
        store,
        snapshot_id="snapshot-newer-stale",
        snapshot_hash=stale_hash,
        as_of=datetime(2026, 8, 31, 3, 15, tzinfo=TZ),
        market_trade_date="2026-08-31",
        data=stale_data,
        batch_size=25,
    )
    assert stale.status == "BLOCKED_SOURCE_GENERATION"
    assert stale.reason_code == "FEATURE_SOURCE_MARKET_DATA_STALE"

    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 31, 4, tzinfo=TZ),
    )

    assert result["status"] == "BLOCKED_SOURCE_GENERATION"
    assert result["reason_code"] == "FEATURE_SOURCE_MARKET_DATA_STALE"


def test_incremental_missing_source_retries_claimed_dirty_item(tmp_path: Path):
    settings = _settings(tmp_path)
    store = ResearchFeatureStore(settings.feature_store_db_path)
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=SYMBOLS[0],
        reason_code="FACT_UPDATE",
        source_version="v1",
        created_at=datetime(2026, 8, 31, 3, tzinfo=TZ),
    )
    result = run_feature_maintenance(
        settings, now=datetime(2026, 8, 31, 3, 30, tzinfo=TZ)
    )
    assert result["status"] == "BLOCKED_SOURCE_GENERATION"
    assert result["reason_code"] == "LIVE_SOURCE_NOT_AVAILABLE"
    dirty = store.list_dirty(statuses=("RETRY",), limit=10)
    assert len(dirty) == 1
    assert dirty[0]["last_error_code"] == "LIVE_SOURCE_NOT_AVAILABLE"


def test_incremental_replaces_one_symbol_from_new_source_and_preserves_others(
    tmp_path: Path,
):
    settings = _settings(tmp_path)
    store, _ = _materialize(settings)
    full = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 4, tzinfo=TZ),
    )
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=SYMBOLS[0],
        reason_code="FUNDAMENTAL_CHANGED",
        source_version="v2",
        created_at=datetime(2026, 8, 31, 3, tzinfo=TZ),
    )
    changed_data = _data(trade_date="2026-08-31", first_roe=99)
    _materialize(
        settings,
        data=changed_data,
        trade_date="2026-08-31",
        as_of=datetime(2026, 8, 31, 3, 15, tzinfo=TZ),
    )
    incremental = run_feature_maintenance(
        settings, now=datetime(2026, 8, 31, 4, tzinfo=TZ)
    )
    assert incremental["status"] == "PUBLISHED"
    assert incremental["processed_count"] == 1
    assert incremental["previous_generation_id"] == full["generation_id"]
    rows = store.get_fundamental_features(
        generation_id=incremental["generation_id"], strict=True
    )
    assert len(rows) == len(SYMBOLS)
    assert next(row for row in rows if row["symbol"] == SYMBOLS[0])[
        "financial_features"
    ]["roe"] == 99
    old_rows = store.get_fundamental_features(
        generation_id=full["generation_id"], strict=True
    )
    assert next(row for row in old_rows if row["symbol"] == SYMBOLS[0])[
        "financial_features"
    ]["roe"] == 12


def test_incremental_without_active_generation_retries_dirty_item(tmp_path: Path):
    settings = _settings(tmp_path)
    store = ResearchFeatureStore(settings.feature_store_db_path)
    store.mark_dirty(
        entity_type="STOCK",
        entity_id=SYMBOLS[0],
        reason_code="FACT_UPDATE",
        source_version="v1",
        created_at=datetime(2026, 8, 31, 3, tzinfo=TZ),
    )
    _materialize(
        settings,
        trade_date="2026-08-31",
        as_of=datetime(2026, 8, 31, 3, 15, tzinfo=TZ),
    )
    result = run_feature_maintenance(
        settings, now=datetime(2026, 8, 31, 4, tzinfo=TZ)
    )
    assert result["status"] == "FAILED"
    assert result["reason_code"] == "FEATURE_ACTIVE_GENERATION_MISSING"
    dirty = store.list_dirty(statuses=("RETRY",), limit=10)
    assert len(dirty) == 1
    assert dirty[0]["last_error_code"] == "FEATURE_ACTIVE_GENERATION_MISSING"
