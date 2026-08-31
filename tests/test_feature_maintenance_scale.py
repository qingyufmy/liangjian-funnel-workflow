from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.feature_maintenance import (
    _canonical_hash,
    materialize_live_source,
    run_feature_maintenance,
)
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


TZ = ZoneInfo("Asia/Shanghai")


def _production_scale_symbols() -> list[str]:
    return [
        *(f"{value:06d}.SZ" for value in range(1, 2001)),
        *(f"{value:06d}.SH" for value in range(600001, 602001)),
        *(f"{value:06d}.BJ" for value in range(830001, 830018)),
    ]


def test_live_source_full_copy_covers_4017_symbols_without_snapshot_read(
    tmp_path: Path,
):
    symbols = _production_scale_symbols()
    assert len(symbols) == 4_017
    data = {
        "g0_symbols": symbols,
        "g0_candidates": [
            {"symbol": symbol, "name": f"fixture-{symbol}"} for symbol in symbols
        ],
        "RECENT_DAILY_BARS": {
            symbol: [
                {
                    "trade_date": "2026-08-28",
                    "close_price": 10,
                    "volume": 100,
                }
            ]
            for symbol in symbols
        },
        "COMPANY_FUNDAMENTALS": {
            symbol: {"roe": 12} for symbol in symbols
        },
        "TRADABILITY_FLAGS": {
            symbol: {"tradable": True} for symbol in symbols
        },
    }
    database = tmp_path / "features.sqlite3"
    store = ResearchFeatureStore(database)
    snapshot_hash = _canonical_hash(data)
    source = materialize_live_source(
        store,
        snapshot_id="snapshot-production-scale-fixture",
        snapshot_hash=snapshot_hash,
        as_of=datetime(2026, 8, 28, 15, 10, tzinfo=TZ),
        market_trade_date="2026-08-28",
        data=data,
        batch_size=50,
    )
    assert source.status == "READY"
    settings = SimpleNamespace(
        root=tmp_path,
        feature_store_db_path=database,
        workflow_progress_path=tmp_path / "state" / "workflow_progress.json",
        feature_maintenance_enabled=True,
        feature_source_batch_size=50,
    )

    result = run_feature_maintenance(
        settings,
        full=True,
        now=datetime(2026, 8, 29, 3, 30, tzinfo=TZ),
    )

    assert result["status"] == "PUBLISHED"
    assert result["processed_count"] == 4_017
    assert result["g0_count"] == 4_017
    assert result["validation"]["source_equivalence"]["counts"] == {
        "members": 4_017,
        "fundamental": 4_017,
        "taxonomy": 0,
        "business": 0,
    }
    with store._connect() as connection:  # noqa: SLF001 - integrity assertion
        assert str(connection.execute("PRAGMA quick_check").fetchone()[0]).lower() == "ok"
