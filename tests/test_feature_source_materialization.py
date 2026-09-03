from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.feature_maintenance import materialize_live_source
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore


TZ = ZoneInfo("Asia/Shanghai")


def _data(count: int = 30, *, trade_date: str = "2026-08-31") -> dict:
    symbols = [f"{600000 + index:06d}.SH" for index in range(count)]
    return {
        "g0_symbols": symbols,
        "g0_candidates": [{"symbol": symbol, "name": symbol} for symbol in symbols],
        "RECENT_DAILY_BARS": {
            symbol: [{"date": trade_date, "close": 10.0, "volume": 1000}]
            for symbol in symbols
        },
        "COMPANY_FUNDAMENTALS": {symbol: {"roe": 12.0} for symbol in symbols},
        "FACTOR_SNAPSHOT": {symbol: {"momentum": 0.5} for symbol in symbols},
        "A2_FACTOR_SNAPSHOT": {symbol: {"tier": "LEADER"} for symbol in symbols},
        "LIQUIDITY_SNAPSHOT": {symbol: {"turnover": 10_000_000} for symbol in symbols},
        "TRADABILITY_FLAGS": {symbol: {"tradable": True} for symbol in symbols},
        "THS_INDUSTRY_MEMBERSHIP": {"records": []},
        "THS_CONCEPT_MEMBERSHIP": {"records": []},
        "MAIN_BUSINESS_EVIDENCE": {},
    }


def test_live_source_is_batched_idempotent_and_never_active(tmp_path: Path, monkeypatch) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    batch_sizes: list[int] = []
    original = store.record_feature_generation_members_batched

    def record(*, generation_id, members, batch_size=50):
        values = list(members)
        batch_sizes.append(len(values))
        return original(
            generation_id=generation_id,
            members=values,
            batch_size=batch_size,
        )

    monkeypatch.setattr(store, "record_feature_generation_members_batched", record)
    kwargs = {
        "snapshot_id": "snapshot-20260831T151000+0800-fixture",
        "snapshot_hash": "a" * 64,
        "as_of": datetime(2026, 8, 31, 15, 10, tzinfo=TZ),
        "market_trade_date": "2026-08-31",
        "data": _data(),
        "batch_size": 25,
    }

    first = materialize_live_source(store, **kwargs)
    second = materialize_live_source(store, **kwargs)

    assert first.status == "READY"
    assert first.member_count == 30
    assert batch_sizes == [25, 5]
    assert second.status == "READY"
    assert second.reused is True
    assert second.generation_id == first.generation_id
    assert store.get_active_feature_generation("RESEARCH") is None
    generation = store.get_feature_generation(str(first.generation_id))
    assert generation["purpose"] == "LIVE_SOURCE"
    assert generation["activation_eligible"] is False
    assert generation["validation_manifest"]["status"] == "READY"


def test_live_source_stale_market_date_fails_closed(tmp_path: Path) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")

    result = materialize_live_source(
        store,
        snapshot_id="snapshot-20260831T092600+0800-stale",
        snapshot_hash="b" * 64,
        as_of=datetime(2026, 8, 31, 9, 26, tzinfo=TZ),
        market_trade_date="2026-08-31",
        data=_data(trade_date="2026-08-28"),
        batch_size=25,
    )

    assert result.status == "BLOCKED_SOURCE_GENERATION"
    assert result.reason_code == "FEATURE_SOURCE_MARKET_DATA_STALE"
    generation = store.get_feature_generation(str(result.generation_id))
    assert generation["status"] == "FAILED"
    assert store.select_latest_live_source() is None


def test_live_source_accepts_shanghai_daily_date_ms(tmp_path: Path) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    data = _data(30, trade_date="2026-09-03")
    expected_ms = int(datetime(2026, 9, 3, 0, 0, tzinfo=TZ).timestamp() * 1000)
    for rows in data["RECENT_DAILY_BARS"].values():
        rows[0] = {"date_ms": expected_ms, "close": 10.0, "volume": 1000}

    result = materialize_live_source(
        store,
        snapshot_id="snapshot-20260903T151000+0800-date-ms",
        snapshot_hash="d" * 64,
        as_of=datetime(2026, 9, 3, 15, 10, tzinfo=TZ),
        market_trade_date="2026-09-03",
        data=data,
        batch_size=25,
    )

    assert result.status == "READY"
    generation = store.get_feature_generation(str(result.generation_id))
    freshness = generation["validation_manifest"]["namespace_freshness"]["RECENT_DAILY_BARS"]
    assert freshness["observed_date_counts"] == {"2026-09-03": 30}
    assert freshness["unexplained_stale_count"] == 0


def test_live_source_old_date_ms_still_fails_closed(tmp_path: Path) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    data = _data(30, trade_date="2026-09-03")
    old_ms = int(
        (datetime(2026, 9, 3, 0, 0, tzinfo=TZ) - timedelta(days=1)).timestamp()
        * 1000
    )
    for rows in data["RECENT_DAILY_BARS"].values():
        rows[0] = {"date_ms": old_ms, "close": 10.0, "volume": 1000}

    result = materialize_live_source(
        store,
        snapshot_id="snapshot-20260903T151000+0800-old-date-ms",
        snapshot_hash="e" * 64,
        as_of=datetime(2026, 9, 3, 15, 10, tzinfo=TZ),
        market_trade_date="2026-09-03",
        data=data,
        batch_size=25,
    )

    assert result.status == "BLOCKED_SOURCE_GENERATION"
    assert result.reason_code == "FEATURE_SOURCE_MARKET_DATA_STALE"


def test_live_source_rejects_unix_seconds_in_date_ms(tmp_path: Path) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    data = _data(30, trade_date="2026-09-03")
    seconds = int(datetime(2026, 9, 3, 0, 0, tzinfo=TZ).timestamp())
    for rows in data["RECENT_DAILY_BARS"].values():
        rows[0] = {"date_ms": seconds, "close": 10.0, "volume": 1000}

    result = materialize_live_source(
        store,
        snapshot_id="snapshot-20260903T151000+0800-seconds",
        snapshot_hash="f" * 64,
        as_of=datetime(2026, 9, 3, 15, 10, tzinfo=TZ),
        market_trade_date="2026-09-03",
        data=data,
        batch_size=25,
    )

    assert result.status == "BLOCKED_SOURCE_GENERATION"
    assert result.reason_code == "FEATURE_SOURCE_MARKET_DATA_STALE"


def test_suspended_symbol_can_explain_one_stale_daily_bar(tmp_path: Path) -> None:
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    data = _data(30)
    symbol = data["g0_symbols"][0]
    data["RECENT_DAILY_BARS"][symbol][0]["date"] = "2026-08-28"
    data["TRADABILITY_FLAGS"][symbol] = {
        "tradable": False,
        "exclusion_reasons": ["SUSPENDED"],
    }

    result = materialize_live_source(
        store,
        snapshot_id="snapshot-20260831T151000+0800-suspended",
        snapshot_hash="c" * 64,
        as_of=datetime(2026, 8, 31, 15, 10, tzinfo=TZ),
        market_trade_date="2026-08-31",
        data=data,
        batch_size=25,
    )

    assert result.status == "READY"
    generation = store.get_feature_generation(str(result.generation_id))
    freshness = generation["validation_manifest"]["namespace_freshness"]["RECENT_DAILY_BARS"]
    assert freshness["fresh_count"] == 29
    assert freshness["explained_stale_count"] == 1
    assert freshness["unexplained_stale_count"] == 0
