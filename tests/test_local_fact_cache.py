from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.local_fact_cache import (
    LocalFactCache,
    canonical_json,
    canonical_json_hash,
)


UTC = timezone.utc
T0 = datetime(2026, 8, 25, 8, 0, tzinfo=UTC)


def daily(symbol: str, minute: int, *, fetched_at: datetime = T0, close: float = 10.0):
    return {
        "symbol": symbol,
        "timestamp": datetime(2026, 8, 25, 9, minute, tzinfo=UTC),
        "adjust": "none",
        "fetched_at": fetched_at,
        "payload": {"close": close, "volume": 100, "nested": {"b": 2, "a": 1}},
    }


def financial(
    symbol: str,
    *,
    published_at: datetime,
    fetched_at: datetime,
    value: float,
    version: str = "1",
):
    return {
        "symbol": symbol,
        "dataset": "income",
        "report_period": "2025Q4",
        "published_at": published_at,
        "fetched_at": fetched_at,
        "version": version,
        "payload": {"net_profit": value},
    }


def test_schema_uses_wal_full_and_canonical_json_drops_credentials(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    with cache._connect() as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 3

    value = {
        "z": 1,
        "a": {"api_key": "sk-test-very-secret", "ok": "value"},
        "url": "https://example.test/x?token=should-not-persist",
    }
    encoded = canonical_json(value)
    assert encoded == '{"a":{"ok":"value"},"url":"https://example.test/x?token=[REDACTED]","z":1}'
    assert "sk-test-very-secret" not in encoded
    assert canonical_json({"b": 2, "a": 1}) == canonical_json({"a": 1, "b": 2})
    assert canonical_json_hash({"a": 1}) == canonical_json_hash({"a": 1})


def test_daily_upsert_is_idempotent_and_incremental_with_latest(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    first = daily("600519.SH", 31)
    second = daily("600519.SH", 32)

    assert cache.upsert_daily_bars([first, second]) == {
        "inserted": 2, "updated": 0, "unchanged": 0, "batches": 1
    }
    assert cache.upsert_daily_bars([first, second]) == {
        "inserted": 0, "updated": 0, "unchanged": 2, "batches": 1
    }
    rows = cache.incremental_daily_bars(
        "600519.SH", after="2026-08-25T09:31:00+00:00"
    )
    assert rows[0]["timestamp"].endswith("09:32:00.000000+00:00")
    assert cache.latest_daily_bar("600519.SH")["payload"]["close"] == 10.0

    corrected = daily(
        "600519.SH", 32,
        fetched_at=datetime(2026, 8, 26, tzinfo=UTC),
        close=10.5,
    )
    assert cache.upsert_daily_bars([corrected])["inserted"] == 1
    assert cache.latest_daily_bar("600519.SH")["payload"]["close"] == 10.5


def test_daily_revision_is_retained_and_as_of_selects_one_revision(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    original = daily(
        "600519.SH", 31, fetched_at=datetime(2026, 8, 25, 10, tzinfo=UTC), close=10
    )
    corrected = daily(
        "600519.SH", 31, fetched_at=datetime(2026, 8, 26, 10, tzinfo=UTC), close=11
    )
    assert cache.upsert_daily_bars([original, corrected])["inserted"] == 2
    assert len(cache.query_daily_bars("600519.SH")) == 1
    assert cache.latest_daily_bar("600519.SH")["payload"]["close"] == 11
    replay = cache.query_daily_bars(
        "600519.SH", as_of="2026-08-25T23:59:59+00:00"
    )
    assert len(replay) == 1
    assert replay[0]["payload"]["close"] == 10


def test_daily_as_of_excludes_later_fetched_observation(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    cache.upsert_daily_bars(
        [
            daily("600519.SH", 31, fetched_at=T0),
            daily("600519.SH", 32, fetched_at=datetime(2026, 8, 26, tzinfo=UTC)),
        ]
    )
    rows = cache.query_daily_bars("600519.SH", as_of="2026-08-25T23:59:59+00:00")
    assert len(rows) == 1
    assert rows[0]["timestamp"].endswith("09:31:00.000000+00:00")


def test_latest_daily_bars_before_returns_closed_watermark_in_batches(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    cache.upsert_daily_bars(
        [
            daily("600519.SH", 31, close=10),
            daily("600519.SH", 32, close=11),
            daily("000001.SZ", 31, close=12),
        ]
    )

    rows = cache.latest_daily_bars_before(
        ["600519.SH", "000001.SZ", "600519.SH"],
        end="2026-08-25T09:32:00+00:00",
        batch_size=1,
    )

    assert set(rows) == {"600519.SH", "000001.SZ"}
    assert rows["600519.SH"]["payload"]["close"] == 10
    assert rows["000001.SZ"]["payload"]["close"] == 12


def test_financial_revisions_are_retained_and_as_of_is_strict(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    original = financial(
        "600519.SH",
        published_at=datetime(2026, 4, 1, tzinfo=UTC),
        fetched_at=datetime(2026, 4, 2, tzinfo=UTC),
        value=100,
    )
    revision = financial(
        "600519.SH",
        published_at=datetime(2026, 4, 10, tzinfo=UTC),
        fetched_at=datetime(2026, 4, 11, tzinfo=UTC),
        value=110,
        version="2",
    )
    assert cache.upsert_financial_facts([original, revision])["inserted"] == 2
    assert cache.upsert_financial_facts([original])["unchanged"] == 1
    assert len(cache.query_financial_facts("600519.SH", dataset="income")) == 2
    assert cache.latest_financial_fact("600519.SH", "income")["payload"]["net_profit"] == 110
    historical = cache.query_financial_facts(
        "600519.SH", dataset="income", as_of="2026-04-05T00:00:00+00:00"
    )
    assert len(historical) == 1
    assert historical[0]["payload"]["net_profit"] == 100
    assert cache.query_financial_facts(
        "600519.SH", dataset="income", as_of="2026-04-12T00:00:00+00:00"
    )[-1]["payload"]["net_profit"] == 110


def test_sync_state_cursor_is_durable_and_omitted_values_are_retained(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    state = cache.update_sync_state(
        "/api/a-share/daily?api_key=sk-state-secret",
        "600519.SH",
        last_success=T0,
        cursor={"page": 3, "next": ["x", 1]},
        status="ok",
        reason="provider token=should-not-persist",
    )
    assert state["endpoint"] == "/api/a-share/daily?api_key=[REDACTED]"
    assert state["reason"] == "provider token=[REDACTED]"
    assert state["cursor"] == {"next": ["x", 1], "page": 3}
    assert cache.update_sync_state(
        "/api/a-share/daily?api_key=sk-state-secret", "600519.SH", status="partial"
    )["cursor"] == {"next": ["x", 1], "page": 3}
    assert cache.get_sync_state(
        "/api/a-share/daily?api_key=sk-state-secret", "600519.SH"
    )["status"] == "partial"
    assert cache.list_sync_state(
        endpoint="/api/a-share/daily?api_key=sk-state-secret"
    )[0]["symbol"] == "600519.SH"


def test_invalid_row_rolls_back_current_batch_and_later_batches_resume(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    valid = [daily("600519.SH", 31), daily("600519.SH", 32)]
    invalid = daily("600519.SH", 33)
    invalid["timestamp"] = "not-a-timestamp"
    with pytest.raises(ValueError, match="invalid timestamp"):
        cache.upsert_daily_bars([*valid, invalid], batch_size=2)
    # The first committed batch remains durable; the malformed second batch
    # has no partial row.
    assert len(cache.query_daily_bars("600519.SH")) == 2
    assert cache.upsert_daily_bars([daily("600519.SH", 33)], batch_size=2)["inserted"] == 1

    invalid_same_batch = daily("600519.SH", 34)
    invalid_same_batch["timestamp"] = "bad"
    with pytest.raises(ValueError):
        cache.upsert_daily_bars([daily("600519.SH", 35), invalid_same_batch], batch_size=2)
    assert len(cache.query_daily_bars("600519.SH")) == 3


def test_short_connections_can_write_concurrently(tmp_path: Path):
    path = tmp_path / "facts.sqlite3"

    def write(index: int):
        cache = LocalFactCache(path)
        return cache.upsert_daily_bars([daily(f"600{index:03d}.SH", 31)])

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(write, range(1, 13)))
    assert sum(result["inserted"] for result in results) == 12
    assert LocalFactCache(path).get_coverage()["daily"]["rows"] == 12


def test_cached_provider_result_is_versioned_redacted_and_freshness_checked(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    first = cache.put_cached_result(
        "cninfo",
        "600519.SH:recent",
        {"ok": True, "api_key": "sk-do-not-store", "items": [1]},
        fetched_at="2026-08-25T08:00:00+00:00",
        expires_at="2026-08-26T08:00:00+00:00",
    )
    assert first["payload"] == {"items": [1], "ok": True}
    assert cache.get_cached_result(
        "cninfo",
        "600519.SH:recent",
        fresh_at="2026-08-26T07:59:59+00:00",
    )["payload"] == {"items": [1], "ok": True}
    assert cache.get_cached_result(
        "cninfo",
        "600519.SH:recent",
        fresh_at="2026-08-26T08:00:01+00:00",
    ) is None

    cache.put_cached_result(
        "cninfo",
        "600519.SH:recent",
        {"ok": True, "items": [2]},
        fetched_at="2026-08-26T08:00:00+00:00",
        expires_at="2026-08-27T08:00:00+00:00",
    )
    historical = cache.get_cached_result(
        "cninfo",
        "600519.SH:recent",
        as_of="2026-08-25T23:59:59+00:00",
    )
    assert historical["payload"]["items"] == [1]


def test_cached_provider_results_batch_selects_latest_valid_revision_and_chunks(tmp_path: Path):
    cache = LocalFactCache(tmp_path)
    cache.put_cached_result(
        "cninfo",
        "a",
        {"value": "old"},
        fetched_at="2026-08-25T08:00:00+00:00",
        expires_at="2026-08-27T08:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "a",
        {"value": "new-but-expired"},
        fetched_at="2026-08-26T08:00:00+00:00",
        expires_at="2026-08-26T12:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "b",
        {"value": "b"},
        fetched_at="2026-08-26T09:00:00+00:00",
        expires_at="2026-08-27T09:00:00+00:00",
    )

    result = cache.get_cached_results(
        "cninfo",
        ["a", "b", "a", "missing"],
        fresh_at="2026-08-26T13:00:00+00:00",
        chunk_size=1,
    )
    assert list(result) == ["a", "b"]
    assert result["a"]["payload"] == {"value": "old"}
    assert result["b"]["payload"] == {"value": "b"}

    historical = cache.get_cached_results(
        "cninfo",
        ["a", "b"],
        as_of="2026-08-25T12:00:00+00:00",
        chunk_size=1,
    )
    assert historical["a"]["payload"] == {"value": "old"}
    assert "b" not in historical


def test_iter_cached_results_preserves_order_deduplicates_and_applies_revision_filters(
    tmp_path: Path,
):
    cache = LocalFactCache(tmp_path)
    cache.put_cached_result(
        "cninfo",
        "a",
        {"value": "old"},
        fetched_at="2026-08-25T08:00:00+00:00",
        expires_at="2026-08-27T08:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "a",
        {"value": "new-but-expired"},
        fetched_at="2026-08-26T08:00:00+00:00",
        expires_at="2026-08-26T12:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "b",
        {"value": "b"},
        fetched_at="2026-08-26T09:00:00+00:00",
        expires_at="2026-08-27T09:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "c",
        {"value": "historical"},
        fetched_at="2026-08-25T09:00:00+00:00",
        expires_at="2026-08-30T09:00:00+00:00",
    )
    cache.put_cached_result(
        "cninfo",
        "c",
        {"value": "future"},
        fetched_at="2026-08-27T09:00:00+00:00",
        expires_at="2026-08-30T09:00:00+00:00",
    )

    rows = list(
        cache.iter_cached_results(
            "cninfo",
            ["b", "a", "b", "missing", "c", "a"],
            fresh_at="2026-08-26T13:00:00+00:00",
            as_of="2026-08-26T23:59:59+00:00",
            chunk_size=2,
        )
    )

    assert [key for key, _ in rows] == ["b", "a", "c"]
    assert [record["payload"] for _, record in rows] == [
        {"value": "b"},
        {"value": "old"},
        {"value": "historical"},
    ]


def test_iter_cached_results_reads_in_bounded_chunks_without_materializing_all_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    cache = LocalFactCache(tmp_path)
    calls: list[tuple[list[str], int]] = []

    def fake_get_cached_results(
        namespace: str,
        cache_keys: list[str],
        *,
        as_of=None,
        fresh_at=None,
        chunk_size: int,
    ):
        assert namespace == "test"
        assert as_of is None
        assert fresh_at is None
        calls.append((list(cache_keys), chunk_size))
        return {
            key: {
                "namespace": namespace,
                "cache_key": key,
                "payload": {"value": key},
            }
            for key in cache_keys
        }

    monkeypatch.setattr(cache, "get_cached_results", fake_get_cached_results)
    source = (f"k{index}" for index in range(2_501))
    rows = list(cache.iter_cached_results("test", source, chunk_size=100))

    assert [key for key, _ in rows] == [f"k{index}" for index in range(2_501)]
    assert len(calls) == 26
    assert all(1 <= len(keys) <= 100 for keys, _ in calls)
    assert all(query_chunk_size == 100 for _, query_chunk_size in calls)
    assert sum(len(keys) for keys, _ in calls) == 2_501


@pytest.mark.parametrize("invalid", [0, -1, True, False, 1.5, "100"])
def test_iter_cached_results_requires_strictly_positive_integer_chunk_size(
    tmp_path: Path, invalid,
):
    cache = LocalFactCache(tmp_path)
    with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
        cache.iter_cached_results("test", [], chunk_size=invalid)
