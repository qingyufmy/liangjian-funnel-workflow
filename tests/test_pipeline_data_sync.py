from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.data_sync import FINANCIAL_DATASETS, HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 26, 15, 10, tzinfo=TZ)


def _result(endpoint: str, rows: list[dict]) -> HithinkFetchResult:
    return HithinkFetchResult(
        endpoint=endpoint,
        ok=True,
        complete=True,
        reason_code="OK",
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        pages=1,
        total=len(rows),
        limit=1000,
        fetch_time=NOW,
    )


class FakeClient:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.history_kwargs: list[dict] = []

    def history_1d(self, symbol, **kwargs):
        self.calls.append(("DAILY", symbol))
        self.history_kwargs.append(kwargs)
        rows = []
        for index in range(31):
            point = NOW - timedelta(days=800 - index * 26)
            rows.append(
                {
                    "date_ms": int(point.timestamp() * 1000),
                    "open_price": 10,
                    "high_price": 11,
                    "low_price": 9,
                    "close_price": 10.5,
                    "volume": 1000,
                    "turnover": 10000,
                }
            )
        requested_end = datetime.fromtimestamp(kwargs["end"] / 1000, tz=TZ)
        rows[-1]["date_ms"] = int((requested_end - timedelta(days=1)).timestamp() * 1000)
        return _result("history", rows)

    def income_statements(self, symbol, **_kwargs):
        self.calls.append(("INCOME", symbol))
        return _result("income", [{"report_date_ms": int((NOW - timedelta(days=90)).timestamp() * 1000), "operating_income": 1}])

    def financial_indicators(self, symbol, **_kwargs):
        self.calls.append(("INDICATORS", symbol))
        return _result("indicators", [{"ability": "growth", "index_id": "roe", "value": 12}])

    def balance_sheets(self, symbol, **_kwargs):
        self.calls.append(("BALANCE", symbol))
        return _result("balance", [{"report_date_ms": int((NOW - timedelta(days=90)).timestamp() * 1000), "total_assets": 2}])

    def cash_flow_statements(self, symbol, **_kwargs):
        self.calls.append(("CASH_FLOW", symbol))
        return _result("cash", [{"report_date_ms": int((NOW - timedelta(days=90)).timestamp() * 1000), "net_cash_flow": 3}])


class MissingIndicatorsClient(FakeClient):
    def financial_indicators(self, symbol, **_kwargs):
        self.calls.append(("INDICATORS", symbol))
        return HithinkFetchResult(
            endpoint="indicators",
            ok=False,
            complete=False,
            reason_code="BUSINESS_ERROR",
            fetch_time=NOW,
        )


def test_incremental_sync_persists_each_symbol_and_warm_run_uses_cache(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    client = FakeClient()
    events = []
    sync = HithinkIncrementalSynchronizer(cache, fundamental_refresh_hours=24, progress_every=1)

    cold = sync.sync(client, ["600519.SH"], as_of=NOW, progress=events.append)
    assert cold.cache_misses == 1
    assert cold.failures == {}
    assert len(cold.daily["600519.SH"]) == 30
    assert {row["_dataset"] for row in cold.fundamental["600519.SH"]} == set(FINANCIAL_DATASETS)
    assert events[-1]["processed"] == 1
    assert cache.get_coverage(symbol="600519.SH")["daily"]["rows"] == 31
    assert len(client.calls) == 5
    assert cold.updated_symbols == ("600519.SH",)

    warm_client = FakeClient()
    warm = sync.sync(warm_client, ["600519.SH"], as_of=NOW)
    assert warm.cache_hits == 1
    assert warm.cache_misses == 0
    assert warm_client.calls == []
    assert warm.updated_symbols == ()


def test_early_discovery_scans_full_cache_before_thirty_bar_compaction(tmp_path):
    class RepairClient(FakeClient):
        def history_1d(self, symbol, **kwargs):
            self.calls.append(("DAILY", symbol))
            prices = [20.] * 45 + [20. - i * .5 for i in range(20)] + [11., 12., 14., 16.]
            end = NOW.replace(hour=0, minute=0)
            return _result("history", [{
                "date_ms": int((end - timedelta(days=len(prices) - i - 1)).timestamp() * 1000),
                "open_price": p, "high_price": p + .1, "low_price": p - .1,
                "close_price": p, "volume": 1000, "turnover": 10000,
            } for i, p in enumerate(prices)])

    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(cache)
    cold_client = RepairClient()
    cold = sync.sync(cold_client, ["603186.SH"], as_of=NOW, collect_early_discovery=True)
    assert len(cold.daily["603186.SH"]) == 30
    assert len(cold_client.calls) == 5
    assert cold.early_discovery["data_gaps"] == []
    lead = cold.early_discovery["records"][0]
    assert lead["signals"] == ["REPAIR_WATCH"]
    assert lead["evidence"]["bar_count"] == 69
    assert lead["evidence"]["adjust_mode"] == "none"
    warm_client = RepairClient()
    warm = sync.sync(warm_client, ["603186.SH"], as_of=NOW, collect_early_discovery=True)
    assert warm_client.calls == []
    assert warm.early_discovery == cold.early_discovery
    assert warm.daily == cold.daily

    radar_client = RepairClient()
    radar_cache = LocalFactCache(tmp_path / "radar.sqlite3")
    radar = HithinkIncrementalSynchronizer(radar_cache).sync(
        radar_client, ["603186.SH"], as_of=NOW, collect_early_discovery=True, include_financial=False,
    )
    assert radar_client.calls == [("DAILY", "603186.SH")]
    assert radar.fundamental == {} and radar.failures == {}
    assert radar.early_discovery == cold.early_discovery
    from liangjian_funnel.pipeline.early_discovery import scan_cached_universe
    assert scan_cached_universe(radar_cache, ["603186.SH"], as_of=NOW) == radar.early_discovery


def test_interrupted_bootstrap_resumes_completed_symbols(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(cache, progress_every=1)
    first = FakeClient()
    sync.sync(first, ["600519.SH"], as_of=NOW)

    resumed = FakeClient()
    result = sync.sync(resumed, ["600519.SH", "000001.SZ"], as_of=NOW)
    assert result.cache_hits == 1
    assert result.cache_misses == 1
    assert all(symbol == "000001.SZ" for _dataset, symbol in resumed.calls)


def test_core_statements_are_returned_when_indicators_are_missing(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(cache, progress_every=1)

    result = sync.sync(MissingIndicatorsClient(), ["600519.SH"], as_of=NOW)

    assert "600519.SH" in result.fundamental
    assert {row["_dataset"] for row in result.fundamental["600519.SH"]} == {
        "INCOME",
        "BALANCE",
        "CASH_FLOW",
    }
    assert "INDICATORS:BUSINESS_ERROR" in result.failures["600519.SH"]
    assert "INDICATORS:CACHE_EMPTY" in result.failures["600519.SH"]
    # Core statements were refreshed, so the entity is dirty even though an
    # optional enrichment dataset failed; a cache-only call is never dirty.
    assert result.updated_symbols == ("600519.SH",)


def test_full_market_sync_retains_only_projected_fundamentals(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(cache, progress_every=1)
    projected_row_counts: list[int] = []

    def project(rows: list[dict]) -> dict:
        projected_row_counts.append(len(rows))
        return {
            "datasets": sorted({row["_dataset"] for row in rows}),
            "row_count": len(rows),
        }

    result = sync.sync(
        FakeClient(),
        ["600519.SH", "000001.SZ"],
        as_of=NOW,
        fundamental_projector=project,
    )

    assert projected_row_counts == [4, 4]
    assert result.fundamental == {
        "600519.SH": {
            "datasets": ["BALANCE", "CASH_FLOW", "INCOME", "INDICATORS"],
            "row_count": 4,
        },
        "000001.SZ": {
            "datasets": ["BALANCE", "CASH_FLOW", "INCOME", "INDICATORS"],
            "row_count": 4,
        },
    }


def test_daily_cache_uses_closed_bar_watermark_instead_of_wall_clock_ttl(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(cache, daily_refresh_hours=4)
    sync.sync(FakeClient(), ["600519.SH"], as_of=NOW)

    next_morning = NOW.replace(hour=9, minute=25) + timedelta(days=1)
    morning = FakeClient()
    morning_result = sync.sync(morning, ["600519.SH"], as_of=next_morning)

    assert morning.calls == []
    assert all(
        datetime.fromtimestamp(row["date_ms"] / 1000, tz=TZ).date() < next_morning.date()
        for row in morning_result.daily["600519.SH"]
    )

    next_close = NOW + timedelta(days=1)
    refreshed = FakeClient()
    sync.sync(refreshed, ["600519.SH"], as_of=next_close)

    assert ("DAILY", "600519.SH") in refreshed.calls
    request_start = datetime.fromtimestamp(refreshed.history_kwargs[0]["start"] / 1000, tz=TZ)
    assert request_start >= NOW - timedelta(days=8)
    assert all(dataset == "DAILY" for dataset, _symbol in refreshed.calls)


def test_weekend_sync_requires_friday_instead_of_nonexistent_saturday_bar(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    friday_close = datetime(2026, 9, 18, 15, 10, tzinfo=TZ)
    saturday = datetime(2026, 9, 19, 20, 50, tzinfo=TZ)

    class FridayClient(FakeClient):
        def history_1d(self, symbol, **kwargs):
            self.calls.append(("DAILY", symbol))
            self.history_kwargs.append(kwargs)
            rows = []
            for index in range(31):
                point = friday_close.replace(hour=0, minute=0) - timedelta(days=30 - index)
                rows.append({
                    "date_ms": int(point.timestamp() * 1000),
                    "open_price": 10,
                    "high_price": 11,
                    "low_price": 9,
                    "close_price": 10.5,
                    "volume": 1000,
                    "turnover": 10000,
                })
            return _result("history", rows)

    client = FridayClient()
    result = HithinkIncrementalSynchronizer(cache).sync(
        client,
        ["600519.SH"],
        as_of=saturday,
        include_financial=False,
    )

    assert result.failures == {}
    assert datetime.fromtimestamp(
        result.daily["600519.SH"][-1]["date_ms"] / 1000,
        tz=TZ,
    ).date() == friday_close.date()
    requested_end = datetime.fromtimestamp(client.history_kwargs[0]["end"] / 1000, tz=TZ)
    assert requested_end == datetime(2026, 9, 19, 0, 0, tzinfo=TZ)


def test_trading_day_morning_requires_previous_closed_session(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    monday_morning = datetime(2026, 9, 21, 9, 20, tzinfo=TZ)

    class FridayClient(FakeClient):
        def history_1d(self, symbol, **kwargs):
            self.calls.append(("DAILY", symbol))
            self.history_kwargs.append(kwargs)
            friday = datetime(2026, 9, 18, 0, 0, tzinfo=TZ)
            rows = [{
                "date_ms": int((friday - timedelta(days=30 - index)).timestamp() * 1000),
                "open_price": 10,
                "high_price": 11,
                "low_price": 9,
                "close_price": 10.5,
                "volume": 1000,
                "turnover": 10000,
            } for index in range(31)]
            return _result("history", rows)

    result = HithinkIncrementalSynchronizer(cache).sync(
        FridayClient(),
        ["600519.SH"],
        as_of=monday_morning,
        include_financial=False,
    )

    assert result.failures == {}
    assert datetime.fromtimestamp(
        result.daily["600519.SH"][-1]["date_ms"] / 1000,
        tz=TZ,
    ).date().isoformat() == "2026-09-18"


def test_stale_fundamentals_rotate_oldest_without_reducing_research_coverage(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    initial = HithinkIncrementalSynchronizer(cache, progress_every=1)
    symbols = ["600519.SH", "000001.SZ", "300750.SZ"]
    initial.sync(FakeClient(), symbols, as_of=NOW)

    events = []
    client = FakeClient()
    rotating = HithinkIncrementalSynchronizer(
        cache,
        fundamental_refresh_hours=24,
        fundamental_refresh_symbols_per_run=1,
        progress_every=1,
    )
    result = rotating.sync(
        client,
        symbols,
        as_of=NOW + timedelta(hours=25),
        progress=events.append,
    )

    financial_calls = [(dataset, symbol) for dataset, symbol in client.calls if dataset != "DAILY"]
    assert len({symbol for _dataset, symbol in financial_calls}) == 1
    assert len(financial_calls) == len(FINANCIAL_DATASETS)
    assert {symbol for dataset, symbol in client.calls if dataset == "DAILY"} == set(symbols)
    assert set(result.fundamental) == set(symbols)
    assert result.financial_refreshes == 1
    assert result.deferred_financial_refreshes == 2
    assert result.daily_updates == 3
    assert events[-1]["financial_refreshes"] == 1
    assert events[-1]["deferred_financial_refreshes"] == 2


def test_missing_core_fundamentals_bypass_zero_rotation_budget(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    sync = HithinkIncrementalSynchronizer(
        cache,
        fundamental_refresh_symbols_per_run=0,
        progress_every=1,
    )

    result = sync.sync(FakeClient(), ["600519.SH"], as_of=NOW)

    assert "600519.SH" in result.fundamental
    assert result.financial_refreshes == 1
    assert result.deferred_financial_refreshes == 0


class DelayedDailyClient(FakeClient):
    def __init__(self, recover=True):
        super().__init__()
        self.recover = recover

    def history_1d(self, symbol, **kwargs):
        result = super().history_1d(symbol, **kwargs)
        calls = self.calls.count(("DAILY", symbol))
        if self.recover and calls > 1:
            return result
        rows = [row.model_dump(mode="python") for row in result.items]
        rows[-1]["date_ms"] -= 86400000
        return _result("history", rows)


def test_delayed_daily_bar_recovers_once_after_initial_pass(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    client = DelayedDailyClient()
    result = HithinkIncrementalSynchronizer(cache).sync(client, ["301565.SZ"], as_of=NOW)
    assert client.calls.count(("DAILY", "301565.SZ")) == 2
    assert result.failures == {}
    assert datetime.fromtimestamp(result.daily["301565.SZ"][-1]["date_ms"] / 1000, TZ).date() == NOW.date()
    assert cache.get_sync_state("HITHINK_DAILY_1D", "301565.SZ")["status"] == "READY"


def test_successful_but_stale_daily_response_remains_failed_and_retry_is_bounded(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    client = DelayedDailyClient(recover=False)
    symbols = [f"{301560 + i}.SZ" for i in range(12)]
    result = HithinkIncrementalSynchronizer(cache).sync(client, symbols, as_of=NOW)
    assert sum(kind == "DAILY" for kind, _ in client.calls) == 22
    for symbol in symbols:
        assert "DAILY:LATEST_CLOSED_DAY_MISSING" in result.failures[symbol]
        assert cache.get_sync_state("HITHINK_DAILY_1D", symbol)["status"] == "FAILED"
