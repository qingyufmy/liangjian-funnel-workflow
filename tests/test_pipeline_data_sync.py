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
