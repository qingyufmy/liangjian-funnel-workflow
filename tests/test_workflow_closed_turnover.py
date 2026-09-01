from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.pipeline.snapshot import UniverseGatePolicy, UniverseSnapshot
from liangjian_funnel.workflow import (
    _latest_closed_market_trade_date,
    _market_snapshot_with_closed_turnover,
)


TZ = ZoneInfo("Asia/Shanghai")


def test_intraday_market_snapshot_uses_latest_closed_daily_turnover(tmp_path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    cache.upsert_daily_bars(
        [
            {
                "symbol": "600519.SH",
                "timestamp": datetime(2026, 8, 27, tzinfo=TZ),
                "adjust": "none",
                "fetched_at": datetime(2026, 8, 27, 15, 10, tzinfo=TZ),
                "payload": {"turnover": 320_000_000},
            },
            {
                "symbol": "600519.SH",
                "timestamp": datetime(2026, 8, 28, tzinfo=TZ),
                "adjust": "none",
                "fetched_at": datetime(2026, 8, 28, 10, 30, tzinfo=TZ),
                "payload": {"turnover": 20_000_000},
            },
            {
                "symbol": "000001.SZ",
                "timestamp": datetime(2026, 8, 27, tzinfo=TZ),
                "adjust": "none",
                "fetched_at": datetime(2026, 8, 27, 15, 10, tzinfo=TZ),
                "payload": {
                    "close_price": 10.5,
                    "volume": 8_000_000,
                    "turnover": 84_000_000,
                },
            },
        ]
    )
    market = HithinkFetchResult(
        endpoint="snapshot",
        ok=True,
        complete=True,
        reason_code="OK",
        items=(
            HithinkRow(thscode="600519.SH", amount=20_000_000, price=1400, volume=10),
            HithinkRow(thscode="000001.SZ", amount=0, price=0, volume=0),
            HithinkRow(thscode="000002.SZ", amount=30_000_000, price=0, volume=0),
        ),
        pages=1,
        fetch_time=datetime(2026, 8, 28, 10, 30, tzinfo=TZ),
    )

    adjusted = _market_snapshot_with_closed_turnover(
        market,
        cache=cache,
        cutoff=datetime(2026, 8, 28, tzinfo=TZ),
    )

    assert adjusted.items[0].model_dump()["amount"] == 320_000_000
    assert adjusted.items[0].model_dump()["price"] == 1400
    assert adjusted.items[0].model_dump()["volume"] == 10
    assert adjusted.items[1].model_dump()["amount"] == 84_000_000
    assert adjusted.items[1].model_dump()["price"] == 10.5
    assert adjusted.items[1].model_dump()["volume"] == 8_000_000
    assert adjusted.items[2].model_dump()["price"] == 0
    assert adjusted.metadata["turnover_metric"] == "LATEST_CLOSED_DAILY_BAR"
    assert adjusted.metadata["turnover_override_count"] == 2
    assert adjusted.metadata["preopen_price_override_count"] == 1
    assert adjusted.metadata["preopen_volume_override_count"] == 1

    catalog = HithinkFetchResult(
        endpoint="catalog",
        ok=True,
        complete=True,
        reason_code="OK",
        items=(
            HithinkRow(thscode="600519.SH", name="贵州茅台"),
            HithinkRow(thscode="000001.SZ", name="平安银行"),
            HithinkRow(thscode="000002.SZ", name="万科A"),
        ),
        pages=1,
        fetch_time=datetime(2026, 8, 28, 10, 30, tzinfo=TZ),
    )
    universe = UniverseSnapshot.from_records(
        catalog,
        adjusted,
        as_of=datetime(2026, 8, 28, 10, 30, tzinfo=TZ),
        gate_policy=UniverseGatePolicy(
            minimum_daily_turnover_cny=50_000_000,
            block_suspended=True,
        ),
    )

    assert universe.ready is True
    assert {item.symbol for item in universe.trade_candidates} == {"000001.SZ", "600519.SH"}
    assert universe.lineage.excluded_by_reason["INVALID_PRICE"] == 1


class _Calendar:
    def is_trading_day(self, value):
        return value.weekday() < 5

    def previous_trading_day(self, value):
        candidate = value
        while True:
            candidate = candidate.fromordinal(candidate.toordinal() - 1)
            if self.is_trading_day(candidate):
                return candidate


def test_latest_closed_market_trade_date_uses_previous_session_before_close():
    calendar = _Calendar()

    assert _latest_closed_market_trade_date(
        datetime(2026, 9, 1, 9, 20, tzinfo=TZ),
        calendar,
    ).isoformat() == "2026-08-31"
    assert _latest_closed_market_trade_date(
        datetime(2026, 9, 1, 15, 10, tzinfo=TZ),
        calendar,
    ).isoformat() == "2026-09-01"
    assert _latest_closed_market_trade_date(
        datetime(2026, 8, 30, 10, 0, tzinfo=TZ),
        calendar,
    ).isoformat() == "2026-08-28"
