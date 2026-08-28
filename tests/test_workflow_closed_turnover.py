from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.workflow import _market_snapshot_with_closed_turnover


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
        ]
    )
    market = HithinkFetchResult(
        endpoint="snapshot",
        ok=True,
        complete=True,
        reason_code="OK",
        items=(
            HithinkRow(thscode="600519.SH", amount=20_000_000, price=1400, volume=10),
            HithinkRow(thscode="000001.SZ", amount=30_000_000, price=10, volume=10),
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
    assert adjusted.items[1].model_dump()["amount"] == 30_000_000
    assert adjusted.metadata["turnover_metric"] == "LATEST_CLOSED_DAILY_BAR"
    assert adjusted.metadata["turnover_override_count"] == 1
