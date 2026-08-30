from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import FetchResult
from liangjian_funnel.data.tencent_minute import ResilientIntradayAdapter, TencentIntradayAdapter


TZ = ZoneInfo("Asia/Shanghai")


def _payload() -> dict:
    return {
        "data": {
            "sz000859": {
                "m1": [
                    ["202608280930", "8.90", "8.90", "8.90", "8.90", "2438", {}, "2.72"],
                    ["202608280931", "8.91", "8.90", "8.91", "8.85", "7955", {}, "8.88"],
                    ["202608280932", "8.89", "8.94", "8.94", "8.87", "7811", {}, "8.72"],
                ]
            }
        }
    }


def _quote_raw(stamp: str = "20260828092615") -> str:
    fields = [""] * 38
    fields[1] = "国风新材"
    fields[2] = "000859"
    fields[3] = "8.90"
    fields[4] = "8.89"
    fields[5] = "8.90"
    fields[6] = "2438"
    fields[30] = stamp
    fields[37] = "2169820"
    return f'v_sz000859="{"~".join(fields)}";'


def test_tencent_minute_normalizes_closed_rows_and_excludes_auction() -> None:
    adapter = TencentIntradayAdapter(json_fetcher=lambda *_args: _payload())
    result = adapter.fetch_bars(
        "000859.SZ",
        "1m",
        2,
        as_of=datetime(2026, 8, 28, 9, 32, tzinfo=TZ),
    )
    assert result.complete is True
    assert [bar.bar_end.strftime("%H:%M") for bar in result.bars] == ["09:31", "09:32"]
    assert all(bar.source_id.startswith("TENCENT:") for bar in result.bars)
    assert result.bars[-1].high == 8.94


def test_tencent_quote_requires_same_day_fresh_positive_auction_volume() -> None:
    adapter = TencentIntradayAdapter(text_fetcher=lambda *_args: _quote_raw())
    result = adapter.fetch_quote(
        "000859.SZ",
        as_of=datetime(2026, 8, 28, 9, 26, tzinfo=TZ),
    )
    assert result.complete is True
    assert result.quote is not None
    assert result.quote.price == 8.90
    stale = adapter.fetch_quote(
        "000859.SZ",
        as_of=datetime(2026, 8, 31, 9, 26, tzinfo=TZ),
    )
    assert stale.complete is False
    assert stale.reason_code == "QUOTE_TRADE_DATE_MISMATCH"


def test_resilient_adapter_uses_tencent_only_for_bounded_intraday_failure() -> None:
    class Primary:
        def fetch_bars(self, symbol, interval, required_bars, *, as_of=None):
            return FetchResult(
                symbol=symbol,
                interval=interval,
                requested_bars=required_bars,
                returned_bars=0,
                reason_code="NODE_REQUEST_FAILED",
            )

    fallback = TencentIntradayAdapter(json_fetcher=lambda *_args: _payload())
    adapter = ResilientIntradayAdapter(Primary(), fallback)  # type: ignore[arg-type]
    result = adapter.fetch_bars(
        "000859.SZ",
        "1m",
        2,
        as_of=datetime(2026, 8, 28, 9, 32, tzinfo=TZ),
    )
    assert result.complete is True
    long_history = adapter.fetch_bars(
        "000859.SZ",
        "5m",
        12_240,
        as_of=datetime(2026, 8, 28, 15, 0, tzinfo=TZ),
    )
    assert long_history.reason_code == "NODE_REQUEST_FAILED"
