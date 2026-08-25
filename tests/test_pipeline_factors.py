from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.factors import FactorEngine


TZ = ZoneInfo("Asia/Shanghai")


def daily_rows(count: int = 6) -> list[dict[str, object]]:
    return [
        {
            "symbol": "600519.SH",
            "date": (datetime(2026, 1, 1, tzinfo=TZ) + timedelta(days=index)).date().isoformat(),
            "open": 10 + index,
            "high": 11 + index,
            "low": 9 + index,
            "close": 10.5 + index,
            "volume": 100,
            "amount": 1050,
        }
        for index in range(count)
    ]


def minute_row(at: datetime, *, interval: str = "5m", close: float = 10.0) -> dict[str, object]:
    return {
        "symbol": "600519.SH",
        "interval": interval,
        "bar_end": at,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": 10,
        "amount": close * 10,
    }


def test_closed_bar_filter_and_ma_shortage_are_explicit():
    cutoff = datetime(2026, 1, 6, 15, tzinfo=TZ)
    rows = daily_rows(6)
    rows[-1]["date"] = "2026-01-07"
    result = FactorEngine("600519.SH").compute(daily_bars=rows, as_of=cutoff)
    daily = result.timeframes["daily"]
    assert len(daily.bars) == 5
    assert daily.moving_averages["ma5"] is not None
    assert daily.moving_averages["ma20"] is None
    assert "INSUFFICIENT_MA20" in daily.reasons
    assert not result.ready


def test_120m_aggregation_splits_lunch_and_never_spans_break():
    morning = [minute_row(datetime(2026, 8, 24, 9, 35, tzinfo=TZ))]
    morning += [minute_row(datetime(2026, 8, 24, 10, 0, tzinfo=TZ))]
    afternoon = [minute_row(datetime(2026, 8, 24, 13, 5, tzinfo=TZ))]
    afternoon += [minute_row(datetime(2026, 8, 24, 14, 0, tzinfo=TZ))]
    result = FactorEngine("600519.SH").compute(minute_bars=morning + afternoon, as_of=datetime(2026, 8, 24, 15, tzinfo=TZ))
    bars = result.timeframes["120m"].bars
    assert len(bars) == 2
    assert [bar.end.strftime("%H:%M") for bar in bars] == ["11:30", "15:00"]
    assert bars[0].end < bars[1].start


def test_unclosed_minute_bar_is_not_used_in_5m():
    closed = minute_row(datetime(2026, 8, 24, 9, 35, tzinfo=TZ))
    open_bar = minute_row(datetime(2026, 8, 24, 9, 40, tzinfo=TZ))
    open_bar["closed"] = False
    result = FactorEngine("600519.SH").compute(
        minute_bars=[closed, open_bar],
        as_of=datetime(2026, 8, 24, 15, tzinfo=TZ),
    )
    assert [bar.end.strftime("%H:%M") for bar in result.timeframes["5m"].bars] == ["09:35"]


def test_ma255_is_none_when_history_is_short():
    rows = [minute_row(datetime(2026, 1, 1, 9, 35, tzinfo=TZ) + timedelta(days=index)) for index in range(10)]
    result = FactorEngine("600519.SH").compute(minute_bars=rows, as_of=datetime(2026, 8, 24, 15, tzinfo=TZ))
    frame = result.timeframes["5m"]
    assert frame.moving_averages["ma255"] is None
    assert "INSUFFICIENT_MA255" in frame.reasons
    assert not result.ready


def test_ma_alignment_event_and_bias_are_deterministic_closed_bar_factors():
    rows = daily_rows(80)
    result = FactorEngine("600519.SH").compute(
        daily_bars=rows,
        as_of=datetime(2026, 8, 24, 15, tzinfo=TZ),
    )
    daily = result.timeframes["daily"]
    assert daily.ma_alignment == "BULL_STACK"
    assert daily.ma_event in {
        "NONE", "PULLBACK_HOLD_MA20", "GOLDEN_CROSS_SHORT", "GOLDEN_CROSS_MID",
        "RECLAIM_MA99", "RECLAIM_MA128", "RECLAIM_MA255",
    }
    assert daily.ma_bias["close_vs_ma20_pct"] is not None
    assert daily.ma_bias["close_vs_ma20_pct"] > 0
    assert result.technical_summary["timeframes"]["daily"]["ma_alignment"] == "BULL_STACK"
