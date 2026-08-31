from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from liangjian_funnel.pipeline.factors import FactorEngine, OHLCVBar, TechnicalFactorSnapshot, TimeframeFactors
from liangjian_funnel.pipeline.technical_aggregates import build_technical_aggregates


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 25, 15, 0, tzinfo=TZ)


def _daily_bar(index: int, *, open_: float | None = None, close: float | None = None, high: float | None = None, low: float | None = None) -> dict[str, object]:
    close_value = close if close is not None else 100.0 + index
    open_value = open_ if open_ is not None else close_value - 0.5
    return {
        "symbol": "600519.SH",
        "date": (datetime(2026, 1, 1, tzinfo=TZ) + timedelta(days=index)).date().isoformat(),
        "open": open_value,
        "high": high if high is not None else max(open_value, close_value) + 1.0,
        "low": low if low is not None else min(open_value, close_value) - 1.0,
        "close": close_value,
        "volume": 100.0,
        "amount": 10000.0,
    }


def _snapshot(*, daily: list[dict[str, object]], minute: list[dict[str, object]] | None = None, as_of: datetime = AS_OF) -> TechnicalFactorSnapshot:
    return FactorEngine("600519.SH").compute(daily_bars=daily, minute_bars=minute or [], as_of=as_of)


def _direct_snapshot(*, daily: tuple[OHLCVBar, ...], minute5: tuple[OHLCVBar, ...] = (), as_of: datetime = AS_OF) -> TechnicalFactorSnapshot:
    daily_frame = TimeframeFactors(timeframe="daily", bars=daily, latest=daily[-1] if daily else None)
    minute_frame = TimeframeFactors(timeframe="5m", bars=minute5, latest=minute5[-1] if minute5 else None)
    return TechnicalFactorSnapshot(
        symbol="600519.SH",
        as_of=as_of,
        timeframes={"daily": daily_frame, "5m": minute_frame},
    )


def test_doji_and_bullish_engulfing_are_deterministic_labels() -> None:
    rows = [_daily_bar(index) for index in range(20)]
    rows[-2] = _daily_bar(18, open_=103.0, close=100.0, high=104.0, low=99.0)
    rows[-1] = _daily_bar(19, open_=99.0, close=105.0, high=106.0, low=98.5)
    result = build_technical_aggregates(_snapshot(daily=rows))
    kline = result["KLINE_PATTERNS"]
    assert kline["available"] is True
    assert kline["direction"] == "BULLISH"
    assert "BULLISH_ENGULFING" in kline["labels"]

    doji = _daily_bar(19, open_=100.0, close=100.05, high=101.0, low=99.0)
    doji_result = build_technical_aggregates(_snapshot(daily=rows[:-1] + [doji]))
    assert "DOJI" in doji_result["KLINE_PATTERNS"]["labels"]


def test_breakout_20_excludes_current_bar_from_reference_window() -> None:
    rows = [_daily_bar(index, close=100.0, high=101.0, low=99.0) for index in range(21)]
    rows[-1] = _daily_bar(20, open_=100.0, close=102.0, high=102.5, low=99.5)
    kline = build_technical_aggregates(_snapshot(daily=rows))["KLINE_PATTERNS"]
    assert kline["breakout_20"]["available"] is True
    assert kline["breakout_20"]["up"] is True
    assert kline["breakout_20"]["reference_high"] == 101.0


def test_future_bar_fails_both_outputs_closed() -> None:
    rows = (
        OHLCVBar(
            symbol="600519.SH",
            timeframe="daily",
            start=datetime(2026, 8, 24, 9, 30, tzinfo=TZ),
            end=datetime(2026, 8, 26, 15, tzinfo=TZ),
            open=100,
            high=101,
            low=99,
            close=100,
        ),
    )
    result = build_technical_aggregates(_direct_snapshot(daily=rows))
    assert result["KLINE_PATTERNS"]["available"] is False
    assert result["KLINE_PATTERNS"]["reason_code"] == "FUTURE_BAR_DETECTED"
    assert result["PRICE_LEVELS"]["available"] is False
    assert result["PRICE_LEVELS"]["reason_code"] == "FUTURE_BAR_DETECTED"


def test_sample_shortage_is_explicit_and_never_zero_filled() -> None:
    rows = [_daily_bar(index) for index in range(3)]
    result = build_technical_aggregates(_snapshot(daily=rows))
    kline = result["KLINE_PATTERNS"]
    price = result["PRICE_LEVELS"]
    assert kline["available"] is True
    assert kline["breakout_20"]["available"] is False
    assert kline["breakout_20"]["up"] is None
    assert price["rolling_20_high"] is None
    assert price["ma255"] is None
    assert price["atr14"] is None
    assert "ma255" in price["missing_fields"]
    assert all(value != 0 for value in (price["rolling_20_high"], price["ma255"], price["atr14"]) if value is not None)


def test_daily_less_than_two_makes_kline_unavailable() -> None:
    result = build_technical_aggregates(_snapshot(daily=[_daily_bar(0)]))
    kline = result["KLINE_PATTERNS"]
    assert kline["available"] is False
    assert kline["reason_code"] == "INSUFFICIENT_DAILY_BARS"
    assert kline["latest_bar"] is None


def test_price_levels_include_daily_rolling_values_atr_and_latest_5m() -> None:
    rows = [_daily_bar(index, close=100.0 + index, high=101.0 + index, low=99.0 + index) for index in range(255)]
    minute = [
        {
            "symbol": "600519.SH",
            "interval": "5m",
            "bar_end": datetime(2026, 8, 25, 14, 55, tzinfo=TZ),
            "open": 120,
            "high": 123,
            "low": 119,
            "close": 122,
            "volume": 1,
            "amount": 122,
        },
    ]
    price = build_technical_aggregates(
        _snapshot(daily=rows, minute=minute, as_of=datetime(2027, 1, 1, 15, tzinfo=TZ))
    )["PRICE_LEVELS"]
    assert price["available"] is True
    assert price["ma20"] is not None
    assert price["ma60"] is not None
    assert price["ma255"] is not None
    assert price["atr14"] == 2.0
    assert price["recent_5m_high"] == 123.0
    assert price["recent_5m_low"] == 119.0
    assert price["source_bar_time"] is not None
    assert "trigger_zone" in price
    assert "planning_constraints" in price


def test_same_snapshot_has_stable_json_projection() -> None:
    snapshot = _snapshot(daily=[_daily_bar(index) for index in range(25)])
    first = json.dumps(build_technical_aggregates(snapshot), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    second = json.dumps(build_technical_aggregates(snapshot), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert first == second


def test_price_constraints_use_effective_regime_thresholds() -> None:
    snapshot = _snapshot(daily=[_daily_bar(index) for index in range(25)])
    price = build_technical_aggregates(
        snapshot,
        minimum_reward_risk=2.5,
        max_stop_distance_pct=0.04,
    )["PRICE_LEVELS"]

    assert price["planning_constraints"]["minimum_reward_risk"] == 2.5
    assert price["planning_constraints"]["max_stop_distance_pct"] == 0.04


def test_malformed_ohlc_geometry_is_rejected_before_aggregation() -> None:
    with pytest.raises(ValidationError, match="OHLC geometry"):
        OHLCVBar(
            symbol="600519.SH",
            timeframe="daily",
            start=datetime(2026, 8, 25, 9, 30, tzinfo=TZ),
            end=datetime(2026, 8, 25, 15, 0, tzinfo=TZ),
            open=100,
            high=99,
            low=98,
            close=101,
        )
