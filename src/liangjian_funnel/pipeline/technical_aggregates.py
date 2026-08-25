"""Deterministic K-line and price-level projections from closed factor bars.

This module deliberately does not fetch data or infer missing bars.  It is a
small, pure projection over :class:`TechnicalFactorSnapshot`, which means the
same frozen input always produces the same JSON-ready result.  The two
projections are kept separate so a missing minute feed cannot silently turn a
daily K-line result into a price-level claim (or vice versa).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from .factors import OHLCVBar, TechnicalFactorSnapshot


SOURCE = "DETERMINISTIC_CLOSED_BARS"
_KLINE_MISSING_FIELDS = (
    "latest_bar",
    "body",
    "range",
    "body_ratio",
    "upper_shadow_ratio",
    "lower_shadow_ratio",
    "direction",
    "labels",
    "breakout_20",
)
_PRICE_FIELDS = (
    "latest_close",
    "previous_day_high",
    "previous_day_low",
    "rolling_20_high",
    "rolling_20_low",
    "rolling_60_high",
    "rolling_60_low",
    "ma20",
    "ma60",
    "ma255",
    "atr14",
    "recent_5m_high",
    "recent_5m_low",
    "trigger_zone",
    "invalidation",
    "stop_distance_pct",
    "first_resistance",
    "reward_risk",
    "max_chase_price",
    "source_bar_time",
)


def build_technical_aggregates(snapshot: TechnicalFactorSnapshot) -> dict[str, Any]:
    """Build the deterministic ``KLINE_PATTERNS`` and ``PRICE_LEVELS`` maps.

    ``TechnicalFactorSnapshot`` normally contains only bars at or before
    ``snapshot.as_of`` because ``FactorEngine`` applies that cutoff.  We still
    validate the frozen object here.  A future bar is a data-integrity error,
    not a bar to drop: both projections fail closed with
    ``FUTURE_BAR_DETECTED``.
    """

    future = _future_bar(snapshot)
    return {
        "KLINE_PATTERNS": _build_kline(snapshot, future=future),
        "PRICE_LEVELS": _build_price_levels(snapshot, future=future),
    }


def build_kline_patterns(snapshot: TechnicalFactorSnapshot) -> dict[str, Any]:
    """Build only the daily closed-bar K-line projection."""

    return _build_kline(snapshot, future=_future_bar(snapshot))


def build_price_levels(snapshot: TechnicalFactorSnapshot) -> dict[str, Any]:
    """Build only the daily/5-minute closed-bar price-level projection."""

    return _build_price_levels(snapshot, future=_future_bar(snapshot))


def _build_kline(snapshot: TechnicalFactorSnapshot, *, future: OHLCVBar | None) -> dict[str, Any]:
    base = _base(snapshot, available=False, reason_code="INSUFFICIENT_DAILY_BARS")
    base.update({
        "latest_bar": None,
        "body": None,
        "range": None,
        "body_ratio": None,
        "upper_shadow_ratio": None,
        "lower_shadow_ratio": None,
        "direction": None,
        "labels": [],
        "breakout_20": _breakout_map(None, None, None, available=False),
        "breakout_20_up": None,
        "breakdown_20_down": None,
        "ma_context": None,
        "volume_percentile_60": None,
        "source_bar_time": None,
        "invalidation_condition": None,
    })
    if future is not None:
        return _fail_closed(base, reason_code="FUTURE_BAR_DETECTED", missing=_KLINE_MISSING_FIELDS)

    daily = _closed_bars(snapshot, "daily")
    if len(daily) < 2:
        return _fail_closed(base, reason_code="INSUFFICIENT_DAILY_BARS", missing=_KLINE_MISSING_FIELDS)

    latest = daily[-1]
    previous = daily[-2]
    body = abs(latest.close - latest.open)
    bar_range = latest.high - latest.low
    upper_shadow = latest.high - max(latest.open, latest.close)
    lower_shadow = min(latest.open, latest.close) - latest.low
    # OHLCVBar validates positive prices but keeps the high/low relation as a
    # source contract.  Protect this projection from a malformed object too.
    if not _finite_positive(bar_range) or upper_shadow < 0 or lower_shadow < 0:
        return _fail_closed(base, reason_code="MALFORMED_DAILY_BAR", missing=_KLINE_MISSING_FIELDS)

    labels = _pattern_labels(latest, previous, body=body, bar_range=bar_range, upper_shadow=upper_shadow, lower_shadow=lower_shadow)
    breakout_available = len(daily) >= 21
    prior20 = daily[-21:-1] if breakout_available else ()
    reference_high = max((bar.high for bar in prior20), default=None)
    reference_low = min((bar.low for bar in prior20), default=None)
    breakout_up = latest.close > reference_high if reference_high is not None else None
    breakout_down = latest.close < reference_low if reference_low is not None else None
    failed_breakout = bool(reference_high is not None and latest.high > reference_high and latest.close <= reference_high)
    if failed_breakout:
        labels.append("FAILED_BREAKOUT_20")
    volume_percentile = _volume_percentile(daily[-60:-1], latest.volume)
    daily_frame = snapshot.timeframes.get("daily")
    moving = dict(daily_frame.moving_averages) if daily_frame is not None else {}
    ma_context = {
        "ma20": _number(moving.get("ma20")),
        "ma60": _number(moving.get("ma60")),
        "close_vs_ma20": _position(latest.close, moving.get("ma20")),
        "close_vs_ma60": _position(latest.close, moving.get("ma60")),
    }

    base.update({
        "available": True,
        "reason_code": "OK" if breakout_available else "PARTIAL_SAMPLE",
        "latest_bar": _bar_map(latest),
        "body": _number(body),
        "range": _number(bar_range),
        "body_ratio": _ratio(body, bar_range),
        "upper_shadow_ratio": _ratio(upper_shadow, bar_range),
        "lower_shadow_ratio": _ratio(lower_shadow, bar_range),
        "direction": _direction(latest),
        "labels": labels,
        # The explicit top-level aliases make the two claims easy to consume,
        # while the nested map keeps their shared reference window visible.
        "breakout_20": _breakout_map(
            breakout_up,
            breakout_down,
            reference_high,
            reference_low=reference_low,
            available=breakout_available,
        ),
        "breakout_20_up": breakout_up,
        "breakdown_20_down": breakout_down,
        "ma_context": ma_context,
        "volume_percentile_60": volume_percentile,
        "source_bar_time": latest.end.isoformat(),
        "invalidation_condition": {
            "type": "CLOSE_BELOW_PATTERN_LOW",
            "level": _number(min(previous.low, latest.low)),
        },
        "missing_fields": [
            name
            for name, value in (
                ("breakout_20", breakout_up if breakout_available else None),
                ("ma20", ma_context["ma20"]),
                ("ma60", ma_context["ma60"]),
                ("volume_percentile_60", volume_percentile),
            )
            if value is None
        ],
    })
    return base


def _build_price_levels(snapshot: TechnicalFactorSnapshot, *, future: OHLCVBar | None) -> dict[str, Any]:
    base = _base(snapshot, available=False, reason_code="NO_CLOSED_BARS")
    base.update({field: None for field in _PRICE_FIELDS})
    if future is not None:
        return _fail_closed(base, reason_code="FUTURE_BAR_DETECTED", missing=_PRICE_FIELDS)

    daily = _closed_bars(snapshot, "daily")
    minute5 = _closed_bars(snapshot, "5m")
    if not daily:
        return _fail_closed(base, reason_code="NO_CLOSED_BARS", missing=_PRICE_FIELDS)

    closes = [bar.close for bar in daily]
    latest = daily[-1]
    values: dict[str, float | None] = {
        "latest_close": _number(latest.close),
        "previous_day_high": _number(daily[-2].high) if len(daily) >= 2 else None,
        "previous_day_low": _number(daily[-2].low) if len(daily) >= 2 else None,
        "rolling_20_high": _window_extreme(daily, 20, high=True),
        "rolling_20_low": _window_extreme(daily, 20, high=False),
        "rolling_60_high": _window_extreme(daily, 60, high=True),
        "rolling_60_low": _window_extreme(daily, 60, high=False),
        "ma20": _moving_average(closes, 20),
        "ma60": _moving_average(closes, 60),
        "ma255": _moving_average(closes, 255),
        "atr14": _atr14(daily),
        "recent_5m_high": _number(minute5[-1].high) if minute5 else None,
        "recent_5m_low": _number(minute5[-1].low) if minute5 else None,
    }
    daily_frame = snapshot.timeframes.get("daily")
    five_frame = snapshot.timeframes.get("5m")
    daily_ma = dict(daily_frame.moving_averages) if daily_frame is not None else {}
    five_ma = dict(five_frame.moving_averages) if five_frame is not None else {}
    support_candidates = [
        _number(values.get("previous_day_low")),
        _number(values.get("rolling_20_low")),
        _number(daily_ma.get("ma20")),
        _number(daily_ma.get("ma60")),
        _number(five_ma.get("ma20")),
        _number(five_ma.get("ma99")),
        _number(five_ma.get("ma128")),
        _number(five_ma.get("ma255")),
        _number(values.get("recent_5m_low")),
    ]
    supports = [value for value in support_candidates if value is not None and value <= latest.close]
    support = max(supports, default=None)
    resistance_candidates = [
        _number(values.get("previous_day_high")),
        _number(values.get("rolling_20_high")),
        _number(values.get("rolling_60_high")),
    ]
    resistances = [value for value in resistance_candidates if value is not None and value > latest.close]
    resistance = min(resistances, default=None)
    atr = values.get("atr14")
    trigger_zone: dict[str, float] | None = None
    invalidation: float | None = None
    stop_distance: float | None = None
    reward_risk: float | None = None
    max_chase: float | None = None
    if support is not None and latest.close > 0:
        trigger_low = _price(support)
        trigger_high = _price(min(latest.close, support * 1.01))
        if trigger_high is not None and trigger_low is not None and trigger_high >= trigger_low:
            trigger_zone = {"low": trigger_low, "high": trigger_high}
    if trigger_zone is not None and atr is not None and atr > 0:
        floor_candidates = [
            value
            for value in (
                _number(values.get("rolling_20_low")),
                _number(values.get("previous_day_low")),
                _number(trigger_zone["low"] - atr),
            )
            if value is not None and value < trigger_zone["low"]
        ]
        invalidation = _price(max(floor_candidates, default=trigger_zone["low"] - atr))
    if trigger_zone is not None and invalidation is not None and trigger_zone["high"] > invalidation:
        risk = trigger_zone["high"] - invalidation
        stop_distance = _number(risk / trigger_zone["high"])
        if resistance is not None and resistance > trigger_zone["high"]:
            reward_risk = _number((resistance - trigger_zone["high"]) / risk)
    if resistance is not None:
        max_chase = _price(min(resistance, latest.close * 1.03))
    planning = {
        "trigger_zone": trigger_zone,
        "invalidation": invalidation,
        "stop_distance_pct": stop_distance,
        "first_resistance": _price(resistance),
        "reward_risk": reward_risk,
        "max_chase_price": max_chase,
        "source_bar_time": latest.end.isoformat(),
    }
    values.update(planning)
    missing = [field for field in _PRICE_FIELDS if values.get(field) is None]
    base.update(values)
    base.update({
        "available": True,
        "reason_code": "OK" if not missing else "PARTIAL_SAMPLE",
        "missing_fields": missing,
        "planning_ready": all(planning[field] is not None for field in planning),
        "planning_constraints": {
            "max_stop_distance_pct": 0.06,
            "minimum_reward_risk": 2.0,
            "passes_stop_distance": stop_distance is not None and stop_distance <= 0.06,
            "passes_reward_risk": reward_risk is not None and reward_risk >= 2.0,
        },
    })
    return base


def _base(snapshot: TechnicalFactorSnapshot, *, available: bool, reason_code: str) -> dict[str, Any]:
    return {
        "available": available,
        "reason_code": reason_code,
        "as_of": snapshot.as_of.isoformat(),
        "symbol": snapshot.symbol,
        "source": SOURCE,
        "missing_fields": [],
    }


def _fail_closed(base: dict[str, Any], *, reason_code: str, missing: Sequence[str]) -> dict[str, Any]:
    base["available"] = False
    base["reason_code"] = reason_code
    base["missing_fields"] = list(dict.fromkeys(missing))
    return base


def _closed_bars(snapshot: TechnicalFactorSnapshot, timeframe: str) -> tuple[OHLCVBar, ...]:
    frame = snapshot.timeframes.get(timeframe)
    if frame is None:
        return ()
    bars = getattr(frame, "bars", ())
    return tuple(sorted((bar for bar in bars if getattr(bar, "closed", True)), key=lambda item: item.end))


def _future_bar(snapshot: TechnicalFactorSnapshot) -> OHLCVBar | None:
    for frame in snapshot.timeframes.values():
        for bar in getattr(frame, "bars", ()):
            if bar.end > snapshot.as_of:
                return bar
    return None


def _bar_map(bar: OHLCVBar) -> dict[str, Any]:
    return {
        "start": bar.start.isoformat(),
        "end": bar.end.isoformat(),
        "open": _number(bar.open),
        "high": _number(bar.high),
        "low": _number(bar.low),
        "close": _number(bar.close),
        "volume": _number(bar.volume),
        "amount": _number(bar.amount),
    }


def _direction(bar: OHLCVBar) -> str:
    if bar.close > bar.open:
        return "BULLISH"
    if bar.close < bar.open:
        return "BEARISH"
    return "FLAT"


def _pattern_labels(
    latest: OHLCVBar,
    previous: OHLCVBar,
    *,
    body: float,
    bar_range: float,
    upper_shadow: float,
    lower_shadow: float,
) -> list[str]:
    body_ratio = body / bar_range
    upper_ratio = upper_shadow / bar_range
    lower_ratio = lower_shadow / bar_range
    labels: list[str] = []
    if body_ratio <= 0.10:
        labels.append("DOJI")
    # The thresholds are intentionally explicit and conservative.  They do
    # not depend on a configurable TA library or on future bars.
    if lower_shadow >= max(body * 2.0, bar_range * 0.50) and upper_ratio <= 0.20 and body_ratio <= 0.40:
        labels.append("HAMMER")
    if upper_shadow >= max(body * 2.0, bar_range * 0.50) and lower_ratio <= 0.20 and body_ratio <= 0.40:
        labels.append("SHOOTING_STAR")
    if previous.close < previous.open and latest.close > latest.open and latest.open <= previous.close and latest.close >= previous.open:
        labels.append("BULLISH_ENGULFING")
    if previous.close > previous.open and latest.close < latest.open and latest.open >= previous.close and latest.close <= previous.open:
        labels.append("BEARISH_ENGULFING")
    return labels


def _breakout_map(
    up: bool | None,
    down: bool | None,
    reference_high: float | None,
    reference_low: float | None = None,
    *,
    available: bool,
) -> dict[str, Any]:
    return {
        "available": available,
        "up": up,
        "down": down,
        "reference_high": _number(reference_high),
        "reference_low": _number(reference_low),
    }


def _window_extreme(bars: Sequence[OHLCVBar], period: int, *, high: bool) -> float | None:
    if len(bars) < period:
        return None
    values = [bar.high if high else bar.low for bar in bars[-period:]]
    result = max(values) if high else min(values)
    return _number(result)


def _moving_average(values: Sequence[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return _number(sum(values[-period:]) / period)


def _atr14(bars: Sequence[OHLCVBar]) -> float | None:
    # ATR14 uses 14 true ranges and therefore needs the previous close for
    # each range: 15 closed daily bars are the minimum complete sample.
    if len(bars) < 15:
        return None
    ranges: list[float] = []
    for index in range(1, len(bars)):
        bar = bars[index]
        previous_close = bars[index - 1].close
        true_range = max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close))
        if not math.isfinite(true_range) or true_range < 0:
            return None
        ranges.append(true_range)
    return _number(sum(ranges[-14:]) / 14.0)


def _ratio(numerator: float, denominator: float) -> float | None:
    return _number(numerator / denominator) if denominator > 0 else None


def _volume_percentile(history: Sequence[OHLCVBar], current: float | None) -> float | None:
    if current is None:
        return None
    values = [bar.volume for bar in history if bar.volume is not None]
    if len(values) < 20:
        return None
    return _number(sum(value <= current for value in values) / len(values))


def _position(price: float, moving_average: Any) -> str | None:
    value = _number(moving_average)
    if value is None:
        return None
    return "ABOVE" if price > value else "BELOW" if price < value else "AT"


def _price(value: Any) -> float | None:
    number = _number(value)
    return round(number, 4) if number is not None and number > 0 else None


def _finite_positive(value: float) -> bool:
    return math.isfinite(float(value)) and float(value) > 0


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


__all__ = [
    "SOURCE",
    "build_kline_patterns",
    "build_price_levels",
    "build_technical_aggregates",
]
