"""Deterministic, closed-bar technical factors for A-share A3 inputs."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time as datetime_time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..data.mootdx import MinuteBar


SHANGHAI = ZoneInfo("Asia/Shanghai")
MA_PERIODS = (5, 10, 20, 60, 99, 128, 255)
# ``monthly``/``weekly`` are the formal A3 background frames.  The minute
# frames remain in this tuple for compatibility with the A4 and legacy
# consumers; their readiness is deliberately *not* part of ``a3_ready``.
TIMEFRAMES = ("monthly", "weekly", "daily", "120m", "15m", "5m")
LEGACY_TIMEFRAMES = ("weekly", "daily", "120m", "15m", "5m")
A3_TIMEFRAMES = ("monthly", "weekly", "daily")
_TIMEFRAME_REQUIRED: dict[str, tuple[int, ...]] = {
    # Monthly direction uses the conventional 5/20-month cycle. Requiring a
    # 60-month average both duplicates a five-year regime model and made the
    # normal 2--4 year point-in-time cache return no monthly direction at all.
    "monthly": (5, 20),
    "weekly": (5, 10, 20),
    "daily": (5, 10, 20, 60),
    "120m": (5, 20, 99, 128, 255),
    "15m": (5, 20, 60),
    "5m": (5, 20, 99, 128, 255),
}
# A3 needs enough formal background to calculate every declared cycle. Twenty
# closed months are required for the monthly 5/20 regime; two bars would only
# prove that a period closed, not that the monthly trend was computable.
_A3_MIN_CLOSED_BARS = {"monthly": 20, "weekly": 20, "daily": 60}


class OHLCVBar(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    amount: float | None = None
    # This is source metadata only.  Never infer qfq/hfq/raw when a provider
    # omitted it; ``None`` is surfaced as UNKNOWN in the summary.
    adjust_mode: str | None = None
    closed: Literal[True] = True

    @field_validator("start", "end")
    @classmethod
    def aware_shanghai(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("factor bars require timezone-aware timestamps")
        return value.astimezone(SHANGHAI)

    @field_validator("open", "high", "low", "close")
    @classmethod
    def finite_price(cls, value: float) -> float:
        if not math.isfinite(float(value)) or float(value) <= 0:
            raise ValueError("factor prices must be finite and positive")
        return float(value)

    @field_validator("volume", "amount")
    @classmethod
    def finite_nonnegative(cls, value: float | None) -> float | None:
        if value is not None and (not math.isfinite(float(value)) or float(value) < 0):
            raise ValueError("factor volume and amount must be finite and non-negative")
        return None if value is None else float(value)

    @model_validator(mode="after")
    def valid_ohlc_geometry(self) -> "OHLCVBar":
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close) or self.high < self.low:
            raise ValueError("factor OHLC geometry is inconsistent")
        if self.end <= self.start:
            raise ValueError("factor bar end must be after start")
        return self


class TimeframeFactors(BaseModel):
    model_config = ConfigDict(frozen=True)

    timeframe: str
    bars: tuple[OHLCVBar, ...] = ()
    latest: OHLCVBar | None = None
    # A current month/week is kept separately as observation-only data.  It
    # must not enter formal moving averages or trend conclusions.
    partial_bars: tuple[OHLCVBar, ...] = ()
    latest_partial: OHLCVBar | None = None
    moving_averages: dict[str, float | None] = Field(default_factory=dict)
    previous_moving_averages: dict[str, float | None] = Field(default_factory=dict)
    ma_slopes: dict[str, float | None] = Field(default_factory=dict)
    ma_alignment: str | None = None
    ma_event: str | None = None
    ma_bias: dict[str, float | None] = Field(default_factory=dict)
    vwap: float | None = None
    adjust_mode: str | None = None
    ready: bool = False
    reasons: tuple[str, ...] = ()


class TechnicalFactorSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    as_of: datetime
    ready: bool = False
    # A3 readiness is a separate contract from the historical all-timeframe
    # ``ready`` flag.  It only considers formal monthly/weekly/daily bars.
    a3_ready: bool = False
    a3_reasons: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    timeframes: dict[str, TimeframeFactors] = Field(default_factory=dict)
    technical_summary: dict[str, Any] = Field(default_factory=dict)

    @field_validator("as_of")
    @classmethod
    def aware_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("factor as_of must be timezone-aware")
        return value.astimezone(SHANGHAI)

    @property
    def summary(self) -> dict[str, Any]:
        return self.technical_summary

    @property
    def high_timeframe_ready(self) -> bool:
        """Alias used by callers that do not name the A3 stage explicitly."""

        return self.a3_ready


FactorSnapshot = TechnicalFactorSnapshot


class FactorEngine:
    """Build only from bars that have closed by ``as_of``."""

    def __init__(self, symbol: str | None = None) -> None:
        self.symbol = _canonical_symbol(symbol) if symbol else None

    def compute(
        self,
        *,
        daily_bars: Sequence[Mapping[str, Any] | BaseModel] = (),
        minute_bars: Sequence[MinuteBar | Mapping[str, Any] | BaseModel] = (),
        as_of: datetime | None = None,
        symbol: str | None = None,
    ) -> TechnicalFactorSnapshot:
        resolved_symbol = _canonical_symbol(symbol or self.symbol or _infer_symbol(minute_bars, daily_bars))
        if resolved_symbol is None:
            resolved_symbol = "UNKNOWN"
        cutoff = _as_of(as_of, daily_bars, minute_bars)
        reasons: list[str] = []
        daily, daily_reasons = _daily_bars(daily_bars, resolved_symbol, cutoff)
        reasons.extend(daily_reasons)
        monthly, monthly_partial = _aggregate_daily_monthly(daily, resolved_symbol)
        weekly, weekly_partial = _aggregate_daily_weekly_with_partial(daily, resolved_symbol)
        minute, minute_reasons = _minute_base(minute_bars, resolved_symbol, cutoff)
        reasons.extend(minute_reasons)
        base5 = tuple(bar for bar in minute if bar.timeframe == "5m")
        base1 = tuple(bar for bar in minute if bar.timeframe == "1m")
        if not base5 and base1:
            base5 = _aggregate_minutes(base1, width=5, timeframe="5m", symbol=resolved_symbol)
        if not base5:
            # A source containing only 5m rows is represented as 5m above;
            # missing minute data remains explicit below.
            base5 = ()
        bars_by_tf: dict[str, tuple[OHLCVBar, ...]] = {
            "monthly": monthly,
            "weekly": weekly,
            "daily": daily,
            "5m": base5,
            "15m": _aggregate_minutes(base5, width=15, timeframe="15m", symbol=resolved_symbol),
            "120m": _aggregate_minutes(base5, width=120, timeframe="120m", symbol=resolved_symbol),
        }
        frames: dict[str, TimeframeFactors] = {}
        partial_by_tf = {
            "monthly": monthly_partial,
            "weekly": weekly_partial,
        }
        for timeframe in TIMEFRAMES:
            frame = _calculate_frame(
                timeframe,
                bars_by_tf.get(timeframe, ()),
                partial_bars=partial_by_tf.get(timeframe, ()),
            )
            frames[timeframe] = frame
            reasons.extend(f"{timeframe.upper()}_{reason}" for reason in frame.reasons)
        # Preserve the old all-timeframe readiness flag for compatibility.
        # It is intentionally stricter than the A3 contract and still
        # includes minute data and its reasons.
        legacy_reasons = [*daily_reasons, *minute_reasons]
        for name in LEGACY_TIMEFRAMES:
            legacy_reasons.extend(f"{name.upper()}_{reason}" for reason in frames[name].reasons)
        ready = bool(daily and all(frames[name].ready for name in LEGACY_TIMEFRAMES)) and not legacy_reasons
        a3_ready, a3_reasons = _a3_readiness(frames)
        all_reasons = tuple(dict.fromkeys(reasons))
        summary = _technical_summary(
            resolved_symbol,
            cutoff,
            frames,
            ready,
            all_reasons,
            a3_ready=a3_ready,
            a3_reasons=a3_reasons,
        )
        return TechnicalFactorSnapshot(
            symbol=resolved_symbol,
            as_of=cutoff,
            ready=ready,
            a3_ready=a3_ready,
            a3_reasons=a3_reasons,
            reasons=all_reasons,
            timeframes=frames,
            technical_summary=summary,
        )

    calculate = compute
    build = compute

    @classmethod
    def calculate_for(
        cls,
        symbol: str,
        *,
        daily_bars: Sequence[Mapping[str, Any] | BaseModel] = (),
        minute_bars: Sequence[MinuteBar | Mapping[str, Any] | BaseModel] = (),
        as_of: datetime | None = None,
    ) -> TechnicalFactorSnapshot:
        return cls(symbol).compute(daily_bars=daily_bars, minute_bars=minute_bars, as_of=as_of)


def _calculate_frame(
    timeframe: str,
    bars: Sequence[OHLCVBar],
    *,
    partial_bars: Sequence[OHLCVBar] = (),
) -> TimeframeFactors:
    ordered = tuple(sorted((bar for bar in bars if bar.closed), key=lambda item: item.end))
    partial = tuple(sorted((bar for bar in partial_bars if bar.closed), key=lambda item: item.end))
    moving: dict[str, float | None] = {}
    closes = [bar.close for bar in ordered]
    for period in MA_PERIODS:
        moving[f"ma{period}"] = _moving_average(closes, period)
    total_volume = sum(bar.volume for bar in ordered if bar.volume is not None)
    amount_available = all(bar.amount is not None for bar in ordered)
    total_amount = sum(bar.amount or 0.0 for bar in ordered) if amount_available else 0.0
    vwap = total_amount / total_volume if amount_available and total_volume > 0 else None
    reasons = [f"INSUFFICIENT_MA{period}" for period in _TIMEFRAME_REQUIRED.get(timeframe, MA_PERIODS) if moving[f"ma{period}"] is None]
    if not ordered:
        reasons.insert(0, "NO_CLOSED_BARS")
    if vwap is None:
        reasons.append("VWAP_UNAVAILABLE")
    ready = not reasons
    required_periods = _TIMEFRAME_REQUIRED.get(timeframe, MA_PERIODS)
    previous_moving = {
        f"ma{period}": _moving_average(closes[:-1], period)
        for period in required_periods
    }
    slopes = {
        key: (
            (float(moving[key]) - float(previous)) / abs(float(previous))
            if moving.get(key) is not None and previous not in {None, 0}
            else None
        )
        for key, previous in previous_moving.items()
    }
    alignment = _ma_alignment(moving, required_periods, ordered[-1].close if ordered else None)
    event = _ma_event(
        ordered,
        moving,
        previous_moving,
        required_periods,
    )
    bias = _ma_bias(ordered[-1].close if ordered else None, moving)
    return TimeframeFactors(
        timeframe=timeframe,
        bars=ordered,
        latest=ordered[-1] if ordered else None,
        partial_bars=partial,
        latest_partial=partial[-1] if partial else None,
        moving_averages=moving,
        previous_moving_averages=previous_moving,
        ma_slopes=slopes,
        ma_alignment=alignment,
        ma_event=event,
        ma_bias=bias,
        vwap=vwap,
        adjust_mode=_common_adjust_mode(ordered),
        ready=ready,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def _ma_alignment(
    moving: Mapping[str, float | None],
    periods: Sequence[int],
    close: float | None,
) -> str | None:
    values = [moving.get(f"ma{period}") for period in periods]
    if close is None or any(value is None for value in values):
        return None
    resolved = [float(value) for value in values if value is not None]
    if all(left > right for left, right in zip(resolved, resolved[1:])):
        return "BULL_STACK"
    if all(left < right for left, right in zip(resolved, resolved[1:])):
        return "BEAR_STACK"
    ma5 = _number(moving.get("ma5"))
    ma20 = _number(moving.get("ma20"))
    if ma5 is not None and ma20 is not None and ma5 > ma20 and close > ma20:
        return "BULL_PARTIAL"
    if ma5 is not None and ma20 is not None and ma5 < ma20 and close < ma20:
        return "BEAR_PARTIAL"
    return "ENTANGLED"


def _ma_event(
    bars: Sequence[OHLCVBar],
    moving: Mapping[str, float | None],
    previous: Mapping[str, float | None],
    periods: Sequence[int],
) -> str | None:
    if len(bars) < 2 or any(moving.get(f"ma{period}") is None for period in periods):
        return None

    def crossed_above(fast: str, slow: str) -> bool:
        return (
            _number(previous.get(fast)) is not None
            and _number(previous.get(slow)) is not None
            and float(previous[fast]) <= float(previous[slow])
            and float(moving[fast]) > float(moving[slow])
        )

    def crossed_below(fast: str, slow: str) -> bool:
        return (
            _number(previous.get(fast)) is not None
            and _number(previous.get(slow)) is not None
            and float(previous[fast]) >= float(previous[slow])
            and float(moving[fast]) < float(moving[slow])
        )

    if crossed_above("ma5", "ma20"):
        return "GOLDEN_CROSS_SHORT"
    if "ma99" in moving and (crossed_above("ma20", "ma99") or crossed_above("ma20", "ma128")):
        return "GOLDEN_CROSS_MID"
    if crossed_below("ma5", "ma20"):
        return "DEAD_CROSS_SHORT"
    if "ma99" in moving and (crossed_below("ma20", "ma99") or crossed_below("ma20", "ma128")):
        return "DEAD_CROSS_MID"

    latest = bars[-1]
    prior_close = bars[-2].close
    for period in (255, 128, 99):
        key = f"ma{period}"
        current_ma = _number(moving.get(key))
        previous_ma = _number(previous.get(key))
        if current_ma is None or previous_ma is None:
            continue
        if prior_close < previous_ma <= latest.close:
            return f"RECLAIM_MA{period}"
        if prior_close >= previous_ma > latest.close:
            return f"LOSE_MA{period}"
    for period in (20, 99, 128):
        key = f"ma{period}"
        current_ma = _number(moving.get(key))
        if current_ma is not None and latest.low <= current_ma <= latest.close:
            return f"PULLBACK_HOLD_MA{period}"
    return "NONE"


def _ma_bias(close: float | None, moving: Mapping[str, float | None]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for period in (20, 99):
        average = _number(moving.get(f"ma{period}"))
        result[f"close_vs_ma{period}_pct"] = (
            (float(close) / average) - 1.0
            if close is not None and average is not None and average > 0
            else None
        )
    return result


def _moving_average(values: Sequence[float], period: int) -> float | None:
    if len(values) < period:
        return None
    result = sum(values[-period:]) / period
    return float(result) if math.isfinite(result) else None


def _daily_bars(
    rows: Sequence[Mapping[str, Any] | BaseModel],
    symbol: str,
    cutoff: datetime,
) -> tuple[tuple[OHLCVBar, ...], tuple[str, ...]]:
    result: list[OHLCVBar] = []
    reasons: list[str] = []
    seen: set[datetime] = set()
    for row in rows:
        data = _mapping(row)
        if data.get("closed") is False or data.get("is_closed") is False:
            continue
        end = _row_datetime(data, daily=True)
        if end is None or end > cutoff:
            if end is not None and end > cutoff:
                continue
            reasons.append("MALFORMED_DAILY_BAR")
            continue
        parsed = _make_bar(
            symbol=symbol,
            timeframe="daily",
            start=end.replace(hour=9, minute=30),
            end=end,
            data=data,
        )
        if parsed is None:
            reasons.append("MALFORMED_DAILY_BAR")
            continue
        if end in seen:
            reasons.append("DUPLICATE_DAILY_BAR")
            continue
        seen.add(end)
        result.append(parsed)
    return tuple(sorted(result, key=lambda item: item.end)), tuple(dict.fromkeys(reasons))


def _aggregate_daily_monthly(
    bars: Sequence[OHLCVBar],
    symbol: str,
) -> tuple[tuple[OHLCVBar, ...], tuple[OHLCVBar, ...]]:
    """Aggregate daily bars and keep the current month observation separate.

    A period is formal only when a later period is present in the same
    point-in-time input.  This deliberately does not assume that Friday or
    the calendar month-end was a trading day; a missing next-period bar leaves
    the latest period partial.
    """

    return _aggregate_daily_period(bars, symbol, period="monthly")


def _aggregate_daily_weekly_with_partial(
    bars: Sequence[OHLCVBar],
    symbol: str,
) -> tuple[tuple[OHLCVBar, ...], tuple[OHLCVBar, ...]]:
    """Aggregate daily bars and keep the current week observation separate."""

    return _aggregate_daily_period(bars, symbol, period="weekly")


def _aggregate_daily_weekly(bars: Sequence[OHLCVBar], symbol: str) -> tuple[OHLCVBar, ...]:
    """Backward-compatible closed-week-only aggregation helper."""

    closed, _partial = _aggregate_daily_weekly_with_partial(bars, symbol)
    return closed


def _aggregate_daily_period(
    bars: Sequence[OHLCVBar],
    symbol: str,
    *,
    period: str,
) -> tuple[tuple[OHLCVBar, ...], tuple[OHLCVBar, ...]]:
    groups: dict[tuple[int, int], list[OHLCVBar]] = defaultdict(list)
    for bar in bars:
        if period == "weekly":
            iso = bar.end.isocalendar()
            key = (int(iso.year), int(iso.week))
            timeframe = "weekly"
        elif period == "monthly":
            key = (bar.end.year, bar.end.month)
            timeframe = "monthly"
        else:  # pragma: no cover - internal callers use the two supported periods
            raise ValueError(f"unsupported daily aggregation period: {period}")
        groups[key].append(bar)

    ordered_groups = sorted(groups.items(), key=lambda item: min(bar.end for bar in item[1]))
    closed: list[OHLCVBar] = []
    partial: list[OHLCVBar] = []
    for index, (_key, grouped) in enumerate(ordered_groups):
        ordered = sorted(grouped, key=lambda item: item.end)
        aggregate = _aggregate_group(ordered, timeframe=timeframe, symbol=symbol)
        # Only a following group proves that this group ended.  The latest
        # group is an observation even when its date happens to be Friday or
        # the last calendar day of a month.
        (closed if index < len(ordered_groups) - 1 else partial).append(aggregate)
    return tuple(closed), tuple(partial)


def _minute_base(
    rows: Sequence[MinuteBar | Mapping[str, Any] | BaseModel],
    symbol: str,
    cutoff: datetime,
) -> tuple[tuple[OHLCVBar, ...], tuple[str, ...]]:
    result: list[OHLCVBar] = []
    reasons: list[str] = []
    seen: set[tuple[str, datetime]] = set()
    for row in rows:
        if isinstance(row, MinuteBar):
            data = {
                "symbol": row.symbol,
                "interval": row.interval,
                "bar_end": row.bar_end,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "amount": row.amount,
                "adjust_mode": row.adjust_mode,
            }
        else:
            data = _mapping(row)
        if data.get("closed") is False or data.get("is_closed") is False:
            continue
        end = _row_datetime(data, daily=False)
        if end is None:
            reasons.append("MALFORMED_MINUTE_BAR")
            continue
        end = end.astimezone(SHANGHAI)
        if end > cutoff:
            continue
        interval = str(data.get("interval") or data.get("frequency") or "5m").lower()
        if interval in {"1", "1min", "1minute"}:
            interval = "1m"
        elif interval in {"5", "5min", "5minute"}:
            interval = "5m"
        if interval not in {"1m", "5m"}:
            reasons.append("UNSUPPORTED_MINUTE_INTERVAL")
            continue
        key = (interval, end)
        if key in seen:
            reasons.append("DUPLICATE_MINUTE_BAR")
            continue
        parsed = _make_bar(
            symbol=symbol,
            timeframe=interval,
            start=end - timedelta(minutes=int(interval[:-1])),
            end=end,
            data=data,
        )
        if parsed is None:
            reasons.append("MALFORMED_MINUTE_BAR")
            continue
        seen.add(key)
        result.append(parsed)
    return tuple(sorted(result, key=lambda item: (item.end, item.timeframe))), tuple(dict.fromkeys(reasons))


def _aggregate_minutes(
    bars: Sequence[OHLCVBar],
    *,
    width: int,
    timeframe: str,
    symbol: str,
) -> tuple[OHLCVBar, ...]:
    if not bars:
        return ()
    grouped: dict[tuple[date, datetime], list[OHLCVBar]] = defaultdict(list)
    for bar in bars:
        bucket = _bucket_end(bar.end, width)
        if bucket is None:
            continue
        grouped[(bar.end.date(), bucket)].append(bar)
    result: list[OHLCVBar] = []
    for (day, bucket), grouped_bars in grouped.items():
        ordered = sorted(grouped_bars, key=lambda item: item.end)
        result.append(_aggregate_group(ordered, timeframe=timeframe, symbol=symbol, end_override=bucket))
    return tuple(sorted(result, key=lambda item: item.end))


def _bucket_end(value: datetime, width: int) -> datetime | None:
    value = value.astimezone(SHANGHAI)
    current = value.timetz().replace(tzinfo=None)
    if datetime_time(9, 30) < current <= datetime_time(11, 30):
        origin = value.replace(hour=9, minute=30, second=0, microsecond=0)
    elif datetime_time(13, 0) < current <= datetime_time(15, 0):
        origin = value.replace(hour=13, minute=0, second=0, microsecond=0)
    else:
        return None
    elapsed = int((value - origin).total_seconds() // 60)
    bucket_minutes = ((elapsed + width - 1) // width) * width
    if bucket_minutes <= 0:
        bucket_minutes = width
    bucket = origin + timedelta(minutes=bucket_minutes)
    if bucket.timetz().replace(tzinfo=None) > datetime_time(11, 30) and origin.hour == 9:
        bucket = origin.replace(hour=11, minute=30)
    if bucket.timetz().replace(tzinfo=None) > datetime_time(15, 0):
        return None
    return bucket


def _aggregate_group(
    bars: Sequence[OHLCVBar],
    *,
    timeframe: str,
    symbol: str,
    end_override: datetime | None = None,
) -> OHLCVBar:
    ordered = sorted(bars, key=lambda item: item.end)
    volumes = [bar.volume for bar in ordered]
    amounts = [bar.amount for bar in ordered]
    return OHLCVBar(
        symbol=symbol,
        timeframe=timeframe,
        start=ordered[0].start,
        end=end_override or ordered[-1].end,
        open=ordered[0].open,
        high=max(bar.high for bar in ordered),
        low=min(bar.low for bar in ordered),
        close=ordered[-1].close,
        volume=sum(volumes) if all(value is not None for value in volumes) else None,
        amount=sum(amounts) if all(value is not None for value in amounts) else None,
        adjust_mode=_common_adjust_mode(ordered),
    )


def _make_bar(*, symbol: str, timeframe: str, start: datetime, end: datetime, data: Mapping[str, Any]) -> OHLCVBar | None:
    values: dict[str, Any] = {}
    for name, keys in {
        "open": ("open", "open_price", "开盘"),
        "high": ("high", "high_price", "最高"),
        "low": ("low", "low_price", "最低"),
        "close": ("close", "close_price", "last", "last_price", "收盘", "最新价"),
        "volume": ("volume", "vol", "成交量"),
        "amount": ("amount", "turnover", "成交额"),
        "adjust_mode": ("adjust_mode", "adjust", "adjustment", "复权方式"),
    }.items():
        raw_value = _first(data, keys)
        values[name] = raw_value if name == "adjust_mode" else _number(raw_value)
    if any(values[name] is None for name in ("open", "high", "low", "close")):
        return None
    try:
        return OHLCVBar(symbol=symbol, timeframe=timeframe, start=start, end=end, **values)
    except Exception:
        return None


def _row_datetime(data: Mapping[str, Any], *, daily: bool) -> datetime | None:
    raw = _first(data, ("bar_end", "datetime", "timestamp", "time", "date", "trade_date", "交易日期"))
    if raw is None:
        raw = _first(data, ("date_ms", "timestamp_ms", "trade_date_ms"))
    try:
        if isinstance(raw, datetime):
            result = raw
        elif isinstance(raw, date):
            result = datetime.combine(raw, datetime_time(15, 0) if daily else datetime_time(0, 0), tzinfo=SHANGHAI)
        elif isinstance(raw, (int, float)) and not isinstance(raw, bool):
            number = float(raw)
            seconds = number / 1000 if abs(number) > 10_000_000_000 else number
            result = datetime.fromtimestamp(seconds, tz=SHANGHAI)
        else:
            text = str(raw).strip()
            if text.isdigit() and len(text) >= 10:
                number = float(text)
                seconds = number / 1000 if len(text) >= 13 else number
                result = datetime.fromtimestamp(seconds, tz=SHANGHAI)
            else:
                text = text.replace("Z", "+00:00")
                result = datetime.fromisoformat(text)
        if result.tzinfo is None or result.utcoffset() is None:
            result = result.replace(tzinfo=SHANGHAI)
        result = result.astimezone(SHANGHAI)
        if daily:
            result = result.replace(hour=15, minute=0, second=0, microsecond=0)
        else:
            result = result.replace(second=0, microsecond=0)
        return result
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _as_of(as_of: datetime | None, daily: Sequence[Any], minute: Sequence[Any]) -> datetime:
    if as_of is not None:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        return as_of.astimezone(SHANGHAI)
    candidates: list[datetime] = []
    for row in [*daily, *minute]:
        data = _mapping(row)
        value = _row_datetime(
            data,
            daily=bool(
                data.get("date")
                or data.get("trade_date")
                or data.get("交易日期")
                or data.get("date_ms")
                or data.get("trade_date_ms")
            ),
        )
        if value is not None:
            candidates.append(value)
    return max(candidates, default=datetime.now(SHANGHAI))


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python")
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _first(data: Mapping[str, Any], keys: Sequence[str]) -> Any:
    lowered = {str(key).lower(): value for key, value in data.items()}
    for key in keys:
        if key in data:
            return data[key]
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _canonical_symbol(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper().replace("XSHG", "SH").replace("XSHE", "SZ").replace("-", ".")
    match = re.fullmatch(r"(\d{6})[.]?(SH|SZ|BJ)?", text)
    if not match:
        return None
    code, exchange = match.groups()
    exchange = exchange or ("SH" if code.startswith("6") else "SZ" if code.startswith(("0", "2", "3")) else "BJ" if code.startswith(("4", "8")) else None)
    return f"{code}.{exchange}" if exchange else None


def _infer_symbol(minute: Sequence[Any], daily: Sequence[Any]) -> str | None:
    for row in [*minute, *daily]:
        data = _mapping(row)
        value = _first(data, ("symbol", "thscode", "ticker", "code"))
        parsed = _canonical_symbol(value)
        if parsed:
            return parsed
    return None


def _technical_summary(
    symbol: str,
    as_of: datetime,
    frames: Mapping[str, TimeframeFactors],
    ready: bool,
    reasons: tuple[str, ...],
    *,
    a3_ready: bool | None = None,
    a3_reasons: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    if a3_ready is None or a3_reasons is None:
        a3_ready, a3_reasons = _a3_readiness(frames)
    monthly_frame = frames.get("monthly")
    weekly_frame = frames.get("weekly")
    summary: dict[str, Any] = {
        "symbol": symbol,
        "as_of": as_of.isoformat(),
        "ready": ready,
        "a3_ready": a3_ready,
        "a3_reasons": list(a3_reasons),
        "monthly_closed": bool(monthly_frame and monthly_frame.bars),
        "weekly_closed": bool(weekly_frame and weekly_frame.bars),
        "monthly_formal_latest_end": (
            monthly_frame.latest.end.isoformat() if monthly_frame and monthly_frame.latest else None
        ),
        "weekly_formal_latest_end": (
            weekly_frame.latest.end.isoformat() if weekly_frame and weekly_frame.latest else None
        ),
        "monthly_current_period_closed": bool(monthly_frame and monthly_frame.bars and not monthly_frame.partial_bars),
        "weekly_current_period_closed": bool(weekly_frame and weekly_frame.bars and not weekly_frame.partial_bars),
        "period_state": {
            "monthly": "PARTIAL" if monthly_frame and monthly_frame.partial_bars else "CLOSED" if monthly_frame and monthly_frame.bars else "UNKNOWN",
            "weekly": "PARTIAL" if weekly_frame and weekly_frame.partial_bars else "CLOSED" if weekly_frame and weekly_frame.bars else "UNKNOWN",
        },
        "partial_observations": {
            name: _partial_observation(frames.get(name))
            for name in ("monthly", "weekly")
        },
        "price_series": {
            "daily_adjust_mode": _summary_adjust_mode(frames.get("daily")),
            "calculation_basis": _calculation_basis(frames.get("daily")),
        },
        "reasons": list(reasons),
        "timeframes": {},
    }
    for timeframe in TIMEFRAMES:
        frame = frames.get(timeframe) or TimeframeFactors(timeframe=timeframe)
        summary["timeframes"][timeframe] = {
            "bar_count": len(frame.bars),
            "partial_bar_count": len(frame.partial_bars),
            "latest_end": frame.latest.end.isoformat() if frame.latest else None,
            "latest_partial_end": frame.latest_partial.end.isoformat() if frame.latest_partial else None,
            "latest_close": frame.latest.close if frame.latest else None,
            "ma": dict(frame.moving_averages),
            "previous_ma": dict(frame.previous_moving_averages),
            "ma_slopes": dict(frame.ma_slopes),
            "ma_alignment": frame.ma_alignment,
            "ma_event": frame.ma_event,
            "ma_bias": dict(frame.ma_bias),
            "vwap": frame.vwap,
            "adjust_mode": _summary_adjust_mode(frame),
            "ready": frame.ready,
            "reasons": list(frame.reasons),
        }
    return summary


def _a3_readiness(frames: Mapping[str, TimeframeFactors]) -> tuple[bool, tuple[str, ...]]:
    """Return the independent A3 high-timeframe readiness contract.

    Minute frame shortages, VWAP absence, and legacy frame readiness are not
    considered here.  They remain visible in ``TechnicalFactorSnapshot.ready``
    and per-frame reasons for consumers that still need them.
    """

    reasons: list[str] = []
    for timeframe, minimum in _A3_MIN_CLOSED_BARS.items():
        frame = frames.get(timeframe)
        bars = frame.bars if frame is not None else ()
        if len(bars) < minimum:
            reasons.append(f"INSUFFICIENT_{timeframe.upper()}_CLOSED_BARS")
    daily = frames.get("daily")
    if daily is not None:
        for period in (5, 10, 20, 60):
            if daily.moving_averages.get(f"ma{period}") is None:
                reasons.append(f"INSUFFICIENT_DAILY_MA{period}")
    else:
        reasons.append("NO_DAILY_FRAME")
    return not reasons, tuple(dict.fromkeys(reasons))


def _partial_observation(frame: TimeframeFactors | None) -> dict[str, Any] | None:
    if frame is None or frame.latest_partial is None:
        return None
    bar = frame.latest_partial
    return {
        "available": True,
        "observation_only": True,
        "period_end": bar.end.isoformat(),
        "bar": {
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "amount": bar.amount,
        },
        "adjust_mode": bar.adjust_mode or "UNKNOWN",
    }


def _common_adjust_mode(bars: Sequence[OHLCVBar]) -> str | None:
    if not bars:
        return None
    raw_modes = [str(bar.adjust_mode).strip().lower() if bar.adjust_mode and str(bar.adjust_mode).strip() else None for bar in bars]
    modes = {mode for mode in raw_modes if mode is not None}
    if not modes:
        return None
    if any(mode is None for mode in raw_modes):
        return "mixed"
    return next(iter(modes)) if len(modes) == 1 else "mixed"


def _summary_adjust_mode(frame: TimeframeFactors | None) -> str:
    if frame is None or frame.adjust_mode is None:
        return "UNKNOWN"
    return str(frame.adjust_mode)


def _is_forward_adjusted(frame: TimeframeFactors | None) -> bool:
    if frame is None or frame.adjust_mode is None:
        return False
    return str(frame.adjust_mode).strip().lower() in {
        "qfq",
        "front_adjusted",
        "forward_adjusted",
        "前复权",
    }


def _calculation_basis(frame: TimeframeFactors | None) -> str:
    if _is_forward_adjusted(frame):
        return "ADJUSTED_CONFIRMED"
    mode = _summary_adjust_mode(frame).strip().lower()
    if mode in {"none", "raw", "unadjusted", "不复权"}:
        return "UNADJUSTED_CONFIRMED"
    return "UNKNOWN"


__all__ = [
    "A3_TIMEFRAMES",
    "FactorEngine",
    "FactorSnapshot",
    "LEGACY_TIMEFRAMES",
    "MA_PERIODS",
    "OHLCVBar",
    "TechnicalFactorSnapshot",
    "TimeframeFactors",
]
