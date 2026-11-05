"""Strict offline 5m-to-15m composition, without provider or stock mapping.

The caller supplies normalized original bars, a production trading calendar,
and the session close. Unknown/half-day layouts fail instead of inventing bars.
Reported amount and OHLC estimates remain separate through aggregation.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import re
from typing import Any
from zoneinfo import ZoneInfo


TZ = ZoneInfo('Asia/Shanghai')
ENDS = ('09:45', '10:00', '10:15', '10:30', '10:45', '11:00', '11:15', '11:30',
        '13:15', '13:30', '13:45', '14:00', '14:15', '14:30', '14:45', '15:00')


@dataclass(frozen=True)
class Bar15m:
    symbol: str
    bar_end: datetime
    status: str
    expected_5m: tuple[datetime, ...]
    present_5m: tuple[datetime, ...]
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume_shares: Decimal | None = None
    reported_amount_cny: Decimal | None = None
    estimated_amount_cny: Decimal | None = None
    source_id: str | None = None
    adjust_mode: str | None = None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class Day15m:
    symbol: str
    trade_date: date
    bars: tuple[Bar15m, ...]
    excluded_auction_count: int


def _time(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if (not isinstance(value, datetime) or value.utcoffset() is None
            or value.second or value.microsecond):
        raise ValueError('M15_BAR_TIMESTAMP_INVALID')
    return value.astimezone(TZ)


def _clock(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('M15_OBSERVATION_CLOCK_INVALID')
    return value.astimezone(TZ)


def _number(value: Any, *, positive=False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError('M15_BAR_NUMBER_INVALID')
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation):
        raise ValueError('M15_BAR_NUMBER_INVALID') from None
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError('M15_BAR_NUMBER_INVALID')
    return result


def compose_15m(rows: Sequence[Mapping[str, Any]], *, symbol: str, trade_date: date,
                observed_at: datetime, market_close_at: datetime, calendar) -> Day15m:
    """Return all sixteen slots; no two-row fill or unfinished OHLC is emitted.

    A missing volume declaration does not invalidate valid price bars, but it
    cannot establish anchor volume. Only amount_kind=reported establishes amount.
    Numeric comparison is Decimal and unrounded; no source data is changed.
    """
    if (not isinstance(symbol, str) or re.fullmatch(r'\d{6}\.(SH|SZ)', symbol) is None
            or isinstance(trade_date, datetime) or not isinstance(trade_date, date)):
        raise ValueError('M15_IDENTITY_INVALID')
    now, session_close = _clock(observed_at), _time(market_close_at)
    if calendar.is_trading_day(trade_date) is not True:
        raise ValueError('M15_NON_TRADING_DAY')
    if session_close != datetime.combine(trade_date, time(15), tzinfo=TZ):
        raise ValueError('M15_SESSION_LAYOUT_UNSUPPORTED')
    if now.date() < trade_date:
        raise ValueError('M15_OBSERVATION_BEFORE_SESSION')
    ends = tuple(datetime.combine(trade_date, time.fromisoformat(end), tzinfo=TZ) for end in ENDS)
    expected = {end - timedelta(minutes=offset) for end in ends for offset in (10, 5, 0)}
    auction = datetime.combine(trade_date, time(9, 30), tzinfo=TZ)
    bars: dict[datetime, dict] = {}
    auction_count = 0
    identity = None
    all_times = set()
    for row in rows:
        if (not isinstance(row, Mapping) or row.get('symbol') != symbol or row.get('interval') != '5m'
                or row.get('evidence_kind') != 'MARKET_BAR'):
            raise ValueError('M15_SOURCE_BAR_IDENTITY_INVALID')
        end = _time(row.get('bar_end'))
        if end.date() != trade_date or end > now:
            raise ValueError('M15_FUTURE_OR_OTHER_DAY_BAR')
        if end in all_times:
            raise ValueError('M15_DUPLICATE_BAR')
        all_times.add(end)
        if end == auction:
            auction_count += 1
            continue
        if end not in expected:
            raise ValueError('M15_OUTSIDE_SESSION_GRID')
        source, adjust = row.get('source_id'), row.get('adjust_mode')
        if (not isinstance(source, str) or not source.strip() or adjust not in {'none', 'raw', 'forward'}):
            raise ValueError('M15_SOURCE_OR_ADJUSTMENT_MISSING')
        current = (source, adjust)
        if identity is not None and identity != current:
            raise ValueError('M15_MIXED_SOURCE_OR_ADJUSTMENT')
        identity = current
        values = {key: _number(row.get(key), positive=True) for key in ('open', 'high', 'low', 'close')}
        if (values['low'] > min(values['open'], values['close'])
                or values['high'] < max(values['open'], values['close']) or values['low'] > values['high']):
            raise ValueError('M15_OHLC_INVALID')
        volume_unit, amount_kind = row.get('volume_unit', 'legacy_unspecified'), row.get('amount_kind', 'legacy_unspecified')
        if (volume_unit not in {'shares', 'legacy_unspecified'}
                or amount_kind not in {'reported', 'ohlc_estimate', 'legacy_unspecified'}):
            raise ValueError('M15_UNIT_OR_AMOUNT_KIND_INVALID')
        volume = None if row.get('volume') is None else _number(row['volume'])
        amount = None if row.get('amount') is None else _number(row['amount'])
        amount_unit = row.get('amount_unit')
        if amount_unit not in {None, 'CNY'}:
            raise ValueError('M15_AMOUNT_UNIT_UNSUPPORTED')
        bars[end] = dict(values, volume=volume, amount=amount, volume_unit=volume_unit,
                         amount_kind=amount_kind, amount_unit=amount_unit,
                         source_id=source, adjust_mode=adjust)
    result = []
    for end in ends:
        parts = tuple(end - timedelta(minutes=offset) for offset in (10, 5, 0))
        present = tuple(part for part in parts if part in bars)
        if now < end + timedelta(seconds=60):
            result.append(Bar15m(symbol, end, 'NOT_CLOSED', parts, present, reasons=('CLOSE_DELAY_NOT_MET',)))
            continue
        if len(present) != 3:
            result.append(Bar15m(symbol, end, 'INCOMPLETE', parts, present, reasons=('MISSING_5M_PART',)))
            continue
        group = [bars[part] for part in parts]
        volume = (sum((b['volume'] for b in group), Decimal(0)) if all(
            b['volume_unit'] == 'shares' and b['volume'] is not None for b in group) else None)
        reported = (sum((b['amount'] for b in group), Decimal(0)) if all(
            b['amount_kind'] == 'reported' and b['amount_unit'] == 'CNY'
            and b['amount'] is not None for b in group) else None)
        estimate = (sum((b['amount'] for b in group), Decimal(0)) if all(
            b['amount_kind'] == 'ohlc_estimate' and b['amount_unit'] == 'CNY'
            and b['amount'] is not None for b in group) else None)
        reasons = []
        if volume is None:
            reasons.append('VOLUME_UNIT_OR_VALUE_UNVERIFIED')
        if reported is None:
            reasons.append('REPORTED_AMOUNT_UNAVAILABLE')
        if any(b['amount_unit'] is None for b in group):
            reasons.append('AMOUNT_UNIT_UNVERIFIED')
        result.append(Bar15m(symbol, end, 'CLOSED', parts, present,
            open=group[0]['open'], high=max(b['high'] for b in group), low=min(b['low'] for b in group),
            close=group[-1]['close'], volume_shares=volume, reported_amount_cny=reported,
            estimated_amount_cny=estimate, source_id=group[0]['source_id'],
            adjust_mode=group[0]['adjust_mode'], reasons=tuple(reasons)))
    return Day15m(symbol, trade_date, tuple(result), auction_count)


def dk_reference_anchor(day: Day15m) -> dict:
    """Exact reported differences only; no estimate is accepted as actual amount."""
    end = datetime(2026, 10, 26, 14, 45, tzinfo=TZ)
    if day.symbol != '002463.SZ' or day.trade_date != end.date():
        raise ValueError('DK_ANCHOR_IDENTITY_MISMATCH')
    bar = next((bar for bar in day.bars if bar.bar_end == end), None)
    if bar is None or bar.status != 'CLOSED':
        return dict(status='DATA_BLOCK', reason='ANCHOR_BAR_NOT_CLOSED_OR_INCOMPLETE')
    expected_volume, expected_amount = Decimal('2389400'), Decimal('236399200')
    amount_delta = None if bar.reported_amount_cny is None else bar.reported_amount_cny - expected_amount
    volume_delta = None if bar.volume_shares is None else bar.volume_shares - expected_volume
    close_delta = bar.close - Decimal('98.500')
    volume_passed = volume_delta == 0
    amount_status = 'UNPROVEN' if amount_delta is None else 'MATCH' if amount_delta == 0 else 'MISMATCH'
    status = ('EXACT_MATCH' if volume_passed and amount_delta == 0 and close_delta == 0 else
              'VOLUME_CONFIRMED_AMOUNT_UNPROVEN' if volume_passed and amount_delta is None else
              'VOLUME_CONFIRMED_WITH_DIFFERENCES' if volume_passed else 'NOT_CONFIRMED')
    return dict(status=status, volume_hard_check_passed=volume_passed,
        amount_status=amount_status, amount_note='额未证' if amount_delta is None else '仅比较reported成交额',
        bar_end=end.isoformat(), volume_delta_shares=volume_delta, reported_amount_delta_cny=amount_delta,
        close_delta=close_delta, estimated_amount_cny=bar.estimated_amount_cny, adjust_mode=bar.adjust_mode,
        reason='T2_1124_VOLUME_HARD_CHECK_AMOUNT_SEPARATE_NOT_SIGNAL_CALIBRATION')
