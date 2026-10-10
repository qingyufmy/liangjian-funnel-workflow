"""Outcome-only frozen price-limit evidence; never a trading rule extension.

Ticker prefixes check consistency with an explicitly evidenced board; they do
not establish ordinary/ST or IPO status. No provider, Settings or store is used.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import lru_cache
from zoneinfo import ZoneInfo

from ...runtime.calendar import ExchangeTradingCalendar, TradingCalendarError
from ...runtime.stock_trading_rules import stock_trading_rules

SH = ZoneInfo('Asia/Shanghai')
RULE_FROM = date(2026, 7, 6)
# This research version has only reviewed rules through this date, not eternity.
RULE_REVIEWED_THROUGH = date(2026, 10, 10)
SSE_RULE = 'https://www.sse.com.cn/lawandrules/sselawsrules2025/fund/trading/c/c_20260424_10817739.shtml'
SZSE_RULE = 'https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf'
RATES = {'SSE_MAIN': Decimal('.10'), 'SZSE_MAIN': Decimal('.10'),
         'CHINEXT': Decimal('.20'), 'STAR': Decimal('.20')}
PATHS = (('upper_limit', 'limit_up', ('price_limits', 'upper'), ('price_limit', 'up')),
         ('lower_limit', 'limit_down', ('price_limits', 'lower'), ('price_limit', 'down')))


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     default=str).encode()).hexdigest()


def _decimal(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return result if result.is_finite() and result > 0 else None


def _stamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('AWARE_TIMESTAMP_REQUIRED')
    return result.astimezone(SH)


def _plan_day(plan):
    explicit = plan.get('price_limit_trade_date')
    if explicit is not None:
        return str(explicit)
    # Legacy explicit limits only belong to a proved single-day plan, not to
    # arbitrary subsequent bars or a plan with one missing validity boundary.
    try:
        start = _stamp(plan.get('valid_from')).date()
        end = _stamp(plan.get('valid_until', plan.get('expires_at'))).date()
        return str(start) if start == end else None
    except (TypeError, ValueError):
        return None


def _explicit(row):
    values, invalid, conflict = [], False, False
    for paths in PATHS:
        aliases = []
        for path in paths:
            if isinstance(path, str):
                value = row.get(path)
            else:
                nested = row.get(path[0])
                value = nested.get(path[1]) if isinstance(nested, Mapping) else None
            if value is not None:
                parsed = _decimal(value)
                invalid |= parsed is None
                if parsed is not None:
                    aliases.append(parsed)
        conflict |= len(set(aliases)) > 1
        values.append(aliases[0] if aliases else None)
    return values, invalid, conflict


@lru_cache(maxsize=512)
def _normal_listing_day(listing, day):
    calendar = ExchangeTradingCalendar()
    if not calendar.is_trading_day(day) or listing > day:
        return False
    sixth_session_threshold = day
    for _ in range(5):
        sixth_session_threshold = calendar.previous_trading_day(sixth_session_threshold)
    return listing <= sixth_session_threshold


def _derive(evidence, *, symbol, at):
    if not isinstance(evidence, Mapping):
        return None, 'FROZEN_IDENTITY_AND_PRECLOSE_MISSING'
    day = at.date()
    try:
        board = evidence.get('board')
        expected_board = ('STAR' if symbol.startswith('688') and symbol.endswith('.SH') else
                          'SSE_MAIN' if symbol.startswith('60') and symbol.endswith('.SH') else
                          'CHINEXT' if symbol.startswith(('300', '301')) and symbol.endswith('.SZ') else
                          'SZSE_MAIN' if symbol.startswith(('000', '001', '002', '003')) and symbol.endswith('.SZ') else None)
        if (evidence.get('symbol') != symbol or evidence.get('trade_date') != str(day)
                or board not in RATES or board != expected_board):
            return None, 'IDENTITY_DATE_OR_BOARD_UNPROVEN'
        if (evidence.get('security_type') != 'CASH_A_SHARE'
                or evidence.get('security_status') != 'ORDINARY'
                or evidence.get('is_st') is not False or evidence.get('limit_regime') != 'NORMAL'):
            return None, 'ORDINARY_NORMAL_LIMIT_REGIME_UNPROVEN'
        if not _normal_listing_day(date.fromisoformat(str(evidence.get('listing_date'))), day):
            return None, 'NORMAL_LISTING_PERIOD_UNPROVEN'
        if (evidence.get('rule_effective_from') != str(RULE_FROM)
                or not RULE_FROM <= day <= RULE_REVIEWED_THROUGH):
            return None, 'RULE_DATE_UNPROVEN'
        observed = _stamp(evidence.get('observed_at'))
        if (observed.date() != day or observed > at
                or not isinstance(evidence.get('source_ref'), str) or not evidence['source_ref'].strip()
                or not re.fullmatch('[0-9a-fA-F]{64}', str(evidence.get('source_input_sha256', '')))):
            return None, 'FROZEN_SOURCE_BINDING_UNPROVEN'
        preclose = _decimal(evidence.get('preclose'))
        tick = Decimal(str(stock_trading_rules(symbol).tick))
        if (preclose is None or preclose % tick != 0
                or evidence.get('preclose_basis') != 'EXCHANGE_DISPLAYED_PRECLOSE'):
            return None, 'EXCHANGE_PRECLOSE_UNPROVEN'
        rate = RATES[board]
        upper = (preclose * (1 + rate)).quantize(tick, rounding=ROUND_HALF_UP)
        lower = (preclose * (1 - rate)).quantize(tick, rounding=ROUND_HALF_UP)
        if abs(upper - preclose) < tick:
            upper = preclose + tick
        if abs(lower - preclose) < tick:
            lower = preclose - tick
        upper, lower = max(tick, upper), max(tick, lower)
        return {'upper': float(upper), 'lower': float(lower), 'trade_date': str(day),
                'symbol': symbol, 'basis': 'DERIVED_FROM_PRECLOSE_AND_BOARD_RULE',
                'input_sha256': _hash(dict(evidence)),
                'source_input_sha256': evidence['source_input_sha256'],
                'source_ref': evidence['source_ref'], 'board': board,
                'preclose': str(preclose), 'rate': str(rate), 'tick': str(tick),
                'rounding': 'DECIMAL_ROUND_HALF_UP_MIN_ONE_TICK_FLOOR_ONE_TICK',
                'rule_effective_from': str(RULE_FROM),
                'rule_reviewed_through': str(RULE_REVIEWED_THROUGH),
                'rule_source': SSE_RULE if board in {'SSE_MAIN', 'STAR'} else SZSE_RULE}, None
    except (TypeError, ValueError, InvalidOperation, TradingCalendarError):
        return None, 'FROZEN_PRICE_LIMIT_EVIDENCE_INVALID'


def resolve_price_limits(raw: Mapping, *, symbol: str, at: datetime,
                         fallback_plan: Mapping | None = None) -> dict:
    """Resolve same-day explicit prices or strictly evidenced ordinary limits.

    An exported derived annotation is never trusted as input; it is recomputed
    from price_limit_evidence. Partial legs cannot silently mix two sources.
    """
    at = _stamp(at)
    day = str(at.date())
    result = {'status': 'UNKNOWN', 'upper': None, 'lower': None,
              'reason_code': 'PRICE_LIMITS_UNKNOWN', 'trade_date': day,
              'symbol': symbol, 'derived_limit_prices': None}
    # Even explicit prices cannot lend authority to a different stock/day.
    if raw.get('symbol', symbol) != symbol:
        result['evidence_reason'] = 'RAW_LIMIT_SYMBOL_MISMATCH'
        return result
    try:
        for key in ('bar_end', 'timestamp', 'datetime'):
            if raw.get(key) is not None and _stamp(raw[key]).date() != at.date():
                result['evidence_reason'] = 'RAW_LIMIT_BAR_DATE_MISMATCH'
                return result
    except (TypeError, ValueError):
        result['evidence_reason'] = 'RAW_LIMIT_BAR_DATE_INVALID'
        return result
    if any(raw.get(key) is not None and str(raw[key]) != day for key in ('bar_date', 'trade_date')):
        result['evidence_reason'] = 'RAW_LIMIT_BAR_DATE_MISMATCH'
        return result
    candidates = []
    if raw.get('price_limit_trade_date', day) != day:
        result['evidence_reason'] = 'EXPLICIT_LIMIT_DATE_MISMATCH'
        return result
    candidates.append(raw)
    if (fallback_plan is not None and fallback_plan.get('symbol', symbol) == symbol
            and _plan_day(fallback_plan) == day):
        candidates.append(fallback_plan)
    derived = []
    explicit = []
    invalid = conflict = False
    for candidate in candidates:
        pair, bad, mismatched = _explicit(candidate)
        explicit.append(pair)
        invalid |= bad
        conflict |= mismatched
        computed, why = _derive(candidate.get('price_limit_evidence'), symbol=symbol, at=at)
        if computed:
            derived.append(computed)
        elif candidate.get('price_limit_evidence') is not None:
            result['evidence_reason'] = why
    if derived:
        result['derived_limit_prices'] = derived[0]
        explicit.extend([[_decimal(d['upper']), _decimal(d['lower'])] for d in derived])
    for leg in range(2):
        conflict |= len({pair[leg] for pair in explicit if pair[leg] is not None}) > 1
    if conflict:
        result.update(status='CONFLICT', reason_code='PRICE_LIMITS_CONFLICT')
        return result
    complete = next((pair for pair in explicit if all(v is not None for v in pair)), None)
    if invalid or complete is None:
        result.setdefault('evidence_reason', 'FROZEN_IDENTITY_AND_PRECLOSE_MISSING')
        return result
    upper, lower = complete
    try:
        tick = Decimal(str(stock_trading_rules(symbol).tick))
    except ValueError:
        return result
    try:
        valid = 0 < lower < upper and not upper % tick and not lower % tick
    except InvalidOperation:
        valid = False
    if not valid:
        result['evidence_reason'] = 'EXPLICIT_LIMIT_PRICE_INVALID'
        return result
    result.update(status='KNOWN', upper=float(upper), lower=float(lower), reason_code=None,
                  basis='DERIVED_FROM_PRECLOSE_AND_BOARD_RULE' if derived else 'EXPLICIT_FROZEN_SAME_DAY',
                  input_sha256=_hash({'symbol': symbol, 'trade_date': day,
                                     'explicit': explicit, 'derived': derived}))
    return result


__all__ = ['resolve_price_limits']
