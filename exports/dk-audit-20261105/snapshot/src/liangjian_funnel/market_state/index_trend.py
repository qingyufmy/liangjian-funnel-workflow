"""Appendix A close-only index facts. No network, store or runtime consumer.

Inputs are explicitly normalized index rows, not stock symbols. The caller
must retain the source bytes identified by input_sha256. Missing clocks and
history produce DATA_BLOCK; another index/day never fills the gap.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, localcontext
import re
from zoneinfo import ZoneInfo

from .config import index_identity
from .contract import PitProof
from ..runtime.calendar import TradingCalendarError

TZ = ZoneInfo('Asia/Shanghai')


def number(value):
    if value is None or isinstance(value, bool):
        raise ValueError('INDEX_NUMBER_INVALID')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('INDEX_NUMBER_INVALID') from None
    if not result.is_finite() or result <= 0:
        raise ValueError('INDEX_NUMBER_INVALID')
    return result


def render(value, places=4):
    return None if value is None else format(value, f'.{places}f')


def _day(value):
    if not isinstance(value, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) is None:
        raise ValueError('INDEX_TRADE_DATE_INVALID')
    return date.fromisoformat(value)


def _rows(rows: Sequence[Mapping], *, item, source_id, target, calendar):
    result, previous = [], None
    for row in rows:
        if (not isinstance(row, Mapping) or row.get('thscode') != item['thscode']
                or row.get('source_id') != source_id or row.get('adjust_mode') != 'none'):
            raise ValueError('INDEX_ROW_IDENTITY_OR_SOURCE_MISMATCH')
        day = _day(row.get('trade_date'))
        if day > target or calendar.is_trading_day(day) is not True:
            raise ValueError('INDEX_FUTURE_OR_NON_SESSION_BAR')
        if previous is not None:
            if day <= previous:
                raise ValueError('INDEX_DUPLICATE_OR_UNSORTED_BAR')
            if calendar.next_trading_day(previous) != day:
                raise ValueError('INDEX_HISTORY_GAP')
        values = {name: number(row.get(name)) for name in ('open', 'high', 'low', 'close')}
        if values['low'] > min(values['open'], values['close']) or values['high'] < max(values['open'], values['close']):
            raise ValueError('INDEX_OHLC_INVALID')
        result.append(dict(trade_date=day, **values))
        previous = day
    return result


def append_snapshot_bar(rows, snapshot, *, item, source_id, target, calendar):
    """Only an adjacent, same-source close snapshot may supply the last day."""
    target = _day(target)
    history = _rows(rows, item=item, source_id=source_id, target=target, calendar=calendar)
    if history and history[-1]['trade_date'] == target:
        return [dict(row) for row in rows]
    if (not history or not isinstance(snapshot, Mapping) or snapshot.get('name') != item['name']
            or snapshot.get('thscode') != item['thscode'] or snapshot.get('source_id') != source_id
            or snapshot.get('trade_date') != target.isoformat() or snapshot.get('adjust_mode') != 'none'
            or calendar.next_trading_day(history[-1]['trade_date']) != target):
        raise ValueError('DAY_BAR_CONTINUITY_FAILED')
    provider = datetime.fromisoformat(snapshot['provider_timestamp'])
    close = datetime.combine(target, time(15), tzinfo=TZ)
    if (provider.utcoffset() is None or provider.astimezone(TZ).date() != target or provider < close
            or abs(number(snapshot.get('prev_close')) - history[-1]['close']) > Decimal('.01')):
        raise ValueError('DAY_BAR_CONTINUITY_FAILED')
    row = {name: snapshot[name] for name in ('thscode', 'source_id', 'adjust_mode', 'trade_date', 'open', 'high', 'low', 'close')}
    combined = [*rows, row]
    _rows(combined, item=item, source_id=source_id, target=target, calendar=calendar)
    return combined


def _ma(closes, period, lag=0):
    end = len(closes) - lag
    return None if end < period else sum(closes[end-period:end], Decimal(0)) / period


def _ratio(a, b):
    return None if a is None or b in (None, 0) else a / b - 1


def _matches(close, averages, slopes, spec):
    compare = lambda a, b, op: a > b if op == 'GT' else a < b
    n, op = spec['close_vs_ma']
    s, sop = spec['slope_vs_zero']
    if averages[n] is None or slopes[s] is None:
        return False
    if not compare(close, averages[n], op) or not compare(slopes[s], Decimal(0), sop):
        return False
    order = spec.get('ma_order', [])
    return all(compare(averages[a], averages[b], spec['comparison']) for a, b in zip(order, order[1:]))


def _weekly(rows, *, calendar, rules):
    groups = {}
    for row in rows:
        monday = row['trade_date'] - timedelta(days=row['trade_date'].weekday())
        groups.setdefault(monday, []).append(row)
    weeks, complete = [], []
    for monday, group in groups.items():
        sessions = [monday + timedelta(days=i) for i in range(7) if calendar.is_trading_day(monday + timedelta(days=i))]
        full = [row['trade_date'] for row in group] == sessions
        week = dict(first_day=group[0]['trade_date'].isoformat(), last_day=group[-1]['trade_date'].isoformat(),
            open=group[0]['open'], high=max(row['high'] for row in group), low=min(row['low'] for row in group),
            close=group[-1]['close'], partial=not full)
        weeks.append(week)
        if full:
            complete.append(week)
    closes = [row['close'] for row in complete]
    periods, lag = rules['ma_periods'], rules['slope_lag']
    ma = {n: _ma(closes, n) for n in periods}
    slope = {n: _ratio(ma[n], _ma(closes, n, lag)) for n in periods}
    delta = {n: None if ma[n] is None or _ma(closes, n, lag) is None else ma[n] - _ma(closes, n, lag) for n in periods}
    reasons = ['WEEKLY_TREND_HISTORY_INSUFFICIENT'] if ma[5] is None or ma[10] is None or delta[10] is None else []
    trend = None
    if not reasons:
        trend = next((name for name, spec in rules['trend'].items() if name != 'otherwise'
            and _matches(closes[-1], ma, delta, spec)), rules['trend']['otherwise'])
    latest = weeks[-1] if weeks else None
    return dict(status='DATA_BLOCK' if reasons else 'READY', reasons=reasons, trend=trend,
        partial=latest['partial'] if latest else None, complete_weeks=len(complete),
        ma={str(n): render(value, 2) for n, value in ma.items()},
        slope={str(n): render(value) for n, value in slope.items()},
        slope_delta={str(n): render(value) for n, value in delta.items()},
        field_reasons={f'MA{n}': 'WEEKLY_HISTORY_INSUFFICIENT' for n in periods if ma[n] is None},
        trend_authority=rules['authority'], trend_as_of=complete[-1]['last_day'] if complete else None,
        last_complete_at=complete[-1]['last_day'] if complete else None,
        current=None if latest is None else {k: render(v, 2) if isinstance(v, Decimal) else v for k, v in latest.items()})


def index_close_facts(key, dataset, *, definitions, calendar):
    """Compute a single explicit index, blocking only that index on bad input.

    dataset = {thscode,name,source_id,input_sha256,pit,rows}. Rows repeat the
    code/source/adjustment to detect accidental stock or mixed-source input.
    """
    item = definitions['registry']['indices'].get(key)
    if item is None:
        raise ValueError('INDEX_KEY_UNKNOWN')
    base = dict(key=key, **item, authority='SHADOW_ONLY', status='DATA_BLOCK', reasons=[], daily=None, weekly=None)
    if dataset is None:
        return {**base, 'reasons': ['INDEX_SOURCE_NOT_SUPPLIED']}
    try:
        index_identity(definitions['registry'], key, returned_code=dataset['thscode'], returned_name=dataset['name'])
        source = dataset['source_id']
        if not isinstance(source, str) or not source.strip() or re.fullmatch('[0-9a-f]{64}', dataset['input_sha256']) is None:
            raise ValueError('INDEX_SOURCE_PROOF_INVALID')
        pit = PitProof(**dataset['pit'])
        base.update(source_id=source, input_sha256=dataset['input_sha256'], pit=pit.model_dump())
        if pit.mode != 'CLOSE' or pit.missing_fields():
            raise ValueError('INDEX_CLOSE_PIT_INCOMPLETE')
        target = _day(pit.source_trade_date)
        close = datetime.combine(target, time(15), tzinfo=TZ)
        provider = datetime.fromisoformat(pit.provider_timestamp)
        if (datetime.fromisoformat(pit.market_close_at) != close or datetime.fromisoformat(pit.as_of) != close
                or provider < close or provider.astimezone(TZ).date() != target
                or datetime.fromisoformat(pit.capture_started_at) < close + timedelta(minutes=5)):
            raise ValueError('INDEX_CLOSE_PIT_INCONSISTENT')
        rows = _rows(dataset['rows'], item=item, source_id=source, target=target, calendar=calendar)
        rules = definitions['rules']['daily']
        if not rows or rows[-1]['trade_date'] != target:
            raise ValueError('INDEX_TARGET_DAY_MISSING')
        if len(rows) < rules['minimum_history_bars']:
            raise ValueError('HISTORY_INSUFFICIENT')
        with localcontext() as ctx:
            ctx.prec = 34
            closes = [row['close'] for row in rows]
            ma = {n: _ma(closes, n) for n in rules['ma_periods']}
            slope = {n: _ratio(ma[n], _ma(closes, n, rules['slope_lag'])) for n in ma}
            def trend(table):
                return next((name for name, spec in table.items() if name != 'otherwise'
                    and _matches(closes[-1], ma, slope, spec)), table['otherwise'])
            arrangement = rules['arrangement']['otherwise']
            for name, spec in rules['arrangement'].items():
                if name == 'otherwise':
                    continue
                order = spec['ma_order']
                if all((ma[a] > ma[b] if spec['comparison'] == 'GT' else ma[a] < ma[b]) for a, b in zip(order, order[1:])):
                    arrangement = name
                    break
            low, high = min(r['low'] for r in rows[-20:]), max(r['high'] for r in rows[-20:])
            pos = None if high == low else (closes[-1] - low) / (high - low)
            daily = dict(close=render(closes[-1], 2), ma={str(n): render(v, 2) for n, v in ma.items()},
                slope={str(n): render(slope[n]) for n in rules['distance_periods']},
                dist={str(n): render(_ratio(closes[-1], ma[n])) for n in rules['distance_periods']},
                ret={str(n): render(_ratio(closes[-1], closes[-n-1])) for n in rules['return_periods']},
                pos20=render(pos), arrangement=arrangement, trend_short=trend(rules['trend_short']),
                trend_mid=trend(rules['trend_mid']), field_reasons={'pos20': 'ZERO_RANGE'} if pos is None else {})
            weekly = _weekly(rows, calendar=calendar, rules=definitions['rules']['weekly'])
        return {**base, 'status': 'READY' if weekly['status'] == 'READY' else 'DAILY_READY_WEEKLY_PARTIAL', 'reasons': weekly['reasons'], 'daily': daily, 'weekly': weekly,
            'history_bars': len(rows), 'definition_sha256': definitions['rules']['config_sha256'],
            'registry_sha256': definitions['registry']['config_sha256']}
    except (ValueError, TypeError, KeyError, InvalidOperation, TradingCalendarError) as exc:
        reason = exc.reason_code if isinstance(exc,TradingCalendarError) else str(exc) if type(exc) is ValueError and re.fullmatch('[A-Z0-9_]{1,100}', str(exc)) else 'INDEX_INPUT_REJECTED'
        return {**base, 'reasons': [reason]}


def market_close_facts(datasets, *, definitions, calendar):
    if not isinstance(datasets, Mapping) or set(datasets) - set(definitions['rules']['indices']):
        raise ValueError('INDEX_BATCH_IDENTITY_INVALID')
    facts = {key: index_close_facts(key, datasets.get(key), definitions=definitions, calendar=calendar)
             for key in definitions['rules']['indices']}
    voting = [facts[key] for key in ('SSE', 'HS300', 'CSI1000')]
    # A mixed-date batch is never a same-close market comparison.
    dates = {row['pit']['source_trade_date'] for row in facts.values() if row.get('daily')}
    comparable = all(row['daily'] is not None for row in voting) and len(dates) == 1
    votes = {name: sum(row['daily']['trend_short'] == name for row in voting) for name in ('UP', 'DOWN', 'RANGE')} if comparable else None
    return dict(schema_version='market-index-close-facts/1', authority='SHADOW_ONLY', indices=facts,
        market_trend_votes=votes,
        style_divergence=(facts['HS300']['daily']['trend_short'] != facts['CSI1000']['daily']['trend_short']) if comparable else None,
        comparison_status='READY' if comparable else 'DATA_BLOCK',
        comparison_reasons=[] if comparable else ['INDEX_COMPARISON_MISSING_OR_MIXED_DATE'])
