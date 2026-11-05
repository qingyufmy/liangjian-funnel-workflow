"""Pure parsing and cross-source checks under T2-1124; no HTTP calls here."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
EASTMONEY_URL = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
TENCENT_URL = 'https://ifzq.gtimg.cn/appstock/app/kline/mkline'
EASTMONEY_PARAMS = dict(fields1='f1,f2,f3,f4,f5,f6',
    fields2='f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
    secid='0.002463', klt='5', fqt='0', beg='0', end='20500000')
TENCENT_PARAMS = {'param': 'sz002463,m5,,320'}


def _number(value: Any, *, positive=False) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError('DK_SOURCE_NUMBER_INVALID')
    try:
        parsed = Decimal(str(value))
    except (ValueError, InvalidOperation):
        raise ValueError('DK_SOURCE_NUMBER_INVALID') from None
    if not parsed.is_finite() or parsed < 0 or (positive and parsed == 0):
        raise ValueError('DK_SOURCE_NUMBER_INVALID')
    return parsed


def parse_eastmoney(payload: Mapping) -> list[dict]:
    """Unadjusted equity rows: source lots→shares, reported amount kept in CNY.

    The volume normalization is provisional until exact Tencent OHLCV overlap
    proves it. Parsing never marks the source usable by itself.
    """
    data = payload.get('data')
    if ((payload.get('rc') is not None and (type(payload['rc']) is not int or payload['rc'] != 0))
            or not isinstance(data, Mapping)
            or data.get('code') != '002463' or data.get('name') != '沪电股份'
            or not isinstance(data.get('klines'), list)):
        raise ValueError('DK_EASTMONEY_IDENTITY_OR_RESPONSE_INVALID')
    result, seen = [], set()
    for item in data['klines']:
        if not isinstance(item, str):
            raise ValueError('DK_EASTMONEY_ROW_INVALID')
        fields = item.split(',')
        if len(fields) < 7:
            raise ValueError('DK_EASTMONEY_ROW_INVALID')
        end = datetime.strptime(fields[0], '%Y-%m-%d %H:%M').replace(tzinfo=TZ)
        if end in seen:
            raise ValueError('DK_SOURCE_DUPLICATE_BAR')
        seen.add(end)
        values = dict(zip(('open', 'close', 'high', 'low'), (_number(v, positive=True) for v in fields[1:5])))
        volume = _number(fields[5]) * 100
        # Missing reported amount remains unknown; it is never estimated.
        amount = None if fields[6] in ('', '-', 'null') else _number(fields[6])
        result.append(dict(symbol='002463.SZ', interval='5m', bar_end=end, **values,
            volume=volume, volume_unit='shares', amount=amount,
            amount_kind='reported' if amount is not None else 'legacy_unspecified', amount_unit='CNY',
            source_id='EASTMONEY_EQUITY_M5_UNADJUSTED', adjust_mode='none', evidence_kind='MARKET_BAR'))
    return sorted(result, key=lambda row: row['bar_end'])


def parse_tencent(payload: Mapping) -> list[dict]:
    # Use the actual equity normalizer to retain source units and declared
    # OHLC-estimate semantics. Do not use it for any index symbol.
    from ..data.tencent_minute import TencentIntradayAdapter
    if payload.get('code') is not None and (type(payload['code']) is not int or payload['code'] != 0):
        raise ValueError('DK_TENCENT_RESPONSE_INVALID')
    data = payload.get('data')
    if not isinstance(data, Mapping) or 'sz002463' not in data:
        raise ValueError('DK_TENCENT_IDENTITY_MISMATCH')
    item = data['sz002463']
    if not isinstance(item, Mapping) or not isinstance(item.get('m5'), list):
        raise ValueError('DK_TENCENT_ROWS_MISSING')
    rows = []
    for raw in item['m5']:
        rows.append(TencentIntradayAdapter._bar('002463.SZ', '5m', raw).model_dump())
    if len({row['bar_end'] for row in rows}) != len(rows):
        raise ValueError('DK_SOURCE_DUPLICATE_BAR')
    return sorted(rows, key=lambda row: row['bar_end'])


def compare_overlap(eastmoney: Sequence[Mapping], tencent: Sequence[Mapping]) -> dict:
    """Every row in the shared observed span must exist and match OHLCV."""
    def keyed(rows):
        result = {}
        for row in rows:
            end = row['bar_end']
            if end in result:
                raise ValueError('DK_SOURCE_DUPLICATE_BAR')
            if row.get('symbol') != '002463.SZ' or row.get('interval') != '5m':
                raise ValueError('DK_OVERLAP_IDENTITY_INVALID')
            result[end] = row
        return result
    left, right = keyed(eastmoney), keyed(tencent)
    if not left or not right:
        return dict(status='UNPROVEN', reason='EMPTY_SOURCE', compared=0, differences=[])
    start, end = max(min(left), min(right)), min(max(left), max(right))
    if start > end:
        return dict(status='UNPROVEN', reason='NO_OVERLAP', compared=0, differences=[])
    stamps = sorted({at for at in left if start <= at <= end} | {at for at in right if start <= at <= end})
    differences, matched = [], 0
    for at in stamps:
        a, b = left.get(at), right.get(at)
        if a is None or b is None:
            differences.append(dict(bar_end=at.isoformat(), field='BAR', reason='MISSING_ON_ONE_SOURCE'))
            continue
        fields = []
        if a.get('volume_unit') != 'shares' or b.get('volume_unit') != 'shares':
            fields.append('volume_unit')
        for field in ('open', 'high', 'low', 'close', 'volume'):
            if _number(a.get(field), positive=field != 'volume') != _number(b.get(field), positive=field != 'volume'):
                fields.append(field)
        if fields:
            differences.append(dict(bar_end=at.isoformat(), fields=fields,
                eastmoney={key: str(a.get(key)) for key in fields}, tencent={key: str(b.get(key)) for key in fields}))
        else:
            matched += 1
    return dict(status='MATCH' if stamps and not differences else 'MISMATCH',
        start=start.isoformat(), end=end.isoformat(), compared=len(stamps), matched=matched, differences=differences,
        amount_compared=False, authority='OHLCV_ONLY_AMOUNT_NEVER_SUBSTITUTED')


def warmup_n_limit(bars_before_evaluation: int, *, requested_max=60) -> dict:
    if type(bars_before_evaluation) is not int or bars_before_evaluation < 0 or type(requested_max) is not int or not 5 <= requested_max <= 60:
        raise ValueError('DK_WARMUP_ARGUMENT_INVALID')
    upper = min(requested_max, bars_before_evaluation // 3)
    return dict(status='AVAILABLE' if upper >= 5 else 'HISTORY_INSUFFICIENT',
        minimum_n=5, maximum_n=upper if upper >= 5 else None, available_preheat=bars_before_evaluation,
        required_preheat_rule='AT_LEAST_3_N', requested_maximum_n=requested_max,
        truncated=upper < requested_max, no_solution_claim=False)
