"""Explicit Tencent index request identity and pure 5m response parsing.

This module never imports the equity adapter or map_symbol. Index volume
units remain unverified and amount is absent; SSL uses OHLC only.
"""
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation

from .bars15m import TZ
from .ssl import CANONICAL_SOURCE


def index_request(registry, key):
    item = registry.get('indices', {}).get(key)
    if item is None:
        raise ValueError('INDEX_IDENTITY_MISMATCH')
    return dict(url='https://ifzq.gtimg.cn/appstock/app/kline/mkline',
        params={'param': f"{item['tencent_code']},m5,,320"})


def parse_index_5m(payload, *, registry, key):
    item = registry.get('indices', {}).get(key)
    if item is None or not isinstance(payload, Mapping) or type(payload.get('code')) is not int or payload['code'] != 0:
        raise ValueError('INDEX_IDENTITY_OR_RESPONSE_INVALID')
    provider = item['tencent_code']
    data = payload.get('data')
    if not isinstance(data, Mapping) or set(data) != {provider} or not isinstance(data[provider], Mapping):
        raise ValueError('INDEX_IDENTITY_MISMATCH')
    value = data[provider]
    qt = value.get('qt')
    quote = qt.get(provider) if isinstance(qt, Mapping) else None
    if (not isinstance(quote, (list, tuple)) or len(quote) < 3
            or quote[1] != item['name'] or quote[2] != item['thscode'][:6]):
        raise ValueError('INDEX_IDENTITY_MISMATCH')
    if not isinstance(value.get('m5'), list) or len(value['m5']) > 320:
        raise ValueError('INDEX_MINUTE_ROWS_INVALID')
    result, previous = [], None
    for row in value['m5']:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            raise ValueError('INDEX_MINUTE_ROW_INVALID')
        try:
            end = datetime.strptime(row[0], '%Y%m%d%H%M').replace(tzinfo=TZ)
            numbers = [Decimal(str(v)) for v in row[1:5]]
        except (TypeError, ValueError, InvalidOperation):
            raise ValueError('INDEX_MINUTE_ROW_INVALID') from None
        if (any(not v.is_finite() or v <= 0 for v in numbers)
                or previous is not None and end <= previous):
            raise ValueError('INDEX_MINUTE_ROW_INVALID')
        o, c, h, low = numbers
        if low > min(o, c) or h < max(o, c):
            raise ValueError('INDEX_MINUTE_ROW_INVALID')
        result.append(dict(symbol=item['thscode'], interval='5m', bar_end=end,
            open=o, high=h, low=low, close=c, volume=None, volume_unit='legacy_unspecified',
            amount=None, amount_unit=None, amount_kind='legacy_unspecified',
            source_id=CANONICAL_SOURCE, adjust_mode='none', evidence_kind='MARKET_BAR'))
        previous = end
    return result
