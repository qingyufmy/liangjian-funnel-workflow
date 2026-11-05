"""Tencent-only raw daily index points and provider-owned identity/clocks.

The endpoint convention is based on the installed akshare Tencent index
adapter, using an empty adjustment field and only the unadjusted `day` key.
No stock mapper, forward-adjusted fallback, paging or source mixing.
"""
from collections.abc import Mapping
from datetime import datetime, time
from decimal import Decimal
import json
import re

from .bars15m import TZ
from .index_trend import number

DAILY_URL = 'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get'
QUOTE_URL = 'https://qt.gtimg.cn/q'
DAILY_SOURCE = 'TENCENT:proxy.finance.qq.com:INDEX_DAY_RAW'


def daily_request(registry, key):
    item = registry.get('indices', {}).get(key)
    if item is None:
        raise ValueError('INDEX_IDENTITY_MISMATCH')
    return dict(url=DAILY_URL, params={'param': f"{item['tencent_code']},day,,,320,"})


def parse_quote_fields(fields, *, item):
    if (not isinstance(fields, (list, tuple)) or len(fields) <= 30
            or fields[1] != item['name'] or fields[2] != item['thscode'][:6]
            or not isinstance(fields[30], str) or re.fullmatch(r'\d{14}', fields[30]) is None):
        raise ValueError('INDEX_IDENTITY_OR_PROVIDER_CLOCK_MISMATCH')
    stamp = datetime.strptime(fields[30], '%Y%m%d%H%M%S').replace(tzinfo=TZ)
    return dict(thscode=item['thscode'], name=fields[1], close=str(number(fields[3])),
        previous_close=str(number(fields[4])), provider_timestamp=stamp.isoformat())


def parse_quote_reply(raw, *, registry):
    text = raw.decode('gb18030', errors='strict')
    result = {}
    for key, item in registry['indices'].items():
        match = re.search(r'\bv_'+re.escape(item['tencent_code'])+r'="([^"]*)";', text)
        if match is None:
            raise ValueError('INDEX_QUOTE_IDENTITY_MISSING')
        result[key] = parse_quote_fields(match.group(1).split('~'), item=item)
    return result


def parse_daily(payload, *, registry, key, trade_date, input_sha256, capture_started_at, capture_completed_at,
                independent_quote=None):
    item = registry.get('indices', {}).get(key)
    if (item is None or not isinstance(payload, Mapping) or type(payload.get('code')) is not int
            or payload['code'] != 0 or not isinstance(payload.get('data'), Mapping)
            or set(payload['data']) != {item['tencent_code']}):
        raise ValueError('INDEX_DAILY_IDENTITY_OR_RESPONSE_INVALID')
    data = payload['data'][item['tencent_code']]
    # Actual 2026-10-30 reply has320 preceding points plus today's close.
    # Keep every source row; the downstream calendar/duplicate checks still
    # validate the entire321-row sequence, with no truncation or deduplication.
    if not isinstance(data, Mapping) or not isinstance(data.get('day'), list) or len(data['day']) > 321:
        raise ValueError('INDEX_UNADJUSTED_DAY_ROWS_REQUIRED')
    qt = data.get('qt', {})
    fields = qt.get(item['tencent_code']) if isinstance(qt, Mapping) else None
    quote = parse_quote_fields(fields, item=item) if fields is not None else independent_quote
    if (not isinstance(quote, Mapping) or quote.get('thscode') != item['thscode'] or quote.get('name') != item['name']):
        raise ValueError('INDEX_DAILY_PROVIDER_IDENTITY_MISSING')
    close = datetime.combine(trade_date, time(15), tzinfo=TZ)
    provider = datetime.fromisoformat(quote['provider_timestamp'])
    if provider.utcoffset() is None or provider < close or provider.astimezone(TZ).date() != trade_date:
        raise ValueError('INDEX_DAILY_PROVIDER_NOT_CLOSED')
    rows = []
    for row in data['day']:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            raise ValueError('INDEX_DAILY_ROW_INVALID')
        o, c, h, low = (number(value) for value in row[1:5])
        rows.append(dict(thscode=item['thscode'], source_id=DAILY_SOURCE, adjust_mode='none',
            trade_date=row[0], open=str(o), close=str(c), high=str(h), low=str(low)))
    if not rows or rows[-1]['trade_date'] != trade_date.isoformat():
        raise ValueError('INDEX_DAILY_TARGET_DAY_MISSING')
    if Decimal(rows[-1]['close']) != number(quote['close']):
        raise ValueError('INDEX_DAILY_QUOTE_CLOSE_MISMATCH')
    return dict(thscode=item['thscode'], name=item['name'], source_id=DAILY_SOURCE,
        input_sha256=input_sha256, normalized_encoding='EXPLICIT_TENCENT_DAY_OHLC_FIELDS', rows=rows,
        requested_history_count=320, returned_row_count=len(rows), includes_extra_current_day=len(rows)==321,
        pit=dict(source_trade_date=trade_date.isoformat(), target_trade_date=trade_date.isoformat(), mode='CLOSE',
            as_of=close.isoformat(), market_close_at=close.isoformat(), provider_timestamp=provider.isoformat(),
            capture_started_at=capture_started_at, capture_completed_at=capture_completed_at))


def decode_payload(raw):
    # Tencent JSON may be encoded as GB18030 or UTF-8. Each is decoded
    # strictly; replacement characters are never accepted as an index name.
    for encoding in ('utf-8', 'gb18030'):
        try:
            return json.loads(raw.decode(encoding)), encoding
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    raise ValueError('TENCENT_PAYLOAD_ENCODING_INVALID')
