"""Immutable source-specific capital observations; never an execution factor.

Historical reception time is not historical availability. Ratios cannot be
averaged into multi-day ratios, and observations from different vendors cannot
be spliced into one window. No network request is made by this module.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

SCHEMA = 'capital-observation/1'
EASTMONEY_HISTORY = 'EASTMONEY_STOCK_FFLOW_DAYKLINE_RESEARCH_ONLY'
TENCENT_DAILY = 'TENCENT_QQ_FINANCE_FUND_FLOW_DAILY_ARCHIVE'
AMOUNTS = ('main_net', 'small_net', 'medium_net', 'large_net', 'super_net')
RATIOS = ('main_ratio', 'small_ratio', 'medium_ratio', 'large_ratio', 'super_ratio')


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', value):
        raise ValueError('CAPITAL_SYMBOL_INVALID')
    return value


def _aware(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('CAPITAL_TIMEZONE_REQUIRED')
    return parsed.astimezone(ZoneInfo('Asia/Shanghai'))


def _number(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (ValueError, InvalidOperation):
        return None


def parse_eastmoney_history(raw: bytes, *, symbol: str, received_at: datetime) -> dict:
    """Validate real provider dates/identity and retain unscaled native values."""
    symbol = _symbol(symbol); received = _aware(received_at)
    if not isinstance(raw, bytes) or len(raw) > 4 * 1024**2:
        raise ValueError('CAPITAL_RAW_BYTES_LIMIT')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('CAPITAL_PROVIDER_ENVELOPE_INVALID')
    data = value.get('data')
    code, exchange = symbol.split('.')
    if (value.get('rc') != 0 or not isinstance(data, dict) or data.get('code') != code
        or type(data.get('market')) is not int or data['market'] != (1 if exchange == 'SH' else 0)):
        raise ValueError('CAPITAL_PROVIDER_IDENTITY_MISMATCH')
    lines = data.get('klines')
    if not isinstance(lines, list) or not lines or len(lines) > 1000:
        raise ValueError('CAPITAL_HISTORY_MISSING_OR_UNBOUNDED')
    rows = []; seen = set()
    for line in lines:
        fields = line.split(',') if isinstance(line, str) else []
        if len(fields) != 15:
            raise ValueError('CAPITAL_HISTORY_FIELD_COUNT')
        day = date.fromisoformat(fields[0])
        if day.isoformat() != fields[0] or day > received.date() or fields[0] in seen:
            raise ValueError('CAPITAL_HISTORY_DATE_INVALID_OR_DUPLICATE')
        seen.add(fields[0]); numbers = [_number(x) for x in fields[1:13]]
        missing = [i + 1 for i, n in enumerate(numbers) if n is None]
        # Exact main=large+super is a structural field check, not a buy gate.
        if not missing and abs(numbers[0] - numbers[3] - numbers[4]) > Decimal('0.01'):
            raise ValueError('CAPITAL_MAIN_BUCKET_IDENTITY_MISMATCH')
        if any(n is not None and abs(n) > 100 for n in numbers[5:10]):
            raise ValueError('CAPITAL_PERCENT_OUT_OF_RANGE')
        row = dict(trade_date=fields[0], raw_line_sha256=digest(line.encode()),
            availability_state='SOURCE_UNAVAILABLE' if missing else 'OBSERVED_VALUE',
            missing_field_positions=missing,
            values={key: str(n) if n is not None else None
                    for key, n in zip((*AMOUNTS, *RATIOS, 'close', 'change_ratio'), numbers)})
        rows.append(row)
    return dict(schema=SCHEMA, source_id=EASTMONEY_HISTORY, symbol=symbol, provider_name=data.get('name'),
        raw_sha256=digest(raw), received_at=received.isoformat(), rows=sorted(rows, key=lambda r:r['trade_date']),
        amount_unit='PROVIDER_NATIVE_AMOUNT_NOT_CONFIRMED', ratio_unit='PERCENT',
        observation_basis='HISTORICAL_RECONSTRUCTION_AT_ACTUAL_RECEIPT',
        historical_point_in_time_proven=False, execution_authority=False, formal_factor_eligible=False)


def tencent_daily_observation(raw: bytes, *, symbol: str, provider_timestamp: str,
                             received_at: datetime) -> dict:
    """Archive raw flow buckets while the real daily source is being collected.

    The paired quote establishes its quoted trading day, not the fund record's
    event time. This distinction remains explicit instead of signing legacy
    cache entries as point-in-time evidence after the fact.
    """
    _symbol(symbol); received = _aware(received_at)
    if len(raw) > 4 * 1024**2 or not re.fullmatch(r'\d{14}', provider_timestamp):
        raise ValueError('CAPITAL_TENCENT_ORIGINAL_INVALID')
    quoted = datetime.strptime(provider_timestamp, '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo('Asia/Shanghai'))
    if quoted > received:
        raise ValueError('CAPITAL_TENCENT_FUTURE_QUOTE')
    value = json.loads(raw)
    data = value.get('data') if isinstance(value, Mapping) else None
    today = data.get('todayFundFlow') if isinstance(data, Mapping) else None
    if not isinstance(today, Mapping):
        raise ValueError('CAPITAL_TENCENT_ORIGINAL_INVALID')
    code, market = symbol.split('.')
    if today.get('stockCode') not in {code, market.lower()+code, symbol}:
        raise ValueError('CAPITAL_TENCENT_IDENTITY_UNPROVEN')
    names = ('mainNetIn', 'mainIn', 'mainOut', 'retailIn', 'retailOut', 'superFlow', 'bigFlow')
    values = {key: _number(today.get(key)) for key in names}
    gross_names = ('mainIn', 'mainOut', 'retailIn', 'retailOut')
    gross = (sum((values[key] for key in gross_names), Decimal(0))
        if all(values[key] is not None and values[key] >= 0 for key in gross_names) else None)
    denominator = gross if gross is not None and gross > 0 else None
    main = values['mainNetIn']
    row = dict(trade_date=quoted.date().isoformat(),
        availability_state='OBSERVED_VALUE' if main is not None else 'SOURCE_UNAVAILABLE',
        values=dict(main_net=str(main) if main is not None else None,
            main_ratio=str(main/denominator*100) if main is not None and denominator else None,
            compatible_gross_denominator=str(denominator) if denominator else None),
        raw_buckets={key:str(v) if v is not None else None for key,v in values.items()})
    return dict(schema=SCHEMA, source_id=TENCENT_DAILY, symbol=symbol, raw_sha256=digest(raw),
        received_at=received.isoformat(), provider_quote_timestamp=provider_timestamp, rows=[row],
        amount_unit='TENCENT_PROVIDER_NATIVE_AMOUNT', ratio_unit='PERCENT',
        observation_basis='DAILY_FLOW_RECEIVED_WITH_PAIRED_QUOTE_DATE',
        fund_event_time_proven=False, historical_point_in_time_proven=False,
        execution_authority=False, formal_factor_eligible=False)


def archive_observation(root: str | Path, *, raw: bytes, observation: Mapping) -> dict:
    """Content-addressed append-only envelope plus exact original bytes.

    A second identical write verifies existing bytes. A changed receipt gets a
    different identity; a corrupt existing file is never overwritten.
    """
    source = observation.get('source_id')
    if source not in {EASTMONEY_HISTORY, TENCENT_DAILY}:
        raise ValueError('CAPITAL_SOURCE_NAMESPACE_INVALID')
    symbol = _symbol(observation.get('symbol'))
    if observation.get('raw_sha256') != digest(raw):
        raise ValueError('CAPITAL_RAW_HASH_MISMATCH')
    _aware(observation.get('received_at'))
    if observation.get('execution_authority') is not False:
        raise ValueError('CAPITAL_ARCHIVE_AUTHORITY_CONFLICT')
    envelope = _json(dict(observation)); identity = digest(envelope)
    namespace = {EASTMONEY_HISTORY:'em-history', TENCENT_DAILY:'qq-daily'}[source]
    folder = Path(root) / namespace / symbol / identity
    folder.mkdir(parents=True, exist_ok=True)
    for name, content in (('original.raw', raw), ('observation.json', envelope)):
        path = folder / name
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != content:
                raise ValueError('CAPITAL_IMMUTABLE_ORIGINAL_CONFLICT')
        else:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(content); stream.flush(); os.fsync(stream.fileno())
    return dict(path=str(folder / 'observation.json'), sha256=identity, raw_sha256=digest(raw))


def read_observation(reference: Mapping) -> dict:
    path = Path(reference['path'])
    if path.is_symlink() or path.stat().st_size > 8 * 1024**2:
        raise ValueError('CAPITAL_ARCHIVE_REFERENCE_INVALID')
    envelope = path.read_bytes()
    if digest(envelope) != reference['sha256']:
        raise ValueError('CAPITAL_ARCHIVE_HASH_MISMATCH')
    value = json.loads(envelope); raw_path = path.with_name('original.raw')
    if raw_path.is_symlink() or raw_path.stat().st_size > 4 * 1024**2:
        raise ValueError('CAPITAL_RAW_REFERENCE_INVALID')
    if digest(raw_path.read_bytes()) != value['raw_sha256']:
        raise ValueError('CAPITAL_RAW_HASH_MISMATCH')
    return value


def research_windows(observations: Sequence[Mapping], *, symbol: str, source_id: str,
                     sessions: Sequence[str], known_by: datetime, calendar=None) -> dict:
    """Exact trading-calendar windows; gaps/late receipts never become zero.

    Callers supply the exchange calendar's exact ordered sessions. This reader
    verifies their ordering and uniqueness, and never guesses missing holidays.
    Net amounts may be summed within a single source; N-day net ratios remain
    unavailable without the vendor's compatible gross denominator.
    """
    _symbol(symbol); cutoff = _aware(known_by)
    if (not sessions or len(sessions) != len(set(sessions)) or list(sessions) != sorted(sessions)
        or any(date.fromisoformat(day) > cutoff.date() for day in sessions)):
        raise ValueError('CAPITAL_CALENDAR_SESSIONS_INVALID')
    if calendar is None:
        from ..runtime.calendar import ExchangeTradingCalendar
        calendar = ExchangeTradingCalendar()
    days = [date.fromisoformat(day) for day in sessions]
    if (not all(calendar.is_trading_day(day) for day in days)
        or any(calendar.previous_trading_day(current) != prior for prior,current in zip(days,days[1:]))):
        raise ValueError('CAPITAL_NONCONTIGUOUS_EXCHANGE_SESSIONS')
    if source_id not in {EASTMONEY_HISTORY, TENCENT_DAILY}:
        raise ValueError('CAPITAL_SOURCE_NAMESPACE_INVALID')
    rows = {}; units = {}; conflicts = set(); invalid = {}; partial = {}; rejected_receipts = 0
    for observation in observations:
        if observation.get('symbol') != symbol or observation.get('source_id') != source_id:
            raise ValueError('CAPITAL_MIXED_SOURCE_OR_SYMBOL')
        if observation.get('execution_authority') is not False:
            raise ValueError('CAPITAL_ARCHIVE_AUTHORITY_CONFLICT')
        received = _aware(observation['received_at'])
        if received > cutoff:
            rejected_receipts += 1; continue
        for row in observation.get('rows', []):
            day = row['trade_date']
            if day not in sessions:
                continue
            reasons = []
            values = row.get('values')
            if not isinstance(values, Mapping) or _number(values.get('main_net')) is None:
                reasons.append('MAIN_NET_INVALID')
            if row.get('availability_state') != 'OBSERVED_VALUE':
                reasons.append('SOURCE_VALUE_NOT_OBSERVED')
            unit = observation.get('amount_unit')
            if not isinstance(unit, str) or not unit.strip():
                reasons.append('AMOUNT_UNIT_UNRESOLVED')
                unit = None
            if source_id == TENCENT_DAILY:
                quote = observation.get('provider_quote_timestamp')
                try:
                    quoted = datetime.strptime(quote, '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo('Asia/Shanghai'))
                except (ValueError, TypeError):
                    quoted = None
                if quoted is None or quoted.date().isoformat() != day or quoted > received:
                    reasons.append('PAIRED_QUOTE_DATE_UNPROVEN')
                elif quoted.hour < 15:
                    reasons.append('SESSION_NOT_CLOSED')
            elif date.fromisoformat(day) == received.date() and received.hour < 15:
                reasons.append('SESSION_NOT_CLOSED')
            if 'SESSION_NOT_CLOSED' in reasons:
                # A forming quote is a different observation scope, not a
                # revision of a completed session. Retain its rejection in
                # the audit without poisoning a later genuine close.
                partial[day] = partial.get(day, 0) + 1
                continue
            if reasons:
                invalid.setdefault(day, set()).update(reasons)
            if day in rows and _json(row) != _json(rows[day]): conflicts.add(day)
            if day in units and units[day] != unit: conflicts.add(day)
            rows[day] = row
            units[day] = unit
    for day in partial:
        if day not in rows:
            invalid.setdefault(day,set()).add('SESSION_NOT_CLOSED')
    result = {}
    for count in (1, 3, 5, 10):
        days = list(sessions[-count:]); missing = [d for d in days if d not in rows]
        bad = [d for d in days if d in conflicts or d in invalid]
        mixed_units = len({units[d] for d in days if d in units}) > 1
        complete = len(days) == count and not missing and not bad and not mixed_units
        gross_values = [_number(rows[d]['values'].get('compatible_gross_denominator')) for d in days] if complete else []
        gross = sum(gross_values, Decimal(0)) if gross_values and all(n is not None and n > 0 for n in gross_values) else None
        net = sum((_number(rows[d]['values']['main_net']) for d in days), Decimal(0)) if complete else None
        ratio = (str(net/gross*100) if gross else rows[days[0]]['values']['main_ratio'] if complete and count == 1 else None)
        result[str(count)+'d'] = dict(availability_state='OBSERVED_VALUE' if complete else 'SOURCE_UNAVAILABLE',
            sessions=days, missing_sessions=missing, invalid_or_conflicting_sessions=bad,
            invalid_session_reasons={d:sorted(invalid.get(d,set()) | ({'CONFLICTING_REVISIONS'} if d in conflicts else set())) for d in bad},
            partial_observations_excluded={d:partial[d] for d in days if d in partial},
            amount_unit=units.get(days[-1]) if complete else None,
            window_reason='AMOUNT_UNIT_MISMATCH' if mixed_units else 'OK' if complete else 'INCOMPLETE_OR_INVALID_SESSIONS',
            net_amount_native=str(net) if complete else None,
            net_ratio_percent=ratio,
            ratio_reason=None if ratio is not None else 'COMPATIBLE_GROSS_DENOMINATOR_NOT_OBSERVED')
    return dict(symbol=symbol, source_id=source_id, known_by=cutoff.isoformat(), windows=result,
        late_receipts_rejected=rejected_receipts, scope='RESEARCH_ONLY',
        historical_point_in_time_proven=False, formal_factor_eligible=False, execution_authority=False)


def archived_research_windows(root: str | Path, *, symbol: str, source_id: str,
                              sessions: Sequence[str], known_by: datetime,
                              maximum_observations: int = 512, calendar=None) -> dict:
    """Read only exact immutable envelopes for one source and stock.

    This is an explicit research consumer, never a source fallback or a
    relabeling of legacy normalized caches. All bytes/identities are checked;
    an untrusted archive is reported rather than silently omitted.
    """
    _symbol(symbol)
    if source_id not in {EASTMONEY_HISTORY,TENCENT_DAILY}:
        raise ValueError('CAPITAL_SOURCE_NAMESPACE_INVALID')
    if type(maximum_observations) is not int or not 1 <= maximum_observations <= 2048:
        raise ValueError('CAPITAL_ARCHIVE_READ_LIMIT_INVALID')
    namespace={EASTMONEY_HISTORY:'em-history',TENCENT_DAILY:'qq-daily'}[source_id]
    base=Path(root);folder=base/namespace/symbol
    if any(p.is_symlink() for p in (base,base/namespace,folder)):
        raise ValueError('CAPITAL_ARCHIVE_REFERENCE_INVALID')
    references=[];values=[]
    if folder.exists():
        for entry in folder.iterdir():
            if entry.is_symlink() or not entry.is_dir() or not re.fullmatch('[0-9a-f]{64}',entry.name):
                raise ValueError('CAPITAL_ARCHIVE_REFERENCE_INVALID')
            if len(references)>=maximum_observations:
                raise ValueError('CAPITAL_ARCHIVE_READ_LIMIT_EXCEEDED')
            references.append(dict(path=str(entry/'observation.json'),sha256=entry.name))
        references.sort(key=lambda ref:ref['sha256'])
        for reference in references:
            value=read_observation(reference)
            if (value.get('schema')!=SCHEMA or value.get('symbol')!=symbol
                or value.get('source_id')!=source_id):
                raise ValueError('CAPITAL_ARCHIVE_ENVELOPE_IDENTITY_MISMATCH')
            values.append(value)
    result=research_windows(values,symbol=symbol,source_id=source_id,sessions=sessions,
        known_by=known_by,calendar=calendar)
    result['archive_originals']=references
    result['archive_observation_count']=len(values)
    result['new_source_http']=0
    return result
