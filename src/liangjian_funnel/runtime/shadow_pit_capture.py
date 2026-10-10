"""Standalone auction PIT capture: explicit inputs, no default source or state.

Only same-target plans are observed. This module cannot activate/publish plans
or notify customers. Source bytes, normalized object hashes and displayed field
claims have distinct lineage; missing information is never filled from prices.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from collections.abc import Mapping
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo

from ..evaluation.ablation.price_limits import resolve_price_limits
from .calendar import ExchangeTradingCalendar

SH = ZoneInfo('Asia/Shanghai')
VERSION = 'shadow-pit-capture/1'
FIELDS = ('symbol', 'trade_date', 'observed_at', 'security_name', 'security_name_basis',
          'preclose', 'preclose_basis', 'upper_limit', 'lower_limit', 'listing_date',
          'listing_date_basis', 'listing_observed_at', 'board', 'security_type',
          'security_status', 'is_st', 'limit_regime', 'rule_effective_from', 'prior_raw_close')
REQUIRED_IDENTITY = ('security_name', 'preclose', 'listing_date', 'board',
                     'security_type', 'security_status', 'is_st', 'limit_regime')
SYMBOL = re.compile(r'[0-9]{6}\.(SH|SZ|BJ)')


class CaptureError(ValueError):
    """Stable public code, never underlying provider exception text."""


@dataclass(frozen=True)
class PITSourceReceipt:
    source_ref: str
    captured_at: object
    complete: bool
    format: str = 'EXPLICIT_JSON_FIELDS'
    raw_response: bytes | None = None
    normalized: dict | None = None
    byte_kind: str = 'CONSUMER_INPUT_BYTES'
    field_map: dict | None = None


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(SH)
    except (ValueError, TypeError, AttributeError):
        raise CaptureError('TIMESTAMP_UNPROVEN') from None


def _json(body):
    def pairs(values):
        result = {}
        for k, v in values:
            if k in result:
                raise CaptureError('SOURCE_JSON_DUPLICATE_KEY')
            result[k] = v
        return result
    try:
        result = json.loads(body, object_pairs_hook=pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(CaptureError('SOURCE_JSON_NONFINITE')))
        if not isinstance(result, dict):
            raise CaptureError('SOURCE_JSON_OBJECT_REQUIRED')
        return result
    except (json.JSONDecodeError, UnicodeError, TypeError):
        raise CaptureError('SOURCE_JSON_INVALID') from None


def _safe_ref(value):
    if (not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9:/. _-]{1,512}', value)
            or re.search(r'(?i)api[_-]?key|token|secret|password|bearer', value)):
        raise CaptureError('SOURCE_REFERENCE_UNSAFE')
    return value


def _plan_scope(row, now):
    if not isinstance(row, Mapping):
        raise CaptureError('PLAN_ROW_INVALID')
    pid, symbol, status = row.get('plan_id'), row.get('symbol'), row.get('status')
    if not isinstance(pid, str) or not pid or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
        raise CaptureError('PLAN_IDENTITY_UNPROVEN')
    if status not in {'PENDING_MORNING_REVIEW', 'ACTIVE_TODAY'}:
        raise CaptureError('PLAN_STATUS_OUT_OF_SCOPE')
    payload = row.get('payload_json', row.get('payload'))
    if isinstance(payload, str):
        payload = _json(payload)
    if not isinstance(payload, Mapping):
        raise CaptureError('PLAN_PAYLOAD_UNPROVEN')
    target = payload.get('target_trade_date')
    if target is None:
        raise CaptureError('TARGET_DAY_UNPROVEN')
    if target != now.date().isoformat():
        raise CaptureError('PLAN_TARGET_OUT_OF_SCOPE')
    if payload.get('symbol', symbol) != symbol:
        raise CaptureError('PLAN_PAYLOAD_SYMBOL_MISMATCH')
    expiry = _stamp(row.get('expires_at'))
    if expiry.date() != now.date() or expiry < now:
        raise CaptureError('PLAN_EXPIRED_OR_DATE_MISMATCH')
    if row.get('invalidated_at') is not None and _stamp(row['invalidated_at']) <= now:
        raise CaptureError('PLAN_ALREADY_INVALIDATED')
    start = None
    if row.get('valid_from') is not None:
        start = _stamp(row['valid_from'])
        if start.date() != now.date() or start > expiry:
            raise CaptureError('PLAN_VALIDITY_UNPROVEN')
    elif status == 'ACTIVE_TODAY':
        raise CaptureError('ACTIVE_PLAN_START_UNPROVEN')
    return {'plan_id': pid, 'symbol': symbol, 'plan_status': status,
            'target_trade_date': target, 'entry_effective_now': status == 'ACTIVE_TODAY' and start <= now,
            'plan_input_sha256': hashlib.sha256(_canonical(dict(row))).hexdigest()}


def _pointer(root, path):
    if not isinstance(path, str) or not path.startswith('/') or len(path) > 512 or len(path.split('/')) > 12:
        raise CaptureError('FIELD_POINTER_INVALID')
    value = root
    try:
        for key in path.split('/')[1:]:
            key = key.replace('~1', '/').replace('~0', '~')
            value = value[int(key)] if isinstance(value, list) else value[key]
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _extract(source):
    lineage = {'source_ref': None, 'raw_response_sha256': None, 'raw_http_bytes_available': False,
               'normalized_object_sha256': None, 'source_authenticated': False, 'field_paths': {}}
    if source is None:
        raise CaptureError('SOURCE_UNWIRED')
    if not isinstance(source, PITSourceReceipt):
        raise CaptureError('SOURCE_RECEIPT_INVALID')
    lineage['source_ref'] = _safe_ref(source.source_ref)
    if source.complete is not True:
        raise CaptureError('SOURCE_PARTIAL')
    if source.format == 'EXPLICIT_JSON_FIELDS':
        if (not isinstance(source.raw_response, bytes) or not source.raw_response
                or len(source.raw_response) > 16 * 1024 * 1024):
            raise CaptureError('SOURCE_BYTES_MISSING')
        if source.normalized is not None:
            raise CaptureError('SOURCE_FORMAT_AMBIGUOUS')
        if source.byte_kind not in {'ORIGINAL_HTTP_RESPONSE_BYTES', 'CONSUMER_INPUT_BYTES'}:
            raise CaptureError('SOURCE_BYTE_KIND_UNPROVEN')
        root = _json(source.raw_response)
        paths = source.field_map if source.field_map is not None else {k: '/' + k for k in FIELDS}
        if not isinstance(paths, Mapping) or any(k not in FIELDS for k in paths):
            raise CaptureError('FIELD_MAPPING_INVALID')
        values = {k: _pointer(root, path) for k, path in paths.items()}
        lineage.update(raw_response_sha256=hashlib.sha256(source.raw_response).hexdigest(),
                       raw_http_bytes_available=source.byte_kind == 'ORIGINAL_HTTP_RESPONSE_BYTES',
                       byte_kind=source.byte_kind, field_paths=dict(paths))
        return values, lineage, source.raw_response
    if source.format == 'NORMALIZED_QUOTE':
        if source.raw_response is not None or not isinstance(source.normalized, Mapping):
            raise CaptureError('NORMALIZED_SOURCE_INVALID')
        root = dict(source.normalized)
        lineage['normalized_object_sha256'] = hashlib.sha256(_canonical(root)).hexdigest()
        values = {k: root.get(k) for k in FIELDS}
        values['security_name'] = root.get('security_name', root.get('name'))
        values['preclose'] = root.get('preclose', root.get('previous_close'))
        values['observed_at'] = root.get('observed_at', root.get('quote_time'))
        values['trade_date'] = root.get('trade_date') or _stamp(values['observed_at']).date().isoformat()
        # Previous_close is an observed normalized provider field, not a
        # declaration of EXCHANGE_DISPLAYED_PRECLOSE or an original HTTP body.
        values['preclose_basis'] = 'NORMALIZED_PROVIDER_PREVIOUS_CLOSE'
        values['security_name_basis'] = 'NORMALIZED_PROVIDER_NAME'
        lineage['byte_kind'] = 'NORMALIZED_OBJECT'
        return values, lineage, None
    raise CaptureError('SOURCE_FORMAT_UNSUPPORTED')


def _empty(scope, now, reason):
    return {**scope, 'status': 'DATA_LIMITED', 'reason_codes': [reason], 'evidence': None,
            'raw_response': None, 'observed_at': now.isoformat(), 'field_status': {},
            'source_lineage': {}, 'sealed_receipt': None,
            'limits': {'status': 'UNKNOWN', 'upper': None, 'lower': None,
                       'reason_code': 'PRICE_LIMITS_UNKNOWN'}}


def build_plan_pit_capture(plan, source, *, observed_at):
    """Pure preparation for identity_provider: evidence/raw_response/observed_at.

    Explicit JSON fields are parsed from actual bytes (optional JSON pointers).
    Normalized quote-only inputs remain limited and cannot supply a raw SHA.
    """
    now = _stamp(observed_at)
    if not time(9,26) <= now.time().replace(tzinfo=None) < time(9,30):
        return _empty({'plan_id':plan.get('plan_id') if isinstance(plan, Mapping) else None}, now, 'CAPTURE_WINDOW_MISSED')
    if not ExchangeTradingCalendar().is_trading_day(now.date()):
        return _empty({'plan_id':plan.get('plan_id') if isinstance(plan, Mapping) else None}, now, 'NON_TRADING_DAY')
    try:
        scope = _plan_scope(plan, now)
    except CaptureError as exc:
        return _empty({'plan_id': plan.get('plan_id') if isinstance(plan, Mapping) else None}, now, str(exc))
    try:
        values, lineage, raw = _extract(source)
        capture, quote_at = _stamp(source.captured_at), _stamp(values.get('observed_at'))
        if values.get('symbol') != scope['symbol']:
            raise CaptureError('QUOTE_SYMBOL_MISMATCH')
        if values.get('trade_date') != now.date().isoformat() or quote_at.date() != now.date():
            raise CaptureError('QUOTE_DATE_MISMATCH')
        if quote_at > capture or capture > now:
            raise CaptureError('QUOTE_FUTURE')
        if quote_at.time().replace(tzinfo=None) < time(9,25) or (now-quote_at).total_seconds() > 180:
            raise CaptureError('QUOTE_AUCTION_EXPIRED')
        evidence = {k: values.get(k) for k in FIELDS}
        evidence.update(source_ref=lineage['source_ref'],
                        source_input_sha256=lineage['raw_response_sha256'])
        reasons = []
        name = evidence['security_name']
        if not isinstance(name, str) or not name.strip():
            evidence['security_name'] = None
        elif 'ST' in name.upper():
            evidence['is_st'] = True
        # Separate, same-day dated listing observation; never borrow yesterday's
        # cache and label its mtime/capture as fresh listing identity.
        try:
            listing_at = _stamp(evidence['listing_observed_at'])
            listing_valid = (listing_at.date() == now.date() and listing_at <= capture
                             and evidence['listing_date_basis'] == 'SOURCE_LISTING_DATE'
                             and isinstance(evidence['listing_date'], str)
                             and date.fromisoformat(evidence['listing_date']) <= now.date())
        except (ValueError, TypeError):
            listing_valid = False
        if not listing_valid:
            evidence['listing_date'] = None
        for key in ('preclose', 'upper_limit', 'lower_limit'):
            value = evidence.get(key)
            if (isinstance(value, bool) or not isinstance(value, (float, int, str))):
                evidence[key] = None
            elif value is not None:
                try:
                    if not math.isfinite(float(value)) or float(value) <= 0:
                        evidence[key] = None
                except (TypeError, ValueError):
                    evidence[key] = None
        if type(evidence['is_st']) is not bool:
            evidence['is_st'] = None
        field_status = {k: 'OBSERVED_SOURCE_FIELD' if evidence.get(k) is not None else 'UNKNOWN'
                        for k in REQUIRED_IDENTITY}
        missing = [k for k, value in field_status.items() if value == 'UNKNOWN']
        if evidence.get('preclose_basis') != 'EXCHANGE_DISPLAYED_PRECLOSE':
            missing.append('EXCHANGE_DISPLAYED_PRECLOSE')
        if raw is None:
            reasons.append('ORIGINAL_SOURCE_BYTES_UNAVAILABLE')
        if missing:
            reasons.append('IDENTITY_FIELDS_INCOMPLETE')
        resolved = resolve_price_limits({**evidence, 'price_limit_evidence': evidence},
                                         symbol=scope['symbol'], at=now)
        if raw is None:
            resolved = {'status':'UNKNOWN','upper':None,'lower':None,
                        'reason_code':'PRICE_LIMITS_UNKNOWN','evidence_reason':'ORIGINAL_SOURCE_BYTES_UNAVAILABLE'}
        if resolved['status'] != 'KNOWN':
            reasons.append(resolved.get('evidence_reason') or resolved.get('reason_code') or 'PRICE_LIMITS_UNKNOWN')
        return {**scope, 'status': 'COMPLETE' if not reasons else 'DATA_LIMITED',
                'reason_codes': list(dict.fromkeys(reasons)), 'evidence': evidence,
                'raw_response': raw, 'observed_at': now.isoformat(), 'field_status': field_status,
                'missing_fields': missing, 'source_lineage': {**lineage, 'captured_at': capture.isoformat()},
                'sealed_receipt': None, 'limits': resolved}
    except CaptureError as exc:
        return _empty(scope, now, str(exc))
    except Exception:
        return _empty(scope, now, 'SOURCE_RECEIPT_INVALID')


def capture_shadow_pit(plans, *, observed_at, source_receipts=None, source_fetcher=None,
                       ledger=None, clock=None):
    """Independent batch capture. No default fetcher, retry, limiter or publisher.

    A supplied fetcher retains its own existing bounds. Pass a real wall clock
    for asynchronous/live source arrival; omission uses the explicit known time
    and cannot accept later future receipts.
    """
    now = _stamp(observed_at)
    result = {'schema_version':VERSION, 'source_kind':'REALTIME_SHADOW', 'observed_at':now.isoformat(),
              'status':'DATA_LIMITED','reason_codes':[], 'entries':[], 'excluded_plans':[],
              'selected_plan_count':0, 'pending_plan_count':0, 'active_plan_count':0,
              'source_fetch_count':0, 'execution_publication':'UNCHANGED',
              'customer_notification_calls':0, 'model_calls':0,
              'source_authenticated':False}
    if not time(9,26) <= now.time().replace(tzinfo=None) < time(9,30):
        result['reason_codes'] = ['CAPTURE_WINDOW_MISSED']
        return result
    try:
        if not ExchangeTradingCalendar().is_trading_day(now.date()):
            result['reason_codes'] = ['NON_TRADING_DAY']
            return result
        if not isinstance(plans, (list, tuple)) or (source_receipts is not None and source_fetcher is not None):
            raise CaptureError('CAPTURE_INPUT_AMBIGUOUS')
        selected, ids = [], set()
        for row in plans:
            try:
                scope = _plan_scope(row, now)
                if scope['plan_id'] in ids:
                    raise CaptureError('DUPLICATE_PLAN_ID')
                ids.add(scope['plan_id'])
                selected.append((row, scope))
            except CaptureError as exc:
                if str(exc) == 'DUPLICATE_PLAN_ID':
                    raise
                result['excluded_plans'].append({'plan_id':row.get('plan_id') if isinstance(row, Mapping) else None,
                                                 'reason_code':str(exc)})
        sources = {}
        for symbol in sorted({scope['symbol'] for _, scope in selected}):
            if source_fetcher is not None:
                result['source_fetch_count'] += 1
                try:
                    sources[symbol] = source_fetcher(symbol, observed_at=now)
                except Exception:
                    sources[symbol] = 'SOURCE_FETCH_FAILED'
            else:
                sources[symbol] = source_receipts.get(symbol) if isinstance(source_receipts, Mapping) else None
        known = _stamp(clock()) if clock is not None else now
        if known < now or known.date() != now.date() or known.time().replace(tzinfo=None) >= time(9,30):
            raise CaptureError('CAPTURE_WINDOW_MISSED')
        for row, scope in selected:
            source = sources[scope['symbol']]
            entry = (_empty(scope, known, 'SOURCE_FETCH_FAILED') if source == 'SOURCE_FETCH_FAILED'
                     else build_plan_pit_capture(row, source, observed_at=known))
            if ledger is not None and entry.get('evidence') is not None:
                # A normalized object may be audited but cannot masquerade as
                # an original bytes receipt or become limit evidence here.
                try:
                    seal = ledger.seal_price_limit_evidence(scope['plan_id'], entry['evidence'],
                            observed_at=known, raw_response=entry['raw_response'])
                except Exception:
                    seal = {'ok':False,'stored':False,'error_code':'SHADOW_EVIDENCE_WRITE_FAILED'}
                entry['sealed_receipt'] = seal
                if not seal.get('ok') or seal.get('mirror_status') == 'PENDING':
                    entry['status'] = 'DATA_LIMITED'
                    entry['reason_codes'].append(seal.get('error_code') or 'SHADOW_MIRROR_PENDING')
            result['entries'].append(entry)
            result['pending_plan_count'] += scope['plan_status'] == 'PENDING_MORNING_REVIEW'
            result['active_plan_count'] += scope['plan_status'] == 'ACTIVE_TODAY'
        result['selected_plan_count'] = len(selected)
        result['status'] = 'COMPLETE' if all(e['status']=='COMPLETE' for e in result['entries']) and not result['excluded_plans'] else 'DATA_LIMITED'
        if not selected:
            result['status'] = 'DATA_LIMITED'
            result['reason_codes'] = ['NO_CURRENT_TARGET_PLANS']
    except CaptureError as exc:
        result['reason_codes'] = [str(exc)]
    except Exception:
        result['reason_codes'] = ['CAPTURE_INPUT_INVALID']
    return result


def serializable_capture_report(result):
    """Raw bytes stay in original source files; JSON output contains their SHA."""
    return {**result, 'entries':[{k:v for k,v in row.items() if k!='raw_response'} for row in result['entries']]}


__all__ = ['PITSourceReceipt','build_plan_pit_capture','capture_shadow_pit','serializable_capture_report']
