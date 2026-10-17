"""Same-day shadow-only derivation from revalidated original rule receipts.

No production mutation, network, store, clock sampling, model or writer. The
ordinary formula is explicit here; the original reviewed-through API stays shut.
Caller-supplied bytes/checksums do not authenticate an HTTP transport or source.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from ..evaluation.ablation import price_limits as frozen
from . import shadow_rule_preflight as rules
from .calendar import TradingCalendarError
from .shadow_pit_capture import FIELDS, _extract, build_plan_pit_capture
from .shadow_pit_sources import record_raw_response
from .stock_trading_rules import stock_trading_rules

SH=ZoneInfo('Asia/Shanghai')
VERSION='shadow-price-limit-derive/1'
# Explicit ordinary rates only. No ST/IPO/new revision interpretation is added.
RATES={'SSE_MAIN':Decimal('.10'),'SZSE_MAIN':Decimal('.10'),
       'CHINEXT':Decimal('.20'),'STAR':Decimal('.20')}


def _canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()


def _stamp(value):
    result=value if isinstance(value,datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('AWARE_TIMESTAMP_REQUIRED')
    return result.astimezone(SH)


def validate_shadow_rule_receipt(approved,receipt,*,at):
    """Rebuild original observations and re-run the approved formal-body check.

    A canonical checksum is only local identity, never authorization by itself.
    Returned summaries contain no raw body, base64 or caller exception text.
    """
    result=dict(schema_version=VERSION,status='DATA_LIMITED',confirmed_trade_date=None,
        receipt_sha256=None,metadata_conflicts=[],source_authenticated=False,
        original_response_hash_basis='HTTP_RESPONSE_CONTENT_BYTES',reason_codes=[])
    try:
        now=_stamp(at)
        if not isinstance(receipt,Mapping):
            raise ValueError('RULE_RECEIPT_MISSING')
        pin=receipt.get('receipt_sha256')
        if not isinstance(pin,str) or not re.fullmatch('[a-f0-9]{64}',pin) or (
            hashlib.sha256(rules.canonical_receipt_bytes(receipt)).hexdigest()!=pin):
            raise ValueError('RULE_RECEIPT_HASH_MISMATCH')
        known=_stamp(receipt.get('known_at'))
        if (known.date()!=now.date() or known>now or
                known.time().replace(tzinfo=None)>=time(9,26)):
            raise ValueError('RULE_RECEIPT_DAY_OR_CUTOFF_UNPROVEN')
        rows=receipt.get('rows')
        if not isinstance(rows,list) or len(rows)!=3:
            raise ValueError('RULE_THREE_ORIGINALS_REQUIRED')
        observations=[]
        for row in rows:
            if not isinstance(row,Mapping):
                raise ValueError('RULE_ORIGINAL_METADATA_UNPROVEN')
            encoded=row.get('raw_response_base64')
            if not isinstance(encoded,str) or len(encoded)>23*1024*1024:
                raise ValueError('RULE_ORIGINAL_BYTES_UNPROVEN')
            raw=base64.b64decode(encoded,validate=True)
            record=record_raw_response(raw,**{key:row[key] for key in (
                'source_ref','endpoint','request_parameters','request_options','request_started_at',
                'response_received_at','http_status','complete','byte_kind','clock_basis')})
            if (row.get('source_receipt_sha256')!=record.receipt_sha256 or
                    row.get('raw_sha256')!=record.raw_sha256):
                raise ValueError('RULE_ORIGINAL_BINDING_MISMATCH')
            observations.append(record)
        # Uses fixed approved archive SHA/URLs/raw pins, original formal body,
        # actual arrival metadata and same-day <09:26 clock; no auto new version.
        verified=rules.confirm_rule_version(approved,observations,
            trade_date=now.date().isoformat(),known_at=known)
        if verified['status']!='RULE_VERSION_CONFIRMED':
            result['reason_codes']=list(verified['reason_codes'])
            return result
        count=receipt.get('source_requests_started')
        if type(count) is not int or count not in (0,3):
            raise ValueError('RULE_RECEIPT_EXECUTION_METADATA_UNPROVEN')
        verified['source_requests_started']=count
        if rules.canonical_receipt_bytes(verified)!=rules.canonical_receipt_bytes(receipt):
            raise ValueError('RULE_RECEIPT_REPLAY_MISMATCH')
        conflicts=sorted({v for row in verified['rows'] for v in row['metadata_conflicts']})
        result.update(status='RULE_VERSION_CONFIRMED',confirmed_trade_date=now.date().isoformat(),
            receipt_sha256=pin,metadata_conflicts=conflicts,
            rule_review_observed_at=verified['rule_review_observed_at'],
            planned_revalidation_due=verified['planned_revalidation_due'],known_at=verified['known_at'],
            rule_source_effective_date=verified['rule_source_effective_date'],reason_codes=[])
    except Exception as exc:
        # Only our literal stable errors may escape; never body/transport text.
        code=str(exc) if type(exc) is ValueError and re.fullmatch('RULE_[A-Z_]+',str(exc)) else 'RULE_RECEIPT_UNPROVEN'
        result['reason_codes']=[code]
    return result


def _derive_confirmed_day(evidence,*,symbol,at,confirmed_day):
    """Private formula; caller must first validate the public receipt gate.

    Rates, geometry and rounding match the frozen ordinary implementation;
    confirmed_day replaces only its reviewed-through authorization boundary.
    """
    if not isinstance(evidence,Mapping):
        return None,'FROZEN_IDENTITY_AND_PRECLOSE_MISSING'
    day=at.date()
    try:
        board=evidence.get('board')
        expected=('STAR' if symbol.startswith('688') and symbol.endswith('.SH') else
                  'SSE_MAIN' if symbol.startswith('60') and symbol.endswith('.SH') else
                  'CHINEXT' if symbol.startswith(('300','301')) and symbol.endswith('.SZ') else
                  'SZSE_MAIN' if symbol.startswith(('000','001','002','003')) and symbol.endswith('.SZ') else None)
        if evidence.get('symbol')!=symbol or evidence.get('trade_date')!=str(day) or board not in RATES or board!=expected:
            return None,'IDENTITY_DATE_OR_BOARD_UNPROVEN'
        if (evidence.get('security_type')!='CASH_A_SHARE' or evidence.get('security_status')!='ORDINARY'
                or evidence.get('is_st') is not False or evidence.get('limit_regime')!='NORMAL'):
            return None,'ORDINARY_NORMAL_LIMIT_REGIME_UNPROVEN'
        if not frozen._normal_listing_day(date.fromisoformat(str(evidence.get('listing_date'))),day):
            return None,'NORMAL_LISTING_PERIOD_UNPROVEN'
        if (evidence.get('rule_effective_from')!=str(frozen.RULE_FROM) or day<frozen.RULE_FROM or day!=confirmed_day):
            return None,'RULE_DATE_UNPROVEN'
        observed=_stamp(evidence.get('observed_at'))
        if (observed.date()!=day or observed>at or not isinstance(evidence.get('source_ref'),str)
                or not evidence['source_ref'].strip() or not re.fullmatch('[a-fA-F0-9]{64}',str(evidence.get('source_input_sha256','')))):
            return None,'FROZEN_SOURCE_BINDING_UNPROVEN'
        preclose=frozen._decimal(evidence.get('preclose'))
        tick=Decimal(str(stock_trading_rules(symbol).tick))
        if preclose is None or preclose%tick!=0 or evidence.get('preclose_basis')!='EXCHANGE_DISPLAYED_PRECLOSE':
            return None,'EXCHANGE_PRECLOSE_UNPROVEN'
        rate=RATES[board]
        upper=(preclose*(1+rate)).quantize(tick,rounding=ROUND_HALF_UP)
        lower=(preclose*(1-rate)).quantize(tick,rounding=ROUND_HALF_UP)
        if abs(upper-preclose)<tick:upper=preclose+tick
        if abs(lower-preclose)<tick:lower=preclose-tick
        upper,lower=max(tick,upper),max(tick,lower)
        return dict(upper=float(upper),lower=float(lower),trade_date=str(day),symbol=symbol,
            basis='DERIVED_FROM_PRECLOSE_AND_BOARD_RULE',input_sha256=frozen._hash(dict(evidence)),
            source_input_sha256=evidence['source_input_sha256'],source_ref=evidence['source_ref'],
            board=board,preclose=str(preclose),rate=str(rate),tick=str(tick),
            rounding='DECIMAL_ROUND_HALF_UP_MIN_ONE_TICK_FLOOR_ONE_TICK',rule_effective_from=str(frozen.RULE_FROM),
            rule_confirmed_trade_date=str(confirmed_day),
            rule_source=frozen.SSE_RULE if board in {'SSE_MAIN','STAR'} else frozen.SZSE_RULE),None
    except (TypeError,ValueError,InvalidOperation,TradingCalendarError):
        return None,'FROZEN_PRICE_LIMIT_EVIDENCE_INVALID'


def derive_shadow_price_limits(evidence,*,symbol,at,rule_receipt,approved,source=None):
    """Strict same-day ordinary shadow prices, never a production admission."""
    result=dict(schema_version=VERSION,status='DATA_LIMITED',upper=None,lower=None,
        reason_codes=[],source_authenticated=False,production_integration='UNWIRED',
        rule_constant_modified=False,derived_limit_prices=None)
    try:
        now=_stamp(at)
        confirmation=validate_shadow_rule_receipt(approved,rule_receipt,at=now)
        result['rule_confirmation']=confirmation
        if confirmation['status']!='RULE_VERSION_CONFIRMED':
            result['reason_codes']=confirmation['reason_codes']
            return result
        values,lineage,raw=_extract(source)
        if raw is None or not isinstance(evidence,Mapping):
            raise ValueError('PRICE_SOURCE_BYTES_UNPROVEN')
        if (evidence.get('source_input_sha256')!=hashlib.sha256(raw).hexdigest() or
                evidence.get('source_ref')!=lineage['source_ref'] or
                any(evidence.get(k)!=values.get(k) for k in FIELDS)):
            raise ValueError('PRICE_SOURCE_FIELDS_OR_HASH_MISMATCH')
        quote_at=_stamp(values.get('observed_at'));captured=_stamp(source.captured_at)
        listing_at=_stamp(values.get('listing_observed_at'))
        if (quote_at.date()!=now.date() or captured.date()!=now.date() or
                not quote_at<=captured<=now or quote_at.time().replace(tzinfo=None)<time(9,25) or
                (now-quote_at).total_seconds()>180 or listing_at.date()!=now.date() or listing_at>captured or
                values.get('listing_date_basis')!='SOURCE_LISTING_DATE'):
            raise ValueError('PRICE_SOURCE_CLOCK_OR_LISTING_UNPROVEN')
        name=values.get('security_name')
        if not isinstance(name,str) or not name.strip() or 'ST' in name.upper():
            raise ValueError('ORDINARY_SECURITY_NAME_UNPROVEN')
        derived,why=_derive_confirmed_day(evidence,symbol=symbol,at=now,confirmed_day=now.date())
        if derived is None:
            result['reason_codes']=[why]
            return result
        explicit,bad,conflict=frozen._explicit(evidence)
        if conflict or any(v is not None and v!=Decimal(str(derived[k])) for v,k in zip(explicit,('upper','lower'))):
            result.update(status='CONFLICT',reason_codes=['PRICE_LIMITS_CONFLICT'])
            return result
        if bad:
            result['reason_codes']=['EXPLICIT_LIMIT_PRICE_INVALID']
            return result
        result.update(derived,status='KNOWN',derived_limit_prices=derived,reason_codes=[],
            rule_confirmation=confirmation,source_hash_basis=lineage['byte_kind'],
            implementation_sha256={'shadow_price_limits.py':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'price_limits.py':hashlib.sha256(Path(frozen.__file__).read_bytes()).hexdigest(),
                'shadow_rule_preflight.py':hashlib.sha256(Path(rules.__file__).read_bytes()).hexdigest()})
    except Exception as exc:
        code=str(exc) if type(exc) is ValueError and re.fullmatch('[A-Z_]+',str(exc)) else 'PRICE_SOURCE_EVIDENCE_UNPROVEN'
        result['reason_codes']=[code]
    return result


def build_shadow_plan_pit_capture(plan,source,*,observed_at,rule_receipt,approved):
    """New opt-in wrapper; original plan/PIT gates and explicit route unchanged.

    No sealing/writing or shared entry replacement. Only a verified rule-date
    gap may be removed. Any other source/identity/plan gap remains blocking.
    """
    entry=build_plan_pit_capture(plan,source,observed_at=observed_at)
    if 'RULE_DATE_UNPROVEN' not in entry.get('reason_codes',[]) or entry.get('evidence') is None:
        return entry
    resolved=derive_shadow_price_limits(entry['evidence'],symbol=entry['symbol'],at=observed_at,
        rule_receipt=rule_receipt,approved=approved,source=source)
    if resolved['status']=='KNOWN':
        reasons=[v for v in entry['reason_codes'] if v!='RULE_DATE_UNPROVEN']
        return {**entry,'limits':resolved,'reason_codes':reasons,'status':'COMPLETE' if not reasons else 'DATA_LIMITED',
                'shadow_rule_receipt_sha256':resolved['rule_confirmation']['receipt_sha256']}
    return {**entry,'reason_codes':list(dict.fromkeys(entry['reason_codes']+resolved['reason_codes'])),
            'shadow_rule_derivation':resolved}


__all__=['validate_shadow_rule_receipt','derive_shadow_price_limits','build_shadow_plan_pit_capture']
