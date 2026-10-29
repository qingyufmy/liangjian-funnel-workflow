"""A1 source gaps for natural and recovery publications; no invented facts."""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from .calendar import ExchangeTradingCalendar, TradingCalendarError
from .closed_session_capture import KEY, validate_capture_proof

_CAPTURE_CALENDAR = ExchangeTradingCalendar()


def source_gap_detail(snapshot_data, symbol, *, strict=True, expected_source_date=None):
    manifest = snapshot_data.get('snapshot_manifest') or {}
    failures = manifest.get('source_failures') if isinstance(manifest, Mapping) else None
    if not isinstance(failures, Mapping):
        if strict:raise ValueError('REGEN_A1_SOURCE_FAILURE_MANIFEST_MISSING')
        failures={symbol:['A1_SOURCE_FAILURE_MANIFEST_MISSING']}
    codes = failures.get(symbol, [])
    if not isinstance(codes, (list, tuple)) or any(not isinstance(v, str) or not v.strip() for v in codes):
        raise ValueError('REGEN_A1_SOURCE_FAILURE_DETAIL_INVALID')
    if expected_source_date is not None:
        try:
            at=datetime.fromisoformat(manifest.get('as_of',''))
            local=at.astimezone(ZoneInfo('Asia/Shanghai')) if at.tzinfo is not None else None
            if KEY in snapshot_data:
                # The immutable manifest clock is capture, not market day.
                # An explicit invalid proof must never fall back to legacy.
                proof=validate_capture_proof(snapshot_data[KEY],captured_at=at,
                    calendar=_CAPTURE_CALENDAR,source_trade_date=str(expected_source_date))
                current=snapshot_data.get('MARKET_DATA_AS_OF')==proof['source_market_close_at']
            else:
                current=(local is not None and local.date().isoformat()==str(expected_source_date)
                    and (local.hour,local.minute)>=(15,0))
        except (TypeError,ValueError,TradingCalendarError):current=False
        if not current:codes=[*codes,'A1_SOURCE_AS_OF_UNPROVEN']
    if not codes:
        return None
    return dict(status='DATA_LIMITED', reason_codes=list(dict.fromkeys(codes)),
        authority='0498_REGENERATION_A1_SOURCE_GAPS' if strict else '0589_A1_SOURCE_GAPS', symbol=symbol,
        source_failures_sha256=hashlib.sha256(json.dumps(failures, ensure_ascii=False,
            sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest(),
        source_as_of=manifest.get('as_of'), a6_explicit_review_required=True,
        maximum_risk_unit='PROBE')


def project_data_limited(payload, detail):
    if detail is None:
        return payload
    result = deepcopy(payload)
    result['DATA_LIMITED'] = True
    previous=result.get('source_data_limits') or {}
    if not isinstance(previous,Mapping):raise ValueError('EXISTING_SOURCE_GAP_DETAIL_INVALID')
    result['source_data_limits'] = {**deepcopy(previous),**deepcopy(detail),
        'reason_codes':list(dict.fromkeys([*previous.get('reason_codes',()),*detail.get('reason_codes',())]))}
    result['degradation_tags'] = sorted(set(result.get('degradation_tags', ())) | {'DATA_LIMITED'})
    result['risk_unit'] = 'PROBE'
    return result


def a6_gap_requirements(catalog, envelopes):
    """The exact frozen PLAN fact, not a model-invented source-gap statement."""
    tagged = {e.plan_id for e in envelopes if 'DATA_LIMITED' in e.degradation_tags}
    result = {}
    for item in catalog.get('items', []):
        if not isinstance(item, Mapping) or item.get('category') != 'PLAN':
            continue
        value = item.get('value') or {}
        if not isinstance(value, Mapping) or value.get('plan_id') not in tagged:
            continue
        detail = (value.get('publication_facts') or {}).get('source_data_limits')
        if (isinstance(detail, Mapping) and detail.get('authority') in {'0498_REGENERATION_A1_SOURCE_GAPS','0589_A1_SOURCE_GAPS','0589_PLAN_HISTORY_GAPS','LJ103_STOCK_NEWS_SOURCE_GAPS'}
                and isinstance(detail.get('reason_codes'), list) and detail['reason_codes']
                and all(isinstance(v, str) and v for v in detail['reason_codes'])):
            result[value['plan_id']] = dict(plan_ref=item['id'], reason_codes=detail['reason_codes'])
            if any(code.startswith('STOCK_NEWS_') for code in detail['reason_codes']):
                result[value['plan_id']]['explicit_gap_treatment_required']=True
    # A corrupt/missing fact never silently disables the review gate.
    for pid in tagged - result.keys():
        result[pid] = dict(plan_ref=None, reason_codes=['SOURCE_GAP_DETAILS_MISSING'])
    return result


def gap_reviewed_ids(output, requirements, *, allow_capacity_trimmed=False):
    rows = output.get('envelope_adjustments', []) if isinstance(output, Mapping) else []
    if not isinstance(rows, list):
        return set()
    result = set()
    for pid, requirement in requirements.items():
        candidates = [row for row in rows if isinstance(row, Mapping) and row.get('plan_id') == pid]
        if len(candidates) != 1 or requirement['plan_ref'] is None:
            continue
        row = candidates[0]
        if requirement.get('explicit_gap_treatment_required'):
            treatment=row.get('gap_review');action=row.get('adjustment')
            if not isinstance(treatment,str) or treatment not in {'KEEP_WITH_GAP','DOWNGRADE','REJECT'}:continue
            if treatment=='REJECT' and action!='SUSPEND':continue
            if treatment=='DOWNGRADE' and action not in {'TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND'}:continue
        texts = row.get('what_would_change_my_mind')
        refs = row.get('supporting_refs')
        if ((row.get('reason_code') != 'DATA_LIMITED_REVIEWED' and not (allow_capacity_trimmed
                and row.get('adjustment')=='SUSPEND' and row.get('reason_code')=='CAPACITY_TRIMMED')) or not isinstance(texts, list)
                or not all(isinstance(t, str) for t in texts) or not isinstance(refs, list)
                or requirement['plan_ref'] not in refs):
            continue
        reviewed = '\n'.join(texts)
        if all(code in reviewed for code in requirement['reason_codes']):
            result.add(pid)
    return result


def restrict_unreviewed(envelopes, reviewed_ids=()):
    reviewed_ids = set(reviewed_ids)
    return tuple(replace(e, eligible=False, risk_unit='NONE', risk_budget=Decimal(0),
        max_shares_cap=0, a6_tightened=True, blocked_reasons=tuple(dict.fromkeys(
            (*e.blocked_reasons, 'A6_DATA_LIMITED_REVIEW_REQUIRED'))))
        if 'DATA_LIMITED' in e.degradation_tags and e.plan_id not in reviewed_ids else e
        for e in envelopes)
