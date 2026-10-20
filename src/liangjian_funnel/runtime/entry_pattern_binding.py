"""Server B confirmation -> immutable conservative execution geometry.

No source, model, database or broker calls. Runtime overlays never edit the
published plan. The original plan and condition proof remain hash-addressed.
"""
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import re


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),default=str).encode()).hexdigest()


def _positive(value):
    if isinstance(value,bool):raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    try:number=Decimal(str(value))
    except Exception:raise ValueError('ENTRY_PATTERN_BINDING_INVALID') from None
    if not number.is_finite() or number<=0:raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    return number


def make_b_execution_binding(plan,evidence,*,source_sha256=None,plan_id=None):
    from .entry_confirmation import _levels
    if (not isinstance(evidence,Mapping) or evidence.get('mode')!='pattern_v1'
            or evidence.get('pattern')!='B_BREAKOUT_RETEST' or evidence.get('confirmed') is not True
            or evidence.get('force_probe') is not True):raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    conditions=evidence.get('conditions',{});required=evidence.get('required_conditions',())
    expected={'B_KEY_LEVEL','B_KEY_BELOW_NO_CHASE','B_EFFECTIVE_BREAKOUT','B_RETEST','B_RETEST_VOLUME',
        'B_ACCEPTED','B_STOP_ONLY_RAISED','B_NO_CHASE','ENTRY_RR'}
    if set(required)!=expected or any(conditions.get(k,{}).get('status')!='MET' for k in expected):
        raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    old,target,minimum,chase=map(_positive,_levels(plan))
    entry=_positive(evidence.get('current_entry_close'));stop=_positive(evidence.get('tightened_stop'))
    key=_positive(evidence.get('metrics',{}).get('key_level'))
    if not old<=stop<entry<target or entry>chase or key>chase or (target-entry)/(entry-stop)<minimum:
        raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    try:at=datetime.fromisoformat(str(evidence['source_as_of']))
    except (KeyError,ValueError):raise ValueError('ENTRY_PATTERN_BINDING_INVALID') from None
    if at.utcoffset() is None or at.minute%5 or at.second or at.microsecond:
        raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    source_sha256=source_sha256 or digest(plan)
    if not re.fullmatch('[a-f0-9]{64}',source_sha256):raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    body=dict(version='entry-pattern-b/1',pattern='B_BREAKOUT_RETEST',source_plan_sha256=source_sha256,
        proof_sha256=digest(evidence),symbol=plan.get('symbol'),plan_id=plan_id or plan.get('plan_id'),
        original_stop=str(old),stop=str(stop),entry_price=str(entry),target=str(target),
        minimum_reward_risk=str(minimum),reward_risk=str((target-entry)/(entry-stop)),
        no_chase=str(chase),key_level=str(key),source_as_of=at.isoformat(),risk_unit='PROBE')
    return {**body,'binding_sha256':digest(body)}


def checked_binding(binding):
    if not isinstance(binding,Mapping):raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    body={k:v for k,v in binding.items() if k!='binding_sha256'}
    if (body.get('version')!='entry-pattern-b/1' or body.get('risk_unit')!='PROBE'
            or body.get('pattern')!='B_BREAKOUT_RETEST' or digest(body)!=binding.get('binding_sha256')
            or not all(re.fullmatch('[a-f0-9]{64}',str(body.get(k,''))) for k in ('source_plan_sha256','proof_sha256'))):
        raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    old,stop,entry,target,minimum,chase,key=map(lambda k:_positive(body.get(k)),
        ('original_stop','stop','entry_price','target','minimum_reward_risk','no_chase','key_level'))
    if not old<=stop<entry<target or entry>chase or key>chase or (target-entry)/(entry-stop)<minimum:
        raise ValueError('ENTRY_PATTERN_BINDING_INVALID')
    return dict(binding)


def bind_team_plan(plan,binding):
    proof=checked_binding(binding)
    if (proof['symbol']!=plan.symbol or proof.get('plan_id') not in (None,plan.plan_id)
            or _positive(proof['original_stop'])!=plan.stop_price or plan.status!='ACTIVE_TODAY'):
        raise ValueError('ENTRY_PATTERN_PLAN_BINDING_CONFLICT')
    entry=_positive(proof['entry_price']);stop=_positive(proof['stop'])
    if _positive(proof['no_chase'])!=plan.no_chase:raise ValueError('ENTRY_PATTERN_PLAN_BINDING_CONFLICT')
    # A B confirmation is a separate, single-price current runtime window.
    # It cannot resurrect a plan excluded from publication or morning scope.
    return replace(plan,price=entry,stop_price=stop,entry_low=entry,entry_high=entry)


def probe_envelope(envelope):
    from .risk_engine import conservative_envelope
    return conservative_envelope(envelope,consecutive_failures=1)


def validate_team_intent(plan,envelope,binding):
    proof=checked_binding(binding)
    if (plan.symbol!=proof['symbol'] or proof.get('plan_id') not in (None,plan.plan_id)
            or plan.stop_price!=_positive(proof['stop']) or plan.price!=_positive(proof['entry_price'])
            or not plan.entry_low==plan.price==plan.entry_high or plan.no_chase!=_positive(proof['no_chase'])
            or envelope.eligible and envelope.risk_unit!='PROBE'):
        raise ValueError('ENTRY_PATTERN_INTENT_BINDING_CONFLICT')
    return proof


def held_stop_overlays(plans,snapshot):
    """Restore the admitted B stop across ticks/restarts, only upwards.

    Dispatched buys are durable owned-paper evidence, not model proposals or
    merely evaluated signals. Rejected/non-dispatched admissions do not count.
    """
    result=dict(plans)
    held={(p['plan_id'],p['symbol']) for p in snapshot.get('positions',())}
    # The existing broker reader already joins open lots -> original fill ->
    # admission and exposes its immutable stop. Use that narrow owned proof
    # rather than a current-day admissions-only query (which loses prior days).
    for pid,references in snapshot.get('entry_risk_references',{}).items():
        plan=result.get(pid)
        if plan is None or (pid,plan.symbol) not in held:continue
        for reference in references:
            if reference.get('basis')!='OWNED_OPEN_LOT_AND_ORIGINAL_ADMISSION_STOP':continue
            stop=_positive(reference.get('stop_price'));entry=_positive(reference.get('entry_price'))
            if entry<=stop or type(reference.get('qty')) is not int or reference['qty']<=0:
                raise ValueError('ENTRY_PATTERN_HELD_STOP_CONFLICT')
            plan=replace(plan,stop_price=max(plan.stop_price,stop))
        result[pid]=plan
    for row in snapshot.get('admissions',()):
        source=row.get('binding',{});binding=source.get('entry_pattern_binding')
        if not row.get('dispatched') or source.get('action') not in {'BUY_SIGNAL','ADD_SIGNAL'} or binding is None:continue
        proof=checked_binding(binding);pid=source.get('plan_id');plan=result.get(pid)
        if plan is None or (pid,plan.symbol) not in held:continue
        if proof['symbol']!=plan.symbol or _positive(source.get('plan',{}).get('stop_price'))!=_positive(proof['stop']):
            raise ValueError('ENTRY_PATTERN_HELD_STOP_CONFLICT')
        result[pid]=replace(plan,stop_price=max(plan.stop_price,_positive(proof['stop'])))
    return result
