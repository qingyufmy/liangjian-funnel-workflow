"""Deterministic Team publication-only intersection with the existing RR gate.

Original A3 opinions, target/stop and A4 strategy thresholds stay untouched.
No data acquisition, store, publisher, model or execution permission exists here.
"""
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math

GEOMETRY_FIELD = 'team_plan_geometry'
PUBLICATION_CONTRACT = 'team-publication/rr-geometry-1'
STOP_PUBLICATION_CONTRACT = 'team-publication/stop-rr-geometry-2'
NEWS_STOP_PUBLICATION_CONTRACT = 'team-publication/stop-rr-news-3'
TEAM_PUBLICATION_CONTRACTS = frozenset((PUBLICATION_CONTRACT, STOP_PUBLICATION_CONTRACT, NEWS_STOP_PUBLICATION_CONTRACT))
LEGACY_PUBLICATION_CONTRACT = 'legacy-publication/1'
NEWS_PUBLICATION_CONTRACT = 'legacy-publication/news-limits-2'
SUPPORTED_PUBLICATION_CONTRACTS = TEAM_PUBLICATION_CONTRACTS|{LEGACY_PUBLICATION_CONTRACT,NEWS_PUBLICATION_CONTRACT}
GEOMETRY_FIELDS = frozenset((GEOMETRY_FIELD, 'trigger_high', 'trigger_zone',
    'entry_reference_zone', 'entry_zone', 'pullback_zone', 'stop_level',
    'invalidation_level', 'daily_invalidation', 'invalidation', 'risk',
    'reward_risk', 'stop_distance_pct', 'invalidation_distance_from_low_pct'))


def project_stop_rr_geometry(payload, *, minimum_reward_risk=None):
    """Version 2 only. Preserve v1 reproduction, then enforce the new stop floor.

    Widening the stop can invalidate a formerly valid RR. Such a row is rejected;
    this policy does not widen the target or change the original model opinion.
    """
    from .plan_stop_geometry import minimum_stop, derived_geometry, POLICY
    legacy=project_team_plan_geometry(payload,minimum_reward_risk=minimum_reward_risk)
    audit=deepcopy(legacy['audit'])
    audit.update(schema_version='team-plan-geometry/2',stop_geometry_policy=POLICY,
                 minimum_low_distance='0.01',minimum_total_distance='0.02',
                 total_distance_denominator='effective_trigger_high',
                 original_reward_risk=deepcopy(payload.get('reward_risk')),
                 original_stop=audit['stop'])
    if legacy['status']!='PUBLISHABLE':return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    low,high,stop,target=(_decimal(audit[k]) for k in ('trigger_low','effective_trigger_high','stop','target'))
    newstop=minimum_stop(low,high,stop)
    derived=derived_geometry(low,high,newstop,target)
    audit.update(stop=str(newstop),derived_geometry=derived)
    if Decimal(str(target))-high < _decimal(audit['minimum_reward_risk'])*(high-Decimal(str(newstop))):
        audit.update(classification='EMPTY',reason_code='PLAN_MINIMUM_STOP_RR_INFEASIBLE')
        return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    result=legacy['payload']
    for key in ('stop_level','invalidation_level','daily_invalidation','invalidation'):
        if key in result:result[key]=newstop
    # A4 consumes stop_level with first precedence; make the common source explicit.
    result['stop_level']=newstop
    if isinstance(result.get('risk'),Mapping):
        risk=deepcopy(dict(result['risk']))
        for key in ('stop_level','invalidation_level'):
            if key in risk:risk[key]=newstop
        for key in ('reward_risk','stop_distance_pct'):
            if key in risk:risk[key]=derived[key]
        result['risk']=risk
    result.update(derived)
    audit['reason_code']='PLAN_STOP_AND_RR_GEOMETRY_VERIFIED'
    result[GEOMETRY_FIELD]=audit
    return dict(status='PUBLISHABLE',payload=result,audit=audit)


def _hash(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _lookup(root, *paths):
    for path in paths:
        value=root
        for key in path:
            if not isinstance(value,Mapping) or key not in value:
                value=None;break
            value=value[key]
        if value is not None:return value
    return None


def _decimal(value):
    if value is None or isinstance(value,bool):return None
    try:value=Decimal(str(value))
    except (ValueError,InvalidOperation):return None
    return value if value.is_finite() and value>0 else None


def _float_not_above(value):
    number=float(value)
    if not math.isfinite(number):raise ValueError('PLAN_GEOMETRY_NUMBER_INVALID')
    if Decimal(str(number))>value:number=math.nextafter(number,-math.inf)
    return number


def project_team_plan_geometry(payload, *, minimum_reward_risk=None):
    """Project an unmodified normalized publication payload, never raise a price.

    The threshold follows A4's field precedence, with the actual frozen snapshot
    threshold used only when no explicit per-plan threshold exists. A missing
    target is UNPROVEN, not an invented resistance or an EMPTY market finding.
    """
    if not isinstance(payload,Mapping) or GEOMETRY_FIELD in payload:
        raise ValueError('PLAN_GEOMETRY_ORIGINAL_INVALID')
    zone=_lookup(payload,('trigger_zone',),('entry_reference_zone',),('entry_zone',),
        ('pullback_zone',),('strategy_facts','entry_reference_zone'))
    low=_decimal(_lookup(zone,('low',),('min',))) if isinstance(zone,Mapping) else _decimal(payload.get('trigger_low'))
    high=_decimal(_lookup(zone,('high',),('max',))) if isinstance(zone,Mapping) else _decimal(payload.get('trigger_high'))
    stop=_decimal(_lookup(payload,('stop_level',),('invalidation_level',),('daily_invalidation',),
        ('risk','stop_level'),('risk','invalidation_level')))
    target=_decimal(_lookup(payload,('first_resistance',),('pressure_reduce_price',),
        ('observation_target',),('strategy_facts','observation_targets','r2')))
    raw_rr=_lookup(payload,('deterministic_price_evidence','minimum_reward_risk'),
        ('minimum_reward_risk',),('min_reward_risk',),('risk','minimum_reward_risk'))
    rr=_decimal(minimum_reward_risk if raw_rr is None else raw_rr)
    audit=dict(schema_version='team-plan-geometry/1',authority='SERVER_PUBLICATION_TIGHTENING_NOT_MODEL_OPINION',
        original_payload_sha256=_hash(payload),minimum_reward_risk=str(rr) if rr is not None else None,
        threshold_source='FROZEN_SNAPSHOT_MIN_REWARD_RISK' if raw_rr is None else 'ORIGINAL_PLAN_RR_FIELD',
        trigger_low=str(low) if low is not None else None,original_trigger_high=str(high) if high is not None else None,
        stop=str(stop) if stop is not None else None,target=str(target) if target is not None else None,
        rr_entry_ceiling=None,effective_trigger_high=None,classification='UNPROVEN',reason_code='PLAN_GEOMETRY_RR_UNPROVEN')
    if None in (low,high,stop,target,rr) or low>high:
        return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    # target<=stop proves no positive-payoff entry above the existing stop.
    with localcontext() as context:
        context.prec=50
        ceiling=(target+rr*stop)/(1+rr)
    audit['rr_entry_ceiling']=str(ceiling)
    if target<=stop or ceiling<low or high<=stop:
        audit.update(classification='EMPTY',reason_code='PLAN_GEOMETRY_RR_INFEASIBLE')
        return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    # This contract only lowers upper. It must not raise a lower that lies at
    # or below stop; that unresolved geometry is kept non-executable.
    if low<=stop:return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    upper=min(high,ceiling);number=_float_not_above(upper)
    while target-Decimal(str(number)) < rr*(Decimal(str(number))-stop):
        number=math.nextafter(number,-math.inf)
    if Decimal(str(number))<low:
        audit.update(classification='EMPTY',reason_code='PLAN_GEOMETRY_RR_INFEASIBLE')
        return dict(status='OBSERVATION_ONLY',payload=None,audit=audit)
    audit.update(classification='PARTIAL' if ceiling<high else 'FULL',
        effective_trigger_high=str(Decimal(str(number))) if ceiling<high else str(high),
        reason_code='PLAN_GEOMETRY_RR_TIGHTENED' if ceiling<high else 'PLAN_GEOMETRY_RR_FEASIBLE')
    result=deepcopy(dict(payload))
    if ceiling<high:
        bound=Decimal(str(number))
        # Aliases can already be stricter than the selected A4 zone. Clamp
        # every existing upper to its OWN original value, not to another
        # alias's wider ceiling. Unknown/missing uppers remain unknown.
        scalar=_decimal(result.get('trigger_high'))
        if scalar is not None and scalar>bound:
            result['trigger_high']=number
        for name in ('trigger_zone','entry_reference_zone','entry_zone','pullback_zone'):
            if isinstance(result.get(name),Mapping):
                value=dict(result[name])
                for key in ('high','max'):
                    original=_decimal(value.get(key))
                    if original is not None and original>bound:
                        value[key]=number
                result[name]=value
        # A nested-only original keeps the original nested facts immutable;
        # the new authoritative top-level zone is a narrower explicit alias.
        if not any(isinstance(result.get(k),Mapping) for k in ('trigger_zone','entry_reference_zone','entry_zone','pullback_zone')):
            result['trigger_zone']={'low':str(low),'high':number}
    result[GEOMETRY_FIELD]=audit
    return dict(status='PUBLISHABLE',payload=result,audit=audit)
