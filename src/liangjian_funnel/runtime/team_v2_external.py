"""Existing A1 cache -> bounded overnight facts or explicit -10pp fallback.

Never fetches sources, guesses missing index values from rates/percentiles, or
changes stale observation times. Expansion follows Claude 0242 risk YAML.
"""
from collections.abc import Mapping
from datetime import datetime,timedelta,time
from decimal import Decimal,InvalidOperation
import hashlib
import json

from ..data.open_macro import _content_hash
from .team_v2_sources import stamp
from .team_v2_external_policy import validate_external_policy

FIELDS=('dow_change_pct','sp500_change_pct','nasdaq_change_pct','nasdaq_futures_change_pct',
        'a50_futures_change_pct','hong_kong_change_pct','usd_cnh','vix')


def assess_external(snapshot,*,now,policy):
    policy=validate_external_policy(policy)
    now=stamp(now);reasons=[];values={};sources={};vix_unavailable=False
    latest_allowed=now.replace(hour=8,minute=30,second=0,microsecond=0)
    def fresh(value):
        try:
            at=stamp(value)
            return at<=latest_allowed<=now and now-at<=timedelta(hours=policy['max_age_hours'])
        except (ValueError,TypeError):return False
    if not isinstance(snapshot,Mapping) or snapshot.get('available') is not True or not fresh(snapshot.get('as_of')):
        reasons.append('EXTERNAL_SNAPSHOT_MISSING')
    else:
        observations=snapshot.get('market_observations')
        if not isinstance(observations,Mapping):observations={}
        for key in FIELDS:
            row=observations.get(key)
            # An unavailable VIX is NULL, never zero and never permission to
            # expand. Other missing/stale fields retain the -10pp fallback.
            if key=='vix' and (row is None or isinstance(row,Mapping) and row.get('value') is None):
                vix_unavailable=True;continue
            if (not isinstance(row,Mapping) or not fresh(row.get('as_of'))
                    or not isinstance(row.get('source_ref'),str) or not row['source_ref'].strip()):
                reasons.append(key);continue
            try:
                value=Decimal(str(row['value']))
                if isinstance(row['value'],bool) or not value.is_finite():raise ValueError
                if key in ('usd_cnh','vix') and value<=0:raise ValueError
            except (KeyError,InvalidOperation,ValueError,TypeError):reasons.append(key);continue
            values[key]=str(value);sources[key]=dict(row)
    if reasons:
        return dict(status='CONSERVATIVE_FALLBACK',reason_code='EXTERNAL_SNAPSHOT_MISSING',
            value_pp=policy['missing_adjustment_pp'],missing_or_stale=reasons,available_observations=sources,
            unavailable_values_are_not_zero=True)
    contraction=(Decimal(values['nasdaq_change_pct'])<=Decimal(str(policy['nasdaq_contraction_lte_pct']))
        or Decimal(values['a50_futures_change_pct'])<=Decimal(str(policy['a50_contraction_lte_pct'])))
    expansion=bool(policy['expansion_enabled'] and not vix_unavailable
        and Decimal(values['nasdaq_change_pct'])>=Decimal(str(policy['nasdaq_expansion_gte_pct']))
        and Decimal(values['a50_futures_change_pct'])>=Decimal(str(policy['a50_expansion_gte_pct']))
        and Decimal(values['vix'])<=Decimal(str(policy['expansion_vix_lte'])))
    return dict(status='FROZEN_PARTIAL_VIX_UNAVAILABLE' if vix_unavailable else 'FROZEN_VALIDATED',
        reason_code='EXTERNAL_RISK_CONTRACTION' if contraction else 'EXTERNAL_RISK_EXPANSION' if expansion else 'EXTERNAL_NO_ADJUSTMENT',
        value_pp=policy['contraction_adjustment_pp'] if contraction else policy['expansion_adjustment_pp'] if expansion else 0,
        observations=sources,vix=None if vix_unavailable else values['vix'],
        policy_version=policy['version'],expansion_enabled=policy['expansion_enabled'])


def read_external(settings,*,now,policy):
    now=stamp(now);candidates=[];observed=[]
    # A 12h morning window spans at most today and yesterday. No directory-wide
    # scan, source call, freshness reset, or stale fallback collection.
    for delta in (0,1):
        path=settings.open_macro_cache_dir/f"open-macro-{(now-timedelta(days=delta)).strftime('%Y%m%d')}.json"
        if not path.is_file() or path.is_symlink():continue
        try:
            raw=path.read_bytes();body=json.loads(raw)
            at=stamp(body['as_of'])
            content={key:value for key,value in body.items() if key!='content_hash'}
            if body.get('content_hash')!=_content_hash(content):raise ValueError
            if at>now.replace(hour=8,minute=30,second=0,microsecond=0):raise ValueError
            snapshot=body.get('GLOBAL_MACRO_SNAPSHOT')
            if not isinstance(snapshot,Mapping):raise ValueError
            if snapshot.get('content_hash')!=_content_hash({k:v for k,v in snapshot.items() if k!='content_hash'}):raise ValueError
            if body.get('cache_status')=='STALE_FALLBACK':raise ValueError
            candidates.append((at,snapshot,hashlib.sha256(raw).hexdigest()))
        except (OSError,ValueError,TypeError,KeyError):
            observed.append('EXTERNAL_CACHE_INVALID')
    selected=max(candidates,key=lambda row:row[0]) if candidates else None
    result=assess_external(selected[1] if selected else None,now=now,policy=policy)
    return {**result,'cache_sha256':selected[2] if selected else None,'cache_issues':observed,
        'source_contract':'A1_GLOBAL_MACRO_SNAPSHOT','source_requests_started':0}
