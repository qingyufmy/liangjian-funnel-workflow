"""Compact frozen PM inputs and explicit conservative server fallback.

This never manufactures a model judgment or new eligibility. All allocations
remain bounded by the A6 envelopes and ordinary paper admission rules.
"""
from copy import deepcopy
from collections import Counter
from pathlib import Path
import json
import math

from .risk_engine import conservative_envelope

DEFAULT_POLICY=dict(schema_version='team-pm-runtime/1',fallback_enabled=True,
    timeout_seconds=106,output_base_tokens=2048,output_per_plan_tokens=512)
PLAN_FIELDS=frozenset(('strategy_profile','strategy_version','primary_theme','theme_id',
    'trigger_low','trigger_high','trigger_zone','entry_reference_zone','stop_level',
    'no_chase_price','first_resistance','pressure_reduce_price','reference_price',
    'market_regime','market_environment','risk_unit','max_batches','plan_priority',
    'plan_priority_rank','expected_holding_sessions','time_stop_sessions','setup_type'))


def load_pm_policy(root):
    import yaml
    path=Path(root)/'config/team_v2_pm.yaml'
    value=yaml.safe_load(path.read_text(encoding='utf-8')) if path.is_file() else dict(DEFAULT_POLICY)
    if (not isinstance(value,dict) or set(value)!=set(DEFAULT_POLICY)
            or value['schema_version']!=DEFAULT_POLICY['schema_version']
            or type(value['fallback_enabled']) is not bool
            or type(value['timeout_seconds']) not in (int,float) or not 0<value['timeout_seconds']<=106
            or any(type(value[k]) is not int or not 0<value[k]<=16384
                   for k in ('output_base_tokens','output_per_plan_tokens'))):
        raise ValueError('PM_RUNTIME_POLICY_INVALID')
    return value


def pm_output_budget(policy,count,configured_max):
    if type(count) is not int or count<0:raise ValueError('PM_PLAN_COUNT_INVALID')
    estimate=policy['output_base_tokens']+count*policy['output_per_plan_tokens']
    return min(configured_max,max(4096,math.ceil(estimate/4096)*4096))


def priority_order(plans):
    def key(row):
        facts=row.get('publication_facts',row)
        rank=facts.get('plan_priority_rank')
        priority={'P1':1,'P2':2,'P3':3}.get(facts.get('plan_priority'),4)
        return (rank if type(rank) is int and rank>0 else 10**9,
                priority,row.get('symbol',''),row['plan_id'])
    return [r['plan_id'] for r in sorted(plans,key=key)]


def project_pm_values(values):
    """One authoritative catalogue, no repeated A3 audit/long trade briefs."""
    result=deepcopy(dict(values));catalog=deepcopy(result['EVIDENCE_CATALOG'])
    for item in catalog.get('items',[]):
        if item.get('category')!='PLAN' or not isinstance(item.get('value'),dict):continue
        row=item['value'];facts=row.get('publication_facts',{})
        row['publication_facts']={k:v for k,v in facts.items() if k in PLAN_FIELDS}
        # Exact owner/hash/date/cluster metadata is retained; only narrative
        # publication fields are removed, never evidence identifiers.
    result['EVIDENCE_CATALOG']=catalog
    ids=[x['id'] for x in catalog.get('items',[]) if x.get('category')=='PLAN']
    for field in ('PLANS','PLANS_TODAY'):
        if field in result:result[field]={'source':'EVIDENCE_CATALOG','plan_evidence_ids':ids}
    return result


def derive_pm_fallback(*,envelopes,plan_clusters,priority,free_slots,max_new_entries,
                       cluster_cap=2,a6_validated=False,utilization_cap=0,a6_degraded_conservative=False):
    if (type(free_slots) is not int or free_slots<0 or type(max_new_entries) is not int
            or max_new_entries<0 or type(cluster_cap) is not int or cluster_cap<1):
        raise ValueError('PM_FALLBACK_CONTEXT_INVALID')
    by_id={e.plan_id:e for e in envelopes}
    if (len(by_id)!=len(envelopes) or set(by_id)!=set(plan_clusters)
            or len(priority)!=len(set(priority)) or set(priority)!=set(by_id)):
        raise ValueError('PM_FALLBACK_DOMAIN_INVALID')
    if type(a6_degraded_conservative) is not bool:
        raise ValueError('PM_FALLBACK_CONTEXT_INVALID')
    if a6_degraded_conservative and any(e.eligible and
            (not e.a6_tightened or e.risk_unit!='PROBE' or e.max_batches!=1) for e in envelopes):
        raise ValueError('PM_FALLBACK_CONSERVATIVE_ENVELOPE_INVALID')
    if not (a6_validated or a6_degraded_conservative) or utilization_cap<=0:
        return dict(slot_plan=[],plan_allocations=[],reason_code='PM_FALLBACK_A6_UNAVAILABLE')
    counts=Counter();selected=[]
    for pid in priority:
        e=by_id[pid];cluster=plan_clusters[pid]
        # Use the admission path's exact PROBE/round-lot projection before
        # consuming a scarce slot. A 100-share STANDARD cap may become zero.
        if e.risk_unit=='STANDARD':e=conservative_envelope(e,consecutive_failures=1)
        if (not e.eligible or e.risk_unit not in ('STANDARD','PROBE') or e.max_shares_cap<=0
                or e.risk_budget<=0 or counts[cluster]>=cluster_cap):continue
        if len(selected)>=min(free_slots,max_new_entries):break
        selected.append(pid);counts[cluster]+=1
    slots=[dict(slot=i,primary_plan_id=pid,backup_plan_id='',cluster=plan_clusters[pid],
        accept_order=i,supporting_refs=[],opposing_refs=[]) for i,pid in enumerate(selected,1)]
    allocations=[dict(plan_id=pid,intended_risk_unit='PROBE',max_batches=1,conviction='NORMAL',
        reason_code='PM_UNAVAILABLE_SERVER_FALLBACK',supporting_refs=[],opposing_refs=[]) for pid in selected]
    return dict(slot_plan=slots,plan_allocations=allocations,
        reason_code='PM_UNAVAILABLE_SERVER_FALLBACK' if selected else 'PM_FALLBACK_NO_ELIGIBLE_PLANS')
