"""Server-owned PM allocation, enforced inside the paper admission transaction.

The deployment target is an objective, not a new risk limit or forced buy.
Current holdings/open buys own a slot before fills, so primary and backup
cannot both acquire it. No current plan or A6 envelope is widened here.
"""
from collections.abc import Mapping
from dataclasses import dataclass,replace
from decimal import Decimal

from .risk_engine import conservative_envelope


@dataclass(frozen=True)
class PMAllocation:
    account_id: str
    trade_date: str
    meeting_id: str
    input_sha256: str
    # (slot, primary, backup), (plan, symbol, cluster), (plan, unit, batches)
    slots: tuple
    plan_bindings: tuple
    allocations: tuple


def compile_pm_allocation(result,*,account_id,trade_date,meeting_id,input_sha256):
    if not isinstance(result,Mapping) or result.get('status')!='READY':return None
    if (result.get('session')!='PM_PREMARKET' or result.get('trade_date')!=trade_date
            or not isinstance(result.get('source_call_id'),str)):
        raise ValueError('PM_ALLOCATION_BINDING_INVALID')
    bindings=result.get('allocation_plan_bindings')
    if (not isinstance(bindings,Mapping) or any(not isinstance(k,str) or not k or
            not isinstance(v,list) or len(v)!=2 or any(not isinstance(x,str) or not x for x in v)
            for k,v in bindings.items())):raise ValueError('PM_ALLOCATION_BINDING_INVALID')
    slots=[];used=[];alloc=[]
    for row in result['slot_plan']:
        number,primary,backup=row['slot'],row['primary_plan_id'],row['backup_plan_id']
        if (type(number) is not int or number<1 or not isinstance(primary,str) or primary not in bindings
                or not isinstance(backup,str) or (backup and backup not in bindings)
                or row['cluster']!=bindings[primary][1]):raise ValueError('PM_ALLOCATION_SLOT_INVALID')
        slots.append((number,primary,backup));used.extend([primary]+([backup] if backup else []))
    for row in result['plan_allocations']:
        pid,unit,batches=row['plan_id'],row['intended_risk_unit'],row['max_batches']
        if (not isinstance(pid,str) or pid not in bindings or unit not in ('STANDARD','PROBE')
                or type(batches) is not int or batches<1):raise ValueError('PM_ALLOCATION_UNIT_INVALID')
        alloc.append((pid,unit,batches))
    if (len({s[0] for s in slots})!=len(slots) or len(set(used))!=len(used)
            or len({a[0] for a in alloc})!=len(alloc) or not set(used).issubset({a[0] for a in alloc})):
        raise ValueError('PM_ALLOCATION_DOMAIN_INVALID')
    return PMAllocation(account_id,trade_date,meeting_id,input_sha256,tuple(slots),
        tuple((pid,*value) for pid,value in sorted(bindings.items())),tuple(alloc))


def enforce_pm_allocation(policy,*,plan,envelope,trade_date,snapshot):
    """Return a tighter envelope and reason, without modifying any source row."""
    if not isinstance(policy,PMAllocation):return envelope,'PM_ALLOCATION_UNAVAILABLE'
    bindings={pid:(symbol,cluster) for pid,symbol,cluster in policy.plan_bindings}
    if policy.trade_date!=trade_date or bindings.get(plan.plan_id)!=(plan.symbol,plan.cluster):
        return envelope,'PM_ALLOCATION_BINDING_INVALID'
    slot=next((row for row in policy.slots if plan.plan_id in row[1:]),None)
    allocation=next((row for row in policy.allocations if row[0]==plan.plan_id),None)
    if slot is None or allocation is None:return envelope,'PM_PLAN_NOT_ALLOCATED'
    occupied={row['plan_id'] for row in snapshot['positions'] if row['qty']>0}
    occupied.update(row['plan_id'] for row in snapshot['open_orders'] if row['side']=='BUY' and row['remaining_qty']>0)
    if (set(slot[1:])-{plan.plan_id,''}) & occupied:return envelope,'PM_SLOT_OCCUPIED'
    changed=envelope
    if allocation[1]=='PROBE' and envelope.risk_unit=='STANDARD':
        changed=conservative_envelope(envelope,consecutive_failures=1)
    return replace(changed,max_batches=min(changed.max_batches,allocation[2])),None
