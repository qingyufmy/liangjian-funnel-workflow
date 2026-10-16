"""Fresh execution caps intersect (never promote) the frozen A6 morning caps.

Activation/current source proof belongs to the source edge. This pure stitch
never rewrites a PLANNING envelope's purpose to make it executable. Held exits
still pass through the separate quantity/T+1 path, not entry eligibility.
"""
from dataclasses import replace
from decimal import Decimal

from .risk_engine import build_risk_envelope,conservative_envelope


def refresh_execution_envelopes(plans, *, morning_envelopes,account,positions,limits,now,regime,
        consecutive_failures=0):
    if type(consecutive_failures) is not int or consecutive_failures<0:
        raise ValueError('EXECUTION_A6_FAILURE_STATE_INVALID')
    original={e.plan_id:e for e in morning_envelopes}
    if (len(original)!=len(morning_envelopes) or any(e.purpose!='PLANNING' for e in morning_envelopes)
            or len({p.plan_id for p in plans})!=len(plans)):
        raise ValueError('EXECUTION_ENVELOPE_SOURCE_DOMAIN_INVALID')
    refreshed=[]
    for plan in plans:
        fresh=build_risk_envelope(plan,account,positions,limits,now=now,regime=regime)
        previous=original.get(plan.plan_id)
        if previous is None:
            refreshed.append(replace(fresh,eligible=False,risk_unit='NONE',risk_budget=Decimal(0),
                max_shares_cap=0,blocked_reasons=tuple(dict.fromkeys((*fresh.blocked_reasons,'A6_MORNING_PLAN_NOT_REVIEWED')))))
            continue
        if (previous.symbol!=plan.symbol or previous.trade_date!=now.date().isoformat()):
            raise ValueError('EXECUTION_ENVELOPE_SOURCE_BINDING_MISMATCH')
        if previous.risk_unit=='PROBE' and fresh.risk_unit=='STANDARD':
            fresh=conservative_envelope(fresh,consecutive_failures=1)
        reasons=tuple(dict.fromkeys((*fresh.blocked_reasons,*previous.blocked_reasons)))
        eligible=fresh.eligible and previous.eligible and not reasons
        value=replace(fresh,eligible=eligible,risk_unit=fresh.risk_unit if eligible else 'NONE',
            max_shares_cap=min(fresh.max_shares_cap,previous.max_shares_cap) if eligible else 0,
            max_batches=min(fresh.max_batches,previous.max_batches),
            risk_budget=min(fresh.risk_budget,previous.risk_budget),blocked_reasons=reasons,
            a6_tightened=fresh.a6_tightened or previous.a6_tightened,high_conviction=False)
        # Persistent failure latch is additional authority, not reset by a new
        # price, activation status, successful PM session or process restart.
        if consecutive_failures>=1:value=conservative_envelope(value,consecutive_failures=consecutive_failures)
        refreshed.append(value)
    return tuple(refreshed)
