"""0298 faithful A4 projections and deterministic scope partitioning.

Only model-facing views change. Full plan/context sources and all engine risk
inputs remain owned by the caller. Missing 5m ATR is not a guessed daily ATR.
"""
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal,InvalidOperation
import hashlib

from .team_v2_store import canonical
from .team_v2_sources import meeting_plan_facts


def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def _positive(value):
    if isinstance(value,bool):return None
    try:number=Decimal(str(value))
    except (ValueError,InvalidOperation,TypeError):return None
    return number if number.is_finite() and number>0 else None


def select_model_scope(plans,*,active_plan_ids,held_plan_ids,evaluations,atr5m=None):
    """Unknown proximity remains explicit/included; never invent OUT_OF_WINDOW."""
    held=set(held_plan_ids.values());selected=set(held);decisions=[];atr5m=atr5m or {}
    if not (set(active_plan_ids)|held)<=set(plans):raise ValueError('TRADER_INPUT_SCOPE_UNPROVEN')
    for pid in sorted(set(active_plan_ids)|held):
        plan=plans[pid];action=evaluations.get(pid,{}).get('action')
        if pid in held:reason='HELD_ALWAYS'
        elif action is None:reason='ENGINE_ACTION_UNPROVEN'
        elif action!='NO_ACTION':reason='ENGINE_EVENT'
        elif plan.entry_low<=plan.price<=plan.entry_high:reason='INSIDE_ENTRY_BAND'
        else:
            atr=_positive(atr5m.get(pid));invalid=plan.invalidation_price
            if atr is None or invalid is None:reason='ATR5M_PROXIMITY_UNPROVEN'
            else:
                distance=min(abs(plan.price-plan.entry_low),abs(plan.price-plan.entry_high),abs(plan.price-invalid))
                reason='NEAR_TRIGGER_WITHIN_ATR5M' if distance<=atr else 'OUT_OF_WINDOW'
        sent=reason!='OUT_OF_WINDOW'
        if sent:selected.add(pid)
        decisions.append(dict(plan_id=pid,action='MODEL_PENDING' if sent else 'NO_ACTION',
            reason_code=reason,not_sent_to_model=not sent,model_judgment_denominator=sent))
    return dict(plan_ids=sorted(selected),decisions=decisions,
        r1_proximity_complete=all(r['reason_code']!='ATR5M_PROXIMITY_UNPROVEN' for r in decisions))


def shard_model_scope(plan_ids,held_plan_ids):
    ids=set(plan_ids);held=set(held_plan_ids.values())
    if not held<=ids or len(held)>8:raise ValueError('TRADER_HELD_FIRST_SHARD_UNPROVEN')
    ordered=[*sorted(held),*sorted(ids-held)]
    return [ordered[start:start+8] for start in range(0,len(ordered),8)]


def model_closed_context(context,one_minute,*,cutoff):
    """R3 model bars only; engine technical statistics remain byte-identical."""
    from ..data.execution_evidence import execution_evidence
    bars=tuple(row for row in one_minute if row.bar_end<=cutoff and row.bar_end.date()==cutoff.date())
    aggregated,_=execution_evidence(bars,(),as_of=cutoff)
    view=deepcopy(context)
    view['closed_bars']={'1m':[row.model_dump(mode='json') for row in bars[-60:]],
        '5m':list(aggregated['5m']),'15m':list(aggregated['15m'])}
    return view


def project_trader_values(values,*,plan_rows,contexts,model_contexts=None):
    """Original IDs + raw fact hashes + explicit projected field paths."""
    result=deepcopy(values);active=result.get('ACTIVE_PLANS',[])
    ids=[row['plan_id'] for row in active]
    held={row['plan_id'] for row in result.get('POSITIONS',[])}
    visible=set(ids)|held
    facts={row['plan_id']:row for row in meeting_plan_facts(plan_rows)}
    if not visible<=set(facts) or not visible<=set(contexts):raise ValueError('TRADER_PROJECTION_SOURCE_MISSING')
    for row in active:
        fact=facts[row['plan_id']];publication=fact['publication_facts']
        row.update(strategy_profile=publication.get('strategy_profile','MISSING_ORIGINAL_PROFILE'),
            trade_brief=publication.get('trade_brief',publication.get('trade_plan_brief',
                publication.get('trading_plan_brief','MISSING_ORIGINAL_BRIEF'))),
            publication_facts=publication,source_row_sha256=fact['source_row_sha256'],
            original_payload_sha256=fact['original_payload_sha256'])
    result['BARS']={pid:deepcopy((model_contexts or contexts)[pid]) for pid in sorted(visible)}
    result['TRADABILITY']={pid:deepcopy(contexts[pid].get('original_tradability_contract',
        {'status':'DATA_LIMITED','reason_code':'ORIGINAL_TRADABILITY_PROOF_MISSING'})) for pid in sorted(visible)}
    for name in ('RISK_ENVELOPES','TECH_REFERENCE'):
        if isinstance(result.get(name),Mapping):result[name]={pid:v for pid,v in result[name].items() if pid in visible}
    items=[];index={pid:i for i,pid in enumerate(ids)}
    for original in result['EVIDENCE_CATALOG']['items']:
        item=deepcopy(original);category=item['category'];pid=None
        if category=='PLAN':
            pid=next((key for key in visible if item['id'].endswith('PLAN:'+key)),None)
            if pid is None:continue
            # Held-only owners remain PLAN facts even if no new-entry signal is allowed.
            path=['ACTIVE_PLANS',index[pid],'publication_facts'] if pid in index else ['HELD_PLAN_FACTS',pid]
            if pid not in index:result.setdefault('HELD_PLAN_FACTS',{})[pid]=facts[pid]['publication_facts']
            item['value']=dict(id=item['id'],original_fact_sha256=digest(original['value']),input_ref=path,
                original_payload_sha256=facts[pid]['original_payload_sha256'])
        elif category=='STOCK_TECH':
            pid=next((key for key in visible if item['id'].endswith('TECH:'+key)),None)
            if pid is None:continue
            item['value']=dict(id=item['id'],original_fact_sha256=digest(contexts[pid]),
                projected_fact_sha256=digest(result['BARS'][pid]),input_ref=['BARS',pid],
                projection='R3_1M_LAST60_5M_15M_ALL_CLOSED_TODAY_ENGINE_STATS_UNCHANGED')
        items.append(item)
    result['EVIDENCE_CATALOG']['items']=items
    return result
