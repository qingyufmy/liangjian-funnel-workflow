"""A5 causal facts from frozen originals, not a re-evaluation or counterfactual fill."""
from collections import Counter
from copy import deepcopy
from datetime import datetime
import hashlib,json


def _hash(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()


def plan_causal_projection(full):
    legacy=full.get('legacy_facts') or {};a4=legacy.get('a4') or {}
    replay=legacy.get('a3_plan_replay') or {};frozen=replay.get('frozen_plans') or {}
    plans=full.get('a3',{}).get('plans') or []
    by_plan={}
    for plan in plans:
        pid=plan.get('plan_id')
        if not pid:continue
        by_plan[pid]=dict(plan=deepcopy(plan),original_frozen_plan=deepcopy(frozen.get(pid)),
            condition_samples=[],legacy_lifecycle=[],replay_summary=[])
    # Keep every distinct observed condition state, with the full source count
    # and endpoints. Sampling gaps are not silently interpreted as continuity.
    states={pid:{} for pid in by_plan};excluded=0
    for event in a4.get('events',[]):
        pid=event.get('plan_id')
        if pid not in by_plan:excluded+=1;continue
        condition={k:deepcopy(event.get(k)) for k in ('action','reason_code','diagnostic_code','effective','strategy_reason_codes','unmet_conditions')}
        key=_hash(condition);previous=states[pid].get(key)
        if previous is None:
            states[pid][key]=dict(condition=condition,count=1,first_at=event.get('minute_end'),last_at=event.get('minute_end'),
                first_evidence_id=event.get('evidence_id'),last_evidence_id=event.get('evidence_id'))
        else:
            previous['count']+=1;previous['last_at']=event.get('minute_end');previous['last_evidence_id']=event.get('evidence_id')
    for pid in by_plan:by_plan[pid]['condition_samples']=list(states[pid].values())
    archived=replay.get('archived_inputs') or {};day=full.get('trade_date')
    columns=['bar_end','open','high','low','close','volume','amount']
    for pid,entry in by_plan.items():
        symbol=entry['plan'].get('symbol');packet=archived.get(symbol) or {};bars=[]
        for bar in packet.get('five_minute',[]):
            try:at=datetime.fromisoformat(bar['bar_end'])
            except (ValueError,TypeError,KeyError):continue
            if at.utcoffset() is not None and at.date().isoformat()==day and bar.get('interval')=='5m':
                bars.append([deepcopy(bar.get(column)) for column in columns])
        entry['closed_5m']=dict(columns=columns,rows=bars,bar_count=len(bars),
            source_input_sha256=packet.get('sha256'),source_evidence_id=packet.get('evidence_id'),
            source_path='$.legacy_facts.a3_plan_replay.archived_inputs.'+str(symbol)+'.five_minute',
            availability='SOURCE_SAMPLES_NOT_COMPLETENESS_ASSERTION',not_a_hypothetical_fill=True)
    for field,target in (('lifecycles','legacy_lifecycle'),):
        for item in a4.get(field,[]):
            if item.get('plan_id') in by_plan:by_plan[item['plan_id']][target].append(deepcopy(item))
    for item in replay.get('plans',[]):
        if item.get('plan_id') in by_plan:by_plan[item['plan_id']]['replay_summary'].append(deepcopy(item))
    return dict(schema='a5-plan-causal-facts/1',by_plan=by_plan,
        legacy_account_scope='CUSTOMER_PAPER_ACCOUNT_NOT_TEAM',
        team_paper_account=deepcopy(full.get('paper_broker')),
        morning_source_reference='$.a6.rows',
        source_paths=['$.a3.plans','$.legacy_facts.a3_plan_replay.frozen_plans','$.legacy_facts.a4.events',
            '$.legacy_facts.a4.lifecycles','$.paper_broker'],
        source_sha256={name:_hash(value) for name,value in dict(plans=plans,events=a4.get('events',[]),
            lifecycles=a4.get('lifecycles',[]),replay=replay.get('plans',[])).items()},
        excluded_non_current_plan_event_count=excluded,
        coverage='MULTI_SAMPLE_NOT_CONTINUOUS;MFE_MAE_R_ONLY_WHEN_ORIGINAL_SUPPLIES_IT',
        missing_plan_count=max(0,len(frozen)-len(by_plan)),
        unavailable_fields='Missing source fields remain missing; no model/source acquisition or invented metrics')
