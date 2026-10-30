"""Concurrent bounded PM requests; one deterministic audited aggregation.

An aggregate is NOT a synthetic model call. Each contributing reply has its
own immutable call/catalog; evidence consumers revalidate every child before
recomputing the aggregate. Unreviewed IDs can never receive an allocation.
"""
from concurrent.futures import ThreadPoolExecutor,wait
from copy import deepcopy
from datetime import datetime,time
from decimal import Decimal
from dataclasses import asdict
import hashlib
import json
from zoneinfo import ZoneInfo

from .team_v2_store import canonical
from ..pipeline.team_v2_pm_schema import project_pm_compact

TZ=ZoneInfo('Asia/Shanghai')
digest=lambda value:hashlib.sha256(canonical(value).encode()).hexdigest()


def aggregate_pm_outputs(outputs,*,catalog,plan_refs,context):
    if not outputs:raise ValueError('PM_BATCH_NO_VALID_OUTPUT')
    # Every global target is an original validated opinion. Taking the lowest
    # is a monotone restriction; no averaged or invented target is authored.
    base=deepcopy(min(outputs,key=lambda x:Decimal(str(x['deployment_target']['target_pct']))))
    base['plan_decisions']=[deepcopy(row) for item in outputs for row in item['plan_decisions']]
    derived,validation=project_pm_compact(base,catalog,plan_refs=plan_refs,**context)
    if not validation.valid:raise ValueError('PM_BATCH_AGGREGATE_INVALID:'+','.join(validation.issues))
    return base,derived


def run_pm_batches(*,run_one,arguments,all_values,shortlist,plan_refs):
    args=dict(arguments);store=args['store'];clock=args['clock'];day=args['trade_date']
    from .morning_window_authority import morning_deadline
    authorized=morning_deadline(day,args['source_trade_date'],authorization=args.get('window_authorization'))
    deadline=args.get('deadline_at') or authorized
    if (deadline.utcoffset() is None or deadline.astimezone(TZ).date().isoformat()!=day
            or deadline>authorized):
        raise ValueError('PM_BATCH_DEADLINE_INVALID')
    ids=shortlist['selected_plan_ids'];capacity=2*args['max_positions']
    batches=[ids[i:i+capacity] for i in range(0,len(ids),capacity)]
    signature=digest(dict(source_input=all_values,shortlist=shortlist,account_id=args['account_id'],
        run_id=args['run_id'],call_id=args['call_id'],source_day=args['source_trade_date'],day=day,
        envelopes=[asdict(e) for e in args['envelopes']],clusters=args['plan_clusters'],free_slots=args['free_slots'],
        baseline=args['baseline_range'],utilization_cap=args['utilization_cap'],deadline=deadline.isoformat(),
        **({'window_authorization':args['window_authorization']}if args.get('window_authorization')is not None else {})))
    previous=store.load_pm(args['meeting_id'],input_sha256=signature)
    if previous is not None:return previous
    pool=ThreadPoolExecutor(max_workers=len(batches),thread_name_prefix='pm-frozen-batch')
    futures={};audit=[]
    for ordinal,selected in enumerate(batches,1):
        subset=set(selected);child=dict(args);values=deepcopy(dict(args['replacements']))
        values['EVIDENCE_CATALOG']['items']=[item for item in values['EVIDENCE_CATALOG']['items']
            if item.get('category')!='PLAN' or item['value']['plan_id'] in subset]
        child.update(replacements=values,envelopes=tuple(e for e in args['envelopes'] if e.plan_id in subset),
            plan_clusters={pid:args['plan_clusters'][pid] for pid in selected},
            geometry_plan_ids=subset,high_conviction_ids=set(args['high_conviction_ids'])&subset,
            meeting_id=args['meeting_id']+f':batch:{ordinal:03}',call_id=args['call_id']+f':batch:{ordinal:03}',
            fallback_enabled=False,fallback_priority_order=tuple(selected),
            deadline_at=None,
            timeout_seconds=min(args['timeout_seconds'],max(0,(deadline-clock()).total_seconds()-2)))
        frozen=store.freeze_control_context('pm-batch-budget:'+child['meeting_id'],source_sha256=signature,
            body=dict(timeout_seconds=child['timeout_seconds']))
        remaining=(deadline-clock()).total_seconds()-2
        child['timeout_seconds']=frozen['timeout_seconds']
        row=dict(ordinal=ordinal,plan_ids=selected,meeting_id=child['meeting_id'],call_id=child['call_id'],
            budget_observed_at=clock().isoformat(),remaining_seconds=remaining,
            frozen_timeout_seconds=frozen['timeout_seconds'],status='PM_NOT_REVIEWED_TIME_BUDGET')
        audit.append(row)
        if child['timeout_seconds']>0 and (child['timeout_seconds']<=remaining or args['journal'].has_call_claim(child['call_id'])):
            futures[pool.submit(run_one,**child)]=row
    completed,pending=wait(futures,timeout=max(0,(deadline-clock()).total_seconds()-2))
    pool.shutdown(wait=False,cancel_futures=True)
    valid=[];unreviewed=[]
    for future,row in futures.items():
        if future not in completed:
            unreviewed.extend(row['plan_ids']);continue
        try:decision=future.result()
        except Exception as exc:
            row.update(status='PM_BATCH_FAILED',exception_class=type(exc).__name__)
            unreviewed.extend(row['plan_ids']);continue
        if decision.get('model_validated') is not True or decision.get('decision_authority')!='VALIDATED_MODEL':
            from contextlib import closing
            with closing(args['journal']._connect()) as db:
                trace=db.execute('SELECT body_json FROM model_call_telemetry WHERE call_id=?',(row['call_id'],)).fetchone()
            failure_code=json.loads(trace[0]).get('failure_code') if trace else None
            row.update(status='PM_NOT_REVIEWED_TIME_BUDGET' if (clock()>=deadline or any(
                'TIMEOUT' in code or 'DEADLINE' in code for code in decision.get('reason_codes',[])) or
                failure_code in {'PM_FIRST_PACKET_TIMEOUT','PM_TOTAL_TIMEOUT','MODEL_WALL_CLOCK_TIMEOUT','MODEL_TOTAL_DEADLINE_EXCEEDED'}) else 'PM_BATCH_FAILED',
                reason_codes=decision.get('reason_codes',[]),failure_code=failure_code)
            unreviewed.extend(row['plan_ids']);continue
        raw=decision.get('model_compact_output')
        if not isinstance(raw,dict):raise ValueError('PM_BATCH_ORIGINAL_MISSING')
        from contextlib import closing
        with closing(args['journal']._connect()) as db:
            original=db.execute('SELECT captured_at FROM model_calls WHERE call_id=?',(row['call_id'],)).fetchone()
        if original is None:raise ValueError('PM_BATCH_CAPTURE_MISSING')
        if datetime.fromisoformat(original[0])>=deadline:
            row.update(status='PM_NOT_REVIEWED_TIME_BUDGET',late_capture_at=original[0])
            unreviewed.extend(row['plan_ids']);continue
        row.update(status='VALIDATED_MODEL',input_hash=decision['input_hash'],
            raw_sha256=decision['server_projection']['raw_output_sha256'],
            result_sha256=digest(decision))
        valid.append((row['ordinal'],raw,row))
    unsent={pid for row in audit if row['call_id'] not in {r['call_id'] for r in futures.values()} for pid in row['plan_ids']}
    unreviewed=sorted(set(unreviewed)|unsent,key=ids.index)
    valid.sort(key=lambda x:x[0]);reviewed={pid for _,_,row in valid for pid in row['plan_ids']}
    completed_refs={ref:pid for ref,pid in plan_refs.items() if pid in reviewed}
    context=dict(required_envelope=args['required_envelope'],envelopes=args['envelopes'],
        plan_clusters=args['plan_clusters'],free_slots=args['free_slots'],baseline_range=args['baseline_range'],
        utilization_cap=args['utilization_cap'],high_conviction_ids=args['high_conviction_ids'])
    failed=any(row['status']=='PM_BATCH_FAILED' for row in audit)
    if valid and not failed:
        raw,derived=aggregate_pm_outputs([x[1] for x in valid],catalog=all_values['EVIDENCE_CATALOG'],
            plan_refs=completed_refs,context=context)
    else:raw={};derived=dict(slot_plan=[],plan_ranking=[],plan_allocations=[],derived_shortfall='MODEL_INVALID')
    verified=bool(valid) and not failed
    original_set=[dict(call_id=row['call_id'],input_hash=row['input_hash'],raw_sha256=row['raw_sha256']) for _,_,row in valid]
    result=dict(status='READY' if verified else 'PM_DEGRADED_NO_ALLOCATION',session='PM_PREMARKET',trade_date=day,
        source_call_id=args['call_id'],input_hash=signature,input_projection=dict(original_input_hash=signature),
        model_validated=verified,decision_authority='VALIDATED_MODEL' if verified else 'NO_ALLOCATION',
        model_validation_scope='REVIEWED_BATCHES_ONLY',review_completeness='PARTIAL_TIME_BUDGET' if unreviewed else 'COMPLETE',
        reason_codes=['PM_BATCH_FAILED'] if failed else ['PM_NOT_REVIEWED_TIME_BUDGET'] if unreviewed else [],
        batch_audit=dict(version='pm-batches/1',deadline_at=deadline.isoformat(),parallel=True,capacity=capacity,
            **({'window_authorization':dict(args['window_authorization'])}if args.get('window_authorization')is not None else {}),
            cutoff_observed_at=clock().isoformat(),
            expected_plan_ids=ids,batches=audit,reviewed_plan_ids=[pid for pid in ids if pid in reviewed],
            unreviewed_plan_ids=unreviewed,actual_call_ids=[row['call_id'] for row in audit if row['call_id'] in {r['call_id'] for r in futures.values()}],
            more_than_three_calls=len(futures)>3),
        shortlist_audit=shortlist,server_projection=dict(schema='PM_BATCH_SET_PROJECTION/1',
            source_kind='ACTUAL_MODEL_CALL_SET_NOT_SYNTHETIC_CAPTURE',raw_output_sha256=digest(original_set),
            original_calls=original_set,derived_output_sha256=digest(derived)),
        allocation_plan_bindings={e.plan_id:[e.symbol,args['plan_clusters'][e.plan_id]] for e in args['envelopes']} if verified else {},
        **{key:derived[key] for key in ('slot_plan','plan_ranking','plan_allocations','derived_shortfall')},
        **{key:raw.get(key) for key in ('deployment_target','shortfall_reason','handoff_notes','what_would_change_my_mind')})
    if unreviewed:
        from .team_v2_internal import InternalOutbox
        # Absolute cutoff disposition is immutable for this meeting. Late
        # child captures remain originals, never auto-upgrade the allocation.
        InternalOutbox(store).enqueue(account_id=args['account_id'],trade_date=day,kind='PM_NOT_REVIEWED_TIME_BUDGET',
            source_id=args['meeting_id'],observed_at=clock(),priority=5,body=dict(msg_type='text',content=dict(
                text='晨会存在未审核候选：'+str(len(unreviewed))+'只；PM_NOT_REVIEWED_TIME_BUDGET；'+
                '、'.join(next(e.symbol for e in args['envelopes'] if e.plan_id==pid) for pid in unreviewed)+
                '；不记为WATCH，不分配未审计划，已审部分按原门切换。')))
    return store.record_pm(meeting_id=args['meeting_id'],source_call_id=args['call_id'],account_id=args['account_id'],
        trade_date=day,input_sha256=signature,result=result)
