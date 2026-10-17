"""Bounded parallel model capture, then one deterministic global admission lane.

No shard ever places orders. Each original call has its own durable journal;
only validated captured outputs enter the existing globally locked risk engine.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict,replace
import hashlib,time

from liangjian_paper_broker.ledger import dump
from .team_v2_execution import admit_trader_signal
from .team_v2_trader_inputs import select_model_scope,shard_model_scope
from .team_v2_trader_session import run_trader_session


def run_trader_batch(**args):
    if not args.get('allow_model_call'):raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    projection=args.get('input_projection')
    if projection is None:return run_trader_session(**args)
    monotonic=args.get('monotonic',time.monotonic);deadline=args.get('deadline_monotonic')
    if deadline is None:raise ValueError('TRADER_BATCH_GLOBAL_DEADLINE_REQUIRED')
    selected=select_model_scope(args['all_plans'],active_plan_ids=args['active_plan_ids'],
        held_plan_ids=args['held_plan_ids'],evaluations=projection['evaluations'],atr5m=projection.get('atr5m'))
    shards=shard_model_scope(selected['plan_ids'],args['held_plan_ids'])
    # Bind all global engine facts, not just the model's reduced shard domain.
    source={key:args[key] for key in ('account_id','decision_id','minute_snapshot_id','call_id','run_id','model',
        'trade_date','source_trade_date','decision_at','closed_5m_end','mark_as_of','mode','regime',
        'required_envelope','replacements','position_snapshot','held_plan_ids','input_projection',
        'live_market_ready','snapshot_ready','max_output_tokens','timeout_seconds','tech_reference')}
    source.update(plans={pid:asdict(p) for pid,p in sorted(args['all_plans'].items())},
        envelopes={pid:asdict(e) for pid,e in sorted(args['envelopes'].items())},limits=asdict(args['limits']),
        paper_identity=str(getattr(args['broker'],'ledger',args['broker']).path),scope=selected,shards=shards,
        active_plan_ids=sorted(args['active_plan_ids']),
        pm_meeting_id=args.get('pm_meeting_id'),utilization_cap_pct=args.get('utilization_cap_pct'))
    binding=hashlib.sha256(dump(source).encode()).hexdigest();store=args['store']
    prior=store.load_trader(args['decision_id'],input_sha256=binding)
    if prior is not None:return prior
    store.claim_trader(args['decision_id'],input_sha256=binding)
    # Fixed per-shard timeout is part of the frozen request binding. It never
    # grows on restart. Queued waves still check the same absolute deadline.
    def capture(item):
        index,ids=item;child=dict(args);held=args['held_plan_ids'] if index==0 else {}
        child_decision=args['decision_id']+f':S{index:03d}'
        try:
            timeout=args['journal'].freeze_transport_budget(args['call_id']+f':S{index:03d}',source_sha256=binding,
                timeout_seconds=min(30,args['timeout_seconds'],max(0,deadline-monotonic()-2)))
        except Exception:
            return dict(index=index,plan_ids=ids,status='TRADER_SHARD_SOURCE_UNPROVEN',model_validated=False,
                reason_codes=['TRADER_ORIGINAL_TRANSPORT_BUDGET_UNPROVEN'],admissions=[],source_call_id=args['call_id']+f':S{index:03d}')
        if timeout<=0:
            return dict(index=index,plan_ids=ids,status='TRADER_SHARD_DEADLINE_NO_ACTION',model_validated=False,
                reason_codes=['TRADER_GLOBAL_DEADLINE_EXCEEDED'],admissions=[],source_call_id=args['call_id']+f':S{index:03d}')
        child.update(decision_id=child_decision,call_id=args['call_id']+f':S{index:03d}',
            active_plan_ids=set(ids)&args['active_plan_ids'],held_plan_ids=held,
            position_snapshot=[row for row in args['position_snapshot'] if row['plan_id'] in set(held.values())],
            tech_reference={pid:args['tech_reference'][pid] for pid in ids if pid in args['active_plan_ids']},
            timeout_seconds=timeout,capture_only=True)
        try:result=run_trader_session(**child)
        except Exception:
            result=dict(status='TRADER_SHARD_FAILED_NO_ACTION',model_validated=False,
                reason_codes=['TRADER_SHARD_FAILED_NO_ACTION'],admissions=[],source_call_id=child['call_id'])
        return dict(index=index,plan_ids=ids,**result)
    with ThreadPoolExecutor(max_workers=4,thread_name_prefix='team-a4-capture') as executor:
        captures=list(executor.map(capture,enumerate(shards)))
    allocation=None
    if args.get('pm_meeting_id') and args['active_plan_ids']:
        try:allocation=store.load_pm_allocation(args['pm_meeting_id'],account_id=args['account_id'],trade_date=args['trade_date'])
        except ValueError as exc:
            if str(exc)!='PM_ALLOCATION_ACCOUNT_DATE_MISMATCH':raise
    commands=[]
    for part in captures:
        if not part['model_validated']:continue
        output=part['captured_output']
        commands.extend(('plan',row['plan_id'],row,part) for row in output['signals'])
        commands.extend(('position',args['held_plan_ids'][row['symbol']],row,part) for row in output['position_actions'])
    commands.sort(key=lambda cmd:(cmd[0]!='position',cmd[1]))
    admissions=[];expired=False
    for kind,pid,row,part in commands:
        if monotonic()>=deadline:expired=True;break
        if row['action']=='HOLD':continue
        risk=args['envelopes'][pid]
        if row['action'] in {'BUY_SIGNAL','ADD_SIGNAL'} and kind=='plan':
            hint=row['handoff_to_a6'];risk=replace(risk,max_batches=min(risk.max_batches,hint['batches_hint']))
            if hint['risk_unit_hint']=='PROBE' and risk.risk_unit=='STANDARD':
                risk=replace(risk,risk_unit='PROBE',risk_budget=risk.risk_budget*args['limits'].probe_multiplier)
        sid='A4:'+hashlib.sha256(dump([args['account_id'],args['decision_id'],kind,pid]).encode()).hexdigest()
        admission=admit_trader_signal(broker=args['broker'],signal_id=sid,action=row['action'],
            plan=args['all_plans'][pid],envelope=risk,limits=args['limits'],all_plans=args['all_plans'],now=args['decision_at'],
            closed_5m_end=args['closed_5m_end'],mark_as_of=args['mark_as_of'],regime=args['regime'],
            live_market_ready=args['live_market_ready'],snapshot_ready=args['snapshot_ready'],mode=args['mode'],
            calendar=args['calendar'],input_sha256=part['input_hash'],pm_allocation=allocation,
            utilization_cap_pct=args.get('utilization_cap_pct'),deadline_monotonic=deadline,monotonic=monotonic)
        admissions.append({**admission,'source_call_id':part['source_call_id']})
    valid=all(p['model_validated'] for p in captures) if captures else None
    result=dict(status='NO_MODEL_REQUIRED_OUT_OF_WINDOW' if not captures else 'TRADER_DEADLINE_NO_ACTION' if expired and not admissions else 'TRADER_PARTIAL' if not valid else 'EVALUATED',
        decision_id=args['decision_id'],source_call_id=args['call_id'],minute_snapshot_id=args['minute_snapshot_id'],
        trade_date=args['trade_date'],input_hash=binding,model_validated=valid,reason_codes=sorted({
            code for part in captures for code in part['reason_codes']}|({'TRADER_GLOBAL_DEADLINE_EXCEEDED'} if expired else set())),
        admissions=admissions,scope=selected,shards=captures,model_calls_expected=len(shards),
        handoff_notes={key:[value for part in captures if part['model_validated'] for value in part['handoff_notes'].get(key,[])]
            for key in ('to_a6','to_a3')},
        what_would_change_my_mind=[value for part in captures if part['model_validated'] for value in part['what_would_change_my_mind']])
    return store.record_trader(decision_id=args['decision_id'],source_call_id=args['call_id'],account_id=args['account_id'],
        trade_date=args['trade_date'],input_sha256=binding,result=result)
