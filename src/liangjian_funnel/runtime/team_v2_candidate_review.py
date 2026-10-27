"""0308 engine-led candidate review, never broad discretionary trading.

At most two current deterministic candidates share one model request. Entries
fail closed; failed exit reviews keep the original engine exit action. Global
guard/risk/PM/owned-paper admission remains the only order authority.
"""
from dataclasses import asdict,replace
import hashlib
import json
import time

from liangjian_paper_broker.ledger import dump
from ..pipeline.model_client import ModelClientError
from .team_v2_execution import admit_trader_signal
from .team_v2_trader_session import run_trader_session


def engine_review_candidates(args):
    projection=args['input_projection'];held=set(args['held_plan_ids'].values())
    entries={};exits={}
    for pid,plan in args['all_plans'].items():
        action=projection['evaluations'].get(pid,{}).get('action')
        if pid in held and action in {'SELL_SIGNAL','REDUCE_SIGNAL'}:
            exits[pid]=action
        elif (pid in args['active_plan_ids'] and action in {'BUY_SIGNAL','ADD_SIGNAL'}
                and args['envelopes'][pid].eligible and plan.data_ready and plan.tradable
                and plan.status=='ACTIVE_TODAY' and plan.valid_from is not None
                and plan.valid_from<=args['decision_at']<plan.expires_at
                and plan.stop_price<plan.price and (plan.invalidation_price is None or plan.price>plan.invalidation_price)
                and plan.entry_low<=plan.price<=plan.entry_high and plan.price<=plan.no_chase):
            entries[pid]=action
    ranks={row['plan_id']:json.loads(row['payload_json']).get('plan_priority_rank') for row in projection['plan_rows']}
    def rank(pid):
        value=ranks.get(pid)
        return (pid not in exits,value if type(value) is int and value>0 else 10**9,pid)
    ordered=sorted(set(entries)|set(exits),key=rank)
    return entries,exits,ordered[:2],ordered[2:]


def run_candidate_review(**args):
    monotonic=args.get('monotonic',time.monotonic);deadline=args.get('deadline_monotonic')
    if deadline is None or not args.get('allow_model_call'):raise ValueError('TRADER_REVIEW_AUTHORITY_OR_DEADLINE_REQUIRED')
    entries,exits,selected,pending=engine_review_candidates(args)
    source={key:args[key] for key in ('account_id','decision_id','call_id','run_id','model','trade_date',
        'source_trade_date','decision_at','minute_snapshot_id','required_envelope','replacements',
        'input_projection','tech_reference','regime','mode','position_snapshot','held_plan_ids',
        'live_market_ready','snapshot_ready','timeout_seconds','max_output_tokens')}
    source.update(all_plans={pid:asdict(p) for pid,p in args['all_plans'].items()},
        envelopes={pid:asdict(e) for pid,e in args['envelopes'].items()},limits=asdict(args['limits']),
        paper_identity=str(getattr(args['broker'],'ledger',args['broker']).path),
        pm_meeting_id=args.get('pm_meeting_id'),utilization_cap_pct=args.get('utilization_cap_pct'),
        active=sorted(args['active_plan_ids']),entries=entries,exits=exits,selected=selected,pending=pending)
    binding=hashlib.sha256(dump(source).encode()).hexdigest();store=args['store']
    previous=store.load_trader(args['decision_id'],input_sha256=binding)
    if previous is not None:return previous
    store.claim_trader(args['decision_id'],input_sha256=binding)
    from .team_v2_review_latch import ReviewDayLatch
    latch=ReviewDayLatch(store);day_state=latch.read(args['account_id'],args['trade_date'])
    # A crash after the third failed capture must not reclassify that same
    # decision as engine-only and admit its originally blocked entry on retry.
    frozen_day=store.freeze_control_context('candidate-review-day:'+args['decision_id'],
        source_sha256=binding,body={'engine_only':day_state['engine_only']})
    engine_only=frozen_day['engine_only']
    latch.ensure_alert(args['account_id'],args['trade_date'],day_state)
    truncations=[]
    if pending and not engine_only:
        from .team_v2_internal import InternalOutbox
        outbox=InternalOutbox(store)
        for pid in pending:
            source_id='A4_REVIEW_TRUNCATED:'+hashlib.sha256(dump([args['decision_id'],pid]).encode()).hexdigest()
            event=outbox.enqueue(account_id=args['account_id'],trade_date=args['trade_date'],
                kind='A4_REVIEW_TRUNCATED',source_id=source_id,observed_at=args['decision_at'],priority=10,
                body=dict(msg_type='text',content=dict(text='TRUNCATED：本节拍模型容量不足，'+
                    args['all_plans'][pid].symbol+' 未送模型；容量2，候选'+str(len(entries)+len(exits))+
                    '；本次不以未审核冒无信号，持仓退出仍按引擎保护。')))
            truncations.append(dict(plan_id=pid,symbol=args['all_plans'][pid].symbol,
                status='TRUNCATED',reason_code='A4_REVIEW_CAPACITY_TRUNCATED',notification_event_id=event))
    selected_entries={pid:entries[pid] for pid in selected if pid in entries}
    selected_exits={pid:exits[pid] for pid in selected if pid in exits}
    child=dict(args);call_id=args['call_id']+':REVIEW';capture=None;sent=False
    detail_id='candidate-review-failure:'+call_id
    class ObservedClient:
        def complete(self,*positional,**keyword):
            try:return args['client'].complete(*positional,**keyword)
            except ModelClientError as exc:
                # Stable diagnostic classification only. Never store exception
                # text, request headers, endpoint tokens or credentials.
                code=('WALL_CLOCK_TIMEOUT' if exc.reason_code in {
                    'WALL_CLOCK_TIMEOUT','MODEL_WALL_CLOCK_TIMEOUT','MODEL_TOTAL_DEADLINE_EXCEEDED'}
                    else 'UNPARSEABLE_RESPONSE' if exc.reason_code in {'STRICT_JSON_INVALID','JSON_OBJECT_REQUIRED'}
                    or exc.reason_code.startswith('STREAM_') else 'OTHER_TRANSPORT_FAILURE')
                store.freeze_control_context(detail_id,source_sha256=binding,body={'reason_code':code})
                raise
    if selected and not engine_only:
        child.update(decision_id=args['decision_id']+':REVIEW',call_id=call_id,capture_only=True,
            active_plan_ids=set(selected_entries),
            held_plan_ids={s:pid for s,pid in args['held_plan_ids'].items() if pid in selected_exits},
            position_snapshot=[p for p in args['position_snapshot'] if p['plan_id'] in selected_exits],
            review_entry_positions=[p for p in args['position_snapshot']
                if selected_entries.get(p['plan_id'])=='ADD_SIGNAL'],
            tech_reference={pid:args['tech_reference'][pid] for pid in selected_entries},
            review_candidate_actions=selected_entries,
            review_position_actions={args['all_plans'][pid].symbol:action for pid,action in selected_exits.items()},client=ObservedClient())
        try:
            child['timeout_seconds']=args['journal'].freeze_transport_budget(call_id,source_sha256=binding,
                timeout_seconds=min(30,args['timeout_seconds'],max(0,deadline-monotonic()-2)))
            capture=run_trader_session(**child)
        except Exception:
            capture=dict(model_validated=False,reason_codes=['TRADER_CANDIDATE_REVIEW_FAILED'],
                source_call_id=call_id,admissions=[])
        sent=args['journal'].has_call_claim(call_id)
        if sent:
            day_state=latch.record(account_id=args['account_id'],trade_date=args['trade_date'],
                decision_id=args['decision_id'],valid=bool(capture['model_validated']),observed_at=args['decision_at'],
                counts_as_failure=not capture['model_validated'] and not capture.get('explicit_original_decision',False))
            latch.ensure_alert(args['account_id'],args['trade_date'],day_state)
            if capture['model_validated']:
                latch.ensure_recovery_alert(args['account_id'],args['trade_date'],args['decision_at'])
    commands=[];vetoed=0;confirmed=0;fallback=0
    if engine_only:
        commands.extend((pid,({**entries,**exits})[pid],None,'ENGINE_ONLY_DAILY_REVIEW_FALLBACK')
            for pid in (*selected,*pending))
        fallback=len(exits)
    elif capture and capture['model_validated']:
        raw=capture['captured_output']
        for row in raw['signals']:
            if row['action']=='NO_ACTION':vetoed+=1
            else:confirmed+=1;commands.append((row['plan_id'],row['action'],row,'MODEL_CONFIRMED'))
        for row in raw['position_actions']:
            if row['action']=='HOLD':vetoed+=1
            else:
                confirmed+=1
                commands.append((args['held_plan_ids'][row['symbol']],row['action'],row,'MODEL_CONFIRMED'))
    else:
        # This is a separately labelled engine action, not a repaired LLM vote.
        commands.extend((pid,action,None,'ENGINE_EXIT_FALLBACK') for pid,action in selected_exits.items())
        fallback+=len(selected_exits)
    # Capacity does not suspend an existing engine exit. Unreviewed entries wait.
    if not engine_only:
        commands.extend((pid,exits[pid],None,'ENGINE_EXIT_NOT_REVIEWED') for pid in pending if pid in exits)
        fallback+=sum(pid in exits for pid in pending)
    allocation=None
    if args.get('pm_meeting_id') and entries:
        try:allocation=store.load_pm_allocation(args['pm_meeting_id'],account_id=args['account_id'],trade_date=args['trade_date'])
        except ValueError as exc:
            if str(exc)!='PM_ALLOCATION_ACCOUNT_DATE_MISMATCH':raise
    admissions=[]
    for pid,action,row,origin in commands:
        risk=args['envelopes'][pid]
        if action in {'BUY_SIGNAL','ADD_SIGNAL'} and row is not None:
            hint=row['handoff_to_a6'];risk=replace(risk,max_batches=min(risk.max_batches,hint['batches_hint']))
            if hint['risk_unit_hint']=='PROBE' and risk.risk_unit=='STANDARD':
                risk=replace(risk,risk_unit='PROBE',risk_budget=risk.risk_budget*args['limits'].probe_multiplier)
        sid='A4:'+hashlib.sha256(dump([args['account_id'],args['decision_id'],'candidate',pid]).encode()).hexdigest()
        pattern_binding=(args.get('input_projection',{}).get('evaluations',{}).get(pid,{}).get('entry_pattern_binding')
            if action in {'BUY_SIGNAL','ADD_SIGNAL'} else None)
        extra_policy={}
        if engine_only and action in {'BUY_SIGNAL','ADD_SIGNAL'}:
            from .team_v2_engine_only_policy import engine_only_geometry
            extra_policy['engine_only_policy']=engine_only_geometry(args['all_plans'][pid],
                entry_pattern_binding=pattern_binding,plan_rows=args.get('input_projection',{}).get('plan_rows',[]))
        outcome=admit_trader_signal(broker=args['broker'],signal_id=sid,action=action,plan=args['all_plans'][pid],
            envelope=risk,limits=args['limits'],all_plans=args['all_plans'],now=args['decision_at'],
            closed_5m_end=args['closed_5m_end'],mark_as_of=args['mark_as_of'],regime=args['regime'],
            live_market_ready=args['live_market_ready'],snapshot_ready=args['snapshot_ready'],mode=args['mode'],
            calendar=args['calendar'],input_sha256=capture.get('input_hash',binding) if capture else binding,
            pm_allocation=allocation,utilization_cap_pct=args.get('utilization_cap_pct'),
            entry_pattern_binding=pattern_binding,
            deadline_monotonic=deadline,monotonic=monotonic,**extra_policy)
        if engine_only:
            from .team_v2_internal import InternalOutbox
            judgment=outcome.get('judgment') or {}
            policy=extra_policy.get('engine_only_policy') or {}
            plan=args['all_plans'][pid]
            alert=InternalOutbox(store).enqueue(account_id=args['account_id'],trade_date=args['trade_date'],
                kind='A4_ENGINE_ONLY_ADMISSION',source_id=sid,observed_at=args['decision_at'],priority=5,
                body=dict(msg_type='text',content=dict(text='降级模式·仅引擎：'+plan.symbol+' '+action+
                    '；绑定价 '+str(plan.price)+'；更严区间 '+str(policy.get('entry_low','不可用'))+'–'+str(policy.get('entry_high','不可用'))+
                    '；严格禁追价 '+str(policy.get('no_chase',plan.no_chase))+'；止损 '+str(plan.stop_price)+
                    '；正常总仓位上限50%，已有持仓及挂单占用计入，仅向下整手取整；结果 '+str(judgment.get('status','不可用'))+
                    '；数量 '+str(judgment.get('shares',0))+'；原因 '+','.join(judgment.get('reason_codes',[]))+
                    '；准入ID '+sid)))
            outcome={**outcome,'engine_only_notification_event_id':alert}
        admissions.append({**outcome,'origin':origin,'source_call_id':call_id if sent else None})
    issues=capture.get('reason_codes',[]) if capture else []
    failure=store.read_control_context(detail_id,source_sha256=binding) if selected else None
    timed_out=bool(failure and failure['reason_code']=='WALL_CLOCK_TIMEOUT' or 'TRADER_GLOBAL_DEADLINE_EXCEEDED' in issues)
    unknown_failure=bool(capture and not capture['model_validated'] and 'TEAM_MODEL_CALL_FAILED' in issues and failure is None)
    result=dict(status='ENGINE_ONLY_DAILY_REVIEW_FALLBACK' if engine_only else 'ENGINE_TRACKING_NO_REVIEW_CANDIDATE' if not selected else
        'EVALUATED' if capture['model_validated'] else 'CANDIDATE_REVIEW_FAILED_ENTRIES_BLOCKED',
        decision_id=args['decision_id'],source_call_id=args['call_id'],trade_date=args['trade_date'],
        minute_snapshot_id=args['minute_snapshot_id'],input_hash=binding,
        model_validated=capture['model_validated'] if capture else None,admissions=admissions,reason_codes=issues,
        scope=dict(decisions=[dict(plan_id=pid,reason_code='ENGINE_REVIEW_CANDIDATE',
            selected_for_model=pid in selected and not engine_only,model_judgment_denominator=sent and pid in selected)
            for pid in (*entries,*exits)]),
        shards=[dict(capture,plan_ids=selected)] if capture else [],
        review_day_state=day_state,
        candidate_review=dict(mode='ENGINE_ONLY_DAILY_REVIEW_FALLBACK' if engine_only else 'ENGINE_LED_SINGLE_REQUEST',candidate_count=len(entries)+len(exits),
            engine_candidates=[dict(plan_id=pid,action=action) for pid,action in {**entries,**exits}.items()],
            selected_count=0 if engine_only else len(selected),unreviewed_candidate_ids=[] if engine_only else pending,actual_model_objects=len(selected) if sent else 0,
            truncated=truncations,
            confirmed_count=confirmed,veto_count=vetoed,failed_review_count=len(selected) if capture and not capture['model_validated'] else 0,
            timeout_count=len(selected) if timed_out and sent else None if unknown_failure else 0,
            transport_failure_classification=failure['reason_code'] if failure else 'UNKNOWN' if unknown_failure else 'NOT_TIMEOUT',
            engine_exit_fallback_count=fallback),
        handoff_notes=capture.get('handoff_notes',{}) if capture else {},
        what_would_change_my_mind=capture.get('what_would_change_my_mind',[]) if capture else [])
    return store.record_trader(decision_id=args['decision_id'],source_call_id=args['call_id'],account_id=args['account_id'],
        trade_date=args['trade_date'],input_sha256=binding,result=result)
