"""Closed-minute paper matching and hard exits precede any five-minute model.

Consumes already-frozen typed source inputs; no source calls, legacy DB/store,
customer channel or history repair. The Workflow/CLI collector is a separate
edge and must not claim this isolated coordinator as natural execution.
"""
from dataclasses import asdict,replace
from datetime import datetime,timedelta
import hashlib
import json
import time
from zoneinfo import ZoneInfo

from liangjian_paper_broker.ledger import dump
from .team_v2_morning import _envelopes
from .team_v2_trader_session import run_trader_session
from .team_v2_trader_batch import run_trader_batch
from .trader_guardrails import is_closed_trader_tick

TZ=ZoneInfo('Asia/Shanghai')


def _hard_exit(broker,*,plan,bar,at,account_id):
    signal_id='HARD:'+hashlib.sha256(dump([account_id,plan.plan_id,bar.minute_end]).encode()).hexdigest()
    binding=dict(plan_id=plan.plan_id,symbol=plan.symbol,trade_date=at.date().isoformat(),
        action='SELL_SIGNAL',origin='ENGINE_HARD_RISK',closed_bar=asdict(bar),stop_price=str(plan.stop_price),
        invalidation_price=str(plan.invalidation_price) if plan.invalidation_price is not None else None)
    def decide(snapshot):
        def reject(reason):return dict(judgment=dict(status='REJECTED',reason_codes=[reason],shares=0,risk_unit='NONE'),order=None)
        # Hard exits do not consult a PM slot, morning eligibility or model.
        # They still respect owned quantity, pending sells, T+1 and source flags.
        if not bar.closed or not bar.tradability_known or bar.halted:return reject('HARD_EXIT_TRADABILITY_UNPROVEN')
        quantity=sum(p['sellable_qty'] for p in snapshot['positions'] if p['plan_id']==plan.plan_id and p['symbol']==plan.symbol)
        quantity-=sum(p['remaining_qty'] for p in snapshot['open_orders'] if p['side']=='SELL'
            and p['plan_id']==plan.plan_id and p['symbol']==plan.symbol)
        if quantity<=0:return reject('T1_OR_SELL_ALREADY_RESERVED')
        return dict(judgment=dict(status='APPROVED',reason_codes=['ENGINE_HARD_STOP'],shares=quantity,risk_unit='NONE'),
            order=dict(side='SELL',qty=quantity,order_type='MARKET',limit_price=None,
                no_chase_price=str(plan.no_chase),tradable=True))
    return broker.admit_signal(signal_id,binding=json.loads(dump(binding)),decide=decide,mode='control')


def run_control_minute(*,session_kwargs,closed_bars,deadline_monotonic,monotonic=time.monotonic,envelope_refresh=None,
        model_enabled=True,exception_handler=None):
    """One absolute caller deadline; every late order rechecks it under lock.

    A 10:00 complete bar is consumed at 10:01 under the existing one-minute
    isolation contract. The model runs only when this bar closes a 5m bucket.
    Missing buckets are not fabricated or retrospectively modelled.
    """
    args=dict(session_kwargs);broker=args['broker'];store=args['store'];at=args['clock']()
    if not isinstance(at,datetime) or at.utcoffset() is None:raise ValueError('CONTROL_CLOCK_INVALID')
    current=at.astimezone(TZ);minute=current.replace(second=0,microsecond=0)
    expected=minute-timedelta(minutes=1)
    if (not isinstance(closed_bars,dict) or any(symbol!=bar.symbol or not bar.closed
            or bar.minute_end.astimezone(TZ)!=expected for symbol,bar in closed_bars.items())):
        raise ValueError('CONTROL_CURRENT_CLOSED_BAR_UNPROVEN')
    fills=[];hard=[];errors=[];bad_symbols=set();bad_plans=set()
    for symbol,bar in sorted(closed_bars.items()):
        try:fills.extend(broker.on_bar(bar))
        except Exception:
            bad_symbols.add(symbol)
            errors.append(dict(symbol=symbol,reason_code='CONTROL_PAPER_BAR_FAILED'))
    # Match all already-placed intents first, then derive the next intent from
    # the same closed bar. Never fill a new stop intent using its trigger bar.
    positions=broker.get_positions()
    for position in positions:
        plan=args['all_plans'].get(position['plan_id']);bar=closed_bars.get(position['symbol'])
        if plan is None or plan.symbol!=position['symbol']:
            errors.append(dict(plan_id=position['plan_id'],reason_code='CONTROL_HELD_PLAN_MISSING'));continue
        if bar is None or plan.symbol in bad_symbols:
            errors.append(dict(plan_id=position['plan_id'],reason_code='CONTROL_HELD_MINUTE_MISSING'));continue
        thresholds=[plan.stop_price]+([plan.invalidation_price] if plan.invalidation_price is not None else [])
        if any(bar.low<=threshold for threshold in thresholds):
            try:hard.append(_hard_exit(broker,plan=plan,bar=bar,at=current,account_id=args['account_id']))
            except Exception:
                bad_plans.add(plan.plan_id)
                errors.append(dict(plan_id=plan.plan_id,reason_code='CONTROL_HARD_EXIT_FAILED'))
    result=dict(status='DEGRADED' if errors else 'MINUTE_PROTECTED',minute_end=minute.isoformat(),
        execution_cutoff=expected.isoformat(),fills=fills,hard_exits=hard,errors=errors,trader=None)
    if not model_enabled:return {**result,'trader_skip_reason':'CONTROL_MODEL_CONTEXT_UNAVAILABLE'}
    if not is_closed_trader_tick(expected):return result
    if (not (set(closed_bars)-bad_symbols)
            or any(p['plan_id'] not in args['all_plans'] for p in positions)):
        return {**result,'status':'DATA_LIMITED','trader_skip_reason':'CONTROL_SOURCE_OR_ACCOUNT_INCOMPLETE'}
    remaining=deadline_monotonic-monotonic()
    if remaining<=2:
        return {**result,'status':'DEGRADED','trader_skip_reason':'CONTROL_GLOBAL_BUDGET_INSUFFICIENT'}
    # No current/future 5m context can be relabelled as the isolated prior bucket.
    if args['closed_5m_end']!=expected or args['mark_as_of']!=expected:
        raise ValueError('CONTROL_TRADER_BUCKET_UNPROVEN')
    if envelope_refresh is not None:
        try:args['envelopes']=envelope_refresh()
        except Exception:
            return {**result,'status':'DATA_LIMITED','trader_skip_reason':'CONTROL_FRESH_ACCOUNT_ENVELOPE_UNPROVEN'}
    plans={};envelopes={}
    for pid,plan in args['all_plans'].items():
        bar=closed_bars.get(plan.symbol);envelope=args['envelopes'][pid]
        ready=bool(bar is not None and bar.tradability_known and args['snapshot_ready']
            and plan.symbol not in bad_symbols and pid not in bad_plans)
        plans[pid]=replace(plan,data_ready=plan.data_ready and ready,
            tradable=plan.tradable and ready and not bar.halted if bar is not None else False)
        if not ready or bar.halted or bar.limit_up:
            envelope=replace(envelope,eligible=False,blocked_reasons=tuple(dict.fromkeys(
                (*envelope.blocked_reasons,'CONTROL_TRADABILITY_NOT_READY' if not ready else 'NOT_TRADABLE'))))
        envelopes[pid]=envelope
    args.update(all_plans=plans,envelopes=envelopes)
    ledger=getattr(broker,'ledger',broker)
    source=dict(account_id=args['account_id'],paper_identity=str(ledger.path),
        run_id=args['run_id'],call_id=args['call_id'],model=args['model'],
        trade_date=args['trade_date'],source_trade_date=args['source_trade_date'],
        active_plan_ids=sorted(args['active_plan_ids']),pm_meeting_id=args['pm_meeting_id'],
        limits=asdict(args['limits']),required_envelope=args['required_envelope'],
        replacements=args['replacements'],live_market_ready=args['live_market_ready'],
        snapshot_ready=args['snapshot_ready'],
        minute_snapshot_id=args['minute_snapshot_id'],decision_at=minute,
        bars={key:asdict(value) for key,value in sorted(closed_bars.items())},
        plans={pid:asdict(plan) for pid,plan in sorted(args['all_plans'].items())},
        tech_reference=args['tech_reference'],regime=args['regime'],mode=args['mode'])
    if args.get('input_projection') is not None:source['input_projection']=args['input_projection']
    source_sha=hashlib.sha256(dump(source).encode()).hexdigest()
    frozen=store.freeze_control_context(args['decision_id'],source_sha256=source_sha,body=json.loads(dump(dict(
        positions=broker.get_positions(),envelopes={pid:asdict(e) for pid,e in args['envelopes'].items()},
        utilization_cap_pct=args.get('utilization_cap_pct'),
        timeout_seconds=min(args['timeout_seconds'],30,remaining-2)))))
    positions=frozen['positions'];held={p['symbol']:p['plan_id'] for p in positions}
    if len(held)!=len(positions):raise ValueError('CONTROL_HELD_SYMBOL_AMBIGUOUS')
    args.update(position_snapshot=positions,held_plan_ids=held,decision_at=minute,
        envelopes={pid:_envelopes([row])[0] for pid,row in frozen['envelopes'].items()},
        utilization_cap_pct=frozen['utilization_cap_pct'],
        timeout_seconds=frozen['timeout_seconds'],deadline_monotonic=deadline_monotonic,monotonic=monotonic)
    if args.get('input_projection') is not None and args['required_envelope'].get('prompt_version')=='agent-4-trader/1.1.0':
        from .team_v2_candidate_review import run_candidate_review
        result['trader']=run_candidate_review(**args)
    else:
        result['trader']=run_trader_batch(**args) if args.get('input_projection') is not None else run_trader_session(**args)
    if exception_handler is not None and args['mode']=='control':
        risk_started=monotonic()
        try:
            result['risk_exception']=exception_handler(trader_result=result['trader'],envelopes=args['envelopes'],
                all_plans=args['all_plans'],closed_bars=closed_bars,deadline_monotonic=deadline_monotonic,monotonic=monotonic)
            if result['risk_exception']['status'] in ('DEGRADED','HALT','DATA_LIMITED'):result['status']='DEGRADED'
        except Exception:
            from .team_v2_exception_coordinator import record_coordinator_failure
            try:failed=record_coordinator_failure(args)
            except Exception:failed=dict(status='DEGRADED',failure_persisted=False,internal_alert_status='PENDING')
            result.update(status='DEGRADED',risk_exception=dict(failed,reason_code='A6_EXCEPTION_COORDINATOR_FAILED'))
        result['phase_timings']={'risk_exception_ms':max(0,(monotonic()-risk_started)*1000)}
    return result
