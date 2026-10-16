"""Frozen A4 vote -> fresh account-owned risk -> atomic local-paper admission.

Only owned paper state is written. No model, notification or legacy runtime DB.
Queued buys reserve stock/cluster/cash/slot capacity before their first fill.
"""
from dataclasses import asdict,replace
from datetime import datetime, timedelta
from decimal import Decimal as D
import json
import re
import time
from zoneinfo import ZoneInfo

from liangjian_paper_broker.ledger import dump
from .risk_engine import AccountRisk, PositionRisk
from .trader_guardrails import guard_trader_signal
from .team_v2_allocation import enforce_pm_allocation

TZ=ZoneInfo('Asia/Shanghai')
ENTRY={'BUY_SIGNAL','ADD_SIGNAL'}


def _views(snapshot,*,plan,all_plans,mark_as_of,entry):
    orders=snapshot['open_orders']
    positions=[]
    for row in snapshot['positions']:
        if not entry and (row['plan_id']!=plan.plan_id or row['symbol']!=plan.symbol):continue
        owner=all_plans.get(row['plan_id'])
        if owner is None or owner.symbol!=row['symbol']:raise ValueError('PAPER_POSITION_SCOPE_UNPROVEN')
        if entry and (row['market_value'] is None or row['mark_minute_end']!=mark_as_of.isoformat()):
            raise ValueError('PAPER_POSITION_MARK_STALE')
        reserved=sum(order['remaining_qty'] for order in orders if order['side']=='SELL'
            and order['plan_id']==row['plan_id'] and order['symbol']==row['symbol'])
        value=D(row['market_value']) if entry else plan.price*row['qty']
        positions.append(PositionRisk(row['plan_id'],row['symbol'],owner.cluster,row['qty'],
            max(0,row['sellable_qty']-reserved),value,D(row['avg_cost'])))
    if not entry:return None,tuple(positions)
    basis=snapshot['risk_day']
    if basis is None or snapshot['account']['nav'] is None:raise ValueError('PAPER_RISK_BASIS_MISSING')
    if basis['halt_reason_codes']:raise ValueError(basis['halt_reason_codes'][0])
    if snapshot['global_halt']:raise ValueError('DRAWDOWN_HALT')
    for order in orders:
        if order['side']!='BUY':continue
        owner=all_plans.get(order['plan_id'])
        if owner is None or owner.symbol!=order['symbol']:raise ValueError('PAPER_ORDER_SCOPE_UNPROVEN')
        price=min(D(order['limit_price']),D(order['no_chase_price'])) if order['order_type']=='LIMIT' else D(order['no_chase_price'])
        positions.append(PositionRisk(owner.plan_id,owner.symbol,owner.cluster,order['remaining_qty'],0,
            price*order['remaining_qty'],price))
    action_by_order={row['order_id']:row['binding'].get('action') for row in snapshot['admissions'] if row['dispatched']}
    new_plans={row['plan_id'] for row in orders if row['side']=='BUY' and action_by_order.get(row['order_id'])!='ADD_SIGNAL'}
    new_plans.update(row['plan_id'] for row in snapshot['fills_today'] if row['side']=='BUY' and action_by_order.get(row['order_id'])!='ADD_SIGNAL')
    account=AccountRisk(D(snapshot['account']['nav']),D(snapshot['account']['cash']),D(basis['day_open_nav']),
        max(D(snapshot['peak_nav']),D(snapshot['account']['nav'])),D(snapshot['account']['frozen']),len(new_plans))
    return account,tuple(positions)


def admit_trader_signal(*,broker,signal_id,action,plan,envelope,limits,all_plans,now,closed_5m_end,
                        mark_as_of,regime,live_market_ready,snapshot_ready,mode,calendar,input_sha256,pm_allocation=None,
                        deadline_monotonic=None,monotonic=time.monotonic,utilization_cap_pct=None):
    if (not isinstance(now,datetime) or now.utcoffset() is None or not isinstance(mark_as_of,datetime)
            or mark_as_of.utcoffset() is None or not re.fullmatch(r'[a-f0-9]{64}',input_sha256)
            or plan.plan_id not in all_plans or all_plans[plan.plan_id]!=plan):
        raise ValueError('TRADER_EXECUTION_CONTEXT_INVALID')
    if utilization_cap_pct is not None:
        from ..pipeline.team_v2_risk_schema import number
        utilization_cap_pct=number(utilization_cap_pct)
        if utilization_cap_pct is None or not 0<=utilization_cap_pct<=100:raise ValueError('TRADER_UTILIZATION_CAP_INVALID')
    current=now.astimezone(TZ)
    binding=json.loads(dump(dict(plan_id=plan.plan_id,symbol=plan.symbol,trade_date=current.date().isoformat(),
        cluster=plan.cluster,action=action,plan=asdict(plan),envelope=asdict(envelope),limits=asdict(limits),
        decision_at=current,closed_5m_end=closed_5m_end,mark_as_of=mark_as_of,input_sha256=input_sha256,
        regime=regime,live_market_ready=live_market_ready,snapshot_ready=snapshot_ready,deadline_guard=deadline_monotonic is not None,
        pm_allocation=asdict(pm_allocation) if pm_allocation is not None else None,
        utilization_cap_pct=utilization_cap_pct,
        plan_clusters={pid:[item.symbol,item.cluster] for pid,item in sorted(all_plans.items())})))
    def decide(snapshot):
        def block(code):return dict(judgment=dict(status='REJECTED',shares=0,risk_unit='NONE',reason_codes=[code]),order=None)
        if deadline_monotonic is not None and monotonic()>=deadline_monotonic:
            return block('TRADER_GLOBAL_DEADLINE_EXCEEDED')
        if action not in ENTRY|{'SELL_SIGNAL','REDUCE_SIGNAL','CANCEL_SIGNAL'}:
            return dict(judgment=dict(status='NO_ORDER_ACTION',shares=0,risk_unit='NONE',reason_codes=[]),order=None)
        # No model completion from a past tick may create a current order.
        observed=datetime.fromisoformat(snapshot['observed_at']).astimezone(TZ)
        if observed.date()!=current.date() or observed.replace(second=0,microsecond=0)!=current.replace(second=0,microsecond=0):
            return block('TRADER_EXECUTION_CLOCK_MOVED')
        if action=='CANCEL_SIGNAL':
            # Exact order IDs come only from the current owned paper snapshot.
            # Cancel entry reservations, never an independent protective exit.
            ids=[row['order_id'] for row in snapshot['open_orders'] if row['plan_id']==plan.plan_id
                and row['symbol']==plan.symbol and row['side']=='BUY'
                and datetime.fromisoformat(row['submitted_at']).astimezone(TZ).date()==current.date()]
            return dict(judgment=dict(status='NO_ORDER_ACTION',shares=0,risk_unit='NONE',reason_codes=[]),
                order=None,cancel_order_ids=ids)
        minute=current.replace(second=0,microsecond=0)
        if not minute-timedelta(minutes=1)<=mark_as_of.astimezone(TZ)<=minute:
            return block('TRADER_MARK_TIME_UNPROVEN')
        try:
            account,positions=_views(snapshot,plan=plan,all_plans=all_plans,mark_as_of=mark_as_of,entry=action in ENTRY)
        except ValueError as exc:
            if str(exc) not in {'PAPER_POSITION_SCOPE_UNPROVEN','PAPER_POSITION_MARK_STALE','PAPER_RISK_BASIS_MISSING',
                    'PAPER_ORDER_SCOPE_UNPROVEN','DAILY_LOSS_HALT','DRAWDOWN_HALT','A6_DEGRADED_HALT'}:raise
            return block(str(exc))
        permitted=envelope;pm_reason=None
        if action in ENTRY:
            permitted,pm_reason=enforce_pm_allocation(pm_allocation,plan=plan,envelope=envelope,
                trade_date=current.date().isoformat(),snapshot=snapshot)
            if utilization_cap_pct is not None:
                # Inside the same reservation transaction: open buys already
                # count as exposure, so two plans cannot spend the same room.
                room=account.nav*utilization_cap_pct/D(100)-sum((p.market_value for p in positions),D(0))
                shares=max(0,int(room/plan.price/D(100))*100) if plan.price>0 else 0
                permitted=replace(permitted,max_shares_cap=min(permitted.max_shares_cap,shares),a6_tightened=True)
                if shares<=0:permitted=replace(permitted,eligible=False,risk_unit='NONE',
                    blocked_reasons=tuple(dict.fromkeys((*permitted.blocked_reasons,'A6_UTILIZATION_CAP'))))
        cluster_count=sum(row['dispatched'] and row['binding'].get('action') in ENTRY
            and row['binding'].get('cluster')==plan.cluster for row in snapshot['admissions'])
        active_orders={row['order_id'] for row in snapshot['open_orders']}
        filled_orders={row['order_id'] for row in snapshot['fills_today']}
        batches=sum(row['dispatched'] and row['binding']['plan_id']==plan.plan_id
            and row['binding'].get('action') in ENTRY and row['order_id'] in active_orders|filled_orders
            for row in snapshot['admissions'])
        guarded=guard_trader_signal(signal_id=signal_id,action=action,signal_plan_id=plan.plan_id,
            signal_symbol=plan.symbol,plan=plan,envelope=permitted,account=account,positions=positions,
            limits=limits,now=observed,closed_5m_end=closed_5m_end,regime=regime,
            live_market_ready=live_market_ready,snapshot_ready=snapshot_ready,schema_valid=True,mode=mode,
            cluster_signals_today=cluster_count,batches_executed=batches,calendar=calendar)
        risk=guarded.risk_decision
        # Hard loss/drawdown latches retain priority even if PM is unavailable.
        if pm_reason and not set(guarded.reason_codes)&{'DRAWDOWN_HALT','DAILY_LOSS_HALT','A6_DEGRADED_HALT'}:
            return block(pm_reason)
        result=dict(judgment=asdict(risk) if risk is not None else dict(status='REJECTED',shares=0,
            risk_unit='NONE',reason_codes=list(guarded.reason_codes)),order=None)
        if risk is not None and risk.status!='REJECTED' and guarded.dispatch_allowed:
            result['order']=dict(side='BUY' if action in ENTRY else 'SELL',qty=risk.shares,
                order_type='LIMIT',limit_price=str(plan.price),no_chase_price=str(plan.no_chase),tradable=plan.tradable)
        if action in ENTRY:
            for code in ('DRAWDOWN_HALT','DAILY_LOSS_HALT','A6_DEGRADED_HALT'):
                if code in guarded.reason_codes:result['halt_reason']=code;break
        if result['order'] is not None and deadline_monotonic is not None and monotonic()>=deadline_monotonic:
            return block('TRADER_GLOBAL_DEADLINE_EXCEEDED')
        return result
    return broker.admit_signal(signal_id,binding=binding,decide=decide,mode=mode)
