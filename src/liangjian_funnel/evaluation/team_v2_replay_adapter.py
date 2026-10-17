"""Typed sealed-source bridge to the continuous owned-account coordinator.

No meeting, allocation, model client or historical market authority is invented.
The optional session factory supplies the real journal/meeting execution edge;
without it matching and hard exits still run, but no model can be called.
"""
from dataclasses import replace
from decimal import Decimal

from ..review.observability import _cluster
from ..runtime.risk_engine import PlanRisk
from ..runtime.team_v2_sources import stamp


class FrozenReplayAdapter:
    def __init__(self, materializer, *, broker, store, clock, account_id, session_factory=None):
        self.materializer=materializer
        self.broker=broker;self.store=store;self.clock=clock;self.account_id=account_id
        self.session_factory=session_factory
        self.known_plans={}

    def __call__(self, batch, broker, store):
        at=stamp(batch['decision_at'])
        if (broker is not self.broker or store is not self.store
                or stamp(self.clock())!=at or stamp(broker.clock())!=at):
            raise ValueError('REPLAY_ADAPTER_ACCOUNT_OR_CLOCK_REBIND')
        source=self.materializer.materialize(batch)
        if (source['decision_at']!=batch['decision_at'] or source['trade_date']!=batch['trade_date']
                or source['quote_basis']!='REPLAY_BAR_DERIVED'
                or source['historical_live_state'].get('status')!='DATA_BLOCKED'):
            raise ValueError('REPLAY_ADAPTER_SOURCE_BINDING_INVALID')
        plans={};limited=list(source['limited'])
        for pid,view in source['projected'].items():
            row=view['plan_row'];raw=view['original_plan'];cluster,_=_cluster(row)
            try:
                if row['plan_id']!=pid or cluster=='UNKNOWN':
                    raise ValueError('REPLAY_CLUSTER_OR_PLAN_UNPROVEN')
                entry=pid in source['entry_plan_ids']
                plan=PlanRisk(pid,row['symbol'],cluster,'ACTIVE_TODAY' if entry else row['status'],
                    stamp(row['valid_from']) if row.get('valid_from') else None,stamp(row['expires_at']),
                    view['quote'].last,Decimal(str(raw['stop_level'])),Decimal(str(raw['trigger_low'])),
                    Decimal(str(raw['trigger_high'])),Decimal(str(raw['no_chase'])),
                    invalidation_price=Decimal(str(raw['stop_level'])),
                    data_ready=True,tradable=not view['tradability']['halted'])
                if not 0<plan.stop_price<plan.entry_low<=plan.entry_high<=plan.no_chase:
                    raise ValueError('REPLAY_GEOMETRY_UNPROVEN')
            except (ValueError,KeyError,TypeError,ArithmeticError):
                limited.append(dict(plan_id=pid,reason_codes=['REPLAY_TYPED_PLAN_UNPROVEN']))
                continue
            plans[pid]=plan;self.known_plans[pid]=plan
        held=broker.get_positions();missing_held=[]
        for position in held:
            pid=position['plan_id']
            if pid in plans and plans[pid].symbol==position['symbol']:continue
            old=self.known_plans.get(pid)
            if old is None or old.symbol!=position['symbol']:
                missing_held.append(pid)
            else:
                # Keep the owner/geometry, never borrow a previous minute mark.
                plans[pid]=replace(old,data_ready=False,tradable=False)
                limited.append(dict(plan_id=pid,reason_codes=['REPLAY_HELD_CURRENT_SOURCE_MISSING']))
        active=set(source['entry_plan_ids']) & set(plans)
        fixed=dict(broker=broker,store=store,clock=self.clock,account_id=self.account_id,all_plans=plans)
        context=dict(source=source,all_plans=plans,active_plan_ids=active,limited=limited,
            missing_held_plan_ids=missing_held,decision_at=at,broker=broker,store=store)
        invocation=dict(session_kwargs=fixed,closed_bars=source['closed_bars'],model_enabled=False,
            source_audit=dict(source_digest=source['source_digest'],limited=limited,
                absent_original_plan_ids=source['absent_original_plan_ids'],missing_held_plan_ids=missing_held,
                quote_basis='REPLAY_BAR_DERIVED',model_edge='UNAVAILABLE_NO_CALL',
                authority='SOURCE_TO_COORDINATOR_COMPONENT_NOT_CONTROL_RELEASE_GATE'))
        if self.session_factory is None or missing_held:return invocation
        edge=self.session_factory(context)
        args=dict(edge['session_kwargs'])
        # The factory may choose not to run, but cannot replace source/account
        # facts or upgrade bar-derived quotes into natural live authority.
        for key in fixed:
            if key in args:raise ValueError('REPLAY_ADAPTER_RESERVED_ARGUMENT')
        if (set(args.get('active_plan_ids',()))!=active or args.get('regime')!='DATA_BLOCK'
                or args.get('live_market_ready') is not False
                or args.get('trade_date')!=batch['trade_date']):
            raise ValueError('REPLAY_ADAPTER_ENTRY_OR_MARKET_AUTHORITY_INVALID')
        envelopes=args.get('envelopes',{})
        if set(envelopes)!=set(plans) or any(
                e.plan_id!=pid or e.symbol!=plans[pid].symbol or e.trade_date!=batch['trade_date']
                for pid,e in envelopes.items()):
            raise ValueError('REPLAY_ADAPTER_ENVELOPE_BINDING_INVALID')
        if type(edge['model_enabled']) is not bool:
            raise ValueError('REPLAY_ADAPTER_MODEL_PERMISSION_INVALID')
        args.update(fixed)
        invocation.update(session_kwargs=args,model_enabled=edge['model_enabled'])
        for key in ('envelope_refresh','exception_handler'):
            if key in edge:invocation[key]=edge[key]
        invocation['source_audit']['model_edge']='EXPLICIT_FACTORY_NOT_HISTORICAL_MEETING_PROOF'
        return invocation
