"""One continuous owned account across the sealed six-day decision timeline.

Adapters must supply original source-bound minute inputs and actual or explicit
failed meeting results. This driver never fabricates a PM allocation, source,
model response, order or close mark. Repeatability is a separate second pass.
"""
from collections import Counter
from datetime import timedelta
import time

from .team_v2_replay_source import SIX_DAYS,session_minutes
from ..runtime.team_v2_sources import stamp
from ..runtime.team_v2_control_minute import run_control_minute
from ..runtime.trader_guardrails import is_closed_trader_tick


def execute_timeline(batches,*,broker,store,set_clock,prepare_day,prepare_minute,consume_close,
                     account_id,monotonic=time.monotonic):
    """Callbacks receive the SAME broker; none may start another account.

    prepare_minute(batch, broker, store) returns session_kwargs, closed_bars,
    model_enabled and optional envelope_refresh/exception_handler. Only the
    existing coordinator can perform the model -> guard -> paper path.
    """
    batches=tuple(batches)
    expected=tuple(at.isoformat() for day in SIX_DAYS for at in session_minutes(day))
    if tuple(row['decision_at'] for row in batches)!=expected:
        raise ValueError('REPLAY_COMPLETE_CONTIGUOUS_TIMELINE_REQUIRED')
    for row in batches:
        at=stamp(row['decision_at'])
        if (row['trade_date']!=at.date().isoformat() or row['quote_basis']!='REPLAY_BAR_DERIVED'
                or type(row['model_tick']) is not bool
                or row['model_tick']!=is_closed_trader_tick(at-timedelta(minutes=1))):
            raise ValueError('REPLAY_TIMELINE_SOURCE_BINDING_INVALID')
    paper_identity=str(broker.ledger.path);days=[];cursor=0
    for day in SIX_DAYS:
        base=stamp(day+'T00:00:00+08:00');set_clock(base.replace(hour=8,minute=33))
        broker.open_risk_day(day)
        morning=prepare_day(day,broker,store)
        counts=Counter();reasons=Counter();admission_statuses=Counter()
        for row in batches[cursor:cursor+240]:
            at=stamp(row['decision_at']);set_clock(at)
            if str(broker.ledger.path)!=paper_identity:raise ValueError('REPLAY_PAPER_IDENTITY_CHANGED')
            invocation=prepare_minute(row,broker,store)
            args=dict(invocation['session_kwargs'])
            if (args.get('broker') is not broker or args.get('store') is not store
                    or args.get('account_id')!=account_id or stamp(args['clock']())!=at
                    or stamp(broker.clock())!=at or type(invocation['model_enabled']) is not bool):
                raise ValueError('REPLAY_ACCOUNT_OR_CLOCK_REBIND_FORBIDDEN')
            # Only original eligible entry frames can be offered as entries.
            entry_ids={r['plan_id'] for r in row['entry_source_frames']}
            if not set(args.get('active_plan_ids',()))<=entry_ids:
                raise ValueError('REPLAY_ENTRY_SCOPE_EXPANSION_FORBIDDEN')
            enabled=invocation['model_enabled'] and row['model_tick']
            result=run_control_minute(session_kwargs=args,closed_bars=invocation['closed_bars'],
                deadline_monotonic=monotonic()+47,monotonic=monotonic,model_enabled=enabled,
                envelope_refresh=invocation.get('envelope_refresh'),exception_handler=invocation.get('exception_handler'))
            counts['scheduled_minutes']+=1;counts['regular_model_ticks']+=row['model_tick']
            counts['minute_fills']+=len(result['fills']);counts['hard_exit_intents']+=len(result['hard_exits'])
            counts['source_limited_plan_minutes']+=len(row['data_limited_source_frames'])
            if result.get('trader_skip_reason'):reasons[result['trader_skip_reason']]+=1
            trader=result.get('trader')
            if trader is not None:
                counts['evaluated_model_batches']+=1;counts['validated_model_batches']+=bool(trader.get('model_validated'))
                reasons.update(trader.get('reason_codes',[]))
                for admission in trader.get('admissions',[]):
                    admission_statuses[admission['judgment']['status']]+=1
            if broker.reconcile()['status']!='MATCH':raise ValueError('REPLAY_RECONCILIATION_MISMATCH')
        cursor+=240
        set_clock(base.replace(hour=15,minute=1));close=consume_close(day,broker,store)
        set_clock(base.replace(hour=15,minute=30));settlement=broker.settle(day)
        if broker.reconcile()['status']!='MATCH':raise ValueError('REPLAY_RECONCILIATION_MISMATCH')
        days.append(dict(trade_date=day,morning=morning,counts=dict(counts),reason_codes=dict(reasons),
            admission_statuses=dict(admission_statuses),close=close,settlement=settlement))
    model_batches=sum(d['counts'].get('evaluated_model_batches',0) for d in days)
    validated=sum(d['counts'].get('validated_model_batches',0) for d in days)
    return dict(status='CONTIGUOUS_TIMELINE_EXECUTED',days=days,paper_identity=paper_identity,
        scheduled_minutes=cursor,regular_model_ticks=sum(d['counts']['regular_model_ticks'] for d in days),
        model_batch_legal_rate=None if model_batches==0 else validated/model_batches,
        legal_rate_denominator=model_batches,production_writes=0,
        control_release_gate='NOT_EVALUATED_NEEDS_SOURCE_AND_GUARD_AND_TWO_PASS_AUDIT',
        returns_claim='NONE_REPLAY_BAR_DERIVED')
