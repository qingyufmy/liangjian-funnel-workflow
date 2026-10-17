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


def model_evidence_counts(trader,journal):
    """Count actual attempts, not CARRIED_WAIT, capacity deferrals or roots.

    Shard scope flags are set by the coordinator after durable call claims.
    When a journal is supplied, check those claims again without network I/O.
    A legacy single-session status alone is not evidence that a request ran.
    """
    counts=Counter()
    if 'shards' not in trader:
        identity=trader.get('source_call_id')
        if journal is not None and identity and journal.has_call_claim(identity):
            counts['attempted_model_calls']=1
            counts['validated_model_calls']=int(trader.get('model_validated') is True)
        return counts
    decisions=trader['scope']['decisions']
    sent={r['plan_id'] for r in decisions if r.get('model_judgment_denominator') is True}
    counts.update(model_attempted_plan_ticks=len(sent),
        selected_not_sent_plan_ticks=sum(bool(r.get('selected_for_model')) and r['plan_id'] not in sent for r in decisions),
        deferred_capacity_plan_ticks=sum(r['reason_code']=='DEFERRED_CAPACITY' for r in decisions),
        carried_wait_plan_ticks=sum(r['reason_code']=='CARRIED_WAIT' for r in decisions))
    covered=set()
    for part in trader['shards']:
        attempted=set(part['plan_ids']) & sent
        if not attempted:continue
        if (covered & attempted or journal is not None and not journal.has_call_claim(part['source_call_id'])):
            raise ValueError('REPLAY_MODEL_ATTEMPT_BINDING_INVALID')
        covered.update(attempted)
        counts['attempted_model_calls']+=1
        if part.get('model_validated') is True:
            counts['validated_model_calls']+=1
            counts['model_legal_plan_ticks']+=len(attempted)
    if covered!=sent:raise ValueError('REPLAY_MODEL_ATTEMPT_SCOPE_MISSING')
    return counts


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
                evidence=model_evidence_counts(trader,args.get('journal'))
                counts.update(evidence)
                counts['evaluated_model_batches']+=bool(evidence['attempted_model_calls'])
                counts['validated_model_batches']+=bool(evidence['attempted_model_calls']) and trader.get('model_validated') is True
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
    model_batches=sum(d['counts'].get('attempted_model_calls',0) for d in days)
    validated=sum(d['counts'].get('validated_model_calls',0) for d in days)
    plan_attempts=sum(d['counts'].get('model_attempted_plan_ticks',0) for d in days)
    legal_plans=sum(d['counts'].get('model_legal_plan_ticks',0) for d in days)
    return dict(status='CONTIGUOUS_TIMELINE_EXECUTED',days=days,paper_identity=paper_identity,
        scheduled_minutes=cursor,regular_model_ticks=sum(d['counts']['regular_model_ticks'] for d in days),
        model_batch_legal_rate=None if model_batches==0 else validated/model_batches,
        legal_rate_denominator=model_batches,legal_rate_unit='ACTUAL_MODEL_CALL_SHARD',
        model_plan_legal_rate=None if plan_attempts==0 else legal_plans/plan_attempts,
        model_plan_legal_rate_denominator=plan_attempts,production_writes=0,
        control_release_gate='NOT_EVALUATED_NEEDS_SOURCE_AND_GUARD_AND_TWO_PASS_AUDIT',
        returns_claim='NONE_REPLAY_BAR_DERIVED')
