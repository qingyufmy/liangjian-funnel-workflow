"""A4 actual client -> frozen validated vote -> engine -> independent paper.

Models run outside broker transactions. Captured output and stable admissions
resume separately, so a crash between either commit cannot duplicate orders.
This is an isolated runtime boundary, not production CLI/source wiring.
"""
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import datetime
import hashlib
import json
from zoneinfo import ZoneInfo

from liangjian_paper_broker.ledger import dump
from ..pipeline.team_v2_model import call_team_model
from ..pipeline.team_v2_trader_schema import validate_trader_output
from .team_v2_execution import admit_trader_signal
from .trader_guardrails import is_closed_trader_tick

TZ=ZoneInfo('Asia/Shanghai')


def run_trader_session(*,repository,client,journal,store,broker,decision_id,minute_snapshot_id,
        call_id,account_id,run_id,model,trade_date,source_trade_date,clock,decision_at,
        closed_5m_end,mark_as_of,required_envelope,replacements,active_plan_ids,all_plans,
        envelopes,held_plan_ids,position_snapshot,tech_reference,limits,regime,
        live_market_ready,snapshot_ready,mode,calendar,timeout_seconds,max_output_tokens,
        allow_model_call=False):
    if not allow_model_call:raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now=clock()
    if (any(not isinstance(v,datetime) or v.utcoffset() is None for v in (now,decision_at,mark_as_of))
            or now.astimezone(TZ).date().isoformat()!=trade_date
            or decision_at.astimezone(TZ).date().isoformat()!=trade_date
            or not is_closed_trader_tick(closed_5m_end) or closed_5m_end>decision_at
            or source_trade_date>trade_date or mode not in {'record','control'}
            or not isinstance(all_plans,Mapping) or not isinstance(envelopes,Mapping)
            or not isinstance(active_plan_ids,(set,frozenset)) or not active_plan_ids.issubset(all_plans)
            or not isinstance(held_plan_ids,Mapping)
            or any(pid not in all_plans or all_plans[pid].symbol!=symbol for symbol,pid in held_plan_ids.items())
            or set(envelopes)!=set(all_plans)
            or any(p.plan_id!=pid or envelopes[pid].plan_id!=pid or envelopes[pid].symbol!=p.symbol
                or envelopes[pid].trade_date!=trade_date for pid,p in all_plans.items())
            or not isinstance(position_snapshot,(list,tuple))
            or any(not isinstance(p,Mapping) or held_plan_ids.get(p.get('symbol'))!=p.get('plan_id')
                or type(p.get('qty')) is not int or p['qty']<=0 for p in position_snapshot)
            or {p['symbol'] for p in position_snapshot}!=set(held_plan_ids)):
        raise ValueError('TRADER_SERVER_CONTEXT_INVALID')
    # The file-backed paper identity is part of the immutable input. A completed
    # session from account A cannot be re-used against a different paper ledger.
    ledger=getattr(broker,'ledger',broker)
    context=dict(account_id=account_id,paper_identity=str(ledger.path),run_id=run_id,call_id=call_id,
        decision_at=decision_at,closed_5m_end=closed_5m_end,mark_as_of=mark_as_of,mode=mode,
        plans={pid:asdict(p) for pid,p in sorted(all_plans.items())},active_plan_ids=sorted(active_plan_ids),
        envelopes={pid:asdict(e) for pid,e in sorted(envelopes.items())},limits=asdict(limits),
        held_plan_ids=dict(held_plan_ids),regime=regime,live_market_ready=live_market_ready,
        snapshot_ready=snapshot_ready,timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)
    execution_hash=hashlib.sha256(dump(context).encode()).hexdigest()
    values=dict(replacements)
    values.update(DECISION_ID=decision_id,MINUTE_SNAPSHOT_ID=minute_snapshot_id,
        ACTIVE_PLANS=[context['plans'][pid] for pid in sorted(active_plan_ids)],
        RISK_ENVELOPES=context['envelopes'],POSITIONS=position_snapshot,TECH_REFERENCE=tech_reference,
        CURRENT_TIME=decision_at.isoformat(),TIME_RULES=dict(NO_NEW_ENTRY_AFTER=limits.no_new_entry_after,
            execution_context_sha256=execution_hash))
    # Decimal/datetime normalization is fact serialization, never a role rewrite.
    values=json.loads(dump(values))
    request=repository.prepare_messages('A4',values,required_envelope=required_envelope,session='INTRADAY')
    validation_context=dict(required_envelope=required_envelope,decision_id=decision_id,
        minute_snapshot_id=minute_snapshot_id,plans={pid:all_plans[pid].symbol for pid in active_plan_ids},
        held_symbols=set(held_plan_ids),tech_reference=tech_reference)
    validate_trader_output(None,values['EVIDENCE_CATALOG'],**validation_context)
    binding=hashlib.sha256(dump(dict(execution_hash=execution_hash,input_hash=request.input_hash)).encode()).hexdigest()
    previous=store.load_trader(decision_id,input_sha256=binding)
    if previous is not None:return previous
    store.claim_trader(decision_id,input_sha256=binding)
    expected=dict(run_id=run_id,role='A4',session='INTRADAY',source_trade_date=source_trade_date,
        execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
    captured=journal.load_captured(call_id,expected=expected)
    output=None;valid=False;issues=()
    if captured is not None:
        output=captured['output']
        checked=validate_trader_output(output,values['EVIDENCE_CATALOG'],**validation_context)
        valid=captured['validated'] and checked.valid
        issues=tuple(dict.fromkeys((*captured['issues'],*checked.issues)))
    else:
        try:
            outcome=call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,
                session='INTRADAY',source_trade_date=source_trade_date,execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'],timeout_seconds=timeout_seconds,
                max_output_tokens=max_output_tokens,validator=lambda raw:validate_trader_output(
                    raw,values['EVIDENCE_CATALOG'],**validation_context),clock=clock,allow_model_call=True)
            output=outcome.response.output;valid=outcome.validation.valid;issues=outcome.validation.issues
        except ValueError as exc:
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
            issues=(str(exc),)
    admissions=[]
    if valid:
        commands=[('plan',row['plan_id'],row) for row in output['signals']]
        commands.extend(('position',held_plan_ids[row['symbol']],row) for row in output['position_actions'])
        for kind,pid,row in commands:
            if row['action']=='HOLD':continue
            risk=envelopes[pid]
            if row['action'] in {'BUY_SIGNAL','ADD_SIGNAL'} and kind=='plan':
                hint=row['handoff_to_a6']
                risk=replace(risk,max_batches=min(risk.max_batches,hint['batches_hint']))
                if hint['risk_unit_hint']=='PROBE' and risk.risk_unit=='STANDARD':
                    risk=replace(risk,risk_unit='PROBE',risk_budget=risk.risk_budget*limits.probe_multiplier)
            sid='A4:'+hashlib.sha256(dump([account_id,decision_id,kind,pid]).encode()).hexdigest()
            admissions.append(admit_trader_signal(broker=broker,signal_id=sid,action=row['action'],
                plan=all_plans[pid],envelope=risk,limits=limits,all_plans=all_plans,now=decision_at,
                closed_5m_end=closed_5m_end,mark_as_of=mark_as_of,regime=regime,
                live_market_ready=live_market_ready,snapshot_ready=snapshot_ready,mode=mode,
                calendar=calendar,input_sha256=request.input_hash))
    result=dict(status='EVALUATED' if valid else 'TRADER_DEGRADED_NO_ACTION',
        decision_id=decision_id,source_call_id=call_id,minute_snapshot_id=minute_snapshot_id,
        trade_date=trade_date,input_hash=request.input_hash,model_validated=valid,
        reason_codes=list(issues),admissions=admissions,
        handoff_notes=output['handoff_notes'] if valid else {},
        what_would_change_my_mind=output['what_would_change_my_mind'] if valid else [])
    return store.record_trader(decision_id=decision_id,source_call_id=call_id,account_id=account_id,
        trade_date=trade_date,input_sha256=binding,result=result)
