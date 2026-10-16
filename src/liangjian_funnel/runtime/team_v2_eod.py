"""A3-PM -> A6 close review, owned durable advisory state, never direct orders.

No WorkflowApplication/RuntimeStore, source fetch, customer channel or legacy
plan mutation. Failure retains the original PM proposal but grants no reviewed
overnight action; 1m hard-risk execution remains independent of this session.
"""
from collections.abc import Mapping
from datetime import datetime,time
import hashlib
import json
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_model import call_team_model,team_call_binding
from ..pipeline.team_v2_pm_schema import validate_pm_post_close
from ..pipeline.team_v2_risk_schema import validate_risk_post_close
from .team_v2_risk_meeting import review_overnight
from .team_v2_store import canonical


def _response(*,request,client,journal,call_id,run_id,model,source_trade_date,trade_date,
        timeout_seconds,max_output_tokens,validator,catalog,clock,session):
    expected=dict(run_id=run_id,role=request.prompt.role,session=session,source_trade_date=source_trade_date,
        execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
    captured=journal.load_captured(call_id,expected=expected)
    if captured is not None:
        checked=validator(captured['output'])
        return captured['output'],captured['validated'] and checked.valid,tuple(dict.fromkeys((*captured['issues'],*checked.issues)))
    failure=journal.load_failed(call_id,request_binding=team_call_binding(request,run_id=run_id,
        model=model,session=session,timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens))
    if failure:return None,False,(failure,)
    try:
        outcome=call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,
            session=session,source_trade_date=source_trade_date,execution_trade_date=trade_date,catalog=catalog,
            timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens,validator=validator,
            clock=clock,allow_model_call=True)
        return outcome.response.output,outcome.validation.valid,outcome.validation.issues
    except ValueError as exc:
        if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
        return None,False,(str(exc),)


def run_team_eod(*,repository,client,journal,store,meeting_id,pm_call_id,risk_call_id,account_id,
        run_id,model,trade_date,source_trade_date,clock,pm_required_envelope,risk_required_envelope,
        pm_replacements,risk_replacements,positions,risk_events,catalog,engine_utilization_cap,
        capacity_slots,cluster_caps,timeout_seconds,max_output_tokens,allow_model_call=False):
    if not allow_model_call:raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now=clock()
    if (not isinstance(now,datetime) or now.utcoffset() is None
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
            or now.astimezone(ZoneInfo('Asia/Shanghai')).time().replace(tzinfo=None)<time(15,10)
            or source_trade_date>trade_date):raise ValueError('EOD_WINDOW_INVALID')
    if (not isinstance(positions,(list,tuple)) or not isinstance(risk_events,(list,tuple))
            or any(not isinstance(p,Mapping) or not isinstance(p.get('symbol'),str) or not p['symbol'] for p in positions)
            or any(not isinstance(e,Mapping) or not isinstance(e.get('event_id'),str) or not e['event_id'] for e in risk_events)):
        raise ValueError('EOD_SERVER_CONTEXT_INVALID')
    symbols={p['symbol'] for p in positions};events={e['event_id'] for e in risk_events}
    if len(symbols)!=len(positions) or len(events)!=len(risk_events) or pm_call_id==risk_call_id:
        raise ValueError('EOD_SERVER_CONTEXT_INVALID')
    pm_args=dict(required_envelope=pm_required_envelope,position_symbols=symbols,
        allowed_clusters=set(cluster_caps),max_positions=capacity_slots)
    risk_args=dict(required_envelope=risk_required_envelope,pm_stances={},engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps,event_ids=events)
    validate_pm_post_close(None,catalog,**pm_args)
    validate_risk_post_close(None,catalog,**risk_args)
    context=dict(account_id=account_id,run_id=run_id,model=model,trade_date=trade_date,
        source_trade_date=source_trade_date,pm_call_id=pm_call_id,risk_call_id=risk_call_id,
        positions=positions,risk_events=risk_events,engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps,timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens,pm_envelope=pm_required_envelope,risk_envelope=risk_required_envelope,
        pm_replacements=pm_replacements,risk_replacements=risk_replacements,catalog=catalog,
        prompt_sha256={
            'A3':dict(repository.prepare('A3',pm_replacements,session='PM_POST_CLOSE').source_hashes),
            'A6':dict(repository.prepare('A6',dict(risk_replacements,RISK_SESSION='POST_CLOSE'),session='POST_CLOSE').source_hashes)})
    binding=hashlib.sha256(canonical(context).encode()).hexdigest()
    store.claim_eod(meeting_id,input_sha256=binding)
    previous=store.load_eod(meeting_id,input_sha256=binding)
    if previous is not None:return previous
    common=dict(client=client,journal=journal,run_id=run_id,model=model,source_trade_date=source_trade_date,
        trade_date=trade_date,timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens,catalog=catalog,clock=clock)
    values=dict(pm_replacements)
    values.update(POSITIONS=positions,EVIDENCE_CATALOG=catalog,RISK_EVENTS_TODAY=risk_events,
        ACCOUNT_SNAPSHOT=dict(source_snapshot=pm_replacements['ACCOUNT_SNAPSHOT'],team_account_id=account_id,
            trade_date=trade_date,eod_request_sha256=binding),
        RISK_CAPACITY=dict(plans_acceptable=capacity_slots,cluster_quota=cluster_caps))
    request=repository.prepare_messages('A3',json.loads(canonical(values)),required_envelope=pm_required_envelope,session='PM_POST_CLOSE')
    pm,pm_valid,issues=_response(request=request,call_id=pm_call_id,session='PM_POST_CLOSE',
        validator=lambda raw:validate_pm_post_close(raw,catalog,**pm_args),**common)
    store.record_pm(meeting_id=meeting_id+':pm',source_call_id=pm_call_id,account_id=account_id,
        trade_date=trade_date,input_sha256=binding,result=dict(session='PM_POST_CLOSE',
            status='READY' if pm_valid else 'PM_EOD_NO_PROPOSAL',source_call_id=pm_call_id,
            overnight_stances=pm['overnight_stances'] if pm_valid else [],
            capacity_request=pm['capacity_request'] if pm_valid else None,reason_codes=list(issues)))
    result=dict(status='PM_EOD_NO_PROPOSAL',session='RISK_EOD',trade_date=trade_date,account_id=account_id,
        source_pm_call_id=pm_call_id,source_risk_call_id=None,pm_proposal=None,
        overnight_actions=[],capacity_for_tomorrow=None,reason_codes=list(issues))
    if pm_valid:
        result.update(status='A6_EOD_UNREVIEWED',pm_proposal=pm,source_risk_call_id=risk_call_id)
        risk_args['pm_stances']={row['symbol']:row['stance'] for row in pm['overnight_stances']}
        values=dict(risk_replacements)
        values.update(RISK_SESSION='POST_CLOSE',POSITIONS=positions,PM_PROPOSAL=pm,EVIDENCE_CATALOG=catalog,
            ACCOUNT_SNAPSHOT=dict(source_snapshot=risk_replacements['ACCOUNT_SNAPSHOT'],team_account_id=account_id,
                trade_date=trade_date,eod_request_sha256=binding),
            RISK_EVENTS_TODAY=risk_events,ENGINE_ENVELOPES=[],EXCEPTION_CONTEXT=[],
            RISK_LIMITS=dict(policy=risk_replacements['RISK_LIMITS'],engine_utilization_cap=engine_utilization_cap,
                capacity_slots=capacity_slots,cluster_caps=cluster_caps))
        request=repository.prepare_messages('A6',json.loads(canonical(values)),required_envelope=risk_required_envelope,session='POST_CLOSE')
        risk,valid,issues=_response(request=request,call_id=risk_call_id,session='POST_CLOSE',
            validator=lambda raw:validate_risk_post_close(raw,catalog,**risk_args),**common)
        result['reason_codes']=list(issues)
        if valid:
            actions=review_overnight(risk_args['pm_stances'],risk['overnight_reviews'])
            # Full original review evidence accompanies each effective stance;
            # pm_override is only a stricter change, not an AGREE fabrication.
            evidence={row['symbol']:row for row in risk['overnight_reviews']}
            result.update(status='READY',overnight_actions=[dict(row,a6_review=evidence[row['symbol']]) for row in actions],
                capacity_for_tomorrow=risk['capacity_for_tomorrow'])
    return store.record_eod(meeting_id=meeting_id,input_sha256=binding,pm_call_id=pm_call_id,
        risk_call_id=risk_call_id,result=result)
