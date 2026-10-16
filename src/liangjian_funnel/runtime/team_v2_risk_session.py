"""Actual A6 PREMARKET client, strict schema and durable per-account latch.

No provider retries after an uncertain/captured call, no legacy DB or orders.
The caller owns source acquisition; returned PLANNING envelopes cannot trade.
"""
from dataclasses import asdict
from datetime import datetime,time
import hashlib
import json
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_model import call_team_model,team_call_binding
from ..pipeline.team_v2_risk_schema import validate_risk_premarket
from .team_v2_store import canonical


def run_risk_premarket(*,repository,client,journal,store,meeting_id,call_id,account_id,run_id,model,
        trade_date,source_trade_date,clock,required_envelope,replacements,envelopes,
        engine_utilization_cap,capacity_slots,cluster_caps,unready_plan_ids,limits,
        timeout_seconds,max_output_tokens,allow_model_call=False):
    if not allow_model_call:raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now=clock()
    if (not isinstance(now,datetime) or now.utcoffset() is None
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
            or now.astimezone(ZoneInfo('Asia/Shanghai')).time().replace(tzinfo=None)>=time(9,30)
            or any(e.trade_date!=trade_date or e.purpose!='PLANNING' for e in envelopes)
            or source_trade_date>trade_date):
        raise ValueError('A6_PREMARKET_WINDOW_INVALID')
    validator_args=dict(required_envelope=required_envelope,envelopes=envelopes,
        engine_utilization_cap=engine_utilization_cap,capacity_slots=capacity_slots,
        cluster_caps=cluster_caps,unready_plan_ids=unready_plan_ids)
    values=dict(replacements)
    context=dict(account_id=account_id,call_id=call_id,engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=dict(cluster_caps),unready_plan_ids=sorted(unready_plan_ids),
        timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)
    context_hash=hashlib.sha256(canonical(context).encode()).hexdigest()
    values.update(RISK_SESSION='PREMARKET',ENGINE_ENVELOPES=[asdict(e) for e in envelopes],
        RISK_LIMITS=dict(policy=asdict(limits),engine_upper_bounds=context,session_binding_sha256=context_hash))
    values=json.loads(canonical(values))
    validate_risk_premarket(None,values['EVIDENCE_CATALOG'],**validator_args)
    request=repository.prepare_messages('A6',values,required_envelope=required_envelope,session='PREMARKET')
    binding=hashlib.sha256(canonical(dict(input_hash=request.input_hash,run_id=run_id,model=model,
        trade_date=trade_date,source_trade_date=source_trade_date,context_hash=context_hash)).encode()).hexdigest()
    original_observed=store.claim_risk_session(meeting_id,input_sha256=binding,observed_at=now)
    previous=store.load_risk_session(meeting_id,input_sha256=binding)
    if previous is not None:return previous
    expected=dict(run_id=run_id,role='A6',session='PREMARKET',source_trade_date=source_trade_date,
        execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
    captured=journal.load_captured(call_id,expected=expected)
    output=None;valid=False
    if captured is not None:
        output=captured['output']
        valid=captured['validated'] and validate_risk_premarket(output,values['EVIDENCE_CATALOG'],**validator_args).valid
    elif journal.load_failed(call_id,request_binding=team_call_binding(request,run_id=run_id,model=model,
            session='PREMARKET',timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)) is None:
        try:
            outcome=call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,
                session='PREMARKET',source_trade_date=source_trade_date,execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'],timeout_seconds=timeout_seconds,
                max_output_tokens=max_output_tokens,validator=lambda raw:validate_risk_premarket(
                    raw,values['EVIDENCE_CATALOG'],**validator_args),clock=clock,allow_model_call=True)
            output=outcome.response.output;valid=outcome.validation.valid
        except ValueError as exc:
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
    return store.record_premarket(meeting_id=meeting_id,source_call_id=call_id,account_id=account_id,
        trade_date=trade_date,observed_at=original_observed,envelopes=envelopes,output=output,validated=valid,
        engine_utilization_cap=engine_utilization_cap)
