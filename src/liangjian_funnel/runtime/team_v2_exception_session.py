"""A6 exception HTTP edge, frozen trigger scope and restart-safe review commit.

Called only by a trigger coordinator with the monitor's absolute deadline.
It neither detects new thresholds nor submits position directives as orders.
"""
from dataclasses import asdict
from datetime import datetime,timedelta
import hashlib
import json
import math
import time
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_model import call_team_model,team_call_binding
from ..pipeline.team_v2_risk_schema import validate_risk_exception
from .team_v2_risk_exception import RiskExceptionRuntime
from .team_v2_store import canonical
from .trader_guardrails import is_closed_trader_tick


def run_risk_exception(*,repository,client,journal,store,meeting_id,call_id,account_id,run_id,model,
        trade_date,source_trade_date,clock,required_envelope,replacements,targets,hard_halt,
        engine_utilization_cap,capacity_slots,cluster_caps,limits,timeout_seconds,max_output_tokens,
        deadline_monotonic,monotonic=time.monotonic,allow_model_call=False):
    if not allow_model_call:raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now=clock()
    if (not isinstance(now,datetime) or now.utcoffset() is None
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
            or source_trade_date>trade_date):raise ValueError('A6_EXCEPTION_WINDOW_INVALID')
    if (type(timeout_seconds) not in (int,float) or not math.isfinite(timeout_seconds)
            or not 0<timeout_seconds<=20 or type(deadline_monotonic) not in (int,float)
            or not math.isfinite(deadline_monotonic) or type(max_output_tokens) is not int or max_output_tokens<=0):
        raise ValueError('A6_EXCEPTION_BUDGET_INVALID')
    runtime=RiskExceptionRuntime(store)
    if (runtime.context(meeting_id) is None and not is_closed_trader_tick(
            now.astimezone(ZoneInfo('Asia/Shanghai')).replace(second=0,microsecond=0)-timedelta(minutes=1))):
        raise ValueError('A6_EXCEPTION_WINDOW_INVALID')
    values=dict(replacements)
    context=dict(account_id=account_id,call_id=call_id,engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps,timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens,targets=targets,hard_halt=hard_halt,policy=asdict(limits))
    # Bind the request before reading mutable state, so a retry recovers the
    # original restrictions rather than issuing a differently scoped model call.
    binding=hashlib.sha256(canonical(dict(values=values,required_envelope=required_envelope,context=context,
        run_id=run_id,model=model,trade_date=trade_date,source_trade_date=source_trade_date,
        prompt_sha256=repository.document('A6').sha256)).encode()).hexdigest()
    frozen=runtime.claim(meeting_id,account_id=account_id,trade_date=trade_date,observed_at=now,
        input_sha256=binding,targets=targets,hard_halt=hard_halt,
        deadline_at=now+timedelta(seconds=max(0,deadline_monotonic-monotonic())))
    previous=runtime.load(meeting_id,input_sha256=binding)
    if previous is not None:return previous
    # Reserve two seconds for local validation/commit; bind the first request's
    # smaller HTTP budget through its frozen timestamps, not each retry's clock.
    effective_timeout=min(timeout_seconds,max(0,(datetime.fromisoformat(frozen['deadline_at'])-
        datetime.fromisoformat(frozen['observed_at'])).total_seconds()-2))
    state=frozen['day_state'];latched=bool(state and (state[1] or state[0]>=2))
    validator_args=dict(required_envelope=required_envelope,targets=frozen['targets'],
        resume_allowed={(r['target'],r['target_id']) for r in frozen['controls']},
        hard_halt=hard_halt or latched,engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=cluster_caps)
    values.update(RISK_SESSION='INTRADAY_EXCEPTION',EXCEPTION_CONTEXT=dict(targets=frozen['targets'],
        previous_a6_controls=frozen['controls'],hard_halt=hard_halt or latched),
        RISK_LIMITS=context,ENGINE_ENVELOPES=values.get('ENGINE_ENVELOPES',[]))
    values=json.loads(canonical(values))
    validate_risk_exception(None,values['EVIDENCE_CATALOG'],**validator_args)
    request=repository.prepare_messages('A6',values,required_envelope=required_envelope,session='INTRADAY_EXCEPTION')
    expected=dict(run_id=run_id,role='A6',session='INTRADAY_EXCEPTION',source_trade_date=source_trade_date,
        execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
    captured=journal.load_captured(call_id,expected=expected)
    output=None;valid=False
    if captured is not None:
        output=captured['output']
        valid=(captured['validated']
            and datetime.fromisoformat(captured['captured_at'])<=datetime.fromisoformat(frozen['deadline_at'])
            and now<=datetime.fromisoformat(frozen['deadline_at'])
            and validate_risk_exception(output,values['EVIDENCE_CATALOG'],**validator_args).valid)
    elif (not latched and effective_timeout>0 and deadline_monotonic-monotonic()>effective_timeout+1
            and journal.load_failed(call_id,request_binding=team_call_binding(request,run_id=run_id,model=model,
                session='INTRADAY_EXCEPTION',timeout_seconds=effective_timeout,max_output_tokens=max_output_tokens)) is None):
        try:
            outcome=call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,
                session='INTRADAY_EXCEPTION',source_trade_date=source_trade_date,execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'],timeout_seconds=effective_timeout,max_output_tokens=max_output_tokens,
                validator=lambda raw:validate_risk_exception(raw,values['EVIDENCE_CATALOG'],**validator_args),
                clock=clock,allow_model_call=True)
            output=outcome.response.output;valid=outcome.validation.valid
            # Even a transport returning after its deadline cannot lift a cap.
            if monotonic()>=deadline_monotonic:valid=False
        except ValueError as exc:
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
    return runtime.record(meeting_id,source_call_id=call_id,input_sha256=binding,output=output,validated=valid)
