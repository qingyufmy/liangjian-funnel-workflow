"""0315 separate radar/PMI model sessions: bounded research judgments only.

No scheduler, plan publisher, order admission, broker, or customer sender.
Live activation is a separate integration/review gate, not implied by JSON PASS.
"""
from copy import deepcopy
from datetime import timedelta

from ..pipeline.team_v2_model import call_team_model
from ..pipeline.team_v2_intraday_schema import validate_intraday_radar,validate_pm_intraday
from .team_v2_sources import stamp


def radar_market_context(confirmed,*,observed_at):
    """Adapt existing G5 EMOTION_ON vocabulary to author ON without new votes."""
    at=stamp(observed_at);body=deepcopy(confirmed) if isinstance(confirmed,dict) else {}
    original=body.get('emotion_state');ready=False
    try:
        tick=stamp(body['state']['last_tick'])
        ready=(tick.date()==at.date() and timedelta(0)<=at-tick<=timedelta(minutes=5)
            and body.get('live_authority') is True and body.get('primary_status')=='READY'
            and body['state'].get('emotion_confirmed')==original)
    except (ValueError,KeyError,TypeError):pass
    state=('ON' if ready and original=='EMOTION_ON' and body.get('radar_open') is True
            and body.get('leader_open') is True else 'OFF' if ready and original=='EMOTION_OFF' else 'DATA_BLOCK')
    return dict(body,original_emotion_state=original,emotion_state=state,
        normalization='G5_CONFIRMED_ENUM_MAPPING_NOT_NEW_EMOTION_CLASSIFIER')


def call_intraday_research(*,role,repository,client,journal,call_id,run_id,model,
        source_trade_date,execution_trade_date,required_envelope,replacements,server_context,
        clock,max_output_tokens,allow_model_call=False):
    """Exactly one existing transport attempt, fixed 60s per authored session."""
    if role not in {'A2','A3'}:raise ValueError('INTRADAY_ROLE_INVALID')
    session='INTRADAY_RADAR' if role=='A2' else 'PM_INTRADAY'
    validator=validate_intraday_radar if role=='A2' else validate_pm_intraday
    values=deepcopy(replacements);context=deepcopy(server_context);envelope=deepcopy(required_envelope)
    catalog=values['EVIDENCE_CATALOG']
    validator(None,catalog,required_envelope=envelope,**context)
    request=repository.prepare_messages(role,values,required_envelope=envelope,session=session)
    return call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,session=session,
        source_trade_date=source_trade_date,execution_trade_date=execution_trade_date,catalog=catalog,
        timeout_seconds=60,max_output_tokens=max_output_tokens,
        validator=lambda raw:validator(raw,catalog,required_envelope=envelope,**context),
        clock=clock,allow_model_call=allow_model_call)
