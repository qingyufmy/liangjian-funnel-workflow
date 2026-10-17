"""Actual A3-PM client -> evidence validation -> independent decision journal.

No WorkflowApplication/RuntimeStore, broker, notifications, or production plans.
Node/CLI source acquisition is a separate integration boundary. Inputs here must
already be frozen and their dates/ownership established by that boundary.
"""
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_model import call_team_model,team_call_binding
from ..pipeline.team_v2_pm_schema import validate_pm_premarket,derive_pm_slots,PM_PRIMARY_CLUSTER_CAP
from .team_v2_store import canonical


def run_pm_premarket(*, repository, client, journal, store, meeting_id: str, call_id: str,
                      account_id: str, run_id: str, model: str, trade_date: str,
                      source_trade_date: str, clock, required_envelope: Mapping,
                      replacements: Mapping, envelopes: tuple, plan_clusters: Mapping,
                      free_slots: int, baseline_range: tuple[Decimal, Decimal],
                      utilization_cap: Decimal, high_conviction_ids: set[str],
                      timeout_seconds: float, max_output_tokens: int,
                      allow_model_call: bool = False) -> dict:
    now = clock()
    if (not isinstance(now, datetime) or now.utcoffset() is None
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat() != trade_date
            or any(row.trade_date != trade_date for row in envelopes)):
        raise ValueError('PM_SESSION_DATE_INVALID')
    # Authorization is checked even on a replay, avoiding an accidental live
    # enable by a read-only caller which happens to know an old meeting ID.
    if not allow_model_call: raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    values = dict(replacements)
    # Authoritative derived inputs overwrite caller decoration, never the other
    # way round. Current geometry is not changed and planning is not activation.
    values['RISK_ENVELOPES'] = json.loads(canonical([asdict(row) for row in envelopes]))
    values['DEPLOYMENT_BASELINE'] = {'baseline_range': [str(x) for x in baseline_range],
                                    'utilization_cap': str(utilization_cap), 'free_slots': free_slots,
                                    'primary_cluster_cap':PM_PRIMARY_CLUSTER_CAP}
    request = repository.prepare_messages('A3', values, required_envelope=required_envelope,
                                           session='PM_PREMARKET')
    validator_args = dict(required_envelope=required_envelope, envelopes=envelopes,
        plan_clusters=plan_clusters, free_slots=free_slots, baseline_range=baseline_range,
        utilization_cap=utilization_cap, high_conviction_ids=high_conviction_ids)
    # Validate server domain before HTTP, not just inside the model callback.
    validate_pm_premarket(None, values['EVIDENCE_CATALOG'], **validator_args)
    binding = hashlib.sha256(canonical(dict(account_id=account_id, run_id=run_id, model=model,
        trade_date=trade_date, source_trade_date=source_trade_date, call_id=call_id,
        input_hash=request.input_hash, plan_clusters=dict(plan_clusters), free_slots=free_slots,
        high_conviction_ids=sorted(high_conviction_ids), timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens)).encode()).hexdigest()
    previous = store.load_pm(meeting_id, input_sha256=binding)
    if previous is not None: return previous
    expected = dict(run_id=run_id, role='A3', session='PM_PREMARKET', source_trade_date=source_trade_date,
        execution_trade_date=trade_date, model=model, prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash, catalog_hash=request.evidence_catalog_hash)
    captured = journal.load_captured(call_id, expected=expected)
    issues = (); output = None; valid = False
    if captured is not None:
        output = captured['output']
        checked = validate_pm_premarket(output, values['EVIDENCE_CATALOG'], **validator_args)
        valid = captured['validated'] and checked.valid
        issues = tuple(dict.fromkeys((*captured['issues'], *checked.issues)))
    elif (failure := journal.load_failed(call_id,request_binding=team_call_binding(request,
            run_id=run_id,model=model,session='PM_PREMARKET',timeout_seconds=timeout_seconds,
            max_output_tokens=max_output_tokens))) is not None:
        issues=(failure,)
    else:
        try:
            outcome = call_team_model(request, client, journal, call_id=call_id, run_id=run_id, model=model,
                session='PM_PREMARKET', source_trade_date=source_trade_date, execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'], timeout_seconds=timeout_seconds,
                max_output_tokens=max_output_tokens, validator=lambda result: validate_pm_premarket(
                    result, values['EVIDENCE_CATALOG'], **validator_args), clock=clock, allow_model_call=True)
            output = outcome.response.output; valid = outcome.validation.valid; issues = outcome.validation.issues
        except ValueError as exc:
            # No hidden retry or model-chosen defaults. A failed/uncertain call
            # produces no slot or new entry permission. Storage/binding errors
            # remain errors instead of being mistaken for model degradation.
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED', 'TEAM_MODEL_RESULT_INVALID'}: raise
            issues = (str(exc),)
    derived=derive_pm_slots(output['plan_ranking'],envelopes=envelopes,plan_clusters=plan_clusters,
        free_slots=free_slots) if valid else dict(slot_plan=[],derived_shortfall='MODEL_INVALID')
    decision = {'status': 'READY' if valid else 'PM_DEGRADED_NO_ALLOCATION',
        'session': 'PM_PREMARKET', 'trade_date': trade_date, 'source_call_id': call_id,
        'input_hash': request.input_hash, 'reason_codes': list(issues),
        'deployment_target': output['deployment_target'] if valid else None,
        'slot_plan': derived['slot_plan'],
        'plan_ranking': output['plan_ranking'] if valid else [],
        'derived_shortfall': derived['derived_shortfall'],
        'allocation_plan_bindings': {row.plan_id:[row.symbol,plan_clusters[row.plan_id]] for row in envelopes} if valid else {},
        'plan_allocations': output['plan_allocations'] if valid else [],
        'shortfall_reason': output['shortfall_reason'] if valid else 'ENVELOPE_TIGHTENED',
        'handoff_notes': output['handoff_notes'] if valid else {},
        'what_would_change_my_mind': output['what_would_change_my_mind'] if valid else []}
    return store.record_pm(meeting_id=meeting_id, source_call_id=call_id, account_id=account_id,
        trade_date=trade_date, input_sha256=binding, result=decision)
