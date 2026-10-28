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
from ..pipeline.team_v2_pm_schema import validate_pm_premarket,project_pm_compact,project_pm_notes,PM_PRIMARY_CLUSTER_CAP
from .team_v2_store import canonical
from .team_v2_pm_policy import pm_shortlist, derive_pm_fallback


def _notify_fallback(store,decision,*,account_id,trade_date):
    if decision.get('decision_authority')!='SERVER_FALLBACK_NOT_MODEL_VALIDATED':return
    from .team_v2_internal import InternalOutbox
    envelope_basis=('仅服务端 A6 首次失败保守信封' if decision.get('fallback_envelope_authority')==
        'SERVER_A6_DEGRADED_CONSERVATIVE' else '仅A6合格计划')
    InternalOutbox(store).enqueue(account_id=account_id,trade_date=trade_date,kind='ALERT',
        source_id='PM_UNAVAILABLE_SERVER_FALLBACK:'+trade_date,
        observed_at=datetime.fromisoformat(decision['fallback_observed_at']),priority=0,
        body={'msg_type':'text','content':{'text':'模拟盘｜PM失败，服务端保守分配\n'
            'PM_UNAVAILABLE_SERVER_FALLBACK\n'+envelope_basis+'、PROBE，仍须盘中条件及全部风控准入'}})


def run_pm_premarket(*, repository, client, journal, store, meeting_id: str, call_id: str,
                      account_id: str, run_id: str, model: str, trade_date: str,
                      source_trade_date: str, clock, required_envelope: Mapping,
                      replacements: Mapping, envelopes: tuple, plan_clusters: Mapping,
                      free_slots: int, baseline_range: tuple[Decimal, Decimal],
                      utilization_cap: Decimal, high_conviction_ids: set[str],
                      timeout_seconds: float, max_output_tokens: int,
                      allow_model_call: bool = False, fallback_enabled: bool = True,
                      fallback_priority_order: tuple = (), max_new_entries: int = 3,
                      a6_validated: bool = False, a6_degraded_conservative: bool = False,
                      max_positions: int = 6, geometry_plan_ids=None,geometry_audit=None,
                      deadline_at=None) -> dict:
    now = clock()
    if (not isinstance(now, datetime) or now.utcoffset() is None
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat() != trade_date
            or any(row.trade_date != trade_date for row in envelopes)):
        raise ValueError('PM_SESSION_DATE_INVALID')
    # Authorization is checked even on a replay, avoiding an accidental live
    # enable by a read-only caller which happens to know an old meeting ID.
    if not allow_model_call: raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    if (type(fallback_enabled) is not bool or type(a6_validated) is not bool
            or type(a6_degraded_conservative) is not bool):
        raise ValueError('PM_FALLBACK_CONTEXT_INVALID')
    values,shortlist,plan_refs=pm_shortlist(replacements,envelopes=envelopes,
        plan_clusters=plan_clusters,geometry_plan_ids=geometry_plan_ids,max_positions=max_positions)
    if len(shortlist['selected_plan_ids'])>2*max_positions:
        from .team_v2_pm_batches import run_pm_batches
        import inspect
        scope=locals()
        arguments={key:scope[key] for key in inspect.signature(run_pm_premarket).parameters}
        return run_pm_batches(run_one=run_pm_premarket,arguments=arguments,
            all_values=values,shortlist=shortlist,plan_refs=plan_refs)
    shortlist['geometry_source_audit']=geometry_audit
    # Authoritative derived inputs overwrite caller decoration, never the other
    # way round. Current geometry is not changed and planning is not activation.
    values['RISK_ENVELOPES'] = json.loads(canonical([asdict(row) for row in envelopes if row.plan_id in plan_refs.values()]))
    values['DEPLOYMENT_BASELINE'] = {'baseline_range': [str(x) for x in baseline_range],
                                    'utilization_cap': str(utilization_cap), 'free_slots': free_slots,
                                    'primary_cluster_cap':PM_PRIMARY_CLUSTER_CAP,
                                    'max_positions':max_positions,'shortlist_limit':2*max_positions}
    original_values=dict(replacements)
    original_values.update(RISK_ENVELOPES=json.loads(canonical([asdict(row) for row in envelopes])),
        DEPLOYMENT_BASELINE=values['DEPLOYMENT_BASELINE'])
    original_request=repository.prepare_messages('A3',original_values,required_envelope=required_envelope,
        session='PM_PREMARKET')
    request = repository.prepare_messages('A3', values, required_envelope=required_envelope,
                                           session='PM_PREMARKET')
    input_projection=dict(original_input_hash=original_request.input_hash,projected_input_hash=request.input_hash,
        original_chars=sum(len(m['content']) for m in original_request.messages),
        projected_chars=sum(len(m['content']) for m in request.messages),
        original_bytes=sum(len(m['content'].encode('utf-8')) for m in original_request.messages),
        projected_bytes=sum(len(m['content'].encode('utf-8')) for m in request.messages),
        evidence_ids=[i['id'] for i in values['EVIDENCE_CATALOG'].get('items',[])],
        authority='SAME_FROZEN_SOURCE_NECESSARY_PM_FIELDS_NOT_NEW_FACTS')
    validator_args = dict(required_envelope=required_envelope, envelopes=envelopes,
        plan_clusters=plan_clusters, free_slots=free_slots, baseline_range=baseline_range,
        utilization_cap=utilization_cap, high_conviction_ids=high_conviction_ids)
    # Validate server domain before HTTP, not just inside the model callback.
    validate_pm_premarket(None, values['EVIDENCE_CATALOG'], **validator_args)
    deadline_exceeded=False
    if deadline_at is not None:
        if (deadline_at.utcoffset() is None or deadline_at.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
                or deadline_at.astimezone(ZoneInfo('Asia/Shanghai')).time().replace(tzinfo=None)>datetime.strptime('08:45','%H:%M').time()):
            raise ValueError('PM_DEADLINE_INVALID')
        budget_source=hashlib.sha256(canonical(dict(call_id=call_id,input_hash=request.input_hash,
            deadline=deadline_at.isoformat(),configured_timeout=timeout_seconds)).encode()).hexdigest()
        frozen=store.freeze_control_context('pm-single-budget:'+meeting_id,source_sha256=budget_source,
            body=dict(timeout_seconds=min(timeout_seconds,max(0,(deadline_at-clock()).total_seconds()-2))))
        timeout_seconds=frozen['timeout_seconds']
        deadline_exceeded=timeout_seconds<=0 or clock()>=deadline_at
    binding = hashlib.sha256(canonical(dict(account_id=account_id, run_id=run_id, model=model,
        trade_date=trade_date, source_trade_date=source_trade_date, call_id=call_id,
        input_hash=request.input_hash, plan_clusters=dict(plan_clusters), free_slots=free_slots,
        high_conviction_ids=sorted(high_conviction_ids), timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens,fallback_enabled=fallback_enabled,
        fallback_priority_order=list(fallback_priority_order),max_new_entries=max_new_entries,
        a6_validated=a6_validated,a6_degraded_conservative=a6_degraded_conservative,
        shortlist=shortlist,deadline_at=deadline_at.isoformat() if deadline_at else None)).encode()).hexdigest()
    previous = store.load_pm(meeting_id, input_sha256=binding)
    if previous is not None:
        _notify_fallback(store,previous,account_id=account_id,trade_date=trade_date)
        return previous
    expected = dict(run_id=run_id, role='A3', session='PM_PREMARKET', source_trade_date=source_trade_date,
        execution_trade_date=trade_date, model=model, prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash, catalog_hash=request.evidence_catalog_hash)
    captured = journal.load_captured(call_id, expected=expected)
    issues = (); output = None; valid = False
    if captured is not None:
        output = captured['output']
        _,checked = project_pm_compact(output, values['EVIDENCE_CATALOG'],plan_refs=plan_refs, **validator_args)
        valid = captured['validated'] and checked.valid
        issues = tuple(dict.fromkeys((*captured['issues'], *checked.issues)))
    elif deadline_exceeded:
        issues=('PM_NOT_REVIEWED_TIME_BUDGET',)
    elif (failure := journal.load_failed(call_id,request_binding=team_call_binding(request,
            run_id=run_id,model=model,session='PM_PREMARKET',timeout_seconds=timeout_seconds,
            max_output_tokens=max_output_tokens))) is not None:
        issues=(failure,)
    else:
        try:
            outcome = call_team_model(request, client, journal, call_id=call_id, run_id=run_id, model=model,
                session='PM_PREMARKET', source_trade_date=source_trade_date, execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'], timeout_seconds=timeout_seconds,
                max_output_tokens=max_output_tokens, validator=lambda result: project_pm_compact(
                    result, values['EVIDENCE_CATALOG'],plan_refs=plan_refs, **validator_args)[1], clock=clock, allow_model_call=True)
            output = outcome.response.output; valid = outcome.validation.valid; issues = outcome.validation.issues
            if deadline_at is not None and clock()>=deadline_at:
                valid=False;issues=('PM_NOT_REVIEWED_TIME_BUDGET',)
        except ValueError as exc:
            # No hidden retry or model-chosen defaults. A failed/uncertain call
            # produces no slot or new entry permission. Storage/binding errors
            # remain errors instead of being mistaken for model degradation.
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED', 'TEAM_MODEL_RESULT_INVALID'}: raise
            issues = (str(exc),)
    # Raw provider JSON and its captured journal remain unchanged. This separate
    # source-link projection is not a fabricated opposing-evidence judgment.
    derived=project_pm_compact(output,values['EVIDENCE_CATALOG'],plan_refs=plan_refs,
        **validator_args)[0] if valid else dict(slot_plan=[],derived_shortfall='MODEL_INVALID')
    decision = {'status': 'READY' if valid else 'PM_DEGRADED_NO_ALLOCATION',
        'session': 'PM_PREMARKET', 'trade_date': trade_date, 'source_call_id': call_id,
        'input_hash': request.input_hash, 'reason_codes': list(issues),
        'deployment_target': output['deployment_target'] if valid else None,
        'slot_plan': derived['slot_plan'],
        'plan_ranking': derived['plan_ranking'] if valid else [],
        'derived_shortfall': derived['derived_shortfall'],
        'allocation_plan_bindings': {row.plan_id:[row.symbol,plan_clusters[row.plan_id]] for row in envelopes} if valid else {},
        'plan_allocations': derived['plan_allocations'] if valid else [],
        'shortfall_reason': output['shortfall_reason'] if valid else 'ENVELOPE_TIGHTENED',
        'handoff_notes': output['handoff_notes'] if valid else {},
        'what_would_change_my_mind': output['what_would_change_my_mind'] if valid else []}
    decision['model_validated']=bool(valid)
    decision['model_compact_output']=output if valid else None
    decision['input_projection']=input_projection
    decision['shortlist_audit']=shortlist
    decision['server_projection']={'schema':'PM_COMPACT_SOURCE_LINK_PROJECTION/1',
        'note_normalization':project_pm_notes(output)[1],
        'source_link_authority':'FROZEN_PLAN_REFERENCE_NOT_MODEL_SUPPORTING_OR_OPPOSING_JUDGMENT',
        'raw_output_sha256':hashlib.sha256(canonical(output).encode()).hexdigest(),
        'derived_output_sha256':hashlib.sha256(canonical(derived).encode()).hexdigest() if valid else None}
    decision['decision_authority']='VALIDATED_MODEL' if valid else 'NO_ALLOCATION'
    if deadline_at is not None:
        decision['deadline_audit']=dict(deadline_at=deadline_at.isoformat(),timeout_seconds=timeout_seconds,
            observed_at=clock().isoformat(),unreviewed_plan_ids=list(plan_refs.values()) if 'PM_NOT_REVIEWED_TIME_BUDGET' in issues else [])
    if 'PM_NOT_REVIEWED_TIME_BUDGET' in issues:
        from .team_v2_internal import InternalOutbox
        InternalOutbox(store).enqueue(account_id=account_id,trade_date=trade_date,kind='PM_NOT_REVIEWED_TIME_BUDGET',
            source_id=meeting_id,observed_at=clock(),priority=5,body=dict(msg_type='text',content=dict(
                text='晨会未审核候选：'+str(len(plan_refs))+'只；PM_NOT_REVIEWED_TIME_BUDGET；不分配未审计划。')))
    if not valid and 'PM_NOT_REVIEWED_TIME_BUDGET' not in issues and fallback_enabled and fallback_priority_order:
        fallback=derive_pm_fallback(envelopes=envelopes,plan_clusters=plan_clusters,
            priority=fallback_priority_order,free_slots=free_slots,max_new_entries=max_new_entries,
            a6_validated=a6_validated,utilization_cap=utilization_cap,
            a6_degraded_conservative=a6_degraded_conservative)
        decision['reason_codes']=list(dict.fromkeys([*issues,fallback['reason_code']]))
        if fallback['slot_plan']:
            decision.update(status='READY',decision_authority='SERVER_FALLBACK_NOT_MODEL_VALIDATED',
                fallback_envelope_authority=('VALIDATED_A6' if a6_validated else 'SERVER_A6_DEGRADED_CONSERVATIVE'),
                fallback_observed_at=now.isoformat(),slot_plan=fallback['slot_plan'],
                plan_allocations=fallback['plan_allocations'],plan_ranking=[],
                derived_shortfall='PM_UNAVAILABLE_SERVER_FALLBACK',
                allocation_plan_bindings={e.plan_id:[e.symbol,plan_clusters[e.plan_id]] for e in envelopes},
                deployment_target=dict(target_pct=str(min(baseline_range[1],utilization_cap)),
                    baseline_range=[str(x) for x in baseline_range],
                    basis='SERVER_FALLBACK_OBJECTIVE_NOT_ENTRY_PERMISSION',supporting_refs=[],opposing_refs=[]))
    decision=store.record_pm(meeting_id=meeting_id, source_call_id=call_id, account_id=account_id,
        trade_date=trade_date, input_sha256=binding, result=decision)
    _notify_fallback(store,decision,account_id=account_id,trade_date=trade_date)
    return decision
