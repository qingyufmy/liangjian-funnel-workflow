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
from ..pipeline.team_v2_pm_schema import PM_PRIMARY_CLUSTER_CAP
from ..pipeline.team_v2_cluster_quota import checked_cluster_quota_context,normalize_cluster_quota
from .team_v2_risk_policy import project_risk_values
from .regeneration_data_limits import a6_gap_requirements, gap_reviewed_ids, restrict_unreviewed
from ..pipeline.team_v2_prompts import with_runtime_contract
from ..pipeline.team_v2_contract import TeamValidation


def run_risk_premarket(*,repository,client,journal,store,meeting_id,call_id,account_id,run_id,model,
        trade_date,source_trade_date,clock,required_envelope,replacements,envelopes,
        engine_utilization_cap,capacity_slots,cluster_caps,unready_plan_ids,limits,
        timeout_seconds,max_output_tokens,allow_model_call=False,quota_context=None):
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
    if quota_context is not None:
        quota_context=checked_cluster_quota_context(quota_context)
        if (quota_context['engine_slots']!=capacity_slots or quota_context['cluster_caps']!=dict(cluster_caps)
                or set(quota_context['plan_clusters'])!={e.plan_id for e in envelopes}):
            raise ValueError('A6_CLUSTER_QUOTA_SERVER_CONTEXT_MISMATCH')
        validator_args['cluster_plan_counts']=quota_context['cluster_plan_counts']
    values=dict(replacements)
    context=dict(account_id=account_id,call_id=call_id,engine_utilization_cap=engine_utilization_cap,
        capacity_slots=capacity_slots,cluster_caps=dict(cluster_caps),unready_plan_ids=sorted(unready_plan_ids),
        timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)
    if quota_context is not None:context['server_cluster_capacity']=quota_context
    context_hash=hashlib.sha256(canonical(context).encode()).hexdigest()
    values.update(RISK_SESSION='PREMARKET',ENGINE_ENVELOPES=[asdict(e) for e in envelopes],
        RISK_LIMITS=dict(policy=asdict(limits),engine_upper_bounds=context,session_binding_sha256=context_hash))
    market=dict(values.get('MARKET_STATE') or {})
    regime=market.get('market_regime') or required_envelope.get('market_regime')
    market['allowed_risk_units']=([] if regime in {'ICE_POINT','ICE_POINT_NO_NEW_ENTRY'} else
        ['PROBE'] if regime in {'BEAR_RISK','RISK_OFF_RETREAT'} else ['PROBE','STANDARD'])
    values['MARKET_STATE']=market
    values['RISK_LIMITS']['primary_cluster_cap']=PM_PRIMARY_CLUSTER_CAP
    if quota_context is not None:values['RISK_LIMITS']['server_cluster_capacity']=quota_context
    values=json.loads(canonical(values))
    gap_requirements=a6_gap_requirements(values['EVIDENCE_CATALOG'],envelopes)
    original_values=values
    values=project_risk_values(values)
    validate_risk_premarket(None,values['EVIDENCE_CATALOG'],**validator_args)
    original_request=repository.prepare_messages('A6',original_values,required_envelope=required_envelope,session='PREMARKET')
    request=repository.prepare_messages('A6',values,required_envelope=required_envelope,session='PREMARKET')
    if gap_requirements:
        request=with_runtime_contract(request,dict(authority='0498_REGENERATION_A1_SOURCE_GAPS',
            data_limited_plan_reviews=gap_requirements, maximum_risk_unit='PROBE',
            required_adjustment_reason_code='DATA_LIMITED_REVIEWED',
            review_instruction='For EACH listed plan, explicitly review EACH exact reason_code in '
                'what_would_change_my_mind and cite its plan_ref in supporting_refs. '
                'Suspend if insufficient; missing reviews cannot use conservative fallback.'))
    def validate(raw):
        checked=validate_risk_premarket(raw,values['EVIDENCE_CATALOG'],**validator_args)
        missing=set(gap_requirements)-gap_reviewed_ids(raw,gap_requirements)
        issues=(*checked.issues,*(f'A6_DATA_LIMITED_REVIEW_REQUIRED:{pid}' for pid in sorted(missing)))
        return TeamValidation(not issues,issues,checked.judgments,checked.sentinel_count)
    binding=hashlib.sha256(canonical(dict(input_hash=request.input_hash,run_id=run_id,model=model,
        trade_date=trade_date,source_trade_date=source_trade_date,context_hash=context_hash)).encode()).hexdigest()
    original_observed=store.claim_risk_session(meeting_id,input_sha256=binding,observed_at=now)
    previous=store.load_risk_session(meeting_id,input_sha256=binding)
    if previous is not None:return previous
    store.freeze_control_context('a6-input-projection:'+meeting_id,source_sha256=request.input_hash,
        body=dict(original_input_hash=original_request.input_hash,projected_input_hash=request.input_hash,
            original_chars=sum(len(m['content']) for m in original_request.messages),
            projected_chars=sum(len(m['content']) for m in request.messages),
            catalogue_sha256=hashlib.sha256(canonical(values['EVIDENCE_CATALOG']).encode()).hexdigest(),
            plan_refs=values['PLANS']['plan_evidence_ids'],all_resolved_values_equal=True,
            authority='UNCHANGED_FULL_CATALOG_DUPLICATE_PLANS_REFS_ONLY'))
    expected=dict(run_id=run_id,role='A6',session='PREMARKET',source_trade_date=source_trade_date,
        execution_trade_date=trade_date,model=model,prompt_hash=request.prompt.sha256,
        input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
    captured=journal.load_captured(call_id,expected=expected)
    output=None;valid=False;provider_issues=()
    if captured is not None:
        output=captured['output']
        checked=validate(output)
        valid=captured['validated'] and checked.valid
        provider_issues=tuple(dict.fromkeys((*captured['issues'],*checked.issues)))
    elif journal.load_failed(call_id,request_binding=team_call_binding(request,run_id=run_id,model=model,
            session='PREMARKET',timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens)) is None:
        try:
            outcome=call_team_model(request,client,journal,call_id=call_id,run_id=run_id,model=model,
                session='PREMARKET',source_trade_date=source_trade_date,execution_trade_date=trade_date,
                catalog=values['EVIDENCE_CATALOG'],timeout_seconds=timeout_seconds,
                max_output_tokens=max_output_tokens,validator=validate,clock=clock,allow_model_call=True)
            output=outcome.response.output;valid=outcome.validation.valid
            provider_issues=outcome.validation.issues
        except ValueError as exc:
            if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
    if quota_context is not None and output is not None:
        projection=normalize_cluster_quota(output,quota_context)
        if projection is not None:
            effective,audit=projection
            checked=validate(effective)
            # Only capacity arithmetic is repairable. In particular a prior
            # validator exception is not a quota vote, even if revalidation of
            # the derived JSON happens to succeed after restart.
            quota_issues_only=all(issue=='A6_CAPACITY_INVALID' or issue.startswith(
                ('A6_CAPACITY_DETAIL:cluster_quota:','A6_CAPACITY_DETAIL:plans_acceptable:'))
                for issue in provider_issues)
            effective_valid=checked.valid and quota_issues_only
            sha=lambda value:hashlib.sha256(canonical(value).encode()).hexdigest()
            store.freeze_control_context('a6-quota-normalization:'+meeting_id,source_sha256=request.input_hash,
                body=dict(**audit,source_call_id=call_id,provider_validated=bool(valid),
                    provider_issues=list(provider_issues),provider_raw_sha256=sha(output),
                    effective_raw_sha256=sha(effective),effective_raw=effective,
                    effective_validation_issues=list(checked.issues),status='VALIDATED' if effective_valid else 'BLOCKED',
                    quota_context=quota_context))
            output=effective;valid=effective_valid
    if gap_requirements:
        reviewed=gap_reviewed_ids(output,gap_requirements) if valid else set()
        envelopes=restrict_unreviewed(envelopes,reviewed)
        store.freeze_control_context('a6-source-gap-review:'+meeting_id,source_sha256=request.input_hash,
            body=dict(authority='0498_REGENERATION_A1_SOURCE_GAPS',source_call_id=call_id,
                requirements=gap_requirements,provider_validated=bool(valid),
                reviewed_plan_ids=sorted(reviewed),blocked_plan_ids=sorted(set(gap_requirements)-reviewed)))
    return store.record_premarket(meeting_id=meeting_id,source_call_id=call_id,account_id=account_id,
        trade_date=trade_date,observed_at=original_observed,envelopes=envelopes,output=output,validated=valid,
        engine_utilization_cap=engine_utilization_cap)
