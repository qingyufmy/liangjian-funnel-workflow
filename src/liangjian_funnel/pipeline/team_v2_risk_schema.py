"""Claude-authored A6 PREMARKET JSON, exact server scope and no loosening.

Other sessions are not accepted as a morning review. This grants no orders.
"""
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import json
import re

from .team_v2_contract import TeamValidation, validate_team_output
from ..runtime.team_v2_risk_meeting import review_overnight

ROOT=frozenset('envelope schema_version session envelope_adjustments utilization_cap exception_actions '
    'overnight_reviews capacity_for_tomorrow risk_event_attribution handoff_notes '
    'what_would_change_my_mind unresolved_questions'.split())
REFS={'supporting_refs','opposing_refs'}
OPTIONAL={'opposing_search_scope'}


def text(value):return isinstance(value,str) and bool(value.strip()) and len(value)<=2000


def texts(value,limit=2):return isinstance(value,list) and 1<=len(value)<=limit and all(text(v) for v in value)


def number(value):
    if isinstance(value,bool):return None
    try:result=Decimal(str(value))
    except (ValueError,InvalidOperation):return None
    return result if result.is_finite() else None


def item(row,keys):return isinstance(row,Mapping) and keys.issubset(row) and set(row)<=keys|OPTIONAL


def validate_risk_premarket(output,catalog,*,required_envelope,envelopes,engine_utilization_cap,
        capacity_slots,cluster_caps,unready_plan_ids,cluster_plan_counts=None):
    by_id={e.plan_id:e for e in envelopes};cap=number(engine_utilization_cap)
    if (len(by_id)!=len(envelopes) or cap is None or not 0<=cap<=100
            or type(capacity_slots) is not int or capacity_slots<0 or not isinstance(cluster_caps,Mapping)
            or any(not text(k) or type(v) is not int or v<0 for k,v in cluster_caps.items())
            or not isinstance(unready_plan_ids,(set,frozenset)) or not unready_plan_ids.issubset(by_id)):
        raise ValueError('A6_SERVER_CONTEXT_INVALID')
    if cluster_plan_counts is not None:
        if (not isinstance(cluster_plan_counts,Mapping) or set(cluster_plan_counts)!=set(cluster_caps)
                or any(type(value) is not int or value<0 for value in cluster_plan_counts.values())
                or sum(cluster_plan_counts.values())!=len(by_id)):
            raise ValueError('A6_SERVER_CONTEXT_INVALID')
        cluster_caps={key:min(value,cluster_plan_counts[key]) for key,value in cluster_caps.items()}
        capacity_slots=min(capacity_slots,sum(cluster_caps.values()))
    common=validate_team_output('A6',output,catalog,session='PREMARKET')
    issues=list(common.issues)
    def issue(code):issues.append('A6_'+code)
    if not isinstance(output,Mapping):return common
    if set(output)!=ROOT:issue('ROOT_FIELDS_INVALID')
    try:
        bound=json.dumps(output.get('envelope'),sort_keys=True,allow_nan=False)==json.dumps(required_envelope,sort_keys=True,allow_nan=False)
    except (ValueError,TypeError):bound=False
    if not bound:issue('ENVELOPE_BINDING_MISMATCH')
    if output.get('schema_version')!='a6-risk/1.0.0':issue('SCHEMA_VERSION_INVALID')
    notes=output.get('handoff_notes')
    if not isinstance(notes,Mapping) or set(notes)!={'to_a3','to_a4','to_a5'}:issue('HANDOFF_RECIPIENT_INVALID')
    adjustments=output.get('envelope_adjustments');seen=[]
    if not isinstance(adjustments,list):issue('ENVELOPE_DOMAIN_INVALID');adjustments=[]
    for row in adjustments:
        if (not isinstance(row,Mapping) or not item({k:v for k,v in row.items() if k!='gap_review'},
                {'plan_id','adjustment','reason_code','what_would_change_my_mind'}|REFS)):
            issue('ADJUSTMENT_FIELDS_INVALID');continue
        if 'gap_review' in row and (not isinstance(row['gap_review'],str) or row['gap_review'] not in {'KEEP_WITH_GAP','DOWNGRADE','REJECT'}):
            issue('GAP_REVIEW_ENUM_INVALID')
        pid=row['plan_id'];action=row['adjustment']
        if not isinstance(pid,str) or pid not in by_id:issue('ENVELOPE_DOMAIN_INVALID');continue
        seen.append(pid)
        if (not isinstance(action,str) or action not in {'KEEP','TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND'}
                or not isinstance(row['reason_code'],str) or not re.fullmatch('[A-Z][A-Z0-9_]{1,79}',row['reason_code'])
                or not texts(row['what_would_change_my_mind'])):
            issue('ADJUSTMENT_INVALID')
            if not isinstance(action,str) or action not in {'KEEP','TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND'}:
                issue('ADJUSTMENT_DETAIL:'+pid+':adjustment:ENUM')
            if not isinstance(row['reason_code'],str) or not re.fullmatch('[A-Z][A-Z0-9_]{1,79}',row['reason_code']):
                issue('ADJUSTMENT_DETAIL:'+pid+':reason_code:REGEX_2_TO_80')
            if not texts(row['what_would_change_my_mind']):
                issue('ADJUSTMENT_DETAIL:'+pid+':what_would_change_my_mind:NONEMPTY_MAX2_TEXT')
        if pid in unready_plan_ids and action=='KEEP':issue('MISSING_DATA_KEEP_FORBIDDEN')
    if len(seen)!=len(set(seen)) or set(seen)!=set(by_id):issue('ENVELOPE_DOMAIN_INVALID')
    utilization=output.get('utilization_cap')
    if not item(utilization,{'cap_pct','basis'}|REFS):issue('UTILIZATION_CAP_INVALID')
    else:
        amount=number(utilization['cap_pct'])
        if amount is None or not 0<=amount<=cap or not text(utilization['basis']):issue('UTILIZATION_CAP_INVALID')
    # Unified authored root retains the inactive session arrays, but none may
    # smuggle an exception/overnight action into the premarket meeting.
    for key in ('exception_actions','overnight_reviews','risk_event_attribution'):
        if output.get(key)!=[]:issue('INACTIVE_SESSION_ACTIONS')
    capacity=output.get('capacity_for_tomorrow')
    if not item(capacity,{'plans_acceptable','cluster_quota','risk_unit','response_to_pm','notes'}|REFS):
        issue('CAPACITY_INVALID')
        issue('CAPACITY_DETAIL:fields:EXACT_SCHEMA')
    else:
        slots=capacity['plans_acceptable'];quota=capacity['cluster_quota']
        if (type(slots) is not int or not 0<=slots<=capacity_slots
                or not isinstance(quota,Mapping) or not set(quota).issubset(cluster_caps)
                or any(type(v) is not int or not 0<=v<=cluster_caps[k] for k,v in quota.items())
                or (isinstance(quota,Mapping) and all(type(v) is int for v in quota.values())
                    and type(slots) is int and sum(quota.values())>slots)
                or not isinstance(capacity['risk_unit'],str) or capacity['risk_unit'] not in {'STANDARD','PROBE','NONE'}
                or not text(capacity['response_to_pm']) or not texts(capacity['notes'],3)):
            issue('CAPACITY_INVALID')
            if type(slots) is not int or not 0<=slots<=capacity_slots:
                issue('CAPACITY_DETAIL:plans_acceptable:MAX_'+str(capacity_slots))
            if not texts(capacity['notes'],3):issue('CAPACITY_DETAIL:notes:NONEMPTY_MAX3_TEXT')
            if not text(capacity['response_to_pm']):issue('CAPACITY_DETAIL:response_to_pm:NONEMPTY_MAX2000')
            if not isinstance(quota,Mapping):issue('CAPACITY_DETAIL:cluster_quota:MAPPING')
            else:
                for key,value in quota.items():
                    if key not in cluster_caps:issue('CAPACITY_DETAIL:cluster_quota:OUTSIDE_DOMAIN')
                    elif type(value) is not int or not 0<=value<=cluster_caps[key]:
                        issue('CAPACITY_DETAIL:cluster_quota:'+key+':MAX_'+str(cluster_caps[key]))
                if type(slots) is int and all(type(v) is int for v in quota.values()) and sum(quota.values())>slots:
                    issue('CAPACITY_DETAIL:cluster_quota:SUM_GT_PLANS_ACCEPTABLE')
            if not isinstance(capacity['risk_unit'],str) or capacity['risk_unit'] not in {'STANDARD','PROBE','NONE'}:
                issue('CAPACITY_DETAIL:risk_unit:ENUM')
    unresolved=output.get('unresolved_questions')
    if not isinstance(unresolved,list) or any(not text(v) for v in unresolved):issue('UNRESOLVED_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)


def validate_risk_post_close(output,catalog,*,required_envelope,pm_stances,
        engine_utilization_cap,capacity_slots,cluster_caps,event_ids):
    """Exact PM holdings and original risk events; no invented exit or capacity."""
    # The same bounded unified fields are used by all A6 sessions. Validate the
    # inactive morning domain separately, without treating POST_CLOSE as morning.
    cap=number(engine_utilization_cap)
    if (cap is None or not 0<=cap<=100 or type(capacity_slots) is not int or capacity_slots<0
            or not isinstance(cluster_caps,Mapping)
            or any(not text(k) or type(v) is not int or v<0 for k,v in cluster_caps.items())
            or not isinstance(event_ids,(set,frozenset)) or any(not text(v) for v in event_ids)):
        raise ValueError('A6_SERVER_CONTEXT_INVALID')
    review_overnight(pm_stances,[dict(symbol=s,pm_stance=v,review='AGREE') for s,v in pm_stances.items()])
    common=validate_team_output('A6',output,catalog,session='POST_CLOSE')
    issues=list(common.issues)
    def issue(code):issues.append('A6_'+code)
    if not isinstance(output,Mapping):return common
    if set(output)!=ROOT:issue('ROOT_FIELDS_INVALID')
    try:
        bound=json.dumps(output.get('envelope'),sort_keys=True,allow_nan=False)==json.dumps(required_envelope,sort_keys=True,allow_nan=False)
    except (ValueError,TypeError):bound=False
    if not bound:issue('ENVELOPE_BINDING_MISMATCH')
    if output.get('schema_version')!='a6-risk/1.0.0':issue('SCHEMA_VERSION_INVALID')
    notes=output.get('handoff_notes')
    if not isinstance(notes,Mapping) or set(notes)!={'to_a3','to_a4','to_a5'}:issue('HANDOFF_RECIPIENT_INVALID')
    for key in ('exception_actions','envelope_adjustments'):
        if output.get(key)!=[]:issue('INACTIVE_SESSION_ACTIONS')
    utilization=output.get('utilization_cap')
    if not item(utilization,{'cap_pct','basis'}|REFS):issue('UTILIZATION_CAP_INVALID')
    else:
        value=number(utilization['cap_pct'])
        if value is None or not 0<=value<=cap or not text(utilization['basis']):issue('UTILIZATION_CAP_INVALID')
    capacity=output.get('capacity_for_tomorrow')
    if not item(capacity,{'plans_acceptable','cluster_quota','risk_unit','response_to_pm','notes'}|REFS):issue('CAPACITY_INVALID')
    else:
        slots=capacity['plans_acceptable'];quota=capacity['cluster_quota']
        if (type(slots) is not int or not 0<=slots<=capacity_slots
                or not isinstance(quota,Mapping) or not set(quota).issubset(cluster_caps)
                or any(type(v) is not int or not 0<=v<=cluster_caps[k] for k,v in quota.items())
                or (isinstance(quota,Mapping) and all(type(v) is int for v in quota.values())
                    and type(slots) is int and sum(quota.values())>slots)
                or capacity['risk_unit'] not in ('STANDARD','PROBE','NONE')
                or not text(capacity['response_to_pm']) or not texts(capacity['notes'],3)):
            issue('CAPACITY_INVALID')
    rows=output.get('overnight_reviews')
    if not isinstance(rows,list):issue('OVERNIGHT_DOMAIN_INVALID');rows=[]
    for row in rows:
        if (not item(row,{'symbol','pm_stance','review','reason_code'}|REFS)
                or not text(row['reason_code'])):issue('OVERNIGHT_FIELDS_INVALID')
    try:review_overnight(pm_stances,rows)
    except (ValueError,TypeError):issue('OVERNIGHT_DOMAIN_OR_LOOSENING_INVALID')
    events=output.get('risk_event_attribution');seen=[]
    if not isinstance(events,list):issue('EVENT_DOMAIN_INVALID');events=[]
    for row in events:
        if not item(row,{'event_id','verdict','note'}|REFS):issue('EVENT_FIELDS_INVALID');continue
        if not isinstance(row['event_id'],str) or row['event_id'] not in event_ids:issue('EVENT_DOMAIN_INVALID')
        else:seen.append(row['event_id'])
        if row['verdict'] not in ('RULE_CORRECT','RULE_TOO_TIGHT','DATA_ISSUE','PENDING') or not text(row['note']):issue('EVENT_VERDICT_INVALID')
    if len(seen)!=len(set(seen)) or set(seen)!=event_ids:issue('EVENT_DOMAIN_INVALID')
    unresolved=output.get('unresolved_questions')
    if not isinstance(unresolved,list) or any(not text(v) for v in unresolved):issue('UNRESOLVED_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)


def exception_domain(targets):
    """Server-owned trigger facts, not objects or release permission from JSON."""
    if not isinstance(targets,(list,tuple)) or not targets:
        raise ValueError('A6_EXCEPTION_CONTEXT_INVALID')
    domain={}
    for row in targets:
        if (not isinstance(row,Mapping) or set(row)!={'target','target_id','trigger_cleared','data_ready'}
                or row['target'] not in ('symbol','cluster','account') or not text(row['target_id'])
                or type(row['trigger_cleared']) is not bool or type(row['data_ready']) is not bool):
            raise ValueError('A6_EXCEPTION_CONTEXT_INVALID')
        key=(row['target'],row['target_id'])
        if key in domain:raise ValueError('A6_EXCEPTION_CONTEXT_INVALID')
        domain[key]=row
    return domain


def validate_risk_exception(output,catalog,*,required_envelope,targets,resume_allowed,hard_halt,
        engine_utilization_cap,capacity_slots,cluster_caps):
    domain=exception_domain(targets);cap=number(engine_utilization_cap)
    if (not isinstance(resume_allowed,(set,frozenset))
            or any(not isinstance(k,tuple) or len(k)!=2 or k[0] not in ('symbol','cluster','account')
                or not text(k[1]) for k in resume_allowed) or type(hard_halt) is not bool
            or cap is None or not 0<=cap<=100 or type(capacity_slots) is not int or capacity_slots<0
            or not isinstance(cluster_caps,Mapping)
            or any(not text(k) or type(v) is not int or v<0 for k,v in cluster_caps.items())):
        raise ValueError('A6_SERVER_CONTEXT_INVALID')
    common=validate_team_output('A6',output,catalog,session='INTRADAY_EXCEPTION')
    issues=list(common.issues)
    def issue(code):issues.append('A6_'+code)
    if not isinstance(output,Mapping):return common
    if set(output)!=ROOT:issue('ROOT_FIELDS_INVALID')
    try:
        bound=json.dumps(output.get('envelope'),sort_keys=True,allow_nan=False)==json.dumps(required_envelope,sort_keys=True,allow_nan=False)
    except (ValueError,TypeError):bound=False
    if not bound:issue('ENVELOPE_BINDING_MISMATCH')
    if output.get('schema_version')!='a6-risk/1.0.0':issue('SCHEMA_VERSION_INVALID')
    notes=output.get('handoff_notes')
    if not isinstance(notes,Mapping) or set(notes)!={'to_a3','to_a4','to_a5'}:issue('HANDOFF_RECIPIENT_INVALID')
    for key in ('envelope_adjustments','overnight_reviews','risk_event_attribution'):
        if output.get(key)!=[]:issue('INACTIVE_SESSION_ACTIONS')
    rows=output.get('exception_actions');seen=[]
    if not isinstance(rows,list):issue('EXCEPTION_DOMAIN_INVALID');rows=[]
    for row in rows:
        if not item(row,{'target','target_id','action','reason_code','release_condition','what_would_change_my_mind'}|REFS):
            issue('EXCEPTION_FIELDS_INVALID');continue
        if not isinstance(row['target'],str) or not isinstance(row['target_id'],str):
            issue('EXCEPTION_DOMAIN_INVALID');continue
        key=(row['target'],row['target_id'])
        if key not in domain:issue('EXCEPTION_DOMAIN_INVALID');continue
        seen.append(key);action=row['action'];source=domain[key]
        if (not isinstance(action,str) or action not in {'REDUCE_HALF','EXIT','HALT_NEW_ENTRY','RESUME','NO_ACTION'}
                or not isinstance(row['reason_code'],str) or not re.fullmatch('[A-Z][A-Z0-9_]{1,79}',row['reason_code'])
                or not text(row['release_condition']) or not texts(row['what_would_change_my_mind'])):
            issue('EXCEPTION_ACTION_INVALID')
        if action=='RESUME' and (hard_halt or key not in resume_allowed
                or not source['trigger_cleared'] or not source['data_ready']):issue('RESUME_FORBIDDEN')
        if not source['data_ready'] and action in ('NO_ACTION','RESUME'):issue('MISSING_DATA_KEEP_FORBIDDEN')
    if len(seen)!=len(set(seen)) or set(seen)!=set(domain):issue('EXCEPTION_DOMAIN_INVALID')
    utilization=output.get('utilization_cap')
    if not item(utilization,{'cap_pct','basis'}|REFS):issue('UTILIZATION_CAP_INVALID')
    else:
        value=number(utilization['cap_pct'])
        if value is None or not 0<=value<=cap or not text(utilization['basis']):issue('UTILIZATION_CAP_INVALID')
    capacity=output.get('capacity_for_tomorrow')
    if not item(capacity,{'plans_acceptable','cluster_quota','risk_unit','response_to_pm','notes'}|REFS):issue('CAPACITY_INVALID')
    else:
        slots=capacity['plans_acceptable'];quota=capacity['cluster_quota']
        if (type(slots) is not int or not 0<=slots<=capacity_slots
                or not isinstance(quota,Mapping) or not set(quota).issubset(cluster_caps)
                or any(type(v) is not int or not 0<=v<=cluster_caps[k] for k,v in quota.items())
                or (isinstance(quota,Mapping) and all(type(v) is int for v in quota.values())
                    and type(slots) is int and sum(quota.values())>slots)
                or capacity['risk_unit'] not in ('STANDARD','PROBE','NONE')
                or not text(capacity['response_to_pm']) or not texts(capacity['notes'],3)):issue('CAPACITY_INVALID')
    unresolved=output.get('unresolved_questions')
    if not isinstance(unresolved,list) or any(not text(v) for v in unresolved):issue('UNRESOLVED_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)
