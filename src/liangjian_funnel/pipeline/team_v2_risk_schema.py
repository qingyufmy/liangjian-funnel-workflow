"""Claude-authored A6 PREMARKET JSON, exact server scope and no loosening.

Other sessions are not accepted as a morning review. This grants no orders.
"""
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import json
import re

from .team_v2_contract import TeamValidation, validate_team_output

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
        capacity_slots,cluster_caps,unready_plan_ids):
    by_id={e.plan_id:e for e in envelopes};cap=number(engine_utilization_cap)
    if (len(by_id)!=len(envelopes) or cap is None or not 0<=cap<=100
            or type(capacity_slots) is not int or capacity_slots<0 or not isinstance(cluster_caps,Mapping)
            or any(not text(k) or type(v) is not int or v<0 for k,v in cluster_caps.items())
            or not isinstance(unready_plan_ids,(set,frozenset)) or not unready_plan_ids.issubset(by_id)):
        raise ValueError('A6_SERVER_CONTEXT_INVALID')
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
        if not item(row,{'plan_id','adjustment','reason_code','what_would_change_my_mind'}|REFS):
            issue('ADJUSTMENT_FIELDS_INVALID');continue
        pid=row['plan_id'];action=row['adjustment']
        if not isinstance(pid,str) or pid not in by_id:issue('ENVELOPE_DOMAIN_INVALID');continue
        seen.append(pid)
        if (not isinstance(action,str) or action not in {'KEEP','TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND'}
                or not isinstance(row['reason_code'],str) or not re.fullmatch('[A-Z][A-Z0-9_]{1,79}',row['reason_code'])
                or not texts(row['what_would_change_my_mind'])):
            issue('ADJUSTMENT_INVALID')
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
    unresolved=output.get('unresolved_questions')
    if not isinstance(unresolved,list) or any(not text(v) for v in unresolved):issue('UNRESOLVED_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)
