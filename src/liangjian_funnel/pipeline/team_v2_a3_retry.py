"""Concrete, last-user-message A3 schema correction; never engine verdicts."""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import MappingProxyType

from .team_v2_plan_review_schema import BASE,PASS,OPTIONAL,ROOT,validate_plan_review


def _input_hash(messages):
    return hashlib.sha256(json.dumps([dict(m) for m in messages],ensure_ascii=False,
        sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def a3_validation_feedback(raw,catalog,issues,*,required_envelope,candidate_domain):
    source=raw if isinstance(raw,Mapping) else {}
    rows=[]
    for pool in ('core_watch_pool','secondary_watch_pool','rejected_candidates'):
        values=source.get(pool)
        if not isinstance(values,list):continue
        for index,row in enumerate(values):
            if not isinstance(row,Mapping):
                rows.append(dict(pool=pool,index=index,actual=row,legal='object'))
                continue
            symbol=row.get('symbol');domain=candidate_domain.get(symbol) if isinstance(symbol,str) else None
            local=deepcopy(dict(source));item=deepcopy(dict(row))
            if pool=='core_watch_pool' and type(item.get('plan_priority_rank')) is int and item['plan_priority_rank']>0:
                item['plan_priority_rank']=1
            local.update(core_watch_pool=[],secondary_watch_pool=[],rejected_candidates=[])
            local[pool]=[item]
            checked=validate_plan_review(local,catalog,required_envelope=required_envelope,
                candidate_domain={symbol:domain} if domain else candidate_domain)
            # Keep all genuinely offending rows, without labeling a valid row
            # wrong merely because a separate global root field was omitted.
            row_codes=[code for code in checked.issues if code not in {
                'A3_PLAN_ROOT_FIELDS_INVALID','A3_PLAN_ENVELOPE_BINDING_MISMATCH',
                'A3_PLAN_HANDOFF_RECIPIENT_INVALID','A3_PLAN_UNRESOLVED_QUESTIONS_INVALID'}]
            if not row_codes:continue
            legal=dict(symbol=symbol if domain else sorted(candidate_domain),
                strategy_profile=domain['strategy_profile'] if domain else None,
                cluster=domain['cluster'] if domain else None,
                required_fields=sorted(PASS if pool=='core_watch_pool' else BASE),optional_fields=sorted(OPTIONAL),
                review_status=['PASS'] if pool=='core_watch_pool' else ['VETO','DATA_GAP'],
                risk_unit=(['PROBE'] if domain and domain['risk_unit']=='PROBE' else ['STANDARD','PROBE'])
                    if pool=='core_watch_pool' else ['NO_ENTRY'],
                trade_brief_fields=['likely_failure_path','key_levels_note','confirm_cadence','note_to_trader'],
                confirm_cadence=['5m','15m'],item_falsifier_count=[1,2])
            rows.append(dict(pool=pool,index=index,symbol=symbol,validation_errors=row_codes,
                actual_fields=deepcopy(dict(row)),legal_fields=legal))
    return dict(validation_errors=list(issues),row_violations=rows,
        observed_root_fields=sorted(source),observed_envelope=deepcopy(source.get('envelope')),
        observed_session=source.get('session'),observed_handoff_notes=deepcopy(source.get('handoff_notes')),
        required_envelope=deepcopy(required_envelope),required_root_fields=sorted(ROOT),
        allowed_symbols=sorted(candidate_domain),evidence_reference_ids=sorted(item['id'] for item in catalog['items']),
        core_priority_rule='Core ranks must be unique and exactly 1..number_of_core_rows.',
        correction_scope='Return the complete corrected JSON. Keep frozen identity, source facts and risk caps unchanged; no invented strategy, cluster, prices or permission.')


def bind_a3_retry_feedback(request,feedback,*,correction_attempt):
    if (request.prompt.role!='A3' or correction_attempt not in (2,3) or type(correction_attempt) is not int
            or not isinstance(feedback,Mapping) or not feedback.get('validation_errors')):
        raise ValueError('TEAM_A3_RETRY_FEEDBACK_SCOPE_INVALID')
    if (len(request.messages)!=2 or request.messages[0].get('role')!='system'
            or request.messages[1].get('role')!='user' or _input_hash(request.messages)!=request.input_hash
            or hashlib.sha256(request.messages[0]['content'].encode()).hexdigest()!=request.prompt.sha256):
        raise ValueError('TEAM_MESSAGE_BINDING_MISMATCH')
    content=request.messages[1]['content']
    if not content.startswith('RUNTIME_INPUT\n'):raise ValueError('TEAM_A3_RETRY_FEEDBACK_SCOPE_INVALID')
    runtime=json.loads(content.removeprefix('RUNTIME_INPUT\n'))
    if (runtime.get('session')!='PLAN_REVIEW' or runtime.get('required_envelope',{}).get('stage_id')!='A3'
            or {'validation_feedback','correction_attempt'}&set(runtime)):
        raise ValueError('TEAM_A3_RETRY_FEEDBACK_SCOPE_INVALID')
    runtime=deepcopy(runtime)
    runtime['validation_feedback']=deepcopy(dict(feedback));runtime['correction_attempt']=correction_attempt
    body='RUNTIME_INPUT\n'+json.dumps(runtime,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    messages=(request.messages[0],MappingProxyType(dict(role='user',content=body)))
    return replace(request,messages=messages,input_hash=_input_hash(messages))
