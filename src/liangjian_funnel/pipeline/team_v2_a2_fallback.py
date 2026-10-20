"""P0-9 deterministic A1/engine handoff when the A2 role is unavailable.

This is not a valid model reply. It never expands the frozen REVIEW_CANDIDATE
domain or creates a technical plan; A3 and publication keep all their gates.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
import hashlib,json


def _sha(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()


def build_a2_unavailable(*,failed_audit,decisions,a1_symbols,contexts,envelope,
                         snapshot_id,snapshot_hash,market_emotion,project_row):
    if failed_audit.status!='BLOCKED' or not isinstance(contexts,Mapping):return None
    if not isinstance(a1_symbols,(set,frozenset)) or not all(isinstance(s,str) for s in a1_symbols):return None
    by_symbol={r['symbol']:r for r in decisions if isinstance(r,Mapping) and isinstance(r.get('symbol'),str)
        and r.get('status')=='REVIEW_CANDIDATE'}
    scope=sorted(a1_symbols&set(by_symbol))
    if any(not isinstance(contexts.get(s),Mapping) or contexts[s].get('deterministic_status')!='REVIEW_CANDIDATE' for s in scope):return None
    stage=market_emotion.get('emotion_cycle_stage') if isinstance(market_emotion,Mapping) else None
    rows=[]
    for symbol in scope:
        row=project_row(by_symbol[symbol],'A2','WATCH_ONLY')
        row.update(A2_UNAVAILABLE=True,A2_REVIEW_DEGRADED=True,candidate_origin='WATCH_ONLY',risk_unit='PROBE',
            a2_judgment_authority='DETERMINISTIC_ENGINE_NOT_MODEL',theme_score=by_symbol[symbol].get('score'),
            theme_stage=stage,a2_engine_stage_source='MARKET_EMOTION_SNAPSHOT.emotion_cycle_stage' if stage is not None else 'ENGINE_STAGE_NOT_AVAILABLE')
        row['reason_codes']=list(dict.fromkeys([*row.get('reason_codes',[]),'A2_UNAVAILABLE']))
        rows.append(row)
    proof=dict(schema_version='a2-unavailable-engine-handoff/1',snapshot_id=snapshot_id,snapshot_hash=snapshot_hash,
        a1_qualified_symbols=sorted(a1_symbols),engine_review_candidate_symbols=sorted(by_symbol),allowed_scope=scope,
        failed_stage_status=failed_audit.status,failed_input_hash=failed_audit.input_hash,
        failed_prompt_hash=failed_audit.prompt_hash,failed_reason_codes=list(failed_audit.reason_codes),
        model_review_passed=False,source='A1_QUALIFIED_INTERSECT_ENGINE_REVIEW_CANDIDATE',risk_unit_cap='PROBE')
    proof['sha256']=_sha(proof)
    output=dict(envelope=dict(envelope,status='DEGRADED'),active_themes=[],focus_pool=[],watch_only_pool=rows,
        rotation_reviews=[],A2_REVIEW_DEGRADED=True,A2_UNAVAILABLE=True,A2_UNAVAILABLE_SCOPE=scope,
        A2_UNAVAILABLE_PROOF_SHA256=proof['sha256'],
        handoff_notes={'to_a3':['A2_UNAVAILABLE：仅消费A1合格且引擎REVIEW_CANDIDATE事实，A3独立技术审核；所有可执行计划上限PROBE，全部原有风控与发布过滤保持。']},
        analysis_summary={'review_status':'A2_UNAVAILABLE','decision_source':proof['source'],'model_review_passed':False})
    return output,proof


def proven_a2_unavailable(output,snapshot_data):
    if not isinstance(output,Mapping) or output.get('A2_UNAVAILABLE') is not True:return False
    proof=snapshot_data.get('A2_UNAVAILABLE_EVIDENCE')
    if not isinstance(proof,Mapping) or proof.get('schema_version')!='a2-unavailable-engine-handoff/1':return False
    body={k:v for k,v in proof.items() if k!='sha256'}
    if (proof.get('sha256')!=_sha(body) or proof.get('model_review_passed') is not False
            or proof.get('failed_stage_status')!='BLOCKED' or proof.get('risk_unit_cap')!='PROBE'
            or output.get('A2_UNAVAILABLE_PROOF_SHA256')!=proof['sha256']):return False
    a1=proof.get('a1_qualified_symbols');review=proof.get('engine_review_candidate_symbols');scope=proof.get('allowed_scope')
    if not all(isinstance(v,list) and all(isinstance(s,str) for s in v) for v in (a1,review,scope)):return False
    if scope!=sorted(set(a1)&set(review)) or output.get('A2_UNAVAILABLE_SCOPE')!=scope:return False
    contexts=snapshot_data.get('A2_BOTTLENECK_CONTEXT')
    return isinstance(contexts,Mapping) and all(isinstance(contexts.get(s),Mapping)
        and contexts[s].get('deterministic_status')=='REVIEW_CANDIDATE' for s in scope)


def notify_a2_unavailable(store,*,run_id,observed_at):
    from ..runtime.team_v2_internal import InternalOutbox
    if not isinstance(observed_at,datetime) or observed_at.utcoffset() is None:raise ValueError('A2_FALLBACK_CLOCK_INVALID')
    InternalOutbox(store).enqueue(account_id='local-paper',trade_date=observed_at.date().isoformat(),kind='ALERT',
        source_id='A2_UNAVAILABLE:'+run_id,observed_at=observed_at,priority=0,
        body={'msg_type':'text','content':{'text':'模拟盘｜A2_UNAVAILABLE：A2最终失败，A3仅使用A1合格∩引擎REVIEW_CANDIDATE继续独立技术审核。所有计划上限PROBE，原几何、行情和风控门不变。'}})
