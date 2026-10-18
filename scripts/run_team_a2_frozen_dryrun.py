"""Independent-stage A2 comparison against the exact original closed inputs.

Uses the original A1 upstream, current approved deterministic gate/Team adapter,
and actual capture clock. Never runs A1/A3, a workflow, publisher or collector.
"""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from run_team_a5_frozen_dryrun import model_settings,field_diff,save
TZ=ZoneInfo('Asia/Shanghai')


def source(sources,day):
    import ijson
    verification=json.loads((sources/'LOCAL_HASH_VERIFICATION.json').read_bytes())
    if verification['status']!='PASS':raise ValueError('DRYRUN_ORIGINAL_COPY_NOT_VERIFIED')
    records=json.loads((sources/'RECEIPT.json').read_bytes())['files']
    def find(kind,suffix):
        found=[r for r in records if r.get('source_kind')==kind and r['trade_date']==day and r['relative'].endswith(suffix)]
        if len(found)!=1:raise ValueError('DRYRUN_ORIGINAL_NOT_UNIQUE')
        proof=found[0];path=sources/'files'/proof['relative']
        if hashlib.file_digest(path.open('rb'),'sha256').hexdigest()!=proof['sha256']:raise ValueError('DRYRUN_ORIGINAL_HASH_INVALID')
        return path,proof
    path,snapshot_proof=find('snapshot','.json');snapshot=json.loads(path.read_bytes())
    verified=[r for r in verification['files'] if r['relative']==snapshot_proof['relative']]
    if len(verified)!=1 or verified[0].get('snapshot_sha256')!=snapshot['snapshot_hash']:
        raise ValueError('DRYRUN_SNAPSHOT_CANONICAL_PROOF_REQUIRED')
    path,lane_proof=find('research','lane_1.json')
    with path.open('rb') as f:stages=list(ijson.items(f,'stages.item',use_float=True))
    a1=[r for r in stages if r['stage']=='A1'];a2=[r for r in stages if r['stage']=='A2']
    if len(a1)!=1 or len(a2)!=1 or not a1[0].get('output'):raise ValueError('DRYRUN_ORIGINAL_UPSTREAM_REQUIRED')
    return snapshot,a1[0],a2[0],dict(snapshot=snapshot_proof,lane=lane_proof)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
    p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    settings,provider=model_settings(output,'vm-runtime')
    # Existing production metadata verified both research and review use this
    # route. Never silently select the library's different default model.
    if settings.review_model!='deepseek-v4-pro':raise ValueError('DRYRUN_RESEARCH_MODEL_REVERIFY_REQUIRED')
    settings=settings.model_copy(update={'source_config_path':ROOT/'config/funnel_config_v2.yaml','prompt_dir':ROOT/'prompts'})
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
    from liangjian_funnel.pipeline.research.common import screen_a2,_with_authorized_rotation_policy,_with_a2_bottleneck_context
    from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    original,a1,a2,proof=source(a.sources.resolve(),a.day)
    snapshot=FrozenInputSnapshot(original['snapshot_id'],original['data'],original['snapshot_hash'],datetime.fromisoformat(original['as_of']))
    snapshot=_with_authorized_rotation_policy(snapshot)
    gate=screen_a2(snapshot.data,a1['output'],minimum_identifiability_score=float(snapshot.data.get('MIN_IDENTIFIABILITY_SCORE') or 60),
        llm_top_n_per_theme=settings.a2_llm_top_n_per_theme,review_all_eligible=settings.a2_review_all_eligible,
        rotation_theme_count=int(snapshot.data.get('A2_ROTATION_THEME_COUNT') or 5))
    snapshot=_with_a2_bottleneck_context(snapshot,gate)
    adapter=configured_research_adapter(settings,calendar=ExchangeTradingCalendar())
    snapshot=adapter.prepare_a2_snapshot(snapshot,set(gate.review_symbols))
    run='team-a2-frozen-dryrun:'+a.day+':'+output.name
    header=dict(classification='POST_HOC_INDEPENDENT_STAGE_FROZEN_DRYRUN',actual_at=datetime.now(TZ).isoformat(),
        source_trade_date=a.day,context_time=original['as_of'],run_id=run,
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_proofs=proof,
        parent_snapshot_sha256=original['snapshot_hash'],derived_snapshot_sha256=snapshot.snapshot_hash,
        original_a1_status=a1['status'],original_a1_output_sha256=a1['output_hash'],
        evaluated_count=len(gate.decisions),review_symbols=sorted(gate.review_symbols),
        nomination_source=snapshot.data['_TEAM_A2_NOMINATION_FREEZE'],score_freeze=snapshot.data['_TEAM_A2_SCORE_FREEZE'],
        provider_identity=provider,production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',review_count=len(gate.review_symbols),nomination_count=len(snapshot.data['A2_QUANT_FILTERED_SAMPLE']))));return
    failures=[];http_calls=[]
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            http_calls.append(dict(actual_started_at=datetime.now(TZ).isoformat(),stage=kwargs.get('stage')))
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts))
                raise
    pipeline=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',model_client=AuditedClient(settings,max_attempts=1),
        now=lambda:datetime.now(TZ),enable_feature_store=False,team_research_adapter=adapter,output_dir=output/'research')
    # Full quant gate membership is frozen before role selection. No manual
    # stock slicing or successful old response can stand in for a new request.
    audit=pipeline._run_stage(lane_id='lane_1',model=settings.review_model,stage='A2',snapshot=snapshot,
        upstream_output=a1['output'],upstream_symbols=set(gate.review_symbols),projection_symbols=set(gate.review_symbols),
        bundle=pipeline.prompts.load(),run_id=run)
    # Preserve raw outputs separately; diagnostics name the exact frozen-domain
    # members rather than repairing model IDs or relaxing the validator.
    from contextlib import closing
    import sqlite3
    from liangjian_funnel.pipeline.team_v2_a2_schema import frozen_domain_diagnostics
    diagnostics=[]
    with closing(sqlite3.connect(adapter.journal.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        for call_id,raw,raw_hash in db.execute('SELECT call_id,output_json,output_hash FROM model_calls WHERE run_id=? ORDER BY captured_at,call_id',(run,)):
            from liangjian_funnel.pipeline.research.common import _project_a2_bottleneck_context
            domains=_project_a2_bottleneck_context(snapshot.data['A2_BOTTLENECK_CONTEXT'],set(gate.review_symbols))
            diagnostics.append(dict(call_id=call_id,original_output_sha256=raw_hash,
                details=frozen_domain_diagnostics(json.loads(raw),candidate_ids=set(gate.review_symbols),
                    allowed_theme_ids=set(domains['_theme_review_scope']),
                    direction_symbols={k:set(v) for k,v in domains['_rotation_review_scope'].items()})))
    save(output/'STAGE_AUDIT.json',audit.as_dict()|dict(frozen_domain_diagnostics=diagnostics))
    save(output/'FIELD_DIFF.json',dict(basis='ORIGINAL_A2_POST_POLICY_VS_NEW_ROLE_POST_POLICY',
        original_status=a2['status'],new_status=audit.status,fields=field_diff(a2['output'],audit.as_dict()['output'])))
    def pools(body):
        body=body or {}
        return {k:sorted(r.get('symbol') for r in body.get(k,[]) if isinstance(r,dict) and r.get('symbol'))
            for k in ('focus_pool','watch_only_pool','rejected_candidates','model_nominations')}
    result=dict(header,status=audit.status,reason_codes=list(audit.reason_codes),
        model_requests=len(http_calls),request_times=http_calls,provider_failures=failures,
        original_pools=pools(a2['output']),new_pools=pools(audit.output),actual_finished_at=datetime.now(TZ).isoformat(),
        schema_only_not_strategy_or_natural_acceptance=True)
    save(output/'RESULT.json',result)
    print(json.dumps({k:result[k] for k in ('status','reason_codes','model_requests','provider_failures','actual_finished_at')},ensure_ascii=True))
    return 0 if audit.status.startswith('VALIDATED') else 2


if __name__=='__main__':sys.exit(main())
