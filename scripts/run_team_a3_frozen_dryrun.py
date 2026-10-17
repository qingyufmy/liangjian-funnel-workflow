"""Independent original A2 -> new A3 blind review, no upstream model rerun.

Uses the actual quantitative candidate builder, A3 hard gate and batch review.
Only the closed snapshot/original A2 output is consumed; no publication, live
collector, broker, WorkflowApplication or historical row mutation is involved.
"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
TZ=ZoneInfo('Asia/Shanghai')


def prepare(sources,output,day,settings,provider):
    import ijson
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
    from liangjian_funnel.pipeline.research.common import (_build_a3_candidate_domain,
        _with_a3_candidate_context,_with_a3_deterministic_context,screen_a3)
    from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.runtime.team_v2_store import canonical
    original,_,a2,proof=source(sources,day)
    if not a2['status'].startswith('VALIDATED') or not a2.get('output'):
        raise ValueError('A3_DRYRUN_ORIGINAL_VALID_A2_REQUIRED')
    lane=sources/'files'/proof['lane']['relative']
    with lane.open('rb') as stream:
        original_a3=next(r for r in ijson.items(stream,'stages.item',use_float=True) if r['stage']=='A3')
    snapshot=FrozenInputSnapshot(original['snapshot_id'],original['data'],original['snapshot_hash'],
        datetime.fromisoformat(original['as_of']))
    upstream,origins=_build_a3_candidate_domain(a2['output'])
    snapshot=_with_a3_candidate_context(snapshot,origins)
    gate=screen_a3(snapshot.data,upstream)
    snapshot=_with_a3_deterministic_context(snapshot,gate)
    adapter=configured_research_adapter(settings,calendar=ExchangeTradingCalendar())
    run='team-a3-frozen-dryrun:'+day+':'+output.name
    header=dict(classification='POST_HOC_INDEPENDENT_STAGE_FROZEN_DRYRUN',actual_prepared_at=datetime.now(TZ).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),run_id=run,
        source_trade_date=day,context_time=original['as_of'],source_proofs=proof,
        parent_snapshot_sha256=original['snapshot_hash'],derived_snapshot_sha256=snapshot.snapshot_hash,
        original_a2_output_sha256=a2['output_hash'],original_a3_output_sha256=original_a3.get('output_hash'),
        candidate_origins=origins,quant_review_symbols=sorted(gate.review_symbols),
        quant_decisions_sha256=__import__('hashlib').sha256(canonical(gate.decisions).encode()).hexdigest(),
        evaluated_count=len(gate.decisions),provider_identity=provider,production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    return snapshot,upstream,origins,gate,adapter,original_a3,header


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    settings,provider=model_settings(output,'vm-runtime')
    settings=settings.model_copy(update={'source_config_path':ROOT/'config/funnel_config_v2.yaml','prompt_dir':ROOT/'prompts'})
    snapshot,upstream,origins,gate,adapter,old,header=prepare(a.sources.resolve(),output,a.day,settings,provider)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',candidates=len(origins),review=len(gate.review_symbols))));return 0
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.pipeline.research import ResearchPipeline
    requests=[];failures=[]
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            requests.append(dict(actual_started_at=datetime.now(TZ).isoformat(),stage=kwargs.get('stage')))
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts));raise
    pipeline=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',model_client=AuditedClient(settings,max_attempts=1),
        now=lambda:datetime.now(TZ),enable_feature_store=False,team_research_adapter=adapter,output_dir=output/'research')
    audit=pipeline._run_v2_downstream_review(lane_id='lane_1',model=settings.review_model,stage='A3',snapshot=snapshot,
        upstream_output=upstream,full_upstream_symbols=set(origins),gate=gate,bundle=pipeline.prompts.load(),run_id=header['run_id'])
    save(output/'STAGE_AUDIT.json',audit.as_dict())
    save(output/'FIELD_DIFF.json',dict(original_schema='PRODUCTION_A3',new_schema='agent-3/4.0.2',
        fields=field_diff(old.get('output'),audit.output)))
    def core(body):return sorted(r['symbol'] for r in (body or {}).get('core_watch_pool',[]))
    previous,current=core(old.get('output')),core(audit.output)
    valid=audit.status.startswith('VALIDATED')
    result=dict(status=audit.status,reason_codes=list(audit.reason_codes),source_head=header['head'],
        actual_finished_at=datetime.now(TZ).isoformat(),source_trade_date=a.day,model_requests=len(requests),
        requests=requests,provider_failures=failures,original_core=previous,new_core=current,
        pass_count_decline_gt20pct=(len(current)*100<len(previous)*80) if previous and valid else None,
        classification=header['classification'],original_upstream='PRODUCTION_A2_NOT_NEW_ROLE_A2',
        production_writes=0,customer_notifications=0,publication=False,natural_acceptance=False)
    save(output/'RESULT.json',result);print(json.dumps(result,ensure_ascii=True))
    return 0 if valid else 2


if __name__=='__main__':sys.exit(main())
