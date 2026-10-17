"""Validated frozen discovery -> original screen_a1 -> A1 company batches only.

No source collection, A2/A3, WorkflowApplication, production store or publisher.
The actual native batch executor owns domain/split policy; no manual retry.
"""
from contextlib import closing
from dataclasses import fields
from datetime import datetime
import argparse,gzip,hashlib,json,sqlite3,subprocess,sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
from liangjian_funnel.runtime.team_v2_store import canonical
TZ=ZoneInfo('Asia/Shanghai')


def verified_discovery(directory,snapshot):
    from liangjian_funnel.pipeline.research.common import StageAudit
    raw=json.loads((directory/'STAGE_AUDIT.json').read_bytes())
    prepared=json.loads((directory/'PREPARED.json').read_bytes())
    if (raw['status']!='VALIDATED' or raw['snapshot_id']!=snapshot.snapshot_id
            or prepared['parent_snapshot_sha256']!=snapshot.snapshot_hash
            or hashlib.sha256(canonical(raw['output']).encode()).hexdigest()!=raw['output_hash']):
        raise ValueError('A1_MAPPING_DISCOVERY_HASH_OR_SOURCE_INVALID')
    identity=raw['diagnostics']['team_call_id']
    with closing(sqlite3.connect((directory/'state/team_v2/model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        row=db.execute('SELECT role,session,validated,output_json,output_hash FROM model_calls WHERE call_id=?',(identity,)).fetchone()
    if (row is None or row[:3]!=('A1','POLICY_MACRO_DISCOVERY',1)
            or hashlib.sha256(canonical(json.loads(row[3])).encode()).hexdigest()!=row[4]):
        raise ValueError('A1_MAPPING_DISCOVERY_ACTUAL_CAPTURE_UNPROVEN')
    return StageAudit(**{key:raw[key] for key in (f.name for f in fields(StageAudit)) if key in raw}),prepared


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--discovery',type=Path,required=True);p.add_argument('--day',required=True,choices=['2026-10-14','2026-10-15','2026-10-16'])
    p.add_argument('--output',type=Path,required=True)
    execution=p.add_mutually_exclusive_group();execution.add_argument('--execute-real-model',action='store_true')
    execution.add_argument('--execute-native-zero-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    if a.execute_real_model:settings,provider=model_settings(output,'vm-runtime')
    else:
        from liangjian_funnel.settings import Settings
        settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'OFFLINE_NOT_A_CREDENTIAL','LIANGJIAN_REVIEW_MODEL':'deepseek-v4-pro'},root=output)
        provider={'source':'OFFLINE_NOT_CALLED','credential_reads':0}
    settings=settings.model_copy(update={'source_config_path':ROOT/'config/funnel_config_v2.yaml','prompt_dir':ROOT/'prompts'})
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
    from liangjian_funnel.pipeline.research.common import _extract_g0
    from liangjian_funnel.pipeline.deterministic import screen_a1
    from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    original,old,_,proof=source(a.sources.resolve(),a.day)
    snapshot=FrozenInputSnapshot(original['snapshot_id'],original['data'],original['snapshot_hash'],datetime.fromisoformat(original['as_of']))
    discovery,pre=verified_discovery(a.discovery.resolve(),snapshot)
    gate=screen_a1(snapshot.data,discovery.output,local_top_n_per_node=settings.a1_local_top_n_per_node,
        llm_top_n_per_theme=settings.a1_llm_representatives_per_theme)
    context=dict(mode='COMPANY_MAPPING',structural_themes=discovery.output['structural_themes'],
        industry_chain_graph=discovery.output['industry_chain_graph'],taxonomy_links=list(gate.taxonomy_links),
        monthly_strategy_context=pre['monthly_strategy_context'],
        local_candidates={str(row['symbol']):row for row in gate.decisions if row.get('sent_to_llm') is True})
    run='team-a1-frozen-company:'+a.day+':'+output.name
    header=dict(classification='POST_HOC_INDEPENDENT_A1_COMPANY_MAPPING_ONLY',actual_prepared_at=datetime.now(TZ).isoformat(),
        source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_trade_date=a.day,
        context_time=original['as_of'],run_id=run,snapshot_sha256=snapshot.snapshot_hash,source_proofs=proof,
        discovery_source=str(a.discovery),discovery_output_sha256=discovery.output_hash,
        local_screen=gate.summary,review_symbols=list(gate.review_symbols),native_batch_size=settings.research_a1_batch_size,
        local_top_n_per_node=settings.a1_local_top_n_per_node,llm_representatives_per_theme=settings.a1_llm_representatives_per_theme,
        original_a1_status=old['status'],original_a1_output_hash=old['output_hash'],provider_identity=provider,
        production_writes=0,customer_notifications=0,natural_acceptance=False)
    save(output/'PREPARED.json',header)
    if a.execute_native_zero_model and gate.review_symbols:raise ValueError('A1_NATIVE_ZERO_MODEL_SCOPE_NOT_EMPTY')
    if not (a.execute_real_model or a.execute_native_zero_model):
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',review_count=len(gate.review_symbols),g0_count=len(_extract_g0(snapshot.data)),
            batch_count=(len(gate.review_symbols)+settings.research_a1_batch_size-1)//settings.research_a1_batch_size)));return 0
    requests=[];failures=[]
    class Client(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            if not a.execute_real_model:raise AssertionError('A1_NATIVE_ZERO_MODEL_FORBIDS_HTTP')
            requests.append(dict(actual_started_at=datetime.now(TZ).isoformat(),stage=kwargs.get('stage'),input_hash=kwargs.get('input_hash')))
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts));raise
    app=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',model_client=Client(settings,max_attempts=1),
        now=lambda:datetime.now(TZ),enable_feature_store=False,team_research_adapter=configured_research_adapter(settings,calendar=ExchangeTradingCalendar()),
        output_dir=output/'research')
    started=datetime.now(TZ)
    audit=app._run_v2_a1_review(lane_id='lane_1',model=settings.review_model,snapshot=snapshot,g0=_extract_g0(snapshot.data),
        bundle=app.prompts.load(),run_id=run,discovery=discovery,discovery_output=discovery.output,discovery_context=context,gate=gate)
    save(output/'STAGE_AUDIT.json',audit.as_dict())
    diff=dict(original_reference_kind='REUSED_PREVIOUS_PRODUCTION_A1_NOT_SAME_NEW_DISCOVERY',fields=field_diff(old.get('output'),audit.output))
    with gzip.open(output/'FIELD_DIFF.json.gz','xb') as stream:
        for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(diff):stream.write(chunk.encode())
    result=dict(header,status=audit.status,reason_codes=list(audit.reason_codes),model_requests=len(requests),requests=requests,
        provider_failures=failures,actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),
        original_active_count=len((old.get('output') or {}).get('active_research_pool',[])),
        new_active_count=len((audit.output or {}).get('active_research_pool',[])),three_day_release_gate='NOT_PASSED',
        company_model_gate='NO_NATIVE_REVIEW_CANDIDATES_NOT_CALLED' if not gate.review_symbols else 'ACTUAL_NATIVE_BATCH_CALLS')
    save(output/'RESULT.json',result)
    print(json.dumps({key:result[key] for key in ('status','reason_codes','model_requests','actual_finished_at','new_active_count')}))
    return 0 if audit.status.startswith('VALIDATED') else 2


if __name__=='__main__':sys.exit(main())
