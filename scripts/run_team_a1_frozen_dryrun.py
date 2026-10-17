"""Independent frozen A1 macro discovery; no production research/stock mapping.

The historical A1 was reused. Its output is a comparison reference, not proof
that the same new role ran on the source day. Company mapping is a separate gate.
"""
import argparse,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
TZ=ZoneInfo('Asia/Shanghai')


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
 p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
 p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
 output=a.output.resolve()
 if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
 output.mkdir(parents=True);settings,provider=model_settings(output,'vm-runtime')
 settings=settings.model_copy(update={'source_config_path':ROOT/'config/funnel_config_v2.yaml','prompt_dir':ROOT/'prompts'})
 from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
 from liangjian_funnel.pipeline.monthly_strategy import build_monthly_strategy_context
 from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter
 from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
 from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
 original,old,_,proof=source(a.sources.resolve(),a.day)
 snapshot=FrozenInputSnapshot(original['snapshot_id'],original['data'],original['snapshot_hash'],datetime.fromisoformat(original['as_of']))
 context=build_monthly_strategy_context(snapshot.data,as_of=snapshot.as_of,prior_registry=None,
  policy_lookback_days=settings.a1_policy_lookback_days,policy_document_limit=settings.a1_policy_document_limit)
 run='team-a1-frozen-discovery:'+a.day+':'+output.name
 header=dict(classification='POST_HOC_INDEPENDENT_A1_DISCOVERY_ONLY',source_trade_date=a.day,
  context_time=original['as_of'],actual_prepared_at=datetime.now(TZ).isoformat(),run_id=run,
  source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
  source_proofs=proof,parent_snapshot_sha256=original['snapshot_hash'],original_a1_status=old['status'],
  original_a1_reason_codes=old['reason_codes'],original_a1_output_sha256=old['output_hash'],
  original_reference_kind='REUSED_PREVIOUS_A1_NOT_SAME_INPUT_NEW_MODEL_BASELINE',
  monthly_strategy_context=context,provider_identity=provider,stock_mapping=False,production_writes=0,
  customer_notifications=0,natural_acceptance=False)
 save(output/'PREPARED.json',header)
 if not a.execute_real_model:print(json.dumps(dict(status='PREPARED_NOT_CALLED',source_day=a.day)));return 0
 requests=[];failures=[]
 class AuditedClient(OpenAICompatibleModelClient):
  def complete(self,*args,**kwargs):
   requests.append(dict(actual_started_at=datetime.now(TZ).isoformat(),stage=kwargs.get('stage')))
   try:return super().complete(*args,**kwargs)
   except ModelClientError as exc:failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts));raise
 adapter=configured_research_adapter(settings,calendar=ExchangeTradingCalendar())
 pipeline=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',model_client=AuditedClient(settings,max_attempts=1),
  now=lambda:datetime.now(TZ),enable_feature_store=False,team_research_adapter=adapter,output_dir=output/'research')
 audit=pipeline._run_stage(lane_id='lane_1',model=settings.review_model,stage='A1',snapshot=snapshot,
  upstream_output=None,upstream_symbols=set(),projection_symbols=set(),bundle=pipeline.prompts.load(),run_id=run,
  a1_discovery_context={'mode':'POLICY_MACRO_DISCOVERY','monthly_strategy_context':context})
 save(output/'STAGE_AUDIT.json',audit.as_dict());save(output/'FIELD_DIFF.json',dict(
  original_reference_kind=header['original_reference_kind'],fields=field_diff(old.get('output'),audit.output)))
 result=dict(header,status=audit.status,reason_codes=list(audit.reason_codes),model_requests=len(requests),
  requests=requests,provider_failures=failures,actual_finished_at=datetime.now(TZ).isoformat(),
  company_mapping_gate='NOT_RUN',three_day_release_gate='NOT_PASSED')
 save(output/'RESULT.json',result);print(json.dumps({k:result[k] for k in ('status','reason_codes','model_requests','actual_finished_at')}))
 return 0 if audit.status.startswith('VALIDATED') else 2


if __name__=='__main__':sys.exit(main())
