"""0315 authorized three-day radar/PMI format probes; never a live trigger.

Absent historical G5/Team account/LEADER gate evidence remains absent. Empty
non-ON samples prove only transport/schema/latency, not positive selection.
"""
import argparse,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
from replay_team_g5_five_day import sealed_caches,original_facts
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.runtime.team_v2_intraday_sessions import call_intraday_research,radar_market_context
from liangjian_funnel.runtime.team_v2_regime import load_rules
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
TZ=ZoneInfo('Asia/Shanghai');DAYS=('2026-10-14','2026-10-15','2026-10-16')


def prepare(source,output,day,role,model):
    at=datetime.fromisoformat(day+'T10:00:00+08:00');caches,_=sealed_caches(source)
    cache=caches.get((day,at.isoformat()))
    if cache is None:raise ValueError('ORIGINAL_1000_CACHE_MISSING')
    facts,limitations=original_facts(cache,tick=at,observed_at=at)
    # One isolated original bucket is not a historical sequence or a morning
    # permission. Missing emotion counts are DATA_BLOCK, never invented OFF.
    state=TeamMeetingStore(output/'research-g5.db').record_live_regime(
        facts=facts,tick=at,observed_at=at,prior='DATA_BLOCK',rules=load_rules(ROOT/'config/regime_live.yaml'))
    market=radar_market_context(state,observed_at=at)
    if market['emotion_state']=='ON':raise ValueError('POSITIVE_GATE_EVIDENCE_REQUIRES_SEPARATE_PREPARATION')
    market.update(source_basis='ORIGINAL_1000_CACHE_ISOLATED_FORMAT_SAMPLE',original_cache_sha256=cache[1],
        limitations=limitations,historical_team_g5_sequence='NO_ORIGINAL_SESSION')
    capacity=dict(status='UNAVAILABLE',free_slots=0,remaining_intraday_daily=0,cluster_used={},cluster_caps={},
        reason_code='NO_HISTORICAL_TEAM_ACCOUNT_OR_PM_PERMISSION',first_five_days=True,
        first_five_days_basis='PROSPECTIVE_SCHEMA_FIXTURE_NOT_HISTORICAL_DAY_COUNT')
    catalog=dict(categories=['MARKET','CAPACITY','SOURCE'],items=[
        dict(id='RADAR:MARKET',category='MARKET',value=market),
        dict(id='RADAR:CAPACITY',category='CAPACITY',value=capacity),
        dict(id='RADAR:SOURCE',category='SOURCE',value=dict(original_cache_sha256=cache[1],
            candidate_domain='EMPTY_NO_PROVEN_LEADER_PROFILE_OR_MORNING_THEME_EVIDENCE'))])
    repo=TeamPromptRepository(ROOT/'prompts/team_v2');session='INTRADAY_RADAR' if role=='A2' else 'PM_INTRADAY'
    values={key:dict(status='UNAVAILABLE') for key in repo.document(role,session).placeholders}
    values.update(LIVE_MARKET_STATE=market,EVIDENCE_CATALOG=catalog)
    if role=='A2':
        values.update(THEME_STATE={},RADAR_CANDIDATES={},EXISTING_EXPOSURE=capacity)
        context=dict(candidates={},themes={},emotion_state=market['emotion_state'])
    else:
        values.update(RADAR_PICKS=[],INTRADAY_PLAN_DRAFTS=[],CAPACITY_STATE=capacity,RISK_ENVELOPES={})
        context=dict(drafts={},envelopes={},first_five_days=True)
    run='team-intraday-format:'+output.name;prompt=repo.prepare(role,values,session=session)
    config=hashlib.sha256((ROOT/'config/regime_live.yaml').read_bytes()).hexdigest()
    envelope=dict(schema_version='team-envelope/1',run_id=run,stage_id=role,source_trade_date=day,
        execution_trade_date=day,as_of=at.isoformat(),knowledge_cutoff=at.isoformat(),input_snapshot_ids=[cache[1]],
        config_version='team-v2/1',config_hash=config,prompt_version=prompt.role_version,
        model_name=model,market_regime='DATA_BLOCK',status='FROZEN',reason_codes=limitations)
    prepared=dict(actual_at=datetime.now(TZ).isoformat(),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        role=role,session=session,source_day=day,original_source_sha256=cache[1],knowledge_cutoff=at.isoformat(),
        original_session='NO_ORIGINAL_SESSION',classification='AUTHOR315_EMPTY_NON_ON_FORMAT_ONLY',
        source_limitations=limitations,required_envelope=envelope,replacements=values,
        forced_format_sample_not_live_trigger=True,positive_candidate_gate=False,paper_ledger_writes=0,production_writes=0)
    return repo,context,prepared


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--day',choices=DAYS,required=True);p.add_argument('--role',choices=['A2','A3'],required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    output.mkdir(parents=True)
    if a.execute_real_model:settings,provider=model_settings(output,'vm-runtime');model=settings.team_intraday_model
    else:provider=dict(source='NOT_LOADED_NO_MODEL_CALL');model='deepseek-v4-flash-0731'
    repo,context,prepared=prepare(a.source,output,a.day,a.role,model);prepared['provider']=provider
    save(output/'PREPARED.json',prepared)
    if not a.execute_real_model:print(json.dumps(dict(status='PREPARED_NOT_CALLED')));return 0
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    attempts=[]
    class Client(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            attempts.append(dict(actual_started_at=datetime.now(TZ).isoformat()))
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/'PROVIDER_FAILURE.json',dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts));raise
    start=datetime.now(TZ);journal=ModelJudgmentJournal(output/'model.db');run=prepared['required_envelope']['run_id']
    try:
        outcome=call_intraday_research(role=a.role,repository=repo,client=Client(settings,max_attempts=1),journal=journal,
            call_id=a.role+':'+run,run_id=run,model=model,source_trade_date=a.day,execution_trade_date=a.day,
            required_envelope=prepared['required_envelope'],replacements=prepared['replacements'],server_context=context,
            clock=lambda:datetime.now(TZ),max_output_tokens=settings.model_max_output_tokens,allow_model_call=True)
    except ValueError as exc:
        # Only a fixed local reason code; never an upstream exception body.
        save(output/'RESULT.json',dict(status='FAILED',reason_code=str(exc) if str(exc).startswith('TEAM_') else 'LOCAL_VALIDATION_FAILURE',
            actual_started_at=start.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),model_calls=len(attempts),production_writes=0))
        print(json.dumps(dict(status='FAILED_MODEL_OR_VALIDATION')));return 2
    valid=outcome.validation.valid and outcome.response.latency_ms<=60000
    body=dict(status='PASS_EMPTY_NON_ON_FORMAT_LATENCY_ONLY' if valid else 'FAILED_FORMAT_OR_LATENCY',
        role=a.role,session=prepared['session'],source_day=a.day,actual_started_at=start.isoformat(),
        actual_finished_at=datetime.now(TZ).isoformat(),model_calls=len(attempts),latency_ms=outcome.response.latency_ms,
        validation=dict(valid=outcome.validation.valid,issues=list(outcome.validation.issues)),receipt=outcome.receipt,
        output=outcome.response.output,original_session='NO_ORIGINAL_SESSION',positive_candidate_gate=False,
        live_trigger_acceptance=False,orders=0,paper_ledger_writes=0,production_writes=0)
    save(output/'RESULT.json',body);save(output/'FIELD_DIFF.json',dict(original_session='NO_ORIGINAL_SESSION',fields=field_diff({},outcome.response.output)))
    print(json.dumps({k:v for k,v in body.items() if k not in {'output','receipt'}},ensure_ascii=True));return 0 if valid else 2

if __name__=='__main__':sys.exit(main())
