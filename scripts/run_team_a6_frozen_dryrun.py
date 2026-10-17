"""Real A6 PREMARKET schema dryrun on original published frozen plan rows.

The starting paper account is a declared simulation input, not a claim about
the historical production account. No broker, meeting executor or orders run.
"""
import argparse
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
from run_team_a2_frozen_dryrun import source as original_source
TZ=ZoneInfo('Asia/Shanghai')


def prepare(sources,database,database_hash,output,day,settings,provider):
    import ijson
    import yaml
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.review.observability import _cluster
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.runtime.risk_engine import AccountRisk,PlanRisk,RiskLimits,build_planning_envelope
    from liangjian_funnel.runtime.team_v2_sources import stamp,historical_meeting_plan_facts
    from liangjian_funnel.runtime.team_v2_store import canonical
    snapshot,_,_,proof=original_source(sources,day)
    lane=sources/'files'/proof['lane']['relative']
    with lane.open('rb') as stream:a3=next(r for r in ijson.items(stream,'stages.item',use_float=True) if r['stage']=='A3')
    if a3['status']!='VALIDATED' or not a3.get('output'):raise ValueError('A6_DRYRUN_ORIGINAL_A3_REQUIRED')
    if hashlib.file_digest(database.open('rb'),'sha256').hexdigest()!=database_hash:
        raise ValueError('A6_DRYRUN_DATABASE_HASH_MISMATCH')
    calendar=ExchangeTradingCalendar();execution=calendar.next_trading_day(stamp(snapshot['as_of']).date()).isoformat()
    at=stamp(execution+'T08:35:00+08:00')
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        rows=[dict(r) for r in db.execute("SELECT * FROM execution_plans WHERE lane_id='lane_1' AND substr(expires_at,1,10)=? ORDER BY plan_id",(execution,))]
        db.rollback()
    name=Path(proof['lane']['relative']).name
    if not name.startswith('research_') or not name.endswith('_lane_1.json'):
        raise ValueError('A6_DRYRUN_ORIGINAL_RUN_ID_UNPROVEN')
    original_run=name.removeprefix('research_').removesuffix('_lane_1.json')
    rows=[r for r in rows if json.loads(r['payload_json']).get('source_run_id')==original_run]
    core={r['symbol'] for r in a3['output']['core_watch_pool']}
    if not rows or {r['symbol'] for r in rows}!=core:
        raise ValueError('A6_DRYRUN_ORIGINAL_PUBLICATION_SCOPE_MISMATCH')
    limits=RiskLimits.from_yaml(ROOT/'config/risk_limits.yaml')
    account=AccountRisk(limits.initial_capital,limits.initial_capital,limits.initial_capital,limits.initial_capital)
    plans=[];regimes=set()
    for row in rows:
        raw=json.loads(row['payload_json']);cluster,_=_cluster(row)
        if cluster=='UNKNOWN':raise ValueError('A6_DRYRUN_CLUSTER_UNPROVEN')
        regimes.add(raw.get('market_regime') or raw.get('market_environment'))
        plans.append(PlanRisk(row['plan_id'],row['symbol'],cluster,'PENDING_MORNING_REVIEW',None,stamp(row['expires_at']),
            D(str(raw['reference_price'])),D(str(raw['stop_level'])),D(str(raw['trigger_low'])),D(str(raw['trigger_high'])),
            D(str(raw['no_chase'])),invalidation_price=D(str(raw['stop_level']))))
    if len(regimes)!=1:raise ValueError('A6_DRYRUN_ORIGINAL_REGIME_CONFLICT')
    regime=regimes.pop();policy=yaml.safe_load((ROOT/'config/team_v2_deployment.yaml').read_text(encoding='utf-8'))
    bounds=policy['ranges'][regime]
    # Missing original overnight values follow 0237's conservative -10pp path.
    external=yaml.safe_load((ROOT/'config/risk_limits.yaml').read_text(encoding='utf-8'))['external_adjustment']
    shift=external['missing_adjustment_pp']
    cap=max(0,min(external['maximum_utilization_pct'],bounds[1]+shift))
    envelopes=tuple(build_planning_envelope(p,account,(),limits,now=at,regime=regime) for p in plans)
    cluster_caps={p.cluster:limits.max_positions for p in plans}
    context=dict(classification='POST_HOC_FROZEN_INPUT_MODEL_DRYRUN',source_trade_date=day,execution_trade_date=execution,
        account_basis='ISOLATED_INITIAL_CAPITAL_FROM_APPROVED_YAML_NOT_HISTORICAL_ACCOUNT',
        activation_basis='PLANNING_ONLY_NO_HISTORICAL_OR_CURRENT_ACTIVATION',
        original_a3_run_id=original_run,original_a3_output_hash=a3['output_hash'],source_database_sha256=database_hash)
    account_value=dict(nav=str(account.nav),cash=str(account.cash),positions=[],basis=context['account_basis'])
    meeting_plans=historical_meeting_plan_facts(rows)
    catalog=dict(categories=['ACCOUNT','MARKET','PLAN'],items=[
        dict(id='DRY:ACCOUNT',category='ACCOUNT',value=account_value),
        dict(id='DRY:MARKET',category='MARKET',value=dict(market_regime=regime,external_status='UNAVAILABLE',external_adjustment_pp=shift)),
        *(dict(id='DRY:PLAN:'+r['plan_id'],category='PLAN',value=r) for r in meeting_plans)])
    repository=TeamPromptRepository(ROOT/'prompts/team_v2')
    values={key:dict(status='UNAVAILABLE') for key in repository.document('A6').placeholders}
    run='team-a6-frozen-dryrun:'+day+':'+output.name
    upper=dict(account_id='isolated-paper',call_id='A6:'+run,engine_utilization_cap=cap,
        capacity_slots=limits.max_positions,cluster_caps=cluster_caps,unready_plan_ids=[e.plan_id for e in envelopes if not e.eligible],
        timeout_seconds=100,max_output_tokens=16384)
    values.update(RISK_SESSION='PREMARKET',EVIDENCE_CATALOG=catalog,ACCOUNT_SNAPSHOT=account_value,POSITIONS=[],
        PLANS=meeting_plans,ENGINE_ENVELOPES=[asdict(e) for e in envelopes],
        RISK_LIMITS=dict(policy=asdict(limits),engine_upper_bounds=upper,
            session_binding_sha256=hashlib.sha256(canonical(upper).encode()).hexdigest()),
        MARKET_STATE=dict(market_regime=regime,basis='ORIGINAL_PRIOR_NOT_LIVE',dryrun_context=context),
        PRIOR_OUTCOME_FEEDBACK=dict(status='INSUFFICIENT_TEN_DAYS'))
    values=json.loads(canonical(values))
    prompt=repository.prepare('A6',values,session='PREMARKET')
    required=dict(schema_version='team-envelope/1',run_id=run,stage_id='A6',source_trade_date=day,execution_trade_date=execution,
        as_of=at.isoformat(),knowledge_cutoff=at.isoformat(),input_snapshot_ids=[snapshot['snapshot_hash'],database_hash],
        config_version='team-v2/1',config_hash=hashlib.sha256((ROOT/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        prompt_version=prompt.role_version,model_name=settings.review_model,market_regime=regime,status='FROZEN',reason_codes=[])
    request=repository.prepare_messages('A6',values,required_envelope=required,session='PREMARKET')
    header=dict(context,actual_prepared_at=datetime.now(TZ).isoformat(),context_time=at.isoformat(),run_id=run,
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_proofs=proof,
        original_plan_rows=rows,planning_envelopes=values['ENGINE_ENVELOPES'],provider_identity=provider,
        request_input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,
        prompt_chars=sum(len(m['content']) for m in request.messages),production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    return request,dict(required_envelope=required,envelopes=envelopes,engine_utilization_cap=D(cap),
        capacity_slots=limits.max_positions,cluster_caps=cluster_caps,unready_plan_ids=set(upper['unready_plan_ids'])),header,values


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--frozen-db',type=Path,required=True);p.add_argument('--frozen-db-sha256',required=True)
    p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_model import call_team_model
    from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_premarket
    settings,provider=model_settings(output,'vm-runtime')
    request,args,header,values=prepare(a.sources,a.frozen_db,a.frozen_db_sha256,output,a.day,settings,provider)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',plans=len(args['envelopes']))));return 0
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/'PROVIDER_FAILURE.json',dict(reason_code=exc.reason_code,status_code=exc.status_code,
                    attempts=exc.attempts,actual_at=datetime.now(TZ).isoformat()));raise
    started=datetime.now(TZ);journal=ModelJudgmentJournal(output/'model.db')
    result=None
    try:
        result=call_team_model(request,AuditedClient(settings,max_attempts=1),journal,call_id='A6:'+header['run_id'],
            run_id=header['run_id'],model=settings.review_model,session='PREMARKET',source_trade_date=a.day,
            execution_trade_date=header['execution_trade_date'],catalog=values['EVIDENCE_CATALOG'],timeout_seconds=100,
            max_output_tokens=16384,validator=lambda raw:validate_risk_premarket(raw,values['EVIDENCE_CATALOG'],**args),
            clock=lambda:datetime.now(TZ),allow_model_call=True)
        save(output/'FIELD_DIFF.json',dict(original_baseline='NO_PRODUCTION_A6_ROLE_CALL',fields=field_diff({},result.response.output)))
    except ValueError as exc:
        if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
    summary=dict(status='MODEL_CALL_FAILED' if result is None else 'PASS' if result.validation.valid else 'SCHEMA_FAILED',
        issues=list(result.validation.issues) if result else [],
        reason_code='TEAM_MODEL_CALL_FAILED' if result is None else None,
        source_head=header['head'],classification=header['classification'],day=a.day,context_time=header['context_time'],
        actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),
        response_output_sha256=result.response.output_hash if result else None,latency_ms=result.response.latency_ms if result else None,
        input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,provider_attempts=result.response.attempts if result else None,
        model_calls=1,production_writes=0,customer_notifications=0,provider_identity=provider,
        historical_account='UNAVAILABLE_SEPARATE_INITIAL_ACCOUNT_ASSUMPTION',natural_acceptance=False)
    save(output/'RESULT.json',summary);print(json.dumps(summary,ensure_ascii=True))
    return 0 if result and result.validation.valid else 2


if __name__=='__main__':sys.exit(main())
