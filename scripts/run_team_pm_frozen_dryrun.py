"""Real PM morning call after an actual captured or failed A6 frozen request.

Consumes original frozen plans and either a verified A6 vote or its fallback.
No synthetic A6 success, live source, account writer or production activation.
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
TZ=ZoneInfo('Asia/Shanghai')


def verified_a6_capture(directory,source,result):
    if (result.get('input_sha256')!=source['request_input_sha256']
            or result.get('source_head')!=source['head']):raise ValueError('PM_DRYRUN_A6_SOURCE_BINDING_INVALID')
    with closing(sqlite3.connect((directory/'model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
        claims=[tuple(r) for r in db.execute('SELECT call_id,state,failure_code FROM model_call_claims')]
        if result.get('status')=='MODEL_CALL_FAILED':
            if claims!=[('A6:'+source['run_id'],'FAILED','TEAM_MODEL_CALL_FAILED')]:
                raise ValueError('PM_DRYRUN_ORIGINAL_A6_FAILURE_CLAIM_INVALID')
            return None,False,'ACTUAL_FAILURE_CONSERVATIVE_FALLBACK'
        rows=[dict(r) for r in db.execute('SELECT * FROM model_calls')]
    if result.get('status')!='PASS' or len(rows)!=1 or claims!=[('A6:'+source['run_id'],'CAPTURED',None)]:
        raise ValueError('PM_DRYRUN_VALIDATED_A6_CAPTURE_REQUIRED')
    row=rows[0]
    expected=dict(call_id='A6:'+source['run_id'],run_id=source['run_id'],role='A6',session='PREMARKET',
        source_trade_date=source['source_trade_date'],execution_trade_date=source['execution_trade_date'],
        input_hash=source['request_input_sha256'],prompt_hash=source['prompt_hash'])
    if (any(row.get(k)!=v for k,v in expected.items()) or row['validated']!=1
            or json.loads(row['issues_json']) or row['output_hash']!=result.get('response_output_sha256')
            or hashlib.sha256(row['output_json'].encode()).hexdigest()!=row['output_hash']):
        raise ValueError('PM_DRYRUN_A6_CAPTURE_HASH_OR_BINDING_INVALID')
    return json.loads(row['output_json']),True,'ACTUAL_VALIDATED_A6_CAPTURE'


def prepare(a6_source,database,database_hash,output,settings,provider):
    import yaml
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.runtime.team_v2_morning import _envelopes
    from liangjian_funnel.runtime.team_v2_sources import historical_meeting_plan_facts,stamp
    from liangjian_funnel.runtime.team_v2_store import canonical
    from liangjian_funnel.runtime.team_v2_risk_meeting import apply_premarket_review
    source=json.loads((a6_source/'PREPARED.json').read_bytes());prior=json.loads((a6_source/'RESULT.json').read_bytes())
    if source['source_database_sha256']!=database_hash:raise ValueError('PM_DRYRUN_ORIGINAL_DATABASE_HASH_INVALID')
    a6_output,a6_valid,a6_basis=verified_a6_capture(a6_source,source,prior)
    if hashlib.file_digest(database.open('rb'),'sha256').hexdigest()!=database_hash:
        raise ValueError('PM_DRYRUN_ORIGINAL_DATABASE_HASH_INVALID')
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        for row in source['original_plan_rows']:
            original=db.execute('SELECT * FROM execution_plans WHERE plan_id=?',(row['plan_id'],)).fetchone()
            if original is None or dict(original)!=row:raise ValueError('PM_DRYRUN_ORIGINAL_PLAN_ROW_CHANGED')
        db.rollback()
    day=source['execution_trade_date'];at=stamp(day+'T08:38:00+08:00')
    plans=historical_meeting_plan_facts(source['original_plan_rows']);regimes={p['publication_facts'].get('market_regime') for p in plans}
    if len(regimes)!=1:raise ValueError('PM_DRYRUN_ORIGINAL_REGIME_CONFLICT')
    regime=regimes.pop();deployment=yaml.safe_load((ROOT/'config/team_v2_deployment.yaml').read_text(encoding='utf-8'))
    external=yaml.safe_load((ROOT/'config/risk_limits.yaml').read_text(encoding='utf-8'))['external_adjustment']
    bounds=tuple(D(max(0,min(external['maximum_utilization_pct'],v+external['missing_adjustment_pp'])))
        for v in deployment['ranges'][regime])
    reviewed=apply_premarket_review(_envelopes(source['planning_envelopes']),a6_output,
        engine_utilization_cap=bounds[1],validated=a6_valid,previous_failures=0)
    if reviewed.status!=('READY' if a6_valid else 'DEGRADED'):raise ValueError('PM_DRYRUN_A6_REVIEW_NOT_APPLIED')
    clusters={p['plan_id']:p['cluster'] for p in plans};context=dict(
        classification='POST_HOC_FROZEN_INPUT_MODEL_DRYRUN',account_basis=source['account_basis'],
        source_a6_request_sha256=source['request_input_sha256'],a6_status=a6_basis,
        a6_provider_calls_this_run=0,source_a6_result_sha256=hashlib.sha256((a6_source/'RESULT.json').read_bytes()).hexdigest())
    from liangjian_funnel.runtime.risk_engine import RiskLimits
    limits=RiskLimits.from_yaml(ROOT/'config/risk_limits.yaml')
    account=dict(nav=str(limits.initial_capital),cash=str(limits.initial_capital),positions=[],basis=source['account_basis'])
    market=dict(market_regime=regime,basis='ORIGINAL_PRIOR_NOT_LIVE',external_status='UNAVAILABLE',
        external_adjustment_pp=external['missing_adjustment_pp'],dryrun_context=context)
    catalog=dict(categories=['ACCOUNT','MARKET','PLAN'],items=[dict(id='PM:ACCOUNT',category='ACCOUNT',value=account),
        dict(id='PM:MARKET',category='MARKET',value=market),
        *(dict(id='PM:PLAN:'+p['plan_id'],category='PLAN',value=p) for p in plans)])
    repository=TeamPromptRepository(ROOT/'prompts/team_v2');run='team-pm-frozen-dryrun:'+output.name
    values={key:dict(status='UNAVAILABLE') for key in repository.document('A3').placeholders}
    values.update(RISK_SESSION='PM_PREMARKET',ACCOUNT_SNAPSHOT=account,POSITIONS=[],PLANS=plans,PLANS_TODAY=plans,
        MARKET_STATE=market,EVIDENCE_CATALOG=catalog,PRIOR_OUTCOME_FEEDBACK=dict(status='INSUFFICIENT_TEN_DAYS'),
        OVERNIGHT_EXTERNAL_SNAPSHOT=market,RISK_ENVELOPES=[asdict(e) for e in reviewed.envelopes],
        DEPLOYMENT_BASELINE=dict(baseline_range=[str(v) for v in bounds],utilization_cap=str(reviewed.utilization_cap),
            free_slots=limits.max_positions))
    values=json.loads(canonical(values));prompt=repository.prepare('A3',values,session='PM_PREMARKET')
    required=dict(schema_version='team-envelope/1',run_id=run,stage_id='A3',source_trade_date=source['source_trade_date'],
        execution_trade_date=day,as_of=at.isoformat(),knowledge_cutoff=at.isoformat(),input_snapshot_ids=[database_hash],
        config_version='team-v2/1',config_hash=hashlib.sha256((ROOT/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        prompt_version=prompt.role_version,model_name=settings.review_model,market_regime=regime,status='FROZEN',reason_codes=[])
    request=repository.prepare_messages('A3',values,required_envelope=required,session='PM_PREMARKET')
    header=dict(context,run_id=run,source_trade_date=source['source_trade_date'],execution_trade_date=day,
        context_time=at.isoformat(),actual_prepared_at=datetime.now(TZ).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),provider_identity=provider,
        source_plan_rows_sha256=hashlib.sha256(canonical(source['original_plan_rows']).encode()).hexdigest(),
        conservative_envelopes=values['RISK_ENVELOPES'],required_envelope=required,
        input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,prompt_chars=sum(len(m['content']) for m in request.messages),
        production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    return request,values,header,dict(required_envelope=required,envelopes=reviewed.envelopes,plan_clusters=clusters,
        free_slots=limits.max_positions,baseline_range=bounds,utilization_cap=reviewed.utilization_cap,high_conviction_ids=set())


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--a6-source',type=Path,required=True)
    p.add_argument('--frozen-db',type=Path,required=True);p.add_argument('--frozen-db-sha256',required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.pipeline.team_v2_model import call_team_model
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_pm_schema import validate_pm_premarket
    settings,provider=model_settings(output,'vm-runtime')
    request,values,header,args=prepare(a.a6_source,a.frozen_db,a.frozen_db_sha256,output,settings,provider)
    # Check the server-owned domain before any provider call, as the real edge does.
    validate_pm_premarket(None,values['EVIDENCE_CATALOG'],**args)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',plans=len(args['envelopes']),chars=header['prompt_chars'])));return 0
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/'PROVIDER_FAILURE.json',dict(reason_code=exc.reason_code,status_code=exc.status_code,
                    attempts=exc.attempts,actual_at=datetime.now(TZ).isoformat()));raise
    started=datetime.now(TZ);outcome=None
    try:
        outcome=call_team_model(request,AuditedClient(settings,max_attempts=1),ModelJudgmentJournal(output/'model.db'),
            call_id='A3:'+header['run_id'],run_id=header['run_id'],model=settings.review_model,session='PM_PREMARKET',
            source_trade_date=header['source_trade_date'],execution_trade_date=header['execution_trade_date'],
            catalog=values['EVIDENCE_CATALOG'],timeout_seconds=100,max_output_tokens=16384,
            validator=lambda raw:validate_pm_premarket(raw,values['EVIDENCE_CATALOG'],**args),clock=lambda:datetime.now(TZ),allow_model_call=True)
        save(output/'FIELD_DIFF.json',dict(original_baseline='NO_PRODUCTION_PM_ROLE_CALL',fields=field_diff({},outcome.response.output)))
    except ValueError as exc:
        if str(exc) not in {'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:raise
    result=dict(status='MODEL_CALL_FAILED' if outcome is None else 'PASS' if outcome.validation.valid else 'SCHEMA_FAILED',
        reason_code='TEAM_MODEL_CALL_FAILED' if outcome is None else None,
        issues=list(outcome.validation.issues) if outcome else [],latency_ms=outcome.response.latency_ms if outcome else None,
        actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),source_head=header['head'],
        input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,classification=header['classification'],
        source_a6_status=header['a6_status'],model_calls=1,production_writes=0,customer_notifications=0,natural_acceptance=False)
    save(output/'RESULT.json',result);print(json.dumps(result,ensure_ascii=True))
    return 0 if outcome and outcome.validation.valid else 2


if __name__=='__main__':sys.exit(main())
