"""Historical frozen-input A5 model dryrun, never a natural report or trade.

Context time is the original fact time. Model capture time is the actual time.
The absent historical Team account stays UNAVAILABLE, not a fabricated curve.
"""
import argparse
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
TZ=ZoneInfo('Asia/Shanghai')


def digest(value):
    encoder=json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str)
    h=hashlib.sha256()
    for part in encoder.iterencode(value):h.update(part.encode())
    return h.hexdigest()


def field_diff(before,after,path='$'):
    if type(before)!=type(after):return [dict(path=path,kind='TYPE_CHANGED',before=before,after=after)]
    if isinstance(before,dict):
        result=[]
        for key in sorted(set(before)|set(after)):
            if key not in before:result.append(dict(path=path+'.'+key,kind='ADDED',after=after[key]))
            elif key not in after:result.append(dict(path=path+'.'+key,kind='REMOVED',before=before[key]))
            else:result.extend(field_diff(before[key],after[key],path+'.'+key))
        return result
    if isinstance(before,list):
        result=[]
        for i in range(max(len(before),len(after))):
            if i>=len(before):result.append(dict(path=f'{path}[{i}]',kind='ADDED',after=after[i]))
            elif i>=len(after):result.append(dict(path=f'{path}[{i}]',kind='REMOVED',before=before[i]))
            else:result.extend(field_diff(before[i],after[i],f'{path}[{i}]'))
        return result
    return [] if before==after else [dict(path=path,kind='CHANGED',before=before,after=after)]


def save(path,value):
    with path.open('x',encoding='utf-8') as f:json.dump(value,f,ensure_ascii=False,indent=2)


def prepare(sources,output,day,model):
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.runtime.team_v2_review_command import coach_projection
    from liangjian_funnel.runtime.team_v2_review_facts import build_team_review_facts
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    source=sources/'files/a5-original'/f'{day}.json'
    audit_path=sources/'LOCAL_HASH_VERIFICATION.json'
    audit=json.loads(audit_path.read_bytes())
    if audit['status']!='PASS' or sum(r.get('canonical_verified',False) for r in audit['files'])!=6:
        raise ValueError('DRYRUN_ORIGINAL_COPY_NOT_VERIFIED')
    export=json.loads((sources/'RECEIPT.json').read_bytes())
    proof=[r for r in export['files'] if r.get('relative')=='a5-original/'+day+'.json']
    if len(proof)!=1 or hashlib.file_digest(source.open('rb'),'sha256').hexdigest()!=proof[0]['sha256']:
        raise ValueError('DRYRUN_ORIGINAL_FILE_HASH_INVALID')
    original=json.loads(source.read_bytes());legacy=original['facts'];source_hash=legacy['input_hash']
    if legacy['trade_date']!=day or legacy['review_kind']!='POST_CLOSE' or digest({k:v for k,v in legacy.items() if k!='input_hash'})!=source_hash or source_hash!=original['input_hash']:
        raise ValueError('DRYRUN_ORIGINAL_FACT_HASH_INVALID')
    observed=datetime.fromisoformat(original['created_at'])
    if observed.utcoffset() is None or observed.astimezone(TZ).date().isoformat()!=day:
        raise ValueError('DRYRUN_ORIGINAL_CLOCK_INVALID')
    clean={k:v for k,v in legacy.items() if k not in {'input_hash','review_history'}}
    clean['source_archive_input_sha256']=source_hash;clean['input_hash']=digest(clean)
    store=TeamMeetingStore(output/'team.db');journal=ModelJudgmentJournal(output/'model.db')
    account='HISTORICAL_DRYRUN_NO_TEAM_ACCOUNT'
    missing=dict(status='UNAVAILABLE',settlement=None,reason_code='NO_HISTORICAL_TEAM_ACCOUNT')
    full=build_team_review_facts(legacy_facts=clean,store=store,journal=journal,paper_snapshot=missing,
        settlement=None,account_id=account,trade_date=day,review_kind='POST_CLOSE',observed_at=observed)
    facts,domain=coach_projection(full)
    facts['dryrun_context']=dict(classification='POST_HOC_FROZEN_INPUT_MODEL_DRYRUN',
        original_fact_created_at=original['created_at'],original_fact_input_sha256=source_hash,
        historical_team_account='UNAVAILABLE_NOT_SIMULATED',natural_execution=False)
    facts.pop('input_hash');facts['input_hash']=digest(facts)
    counterexamples={r['symbol'] for r in facts.get('counterexamples',[]) if isinstance(r,dict) and r.get('symbol')}
    repository=TeamPromptRepository(ROOT/'prompts/team_v2')
    replacements=dict(A5_FACT_SNAPSHOT=facts,EVIDENCE_CATALOG=facts['citation_catalog'])
    prompt=repository.prepare('A5',replacements)
    run='team-a5-frozen-dryrun:'+day+':'+output.name
    envelope=dict(schema_version='team-envelope/1',run_id=run,stage_id='A5',source_trade_date=day,
        execution_trade_date=day,as_of=observed.isoformat(),knowledge_cutoff=observed.isoformat(),
        input_snapshot_ids=[facts['input_hash'],source_hash],config_version='team-v2/1',
        config_hash=hashlib.sha256((ROOT/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        prompt_version=prompt.role_version,model_name=model,market_regime='DATA_BLOCK',status='FROZEN',reason_codes=[])
    request=repository.prepare_messages('A5',replacements,required_envelope=envelope,session='POST_CLOSE')
    chars=sum(len(m['content']) for m in request.messages)
    if chars>250000:raise ValueError('A5_MODEL_CONTEXT_TOO_LARGE')
    prepared=dict(classification='POST_HOC_FROZEN_INPUT_MODEL_DRYRUN',trade_date=day,run_id=run,
        source_proof=proof[0],original_fact_input_sha256=source_hash,original_fact_created_at=original['created_at'],
        source_verification_sha256=hashlib.sha256(audit_path.read_bytes()).hexdigest(),
        source_context_time=observed.isoformat(),actual_prepared_at=datetime.now(TZ).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        facts=facts,signal_domain=domain,counterexample_domain=sorted(counterexamples),confirmed_defect_symbols=[],
        envelope=envelope,request_input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,
        prompt_sources=dict(prompt.source_hashes),prompt_chars=chars,original_report=original['report'],
        production_writes=0,customer_notifications=0,model_calls=0)
    save(output/'PREPARED.json',prepared)
    return prepared,request,journal


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
    p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve();scope=(ROOT/'artifacts/team-v2-20261016').resolve()
    if not output.is_relative_to(scope) or output.exists():raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    from liangjian_funnel.settings import Settings,load_dotenv
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_review_schema import validate_daily_review
    from liangjian_funnel.pipeline.team_v2_model import call_team_model
    env=load_dotenv(Path('D:/dev_A股/liangjian_funnel_workflow/.env'))
    allowed={'LIANGJIAN_MODEL_API_KEY','LIANGJIAN_MODEL_BASE_URL','LIANGJIAN_REVIEW_MODEL'}
    settings=Settings.from_env({k:v for k,v in env.items() if k in allowed},root=output)
    prepared,request,journal=prepare(a.sources.resolve(),output,a.day,settings.review_model)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',day=a.day,chars=prepared['prompt_chars'],input_sha256=request.input_hash)));return
    if not settings.model_api_key:raise ValueError('DRYRUN_MODEL_CREDENTIAL_MISSING')
    started=datetime.now(TZ)
    def validate(raw):
        return validate_daily_review(raw,prepared['facts']['citation_catalog'],required_envelope=prepared['envelope'],
            trade_date=a.day,review_kind='POST_CLOSE',signal_domain=prepared['signal_domain'],
            counterexample_domain=set(prepared['counterexample_domain']),metrics=prepared['facts']['metrics'])
    try:
        result=call_team_model(request,OpenAICompatibleModelClient(settings,max_attempts=1),journal,
            call_id='A5:'+prepared['run_id'],run_id=prepared['run_id'],model=settings.review_model,session='POST_CLOSE',
            source_trade_date=a.day,execution_trade_date=a.day,catalog=prepared['facts']['citation_catalog'],
            timeout_seconds=180,max_output_tokens=16384,validator=validate,clock=lambda:datetime.now(TZ),allow_model_call=True)
        summary=dict(status='PASS' if result.validation.valid else 'SCHEMA_FAILED',issues=list(result.validation.issues),
            validated=result.validation.valid,response_output_sha256=result.response.output_hash,model=settings.review_model,
            latency_ms=result.response.latency_ms,provider_attempts=result.response.attempts,
            actual_capture=result.receipt,model_calls=1)
        save(output/'FIELD_DIFF.json',dict(original_schema='LEGACY_A5',new_schema='TEAM_A5_204',
            schema_changed=True,fields=field_diff(prepared['original_report'],result.response.output)))
    except ValueError as exc:
        safe=str(exc) if str(exc).startswith(('TEAM_','A5_')) else 'DRYRUN_FAILED'
        summary=dict(status='MODEL_CALL_FAILED',reason_code=safe,model_calls=1)
    summary.update(day=a.day,actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),
        context_time=prepared['source_context_time'],input_sha256=request.input_hash,prompt_hash=request.prompt.sha256,
        source_head=prepared['head'],classification=prepared['classification'],historical_account='UNAVAILABLE',
        production_writes=0,customer_notifications=0,natural_acceptance=False)
    save(output/'RESULT.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k!='actual_capture'},ensure_ascii=True))
    return 0 if summary['status']=='PASS' else 2


if __name__=='__main__':sys.exit(main())
