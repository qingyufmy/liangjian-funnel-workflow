"""0315 original v1 position fixtures, PM then A6; no paper ledger or orders."""
import argparse,hashlib,json,sqlite3,subprocess,sys
from contextlib import closing
from datetime import datetime
from decimal import Decimal as D
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import model_settings,save,field_diff
TZ=ZoneInfo('Asia/Shanghai');BASIS='FIXTURE_FROM_V1_PAPER_NOT_TEAM_ACCOUNT'
DAYS=('2026-10-14','2026-10-15','2026-10-16')


def fixtures(directory,bars_db,expected_bars_sha):
    from liangjian_funnel.review.observability import _cluster
    path=directory/'SOURCE.json';raw=path.read_bytes();proof=json.loads((directory/'RECEIPT.json').read_bytes())
    if hashlib.sha256(raw).hexdigest()!=proof['sha256']:raise ValueError('V1_FIXTURE_SOURCE_HASH_MISMATCH')
    with bars_db.open('rb') as stream:
        if hashlib.file_digest(stream,'sha256').hexdigest()!=expected_bars_sha:raise ValueError('V1_FIXTURE_BARS_HASH_MISMATCH')
    original=json.loads(raw);rows=original['rows'];account='paper:lane_1'
    positions={p['symbol']:p for p in rows['positions'] if p['account_id']==account}
    plans={p['plan_id']:p for p in rows['plan_rows']};result={}
    with closing(sqlite3.connect(bars_db.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        for day in DAYS:
            at=datetime.fromisoformat(day+'T16:30:00+08:00');qty={};selected=[]
            for fill in rows['fills']:
                if fill['account_id']!=account or datetime.fromisoformat(fill['bar_end'])>at:continue
                if fill['action'] not in {'BUY','SELL'}:raise ValueError('V1_FIXTURE_FILL_ACTION_UNKNOWN')
                qty[fill['symbol']]=qty.get(fill['symbol'],0)+fill['qty']*(1 if fill['action']=='BUY' else -1)
                if qty[fill['symbol']]<0:raise ValueError('V1_FIXTURE_NEGATIVE_HOLDING')
            for symbol,n in qty.items():
                if not n:continue
                p=positions.get(symbol)
                # Historical cost is not reconstructed or guessed. Only an
                # unchanged original v1 holding with no later fill may be used.
                if (p is None or p['total_qty']!=n or datetime.fromisoformat(p['updated_at'])>at
                        or any(f['account_id']==account and f['symbol']==symbol and datetime.fromisoformat(f['bar_end'])>at
                            for f in rows['fills'])):raise ValueError('V1_FIXTURE_ORIGINAL_POSITION_NOT_PROVEN')
                daily={}
                for _,fetched,payload,pin in db.execute('SELECT bar_timestamp,fetched_at,payload_json,content_hash FROM daily_bars WHERE symbol=?',(symbol,)):
                    if hashlib.sha256(payload.encode()).hexdigest()!=pin:raise ValueError('V1_DAILY_PAYLOAD_HASH_INVALID')
                    if datetime.fromisoformat(fetched)>at:continue
                    data=json.loads(payload);bar_day=datetime.fromtimestamp(data['date_ms']/1000,TZ).date().isoformat()
                    if bar_day<=day:daily[bar_day]=(data,pin)
                dates=sorted(daily)
                if day not in daily or len(dates)<2:raise ValueError('V1_FIXTURE_DAY_RETURN_NOT_PROVEN')
                prior=max(d for d in dates if d<day);close=D(str(daily[day][0]['close_price']));previous=D(str(daily[prior][0]['close_price']))
                cluster,_=_cluster(plans[p['plan_id']])
                if cluster=='UNKNOWN':raise ValueError('V1_FIXTURE_CLUSTER_UNPROVEN')
                selected.append(dict(symbol=symbol,qty=n,average_price=str(p['avg_cost']),sellable_qty=p['sellable_qty'],
                    plan_id=p['plan_id'],cluster=cluster,close_price=str(close),
                    day_change_pct=str((close/previous-1)*100),basis=BASIS,
                    original_position=p,price_proof=dict(current_sha256=daily[day][1],previous_sha256=daily[prior][1])))
            events=[e for e in rows['risk_events'] if e['minute_end'][:10]==day]
            result[day]=dict(basis=BASIS,positions=selected,risk_events=events,
                source_sha256=proof['sha256'],bars_database_sha256=expected_bars_sha,
                position_status='NONEMPTY_ORIGINAL_V1_HOLDING' if selected else 'EMPTY_ORIGINAL_V1_HOLDING',
                historical_team_account='UNAVAILABLE',original_session='NO_ORIGINAL_SESSION')
    if not any(x['positions'] for x in result.values()):raise ValueError('V1_FIXTURE_ALL_EMPTY_REQUIRE_RECENT_NONEMPTY_DAY')
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--bars-db',type=Path,required=True);p.add_argument('--bars-db-sha256',required=True)
    p.add_argument('--day',choices=DAYS,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--execute-real-model',action='store_true');a=p.parse_args();output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    all_fixtures=fixtures(a.source,a.bars_db,a.bars_db_sha256);output.mkdir(parents=True);save(output/'FIXTURES.json',all_fixtures)
    fixture=all_fixtures[a.day];settings,provider=model_settings(output,'vm-runtime')
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore,canonical
    from liangjian_funnel.runtime.team_v2_eod import run_team_eod
    from liangjian_funnel.runtime.risk_engine import RiskLimits
    limits=RiskLimits.from_yaml(ROOT/'config/risk_limits.yaml');at=datetime.fromisoformat(a.day+'T16:30:00+08:00')
    # No historical Team morning/A6 exists. Its normal fail-closed cap is zero;
    # position format and overnight direction still exercise the nonempty case.
    cap=D(0);slots=min(limits.max_new_entries,max(0,limits.max_positions-len(fixture['positions'])))
    clusters={r['cluster']:min(limits.max_positions,2) for r in fixture['positions']}
    account=dict(status='UNAVAILABLE',basis=BASIS,reason_code='NO_HISTORICAL_TEAM_ACCOUNT_NO_TEAM_NAV')
    market=dict(status='UNAVAILABLE',basis=BASIS,reason_code='NO_TEAM_EOD_MARKET_OR_MORNING_PERMISSION')
    catalog=dict(categories=['ACCOUNT','MARKET','POSITION','RISK_EVENT'],items=[
        dict(id='EOD:ACCOUNT',category='ACCOUNT',value=account),dict(id='EOD:MARKET',category='MARKET',value=market),
        *[dict(id='EOD:POSITION:'+r['symbol'],category='POSITION',value=r) for r in fixture['positions']],
        *[dict(id=e['event_id'],category='RISK_EVENT',value=e) for e in fixture['risk_events']]])
    repository=TeamPromptRepository(ROOT/'prompts/team_v2');run='team-eod-fixture:'+output.name;values={};envelopes={}
    pin=hashlib.sha256((ROOT/'config/risk_limits.yaml').read_bytes()).hexdigest()
    for role,session in (('A3','PM_POST_CLOSE'),('A6','POST_CLOSE')):
        value={key:dict(status='UNAVAILABLE') for key in repository.document(role).placeholders}
        value.update(ACCOUNT_SNAPSHOT=account,POSITIONS=fixture['positions'],MARKET_STATE=market,
            RISK_EVENTS_TODAY=fixture['risk_events'],THEME_REVIEWS=dict(status='UNAVAILABLE'),EVIDENCE_CATALOG=catalog,
            RISK_LIMITS=dict(config_sha256=pin),RISK_SESSION='POST_CLOSE')
        prompt=repository.prepare(role,value,session=session)
        envelopes[role]=dict(schema_version='team-envelope/1',run_id=run,stage_id=role,source_trade_date=a.day,
            execution_trade_date=a.day,as_of=at.isoformat(),knowledge_cutoff=at.isoformat(),
            input_snapshot_ids=[fixture['source_sha256']],config_version='team-v2/1',config_hash=pin,
            prompt_version=prompt.role_version,model_name=settings.review_model,market_regime='DATA_BLOCK',status='FROZEN',reason_codes=[])
        values[role]=value
    save(output/'PREPARED.json',dict(classification=BASIS,actual_at=datetime.now(TZ).isoformat(),day=a.day,
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),positions=len(fixture['positions']),
        fixture_sha256=hashlib.sha256(canonical(fixture).encode()).hexdigest(),provider=provider,
        engine_utilization_cap='0',capacity_basis='EXISTING_EOD_FAIL_CLOSED_MISSING_TEAM_MORNING_PERMISSION',
        model_budgets_seconds={'PM':50,'A6':50},paper_ledger_writes=0,original_session='NO_ORIGINAL_SESSION'))
    if not a.execute_real_model:print(json.dumps(dict(status='PREPARED_NOT_CALLED',positions=len(fixture['positions']))));return 0
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    class Client(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/('PROVIDER_FAILURE_'+str(time_calls[0])+'.json'),dict(reason_code=exc.reason_code,
                    status_code=exc.status_code,attempts=exc.attempts));raise
            finally:time_calls[0]+=1
    time_calls=[0];started=datetime.now(TZ);journal=ModelJudgmentJournal(output/'model.db')
    result=run_team_eod(repository=repository,client=Client(settings,max_attempts=1),journal=journal,
        store=TeamMeetingStore(output/'research-meetings.db'),meeting_id=run,pm_call_id='A3:'+run,risk_call_id='A6:'+run,
        account_id='FIXTURE_V1_NOT_TEAM_ACCOUNT',run_id=run,model=settings.review_model,trade_date=a.day,
        source_trade_date=a.day,clock=lambda:at,capture_clock=lambda:datetime.now(TZ),
        pm_required_envelope=envelopes['A3'],risk_required_envelope=envelopes['A6'],pm_replacements=values['A3'],
        risk_replacements=values['A6'],positions=fixture['positions'],risk_events=fixture['risk_events'],catalog=catalog,
        engine_utilization_cap=cap,capacity_slots=slots,cluster_caps=clusters,timeout_seconds=50,
        max_output_tokens=settings.model_max_output_tokens,allow_model_call=True)
    with closing(journal._connect()) as db:captures=[dict(role=r[0],session=r[1],latency_ms=r[2],validated=bool(r[3]),output_sha256=r[4])
        for r in db.execute('SELECT role,session,latency_ms,validated,output_hash FROM model_calls ORDER BY captured_at')]
    body=dict(status='PASS_FIXTURE_FORMAT_LATENCY_ONLY' if result['status']=='READY' else 'FAILED_FIXTURE_SESSION',
        result=result,classification=BASIS,positions=len(fixture['positions']),source_day=a.day,captures=captures,
        actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),model_calls=time_calls[0],
        production_writes=0,paper_ledger_writes=0,original_session='NO_ORIGINAL_SESSION',natural_acceptance=False)
    save(output/'RESULT.json',body);save(output/'FIELD_DIFF.json',dict(original_session='NO_ORIGINAL_SESSION',fields=field_diff({},result)))
    print(json.dumps({k:v for k,v in body.items() if k!='result'},ensure_ascii=True));return 0 if result['status']=='READY' else 2

if __name__=='__main__':sys.exit(main())
