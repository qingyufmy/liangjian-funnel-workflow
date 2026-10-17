"""One original frozen A4 batch through the real coordinator, not a six-day PASS.

Historical Team PM/account records do not exist. Start an explicitly isolated
paper account, keep missing PM permission blocked, and never invent positions.
"""
import argparse
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal as D
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import zlib
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a5_frozen_dryrun import model_settings,save
from liangjian_funnel.evaluation.team_v2_replay_source import activation_reason,sha
from liangjian_funnel.runtime.team_v2_sources import stamp
TZ=ZoneInfo('Asia/Shanghai')


def prepare(source_pack,output,at,settings):
    from liangjian_funnel.data.mootdx import MinuteBar
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.review.observability import _cluster
    from liangjian_funnel.runtime.broker.local_paper import create_broker
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.runtime.risk_engine import AccountRisk,PlanRisk,RiskLimits,build_risk_envelope
    from liangjian_funnel.runtime.team_v2_minute_projection import project_closed_minute
    from liangjian_funnel.runtime.team_v2_quote_evidence import replay_quote_from_closed_bar
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.runtime.team_v2_tradability import production_monitor_tradability
    from liangjian_funnel.runtime.trader_guardrails import is_closed_trader_tick
    from liangjian_funnel.workflow import _intraday_market_context
    at=stamp(at);cutoff=at-timedelta(minutes=1);day=at.date().isoformat()
    if at.second or at.microsecond or not is_closed_trader_tick(cutoff):raise ValueError('REPLAY_CLOSED_5M_DECISION_REQUIRED')
    receipt=json.loads((source_pack/'PREPARATION.json').read_bytes());archive=source_pack/'frames.jsonl.gz'
    if (receipt['status']!='SOURCE_PREPARED_NOT_EXECUTED'
            or hashlib.file_digest(archive.open('rb'),'sha256').hexdigest()!=receipt['archive_sha256']):
        raise ValueError('REPLAY_PREPARED_ARCHIVE_HASH_INVALID')
    frames=[];terminal=set()
    with gzip.open(archive,'rt',encoding='utf-8') as stream:
        for line in stream:
            frame=json.loads(line)
            if (frame['kind']=='plan_minute' and frame['decision_at']<=at.isoformat()
                    and frame['original_action'] in {'PLAN_INVALIDATED','PLAN_EXPIRED'}):
                terminal.add(frame['plan_id'])
            if frame['kind']=='plan_minute' and frame['decision_at']==at.isoformat():
                if sha({k:v for k,v in frame.items() if k!='frame_sha256'})!=frame['frame_sha256']:
                    raise ValueError('REPLAY_FRAME_HASH_INVALID')
                frames.append(frame)
    if not frames:raise ValueError('REPLAY_ORIGINAL_BATCH_MISSING')
    source=Path(receipt['source_path'])
    if hashlib.file_digest(source.open('rb'),'sha256').hexdigest()!=receipt['source_database_sha256']:
        raise ValueError('REPLAY_FROZEN_DATABASE_HASH_MISMATCH')
    calendar=ExchangeTradingCalendar();limits=RiskLimits.from_yaml(ROOT/'config/risk_limits.yaml')
    replay_clock=[at.replace(hour=8,minute=33)]
    broker=create_broker('local_paper',database=output/'paper.db',paper_config=ROOT/'config/paper_broker.yaml',
        risk_config=ROOT/'config/risk_limits.yaml',clock=lambda:replay_clock[0],calendar=calendar,run_id='frozen-probe:'+output.name)
    broker.open_risk_day(day)
    replay_clock[0]=at
    account=AccountRisk(limits.initial_capital,limits.initial_capital,limits.initial_capital,limits.initial_capital)
    plans={};envelopes={};bars={};refs={};facts={};original={};skipped=[]
    live=dict(status='DATA_BLOCKED',reason_code='FROZEN_PROBE_LIVE_MARKET_NOT_BOUND',
        emotion_state='DATA_BLOCK',leader_open=False,quote_basis='REPLAY_BAR_DERIVED')
    with closing(sqlite3.connect(source.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        for frame in frames:
            if frame['status']!='SOURCE_BOUND_RULES_PENDING':
                skipped.append(dict(plan_id=frame['plan_id'],reasons=frame['reason_codes']));continue
            row=dict(db.execute('SELECT * FROM execution_plans WHERE plan_id=?',(frame['plan_id'],)).fetchone())
            if sha(row)!=frame['original_plan_row_sha256']:raise ValueError('REPLAY_PLAN_ROW_HASH_INVALID')
            raw=json.loads(row['payload_json']);pid=row['plan_id'];symbol=row['symbol'];cluster,_=_cluster(row)
            inactive=activation_reason(frame,row,terminal_seen=pid in terminal)
            if inactive or cluster=='UNKNOWN':
                skipped.append(dict(plan_id=pid,reasons=[inactive or 'REPLAY_CLUSTER_UNPROVEN']));continue
            snapshot=db.execute("SELECT * FROM minute_decision_snapshots WHERE snapshot_id=? AND symbol=? AND interval='1m'",
                (frame['original_snapshot_id'],symbol)).fetchone()
            payload=zlib.decompress(snapshot['payload_zlib'])
            if hashlib.sha256(payload).hexdigest()!=frame['source']['original_snapshot_sha256']:
                raise ValueError('REPLAY_SNAPSHOT_HASH_INVALID')
            one=tuple(MinuteBar.model_validate(v) for v in json.loads(payload))
            projected=project_closed_minute(one,(),observed_at=at)
            bar=projected.get('paper_bar')
            if bar is None:
                skipped.append(dict(plan_id=pid,reasons=projected['reason_codes']));continue
            quote=replay_quote_from_closed_bar(one[-1],decision_as_of=at)
            proof=production_monitor_tradability(settings,symbol=symbol,now=at,bar=bar,quote=quote,plan=raw,minute_bars=one)
            known=proof.get('status')=='KNOWN'
            if known:bars[symbol]=replace(bar,tradability_known=True,halted=proof['halted'],
                limit_up=proof['limit_up'],limit_down=proof['limit_down'])
            plan=PlanRisk(pid,symbol,cluster,'ACTIVE_TODAY',stamp(row['valid_from']),stamp(row['expires_at']),
                quote.last,D(str(raw['stop_level'])),D(str(raw['trigger_low'])),D(str(raw['trigger_high'])),D(str(raw['no_chase'])),
                invalidation_price=D(str(raw['stop_level'])),tradable=known and not proof.get('halted',True),data_ready=known)
            envelope=build_risk_envelope(plan,account,(),limits,now=at,regime='DATA_BLOCK')
            # This probe must not fabricate a historical A3 allocation merely
            # to generate trades. Formal funded replay remains a separate gate.
            envelopes[pid]=replace(envelope,eligible=False,risk_unit='NONE',max_shares_cap=0,
                blocked_reasons=tuple(dict.fromkeys((*envelope.blocked_reasons,'REPLAY_PM_ALLOCATION_UNAVAILABLE'))))
            plans[pid]=plan
            event=json.loads(frame['original_events'][0]['payload_json']);strategy=event.get('strategy') or {}
            refs[pid]={**{k:'MET' for k in strategy.get('met_conditions',[])},
                       **{k:'UNMET' for k in strategy.get('unmet_conditions',[])}}
            context=_intraday_market_context(symbol,one,(),current=at,execution_cutoff=cutoff,live_market_state=live)
            context.update(realtime_quote=frame['source']['quote'],market_authority='REPLAY_ONLY_NO_LIVE_AUTHORITY',
                original_tradability_contract=proof)
            facts[pid]=dict(plan=raw,closed_minute_context=context,quote_basis='REPLAY_BAR_DERIVED')
            original[pid]=dict(action=frame['original_action'],reason_code=frame['original_reason_code'],
                frame_sha256=frame['frame_sha256'])
        db.rollback()
    if not plans or not bars:raise ValueError('REPLAY_NO_ORIGINAL_PROJECTABLE_BATCH')
    repository=TeamPromptRepository(ROOT/'prompts/team_v2')
    catalog=dict(categories=['MARKET','PLAN','STOCK_TECH'],items=[
        dict(id='REPLAY:MARKET',category='MARKET',value=live),
        *(dict(id='REPLAY:PLAN:'+pid,category='PLAN',value=facts[pid]['plan']) for pid in sorted(plans)),
        *(dict(id='REPLAY:TECH:'+pid,category='STOCK_TECH',value=facts[pid]['closed_minute_context']) for pid in sorted(plans))])
    values={key:dict(status='UNAVAILABLE') for key in repository.document('A4').placeholders}
    context=dict(classification='POST_HOC_FROZEN_CORRECTNESS_REPLAY',quote_basis='REPLAY_BAR_DERIVED',
        source_pack_sha256=receipt['archive_sha256'],source_database_sha256=receipt['source_database_sha256'],
        historical_team_account='UNAVAILABLE_SEPARATE_ISOLATED_INITIAL_ACCOUNT',funded_replay_gate='NOT_RUN')
    values.update(EVIDENCE_CATALOG=catalog,TIME_RULES={'NO_NEW_ENTRY_AFTER':limits.no_new_entry_after},
        LIVE_MARKET_STATE=live,MINUTE_SOURCE=context)
    run='team-a4-frozen-probe:'+output.name;decision='REPLAY:'+at.strftime('%Y%m%dT%H%M');snapshot_id=sha(original)
    required=dict(schema_version='team-envelope/1',run_id=run,stage_id='A4',source_trade_date=day,execution_trade_date=day,
        as_of=at.isoformat(),knowledge_cutoff=at.isoformat(),input_snapshot_ids=[snapshot_id],config_version='team-v2/1',
        config_hash=hashlib.sha256((ROOT/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        prompt_version=repository.prepare('A4',values,session='INTRADAY').role_version,
        model_name=settings.review_model,market_regime='DATA_BLOCK',status='FROZEN',reason_codes=[])
    header=dict(context,actual_prepared_at=datetime.now(TZ).isoformat(),context_time=at.isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),original_actions=original,
        original_batch_count=len(frames),projected_plan_count=len(plans),skipped_original_rows=skipped,
        required_envelope=required,production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    args=dict(repository=repository,journal=ModelJudgmentJournal(output/'model.db'),store=TeamMeetingStore(output/'team.db'),
        broker=broker,decision_id=decision,minute_snapshot_id=snapshot_id,call_id='A4:'+run,account_id='replay-paper',
        run_id=run,model=settings.review_model,trade_date=day,source_trade_date=day,clock=lambda:at,
        capture_clock=lambda:datetime.now(TZ),decision_at=at,closed_5m_end=cutoff,mark_as_of=cutoff,
        required_envelope=required,replacements=values,active_plan_ids=set(plans),all_plans=plans,envelopes=envelopes,
        held_plan_ids={},position_snapshot=[],tech_reference=refs,limits=limits,regime='DATA_BLOCK',
        live_market_ready=False,snapshot_ready=True,mode='control',calendar=calendar,timeout_seconds=30,
        max_output_tokens=16384,allow_model_call=True,pm_meeting_id=None)
    return args,bars,header


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-pack',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--decision-at',required=True)
    p.add_argument('--execute-real-model',action='store_true');a=p.parse_args();output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('REPLAY_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
    settings,provider=model_settings(output,'vm-runtime');args,bars,header=prepare(a.source_pack,output,a.decision_at,settings)
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',plans=len(args['all_plans']))));return 0
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/'PROVIDER_FAILURE.json',dict(reason_code=exc.reason_code,status_code=exc.status_code,
                    attempts=exc.attempts,actual_at=datetime.now(TZ).isoformat()))
                raise
    started=datetime.now(TZ);args['client']=AuditedClient(settings,max_attempts=1)
    result=run_control_minute(session_kwargs=args,closed_bars=bars,deadline_monotonic=time.monotonic()+47)
    trader=result.get('trader') or {};valid=bool(trader.get('model_validated'))
    with closing(sqlite3.connect((output/'model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        raw=db.execute('SELECT output_json,output_hash,latency_ms,captured_at,issues_json FROM model_calls').fetchone()
    if raw:
        responses=json.loads(raw['output_json'])
        save(output/'DISAGREEMENT.json',[dict(plan_id=r['plan_id'],new_trader_action=r['action'],
            original_deterministic=header['original_actions'][r['plan_id']],quote_basis='REPLAY_BAR_DERIVED')
            for r in responses.get('signals',[]) if r.get('plan_id') in header['original_actions']])
    body=dict(status='PASS_PROBE_ONLY' if valid else 'SCHEMA_OR_TRANSPORT_FAILED',
        actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),
        classification=header['classification'],source_head=header['head'],context_time=header['context_time'],
        quote_basis='REPLAY_BAR_DERIVED',provider_identity=provider,result=result,
        latency_ms=raw['latency_ms'] if raw else None,issues=json.loads(raw['issues_json']) if raw else [],
        model_calls=1,orders=len(args['broker'].get_orders(args['trade_date'])),
        reconcile=args['broker'].reconcile(),production_writes=0,customer_notifications=0,
        release_gate='NOT_PASSED_ONE_BATCH_NO_HISTORICAL_PM_OR_FUNDED_CURVE')
    save(output/'RESULT.json',body)
    print(json.dumps({k:v for k,v in body.items() if k!='result'},ensure_ascii=True))
    return 0 if valid else 2


if __name__=='__main__':sys.exit(main())
