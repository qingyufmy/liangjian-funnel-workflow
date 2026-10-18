"""0315 exactly one research-only 10-13 13:26 candidate; no admission.

Engine-only envelopes are explicitly NOT historical PM/A6 permission. This
single format/latency probe cannot establish CONTROL, a funded account or PnL.
"""
import argparse,hashlib,json,subprocess,sys,time
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a4_frozen_probe import prepare
from run_team_a5_frozen_dryrun import model_settings,save
from liangjian_funnel.runtime.team_v2_sources import stamp
TZ=ZoneInfo('Asia/Shanghai')
PID='2026-10-12-close-49c3a76441d9:lane_1:faada0f50dc057e5'
AT='2026-10-13T13:26:00+08:00'
CLASSIFICATION='RESEARCH_ENVELOPE_ENGINE_ONLY_NO_PM_NO_A6'


def prepare_one(source_pack,inventory,output,settings):
    from liangjian_funnel.evaluation.team_v2_replay_batches import read_sealed_source
    from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
    from liangjian_funnel.evaluation.ablation.exporter import _audit_helpers
    from liangjian_funnel.runtime.risk_engine import AccountRisk,build_risk_envelope
    from liangjian_funnel.runtime.strategies import evaluate_strategy
    from liangjian_funnel.workflow import _intraday_market_context
    proof=json.loads(inventory.read_bytes());chosen=proof['deterministic_probe_selection']
    if len(chosen)!=1 or chosen[0]['plan_id']!=PID or chosen[0]['decision_at']!=AT:
        raise ValueError('AUTHOR315_EXACT_SINGLE_CANDIDATE_REQUIRED')
    for name,pin in proof['source_hashes'].items():
        with (ROOT/name).open('rb') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest()!=pin:raise ValueError('PROBE_KERNEL_SOURCE_CHANGED')
    args,_,header=prepare(source_pack,output,AT,settings,author298_inputs=True)
    receipt,records,rows=read_sealed_source(source_pack)
    frame=next(r for r in records if r.get('plan_id')==PID and r.get('decision_at')==AT)
    if frame['frame_sha256']!=chosen[0]['frame_sha256']:raise ValueError('PROBE_FRAME_CHANGED')
    market_file=next(m for m in proof['market_files'] if m['sha256']==chosen[0]['market_sha256'])
    raw_market=Path(market_file['path']).read_bytes()
    if hashlib.sha256(raw_market).hexdigest()!=chosen[0]['market_sha256']:raise ValueError('PROBE_MARKET_CHANGED')
    market=json.loads(raw_market);at=stamp(AT);cutoff=args['closed_5m_end'];raw=json.loads(rows[PID]['payload_json'])
    frozen=json.loads(frame['original_events'][0]['payload_json'])['strategy']
    with FrozenMinuteMaterializer(receipt['source_path'],expected_sha256=receipt['source_database_sha256'],
            plans=rows,settings=SimpleNamespace(root=ROOT)) as reader:
        bars,window_proof=reader._window(frame,rows[PID])
    _,overlay,_=_audit_helpers(None)
    context=overlay(_intraday_market_context(rows[PID]['symbol'],bars,(),current=cutoff,live_market_state=market),frozen)
    price=frozen.get('live_entry_price')
    if isinstance(price,(int,float)) and not isinstance(price,bool) and price>0:
        context['realtime_quote']=dict(available=True,price=price,source='FROZEN_KERNEL_LIVE_ENTRY_PRICE_NOT_FULL_QUOTE')
    result=evaluate_strategy(raw,bars,now=cutoff,decision_time=at,position=None,market_context=context).model_dump(mode='json')
    from liangjian_funnel.evaluation.team_v2_replay_source import sha
    if result['action']!='BUY_SIGNAL' or sha(result)!=chosen[0]['current_result_sha256']:
        raise ValueError('PROBE_CURRENT_KERNEL_CANDIDATE_NOT_REPRODUCED')
    plan=args['all_plans'][PID];limits=args['limits'];regime=raw['market_regime']
    account=AccountRisk(limits.initial_capital,limits.initial_capital,limits.initial_capital,limits.initial_capital)
    risk=build_risk_envelope(plan,account,(),limits,now=at,regime=regime)
    if not risk.eligible:raise ValueError('PROBE_ORIGINAL_ENGINE_ENVELOPE_BLOCKED')
    projection=args['input_projection']
    args.update(all_plans={PID:plan},envelopes={PID:risk},active_plan_ids={PID},
        tech_reference={PID:{**{k:'MET' for k in result['met_conditions']},**{k:'UNMET' for k in result['unmet_conditions']}}},
        capture_only=True,review_candidate_actions={PID:'BUY_SIGNAL'},review_position_actions={},
        regime=regime,live_market_ready=False,pm_meeting_id=None)
    projection.update(plan_rows=[r for r in projection['plan_rows'] if r['plan_id']==PID],
        contexts={PID:projection['contexts'][PID]},model_contexts={PID:projection['model_contexts'][PID]},
        evaluations={PID:result},atr5m={PID:projection['atr5m'][PID]})
    args['required_envelope']['market_regime']=regime
    args['replacements']['LIVE_MARKET_STATE']=dict(market,authority=CLASSIFICATION,
        historical_team_pm='UNAVAILABLE',historical_team_a6='UNAVAILABLE')
    args['replacements']['MINUTE_SOURCE'].update(research_envelope_basis=CLASSIFICATION)
    args['replacements']['EVIDENCE_CATALOG']['items'][0]['value']=args['replacements']['LIVE_MARKET_STATE']
    from liangjian_paper_broker.ledger import dump
    save(output/'RESEARCH_PREPARED.json',json.loads(dump(dict(classification=CLASSIFICATION,head=header['head'],
        candidate=chosen[0],actual_prepared_at=datetime.now(TZ).isoformat(),engine_envelope=asdict(risk),
        envelope_regime_basis='ORIGINAL_FROZEN_PLAN_MARKET_REGIME_NOT_LIVE_PM_OR_A6',
        window_proof=window_proof,historical_pm_a6='UNAVAILABLE',capture_only=True,admission_calls_permitted=0))))
    return args


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-pack',type=Path,required=True)
    p.add_argument('--inventory',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--execute-real-model',action='store_true');a=p.parse_args();output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('PROBE_NEW_OWNED_OUTPUT_REQUIRED')
    output.mkdir(parents=True);settings,provider=model_settings(output,'vm-runtime')
    args=prepare_one(a.source_pack,a.inventory,output,settings)
    if not a.execute_real_model:print(json.dumps(dict(status='PREPARED_NOT_CALLED')));return 0
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.runtime import team_v2_trader_session as session
    def forbidden_admission(**kw):raise AssertionError('RESEARCH_PROBE_ADMISSION_FORBIDDEN')
    session.admit_trader_signal=forbidden_admission
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                save(output/'PROVIDER_FAILURE.json',dict(reason_code=exc.reason_code,status_code=exc.status_code,
                    attempts=exc.attempts,actual_at=datetime.now(TZ).isoformat()));raise
    args['client']=AuditedClient(settings,max_attempts=1);started=datetime.now(TZ)
    args['deadline_monotonic']=time.monotonic()+31
    result=session.run_trader_session(**args)
    with closing(args['journal']._connect()) as db:
        raw=db.execute('SELECT latency_ms FROM model_calls').fetchone()
    latency=raw[0] if raw else None;valid=result['model_validated'];orders=args['broker'].get_orders(args['trade_date'])
    if orders or result['admissions']:raise AssertionError('RESEARCH_PROBE_UNEXPECTED_ADMISSION')
    body=dict(status='PASS_SINGLE_FORMAT_LATENCY_PROBE_ONLY' if valid and latency is not None and latency<=30000 else 'FAILED_STOP_NO_MORE_SAMPLES',
        classification=CLASSIFICATION,actual_started_at=started.isoformat(),actual_finished_at=datetime.now(TZ).isoformat(),
        source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        result=result,latency_ms=latency,model_calls=1,admission_calls=0,orders=0,production_writes=0,
        conditional_sunday_review=bool(valid and latency is not None and latency<=30000),
        CONTROL_GATE='NOT_PASSED',funded_account=False,three_day_business_gate='NOT_PASSED',returns='NOT_CLAIMED')
    save(output/'RESULT.json',body)
    print(json.dumps({k:v for k,v in body.items() if k!='result'},ensure_ascii=True))
    return 0 if body['conditional_sunday_review'] else 2


if __name__=='__main__':sys.exit(main())
