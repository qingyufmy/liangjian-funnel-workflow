"""One authorized original frozen batch; real shard HTTP, never funded PASS."""
from contextlib import closing
from datetime import datetime
import argparse,hashlib,json,sqlite3,subprocess,sys,threading,time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a4_frozen_probe import prepare
from run_team_a5_frozen_dryrun import model_settings,save


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);a=p.parse_args();output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    settings,provider=model_settings(output,'vm-runtime')
    args,bars,source=prepare(ROOT/'artifacts/team-v2-20261016/correctness-source-20261017T0909',output,
        '2026-10-13T10:01:00+08:00',settings,author298_inputs=True)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
    failures=[];lock=threading.Lock()
    class Client(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                with lock:failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts))
                raise
    args['client']=Client(settings,max_attempts=1,thinking_enabled=False)
    started=datetime.now(ZoneInfo('Asia/Shanghai'));begin=time.monotonic()
    result=run_control_minute(session_kwargs=args,closed_bars=bars,deadline_monotonic=begin+47)
    elapsed=time.monotonic()-begin
    with closing(sqlite3.connect((output/'model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        calls=[dict(row) for row in db.execute('SELECT call_id,input_hash,output_hash,latency_ms,validated,issues_json FROM model_calls ORDER BY call_id')]
        claims=[dict(row) for row in db.execute('SELECT call_id,state,failure_code FROM model_call_claims ORDER BY call_id')]
    trader=result.get('trader') or {};parts=trader.get('shards',[])
    valid=bool(parts) and all(part['model_validated'] for part in parts)
    gate=valid and len(calls)==len(parts) and all(row['latency_ms']<=30000 for row in calls) and elapsed<=47
    body=dict(status='PASS_PROBE_ONLY' if gate else 'SCHEMA_LATENCY_OR_BUDGET_FAILED',actual_started_at=started.isoformat(),
        actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),source_head=source['head'],
        source_trade_date='2026-10-13',context_time='2026-10-13T10:01:00+08:00',classification='POST_HOC_FROZEN_CORRECTNESS_REPLAY',
        quote_basis='REPLAY_BAR_DERIVED',provider_identity=provider,model_validated=valid,model_calls=calls,claims=claims,
        provider_failures=failures,elapsed_seconds=elapsed,global_budget_seconds=47,per_shard_budget_seconds=30,
        thinking_enabled=False,first_packet_latency_ms=None,
        first_packet_unavailable_reason='EXISTING_NON_STREAMING_TRANSPORT_NO_TTFB_CAPTURE',
        original_scope=44,projected_scope=43,parts=parts,coordinator_result=result,production_writes=0,
        customer_notifications=0,orders=len(args['broker'].get_orders(args['trade_date'])),
        formal_six_day_gate='NOT_RUN',historical_account='UNAVAILABLE_SEPARATE_INITIAL_ACCOUNT',natural_acceptance=False)
    save(output/'RESULT.json',body)
    print(json.dumps({k:body[k] for k in ('status','actual_finished_at','model_validated','elapsed_seconds','orders')}))
    print(json.dumps(dict(model_responses=len(calls),claims=len(claims),latencies=[row['latency_ms'] for row in calls],
        issues=[row['issues_json'] for row in calls],provider_failures=failures)))
    return 0 if gate else 2


if __name__=='__main__':sys.exit(main())
