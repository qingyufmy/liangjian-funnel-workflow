"""Actual sharded renderer, offline only; stop before any model claim/HTTP."""
from contextlib import closing
from datetime import datetime
import argparse,gzip,hashlib,json,sqlite3,subprocess,sys,threading,time
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a4_frozen_probe import prepare
from run_team_a5_frozen_dryrun import save
from profile_team_a4_frozen_input import describe
from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute


class CapturedBeforeClaim(Exception):pass


class Capture:
    def __init__(self,repository):self.repository=repository;self.rows=[];self.lock=threading.Lock()
    def prepare_messages(self,role,values,**kwargs):
        request=self.repository.prepare_messages(role,values,**kwargs)
        with self.lock:self.rows.append((values,request))
        raise CapturedBeforeClaim()


class NoModel:
    def complete(self,*args,**kwargs):raise AssertionError('OFFLINE_PROFILE_MUST_NOT_CALL_PROVIDER')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();output=args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_DIRECTORY_REQUIRED')
    started=datetime.now(ZoneInfo('Asia/Shanghai'));head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    output.mkdir(parents=True)
    kw,bars,source=prepare(ROOT/'artifacts/team-v2-20261016/correctness-source-20261017T0909',output,
        '2026-10-13T10:01:00+08:00',SimpleNamespace(root=ROOT,review_model='deepseek-v4-pro-0813'),author298_inputs=True)
    capture=Capture(kw['repository']);kw.update(repository=capture,client=NoModel())
    result=run_control_minute(session_kwargs=kw,closed_bars=bars,deadline_monotonic=time.monotonic()+47)
    with closing(sqlite3.connect((output/'model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        calls=db.execute('SELECT count(*) FROM model_calls').fetchone()[0];claims=db.execute('SELECT count(*) FROM model_call_claims').fetchone()[0]
    if calls or claims:raise AssertionError('OFFLINE_PROFILE_MODEL_CLAIM_CREATED')
    rows=[]
    for values,request in sorted(capture.rows,key=lambda row:row[0]['DECISION_ID']):
        from liangjian_funnel.runtime.team_v2_compact_bars import decode_bars
        from liangjian_funnel.runtime.team_v2_store import canonical
        def chars(value):return len(canonical(value))
        per_plan=[]
        for i,plan in enumerate(values['ACTIVE_PLANS']):
            pid=plan['plan_id']
            for interval,encoded in values['BARS'][pid]['closed_bars'].items():
                if decode_bars(encoded)!=kw['input_projection']['model_contexts'][pid]['closed_bars'][interval]:
                    raise AssertionError('LOSSLESS_BAR_INVERSE_MISMATCH')
            catalog=[item for item in values['EVIDENCE_CATALOG']['items'] if item['id'].endswith(('PLAN:'+pid,'TECH:'+pid))]
            parts=dict(plan=chars(plan),bars=chars(values['BARS'][pid]),tech=chars(values['TECH_REFERENCE'][pid]),
                catalog=chars(catalog),other=chars(values['RISK_ENVELOPES'][pid])+chars(values['TRADABILITY'][pid]))
            per_plan.append(dict(plan_id=pid,components=parts,total_component_chars=sum(parts.values()),
                target_6000_met=sum(parts.values())<=6000))
        record=dict(decision_id=values['DECISION_ID'],plan_ids=[r['plan_id'] for r in values['ACTIVE_PLANS']],
            per_plan_character_breakdown=per_plan,lossless_bar_inverse='PASS_ALL_ORIGINAL_FIELDS',
            profile=describe(values,request),bar_counts={pid:{interval:bars['count'] if isinstance(bars,dict) else len(bars) for interval,bars in context['closed_bars'].items()}
                for pid,context in values['BARS'].items()},
            original_raw_sources='SEALED_SOURCE_DB_AND_FRAME_RECEIPTS_NOT_TRUNCATED',engine_statistics_changed=False)
        rows.append(record)
        body=json.dumps(dict(values=values,messages=[dict(m) for m in request.messages]),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
        path=output/(values['DECISION_ID'].rsplit(':',1)[-1]+'.json.gz')
        with gzip.open(path,'xb') as stream:stream.write(body)
        record['uncompressed_sha256']=hashlib.sha256(body).hexdigest()
    trader=result['trader'];receipt=dict(status='OFFLINE_SHARDED_RENDER_ONLY',actual_started_at=started.isoformat(),
        actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),source_head=head,
        original_batch_count=source['original_batch_count'],projected_plan_count=source['projected_plan_count'],
        selected_plan_count=len(trader['scope']['plan_ids']),scope=trader['scope'],shards=rows,max_concurrent=4,
        per_shard_max_seconds=30,global_deadline_seconds=47,model_calls=calls,model_claims=claims,
        orders=len(kw['broker'].get_orders(kw['trade_date'])),production_writes=0,
        r1_formal_gate='PENDING_ATR5M_AUTHORITY' if not trader['scope']['r1_proximity_complete'] else 'DOMAIN_PROVEN',
        model_latency='NOT_MEASURED',formal_six_day_gate='NOT_RUN')
    save(output/'PROFILE.json',receipt)
    print(json.dumps({k:v for k,v in receipt.items() if k not in ('scope','shards')}))
    print(json.dumps([dict(plan_count=len(row['plan_ids']),chars=row['profile']['exact_message_content_chars']) for row in rows]))


if __name__=='__main__':main()
