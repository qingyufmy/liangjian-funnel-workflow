"""Replay today's persisted no-position decisions from frozen bars, read-only."""
import argparse,inspect,json,subprocess
from datetime import datetime
from pathlib import Path
from liangjian_funnel.reporting import atomic_write_json


def replay_clocks(payload, stamp):
    """Recover the recorded observation and dispatch clocks, never guess a lag."""
    dispatch = datetime.fromisoformat(stamp)
    strategy = payload.get('strategy') or {}
    observation = strategy.get('as_of')
    packet_cutoff = (strategy.get('execution_data') or {}).get('market_cutoff')
    if observation and packet_cutoff and observation != packet_cutoff:
        raise ValueError('FROZEN_CUTOFF_CONFLICT')
    basis = 'STRATEGY_AS_OF' if observation else 'EXECUTION_PACKET_CUTOFF' if packet_cutoff else 'LEGACY_EVENT_CLOCK'
    closed = datetime.fromisoformat(observation or packet_cutoff or stamp)
    if any(value.tzinfo is None or value.utcoffset() is None for value in (closed, dispatch)):
        raise ValueError('FROZEN_CLOCK_TIMEZONE_MISSING')
    if closed > dispatch or closed.date() != dispatch.date():
        raise ValueError('FROZEN_CLOCK_INVALID')
    return closed, dispatch, basis


def frozen_market_overlay(context, strategy):
    """Reuse the recorded permission facts; do not infer a missing live quote."""
    result = dict(context)
    gate = strategy.get('market_gate') or {}
    if gate:
        result['live_market_state'] = {
            'status': gate.get('state_status'), 'as_of': gate.get('as_of'),
            'trade_date': gate.get('trade_date'), 'decision': gate.get('decision'),
            'suggested_position_cap_pct': gate.get('suggested_position_cap_pct'),
        }
    return result


REMOTE=r'''
import collections,hashlib,inspect,json,sqlite3,zlib
from datetime import datetime
from pathlib import Path
from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.workflow import _intraday_market_context
s=Settings.from_env(root=Path.cwd());c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
if c.execute('select count(*) from a4_signal_lifecycles where trade_date=?',(DAY,)).fetchone()[0]:raise ValueError('POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE')
plans={r['plan_id']:json.loads(r['payload_json']) for r in c.execute('select plan_id,payload_json from execution_plans where expires_at>=? and expires_at<?',(DAY,DAY+'T15:01'))}
cache=sqlite3.connect((s.minute_cache_dir/'minute_bars.sqlite3').resolve().as_uri()+'?mode=ro',uri=True);cache.row_factory=sqlite3.Row
counts=collections.Counter();changes=[];gaps=[];reasons=collections.Counter(); market={}
def bars(sym,interval,stamp):
 r=cache.execute('select payload_zlib,payload_sha256 from minute_decision_snapshots where symbol=? and interval=? and decision_as_of=?',(sym,interval,stamp)).fetchone()
 if r is None:return None
 raw=zlib.decompress(r['payload_zlib'])
 if hashlib.sha256(raw).hexdigest()!=r['payload_sha256']:raise ValueError('SNAPSHOT_HASH_MISMATCH')
 data=[MinuteBar.model_validate(x) for x in json.loads(raw)]
 if any(x.bar_end>datetime.fromisoformat(stamp) for x in data):raise ValueError('FUTURE_BAR')
 return data
for r in c.execute('select event_id,minute_end,action,reason_code,payload_json from monitor_events where minute_end>=? and minute_end<? order by minute_end,event_id',(DAY,DAY+'T15:01')):
 counts['records']+=1;p=json.loads(r['payload_json']);pid=p.get('plan_id')
 if not pid:counts['non_strategy_record']+=1;continue
 sym=p['symbol'];stamp=r['minute_end']
 try:now,dispatch,basis=replay_clocks(p,stamp)
 except ValueError as exc:
  gaps.append({'symbol':sym,'at':stamp,'missing':str(exc)});continue
 counts['clock_'+basis]+=1
 one=bars(sym,'1m',stamp);five=bars(sym,'5m',stamp)
 if not one:gaps.append({'symbol':sym,'at':stamp,'missing':'1m'});continue
 if any(x.bar_end>now for x in one or []):
  gaps.append({'symbol':sym,'at':stamp,'missing':'FUTURE_BAR_AT_FROZEN_CUTOFF'});continue
 bucket=dispatch.replace(minute=dispatch.minute//5*5).strftime('%H%M')
 if bucket not in market:
  f=s.fact_store_dir/'a4_live_market'/DAY/(bucket+'.json');market[bucket]=json.loads(f.read_text()) if f.exists() else {}
 ctx=_intraday_market_context(sym,one,five or [],current=now,live_market_state=market[bucket])
 ctx=frozen_market_overlay(ctx,p.get('strategy') or {})
 result=evaluate_strategy(plans[pid],one,now=now,decision_time=dispatch,position=None,market_context=ctx).model_dump(mode='json')
 action=result.get('action');reason=(result.get('reason_codes') or [None])[0]
 if result.get('state')=='PLAN_INVALIDATED':action='PLAN_INVALIDATED'
 # Mirror the existing MonitorEngine warm-up presentation boundary, rather
 # than treating normal pre-09:45 missing closed frames as replay defects.
 if action=='DATA_BLOCK' and reason in ('NO_CLOSED_5M','NO_CLOSED_15M'):
  counts['warmup_projected']+=1;action='START_CONFIRMATION';reason='A4_SESSION_WARMUP'
 counts['replayed']+=1;reasons[str(reason)]+=1
 if action!=r['action'] or reason!=r['reason_code']:
  changes.append({'event_id':r['event_id'],'symbol':sym,'at':stamp,'original_action':r['action'],'replay_action':action,'original_reason':r['reason_code'],'replay_reason':reason})
c.close();cache.close()
print(json.dumps({'date':DAY,'scope':'FROZEN_STRATEGY_DECISION_NO_POSITION_NO_LLM_NO_ORDERS','counts':counts,'reasons':reasons,'changes':changes,'gaps':gaps,'limitations':['Realtime quote arrival/price is not reconstructed; this is a closed-bar strategy audit, not order/fill proof.','Legacy records without an observation clock use the recorded event clock, not an assumed minus-one-minute rule.'],'strategy_sha256':hashlib.sha256(Path(inspect.getfile(evaluate_strategy)).read_bytes()).hexdigest()},ensure_ascii=False))
'''
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--day',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if a.output.exists():raise SystemExit('REFUSE_OVERWRITE')
 helpers=inspect.getsource(replay_clocks)+'\n'+inspect.getsource(frozen_market_overlay)
 r=subprocess.run(['ssh','-o','BatchMode=yes','aurum-vm','cd /www/wwwroot/Agu/liangjian-funnel-workflow && nice -n 10 .venv/bin/python -'],input=('from datetime import datetime\n'+helpers+'\nDAY='+repr(a.day)+'\n'+REMOTE).encode(),capture_output=True,timeout=300)
 if r.returncode:raise SystemExit(r.stderr.decode(errors='replace')[-1600:])
 result=json.loads(r.stdout);atomic_write_json(a.output,result)
 print(json.dumps({**{k:v for k,v in result.items() if k not in ('changes','gaps')},'changes':len(result['changes']),'gaps':len(result['gaps'])},ensure_ascii=False))
if __name__=='__main__':main()
