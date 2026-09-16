"""Read-only notification -> frozen input -> isolated strategy audit, no replay orders."""
import argparse
import json
from pathlib import Path
import subprocess
from liangjian_funnel.reporting import atomic_write_json

REMOTE = r'''
import collections,hashlib,json,sqlite3,zlib
from datetime import datetime,timedelta
from pathlib import Path
from liangjian_funnel.settings import Settings
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.execution_evidence import execution_evidence
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.workflow import _intraday_market_context
s=Settings.from_env(root=Path.cwd())
def connect(path):
 c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row;c.execute('BEGIN');return c
c=connect(s.state_db_path);d=connect(s.minute_cache_dir/'minute_bars.sqlite3')
start=DAY+'T09:00:00+08:00';end=datetime.fromisoformat(CUTOFF)
notifications=[]
for r in c.execute('select kind,created_at,status,payload_json from notification_deliveries where created_at>=? and created_at<=? order by created_at',(start,CUTOFF)):
 if not r['kind'].startswith('A4'):continue
 p=json.loads(r['payload_json']);notifications.append({'kind':r['kind'],'at':r['created_at'],'status':r['status'],
   **{k:p.get(k) for k in ('state','symbols','reasons','affected_count','reason_code')}})
plans=collections.defaultdict(list)
for r in c.execute('select plan_id,symbol,payload_json from execution_plans where valid_from<=? and expires_at>=?',(CUTOFF,start)):
 plans[r['symbol']].append((r['plan_id'],json.loads(r['payload_json'])))
def snapshot(symbol,period,at):
 r=d.execute('select payload_zlib,payload_sha256,captured_at from minute_decision_snapshots where symbol=? and interval=? and decision_as_of=?',(symbol,period,at.isoformat())).fetchone()
 if r is None:return None
 raw=zlib.decompress(r['payload_zlib']);assert hashlib.sha256(raw).hexdigest()==r['payload_sha256']
 return {'bars':tuple(MinuteBar.model_validate(b) for b in json.loads(raw)), 'hash':r['payload_sha256'],'captured_at':r['captured_at']}
checks=[]
for notice in notifications:
 if notice['kind']!='A4_MINUTE_SOURCE_HEALTH' or notice['state']!='BLOCKED':continue
 at=datetime.fromisoformat(notice['at']).replace(second=0,microsecond=0)
 for symbol in notice['symbols'] or []:
  row={'symbol':symbol,'at':at.isoformat()};one=snapshot(symbol,'1m',at);five=snapshot(symbol,'5m',at)
  if one is None or five is None:row['error']='SNAPSHOT_MISSING';checks.append(row);continue
  _,e=execution_evidence(one['bars'],five['bars'],as_of=at)
  row.update({'one_hash':one['hash'],'five_hash':five['hash'],'comparison':e['native_5m_comparison']})
  nxt=at+timedelta(minutes=1)
  n_one=snapshot(symbol,'1m',nxt) if nxt<=end else None
  n_five=snapshot(symbol,'5m',nxt) if nxt<=end else None
  if n_one and n_five:
   _,e=execution_evidence(n_one['bars'],n_five['bars'],as_of=nxt)
   row['next_minute_comparison']=e['native_5m_comparison']
  else:row['next_minute_comparison']={'status':'NOT_AVAILABLE_WITHIN_CUTOFF'}
  if len(plans[symbol])==1:
   plan_id,plan=plans[symbol][0]
   path=s.fact_store_dir/'a4_live_market'/DAY/(at.strftime('%H')+f'{at.minute//5*5:02d}'+'.json')
   market=json.loads(path.read_text()) if path.exists() else {}
   context=_intraday_market_context(symbol,one['bars'],five['bars'],current=at,live_market_state=market)
   result=evaluate_strategy(plan,one['bars'],now=at,position=None,market_context=context)
   row['isolated_original_input_strategy']={'action':result.action,'reason_codes':result.reason_codes,'plan_id':plan_id}
  else:row['strategy_error']='PLAN_IDENTITY_AMBIGUOUS'
  checks.append(row)
events=[dict(r) for r in c.execute('select action,reason_code,count(*) as n from monitor_events where minute_end>=? and minute_end<=? group by action,reason_code',(start,CUTOFF))]
intents=[dict(r) for r in c.execute('select action,status,count(*) as n from simulation_intents where created_at>=? and created_at<=? group by action,status',(start,CUTOFF))]
fills=c.execute('select count(*) from virtual_fills where bar_end>=? and bar_end<=?',(start,CUTOFF)).fetchone()[0]
out={'cutoff':CUTOFF,'mode':'READ_ONLY_ORIGINAL_INPUT_DIAGNOSTIC_NOT_STATEFUL_REPLAY', 'notifications':notifications,
 'notification_counts':dict(collections.Counter(n['kind'] for n in notifications)),
 'source_states':dict(collections.Counter(n['state'] for n in notifications if n['kind']=='A4_MINUTE_SOURCE_HEALTH')),
 'affected_symbol_minutes':len(checks),'checks':checks,'event_counts':events,'simulation_intents':intents,'fills':fills,
 'strategy_actions':dict(collections.Counter(v.get('isolated_original_input_strategy',{}).get('action','UNVERIFIED') for v in checks)),
 'next_statuses':dict(collections.Counter(v.get('next_minute_comparison',{}).get('status','MISSING') for v in checks))}
c.rollback();d.rollback();print(json.dumps(out,ensure_ascii=False))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', required=True)
    parser.add_argument('--cutoff', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists(): raise SystemExit('REFUSE_OVERWRITE')
    result = subprocess.run(['ssh', 'aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && sudo -u www .venv/bin/python -B -'],
        input=('DAY='+repr(args.day)+'\nCUTOFF='+repr(args.cutoff)+'\n'+REMOTE).encode(), capture_output=True, timeout=90)
    if result.returncode: raise SystemExit(result.stderr.decode(errors='replace')[-1800:])
    report=json.loads(result.stdout);atomic_write_json(args.output,report)
    print(json.dumps({k:v for k,v in report.items() if k not in {'checks','notifications'}},ensure_ascii=False))


if __name__=='__main__': main()
