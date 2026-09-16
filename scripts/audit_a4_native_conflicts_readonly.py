"""Read frozen A4 conflicts on the VM; no provider/model calls or DB writes."""
import argparse
import json
import subprocess
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json

REMOTE = r'''
import collections,hashlib,json,sqlite3,zlib,subprocess
from datetime import datetime
from pathlib import Path
from liangjian_funnel.settings import Settings
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.execution_evidence import execution_evidence
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.workflow import _intraday_market_context
s=Settings.from_env(root=Path.cwd())
def connect(p):
 c=sqlite3.connect(p.resolve().as_uri()+'?mode=ro',uri=True,timeout=5);c.row_factory=sqlite3.Row;return c
c=connect(s.state_db_path); d=connect(s.minute_cache_dir/'minute_bars.sqlite3')
c.execute('BEGIN');d.execute('BEGIN')
events=[dict(r) for r in c.execute('select * from monitor_events where minute_end>=? and minute_end<=? order by minute_end',(DAY,CUTOFF))]
notifications=[]
for r in c.execute("select * from notification_deliveries where created_at>=? and kind in ('A4_DATA_ALERT','A4_MINUTE_SOURCE_HEALTH') order by created_at",(DAY,)):
 p=json.loads(r['payload_json']);notifications.append({'kind':r['kind'],'created_at':r['created_at'],'status':r['status'],'payload':p})
plans={r['plan_id']:json.loads(r['payload_json']) for r in c.execute('select plan_id,payload_json from execution_plans')}
snapshots={}
for r in d.execute('select * from minute_decision_snapshots where decision_as_of>=? and decision_as_of<=?',(DAY,CUTOFF)):
 raw=zlib.decompress(r['payload_zlib'])
 assert hashlib.sha256(raw).hexdigest()==r['payload_sha256']
 snapshots[(r['symbol'],r['interval'],r['decision_as_of'])]={'bars':json.loads(raw),'captured_at':r['captured_at'],'sha256':r['payload_sha256']}
details=[];counts=collections.Counter(); event_counts=collections.Counter(); coverage=collections.defaultdict(set)
window_checks=[]; revisions=[]
for symbol in sorted({k[0] for k in snapshots}):
 for interval in ('1m','5m'):
  times=sorted(k[2] for k in snapshots if k[0]==symbol and k[1]==interval)
  for before,after in zip(times,times[1:]):
   old=snapshots[(symbol,interval,before)];new=snapshots[(symbol,interval,after)]
   oldbars={b['bar_end']:b for b in old['bars']}
   for bar in new['bars']:
    prior=oldbars.get(bar['bar_end'])
    if not prior:continue
    changed={f:[prior[f],bar[f]] for f in ('open','high','low','close','volume') if prior[f]!=bar[f]}
    if changed:revisions.append({'symbol':symbol,'interval':interval,'before':before,'after':after,
     'bar_end':bar['bar_end'],'changes':changed,'initial_capture':old['captured_at'],'later_capture':new['captured_at']})
for (symbol,interval,at),one in sorted(snapshots.items()):
 if interval!='1m':continue
 five=snapshots.get((symbol,'5m',at))
 if not five:continue
 _,e=execution_evidence(tuple(MinuteBar.model_validate(b) for b in one['bars']),tuple(MinuteBar.model_validate(b) for b in five['bars']),as_of=datetime.fromisoformat(at))
 if e['native_5m_comparison']['status']=='CONFLICT':
  matching=[event for event in events if event['minute_end']==at and json.loads(event['payload_json']).get('symbol')==symbol]
  check={'symbol':symbol,'at':at,'comparison':e['native_5m_comparison'],'event_count':len(matching)}
  prior_events=[event for event in events if event['minute_end']<at and json.loads(event['payload_json']).get('symbol')==symbol]
  if not matching and prior_events:
   previous_payload=json.loads(prior_events[-1]['payload_json']); now=datetime.fromisoformat(at)
   market_path=s.fact_store_dir/'a4_live_market'/DAY/(now.strftime('%H')+f'{now.minute//5*5:02d}'+'.json')
   market=json.loads(market_path.read_text()) if market_path.exists() else {}
   one_bars=tuple(MinuteBar.model_validate(b) for b in one['bars']); five_bars=tuple(MinuteBar.model_validate(b) for b in five['bars'])
   context=_intraday_market_context(symbol,one_bars,five_bars,current=now,live_market_state=market)
   candidate=evaluate_strategy(plans[previous_payload['plan_id']],one_bars,now=now,position=None,market_context=context).model_dump(mode='json')
   check['hypothetical_one_minute_only']={'action':candidate['action'],'reason_codes':candidate['reason_codes']}
  window_checks.append(check)
for event in events:
 event_counts[(event['action'],event['reason_code'])]+=1
 p=json.loads(event['payload_json']);symbol=p.get('symbol');at=event['minute_end'];coverage[at].add(symbol)
 if event['reason_code']!='EXECUTION_NATIVE_5M_CONFLICT':continue
 one=snapshots.get((symbol,'1m',at));five=snapshots.get((symbol,'5m',at))
 if not one or not five:raise ValueError('FROZEN_INPUT_MISSING')
 one_bars=tuple(MinuteBar.model_validate(b) for b in one['bars']);five_bars=tuple(MinuteBar.model_validate(b) for b in five['bars'])
 now=datetime.fromisoformat(at)
 agg,evidence=execution_evidence(one_bars,five_bars,as_of=now)
 later_at=sorted({k[2] for k in snapshots if k[0]==symbol and k[1]=='1m' and k[2]>at})
 later=None
 if later_at:
  nxt=later_at[0]; n_one=snapshots[(symbol,'1m',nxt)];n_five=snapshots.get((symbol,'5m',nxt))
  if n_five:
   _,e=execution_evidence(tuple(MinuteBar.model_validate(b) for b in n_one['bars']),tuple(MinuteBar.model_validate(b) for b in n_five['bars']),as_of=datetime.fromisoformat(nxt))
   changes=[]
   for kind,old,new in [('1m',one,n_one),('5m',five,n_five)]:
    previous={b['bar_end']:b for b in old['bars']}
    for b in new['bars']:
     oldbar=previous.get(b['bar_end'])
     if oldbar:
      changed={field:[oldbar[field],b[field]] for field in ['open','high','low','close','volume'] if oldbar[field]!=b[field]}
      if changed:changes.append({'interval':kind,'bar_end':b['bar_end'],'changes':changed})
   later={'at':nxt,'comparison':e['native_5m_comparison'],'revisions':changes}
 # Isolated diagnostic only: evaluate the existing strategy on frozen 1m.
 # This is NOT permission to ignore a production data conflict or place orders.
 market_path=s.fact_store_dir/'a4_live_market'/DAY/(now.strftime('%H')+f'{now.minute//5*5:02d}'+'.json')
 market=json.loads(market_path.read_text()) if market_path.exists() else {}
 context=_intraday_market_context(symbol,one_bars,five_bars,current=now,live_market_state=market)
 strategy=evaluate_strategy(plans[p['plan_id']],one_bars,now=now,position=None,market_context=context).model_dump(mode='json')
 for difference in evidence['native_5m_comparison']['conflicts']:counts[difference['field']]+=1
 details.append({'symbol':symbol,'at':at,'plan_id':p['plan_id'],'entry_window_complete':evidence['entry_window_complete'],
  'one_capture':one['captured_at'],'five_capture':five['captured_at'],'one_hash':one['sha256'],'five_hash':five['sha256'],
  'comparison':evidence['native_5m_comparison'],'later':later,
  'hypothetical_one_minute_only':{'action':strategy['action'],'reason_codes':strategy['reason_codes']},
  'tail_one':one['bars'][-5:],'tail_native':five['bars'][-1:]})
out={'day':DAY,'cutoff':CUTOFF,'read_only':True,'conflict_events':len(details),'conflict_fields':dict(counts),
 'event_counts':[{'action':a,'reason':r,'n':n} for (a,r),n in event_counts.items()],
 'coverage':{k:len(v-{None}) for k,v in coverage.items()},'notifications':notifications,'details':details,'all_conflict_windows':window_checks,'all_observed_revisions':revisions,
 'positions':[dict(r) for r in c.execute('select * from virtual_positions where total_qty>0')]}
c.rollback();d.rollback();print(json.dumps(out,ensure_ascii=False))
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--day', required=True)
    p.add_argument('--cutoff', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    result = subprocess.run(['ssh', 'aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && sudo -u www .venv/bin/python -'],
        input=('DAY='+repr(args.day)+'\nCUTOFF='+repr(args.cutoff)+'\n'+REMOTE).encode(),
        capture_output=True, timeout=60)
    if result.returncode:
        raise SystemExit(result.stderr.decode(errors='replace')[-2000:])
    report = json.loads(result.stdout)
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ('cutoff', 'conflict_events', 'conflict_fields', 'event_counts', 'coverage')}, ensure_ascii=False))
    for item in report['details']:
        print(json.dumps({key: item[key] for key in ('symbol','at','comparison','later','hypothetical_one_minute_only')},ensure_ascii=False))


if __name__ == '__main__':
    main()
