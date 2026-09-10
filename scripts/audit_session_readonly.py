"""Bounded, read-only VM session audit; write only a new local evidence file."""
import argparse
import json
import subprocess
from pathlib import Path
from liangjian_funnel.reporting import atomic_write_json

REMOTE = r'''
import collections,hashlib,json,sqlite3,subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
s=Settings.from_env(root=Path.cwd()); lo=DAY; hi=DAY+'T15:01'
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5);c.row_factory=sqlite3.Row
c.execute('BEGIN')
def rows(q,args=()):return [dict(r) for r in c.execute(q,args)]
out={'captured_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),'day':DAY,'root':str(Path.cwd()),
 'plans':rows('select * from execution_plans where expires_at>=? and expires_at<?',(lo,hi)),
 'runs':rows('select * from workflow_runs where trade_date>=? order by created_at limit 30',(DAY,)),
 'leases':rows('select * from scheduler_leases'),
 'a4_counts':rows('select action,reason_code,effective,count(*) n,min(minute_end) first,max(minute_end) last from monitor_events where minute_end>=? and minute_end<? group by action,reason_code,effective',(lo,hi)),
 'minute_coverage':rows('select count(distinct minute_end) minutes,min(minute_end) first,max(minute_end) last,count(*) observations from monitor_events where minute_end>=? and minute_end<?',(lo,hi)),
 'lifecycles':rows('select * from a4_signal_lifecycles where trade_date=? or updated_at>=? and updated_at<?',(DAY,lo,DAY+'T23:59:59')),
 'fills':rows('select * from virtual_fills where bar_end>=? and bar_end<?',(lo,hi)),
 'positions':rows('select * from virtual_positions where total_qty>0'),
 'reviews':rows('select review_id,review_kind,status,cutoff_at,model,input_hash,prompt_hash,thinking_variant,markdown_path,created_at from a5_daily_reviews where trade_date=?',(DAY,)),
 'notifications':rows('select delivery_id,kind,source_id,status,last_reason_code,sent_at,created_at from notification_deliveries where created_at>=? order by created_at',(DAY,)),
 'outcomes':rows('select trade_date,stage,count(*) n,sum(labeled_at is not null) labeled,sum(fwd_return_1d is not null) t1,sum(reason_codes="[]") empty_reasons from astock_outcome_labels where trade_date>=? group by trade_date,stage',('2026-09-08',))}
counter=collections.Counter();per=collections.defaultdict(lambda:{'n':0,'minutes':set(),'first':None,'last':None,'effective':0,'reasons':collections.Counter()}); examples={}; digest=hashlib.sha256(); effective=[]
for r in c.execute('select * from monitor_events where minute_end>=? and minute_end<? order by event_id',(lo,hi)):
 d=dict(r);digest.update(json.dumps(d,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode());p=json.loads(r['payload_json']);sym=p.get('symbol','EMPTY')
 reasons=p.get('strategy_reason_codes') or p.get('strategy',{}).get('reason_codes') or [r['reason_code']]
 for reason in reasons:counter[str(reason)]+=1;per[sym]['reasons'][str(reason)]+=1;examples.setdefault(str(reason),d)
 x=per[sym];x['n']+=1;x['minutes'].add(r['minute_end']);x['first']=min(x['first'] or r['minute_end'],r['minute_end']);x['last']=max(x['last'] or r['minute_end'],r['minute_end']);x['effective']+=r['effective']
 if r['effective']:effective.append(d)
out.update(events_sha256=digest.hexdigest(),strategy_reasons=dict(counter),reason_examples=examples,effective_events=effective,
 per_symbol={k:{**v,'minutes':len(v['minutes']),'reasons':dict(v['reasons'])} for k,v in per.items()})
c.rollback();c.close()
cache=sqlite3.connect((s.minute_cache_dir/'minute_bars.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=5);cache.row_factory=sqlite3.Row
out['archives']=[dict(r) for r in cache.execute('select symbol,interval,count(*) n,min(bar_end) first,max(bar_end) last from minute_bars where bar_end>=? and bar_end<? group by symbol,interval',(lo,hi))]
out['snapshots']=[dict(r) for r in cache.execute('select interval,count(*) n,count(distinct symbol) symbols,count(distinct decision_as_of) minutes,sum(length(payload_zlib)) bytes from minute_decision_snapshots where decision_as_of>=? and decision_as_of<? group by interval',(lo,hi))]
cache.close();out['job_logs']=[]
for line in Path('outputs/node/node-'+DAY+'.jsonl').read_text().splitlines():
 try:r=json.loads(line)
 except ValueError:continue
 if r.get('job') in ('close','a5-midday','a5-close','outcomes','premarket','morning') or '控制面已启动' in r.get('message',''):out['job_logs'].append(r)
out['commit']=subprocess.check_output(['git','-c','safe.directory='+str(Path.cwd()),'rev-parse','HEAD'],text=True).strip()
print(json.dumps(out,ensure_ascii=False))
'''

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--day',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    from datetime import date
    date.fromisoformat(a.day)
    if a.output.exists():raise SystemExit('REFUSE_OVERWRITE')
    r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','aurum-vm','cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],input=('DAY='+repr(a.day)+'\n'+REMOTE).encode(),capture_output=True,timeout=60)
    if r.returncode:raise SystemExit(r.stderr.decode(errors='replace')[-1200:])
    out=json.loads(r.stdout);atomic_write_json(a.output,out)
    print(json.dumps({k:out[k] for k in ('captured_at','commit','minute_coverage','a4_counts','strategy_reasons','snapshots','outcomes')},ensure_ascii=False))
    print('artifact',str(a.output),'bytes',a.output.stat().st_size)

if __name__=='__main__':main()
