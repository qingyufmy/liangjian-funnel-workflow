"""Export explicitly bounded original tables through SSH; production read-only."""
from __future__ import annotations
import argparse,base64,datetime,gzip,hashlib,json,sqlite3,subprocess
from pathlib import Path

REMOTE=r'''
import base64,gzip,hashlib,json,sqlite3,subprocess,sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=Path.cwd();s=Settings.from_env(root=root);now=datetime.now(ZoneInfo('Asia/Shanghai'))
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
assert root==Path('/www/wwwroot/Agu/liangjian-funnel-workflow')
assert now.date().isoformat()=='2026-10-15'
gz=gzip.GzipFile(fileobj=sys.stdout.buffer,mode='wb',mtime=0)
def emit(value):gz.write((json.dumps(value,ensure_ascii=True,separators=(',',':'))+'\n').encode())
def ro(path):
 c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=3);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('BEGIN');return c
def table(c,name,where,args):
 cols=[dict(r) for r in c.execute('pragma table_info('+name+')')]
 if not cols:emit({'kind':'missing_table','table':name});return
 emit({'kind':'schema','table':name,'columns':cols})
 n=0
 for row in c.execute('select * from '+name+' where '+where,args):
  record={k:({'__b64__':base64.b64encode(v).decode()} if isinstance(v,bytes) else v) for k,v in dict(row).items()}
  emit({'kind':'row','table':name,'row':record});n+=1
 emit({'kind':'table_end','table':name,'count':n})
emit({'kind':'manifest','captured_at':now.isoformat(),'day_range':['2026-10-08','2026-10-15'],
 'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'production_writes':0,'provider_calls':0,'model_calls':0})
c=ro(s.state_db_path)
symbols=sorted({r[0] for r in c.execute('select distinct symbol from execution_plans where expires_at>=? and expires_at<?',('2026-10-08','2026-10-16'))})
for name,column in [('execution_plans','expires_at'),('monitor_events','minute_end'),('a4_signal_lifecycles','trade_date'),('astock_outcome_labels','trade_date'),('notification_deliveries','created_at')]:
 table(c,name,column+'>=? and '+column+'<?',('2026-10-08','2026-10-16'))
c.rollback();c.close()
c=ro(s.minute_cache_dir/'minute_bars.sqlite3')
for name,col in [('minute_decision_snapshots','decision_as_of'),('minute_bars','bar_end'),('minute_bar_versions','bar_end')]:
 table(c,name,col+'>=? and '+col+'<?',('2026-10-08','2026-10-16'))
c.rollback();c.close()
c=ro(s.fact_cache_db_path)
if symbols:
 table(c,'daily_bars','symbol in ('+','.join('?' for _ in symbols)+') and bar_timestamp>=? and bar_timestamp<?',(*symbols,'2026-10-08','2026-10-16'))
c.rollback();c.close()
for day in ('2026-10-08','2026-10-09','2026-10-12','2026-10-13','2026-10-14','2026-10-15'):
 directory=s.fact_store_dir/'a4_live_market'/day
 for path in sorted(directory.glob('*.json')) if directory.is_dir() else ():
  raw=path.read_bytes();emit({'kind':'file','relative':'a4_live_market/'+day+'/'+path.name,'sha256':hashlib.sha256(raw).hexdigest(),'data_b64':base64.b64encode(raw).decode()})
 directory=s.state_db_path.parent/'lark_cards'/day
 for path in sorted(directory.glob('*.json')) if directory.is_dir() else ():
  raw=path.read_bytes();emit({'kind':'file','relative':'lark_cards/'+day+'/'+path.name,'sha256':hashlib.sha256(raw).hexdigest(),'data_b64':base64.b64encode(raw).decode()})
emit({'kind':'complete','captured_finished_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),'symbols':symbols})
gz.close()
'''

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if a.output.exists():raise SystemExit('REFUSE_OVERWRITE')
 a.output.mkdir(parents=True)
 cmd=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
 'cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- nice -n 10 .venv/bin/python -B -']
 with (a.output/'source.jsonl.gz').open('xb') as out,(a.output/'ssh.stderr.log').open('xb') as err:
  proc=subprocess.run(cmd,input=REMOTE.encode(),stdout=out,stderr=err,timeout=300)
 with (a.output/'command.json').open('x') as f:json.dump({'command':cmd,'exit_code':proc.returncode,'actual_at':datetime.datetime.now().astimezone().isoformat()},f)
 if proc.returncode:raise SystemExit('SSH_EXPORT_FAILED:'+str(proc.returncode))
 db=sqlite3.connect(a.output/'frozen.sqlite3');counts={};complete=False;meta=None
 with gzip.open(a.output/'source.jsonl.gz','rt',encoding='utf-8') as stream:
  for line in stream:
   item=json.loads(line);kind=item['kind']
   if kind=='manifest':meta=item
   elif kind=='schema':
    name=item['table'];assert name.replace('_','').isalnum()
    columns=item['columns'];assert all(c['name'].replace('_','').isalnum() for c in columns)
    db.execute('CREATE TABLE '+name+' ('+','.join('"'+c['name']+'" '+(c['type'] or 'TEXT') for c in columns)+')')
   elif kind=='row':
    row=item['row'];values=[base64.b64decode(v['__b64__']) if isinstance(v,dict) and set(v)=={'__b64__'} else v for v in row.values()]
    db.execute('INSERT INTO '+item['table']+' VALUES ('+','.join('?' for _ in values)+')',values)
   elif kind=='table_end':counts[item['table']]=item['count'];db.commit()
   elif kind=='file':
    rel=Path(item['relative']);assert not rel.is_absolute() and '..' not in rel.parts
    raw=base64.b64decode(item['data_b64']);assert hashlib.sha256(raw).hexdigest()==item['sha256']
    path=a.output/'files'/rel;path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:f.write(raw)
   elif kind=='complete':complete=True
 assert complete and meta,'EXPORT_NOT_COMPLETE'
 for table,columns in [('minute_decision_snapshots','symbol,interval,decision_as_of'),('monitor_events','minute_end'),('minute_bar_versions','symbol,bar_end,observed_at'),('minute_bars','symbol,bar_end'),('daily_bars','symbol,bar_timestamp')]:
  if table in counts:db.execute('CREATE INDEX idx_'+table+' ON '+table+' ('+columns+')')
 db.commit();db.close()
 digest=hashlib.file_digest((a.output/'source.jsonl.gz').open('rb'),'sha256').hexdigest()
 receipt={'source':meta,'table_counts':counts,'source_gzip_sha256':digest,'local_db':str(a.output/'frozen.sqlite3'),'production_writes':0}
 with (a.output/'EXPORT_RECEIPT.json').open('x',encoding='utf-8') as f:json.dump(receipt,f,ensure_ascii=False,indent=2)
 print(json.dumps(receipt,ensure_ascii=True))

if __name__=='__main__':main()
