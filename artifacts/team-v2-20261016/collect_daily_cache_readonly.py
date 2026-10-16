"""Read existing two-day cache and version metadata only; bounded SQLite CPU time."""
import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
ROOT=pathlib.Path(__file__).resolve().parents[2];OUT=ROOT/'artifacts/observations-20261016'
context=json.loads(sorted(OUT.glob('*-context-stdout.json'))[-1].read_bytes())
symbols=set()
for item in context['files']:
 if '/board_reference/' not in item['path']:continue
 body=json.loads(item['text'])
 symbols.update(row['symbol'] for row in body.get('records',[]) if isinstance(row,dict) and isinstance(row.get('symbol'),str))
assert symbols
CODE=r'''
import datetime,hashlib,json,pathlib,sqlite3,subprocess,time
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow' and subprocess.check_output(['hostname'],text=True).strip()=='debian'
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert str(at.date())=='2026-10-16'
s=Settings.from_env(root=r);c=sqlite3.connect(s.fact_cache_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON')
end=time.monotonic()+8;c.set_progress_handler(lambda:int(time.monotonic()>end),10000)
out={'actual_at':at.isoformat(),'production_writes':0,'new_source_calls':0,'scope':'EXISTING_CACHE_ONLY_NOT_FULL_MARKET_ASSERTION'}
try:
 c.execute('BEGIN')
 out['requested_symbols']=len(SYMBOLS);out['bars']=[]
 for symbol in SYMBOLS:
  # Cache timestamps are UTC instants (16:00Z = next Shanghai calendar day).
  # Widen the indexed textual range, then select business dates locally.
  out['bars'].extend(dict(v) for v in c.execute("SELECT * FROM daily_bars WHERE symbol=? AND bar_timestamp>=? AND bar_timestamp<? AND adjust=?",(symbol,'2026-10-14','2026-10-17','none')))
except sqlite3.OperationalError as exc:out['bounded_query_error']=str(exc)
finally:c.rollback();c.close()
print(json.dumps(out,ensure_ascii=True))
'''
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-daily-cache'
CODE='SYMBOLS='+repr(sorted(symbols))+'\n'+CODE
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=25)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (OUT/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest()}
with (OUT/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps({'prefix':prefix,**receipt}));print(p.stderr.decode(errors='replace')[-1500:] if p.returncode else 'CACHE_EXPORT_SAVED')
sys.exit(p.returncode)
