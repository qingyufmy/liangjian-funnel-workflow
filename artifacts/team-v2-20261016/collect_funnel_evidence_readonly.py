"""Stream existing NDJSON per item; never parse the 328MB monolithic research file."""
import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
OUT=pathlib.Path(__file__).resolve().parents[2]/'artifacts/observations-20261016'
ctx=json.loads(sorted(OUT.glob('*context-stdout.json'))[-1].read_bytes());names={};members={}
for file in ctx['files']:
 if '/board_reference/' not in file['path']:continue
 b=json.loads(file['text'])
 if b.get('kind')!='members':continue
 for x in b.get('records',[]):
  if isinstance(x,dict) and x.get('symbol'):
   names[x['symbol']]=x.get('name');members.setdefault(x['symbol'],set()).add(b.get('board_name'))
daily=json.loads(sorted(OUT.glob('*daily-cache-stdout.json'))[-1].read_bytes());versions={}
for row in daily.get('bars',[]):
 day=datetime.datetime.fromisoformat(row['bar_timestamp']).astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat();key=(row['symbol'],day)
 if key not in versions or row['fetched_at']>versions[key]['fetched_at']:versions[key]=row
rank=[]
for (symbol,day),row in versions.items():
 if day!='2026-10-16' or (symbol,'2026-10-15') not in versions:continue
 before=json.loads(versions[(symbol,'2026-10-15')]['payload_json']);after=json.loads(row['payload_json'])
 if not before.get('close_price') or not after.get('close_price'):continue
 name=names.get(symbol) or ''
 if 'ST' in name.upper():continue
 rank.append({'symbol':symbol,'name':name,'return_pct':(after['close_price']/before['close_price']-1)*100,'member_boards':sorted(members.get(symbol,set())),'today_source_hash':row['content_hash'],'prior_source_hash':versions[(symbol,'2026-10-15')]['content_hash'],'ipo_age_status':'UNVERIFIED_NOT_CLAIMED_FULL_MARKET_TOP50'})
rank.sort(key=lambda x:(-x['return_pct'],x['symbol']))
selected=rank[:50];symbols={x['symbol'] for x in selected}
military=[x for x in rank if any(any(k in b for k in ('军工','航空装备','航天装备','国防')) for b in x['member_boards'])]
symbols.update(x['symbol'] for x in military)
CODE=r'''
import datetime,hashlib,json,pathlib,sqlite3,subprocess
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow' and subprocess.check_output(['hostname'],text=True).strip()=='debian'
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert str(now.date())=='2026-10-16'
out={'actual_at':now.isoformat(),'production_writes':0,'funnel_files':{}}
for run in ('2026-10-15-close-b3d3c85c42f4','2026-10-16-close-624199bb9c19'):
 p=r/'outputs/research'/('research_'+run+'_lane_1.decisions.ndjson');h=hashlib.sha256();kept=[];counts={}
 with p.open('rb') as f:
  for line in f:
   h.update(line);v=json.loads(line);key=v.get('stage','?')+':'+v.get('pool','?');counts[key]=counts.get(key,0)+1
   if v.get('symbol') in SYMBOLS or v.get('stage') in ('A2','A3'):kept.append(v)
 out['funnel_files'][str(p.relative_to(r))]={'sha256':h.hexdigest(),'counts':counts,'items':kept}
s=Settings.from_env(root=r);c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
out['next_plans']=[dict(x) for x in c.execute('SELECT * FROM execution_plans WHERE expires_at>=? AND expires_at<?',('2026-10-17','2026-10-20'))]
out['notifications']=[dict(x) for x in c.execute('SELECT kind,source_id,status,last_reason_code,sent_at,created_at FROM notification_deliveries WHERE created_at>=? AND created_at<?',('2026-10-16','2026-10-17'))]
reviews=[dict(x) for x in c.execute('SELECT review_id,review_kind,status,input_hash,markdown_path,created_at FROM a5_daily_reviews WHERE trade_date=?',('2026-10-16',))]
c.rollback();c.close();out['a5_originals']=[]
for row in reviews:
 p=pathlib.Path(row['markdown_path']);p=p if p.is_absolute() else r/p
 if p.is_relative_to(r) and p.is_file() and p.stat().st_size<1000000:
  raw=p.read_bytes();out['a5_originals'].append({**row,'sha256':hashlib.sha256(raw).hexdigest(),'markdown':raw.decode('utf-8')})
print(json.dumps(out,ensure_ascii=True))
'''
CODE='SYMBOLS='+repr(sorted(symbols))+'\n'+CODE
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-funnel-evidence'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=30)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (OUT/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
receipt={'actual_at':at.isoformat(),'exit_code':p.returncode,'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'remote_code_sha256':hashlib.sha256(CODE.encode()).hexdigest(),'ranking_basis':'EXISTING_MEMBER_CACHE_WITH_ST_FILTER_IPO_AGE_UNVERIFIED','ranked_symbols':len(rank),'selected':selected,'military_members':military}
with (OUT/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,ensure_ascii=False,indent=2)
print(json.dumps({'prefix':prefix,'exit_code':p.returncode,'ranked_symbols':len(rank),'selected_count':len(selected),'military_member_count':len(military),'stdout_sha256':receipt['stdout_sha256']}));print(p.stderr.decode(errors='replace')[-1000:] if p.returncode else 'EXISTING_FUNNEL_AND_ORIGINAL_A5_EXPORTED')
sys.exit(p.returncode)
