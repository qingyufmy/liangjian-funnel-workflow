import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
OUT=pathlib.Path(__file__).resolve().parents[2]/'artifacts/observations-20261016'
CODE=r'''
import datetime,hashlib,json,pathlib,subprocess
from zoneinfo import ZoneInfo
r=pathlib.Path.cwd();assert str(r)=='/www1/Agu/liangjian-funnel-workflow' and subprocess.check_output(['hostname'],text=True).strip()=='debian'
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));assert str(now.date())=='2026-10-16'
out={'actual_at':now.isoformat(),'production_writes':0,'layouts':{}}
for name in ('2026-10-15-close-b3d3c85c42f4','2026-10-16-close-624199bb9c19'):
 p=r/'outputs/research'/('research_'+name+'_lane_1.decisions.ndjson')
 if not p.exists():continue
 rows=[]
 with p.open() as f:
  for i,line in enumerate(f):
   if i>=6:break
   v=json.loads(line);rows.append({'row':i,'bytes':len(line),'keys':list(v),'types':{k:type(x).__name__ for k,x in v.items()},'scalar_values':{k:x for k,x in v.items() if isinstance(x,(str,int,float,bool,type(None))) and len(str(x))<200},'nested_keys':{k:list(x)[:30] for k,x in v.items() if isinstance(x,dict)}})
 out['layouts'][str(p.relative_to(r))]=rows
print(json.dumps(out,ensure_ascii=True))
'''
at=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=at.strftime('%Y%m%dT%H%M%S%f')+'-funnel-layout'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=CODE.encode(),capture_output=True,timeout=20)
for suffix,raw in [('stdout.json',p.stdout),('stderr.log',p.stderr)]:
 with (OUT/(prefix+'-'+suffix)).open('xb') as f:f.write(raw)
print(p.stdout.decode() if p.returncode==0 else p.stderr.decode());sys.exit(p.returncode)
