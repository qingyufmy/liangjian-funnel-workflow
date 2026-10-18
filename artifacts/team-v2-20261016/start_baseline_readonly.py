import datetime,hashlib,json,pathlib,subprocess
from zoneinfo import ZoneInfo
out=pathlib.Path(__file__).resolve().parent
source=r'''
import collections,datetime,hashlib,json,pathlib,subprocess,urllib.request
from zoneinfo import ZoneInfo
r=pathlib.Path('/www1/Agu/liangjian-funnel-workflow')
assert pathlib.Path.cwd()==r and subprocess.check_output(['hostname'],text=True).strip()=='debian'
now=datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
assert now.date().isoformat()=='2026-10-16'
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open('http://127.0.0.1:3210/api/shadow/job-runs-readonly',timeout=5) as f:jobs=json.loads(f.read())
runs=jobs.get('recentJobRuns',[]);monitor=[x for x in runs if x.get('job')=='monitor'];failed=[x for x in monitor if x.get('exitCode') not in (None,0)]
keep=('runId','job','status','startedAt','finishedAt','exitCode','durationMs')
res={'actual_at':now.isoformat(),'head':subprocess.check_output(['runuser','-u','www','--','git','rev-parse','HEAD'],text=True).strip(),'node':jobs.get('node'),'running':[ {k:x.get(k) for k in keep} for x in runs if x.get('status')=='running'],'monitor_counts':dict(collections.Counter(str(x.get('exitCode')) for x in monitor)),'latest_failed_monitor':[{k:x.get(k) for k in keep} for x in failed[:5]],'memory':subprocess.check_output(['free','-m'],text=True),'production_writes':0,'dispatches':0}
log=r/'outputs/node/node-2026-10-16.jsonl';targets={x['runId'] for x in failed[:3]};selected=[];chunks=collections.defaultdict(list)
for line in log.open():
 try:v=json.loads(line)
 except ValueError:continue
 if v.get('runId') not in targets:continue
 if v.get('stream')=='stdout':chunks[v['runId']].append(str(v.get('message','')))
 item={k:v[k] for k in ('timestamp','level','job','runId','event','exitCode','phase') if k in v};item['keys']=sorted(v)
 # Extract safe JSON reason/status only; never seal raw subprocess text here.
 for value in v.values():
  if not isinstance(value,str):continue
  try:body=json.loads(value.strip())
  except ValueError:continue
  if isinstance(body,dict):item['safe_body']={k:body[k] for k in ('status','reason_code','reason_codes') if k in body}
 selected.append(item)
res['failed_log_metadata']=selected[-24:]
res['failed_business_reasons']={}
def safe_reasons(value):
 found=[]
 if isinstance(value,dict):
  for key,body in value.items():
   if key in ('reason_code','reason_codes','status'):
    vals=body if isinstance(body,list) else [body]
    found.extend(x for x in vals if isinstance(x,str) and __import__('re').fullmatch('[A-Z][A-Z0-9_]{2,120}',x))
   elif isinstance(body,(dict,list)):found.extend(safe_reasons(body))
 elif isinstance(value,list):
  for body in value:found.extend(safe_reasons(body))
 return found
for rid,parts in chunks.items():
 text='\n'.join(parts);body=None
 try:body=json.loads(text)
 except ValueError:
  for i,ch in enumerate(text):
   if ch!='{':continue
   try:body,_=json.JSONDecoder().raw_decode(text[i:]);break
   except ValueError:pass
 res['failed_business_reasons'][rid]={'json_parsed':isinstance(body,dict),'codes':dict(collections.Counter(safe_reasons(body)))}
print(json.dumps(res,ensure_ascii=True))
'''
start=datetime.datetime.now(ZoneInfo('Asia/Shanghai'));prefix=start.strftime('%Y%m%dT%H%M%S%f')+'-start-baseline'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm','cd /www1/Agu/liangjian-funnel-workflow && .venv/bin/python -B -'],input=source.encode(),capture_output=True,timeout=30)
for suffix,data in (('stdout.json',p.stdout),('stderr.log',p.stderr)):
 with (out/(prefix+'-'+suffix)).open('xb') as f:f.write(data)
receipt={'actual_at':start.isoformat(),'exit_code':p.returncode,'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'stdout_sha256':hashlib.sha256(p.stdout).hexdigest()}
with (out/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(prefix);print(p.stdout.decode() if p.returncode==0 else p.stderr.decode())
