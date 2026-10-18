"""Save exact local command, actual result and content hashes; never credentials."""
import datetime,hashlib,json,pathlib,subprocess,sys
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8',errors='replace')
root=pathlib.Path(__file__).resolve().parents[2]
label=sys.argv[1];command=sys.argv[2:]
start=datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
prefix=start.strftime('%Y%m%dT%H%M%S%f')+'-'+label
p=subprocess.run(command,cwd=root,capture_output=True)
out=pathlib.Path(__file__).resolve().parent
for suffix,data in (('stdout.log',p.stdout),('stderr.log',p.stderr)):
    with (out/(prefix+'-'+suffix)).open('xb') as f:f.write(data)
paths=[x for folder in ('src/liangjian_funnel/pipeline','tests') for x in sorted((root/folder).glob('*team_v2*')) if x.is_file()]
paths += [x for x in (root/'src/liangjian_funnel/runtime/risk_engine.py',root/'config/risk_limits.yaml',root/'config/paper_broker.yaml') if x.is_file()]
paths += [x for folder in ('src/liangjian_paper_broker','src/liangjian_funnel/runtime/broker') if (root/folder).exists() for x in sorted((root/folder).rglob('*.py'))]
receipt={'actual_started_at':start.isoformat(),'finished_at':datetime.datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),'command':command,'exit_code':p.returncode,'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'stderr_sha256':hashlib.sha256(p.stderr).hexdigest(),'source_hashes':{str(x.relative_to(root)):hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
with (out/(prefix+'-receipt.json')).open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps({'prefix':prefix,**receipt}));print(p.stdout.decode(errors='replace'));print(p.stderr.decode(errors='replace'))
sys.exit(p.returncode)
