"""0319 production source-code check only; no runtime/app/store construction."""
import argparse,base64,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
from run_team_a5_frozen_dryrun import save


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    out=a.output.resolve()
    if out.exists() or not out.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
    if 'hostname 192.168.0.254' not in config.splitlines():raise ValueError('SSH_HOST_INVALID')
    known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
    pins={base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=')
        for r in known.splitlines() if r and not r.startswith('#')}
    if 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' not in pins:raise ValueError('SSH_PIN_INVALID')
    code=b'''import ast,hashlib,json,subprocess
from pathlib import Path
root=Path.cwd();assert root==Path('/www1/Agu/liangjian-funnel-workflow')
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f'
path=root/'src/liangjian_funnel/pipeline/research/common.py';raw=path.read_bytes();text=raw.decode('utf-8-sig')
function=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='_prompt_replacements')
found=[]
for node in ast.walk(function):
 if isinstance(node,ast.If) and 'policy_macro_discovery and name in' in ast.unparse(node.test):
  fields=[e.value for n in ast.walk(node.test) if isinstance(n,ast.Set) for e in n.elts if isinstance(e,ast.Constant)]
  found.append(dict(fields=fields,source=ast.get_source_segment(text,node),line=node.lineno))
assert len(found)==1
print(json.dumps(dict(head=head,root=str(root),source_sha256=hashlib.sha256(raw).hexdigest(),projection=found[0],production_writes=0)))
'''
    start=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    response=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
        'cd /www1/Agu/liangjian-funnel-workflow && runuser -u www -- .venv/bin/python -B -'],
        input=code,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
    if response.returncode:raise ValueError('A1_PROJECTION_READONLY_FAILED_EXIT_'+str(response.returncode))
    result=json.loads(response.stdout);result.update(actual_started_at=start,actual_finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        reason='EXISTING_PRODUCTION_DISCOVERY_EXPLICITLY_SETS_RAW_FIELDS_NONE_PACKET_IS_SOLE_MODEL_SOURCE',exit_code=response.returncode)
    out.mkdir(parents=True);save(out/'PRODUCTION_CODE.json',result)
    print(json.dumps(dict(path=str(out/'PRODUCTION_CODE.json'),sha256=hashlib.sha256((out/'PRODUCTION_CODE.json').read_bytes()).hexdigest(),
        head=result['head'],line=result['projection']['line'],models=0,production_writes=0)));return 0

if __name__=='__main__':sys.exit(main())
