"""Stream the approved immutable morning snapshot to local disk; hash locally."""
from pathlib import Path
import datetime,hashlib,json,subprocess,sys
from liangjian_funnel.workflow import _hash_json

REMOTE=r'''
from pathlib import Path
import hashlib,json,subprocess,sys
root=Path.cwd()
assert root==Path('/www/wwwroot/Agu/liangjian-funnel-workflow') and subprocess.check_output(['hostname'],text=True).strip()=='debian'
marker=json.loads((root/'outputs/auction_base/2026-10-15.json').read_text())
spec=marker['prepared_snapshot'];path=Path(spec['path']).resolve()
assert marker['status']=='READY' and path.is_relative_to((root/'storage/snapshots').resolve())
assert spec['snapshot_id']=='snapshot-20261015T072030+0800-164135b992c3'
before=path.stat();digest=hashlib.sha256()
with path.open('rb') as src:
 for chunk in iter(lambda:src.read(1048576),b''):
  sys.stdout.buffer.write(chunk);digest.update(chunk)
sys.stdout.buffer.flush();after=path.stat()
proof={'source':str(path),'raw_sha256':digest.hexdigest(),'bytes':before.st_size,
 'file_identity_stable':(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns),
 'spec':spec,'scope_symbols':marker['scope_symbols'],'started_at':marker['started_at'],'finished_at':marker['finished_at'],'production_writes':0}
print(json.dumps(proof,ensure_ascii=True),file=sys.stderr)
'''

def main():
 directory=Path(sys.argv[1]);directory.mkdir(parents=True,exist_ok=False)
 path=directory/'snapshot.json'
 cmd=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
 'cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- nice -n 10 .venv/bin/python -B -']
 with path.open('xb') as output, (directory/'source.stderr.json').open('xb') as error:
  p=subprocess.run(cmd,input=REMOTE.encode(),stdout=output,stderr=error,timeout=90)
 with (directory/'command.json').open('x') as out:json.dump({'command':cmd,'exit_code':p.returncode},out)
 if p.returncode:raise SystemExit('COPY_FAILED')
 proof=json.loads((directory/'source.stderr.json').read_text())
 with path.open('rb') as handle:raw_sha=hashlib.file_digest(handle,'sha256').hexdigest()
 assert proof['file_identity_stable'] and raw_sha==proof['raw_sha256'] and path.stat().st_size==proof['bytes']
 with path.open(encoding='utf-8') as handle:obj=json.load(handle)
 digest=_hash_json(obj['data']);assert digest==obj['snapshot_hash']==proof['spec']['snapshot_hash']
 assert obj['snapshot_id']==proof['spec']['snapshot_id'] and obj['as_of'][:10]=='2026-10-15'
 data=obj['data'];scope=set(proof['scope_symbols']);g0=set(data['g0_symbols'])
 excluded={r['symbol'] for r in data.get('universe_candidates',[]) if isinstance(r,dict) and r.get('research_eligible') is False and r.get('exclusion_reasons')}
 missing=sorted(scope-g0-excluded)
 result={'verified_at':datetime.datetime.now().astimezone().isoformat(),'raw_sha256':raw_sha,
  'canonical_hash':digest,'canonical_hash_matches':True,'snapshot_id':obj['snapshot_id'],'as_of':obj['as_of'],
  'requested_scope_count':len(scope),'g0_count':len(g0),'explicitly_excluded_from_scope':len(scope&excluded),'unexplained_missing':missing,
  'original_five_missing_g0':sorted({'603565.SH','600026.SH','601866.SH','603209.SH','601872.SH'}-g0),
  'data_keys':sorted(data),'production_writes':0,'models':0,'source_unchanged':True}
 with (directory/'VERIFICATION.json').open('x',encoding='utf-8') as out:json.dump(result,out,ensure_ascii=False,indent=2)
 print(json.dumps(result,ensure_ascii=True))

if __name__=='__main__':main()
