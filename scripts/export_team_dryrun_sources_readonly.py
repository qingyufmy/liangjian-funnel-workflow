"""Copy original three-day model facts/outputs, never execute a workflow.

Remote work is streaming, nice-priority and bounded. Hash verification and JSON
parsing happen locally. No environment or credential values leave the VM.
"""
import argparse
import base64
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo


REMOTE = r'''
import base64,gzip,hashlib,json,sqlite3,subprocess,sys,time,urllib.request
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
root=Path.cwd();now=datetime.now(ZoneInfo('Asia/Shanghai'))
assert root==Path('/www1/Agu/liangjian-funnel-workflow')
assert subprocess.check_output(['hostname'],text=True).strip()=='debian'
assert now.date().isoformat()=='2026-10-17'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head=='2b077f393321c1858f17546682191caeb8eaa41f'
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open('http://127.0.0.1:3210/api/shadow/job-runs-readonly',timeout=3) as f:jobs=json.loads(f.read(4*1024*1024))
assert not [j for j in jobs['recentJobRuns'] if str(j.get('status','')).upper()=='RUNNING'],'ACTIVE_TASK_PRESERVED'
mem=[int(r.split()[1]) for r in Path('/proc/meminfo').read_text().splitlines() if r.startswith('MemAvailable:')][0]
assert mem>=800*1024,'RESOURCE_GATE'
s=Settings.from_env(root=root);deadline=time.monotonic()+70
sources=[]
for day,key in [('2026-10-14','0d4c70977b7e'),('2026-10-15','b3d3c85c42f4'),('2026-10-16','624199bb9c19')]:
 snapshots=sorted(s.snapshot_dir.glob('snapshot-'+day.replace('-','')+'T*-'+key+'.json'))
 assert len(snapshots)==1,'CLOSE_SNAPSHOT_NOT_UNIQUE:'+day
 sources.append(('snapshot',day,snapshots[0]))
 prefix='research_'+day+'-close-'+key+'_lane_1'
 for suffix in ('.json','.decisions.json','.decisions.ndjson'):
  p=s.workflow_output_dir/'research'/(prefix+suffix)
  assert p.is_file() and not p.is_symlink(),'ORIGINAL_LANE_SOURCE_MISSING'
  sources.append(('research',day,p))
inventory=[dict(kind=kind,trade_date=day,relative=str(p.relative_to(root)),bytes=p.stat().st_size) for kind,day,p in sources]
if not EXPORT:
 print(json.dumps(dict(actual_at=now.isoformat(),head=head,files=inventory,mem_available_kib=mem,production_writes=0,models=0,providers=0)));sys.exit(0)
gz=gzip.GzipFile(fileobj=sys.stdout.buffer,mode='wb',mtime=0,compresslevel=1)
def emit(body):gz.write((json.dumps(body,separators=(',',':'),ensure_ascii=True)+'\n').encode())
emit(dict(kind='manifest',actual_at=now.isoformat(),head=head,files=inventory,mem_available_kib=mem,production_writes=0,models=0,providers=0))
total=0
def content(relative,kind,day,parts):
 global total
 h=hashlib.sha256();n=0
 emit(dict(kind='file_start',relative=relative,source_kind=kind,trade_date=day))
 for raw in parts:
  n+=len(raw);total+=len(raw)
  assert n<=512*1024*1024 and total<=2*1024**3 and time.monotonic()<deadline,'EXPORT_BOUND'
  h.update(raw);emit(dict(kind='chunk',data=base64.b64encode(raw).decode()))
 emit(dict(kind='file_end',relative=relative,bytes=n,sha256=h.hexdigest()))
for kind,day,p in sources:
 assert p.resolve().is_relative_to(root) and not p.is_symlink(),'SOURCE_SCOPE'
 before=p.stat()
 with p.open('rb') as f:content(str(p.relative_to(root)),kind,day,iter(lambda:f.read(65536),b''))
 after=p.stat()
 assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns),'SOURCE_CHANGED'
# Exact original pre-model facts and response, not a successful regenerated A5.
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
c.execute('PRAGMA query_only=ON');c.set_progress_handler(lambda:int(time.monotonic()>deadline),1000);c.execute('BEGIN')
for day in ('2026-10-14','2026-10-15','2026-10-16'):
 row=c.execute('SELECT input_hash,fact_snapshot_json,report_json,created_at FROM a5_daily_reviews WHERE trade_date=? AND review_kind=? ORDER BY created_at DESC LIMIT 1',(day,'POST_CLOSE')).fetchone()
 if row is None:emit(dict(kind='missing',source_kind='a5_original',trade_date=day));continue
 body=json.dumps(dict(input_hash=row[0],facts=json.loads(row[1]),report=json.loads(row[2]) if row[2] else None,created_at=row[3]),ensure_ascii=False,separators=(',',':')).encode()
 content('a5-original/'+day+'.json','a5_original',day,(body[i:i+65536] for i in range(0,len(body),65536)))
c.rollback();c.close()
emit(dict(kind='complete',finished_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),bytes=total));gz.close()
'''


def unpack(archive, directory):
    rows=[];manifest=None;complete=None;target=None;handle=None;digest=None;size=0
    try:
        with gzip.open(archive,'rt',encoding='utf-8') as stream:
            for line in stream:
                item=json.loads(line);kind=item['kind']
                if kind=='manifest':manifest=item
                elif kind=='file_start':
                    if handle is not None:raise ValueError('ARCHIVE_ORDER')
                    rel=Path(item['relative'])
                    if rel.is_absolute() or '..' in rel.parts or ':' in str(rel):raise ValueError('ARCHIVE_SCOPE')
                    target=directory/rel;target.parent.mkdir(parents=True,exist_ok=True)
                    handle=target.open('xb');digest=hashlib.sha256();size=0;meta=item
                elif kind=='chunk':
                    if handle is None:raise ValueError('ARCHIVE_ORDER')
                    raw=base64.b64decode(item['data'],validate=True);handle.write(raw);digest.update(raw);size+=len(raw)
                elif kind=='file_end':
                    if handle is None or meta['relative']!=item['relative'] or size!=item['bytes'] or digest.hexdigest()!=item['sha256']:
                        raise ValueError('ARCHIVE_HASH_MISMATCH')
                    handle.close();handle=None;rows.append(dict(meta,sha256=item['sha256'],bytes=size))
                elif kind=='missing':rows.append(item)
                elif kind=='complete':complete=item
                else:raise ValueError('ARCHIVE_KIND')
        if handle is not None or manifest is None or complete is None:raise ValueError('ARCHIVE_INCOMPLETE')
        return dict(source=manifest,files=rows,completion=complete,archive_sha256=hashlib.file_digest(archive.open('rb'),'sha256').hexdigest())
    finally:
        if handle is not None:handle.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--export',action='store_true');a=p.parse_args()
    config=subprocess.check_output(['ssh','-G','aurum-vm'],text=True,stderr=subprocess.DEVNULL)
    assert 'hostname 192.168.0.254' in config.splitlines()
    known=subprocess.check_output(['ssh-keygen','-F','192.168.0.254'],text=True)
    pins={base64.b64encode(hashlib.sha256(base64.b64decode(r.split()[2])).digest()).decode().rstrip('=') for r in known.splitlines() if r and not r.startswith('#')}
    assert 'LnPSC/kfZ7jQtzj55siqgWOkW0xy/6cGyoAwglS6yQ0' in pins
    if a.output.exists():raise ValueError('REFUSE_OVERWRITE')
    a.output.mkdir(parents=True)
    code=('EXPORT='+repr(a.export)+'\n'+REMOTE).encode()
    cmd=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','aurum-vm',
        'cd /www1/Agu/liangjian-funnel-workflow && runuser -u www -- nice -n 10 .venv/bin/python -B -']
    archive=a.output/('sources.jsonl.gz' if a.export else 'inventory.json')
    with archive.open('xb') as out,(a.output/'ssh.stderr.log').open('xb') as err:
        result=subprocess.run(cmd,input=code,stdout=out,stderr=err,timeout=90)
    command=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),command=cmd,exit_code=result.returncode,remote_code_sha256=hashlib.sha256(code).hexdigest(),strict_original_pin=True)
    with (a.output/'command.json').open('x') as f:json.dump(command,f,indent=2)
    if result.returncode:raise SystemExit('SOURCE_EXPORT_FAILED:'+str(result.returncode))
    receipt=unpack(archive,a.output/'files') if a.export else json.loads(archive.read_text())
    with (a.output/'RECEIPT.json').open('x',encoding='utf-8') as f:json.dump(receipt,f,ensure_ascii=False,indent=2)
    print(json.dumps(dict(exit_code=0,actual_at=command['actual_at'],files=len(receipt['files']),bytes=receipt.get('completion',{}).get('bytes'),production_writes=0,model_calls=0)))


if __name__=='__main__':main()
