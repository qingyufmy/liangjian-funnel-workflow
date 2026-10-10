"""Local W3 small-package delivery. No review, scheduler or production wiring.

The only remote operation is an explicitly enabled strict read-only SSH reader.
Hash bindings prove byte consistency, not historical PIT or source authenticity.
"""
from __future__ import annotations
import argparse
import base64
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import subprocess
from time import monotonic
from zoneinfo import ZoneInfo

MAX_FILE_BYTES=1024*1024
MAX_PACKAGE_BYTES=10*1024*1024
TZ=ZoneInfo('Asia/Shanghai')


def sha(raw):return hashlib.sha256(raw).hexdigest()


def _pin(value):
    if not isinstance(value,str) or re.fullmatch('[a-f0-9]{64}',value) is None:
        raise ValueError('EXPLICIT_RAW_SHA_REQUIRED')
    return value


def _pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('DUPLICATE_JSON_KEY')
        result[key]=value
    return result


def _json(raw):
    result=json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('NONFINITE_JSON')))
    if not isinstance(result,dict):raise ValueError('JSON_OBJECT_REQUIRED')
    return result


def _clock(value):
    value=value if isinstance(value,datetime) else datetime.fromisoformat(value)
    if value.tzinfo is None or value.utcoffset() is None:raise ValueError('AWARE_CLOCK_REQUIRED')
    return value.astimezone(TZ)


def _remote(value):
    if (not isinstance(value,str) or not re.fullmatch('/[A-Za-z0-9_./-]+',value)
            or any(p in ('','.','..') for p in value.split('/')[1:])
            or str(PurePosixPath(value))!=value):raise ValueError('UNSAFE_REMOTE_PATH')
    return PurePosixPath(value)


def _local(path):
    path=Path(path)
    if (not path.is_absolute() or '..' in path.parts or path.name!='shadow_outbox'
            or any(p.lower() in {'inbox','outbox','state'} for p in path.parts)):
        raise ValueError('INDEPENDENT_SHADOW_OUTBOX_REQUIRED')
    _no_alias(path)
    return path


def _no_alias(path):
    for p in (path,*path.parents):
        if p.is_symlink():raise ValueError('OUTPUT_ALIAS_REJECTED')
        if p.exists():
            s=p.lstat()
            if getattr(s,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',1024):
                raise ValueError('OUTPUT_ALIAS_REJECTED')
            if p.is_file() and s.st_nlink!=1:raise ValueError('OUTPUT_ALIAS_REJECTED')


def fetch_report(*,reader,remote_root,archive_root,report_root,bridge_root,trade_date,
                 manifest_sha256,shadow_outbox,observed_at):
    """Fetch/validate everything before local writes; source failure creates nothing.

    Interrupted local writes may leave only new independently pinned files;
    the receipt is last. A retry verifies same bytes or refuses a conflict.
    """
    if date.fromisoformat(trade_date).isoformat()!=trade_date:raise ValueError('EXPLICIT_DATE_REQUIRED')
    observed=_clock(observed_at);root=_remote(remote_root)
    roots=[_remote(p) for p in (archive_root,report_root,bridge_root)]
    if any(p==root or not p.is_relative_to(root) for p in roots):raise ValueError('REMOTE_SCOPE_INVALID')
    if any(a==b or a.is_relative_to(b) or b.is_relative_to(a) for i,a in enumerate(roots) for b in roots[i+1:]):
        raise ValueError('REMOTE_ROOT_ALIAS')
    local=_local(shadow_outbox)
    arch,docs,bridge=roots
    manifest_path=str(arch/trade_date/'final-manifest.json')
    paths={str(arch/trade_date/'report.json'):'report.json',
        str(docs/('SHADOW_DAILY_'+trade_date+'.md')):'SHADOW_DAILY_'+trade_date+'.md',
        str(docs/('PRODUCTION_EQUIVALENCE_'+trade_date+'.json')):'PRODUCTION_EQUIVALENCE_'+trade_date+'.json',
        str(bridge/trade_date/'W3_SHADOW_DAILY.json'):'W3_SHADOW_DAILY.json'}
    total=0
    def read(path):
        nonlocal total
        body=reader(path)
        if type(body) is not bytes or len(body)>MAX_FILE_BYTES:raise ValueError('FILE_BYTES_OR_SIZE_INVALID')
        total+=len(body)
        if total>MAX_PACKAGE_BYTES:raise ValueError('PACKAGE_SIZE_INVALID')
        return body
    manifest_raw=read(manifest_path)
    if sha(manifest_raw)!=_pin(manifest_sha256):raise ValueError('MANIFEST_PIN_MISMATCH')
    manifest=_json(manifest_raw)
    if (manifest.get('schema_version')!='shadow-reporting/1' or manifest.get('trade_date')!=trade_date
            or _clock(manifest.get('as_of'))>observed or _clock(manifest['as_of']).date().isoformat()!=trade_date
            or not isinstance(manifest.get('outputs'),dict) or set(manifest['outputs'])!=set(paths)):
        raise ValueError('FINAL_MANIFEST_SCOPE_OR_CLOCK_INVALID')
    for pin in manifest['outputs'].values():_pin(pin)
    bodies={}
    for path,name in paths.items():
        body=read(path)
        if sha(body)!=manifest['outputs'][path]:raise ValueError('OUTPUT_PIN_MISMATCH')
        bodies[name]=body
    report=_json(bodies['report.json']);proof=_json(bodies['PRODUCTION_EQUIVALENCE_'+trade_date+'.json'])
    delivery=_json(bodies['W3_SHADOW_DAILY.json'])
    expected_delivery={p:h for p,h in manifest['outputs'].items() if p!=str(bridge/trade_date/'W3_SHADOW_DAILY.json')}
    if (report.get('schema') not in ('shadow-day-adapter/1','shadow-reporting-gap/1')
            or report.get('trade_date')!=trade_date or proof.get('schema')!='shadow-production-equivalence/1'
            or report.get('production_equivalence')!=proof or delivery.get('schema_version')!='shadow-reporting/1'
            or delivery.get('trade_date')!=trade_date or delivery.get('outputs')!=expected_delivery
            or delivery.get('report_sha256')!=sha(bodies['report.json'])
            or type(delivery.get('notifications')) is not int or delivery['notifications']!=0
            or delivery.get('production_mutation') is not False):
        raise ValueError('DELIVERY_CONTENT_BINDING_INVALID')
    if read(manifest_path)!=manifest_raw:raise ValueError('FINAL_MANIFEST_CHANGED')
    bodies['final-manifest.json']=manifest_raw
    receipt=dict(schema_version='shadow-report-fetch/1',trade_date=trade_date,
        remote_manifest=manifest_path,manifest_raw_sha256=manifest_sha256,
        files={name:sha(b) for name,b in bodies.items()},status='BYTES_VERIFIED',
        source_authentication='UNPROVEN_LOCAL_HASH_BINDING',claude_review_received=False,
        production_mutation=False,notifications=0,models=0)
    bodies['fetch-receipt.json']=(json.dumps(receipt,sort_keys=True,ensure_ascii=False,indent=2)+'\n').encode()
    target=local/trade_date
    existing=[]
    for name,body in bodies.items():
        p=target/name;_no_alias(p)
        if p.exists():
            if not p.is_file() or p.stat().st_size>MAX_FILE_BYTES or p.read_bytes()!=body:
                raise ValueError('LOCAL_CONTENT_CONFLICT')
            existing.append(name)
    # All remote bytes and all pre-existing destinations pass before mkdir.
    target.mkdir(parents=True,exist_ok=True);_no_alias(target)
    for name,body in bodies.items():
        p=target/name;_no_alias(p)
        try:
            fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        except FileExistsError:
            _no_alias(p)
            if not p.is_file() or p.stat().st_size>MAX_FILE_BYTES or p.read_bytes()!=body:
                raise ValueError('LOCAL_CONTENT_CONFLICT')
        else:
            with os.fdopen(fd,'wb') as f:f.write(body);f.flush();os.fsync(f.fileno())
    return {**receipt,'status':'ALREADY_PRESENT' if len(existing)==len(bodies) else 'BYTES_VERIFIED'}


REMOTE_READER='''import os,sys,json,base64,hashlib,stat
p=sys.argv[1];limit=int(sys.argv[2])
if os.path.realpath(p)!=p:raise ValueError("REMOTE_ALIAS")
fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
with os.fdopen(fd,"rb") as f:
 a=os.fstat(f.fileno())
 if not stat.S_ISREG(a.st_mode) or a.st_size>limit:raise ValueError("REMOTE_FILE_LIMIT")
 raw=f.read(limit+1);b=os.fstat(f.fileno())
 if len(raw)>limit or (a.st_size,a.st_mtime_ns,a.st_ino,a.st_dev)!=(b.st_size,b.st_mtime_ns,b.st_ino,b.st_dev):raise ValueError("REMOTE_CHANGED")
 print(json.dumps(dict(path=p,raw_sha256=hashlib.sha256(raw).hexdigest(),body_base64=base64.b64encode(raw).decode("ascii"))))
'''


def ssh_reader(*,run=subprocess.run,budget_seconds=30):
    if type(budget_seconds) not in (float,int) or not 1<=budget_seconds<=45:raise ValueError('FETCH_BUDGET_INVALID')
    deadline=monotonic()+budget_seconds
    def read(path):
        path=str(_remote(path));remaining=deadline-monotonic()
        if remaining<=0:raise ValueError('FETCH_BUDGET_EXHAUSTED')
        remote='python3 -c '+shlex.quote(REMOTE_READER)+' '+shlex.quote(path)+' '+str(MAX_FILE_BYTES)
        command=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=5',
                 '-o','ConnectionAttempts=1','aurum-vm',remote]
        response=run(command,capture_output=True,timeout=min(8,remaining),check=False)
        if response.returncode!=0 or len(response.stdout)>2*MAX_FILE_BYTES or monotonic()>deadline:
            raise ValueError('SSH_READ_FAILED')
        envelope=_json(response.stdout)
        body=base64.b64decode(envelope.get('body_base64',''),validate=True)
        if envelope.get('path')!=path or len(body)>MAX_FILE_BYTES or sha(body)!=_pin(envelope.get('raw_sha256')):
            raise ValueError('SSH_BYTE_BINDING_INVALID')
        return body
    return read


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('remote-root','archive-root','report-root','bridge-root','trade-date','manifest-sha256','shadow-outbox'):
        p.add_argument('--'+key,required=True)
    p.add_argument('--enable-ssh-readonly',action='store_true',help='Explicit standalone action only; no scheduler')
    args=p.parse_args(argv)
    try:
        if not args.enable_ssh_readonly:raise ValueError('SSH_UNWIRED_EXPLICIT_ENABLE_REQUIRED')
        result=fetch_report(reader=ssh_reader(),remote_root=args.remote_root,archive_root=args.archive_root,
            report_root=args.report_root,bridge_root=args.bridge_root,trade_date=args.trade_date,
            manifest_sha256=args.manifest_sha256,shadow_outbox=args.shadow_outbox,
            observed_at=datetime.now(TZ))
        print(json.dumps(result,ensure_ascii=False));return 0
    except Exception:
        # No provider stderr/credentials/body in status output; owner unaffected.
        print(json.dumps(dict(status='SHADOW_DELIVERY_FAILED',production_mutation=False,
            notifications=0,models=0,claude_review_received=False)));return 2


if __name__=='__main__':raise SystemExit(main())
