"""Verify a fixed, operator-collected runtime receipt. Never collects or writes it.

The serving Node injects its own PID and Linux /proc start ticks, not HTTP input.
PID reuse, checkout/config changes, missing provenance or a modified receipt all
fail closed. A policy limit is configuration evidence, not per-order sizing.
"""
from datetime import datetime,timezone,timedelta
import hashlib
import json
from pathlib import Path
import re

PATHS={'.env','config/risk_limits.yaml','config/team_v2_deployment.yaml'}

def digest(raw):return hashlib.sha256(raw).hexdigest()
def canonical(body):return json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def small(path,budget):
    if path.is_symlink() or not path.is_file() or path.stat().st_size>budget:raise ValueError('RUNTIME_SOURCE_SCOPE')
    return path.read_bytes()

def git_head(root):
    git=root/'.git'
    if git.is_file():
        pointer=small(git,4096).decode().strip()
        if not pointer.startswith('gitdir: '):raise ValueError('RUNTIME_HEAD_UNKNOWN')
        git=(root/pointer[8:]).resolve()
    else:git=git.resolve()
    value=small(git/'HEAD',4096).decode().strip()
    if re.fullmatch('[a-f0-9]{40}',value):return value
    if not value.startswith('ref: '):raise ValueError('RUNTIME_HEAD_UNKNOWN')
    ref=value[5:]
    if not re.fullmatch(r'refs/[A-Za-z0-9_./-]+',ref) or '..' in ref.split('/'):raise ValueError('RUNTIME_HEAD_UNKNOWN')
    common=git
    if (git/'commondir').is_file():common=(git/small(git/'commondir',4096).decode().strip()).resolve()
    for directory in (git,common):
        path=directory/ref
        if path.is_file():
            head=small(path,4096).decode().strip()
            if re.fullmatch('[a-f0-9]{40}',head):return head
        packed=directory/'packed-refs'
        if packed.is_file():
            for line in small(packed,4*1024*1024).decode().splitlines():
                if line.endswith(' '+ref) and re.fullmatch('[a-f0-9]{40}',line[:40]):return line[:40]
    raise ValueError('RUNTIME_HEAD_UNKNOWN')

def read_runtime_receipt(root,query):
    missing={'status':'UNVERIFIED','missing_reason':'RUNTIME_RECEIPT_UNAVAILABLE'}
    try:
        runtime=query.get('_runtime') or {};pid=runtime.get('pid');ticks=runtime.get('process_start_ticks')
        if type(pid) is not int or pid<1 or not isinstance(ticks,str) or not re.fullmatch(r'\d{1,24}',ticks):raise ValueError('SERVING_NODE_IDENTITY_UNPROVEN')
        path=root/'state/team_v2/ui_runtime_status.json'
        if not path.resolve().is_relative_to(root):raise ValueError('RUNTIME_RECEIPT_SCOPE')
        raw=small(path,16384);receipt=json.loads(raw);body=receipt['body']
        if not isinstance(body,dict):raise ValueError('RUNTIME_RECEIPT_SCHEMA')
        if receipt.get('schema')!='team-ui-runtime-status/1' or digest(canonical(body).encode())!=receipt.get('body_sha256'):raise ValueError('RUNTIME_RECEIPT_HASH_INVALID')
        if type(body.get('pid')) is not int or body.get('pid')!=pid or body.get('process_start_ticks')!=ticks:raise ValueError('RUNTIME_NODE_IDENTITY_CHANGED')
        if body.get('head')!=git_head(root):raise ValueError('RUNTIME_HEAD_CHANGED')
        now=datetime.now(timezone.utc);observed=datetime.fromisoformat(body['observed_at'])
        if observed.utcoffset() is None or observed>now:raise ValueError('RUNTIME_OBSERVATION_INVALID')
        if query['trade_date']!=now.astimezone(timezone(timedelta(hours=8))).date().isoformat():raise ValueError('RUNTIME_NOT_HISTORICAL_RECONSTRUCTION')
        sources=body['sources']
        if not isinstance(sources,list) or len(sources)!=3 or any(not isinstance(x,dict) for x in sources) or {x['path'] for x in sources}!=PATHS:raise ValueError('RUNTIME_CONFIG_SOURCE_INCOMPLETE')
        for item in sources:
            file=root/item['path']
            if not file.resolve().is_relative_to(root) or digest(small(file,262144))!=item['sha256']:raise ValueError('RUNTIME_CONFIG_CHANGED')
        settings=body['settings']
        if not isinstance(settings,dict):raise ValueError('RUNTIME_SETTINGS_UNPROVEN')
        if settings.get('a4_trader_mode') not in {'control','record'} or settings.get('broker_adapter')!='local_paper':raise ValueError('RUNTIME_SETTINGS_UNPROVEN')
        names=[settings.get(k) for k in ('monitor_model','team_intraday_model','review_model')]
        research=settings.get('research_models')
        if not isinstance(research,list) or not 1<=len(research)<=5:raise ValueError('RUNTIME_SETTINGS_UNPROVEN')
        if any(not isinstance(x,str) or not re.fullmatch(r'[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)?',x) for x in names+research):raise ValueError('RUNTIME_SETTINGS_UNPROVEN')
        allowed={k:settings[k] for k in ('a4_trader_mode','broker_adapter','monitor_model','team_intraday_model','research_models','review_model')}
        limits={}
        configured=body.get('risk_limits') or {}
        if not isinstance(configured,dict):raise ValueError('RUNTIME_LIMITS_UNPROVEN')
        for key,value in configured.items():
            if key in {'max_new_entries','max_cluster_signals'} and type(value) is int and 0<=value<=100:limits[key]=value
            elif key in {'min_cash_reserve_pct','max_position_pct','probe_max_position_pct'} and type(value) in (int,float,str) and re.fullmatch(r'0(?:\.\d+)?|1(?:\.0+)?',str(value)):limits[key]=float(value)
            elif key=='no_new_entry_after' and isinstance(value,str) and re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',value):limits[key]=value
        return dict(status='VERIFIED_PROCESS_AND_CONFIG_BOUND',observed_at=body['observed_at'],pid=pid,head=body['head'],settings=allowed,
            configured_limits=limits,limits_basis='CONFIGURATION_ONLY_NOT_PER_ORDER_QUANTITY_AUDIT',
            source={'table':'ui_runtime_status','id':'current-serving-node','sha256':digest(raw),'body_sha256':receipt['body_sha256']})
    except (OSError,ValueError,KeyError,TypeError,UnicodeError):return missing
