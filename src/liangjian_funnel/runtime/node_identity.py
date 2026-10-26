"""Durable process/host identity observation by the existing internal job.

No process is restarted and a change is not labelled a crash or an OOM.
Notification identity binds both the previous and newly observed process.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path

from .team_v2_store import canonical


def capture_identity(settings):
    root=Path(settings.root).resolve(); pid=os.getppid(); proc=Path('/proc')/str(pid)
    if proc.joinpath('cwd').resolve()!=root or proc.joinpath('exe').resolve().name not in {'node','nodejs'}:
        raise ValueError('NODE_PARENT_IDENTITY_UNPROVEN')
    stat=proc.joinpath('stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
    return dict(root=str(root),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                pid=pid,start_ticks=fields[19])


def observe_node_identity(settings, store, *, now, identity=None):
    from .team_v2_internal import InternalOutbox
    body=dict(identity if identity is not None else capture_identity(settings))
    if (body.get('root')!=str(Path(settings.root).resolve()) or type(body.get('pid')) is not int
            or body['pid']<=0 or not str(body.get('start_ticks','')).isdigit() or not body.get('boot_id')):
        raise ValueError('NODE_RUNTIME_IDENTITY_INVALID')
    text=canonical(body); digest=hashlib.sha256(text.encode()).hexdigest()
    root_hash=hashlib.sha256(body['root'].encode()).hexdigest()
    with closing(store._connect()) as db, db:
        db.execute('CREATE TABLE IF NOT EXISTS node_runtime_identity ('
            'root_hash TEXT PRIMARY KEY,body_json TEXT NOT NULL,body_sha256 TEXT NOT NULL,observed_at TEXT NOT NULL)')
        old=db.execute('SELECT body_json,body_sha256 FROM node_runtime_identity WHERE root_hash=?',(root_hash,)).fetchone()
    if old and hashlib.sha256(old[0].encode()).hexdigest()!=old[1]:
        raise ValueError('NODE_RUNTIME_IDENTITY_MARKER_INVALID')
    if old and old[1]==digest:return dict(status='UNCHANGED',identity_sha256=digest,alert_enqueued=False)
    event=None
    if old:
        previous=json.loads(old[0])
        source='node-identity:'+hashlib.sha256((old[1]+digest).encode()).hexdigest()
        event=InternalOutbox(store).enqueue(account_id='local-paper',trade_date=now.date().isoformat(),
            kind='ALERT',source_id=source,observed_at=now,priority=0,
            body=dict(msg_type='text',content={'text':'量见运行身份变化：'+
                ('主机启动ID变化；' if previous['boot_id']!=body['boot_id'] else 'Node进程变化；')+
                f"旧PID={previous['pid']}，新PID={body['pid']}。原因需核验；此观察不自动解锁、重启或放行交易。"}))
    # Queue before advancing the cursor. Interrupted delivery is retried by the
    # existing durable sender under the same event identity, never resent here.
    with closing(store._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        current=db.execute('SELECT body_sha256 FROM node_runtime_identity WHERE root_hash=?',(root_hash,)).fetchone()
        if (current[0] if current else None)!=(old[1] if old else None):
            return dict(status='CONCURRENT_OBSERVATION',alert_enqueued=event is not None,event_id=event)
        db.execute('INSERT INTO node_runtime_identity VALUES (?,?,?,?) ON CONFLICT(root_hash) DO UPDATE '
            'SET body_json=excluded.body_json,body_sha256=excluded.body_sha256,observed_at=excluded.observed_at',
            (root_hash,text,digest,now.isoformat()))
    return dict(status='CHANGED' if old else 'BASELINED',identity_sha256=digest,
                alert_enqueued=event is not None,event_id=event)
