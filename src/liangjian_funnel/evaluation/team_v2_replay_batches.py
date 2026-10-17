"""Index sealed six-day evidence into chronological coordinator inputs.

Every scheduled minute remains explicit, including missing evidence and empty
scope. Terminal plans stay available for held-position protection, but never
become fresh-entry candidates. No models, stores or providers are constructed.
"""
from contextlib import closing
from datetime import timedelta
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

from .team_v2_replay_source import SIX_DAYS,activation_reason,session_minutes,sha
from ..runtime.team_v2_sources import stamp
from ..runtime.trader_guardrails import is_closed_trader_tick


def _file_hash(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def read_sealed_source(directory):
    """Validate existing source once, not another market-data acquisition."""
    directory=Path(directory).resolve();receipt=json.loads((directory/'PREPARATION.json').read_bytes())
    archive=directory/'frames.jsonl.gz';database=Path(receipt['source_path'])
    if (receipt.get('status')!='SOURCE_PREPARED_NOT_EXECUTED' or tuple(receipt['required_six_days'])!=SIX_DAYS
            or archive.is_symlink() or database.is_symlink()
            or _file_hash(archive)!=receipt['archive_sha256']
            or _file_hash(database)!=receipt['source_database_sha256']):raise ValueError('REPLAY_SEALED_SOURCE_INVALID')
    records=[];digest=hashlib.sha256()
    with gzip.open(archive,'rb') as stream:
        for raw in stream:
            digest.update(raw);record=json.loads(raw)
            if record.get('quote_basis')!='REPLAY_BAR_DERIVED':raise ValueError('REPLAY_BATCH_QUOTE_BASIS_INVALID')
            if record['kind']=='plan_minute' and sha({k:v for k,v in record.items() if k!='frame_sha256'})!=record['frame_sha256']:
                raise ValueError('REPLAY_BATCH_FRAME_HASH_INVALID')
            records.append(record)
    if digest.hexdigest()!=receipt['uncompressed_frames_sha256']:raise ValueError('REPLAY_BATCH_STREAM_HASH_INVALID')
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        plans={r['plan_id']:dict(r) for r in db.execute('SELECT * FROM execution_plans')};db.rollback()
    if _file_hash(database)!=receipt['source_database_sha256']:raise ValueError('REPLAY_SOURCE_CHANGED')
    return receipt,records,plans


def chronological_batches(records,plans,*,days=SIX_DAYS):
    """Entry eligibility only: no claim of PM/risk/quote or model permission."""
    if tuple(days)!=SIX_DAYS:raise ValueError('REPLAY_SIX_DAYS_REQUIRED')
    manifests={};groups={}
    for record in records:
        day=record['trade_date']
        if day not in days:raise ValueError('REPLAY_BATCH_OUTSIDE_DAY_SCOPE')
        if record['kind']=='day':
            if day in manifests:raise ValueError('REPLAY_DUPLICATE_DAY_MANIFEST')
            manifests[day]=record
        elif record['kind']=='plan_minute':
            key=(day,record['decision_at']);groups.setdefault(key,[]).append(record)
    if set(manifests)!=set(days):raise ValueError('REPLAY_DAY_MANIFEST_MISSING')
    for day in days:
        terminal=set();scheduled=session_minutes(day);scope=set(manifests[day]['observed_plan_ids'])
        allowed={at.isoformat() for at in scheduled}
        if any(key[0]==day and key[1] not in allowed for key in groups):raise ValueError('REPLAY_BATCH_OUTSIDE_SESSION')
        for at in scheduled:
            frames=sorted(groups.get((day,at.isoformat()),[]),key=lambda r:(r['lane_id'],r['plan_id']))
            seen=set();entries=[];exit_only=[];limited=[]
            for frame in frames:
                identity=(frame['lane_id'],frame['plan_id'])
                if identity in seen:raise ValueError('REPLAY_DUPLICATE_PLAN_MINUTE')
                seen.add(identity);pid=frame['plan_id'];plan=plans.get(pid)
                if frame['original_action'] in {'PLAN_INVALIDATED','PLAN_EXPIRED'}:terminal.add(pid)
                reason='REPLAY_ORIGINAL_PLAN_BINDING_MISSING' if plan is None else activation_reason(frame,plan,terminal_seen=pid in terminal)
                if plan is not None and sha(plan)!=frame['original_plan_row_sha256']:raise ValueError('REPLAY_BATCH_PLAN_HASH_INVALID')
                if frame['status']!='SOURCE_BOUND_RULES_PENDING':limited.append(dict(frame=frame,reason_codes=frame['reason_codes']))
                elif reason:exit_only.append(dict(frame=frame,reason_code=reason))
                else:entries.append(frame)
            observed={r['plan_id'] for r in frames}
            yield dict(trade_date=day,decision_at=at.isoformat(),closed_bar_end=(at-timedelta(minutes=1)).isoformat(),
                model_tick=is_closed_trader_tick(at-timedelta(minutes=1)),source_frames=frames,
                entry_source_frames=entries,exit_only_source_frames=exit_only,data_limited_source_frames=limited,
                absent_original_plan_ids=sorted(scope-observed),
                status='EMPTY_SCOPE' if not scope else 'NO_ORIGINAL_EVENTS' if not frames else 'ORIGINAL_EVENTS_BOUND',
                quote_basis='REPLAY_BAR_DERIVED',emotion_state='DATA_BLOCK',leader_open=False,
                model_permission='PM_AND_RISK_AND_SOURCE_GATES_STILL_REQUIRED',
                authority='SOURCE_BATCH_INDEX_NOT_EXECUTION_OR_RETURN_PROOF')
