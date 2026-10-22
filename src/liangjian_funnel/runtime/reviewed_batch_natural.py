"""Natural close candidate -> real morning edges -> one workflow-DB switch.

Unreviewed rows never enter execution_plans. Account/lot/T+1 storage is not
opened here. Natural morning's ordinary engine owns its actual account capture.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from .reviewed_batch_switch import canonical, rowset_sha256

ACCOUNT='local-paper'


def sha(value): return hashlib.sha256(canonical(value).encode()).hexdigest()


def stage_natural_candidate(store, batch, *, run_id, observed_at, regeneration=None):
    """Persist original intended rows and old-target CAS, never publish them."""
    source_scope=getattr(regeneration,'source_scope',None)
    if source_scope is not None:
        from .close_regeneration import require_regeneration_source_unchanged
        require_regeneration_source_unchanged(regeneration)
        if run_id!=regeneration.run_id:raise ValueError('REGEN_TARGET_RUN_CHANGED')
    if not batch:
        if source_scope is not None:
            def preserve(db):
                from .close_regeneration import require_regeneration_candidate_scope
                require_regeneration_candidate_scope(db,source_scope)
                return dict(status='EMPTY_CANDIDATE_NOT_STAGED',plan_ids=[],execution_plans_written=0)
            return store._write(preserve)
        return dict(status='EMPTY_CANDIDATE_NOT_STAGED',plan_ids=[])
    if not isinstance(run_id,str) or not run_id or len(run_id)>64:raise ValueError('NATURAL_CANDIDATE_RUN_INVALID')
    rows=[];days=set();lanes=set();ids=set()
    for item in batch:
        value=json.loads(canonical(item));end=datetime.fromisoformat(value['expires_at'])
        if (end.utcoffset() is None or value.get('status')!='PENDING_MORNING_REVIEW'
                or value.get('valid_from') is not None or value.get('parent_plan_id') is not None
                or value['payload'].get('source_run_id')!=run_id or value['payload'].get('symbol')!=value['symbol']
                or value['plan_id'] in ids):raise ValueError('NATURAL_CANDIDATE_ROW_INVALID')
        ids.add(value['plan_id']);days.add(end.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat());lanes.add(value['lane_id'])
        rows.append(dict(plan_id=value['plan_id'],lane_id=value['lane_id'],symbol=value['symbol'],
            status='PENDING_MORNING_REVIEW',plan_version=value.get('plan_version',1),valid_from=None,
            expires_at=end.isoformat(),payload_json=canonical(value['payload']),
            created_at=observed_at.isoformat(),updated_at=observed_at.isoformat()))
    if len(days)!=1 or len(lanes)!=1:raise ValueError('NATURAL_CANDIDATE_MIXED_SCOPE')
    day,lane=next(iter(days)),next(iter(lanes));rows.sort(key=lambda r:(r['symbol'],r['plan_id']))
    if source_scope is not None and (day!=source_scope['target_trade_date'] or lane!=source_scope['lane_id']):
        raise ValueError('REGEN_TARGET_CANDIDATE_SCOPE_CHANGED')
    scope='bd_'+sha([run_id,day,lane])[:28]
    def operation(db):
        if source_scope is not None:
            from .close_regeneration import require_regeneration_candidate_scope
            require_regeneration_candidate_scope(db,source_scope)
        old=[dict(r) for r in db.execute("SELECT * FROM execution_plans WHERE lane_id=? AND substr(expires_at,1,10)=? AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY') ORDER BY plan_id",(lane,day))]
        pointer=db.execute('SELECT revision FROM active_reviewed_batch_scope WHERE account_id=? AND trade_date=?',(ACCOUNT,day)).fetchone()
        body=dict(schema_version='natural-review-candidate/1',source_namespace='NATURAL_MORNING_CANDIDATE',
            scope_id=scope,account_id=ACCOUNT,trade_date=day,lane_id=lane,run_id=run_id,plan_rows=rows,
            expected_old_plan_ids=[r['plan_id'] for r in old],expected_old_rows_sha256=rowset_sha256(old),
            expected_pointer_revision=pointer['revision'] if pointer else None)
        text=canonical(body);digest=sha(body)
        prior=db.execute('SELECT body_sha256,body_json,state FROM natural_review_candidates WHERE scope_id=?',(scope,)).fetchone()
        if prior is not None:
            if prior['body_sha256']!=digest or prior['body_json']!=text:raise ValueError('NATURAL_CANDIDATE_IMMUTABLE_CONFLICT')
        else:
            db.execute('INSERT INTO natural_review_candidates VALUES (?,?,?,?,?,?,?,?,?)',
                (scope,ACCOUNT,day,lane,run_id,digest,text,observed_at.isoformat(),'PENDING'))
        head=db.execute('SELECT * FROM natural_review_candidate_head WHERE account_id=? AND trade_date=? AND lane_id=?',(ACCOUNT,day,lane)).fetchone()
        revision=head['revision'] if head and head['scope_id']==scope else (head['revision']+1 if head else 1)
        if head is None or head['scope_id']!=scope:
            db.execute('INSERT INTO natural_review_candidate_head VALUES (?,?,?,?,?,?) ON CONFLICT(account_id,trade_date,lane_id) DO UPDATE SET scope_id=excluded.scope_id,body_sha256=excluded.body_sha256,revision=excluded.revision',
                (ACCOUNT,day,lane,scope,digest,revision))
        return dict(status='CANDIDATE_PENDING_REVIEW',scope_id=scope,body_sha256=digest,revision=revision,
            target_trade_date=day,plan_ids=[r['plan_id'] for r in rows],execution_plans_written=0)
    return store._write(operation)


def read_natural_candidate(settings, *, day):
    path=Path(settings.state_db_path)
    if not path.is_file() or path.is_symlink():raise ValueError('NATURAL_CANDIDATE_DATABASE_UNAVAILABLE')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='natural_review_candidate_head'").fetchone():return None
        row=db.execute('SELECT h.scope_id,h.body_sha256,h.revision,c.body_json,c.body_sha256 AS stored_sha256,c.state FROM natural_review_candidate_head h LEFT JOIN natural_review_candidates c ON c.scope_id=h.scope_id WHERE h.account_id=? AND h.trade_date=? AND h.lane_id=?',
            (ACCOUNT,day,settings.research_primary_lane_id)).fetchone()
        if row is None:return None
        if (row['body_json'] is None or row['stored_sha256']!=row['body_sha256']
                or type(row['revision']) is not int or row['revision']<1
                or hashlib.sha256(row['body_json'].encode()).hexdigest()!=row['body_sha256']):
            raise ValueError('NATURAL_CANDIDATE_HEAD_UNPROVEN')
        body=json.loads(row['body_json'])
        if (body.get('scope_id')!=row['scope_id'] or body.get('account_id')!=ACCOUNT or body.get('trade_date')!=day
                or body.get('lane_id')!=settings.research_primary_lane_id or row['state'] not in {'PENDING','SWITCHED'}):
            raise ValueError('NATURAL_CANDIDATE_SCOPE_CONFLICT')
        return dict(body=body,scope_id=row['scope_id'],body_sha256=row['body_sha256'],revision=row['revision'],state=row['state'])


def candidate_planning_source(settings, candidate, *, now, calendar):
    from .team_v2_sources import project_planning_rows
    result=project_planning_rows(settings,deepcopy(candidate['body']['plan_rows']),now=now,calendar=calendar)
    result.update(source_namespace='NATURAL_MORNING_CANDIDATE',candidate_scope_id=candidate['scope_id'],
        candidate_sha256=candidate['body_sha256'],candidate_revision=candidate['revision'])
    return result


def finish_natural_candidate(settings, candidate, *, calendar):
    from .reviewed_batch_evidence import build_reviewed_batch_evidence
    from .state import RuntimeStore
    body=candidate['body'];scope=candidate['scope_id']
    rows=[dict(plan_id=r['plan_id'],lane_id=r['lane_id'],symbol=r['symbol'],status=r['status'],
        plan_version=r['plan_version'],valid_from=None,expires_at=r['expires_at'],payload=json.loads(r['payload_json']))
        for r in body['plan_rows']]
    bundle=build_reviewed_batch_evidence(review_root=Path(settings.root).resolve(),source_namespace='NATURAL_MORNING',
        workflow_db_path=settings.state_db_path,scope_id=scope,account_id=ACCOUNT,trade_date=body['trade_date'],
        lane_id=body['lane_id'],run_id=body['run_id'],a6_meeting_id=scope+':risk',pm_meeting_id=scope+':pm',candidate_rows=rows)
    store=RuntimeStore(settings.state_db_path)
    store.stage_reviewed_batch(bundle)
    result=store.commit_reviewed_batch_switch(scope,expected_old_plan_ids=body['expected_old_plan_ids'],
        expected_old_rows_sha256=body['expected_old_rows_sha256'],expected_pointer_revision=body['expected_pointer_revision'])
    return result
