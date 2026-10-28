"""Natural close candidate -> real morning edges -> one workflow-DB switch.

Unreviewed rows never enter execution_plans. Account/lot/T+1 storage is not
opened here. Natural morning's ordinary engine owns its actual account capture.
"""
from ..research_slots import research_slot, require_research_slot
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


def stage_formal_empty(store, *, run_id, source_trade_date, target_trade_date,
                       lane_id, source_evidence, blocked, observed_at, calendar):
    """Persist a proved zero-candidate result without touching execution rows.

    Only the deterministic formalization entry calls this after the unchanged
    publication filters. It cannot replace a pending/nonempty candidate or an
    activated batch. Repeating the same source is an idempotent read.
    """
    from datetime import date
    import re
    source=date.fromisoformat(source_trade_date);target=date.fromisoformat(target_trade_date)
    if (calendar.next_trading_day(source)!=target or observed_at.utcoffset() is None
            or not isinstance(blocked,list) or not blocked or not source_evidence
            or any(not re.fullmatch('[0-9a-f]{64}',v) for v in source_evidence.values())
            or any(not isinstance(r,dict) or not r.get('reason') for r in blocked)):
        raise ValueError('FORMAL_EMPTY_SOURCE_UNPROVEN')
    source_hash=sha(dict(run_id=run_id,source_trade_date=source_trade_date,
        target_trade_date=target_trade_date,lane_id=lane_id,source_evidence=source_evidence,blocked=blocked))
    scope='empty_'+source_hash[:28]
    body=dict(schema_version='natural-review-empty/1',source_namespace='NATURAL_ZERO_PLAN_SOURCE',
        scope_id=scope,account_id=ACCOUNT,trade_date=target_trade_date,source_trade_date=source_trade_date,
        lane_id=lane_id,run_id=run_id,plan_rows=[],blocked=blocked,source_evidence=source_evidence,
        source_evidence_sha256=source_hash,execution_authority=False,model_http=0)
    text=canonical(body);digest=sha(body)
    def operation(db):
        if db.execute("SELECT 1 FROM execution_plans WHERE lane_id=? AND substr(expires_at,1,10)=? AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY') LIMIT 1",(lane_id,target_trade_date)).fetchone():
            raise ValueError('FORMAL_EMPTY_EXISTING_PLANS_PRESERVED')
        if db.execute('SELECT 1 FROM active_reviewed_batch_scope WHERE account_id=? AND trade_date=?',(ACCOUNT,target_trade_date)).fetchone():
            raise ValueError('FORMAL_EMPTY_ACTIVE_SCOPE_PRESERVED')
        head=db.execute('SELECT scope_id,body_sha256,revision FROM natural_review_candidate_head WHERE account_id=? AND trade_date=? AND lane_id=?',(ACCOUNT,target_trade_date,lane_id)).fetchone()
        if head and (head['scope_id']!=scope or head['body_sha256']!=digest):
            raise ValueError('FORMAL_EMPTY_EXISTING_CANDIDATE_PRESERVED')
        prior=db.execute('SELECT body_sha256,body_json,state FROM natural_review_candidates WHERE scope_id=?',(scope,)).fetchone()
        if prior and (prior['body_sha256']!=digest or prior['body_json']!=text or prior['state']!='EMPTY'):
            raise ValueError('FORMAL_EMPTY_IMMUTABLE_CONFLICT')
        if not prior:db.execute('INSERT INTO natural_review_candidates VALUES (?,?,?,?,?,?,?,?,?)',
            (scope,ACCOUNT,target_trade_date,lane_id,run_id,digest,text,observed_at.isoformat(),'EMPTY'))
        if not head:db.execute('INSERT INTO natural_review_candidate_head VALUES (?,?,?,?,?,?)',
            (ACCOUNT,target_trade_date,lane_id,scope,digest,1))
        return dict(status='FORMAL_EMPTY',reason_code='NATURAL_ZERO_PLAN_SOURCE',scope_id=scope,
            body_sha256=digest,execution_plans_written=0,plan_ids=[],target_trade_date=target_trade_date)
    return store._write(operation)


def sha(value): return hashlib.sha256(canonical(value).encode()).hexdigest()


def stage_natural_candidate(store, batch, *, run_id, observed_at, regeneration=None, artifact_proof=None):
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
    from .candidate_artifacts import verify_candidate_artifacts
    artifact_check=verify_candidate_artifacts(batch,run_id=run_id,proof=artifact_proof)
    scope='bd_'+sha([run_id,day,lane])[:28]
    def operation(db):
        record=db.execute('SELECT trade_date,slot FROM workflow_runs WHERE run_id=? AND lane_id=?',(run_id,lane)).fetchone()
        if not record or research_slot(record['slot'])!='close' or record['trade_date']!=artifact_check['source_trade_date']:
            raise ValueError('CANDIDATE_WORKFLOW_SOURCE_CONFLICT')
        if source_scope is not None:
            from .close_regeneration import require_regeneration_candidate_scope
            require_regeneration_candidate_scope(db,source_scope)
        old=[dict(r) for r in db.execute("SELECT * FROM execution_plans WHERE lane_id=? AND substr(expires_at,1,10)=? AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY') ORDER BY plan_id",(lane,day))]
        pointer=db.execute('SELECT revision FROM active_reviewed_batch_scope WHERE account_id=? AND trade_date=?',(ACCOUNT,day)).fetchone()
        body=dict(schema_version='natural-review-candidate/1',source_namespace='NATURAL_MORNING_CANDIDATE',
            scope_id=scope,account_id=ACCOUNT,trade_date=day,lane_id=lane,run_id=run_id,plan_rows=rows,
            expected_old_plan_ids=[r['plan_id'] for r in old],expected_old_rows_sha256=rowset_sha256(old),
            expected_pointer_revision=pointer['revision'] if pointer else None,artifact_contract=artifact_proof)
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
                or body.get('lane_id')!=settings.research_primary_lane_id or row['state'] not in {'PENDING','SWITCHED','EMPTY'}):
            raise ValueError('NATURAL_CANDIDATE_SCOPE_CONFLICT')
        if row['state']=='EMPTY' and (body.get('schema_version')!='natural-review-empty/1'
                or body.get('source_namespace')!='NATURAL_ZERO_PLAN_SOURCE' or body.get('plan_rows')!=[]
                or body.get('execution_authority') is not False or body.get('model_http')!=0):
            raise ValueError('NATURAL_EMPTY_SCOPE_CONFLICT')
        if row['state']!='EMPTY':
            from .candidate_artifacts import verify_candidate_artifacts,discover_existing_candidate
            plans=body['plan_rows'];proof=body.get('artifact_contract')
            if proof is not None and Path(proof['workflow_output_dir']).resolve()!=Path(settings.workflow_output_dir).resolve():
                raise ValueError('CANDIDATE_PUBLICATION_ROOT_CHANGED')
            if proof is None:
                proof=discover_existing_candidate(settings,plans,run_id=body['run_id'],
                    observed_at=datetime.now(ZoneInfo('Asia/Shanghai')))
            checked=verify_candidate_artifacts(plans,run_id=body['run_id'],proof=proof,verify_original_files=False)
            record=db.execute('SELECT trade_date,slot FROM workflow_runs WHERE run_id=? AND lane_id=?',
                (body['run_id'],body['lane_id'])).fetchone()
            if not record or record['trade_date']!=checked['source_trade_date'] or research_slot(record['slot'])!='close':
                raise ValueError('CANDIDATE_WORKFLOW_SOURCE_CONFLICT')
        return dict(body=body,scope_id=row['scope_id'],body_sha256=row['body_sha256'],revision=row['revision'],state=row['state'])


def candidate_planning_source(settings, candidate, *, now, calendar):
    from .team_v2_sources import project_planning_rows
    result=project_planning_rows(settings,deepcopy(candidate['body']['plan_rows']),now=now,calendar=calendar)
    result.update(source_namespace='NATURAL_MORNING_CANDIDATE',candidate_scope_id=candidate['scope_id'],
        candidate_sha256=candidate['body_sha256'],candidate_revision=candidate['revision'])
    return result


def finish_natural_candidate(settings, candidate, *, calendar,observed_at=None):
    if candidate.get('state')=='EMPTY':raise ValueError('NATURAL_EMPTY_CANNOT_SWITCH')
    from .reviewed_batch_evidence import build_reviewed_batch_evidence
    from .state import RuntimeStore, StateTransitionError
    body=candidate['body'];scope=candidate['scope_id']
    rows=[dict(plan_id=r['plan_id'],lane_id=r['lane_id'],symbol=r['symbol'],status=r['status'],
        plan_version=r['plan_version'],valid_from=None,expires_at=r['expires_at'],payload=json.loads(r['payload_json']))
        for r in body['plan_rows']]
    try:
        bundle=build_reviewed_batch_evidence(review_root=Path(settings.root).resolve(),source_namespace='NATURAL_MORNING',
            workflow_db_path=settings.state_db_path,workflow_output_dir=settings.workflow_output_dir,
            minute_cache_dir=settings.minute_cache_dir,scope_id=scope,account_id=ACCOUNT,trade_date=body['trade_date'],
            lane_id=body['lane_id'],run_id=body['run_id'],a6_meeting_id=scope+':risk',pm_meeting_id=scope+':pm',candidate_rows=rows)
    except StateTransitionError as exc:
        # This exact rejection occurs only after the evidence builder has
        # revalidated both original replies and all immutable scope bindings.
        # Legitimate abstention must not fail the scheduler or replace the old
        # domain. The original PM journal remains the durable zero-allocation
        # record; never relabel the nonempty source candidate as source EMPTY.
        if str(exc)!='REVIEWED_BATCH_NO_ALLOCATABLE_PRIMARY':raise
        return dict(status='NOOP',reason_code='NATURAL_CANDIDATE_REVIEW_ZERO_ALLOCATION',
            scope_id=scope,candidate_sha256=candidate['body_sha256'],
            old_batch_preserved=True,execution_plans_written=0,
            original_a6_call=scope+':A6',original_pm_call=scope+':PM')
    store=RuntimeStore(settings.state_db_path)
    store.stage_reviewed_batch(bundle)
    result=store.commit_reviewed_batch_switch(scope,expected_old_plan_ids=body['expected_old_plan_ids'],
        expected_old_rows_sha256=body['expected_old_rows_sha256'],expected_pointer_revision=body['expected_pointer_revision'])
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from .lark_notifications import WorkflowLarkPublisher
    publisher=WorkflowLarkPublisher(store,None,webhook_path=settings.lark_webhook_path,
        timeout_seconds=settings.lark_timeout_seconds,policy_root=settings.root)
    try:
        notification=publisher.publish_batch_switch_status(trade_date=body['trade_date'],scope_id=scope,
            plan_count=len(body['plan_rows']),body_sha256=candidate['body_sha256'],
            capacity_review=bundle['evidence']['a6'].get('effective_review'),
            observed_at=observed_at or datetime.now(ZoneInfo('Asia/Shanghai')))
    except Exception as exc:
        notification=dict(status='FAILED',reason_code='MORNING_FINAL_NOTIFICATION_FAILED',
            failure_phase='POST_COMMIT_NOTIFICATION',exception_class=type(exc).__name__)
    return dict(result,final_notification=notification)
