"""Read research intent separately from execution, without granting authority."""
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import time


def _read(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=.4)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    db.execute('PRAGMA mmap_size=0')
    db.execute('PRAGMA cache_size=-512')
    deadline = time.monotonic() + 2
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    return db


def research_execution_summary(workflow_path, *, day, lane, cutoff):
    """Bind the current head, original bytes, publication rows and A6 capture.

    Missing originals remain unavailable; execution zero never establishes a
    failed model call. No projection here can publish or normalize an A6 reply.
    """
    unavailable = dict(status='NOT_AVAILABLE', reason_code='RESEARCH_EXECUTION_ORIGINAL_UNAVAILABLE')
    try:
        if cutoff.utcoffset() is None or cutoff.date().isoformat() != day:
            return unavailable
        path = Path(workflow_path)
        with closing(_read(path)) as db:
            row = db.execute('SELECT c.* FROM natural_review_candidate_head h '
                'JOIN natural_review_candidates c ON c.scope_id=h.scope_id '
                'AND c.body_sha256=h.body_sha256 '
                'WHERE h.account_id=? AND h.trade_date=? AND h.lane_id=? '
                'AND c.account_id=h.account_id AND c.trade_date=h.trade_date AND c.lane_id=h.lane_id '
                'AND length(c.body_json)<=524288', ('local-paper', day, lane)).fetchone()
            if row is None or datetime.fromisoformat(row['created_at']) > cutoff:
                return unavailable
            text = row['body_json']; body = json.loads(text)
            if hashlib.sha256(text.encode()).hexdigest() != row['body_sha256']:
                return unavailable
            plans = body['plan_rows']
            if (body['scope_id'] != row['scope_id'] or body['run_id'] != row['run_id']
                    or body['trade_date'] != day or body['lane_id'] != lane
                    or len(plans) > 100):
                return unavailable
            ids = [p['plan_id'] for p in plans]
            if len(ids) != len(set(ids)) or any(p['lane_id'] != lane for p in plans):
                return unavailable
            execution = []
            for plan_id in ids:
                item = db.execute('SELECT plan_id,created_at,valid_from FROM execution_plans '
                    'WHERE plan_id=? AND lane_id=?', (plan_id, lane)).fetchone()
                if item is not None:
                    if datetime.fromisoformat(item['created_at']) > cutoff:
                        return unavailable  # Cannot reconstruct a later write at the cutoff.
                    execution.append(dict(item))
        a6 = dict(status='UNKNOWN', reason_code='A6_ORIGINAL_NOT_OBSERVED')
        model_path = path.parent / 'team_v2/model.db'
        if model_path.is_file():
            with closing(_read(model_path)) as db:
                calls = db.execute('SELECT call_id,output_json,output_hash,validated,issues_json,captured_at '
                    'FROM model_calls WHERE substr(call_id,1,?)=? AND length(output_json)<=524288 '
                    'ORDER BY captured_at DESC LIMIT 20',
                    (len(row['scope_id'] + ':A6'), row['scope_id'] + ':A6')).fetchall()
            for call in calls:
                if call['call_id'] != row['scope_id'] + ':A6' and not call['call_id'].startswith(row['scope_id'] + ':A6:'):
                    continue
                if datetime.fromisoformat(call['captured_at']) > cutoff:
                    continue
                if hashlib.sha256(call['output_json'].encode()).hexdigest() != call['output_hash']:
                    return unavailable
                issues = json.loads(call['issues_json'])
                if call['validated'] not in (0, 1) or not isinstance(issues, list):
                    return unavailable
                a6 = dict(status='VALIDATED' if call['validated'] else 'VALIDATION_FAILED',
                    call_id=call['call_id'], output_sha256=call['output_hash'],
                    captured_at=call['captured_at'], issues=issues)
                break
        return dict(status='AVAILABLE', scope_id=row['scope_id'], run_id=row['run_id'],
            body_sha256=row['body_sha256'], created_at=row['created_at'], candidate_state=row['state'],
            research_candidate_count=len(ids), published_execution_count=len(execution),
            research_plan_ids=ids, published_execution_ids=[r['plan_id'] for r in execution],
            a6=a6, authority='READ_ONLY_RESEARCH_VS_PUBLISHED_EXECUTION_NOT_ELIGIBILITY',
            cutoff_at=cutoff.isoformat())
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError):
        return unavailable
