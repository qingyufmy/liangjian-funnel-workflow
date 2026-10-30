"""Immutable candidate reviews and one-DB atomic publication/scope switch.

No paper/team/model database is opened here. A prepared bundle is inert until
the workflow DB transaction publishes its plans and changes the one pointer.
"""
from collections.abc import Mapping
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

SCHEMA = 'reviewed-batch/1'
_SHA = re.compile(r'[0-9a-f]{64}')
_SCOPE = re.compile(r'[A-Za-z0-9_-]{1,40}')


def _now():
    return datetime.now(ZoneInfo('Asia/Shanghai'))


def canonical(value):
    def encode(item):
        if isinstance(item, Decimal): return str(item)
        if isinstance(item, datetime):
            if item.utcoffset() is None: raise ValueError('REVIEWED_BATCH_NAIVE_TIME')
            return item.isoformat()
        raise ValueError('REVIEWED_BATCH_JSON_INVALID')
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False, default=encode)


def rowset_sha256(rows):
    return hashlib.sha256(canonical(sorted((dict(row) for row in rows), key=lambda row: row['plan_id'])).encode()).hexdigest()


def _fail(code):
    from .state import StateTransitionError
    raise StateTransitionError(code)


def _time(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if result.utcoffset() is None: raise ValueError()
        return result.astimezone(ZoneInfo('Asia/Shanghai')).isoformat()
    except (ValueError, TypeError): _fail('REVIEWED_BATCH_DATE_INVALID')


def validate_bundle(body):
    """Validate immutable, already-reviewed facts; never manufacture a review."""
    body = json.loads(canonical(body))
    if (body.get('schema_version') != SCHEMA or body.get('status') != 'READY'
            or body.get('switch_mode') != 'PRE_AUCTION'
            or any(not isinstance(body.get(key), str) or not body[key].strip()
                for key in ('scope_id', 'account_id', 'trade_date', 'lane_id', 'run_id'))):
        _fail('REVIEWED_BATCH_IDENTITY_INVALID')
    if _SCOPE.fullmatch(body['scope_id']) is None or len(body['run_id']) > 64:
        _fail('REVIEWED_BATCH_IDENTITY_INVALID')
    try:
        if date.fromisoformat(body['trade_date']).isoformat() != body['trade_date']: raise ValueError()
    except ValueError: _fail('REVIEWED_BATCH_DATE_INVALID')
    engine, risk, pm, evidence = (body.get(key) for key in ('engine', 'risk', 'pm', 'evidence'))
    if not all(isinstance(value, Mapping) for value in (engine, risk, pm, evidence)):
        _fail('REVIEWED_BATCH_REVIEWS_MISSING')
    if (engine.get('status') != 'COMPLETE' or engine.get('trade_date') != body['trade_date']
            or engine.get('account_id') != body['account_id']
            or risk.get('status') != 'READY' or risk.get('consecutive_failures') != 0
            or pm.get('status') != 'READY' or pm.get('model_validated') is not True
            or pm.get('decision_authority') != 'VALIDATED_MODEL'
            or pm.get('session') != 'PM_PREMARKET' or pm.get('trade_date') != body['trade_date']
            or any(key.startswith('fallback_') for key in pm)):
        _fail('REVIEWED_BATCH_NOT_STRICTLY_READY')
    if not {'observed_at', 'envelopes', 'capacity_slots', 'cluster_caps', 'baseline_range',
            'engine_utilization_cap', 'risk_limits_sha256', 'snapshot', 'held_plan_bindings'} <= set(engine):
        _fail('REVIEWED_BATCH_ENGINE_INCOMPLETE')
    for role in ('a6', 'pm'):
        item = evidence.get(role)
        if (not isinstance(item, Mapping) or item.get('validated') is not True
                or any(not isinstance(item.get(key), str) or not item[key].strip()
                    for key in ('call_id', 'meeting_id', 'journal_ref'))
                or any(not isinstance(item.get(key), str) or _SHA.fullmatch(item[key]) is None
                    for key in ('raw_sha256', 'input_sha256'))):
            _fail('REVIEWED_BATCH_EVIDENCE_UNPROVEN')
    if (pm.get('source_call_id') != evidence['pm']['call_id']
            or pm.get('server_projection', {}).get('raw_output_sha256') != evidence['pm']['raw_sha256']):
        _fail('REVIEWED_BATCH_PM_BINDING_CONFLICT')
    candidates = body.get('candidate_rows')
    if not isinstance(candidates, list) or not candidates:
        _fail('REVIEWED_BATCH_EMPTY_CANDIDATES')
    ids = set()
    for row in candidates:
        if (not isinstance(row, Mapping) or any(not isinstance(row.get(key), str) or not row[key].strip()
                for key in ('plan_id', 'lane_id', 'symbol', 'status'))
                or row['plan_id'] in ids or row['lane_id'] != body['lane_id']
                or row['status'] != 'PENDING_MORNING_REVIEW' or not isinstance(row.get('payload'), Mapping)
                or row['payload'].get('source_run_id') != body['run_id']
                or row['payload'].get('symbol') != row['symbol']):
            _fail('REVIEWED_BATCH_PLAN_INVALID')
        row['expires_at'] = _time(row.get('expires_at'))
        if row['expires_at'][:10] != body['trade_date'] or row.get('valid_from') is not None:
            _fail('REVIEWED_BATCH_PLAN_DATE_INVALID')
        if row.get('parent_plan_id') is not None:
            _fail('REVIEWED_BATCH_PARENT_MUTATION_FORBIDDEN')
        row['plan_version'] = row.get('plan_version', 1)
        if type(row['plan_version']) is not int or row['plan_version'] < 1:
            _fail('REVIEWED_BATCH_PLAN_INVALID')
        ids.add(row['plan_id'])
    source = engine.get('source')
    if (not isinstance(source, Mapping) or source.get('status') != 'COMPLETE'
            or source.get('trade_date') != body['trade_date'] or source.get('lane_id') != body['lane_id']
            or not isinstance(source.get('plan_rows'), list)):
        _fail('REVIEWED_BATCH_ENGINE_SCOPE_INVALID')
    if body.get('morning_window_authorization')is not None or engine.get('morning_window_authorization')is not None:
        from .morning_window_authority import morning_deadline
        if body.get('morning_window_authorization')!=engine.get('morning_window_authorization'):_fail('REVIEWED_BATCH_MORNING_AUTHORITY_CONFLICT')
        morning_deadline(body['trade_date'],source.get('source_trade_date'),authorization=engine['morning_window_authorization'],source=source)
    source_rows = source['plan_rows']
    if hashlib.sha256(canonical(source_rows).encode()).hexdigest() != source.get('plan_rowset_sha256'):
        _fail('REVIEWED_BATCH_ENGINE_ROWSET_CONFLICT')
    observed = _time(engine.get('observed_at'))
    source_observed = _time(source.get('observed_at'))
    if observed[:10] != source_observed[:10] or observed[:10] > body['trade_date']:
        _fail('REVIEWED_BATCH_OBSERVATION_DATE_INVALID')
    if datetime.fromisoformat(observed) > _now() or datetime.fromisoformat(source_observed) > _now():
        _fail('REVIEWED_BATCH_OBSERVATION_IN_FUTURE')
    by_id = {row.get('plan_id'): row for row in source_rows if isinstance(row, Mapping)}
    if set(by_id) != ids or len(by_id) != len(source_rows):
        _fail('REVIEWED_BATCH_ENGINE_SCOPE_INVALID')
    for row in candidates:
        frozen = by_id[row['plan_id']]
        if (frozen.get('symbol') != row['symbol'] or frozen.get('expires_at') != row['expires_at']
                or frozen.get('payload_json') != canonical(row['payload'])):
            _fail('REVIEWED_BATCH_ENGINE_PLAN_CONFLICT')
    envelopes = risk.get('envelopes')
    if (not isinstance(envelopes, list) or {e.get('plan_id') for e in envelopes if isinstance(e, Mapping)} != ids
            or len(envelopes) != len(ids)
            or any(e.get('trade_date') != body['trade_date'] for e in envelopes)):
        _fail('REVIEWED_BATCH_RISK_SCOPE_INVALID')
    bindings = pm.get('allocation_plan_bindings')
    if not isinstance(bindings, Mapping) or set(bindings) != ids:
        _fail('REVIEWED_BATCH_PM_SCOPE_INVALID')
    a6 = evidence['a6']
    meeting = a6.get('meeting_record')
    risk_sha = hashlib.sha256(canonical(risk).encode()).hexdigest()
    engine_sha = hashlib.sha256(canonical(engine.get('envelopes')).encode()).hexdigest()
    if (not isinstance(meeting, Mapping)
            or any(meeting.get(key) != a6.get(evidence_key) for key, evidence_key in
                (('meeting_id', 'meeting_id'), ('source_call_id', 'call_id'), ('input_sha256', 'input_sha256')))
            or meeting.get('result_sha256') != risk_sha or a6.get('risk_result_sha256') != risk_sha
            or not isinstance(engine.get('envelopes'), list) or a6.get('engine_envelopes_sha256') != engine_sha
            or a6.get('plan_rowset_sha256') != source['plan_rowset_sha256']):
        _fail('REVIEWED_BATCH_A6_RESULT_BINDING_CONFLICT')
    try:
        # Pure consumers only: no TeamMeetingStore construction/DB is opened.
        from .team_v2_store import TeamMeetingStore
        from .team_v2_allocation import compile_pm_allocation
        decoded = TeamMeetingStore._decode(canonical(risk))
        allocation = compile_pm_allocation(pm, account_id=body['account_id'], trade_date=body['trade_date'],
            meeting_id=evidence['pm']['meeting_id'], input_sha256=evidence['pm']['input_sha256'])
        if allocation is None or any(e.symbol != next(r['symbol'] for r in candidates if r['plan_id'] == e.plan_id)
                or e.purpose != 'PLANNING' for e in decoded.envelopes):
            raise ValueError()
    except (KeyError, TypeError, ValueError, ArithmeticError):
        _fail('REVIEWED_BATCH_RESULT_INCOMPLETE')
    # A valid empty PM result remains a legitimate saved research/review result,
    # but cannot replace the old opening domain. Only real PM primary slots with
    # an actual positive A6 planning envelope qualify, not backups or READY alone.
    primary_ids = {primary for _, primary, _ in allocation.slots}
    allocated_ids = {plan_id for plan_id, _, _ in allocation.allocations}
    if not any(e.plan_id in primary_ids & allocated_ids and e.eligible is True
            and e.risk_unit in {'PROBE', 'STANDARD'} and e.risk_budget > 0
            and e.max_shares_cap > 0 and e.max_batches > 0 for e in decoded.envelopes):
        _fail('REVIEWED_BATCH_NO_ALLOCATABLE_PRIMARY')
    return body


def stage_reviewed_batch(store, bundle):
    body = validate_bundle(bundle)
    text = canonical(body)
    sha = hashlib.sha256(text.encode()).hexdigest()
    now = _now().isoformat()
    def operation(db):
        previous = db.execute('SELECT bundle_sha256,body_json FROM reviewed_batch_bundles WHERE scope_id=?', (body['scope_id'],)).fetchone()
        if previous:
            if previous['bundle_sha256'] != sha or previous['body_json'] != text:
                _fail('REVIEWED_BATCH_IMMUTABLE_CONFLICT')
        else:
            db.execute('INSERT INTO reviewed_batch_bundles VALUES (?,?,?,?,?,?,?)',
                (body['scope_id'], body['account_id'], body['trade_date'], body['lane_id'], sha, text, now))
        return dict(scope_id=body['scope_id'], bundle_sha256=sha)
    return store._write(operation)


def commit_reviewed_batch_switch(store, scope_id, *, expected_old_rows_sha256,
        expected_old_plan_ids, expected_pointer_revision):
    """CAS one original batch before auction; never activate candidate plans.

    Review preparation may precede the target date. Publication must use the
    actual target-day clock before 09:26, not an injected business timestamp.
    """
    expected_ids = tuple(expected_old_plan_ids)
    if (not isinstance(scope_id, str) or _SCOPE.fullmatch(scope_id) is None
            or len(set(expected_ids)) != len(expected_ids)
            or not isinstance(expected_old_rows_sha256, str) or _SHA.fullmatch(expected_old_rows_sha256) is None
            or (expected_pointer_revision is not None and (type(expected_pointer_revision) is not int or expected_pointer_revision < 1))):
        _fail('REVIEWED_BATCH_CAS_INVALID')
    wall = _now().astimezone(ZoneInfo('Asia/Shanghai'))
    now = wall.isoformat()
    def operation(db):
        stored = db.execute('SELECT * FROM reviewed_batch_bundles WHERE scope_id=?', (scope_id,)).fetchone()
        if stored is None: _fail('REVIEWED_BATCH_MISSING')
        if hashlib.sha256(stored['body_json'].encode()).hexdigest() != stored['bundle_sha256']:
            _fail('REVIEWED_BATCH_HASH_CONFLICT')
        body = validate_bundle(json.loads(stored['body_json']))
        deadline=datetime.combine(wall.date(),time(9,26),wall.tzinfo)
        if body.get('morning_window_authorization')is not None:
            from .morning_window_authority import morning_deadline
            deadline=morning_deadline(body['trade_date'],body['engine']['source'].get('source_trade_date'),authorization=body['morning_window_authorization'],source=body['engine']['source'])
        if wall.date().isoformat() != body['trade_date'] or wall>=deadline:
            _fail('REVIEWED_BATCH_PRE_AUCTION_WINDOW_CLOSED')
        pointer = db.execute('SELECT * FROM active_reviewed_batch_scope WHERE account_id=? AND trade_date=?',
            (body['account_id'], body['trade_date'])).fetchone()
        if pointer and pointer['scope_id'] == scope_id:
            if pointer['bundle_sha256'] != stored['bundle_sha256']: _fail('REVIEWED_BATCH_POINTER_CONFLICT')
            for row in body['candidate_rows']:
                current = db.execute('SELECT lane_id,symbol,expires_at,payload_json FROM execution_plans WHERE plan_id=?', (row['plan_id'],)).fetchone()
                if current is None or tuple(current) != (row['lane_id'], row['symbol'], row['expires_at'], canonical(row['payload'])):
                    _fail('REVIEWED_BATCH_IDEMPOTENT_PLAN_CONFLICT')
            return dict(scope_id=scope_id, bundle_sha256=stored['bundle_sha256'], pointer_revision=pointer['revision'], idempotent=True)
        natural=body.get('review_source_namespace')=='NATURAL_MORNING'
        if natural:
            proof=body.get('natural_candidate') or {}
            selected=db.execute('SELECT h.scope_id,h.body_sha256,h.revision,c.state FROM natural_review_candidate_head h JOIN natural_review_candidates c ON c.scope_id=h.scope_id WHERE h.account_id=? AND h.trade_date=? AND h.lane_id=?',
                (body['account_id'],body['trade_date'],body['lane_id'])).fetchone()
            if (selected is None or selected['state']!='PENDING' or dict(selected) !=
                    dict(scope_id=scope_id,body_sha256=proof.get('body_sha256'),revision=proof.get('revision'),state='PENDING')):
                _fail('REVIEWED_BATCH_CANDIDATE_CAS_FAILED')
        elif not expected_ids:_fail('REVIEWED_BATCH_CAS_INVALID')
        if (pointer['revision'] if pointer else None) != expected_pointer_revision:
            _fail('REVIEWED_BATCH_POINTER_CAS_FAILED')
        placeholders = ','.join('?' for _ in expected_ids)
        old = [dict(row) for row in db.execute('SELECT * FROM execution_plans WHERE plan_id IN (' + placeholders + ') ORDER BY plan_id', expected_ids)]
        old_runs = {json.loads(row['payload_json']).get('source_run_id') for row in old}
        if ({row['plan_id'] for row in old} != set(expected_ids) or (expected_ids and (len(old_runs) != 1 or not next(iter(old_runs), None)))
                or any(row['lane_id'] != body['lane_id'] or row['expires_at'][:10] != body['trade_date']
                    or row['status'] not in {'PENDING_MORNING_REVIEW', 'ACTIVE_TODAY'} for row in old)
                or rowset_sha256(old) != expected_old_rows_sha256):
            _fail('REVIEWED_BATCH_OLD_SCOPE_CAS_FAILED')
        old_run = next(iter(old_runs),None)
        complete_old = {row['plan_id'] for row in db.execute(
            "SELECT plan_id FROM execution_plans WHERE lane_id=? AND substr(expires_at,1,10)=? "
            "AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY') AND (? IS NULL OR json_extract(payload_json,'$.source_run_id')=?)",
            (body['lane_id'], body['trade_date'], old_run,old_run))}
        unexpected_pending = [row for row in db.execute("SELECT plan_id FROM execution_plans WHERE lane_id=? AND substr(expires_at,1,10)=? AND status='PENDING_MORNING_REVIEW'",
            (body['lane_id'], body['trade_date'])) if row['plan_id'] not in expected_ids]
        if complete_old != set(expected_ids) or unexpected_pending:
            _fail('REVIEWED_BATCH_OLD_SCOPE_CAS_FAILED')
        rows = body['candidate_rows']
        for row in rows:
            if row['plan_id'] in expected_ids or db.execute('SELECT 1 FROM execution_plans WHERE plan_id=?', (row['plan_id'],)).fetchone():
                _fail('REVIEWED_BATCH_NEW_PLAN_ID_EXISTS')
        for row in rows:
            db.execute('INSERT INTO execution_plans(plan_id,lane_id,symbol,status,plan_version,valid_from,expires_at,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (row['plan_id'], row['lane_id'], row['symbol'], row['status'], row['plan_version'], None,
                 row['expires_at'], canonical(row['payload']), now, now))
        for plan_id in expected_ids:
            db.execute('INSERT INTO plan_supersessions VALUES (?,?,?,?)', (plan_id, body['run_id'], 'reviewed-batch-switch:' + scope_id, now))
            changed = db.execute("UPDATE execution_plans SET status=?,updated_at=? WHERE plan_id=? AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY')",
                ('SUPERSEDED', now, plan_id)).rowcount
            if changed != 1: _fail('REVIEWED_BATCH_OLD_SCOPE_CAS_FAILED')
        revision = (pointer['revision'] if pointer else 0) + 1
        db.execute('INSERT INTO active_reviewed_batch_scope VALUES (?,?,?,?,?,?) ON CONFLICT(account_id,trade_date) DO UPDATE SET scope_id=excluded.scope_id,bundle_sha256=excluded.bundle_sha256,revision=excluded.revision,updated_at=excluded.updated_at',
            (body['account_id'], body['trade_date'], scope_id, stored['bundle_sha256'], revision, now))
        db.execute('INSERT INTO reviewed_batch_switch_audit VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (scope_id, body['account_id'], body['trade_date'], pointer['scope_id'] if pointer else None,
             pointer['revision'] if pointer else None, revision, expected_old_rows_sha256,
             canonical(list(expected_ids)), canonical([row['plan_id'] for row in rows]), stored['bundle_sha256'], now))
        if natural:
            if db.execute("UPDATE natural_review_candidates SET state='SWITCHED' WHERE scope_id=? AND state='PENDING'",(scope_id,)).rowcount!=1:
                _fail('REVIEWED_BATCH_CANDIDATE_CAS_FAILED')
            db.execute("UPDATE workflow_runs SET status='PUBLISHED',updated_at=? WHERE run_id=? AND lane_id=? AND status='READY_TO_PUBLISH'",
                (now,body['run_id'],body['lane_id']))
        return dict(scope_id=scope_id, bundle_sha256=stored['bundle_sha256'], pointer_revision=revision, idempotent=False)
    return store._write(operation)


def load_active_reviewed_batch(path, *, account_id, trade_date):
    """One RO snapshot; no active pointer means the existing legacy morning."""
    path = Path(path)
    if not path.is_file() or path.is_symlink(): raise ValueError('REVIEWED_BATCH_DATABASE_UNAVAILABLE')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('active_reviewed_batch_scope','reviewed_batch_bundles')")}
        if not tables: return None
        if len(tables) != 2: raise ValueError('REVIEWED_BATCH_SCHEMA_INCOMPLETE')
        row = db.execute('SELECT p.scope_id,p.bundle_sha256,p.revision,b.body_json,b.bundle_sha256 AS stored_sha256 FROM active_reviewed_batch_scope p LEFT JOIN reviewed_batch_bundles b ON b.scope_id=p.scope_id WHERE p.account_id=? AND p.trade_date=?',
            (account_id, trade_date)).fetchone()
        if row is None: return None
        if (row['body_json'] is None or row['stored_sha256'] != row['bundle_sha256']
                or type(row['revision']) is not int or row['revision'] < 1
                or hashlib.sha256(row['body_json'].encode()).hexdigest() != row['bundle_sha256']):
            raise ValueError('REVIEWED_BATCH_POINTER_UNPROVEN')
        body = validate_bundle(json.loads(row['body_json']))
        if body['account_id'] != account_id or body['trade_date'] != trade_date or body['scope_id'] != row['scope_id']:
            raise ValueError('REVIEWED_BATCH_POINTER_UNPROVEN')
        return dict(scope_id=row['scope_id'], bundle_sha256=row['bundle_sha256'], bundle=body, pointer_revision=row['revision'])
