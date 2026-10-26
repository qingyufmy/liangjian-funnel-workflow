"""Explicit, audited release of one account/day latch; no plan or risk writes.

The original latch is never deleted or rewritten. A release is effective only
for its exact latch hash. A subsequent anomaly appends a RELOCK, so an approval
cannot silently suppress another safety event later in the same session.
"""
from contextlib import closing
from datetime import datetime
import getpass
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from .team_v2_sources import stamp
from .team_v2_store import canonical, assert_database_scope


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def ensure_audit(db):
    db.execute('''CREATE TABLE IF NOT EXISTS control_record_authorizations (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id TEXT NOT NULL, trade_date TEXT NOT NULL,
        body_json TEXT NOT NULL, body_sha256 TEXT NOT NULL)''')


def read_release(db, *, account_id, day, latch_sha256):
    """Validate the complete bounded authorization chain before using a release."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_record_authorizations'").fetchone():
        return None
    rows = db.execute('SELECT body_json,body_sha256 FROM control_record_authorizations '
                      'WHERE account_id=? AND trade_date=? ORDER BY sequence LIMIT 129',
                      (account_id, day)).fetchall()
    if len(rows) > 128:
        raise ValueError('RECORD_AUTHORIZATION_CHAIN_LIMIT')
    previous = None
    last = None
    for raw, stored_hash in rows:
        body = json.loads(raw)
        if (hashlib.sha256(raw.encode()).hexdigest() != stored_hash
                or body.get('previous_audit_sha256') != previous
                or body.get('account_id') != account_id or body.get('trade_date') != day
                or body.get('latch_sha256') != latch_sha256
                or body.get('action') not in {'UNLOCK', 'RELOCK'}):
            raise ValueError('RECORD_AUTHORIZATION_CHAIN_INVALID')
        previous = stored_hash
        last = body
    return dict(body=last, audit_sha256=previous) if last is not None else None


def relock_if_released(db, *, account_id, day, latch_sha256, now, reason_code):
    release = read_release(db, account_id=account_id, day=day, latch_sha256=latch_sha256)
    if not release or release['body']['action'] != 'UNLOCK':
        return
    body = dict(schema='record-authorization/1', action='RELOCK', account_id=account_id,
                trade_date=day, latch_sha256=latch_sha256, observed_at=stamp(now).isoformat(),
                operator='SAFETY_LATCH', reason=reason_code,
                previous_audit_sha256=release['audit_sha256'],
                before_sha256=release['body']['after_sha256'], after_sha256=latch_sha256)
    ensure_audit(db)
    db.execute('INSERT INTO control_record_authorizations(account_id,trade_date,body_json,body_sha256) VALUES (?,?,?,?)',
               (account_id, day, canonical(body), digest(body)))


def unlock_record(settings, *, approval_id, reason, expected_latch_sha256,
                  expected_authorization_sha256=None,account_id='local-paper', now=None, operator=None):
    """CLI operation. Caller approval is explicit and recorded, never inferred."""
    current = stamp(now or datetime.now().astimezone())
    day = current.date().isoformat()
    if (account_id != 'local-paper' or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}', approval_id or '')
            or not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 1000
            or not re.fullmatch(r'[0-9a-f]{64}', expected_latch_sha256 or '')):
        raise ValueError('RECORD_UNLOCK_AUTHORIZATION_REQUIRED')
    root = Path(settings.root).resolve()
    directory = root / 'state/team_v2'
    path = directory / 'team.db'
    if (directory.is_symlink() or path.is_symlink() or not path.is_file()
            or directory.resolve() != directory or (root/'state').is_symlink()):
        raise ValueError('RECORD_UNLOCK_DATABASE_SCOPE_INVALID')
    assert_database_scope(path, marker='team_v2_meta', version='team-runtime/1')
    with closing(sqlite3.connect(path.as_uri()+'?mode=rw', uri=True, timeout=2)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT body_json,body_sha256 FROM control_record_latch WHERE account_id=? AND trade_date=?',
                         (account_id, day)).fetchone()
        if row is None:
            raise ValueError('RECORD_UNLOCK_NO_CURRENT_LATCH')
        raw, actual = row
        body = json.loads(raw)
        if (hashlib.sha256(raw.encode()).hexdigest() != actual or actual != expected_latch_sha256
                or body.get('account_id') != account_id or body.get('trade_date') != day or body.get('mode') != 'record'):
            raise ValueError('RECORD_UNLOCK_LATCH_CHANGED')
        prior = read_release(db, account_id=account_id, day=day, latch_sha256=actual)
        if prior and prior['body']['action'] == 'UNLOCK':
            if prior['body']['approval_id'] == approval_id and prior['body']['reason'] == reason.strip():
                return dict(status='ALREADY_RELEASED', audit_sha256=prior['audit_sha256'],
                            before_sha256=actual, after_sha256=prior['body']['after_sha256'])
            raise ValueError('RECORD_UNLOCK_ALREADY_RELEASED')
        if prior and any(json.loads(row[0]).get('approval_id')==approval_id for row in
                db.execute('SELECT body_json FROM control_record_authorizations WHERE account_id=? AND trade_date=?',
                           (account_id,day))):
            raise ValueError('RECORD_UNLOCK_APPROVAL_ALREADY_USED')
        if prior and expected_authorization_sha256!=prior['audit_sha256']:
            raise ValueError('RECORD_UNLOCK_NEW_CONFLICT_AUDIT_CHANGED')
        who = operator or getpass.getuser()
        if not isinstance(who, str) or not who.strip() or len(who) > 200:
            raise ValueError('RECORD_UNLOCK_OPERATOR_REQUIRED')
        audit = dict(schema='record-authorization/1', action='UNLOCK', account_id=account_id,
                     trade_date=day, latch_sha256=actual, approval_id=approval_id,
                     reason=reason.strip(), operator=who, observed_at=current.isoformat(),
                     previous_audit_sha256=prior['audit_sha256'] if prior else None,
                     before_sha256=actual,
                     after_sha256=digest(dict(account_id=account_id, trade_date=day,
                                             released_latch_sha256=actual, approval_id=approval_id)))
        ensure_audit(db)
        db.execute('INSERT INTO control_record_authorizations(account_id,trade_date,body_json,body_sha256) VALUES (?,?,?,?)',
                   (account_id, day, canonical(audit), digest(audit)))
    return dict(status='RELEASED', audit_sha256=digest(audit), before_sha256=actual,
                after_sha256=audit['after_sha256'], account_id=account_id, trade_date=day,
                original_latch_preserved=True, execution_plans_changed=False, risk_limits_changed=False)
