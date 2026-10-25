"""Once-per-weekday full verification, separate from models and minute startup."""
from contextlib import closing
from datetime import datetime,timedelta,time as daytime
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo

from .integrity import IntegrityDeadlineExceeded, marker_path, verify_integrity, verified_identity
from .shared_json import atomic_json


def run_integrity_maintenance(settings, *, now=None, timeout_seconds=540):
    if type(timeout_seconds) is not int or not 1<=timeout_seconds<=540:
        raise ValueError('INTEGRITY_MAINTENANCE_TIMEOUT_INVALID')
    deadline=time.monotonic()+timeout_seconds
    supplied=now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if supplied.utcoffset() is None:
        raise ValueError('INTEGRITY_MAINTENANCE_AWARE_CLOCK_REQUIRED')
    current=supplied.astimezone(ZoneInfo('Asia/Shanghai'))
    if current.weekday()>=5 or (current.hour,current.minute)<(6,50):
        return dict(status='NOT_DUE',reason_code='INTEGRITY_WEEKDAY_0650_OR_LATER_ONLY')
    database=Path(settings.state_db_path).resolve()
    if daytime(9)<=current.time().replace(tzinfo=None)<daytime(15):
        # A late Node start must not turn a minute command into a cold full
        # scan during the session. Check the same identity and the unchanged
        # 36-hour receipt, with enough remaining life through today's close.
        # This is reuse, not re-signing or an extension of the old receipt.
        try:
            raw=marker_path(database).read_bytes();receipt=json.loads(raw)
            expires=datetime.fromisoformat(receipt['checked_at'])+timedelta(hours=36)
            with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
                db.execute('PRAGMA query_only=ON')
                valid=verified_identity(database,db) and expires>=current.replace(hour=15,minute=0,second=0,microsecond=0)
        except (OSError,ValueError,KeyError,TypeError,sqlite3.Error):valid=False
        return dict(status='PASS' if valid else 'BLOCKED',schema_version='runtime-integrity-maintenance/1',
            trade_date=current.date().isoformat(),database_path=str(database),reused=True,
            reason_code='SESSION_EXISTING_RECEIPT_COVERS_CLOSE' if valid else 'SESSION_COLD_SCAN_FORBIDDEN_RECEIPT_UNPROVEN',
            models_called=0,migrations_run=False,full_scans=0)
    directory=Path(settings.root)/'state/runtime-integrity-maintenance'/current.date().isoformat()
    directory.parent.mkdir(parents=True,exist_ok=True)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        # A crash is not a PASS and never silently reruns a costly scan.
        terminal=directory/'result.json'
        try:
            result=json.loads(terminal.read_bytes())
            if (result.get('schema_version')!='runtime-integrity-maintenance/1'
                    or result.get('trade_date')!=current.date().isoformat()
                    or result.get('database_path')!=str(database)
                    or result.get('status') not in {'PASS','BLOCKED'}):
                raise ValueError('INTEGRITY_DAY_RECEIPT_MISMATCH')
            if result['status']!='PASS':
                return dict(result,reused=True)
            with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
                db.execute('PRAGMA query_only=ON')
                valid=verified_identity(database,db)
            return dict(result,status='PASS' if valid else 'BLOCKED',reused=True,
                reason_code='CURRENT_RECEIPT_VALID' if valid else 'CURRENT_RECEIPT_INVALID')
        except (OSError,ValueError,KeyError,TypeError,sqlite3.Error):
            return dict(status='BLOCKED',reason_code='INTEGRITY_ATTEMPT_INTERRUPTED_OR_UNREADABLE',reused=True)
    marker=marker_path(database)
    previous=None
    try:
        if marker.exists():
            raw=marker.read_bytes()
            with (directory/'previous-receipt.json').open('xb') as stream:
                stream.write(raw)
            previous=hashlib.sha256(raw).hexdigest()
        receipt=verify_integrity(database,deadline=deadline)
        result=dict(status='PASS',schema_version='runtime-integrity-maintenance/1',
            trade_date=current.date().isoformat(),database_path=str(database),previous_receipt_sha256=previous,
            receipt_sha256=hashlib.sha256(marker.read_bytes()).hexdigest(),
            checked_at=receipt['checked_at'],integrity=receipt,models_called=0,migrations_run=False)
    except (OSError,ValueError,sqlite3.Error) as exc:
        # Keep the old receipt above, but do not leave a failed scan covered
        # by an unexpired PASS marker, including long-lived cached writers.
        reason=('RUNTIME_INTEGRITY_DEADLINE_EXCEEDED' if isinstance(exc,IntegrityDeadlineExceeded)
            else 'SQLITE_INTEGRITY_CHECK_FAILED' if isinstance(exc,sqlite3.Error) else 'INTEGRITY_MAINTENANCE_FAILED')
        failed=dict(schema_version='runtime-integrity/2',status='FAIL',reason_code=reason,
            checked_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),previous_receipt_sha256=previous)
        atomic_json(marker,failed)
        result=dict(status='BLOCKED',schema_version='runtime-integrity-maintenance/1',
            trade_date=current.date().isoformat(),database_path=str(database),reason_code=reason,previous_receipt_sha256=previous,
            models_called=0,migrations_run=False)
    atomic_json(directory/'result.json',result)
    return result
