"""Full SQLite verification is maintenance, not a per-minute startup scan."""
from datetime import datetime, timezone
import json
import hashlib
import math
from pathlib import Path
import sqlite3
import stat
import time
from .shared_json import atomic_json

STATE_CONTRACT=hashlib.sha256(Path(__file__).with_name('state.py').read_bytes()).hexdigest()


def marker_path(path):return Path(str(path)+'.integrity.json')


def identity(path, connection):
    """Legacy v1 identity; accepted only with its original strict device match."""
    stat=Path(path).stat()
    return dict(device=stat.st_dev,inode=stat.st_ino,state_contract=STATE_CONTRACT,
                schema_version=connection.execute('PRAGMA schema_version').fetchone()[0])


def _volume_identity(device):
    """Resolve a block filesystem without opening a device or running a command.

    Kernel device numbers are boot-local. UUIDs survive their renumbering. On
    filesystems without a discoverable UUID retain the old strict device gate;
    never silently accept a path/inode-only identity.
    """
    directory=Path('/dev/disk/by-uuid')
    matches=set()
    try:
        entries=list(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        entries=[]
    for entry in entries:
        try:
            observed=entry.stat()
        except OSError:
            continue
        if stat.S_ISBLK(observed.st_mode) and observed.st_rdev==device:
            matches.add(entry.name)
    if len(matches)>1:
        raise ValueError('RUNTIME_VOLUME_IDENTITY_AMBIGUOUS')
    if matches:
        return dict(kind='filesystem_uuid',uuid=next(iter(matches)))
    return dict(kind='strict_device',device=device)


def _stable_identity(path, connection):
    path=Path(path).resolve()
    # A receipt for one database cannot certify an unrelated open connection.
    database=next((row[2] for row in connection.execute('PRAGMA database_list') if row[1]=='main'), '')
    if not database or Path(database).resolve()!=path:
        raise ValueError('RUNTIME_DATABASE_PATH_MISMATCH')
    observed=path.stat()
    return dict(volume=_volume_identity(observed.st_dev),path=str(path),inode=observed.st_ino,
                state_contract=STATE_CONTRACT,
                schema_version=connection.execute('PRAGMA schema_version').fetchone()[0])


def _file_identity(path):
    observed=path.stat()
    return observed.st_dev,observed.st_ino


class IntegrityDeadlineExceeded(ValueError):
    def __init__(self):super().__init__('RUNTIME_INTEGRITY_DEADLINE_EXCEEDED')


def check_integrity_deadline(deadline):
    if (isinstance(deadline,bool) or not isinstance(deadline,(int,float))
            or not math.isfinite(deadline)):
        raise ValueError('RUNTIME_INTEGRITY_DEADLINE_INVALID')
    if time.monotonic()>=deadline:raise IntegrityDeadlineExceeded()


def verify_integrity(path, *, timeout=180, deadline=None):
    """Complete the same full check within one non-renewable caller budget.

    An explicit monotonic deadline replaces the legacy per-scan 180s budget,
    so deployment can share one budget across both scans and migrations.
    Progress callbacks never renew it or claim a scanned-page percentage.
    """
    if (isinstance(timeout,bool) or not isinstance(timeout,(int,float))
            or not math.isfinite(timeout) or timeout<=0):
        raise ValueError('RUNTIME_INTEGRITY_TIMEOUT_INVALID')
    path=Path(path).resolve();started=time.monotonic()
    deadline=started+timeout if deadline is None else deadline
    check_integrity_deadline(deadline)
    expired=False
    def progress():
        nonlocal expired
        expired=time.monotonic()>=deadline
        return int(expired)
    original_file=_file_identity(path)
    try:
        with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1) as db:
            db.execute('PRAGMA query_only=ON')
            db.set_progress_handler(progress,10000)
            db.execute('BEGIN')
            before=_stable_identity(path,db)
            check_integrity_deadline(deadline)
            if _file_identity(path)!=original_file:
                raise ValueError('RUNTIME_DATABASE_CHANGED_DURING_VERIFICATION')
            results=[r[0] for r in db.execute('PRAGMA quick_check')]
            check_integrity_deadline(deadline)
            if results!=['ok']:raise ValueError('SQLITE_INTEGRITY_FAILED')
            after=_stable_identity(path,db)
            check_integrity_deadline(deadline)
            if before!=after or _file_identity(path)!=original_file:
                raise ValueError('RUNTIME_DATABASE_CHANGED_DURING_VERIFICATION')
            receipt=dict(schema_version='runtime-integrity/2',identity=after,
                observed_device=original_file[0],
                checked_at=datetime.now(timezone.utc).isoformat(),elapsed_ms=round((time.monotonic()-started)*1000),status='PASS')
    except sqlite3.OperationalError:
        if expired:raise IntegrityDeadlineExceeded() from None
        raise
    if _file_identity(path)!=original_file:
        raise ValueError('RUNTIME_DATABASE_CHANGED_DURING_VERIFICATION')
    check_integrity_deadline(deadline)
    atomic_json(marker_path(path),receipt)
    return receipt


def verified_identity(path, connection, *, max_age=36*3600):
    try:
        receipt=json.loads(marker_path(path).read_bytes())
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(receipt['checked_at'])).total_seconds()
        if receipt['status']!='PASS' or not 0<=age<=max_age:
            return False
        version=receipt.get('schema_version')
        if version=='runtime-integrity/1':
            return receipt['identity']==identity(path,connection)
        if version=='runtime-integrity/2':
            return receipt['identity']==_stable_identity(path,connection)
        return False
    except (OSError,ValueError,KeyError,TypeError):return False
