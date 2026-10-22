"""Full SQLite verification is maintenance, not a per-minute startup scan."""
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import time
from .shared_json import atomic_json

STATE_CONTRACT=hashlib.sha256(Path(__file__).with_name('state.py').read_bytes()).hexdigest()


def marker_path(path):return Path(str(path)+'.integrity.json')


def identity(path, connection):
    stat=Path(path).stat()
    return dict(device=stat.st_dev,inode=stat.st_ino,state_contract=STATE_CONTRACT,
                schema_version=connection.execute('PRAGMA schema_version').fetchone()[0])


def verify_integrity(path, *, timeout=180):
    path=Path(path).resolve();started=time.monotonic()
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1) as db:
        db.execute('PRAGMA query_only=ON')
        db.set_progress_handler(lambda:int(time.monotonic()-started>timeout),10000)
        results=[r[0] for r in db.execute('PRAGMA quick_check')]
        if results!=['ok']:raise ValueError('SQLITE_INTEGRITY_FAILED')
        receipt=dict(schema_version='runtime-integrity/1',identity=identity(path,db),
            checked_at=datetime.now(timezone.utc).isoformat(),elapsed_ms=round((time.monotonic()-started)*1000),status='PASS')
    atomic_json(marker_path(path),receipt)
    return receipt


def verified_identity(path, connection, *, max_age=36*3600):
    try:
        receipt=json.loads(marker_path(path).read_bytes())
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(receipt['checked_at'])).total_seconds()
        return (receipt.get('schema_version')=='runtime-integrity/1' and receipt['status']=='PASS' and 0<=age<=max_age
                and receipt['identity']==identity(path,connection))
    except (OSError,ValueError,KeyError,TypeError):return False
