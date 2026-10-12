"""Shadow-only SQLite readers. Never create production schema or WAL files."""
from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
import time


@contextmanager
def bounded_live_transaction(path, *, database, clock=time.monotonic, sleep=time.sleep):
    """One-shot source use with the same 0.5s cadence and 120s loss bound."""
    db = None
    deadline = clock()+120
    while db is None:
        try:
            db = open_live_readonly(path)
        except (sqlite3.Error, OSError) as exc:
            if clock() >= deadline:
                raise SourceUnavailable('SOURCE_'+database.upper()+'_OPEN_FAILED', database, 'open', exc) from exc
            sleep(min(.5, max(0, deadline-clock())))
    try:
        db.execute('BEGIN')
        yield db
    except sqlite3.Error as exc:
        raise SourceUnavailable('SOURCE_'+database.upper()+'_OPEN_FAILED', database, 'read', exc) from exc
    finally:
        try: db.rollback()
        finally: db.close()


class SourceUnavailable(ValueError):
    def __init__(self, code, database, stage, original=None):
        super().__init__(code)
        message = str(original) if original is not None else code
        # Keep the exception class and useful SQLite message, not credentials/paths.
        message = re.sub(r'https?://\S+|(?:[A-Za-z]:[\\/]|/)[^\s\'\"]+', '[REDACTED]', message)
        message = re.sub(r'(?i)(token|secret|password|webhook|api[_-]?key)\s*[:=]\s*\S+', r'\1=[REDACTED]', message)
        self.diagnostic = {'code': code, 'database': database, 'stage': stage,
                           'exception_class': type(original).__name__ if original is not None else type(self).__name__,
                           'message': message[:240]}


def open_live_readonly(path):
    path = Path(path).resolve()
    if not path.is_file(): raise OSError('source file not present')
    db = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=.2)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        # Force real WAL/schema access: connect() alone is not readiness.
        db.execute('SELECT name FROM sqlite_master LIMIT 1').fetchone()
        db.rollback()
        return db
    except Exception:
        db.close()
        raise


def _fact_identity(path):
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns,
            Path(str(path)+'-wal').exists(), Path(str(path)+'-shm').exists())


@contextmanager
def fact_readonly_transaction(path):
    """Cold immutable only for closed daily facts; reject a writer race."""
    path = Path(path).resolve()
    db = None
    before = None
    cold = False
    stage = 'open'
    try:
        before = _fact_identity(path)
        cold = not before[-2] and not before[-1]
        uri = path.as_uri()+'?mode=ro'+('&immutable=1' if cold else '')
        db = sqlite3.connect(uri, uri=True, timeout=.2)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        db.execute('SELECT name FROM sqlite_master LIMIT 1').fetchone()
        stage = 'read'
        yield db
    except SourceUnavailable:
        raise
    except (sqlite3.Error, OSError) as exc:
        raise SourceUnavailable('FACT_DB_OPEN_FAILED' if stage == 'open' else 'FACT_DB_READ_FAILED',
                                'fact', stage, exc) from exc
    finally:
        try:
            if db is not None:
                try: db.rollback()
                finally: db.close()
        finally:
            if cold and before is not None:
                try: unchanged = _fact_identity(path) == before
                except OSError: unchanged = False
                if not unchanged:
                    raise SourceUnavailable('FACT_DB_CHANGED_DURING_READ', 'fact', 'identity_check')
