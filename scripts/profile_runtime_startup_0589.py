"""Offline page-scale RuntimeStore startup profile; NOT production monitor p95.

Creates one fresh owned child directory below --directory. Ballast is generated
SQLite data, never a production copy. --cleanup removes only this run's named
files after ownership/path checks; it never removes the supplied directory.
"""
from argparse import ArgumentParser
from contextlib import closing
import json
import gc
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid


CHILD = r'''
import json, os, socket, sys, time
def denied(*a, **k): raise RuntimeError('OFFLINE_PROFILE_NETWORK_FORBIDDEN')
socket.create_connection = denied
socket.getaddrinfo = denied
socket.socket.connect = denied
started = time.perf_counter()
from liangjian_funnel.runtime.state import RuntimeStore
imported = time.perf_counter()
store = RuntimeStore(sys.argv[1])
constructed = time.perf_counter()
healthy = store.healthy
checked = time.perf_counter()
if not healthy: raise RuntimeError('PROFILE_STORE_NOT_HEALTHY')
store.assert_writable()
finished = time.perf_counter()
print(json.dumps(dict(pid=os.getpid(), healthy=healthy,
 import_ms=(imported-started)*1000, constructor_ms=(constructed-imported)*1000,
 healthy_ms=(checked-constructed)*1000, assert_ms=(finished-checked)*1000,
 process_inner_ms=(finished-started)*1000)))
'''


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def fresh_sample(database, environment):
    started = time.perf_counter()
    completed = subprocess.run([sys.executable, '-B', '-c', CHILD, str(database)],
        env=environment, capture_output=True, text=True, timeout=60, check=True)
    result = json.loads(completed.stdout)
    result['wall_ms'] = (time.perf_counter() - started) * 1000
    return result


def clean_owned(directory, parent, owner):
    directory, parent = directory.resolve(), parent.resolve()
    if (directory.parent != parent or not directory.name.startswith('runtime-profile-0589-')
            or directory.is_symlink() or json.loads((directory / 'OWNER.json').read_text()) != owner):
        raise ValueError('PROFILE_CLEANUP_OWNERSHIP_CONFLICT')
    allowed = {'OWNER.json', 'runtime.sqlite3', 'runtime.sqlite3-wal', 'runtime.sqlite3-shm',
        'runtime.sqlite3-journal', 'runtime.sqlite3.integrity.json'}
    files = list(directory.iterdir())
    if any(p.name not in allowed or p.is_symlink() or not p.is_file() for p in files):
        raise ValueError('PROFILE_CLEANUP_UNEXPECTED_FILE')
    for path in sorted(files, key=lambda item: item.name == 'OWNER.json'): path.unlink()
    directory.rmdir()


def main(argv=None):
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True, help='existing isolated parent; never a production root')
    parser.add_argument('--bytes', type=int, default=1_490_000_000, help='minimum generated database bytes, not production data')
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--cleanup', action='store_true', help='explicitly remove only this run-owned child and known generated files')
    args = parser.parse_args(argv)
    parent = args.directory.resolve()
    if not parent.is_dir() or args.directory.is_symlink(): parser.error('--directory must be an existing real directory')
    production = Path('/media/qy/120aebe1-8c98-412b-a490-3797cae5656b/Agu/liangjian-funnel-workflow').resolve()
    if parent == production or parent.is_relative_to(production): parser.error('production paths are forbidden')
    if args.bytes < 0 or not 1 <= args.samples <= 100: parser.error('invalid byte count/sample count')
    if shutil.disk_usage(parent).free < args.bytes + 256 * 1024 * 1024: parser.error('insufficient isolated free space')
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment['PYTHONPATH'] = str(root / 'src')
    if str(root / 'src') not in sys.path: sys.path.insert(0, str(root / 'src'))
    from liangjian_funnel.runtime.state import RuntimeStore
    directory = Path(tempfile.mkdtemp(prefix='runtime-profile-0589-', dir=parent))
    owner = dict(schema_version='runtime-startup-profile-owner/1', run_id=uuid.uuid4().hex)
    (directory / 'OWNER.json').write_text(json.dumps(owner), encoding='utf-8')
    database = directory / 'runtime.sqlite3'
    result = dict(scope='GENERATED_PAGE_SCALE_STARTUP_ONLY', path=str(directory),
        production_copy=False, model_http_calls=0, requested_bytes=args.bytes,
        parent_monitor_p95='NOT_MEASURED', limitations=[
            'Ballast represents database page volume, not production B-tree shape or real monitor work.',
            'Fresh interpreters may use warm OS page cache; not a cold-storage claim.',
            'Read-only concurrency is tested, not prolonged exclusive-writer contention.'])
    try:
        RuntimeStore(database)
        gc.collect()  # Release sqlite context-manager connection cycles on Windows.
        started = time.perf_counter()
        with closing(sqlite3.connect(database)) as db:
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('CREATE TABLE profile_ballast(payload BLOB NOT NULL)')
            while database.stat().st_size < args.bytes:
                remaining = args.bytes - database.stat().st_size
                for _ in range(min(8, max(1, math.ceil(remaining / 1_048_576)))):
                    db.execute('INSERT INTO profile_ballast VALUES (zeroblob(?))', (min(1_048_576, max(1, remaining)),))
                db.commit()
            db.execute('PRAGMA journal_mode=WAL')
        result.update(actual_bytes=database.stat().st_size, generation_ms=(time.perf_counter()-started)*1000)
        started = time.perf_counter()
        # Explicit maintenance is allowed to scan pages/migrate, but is excluded
        # from ordinary minute-startup measurements and reported separately.
        maintained = RuntimeStore(database, integrity_maintenance=True)
        if not maintained.healthy: raise RuntimeError('PROFILE_MAINTENANCE_FAILED')
        gc.collect()
        result['maintenance_ms'] = (time.perf_counter()-started)*1000
        result['samples'] = [fresh_sample(database, environment) for _ in range(args.samples)]
        with closing(sqlite3.connect(database.as_uri()+'?mode=ro', uri=True)) as reader:
            reader.execute('PRAGMA query_only=ON'); reader.execute('BEGIN')
            reader.execute('SELECT name FROM sqlite_schema').fetchall()
            result['readonly_transaction_pressure_sample'] = fresh_sample(database, environment)
        walls = [sample['wall_ms'] for sample in result['samples']]
        result.update(sample_p50_wall_ms=percentile(walls, .5), sample_p95_wall_ms=percentile(walls, .95),
            max_wall_ms=max(walls), startup_target_under_10s=all(value < 10000 for value in walls),
            readonly_pressure_under_10s=result['readonly_transaction_pressure_sample']['wall_ms'] < 10000,
            status='PASS_STARTUP_ONLY' if all(value < 10000 for value in walls)
                and result['readonly_transaction_pressure_sample']['wall_ms'] < 10000 else 'FAIL_STARTUP_TARGET')
    finally:
        if args.cleanup:
            gc.collect()
            clean_owned(directory, parent, owner)
            result['generated_files_removed'] = True
        else: result['generated_files_removed'] = False
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get('status') == 'PASS_STARTUP_ONLY' else 1


if __name__ == '__main__':
    raise SystemExit(main())
