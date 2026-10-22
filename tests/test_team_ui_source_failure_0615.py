"""Actual missing DB/SQLite exclusive lock -> safe stderr code, no raw leak."""
from pathlib import Path
import importlib.util,json,sqlite3,subprocess,sys

SCRIPT=Path(__file__).resolve().parents[1]/'scripts/team_ui_readonly.py'
QUERY=dict(kind='fills',trade_date='2026-10-22',limit=50,offset=0,days=20)


def run(root,query=QUERY):
    return subprocess.run([sys.executable,'-B',str(SCRIPT),str(root),json.dumps(query)],capture_output=True,text=True,timeout=5)


def test_actual_missing_database_has_fixed_code_and_no_path(tmp_path):
    p=run(tmp_path)
    assert p.returncode==1 and json.loads(p.stdout)=={'error':'TEAM_UI_SOURCE_UNAVAILABLE'}
    assert p.stderr=='TEAM_UI_SOURCE_REASON:SOURCE_DB_MISSING\n'
    assert str(tmp_path) not in p.stderr and 'SELECT' not in p.stderr


def test_actual_exclusive_sqlite_lock_has_fixed_code_and_does_not_write(tmp_path):
    path=tmp_path/'state/team_v2/paper.db';path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE paper_fills(fill_id TEXT,order_id TEXT,symbol TEXT,plan_id TEXT,side TEXT,qty INTEGER,price TEXT,fee TEXT,minute_end TEXT)')
        db.commit();db.execute('BEGIN EXCLUSIVE')
        p=run(tmp_path)
        assert p.returncode==1 and json.loads(p.stdout)=={'error':'TEAM_UI_SOURCE_UNAVAILABLE'}
        assert p.stderr=='TEAM_UI_SOURCE_REASON:SQLITE_BUSY_OR_LOCKED\n'
        assert str(path) not in p.stderr and 'database is locked' not in p.stderr


def test_normal_success_has_empty_stderr_and_no_outputs(tmp_path):
    p=run(tmp_path,dict(QUERY,kind='settings'))
    assert p.returncode==0 and p.stderr=='' and not (tmp_path/'outputs').exists()


def test_generic_error_text_does_not_spoof_known_failure():
    s=importlib.util.spec_from_file_location('worker0615',SCRIPT);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
    assert m.source_failure_code(sqlite3.OperationalError('database is locked secret=not-real-proof'))=='UNKNOWN_SOURCE_FAILURE'
    assert m.source_failure_code(FileNotFoundError('/secret/missing-but-not-known-db'))=='UNKNOWN_SOURCE_FAILURE'
