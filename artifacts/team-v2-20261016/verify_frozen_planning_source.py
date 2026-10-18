"""Local-only reproduction using the five original 10-16 production rows."""
from datetime import date,datetime
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace

root=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(root/'src'))
from liangjian_funnel.runtime.team_v2_sources import read_planning_scope
from liangjian_funnel.runtime.team_v2_store import canonical

source=root/'artifacts/observations-20261016/20261016T170047035260-close-review-stdout.json'
original=source.read_bytes();rows=json.loads(original)['plans']
columns=tuple(rows[0]);assert all(tuple(row)==columns for row in rows)
assert all(name.replace('_','').isalnum() for name in columns)
with TemporaryDirectory(prefix='team-frozen-plans-') as tmp:
    database=Path(tmp)/'source.sqlite3'
    with closing(sqlite3.connect(database)) as db,db:
        db.execute('CREATE TABLE execution_plans ('+','.join('"'+name+'" '+('INTEGER' if type(rows[0][name]) is int else 'TEXT') for name in columns)+')')
        db.executemany('INSERT INTO execution_plans VALUES ('+','.join('?' for _ in columns)+')',
            [tuple(row[key] for key in columns) for row in rows])
    before=database.read_bytes()
    result=read_planning_scope(SimpleNamespace(state_db_path=database,research_primary_lane_id=rows[0]['lane_id']),
        now=datetime.fromisoformat('2026-10-19T08:33:00+08:00'),
        calendar=SimpleNamespace(previous_trading_day=lambda _:date(2026,10,16)))
    assert result['status']=='COMPLETE',result['reason_codes']
    assert len(result['plans'])==len(rows)==5
    expected=sorted(rows,key=lambda row:(row['symbol'],row['plan_id']))
    assert result['plan_rowset_sha256']==hashlib.sha256(canonical(expected).encode()).hexdigest()
    assert database.read_bytes()==before and source.read_bytes()==original
    print(json.dumps(dict(status='PASS_LOCAL_FROZEN_PRODUCTION_ROWS',count=5,
        source_sha256=hashlib.sha256(original).hexdigest(),plan_rowset_sha256=result['plan_rowset_sha256'],
        clusters=sorted({p['cluster'] for p in result['plans']}),market_regime=result['market_regime'],
        original_rows_unchanged=True,production_writes=0),ensure_ascii=False))
