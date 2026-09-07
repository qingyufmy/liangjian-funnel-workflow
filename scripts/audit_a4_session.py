"""Read-only VM capture and independent end-of-day data collection.

Artifacts only: never instantiates the production workflow or writes its DB.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json


REMOTE = r'''
import collections, hashlib, json, sqlite3, subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from liangjian_funnel.settings import Settings
from liangjian_funnel.data.mootdx import MootdxAdapter
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
root=Path.cwd(); settings=Settings.from_env(root=root)
day=DAY; prefix=PREFIX
db=sqlite3.connect(f'file:{settings.state_db_path}?mode=ro',uri=True);db.row_factory=sqlite3.Row
db.execute('BEGIN')
plans=[dict(x) for x in db.execute('select * from execution_plans where plan_id like ?', (prefix+'%',))]
if len(plans)!=40:raise RuntimeError('EXPECTED_FROZEN_40_PLANS')
events=[dict(x) for x in db.execute('select * from monitor_events where minute_end>=? and minute_end<?',(day,day+'T15:01'))]
notifications=[dict(x) for x in db.execute('select * from notification_deliveries where created_at>=? and created_at<?',(day,day+'T15:06'))]
leases=[dict(x) for x in db.execute('select * from scheduler_leases')]
lifecycles=[dict(x) for x in db.execute('select * from a4_signal_lifecycles where trade_date=?',(day,))]
fills=[dict(x) for x in db.execute('select * from virtual_fills where bar_end>=? and bar_end<?',(day,day+'T15:01'))]
db.rollback();db.close()
symbols=sorted({p['symbol'] for p in plans})
bars=sqlite3.connect(f'file:{settings.minute_cache_dir}/minute_bars.sqlite3?mode=ro',uri=True);bars.row_factory=sqlite3.Row
bars.execute('BEGIN')
archive=[dict(x) for x in bars.execute('select * from minute_bars where bar_end>=? and bar_end<?',(day,day+'T15:01')) if x['symbol'] in symbols]
conflicts=[dict(x) for x in bars.execute('select * from minute_bar_audit where observed_at>=? and observed_at<?',(day,day+'T15:06')) if x['symbol'] in symbols]
bars.rollback();bars.close()
def read_json(p):return json.loads(p.read_text()) if p.exists() else None
morning=read_json(root/'outputs/runs'/f'{day}-morning-review.json')
cut=datetime.fromisoformat(day+'T15:00:00+08:00')
def fetch(task):
 symbol,source,interval=task
 client=(TencentIntradayAdapter(timeout_seconds=8) if source=='tencent' else MootdxAdapter(nodes=settings.mootdx_servers[:2],timeout_seconds=4,max_pages=3))
 result=client.fetch_bars(symbol,interval,240 if interval=='1m' else 48,as_of=cut)
 return {'symbol':symbol,'source':source,'interval':interval,'complete':result.complete,'reason':result.reason_code,'bars':[b.model_dump(mode='json') for b in result.bars if b.bar_end.date()==cut.date()]}
with ThreadPoolExecutor(max_workers=4) as pool:
 fetched=list(pool.map(fetch,[(s,source,i) for s in symbols for source in ['tencent','tdx'] for i in ['1m','5m']]))
paths=['server/scheduler.ts','src/liangjian_funnel/runtime/strategies.py','src/liangjian_funnel/runtime/monitor.py','src/liangjian_funnel/data/cache.py','src/liangjian_funnel/data/tencent_minute.py']
print(json.dumps({'schema':'a4-session-capture/1','trade_date':day,'captured_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),'host':'192.168.31.254','root':str(root),'commit':subprocess.check_output(['git','-c',f'safe.directory={root}','rev-parse','HEAD'],text=True).strip(),'code_hashes':{p:hashlib.sha256((root/p).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in paths},'plans':plans,'events':events,'notifications':notifications,'leases':leases,'lifecycles':lifecycles,'fills':fills,'morning':morning,'archive':archive,'conflicts':conflicts,'verified_fetches':fetched},ensure_ascii=False))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trade-date', required=True)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--market-only', action='store_true')
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_TO_OVERWRITE_FROZEN_CAPTURE')
    code = 'DAY=' + repr(args.trade_date) + '\nPREFIX=' + repr(args.prefix) + '\n' + REMOTE
    if args.market_only:
        code = 'DAY=' + repr(args.trade_date) + '''
import json
from pathlib import Path
from liangjian_funnel.settings import Settings
s=Settings.from_env(root=Path.cwd())
print(json.dumps({p.name:json.loads(p.read_text()) for p in (s.fact_store_dir/'a4_live_market'/DAY).glob('*.json')}))
'''
    result = subprocess.run(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', 'aurum-vm',
         'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
        input=code.encode(), capture_output=True, timeout=300, check=True,
    )
    capture = json.loads(result.stdout)
    atomic_write_json(args.output, capture)
    if args.market_only:
        print(json.dumps({'market_snapshots':len(capture),'path':str(args.output)}))
        return
    print(json.dumps({'path':str(args.output.resolve()),'sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),
                      'plans':len(capture['plans']),'events':len(capture['events']),
                      'fetches':len(capture['verified_fetches']),
                      'fetch_failures':[{k:r[k] for k in ['symbol','source','interval','reason']} for r in capture['verified_fetches'] if not r['complete']]},ensure_ascii=False))


if __name__ == '__main__':
    main()
