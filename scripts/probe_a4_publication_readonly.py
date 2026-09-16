"""One bounded live shadow acquisition; no production code/data/plan writes."""
import argparse
import base64
import json
from pathlib import Path
import subprocess

from liangjian_funnel.reporting import atomic_write_json

REMOTE = r'''
import collections, json, sqlite3, sys, time, types, base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from liangjian_funnel.settings import Settings
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
from liangjian_funnel.data.live_fetch import fetch_live_window
from liangjian_funnel.data.session_windows import TZ, closed_window_ends
m=types.ModuleType('liangjian_funnel.data.publication')
m.__package__='liangjian_funnel.data'
exec(compile(base64.b64decode(SOURCE), '<local-publication-shadow>', 'exec'), m.__dict__)
s=Settings.from_env(root=Path.cwd())
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro', uri=True)
now=datetime.now(TZ)
symbols=[r[0] for r in c.execute("select distinct symbol from execution_plans where status='ACTIVE_TODAY' and expires_at>=?", (now.isoformat(),))]
c.close()
if not 1<=len(symbols)<=50: raise SystemExit('UNEXPECTED_PLAN_COUNT')
at=now.replace(second=0,microsecond=0)+timedelta(minutes=1)
delay=(at+timedelta(seconds=3)-datetime.now(TZ)).total_seconds()
time.sleep(max(0,delay))
started=time.monotonic(); deadline=started+m.ACQUISITION_BUDGET_SECONDS
provider=TencentIntradayAdapter(timeout_seconds=2)
def fetch(symbol):
    result={}
    for period in ('1m','5m'):
        count=len(closed_window_ends(at,period))
        result[period]=fetch_live_window(provider,None,symbol,period,count,at,deadline=deadline) if count else None
    return symbol,result
with ThreadPoolExecutor(max_workers=8) as pool: initial=dict(pool.map(fetch,symbols))
initial_elapsed=time.monotonic()-started
result=m.confirm_publications(initial,fetch,at=at,deadline=deadline)
report={'mode':'READ_ONLY_SHADOW_NO_DECISIONS', 'cutoff':at.isoformat(), 'symbol_count':len(symbols),
 'initial_fetch_seconds':initial_elapsed, 'total_seconds':time.monotonic()-started,
 'states':dict(collections.Counter(v['publication']['state'] for v in result.values())),
 'symbols':{k:{**v['publication'],'decision_error':v.get('publication_error')} for k,v in result.items()}}
print(json.dumps(report))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists(): raise SystemExit('REFUSE_OVERWRITE')
    source = Path(__file__).resolve().parents[1]/'src/liangjian_funnel/data/publication.py'
    prefix = 'SOURCE='+repr(base64.b64encode(source.read_bytes()).decode())+'\n'
    result = subprocess.run(['ssh', 'aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && sudo -u www .venv/bin/python -B -'],
        input=(prefix+REMOTE).encode(), capture_output=True, timeout=115)
    if result.returncode: raise SystemExit(result.stderr.decode(errors='replace')[-1500:])
    report = json.loads(result.stdout)
    atomic_write_json(args.output, report)
    print(json.dumps({k:v for k,v in report.items() if k!='symbols'}, ensure_ascii=False))


if __name__ == '__main__': main()
