"""Capture frozen lunch windows read-only and compare old/new context paths.

No model calls, notifications or production orders. Historical receipt times
are not reconstructed. This is not a full lifecycle/fill backtest.
"""
import ast
import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.execution_evidence import execution_evidence
from liangjian_funnel.data.live_fetch import fetch_live_window
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.strategies import evaluate_strategy
from liangjian_funnel.workflow import _intraday_market_context, _aggregate_closed_15m

REMOTE = r'''
import hashlib,json,sqlite3,zlib
from pathlib import Path
from liangjian_funnel.settings import Settings
s=Settings.from_env(root=Path.cwd())
c=sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
d=sqlite3.connect((s.minute_cache_dir/'minute_bars.sqlite3').resolve().as_uri()+'?mode=ro',uri=True);d.row_factory=sqlite3.Row
plans={r['plan_id']:json.loads(r['payload_json']) for r in c.execute('select plan_id,payload_json from execution_plans')}
windows=[]
for event in c.execute("select * from monitor_events where minute_end>=? and minute_end<=?",(DAY+'T13:01:00+08:00',DAY+'T13:04:00+08:00')):
 p=json.loads(event['payload_json']);symbol=p.get('symbol');pid=p.get('plan_id')
 if not symbol or not pid:continue
 row={'symbol':symbol,'at':event['minute_end'],'event':dict(event),'plan':plans[pid]}
 for interval in ['1m','5m']:
  snap=d.execute('select * from minute_decision_snapshots where symbol=? and interval=? and decision_as_of=?',(symbol,interval,event['minute_end'])).fetchone()
  if not snap:row[interval]=None;continue
  raw=zlib.decompress(snap['payload_zlib'])
  if hashlib.sha256(raw).hexdigest()!=snap['payload_sha256']:raise ValueError('HASH_MISMATCH')
  row[interval]={'bars':json.loads(raw),'captured_at':snap['captured_at'],'hash':snap['payload_sha256']}
 bucket=event['minute_end'][11:16].replace(':','')[:-1]+'0'
 f=s.fact_store_dir/'a4_live_market'/DAY/(bucket+'.json')
 row['market']=json.loads(f.read_text()) if f.exists() else {}
 hist=d.execute("select * from minute_bars where symbol=? and interval='5m' and bar_end<? order by bar_end desc limit 360",(symbol,DAY)).fetchall()
 row['historical_5m']=[{'symbol':symbol,'interval':'5m','bar_end':r['bar_end'],'source_id':r['source_id'],'adjust_mode':r['adjust_mode'],**{k:r[k+'_value'] for k in ['open','high','low','close','volume','amount']}} for r in reversed(hist)]
 windows.append(row)
print(json.dumps(windows,ensure_ascii=False))
'''


def main():
    import argparse
    from types import SimpleNamespace
    from liangjian_funnel.data.mootdx import FetchResult
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    capture = args.output_dir/'capture.json'
    if capture.exists():
        windows = json.loads(capture.read_text(encoding='utf-8'))
    else:
        remote = subprocess.run(['ssh','aurum-vm','cd /www/wwwroot/Agu/liangjian-funnel-workflow && sudo -u www .venv/bin/python -'],
            input=('DAY='+repr(args.day)+'\n'+REMOTE).encode(), capture_output=True, timeout=180)
        if remote.returncode: raise RuntimeError(remote.stderr.decode(errors='replace')[-2000:])
        windows = json.loads(remote.stdout)
        atomic_write_json(capture, windows)
    # Load only the old pure context function, not the old application.
    original = subprocess.check_output(['git','show','cf8bff8:src/liangjian_funnel/workflow.py']).decode('utf-8')
    tree = ast.parse(original)
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name=='_intraday_market_context')
    namespace = {'_aggregate_closed_15m':_aggregate_closed_15m}
    exec('from __future__ import annotations\n'+ast.unparse(function), namespace)
    details=[]
    for row in windows:
        now=datetime.fromisoformat(row['at'])
        one=tuple(MinuteBar.model_validate(b) for b in row['1m']['bars'])
        five=tuple(MinuteBar.model_validate(b) for b in row['5m']['bars']) if row['5m'] else ()
        if any(b.bar_end>now for b in one+five): raise ValueError('FUTURE_BAR')
        aggregate,evidence=execution_evidence(one,five,as_of=now)
        source=SimpleNamespace(fetch_bars=lambda *a, **kw: FetchResult(symbol=row['symbol'],interval='5m',requested_bars=len(five),returned_bars=len(five),bars=five,reason_code='OK',complete=True))
        valid=fetch_live_window(source,None,row['symbol'],'5m',24,now)
        before=namespace['_intraday_market_context'](row['symbol'],one,five,current=now,live_market_state=row['market'])
        after=_intraday_market_context(row['symbol'],one,five,current=now,live_market_state=row['market'])
        before['historical_5m']=after['historical_5m']=row['historical_5m']
        old=evaluate_strategy(row['plan'],one,now=now,position=None,market_context=before).model_dump(mode='json')
        new=evaluate_strategy(row['plan'],one,now=now,position=None,market_context=after).model_dump(mode='json')
        keys=('action','reason_codes','indicator_observations','closed_5m_end','closed_15m_end','live_reward_risk')
        changes=[k for k in keys if old.get(k)!=new.get(k)]
        # Check 15m independently from the native 5m, excluding estimated amount.
        native15=_aggregate_closed_15m(five)
        differences15=[]
        for left,right in zip(aggregate['15m'],native15):
            for k in ('bar_end','open','high','low','close','volume'):
                if isinstance(left[k],(float,int)):
                    if abs(left[k]-right[k])>1e-7:differences15.append(k)
                elif left[k]!=right[k]:differences15.append(k)
        details.append({'symbol':row['symbol'],'at':row['at'],'validator_complete':valid.complete,
            'native_5m':evidence['native_5m_comparison'],'native_15m_fields_differ':differences15,
            'strategy_changes':changes,'before_action':old['action'],'after_action':new['action'],
            'original_action':row['event']['action'],'original_reason':row['event']['reason_code'],
            'recomputed_reasons':new['reason_codes'], 'input_hash':evidence['input_sha256']})
    summary={'windows':len(details),'validation_pass':sum(r['validator_complete'] for r in details),
        'five_minute_conflict_windows':sum(bool(r['native_5m']['conflicts']) for r in details),
        'fifteen_minute_conflict_windows':sum(bool(r['native_15m_fields_differ']) for r in details),
        'strategy_changed_windows':sum(bool(r['strategy_changes']) for r in details),
        'buy_or_sell_candidates':sum(r['after_action'] in ['BUY_SIGNAL','SELL_SIGNAL','ADD_SIGNAL','REDUCE_SIGNAL'] for r in details),
        'boundary':'No orders or LLM calls. Historical warmup from prior-day archive, not a proof of original receipt times. Estimated amount excluded from cross-window equality.'}
    atomic_write_json(args.output_dir/'comparison.json',{'summary':summary,'details':details})
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':main()
