"""Read-only, full-session paired trend timing replay; no account/LLM replay."""
import argparse
import inspect
import json
import subprocess
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime import strategies


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--date', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    source = inspect.getsource(strategies._trend_entry_sequence) + '\n' + inspect.getsource(strategies._evaluate_trend)
    remote = 'DAY=' + repr(args.date) + '\nSOURCE=' + repr(source) + '\n' + r'''
import collections, hashlib, json, sqlite3, zlib
from pathlib import Path
from datetime import datetime
from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime import strategies as st
s=Settings.from_env(root=Path.cwd())
exec(SOURCE, st.__dict__)
db=sqlite3.connect(s.state_db_path.as_uri()+'?mode=ro', uri=True); db.row_factory=sqlite3.Row
plans={r['symbol']:json.loads(r['payload_json']) for r in db.execute(
    'select symbol,payload_json from execution_plans where expires_at>=? and expires_at<? and lane_id=?',
    (DAY,DAY+'T15:01','lane_1'))}
plans={symbol:row for symbol,row in plans.items() if row.get('strategy_profile')=='TREND_MA5'}
db.close()
cache=sqlite3.connect((s.minute_cache_dir/'minute_bars.sqlite3').as_uri()+'?mode=ro',uri=True); cache.row_factory=sqlite3.Row
counts=collections.Counter(); old_actions=collections.Counter(); new_actions=collections.Counter(); changes=[]; by_symbol=collections.Counter(); conditions=collections.Counter()
for row in cache.execute('select symbol,decision_as_of,payload_zlib,payload_sha256 from minute_decision_snapshots where interval=? and decision_as_of>=? and decision_as_of<? order by decision_as_of,symbol',('1m',DAY,DAY+'T15:01')):
    symbol=row['symbol']
    if symbol not in plans: continue
    counts['windows']+=1; raw=zlib.decompress(row['payload_zlib'])
    if hashlib.sha256(raw).hexdigest()!=row['payload_sha256']:
        counts['bad_hash']+=1; continue
    stamp=datetime.fromisoformat(row['decision_as_of']); bars=st._normalize_bars(json.loads(raw), plans[symbol])
    if any(b.end>stamp for b in bars): counts['future']+=1; continue
    five,fifteen=st._aggregate_sessions(bars,as_of=stamp)
    if not five or not fifteen: counts['warming']+=1; continue
    counts['evaluated']+=1; by_symbol[symbol]+=1
    phase=st._trend_entry_sequence(five,st._vwap(five))
    for name,passed in phase.items(): conditions[name]+=int(passed)
    conditions['ALL_PHASES']+=int(all(phase.values()))
    old=st._evaluate_trend({**plans[symbol],'trend_entry_rule_version':'legacy'},five,fifteen,bars[-1],{},False,False)
    new=st._evaluate_trend({**plans[symbol],'trend_entry_rule_version':'trend-ma5/2'},five,fifteen,bars[-1],{},False,False)
    old_actions[old['action']]+=1; new_actions[new['action']]+=1
    if old['action']!=new['action'] or old['reason_codes']!=new['reason_codes']:
        changes.append({'symbol':symbol,'at':stamp.isoformat(),'old_action':old['action'],'new_action':new['action'],
                        'old_reasons':old['reason_codes'],'new_reasons':new['reason_codes']})
cache.close()
print(json.dumps({'trade_date':DAY,'scope':'TREND_ENTRY_TIMING_ONLY_ALL_FROZEN_WINDOWS_NOT_FULL_ACCOUNT_REPLAY',
    'production_updated':False,'strategy_source_sha256':hashlib.sha256(SOURCE.encode()).hexdigest(),
    'plan_count':len(plans),'counts':counts,'by_symbol':by_symbol,'conditions':conditions,'old_actions':old_actions,'new_actions':new_actions,'changes':changes}))
'''
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', 'aurum-vm',
        'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -'],
        input=remote.encode(), capture_output=True, timeout=300)
    if result.returncode:
        raise SystemExit(result.stderr.decode(errors='replace')[-1500:])
    data = json.loads(result.stdout)
    atomic_write_json(args.output, data)
    print(json.dumps({key:value for key,value in data.items() if key not in {'changes','by_symbol'}}))
    if data['counts'].get('bad_hash') or data['counts'].get('future') or not data['counts'].get('evaluated'):
        raise SystemExit('FROZEN_REPLAY_FAILED')


if __name__ == '__main__':
    main()
