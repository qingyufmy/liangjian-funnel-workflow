"""Replay frozen production plans in isolated ledgers, never publish signals.

Completed historical bars are ex-post sensitivity evidence, not a claim that
their final values or missing market breadth were observable at decision time.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationConfig
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.workflow import WorkflowApplication, _intraday_market_context


def session_minutes(day):
    return [datetime.fromisoformat(f'{day}T{start}:00+08:00') + timedelta(minutes=i)
            for start in ('09:31', '13:01') for i in range(120)]


def market_at(snapshots, now, assume_missing=False):
    bucket = now.replace(minute=now.minute // 5 * 5)
    row = snapshots.get(bucket.strftime('%H%M') + '.json')
    if row and datetime.fromisoformat(row['as_of']) <= now:
        return row, False
    if assume_missing:
        return {'status':'READY','entry_permission':'ALLOW','as_of':now.isoformat(),
                'trade_date':now.date().isoformat(),'source':'TEST_ONLY_MISSING_MARKET_ASSUMPTION'}, True
    return {}, True


def archive_bar(row):
    return MinuteBar(symbol=row['symbol'],interval=row['interval'],bar_end=row['bar_end'],
                     source_id=row['source_id'],adjust_mode=row['adjust_mode'],
                     **{k:float(row[k+'_value']) for k in ('open','high','low','close','volume','amount')})


def replay(capture, snapshots, root, variant, data, *, full=False, assume_missing=False):
    path = root / variant
    if (path/'result.json').exists():
        return json.loads((path/'result.json').read_text(encoding='utf-8'))
    resume = path.exists()
    path.mkdir(parents=True,exist_ok=True)
    store = RuntimeStore(path/'state.sqlite3')
    if resume:
        existing = store.list_monitor_events(lane_id='lane_1')
        expected_last = f"{capture['trade_date']}T15:00:00+08:00" if full else max(e['minute_end'] for e in capture['events'])
        if not existing or max(e['minute_end'] for e in existing) != expected_last:
            raise ValueError('REFUSE_TO_RESUME_INCOMPLETE_REPLAY')
    broker = PaperBroker(store,account_id='paper:lane_1',model='TEST_ONLY_AUDIT',
                         config=SimulationConfig(initial_cash=1_000_000))
    app = SimpleNamespace(store=store,brokers={'lane_1':broker})
    activated = set(capture['morning']['activated'])
    day = capture['trade_date']
    plans = {}
    for row in capture['plans']:
        if row['plan_id'] not in activated:
            continue  # Use actual auction result; never derive it from 09:31.
        payload = json.loads(row['payload_json'])
        plans[row['symbol']] = payload
        store.create_execution_plan(row['plan_id'],'lane_1',row['symbol'],
                                    status=PlanStatus.ACTIVE_TODAY,
                                    valid_from=f'{day}T09:32:00+08:00',expires_at=f'{day}T15:00:00+08:00',payload=payload)
    observed = {e['minute_end'] for e in capture['events']}
    model_assumption_calls = 0
    missing_market = []
    for now in ([] if resume else session_minutes(day)):
        if not full and now.isoformat() not in observed:
            continue
        bars, histories, contexts = {}, {}, {}
        state, missing = market_at(snapshots,now,assume_missing)
        if missing:
            missing_market.append(now.isoformat())
        for symbol in plans:
            one = tuple(b for b in data[(symbol,'1m')] if b.bar_end <= now)
            five = tuple(b for b in data[(symbol,'5m')] if b.bar_end <= now)
            if not one or one[-1].bar_end != now:
                continue
            bars[symbol] = one[-1];histories[symbol] = one
            contexts[symbol] = _intraday_market_context(symbol,one,five,current=now,live_market_state=state)
            WorkflowApplication._settle_prior_signals(app,'lane_1',symbol,one[-1])
            if store.get_position('paper:lane_1',symbol):
                store.observe_a4_lifecycle(account_id='paper:lane_1',symbol=symbol,bar_end=now,
                                          high=one[-1].high,low=one[-1].low,close=one[-1].close)
        # This is deliberately an upper-bound deterministic control, not a
        # fabricated historical model response or a production recommendation.
        batch = MonitorEngine(store,llm_veto=lambda _:False,max_seconds=50).process_minute(
            'lane_1',bars,minute_snapshot_id=f'TEST_ONLY:{variant}:{now.isoformat()}',now=now,
            data_ok=True,snapshot_contiguous=True,bar_histories=histories,market_contexts=contexts)
        model_assumption_calls += int(batch.model_called)
    rows = store.list_monitor_events(lane_id='lane_1')
    if resume:
        missing_market = [now.isoformat() for now in session_minutes(day)
                          if (full or now.isoformat() in observed) and market_at(snapshots,now,assume_missing)[1]]
        model_assumption_calls = len({r['minute_end'] for r in rows if r['action']=='BUY_SIGNAL'})
    records = []
    for r in rows:
        p = json.loads(r['payload_json']);s=p.get('strategy') or {}
        records.append({'minute':r['minute_end'],'symbol':p.get('symbol'),'action':r['action'],
                        'reason':r['reason_code'],'effective':bool(r['effective']),
                        'strategy':s})
    report = {'variant':variant,'mode':'EX_POST_DETERMINISTIC_UPPER_BOUND',
              'actual_model_calls':0,'assumed_model_pass_calls':model_assumption_calls,
              'missing_market_minutes':missing_market,'assume_missing_market':assume_missing,
              'events':records,'reason_counts':dict(Counter(r['reason'] for r in records)),
              'effective':[{k:v for k,v in r.items() if k!='strategy'} for r in records if r['effective']],
              'fills':[dict(x) for x in store.list_fills('paper:lane_1')],
              'lifecycles':[dict(x) for x in store.list_a4_signal_lifecycles(limit=1000)],
              'real_orders':False,'notifications_sent':False}
    atomic_write_json(path/'result.json',report)
    print(json.dumps({k:v for k,v in report.items() if k not in ('events','lifecycles')},ensure_ascii=False),flush=True)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args();root=args.root.resolve()
    capture=json.loads((root/'capture.json').read_text(encoding='utf-8'))
    snapshots=json.loads((root/'market_snapshots.json').read_text(encoding='utf-8'))
    original=defaultdict(list)
    for r in capture['archive']:
        original[(r['symbol'],r['interval'])].append(archive_bar(r))
    verified={}
    tdx={}
    for r in capture['verified_fetches']:
        if r['source']=='tencent':
            expected=240 if r['interval']=='1m' else 48
            if not r['complete'] or len(r['bars'])!=expected:
                raise ValueError('INCOMPLETE_VERIFIED_SESSION')
            verified[(r['symbol'],r['interval'])]=[MinuteBar.model_validate(b) for b in r['bars']]
        if r['source']=='tdx':
            bars=[MinuteBar.model_validate(b) for b in r['bars']]
            # Ex-post sensitivity branch only. The captured TDX node labels
            # the lunch-close bar 13:00, while Tencent labels it 11:30; all
            # forty lunch closes were independently compared. Preserve raw
            # input and make this explicit rather than hiding the discrepancy.
            lunch=datetime.fromisoformat(capture['trade_date']+'T11:30:00+08:00')
            reopening=lunch.replace(hour=13,minute=0)
            if any(b.bar_end==reopening for b in bars) and not any(b.bar_end==lunch for b in bars):
                bars=[b.model_copy(update={'bar_end':lunch}) if b.bar_end==reopening else b for b in bars]
            expected=set(session_minutes(capture['trade_date']))
            if r['interval']=='5m':expected={t for t in expected if t.minute%5==0}
            if {b.bar_end for b in bars}!=expected:
                raise ValueError('TDX_SESSION_LABELS_NOT_RESOLVED')
            tdx[(r['symbol'],r['interval'])]=bars
    for dataset in (original,verified,tdx):
        for bars in dataset.values():bars.sort(key=lambda b:b.bar_end)
    variants=[replay(capture,snapshots,root,'original_observed',original),
              replay(capture,snapshots,root,'verified_observed',verified),
              replay(capture,snapshots,root,'verified_full_known_market',verified,full=True),
              replay(capture,snapshots,root,'verified_full_assumed_market',verified,full=True,assume_missing=True),
              replay(capture,snapshots,root,'tdx_full_assumed_market',tdx,full=True,assume_missing=True)]
    production={}
    for e in capture['events']:
        p=json.loads(e['payload_json']);production[(e['minute_end'],p.get('symbol'))]=(e['action'],e['reason_code'])
    comparisons={}
    for variant in variants[:2]:
        diff=[];matched=0
        for r in variant['events']:
            old=production.get((r['minute'],r['symbol']))
            if old==(r['action'],r['reason']):matched+=1
            else:diff.append({'minute':r['minute'],'symbol':r['symbol'],'production':old,
                              'replay':[r['action'],r['reason']]})
        comparisons[variant['variant']]={'matched':matched,'different':len(diff),'differences':diff}
    expected={m.isoformat() for m in session_minutes(capture['trade_date'])}
    actual={e['minute_end'] for e in capture['events']}
    summary={'capture_sha256':hashlib.sha256((root/'capture.json').read_bytes()).hexdigest(),
             'production_missing_minutes':sorted(expected-actual),'comparisons':comparisons,
             'variants':[{k:v for k,v in x.items() if k not in ('events','lifecycles')} for x in variants]}
    atomic_write_json(root/'replay_summary.json',summary)


if __name__=='__main__':main()
