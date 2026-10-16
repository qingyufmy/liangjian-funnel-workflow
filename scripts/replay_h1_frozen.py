"""COUNTERFACTUAL replay from archived raw bytes; local temporary DB only."""
from __future__ import annotations
import argparse
import datetime as dt
import gc
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.entry_contract import entry_bar_for
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.runtime.simulation import PaperBroker
from liangjian_funnel.workflow import WorkflowApplication


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--incident', type=Path, required=True)
    parser.add_argument('--windows', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = args.incident.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == 'd7f16f3943c72334a7b2cdaad2be3335f804be8f1373a48f14855600b8cf7e10'
    incident = json.loads(raw)
    windows_raw = args.windows.read_bytes()
    assert hashlib.sha256(windows_raw).hexdigest() == '0981aa5adfb0cbc48106f437e5c1e87e57ab57582809ad772c332ed2d3cfaa44'
    windows = json.loads(windows_raw)
    event = next(row for row in incident['events'] if row['action'] == 'BUY_SIGNAL')
    payload = json.loads(event['payload_json'])
    plan = next(row for row in incident['today_plans'] if row['plan_id'] == payload['plan_id'])
    eligible = entry_bar_for({**payload, 'signal_time': event['minute_end']})
    snapshots = []
    for snapshot in windows['snapshots']:
        original = snapshot['original_payload_utf8'].encode('utf-8')
        assert hashlib.sha256(original).hexdigest() == snapshot['payload_sha256']
        bars = tuple(MinuteBar.model_validate(row) for row in json.loads(original))
        snapshots.append((snapshot, bars))
    observed = next((snapshot, bars) for snapshot, bars in snapshots
                    if snapshot['snapshot_id'] == 'minute-20261016T111900+0800')
    eligible_bar = next(bar for bar in observed[1] if bar.bar_end == eligible)
    with tempfile.TemporaryDirectory(prefix='liangjian-h1-frozen-local-') as directory:
        store = RuntimeStore(Path(directory) / 'counterfactual.sqlite3')
        store.create_execution_plan(plan['plan_id'], 'lane_1', plan['symbol'],
            status=PlanStatus.ACTIVE_TODAY, valid_from=plan['valid_from'],
            expires_at=plan['expires_at'], payload=json.loads(plan['payload_json']))
        store.record_monitor_event(event_key=event['event_key'], lane_id='lane_1',
            minute_end=dt.datetime.fromisoformat(event['minute_end']), action=event['action'],
            reason_code=event['reason_code'], effective=True, sync_a4_lifecycle=True, payload=payload)
        broker = PaperBroker(store, account_id='paper:lane_1', model='H1_FROZEN_COUNTERFACTUAL')
        app = SimpleNamespace(store=store, brokers={'lane_1': broker})
        assert WorkflowApplication._expire_missed_a4_entries(app, dt.datetime.fromisoformat('2026-10-16T11:19:00+08:00')) == 0
        rows = WorkflowApplication._settle_prior_signals(app, 'lane_1', plan['symbol'],
            observed[1][-1], entry_bars=observed[1])
        assert rows[0]['status'] == 'FILLED'
        lifecycle = store.get_a4_signal_lifecycle(event['event_key'])
        fills = store.list_fills('paper:lane_1')
        assert lifecycle['status'] == 'OPEN' and len(fills) == 1
        assert fills[0]['bar_end'] == eligible.isoformat()
        assert lifecycle['remaining_qty'] == fills[0]['qty']
        cash = store.get_account('paper:lane_1')['cash']
        assert WorkflowApplication._settle_prior_signals(app, 'lane_1', plan['symbol'],
            observed[1][-1], entry_bars=observed[1]) == []
        assert store.get_account('paper:lane_1')['cash'] == cash
        result = {'actual_at': dt.datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
            'classification': 'COUNTERFACTUAL_FROZEN_WINDOW_NOT_HISTORICAL_REPAIR',
            'production_writes': 0, 'source_calls': 0, 'model_calls': 0,
            'all_7_original_snapshot_hashes_verified': len(snapshots) == 7,
            'incident_sha256': hashlib.sha256(raw).hexdigest(),
            'window_archive_sha256': hashlib.sha256(windows_raw).hexdigest(),
            'settlement_snapshot_id': observed[0]['snapshot_id'],
            'original_eligible_bar': eligible_bar.model_dump(mode='json'),
            'result': rows[0]['status'], 'lifecycle_status': lifecycle['status'],
            'fill_qty': fills[0]['qty'], 'fill_price': fills[0]['price'],
            'restart_idempotent': True,
            'local_account_basis': 'ISOLATED_1M_INITIAL_CASH_EXISTING_DEFAULTS_NOT_ORIGINAL_ACCOUNT_RECONSTRUCTION',
            'source_hashes': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (ROOT/'src/liangjian_funnel/workflow.py',
                          ROOT/'src/liangjian_funnel/runtime/state.py',
                          ROOT/'src/liangjian_funnel/runtime/entry_contract.py',
                          ROOT/'src/liangjian_funnel/runtime/simulation.py')}}
        store.close()
        gc.collect()
    with args.output.open('x', encoding='utf-8') as target:
        json.dump(result, target, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
