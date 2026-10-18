"""Post-hoc reconstruction in a temporary LOCAL DB; never production repair."""
import datetime, hashlib, json, pathlib, sys, tempfile, traceback
from types import SimpleNamespace
from zoneinfo import ZoneInfo
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus, StateTransitionError
from liangjian_funnel.workflow import WorkflowApplication
import liangjian_funnel.runtime.state as state_module
assert pathlib.Path(state_module.__file__).resolve().is_relative_to(ROOT/'src')
sys.stdout.reconfigure(encoding='utf-8')
source=pathlib.Path(sys.argv[1]);raw=source.read_bytes();data=json.loads(raw)
event=next(e for e in data['events'] if e['action']=='BUY_SIGNAL')
payload=json.loads(event['payload_json']);pid=payload['plan_id']
plan=next(p for p in data['today_plans'] if p['plan_id']==pid)
fill=next(f for f in data['fills'] if f['symbol']==payload['symbol'])
actual=datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
result={'actual_at':actual.isoformat(),'classification':'POST_HOC_RECONSTRUCTION_LOCAL_DB_ONLY',
        'module_path':state_module.__file__,'module_sha256':hashlib.sha256(pathlib.Path(state_module.__file__).read_bytes()).hexdigest(),
        'input_sha256':hashlib.sha256(raw).hexdigest(),'production_writes':0,
        'signal_time':event['minute_end'],'contract':payload.get('entry_contract'),
        'fill_bar_end':fill['bar_end'],'fill_created_at':fill['created_at']}
with tempfile.TemporaryDirectory(prefix='liangjian-terminal-reconstruction-') as directory:
 store=RuntimeStore(pathlib.Path(directory)/'local.sqlite3')
 store.create_execution_plan(pid,'lane_1',plan['symbol'],status=PlanStatus.ACTIVE_TODAY,
    valid_from=datetime.datetime.fromisoformat(plan['valid_from']),
    expires_at=datetime.datetime.fromisoformat(plan['expires_at']),payload=json.loads(plan['payload_json']))
 store.record_monitor_event(event_key=event['event_key'],lane_id='lane_1',
    minute_end=datetime.datetime.fromisoformat(event['minute_end']),action=event['action'],
    reason_code=event['reason_code'],effective=True,sync_a4_lifecycle=True,payload=payload)
 current=datetime.datetime.fromisoformat('2026-10-16T11:19:00+08:00')
 result['premature_expiry_count']=WorkflowApplication._expire_missed_a4_entries(SimpleNamespace(store=store),current)
 result['status_after_expiry']=store.get_a4_signal_lifecycle(event['event_key'])['status']
 try:store.apply_a4_fill(event['event_key'],fill)
 except StateTransitionError as exc:
  result['reproduced_error']=str(exc);result['traceback']=traceback.format_exc()
 else:raise AssertionError('ORIGINAL_CONFLICT_DID_NOT_REPRODUCE')
 assert result['reproduced_error']=='A4_TERMINAL_STATE_CONFLICT'
 # Counterfactual ORDER ONLY: no terminal relaxation, apply the already-frozen
 # eligible closed-minute fill before expiring stale signals in a fresh DB.
 other=RuntimeStore(pathlib.Path(directory)/'ordered.sqlite3')
 other.create_execution_plan(pid,'lane_1',plan['symbol'],status=PlanStatus.ACTIVE_TODAY,
    valid_from=datetime.datetime.fromisoformat(plan['valid_from']),
    expires_at=datetime.datetime.fromisoformat(plan['expires_at']),payload=json.loads(plan['payload_json']))
 other.record_monitor_event(event_key=event['event_key'],lane_id='lane_1',
    minute_end=datetime.datetime.fromisoformat(event['minute_end']),action=event['action'],
    reason_code=event['reason_code'],effective=True,sync_a4_lifecycle=True,payload=payload)
 row,inserted=other.apply_a4_fill(event['event_key'],fill)
 result['ordered_counterfactual']={'fill_applied':inserted,'status':row['status'],
    'remaining_qty':row['remaining_qty'],'expired_after_fill':WorkflowApplication._expire_missed_a4_entries(SimpleNamespace(store=other),current)}
 store.close();other.close()
 # The legacy store close() is a no-op and sqlite context managers leave
 # connection finalization to GC; release local handles before Windows cleanup.
 import gc
 gc.collect()
target=source.parent/(actual.strftime('%Y%m%dT%H%M%S%f')+'-terminal-reconstruction.json')
with target.open('x',encoding='utf-8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
print(json.dumps(result,ensure_ascii=False,indent=2));print('result',target)
