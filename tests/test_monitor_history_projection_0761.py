from datetime import datetime, timedelta
import json
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import RuntimeStore

NOW=datetime(2026,10,26,10,25,tzinfo=ZoneInfo('Asia/Shanghai'))


def emit(store,key,action,at,*,plan='current',symbol='300817.SZ',effective=False,reason='TEST'):
    store.record_monitor_event(event_key=key,lane_id='lane_1',minute_end=at,action=action,effective=effective,
        reason_code=reason,payload=dict(plan_id=plan,symbol=symbol,strategy={'original':'x'*1000}))


def test_settlement_projection_is_exact_old_filter_with_history_and_other_symbols(tmp_path):
    store=RuntimeStore(tmp_path/'workflow.db')
    emit(store,'entry','BUY_SIGNAL',NOW-timedelta(days=2),effective=True)
    emit(store,'exit','FORCED_RISK_EXIT',NOW-timedelta(days=1),effective=True)
    emit(store,'other','BUY_SIGNAL',NOW,plan='other',symbol='688099.SH',effective=True)
    emit(store,'veto','LLM_VETO',NOW,effective=True)
    for i in range(100):emit(store,f'observation-{i}','DATA_BLOCK',NOW-timedelta(minutes=i))
    actions=('BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','FORCED_RISK_EXIT')
    original=store.list_monitor_events(lane_id='lane_1',effective_only=True)
    expected=tuple(row for row in original if row['action'] in actions and json.loads(row['payload_json'])['symbol']=='300817.SZ')
    actual=store.list_monitor_events(lane_id='lane_1',effective_only=True,actions=actions,symbol='300817.SZ')
    assert actual==expected and len(actual)==2


def test_decision_history_preserves_episode_and_t1_but_drops_irrelevant_payloads(tmp_path):
    store=RuntimeStore(tmp_path/'workflow.db')
    emit(store,'accepted','BUY_SIGNAL',NOW-timedelta(days=2),effective=True)
    emit(store,'prior-veto','LLM_VETO',NOW-timedelta(minutes=1),effective=True)
    emit(store,'risk','NO_ACTION',NOW-timedelta(days=1),reason='T1_LOCKED')
    emit(store,'recent-observation','NO_ACTION',NOW-timedelta(minutes=1),reason='SIGNAL_ALREADY_EMITTED')
    for i in range(100):emit(store,f'old-{i}','DATA_BLOCK',NOW-timedelta(minutes=2+i),plan='other')
    original=store.list_monitor_events(lane_id='lane_1')
    projected=store.list_monitor_decision_history(lane_id='lane_1',plan_ids=['current'],symbols=['300817.SZ'],minute=NOW)
    assert len(projected)==4 and len(original)==104
    for action in ['BUY_SIGNAL','ADD_SIGNAL','REDUCE_SIGNAL','SELL_SIGNAL']:
        assert MonitorEngine._resolved_trigger_episode(projected,plan_id='current',minute=NOW,action=action)==\
               MonitorEngine._resolved_trigger_episode(original,plan_id='current',minute=NOW,action=action)
    assert any(row['reason_code']=='T1_LOCKED' for row in projected)


def test_query_uses_new_indexes_and_empty_actions_do_not_read_history(tmp_path):
    store=RuntimeStore(tmp_path/'workflow.db')
    emit(store,'one','BUY_SIGNAL',NOW,effective=True)
    assert store.list_monitor_events(actions=())==()
    plan=store._read(lambda db:db.execute("EXPLAIN QUERY PLAN SELECT * FROM monitor_events WHERE lane_id=? AND effective=1 AND action IN (?) ORDER BY minute_end,event_id",('lane_1','BUY_SIGNAL')).fetchall())
    assert any('idx_monitor_events_lane_effective_action' in row[3] for row in plan)
