from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus


@pytest.mark.parametrize('model_elapsed,should_signal',[(1,True),(16,False)])
def test_deterministic_time_does_not_consume_model_cap_but_model_and_round_remain_bounded(tmp_path,model_elapsed,should_signal):
    now=datetime(2026,10,26,10,tzinfo=ZoneInfo('Asia/Shanghai'));elapsed=[0.0]
    store=RuntimeStore(tmp_path/'workflow.db')
    store.create_execution_plan('p','lane_1','300817.SZ',status=PlanStatus.ACTIVE_TODAY,
        valid_from=now.replace(hour=9,minute=32),expires_at=now.replace(hour=15),
        payload={'trigger_low':9,'trigger_high':11,'stop_level':8,'confirmation_bars':1})
    bar=MinuteBar(symbol='300817.SZ',interval='1m',bar_end=now,open=10,high=10,low=10,close=10,
        volume=10000,amount=100000,source_id='CONTROLLED_NATIVE')
    def model(_):elapsed[0]+=model_elapsed;return {'p':False}
    engine=MonitorEngine(store,llm_veto=model,clock=lambda:elapsed[0],wall_clock=lambda:now,
        max_seconds=47,model_max_seconds=15,deadline_monotonic=47)
    def quant(*_):elapsed[0]+=18;return True
    engine._deterministic_trigger=quant
    result=engine.process_minute('lane_1',{'300817.SZ':bar},minute_snapshot_id='controlled',now=now)
    actions={row.action for row in result.events}
    assert ('BUY_SIGNAL' in actions)==should_signal
    if not should_signal:assert any(row.reason_code=='MONITOR_OVERRUN' for row in result.events)
    assert store.list_fills('paper:lane_1')==()
