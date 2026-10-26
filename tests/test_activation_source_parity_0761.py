from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.workflow import WorkflowApplication


@pytest.mark.parametrize('entry', ['review_pending_morning', 'activate_latest_a3_for_monitor'])
def test_wrong_source_session_is_rejected_before_quote_or_state_transition(tmp_path, entry):
    now = datetime(2026, 10, 26, 9, 26 if entry=='review_pending_morning' else 31,
                   tzinfo=ZoneInfo('Asia/Shanghai'))
    store = RuntimeStore(tmp_path/'workflow.sqlite3')
    store.record_workflow_run(run_id='formal-20261026-source-20261024-r1', lane_id='lane_1',
        trade_date='2026-10-26', slot='close', model='fixture', status='READY', snapshot_hash='same')
    store.create_execution_plan('original', 'lane_1', '300817.SZ', status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=now.replace(hour=15, minute=0), payload={'source_run_id':'formal-20261026-source-20261024-r1',
        'stop_level':9, 'trigger_high':10, 'no_chase':11})
    def no_quote(*a, **kw):
        pytest.fail('unproved source must be rejected before source HTTP')
    app = SimpleNamespace(store=store, settings=SimpleNamespace(workflow_output_dir=tmp_path),
        brokers={'lane_1':object()}, market_data=SimpleNamespace(fetch_quote=no_quote),
        _ensure_trading_day=lambda _:None)
    result = getattr(WorkflowApplication, entry)(app, now=now)
    assert result['status']=='BLOCKED' and not result['activated'] and not result.get('invalidated')
    assert result['research_contract']['rejected_plans']==[
        dict(plan_id='original', reason_code='MORNING_SOURCE_NOT_PREVIOUS_CLOSE')]
    assert store.get_execution_plan('original')['status']=='PENDING_MORNING_REVIEW'
