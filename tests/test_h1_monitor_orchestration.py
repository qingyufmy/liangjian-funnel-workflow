"""Synthetic full monitor_once boundary: local DB/cache, no model/network/Lark."""
import json
import sqlite3
import os
import time
from datetime import datetime,timedelta
from types import SimpleNamespace, MethodType

import pytest
from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import FetchResult
from liangjian_funnel.data.session_windows import closed_window_ends
from liangjian_funnel.data.tencent_minute import MarketQuote, QuoteResult
from liangjian_funnel.runtime.state import PlanStatus
from liangjian_funnel.workflow import WorkflowApplication
from test_h1_entry_settlement_invariants import seeded, at, corrupt_local_fixture_only


def test_next_session_reconciliation_precedes_real_position_hard_stop_path(tmp_path):
    store,app,bar=seeded(tmp_path)
    WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    original=corrupt_local_fixture_only(tmp_path,store)
    current=datetime.fromisoformat('2026-10-19T09:31:00+08:00')
    app.settings=SimpleNamespace(workflow_output_dir=tmp_path/'outputs')
    app._ensure_trading_day=lambda *_:None
    app._fetch_live_quote=lambda symbol,*_,**__: QuoteResult(symbol=symbol,complete=True,reason_code='OK',
        quote=MarketQuote(symbol=symbol,quote_time=current,price=14.4,previous_close=14.85,
            open=14.85,volume=1000,amount=14400,source_id='SYNTHETIC:H1_NEXT_SESSION'))
    def barrier(**_):
        assert store.get_a4_plan_quarantine('p1') is not None
        assert any(row['action']=='FORCED_RISK_EXIT' for row in store.list_monitor_events(effective_only=True))
        raise RuntimeError('H1_POSITION_PROTECTION_OBSERVED')
    app.activate_latest_a3_for_monitor=barrier
    with pytest.raises(RuntimeError,match='^H1_POSITION_PROTECTION_OBSERVED$'):
        WorkflowApplication.monitor_once(app,now=current)
    assert store.get_a4_signal_lifecycle('h1-entry')==original


def test_full_monitor_with_one_bad_plan_keeps_other12_applicable239_minutes(tmp_path,monkeypatch):
    if os.name=='nt':
        # Windows scanners can briefly deny os.replace while hundreds of
        # snapshots are compressed into seconds. Retry the SAME operation
        # in this temporary fixture only; do not change production semantics.
        original_replace=os.replace
        def fixture_replace(source,target):
            for attempt in range(10):
                try:return original_replace(source,target)
                except PermissionError as exc:
                    if getattr(exc,'winerror',None) not in (5,32) or attempt==9:raise
                    time.sleep(.02)
        monkeypatch.setattr(os,'replace',fixture_replace)
    store,app,seed=seeded(tmp_path)
    others={f'healthy-{i}':f'{600000+i:06d}.SH' for i in range(12)}
    for pid,symbol in others.items():
        store.create_execution_plan(pid,'lane_1',symbol,status=PlanStatus.ACTIVE_TODAY,
            valid_from=at('09:32:00'),expires_at=at('15:00:00'),
            payload={'trigger_low':100,'trigger_high':101,'confirmation_bars':2})
    app.settings=SimpleNamespace(workflow_output_dir=tmp_path/'outputs',research_primary_lane_id='lane_1')
    app.minute_store=MinuteBarStore(tmp_path/'minute')
    app.market_data=object();app.lark_publisher=None
    app._ensure_trading_day=lambda *_: None
    app.activate_latest_a3_for_monitor=lambda **_: {'status':'NOT_APPLICABLE'}
    app._a4_callback=lambda *_,**__: None
    app._record_a4_outcomes=MethodType(WorkflowApplication._record_a4_outcomes,app)
    def history(symbol,interval,required,cutoff,**kwargs):
        bars=tuple(seed.model_copy(update={'symbol':symbol,'bar_end':end})
                   for end in closed_window_ends(cutoff,'1m'))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=required,
            returned_bars=len(bars),bars=bars,reason_code='OK',complete=True)
    app._fetch_live_bars=history
    app._fetch_live_quote=lambda symbol,current,**_: QuoteResult(symbol=symbol,complete=True,reason_code='OK',
        quote=MarketQuote(symbol=symbol,quote_time=current,price=14.85,previous_close=14.85,
            open=14.85,volume=1000000,amount=14850000,source_id='SYNTHETIC:H1_QUOTE'))
    monkeypatch.setattr('liangjian_funnel.workflow.load_or_refresh_live_market_state',
        lambda *_,**kwargs: {'status':'READY','entry_permission':'ALLOW','as_of':kwargs['as_of'].isoformat()})
    with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
        c.execute("CREATE TRIGGER h1_fault BEFORE UPDATE ON a4_signal_lifecycles WHEN OLD.plan_id='p1' BEGIN SELECT RAISE(ABORT,'H1_ONLY_THIS_PLAN'); END")
    minutes=[at('09:32:00')+timedelta(minutes=i) for i in range(119)]
    minutes += [at('13:01:00')+timedelta(minutes=i) for i in range(120)]
    rounds=[]
    for minute in minutes:
        result=WorkflowApplication.monitor_once(app,now=minute)
        assert result['minute_cache']['errors']==[]
        assert not (set(result['observability']['blocked_scope']) & set(others.values()))
        rounds.append(result['observability']['axes']['job_status'])
    assert len(rounds)==239 and set(rounds)=={'SUCCEEDED'}
    events=store.list_monitor_events(lane_id='lane_1')
    for pid in others:
        observed=[row for row in events if json.loads(row['payload_json']).get('plan_id')==pid]
        assert len(observed)==239
        assert len({row['minute_end'] for row in observed})==239
        assert not any(row['action']=='DATA_BLOCK' for row in observed)
    assert store.get_a4_plan_quarantine('p1') is not None
    assert store.list_fills('paper:lane_1')==()
