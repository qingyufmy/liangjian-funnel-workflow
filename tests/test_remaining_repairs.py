from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json

import pytest

from liangjian_funnel.data.live_fetch import fetch_live_window
from liangjian_funnel.data.mootdx import FetchResult, MinuteBar, MootdxAdapter, MootdxNode
from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.runtime.indicator_windows import freeze_observations, load_window
from liangjian_funnel.review.indicator_evidence import audit_event_indicators, kdj
from liangjian_funnel.runtime.close_finalization import collect_close_finalization
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication

TZ = ZoneInfo('Asia/Shanghai')
NOW = datetime(2026, 9, 14, 9, 31, tzinfo=TZ)
SYMBOL = '600519.SH'


def bar(at=NOW, interval='1m', volume=100):
    return MinuteBar(symbol=SYMBOL, interval=interval, bar_end=at, open=10, high=11,
                     low=9, close=10, volume=volume, amount=1000, source_id='TEST')


def result(bars=(), reason='OK', complete=True, interval='1m'):
    return FetchResult(symbol=SYMBOL, interval=interval, requested_bars=len(bars) or 1,
                       returned_bars=len(bars), bars=tuple(bars), reason_code=reason, complete=complete)


def test_retry_only_transient_and_enforces_shared_deadline():
    calls = []
    def source(*args, **kwargs):
        calls.append(1)
        return result([bar()]) if len(calls) == 2 else result(reason='TENCENT_REQUEST_FAILED', complete=False)
    provider = SimpleNamespace(fetch_bars=source)
    assert fetch_live_window(provider, None, SYMBOL, '1m', 1, NOW).complete
    assert len(calls) == 2
    calls.clear()
    blocked = fetch_live_window(provider, None, SYMBOL, '1m', 1, NOW, deadline=0)
    assert not calls and blocked.reason_code == 'MINUTE_FETCH_BUDGET_EXHAUSTED'


def test_invalid_response_not_retried_and_stale_complete_rejected():
    calls = []
    def source(*args, **kwargs):
        calls.append(1)
        return result([bar(NOW-timedelta(days=1))])
    r = fetch_live_window(SimpleNamespace(fetch_bars=source), None, SYMBOL, '1m', 1, NOW)
    assert not r.complete and len(calls) == 1
    assert r.reason_code == 'CURRENT_SESSION_WINDOW_INVALID'


def test_late_response_is_not_decision_input():
    clock_values = iter([1., 3.])
    source = SimpleNamespace(fetch_bars=lambda *a, **k: result([bar()]))
    r = fetch_live_window(source, None, SYMBOL, '1m', 1, NOW, deadline=2, clock=lambda: next(clock_values))
    assert not r.complete and r.reason_code == 'MINUTE_FETCH_BUDGET_EXHAUSTED'


def test_tdx_default_factory_has_no_global_node_state(monkeypatch):
    clients = []
    class Client:
        def __init__(self, **kwargs): clients.append(self)
        def connect(self, host, port, **kwargs): self.host = host; return True
        def get_security_bars(self, frequency, market, code, start, count): return [self.host, market, code]
        def disconnect(self): pass
    monkeypatch.setattr('tdxpy.hq.TdxHq_API', Client)
    adapter = MootdxAdapter()
    a = adapter._default_factory(MootdxNode(host='127.0.0.1'))
    b = adapter._default_factory(MootdxNode(host='127.0.0.2'))
    assert a.bars(symbol=SYMBOL, frequency=8) == ['127.0.0.1', 1, '600519']
    assert b.bars(symbol='000001.SZ', frequency=8) == ['127.0.0.2', 0, '000001']


def test_indicator_windows_deduplicate_and_verify_identity(tmp_path):
    series = [{'end': (NOW+timedelta(minutes=5*i)).isoformat(), 'high': 11, 'low': 9, 'close': 10} for i in range(9)]
    declared = {'available': True, 'timeframe': '5m_closed', 'input_series': series, 'bar_count': 9,
                'closed_bar_end': series[-1]['end'], **kdj(series)}
    observations = freeze_observations(tmp_path, {'kdj': declared}, SYMBOL)
    assert freeze_observations(tmp_path, {'kdj': declared}, SYMBOL) == observations
    assert len(list(tmp_path.iterdir())) == 1 and 'input_series' not in observations['kdj']
    events = [{'minute_end': series[-1]['end'], 'payload_json': {'symbol': SYMBOL, 'strategy': {'indicator_observations': observations}}}]
    audit = audit_event_indicators(events, strategy_profile='MA520_SWING', symbol=SYMBOL,
                                  window_loader=lambda key: load_window(tmp_path, key))
    assert audit['counts']['kdj'] == {'MATCH': 1}
    assert audit['counts']['m15_macd'] == {'DECLARATION_MISSING': 1}
    assert audit_event_indicators(events, symbol='000001.SZ', window_loader=lambda key: load_window(tmp_path,key))['counts']['kdj'] == {'INVALID_INPUT_EVIDENCE': 1}
    with pytest.raises(ValueError): load_window(tmp_path, '../outside')


@pytest.mark.parametrize('authority', ['context', 'audit'])
def test_research_block_cannot_be_published_even_when_model_removes_it(tmp_path, authority):
    store = RuntimeStore(tmp_path/'state.db')
    app = SimpleNamespace(store=store)
    lane = SimpleNamespace(lane='lane_1', status='READY', final_output={'core_watch_pool': [{'symbol': SYMBOL}], 'secondary_watch_pool': []})
    lane.stages = [SimpleNamespace(stage='A2', output={'watch_only_pool': [{'symbol': SYMBOL, 'execution_permission': 'BLOCKED'}]})] if authority=='audit' else []
    run = SimpleNamespace(run_id='shadow-test', lanes=[lane])
    published = WorkflowApplication._publish_plans(app, run, 'close', NOW.replace(hour=17),
        snapshot_data={'A2_BOTTLENECK_CONTEXT': {SYMBOL: {'execution_permission': 'BLOCKED'}}} if authority=='context' else {})
    assert published['created'] == []
    assert published['blocked'][0]['reason'] == 'A3_EMOTION_RESEARCH_ONLY_NO_ENTRY'
    assert store.list_execution_plans() == ()


def test_close_collection_preserves_frozen_decision_and_no_signal(tmp_path):
    store = MinuteBarStore(tmp_path/'bars')
    cutoff = NOW.replace(hour=15, minute=0)
    original = bar(cutoff, volume=0)
    store.write_live([original], as_of=cutoff, snapshot_id='original')
    def source(symbol, interval, count, **kwargs):
        step = 1 if interval == '1m' else 5
        ends = [NOW.replace(hour=9, minute=30)+timedelta(minutes=i*step) for i in range(1,120//step+1)]
        ends += [NOW.replace(hour=13, minute=0)+timedelta(minutes=i*step) for i in range(1,120//step+1)]
        return result([bar(t, interval) for t in ends], interval=interval)
    receipt = collect_close_finalization(SimpleNamespace(fetch_bars=source), store,
        [{'symbol': SYMBOL, 'expires_at': cutoff.isoformat()}], tmp_path/'outputs', now=cutoff+timedelta(minutes=2))
    assert receipt['status'] == 'COMPLETE' and receipt['creates_signals'] is False
    assert all(len(r['attempts']) == 2 and not r['provider_official_final'] for r in receipt['records'])
    assert store.load_decision_snapshot('original',SYMBOL,'1m',as_of=cutoff)[-1].volume == 0
    assert store.latest_decision_snapshot(SYMBOL,'1m',as_of=cutoff)['bars'][-1].volume == 0


def test_source_alert_is_incident_scoped_and_recovery_once():
    from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
    ledger = []
    app = SimpleNamespace(store=SimpleNamespace(list_notification_deliveries=lambda **k: ledger[-1:]))
    def send(**kwargs):
        ledger.append({'status': 'SENT', 'payload_json': json.dumps(kwargs['summary'])})
        return {'status': 'SENT'}
    app._send = send
    method = WorkflowLarkPublisher.publish_minute_source_health
    assert len(method(app, {SYMBOL:'TENCENT_REQUEST_FAILED'}, now=NOW)) == 1
    assert method(app, {SYMBOL:'TENCENT_REQUEST_FAILED'}, now=NOW+timedelta(minutes=1)) == []
    assert len(method(app, {}, now=NOW+timedelta(minutes=2))) == 1
    assert method(app, {}, now=NOW+timedelta(minutes=3)) == []


def test_failed_tdx_nodes_cool_down_across_worker_restarts(tmp_path):
    calls = []
    def factory(node):
        calls.append(node.server)
        raise TimeoutError('test')
    tencent = SimpleNamespace(fetch_bars=lambda *a, **k: result(reason='TENCENT_REQUEST_FAILED', complete=False))
    def adapter():
        source = MootdxAdapter(nodes=[('127.0.0.1',7709),('127.0.0.2',7709)],client_factory=factory)
        source.live_health_path = tmp_path/'health.json'
        return source
    for _ in range(3):
        assert not fetch_live_window(tencent, adapter(), SYMBOL, '1m', 1, NOW).complete
    assert len(calls) == 2 and len(set(calls)) == 2
