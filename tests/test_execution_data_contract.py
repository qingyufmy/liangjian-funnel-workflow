from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json

import pytest

from liangjian_funnel.data.session_windows import closed_window_ends, latest_closed_end
from liangjian_funnel.data.live_fetch import fetch_live_window
from liangjian_funnel.data.mootdx import MinuteBar, FetchResult
from liangjian_funnel.data.execution_evidence import execution_evidence
from liangjian_funnel.workflow import _intraday_market_context
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.review.engineering import operational_evidence

TZ = ZoneInfo('Asia/Shanghai')
SYMBOL = '600519.SH'


def at(time):
    return datetime.fromisoformat('2026-09-15T' + time + '+08:00')


def bar(end, interval='1m'):
    return MinuteBar(symbol=SYMBOL, interval=interval, bar_end=end, open=10,
                     high=11, low=9, close=10, volume=100, amount=1000, source_id='TEST')


@pytest.mark.parametrize('stamp,five,fifteen', [
    ('09:34:59', None, None), ('09:35:00', '09:35:00', None),
    ('11:29:59', '11:25:00', '11:15:00'), ('11:30:00', '11:30:00', '11:30:00'),
    ('13:00:00', '11:30:00', '11:30:00'), ('13:04:59', '11:30:00', '11:30:00'),
    ('13:05:00', '13:05:00', '11:30:00'), ('13:10:00', '13:10:00', '11:30:00'),
    ('13:15:00', '13:15:00', '13:15:00'), ('15:00:00', '15:00:00', '15:00:00'),
])
def test_expected_clock(stamp, five, fifteen):
    assert latest_closed_end(at(stamp), '5m') == (at(five) if five else None)
    assert latest_closed_end(at(stamp), '15m') == (at(fifteen) if fifteen else None)
    assert closed_window_ends(at(stamp), '1m', calendar=SimpleNamespace(is_trading_day=lambda _: False)) == ()


@pytest.mark.parametrize('bad', ['none', 'missing', 'duplicate', 'unordered', 'old'])
def test_lunch_validation_rejects_real_errors(bad):
    now = at('13:02:00')
    rows = [bar(end, '5m') for end in closed_window_ends(now, '5m')]
    if bad == 'missing': rows.pop(5)
    if bad == 'duplicate': rows[5] = rows[4]
    if bad == 'unordered': rows[4], rows[5] = rows[5], rows[4]
    if bad == 'old': rows = [b.model_copy(update={'bar_end': b.bar_end-timedelta(days=1)}) for b in rows]
    source = SimpleNamespace(fetch_bars=lambda *a, **k: FetchResult(symbol=SYMBOL, interval='5m',
        requested_bars=len(rows), returned_bars=len(rows), bars=tuple(rows), reason_code='OK', complete=True))
    result = fetch_live_window(source, None, SYMBOL, '5m', 24, now)
    assert result.complete is (bad == 'none')
    assert result.response_received_at >= result.request_started_at


def test_model_quant_same_ohlcv_and_native_conflicts_retained():
    now = at('13:05:00')
    one = tuple(bar(end) for end in closed_window_ends(now, '1m'))
    agg, evidence = execution_evidence(one, (), as_of=now)
    native = tuple(MinuteBar(**row, source_id='OTHER') for row in agg['5m'])
    ctx = _intraday_market_context(SYMBOL, one, native, current=now)
    assert ctx['closed_bars']['5m'] == list(agg['5m'])[-21:]
    assert ctx['closed_bars']['15m'] == list(agg['15m'])[-21:]
    assert ctx['execution_data']['native_5m_comparison']['status'] == 'MATCH'
    revised = native[:-1] + (native[-1].model_copy(update={'close': 10.1}),)
    _, changed = execution_evidence(one, revised, as_of=now)
    assert changed['native_5m_comparison']['status'] == 'CONFLICT'
    assert changed['input_sha256'] == evidence['input_sha256']
    partial = _intraday_market_context(SYMBOL, one, revised, current=now, native_complete=False)
    assert partial['execution_data']['native_5m_comparison']['status'] == 'UNAVAILABLE'
    with pytest.raises(ValueError, match='FUTURE_BAR'):
        execution_evidence(one + (bar(now+timedelta(minutes=1)),), (), as_of=now)


@pytest.mark.parametrize('global_bad,current_bar,expected', [(False, True, 'FORCED_RISK_EXIT'),
    (False, False, 'DATA_BLOCK'), (True, True, 'DATA_BLOCK')])
def test_entry_gap_cannot_silence_valid_hard_stop(tmp_path, global_bad, current_bar, expected):
    store = RuntimeStore(tmp_path/'state.db')
    store.create_execution_plan('p', 'lane_1', SYMBOL, status=PlanStatus.ACTIVE_TODAY,
                                payload={'stop_level': 9.5})
    # This test checks signal generation only. T+1 settlement tests remain
    # separate; a stop event is never represented here as an actual sale.
    store.get_position = lambda *a: {'quantity': 100, 'sellable_quantity': 0}
    now = at('13:02:00')
    engine = MonitorEngine(store)
    result = engine.process_minute('lane_1', {SYMBOL: bar(now)} if current_bar else {},
        minute_snapshot_id='frozen', now=now, data_ok=not global_bad,
        data_errors={SYMBOL: 'MINUTE_DATA_GAP'})
    assert result.events[0].action == expected


def test_a5_includes_failed_jobs_and_alert_not_future(tmp_path):
    (tmp_path/'node').mkdir()
    rows = [{'id': i, 'timestamp': f'2026-09-15T{time}Z', 'stream': 'node', 'job': 'auction-refresh',
             'message': '任务结束 run-auction-refresh status=terminated'}
            for i, time in enumerate(['01:56:00', '09:00:00'])]
    (tmp_path/'node/node-2026-09-15.jsonl').write_text('\n'.join(json.dumps(r) for r in rows), encoding='utf-8')
    store = SimpleNamespace(list_notification_deliveries=lambda **k: [
        {'created_at': at('13:01:00').isoformat(), 'payload_json': json.dumps({'state':'BLOCKED','reasons':['CURRENT_SESSION_WINDOW_INVALID']})}])
    evidence = operational_evidence(store, tmp_path, cutoff=at('15:00:00'))
    assert [r['kind'] for r in evidence] == ['SOURCE_HEALTH_EVENT', 'JOB_TERMINATED']
    from liangjian_funnel.review.daily import _evidence_ids
    assert all(r['evidence_id'] in _evidence_ids({'operational_evidence': evidence}) for r in evidence)
