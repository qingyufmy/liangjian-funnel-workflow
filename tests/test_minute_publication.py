from datetime import datetime, timedelta
from types import SimpleNamespace
import json

import pytest

from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
from liangjian_funnel.data.session_windows import TZ, closed_window_ends
from liangjian_funnel.data.execution_evidence import execution_evidence
from liangjian_funnel.data.publication import (
    confirm_publications, classify_persistent_pending, reuse_publication,
)
from liangjian_funnel.runtime.monitor import MonitorEngine
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
from liangjian_funnel.review.engineering import operational_evidence

AT = datetime(2026, 9, 16, 9, 40, tzinfo=TZ)
SYMBOL = '002886.SZ'


def pack(close=10, native='match', complete=True):
    bars = tuple(MinuteBar(symbol=SYMBOL, interval='1m', bar_end=t, open=10,
        high=12, low=9, close=close, volume=100, amount=1000, source_id='TEST',
        volume_unit='shares', normalizer_version='test/1') for t in closed_window_ends(AT, '1m'))
    aggregate, _ = execution_evidence(bars, (), as_of=AT)
    five = tuple(MinuteBar(**row, source_id='TEST') for row in aggregate['5m'])
    if native == 'conflict':
        five = five[:-1] + (five[-1].model_copy(update={'close': 11.5}),)
    def result(interval, rows, valid):
        return FetchResult(symbol=SYMBOL, interval=interval, requested_bars=len(rows),
            returned_bars=len(rows), bars=rows, complete=valid,
            reason_code='OK' if valid else 'TENCENT_REQUEST_FAILED',
            request_started_at=AT+timedelta(seconds=1), response_received_at=AT+timedelta(seconds=2))
    return {'1m': result('1m', bars, complete), '5m': result('5m', five, native != 'missing')}


class Clock:
    def __init__(self): self.value = 3.0
    def now(self): return self.value
    def wall(self): return AT+timedelta(seconds=self.value)
    def sleep(self, delay): self.value += delay


def run(initial, replies, *, deadline=33., latency=0.):
    clock = Clock()
    calls = []
    def fetch(symbol):
        calls.append(clock.now())
        clock.value += latency
        result = replies[len(calls)-1]
        if isinstance(result, Exception): raise result
        return symbol, result
    result = confirm_publications({SYMBOL: initial}, fetch, at=AT, deadline=deadline,
        clock=clock.now, wall_clock=clock.wall, sleep=clock.sleep, workers=1)[SYMBOL]
    return result, calls, clock


@pytest.mark.parametrize('native', ['match', 'missing'])
def test_stability_requires_two_late_probes_but_auxiliary_missing_is_not_execution_failure(native):
    result, calls, _ = run(pack(native=native), [pack(native=native), pack(native=native)])
    assert calls == [20., 25.]
    assert result['publication']['state'] == 'OBSERVED_STABLE'
    assert not result.get('publication_error')
    assert result['publication']['exchange_finality_proven'] is False
    assert len(result['publication']['attempts']) == 3
    source = result['publication']['attempts'][-1]['sources']['1m']
    assert source['source_ids'] == ['TEST'] and source['volume_units'] == ['shares']
    assert source['hash'] and source['response_received_at']


def test_early_revision_can_converge_and_late_revision_cannot_trade():
    result, _, _ = run(pack(), [pack(10.2), pack(10.2)])
    assert result['publication']['state'] == 'OBSERVED_STABLE'
    result, _, _ = run(pack(), [pack(), pack(10.2)])
    assert result['publication_error'] == 'MINUTE_PUBLICATION_PENDING'
    assert result['1m'].bars[-1].close == 10.2


@pytest.mark.parametrize('last,expected', [
    ('match', 'OBSERVED_STABLE'), ('conflict', 'CONFLICT'), ('missing', 'CONFLICT')])
def test_conflict_requires_positive_resolution_not_loss_of_auxiliary_source(last, expected):
    result, _, _ = run(pack(native='conflict'), [pack(native='conflict'), pack(native=last)])
    assert result['publication']['state'] == expected


def test_old_auction_scope_limitation_does_not_hide_current_conflict_resolution():
    def tencent(native):
        result = pack(native=native)
        for key, value in result.items():
            result[key] = value.model_copy(update={'bars': tuple(
                bar.model_copy(update={'source_id': 'TENCENT:ifzq.gtimg.cn'}) for bar in value.bars)})
        return result
    result, _, _ = run(tencent('conflict'), [tencent('match'), tencent('match')])
    assert result['publication']['attempts'][-1]['comparison']['status'] == 'DATA_LIMITED'
    assert result['publication']['state'] == 'OBSERVED_STABLE'
    assert result['publication']['attempts'][-1]['unresolved_prior_conflicts'] == []


def test_slow_first_sweep_cannot_compress_independent_probe_spacing():
    result, calls, _ = run(pack(), [pack(), pack()], deadline=33, latency=4)
    assert calls == [20, 29]  # Five seconds after first response, not after request.
    assert result['publication']['state'] == 'OBSERVED_STABLE'


@pytest.mark.parametrize('deadline,latency', [(24, 0), (25.1, 1)])
def test_budget_exhaustion_cannot_certify_early_or_late_result(deadline, latency):
    result, calls, clock = run(pack(), [pack(), pack()], deadline=deadline, latency=latency)
    assert result['publication_error'] == 'MINUTE_PUBLICATION_PENDING'
    assert len(calls) <= 2
    if not latency: assert clock.now() <= deadline


@pytest.mark.parametrize('failure', [pack(complete=False), RuntimeError('do not expose')])
def test_retry_raw_failure_not_hidden_or_replaced_by_earlier_success(failure):
    result, _, _ = run(pack(), [pack(), failure])
    assert result['publication']['state'] == 'INPUT_UNAVAILABLE'
    assert result['publication_error'] in {'TENCENT_REQUEST_FAILED', 'MINUTE_DATA_FETCH_FAILED'}


def test_initial_failure_not_retried_by_confirmation_layer():
    result, calls, _ = run(pack(complete=False), [])
    assert calls == [] and result['publication_error'] == 'TENCENT_REQUEST_FAILED'


def test_pending_escalates_only_consecutive_minutes_and_recovers():
    result, _, _ = run(pack(), [pack(), pack(10.2)])
    first = classify_persistent_pending(result, {}, at=AT)
    second = classify_persistent_pending(result, first['publication'], at=AT+timedelta(minutes=1))
    assert first['publication_error'] == 'MINUTE_PUBLICATION_PENDING'
    assert second['publication_error'] == 'MINUTE_PUBLICATION_UNCONFIRMED'
    assert classify_persistent_pending(result, first['publication'], at=AT+timedelta(minutes=91))['publication']['pending_minutes'] == 1
    ready, _, _ = run(pack(), [pack(), pack()])
    assert classify_persistent_pending(ready, second['publication'], at=AT+timedelta(minutes=2))['publication']['pending_minutes'] == 0


def test_same_decision_cannot_approve_revision_after_restart():
    result, _, _ = run(pack(), [pack(), pack()])
    original = {**result['publication'], 'decision_error': None}
    assert not reuse_publication(pack(), original, at=AT).get('publication_error')
    assert reuse_publication(pack(10.2), original, at=AT)['publication_error'] == 'MINUTE_PUBLICATION_REPLAY_MISMATCH'


def test_one_failed_symbol_does_not_block_other_symbols():
    clock = Clock()
    initial = {'good': pack(), 'bad': pack(complete=False)}
    result = confirm_publications(initial, lambda symbol: (symbol, initial[symbol]),
        at=AT, deadline=33, clock=clock.now, wall_clock=clock.wall, sleep=clock.sleep)
    assert result['good']['publication']['state'] == 'OBSERVED_STABLE'
    assert result['bad']['publication']['state'] == 'INPUT_UNAVAILABLE'


def test_all_blocked_minutes_retained_without_trade_lifecycle_or_duplicate_retry(tmp_path):
    store = RuntimeStore(tmp_path/'state.db')
    store.create_execution_plan('p', 'lane_1', SYMBOL, status=PlanStatus.ACTIVE_TODAY,
        valid_from=AT-timedelta(minutes=1), expires_at=AT+timedelta(hours=5), payload={})
    engine = MonitorEngine(store)
    for offset in (0, 1, 1, 5):
        now = AT+timedelta(minutes=offset)
        engine.process_minute('lane_1', {}, minute_snapshot_id=str(offset), now=now,
            data_errors={SYMBOL: 'EXECUTION_NATIVE_5M_CONFLICT'},
            market_contexts={SYMBOL: {'execution_data': {'publication': {'state': 'CONFLICT'}}}})
    events = store.list_monitor_events(lane_id='lane_1')
    blocks = [e for e in events if e['action'] == 'DATA_BLOCK']
    assert len(blocks) == 3 and not any(e['effective'] for e in blocks)
    assert all(json.loads(e['payload_json'])['strategy']['execution_data']['publication']['state'] == 'CONFLICT' for e in blocks)
    assert not store.list_monitor_events(effective_only=True)


def test_aggregate_alert_no_shrink_spam_no_false_recovery_and_expansion_not_lost():
    ledger = []
    app = SimpleNamespace(store=SimpleNamespace(list_notification_deliveries=lambda **k: ledger[-1:]))
    def send(**kwargs):
        ledger.append({'status': 'SENT', 'payload_json': json.dumps(kwargs['summary'])})
        return kwargs
    app._send = send
    emit = WorkflowLarkPublisher.publish_minute_source_health
    assert emit(app, {}, now=AT, pending_failures={SYMBOL: 'MINUTE_PUBLICATION_PENDING'}) == []
    fail = {SYMBOL: 'EXECUTION_NATIVE_5M_CONFLICT', '301297.SZ': 'EXECUTION_NATIVE_5M_CONFLICT'}
    assert len(emit(app, fail, now=AT)) == 1
    assert emit(app, {SYMBOL: fail[SYMBOL]}, now=AT+timedelta(minutes=1)) == []
    assert emit(app, {}, now=AT+timedelta(minutes=2), pending_failures={SYMBOL: 'MINUTE_PUBLICATION_PENDING'}) == []
    assert len(emit(app, {**fail, '600519.SH': fail[SYMBOL]}, now=AT+timedelta(minutes=3))) == 1
    assert len(emit(app, {}, now=AT+timedelta(minutes=4))) == 1
    assert emit(app, {}, now=AT+timedelta(minutes=5)) == []


def test_a5_sees_quiet_waits_without_future_minutes(tmp_path):
    directory = tmp_path/'monitor'/'data_quality'/AT.date().isoformat()
    directory.mkdir(parents=True)
    for delta in (0, 1):
        data = {'market_cutoff': (AT+timedelta(minutes=delta)).isoformat(), 'symbols': {
            SYMBOL: {'state': 'PENDING_PUBLICATION', 'decision_error': 'MINUTE_PUBLICATION_PENDING'}}}
        (directory/f'{delta}.json').write_text(json.dumps(data), encoding='utf-8')
    store = SimpleNamespace(list_notification_deliveries=lambda **k: [])
    rows = operational_evidence(store, tmp_path, cutoff=AT)
    assert rows[0]['minute_count'] == 1
    assert rows[0]['state_counts'] == {'PENDING_PUBLICATION': 1}


def test_data_notice_and_recovery_are_not_order_outcome_messages():
    ledger = []
    app = SimpleNamespace(store=SimpleNamespace(list_notification_deliveries=lambda **k: ledger[-1:]))
    def send(**kwargs):
        ledger.append({'status': 'SENT', 'payload_json': json.dumps(kwargs['summary'])})
        return kwargs
    app._send = send
    emit = WorkflowLarkPublisher.publish_minute_source_health
    notice = emit(app, {SYMBOL: 'EXECUTION_NATIVE_5M_CONFLICT'}, now=AT)[0]
    assert notice['title'] == 'A4行情校验未通过｜暂停新增仓'
    assert '不是买入信号或委托失败回报' in '\n'.join(notice['lines'])
    assert notice['summary']['event_category'] == 'MARKET_DATA_QUALITY'
    assert notice['summary']['order_outcome'] == 'NOT_AN_ORDER_RESULT'
    restored = emit(app, {}, now=AT+timedelta(minutes=1))[0]
    assert '不代表买点成立或委托成交' in '\n'.join(restored['lines'])
