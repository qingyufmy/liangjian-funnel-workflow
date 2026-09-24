from datetime import datetime, timedelta
from types import SimpleNamespace

from liangjian_funnel.review.plan_replay import replay_plan_pool, model_projection, markdown_lines


NOW = datetime.fromisoformat('2026-09-24T09:33:00+08:00')


def plan(profile='TREND_MA5'):
    return {'plan_id': 'p', 'symbol': '000001.SZ', 'valid_from': NOW.replace(minute=31).isoformat(),
            'expires_at': NOW.replace(hour=15, minute=0).isoformat(),
            'payload_json': {'strategy_profile': profile, 'stock_behavior_type': 'TREND'}}


def bar(t):
    return SimpleNamespace(bar_end=t, model_dump=lambda **_: {'bar_end': t.isoformat(), 'close': 10})


def setup(monkeypatch, *, missing=False):
    import liangjian_funnel.review.plan_replay as replay
    import liangjian_funnel.workflow as workflow
    rows = [bar(NOW - timedelta(minutes=i)) for i in (2, 1, 0, -1) if not (missing and i == 1)]
    store = SimpleNamespace(latest_decision_snapshot=lambda *a, **k: {'bars': rows}, load_latest=lambda *a, **k: [])
    seen = []
    def evaluate(p, history, **kwargs):
        assert all(b.bar_end <= kwargs['now'] <= NOW for b in history)
        seen.append(kwargs['now'])
        return SimpleNamespace(model_dump=lambda **_: {'action': 'BUY_SIGNAL', 'reason_codes': []})
    monkeypatch.setattr(replay, 'evaluate_strategy', evaluate)
    monkeypatch.setattr(workflow, '_intraday_market_context', lambda *a, **k: {})
    monkeypatch.setattr(workflow, '_a4_execution_cutoff', lambda t: t)
    return store, seen


def test_all_windows_no_future_and_triggers_are_not_claimed_as_missed_trades(monkeypatch):
    store, seen = setup(monkeypatch)
    audit = replay_plan_pool([plan()], [], minute_store=store, cutoff=NOW)
    assert len(seen) == 3
    assert audit['plans'][0]['missing_decisions'] == 3
    assert audit['plans'][0]['unconfirmed_trigger_minutes'] == 3
    assert audit['confirmed_missed_trade_count'] is None
    assert audit['strategy_coverage']['LEADER_INTRADAY']['status'] == 'NO_PLAN_NOT_TESTED'
    projected = model_projection(audit)
    assert 'archived_inputs' not in projected
    assert sum(g['count'] for g in projected['trigger_groups']) == len(audit['trigger_windows'])
    assert '未验证' in '\n'.join(markdown_lines(audit))


def test_missing_minute_and_deadline_remain_data_limited(monkeypatch):
    store, _ = setup(monkeypatch, missing=True)
    result = replay_plan_pool([plan()], [], minute_store=store, cutoff=NOW)
    assert result['plans'][0]['missing_bars'] == 1
    assert result['plans'][0]['status'] == 'DATA_LIMITED'
    timed = replay_plan_pool([plan()], [], minute_store=store, cutoff=NOW, max_seconds=0)
    assert timed['plans'][0]['reason_code'] == 'REPLAY_DEADLINE_EXCEEDED'
    assert timed['plans'][0]['replayed_minutes'] == 0


def test_actual_entry_stops_entry_replay_and_invalidated_window_is_bounded(monkeypatch):
    store, seen = setup(monkeypatch)
    event = {'minute_end': NOW.replace(minute=31).isoformat(), 'action': 'BUY_SIGNAL',
             'effective': True, 'payload_json': {'plan_id': 'p'}}
    result = replay_plan_pool([plan()], [event], minute_store=store, cutoff=NOW)
    assert len(seen) == 1
    assert result['plans'][0]['unconfirmed_trigger_minutes'] == 0
    result = replay_plan_pool([plan()], [{**event, 'action': 'PLAN_INVALIDATED'}], minute_store=store, cutoff=NOW)
    assert result['plans'][0]['expected_minutes'] == 1


def test_unactivated_and_archive_absent_are_not_healthy():
    row = plan(); row['valid_from'] = None
    result = replay_plan_pool([row, plan('MA520_SWING')], [], minute_store=None, cutoff=NOW)
    assert [r['reason_code'] for r in result['plans']] == ['ACTIVATION_NOT_PROVEN', 'MINUTE_ARCHIVE_UNAVAILABLE']


def test_market_archive_future_is_not_used(monkeypatch, tmp_path):
    import json
    from liangjian_funnel.runtime.live_market import SCHEMA_VERSION
    store, _ = setup(monkeypatch)
    folder = tmp_path / NOW.date().isoformat()
    folder.mkdir()
    path = folder / '0930.json'
    state = {'schema_version': SCHEMA_VERSION, 'status': 'READY', 'as_of': NOW.isoformat()}
    path.write_text(json.dumps(state), encoding='utf-8')
    result = replay_plan_pool([plan()], [], minute_store=store, cutoff=NOW, market_state_dir=tmp_path)
    assert result['plans'][0]['context_unverified_minutes'] == 2
    assert result['plans'][0]['status'] == 'DATA_LIMITED'
    assert len(result['archived_market_states']) == 1
    assert 'archived_market_states' not in model_projection(result)


def test_real_closed_boundary_does_not_require_current_minute(monkeypatch):
    import liangjian_funnel.workflow as workflow
    cutoff_fn = workflow._a4_execution_cutoff
    store, seen = setup(monkeypatch)
    monkeypatch.setattr(workflow, '_a4_execution_cutoff', cutoff_fn)
    row = plan(); row['valid_from'] = NOW.replace(minute=32).isoformat()
    result = replay_plan_pool([row], [], minute_store=store, cutoff=NOW)
    assert seen == [NOW.replace(minute=31), NOW.replace(minute=32)]
    assert result['plans'][0]['missing_bars'] == 0
    assert cutoff_fn(NOW.replace(hour=13, minute=1)).strftime('%H:%M') == '11:30'
    assert cutoff_fn(NOW.replace(hour=15, minute=0)).strftime('%H:%M') == '14:59'


def test_recorded_buy_without_bar_still_stops_new_entry_replay(monkeypatch):
    store, seen = setup(monkeypatch, missing=True)
    row = plan(); row['valid_from'] = NOW.replace(minute=32).isoformat()
    event = {'minute_end': row['valid_from'], 'action': 'BUY_SIGNAL',
             'effective': True, 'payload_json': {'plan_id': 'p'}}
    result = replay_plan_pool([row], [event], minute_store=store, cutoff=NOW)
    assert not seen
    assert result['plans'][0]['missing_bars'] == 1
    assert result['plans'][0]['reasons']['POSITION_LIFECYCLE_NOT_REPLAYED'] == 1
