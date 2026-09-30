import pytest

from scripts.audit_frozen_a4_decisions import frozen_market_overlay, replay_clocks


def test_replay_uses_observed_closed_minute_separate_from_dispatch_clock():
    closed, dispatch, basis = replay_clocks(
        {'strategy': {'as_of': '2026-09-30T09:59:00+08:00',
                      'execution_data': {'market_cutoff': '2026-09-30T09:59:00+08:00'}}},
        '2026-09-30T10:00:00+08:00',
    )
    assert closed.minute == 59 and dispatch.minute == 0
    assert basis == 'STRATEGY_AS_OF'


def test_legacy_clock_does_not_invent_minus_one_minute():
    closed, dispatch, basis = replay_clocks({}, '2026-09-08T11:30:00+08:00')
    assert closed == dispatch and basis == 'LEGACY_EVENT_CLOCK'


@pytest.mark.parametrize('payload,stamp,reason', [
    ({'strategy': {'as_of': '2026-09-30T10:01:00+08:00'}}, '2026-09-30T10:00:00+08:00', 'FROZEN_CLOCK_INVALID'),
    ({'strategy': {'as_of': '2026-09-30T09:59:00+08:00', 'execution_data': {'market_cutoff': '2026-09-30T09:58:00+08:00'}}}, '2026-09-30T10:00:00+08:00', 'FROZEN_CUTOFF_CONFLICT'),
    ({'strategy': {'as_of': '2026-09-30T09:59:00'}}, '2026-09-30T10:00:00+08:00', 'FROZEN_CLOCK_TIMEZONE_MISSING'),
])
def test_replay_rejects_unknown_or_conflicting_clocks(payload, stamp, reason):
    with pytest.raises(ValueError, match=reason):
        replay_clocks(payload, stamp)


def test_frozen_gate_preserves_actual_permission_without_manufacturing_quote():
    context = {'live_market_state': {'decision': 'ALLOW'}}
    gate = {'state_status': 'READY', 'decision': 'CAUTION', 'as_of': '2026-09-30T10:00:00+08:00',
            'trade_date': '2026-09-30', 'suggested_position_cap_pct': 0.5}
    result = frozen_market_overlay(context, {'market_gate': gate})
    assert result['live_market_state']['decision'] == 'CAUTION'
    assert result['live_market_state']['as_of'] == gate['as_of']
    assert 'realtime_quote' not in result
    assert context['live_market_state']['decision'] == 'ALLOW'
