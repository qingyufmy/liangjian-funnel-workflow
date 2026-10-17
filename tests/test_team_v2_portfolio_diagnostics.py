from copy import deepcopy

from liangjian_funnel.review.portfolio_diagnostics import strategy_portfolio_diagnostics


def source():
    return dict(trade_date='2026-10-16',a3={'plans':[
        dict(plan_id='p1',strategy_profile='TREND_MA5',status='EXPIRED'),
        dict(plan_id='p2',strategy_profile='MA520_SWING',status='INVALIDATED')]},
        a4={'events':[dict(evidence_id='e1',plan_id='p1',symbol='000001.SZ',minute_end='2026-10-16T10:00:00+08:00',
            strategy_profile='TREND_MA5',reason_code='TREND_15M_PRESSURE_NOT_EASING',strategy_reason_codes=['MARKET_RISK_OFF'])],
            'observation_groups':[dict(observation_count=999,reason_code='A4_LIVE_MARKET_BLOCK_WAIT')]})


def test_prior_regime_context_and_waiting_groups_are_not_counted_as_vetoes():
    facts=source();before=deepcopy(facts);r=strategy_portfolio_diagnostics(facts)
    assert r['strategy_profile_counts']=={'MA520_SWING':1,'TREND_MA5':1}
    assert sum(r['regime_entry_block_events_by_profile'].values())==0
    assert r['selected_board_next_session_rank_lag']['value'] is None and facts==before


def test_explicit_block_dedup_and_symbol_minute_are_separate_denominators():
    facts=source();row=facts['a4']['events'][0];row['reason_code']='A4_LIVE_MARKET_BLOCK_WAIT'
    facts['a4']['events'].append(deepcopy(row));other=deepcopy(row);other['evidence_id']='e2';facts['a4']['events'].append(other)
    r=strategy_portfolio_diagnostics(facts)
    assert r['regime_entry_block_events_by_profile']['TREND_MA5']==2
    assert r['regime_entry_block_symbol_minutes_by_profile']['TREND_MA5']==1


def test_unknown_profile_or_conflicting_event_is_not_silently_bound_to_a_strategy():
    facts=source();row=facts['a4']['events'][0];row['reason_code']='A4_LIVE_MARKET_BLOCK_WAIT'
    conflict=deepcopy(row);conflict['strategy_profile']='LEADER_INTRADAY';facts['a4']['events'].append(conflict)
    r=strategy_portfolio_diagnostics(facts)
    assert r['status']=='DATA_LIMITED' and r['conflicting_event_ids']==1
    assert sum(r['regime_entry_block_events_by_profile'].values())==0
    facts['a4']['events']=[conflict];assert strategy_portfolio_diagnostics(facts)['unbound_regime_block_events']==1


def test_absent_source_is_unknown_not_a_zero_twenty_day_history():
    r=strategy_portfolio_diagnostics({})
    assert r['status']=='UNAVAILABLE' and r['strategy_profile_counts'] is None


def test_malformed_frozen_profile_or_reason_is_not_a_crash_or_a_valid_veto():
    facts=source();facts['a3']['plans'][0]['strategy_profile']=['TREND_MA5']
    facts['a4']['events'][0]['strategy_reason_codes']=[{}]
    r=strategy_portfolio_diagnostics(facts)
    assert r['status']=='DATA_LIMITED' and r['strategy_profile_counts']['UNKNOWN']==1
    assert sum(r['regime_entry_block_events_by_profile'].values())==0
    assert 'EVENT_REASON_CODES_INVALID' in r['warnings']
