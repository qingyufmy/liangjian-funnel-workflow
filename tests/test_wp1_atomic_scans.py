from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json

import pytest

from test_wp1_ablation_engine import fixture, run, NOW
from liangjian_funnel.evaluation.ablation import conditions, runner
from liangjian_funnel.evaluation.ablation.statistics import summarize
from liangjian_funnel.evaluation.ablation.engine import evaluate_window
from liangjian_funnel.runtime import strategies


def atomic_failures(decision, **kwargs):
    if hasattr(conditions, 'atomic_failures'):
        return conditions.atomic_failures(decision, **kwargs)
    # Reproduce the prior runner's projection instead of failing at import.
    failed = list(decision.get('unmet_conditions', []))
    if 'A4_LIVE_NO_CHASE_EXCEEDED' in decision.get('reason_codes', []):
        failed.append('A4_LIVE_NO_CHASE_ACCEPTED')
    return {'failed_conditions':failed, 'unresolved_parents':[]}


def scan_scenarios():
    return runner.scan_scenarios() if hasattr(runner, 'scan_scenarios') else []


def phases(values):
    plan, _, context = fixture()
    plan['trend_entry_rule_version'] = 'trend-ma5/2'
    plan['volume_overheated'] = False
    bars = []
    for group, (opening, close, low, volume) in enumerate(values):
        for minute in range(5):
            bars.append(dict(symbol=plan['symbol'],
                bar_end=NOW-timedelta(minutes=len(values)*5-1-group*5-minute),
                open=opening, close=close, low=low, high=max(opening, close)+.01,
                volume=volume/5, amount=close*volume/5))
    return plan, bars, context


def test_geometry_parent_does_not_duplicate_atomic_no_chase():
    decision = {'unmet_conditions':['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'],
                'reason_codes':['A4_LIVE_NO_CHASE_EXCEEDED']}
    projection = atomic_failures(decision)
    assert projection['failed_conditions'] == ['A4_LIVE_NO_CHASE_ACCEPTED']
    assert projection['unresolved_parents'] == []
    row = dict(profile='TREND_MA5', scenario='BASELINE', plan_id='p', minute='m',
               action='START_CONFIRMATION', failed_conditions=projection['failed_conditions'],
               attribution=projection)
    rate = summarize([row])['condition_kill_rates'][0]
    assert rate['condition'] == 'A4_LIVE_NO_CHASE_ACCEPTED'
    assert rate['kill_rate'] == 1
    assert rate['attribution_version'] == 'ATOMIC_UNMET/2'


def test_geometry_expands_all_actual_children_and_keeps_unknown_parent():
    result = atomic_failures({'unmet_conditions':['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'],
        'reason_codes':['A4_LIVE_NO_CHASE_EXCEEDED','A4_LIVE_REWARD_RISK_BELOW_MINIMUM']})
    assert set(result['failed_conditions']) == {'A4_LIVE_NO_CHASE_ACCEPTED','A4_LIVE_REWARD_RISK_ACCEPTED'}
    unknown = atomic_failures({'unmet_conditions':['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED']})
    assert unknown['unresolved_parents'] == ['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED']
    result = atomic_failures({'unmet_conditions':['TREND_5M_REVERSAL_CONFIRMATION',
        'TREND_PRIOR_5M_REVERSAL']}, versioned_trend=True)
    assert result['failed_conditions'] == ['TREND_PRIOR_5M_REVERSAL']


def test_explicit_scan_matrix_separates_equivalent_n_and_research_kn():
    names = dict(scan_scenarios())
    assert names['TREND_PULLBACK_LEN_K:2'] == {'research_sequence_model':'TREND_PULLBACK_LEN_K','pullback_length':2}
    assert names['TREND_CONFIRM_WITHIN_N:6'] == {'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6}
    assert names['MA5_ATR:0.5']['atr_multiplier'] == .5
    assert names['REALTIME_MA5_SHIFT']['zone_model'] == 'REALTIME_MA5_SHIFT'
    assert names['EQUIVALENT_LATEST4:N8']['sequence_window'] == 8
    assert len(names) == 37
    assert names['CROSS:MA5_ATR:0.5:TREND_CONFIRM_WITHIN_N:6'] == {
        'zone_model':'MA5_ATR','atr_multiplier':.5,
        'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6}


def test_k2_contracts_consecutive_pullbacks_and_keeps_latest_confirmation():
    plan, bars, context = phases([(10.4,10.3,10.2,300),(10.3,10.2,10.1,200),
        (10.2,10.1,10,100),(10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.3,10.15,160)])
    # Move successful k2 pattern to latest: the earlier distraction is not selectable.
    bars = bars[-25:]
    result = run(plan, bars, context, variant={'research_sequence_model':'TREND_PULLBACK_LEN_K','pullback_length':2})
    assert result['ablation']['status'] == 'OK'
    assert result['ablation']['sequence_confirmation_anchor'] == 'LATEST_CLOSED_5M'
    assert result['action'] != 'BUY_SIGNAL'
    trace = result['ablation']['sequence_evidence']
    assert len(trace['candidates']) == 1
    assert len(trace['candidates'][0]['phase_ends']) == 5
    assert trace['candidates'][0]['phase_ends'][-1] == NOW.isoformat()


@pytest.mark.parametrize('k',[1,2,3])
def test_k_research_recalculates_each_phase_and_does_not_mutate_globals(k):
    prefix = [(10.5,10.4,10.3,400)]
    pullbacks = [(10.4-i*.1,10.3-i*.1,10.2-i*.1,300-i*50) for i in range(k)]
    last = pullbacks[-1]
    reversal = (last[1],last[1]+.1,last[2]+.05,220)
    confirm = (reversal[1],reversal[1]+.2,reversal[2]+.05,260)
    values = prefix+pullbacks+[reversal,confirm]
    values = [(10.6,10.5,10.4,450)]*(8-len(values))+values
    plan, bars, context = phases(values)
    before = deepcopy((plan,bars,context))
    result = run(plan,bars,context,variant={'research_sequence_model':'TREND_PULLBACK_LEN_K','pullback_length':k})
    assert result['ablation']['status'] == 'OK'
    assert all(result['ablation']['sequence_evidence']['candidates'][0]['conditions'].values())
    assert (plan,bars,context) == before


@pytest.mark.parametrize('violation',[None,'LOW','ZONE','GAP'])
def test_within_n_can_use_earlier_confirmation_but_not_break_or_gap(violation):
    plan,bars,context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    if violation == 'LOW': bars[-1]['low'] = 10.04
    if violation == 'ZONE': plan['trigger_zone'] = {'low':10,'high':10.32}
    if violation == 'GAP': del bars[10]
    result = run(plan,bars,context,variant={'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6})
    if violation == 'GAP':
        assert result['ablation']['status'] == 'DATA_LIMITED'
    else:
        assert result['ablation']['status'] == 'OK'
        trace = result['ablation']['sequence_evidence']
        if violation is None:
            assert trace['selected_candidate']['confirmation_end'] == (NOW-timedelta(minutes=10)).isoformat()
            assert result['action'] == 'BUY_SIGNAL'
        else:
            assert not trace['matched']
            assert result['action'] != 'BUY_SIGNAL'


def test_new_scans_keep_future_and_market_data_protection():
    plan,bars,context = phases([(10.4,10.3,10.2,200)]*8)
    bars.append({**bars[-1], 'bar_end':NOW+timedelta(minutes=1)})
    result = run(plan,bars,context,variant={'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':8})
    assert result['action'] == 'DATA_BLOCK'
    assert result['ablation']['status'] == 'DATA_LIMITED'


def test_k_checks_every_contraction_not_just_first_and_last():
    plan,bars,context = phases([(10.6,10.5,10.4,500),(10.5,10.4,10.3,300),
        (10.4,10.3,10.2,350),(10.3,10.2,10.1,100),
        (10.2,10.3,10.15,200),(10.3,10.5,10.2,250)])
    result = run(plan,bars,context,variant={'research_sequence_model':'TREND_PULLBACK_LEN_K','pullback_length':3})
    assert result['ablation']['sequence_evidence']['selected_candidate']['conditions']['TREND_PULLBACK_VOLUME_CONTRACTION'] is False
    assert result['action'] != 'BUY_SIGNAL'


def test_after_latest_5m_tail_gap_and_intraminute_break_are_not_ignored():
    plan,bars,context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    for i in (1,3): bars.append({**bars[-1], 'bar_end':NOW+timedelta(minutes=i)})
    result = evaluate_window(
        plan,bars,now=NOW+timedelta(minutes=3),decision_time=NOW+timedelta(minutes=3),
        market_context=context,variant={'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6})
    assert result['ablation']['status'] == 'DATA_LIMITED'
    assert result['ablation']['sequence_scan_model'] == 'TREND_CONFIRM_WITHIN_N'
    assert result['ablation']['sequence_candidate_count'] == 0
    assert result['ablation']['sequence_evidence']['matched'] is None


@pytest.mark.parametrize('protection',['STOP','LOCK','MARKET'])
def test_research_scan_preserves_protective_gates_and_no_short_circuit_pass(protection):
    plan,bars,context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    if protection == 'STOP':
        plan['position'] = {'total_qty':100,'sellable_qty':0,'avg_cost':10}
        plan['invalidation_level'] = 10.5
    if protection == 'LOCK': plan['locked_limit_up'] = True
    if protection == 'MARKET': context.clear()
    result = run(plan,bars,context,variant={'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6})
    assert result['action'] != 'BUY_SIGNAL'
    if protection == 'STOP':
        assert result['action'] == 'FORCED_RISK_EXIT'
        assert 'BLOCKED_T1' in result['reason_codes']
        assert result['ablation']['sequence_evidence']['evidence_scope'] == 'NOT_REACHED'
    if protection == 'MARKET': assert result['ablation']['status'] == 'DATA_LIMITED'


def test_concurrent_new_scans_have_call_local_evidence_and_unchanged_production():
    plan,bars,context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    original = dict(vars(strategies))
    variants = [{'research_sequence_model':'TREND_PULLBACK_LEN_K','pullback_length':k} for k in (1,2,3)] + [
        {'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6}]
    sequential = [run(plan,bars,context,variant=variant) for variant in variants]
    with ThreadPoolExecutor(max_workers=4) as pool:
        concurrent = list(pool.map(lambda variant:run(plan,bars,context,variant=variant),variants))
    assert concurrent == sequential
    assert all(vars(strategies)[key] is value for key,value in original.items())


def test_statistics_versions_and_unresolved_parent_do_not_mix_unique_denominators():
    base = dict(profile='TREND_MA5',scenario='BASELINE',plan_id='p',minute='m',
                action='START_CONFIRMATION',failed_conditions=['A4_LIVE_NO_CHASE_ACCEPTED'])
    old = dict(base)
    current = {**base,'attribution':{'version':'ATOMIC_UNMET/2','unresolved_parents':[]}}
    unknown = {**current,'plan_id':'q','failed_conditions':['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'],
               'attribution':{'version':'ATOMIC_UNMET/2','unresolved_parents':['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED']}}
    report = summarize([old,current,unknown])
    assert len(report['condition_kill_rates']) == 2
    assert all(rate['denominator'] == 1 for rate in report['condition_kill_rates'])
    assert report['unresolved_atomic_attribution'][0]['window_count'] == 1
    assert {group['attribution_version'] for group in report['groups']} == {'LEGACY_UNVERSIONED','ATOMIC_UNMET/2'}


def test_runner_writes_explicit_matrix_and_atomic_parent_projection(tmp_path,monkeypatch):
    from test_wp1_ablation_runner import dataset
    plan,bars,context = fixture()
    plan['no_chase'] = 10.1
    data = dataset()
    baseline = run(plan,bars,context)
    data['plans'][0]['payload'] = plan
    data['windows'][0].update(bars=bars,market_context=context,
        recorded_action=baseline['action'],recorded_reason=baseline['reason_codes'][0])
    monkeypatch.setattr('liangjian_funnel.evaluation.ablation.outcomes.evaluate_outcome',
        lambda *args,**kwargs:{'fill_status':'INSUFFICIENT_EVIDENCE','return_kind':'RESEARCH_COUNTERFACTUAL'})
    report = runner.run_experiment([data],output=tmp_path/'atomic-v2',scans=True)
    rows = json.loads((tmp_path/'atomic-v2'/'rows.json').read_text())
    assert report['schema_version'] == 'liangjian-ablation-report/2'
    assert report['attribution_version'] == 'ATOMIC_UNMET/2'
    assert any(path.endswith('conditions.py') for path in report['implementation_source_sha256'])
    assert len(report['parameter_scan_matrix']) == 37
    baseline_row = next(row for row in rows if row['scenario'] == 'BASELINE')
    assert baseline_row['failed_conditions'] == ['A4_LIVE_NO_CHASE_ACCEPTED']
    assert len([row for row in rows if row['scenario'].startswith('SCAN:')]) == 37
    assert all(group['win_rate'] is None for group in report['statistics']['groups'])


@pytest.mark.parametrize('zone',['CURRENT','MA5_ATR','REALTIME_MA5_SHIFT'])
def test_cross_scan_recalculates_joint_zone_sequence_not_a_fabricated_buy(zone):
    plan,bars,context = phases([(10.4,10.3,10.2,200),(10.3,10.1,10,100),
        (10.1,10.2,10.05,120),(10.2,10.4,10.1,180),
        (10.4,10.35,10.15,150),(10.35,10.3,10.1,140)])
    plan['trigger_zone'] = {'low':9,'high':9.5}
    variant = {'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6,
               'zone_model':zone}
    if zone == 'MA5_ATR': variant['atr_multiplier'] = .5
    if zone == 'REALTIME_MA5_SHIFT': plan['daily_indicators']['previous_daily_closes'] = [10.3]*4
    snapshot = deepcopy(plan)
    result = run(plan,bars,context,variant=variant)
    assert result['ablation']['status'] == 'OK'
    assert result['action'] == ('START_CONFIRMATION' if zone == 'CURRENT' else 'BUY_SIGNAL')
    assert plan == snapshot


@pytest.mark.parametrize('zone',['MA5_ATR','REALTIME_MA5_SHIFT'])
def test_cross_scan_missing_price_evidence_is_data_limited(zone):
    plan,bars,context = phases([(10.4,10.3,10.2,200)]*6)
    plan['daily_indicators'] = {'ma5':10.5}
    variant = {'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6,
               'zone_model':zone}
    if zone == 'MA5_ATR': variant['atr_multiplier'] = .5
    result = run(plan,bars,context,variant=variant)
    assert result['ablation']['status'] == 'DATA_LIMITED'
    assert result['ablation']['sequence_evidence']['matched'] is None


def test_scan_summary_does_not_claim_first_unknown_window_is_all_executed_evidence():
    row = dict(profile='TREND_MA5',scenario='SCAN:TREND_CONFIRM_WITHIN_N:6',
        plan_id='p',minute='m',action='START_CONFIRMATION',
        variant={'research_sequence_model':'TREND_CONFIRM_WITHIN_N','confirmation_window':6},
        ablation={'status':'DATA_LIMITED','sequence_scan_model':'TREND_CONFIRM_WITHIN_N',
                  'sequence_candidate_count':0,'sequence_evidence_scope':'NOT_REACHED'})
    executed = {**row,'minute':'n','ablation':{**row['ablation'],'status':'OK',
        'sequence_candidate_count':3,'sequence_evidence_scope':'ACTUAL_ISOLATED_STRATEGY_HELPER_CALL'}}
    group = summarize([row,executed])['groups'][0]
    assert group['sequence_helper_executed_window_count'] == 1
    assert group['sequence_actual_candidate_count_distribution'] == {'3':1}
    assert 'sequence_candidate_count' not in group['scan_contract']
