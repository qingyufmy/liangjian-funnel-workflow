import copy
import json
from datetime import datetime

from liangjian_funnel.review.observability import (
    condition_observations, timing_observations, observability_markdown,
)


CUTOFF = datetime.fromisoformat('2026-10-15T11:30:00+08:00')


def plan(identity='p', **kwargs):
    return {'plan_id': identity, 'symbol': '600001.SH',
            'valid_from': '2026-10-15T09:32:00+08:00',
            'expires_at': '2026-10-15T15:00:00+08:00',
            'payload_json': {'cluster_label': '航运', 'strategy_profile': 'TEST'}, **kwargs}


def event(at='10:01', identity='e', conditions=None, **kwargs):
    return {'event_id': identity, 'minute_end': f'2026-10-15T{at}:00+08:00',
            'action': 'NO_ACTION', 'reason_code': 'FIRST_REASON_IS_NOT_ALL_CONDITIONS',
            'payload_json': {'plan_id': 'p', 'strategy': {'confirmation_results': conditions or {}}}, **kwargs}


def test_counts_only_explicit_available_frozen_conditions_and_preserves_unknown():
    conditions = {'pressure': {'met': True, 'available': True, 'kind': 'CONDITION'},
                  'warmup': {'met': True, 'available': False, 'kind': 'CONDITION'},
                  'unknown': {'met': True, 'kind': 'CONDITION'},
                  'reverse': {'met': False, 'available': True, 'kind': 'CONDITION'},
                  'risk': {'met': False, 'available': True, 'kind': 'VETO'}}
    rows = [event(conditions=conditions), event('10:02', 'later', conditions=conditions),
            event('13:01', 'future', conditions={'fake': {'met': True, 'available': True}})]
    original = copy.deepcopy(rows)
    result = condition_observations([plan()], rows, cutoff=CUTOFF)['plans'][0]
    assert result['max_conditions_met'] == 1
    assert result['max_conditions_met_at'] == rows[0]['minute_end']
    assert result['failing_conditions_at_max'] == ['reverse', 'risk', 'unknown', 'warmup']
    assert result['unavailable_conditions_at_max'] == ['unknown', 'warmup']
    assert result['condition_scope'] == 'OBSERVED_FROZEN_CONFIRMATIONS_ONLY_NOT_ALL_STRATEGY_GATES'
    assert result['confirmation_evidence_id'] == 'A4:EVENT:e'
    assert len(result['confirmation_sha256']) == 64
    assert rows == original


def test_missing_confirmations_first_reason_or_unactivated_never_proves_zero_or_pass():
    result = condition_observations([plan(), plan('not_active', valid_from=None)], [event()], cutoff=CUTOFF)
    assert len(result['plans']) == 1
    assert result['plans'][0]['max_conditions_met'] is None
    assert result['plans'][0]['max_conditions_met_at'] is None
    assert result['plans'][0]['status'] == 'DATA_LIMITED'
    assert result['activated_plan_count'] == 1


def test_out_of_lifetime_data_blocks_and_post_entry_are_not_entry_progress():
    good = {'a': {'met': True, 'available': True, 'kind': 'CONDITION'}}
    events = [event('09:31', conditions=good), event('09:33', conditions=good, action='DATA_BLOCK'),
              event('09:34', conditions=good, action='BUY_SIGNAL', effective=True),
              event('09:35', conditions={**good, 'b': good['a']})]
    row = condition_observations([plan()], events, cutoff=CUTOFF)['plans'][0]
    assert row['max_conditions_met_at'].endswith('09:34:00+08:00')
    assert row['max_conditions_met'] == 1


def test_cluster_counts_and_factor_design_are_not_a3_empty():
    rows = [plan(), plan('q'), plan('r', payload_json={'cluster_label': '医药'})]
    result = condition_observations(rows, [], cutoff=CUTOFF)
    assert result['cluster_counts'] == {'医药': 1, '航运': 2}
    assert result['distinct_clusters'] == 2
    assert [p['cluster_count'] for p in result['plans']] == [2, 2, 1]
    assert result['factor_ready_count_status'] == 'DISABLED_BY_DESIGN_NOT_A3_COUNT'
    assert result['session_plan_count'] == 3


def receipt(directory, run, at, *, elapsed=40000, fetch=26000, finalized=True):
    payload = {'run_id': run, 'observed_at': at, 'elapsed_ms': elapsed,
               'trace_status': 'FINALIZED' if finalized else 'RUNNING',
               'phase_timings': {k: {'duration_ms': fetch if k == 'fetch' else 1000, 'calls': 1}
                                 for k in ('startup', 'fetch', 'judge', 'persist', 'notify')}}
    path = directory / f'{run}.receipt.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    return payload


def test_finalized_timings_have_exact_percentiles_and_budget_warnings(tmp_path):
    receipt(tmp_path, 'monitor-a', '2026-10-15T10:01:00+08:00', elapsed=45001, fetch=26000)
    receipt(tmp_path, 'monitor-b', '2026-10-15T10:02:00+08:00', fetch=24000)
    result = timing_observations(tmp_path, cutoff=CUTOFF)
    assert result['minute_count'] == 2
    assert result['phases']['fetch'] == {'sample_count': 2, 'p50_ms': 24000.0, 'p95_ms': 26000.0, 'max_ms': 26000.0}
    assert result['budget_warnings'] == ['FETCH_P95_OVER_25S', 'MONITOR_MINUTE_OVER_45S']
    assert len(result['over_30s_minutes']) == 2
    assert result['over_30s_minutes'][0]['phase_timings']['fetch'] == 26000.0
    assert '预算预警' in '\n'.join(observability_markdown({'timings': result}))


def test_timing_excludes_future_wrong_day_unfinished_and_rejects_invalid(tmp_path):
    receipt(tmp_path, 'monitor-good', '2026-10-15T10:01:00+08:00', elapsed=45000, fetch=25000)
    receipt(tmp_path, 'monitor-future', '2026-10-15T13:01:00+08:00')
    receipt(tmp_path, 'monitor-old', '2026-10-14T10:01:00+08:00')
    receipt(tmp_path, 'monitor-unfinished', '2026-10-15T10:02:00+08:00', finalized=False)
    receipt(tmp_path, 'monitor-nan', '2026-10-15T10:03:00+08:00', fetch=float('nan'))
    result = timing_observations(tmp_path, cutoff=CUTOFF)
    assert result['minute_count'] == 1
    assert result['budget_warnings'] == []
    assert result['invalid_receipt_count'] == 1
    assert result['status'] == 'DATA_LIMITED'


def test_snapshot_real_entry_wires_observability_and_model_projection_keeps_summaries(tmp_path):
    from liangjian_funnel.review.daily import build_a5_fact_snapshot, A5ReviewKind, _model_fact_projection
    from liangjian_funnel.runtime.state import RuntimeStore
    store = RuntimeStore(tmp_path / 'isolated.db')
    store.list_execution_plans = lambda **kw: [plan()]
    store.list_monitor_events = lambda **kw: [event(conditions={'pressure': {'met': False, 'available': True, 'kind': 'CONDITION'}})]
    result = build_a5_fact_snapshot(store, tmp_path, trade_date=CUTOFF.date(), cutoff_at=CUTOFF,
        review_kind=A5ReviewKind.MIDDAY, lane_id='lane_1')
    obs = result['execution_observability']
    assert obs['conditions']['plans'][0]['max_conditions_met'] == 0
    projected = _model_fact_projection(result)['execution_observability']
    assert projected['conditions']['plans'][0]['failing_conditions_at_max'] == ['pressure']
    assert projected['timings']['over_30s_minute_count'] == 0
    assert 'over_30s_minutes' not in projected['timings']
    from liangjian_funnel.review.daily import _evidence_ids, _projection_evidence_ids
    assert 'A5O:CONDITIONS:p' in _evidence_ids(result)
    assert 'A5O:TIMINGS' in _projection_evidence_ids(_model_fact_projection(result))


def test_route_constants_derived_and_veto_do_not_inflate_leaf_score():
    names = ('A3_TREND_ROUTE_APPROVED', 'TREND_ENTRY_RULE_V2', 'STANDARD_TREND_ROUTE',
             'TREND_5M_REVERSAL_CONFIRMATION', 'TREND_PRIOR_5M_REVERSAL')
    matrix = {key: {'met': True, 'available': True, 'kind': 'CONDITION'} for key in names}
    matrix['OVERHEAT'] = {'met': False, 'available': True, 'kind': 'VETO'}
    result = condition_observations([plan()], [event(conditions=matrix)], cutoff=CUTOFF)['plans'][0]
    assert result['max_conditions_met'] == 1
    assert result['total_conditions_at_max'] == ['TREND_PRIOR_5M_REVERSAL']
    assert result['veto_conditions_at_max'] == ['OVERHEAT']
    assert 'A3_PULLBACK_ZONE' in result['unevaluated_conditions_at_max']


def test_no_cross_minute_or_and_no_mutation_of_model_projection():
    yes = {'met': True, 'available': True, 'kind': 'CONDITION'}
    no = {**yes, 'met': False}
    conditions = condition_observations([plan()], [event(conditions={'a': yes, 'b': no}),
        event('10:02', 'other', conditions={'a': no, 'b': yes})], cutoff=CUTOFF)
    assert conditions['plans'][0]['max_conditions_met'] == 1
    from liangjian_funnel.review.daily import _model_fact_projection
    facts = {'execution_observability': {'conditions': conditions,
             'timings': {'over_30s_minutes': [{'run_id': 'must_stay_archived'}], 'budget_warnings': ['FETCH_P95_OVER_25S']}}}
    before = copy.deepcopy(facts)
    projection = _model_fact_projection(facts)
    assert facts == before
    assert projection['execution_observability']['timings']['budget_warnings'] == ['FETCH_P95_OVER_25S']
    assert 'must_stay_archived' not in json.dumps(projection)


def test_invalidation_confirmation_is_a_failure_not_positive_entry_progress():
    row = plan(payload_json={'strategy_profile': 'TREND_MA5', 'trend_entry_rule_version': 'trend-ma5/2'})
    bad_news = {key: {'met': True, 'available': True, 'kind': 'CONDITION'} for key in
                ('TREND_15M_BREAKDOWN_CONFIRMED', 'TREND_5M_FAILED_RECLAIM', 'TREND_PRE_ENTRY_CLOSED_BREAKDOWN')}
    result = condition_observations([row], [event(conditions=bad_news, action='PLAN_INVALIDATED', effective=True)], cutoff=CUTOFF)['plans'][0]
    assert result['max_conditions_met'] is None
    assert result['status'] == 'DATA_LIMITED'
    assert 'A3_PULLBACK_ZONE' in result['unevaluated_conditions_at_max']


def test_parent_budget_is_not_hidden_by_shorter_python_receipt(tmp_path):
    directory = tmp_path / 'timeout-snapshots'
    directory.mkdir()
    receipt(directory, 'monitor-slow', '2026-10-15T10:01:00+08:00', elapsed=44000, fetch=20000)
    (tmp_path / 'node-2026-10-15.jsonl').write_text(json.dumps({'timestamp': '2026-10-15T02:01:00Z',
        'job': 'monitor', 'stream': 'node', 'runId': 'monitor-slow',
        'message': '任务结束 run-monitor status=succeeded exit=0 signal=none duration_ms=46000'}), encoding='utf-8')
    result = timing_observations(directory, cutoff=CUTOFF)
    assert result['budget_warnings'] == ['MONITOR_MINUTE_OVER_45S']
    assert result['parent_duration_count'] == 1
    assert result['over_30s_minutes'][0]['elapsed_ms'] == 44000
    assert result['over_30s_minutes'][0]['parent_duration_ms'] == 46000


def test_56_plan_observability_remains_under_existing_prompt_target():
    from liangjian_funnel.review.daily import _model_fact_projection
    from liangjian_funnel.review.context import render_a5_prompt
    from liangjian_funnel.pipeline.prompts import PromptRepository
    from pathlib import Path
    plans = [plan(str(i)) for i in range(56)]
    observations = condition_observations(plans, [event()], cutoff=CUTOFF)
    projection = _model_fact_projection({'execution_observability': {'conditions': observations,
        'timings': {'phases': {}, 'over_30s_minutes': [], 'budget_warnings': []}}})
    prompt, diagnostics = render_a5_prompt(PromptRepository(Path(__file__).resolve().parents[1] / 'prompts'),
        'agent_5_daily_reviewer_v1.txt', projection)
    assert diagnostics['prompt_chars'] < 180000
    assert 'DISABLED_BY_DESIGN_NOT_A3_COUNT' in prompt
    assert len(projection['execution_observability']['conditions']['plans']) == 56


def test_unreadable_node_log_limits_evidence_without_breaking_a5(tmp_path, monkeypatch):
    from pathlib import Path
    directory = tmp_path / 'timeout-snapshots'; directory.mkdir()
    receipt(directory, 'monitor-normal', '2026-10-15T10:01:00+08:00')
    log = tmp_path / 'node-2026-10-15.jsonl'; log.write_text('{}', encoding='utf-8')
    original = Path.open
    def guarded(path, *args, **kwargs):
        if path == log:
            raise PermissionError('isolated fixture')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', guarded)
    result = timing_observations(directory, cutoff=CUTOFF)
    assert result['status'] == 'DATA_LIMITED'
    assert result['minute_count'] == 1
    assert result['invalid_receipt_count'] == 1


def test_all_technical_leaves_pass_does_not_hide_price_rr_failure():
    from liangjian_funnel.review.observability import TREND_V2_LEAVES
    row = plan(payload_json={'strategy_profile': 'TREND_MA5', 'trend_entry_rule_version': 'trend-ma5/2'})
    matrix = {key: {'met': True, 'available': True, 'kind': 'CONDITION', 'reason': 'OK'} for key in TREND_V2_LEAVES}
    matrix['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'] = {'met': False, 'available': True, 'kind': 'CONDITION',
                                                'reason': 'A4_LIVE_REWARD_RISK_BELOW_MINIMUM'}
    matrix['ENTRY_BLOCKED_CURRENT_MINUTE'] = {'met': False, 'available': True, 'kind': 'VETO'}
    result = condition_observations([row], [event(conditions=matrix)], cutoff=CUTOFF)['plans'][0]
    assert result['max_conditions_met'] == 7
    assert result['failing_conditions_at_max'] == ['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED', 'ENTRY_BLOCKED_CURRENT_MINUTE']
    assert result['failing_condition_reasons_at_max']['A4_LIVE_ENTRY_GEOMETRY_ACCEPTED'] == 'A4_LIVE_REWARD_RISK_BELOW_MINIMUM'
