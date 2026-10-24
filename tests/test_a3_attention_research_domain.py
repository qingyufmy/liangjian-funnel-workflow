"""Research-only admission cannot launder A1/technical/model/entry authority."""
from copy import deepcopy
from dataclasses import replace
import pytest

from liangjian_funnel.pipeline.a3_attention_research import build_attention_a3_domain, _pretechnical_batches
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.research import _build_a3_candidate_domain
from test_hot100_independent_research_intake import attention_fixture, LEAD, NOW


def inputs(tmp_path):
    app, args, seen = attention_fixture(tmp_path)
    args['snapshot'] = replace(args['snapshot'], data={**args['snapshot'].data,
        'A2_MARKET_REFERENCE': {'market_trade_date': NOW.date().isoformat()}})
    audit = app._run_stage(**args)
    assert audit.status == 'VALIDATED', audit.reason_codes
    assert len(seen) == 1
    return args, audit


def build(args, audit):
    return build_attention_a3_domain(args['snapshot'].data, args['upstream_output'], audit,
                                     source_trade_date=NOW.date().isoformat())


def edit(audit, action):
    output = deepcopy(audit.output); action(output)
    return replace(audit, output=output, output_hash=content_hash(output))


def test_real_a2_adapter_monitor_research_can_reach_separate_scope_without_formal_promotion(tmp_path):
    args, audit = inputs(tmp_path)
    before = deepcopy((dict(args['snapshot'].data), args['upstream_output'], audit.as_dict()))
    result = build(args, audit)
    assert len(result['records']) == 1, result['dispositions']
    row = result['records'][0]
    assert (row['symbol'], row['theme_id']) == (LEAD, 'NEW_THEME')
    assert row['status'] == 'PENDING_ORDINARY_A3_TECHNICAL_REVIEW'
    assert row['execution_permission'] == 'BLOCKED' and row['risk_unit'] == 'NO_ENTRY'
    assert row['fundamental_admission_passed'] is False and row['formal_a1_member'] is False
    assert result['new_model_calls_authorized'] is False
    assert LEAD not in _build_a3_candidate_domain(audit.output)[1]
    assert (args['snapshot'].data, args['upstream_output'], audit.as_dict()) == before


def test_missing_disposition_is_not_silently_an_empty_success(tmp_path):
    args,audit=inputs(tmp_path)
    result=build(args,edit(audit,lambda output:output.update(independent_attention_research_dispositions=[])))
    assert not result['records']
    assert any(r['symbol']==LEAD and r['reason_code']=='A2_RESEARCH_DISPOSITION_MISSING' for r in result['dispositions'])


@pytest.mark.parametrize('defect', ['hot_hash', 'future_hot', 'g0', 'hard_risk', 'tradability', 'intake_hash'])
def test_frozen_source_or_risk_drift_cannot_reuse_old_a2_research(tmp_path, defect):
    args, audit = inputs(tmp_path)
    data = deepcopy(dict(args['snapshot'].data)); upstream = deepcopy(args['upstream_output'])
    if defect == 'hot_hash': data['EASTMONEY_HOT100_SNAPSHOT']['content_hash'] = '0' * 64
    if defect == 'future_hot': data['EASTMONEY_HOT100_SNAPSHOT']['as_of'] = '2099-01-01T15:00:00+08:00'
    if defect == 'g0': data['g0_symbols'].remove(LEAD)
    if defect == 'hard_risk': data['RISK_EVENTS'] = [dict(symbol=LEAD, severity='HIGH')]
    if defect == 'tradability': data['TRADABILITY_FLAGS'] = {LEAD: dict(tradable=False)}
    if defect == 'intake_hash': upstream['independent_research_intake']['candidates'][0]['candidate_id'] = 'f' * 64
    with pytest.raises(ValueError, match='A3_ATTENTION_'):
        build_attention_a3_domain(data, upstream, audit, source_trade_date=NOW.date().isoformat())


@pytest.mark.parametrize('defect', ['stage_failed', 'output_hash', 'call_ids'])
def test_failed_or_unbound_model_research_is_not_an_a3_source(tmp_path, defect):
    args, audit = inputs(tmp_path)
    if defect == 'stage_failed': audit = replace(audit, status='BLOCKED')
    if defect == 'output_hash': audit = replace(audit, output_hash='0' * 64)
    if defect == 'call_ids': audit = replace(audit, diagnostics={})
    with pytest.raises(ValueError, match='A3_ATTENTION_'):
        build(args, audit)


@pytest.mark.parametrize('defect', ['slot', 'candidate', 'call', 'deferred', 'review_day', 'review_hash', 'review_conflict'])
def test_per_case_missing_or_conflicting_evidence_stays_explicitly_unadmitted(tmp_path, defect):
    args, audit = inputs(tmp_path)
    def mutate(output):
        d = next(r for r in output['independent_attention_research_dispositions'] if r['symbol'] == LEAD)
        if defect == 'slot': output['independent_research_sampling_plan']['selected_slots'] = []
        if defect == 'candidate': d['candidate_id'] = 'f' * 64
        if defect == 'call': d['team_call_ids'] = ['FOREIGN_CALL']
        if defect == 'deferred': d['status'] = 'DEFERRED_A2_SIDE_SAMPLE_CAPACITY'
        r = next(r for r in output['independent_theme_reviews'] if r['theme_id'] == 'NEW_THEME')
        if defect == 'review_day':
            r['review']['stage_observed_as_of'] = '2026-10-15'
            r['review_id'] = content_hash({k:v for k,v in r.items() if k != 'review_id'})
        if defect == 'review_hash': r['review_id'] = 'f' * 64
        if defect == 'review_conflict':
            other = deepcopy(r); other['review']['stage'] = 'RETREAT'
            other['review_id'] = content_hash({k:v for k,v in other.items() if k != 'review_id'})
            output['independent_theme_reviews'].append(other)
    result = build(args, edit(audit, mutate))
    assert result['records'] == [] and result['dispositions']
    assert all(r['status'] == 'NOT_ADMITTED' for r in result['dispositions'])


def test_one_stock_two_themes_preserves_two_distinct_cases_without_copying_a_verdict(tmp_path):
    from liangjian_funnel.pipeline.research.common import _with_daily_emotion_overlay
    def reply(raw, attempt):
        other = deepcopy(raw['theme_reviews'][-1]); other['theme_id'] = 'ROBOTICS_ADVANCED_MANUFACTURING'
        raw['theme_reviews'].append(other)
    app, args, _ = attention_fixture(tmp_path, reply)
    args['snapshot'] = replace(args['snapshot'], data={**args['snapshot'].data,
        'A2_MARKET_REFERENCE': {'market_trade_date': NOW.date().isoformat()}})
    args['upstream_output']['taxonomy_links'].append(dict(theme_id='ROBOTICS_ADVANCED_MANUFACTURING',
        node_id='R:CORE',taxonomy='CONCEPT',taxonomy_code='301000.TI',taxonomy_name='新材料概念',
        match_method='MATURE_THEME_REGISTRY_EXACT_NAME'))
    args['upstream_output'], _ = _with_daily_emotion_overlay(args['upstream_output'], args['snapshot'].data,
                                                           set(args['snapshot'].data['g0_symbols']))
    audit = app._run_stage(**args); assert audit.status == 'VALIDATED', audit.reason_codes
    result = build(args, audit)
    assert {r['theme_id'] for r in result['records']} == {'NEW_THEME', 'ROBOTICS_ADVANCED_MANUFACTURING'}, result['dispositions']
    assert len({r['case_id'] for r in result['records']}) == 2
    assert len(result['pretechnical_batches']) == 2


def test_batching_never_places_two_hypotheses_of_one_symbol_in_same_model_domain():
    rows = [dict(theme_id='T',source_slot=1,symbol='603278.SH',hypothesis_id=str(i),case_id=str(i)) for i in range(2)]
    plan = _pretechnical_batches(rows, 16)
    assert [b['case_ids'] for b in plan] == [['0'], ['1']]


def test_domain_hash_and_batch_order_are_independent_of_a2_review_array_order(tmp_path):
    args, audit = inputs(tmp_path)
    first = build(args, audit)
    second = build(args, edit(audit, lambda o:o['independent_theme_reviews'].reverse()))
    # Parent A2 output identity legitimately differs; semantic case scope must not.
    assert [(r['symbol'],r['theme_id'],r['hypothesis_id']) for r in first['records']] == [
        (r['symbol'],r['theme_id'],r['hypothesis_id']) for r in second['records']]


@pytest.mark.parametrize('defect', ['market_date', 'hot_future', 'hypothesis_hash'])
def test_attention_review_clock_is_never_filled_from_unproven_source(tmp_path, defect):
    from liangjian_funnel.pipeline.a2_theme_research import bind_attention_review_times, attention_theme_hypotheses
    args, audit = inputs(tmp_path)
    data = deepcopy(dict(args['snapshot'].data))
    candidate = args['upstream_output']['independent_research_intake']['candidates'][0]
    hypotheses = attention_theme_hypotheses([candidate])
    if defect == 'market_date': data['A2_MARKET_REFERENCE']['market_trade_date'] = '2026-10-15'
    if defect == 'hot_future': data['EASTMONEY_HOT100_SNAPSHOT']['as_of'] = '2099-01-01T15:00:00+08:00'
    if defect == 'hypothesis_hash': hypotheses[LEAD][0]['hypothesis_id'] = 'f' * 64
    unproven = dict(active_themes=[dict(theme_id='NEW_THEME')])
    result = bind_attention_review_times(unproven, data, [dict(theme_id='NEW_THEME')], hypotheses)
    assert 'stage_evidence_hash' not in result['active_themes'][0]


def test_attention_clock_binding_never_rehashes_existing_formal_theme_proof(tmp_path):
    from liangjian_funnel.pipeline.a2_theme_research import bind_attention_review_times, attention_theme_hypotheses
    args, _ = inputs(tmp_path)
    candidate = args['upstream_output']['independent_research_intake']['candidates'][0]
    hypotheses = attention_theme_hypotheses([candidate])
    existing = dict(active_themes=[dict(theme_id='NEW_THEME', stage_evidence_hash='a'*64,
                                       stage_observed_as_of='2026-10-16')])
    before = deepcopy(existing)
    result = bind_attention_review_times(existing, args['snapshot'].data, [dict(theme_id='NEW_THEME')], hypotheses)
    assert result == before and existing == before
