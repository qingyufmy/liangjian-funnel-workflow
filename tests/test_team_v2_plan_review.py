"""Frozen close-review contracts; no provider, production DB or publication."""
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.team_v2_plan_review_schema import validate_plan_review
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository, _ENVELOPE_KEYS

CATALOG = {'categories': ['STOCK_TECH', 'NEWS'], 'items': [
    {'id': 'TECH1', 'category': 'STOCK_TECH', 'summary': 'closed daily prices'},
    {'id': 'NEWS1', 'category': 'NEWS', 'summary': 'original disclosure'}]}
DOMAIN = {'000001.SZ': {'strategy_profile': 'TREND_MA5', 'risk_unit': 'PROBE',
    'cluster': 'bank', 'candidate_origin': 'WATCH_ONLY'}}
ENVELOPE = {key: 'FROZEN' for key in _ENVELOPE_KEYS}
ENVELOPE.update(stage_id='A3', run_id='review-local', model_name='deepseek-v4-pro-0813',
    source_trade_date='2026-10-16', execution_trade_date='2026-10-19')


def response():
    return {'envelope': deepcopy(ENVELOPE), 'session': 'PLAN_REVIEW',
        'core_watch_pool': [{'symbol': '000001.SZ', 'strategy_profile': 'TREND_MA5',
            'review_status': 'PASS', 'risk_unit': 'PROBE', 'plan_priority_rank': 1,
            'cluster': 'bank', 'reason_codes': [], 'supporting_refs': ['TECH1'],
            'opposing_refs': ['NEWS1'], 'trade_brief': {'likely_failure_path': 'daily support breaks',
                'key_levels_note': 'use original frozen support', 'confirm_cadence': '5m',
                'note_to_trader': 'do not chase'}, 'what_would_change_my_mind': ['new disclosure']}],
        'secondary_watch_pool': [], 'rejected_candidates': [],
        'handoff_notes': {'to_a4': ['watch support'], 'to_a6': ['bank exposure']},
        'what_would_change_my_mind': ['changed daily structure'], 'unresolved_questions': []}


def check(output, domain=None):
    return validate_plan_review(output, CATALOG, required_envelope=ENVELOPE,
        candidate_domain=DOMAIN if domain is None else domain)


def test_complete_frozen_review_and_empty_domain_are_valid_without_mutation():
    raw = response(); original = deepcopy(raw)
    assert check(raw).valid and raw == original
    raw['core_watch_pool'] = []
    assert check(raw, {}).valid


@pytest.mark.parametrize('field,value', [
    ('strategy_profile', 'LEADER_INTRADAY'), ('risk_unit', 'STANDARD'),
    ('review_status', 'VETO'), ('cluster', 'new sector'),
    ('plan_priority_rank', 0), ('plan_priority_rank', True), ('plan_priority_rank', 3),
    ('trade_brief', {}), ('trade_brief', {'likely_failure_path': 'no evidence'}),
    ('what_would_change_my_mind', []), ('entry_reference_zone', [1, 100])])
def test_model_cannot_change_geometry_route_risk_or_invent_rank(field, value):
    raw = response(); raw['core_watch_pool'][0][field] = value
    assert not check(raw).valid


@pytest.mark.parametrize('mutate', [
    lambda o: o.pop('envelope'),
    lambda o: o['envelope'].update(execution_trade_date='2026-10-20'),
    lambda o: o.update(external_orders=True),
    lambda o: o.update(handoff_notes={'to_a4': ['x']}),
    lambda o: o.update(unresolved_questions=[None])])
def test_missing_binding_or_extra_root_fields_fail_closed(mutate):
    raw = response(); mutate(raw)
    assert not check(raw).valid


def test_duplicate_ranks_across_two_original_candidates_invalid():
    domain = deepcopy(DOMAIN); domain['000002.SZ'] = deepcopy(domain['000001.SZ'])
    raw = response(); other = deepcopy(raw['core_watch_pool'][0]); other['symbol'] = '000002.SZ'
    raw['core_watch_pool'].append(other)
    assert not check(raw, domain).valid


def test_data_gap_is_not_a_pass_and_no_entry_required():
    raw = response(); row = raw['core_watch_pool'].pop()
    for field in ('plan_priority_rank', 'trade_brief', 'what_would_change_my_mind'):
        row.pop(field)
    row.update(review_status='DATA_GAP', risk_unit='NO_ENTRY')
    raw['secondary_watch_pool'].append(row)
    assert check(raw).valid
    row['risk_unit'] = 'PROBE'
    assert not check(raw).valid


@pytest.mark.parametrize('field', ['symbol', 'strategy_profile', 'review_status', 'risk_unit', 'cluster',
    'reason_codes', 'supporting_refs', 'opposing_refs', 'trade_brief', 'what_would_change_my_mind'])
@pytest.mark.parametrize('value', [None, [], {}])
def test_malformed_values_are_invalid_not_validator_exceptions(field, value):
    raw = response(); raw['core_watch_pool'][0][field] = value
    if field == 'reason_codes' and value == []:
        assert check(raw).valid
    else:
        assert not check(raw).valid


def session_fixture(tmp_path, *, invalid=False, offline=False, eligible=True):
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.settings import Settings
    repository = TeamPromptRepository(Path(__file__).resolve().parents[1] / 'prompts/team_v2')
    replacements = {key: {'status': 'INSUFFICIENT'} for key in repository.document('A3').placeholders}
    replacements.update(EVIDENCE_CATALOG=CATALOG, A3_FACT_SHEET={'000001.SZ': {
        'as_of': '2026-10-16T15:00:00+08:00', 'daily_ma': {'ma5': 10, 'ma20': 9},
        'eligibility': 'QUALIFIED', 'status': 'APPROVED', 'met_conditions': ['ALL_MET'],
        'gate_results': {'decision': 'PASS'}, 'engine_verdict': 'APPROVED',
        'strategy_facts': {'daily_macd': {'dif': 1}, 'review_status': 'PASS',
            'condition_details': {'x': {'status': 'PASS', 'value': 12}}},
        'entry_reference_zone': [10, 11], 'no_chase_price': 12, 'daily_invalidation': 9}})
    raw = response()
    if invalid:
        # An invalid enum is not the authorized STANDARD -> PROBE cap clamp.
        raw['core_watch_pool'][0]['risk_unit'] = 'INVALID_RISK'
    seen = []
    def handler(request):
        seen.append(json.loads(request.read()))
        if offline:
            raise httpx.ConnectError('PRIVATE_PROVIDER_DETAILS', request=request)
        return httpx.Response(200, json={'choices': [{'message': {'content': json.dumps(raw)}}]}, request=request)
    settings = Settings.from_env({'LIANGJIAN_MODEL_API_KEY': 'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'}, root=tmp_path)
    kwargs = dict(repository=repository, client=OpenAICompatibleModelClient(settings,
        transport=httpx.MockTransport(handler), max_attempts=1),
        journal=ModelJudgmentJournal(tmp_path / 'models.sqlite3'), store=TeamMeetingStore(tmp_path / 'team.sqlite3'),
        review_id='a3:original-frozen', call_id='a3:original-frozen:call', run_id='review-local',
        model=ENVELOPE['model_name'], source_trade_date='2026-10-16', trade_date='2026-10-19',
        clock=lambda: datetime.fromisoformat('2026-10-16T16:30:00+08:00'), required_envelope=deepcopy(ENVELOPE),
        replacements=deepcopy(replacements), candidate_domain=deepcopy(DOMAIN),
        engine_decisions={'000001.SZ': {'eligible': eligible, 'reason_codes': ['ORIGINAL_ENGINE_FACT']}},
        timeout_seconds=30, max_output_tokens=4096, allow_model_call=True)
    return kwargs, seen


def test_real_client_blind_request_journal_and_comparison_without_publishing(tmp_path):
    from liangjian_funnel.runtime.team_v2_plan_review import run_plan_review
    kwargs, seen = session_fixture(tmp_path, eligible=False)
    source = deepcopy(kwargs['replacements'])
    result = run_plan_review(**kwargs)
    assert result['status'] == 'READY' and result['publication_authorized'] is False
    assert result['comparisons'] == [{'symbol': '000001.SZ', 'engine_eligible': False,
        'model_review_status': 'PASS', 'engine_and_model_pass': False}]
    assert kwargs['replacements'] == source
    text = json.dumps(seen[0]['messages'])
    assert 'APPROVED' not in text and 'ORIGINAL_ENGINE_FACT' not in text and 'ALL_MET' not in text
    from liangjian_funnel.runtime.team_v2_plan_review import plan_review_inputs
    projected = plan_review_inputs(kwargs['replacements'], kwargs['candidate_domain'], kwargs['engine_decisions'])
    assert projected['A3_FACT_SHEET']['000001.SZ']['strategy_facts']['condition_details'] == {'x': {'value': 12}}
    assert not {'eligibility', 'met_conditions', 'gate_results', 'status', 'engine_verdict'} & set(
        projected['A3_FACT_SHEET']['000001.SZ'])
    assert '"ma5":10' in seen[0]['messages'][0]['content']
    assert '"candidate_origin":"WATCH_ONLY"' in seen[0]['messages'][0]['content']
    assert run_plan_review(**kwargs) == result and len(seen) == 1
    with sqlite3.connect(tmp_path / 'models.sqlite3') as db:
        assert db.execute('SELECT validated FROM model_judgments').fetchall() == [(1,)]
        assert db.execute('SELECT t1_return,t3_return FROM model_judgments').fetchall() == [(None, None)]
    with sqlite3.connect(tmp_path / 'team.sqlite3') as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()


def test_frozen_risk_cap_reaches_model_and_binds_actual_request_without_changing_source(tmp_path):
    from liangjian_funnel.runtime.team_v2_plan_review import plan_review_inputs, run_plan_review
    kwargs, seen = session_fixture(tmp_path, eligible=False)
    originals = deepcopy({key: kwargs[key] for key in
        ('replacements', 'candidate_domain', 'engine_decisions')})

    def follows_visible_cap(request):
        body = json.loads(request.read())
        seen.append(body)
        # Reproduce the transport defect: an absent explicit cap lets the
        # provider return STANDARD. The server must never coerce that raw vote.
        pool, _ = json.JSONDecoder().raw_decode(
            body['messages'][0]['content'].split('A3_CANDIDATE_POOL=', 1)[1])
        raw = response()
        raw['core_watch_pool'][0]['risk_unit'] = pool[0].get('risk_unit', 'STANDARD')
        return httpx.Response(200, json={'choices': [{'message': {'content': json.dumps(raw)}}]},
            request=request)

    kwargs['client'].transport = httpx.MockTransport(follows_visible_cap)
    result = run_plan_review(**kwargs)
    assert result['status'] == 'READY'
    assert result['report']['core_watch_pool'][0]['risk_unit'] == 'PROBE'
    assert not result['comparisons'][0]['engine_and_model_pass']
    assert len(seen) == 1  # No additional A3 semantic retry.
    assert {key: kwargs[key] for key in originals} == originals
    projected = plan_review_inputs(kwargs['replacements'], kwargs['candidate_domain'], kwargs['engine_decisions'])
    assert projected['A3_CANDIDATE_POOL'][0]['risk_unit'] == 'PROBE'
    actual_hash = hashlib.sha256(json.dumps(seen[0]['messages'], ensure_ascii=False,
        sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    prepared = kwargs['repository'].prepare_messages('A3', projected,
        required_envelope=kwargs['required_envelope'], session='PLAN_REVIEW')
    assert actual_hash == prepared.input_hash
    with sqlite3.connect(tmp_path / 'models.sqlite3') as db:
        assert db.execute('SELECT input_hash,validated FROM model_calls').fetchone() == (actual_hash, 1)
    with sqlite3.connect(tmp_path / 'team.sqlite3') as db:
        source = json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            ('a3-plan-source:' + kwargs['review_id'],)).fetchone()[0])
    assert source['prompt_input_hash'] == actual_hash
    assert source['candidate_domain'] == originals['candidate_domain']
    assert source['engine_decisions'] == originals['engine_decisions']
    original_invalid_raw = response()
    original_invalid_raw['core_watch_pool'][0]['risk_unit'] = 'STANDARD'
    assert check(original_invalid_raw).issues == ('A3_PLAN_PASS_RISK_INVALID',)


def test_candidate_cap_projection_is_exact_and_changes_prompt_binding(tmp_path):
    from liangjian_funnel.runtime.team_v2_plan_review import plan_review_inputs
    kwargs, _ = session_fixture(tmp_path)
    domain = deepcopy(kwargs['candidate_domain'])
    requests = []
    for cap in ('PROBE', 'STANDARD'):
        domain['000001.SZ']['risk_unit'] = cap
        projected = plan_review_inputs(kwargs['replacements'], domain, kwargs['engine_decisions'])
        assert projected['A3_CANDIDATE_POOL'] == [dict(symbol='000001.SZ',
            strategy_profile='TREND_MA5', cluster='bank', candidate_origin='WATCH_ONLY', risk_unit=cap)]
        requests.append(kwargs['repository'].prepare_messages('A3', projected,
            required_envelope=kwargs['required_envelope'], session='PLAN_REVIEW'))
    assert requests[0].input_hash != requests[1].input_hash
    assert requests[0].evidence_catalog_hash == requests[1].evidence_catalog_hash


@pytest.mark.parametrize('offline', [False, True])
def test_failed_review_is_durable_data_gap_not_a_hidden_retry_or_pass(tmp_path, offline):
    from liangjian_funnel.runtime.team_v2_plan_review import run_plan_review
    kwargs, seen = session_fixture(tmp_path, invalid=not offline, offline=offline)
    result = run_plan_review(**kwargs)
    assert result['status'] == 'A3_REVIEW_DATA_GAP' and result['report'] is None
    assert result['comparisons'][0]['model_review_status'] == 'DATA_GAP'
    assert not result['comparisons'][0]['engine_and_model_pass']
    assert run_plan_review(**kwargs) == result and len(seen) == 1
    assert b'PRIVATE_PROVIDER_DETAILS' not in (tmp_path / 'team.sqlite3').read_bytes()
    if not offline:
        with sqlite3.connect(tmp_path / 'models.sqlite3') as db:
            assert db.execute('SELECT validated FROM model_judgments').fetchall() == [(0,)]


@pytest.mark.parametrize('offline', [False, True])
def test_capture_before_result_commit_failure_recovers_without_another_http(tmp_path, monkeypatch, offline):
    from liangjian_funnel.runtime.team_v2_plan_review import PlanReviewRuntime, run_plan_review
    kwargs, seen = session_fixture(tmp_path, offline=offline)
    original = PlanReviewRuntime.record
    def fail(*args):
        raise sqlite3.OperationalError('ISOLATED_COMMIT_FAILURE')
    monkeypatch.setattr(PlanReviewRuntime, 'record', fail)
    with pytest.raises(sqlite3.OperationalError):
        run_plan_review(**kwargs)
    monkeypatch.setattr(PlanReviewRuntime, 'record', original)
    assert run_plan_review(**kwargs)['status'] == ('A3_REVIEW_DATA_GAP' if offline else 'READY')
    assert len(seen) == 1


@pytest.mark.parametrize('part', ['candidate_domain', 'engine_decisions', 'replacements'])
def test_same_review_id_cannot_be_rebound_to_new_source(tmp_path, part):
    from liangjian_funnel.runtime.team_v2_plan_review import run_plan_review
    kwargs, seen = session_fixture(tmp_path)
    run_plan_review(**kwargs)
    if part == 'candidate_domain':
        kwargs[part]['000001.SZ']['risk_unit'] = 'STANDARD'
    elif part == 'engine_decisions':
        kwargs[part]['000001.SZ']['eligible'] = False
    else:
        kwargs[part]['A3_FACT_SHEET']['000001.SZ']['daily_ma']['ma5'] = 10.5
    with pytest.raises(ValueError, match='CONFLICT'):
        run_plan_review(**kwargs)
    assert len(seen) == 1


@pytest.mark.parametrize('part', ['authorization', 'domain', 'date', 'facts'])
def test_invalid_server_inputs_reject_before_http(tmp_path, part):
    from liangjian_funnel.runtime.team_v2_plan_review import run_plan_review
    kwargs, seen = session_fixture(tmp_path)
    if part == 'authorization':
        kwargs['allow_model_call'] = False
    elif part == 'domain':
        kwargs['candidate_domain']['000001.SZ']['candidate_origin'] = 'MODEL_NOMINATION'
    elif part == 'date':
        kwargs['source_trade_date'] = '20261016'
    else:
        kwargs['replacements']['A3_FACT_SHEET'] = {}
    with pytest.raises(ValueError):
        run_plan_review(**kwargs)
    assert seen == []
