"""0368 deterministic research projections; no model or production access."""
from fixture_model_stream import sse_model_response
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from test_team_v2_plan_review import CATALOG, DOMAIN, ENVELOPE, response, session_fixture
from liangjian_funnel.pipeline.team_v2_plan_review_schema import validate_plan_review
from liangjian_funnel.runtime.team_v2_plan_recovery import recover_plan_review


def sample():
    domain = deepcopy(DOMAIN)
    domain['000002.SZ'] = deepcopy(domain['000001.SZ'])
    raw = response()
    row = deepcopy(raw['core_watch_pool'][0])
    row.update(symbol='000002.SZ', plan_priority_rank=2)
    raw['core_watch_pool'].append(row)
    return raw, domain


def recover(raw, domain, catalog=CATALOG):
    return recover_plan_review(raw, catalog, required_envelope=ENVELOPE, candidate_domain=domain)


def test_real_14a_raw_remains_invalid_but_audited_projection_keeps_16_pass_with_probe_cap():
    frozen = json.loads((Path(__file__).parent / 'fixtures/team_v2_a3_real_risk_failure_20261019.json').read_text(
        encoding='utf-8'))
    raw = frozen['raw']; original = deepcopy(frozen)
    raw_hash = hashlib.sha256(json.dumps(raw, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    assert raw_hash == frozen['provenance']['output_hash']
    check = lambda value: validate_plan_review(value, frozen['catalog'],
        required_envelope=frozen['envelope'], candidate_domain=frozen['domain'])
    assert check(raw).issues == ('A3_PLAN_PASS_RISK_INVALID',)
    effective, audit = recover_plan_review(raw, frozen['catalog'],
        required_envelope=frozen['envelope'], candidate_domain=frozen['domain'])
    assert frozen == original and check(raw).valid is False
    assert check(effective).valid and len(effective['core_watch_pool']) == 16
    before = {r['symbol']: r for r in raw['core_watch_pool']}
    after = {r['symbol']: r for r in effective['core_watch_pool']}
    assert after['000576.SZ'] == {**before['000576.SZ'], 'risk_unit': 'PROBE'}
    assert all(after[symbol] == row for symbol, row in before.items() if symbol != '000576.SZ')
    assert audit['actions'] == [dict(code='SERVER_CLAMPED_RISK_UNIT', symbol='000576.SZ',
        original_value='STANDARD', effective_value='PROBE', input_cap='PROBE')]


def test_only_above_cap_standard_is_clamped_and_raw_source_stays_invalid():
    raw, domain = sample()
    raw['core_watch_pool'][0]['risk_unit'] = 'STANDARD'
    original = deepcopy(raw); frozen = deepcopy(domain)
    assert validate_plan_review(raw, CATALOG, required_envelope=ENVELOPE,
        candidate_domain=domain).issues == ('A3_PLAN_PASS_RISK_INVALID',)
    effective, audit = recover(raw, domain)
    assert raw == original and domain == frozen
    assert len(effective['core_watch_pool']) == 2
    assert effective['core_watch_pool'][0] == {**original['core_watch_pool'][0], 'risk_unit': 'PROBE'}
    assert audit['status'] == 'VALIDATED'
    assert audit['actions'] == [dict(code='SERVER_CLAMPED_RISK_UNIT', symbol='000001.SZ',
        original_value='STANDARD', effective_value='PROBE', input_cap='PROBE')]
    # A valid STANDARD cap is not lowered, and a provider's PROBE is never upgraded.
    domain['000001.SZ']['risk_unit'] = 'STANDARD'
    assert recover(raw, domain) is None
    raw['core_watch_pool'][0]['risk_unit'] = 'PROBE'
    assert recover(raw, domain) is None


@pytest.mark.parametrize('code,field,value', [
    ('STRATEGY_PROFILE_CHANGED', 'strategy_profile', 'LEADER_INTRADAY'),
    ('CLUSTER_CHANGED', 'cluster', 'model invented cluster'),
    ('TRADE_BRIEF_INVALID', 'trade_brief', {}),
    ('ITEM_FALSIFIERS_INVALID', 'what_would_change_my_mind', []),
    ('REASON_CODES_INVALID', 'reason_codes', ['duplicate', 'duplicate']),
    ('SYMBOL_OUTSIDE_DOMAIN', 'symbol', '999999.SZ'),
])
def test_only_six_authorized_local_errors_quarantine_without_granting_permission(code, field, value):
    raw, domain = sample()
    if code == 'SYMBOL_OUTSIDE_DOMAIN':
        bad = deepcopy(raw['core_watch_pool'][0]); bad.update(symbol=value, plan_priority_rank=3)
        raw['core_watch_pool'].append(bad)
    else:
        raw['core_watch_pool'][0][field] = value
        bad = raw['core_watch_pool'][0]
    original = deepcopy(raw); frozen = deepcopy(domain)
    effective, audit = recover(raw, domain)
    assert raw == original and domain == frozen
    assert audit['status'] == 'VALIDATED'
    quarantines = [a for a in audit['actions'] if a['code'] == 'SERVER_QUARANTINED_ROW']
    assert len(quarantines) == 1
    assert quarantines[0]['original_row'] == bad
    assert quarantines[0]['original_issues'] == ['A3_PLAN_' + code]
    assert all(r['symbol'] != bad['symbol'] for r in effective['core_watch_pool'])
    assert sorted(r['plan_priority_rank'] for r in effective['core_watch_pool']) == list(
        range(1, len(effective['core_watch_pool']) + 1))
    if code == 'SYMBOL_OUTSIDE_DOMAIN':
        assert effective['secondary_watch_pool'] == []
    else:
        row = effective['secondary_watch_pool'][0]
        assert row['symbol'] == '000001.SZ' and row['review_status'] == 'DATA_GAP'
        assert row['risk_unit'] == 'NO_ENTRY'
        assert row['strategy_profile'] == domain[row['symbol']]['strategy_profile']
        assert row['cluster'] == domain[row['symbol']]['cluster']
        assert row['reason_codes'] == ['SERVER_QUARANTINED_ROW', 'A3_PLAN_' + code]
        assert any(a['code'] == 'SERVER_COMPACTED_PRIORITY_AFTER_QUARANTINE' for a in audit['actions'])


@pytest.mark.parametrize('mutate', [
    lambda raw: raw['envelope'].update(execution_trade_date='2026-10-22'),
    lambda raw: raw.update(extra_root=True),
    lambda raw: raw.update(core_watch_pool={}),
    lambda raw: raw['core_watch_pool'][1].update(plan_priority_rank=1),
    lambda raw: raw['core_watch_pool'][1].update(plan_priority_rank=8),
    lambda raw: raw['core_watch_pool'][1].update(supporting_refs=['FAKE_REF']),
    lambda raw: raw['core_watch_pool'][1].update(opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND']),
    lambda raw: raw['core_watch_pool'][1].pop('risk_unit'),
    lambda raw: raw['core_watch_pool'][1].update(risk_unit='UNKNOWN'),
    lambda raw: raw['core_watch_pool'].pop(),
    lambda raw: raw['core_watch_pool'][1].update(symbol='000001.SZ'),
])
def test_global_evidence_rank_shape_missing_domain_or_invalid_risk_cannot_be_hidden(mutate):
    raw, domain = sample()
    raw['core_watch_pool'][0]['trade_brief'] = {}  # Otherwise quarantinable.
    mutate(raw)
    original = deepcopy(raw)
    assert recover(raw, domain) is None
    assert raw == original


def test_unknown_symbol_does_not_replace_a_missing_real_candidate_and_catalog_stays_global():
    raw, domain = sample()
    raw['core_watch_pool'][0]['symbol'] = '999999.SZ'
    assert recover(raw, domain) is None
    raw, domain = sample()
    raw['core_watch_pool'][0]['trade_brief'] = {}
    assert recover(raw, domain, {'categories': ['STOCK_TECH'], 'items': []}) is None


def test_all_quarantined_is_honest_empty_core_not_synthesized_pass():
    raw, domain = sample()
    for row in raw['core_watch_pool']:
        row['trade_brief'] = {}
    effective, audit = recover(raw, domain)
    assert effective['core_watch_pool'] == [] and audit['status'] == 'VALIDATED'
    assert len(effective['secondary_watch_pool']) == 2
    assert all(row['risk_unit'] == 'NO_ENTRY' for row in effective['secondary_watch_pool'])


def test_original_invalid_capture_preserved_and_derived_audit_is_bound_and_idempotent(tmp_path):
    from liangjian_funnel.runtime.team_v2_plan_review import run_plan_review
    import httpx
    kwargs, seen = session_fixture(tmp_path, eligible=False)
    raw = response(); raw['core_watch_pool'][0]['risk_unit'] = 'STANDARD'
    original = deepcopy(raw)
    source = deepcopy({key: kwargs[key] for key in ('replacements', 'candidate_domain', 'engine_decisions')})
    def handler(request):
        seen.append(json.loads(request.read()))
        return sse_model_response(200, json={'choices': [{'message': {'content': json.dumps(raw)}}]}, request=request)
    kwargs['client'].transport = httpx.MockTransport(handler)
    result = run_plan_review(**kwargs)
    assert result['status'] == 'READY' and result['provider_validated'] is False
    assert result['report']['core_watch_pool'][0]['risk_unit'] == 'PROBE'
    assert not result['comparisons'][0]['engine_and_model_pass']
    assert run_plan_review(**kwargs) == result and len(seen) == 3
    assert raw == original and {key: kwargs[key] for key in source} == source
    with sqlite3.connect(tmp_path / 'models.sqlite3') as db:
        captured = db.execute('SELECT validated,issues_json,output_json FROM model_calls').fetchone()
    assert captured[:2] == (0, '["A3_PLAN_PASS_RISK_INVALID"]')
    assert json.loads(captured[2]) == original
    with sqlite3.connect(tmp_path / 'team.sqlite3') as db:
        audit = json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',
            (result['server_projection']['audit_context_key'],)).fetchone()[0])
    assert audit['status'] == 'VALIDATED'
    assert audit['original_issues'] == ['A3_PLAN_PASS_RISK_INVALID']
    assert audit['effective_raw'] == result['report']
    assert audit['provider_raw_sha256'] != audit['effective_raw_sha256']
