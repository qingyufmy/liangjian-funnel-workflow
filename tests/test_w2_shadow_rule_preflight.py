from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime import shadow_rule_preflight as mod
from liangjian_funnel.runtime.shadow_pit_sources import record_raw_response

ROOT = Path(__file__).parents[1]
ARCHIVE = ROOT / 'artifacts/wp5-20261010/official-rules-20261010'
SH = ZoneInfo('Asia/Shanghai')
NOW = datetime(2026, 10, 12, 8, 31, tzinfo=SH)


@pytest.fixture
def approved():
    return mod.load_approved_rule_archive(ARCHIVE)


def observations(approved, stamp=NOW):
    return [record_raw_response(
        row.raw_response, source_ref='official-rule:'+row.document_id,
        endpoint=row.url, request_parameters={}, request_started_at=stamp-timedelta(seconds=1),
        response_received_at=stamp, byte_kind='HTTP_RESPONSE_CONTENT_BYTES',
        clock_basis='CALLER_SUPPLIED_NOT_AUTHENTICATED',
    ) for row in approved.documents]


def check(approved, rows, now=NOW, day='2026-10-12'):
    return mod.confirm_rule_version(approved, rows, trade_date=day, known_at=now)


def test_three_exact_versions_same_day_confirmed_without_rule_date_extension(approved):
    result = check(approved, observations(approved))
    assert result['status'] == 'RULE_VERSION_CONFIRMED'
    assert result['confirmed_trade_date'] == '2026-10-12'
    assert result['rule_review_observed_at'] == '2026-10-10'
    assert result['planned_revalidation_due'] == '2026-10-16'
    assert result['future_no_change_proven'] is False
    assert result['latest_revision_absence_proven'] is False
    assert result['strict_derived_api_status'] == 'RULE_DATE_UNPROVEN'
    assert result['production_integration'] == 'UNWIRED'
    assert result['rule_constant_modified'] is False
    assert len(result['rows']) == 3
    assert result['receipt_sha256'] == hashlib.sha256(mod.canonical_receipt_bytes(result)).hexdigest()


@pytest.mark.parametrize('mode', [
    'missing','duplicate','changed_raw','tampered_hash','wrong_endpoint','wrong_parameters',
    'old_day','future','late','exact_0926','partial','http_error','consumer_bytes','naive',
])
def test_invalid_observation_is_limited(approved, mode):
    rows = observations(approved)
    now = NOW
    if mode == 'missing':
        rows.pop()
    elif mode == 'duplicate':
        rows[2] = rows[0]
    elif mode == 'changed_raw':
        r = rows[0]
        rows[0] = record_raw_response(r.raw_response+b' ', source_ref=r.source_ref, endpoint=r.endpoint,
            request_parameters={}, request_started_at=r.request_started_at,
            response_received_at=r.response_received_at, byte_kind=r.byte_kind)
    elif mode == 'tampered_hash':
        rows[0] = replace(rows[0], raw_sha256='0'*64)
    elif mode == 'wrong_endpoint':
        rows[0] = replace(rows[0], endpoint='https://example.com/rule')
    elif mode == 'wrong_parameters':
        rows[0] = replace(rows[0], request_parameters={'cached':'yes'})
    elif mode == 'old_day':
        rows = observations(approved, NOW-timedelta(days=1))
    elif mode == 'future':
        rows = observations(approved, NOW+timedelta(seconds=1))
    elif mode in {'late','exact_0926'}:
        now = NOW.replace(hour=9, minute=27 if mode == 'late' else 26)
        rows = observations(approved, now)
    elif mode == 'partial':
        rows[0] = replace(rows[0], complete=False)
    elif mode == 'http_error':
        rows[0] = replace(rows[0], http_status=503)
    elif mode == 'consumer_bytes':
        rows[0] = replace(rows[0], byte_kind='CONSUMER_INPUT_BYTES')
    elif mode == 'naive':
        now = NOW.replace(tzinfo=None)
    result = check(approved, rows, now)
    assert result['status'] == 'DATA_LIMITED'
    assert result['confirmed_trade_date'] is None
    assert result['reason_codes']


@pytest.mark.parametrize('day,now', [
    ('2026-10-13',NOW), ('2026-10-10',NOW.replace(day=10)),
    ('2026-10-19',NOW.replace(day=19)), ('2026-10-09',NOW.replace(day=9)),
])
def test_wrong_nontrading_or_out_of_review_plan_day_is_limited(approved, day, now):
    assert check(approved, observations(approved, now), now, day)['status'] == 'DATA_LIMITED'


def test_metadata_not_effective_does_not_override_formal_body(approved):
    result = check(approved, observations(approved))
    sse = next(row for row in result['rows'] if row['document_id'] == 'sse-notice')
    assert sse['formal_effective_date'] == '2026-07-06'
    assert sse['metadata_conflicts'] == ['META_NOT_EFFECTIVE_VS_FORMAL_BODY']
    assert result['status'] == 'RULE_VERSION_CONFIRMED'


def test_unparseable_content_does_not_pass_on_hash_only(approved, monkeypatch):
    monkeypatch.setattr(mod, '_parse_document', lambda *args: (_ for _ in ()).throw(ValueError('bad parser')))
    result = check(approved, observations(approved))
    assert result['status'] == 'DATA_LIMITED'
    assert 'RULE_CONTENT_UNPARSEABLE' in result['reason_codes']


@pytest.mark.parametrize('mode', ['receipt_hash','raw_hash','missing','path_escape'])
def test_approval_raw_archive_cannot_be_silently_reapproved(tmp_path, mode):
    target = tmp_path / 'approved'
    shutil.copytree(ARCHIVE, target)
    if mode == 'receipt_hash':
        (target/'receipt.json').write_bytes((target/'receipt.json').read_bytes()+b' ')
    elif mode == 'raw_hash':
        (target/'sse-notice.html').write_bytes(b'<html>different</html>')
    elif mode == 'missing':
        (target/'szse-rules.pdf').unlink()
    elif mode == 'path_escape':
        body = json.loads((target/'receipt.json').read_bytes())
        body['rows'][0]['path'] = '../outside.html'
        (target/'receipt.json').write_text(json.dumps(body),encoding='utf-8')
    with pytest.raises(mod.RulePreflightError):
        mod.load_approved_rule_archive(target)


def test_forged_approved_dataclass_not_trusted(approved):
    altered = replace(approved, documents=(replace(approved.documents[0], raw_response=b'fake'),)+approved.documents[1:])
    result = check(altered, observations(approved))
    assert result['status'] == 'DATA_LIMITED'
    assert 'APPROVED_ARCHIVE_UNPROVEN' in result['reason_codes']


def test_source_raw_and_receipt_hashes_are_reversible_and_no_input_mutation(approved):
    import base64
    rows = observations(approved)
    before = [row.metadata() for row in rows]
    result = check(approved, rows)
    for row in result['rows']:
        raw = base64.b64decode(row['raw_response_base64'], validate=True)
        assert hashlib.sha256(raw).hexdigest() == row['raw_sha256']
        assert row['clock_basis'] == 'CALLER_SUPPLIED_NOT_AUTHENTICATED'
        assert row['source_authenticated'] is False
    assert [row.metadata() for row in rows] == before


def test_default_http_is_unwired_and_never_fetches(approved):
    result = mod.fetch_rule_preflight(approved, trade_date='2026-10-12', clock=lambda:NOW)
    assert result['status'] == 'DATA_LIMITED'
    assert 'HTTP_UNWIRED' in result['reason_codes']
    assert result['source_requests_started'] == 0


def test_three_injected_calls_original_content_and_receive_clocks(approved):
    calls = []
    lookup = {row.url:row.raw_response for row in approved.documents}
    def fetch(url, timeout):
        calls.append((url,timeout))
        return SimpleNamespace(content=lookup[url],status_code=200,url=url)
    result = mod.fetch_rule_preflight(approved, trade_date='2026-10-12',fetch_response=fetch,
                                     clock=lambda:NOW,budget_seconds=2)
    assert result['status'] == 'RULE_VERSION_CONFIRMED'
    assert len(calls) == 3
    assert len({url for url,_ in calls}) == 3
    assert all(0 < timeout <= 2 for _,timeout in calls)
    assert result['source_requests_started'] == 3
    assert all(row['clock_basis'] == 'INJECTED_CLOCK_NOT_AUTHENTICATED' for row in result['rows'])


def test_failed_http_no_source_exception_or_secret_text_leaks(approved):
    def fetch(*args):
        raise RuntimeError('secret TOKEN private-body')
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',fetch_response=fetch,clock=lambda:NOW)
    assert result['status'] == 'DATA_LIMITED'
    assert 'RULE_HTTP_FAILED' in result['reason_codes']
    assert 'TOKEN' not in json.dumps(result)


def test_late_source_result_discarded_no_followup_requests(approved):
    gate = threading.Event()
    calls = []
    def fetch(url, timeout):
        calls.append(url)
        gate.wait(1)
        return SimpleNamespace(content=approved.documents[0].raw_response,status_code=200,url=url)
    start = time.monotonic()
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',fetch_response=fetch,
                                     clock=lambda:NOW,budget_seconds=.03)
    elapsed = time.monotonic()-start
    assert elapsed < .3
    assert result['status'] == 'DATA_LIMITED'
    assert 'RULE_PREFLIGHT_BUDGET_EXCEEDED' in result['reason_codes']
    assert result['rows'] == []
    gate.set()
    time.sleep(.02)
    assert len(calls) == 1
    assert result['rows'] == []


@pytest.mark.parametrize('budget', [0,-1,True,float('inf'),31])
def test_invalid_budget_rejected_without_source_calls(approved,budget):
    calls = []
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',clock=lambda:NOW,
                                     fetch_response=lambda *args:calls.append(args),budget_seconds=budget)
    assert result['status'] == 'DATA_LIMITED'
    assert 'RULE_PREFLIGHT_BUDGET_INVALID' in result['reason_codes']
    assert calls == []


def test_existing_price_limits_still_rejects_new_day_even_if_preflight_confirmed(approved):
    from liangjian_funnel.evaluation.ablation.price_limits import resolve_price_limits
    assert check(approved, observations(approved))['status'] == 'RULE_VERSION_CONFIRMED'
    evidence = dict(symbol='600189.SH',trade_date='2026-10-12',board='SSE_MAIN',security_type='CASH_A_SHARE',
        security_status='ORDINARY',is_st=False,limit_regime='NORMAL',listing_date='2000-01-01',
        rule_effective_from='2026-07-06',observed_at=NOW.isoformat(),source_ref='fixture:quote',
        source_input_sha256='1'*64,preclose=10,preclose_basis='EXCHANGE_DISPLAYED_PRECLOSE')
    result = resolve_price_limits({'symbol':'600189.SH','price_limit_evidence':evidence},symbol='600189.SH',at=NOW)
    assert result['status'] == 'UNKNOWN'
    assert result['evidence_reason'] == 'RULE_DATE_UNPROVEN'


def test_changed_raw_kept_for_audit_but_not_automatically_accepted(approved):
    rows = observations(approved)
    r = rows[0]
    changed = r.raw_response+b'<!-- changed -->'
    rows[0] = record_raw_response(changed,source_ref=r.source_ref,endpoint=r.endpoint,
        request_parameters={},request_started_at=r.request_started_at,response_received_at=r.response_received_at,
        byte_kind=r.byte_kind)
    result = check(approved, rows)
    assert result['status'] == 'DATA_LIMITED'
    assert result['rows'][0]['raw_sha256'] == hashlib.sha256(changed).hexdigest()
    assert result['rows'][0]['validation_status'] == 'DATA_LIMITED'
    assert result['rows'][0]['binding_verified'] is True
    assert result['rows'][0]['approved_raw_sha256'] != result['rows'][0]['raw_sha256']


def test_unparseable_keeps_received_raw_audit(approved, monkeypatch):
    monkeypatch.setattr(mod,'_parse_document',lambda *args:(_ for _ in ()).throw(ValueError()))
    result = check(approved,observations(approved))
    assert result['status'] == 'DATA_LIMITED'
    assert result['rows'][0]['raw_sha256'] == approved.documents[0].raw_sha256
    assert result['rows'][0]['validation_status'] == 'DATA_LIMITED'


def test_processing_clock_failure_is_bounded_not_an_uncaught_exception(approved):
    calls = [0]
    def clock():
        calls[0] += 1
        if calls[0] > 7:
            raise ValueError('private-secret')
        return NOW
    lookup = {row.url:row.raw_response for row in approved.documents}
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',clock=clock,budget_seconds=2,
        fetch_response=lambda url,timeout:SimpleNamespace(content=lookup[url],status_code=200,url=url))
    assert result['status'] == 'DATA_LIMITED'
    assert result['reason_codes'] == ['RULE_CLOCK_UNPROVEN']
    assert 'private-secret' not in json.dumps(result)


def test_thread_start_failure_is_isolated_from_owner(approved, monkeypatch):
    def bad_start(self):
        raise RuntimeError('private-thread-error')
    monkeypatch.setattr(mod.threading.Thread,'start',bad_start)
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',clock=lambda:NOW,
        fetch_response=lambda *args:None)
    assert result['status'] == 'DATA_LIMITED'
    assert result['reason_codes'] == ['RULE_PREFLIGHT_WORKER_START_FAILED']
    assert result['source_requests_started'] == 0
    assert 'private-thread-error' not in json.dumps(result)


def test_clock_block_past_deadline_never_starts_a_source_request(approved):
    release = threading.Event()
    clock_entered = threading.Event()
    clock_calls = [0]
    source_calls = []
    def clock():
        clock_calls[0] += 1
        if clock_calls[0] == 2:
            clock_entered.set()
            release.wait(1)
        return NOW
    def fetch(url,timeout):
        source_calls.append(url)
        return SimpleNamespace(content=b'unneeded',status_code=200,url=url)
    result = mod.fetch_rule_preflight(approved,trade_date='2026-10-12',clock=clock,
        fetch_response=fetch,budget_seconds=.03)
    assert clock_entered.is_set()
    assert result['status'] == 'DATA_LIMITED'
    assert result['source_requests_started'] == 0
    release.set()
    time.sleep(.02)
    assert source_calls == []
