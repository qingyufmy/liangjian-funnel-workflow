"""Night maintenance is cache work, never admission or publication."""
from copy import deepcopy
from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.close_scope import seal_scope_receipt
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.pipeline.disclosure_maintenance import (
    build_maintenance_queue, run_maintenance, validate_queue, seal_maintenance_queue,
)
from test_wp5_disclosure_prefilter import build

NOW = datetime(2026, 10, 9, 22, tzinfo=ZoneInfo('Asia/Shanghai'))


def inputs(tmp_path, size=3):
    symbols = [f'{i:06d}.SZ' for i in range(size)]
    path = seal_scope_receipt(root=tmp_path, run_id='original-close',
        research_as_of=NOW.replace(hour=15, minute=10),
        market_data_as_of=NOW.replace(hour=15, minute=10),
        a1_reference={'generation_id': 'original-a1', 'payload_hash': 'f'*64},
        a1_symbols=symbols[:-2], hot_payload={'records': [{'symbol': symbols[-2]}]},
        discovery={'records': [{'symbol': symbols[-1], 'review_budget_selected': True}]},
        g0_symbols=symbols, selected_symbols=symbols)
    scope = json.loads(path.read_text(encoding='utf-8'))
    # Fixture observation time, never a rewritten production receipt.
    scope['recorded_at'] = NOW.replace(hour=15, minute=11).isoformat()
    rehash(scope, 'receipt_hash')
    prefilter = build(symbols, hot_symbols=[symbols[-2]], discovery_symbols=[symbols[-1]])
    return scope, prefilter


def queue(tmp_path, size=3):
    return build_maintenance_queue(*inputs(tmp_path, size))


def rehash(value, key):
    value[key] = content_hash({k: v for k, v in value.items() if k != key})


def test_queue_binds_original_generation_and_partition_not_final_a2(tmp_path):
    value = queue(tmp_path)
    assert value['a1_reference']['generation_id'] == 'original-a1'
    assert value['deferred_symbols'] == ['000000.SZ']
    assert value['candidate_symbols'] == ['000001.SZ', '000002.SZ']
    assert value['mode'] == 'SHADOW'
    assert value['execution_authority'] is False
    validate_queue(value)


def test_queue_has_no_candidate_or_night_work_cap(tmp_path):
    value = queue(tmp_path, 403)
    assert len(value['deferred_symbols']) == 401


def test_queue_seal_does_not_overwrite_conflicting_original(tmp_path):
    scope, prefilter = inputs(tmp_path)
    path = seal_maintenance_queue(tmp_path/'queues', scope, prefilter)
    original = path.read_bytes()
    assert seal_maintenance_queue(tmp_path/'queues', scope, prefilter) == path
    assert path.read_bytes() == original
    path.write_text('{}')
    with pytest.raises(ValueError, match='MAINTENANCE_QUEUE_FILE_CONFLICT'):
        seal_maintenance_queue(tmp_path/'queues', scope, prefilter)
    assert path.read_text() == '{}'


@pytest.mark.parametrize('fault', ['receipt_hash', 'prefilter_hash', 'partition', 'source', 'day', 'unbound'])
def test_invalid_inputs_cannot_create_a_night_scope(tmp_path, fault):
    scope, prefilter = inputs(tmp_path)
    if fault == 'receipt_hash':
        scope['a1_reference']['generation_id'] = 'later-a1'
    elif fault == 'prefilter_hash':
        prefilter['deferred_symbols'] = []
    elif fault == 'partition':
        prefilter['deferred_symbols'] += prefilter['candidate_symbols']
        rehash(prefilter, 'scope_hash')
    elif fault == 'source':
        prefilter['source_sets']['discovery'] = []
        rehash(prefilter, 'scope_hash')
    elif fault == 'day':
        prefilter['trade_date'] = '2026-10-08'
        rehash(prefilter, 'scope_hash')
    else:
        scope['a1_reference'] = None
        scope['binding_status'] = 'UNBOUND_A1_REFERENCE'
        rehash(scope, 'receipt_hash')
    with pytest.raises(ValueError):
        build_maintenance_queue(scope, prefilter)


def test_dry_run_does_not_fetch_or_create_cache_or_output(tmp_path):
    value = queue(tmp_path)
    target = tmp_path/'not-created'
    report = run_maintenance(value, output_dir=target, now=NOW)
    assert report['status'] == 'DRY_RUN'
    assert report['query_end'] == '2026-10-09'
    assert not target.exists()


def test_failure_is_separate_from_original_scope_and_never_ready(tmp_path):
    value = queue(tmp_path)
    original = deepcopy(value)
    def fail(symbol, start, end, business):
        raise TimeoutError('secret-url-do-not-persist')
    report = run_maintenance(value, output_dir=tmp_path/'reports', now=NOW,
        execute=True, collect=fail, can_reuse=lambda row, now: False)
    assert report['status'] == 'PARTIAL_FAILURE'
    assert report['failed_symbols'] == ['000000.SZ']
    assert report['rows'][0]['reason_code'] == 'TimeoutError'
    assert 'secret-url' not in json.dumps(report)
    assert value == original
    assert report['production_plans_changed'] is False


def success(symbol, start, end, business):
    return {'symbol': symbol, 'ok': True, 'reason_code': 'CACHE_WARMED',
            'recent_complete': True, 'business_complete': True, 'pdf_complete': True}


def test_resume_requires_same_date_fresh_receipt_and_verified_cache(tmp_path):
    value = queue(tmp_path)
    target = tmp_path/'reports'
    first = run_maintenance(value, output_dir=target, now=NOW,
        execute=True, collect=success, can_reuse=lambda row, now: True)
    def unexpected(*args):
        raise AssertionError('resume must not call collector')
    second = run_maintenance(value, output_dir=target, now=NOW+timedelta(minutes=1),
        execute=True, collect=unexpected, can_reuse=lambda row, now: True)
    assert first['status'] == second['status'] == 'CACHE_WARMED'
    assert second['resumed_symbols'] == ['000000.SZ']
    # Cache deletion/revision must defeat a previously successful receipt.
    third = run_maintenance(value, output_dir=target, now=NOW+timedelta(minutes=2),
        execute=True, collect=success, can_reuse=lambda row, now: False)
    assert third['resumed_symbols'] == []
    next_day = run_maintenance(value, output_dir=target, now=NOW+timedelta(days=1),
        execute=True, collect=success, can_reuse=lambda row, now: True)
    assert next_day['query_end'] == '2026-10-10'
    assert next_day['resumed_symbols'] == []
    assert len(list(target.glob('*report*.json'))) == 4


def test_incomplete_lane_cannot_be_success_or_resumed(tmp_path):
    value = queue(tmp_path)
    def incomplete(*args):
        return {**success(*args), 'business_complete': False}
    report = run_maintenance(value, output_dir=tmp_path/'reports', now=NOW,
        execute=True, collect=incomplete, can_reuse=lambda row, now: True)
    assert report['status'] == 'PARTIAL_FAILURE'
    assert report['rows'][0]['ok'] is False


def test_night_success_does_not_require_or_certify_recent_risk_announcements(tmp_path):
    def business_only(*args):
        return {**success(*args), 'recent_complete': False, 'recent_required': False}
    report = run_maintenance(queue(tmp_path), output_dir=tmp_path/'reports', now=NOW,
        execute=True, collect=business_only, can_reuse=lambda row, now: False)
    assert report['status'] == 'CACHE_WARMED'
    assert report['maintenance_scope'] == 'BUSINESS_AND_PDF_ONLY'
    assert report['rows'][0]['recent_complete'] is False


def test_lock_and_corrupt_resume_fail_closed(tmp_path):
    value = queue(tmp_path)
    target = tmp_path/'reports'
    target.mkdir()
    lock = target/f"{value['queue_hash']}.lock"
    lock.write_text('other owner')
    with pytest.raises(ValueError, match='MAINTENANCE_ALREADY_RUNNING'):
        run_maintenance(value, output_dir=target, now=NOW, execute=True,
            collect=success, can_reuse=lambda row, now: True)
    assert lock.read_text() == 'other owner'
    lock.unlink()
    report = run_maintenance(value, output_dir=target, now=NOW, execute=True,
        collect=success, can_reuse=lambda row, now: True)
    attempt = next(target.glob('*attempt*.json'))
    data = json.loads(attempt.read_text())
    data['row']['ok'] = False
    attempt.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='MAINTENANCE_ATTEMPT_HASH_MISMATCH'):
        run_maintenance(value, output_dir=target, now=NOW, execute=True,
            collect=success, can_reuse=lambda row, now: True)
    assert report['status'] == 'CACHE_WARMED'


def test_cli_dry_run_never_loads_settings_or_workflow(tmp_path, monkeypatch, capsys):
    value = queue(tmp_path)
    path = tmp_path/'queue.json'
    path.write_text(json.dumps(value))
    spec = importlib.util.spec_from_file_location('maintenance_script',
        Path(__file__).resolve().parents[1]/'scripts/run_disclosure_maintenance.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    def fail(*args):
        raise AssertionError('dry run must not load the production adapter')
    monkeypatch.setattr(module, 'open_collector', fail)
    assert module.main(['--queue', str(path), '--output-dir', str(tmp_path/'absent')]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'DRY_RUN'
    assert not (tmp_path/'absent').exists()
