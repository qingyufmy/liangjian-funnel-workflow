"""Declared offline originals/ledger fixtures. No actual HTTP/VM/model calls."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json

import pytest

from liangjian_funnel.runtime import model_change_gate as m


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def hashed(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical(value), encoding='utf-8')
    return dict(path=str(path.resolve()), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def quality_fixture(tmp_path, *, head='c' * 40, source_trade_date='2026-10-22', execution_trade_date='2026-10-23',
                    quality_run_id='DECLARED_OFFLINE_QUALITY', provider_base_url='https://declared.invalid/v1',
                    models=None, role_run_ids=None):
    common = dict(head=head, source_trade_date=source_trade_date, execution_trade_date=execution_trade_date,
        quality_run_id=quality_run_id, provider_base_url=provider_base_url,
        models=models or {role: 'DECLARED_PRO' for role in m.ROLES},
        role_run_ids=role_run_ids or {role: 'DECLARED_' + role + '_REAL_ID' for role in m.ROLES})
    calls, dispatches = [], []
    originals = {}
    now = datetime(2026, 10, 22, 20, tzinfo=timezone.utc)
    for index, (role, (journal_role, session)) in enumerate(m.ROLES.items(), 1):
        cid = 'DECLARED_CALL_' + role
        batch = hashed({'declared': role})
        envelope = dict(stage_id=journal_role, model_name=common['models'][role], run_id=common['role_run_ids'][role],
                        source_trade_date=source_trade_date, execution_trade_date=execution_trade_date)
        output = dict(envelope=envelope, declared_offline_output=role)
        catalog = dict(items=[dict(id=role + ':DECLARED_ORIGINAL', value={'number': index})])
        row = dict(call_id=cid, run_id=common['role_run_ids'][role], role=journal_role, session=session,
            source_trade_date=source_trade_date, execution_trade_date=execution_trade_date, captured_at=(now + timedelta(seconds=index + 1)).isoformat(),
            model=common['models'][role], prompt_hash=hashed(['prompt', role]), input_hash=hashed(['input', role]),
            catalog_hash=hashed(catalog), output_hash=hashed(output), latency_ms=123, provider_attempts=1,
            validated=1, issues_json='[]', sentinel_count=0, output_json=canonical(output), binding_sha256=hashed(['binding', role]))
        original = dict(common, schema_version='model-quality-call-original/0589-v2', quality_role=role,
            logical_batch_id=batch, correction_attempt=1, request_sha256=hashed(['request', role]), journal_row=row,
            model_call_evidence=dict(call_id=cid, catalog_json=canonical(catalog), catalog_sha256=hashed(catalog)))
        ref = put(tmp_path / (role + '-original.json'), original)
        calls.append(dict(quality_role=role, call_id=cid, logical_batch_id=batch,
                          receipt_path=ref['path'], receipt_sha256=ref['sha256']))
        originals[role] = original
        dispatch = {key: row[key] for key in ('run_id', 'model', 'role', 'session', 'source_trade_date',
                                              'execution_trade_date', 'input_hash', 'catalog_hash', 'prompt_hash')}
        dispatch.update(head=head, quality_run_id=quality_run_id, provider_base_url=provider_base_url,
            quality_role=role, call_id=cid, logical_batch_id=batch, correction_attempt=1,
            request_sha256=original['request_sha256'], ordinal=index, dispatch_id='DECLARED_DISPATCH_' + str(index),
            client_attempt_index=1, http_attempt_index=1, response_status=200, response_complete=True,
            dispatch_reserved=True, schema_correction_attempt=1, is_429_retry=False,
            response_body_sha256=hashed(['wire-response', role]), started_at=(now + timedelta(seconds=index)).isoformat(),
            actual_wall=(now + timedelta(seconds=index)).isoformat(), finished_at=(now + timedelta(seconds=index, milliseconds=123)).isoformat())
        dispatches.append(dispatch)
    ledger = dict(common, schema_version='model-quality-http-ledger/0589-v2', capture_kind='LIVE_PROVIDER_DISPATCH',
                  actual_http=len(dispatches), max_client_attempts=1, dispatches=dispatches)
    ledger_ref = put(tmp_path / 'transport-original.json', ledger)
    aggregate = dict(common, schema_version='model-change-quality/0589-v2', actual_http=len(dispatches), calls=calls,
                     transport_ledger=ledger_ref)
    ref = put(tmp_path / 'aggregate.json', aggregate)
    return dict(common=common, originals=originals, ledger=ledger, aggregate=aggregate, ref=ref, root=tmp_path)


def update(fixture, *, role=None, ledger=False):
    if role:
        ref = put(fixture['root'] / (role + '-original.json'), fixture['originals'][role])
        next(r for r in fixture['aggregate']['calls'] if r['quality_role'] == role).update(receipt_path=ref['path'], receipt_sha256=ref['sha256'])
    if ledger:
        fixture['aggregate']['transport_ledger'] = put(fixture['root'] / 'transport-original.json', fixture['ledger'])
    fixture['ref'] = put(fixture['root'] / 'aggregate.json', fixture['aggregate'])


def validate(f):
    return m.validate_model_change_gate(f['ref']['path'], expected_sha256=f['ref']['sha256'], **f['common'])


def test_complete_originals_and_actual_dispatches_pass(tmp_path):
    f = quality_fixture(tmp_path)
    assert validate(f)['actual_http'] == 4
    assert f['originals']['PM']['journal_row']['role'] == 'A3'
    assert f['originals']['PM']['journal_row']['session'] == 'PM_PREMARKET'


def add_schema_correction(f, role='A2', *, previous_succeeded=False):
    """Preserve an actual failed original and add its separate corrected call."""
    refs = [r for r in f['aggregate']['calls'] if r['quality_role'] == role]
    prior_ref = refs[-1]
    prior = json.loads(Path(prior_ref['receipt_path']).read_bytes())
    next_original = deepcopy(prior)
    old_id = prior['journal_row']['call_id']
    if not previous_succeeded:
        row = prior['journal_row']
        output = json.loads(row['output_json'])
        output['envelope']['model_name'] = 'DECLARED_ORIGINAL_WRONG_ENVELOPE'
        row.update(validated=0, issues_json=canonical(['TEAM_ENVELOPE_MODEL_MISMATCH']),
                   output_json=canonical(output), output_hash=hashed(output))
        prior_new_ref = put(Path(prior_ref['receipt_path']), prior)
        prior_ref['receipt_sha256'] = prior_new_ref['sha256']
    attempt = next_original['correction_attempt'] + 1
    new_id = 'DECLARED_CORRECTED_' + role + '_' + str(attempt)
    next_original['correction_attempt'] = attempt
    next_original['request_sha256'] = hashed(['request', role, attempt])
    row = next_original['journal_row']
    # The prior reply may itself have been a correction; final raw is restored
    # faithfully as the separate successful response, never rewriting prior.
    output = json.loads(row['output_json'])
    output['envelope']['model_name'] = f['common']['models'][role]
    row.update(call_id=new_id, input_hash=hashed(['input', role, attempt]), validated=1, issues_json='[]',
               output_json=canonical(output), output_hash=hashed(output), binding_sha256=hashed(['new-binding', role, attempt]))
    next_original['model_call_evidence']['call_id'] = new_id
    ref = put(f['root'] / (role + '-correction-' + str(attempt) + '.json'), next_original)
    index = f['aggregate']['calls'].index(prior_ref) + 1
    f['aggregate']['calls'].insert(index, dict(quality_role=role, call_id=new_id, logical_batch_id=next_original['logical_batch_id'],
        receipt_path=ref['path'], receipt_sha256=ref['sha256']))
    wires = f['ledger']['dispatches']
    previous_wire = next(w for w in reversed(wires) if w['call_id'] == old_id)
    wire = deepcopy(previous_wire)
    wire.update(call_id=new_id, dispatch_id='DECLARED_CORRECTED_WIRE_' + role + '_' + str(attempt),
        input_hash=row['input_hash'], schema_correction_attempt=attempt, correction_attempt=attempt,
        request_sha256=next_original['request_sha256'], http_attempt_index=1, is_429_retry=False,
        response_body_sha256=hashed(['corrected-output-wire', role, attempt]))
    wires.insert(wires.index(previous_wire) + 1, wire)
    now = datetime(2026, 10, 22, 20, tzinfo=timezone.utc)
    for ordinal, item in enumerate(wires, 1):
        item.update(ordinal=ordinal, actual_wall=(now + timedelta(seconds=ordinal)).isoformat())
    f['ledger']['actual_http'] = f['aggregate']['actual_http'] = len(wires)
    update(f, ledger=True)
    return prior_ref, f['aggregate']['calls'][index]


@pytest.mark.parametrize('role', tuple(m.ROLES))
def test_legal_schema_correction_keeps_original_failure_and_wrong_envelope(tmp_path, role):
    f = quality_fixture(tmp_path)
    prior, selected = add_schema_correction(f, role=role)
    original = json.loads(Path(prior['receipt_path']).read_bytes())
    assert original['journal_row']['validated'] == 0
    assert 'WRONG_ENVELOPE' in original['journal_row']['output_json']
    assert selected['call_id'] != prior['call_id']
    assert validate(f)['actual_http'] == 5
    assert Path(prior['receipt_path']).read_text(encoding='utf-8') == canonical(original)


@pytest.mark.parametrize('role', tuple(m.ROLES))
def test_three_schema_attempts_final_valid_is_permitted_and_all_counted(tmp_path, role):
    f = quality_fixture(tmp_path)
    add_schema_correction(f, role=role)
    add_schema_correction(f, role=role)
    assert validate(f)['actual_http'] == 6


def test_retry_after_success_cannot_be_claimed_as_correction(tmp_path):
    f = quality_fixture(tmp_path)
    add_schema_correction(f, previous_succeeded=True)
    with pytest.raises(ValueError, match='SCHEMA_RETRY_AFTER_SUCCESS'):
        validate(f)


def test_last_answer_still_invalid_cannot_pass(tmp_path):
    f = quality_fixture(tmp_path)
    original = f['originals']['A2']
    original['journal_row'].update(validated=0, issues_json='["ACTUAL_SCHEMA_ERROR"]')
    update(f, role='A2')
    with pytest.raises(ValueError, match='BATCH_FINAL_RAW_NOT_VALIDATED'):
        validate(f)


def test_hiding_failed_call_original_does_not_hide_its_http(tmp_path):
    f = quality_fixture(tmp_path)
    prior, _ = add_schema_correction(f)
    f['aggregate']['calls'].remove(prior)
    update(f)
    with pytest.raises(ValueError, match='SCHEMA_BATCH_BUDGET_OR_SEQUENCE'):
        validate(f)


def test_hiding_failed_dispatch_is_missing_evidence_not_final_pass(tmp_path):
    f = quality_fixture(tmp_path)
    prior, _ = add_schema_correction(f)
    wires = f['ledger']['dispatches']
    wires[:] = [w for w in wires if w['call_id'] != prior['call_id']]
    for ordinal, row in enumerate(wires, 1):
        row['ordinal'] = ordinal
    f['ledger']['actual_http'] = f['aggregate']['actual_http'] = len(wires)
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_SCHEMA_CORRECTION_ORDER_OR_PRIOR_NOT_FAILED'):
        validate(f)


def test_aggregate_correction_order_must_be_actual_1_to_n(tmp_path):
    f = quality_fixture(tmp_path)
    add_schema_correction(f)
    f['aggregate']['calls'][0], f['aggregate']['calls'][1] = f['aggregate']['calls'][1], f['aggregate']['calls'][0]
    update(f)
    with pytest.raises(ValueError, match='SCHEMA_BATCH_BUDGET_OR_SEQUENCE'):
        validate(f)


def test_http_correction_order_must_match_actual_1_to_n(tmp_path):
    f = quality_fixture(tmp_path)
    add_schema_correction(f)
    rows = f['ledger']['dispatches']
    rows[0], rows[1] = rows[1], rows[0]
    for ordinal, row in enumerate(rows, 1):
        row.update(ordinal=ordinal, actual_wall=(datetime(2026, 10, 22, 20, tzinfo=timezone.utc) + timedelta(seconds=ordinal)).isoformat())
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_SCHEMA_CORRECTION_ORDER_OR_PRIOR_NOT_FAILED'):
        validate(f)


def test_four_schema_attempts_are_not_permitted(tmp_path):
    f = quality_fixture(tmp_path)
    for _ in range(3):
        add_schema_correction(f)
    with pytest.raises(ValueError, match='CALL_REQUEST_OR_ATTEMPT_INVALID'):
        validate(f)


@pytest.mark.parametrize('field,value', [('head', 'a' * 40), ('source_trade_date', '2026-10-21'),
    ('execution_trade_date', '2026-10-26'), ('quality_run_id', 'OTHER_REAL_RUN'), ('provider_base_url', 'https://other.invalid/v1')])
def test_changed_original_common_cannot_be_blessed_by_new_aggregate_sha(tmp_path, field, value):
    f = quality_fixture(tmp_path)
    f['originals']['A2'][field] = value
    update(f, role='A2')
    with pytest.raises(ValueError, match='ORIGINAL_COMMON_BINDING_MISMATCH'):
        validate(f)


@pytest.mark.parametrize('field,value', [('call_id', 'OTHER_CALL'), ('role', 'A4'), ('session', 'INTRADAY'),
    ('model', 'DECLARED_DIFFERENT_MODEL'), ('run_id', 'OTHER_RUN'), ('source_trade_date', '2026-10-21'),
    ('execution_trade_date', '2026-10-26')])
def test_actual_journal_cross_role_model_date_run_rejected(tmp_path, field, value):
    f = quality_fixture(tmp_path)
    f['originals']['A3']['journal_row'][field] = value
    update(f, role='A3')
    with pytest.raises(ValueError, match='JOURNAL_IDENTITY_BINDING_MISMATCH'):
        validate(f)


def test_duplicate_call_id_does_not_count_twice(tmp_path):
    f = quality_fixture(tmp_path)
    f['aggregate']['calls'].append(deepcopy(f['aggregate']['calls'][0]))
    update(f)
    with pytest.raises(ValueError, match='DUPLICATE_CALL_ID'):
        validate(f)


@pytest.mark.parametrize('field,value', [('validated', 0), ('validated', True), ('issues_json', '["DECLARED_ERROR"]')])
def test_only_actual_raw_validation_passes(tmp_path, field, value):
    f = quality_fixture(tmp_path)
    f['originals']['A6']['journal_row'][field] = value
    update(f, role='A6')
    with pytest.raises(ValueError, match='RAW_NOT_VALIDATED'):
        validate(f)


@pytest.mark.parametrize('field', ['input_hash', 'catalog_hash', 'prompt_hash', 'output_hash', 'binding_sha256'])
def test_hash_fields_not_freeform_labels(tmp_path, field):
    f = quality_fixture(tmp_path)
    f['originals']['A2']['journal_row'][field] = 'INVENTED'
    update(f, role='A2')
    with pytest.raises(ValueError, match='JOURNAL_HASH_FORMAT'):
        validate(f)


def test_actual_output_json_hash_not_aggregate_claim(tmp_path):
    f = quality_fixture(tmp_path)
    f['originals']['PM']['journal_row']['output_json'] = canonical({'different': 'output'})
    update(f, role='PM')
    with pytest.raises(ValueError, match='RAW_OUTPUT_HASH_MISMATCH'):
        validate(f)


def test_output_envelope_cannot_cross_model_even_with_correct_hash(tmp_path):
    f = quality_fixture(tmp_path)
    row = f['originals']['PM']['journal_row']
    output = json.loads(row['output_json'])
    output['envelope']['model_name'] = 'OTHER_MODEL'
    row.update(output_json=canonical(output), output_hash=hashed(output))
    update(f, role='PM')
    with pytest.raises(ValueError, match='RAW_OUTPUT_ENVELOPE_BINDING'):
        validate(f)


def test_actual_catalog_json_must_hash_to_original_catalog(tmp_path):
    f = quality_fixture(tmp_path)
    f['originals']['A2']['model_call_evidence']['catalog_json'] = canonical({'items': []})
    update(f, role='A2')
    with pytest.raises(ValueError, match='CATALOG_ORIGINAL_HASH_MISMATCH'):
        validate(f)


def test_old_aggregate_only_schema_is_not_an_original(tmp_path):
    f = quality_fixture(tmp_path)
    f['aggregate']['schema_version'] = 'legacy-aggregate'
    update(f)
    with pytest.raises(ValueError, match='AGGREGATE_SCHEMA_NOT_ORIGINAL_PROOF'):
        validate(f)


def test_no_transport_evidence_cannot_infer_four_successes_equal_four_http(tmp_path):
    f = quality_fixture(tmp_path)
    del f['aggregate']['transport_ledger']
    update(f)
    with pytest.raises(ValueError, match='HTTP_ORIGINAL_LEDGER_MISSING'):
        validate(f)


def test_actual_dispatch_missing_even_if_total_claimed_pass(tmp_path):
    f = quality_fixture(tmp_path)
    f['ledger']['dispatches'].pop()
    f['ledger']['actual_http'] = 3
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_ACTUAL_DISPATCH_MISSING'):
        validate(f)


@pytest.mark.parametrize('field,value', [('model', 'OTHER'), ('run_id', 'OTHER'), ('quality_role', 'PM'),
    ('request_sha256', 'a' * 64), ('input_hash', 'b' * 64), ('catalog_hash', 'c' * 64), ('head', 'd' * 40)])
def test_wire_dispatch_must_bind_exact_original_call(tmp_path, field, value):
    f = quality_fixture(tmp_path)
    f['ledger']['dispatches'][0][field] = value
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_REQUEST_IDENTITY_OR_SHA_MISMATCH'):
        validate(f)


def test_duplicate_dispatch_id_rejected(tmp_path):
    f = quality_fixture(tmp_path)
    f['ledger']['dispatches'][1]['dispatch_id'] = f['ledger']['dispatches'][0]['dispatch_id']
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_DUPLICATE_OR_UNREFERENCED_DISPATCH'):
        validate(f)


@pytest.mark.parametrize('status,complete', [(500, True), (403, True), (200, False), (429, False)])
def test_unknown_incomplete_or_unsafe_wire_is_not_a_pass(tmp_path, status, complete):
    f = quality_fixture(tmp_path)
    f['ledger']['dispatches'][0].update(response_status=status, response_complete=complete)
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_INCOMPLETE_OR_UNSAFE_RESPONSE'):
        validate(f)


def test_actual_http_budget_not_aggregate_status(tmp_path):
    f = quality_fixture(tmp_path)
    f['ledger']['dispatches'] *= 7
    f['ledger']['actual_http'] = 28
    f['aggregate']['actual_http'] = 28
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_ACTUAL_COUNT_OR_BUDGET'):
        validate(f)


def test_aggregate_http_count_cannot_understate_actual(tmp_path):
    f = quality_fixture(tmp_path)
    f['aggregate']['actual_http'] = 1
    update(f)
    with pytest.raises(ValueError, match='AGGREGATE_HTTP_COUNT_MISMATCH'):
        validate(f)


def test_provider_attempts_require_dispatch_evidence(tmp_path):
    f = quality_fixture(tmp_path)
    f['originals']['A2']['journal_row']['provider_attempts'] = 2
    update(f, role='A2')
    with pytest.raises(ValueError, match='HTTP_PROVIDER_ATTEMPTS_NOT_PROVEN'):
        validate(f)


def add_429(f, role='A2', retries=1):
    original = next(r for r in f['ledger']['dispatches'] if r['quality_role'] == role)
    prior = []
    for i in range(retries):
        row = deepcopy(original)
        body_hash = hashed(['DECLARED_429', i, role])
        error = put(f['root'] / (role + '-reject-' + str(i) + '.json'),
            dict(schema_version='provider-rejection-evidence/1', status_code=429, body_sha256=body_hash,
                body_bytes=123, body={'error': 'DECLARED'}, request_binding=dict(model=original['model'], stage=original['role'],
                session=original['session'], input_hash=original['input_hash'], prompt_hash=original['prompt_hash'])))
        row.update(dispatch_id='DECLARED_RETRY_' + role + '_' + str(i), response_status=429,
            http_attempt_index=i + 1, response_body_sha256=body_hash, rejection_receipt=error, is_429_retry=i > 0)
        prior.append(row)
    original['http_attempt_index'] = retries + 1
    original['is_429_retry'] = retries > 0
    rows = f['ledger']['dispatches']
    at = rows.index(original)
    rows[at:at] = prior
    now = datetime(2026, 10, 22, 20, tzinfo=timezone.utc)
    for index, row in enumerate(rows, 1):
        row.update(ordinal=index, started_at=(now + timedelta(seconds=index)).isoformat(),
                   actual_wall=(now + timedelta(seconds=index)).isoformat(), finished_at=(now + timedelta(seconds=index, milliseconds=100)).isoformat())
    f['ledger']['actual_http'] = f['aggregate']['actual_http'] = len(rows)
    update(f, ledger=True)


def test_429_same_call_actual_http_is_not_client_attempt_count(tmp_path):
    f = quality_fixture(tmp_path)
    add_429(f, retries=2)
    assert f['originals']['A2']['journal_row']['provider_attempts'] == 1
    assert validate(f)['actual_http'] == 6


def test_actual_0538_minimal_dispatch_fields_need_no_invented_telemetry(tmp_path):
    f = quality_fixture(tmp_path)
    add_429(f)
    keep = {'ordinal', 'call_id', 'run_id', 'input_hash', 'request_sha256', 'schema_correction_attempt',
            'is_429_retry', 'dispatch_reserved', 'actual_wall', 'response_status', 'rejection_evidence'}
    row = f['ledger']['dispatches'][0]
    error = json.loads(Path(row['rejection_receipt']['path']).read_bytes())
    row['rejection_evidence'] = dict(receipt_path=row['rejection_receipt']['path'], receipt_sha256=row['rejection_receipt']['sha256'],
        body_sha256=error['body_sha256'], status_code=429, complete_redacted_body_preserved=True)
    for row in f['ledger']['dispatches']:
        for key in list(row):
            if key not in keep:
                del row[key]
    update(f, ledger=True)
    assert validate(f)['actual_http'] == 5


def test_missing_journal_field_is_explicit_missing_evidence(tmp_path):
    f = quality_fixture(tmp_path)
    del f['originals']['A2']['journal_row']['output_json']
    update(f, role='A2')
    with pytest.raises(ValueError, match='RAW_OUTPUT_MISSING'):
        validate(f)


def test_429_without_complete_original_rejection_rejected(tmp_path):
    f = quality_fixture(tmp_path)
    add_429(f)
    del f['ledger']['dispatches'][0]['rejection_receipt']
    update(f, ledger=True)
    with pytest.raises(ValueError, match='HTTP_429_ORIGINAL_REJECTION_MISSING'):
        validate(f)


def test_more_than_four_429_retries_per_call_rejected(tmp_path):
    f = quality_fixture(tmp_path)
    add_429(f, retries=5)
    with pytest.raises(ValueError, match='HTTP_ATTEMPT_SEQUENCE_OR_PER_CALL_BUDGET'):
        validate(f)


def test_global_429_budget_cannot_spread_across_roles(tmp_path):
    f = quality_fixture(tmp_path)
    for role in m.ROLES:
        add_429(f, role=role, retries=4)
    with pytest.raises(ValueError, match='HTTP_GLOBAL429_BUDGET'):
        validate(f)


def test_actual_original_file_bytes_tamper_rejected(tmp_path):
    f = quality_fixture(tmp_path)
    Path(f['aggregate']['calls'][0]['receipt_path']).write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError, match='CALL_ORIGINAL_MISMATCH'):
        validate(f)


def test_no_fields_are_written_by_validation(tmp_path):
    f = quality_fixture(tmp_path)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    validate(f)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
