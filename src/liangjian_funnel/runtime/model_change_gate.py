"""Offline model-quality original gate; no model/DB/claim/marker operations.

No compatibility is claimed for old aggregate-only receipts. New bindings:
head, source_trade_date, execution_trade_date, quality_run_id,
provider_base_url, models={A2,A3,A6,PM:model}, role_run_ids={role:actual_run_id}.
The latter preserves real differing research/morning IDs. PM's original row
stays role=A3/session=PM_PREMARKET; A6 stays A6/session=PREMARKET.

Aggregate schema ``model-change-quality/0589-v2`` includes all binding fields,
actual_http, calls[{quality_role,call_id,logical_batch_id,receipt_path,
receipt_sha256}] and transport_ledger={path,sha256}. Each referenced schema
``model-quality-call-original/0589-v2`` repeats the common binding and carries
quality_role, logical_batch_id, correction_attempt, request_sha256, unaltered
journal_row (the complete actual model_calls row), model_call_evidence
{call_id,catalog_json,catalog_sha256}. All correction originals are retained.
Within each logical batch, correction_attempt is exactly 1..N (N<=3): only
the last call is selected and must have raw validated=1/issues=[] and the
correct output envelope. Earlier calls must have raw validated=0/nonempty
issues; their erroneous output/envelope is preserved, not repaired or blessed.
A successful answer cannot be followed by another schema attempt.

The independent ``model-quality-http-ledger/0589-v2`` repeats the common
binding, capture_kind=LIVE_PROVIDER_DISPATCH, actual_http, max_client_attempts,
dispatches. Dispatches preserve the actual transport journal fields: ordinal,
call_id, run_id, input_hash, request_sha256, schema_correction_attempt,
is_429_retry, dispatch_reserved, actual_wall, response_status. Optional richer
identity fields are checked when present, never invented to fill absent data.
Every 429 has rejection_receipt={path,sha256} or the original guard's
rejection_evidence={receipt_path,receipt_sha256,body_sha256}; only complete
redacted rejection originals may precede
an exact same-call/request retry. Limits: <=4 retries/call, <=12 globally,
<=24 actual dispatches, <=3 schema calls/batch and <=3 A2/A6/PM total.

HTTP count comes only from original dispatches. The journal's provider_attempts
is compared with distinct real client_attempt_index values when captured.
Without that telemetry, only provider_attempts=1 plus the guarded
max_client_attempts=1 binding is accepted. It is never guessed to equal HTTP
count: process-local 429 retries may share one ModelClient attempt.
Missing dispatch originals are missing evidence, never an inferred pass.
"""
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit
import hashlib
import json
import re

ROLES = {'A2': ('A2', 'CLOSE_REVIEW'), 'A3': ('A3', 'PLAN_REVIEW'),
         'A6': ('A6', 'PREMARKET'), 'PM': ('A3', 'PM_PREMARKET')}
COMMON = ('head', 'source_trade_date', 'execution_trade_date', 'quality_run_id', 'provider_base_url', 'models', 'role_run_ids')


def _need(ok, reason):
    if not ok:
        raise ValueError('MODEL_GATE_' + reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _need(key not in result, 'DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def _json(raw):
    return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=lambda _: _need(False, 'NONFINITE_JSON'))


def _original(path, expected_sha, *, label):
    _need(_hash(expected_sha), label + '_SHA_FORMAT')
    path = Path(path)
    _need(path.is_absolute() and path.is_file() and not path.is_symlink(), label + '_ORIGINAL_PATH')
    raw = path.read_bytes()
    _need(_sha(raw) == expected_sha, label + '_ORIGINAL_MISMATCH')
    result = _json(raw)
    _need(isinstance(result, dict), label + '_ORIGINAL_NOT_OBJECT')
    return result


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 256


def _time(value):
    _need(isinstance(value, str), 'HTTP_TIME_MISSING')
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        _need(False, 'HTTP_TIME_INVALID')
    _need(result.utcoffset() is not None, 'HTTP_TIME_NOT_AWARE')
    return result


def _same_common(body, expected):
    _need(all(body.get(key) == expected[key] for key in COMMON), 'ORIGINAL_COMMON_BINDING_MISMATCH')


def _validate_call(ref, expected):
    original = _original(ref['receipt_path'], ref['receipt_sha256'], label='CALL')
    _need(original.get('schema_version') == 'model-quality-call-original/0589-v2', 'CALL_ORIGINAL_SCHEMA')
    _same_common(original, expected)
    role, cid = ref.get('quality_role'), ref.get('call_id')
    _need(role in ROLES and _text(cid), 'CALL_ROLE_OR_ID_INVALID')
    _need(original.get('quality_role') == role and original.get('logical_batch_id') == ref.get('logical_batch_id')
          and _hash(ref.get('logical_batch_id')), 'CALL_BATCH_OR_ROLE_BINDING')
    row = original.get('journal_row')
    _need(isinstance(row, dict), 'JOURNAL_ORIGINAL_ROW_MISSING')
    journal_role, session = ROLES[role]
    fields = dict(call_id=cid, role=journal_role, session=session, model=expected['models'][role],
                  run_id=expected['role_run_ids'][role], source_trade_date=expected['source_trade_date'],
                  execution_trade_date=expected['execution_trade_date'])
    _need(all(row.get(key) == value for key, value in fields.items()), 'JOURNAL_IDENTITY_BINDING_MISMATCH')
    issues = _json(row.get('issues_json', 'null'))
    _need(type(row.get('validated')) is int and row['validated'] in (0, 1) and isinstance(issues, list)
          and ((row['validated'] == 1 and issues == []) or (row['validated'] == 0 and bool(issues))),
          'RAW_NOT_VALIDATED_OR_FAILURE_ISSUES_MISSING_' + role)
    for key in ('input_hash', 'catalog_hash', 'prompt_hash', 'output_hash', 'binding_sha256'):
        _need(_hash(row.get(key)), 'JOURNAL_HASH_FORMAT_' + key)
    _need(type(row.get('provider_attempts')) is int and 1 <= row['provider_attempts'] <= 5, 'PROVIDER_ATTEMPTS_MISSING')
    _need(type(row.get('latency_ms')) is int and row['latency_ms'] >= 0, 'LATENCY_MISSING')
    _need(type(row.get('sentinel_count')) is int and row['sentinel_count'] >= 0, 'JOURNAL_SENTINEL_COUNT_MISSING')
    _time(row.get('captured_at'))
    output_json = row.get('output_json')
    _need(isinstance(output_json, str), 'RAW_OUTPUT_MISSING')
    output = _json(output_json)
    _need(_canonical(output) == output_json and _sha(output_json.encode()) == row['output_hash'],
          'RAW_OUTPUT_HASH_MISMATCH')
    evidence = original.get('model_call_evidence')
    _need(isinstance(evidence, dict) and evidence.get('call_id') == cid and evidence.get('catalog_sha256') == row['catalog_hash']
          and isinstance(evidence.get('catalog_json'), str), 'CATALOG_ORIGINAL_MISSING')
    catalog = _json(evidence['catalog_json'])
    _need(isinstance(catalog, dict) and _canonical(catalog) == evidence['catalog_json']
          and _sha(evidence['catalog_json'].encode()) == row['catalog_hash'], 'CATALOG_ORIGINAL_HASH_MISMATCH')
    attempt = original.get('correction_attempt')
    _need(type(attempt) is int and 1 <= attempt <= 3 and _hash(original.get('request_sha256')), 'CALL_REQUEST_OR_ATTEMPT_INVALID')
    return dict(quality_role=role, call_id=cid, logical_batch_id=original['logical_batch_id'], correction_attempt=attempt,
                request_sha256=original['request_sha256'], journal_row=row, output=output)


def _validate_batch(unit):
    _need(len(unit) <= 3 and [c['correction_attempt'] for c in unit] == list(range(1, len(unit) + 1)),
          'SCHEMA_BATCH_BUDGET_OR_SEQUENCE')
    _need(len({c['journal_row']['catalog_hash'] for c in unit}) == 1, 'SCHEMA_BATCH_CATALOG_CHANGED')
    for prior in unit[:-1]:
        _need(prior['journal_row']['validated'] == 0 and bool(_json(prior['journal_row']['issues_json'])),
              'SCHEMA_RETRY_AFTER_SUCCESS')
    selected = unit[-1]
    row, output = selected['journal_row'], selected['output']
    _need(row['validated'] == 1 and _json(row['issues_json']) == [], 'BATCH_FINAL_RAW_NOT_VALIDATED')
    envelope = output.get('envelope') if isinstance(output, dict) else None
    expected = dict(stage_id=row['role'], model_name=row['model'], run_id=row['run_id'],
                    source_trade_date=row['source_trade_date'], execution_trade_date=row['execution_trade_date'])
    _need(isinstance(envelope, dict) and all(envelope.get(key) == value for key, value in expected.items()),
          'RAW_OUTPUT_ENVELOPE_BINDING')


def _validate_dispatches(ledger, calls, expected):
    _need(ledger.get('schema_version') == 'model-quality-http-ledger/0589-v2'
          and ledger.get('capture_kind') == 'LIVE_PROVIDER_DISPATCH', 'HTTP_ORIGINAL_LEDGER_REQUIRED')
    _same_common(ledger, expected)
    rows = ledger.get('dispatches')
    _need(isinstance(rows, list) and 0 < len(rows) <= 24 and type(ledger.get('actual_http')) is int
          and ledger['actual_http'] == len(rows), 'HTTP_ACTUAL_COUNT_OR_BUDGET')
    grouped, ids, retries, last_started, last_unit_call = defaultdict(list), set(), 0, None, {}
    for ordinal, row in enumerate(rows, 1):
        _need(isinstance(row, dict) and type(row.get('ordinal')) is int and row['ordinal'] == ordinal, 'HTTP_ORDINAL_GAP')
        dispatch_id, cid = row.get('dispatch_id', str(ordinal)), row.get('call_id')
        _need(_text(dispatch_id) and dispatch_id not in ids and cid in calls, 'HTTP_DUPLICATE_OR_UNREFERENCED_DISPATCH')
        ids.add(dispatch_id)
        call, prior = calls[cid], grouped[cid]
        journal = call['journal_row']
        unit_key = (call['quality_role'], call['logical_batch_id'])
        previous_id = last_unit_call.get(unit_key)
        if not prior:
            expected_attempt = calls[previous_id]['correction_attempt'] + 1 if previous_id else 1
            _need(call['correction_attempt'] == expected_attempt and (not previous_id or
                  grouped[previous_id][-1]['response_status'] == 200 and calls[previous_id]['journal_row']['validated'] == 0),
                  'HTTP_SCHEMA_CORRECTION_ORDER_OR_PRIOR_NOT_FAILED')
            last_unit_call[unit_key] = cid
        else:
            _need(previous_id == cid, 'HTTP_RETURNED_TO_OLDER_SCHEMA_CALL')
        required = dict(call_id=cid, run_id=journal['run_id'], input_hash=journal['input_hash'],
                        request_sha256=call['request_sha256'], schema_correction_attempt=call['correction_attempt'])
        optional = {key: call[key] for key in ('quality_role', 'logical_batch_id')}
        optional.update({key: journal[key] for key in ('model', 'role', 'session', 'source_trade_date',
                                                       'execution_trade_date', 'catalog_hash', 'prompt_hash')})
        optional.update(head=expected['head'], quality_run_id=expected['quality_run_id'], provider_base_url=expected['provider_base_url'])
        _need(all(row.get(key) == value for key, value in required.items()) and
              all(key not in row or row[key] == value for key, value in optional.items()), 'HTTP_REQUEST_IDENTITY_OR_SHA_MISMATCH')
        started = _time(row.get('actual_wall'))
        _need(last_started is None or started >= last_started, 'HTTP_ORIGINAL_TIME_ORDER')
        last_started = started
        _need(len(prior) < 5 and ('http_attempt_index' not in row or type(row['http_attempt_index']) is int
              and row['http_attempt_index'] == len(prior) + 1),
              'HTTP_ATTEMPT_SEQUENCE_OR_PER_CALL_BUDGET')
        _need(row.get('dispatch_reserved') is True and type(row.get('response_status')) is int
              and row['response_status'] in {200, 429} and row.get('is_429_retry') is bool(prior)
              and ('response_complete' not in row or row['response_complete'] is True), 'HTTP_INCOMPLETE_OR_UNSAFE_RESPONSE')
        if prior:
            _need(prior[-1]['response_status'] == 429, 'HTTP_RETRY_WITHOUT_COMPLETE_429')
            retries += 1
            _need(retries <= 12, 'HTTP_GLOBAL429_BUDGET')
        if row['response_status'] == 429:
            reject = row.get('rejection_receipt')
            if reject is None and isinstance(row.get('rejection_evidence'), dict):
                raw_ref = row['rejection_evidence']
                reject = dict(path=raw_ref.get('receipt_path'), sha256=raw_ref.get('receipt_sha256'))
            _need(isinstance(reject, dict), 'HTTP_429_ORIGINAL_REJECTION_MISSING')
            error = _original(reject['path'], reject['sha256'], label='HTTP_REJECTION')
            request_binding = error.get('request_binding')
            _need(error.get('schema_version') == 'provider-rejection-evidence/1' and error.get('status_code') == 429
                  and type(error.get('body_bytes')) is int and error['body_bytes'] >= 0 and 'body' in error
                  and isinstance(request_binding, dict) and all(request_binding.get(key) == journal[value]
                    for key, value in [('model', 'model'), ('stage', 'role'), ('session', 'session'),
                                       ('input_hash', 'input_hash'), ('prompt_hash', 'prompt_hash')])
                  and _hash(error.get('body_sha256'))
                  and ('response_body_sha256' not in row or error['body_sha256'] == row['response_body_sha256'])
                  and not row.get('evidence_failure')
                  and not row.get('close_failure'), 'HTTP_429_ORIGINAL_REJECTION_BINDING')
        prior.append(row)
    _need(set(grouped) == set(calls), 'HTTP_ACTUAL_DISPATCH_MISSING')
    for cid, wire in grouped.items():
        _need(wire[-1]['response_status'] == 200, 'HTTP_CALL_NOT_SUCCESSFULLY_TERMINATED')
        attempts = calls[cid]['journal_row']['provider_attempts']
        if all(type(r.get('client_attempt_index')) is int and r['client_attempt_index'] >= 1 for r in wire):
            _need({r['client_attempt_index'] for r in wire} == set(range(1, attempts + 1)), 'HTTP_PROVIDER_ATTEMPTS_NOT_PROVEN')
        else:
            _need(not any('client_attempt_index' in r for r in wire) and attempts == 1
                  and type(ledger.get('max_client_attempts')) is int and ledger['max_client_attempts'] == 1,
                  'HTTP_PROVIDER_ATTEMPTS_NOT_PROVEN')
    return len(rows)


def _validate_model_change_gate(path, *, expected_sha256, head, source_trade_date, execution_trade_date,
                                quality_run_id, role_run_ids, provider_base_url, models):
    """Only pinned original files; new mandatory kwargs are external v2 pins."""
    _need(isinstance(head, str) and re.fullmatch('[0-9a-f]{40}', head), 'HEAD_FORMAT')
    for value in (source_trade_date, execution_trade_date):
        _need(isinstance(value, str) and date.fromisoformat(value).isoformat() == value, 'DATE_FORMAT')
    _need(execution_trade_date > source_trade_date, 'EXECUTION_DATE_NOT_AFTER_SOURCE')
    _need(_text(quality_run_id) and isinstance(models, dict) and set(models) == set(ROLES)
          and all(_text(value) for value in models.values()), 'MODEL_ROLE_BINDING_INVALID')
    _need(isinstance(role_run_ids, dict) and set(role_run_ids) == set(ROLES)
          and all(_text(value) for value in role_run_ids.values()), 'ROLE_RUN_IDS_REQUIRED')
    url = urlsplit(provider_base_url)
    _need(url.scheme == 'https' and url.hostname and not url.username and not url.password and not url.query and not url.fragment,
          'PROVIDER_ENDPOINT_BINDING_INVALID')
    expected = dict(head=head, source_trade_date=source_trade_date, execution_trade_date=execution_trade_date,
                    quality_run_id=quality_run_id, provider_base_url=provider_base_url, models=models, role_run_ids=role_run_ids)
    body = _original(path, expected_sha256, label='AGGREGATE')
    _need(body.get('schema_version') == 'model-change-quality/0589-v2', 'AGGREGATE_SCHEMA_NOT_ORIGINAL_PROOF')
    _same_common(body, expected)
    refs = body.get('calls')
    _need(isinstance(refs, list) and 4 <= len(refs) <= 24, 'CALLS_MISSING_OR_UNBOUNDED')
    calls, batches, roles = {}, defaultdict(list), defaultdict(list)
    for ref in refs:
        _need(isinstance(ref, dict), 'CALL_REFERENCE_INVALID')
        call = _validate_call(ref, expected)
        _need(call['call_id'] not in calls, 'DUPLICATE_CALL_ID')
        calls[call['call_id']] = call
        batches[(call['quality_role'], call['logical_batch_id'])].append(call)
        roles[call['quality_role']].append(call)
    _need(set(roles) == set(ROLES), 'REQUIRED_ROLE_ORIGINAL_MISSING')
    for unit in batches.values():
        _validate_batch(unit)
    _need(all(len(roles[role]) <= 3 for role in ('A2', 'A6', 'PM')), 'ROLE_TOTAL3_BUDGET')
    ref = body.get('transport_ledger')
    _need(isinstance(ref, dict), 'HTTP_ORIGINAL_LEDGER_MISSING')
    ledger = _original(ref['path'], ref['sha256'], label='HTTP_LEDGER')
    actual = _validate_dispatches(ledger, calls, expected)
    _need(type(body.get('actual_http')) is int and body['actual_http'] == actual, 'AGGREGATE_HTTP_COUNT_MISMATCH')
    return body


def validate_model_change_gate(path, *, expected_sha256, head, source_trade_date, execution_trade_date,
                               quality_run_id, role_run_ids, provider_base_url, models):
    """Validate v2 originals; malformed/missing proof is a named failure."""
    try:
        return _validate_model_change_gate(path, expected_sha256=expected_sha256, head=head,
            source_trade_date=source_trade_date, execution_trade_date=execution_trade_date,
            quality_run_id=quality_run_id, role_run_ids=role_run_ids, provider_base_url=provider_base_url, models=models)
    except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError('MODEL_GATE_REQUIRED_ORIGINAL_FIELDS_MISSING_OR_INVALID') from None
