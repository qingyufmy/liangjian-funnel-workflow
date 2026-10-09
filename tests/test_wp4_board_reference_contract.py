"""Local fixed responses only: no provider, store, production route or credentials."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import importlib
import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from liangjian_funnel.data.board_reference import digest
from liangjian_funnel.data.hithink_board_reference import collect_catalog, collect_members
from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.settings import Settings

NOW = datetime(2026, 10, 10, 15, 10, tzinfo=ZoneInfo('Asia/Shanghai'))


def audit(*args, **kwargs):
    # Keep missing implementation a test failure rather than a collection failure.
    return importlib.import_module('liangjian_funnel.data.board_reference_contract').audit_board_reference(*args, **kwargs)


def fixture(tmp_path, age=0, composite=False):
    boards = [{'thscode': '881108.TI', 'name': '化学原料'}]
    if composite:
        boards.append({'thscode': '881109.TI', 'name': '化学制品'})
    bodies = {}
    def respond(request):
        rows = boards if 'catalog' in request.url.path else [
            {'thscode': '600001.SH', 'ticker': '600001', 'name': '甲'}]
        response = httpx.Response(200, json={'code': 0, 'data': {'item': rows}}, request=request)
        bodies[hashlib.sha256(response.content).hexdigest()] = response.content
        return response
    settings = Settings.from_env({'HITHINK_FINANCE_API_KEY': 'local-test-only',
        'ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS': '0'}, root=tmp_path)
    with HithinkClient(settings, transport=httpx.MockTransport(respond), sleep=lambda _: None) as client:
        cat = collect_catalog(client, 'industry', now=lambda: NOW - timedelta(days=age))
        members = [collect_members(client, cat, b['thscode'], now=lambda: NOW - timedelta(days=age)) for b in boards]
    binding = dict(theme_id='CHEMICAL', source_id='HITHINK_THS_API', source_identity='THS_CHEMICAL_V1',
        board_id='STRATEGY_COMPOSITE:CHEMICAL' if composite else boards[0]['thscode'],
        board_name='策略化工' if composite else boards[0]['name'], category='industry', approved=True)
    if composite:
        binding.update(membership_operation='EXPLICIT_COMPONENT_UNION', components=[
            dict(category='industry', board_id=b['thscode'], board_name=b['name']) for b in boards])
    binding['reference_versions'] = [dict(board_id=m['board_id'],
        catalog_hash=cat['content_hash'], membership_hash=m['content_hash']) for m in members]
    return binding, [cat], members, bodies


def run(inputs, **kwargs):
    b, c, m, bodies = inputs
    return audit(b, c, m, as_of=NOW, response_bodies=bodies, **kwargs)


def rehash(obj):
    obj.pop('content_hash', None)
    obj['content_hash'] = digest(obj)


def repin(inputs):
    b, c, m, _ = inputs
    rehash(c[0])
    for obj in m:
        obj['source_catalog_hash'] = c[0]['content_hash']
        rehash(obj)
    b['reference_versions'] = [dict(board_id=obj['board_id'], catalog_hash=c[0]['content_hash'],
        membership_hash=obj['content_hash']) for obj in m]


@pytest.mark.parametrize('age,status,available', [(0, 'FRESH', True), (1, 'DEGRADED', True),
    (3, 'DEGRADED', True), (4, 'UNAVAILABLE', False), (14, 'UNAVAILABLE', False), (-1, 'UNAVAILABLE', False)])
def test_three_day_boundary_never_authorizes_execution(tmp_path, age, status, available):
    result = run(fixture(tmp_path, age))
    assert result['freshness'] == status
    assert result['research_available'] is available
    assert result['actual_execution_authorized'] is False
    assert result['source_authenticated'] is False
    assert result['new_entry_authorized'] is False


def test_fixed_original_bytes_and_version_pins_produce_research_receipt_only(tmp_path):
    inputs = fixture(tmp_path)
    original = deepcopy(inputs)
    result = run(inputs)
    assert result['schema_version'] == 'board_reference/1.0'
    assert result['status'] == 'RESEARCH_CONTRACT_VALID'
    assert result['strategy_direction_id'] == 'CHEMICAL'
    assert result['source_identity'] == 'THS_CHEMICAL_V1'
    component = result['components'][0]
    assert component['expected_page_count'] == component['actual_page_count'] == 1
    assert component['member_count'] == 1
    assert component['membership_hash'] == inputs[2][0]['content_hash']
    assert component['original_object_sha256'] == digest(inputs[2][0])
    assert component['captured_at'] == NOW.isoformat()
    assert component['provider_total'] is None
    assert result['history']['status'] == 'DATA_LIMITED'
    assert inputs == original
    assert run(inputs) == result


def test_object_declarations_without_original_response_are_data_limited(tmp_path):
    b, c, m, _ = fixture(tmp_path)
    result = audit(b, c, m, as_of=NOW)
    assert result['status'] == 'DATA_LIMITED'
    assert not result['research_available']
    assert 'RESPONSE_BODY_MISSING' in result['reason_codes']


@pytest.mark.parametrize('mutation,reason', [
    ('fake_bk', 'SOURCE_IDENTITY_INVALID'), ('eastmoney', 'SOURCE_NOT_THS'),
    ('missing_date', 'CAPTURE_TIME_INVALID'), ('future_time', 'CAPTURE_TIME_FUTURE'),
    ('pages', 'PAGE_COUNT_MISMATCH'), ('missing_component', 'COMPONENT_VERSION_MISSING'),
    ('wrong_version', 'REFERENCE_VERSION_MISSING'), ('rename', 'COMPONENT_IDENTITY_MISMATCH'),
    ('record_change', 'RESPONSE_RECORDS_MISMATCH'), ('pin_change', 'REFERENCE_VERSION_MISSING'),
    ('body_change', 'RESPONSE_BODY_HASH_MISMATCH'), ('request_change', 'REQUEST_IDENTITY_MISMATCH'),
])
def test_fail_closed_counterexamples(tmp_path, mutation, reason):
    inputs = fixture(tmp_path, composite=True)
    b, c, m, bodies = inputs
    if mutation == 'fake_bk': b['source_identity'] = 'BK0001'
    elif mutation == 'eastmoney': b['source_id'] = c[0]['source_id'] = 'EASTMONEY'; repin(inputs)
    elif mutation == 'missing_date': m[0].pop('observed_at'); repin(inputs)
    elif mutation == 'future_time': m[0]['observed_at'] = (NOW+timedelta(seconds=1)).isoformat(); repin(inputs)
    elif mutation == 'pages': m[0]['pagination']['page_count'] = 2; repin(inputs)
    elif mutation == 'missing_component': b['reference_versions'].pop()
    elif mutation == 'wrong_version': m.pop()
    elif mutation == 'rename': b['components'][0]['board_name'] = '仅同名猜映射'
    elif mutation == 'record_change': m[0]['records'][0]['symbol'] = '600999.SH'; repin(inputs)
    elif mutation == 'pin_change': b['reference_versions'][0]['membership_hash'] = '0'*64
    elif mutation == 'body_change': bodies[next(iter(bodies))] = b'{}'
    elif mutation == 'request_change': m[0]['pages'][0]['request_identity'] = {'thscode': '881999.TI'}; repin(inputs)
    result = run(inputs)
    assert not result['research_available']
    assert result['freshness'] == 'UNAVAILABLE'
    assert reason in result['reason_codes']
    assert result['actual_execution_authorized'] is False


def history(inputs):
    b, _, _, _ = inputs
    return dict(schema_version='board_reference_history/1.0', method='CONSTITUENT_EQUAL_WEIGHT_RETURN/1.0',
        source_identity=b['source_identity'], components=[
            {'board_id': v['board_id'], 'membership_hash': v['membership_hash']} for v in b['reference_versions']])


def test_equal_weight_identity_is_not_fabricated_history_or_authorization(tmp_path):
    inputs = fixture(tmp_path, composite=True)
    result = run(inputs, history_contract=history(inputs))
    assert result['research_available']
    assert result['history']['status'] == 'IDENTITY_MATCHED_DATA_LIMITED'
    assert result['history']['prices_available'] is False
    assert result['history']['returns'] is None
    assert result['actual_execution_authorized'] is False


@pytest.mark.parametrize('mutation', ['version', 'missing', 'duplicate', 'method', 'source'])
def test_history_component_versions_exact_not_union_guess(tmp_path, mutation):
    inputs = fixture(tmp_path, composite=True)
    h = history(inputs)
    if mutation == 'version': h['components'][0]['membership_hash'] = '0'*64
    elif mutation == 'missing': h['components'].pop()
    elif mutation == 'duplicate': h['components'].append(h['components'][0])
    elif mutation == 'method': h['method'] = 'EASTMONEY_INDEX'
    else: h['source_identity'] = 'THS_OTHER'
    result = run(inputs, history_contract=h)
    assert result['history']['status'] == 'UNAVAILABLE'
    assert not result['research_available']


def test_component_member_name_conflict_blocks_direction(tmp_path):
    inputs = fixture(tmp_path, composite=True)
    b, c, m, bodies = inputs
    # A valid separately frozen component that disagrees about one member's name.
    payload = json.loads(next(v for v in bodies.values() if b'600001' in v))
    payload['data']['item'][0]['name'] = '乙'
    raw = json.dumps(payload, ensure_ascii=False).encode()
    sha = hashlib.sha256(raw).hexdigest()
    bodies[sha] = raw
    page = m[1]['pages'][0]
    page.update(response_sha256=sha, response_bytes=len(raw), response_records=payload['data']['item'])
    page['api_metadata'].update(response_sha256=sha, response_bytes=len(raw))
    m[1]['records'][0]['name'] = '乙'
    repin(inputs)
    result = run(inputs)
    assert 'COMPONENT_MEMBER_CONFLICT' in result['reason_codes']
    assert not result['research_available']


@pytest.mark.parametrize('extra', [dict(complete='false'), dict(truncated='false'),
    dict(complete=0), dict(has_more=True), dict(pagination={'pages': 2}),
    dict(pagination={'page': True}), dict(total=2)])
def test_original_response_partial_or_wrong_boolean_type_never_ready(tmp_path, extra):
    inputs = fixture(tmp_path)
    b, c, m, bodies = inputs
    page = m[0]['pages'][0]
    payload = json.loads(bodies[page['response_sha256']])
    payload['data'].update(extra)
    raw = json.dumps(payload, ensure_ascii=False).encode()
    sha = hashlib.sha256(raw).hexdigest()
    bodies[sha] = raw
    page.update(response_sha256=sha, response_bytes=len(raw))
    page['api_metadata'].update(response_sha256=sha, response_bytes=len(raw), **extra)
    repin(inputs)
    result = run(inputs)
    assert not result['research_available']
    assert result['actual_execution_authorized'] is False


def test_original_hash_not_self_rehashed_tampering(tmp_path):
    inputs = fixture(tmp_path)
    inputs[2][0]['records'][0]['name'] = '变更'
    result = run(inputs)
    assert 'REFERENCE_OBJECT_INVALID' in result['reason_codes']


def test_age_uses_oldest_catalog_component_not_assembly_time(tmp_path):
    inputs = fixture(tmp_path, age=3, composite=True)
    for member in inputs[2]:
        member['observed_at'] = NOW.isoformat()
    repin(inputs)
    result = run(inputs)
    assert result['freshness'] == 'DEGRADED' and result['age_days'] == 3
    assert result['captured_at'] == (NOW-timedelta(days=3)).isoformat()


@pytest.mark.parametrize('binding', [None, {}, {'source_id': 'HITHINK_THS_API', 'source_identity': b'bad'}])
def test_bad_declarations_return_unavailable_without_authorization(tmp_path, binding):
    result = audit(binding, [], [], as_of=NOW)
    assert not result['research_available']
    assert result['actual_execution_authorized'] is False
