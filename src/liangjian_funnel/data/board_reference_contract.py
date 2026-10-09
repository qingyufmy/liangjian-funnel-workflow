"""Pure WP4 research audit. No collection, persistence or execution integration.

THS_* is a caller's explicitly approved contract identity, not a vendor index
code or authentication certificate. Frozen bytes demonstrate internal evidence
consistency only; even a valid audit cannot authorize an actual order.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re

from .board_reference import ReferenceError, _symbol, _valid, aware, digest

SCHEMA = 'board_reference/1.0'
SOURCE = 'HITHINK_THS_API'
HISTORY_SCHEMA = 'board_reference_history/1.0'
EQUAL_WEIGHT_METHOD = 'CONSTITUENT_EQUAL_WEIGHT_RETURN/1.0'
_ENDPOINTS = {'catalog': '/api/a-share-index/catalog/ths-index-list',
              'members': '/api/a-share-index/constituents/ths-stock-list'}


def _require(condition, reason):
    if not condition:
        raise ReferenceError(reason)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'RESPONSE_DUPLICATE_KEY')
        result[key] = value
    return result


def _capture(reference, cutoff):
    try:
        stamp = aware(datetime.fromisoformat(reference['observed_at']))
    except (ValueError, KeyError, TypeError):
        raise ReferenceError('CAPTURE_TIME_INVALID') from None
    _require(stamp <= cutoff, 'CAPTURE_TIME_FUTURE')
    age = (cutoff.date() - stamp.date()).days
    _require(age <= 3, 'CAPTURE_EXPIRED')
    # Capture is not a vendor revision date. A supplied revision cannot refresh it.
    if reference.get('source_updated_at') is not None:
        try:
            revision = aware(datetime.fromisoformat(reference['source_updated_at']))
        except (ValueError, TypeError):
            raise ReferenceError('SOURCE_REVISION_TIME_INVALID') from None
        _require(revision <= stamp, 'SOURCE_REVISION_TIME_FUTURE')
        _require((cutoff.date() - revision.date()).days <= 3, 'SOURCE_REVISION_EXPIRED')
    return stamp, age


def _page_evidence(reference, bodies, component):
    """Only the existing THS full-list endpoints are supported, not fake pages."""
    kind = reference['kind']
    pages = reference.get('pages')
    _require(isinstance(pages, list) and len(pages) == 1, 'PAGE_COUNT_MISMATCH')
    page = pages[0]
    _require(isinstance(page, dict), 'PAGE_EVIDENCE_INVALID')
    _require(page.get('endpoint') == _ENDPOINTS[kind], 'ENDPOINT_IDENTITY_MISMATCH')
    expected_request = ({'thscode': component['board_id']} if kind == 'members' else
                        {'tag': 'cn_concept' if component['category'] == 'concept' else 'industry'})
    _require(page.get('request_identity') == expected_request, 'REQUEST_IDENTITY_MISMATCH')
    _require(page.get('http_status') == 200 and page.get('reason_code') == 'OK', 'RESPONSE_NOT_SUCCESSFUL')
    sha = page.get('response_sha256')
    _require(isinstance(sha, str) and re.fullmatch('[0-9a-f]{64}', sha), 'RESPONSE_HASH_MISSING')
    raw = bodies.get(sha)
    _require(raw is not None, 'RESPONSE_BODY_MISSING')
    _require(isinstance(raw, bytes) and hashlib.sha256(raw).hexdigest() == sha, 'RESPONSE_BODY_HASH_MISMATCH')
    _require(0 < len(raw) <= 4_000_000 and type(page.get('response_bytes')) is int
             and page['response_bytes'] == len(raw), 'RESPONSE_SIZE_MISMATCH')
    payload = json.loads(raw, object_pairs_hook=_unique_object)
    _require(isinstance(payload, dict) and type(payload.get('code')) in {int, str}
             and str(payload['code']) == '0', 'RESPONSE_BUSINESS_FAILURE')
    data = payload.get('data')
    _require(isinstance(data, dict) and isinstance(data.get('item'), list) and data['item'], 'RESPONSE_ITEMS_INVALID')
    rows = data['item']
    _require(page.get('response_records') == rows and type(page.get('returned_count')) is int
             and page['returned_count'] == len(rows), 'RESPONSE_RECORDS_MISMATCH')
    meta = page.get('api_metadata')
    _require(isinstance(meta, dict) and meta.get('request_identity') == expected_request
             and meta.get('response_sha256') == sha and meta.get('response_bytes') == len(raw)
             and meta.get('http_status') == 200, 'RESPONSE_METADATA_MISMATCH')
    pagination = data.get('pagination', {})
    _require(isinstance(pagination, dict), 'PAGINATION_EVIDENCE_INVALID')
    for metadata in (data, pagination):
        _require(metadata.get('has_more') is None or metadata['has_more'] is False, 'PAGINATION_INCOMPLETE')
        _require(metadata.get('next_offset') is None, 'PAGINATION_INCOMPLETE')
        if 'total' in metadata:
            _require(type(metadata['total']) is int and metadata['total'] == len(rows), 'PAGINATION_TOTAL_MISMATCH')
    for key in ('page', 'pages'):
        _require(type(pagination.get(key, 1)) is int and pagination.get(key, 1) == 1, 'PAGE_COUNT_MISMATCH')
    for key, allowed in (('complete', True), ('truncated', False)):
        if key in data:
            _require(type(data[key]) is bool and data[key] is allowed, 'PAGINATION_INCOMPLETE')
    _require('thscode' not in data or data['thscode'] == expected_request.get('thscode'), 'REQUEST_IDENTITY_MISMATCH')
    basis = ('DECLARED_TOTAL_MATCHED' if 'total' in data or 'total' in pagination else
             'FULL_LIST_ENDPOINT_NOT_INDEPENDENT_TOTAL')
    _require(meta.get('completeness_basis') == basis, 'PAGINATION_BASIS_MISMATCH')
    for key in ('timestamp', 'total', 'has_more', 'next_offset', 'pagination', 'thscode', 'complete', 'truncated'):
        _require((key in meta) == (key in data) and (key not in data or meta[key] == data[key]), 'RESPONSE_METADATA_MISMATCH')
    if kind == 'members':
        projected = []
        for row in rows:
            symbol = _symbol(row['thscode'])
            _require(row.get('ticker') is None or str(row['ticker']) == symbol[:6], 'RESPONSE_RECORDS_MISMATCH')
            projected.append({'symbol': symbol, 'name': row['name'].strip()})
        projected.sort(key=lambda row: row['symbol'])
        p = reference.get('pagination', {})
        total = data.get('total', pagination.get('total'))
        _require(type(p.get('page_count')) is int and p['page_count'] == 1, 'PAGE_COUNT_MISMATCH')
        _require(p.get('count_evidence') == basis and p.get('provider_total') == total,
                 'PAGINATION_BASIS_MISMATCH')
    else:
        projected = [{'board_id': row['thscode'], 'name': row['name'].strip()} for row in rows]
    _require(projected == reference['records'], 'RESPONSE_RECORDS_MISMATCH')
    return {'expected_page_count': 1, 'actual_page_count': len(pages),
            'pagination_basis': basis, 'provider_total': data.get('total', pagination.get('total')),
            'response_sha256': sha, 'evidence_scope': 'LOCAL_BYTES_INTERNAL_CONSISTENCY_NOT_SOURCE_AUTHENTICATION'}


def _component(component, version, catalogs, memberships, cutoff, bodies):
    code, name, category = (component.get(k) for k in ('board_id', 'board_name', 'category'))
    _require(isinstance(code, str) and re.fullmatch(r'\d{6}\.TI', code), 'COMPONENT_IDENTITY_INVALID')
    _require(isinstance(name, str) and bool(name.strip()) and category in {'industry', 'concept'}, 'COMPONENT_IDENTITY_INVALID')
    cats = [c for c in catalogs if isinstance(c, dict) and c.get('content_hash') == version.get('catalog_hash')]
    mems = [m for m in memberships if isinstance(m, dict) and m.get('content_hash') == version.get('membership_hash')]
    _require(len(cats) == len(mems) == 1, 'REFERENCE_VERSION_MISSING')
    cat, mem = cats[0], mems[0]
    _require(cat.get('source_id') == mem.get('source_id') == SOURCE, 'SOURCE_NOT_THS')
    _require(_valid(cat) and _valid(mem), 'REFERENCE_OBJECT_INVALID')
    _require(cat.get('kind') == 'catalog' and mem.get('kind') == 'members'
             and cat.get('catalog_scope') == category and cat.get('board_id') == category
             and mem.get('board_id') == code and mem.get('board_name') == name
             and {'board_id': code, 'name': name} in cat['records'], 'COMPONENT_IDENTITY_MISMATCH')
    _require(mem.get('source_catalog_hash') == cat['content_hash'], 'CATALOG_VERSION_MISMATCH')
    cat_stamp, cat_age = _capture(cat, cutoff)
    mem_stamp, mem_age = _capture(mem, cutoff)
    cat_pages = _page_evidence(cat, bodies, component)
    member_pages = _page_evidence(mem, bodies, component)
    return {**component, **member_pages, 'catalog_pages': cat_pages,
        'catalog_hash': cat['content_hash'], 'membership_hash': mem['content_hash'],
        'catalog_original_object_sha256': digest(cat), 'original_object_sha256': digest(mem),
        'captured_at': mem_stamp.isoformat(), 'catalog_captured_at': cat_stamp.isoformat(),
        'age_days': max(cat_age, mem_age), 'member_count': len(mem['records']),
        'member_records_hash': digest(mem['records']), 'research_available': True}, mem['records']


def _history(history, binding, components):
    result = {'status': 'DATA_LIMITED', 'prices_available': False, 'returns': None,
              'reason_code': 'HISTORY_INPUT_NOT_PROVIDED'}
    if history is None:
        return result
    expected = [{'board_id': c['board_id'], 'membership_hash': c['membership_hash']} for c in components]
    actual = history.get('components') if isinstance(history, dict) else None
    valid = (isinstance(history, dict) and history.get('schema_version') == HISTORY_SCHEMA
             and history.get('method') == EQUAL_WEIGHT_METHOD
             and history.get('source_identity') == binding.get('source_identity')
             and isinstance(actual, list) and len(actual) == len(expected)
             and sorted(actual, key=digest) == sorted(expected, key=digest))
    if not valid:
        return {**result, 'status': 'UNAVAILABLE', 'reason_code': 'HISTORY_IDENTITY_OR_COMPONENT_VERSION_MISMATCH'}
    return {**result, 'status': 'IDENTITY_MATCHED_DATA_LIMITED', 'reason_code': 'HISTORICAL_PRICES_NOT_AUDITED',
            'method': EQUAL_WEIGHT_METHOD, 'contract_hash': digest(history)}


def audit_board_reference(binding: dict, catalogs, memberships, *, as_of: datetime,
                          response_bodies=None, history_contract=None) -> dict:
    """Audit exact pinned objects; never select the newest, infer a name or persist.

    reference_versions must list one exact catalog/membership hash per explicit
    component. response_bodies maps original response SHA256 to local bytes.
    Missing byte evidence is DATA_LIMITED; object/source/age conflicts UNAVAILABLE.
    """
    result = {'schema_version': SCHEMA, 'status': 'UNAVAILABLE', 'freshness': 'UNAVAILABLE',
        'research_available': False, 'actual_execution_authorized': False,
        'new_entry_authorized': False, 'source_authenticated': False,
        'execution_scope': 'RESEARCH_ONLY_NOT_CONNECTED', 'production_publish_forbidden': True,
        'source_identity': binding.get('source_identity') if isinstance(binding, dict)
                           and isinstance(binding.get('source_identity'), str) else None,
        'strategy_direction_id': binding.get('theme_id') if isinstance(binding, dict)
                                 and isinstance(binding.get('theme_id'), str) else None,
        'components': [], 'reason_codes': [], 'history': {'status': 'DATA_LIMITED',
        'prices_available': False, 'returns': None, 'reason_code': 'HISTORY_INPUT_NOT_PROVIDED'}}
    try:
        cutoff = aware(as_of)
        _require(isinstance(binding, dict), 'BINDING_INVALID')
        result.update(as_of=cutoff.isoformat(), binding_hash=digest(binding))
        _require(binding.get('source_id') == SOURCE, 'SOURCE_NOT_THS')
        _require(isinstance(binding.get('source_identity'), str)
                 and re.fullmatch('THS_[A-Z0-9_]+', binding['source_identity']), 'SOURCE_IDENTITY_INVALID')
        _require(isinstance(binding.get('theme_id'), str) and bool(binding['theme_id'].strip())
                 and binding.get('approved') is True, 'EXPLICIT_BINDING_NOT_APPROVED')
        if binding.get('effective_from'):
            _require(datetime.fromisoformat(binding['effective_from']).date() <= cutoff.date(), 'BINDING_NOT_YET_EFFECTIVE')
        operation = binding.get('membership_operation')
        _require(operation in {None, 'EXPLICIT_COMPONENT_UNION'}, 'MEMBERSHIP_OPERATION_UNSUPPORTED')
        composite = operation == 'EXPLICIT_COMPONENT_UNION'
        components = binding.get('components') if composite else [
            {k: binding.get(k) for k in ('board_id', 'board_name', 'category')}]
        _require(isinstance(components, list) and bool(components) and all(isinstance(c, dict) for c in components),
                 'EXPLICIT_COMPONENTS_MISSING')
        _require(composite or not binding.get('components'), 'COMPONENT_DECLARATION_INCONSISTENT')
        _require(not composite or binding.get('board_id') == f"STRATEGY_COMPOSITE:{binding['theme_id']}",
                 'COMPONENT_DECLARATION_INCONSISTENT')
        components = [{k: c.get(k) for k in ('board_id', 'board_name', 'category')} for c in components]
        _require(all(all(isinstance(v, str) for v in c.values()) for c in components), 'COMPONENT_IDENTITY_INVALID')
        ids = [c.get('board_id') for c in components]
        _require(all(isinstance(code, str) for code in ids) and len(set(ids)) == len(ids), 'COMPONENT_DECLARATION_INCONSISTENT')
        versions = binding.get('reference_versions')
        _require(isinstance(versions, list) and all(isinstance(v, dict) for v in versions)
                 and len(versions) == len(ids) and sorted(v.get('board_id', '') for v in versions) == sorted(ids),
                 'COMPONENT_VERSION_MISSING')
        union = {}
        for component in components:
            version = next(v for v in versions if v['board_id'] == component['board_id'])
            try:
                row, records = _component(component, version, catalogs, memberships, cutoff, response_bodies or {})
                for member in records:
                    _require(member['symbol'] not in union or union[member['symbol']] == member['name'], 'COMPONENT_MEMBER_CONFLICT')
                    union[member['symbol']] = member['name']
                result['components'].append(row)
            except ReferenceError as exc:
                result['components'].append({**component, 'research_available': False, 'reason_code': str(exc)})
                result['reason_codes'].append(str(exc))
        if not result['reason_codes']:
            result['history'] = _history(history_contract, binding, result['components'])
            if result['history']['status'] == 'UNAVAILABLE':
                result['reason_codes'].append(result['history']['reason_code'])
        if not result['reason_codes']:
            age = max(c['age_days'] for c in result['components'])
            result.update(status='RESEARCH_CONTRACT_VALID', freshness='FRESH' if age == 0 else 'DEGRADED',
                age_days=age, research_available=True, member_count=len(union),
                captured_at=min(min(c['captured_at'], c['catalog_captured_at']) for c in result['components']),
                membership_method='EXPLICIT_COMPONENT_UNION' if composite else 'NATIVE_THS_MEMBERSHIP')
    except ReferenceError as exc:
        result['reason_codes'].append(str(exc))
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        result['reason_codes'].append('CONTRACT_INPUT_INVALID')
    result['reason_codes'] = sorted(set(result['reason_codes']))
    if result['reason_codes'] and all(r in {'RESPONSE_BODY_MISSING', 'RESPONSE_HASH_MISSING'} for r in result['reason_codes']):
        result['status'] = 'DATA_LIMITED'
    result['content_hash'] = digest(result)
    return result
