"""HiThink THS slow references, independent of Eastmoney catalog availability.

Uses the existing authenticated client. No workflow, database, notifications,
quotes or model calls. Full-list contract is recorded, not an invented total.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import re

from .board_reference import (
    BoardReferenceClient, ReferenceError, _result, _symbol, _valid, aware,
    digest, resolve_theme_reference,
)
from .rotation_theme import build_membership_snapshot, unavailable_membership_snapshot, load_rotation_theme_config

SOURCE = 'HITHINK_THS_API'


def rotation_snapshot_directory(settings):
    root = settings.fact_store_dir / 'rotation_theme'
    return root / 'local_reference' if getattr(settings, 'rotation_membership_source', 'EASTMONEY') == 'LOCAL_REFERENCE' else root


def configured_rotation_memberships(settings, *, as_of, trade_date=None):
    """Shared research/auction routing; no vendor call while loading references."""
    if getattr(settings, 'rotation_membership_source', 'EASTMONEY') != 'LOCAL_REFERENCE':
        return None
    path = settings.rotation_reference_bindings_path or settings.rotation_theme_registry_path.with_name('rotation_reference_bindings_v1.json')
    try:
        bindings = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise ReferenceError('ROTATION_REFERENCE_BINDINGS_UNREADABLE') from exc
    if not isinstance(bindings, list) or any(not isinstance(b, dict) for b in bindings):
        raise ReferenceError('ROTATION_REFERENCE_BINDINGS_INVALID')
    return load_rotation_references(
        settings.rotation_reference_dir or settings.fact_store_dir / 'board_reference' / 'hithink',
        bindings, [t.theme_id for t in load_rotation_theme_config(settings.rotation_theme_registry_path).active(trade_date or as_of.date())],
        as_of=as_of, max_age_days=settings.rotation_membership_max_age_days,
    )


def _evidence(result):
    return {'endpoint': result.endpoint, 'request_identity': result.metadata.get('request_identity'),
            'response_sha256': result.metadata.get('response_sha256'),
            'response_bytes': result.metadata.get('response_bytes'),
            'http_status': result.metadata.get('http_status', result.http_status),
            'api_metadata': result.metadata, 'reason_code': result.reason_code,
            'returned_count': len(result.items), 'response_records': [r.model_dump(mode='json') for r in result.items]}


def collect_catalog(client, category, *, now):
    envelope = BoardReferenceClient(now=now)
    try:
        if category not in {'industry', 'concept'}:
            raise ReferenceError('REFERENCE_CATEGORY_INVALID')
        result = client.ths_index_catalog(tag='industry' if category == 'industry' else 'cn_concept')
        envelope.pages.append(_evidence(result))
        if not result.ok or not result.complete:
            raise ReferenceError(result.reason_code)
        records = []
        for row in result.items:
            raw = row.model_dump(mode='json')
            code, name = raw.get('thscode'), raw.get('name')
            if not isinstance(code, str) or not re.fullmatch(r'\d{6}\.TI', code) or not isinstance(name, str) or not name.strip():
                raise ReferenceError('REFERENCE_BOARD_IDENTITY_INVALID')
            records.append({'board_id': code, 'name': name.strip()})
        if not records or len({r['board_id'] for r in records}) != len(records):
            raise ReferenceError('REFERENCE_CATALOG_EMPTY_OR_DUPLICATED')
        return _result(envelope, 0, SOURCE, 'catalog', category, records,
                       catalog_scope=category, source_time_basis='OBSERVATION_NOT_PROVIDER_REVISION')
    except ReferenceError as exc:
        return _result(envelope, 0, SOURCE, 'catalog', category, error=str(exc))


def collect_members(client, catalog, board_id, *, now):
    envelope = BoardReferenceClient(now=now)
    name = None
    try:
        if not _valid(catalog) or catalog['source_id'] != SOURCE:
            raise ReferenceError('REFERENCE_CATALOG_INVALID')
        board = next((r for r in catalog['records'] if r['board_id'] == board_id), None)
        if board is None:
            raise ReferenceError('REFERENCE_BOARD_NOT_IN_CATALOG')
        name = board['name']
        result = client.ths_index_constituents(board_id)
        envelope.pages.append(_evidence(result))
        if not result.ok or not result.complete:
            raise ReferenceError(result.reason_code)
        records = []
        for row in result.items:
            raw = row.model_dump(mode='json')
            symbol = _symbol(raw.get('thscode'))
            if raw.get('ticker') is not None and str(raw['ticker']) != symbol[:6]:
                raise ReferenceError('REFERENCE_MEMBER_TICKER_MISMATCH')
            if not isinstance(raw.get('name'), str) or not raw['name'].strip():
                raise ReferenceError('REFERENCE_MEMBER_NAME_INVALID')
            records.append({'symbol': symbol, 'name': raw['name'].strip()})
        if not records or len({r['symbol'] for r in records}) != len(records):
            raise ReferenceError('REFERENCE_MEMBERS_EMPTY_OR_DUPLICATED')
        return _result(envelope, 0, SOURCE, 'members', board_id, sorted(records, key=lambda r: r['symbol']),
            board_name=name, source_catalog_hash=catalog['content_hash'],
            source_time_basis='OBSERVATION_NOT_PROVIDER_REVISION', pagination={
                'page_count': 1, 'provider_total': result.metadata.get('total', result.metadata.get('pagination', {}).get('total')),
                'received_count': len(records), 'complete': True,
                'count_evidence': result.metadata.get('completeness_basis'),
            })
    except (ReferenceError, ValueError, TypeError) as exc:
        return _result(envelope, 0, SOURCE, 'members', board_id, error=str(exc), board_name=name)


def load_rotation_references(root, bindings, theme_ids, *, as_of, max_age_days=14):
    """Validated local-only adapter. Does not rewrite Eastmoney member archives."""
    now = aware(as_of)
    catalogs, members = [], []
    for path in Path(root).glob('board-reference-*.json'):
        try:
            value = json.loads(path.read_text(encoding='utf-8'))
            if _valid(value):
                (catalogs if value['kind'] == 'catalog' else members if value['kind'] == 'members' else []).append(value)
        except (OSError, ValueError, KeyError, TypeError):
            continue
    snapshots = {}
    for theme_id in theme_ids:
        try:
            matching = [b for b in bindings if b.get('theme_id') == theme_id]
            if not matching:
                raise ReferenceError('REFERENCE_THEME_UNMAPPED')
            if not any(b.get('approved') is True for b in matching):
                raise ReferenceError('REFERENCE_THEME_REVIEW_REQUIRED')
            resolved = resolve_theme_reference(theme_id, bindings, catalogs, members,
                                               now=now, max_age_days=max_age_days)
            if not resolved['available']:
                raise ReferenceError(resolved['reason_code'])
            p = resolved['projection']
            member = next(m for m in members if m['content_hash'] == p['source_membership_hash'])
            stamp = aware(datetime.fromisoformat(p['observed_at']))
            if stamp.date() > now.date():
                raise ReferenceError('REFERENCE_TIME_FUTURE')
            snapshot = build_membership_snapshot(theme_id=theme_id, members=member['records'],
                captured_at=stamp, effective_from=stamp.date(), source=p['source_id'],
                pagination_evidence={'complete': True, 'total': len(member['records']), 'pages': [{
                    'source_board_id': p['source_board_id'], 'source_board_name': p['source_board_name'],
                    'count_evidence': member['pagination']['count_evidence'] if 'count_evidence' in member['pagination'] else None,
                    'source_reference_hash': p['source_membership_hash'],
                }]})
            snapshot.update(source_board_id=p['source_board_id'], source_board_name=p['source_board_name'],
                source_reference_hash=p['source_membership_hash'], source_catalog_hash=p['source_catalog_hash'],
                binding_hash=digest([b for b in bindings if b.get('theme_id') == theme_id]),
                age_days=(now.date()-stamp.date()).days, reference_selection=resolved['attempts'])
            snapshot.pop('content_hash'); snapshot['content_hash'] = digest(snapshot)
            snapshots[theme_id] = snapshot
        except ReferenceError as exc:
            snapshots[theme_id] = unavailable_membership_snapshot(theme_id, now.date(), str(exc))
    return snapshots
