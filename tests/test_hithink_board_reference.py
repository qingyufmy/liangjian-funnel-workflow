from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from liangjian_funnel.data.board_reference import ReferenceError, write_reference, project_theme_binding
from liangjian_funnel.data.hithink_board_reference import collect_catalog, collect_members, load_rotation_references
from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.settings import Settings

NOW = datetime(2026, 10, 9, 15, 10, tzinfo=ZoneInfo('Asia/Shanghai'))
BINDING = dict(theme_id='TEST_THEME', source_id='HITHINK_THS_API', board_id='886044.TI',
               board_name='液冷服务器', category='concept', approved=True, priority=1)


def test_explicit_composite_requires_every_component_and_preserves_lineage(tmp_path):
    settings = Settings.from_env({'HITHINK_FINANCE_API_KEY': 'unit-secret',
        'ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS': '0'}, root=tmp_path)
    boards = [{'thscode': '881108.TI', 'name': '化学原料'}, {'thscode': '881109.TI', 'name': '化学制品'}]
    def respond(request):
        rows = boards if 'catalog' in request.url.path else [{'thscode': '600001.SH', 'ticker': '600001', 'name': '甲'}]
        return httpx.Response(200, json={'code': 0, 'data': {'item': rows}}, request=request)
    binding = {**BINDING, 'board_id': 'STRATEGY_COMPOSITE:TEST_THEME', 'board_name': '策略化工',
               'membership_operation': 'EXPLICIT_COMPONENT_UNION', 'components': [
                   {'category': 'industry', 'board_id': b['thscode'], 'board_name': b['name']} for b in boards]}
    with HithinkClient(settings, transport=httpx.MockTransport(respond), sleep=lambda _: None) as c:
        catalog = collect_catalog(c, 'industry', now=lambda: NOW - timedelta(days=1))
        first = collect_members(c, catalog, '881108.TI', now=lambda: NOW - timedelta(days=1))
        second = collect_members(c, catalog, '881109.TI', now=lambda: NOW)
    write_reference(tmp_path, catalog); write_reference(tmp_path, first)
    partial = load_rotation_references(tmp_path, [binding], ['TEST_THEME'], as_of=NOW)['TEST_THEME']
    assert not partial['available'] and 'COMPONENT_BLOCKED' in partial['reason_code']
    write_reference(tmp_path, second)
    complete = load_rotation_references(tmp_path, [binding], ['TEST_THEME'], as_of=NOW)['TEST_THEME']
    assert complete['available'] and len(complete['records']) == 1
    assert complete['membership_basis'] == 'STRATEGY_COMPOSITE_NOT_VENDOR_INDEX'
    assert complete['deduplicated_count'] == 1 and len(complete['components']) == 2
    assert complete['age_days'] == 1  # assembly does not rejuvenate slow data
    wrong = {**binding, 'components': [*binding['components'][:1], {**binding['components'][1], 'board_name': '假名称'}]}
    assert not load_rotation_references(tmp_path, [wrong], ['TEST_THEME'], as_of=NOW)['TEST_THEME']['available']


def client(tmp_path, *, members=None, extra=None):
    settings = Settings.from_env({'HITHINK_FINANCE_API_KEY': 'unit-secret',
        'ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS': '0'}, root=tmp_path)
    def respond(request):
        rows = ([{'thscode': '886044.TI', 'name': '液冷服务器'}] if 'catalog' in request.url.path
                else members if members is not None else [{'thscode': '600001.SH', 'ticker': '600001', 'name': '甲'}])
        return httpx.Response(200, json={'code': 0, 'data': {'timestamp': 1791533400000,
            'item': rows, **(extra or {})}}, request=request)
    return HithinkClient(settings, transport=httpx.MockTransport(respond), sleep=lambda _: None)


def test_complete_reference_and_local_rotation_projection(tmp_path):
    with client(tmp_path) as c:
        catalog = collect_catalog(c, 'concept', now=lambda: NOW)
        member = collect_members(c, catalog, '886044.TI', now=lambda: NOW)
    assert catalog['complete'] and member['complete']
    assert member['pagination']['provider_total'] is None
    assert member['pages'][0]['response_sha256']
    assert 'unit-secret' not in str(member)
    write_reference(tmp_path, catalog)
    write_reference(tmp_path, member)
    result = load_rotation_references(tmp_path, [BINDING], ['TEST_THEME'], as_of=NOW)
    selected = result['TEST_THEME']
    assert selected['available']
    assert selected['source_id'] == 'HITHINK_THS_API'
    assert selected['source_board_id'] == '886044.TI'
    assert selected['records'][0]['symbol'] == '600001.SH'
    assert not any('BK' in str(p) for p in selected['pagination_evidence']['pages'])


@pytest.mark.parametrize('extra', [{'total': 2}, {'total': True}, {'has_more': True},
    {'has_more': 'false'}, {'next_offset': 1}, {'pagination': {'pages': 2}},
    {'pagination': 'invalid'}, {'thscode': '000001.TI'}, {'complete': False}, {'truncated': True},
    {'pagination': {'page': True}}])
def test_declared_partial_or_wrong_identity_cannot_be_complete(tmp_path, extra):
    with client(tmp_path, extra=extra) as c:
        result = c.ths_index_constituents('886044.TI')
    assert not result.ok and not result.complete


@pytest.mark.parametrize('members', [[], [{'thscode': '600001.SH', 'name': '甲'}]*2,
    [{'thscode': '600001.SZ', 'name': '错市场'}], [{'thscode': 'not-stock', 'name': '坏'}],
    [{'thscode': '600001.SH', 'ticker': '000002', 'name': '错代码'}]])
def test_members_empty_duplicate_or_invalid_never_promote(tmp_path, members):
    with client(tmp_path, members=members) as c:
        catalog = collect_catalog(c, 'concept', now=lambda: NOW)
        result = collect_members(c, catalog, '886044.TI', now=lambda: NOW)
    assert not result['available']


@pytest.mark.parametrize('age', [-1, 15])
def test_future_expired_versions_do_not_feed_rotation(tmp_path, age):
    stamp = NOW - timedelta(days=age)
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: stamp)
        mem = collect_members(c, cat, '886044.TI', now=lambda: stamp)
    write_reference(tmp_path, cat); write_reference(tmp_path, mem)
    result = load_rotation_references(tmp_path, [BINDING], ['TEST_THEME'], as_of=NOW)
    assert not result['TEST_THEME']['available']


def test_unapproved_and_unmapped_are_visible(tmp_path):
    result = load_rotation_references(tmp_path, [{**BINDING, 'approved': False}],
                                     ['TEST_THEME', 'OTHER_THEME'], as_of=NOW)
    assert all(not r['available'] for r in result.values())


def test_projections_reject_wrong_catalog_version(tmp_path):
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    from liangjian_funnel.data.board_reference import digest
    cat['records'].append({'board_id': '885001.TI', 'name': '其他'})
    cat.pop('content_hash'); cat['content_hash'] = digest(cat)
    with pytest.raises(ReferenceError, match='CATALOG'):
        project_theme_binding(BINDING, cat, mem, now=NOW)


def test_reference_rotation_uses_tencent_not_eastmoney_or_old_quotes(tmp_path):
    from test_rotation_theme import _write_registry, _top_level_fetchers
    from liangjian_funnel.data.rotation_theme import collect_rotation_theme_snapshot
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    write_reference(tmp_path/'refs', cat); write_reference(tmp_path/'refs', mem)
    local = load_rotation_references(tmp_path/'refs', [BINDING], ['TEST_THEME'], as_of=NOW)
    def forbidden(*args, **kwargs):
        raise AssertionError('Eastmoney must not gate or supply THS factors')
    fetchers = _top_level_fetchers(day=NOW.date(), captured=NOW)
    fetchers.update(eastmoney_catalog=forbidden, eastmoney_members=forbidden,
                    eastmoney_flow=forbidden, eastmoney_history=forbidden)
    result = collect_rotation_theme_snapshot(as_of=NOW, registry_path=_write_registry(tmp_path/'themes.yaml'),
        snapshot_dir=tmp_path/'rotation', reference_memberships=local, fetchers=fetchers)
    assert result['source_health']['tencent_flow'] == 'OK'
    assert result['source_health']['membership_mode'] == 'LOCAL_REFERENCE'
    row = result['boards'][0]
    assert row['membership_source_board_id'] == '886044.TI'
    assert row['membership_reference_hash'] == mem['content_hash']
    assert row['membership_board_codes'] == []
    assert row['component_board_codes'] == []
    assert row['eastmoney_main_net_inflow_cny'] is None
    assert row['price_coverage'] == 1
    assert row['relative_return_pct'] == 5
    assert row['momentum_5d_pct'] is None


def test_missing_reference_is_blocked_not_fallback_to_old_bk(tmp_path):
    from test_rotation_theme import _write_registry, _top_level_fetchers
    from liangjian_funnel.data.rotation_theme import collect_rotation_theme_snapshot
    result = collect_rotation_theme_snapshot(as_of=NOW, registry_path=_write_registry(tmp_path/'themes.yaml'),
        snapshot_dir=tmp_path/'rotation', reference_memberships={},
        fetchers=_top_level_fetchers(day=NOW.date(), captured=NOW))
    assert not result['available']
    assert result['source_health']['tencent_flow'] == 'NOT_REQUESTED'
    assert result['source_health']['unavailable_membership']['TEST_THEME'] == 'REFERENCE_MEMBERSHIP_INVALID'


def test_source_switch_requires_explicit_setting(tmp_path):
    assert Settings.from_env({}, root=tmp_path).rotation_membership_source == 'EASTMONEY'
    assert Settings.from_env({'LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE': 'LOCAL_REFERENCE'},
                             root=tmp_path).rotation_membership_source == 'LOCAL_REFERENCE'
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Settings.from_env({'LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE': 'guess'}, root=tmp_path)


def test_new_catalog_does_not_mask_valid_hash_bound_pair(tmp_path):
    from liangjian_funnel.data.board_reference import digest
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW-timedelta(days=1))
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW-timedelta(days=1))
    write_reference(tmp_path, cat); write_reference(tmp_path, mem)
    newer = {**cat, 'observed_at': NOW.isoformat()}
    newer.pop('content_hash'); newer['content_hash'] = digest(newer)
    write_reference(tmp_path, newer)
    result = load_rotation_references(tmp_path, [BINDING], ['TEST_THEME'], as_of=NOW)
    assert result['TEST_THEME']['source_catalog_hash'] == cat['content_hash']
    assert result['TEST_THEME']['age_days'] == 1


def test_binding_cannot_be_applied_to_earlier_day(tmp_path):
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    write_reference(tmp_path, cat); write_reference(tmp_path, mem)
    result = load_rotation_references(tmp_path, [{**BINDING, 'effective_from': '2026-10-10'}],
                                     ['TEST_THEME'], as_of=NOW)
    assert not result['TEST_THEME']['available']


def test_tampered_member_cannot_feed_rotation(tmp_path):
    import json
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    write_reference(tmp_path, cat)
    path = write_reference(tmp_path, mem)
    mem['records'][0]['symbol'] = '600999.SH'
    path.write_text(json.dumps(mem), encoding='utf-8')
    assert not load_rotation_references(tmp_path, [BINDING], ['TEST_THEME'], as_of=NOW)['TEST_THEME']['available']


def test_known_catalog_identity_change_blocks_old_members(tmp_path):
    from liangjian_funnel.data.board_reference import digest
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW-timedelta(days=1))
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW-timedelta(days=1))
    write_reference(tmp_path, cat); write_reference(tmp_path, mem)
    newer = {**cat, 'observed_at': NOW.isoformat(), 'records': [{'board_id':'886044.TI','name':'改名'}]}
    newer.pop('content_hash'); newer['content_hash'] = digest(newer)
    write_reference(tmp_path, newer)
    assert not load_rotation_references(tmp_path, [BINDING], ['TEST_THEME'], as_of=NOW)['TEST_THEME']['available']


def test_reference_keeps_age_warning_and_strict_price_gate(tmp_path):
    from test_rotation_theme import _write_registry, _top_level_fetchers
    from liangjian_funnel.data.rotation_theme import collect_rotation_theme_snapshot
    old = NOW-timedelta(days=7)
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: old)
        mem = collect_members(c, cat, '886044.TI', now=lambda: old)
    write_reference(tmp_path/'refs', cat); write_reference(tmp_path/'refs', mem)
    refs = load_rotation_references(tmp_path/'refs', [BINDING], ['TEST_THEME'], as_of=NOW)
    fetchers = _top_level_fetchers(day=NOW.date(), captured=NOW)
    fetchers['tencent_flow'] = lambda symbol: {'trade_date': NOW.date().isoformat(), 'main_net_inflow_cny': 100}
    fetchers['tencent_quote'] = lambda symbol: {}
    result = collect_rotation_theme_snapshot(as_of=NOW, registry_path=_write_registry(tmp_path/'themes.yaml'),
        snapshot_dir=tmp_path/'rotation', reference_memberships=refs, fetchers=fetchers)
    assert result['source_health']['membership_update_warnings']['TEST_THEME'] == 'REFERENCE_AGE_WARNING'
    assert result['source_health']['degraded_membership_count'] == 1
    assert result['boards'][0]['price_coverage'] == 0
    assert not result['boards'][0]['selected_for_rotation']


def test_duplicate_json_collection_is_rejected(tmp_path):
    settings = Settings.from_env({'HITHINK_FINANCE_API_KEY': 'unit-secret',
        'ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS': '0'}, root=tmp_path)
    response = lambda request: httpx.Response(200, content=b'{"code":0,"data":{"item":[],"item":[{"thscode":"600001.SH","name":"A"}]}}', request=request)
    with HithinkClient(settings, transport=httpx.MockTransport(response), sleep=lambda _: None) as c:
        result = c.ths_index_constituents('886044.TI')
    assert not result.ok and result.reason_code == 'INVALID_JSON'


def test_research_and_auction_share_configured_local_route(tmp_path):
    import json
    from test_rotation_theme import _write_registry
    from liangjian_funnel.data.hithink_board_reference import configured_rotation_memberships, rotation_snapshot_directory
    binding_path = tmp_path/'bindings.json'
    binding_path.write_text(json.dumps([BINDING]), encoding='utf-8')
    settings = Settings.from_env({}, root=tmp_path).model_copy(update={
        'rotation_membership_source': 'LOCAL_REFERENCE', 'rotation_reference_bindings_path': binding_path,
        'rotation_reference_dir': tmp_path/'refs', 'rotation_theme_registry_path': _write_registry(tmp_path/'registry.yaml')})
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    write_reference(tmp_path/'refs', cat); write_reference(tmp_path/'refs', mem)
    assert configured_rotation_memberships(settings, as_of=NOW)['TEST_THEME']['available']
    assert rotation_snapshot_directory(settings) == settings.fact_store_dir/'rotation_theme'/'local_reference'
    legacy = settings.model_copy(update={'rotation_membership_source': 'EASTMONEY'})
    assert configured_rotation_memberships(legacy, as_of=NOW) is None
    assert rotation_snapshot_directory(legacy) == settings.fact_store_dir/'rotation_theme'


def test_archive_cannot_cross_vendor_mode_or_be_overwritten(tmp_path):
    from test_rotation_theme import _write_registry, _top_level_fetchers
    from liangjian_funnel.data.rotation_theme import collect_rotation_theme_snapshot
    old = NOW-timedelta(days=1)
    registry = _write_registry(tmp_path/'themes.yaml')
    first = collect_rotation_theme_snapshot(as_of=old, registry_path=registry,
        snapshot_dir=tmp_path/'rotation', fetchers=_top_level_fetchers(day=old.date(), captured=old))
    path = Path(first['snapshot_path'])
    before = path.read_bytes()
    result = collect_rotation_theme_snapshot(as_of=NOW, expected_trade_date=old.date(),
        registry_path=registry, snapshot_dir=tmp_path/'rotation', reference_memberships={})
    assert not result['available']
    assert result['reason_code'] == 'ROTATION_ARCHIVE_MEMBERSHIP_SOURCE_MISMATCH'
    assert before == path.read_bytes()


def test_partial_local_taxonomy_never_becomes_green_top_five(tmp_path):
    from test_rotation_theme import _write_registry, _top_level_fetchers, _registry_payload
    from liangjian_funnel.data.rotation_theme import collect_rotation_theme_snapshot
    themes = _registry_payload()['themes']
    themes.append({**themes[0], 'theme_id': 'OTHER_THEME', 'name': '其他方向', 'aliases': ['其他方向'], 'eastmoney_board_codes': ['BK0002']})
    with client(tmp_path) as c:
        cat = collect_catalog(c, 'concept', now=lambda: NOW)
        mem = collect_members(c, cat, '886044.TI', now=lambda: NOW)
    write_reference(tmp_path/'refs', cat); write_reference(tmp_path/'refs', mem)
    refs = load_rotation_references(tmp_path/'refs', [BINDING], ['TEST_THEME','OTHER_THEME'], as_of=NOW)
    result = collect_rotation_theme_snapshot(as_of=NOW, registry_path=_write_registry(tmp_path/'themes.yaml', themes=themes),
        snapshot_dir=tmp_path/'rotation', reference_memberships=refs,
        fetchers=_top_level_fetchers(day=NOW.date(), captured=NOW))
    assert result['reason_code'] == 'ROTATION_REFERENCE_MAPPING_PARTIAL'
    assert not result['available'] and not result['source_health']['reference_mapping_complete']
    assert result['source_health']['available_membership_count'] == 1
