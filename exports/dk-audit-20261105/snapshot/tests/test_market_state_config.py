from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from liangjian_funnel.market_state.config import index_identity, load_market_definitions, read_yaml


CONFIG = Path(__file__).resolve().parents[1] / 'config'


def test_approved_definitions_are_shadow_only_and_exact():
    result = load_market_definitions(CONFIG)
    rules = result['rules']
    assert rules['daily']['ma_periods'] == [5, 10, 20, 60]
    assert rules['daily']['slope_lag'] == 5
    assert rules['daily']['trend_short']['UP'] == {
        'close_vs_ma': [20, 'GT'], 'slope_vs_zero': [20, 'GT'], 'ma_order': [5, 10, 20], 'comparison': 'GT'}
    assert rules['turnover']['expand_gte'] == '1.20'
    assert rules['turnover']['shrink_lte'] == '0.80'
    assert rules['enabled'] is False
    assert rules['dk']['formula'] == 'SSL_SMA'
    assert rules['dk']['period'] == 20 and rules['dk']['comparison_periods'] == [10, 20, 30]
    assert rules['dk']['label'] == '自研、非站点同款'
    assert rules['dk']['site_anchor'] == 'NOT_REPRODUCIBLE_CLOSED'
    assert rules['dk']['canonical_source'] == 'TENCENT' and rules['dk']['mix_sources'] is False
    assert len(rules['config_sha256']) == 64
    assert [v['thscode'] for v in result['registry']['indices'].values()] == [
        '000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000852.SH', '000688.SH']


@pytest.mark.parametrize('key,code,name', [
    ('SSE', '000001', '上证指数'), ('SSE', '000001.SZ', '平安银行'),
    ('SSE', '000001.SH', '平安银行'), ('UNKNOWN', '000001.SH', '上证指数'),
])
def test_index_identity_rejects_stock_mapping_or_wrong_name(key, code, name):
    with pytest.raises(ValueError, match='INDEX_IDENTITY_MISMATCH'):
        index_identity(load_market_definitions(CONFIG)['registry'], key, returned_code=code, returned_name=name)


def test_exact_index_identity():
    assert index_identity(load_market_definitions(CONFIG)['registry'], 'SSE',
                          returned_code='000001.SH', returned_name='上证指数')['tencent_code'] == 'sh000001'


@pytest.mark.parametrize('section,field,value', [
    (None, 'enabled', True), ('daily', 'slope_lag', True),
    ('daily', 'ma_periods', [5, 10, 20, 20]), ('turnover', 'expand_gte', 'NaN'),
    ('turnover', 'shrink_lte', '1.20'), ('weekly', 'exclude_partial_from_indicators', False),
    ('turnover', 'amount_unit', 'CNY'), ('dk', 'enabled', True),
    ('dk', 'formula', 'SITE_CLONE'), ('dk', 'mix_sources', True),
])
def test_invalid_definitions_fail_closed(tmp_path, section, field, value):
    rules = deepcopy(read_yaml(CONFIG / 'market_state.yaml'))
    (rules if section is None else rules[section])[field] = value
    (tmp_path / 'market_state.yaml').write_text(yaml.safe_dump(rules), encoding='utf-8')
    (tmp_path / 'market_indices.yaml').write_bytes((CONFIG / 'market_indices.yaml').read_bytes())
    with pytest.raises(ValueError, match='MARKET_STATE_CONFIG_INVALID'):
        load_market_definitions(tmp_path)


def test_yaml_duplicate_keys_rejected(tmp_path):
    source = tmp_path / 'duplicate.yaml'
    source.write_text('enabled: false\nenabled: true\n', encoding='utf-8')
    with pytest.raises(ValueError, match='DUPLICATE'):
        read_yaml(source)
