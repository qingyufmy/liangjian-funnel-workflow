"""Read and validate shadow M0 definitions without importing production runners."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
import re

import yaml


class _UniqueLoader(yaml.SafeLoader):
    pass


def _unique_mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, (str, int)) or key in result:
            raise ValueError('MARKET_STATE_DUPLICATE_OR_INVALID_YAML_KEY')
        result[key] = loader.construct_object(value_node, deep=True)
    return result


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


def read_yaml(path: str | Path) -> dict:
    raw = Path(path).read_bytes()
    try:
        value = yaml.load(raw, Loader=_UniqueLoader)
    except yaml.YAMLError as exc:
        raise ValueError('MARKET_STATE_YAML_INVALID') from exc
    if not isinstance(value, dict):
        raise ValueError('MARKET_STATE_YAML_OBJECT_REQUIRED')
    return {**value, 'config_sha256': hashlib.sha256(raw).hexdigest()}


def _positive_periods(value):
    return (isinstance(value, list) and bool(value)
            and all(type(n) is int and n > 0 for n in value)
            and value == sorted(set(value)))


def load_market_definitions(config_dir: str | Path) -> dict:
    """Fail closed on unsafe scope, malformed comparisons or incomplete indices.

    The loader returns data only; it does not activate a flag, fetch a source,
    evaluate a trading rule, or approve a pending identity/amount definition.
    """
    directory = Path(config_dir)
    rules = read_yaml(directory / 'market_state.yaml')
    registry = read_yaml(directory / 'market_indices.yaml')
    try:
        if (rules['schema_version'] != 'market-state-config/1'
                or rules['authority'] != 'SHADOW_ONLY' or rules['enabled'] is not False
                or rules['timezone'] != 'Asia/Shanghai' or rules['missing_policy'] != 'DATA_BLOCK'
                or registry['schema_version'] != 'market-indices/1'
                or registry['identity_status'] not in {'PROVIDER_PROBE_PENDING', 'VERIFIED'}):
            raise ValueError
        indices = registry['indices']
        keys = ['SSE', 'SZSE', 'CYB', 'HS300', 'CSI1000', 'KC50']
        if rules['indices'] != keys or set(indices) != set(keys):
            raise ValueError
        identifiers = set()
        for item in indices.values():
            if (not isinstance(item, Mapping) or set(item) != {'name', 'thscode', 'tencent_code'}
                    or not isinstance(item['name'], str) or not item['name'].strip()
                    or not isinstance(item['thscode'], str)
                    or re.fullmatch(r'\d{6}\.(SH|SZ)', item['thscode']) is None
                    or item['tencent_code'] != item['thscode'][-2:].lower() + item['thscode'][:6]
                    or item['thscode'] in identifiers):
                raise ValueError
            identifiers.add(item['thscode'])
        daily = rules['daily']
        periods = daily['ma_periods']
        if (not _positive_periods(periods)
                or not _positive_periods(daily['return_periods'])
                or not _positive_periods(daily['distance_periods'])
                or type(daily['slope_lag']) is not int or daily['slope_lag'] <= 0
                or type(daily['minimum_history_bars']) is not int
                or daily['minimum_history_bars'] < max(periods) + daily['slope_lag']
                or daily['slope_formula'] != 'MA_CURRENT_DIV_MA_LAG_MINUS_ONE'):
            raise ValueError
        for category, pairs, otherwise in (
            ('trend_short', [('UP', 'GT'), ('DOWN', 'LT')], 'RANGE'),
            ('trend_mid', [('MID_UP', 'GT'), ('MID_DOWN', 'LT')], 'MID_FLAT'),
        ):
            table = daily[category]
            if set(table) != {name for name, _ in pairs} | {'otherwise'} or table['otherwise'] != otherwise:
                raise ValueError
            for name, comparison in pairs:
                row = table[name]
                for field in ('close_vs_ma', 'slope_vs_zero'):
                    if (not isinstance(row[field], list) or len(row[field]) != 2
                            or type(row[field][0]) is not int or row[field][0] not in periods
                            or row[field][1] != comparison):
                        raise ValueError
                if category == 'trend_short' and (
                    row['comparison'] != comparison or not _positive_periods(row['ma_order'])
                    or not set(row['ma_order']) <= set(periods)
                ):
                    raise ValueError
        arrangement = daily['arrangement']
        if arrangement['otherwise'] != 'MIXED':
            raise ValueError
        for name, comparison in [('BULL', 'GT'), ('BEAR', 'LT')]:
            if arrangement[name] != {'ma_order': periods, 'comparison': comparison}:
                raise ValueError
        weekly = rules['weekly']
        turnover = rules['turnover']
        if (not _positive_periods(weekly['ma_periods'])
                or not set(weekly['ma_periods']) <= set(periods)
                or type(weekly['slope_lag']) is not int or weekly['slope_lag'] <= 0
                or weekly['exclude_partial_from_indicators'] is not True
                or weekly.get('slope_formula') != 'MA_CURRENT_MINUS_MA_LAG'
                or weekly.get('authority') != 'CLAUDE_M28_20261030'
                or weekly.get('trend') != {
                    'UP': {'close_vs_ma': [10, 'GT'], 'slope_vs_zero': [10, 'GT'], 'ma_order': [5, 10], 'comparison': 'GT'},
                    'DOWN': {'close_vs_ma': [10, 'LT'], 'slope_vs_zero': [10, 'LT'], 'ma_order': [5, 10], 'comparison': 'LT'},
                    'otherwise': 'RANGE'}
                or turnover['mean_excludes_current'] is not True
                or not _positive_periods(turnover['ratio_periods'])
                or turnover['definition'] != 'INDEX_AMOUNT_SSE_PLUS_SZSE'
                or turnover['source_status'] not in {'UNVERIFIED', 'VERIFIED'}):
            raise ValueError
        upper, lower = (Decimal(turnover[key]) for key in ('expand_gte', 'shrink_lte'))
        if not upper.is_finite() or not lower.is_finite() or not 0 < lower < 1 < upper:
            raise ValueError
        if turnover['source_status'] == 'UNVERIFIED' and (
            turnover['source_id'] is not None or turnover['amount_unit'] is not None
        ):
            raise ValueError
        if turnover['source_status'] == 'VERIFIED' and (
            not isinstance(turnover['source_id'], str) or not turnover['source_id'].strip()
            or turnover['amount_unit'] not in {'CNY', 'CNY_10000', 'CNY_100000000'}
        ):
            raise ValueError
        if (rules['numeric'] != {'arithmetic': 'Decimal', 'comparison': 'UNROUNDED',
                                 'price_decimals': 2, 'other_decimals': 4}
                or rules['dk'] != {'status': 'SELF_RESEARCH_PENDING', 'enabled': False, 'formula': 'SSL_SMA',
                    'period': 20, 'comparison_periods': [10, 20, 30], 'label': '自研、非站点同款',
                    'site_anchor': 'NOT_REPRODUCIBLE_CLOSED', 'canonical_source': 'TENCENT',
                    'historical_comparison_source': 'NONE_TENCENT_ONLY', 'mix_sources': False,
                    'authority': 'CLAUDE_M28_20261030'}):
            raise ValueError
    except (KeyError, TypeError, ValueError, InvalidOperation, AttributeError):
        raise ValueError('MARKET_STATE_CONFIG_INVALID') from None
    return {'rules': deepcopy(rules), 'registry': deepcopy(registry)}


def index_identity(registry: Mapping, key: str, *, returned_code: str, returned_name: str) -> dict:
    """Check explicit full index code and exact provider name, never stock mapping."""
    item = registry.get('indices', {}).get(key)
    if not item or returned_code != item['thscode'] or returned_name != item['name']:
        raise ValueError('INDEX_IDENTITY_MISMATCH')
    return deepcopy(item)
