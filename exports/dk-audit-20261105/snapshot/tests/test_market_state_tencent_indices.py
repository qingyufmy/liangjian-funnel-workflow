from copy import deepcopy
from pathlib import Path

import pytest

from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.tencent_indices import index_request, parse_index_5m

REGISTRY = load_market_definitions(Path(__file__).resolve().parents[1]/'config')['registry']


def response(key='SSE'):
    item = REGISTRY['indices'][key]
    provider = item['tencent_code']
    return dict(code=0, data={provider: dict(qt={provider: ['1', item['name'], item['thscode'][:6]]},
        m5=[['202610301435', '100', '101', '102', '99', '123456']])})


@pytest.mark.parametrize('key', list(REGISTRY['indices']))
def test_six_explicit_identities_never_use_equity_map(monkeypatch, key):
    import liangjian_funnel.data.tencent_minute as equity
    def forbidden(*args, **kwargs):
        raise AssertionError('Stock map_symbol was called for an index')
    monkeypatch.setattr(equity, 'map_symbol', forbidden)
    item = REGISTRY['indices'][key]
    assert index_request(REGISTRY, key)['params'] == {'param': item['tencent_code']+',m5,,320'}
    raw = response(key)
    before = deepcopy(raw)
    row = parse_index_5m(raw, registry=REGISTRY, key=key)[0]
    assert row['symbol'] == item['thscode']
    assert row['volume'] is None and row['amount'] is None
    assert raw == before


@pytest.mark.parametrize('mutation', ['bank_key', 'bank_name', 'bank_code', 'missing_qt', 'duplicate', 'nan', 'ohlc'])
def test_provider_identity_and_number_negatives(mutation):
    raw = response()
    value = raw['data']['sh000001']
    if mutation == 'bank_key': raw['data'] = {'sz000001': value}
    elif mutation == 'bank_name': value['qt']['sh000001'][1] = '平安银行'
    elif mutation == 'bank_code': value['qt']['sh000001'][2] = '000300'
    elif mutation == 'missing_qt': value.pop('qt')
    elif mutation == 'duplicate': value['m5'].append(value['m5'][0])
    elif mutation == 'nan': value['m5'][0][2] = 'NaN'
    else: value['m5'][0][3] = '1'
    with pytest.raises(ValueError):
        parse_index_5m(raw, registry=REGISTRY, key='SSE')
