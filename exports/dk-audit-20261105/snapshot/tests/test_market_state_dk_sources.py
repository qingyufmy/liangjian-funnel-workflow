from copy import deepcopy
from datetime import datetime
from decimal import Decimal

import pytest

from liangjian_funnel.market_state.dk_sources import parse_eastmoney, parse_tencent, compare_overlap, warmup_n_limit


def originals():
    em = dict(rc=0, data=dict(code='002463', name='沪电股份', klines=[
        f'2026-10-26 {time},100,98.5,101,98,100,990000,0,0,0,0' for time in ('14:35','14:40','14:45')]))
    qq = dict(code=0, data={'sz002463': {'m5': [
        [f'20261026{time}', '100', '98.5', '101', '98', '100'] for time in ('1435','1440','1445')]}})
    return em, qq


def test_reported_amount_and_real_tencent_units_have_separate_meaning():
    em, qq = originals()
    a, b = parse_eastmoney(em), parse_tencent(qq)
    assert a[0]['volume'] == 10000 and b[0]['volume'] == 10000
    assert a[0]['amount_kind'] == 'reported' and a[0]['amount'] == Decimal('990000')
    assert b[0]['amount_kind'] == 'ohlc_estimate'
    assert type(b[0]['bar_end']) is datetime
    result = compare_overlap(a, b)
    assert result['status'] == 'MATCH' and result['compared'] == 3 and result['amount_compared'] is False


def test_missing_reported_amount_never_uses_estimate():
    em, _ = originals()
    em['data']['klines'][0] = em['data']['klines'][0].replace(',990000,', ',-,')
    row = parse_eastmoney(em)[0]
    assert row['amount'] is None and row['amount_kind'] == 'legacy_unspecified'


@pytest.mark.parametrize('key,value', [('code','000001'), ('name','平安银行')])
def test_wrong_security_identity_is_rejected(key, value):
    em, _ = originals()
    em['data'][key] = value
    with pytest.raises(ValueError, match='IDENTITY'):
        parse_eastmoney(em)


def test_bool_status_is_not_the_integer_zero_enum():
    em, qq = originals()
    em['rc'] = False
    qq['code'] = False
    with pytest.raises(ValueError):
        parse_eastmoney(em)
    with pytest.raises(ValueError):
        parse_tencent(qq)


def test_every_ohlcv_row_must_match_before_em_is_eligible():
    em, qq = originals()
    a, b = parse_eastmoney(em), parse_tencent(qq)
    a[1]['volume'] += 1
    result = compare_overlap(a, b)
    assert result['status'] == 'MISMATCH' and result['matched'] == 2
    assert result['differences'][0]['fields'] == ['volume']
    a = parse_eastmoney(em)
    a.pop(1)
    result = compare_overlap(a, b)
    assert result['status'] == 'MISMATCH' and result['differences'][0]['reason'] == 'MISSING_ON_ONE_SOURCE'


def test_empty_or_disjoint_overlap_is_unproven():
    em, qq = originals()
    a, b = parse_eastmoney(em), parse_tencent(qq)
    assert compare_overlap([], b)['status'] == 'UNPROVEN'
    assert compare_overlap(a[:1], b[-1:])['status'] == 'UNPROVEN'


def test_duplicate_actual_bar_cannot_be_silently_removed():
    em, qq = originals()
    em['data']['klines'].append(em['data']['klines'][0])
    qq['data']['sz002463']['m5'].append(deepcopy(qq['data']['sz002463']['m5'][0]))
    with pytest.raises(ValueError, match='DUPLICATE'):
        parse_eastmoney(em)
    with pytest.raises(ValueError, match='DUPLICATE'):
        parse_tencent(qq)


def test_short_history_truncates_grid_without_claiming_no_solution():
    assert warmup_n_limit(179)['maximum_n'] == 59
    assert warmup_n_limit(180)['maximum_n'] == 60
    short = warmup_n_limit(14)
    assert short['status'] == 'HISTORY_INSUFFICIENT' and short['maximum_n'] is None
    assert short['no_solution_claim'] is False
