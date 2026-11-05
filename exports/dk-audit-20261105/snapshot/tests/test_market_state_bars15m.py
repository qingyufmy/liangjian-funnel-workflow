from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from liangjian_funnel.market_state.bars15m import ENDS, TZ, compose_15m, dk_reference_anchor
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter

DAY = date(2026, 10, 26)


def stamp(clock, day=DAY):
    return datetime.fromisoformat(f'{day.isoformat()}T{clock}+08:00')


def rows():
    result = []
    for end in ENDS:
        close = stamp(end + ':00')
        for offset in (10, 5, 0):
            result.append(dict(symbol='002463.SZ', interval='5m', bar_end=close-timedelta(minutes=offset),
                open='100.00', high='101.00', low='98.00', close='98.500', volume='100', amount='10000',
                volume_unit='shares', amount_kind='reported', amount_unit='CNY',
                source_id='LOCAL_CONTROLLED_ROWS', adjust_mode='none', evidence_kind='MARKET_BAR'))
    return result


def compose(values, **changes):
    kwargs = dict(symbol='002463.SZ', trade_date=DAY, observed_at=stamp('15:01:00'),
                  market_close_at=stamp('15:00:00'), calendar=ExchangeTradingCalendar())
    return compose_15m(values, **(kwargs | changes))


def test_full_closed_day_has_sixteen_exact_three_part_bars():
    result = compose(rows())
    assert len(result.bars) == 16
    assert all(bar.status == 'CLOSED' and len(bar.present_5m) == 3 for bar in result.bars)
    first = result.bars[0]
    assert tuple(t.strftime('%H:%M') for t in first.expected_5m) == ('09:35', '09:40', '09:45')
    assert (first.open, first.high, first.low, first.close) == tuple(map(Decimal, ('100', '101', '98', '98.5')))
    assert first.volume_shares == 300 and first.reported_amount_cny == 30000
    assert first.estimated_amount_cny is None


def test_missing_part_stays_incomplete_with_no_partial_price():
    data = rows()
    data.pop(1)
    result = compose(data)
    assert result.bars[0].status == 'INCOMPLETE'
    assert result.bars[0].close is None and len(result.bars[0].present_5m) == 2
    assert result.bars[1].status == 'CLOSED'


def test_auction_is_excluded_and_does_not_fill_missing_0935():
    data = rows()
    auction = deepcopy(data[0])
    auction['bar_end'] = stamp('09:30:00')
    result = compose([auction, *data[1:]])
    assert result.excluded_auction_count == 1
    assert result.bars[0].status == 'INCOMPLETE'


def test_sixty_second_close_delay_is_strict():
    data = rows()[:3]
    before = compose(data, observed_at=stamp('09:45:59'))
    after = compose(data, observed_at=stamp('09:46:00'))
    assert before.bars[0].status == 'NOT_CLOSED' and before.bars[0].close is None
    assert after.bars[0].status == 'CLOSED'


def test_composition_has_no_prefix_repainting():
    data, full = rows(), compose(rows())
    calendar = ExchangeTradingCalendar()
    for index, end in enumerate(ENDS):
        end = stamp(end + ':00')
        partial = compose(data[:(index+1)*3], observed_at=end+timedelta(seconds=60), calendar=calendar)
        assert partial.bars[:index+1] == full.bars[:index+1]


@pytest.mark.parametrize('change,reason', [
    ({'symbol': '000001.SZ'}, 'IDENTITY'), ({'interval': '1m'}, 'IDENTITY'),
    ({'evidence_kind': 'SYNTHETIC_QUOTE'}, 'IDENTITY'),
    ({'bar_end': '2026-10-26T09:35:00'}, 'TIMESTAMP'),
    ({'bar_end': stamp('11:35:00')}, 'GRID'),
    ({'bar_end': stamp('15:05:00')}, 'FUTURE'),
    ({'open': True}, 'NUMBER'), ({'high': 'NaN'}, 'NUMBER'),
    ({'low': '101'}, 'OHLC'), ({'amount_kind': 'actual'}, 'AMOUNT_KIND'),
    ({'amount_unit': '万元'}, 'AMOUNT_UNIT'),
])
def test_bad_source_rows_are_rejected(change, reason):
    data = rows()
    data[0].update(change)
    with pytest.raises(ValueError, match=reason):
        compose(data)


def test_duplicate_bar_is_not_silently_deduplicated():
    data = rows()
    with pytest.raises(ValueError, match='DUPLICATE'):
        compose([data[0], *data])


def test_missing_evidence_kind_is_not_assumed_to_be_a_real_bar():
    data = rows()
    data[0].pop('evidence_kind')
    with pytest.raises(ValueError, match='IDENTITY'):
        compose(data)


def test_other_day_not_relabelled_as_current():
    data = rows()
    data[0]['bar_end'] = stamp('09:35:00', date(2026, 10, 23))
    with pytest.raises(ValueError, match='OTHER_DAY'):
        compose(data)


@pytest.mark.parametrize('field,value', [('source_id', 'ANOTHER_SOURCE'), ('adjust_mode', 'forward')])
def test_source_and_adjustment_mixing_are_rejected(field, value):
    data = rows()
    data[1][field] = value
    with pytest.raises(ValueError, match='MIXED_SOURCE_OR_ADJUSTMENT'):
        compose(data)


def test_holiday_and_unknown_half_day_layout_fail_closed():
    with pytest.raises(ValueError, match='NON_TRADING_DAY'):
        compose([], trade_date=date(2026, 10, 1), market_close_at=stamp('15:00:00', date(2026, 10, 1)))
    with pytest.raises(ValueError, match='SESSION_LAYOUT_UNSUPPORTED'):
        compose([], market_close_at=stamp('11:30:00'))


def test_estimated_amount_and_undeclared_units_cannot_prove_anchor():
    data = rows()
    for row in data:
        row['amount_kind'] = 'ohlc_estimate'
    result = compose(data)
    assert result.bars[0].reported_amount_cny is None
    assert result.bars[0].estimated_amount_cny == 30000
    assert dk_reference_anchor(result)['status'] == 'NOT_CONFIRMED'
    for row in data:
        row.pop('amount_unit')
        row['volume_unit'] = 'legacy_unspecified'
    unknown = compose(data).bars[0]
    assert unknown.volume_shares is None and unknown.reported_amount_cny is None
    assert unknown.estimated_amount_cny is None


def test_controlled_exact_anchor_and_mismatch_report():
    data = rows()
    anchor = [row for row in data if stamp('14:35:00') <= row['bar_end'] <= stamp('14:45:00')]
    for row, volume, amount in zip(anchor, ('796400', '796500', '796500'), ('78799700', '78799700', '78799800')):
        row.update(volume=volume, amount=amount)
    result = dk_reference_anchor(compose(data))
    assert result['status'] == 'EXACT_MATCH'
    assert result['reported_amount_delta_cny'] == 0
    anchor[-1]['amount'] = '78799801'
    result = dk_reference_anchor(compose(data))
    assert result['status'] == 'VOLUME_CONFIRMED_WITH_DIFFERENCES' and result['reported_amount_delta_cny'] == 1
    assert result['volume_hard_check_passed'] is True and result['amount_status'] == 'MISMATCH'


def test_1124_volume_hard_check_allows_explicit_unproven_amount():
    data = rows()
    anchor = [row for row in data if stamp('14:35:00') <= row['bar_end'] <= stamp('14:45:00')]
    for row, volume in zip(anchor, ('796400', '796500', '796500')):
        row.update(volume=volume, amount_kind='ohlc_estimate')
    result = dk_reference_anchor(compose(data))
    assert result['volume_hard_check_passed'] is True
    assert result['status'] == 'VOLUME_CONFIRMED_AMOUNT_UNPROVEN' and result['amount_note'] == '额未证'


def test_real_tencent_normalizer_output_cannot_be_relabelled_as_reported_amount():
    originals = [TencentIntradayAdapter._bar('002463.SZ', '5m',
        [f'20261026{clock}', 100, 98.5, 101, 98, 100]).model_dump()
        for clock in ('1435', '1440', '1445')]
    assert all(type(row['bar_end']) is datetime and row['amount_kind'] == 'ohlc_estimate' for row in originals)
    result = compose(originals)
    anchor = next(bar for bar in result.bars if bar.bar_end == stamp('14:45:00'))
    assert anchor.status == 'CLOSED' and anchor.volume_shares == 30000
    assert anchor.reported_amount_cny is None and 'AMOUNT_UNIT_UNVERIFIED' in anchor.reasons
    assert dk_reference_anchor(result)['status'] == 'NOT_CONFIRMED'
