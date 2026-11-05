from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pytest

from liangjian_funnel.market_state.bars15m import TZ
from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.dk_shadow import build_dk_shadow,canonical_minute_input
from liangjian_funnel.market_state.ssl import CANONICAL_SOURCE
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

DEFS = load_market_definitions(Path(__file__).resolve().parents[1]/'config')
CAL = ExchangeTradingCalendar()
NOW = datetime(2026, 10, 30, 17, 1, tzinfo=TZ)


def args():
    return dict(plan_symbols=['002463.SZ'], definitions=DEFS, trade_date=date(2026, 10, 30), observed_at=NOW, calendar=CAL)


def test_every_missing_index_and_required_plan_is_explicit():
    result = build_dk_shadow({}, **args())
    assert set(result['series']) == set(DEFS['registry']['indices']) | {'002463.SZ'}
    assert all(v['reasons'] == ['TENCENT_SERIES_NOT_SUPPLIED'] for v in result['series'].values())
    assert result['enabled'] is False and result['production_writes'] == result['source_calls'] == 0


def series(*, stale=False, gap=False):
    day = '20261029' if stale else '20261030'
    rows = []
    for hour, minutes in [(14, [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]), (15, [0])]:
        for minute in minutes:
            rows.append([day+f'{hour:02}{minute:02}', '100', '100', '101', '99', '1000'])
    if gap:
        rows.pop(4)
    return dict(payload=dict(code=0, data={'sz002463':dict(qt={'sz002463':['1', '沪电股份', '002463']},m5=rows)}),
        source_id=CANONICAL_SOURCE, input_sha256='b'*64, collection_status='VERIFIED_COLLECTION',
        pit=dict(source_trade_date='2026-10-30',target_trade_date='2026-10-30',mode='INTRADAY',as_of='2026-10-30T15:00:00+08:00',
            market_close_at='2026-10-30T15:00:00+08:00',provider_timestamp='2026-10-30T15:00:00+08:00',
            capture_started_at='2026-10-30T17:00:00+08:00',capture_completed_at='2026-10-30T17:00:01+08:00'))


def test_current_short_window_is_recorded_but_warmup_and_twenty_days_are_not_passed():
    source = series()
    before = deepcopy(source)
    output = build_dk_shadow({'002463.SZ': source}, **args())['series']['002463.SZ']
    assert output['status'] == 'SHADOW_RECORDED'
    assert set(output['periods']) == {'10', '20', '30'}
    assert output['periods']['20']['current']['status'] == 'DK_WARMUP_INSUFFICIENT'
    assert output['periods']['20']['evaluation']['coverage_status'] == 'HISTORY_INSUFFICIENT_20_TRADE_DAYS'
    assert source == before


def test_stale_or_failed_collection_cannot_become_current_facts():
    source = series(stale=True)
    output = build_dk_shadow({'002463.SZ': source}, **args())['series']['002463.SZ']
    assert output['status'] == 'DATA_BLOCK' and 'DK_SHADOW_INPUT_AS_OF_MISMATCH' in output['reasons']
    source = series()
    source['collection_status'] = 'HISTORICAL_COLLECTION_NONCOMPLIANT'
    output = build_dk_shadow({'002463.SZ': source}, **args())['series']['002463.SZ']
    assert output['status'] == 'DATA_BLOCK' and 'COLLECTION_NONCOMPLIANT_DIAGNOSTIC_ONLY' in output['reasons']


def test_internal_incomplete_15m_is_not_hidden_by_filtering():
    output = build_dk_shadow({'002463.SZ': series(gap=True)}, **args())['series']['002463.SZ']
    assert output['status'] == 'DATA_BLOCK' and output['reasons'] == ['SSL_SERIES_GAP_DUPLICATE_OR_REORDERED']


@pytest.mark.parametrize('symbols', [['002463'], ['002463.SZ', '002463.SZ'], ['002463.BJ']])
def test_bad_plan_scope_rejected(symbols):
    values = args()
    values['plan_symbols'] = symbols
    with pytest.raises(ValueError, match='DK_SHADOW_SCOPE_INVALID'):
        build_dk_shadow({}, **values)


@pytest.mark.parametrize('case',['future_capture','wrong_target','wrong_mode','input_after_as_of','provider_before_input'])
def test_minute_pit_is_bound_to_actual_input_and_observation(case):
    source=series()
    if case=='future_capture':source['pit']['capture_completed_at']='2026-10-30T17:02:00+08:00'
    elif case=='wrong_target':source['pit']['target_trade_date']='2026-11-02'
    elif case=='wrong_mode':source['pit']['mode']='CLOSE'
    elif case=='input_after_as_of':source['pit']['as_of']='2026-10-30T14:55:00+08:00'
    else:source['pit']['provider_timestamp']='2026-10-30T14:59:00+08:00'
    output=build_dk_shadow({'002463.SZ':source},**args())['series']['002463.SZ']
    assert output['status']=='DATA_BLOCK' and not output['periods']


def test_index_cannot_be_declared_a_plan_equity():
    values=args();values['plan_symbols']=['000001.SH']
    with pytest.raises(ValueError,match='DK_PLAN_SCOPE_CONTAINS_INDEX'):
        build_dk_shadow({},**values)


def test_shared_minute_parser_binds_plan_equity_quote_clock_and_keeps_index_keys_separate():
    source=series();payload=source['payload'];fields=['']*31
    fields[1],fields[2],fields[3],fields[4],fields[30]='沪电股份','002463','100','100','20261030150000'
    payload['data']['sz002463']['qt']['sz002463']=fields
    values=dict(definitions=DEFS,trade_date=date(2026,10,30),input_sha256='b'*64,
        capture_started_at='2026-10-30T17:00:00+08:00',capture_completed_at='2026-10-30T17:00:01+08:00')
    dataset,rows=canonical_minute_input(payload,key='002463.SZ',**values)
    assert rows[-1]['bar_end'].isoformat()==dataset['pit']['as_of']
    assert dataset['pit']['provider_timestamp']=='2026-10-30T15:00:00+08:00'
    assert build_dk_shadow({'002463.SZ':dataset},**args())['series']['002463.SZ']['status']=='SHADOW_RECORDED'
    with pytest.raises(ValueError,match='INDEX_REGISTRY_KEY_REQUIRED'):
        canonical_minute_input(payload,key='000001.SH',**values)
    fields[30]='not-a-provider-clock'
    with pytest.raises(ValueError,match='PROVIDER_CLOCK'):
        canonical_minute_input(payload,key='002463.SZ',**values)
