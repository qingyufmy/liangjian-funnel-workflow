from copy import deepcopy
from datetime import datetime,date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.team_v2_emotion_source import derive_identity,emotion_pool_stats
from liangjian_funnel.runtime.shadow_price_limits import round_price_limits,RATES
from liangjian_funnel.evaluation.ablation import price_limits as frozen
from liangjian_funnel.runtime.team_v2_regime import classify_emotion,load_rules
from pathlib import Path

TZ=ZoneInfo('Asia/Shanghai');NOW=datetime(2026,10,19,9,45,3,tzinfo=TZ)
AT=NOW.replace(second=0);CAT=NOW.replace(hour=8,minute=20,second=0)
CAL=ExchangeTradingCalendar()


def row(symbol='000001.SZ',**kw):
    return dict(thscode=symbol,exchange=symbol[-2:],name='普通股票',asset_type='a-share',list_date='2020-01-02')|kw


def quote(symbol='000001.SZ',**kw):
    return dict(thscode=symbol,volume=10000,last_price=10,high_price=10.5,prev_price=10)|kw


def universe():
    cats=[row(f'{n:06}.SZ') for n in range(1,501)]
    prices=[quote(r['thscode']) for r in cats]
    prior=dict(complete=True,trade_date='2026-10-16',known_at='2026-10-19T08:20:00+08:00',
        counts={r['thscode']:0 for r in cats})
    return cats,prices,prior


def stats(cats,prices,prior=None,**kw):
    return emotion_pool_stats(cats,prices,catalogue_timestamp=CAT.isoformat(),
        snapshot_timestamp=AT.isoformat(),observed_at=NOW,calendar=CAL,previous_streaks=prior,
        catalogue_complete=True,snapshot_complete=True,**kw)


def test_real_endpoint_fields_four_counts_route_to_existing_classifier():
    cats,prices,prior=universe()
    for i in range(40):prices[i].update(last_price=11,high_price=11);prior['counts'][cats[i]['thscode']]=2
    for i in range(40,60):prices[i].update(last_price=10.8,high_price=11)
    for i in range(60,65):prices[i].update(last_price=9,high_price=10)
    actual=stats(cats,prices,prior)
    assert actual['status']=='READY'
    assert (actual['limit_up_count'],actual['limit_down_count'],actual['highest_streak'])==(40,5,3)
    assert actual['broken_board_rate']==20/60
    assert actual['authority']=='EMOTION_STATISTICS_ONLY' and actual['source_authenticated'] is False
    rules=load_rules(Path(__file__).resolve().parents[1]/'config/regime_live.yaml')
    assert classify_emotion(actual,'TREND_MAINLINE',rules)=='EMOTION_ON'
    assert all(field['source'] and field['as_of'] for field in actual['identities'][0]['fields'].values())


@pytest.mark.parametrize('symbol,name,board,rate',[
    ('600001.SH','普通','SSE_MAIN','.10'),('000001.SZ','*ST普通','SZSE_MAIN','.05'),
    ('300001.SZ','ST普通','CHINEXT','.20'),('688001.SH','*ST普通','STAR','.20')])
def test_derived_identity_board_st_rates_never_grants_trading(symbol,name,board,rate):
    actual=derive_identity(row(symbol,name=name),quote(symbol),as_of=AT,catalogue_as_of=CAT,calendar=CAL)
    assert actual['fields']['board']['value']==board
    assert Decimal(actual['fields']['limit_rate']['value'])==Decimal(rate)
    assert actual['fields']['security_status']['value']=='TRADING_OBSERVED'
    assert actual['included']==('ST' not in name)
    assert 'entry_tradable' not in actual and 'tradable' not in actual


def test_bj_st_delisting_new_stock_and_no_trades_separate_denominator():
    cats,prices,prior=universe()
    extra=[row('920001.BJ'),row('000501.SZ',name='ST风险'),row('000502.SZ',name='退市股'),
        row('000503.SZ',list_date='2026-10-14'),row('000504.SZ'),row('000505.SZ')]
    cats.extend(extra);prices.extend(quote(r['thscode']) for r in extra)
    prices[-2]['volume']=0;prices[-1]['last_price']=None
    actual=stats(cats,prices,prior)
    assert actual['status']=='READY' and actual['expected_count']==505
    assert actual['observed_count']==500 and actual['coverage']==500/505
    assert actual['excluded_counts']=={'BJ_EXCLUDED':1,'ST_EXCLUDED':1,'DELISTING_EXCLUDED':1,
        'LISTING_PERIOD_UNPROVEN_OR_LT5':1,'NO_TRADES_OR_QUOTE_MISSING':2}


def test_five_listing_sessions_uses_actual_holiday_calendar():
    # Oct 8,9,12,13,14: weekend and National Day cannot count as sessions.
    at=datetime(2026,10,14,9,45,tzinfo=TZ)
    for listing,wanted in [('2026-10-08',True),('2026-10-09',False)]:
        actual=derive_identity(row(list_date=listing),quote(),as_of=at,catalogue_as_of=at,calendar=CAL)
        assert actual['fields']['minimum_five_sessions']['value'] is wanted


@pytest.mark.parametrize('which,value',[
    ('snapshot_timestamp','2026-10-16T14:18:15+08:00'),
    ('snapshot_timestamp','2026-10-19T09:46:00+08:00'),
    ('snapshot_timestamp','2026-10-19T12:00:00+08:00'),
    ('snapshot_timestamp','2026-10-19T09:35:00+08:00'),
    ('catalogue_timestamp','2026-10-16T08:20:00+08:00'),
    ('catalogue_complete',False),('snapshot_complete',False)])
def test_provider_timestamp_not_collection_clock_or_incomplete_positive(which,value):
    cats,prices,prior=universe();args=dict(catalogue_timestamp=CAT.isoformat(),snapshot_timestamp=AT.isoformat(),
        observed_at=NOW,calendar=CAL,previous_streaks=prior,catalogue_complete=True,snapshot_complete=True)
    args[which]=value
    actual=emotion_pool_stats(cats,prices,**args)
    assert actual['status']=='DATA_BLOCK' and actual['limit_up_count'] is None


def test_millisecond_timestamp_preserved_not_relabelled():
    cats,prices,prior=universe()
    actual=emotion_pool_stats(cats,prices,catalogue_timestamp=CAT.timestamp()*1000,
        snapshot_timestamp=AT.timestamp()*1000,observed_at=NOW,calendar=CAL,
        previous_streaks=prior,catalogue_complete=True,snapshot_complete=True)
    assert actual['status']=='READY' and actual['as_of']==AT.isoformat()


@pytest.mark.parametrize('defect',['missing','partial','late','wrong_day','counts_type'])
def test_prior_streak_must_be_morning_sealed_not_assumed_zero(defect):
    cats,prices,prior=universe();prices[0].update(last_price=11,high_price=11)
    if defect=='missing':prior=None
    elif defect=='partial':prior['counts'].pop(cats[0]['thscode'])
    elif defect=='late':prior['known_at']=AT.isoformat()
    elif defect=='wrong_day':prior['trade_date']='2026-10-15'
    else:prior['counts']=[]
    actual=stats(cats,prices,prior)
    assert actual['status']=='DATA_BLOCK' and actual['reason_codes']==['EMOTION_PRIOR_STREAKS_UNPROVEN']
    assert actual['highest_streak'] is None


def test_coverage_and_duplicate_rows_fail_without_invented_universe():
    cats,prices,prior=universe()
    assert stats(cats,prices[:374],prior)['reason_codes']==['EMOTION_COVERAGE_INSUFFICIENT']
    assert stats(cats,prices+prices[:1],prior)['reason_codes']==['EMOTION_DUPLICATE_SYMBOL']
    assert stats(cats+cats[:1],prices,prior)['reason_codes']==['EMOTION_DUPLICATE_SYMBOL']


def test_bad_prevclose_does_not_make_a_fake_limit_hit():
    cats,prices,prior=universe();prices[0].update(prev_price='NaN',last_price=11,high_price=11)
    actual=stats(cats,prices,prior)
    # 499 actual usable symbols is below the existing 500-observation breadth gate.
    assert actual['status']=='DATA_BLOCK' and actual['excluded_counts']['LIMIT_PRICE_INPUT_UNPROVEN']==1


@pytest.mark.parametrize('preclose',['.01','.02','.05','1.05','5.55','10','93.27'])
@pytest.mark.parametrize('board',list(RATES))
def test_public_rounding_matches_frozen_ordinary_geometry(preclose,board):
    # Independent frozen formula, no modified old constants or pin fixture.
    p=Decimal(preclose);rate=frozen.RATES[board];tick=Decimal('.01')
    from decimal import ROUND_HALF_UP
    upper=(p*(1+rate)).quantize(tick,rounding=ROUND_HALF_UP)
    lower=(p*(1-rate)).quantize(tick,rounding=ROUND_HALF_UP)
    if abs(upper-p)<tick:upper=p+tick
    if abs(lower-p)<tick:lower=p-tick
    assert round_price_limits(p,rate,tick)==(max(tick,upper),max(tick,lower))
