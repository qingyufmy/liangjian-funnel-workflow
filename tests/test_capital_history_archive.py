"""Historical dates cannot borrow later knowledge or another source's units."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import pytest
from liangjian_funnel.data.capital_history import (
    EASTMONEY_HISTORY, TENCENT_DAILY, parse_eastmoney_history,
    archive_observation, read_observation, research_windows, tencent_daily_observation,
)

NOW=datetime.fromisoformat('2026-10-24T13:38:49+08:00')
SYMBOL='603278.SH'
DAYS=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16',
      '2026-10-19','2026-10-20','2026-10-21','2026-10-22','2026-10-23']

def raw(days=DAYS):
    return json.dumps(dict(rc=0,data=dict(code='603278',market=1,name='fixture',
        klines=[d+',10,-5,-5,4,6,10,-5,-5,4,6,12,1,0,0' for d in days]))).encode()

def observation(data=None):
    return parse_eastmoney_history(raw() if data is None else data,symbol=SYMBOL,received_at=NOW)

def windows(values,**kw):
    return research_windows(values,symbol=SYMBOL,source_id=EASTMONEY_HISTORY,
        sessions=DAYS,known_by=kw.get('known_by',NOW))

def test_history_archive_replay_has_exact_raw_and_separate_actual_receipt(tmp_path):
    value=observation();ref=archive_observation(tmp_path,raw=raw(),observation=value)
    assert read_observation(ref)==value
    assert archive_observation(tmp_path,raw=raw(),observation=value)==ref
    assert value['historical_point_in_time_proven'] is False
    result=windows([value])
    assert result['windows']['10d']['net_amount_native']=='100'
    assert result['windows']['3d']['net_ratio_percent'] is None
    assert result['formal_factor_eligible'] is False
    assert result['execution_authority'] is False

def test_later_history_receipt_cannot_be_used_as_october_23_known_data():
    result=windows([observation()],known_by=NOW-timedelta(days=1))
    assert result['late_receipts_rejected']==1
    assert all(r['availability_state']=='SOURCE_UNAVAILABLE' for r in result['windows'].values())


@pytest.mark.parametrize('days',[['2026-10-17','2026-10-19'],['2026-10-12','2026-10-14']])
def test_weekend_or_skipped_exchange_session_cannot_fake_a_continuous_window(days):
    with pytest.raises(ValueError,match='EXCHANGE_SESSIONS'):
        research_windows([observation()],symbol=SYMBOL,source_id=EASTMONEY_HISTORY,sessions=days,known_by=NOW)

@pytest.mark.parametrize('kind',['identity','market','duplicate','future','fields','buckets','percentage','empty','envelope'])
def test_provider_contract_failure_is_not_an_observed_zero(kind):
    value=json.loads(raw())
    if kind=='identity':value['data']['code']='601628'
    if kind=='market':value['data']['market']=0
    if kind=='duplicate':value['data']['klines'].append(value['data']['klines'][0])
    if kind=='future':value['data']['klines'][0]=value['data']['klines'][0].replace(DAYS[0],'2099-01-01')
    if kind=='fields':value['data']['klines'][0]+=',0'
    if kind=='buckets':value['data']['klines'][0]=DAYS[0]+',10,-5,-5,1,1,10,-5,-5,4,6,12,1,0,0'
    if kind=='percentage':value['data']['klines'][0]=DAYS[0]+',10,-5,-5,4,6,110,-5,-5,4,6,12,1,0,0'
    if kind=='empty':value['data']['klines']=[]
    if kind=='envelope':value=[]
    with pytest.raises(ValueError):observation(json.dumps(value).encode())

def test_missing_session_or_missing_field_is_visible_without_averaging_percentages():
    value=observation(raw(DAYS[:-1]))
    result=windows([value])
    assert result['windows']['3d']['missing_sessions']==[DAYS[-1]]
    assert result['windows']['3d']['net_amount_native'] is None
    malformed=raw().replace(b'2026-10-23,10,',b'2026-10-23,-,')
    result=windows([observation(malformed)])
    assert result['windows']['1d']['invalid_or_conflicting_sessions']==[DAYS[-1]]

def test_conflicting_revisions_are_not_last_write_wins():
    first=observation();other=deepcopy(first)
    other['rows'][-1]['values']['main_ratio']='11'
    result=windows([first,other]);assert result['windows']['1d']['availability_state']=='SOURCE_UNAVAILABLE'

@pytest.mark.parametrize('kind',['source','symbol'])
def test_tencent_and_eastmoney_or_two_stocks_cannot_be_spliced(kind):
    value=observation();value['source_id' if kind=='source' else 'symbol']=TENCENT_DAILY if kind=='source' else '601628.SH'
    with pytest.raises(ValueError,match='MIXED'):windows([value])

def test_immutable_bytes_tampering_is_not_repaired_or_overwritten(tmp_path):
    ref=archive_observation(tmp_path,raw=raw(),observation=observation())
    from pathlib import Path
    Path(ref['path']).write_text('{}')
    with pytest.raises(ValueError):read_observation(ref)
    with pytest.raises(ValueError):archive_observation(tmp_path,raw=raw(),observation=observation())
    assert Path(ref['path']).read_text()=='{}'

def qq_raw(main='10',gross='100'):
    return json.dumps(dict(data=dict(todayFundFlow=dict(stockCode='sh603278',mainNetIn=main,
        mainIn=gross,mainOut='0',retailIn='0',retailOut='0',superFlow='6',bigFlow='4')))).encode()

def test_new_daily_tencent_archive_keeps_real_gross_denominator_and_quote_limitation():
    observations=[]
    for i,day in enumerate(DAYS):
        raw_value=qq_raw('10' if i%2 else '20','100' if i%2 else '1000')
        obs=tencent_daily_observation(raw_value,symbol=SYMBOL,provider_timestamp=day.replace('-','')+'150000',
            received_at=datetime.fromisoformat(day+'T15:01:00+08:00'))
        assert obs['fund_event_time_proven'] is False
        observations.append(obs)
    result=research_windows(observations,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW)
    assert result['windows']['10d']['net_amount_native']=='150'
    assert result['windows']['10d']['net_ratio_percent'].startswith('2.7272727')
    assert observations[0]['rows'][0]['raw_buckets']['mainIn']=='1000'

def test_actual_tencent_collector_archives_original_without_changing_daily_only_capability(tmp_path):
    from liangjian_funnel.data.a2_market import collect_tencent_capital_flow
    original=qq_raw()
    result=collect_tencent_capital_flow(as_of=NOW.replace(day=23),now=NOW,expected_symbols=[SYMBOL],
        cache_dir=tmp_path,allow_historical_recovery=True,fetch_trade_timestamp=lambda:'20261023150000',workers=1,
        fetch_symbol=lambda s:dict(symbol=s,net_inflow_amount=10,net_inflow_ratio=10,
            _capital_original=dict(raw=original,received_at=NOW)))
    refs=result['daily_archive']['references'];assert len(refs)==1
    assert read_observation(refs[0])['raw_sha256']==refs[0]['raw_sha256']
    assert result['provider_capabilities']==dict(today=True,**{'3d':False,'5d':False,'10d':False})

def test_legacy_normalized_cache_does_not_become_new_raw_evidence(tmp_path):
    from liangjian_funnel.data.a2_market import collect_tencent_capital_flow
    result=collect_tencent_capital_flow(as_of=NOW,now=NOW,expected_symbols=[SYMBOL],cache_dir=tmp_path,
        fetch_trade_timestamp=lambda:'20261024130000',workers=1,
        fetch_symbol=lambda s:dict(symbol=s,net_inflow_amount=10,net_inflow_ratio=10))
    assert result['daily_archive']['references']==[]
    assert result['daily_archive']['raw_originals_missing_or_failed']==[SYMBOL]


def test_date_only_cache_cannot_claim_changed_requested_universe_or_recollect_it(tmp_path):
    from liangjian_funnel.data.a2_market import collect_tencent_capital_flow
    original=collect_tencent_capital_flow(as_of=NOW,now=NOW,expected_symbols=[SYMBOL],cache_dir=tmp_path,
        fetch_trade_timestamp=lambda:'20261024130000',workers=1,
        fetch_symbol=lambda s:dict(symbol=s,net_inflow_amount=10,net_inflow_ratio=10))
    before=(tmp_path/'capital-flow-2026-10-24.json').read_bytes()
    def forbidden(*args):raise AssertionError('must not recollect')
    result=collect_tencent_capital_flow(as_of=NOW,now=NOW,expected_symbols=[SYMBOL,'601628.SH'],cache_dir=tmp_path,
        fetch_trade_timestamp=forbidden,fetch_symbol=forbidden)
    assert result['available'] is False
    assert result['reason_code']=='CAPITAL_FLOW_CACHE_REQUESTED_SCOPE_MISMATCH'
    assert result['rejected_cache_identity']['cached_count']==1
    assert (tmp_path/'capital-flow-2026-10-24.json').read_bytes()==before
    exact=collect_tencent_capital_flow(as_of=NOW,now=NOW,expected_symbols=[SYMBOL],cache_dir=tmp_path,
        fetch_trade_timestamp=forbidden,fetch_symbol=forbidden)
    assert exact==original
