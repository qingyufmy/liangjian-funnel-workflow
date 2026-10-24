"""Full-session history cannot borrow partial quotes, units or later receipts."""
from datetime import datetime
import json
import pytest
from liangjian_funnel.data.capital_history import (
    TENCENT_DAILY, tencent_daily_observation, research_windows,
)

SYMBOL='603278.SH'
DAYS=['2026-10-19','2026-10-20','2026-10-21']
NOW=datetime.fromisoformat('2026-10-21T18:00:00+08:00')

def obs(day,hhmmss='150000',unit=None):
    raw=json.dumps({'data':{'todayFundFlow':dict(stockCode='sh603278',mainNetIn='10',mainIn='60',mainOut='50',retailIn='40',retailOut='50')}}).encode()
    v=tencent_daily_observation(raw,symbol=SYMBOL,provider_timestamp=day.replace('-','')+hhmmss,
        received_at=datetime.fromisoformat(day+'T18:00:00+08:00'))
    if unit is not None:v['amount_unit']=unit
    return v

def windows(values):
    return research_windows(values,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW)

def test_a_1400_quote_is_not_a_completed_day_in_three_session_history():
    v=windows([obs(DAYS[0],'140000'),obs(DAYS[1]),obs(DAYS[2])])
    assert v['windows']['3d']['availability_state']=='SOURCE_UNAVAILABLE'
    assert v['windows']['3d']['net_amount_native'] is None
    assert 'SESSION_NOT_CLOSED' in v['windows']['3d']['invalid_session_reasons'][DAYS[0]]
    assert v['windows']['1d']['net_amount_native']=='10'

def test_currency_unit_change_cannot_be_summed_into_one_source_window():
    v=windows([obs(DAYS[0],unit='CNY'),obs(DAYS[1],unit='CNY_10000'),obs(DAYS[2],unit='CNY')])
    assert v['windows']['3d']['availability_state']=='SOURCE_UNAVAILABLE'
    assert v['windows']['3d']['net_amount_native'] is None
    assert v['windows']['3d']['window_reason']=='AMOUNT_UNIT_MISMATCH'
    assert v['windows']['1d']['net_amount_native']=='10'

@pytest.mark.parametrize('value',[None,'NaN','Infinity','not-a-number'])
def test_claimed_observed_invalid_amount_is_data_gap_instead_of_reader_crash(value):
    rows=[obs(day) for day in DAYS]
    rows[0]['rows'][0]['values']['main_net']=value
    v=windows(rows)
    assert v['windows']['3d']['availability_state']=='SOURCE_UNAVAILABLE'
    assert v['windows']['3d']['net_amount_native'] is None

def test_equivalent_utc_cutoff_has_same_shanghai_trading_day_semantics():
    rows=[obs(day) for day in DAYS]
    utc=datetime.fromisoformat('2026-10-21T10:00:00+00:00')
    other=research_windows(rows,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=utc)
    assert other==windows(rows)


def test_partial_quote_does_not_poison_a_later_completed_session():
    partial=obs(DAYS[0],'140000');partial['rows'][0]['values']['main_net']='5'
    result=windows([partial,*[obs(day) for day in DAYS]])
    assert result['windows']['3d']['net_amount_native']=='30'
    assert result['windows']['3d']['partial_observations_excluded']=={DAYS[0]:1}


def test_exact_archives_have_a_real_readonly_consumer_and_keep_receipt_cutoff(tmp_path):
    from liangjian_funnel.data.capital_history import archive_observation,archived_research_windows
    for day in DAYS:
        value=obs(day)
        raw=json.dumps({'exact':day}).encode()
        import hashlib
        value['raw_sha256']=hashlib.sha256(raw).hexdigest()
        archive_observation(tmp_path,raw=raw,observation=value)
    value=archived_research_windows(tmp_path,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW)
    assert value['windows']['3d']['net_amount_native']=='30'
    assert value['archive_observation_count']==3 and value['new_source_http']==0
    prior=archived_research_windows(tmp_path,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW.replace(hour=17))
    assert prior['late_receipts_rejected']==1
    assert prior['windows']['3d']['net_amount_native'] is None


def test_archive_consumer_cannot_skip_corruption_or_expand_a_bounded_read(tmp_path):
    from liangjian_funnel.data.capital_history import archive_observation,archived_research_windows,digest
    refs=[]
    for day in DAYS:
        value=obs(day);raw=b'original';value['raw_sha256']=digest(raw)
        refs.append(archive_observation(tmp_path,raw=raw,observation=value))
    with pytest.raises(ValueError,match='READ_LIMIT_EXCEEDED'):
        archived_research_windows(tmp_path,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW,maximum_observations=2)
    from pathlib import Path
    Path(refs[0]['path']).write_text('{}')
    with pytest.raises(ValueError,match='HASH_MISMATCH'):
        archived_research_windows(tmp_path,symbol=SYMBOL,source_id=TENCENT_DAILY,sessions=DAYS,known_by=NOW)


@pytest.mark.parametrize('unit',[None,[],{},''])
def test_malformed_unit_metadata_is_an_explicit_data_gap(unit):
    rows=[obs(day) for day in DAYS];rows[0]['amount_unit']=unit
    value=windows(rows)
    assert value['windows']['3d']['net_amount_native'] is None
    assert 'AMOUNT_UNIT_UNRESOLVED' in value['windows']['3d']['invalid_session_reasons'][DAYS[0]]
