"""Frozen source/capture identity, not A6 permission to hide a false gap."""
from copy import deepcopy
from datetime import date,datetime
import hashlib
import json
import pytest

from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_capture import KEY,build_capture_proof
from liangjian_funnel.runtime.regeneration_data_limits import source_gap_detail,project_data_limited

CAPTURE=datetime.fromisoformat('2026-10-23T00:20:00+08:00')
SYMBOL='002354.SZ'

def frozen(*,failures=None):
    proof=build_capture_proof(source_trade_date='2026-10-22',started_at=CAPTURE,
        captured_at=CAPTURE,completed_at=CAPTURE,calendar=ExchangeTradingCalendar())
    return {KEY:proof,'MARKET_DATA_AS_OF':proof['source_market_close_at'],
        'snapshot_manifest':dict(as_of=CAPTURE.isoformat(),source_failures=failures or {})}

@pytest.mark.parametrize('strict',[True,False])
@pytest.mark.parametrize('day',['2026-10-22',date(2026,10,22)])
def test_valid_late_capture_source_is_not_false_data_limited(strict,day):
    data=frozen();before=deepcopy(data)
    assert source_gap_detail(data,SYMBOL,strict=strict,expected_source_date=day) is None
    assert data==before

@pytest.mark.parametrize('strict',[True,False])
def test_true_source_failures_survive_valid_dual_clock_and_keep_probe_gate(strict):
    data=frozen(failures={SYMBOL:['A1:PDF_MISSING','A1:FINANCIAL_STALE']});before=deepcopy(data)
    detail=source_gap_detail(data,SYMBOL,strict=strict,expected_source_date='2026-10-22')
    assert detail['reason_codes']==['A1:PDF_MISSING','A1:FINANCIAL_STALE']
    assert detail['source_as_of']==CAPTURE.isoformat() and detail['a6_explicit_review_required'] is True
    assert detail['source_failures_sha256']==hashlib.sha256(json.dumps(data['snapshot_manifest']['source_failures'],
        ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    result=project_data_limited({'risk_unit':'STANDARD'},detail)
    assert result['risk_unit']=='PROBE' and result['DATA_LIMITED'] is True and data==before

@pytest.mark.parametrize('mutation',[
    lambda d:d.pop(KEY),lambda d:d.update({KEY:None}),
    lambda d:d[KEY].update(authority='FAKE'),
    lambda d:d[KEY].update(source_trade_date='2026-10-21'),
    lambda d:d[KEY].update(target_trade_date='2026-10-26'),
    lambda d:d[KEY].update(capture_as_of='2026-10-22T15:00:00+08:00'),
    lambda d:d[KEY].update(capture_completed_at='2026-10-23T00:19:00+08:00'),
    lambda d:d[KEY].update(publication_deadline='2026-10-23T07:00:00+08:00'),
    lambda d:d.pop('MARKET_DATA_AS_OF'),
    lambda d:d.update(MARKET_DATA_AS_OF='2026-10-21T15:00:00+08:00'),
    lambda d:d['snapshot_manifest'].update(as_of='2026-10-23T00:21:00+08:00'),
    lambda d:d['snapshot_manifest'].update(as_of='2026-10-23T00:20:00'),
])
def test_missing_tampered_or_cross_bound_proof_stays_data_limited(mutation):
    data=frozen(failures={SYMBOL:['A1:PDF_MISSING']});mutation(data)
    detail=source_gap_detail(data,SYMBOL,expected_source_date='2026-10-22')
    assert detail['reason_codes']==['A1:PDF_MISSING','A1_SOURCE_AS_OF_UNPROVEN']
    assert detail['maximum_risk_unit']=='PROBE' and detail['a6_explicit_review_required'] is True

def test_explicit_bad_proof_cannot_use_legacy_same_day_escape():
    data=frozen();data['snapshot_manifest']['as_of']='2026-10-22T16:00:00+08:00';data[KEY]=None
    assert source_gap_detail(data,SYMBOL,expected_source_date='2026-10-22')['reason_codes']==['A1_SOURCE_AS_OF_UNPROVEN']
    data.pop(KEY)
    assert source_gap_detail(data,SYMBOL,expected_source_date='2026-10-22') is None

def test_expected_other_source_day_does_not_borrow_valid_proof():
    assert source_gap_detail(frozen(),SYMBOL,expected_source_date='2026-10-21')['reason_codes']==['A1_SOURCE_AS_OF_UNPROVEN']

def test_missing_failure_manifest_not_masked_by_valid_clock():
    data=frozen();data['snapshot_manifest'].pop('source_failures')
    with pytest.raises(ValueError,match='REGEN_A1_SOURCE_FAILURE_MANIFEST_MISSING'):
        source_gap_detail(data,SYMBOL,expected_source_date='2026-10-22')
    detail=source_gap_detail(data,SYMBOL,strict=False,expected_source_date='2026-10-22')
    assert detail['reason_codes']==['A1_SOURCE_FAILURE_MANIFEST_MISSING']

def test_calendar_failure_cannot_claim_current_source(monkeypatch):
    import liangjian_funnel.runtime.regeneration_data_limits as m
    data=frozen()
    class Calendar:
        def is_trading_day(self,*_):raise m.TradingCalendarError()
    monkeypatch.setattr(m,'_CAPTURE_CALENDAR',Calendar())
    assert m.source_gap_detail(data,SYMBOL,expected_source_date='2026-10-22')['reason_codes']==['A1_SOURCE_AS_OF_UNPROVEN']
