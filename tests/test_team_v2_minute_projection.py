from datetime import datetime,timedelta
from decimal import Decimal

import pytest

from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.team_v2_minute_projection import project_closed_minute


def source(**changes):
    values=dict(symbol='000001.SZ',interval='1m',bar_end=datetime.fromisoformat('2026-10-19T09:31:00+08:00'),
        open=10,low=9.9,high=10.1,close=10,volume=10000,amount=100000,source_id='UNIT_TEST:reported',
        volume_unit='shares',amount_kind='reported',normalizer_version='unit-test/1')
    values.update(changes);return MinuteBar(**values)


def project(rows,clock='09:32:00'):
    return project_closed_minute(rows,(),observed_at=datetime.fromisoformat('2026-10-19T'+clock+'+08:00'))


def test_actual_normalized_bar_contract_to_paper_preserves_vwap_not_tradability():
    bar=source();result=project((bar,))
    assert result['status']=='BAR_PROJECTED_RULES_PENDING'
    assert result['paper_bar'].vwap==Decimal('10') and result['paper_bar'].volume_shares==10000
    assert result['paper_bar'].tradability_known is False and result['source_requests_started']==0
    assert result['execution_data']['entry_window_complete'] is True
    assert bar.amount_kind=='reported'


@pytest.mark.parametrize('changes,reason',[
    ({'amount_kind':'ohlc_estimate'},'CONTROL_REPORTED_MINUTE_VWAP_UNPROVEN'),
    ({'amount_kind':'legacy_unspecified'},'CONTROL_REPORTED_MINUTE_VWAP_UNPROVEN'),
    ({'volume':0,'amount':0},'CONTROL_REPORTED_MINUTE_VWAP_UNPROVEN'),
    ({'volume':10000.5},'CONTROL_SOURCE_VOLUME_NOT_INTEGER_SHARES'),
    ({'evidence_kind':'SYNTHETIC_QUOTE'},'CONTROL_SOURCE_CLOSED_WINDOW_UNPROVEN'),
    ({'volume_unit':'legacy_unspecified'},'CONTROL_SOURCE_CLOSED_WINDOW_UNPROVEN'),
    ({'interval':'5m'},'CONTROL_SOURCE_CLOSED_WINDOW_UNPROVEN'),
    ({'amount':120000},'CONTROL_REPORTED_MINUTE_VWAP_OUTSIDE_BAR')])
def test_no_synthetic_or_estimated_vwap_permission(changes,reason):
    result=project((source(**changes),))
    assert result['status']=='DATA_LIMITED' and result['paper_bar'] is None
    assert result['reason_codes']==[reason]


def test_future_old_duplicate_or_missing_minutes_cannot_prove_current_scope():
    first=source();second=source(bar_end=first.bar_end+timedelta(minutes=1))
    assert project((second,))['status']=='DATA_LIMITED'
    assert project((first,),clock='09:33:00')['reason_codes']==['CONTROL_SOURCE_CLOSED_WINDOW_UNPROVEN']
    for rows in ((second,),(first,first,second)):
        assert project(rows,clock='09:33:00')['reason_codes']==['CONTROL_SOURCE_SESSION_INCOMPLETE']
