from datetime import datetime
import pytest
from liangjian_funnel.runtime.calendar import closed_research_session,ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_policy import RECOVERY_AUTHORIZATION
from liangjian_funnel.runtime.closed_session_capture import build_capture_proof,validate_capture_proof
D=datetime.fromisoformat
@pytest.mark.parametrize('at',['2026-10-29T02:15:00+08:00','2026-10-30T05:00:00+08:00','2026-11-02T05:00:00+08:00'])
def test_old_proof_deadline_unchanged(at):
 assert closed_research_session(D(at),ExchangeTradingCalendar()).publication_deadline.hour==6
@pytest.mark.parametrize('at',['2026-10-30T07:05:00+08:00','2026-10-30T09:14:59+08:00'])
def test_native_capture_and_independent_reader_share_grant(at):
 c=ExchangeTradingCalendar();t=D(at);p=build_capture_proof(source_trade_date='2026-10-29',started_at=t,captured_at=t,completed_at=t,calendar=c)
 assert p['capture_as_of']==at and p['deadline_override_authority']=='Claude1195'
 assert validate_capture_proof(p,captured_at=t,calendar=ExchangeTradingCalendar())==p
 assert p['publication_deadline']=='2026-10-30T09:15:00+08:00'
def test_default_and_unknown_override_remain_closed():
 c=ExchangeTradingCalendar();t=D('2026-10-30T07:05:00+08:00')
 with pytest.raises(ValueError,match='DEADLINE'):closed_research_session(t,c,authorization={})
 with pytest.raises(ValueError,match='AUTHORITY'):closed_research_session(t,c,authorization={'authority':'unknown'})
@pytest.mark.parametrize('at',['2026-10-30T09:15:00+08:00','2026-10-30T09:30:00+08:00','2026-11-02T07:05:00+08:00'])
def test_capture_at_or_after_auction_or_other_date_rejected(at):
 with pytest.raises(ValueError,match='DEADLINE'):closed_research_session(D(at),ExchangeTradingCalendar())
def test_proof_tamper_missing_authority_and_time_rejected():
 c=ExchangeTradingCalendar();t=D('2026-10-30T07:05:00+08:00');p=build_capture_proof(source_trade_date='2026-10-29',started_at=t,captured_at=t,completed_at=t,calendar=c)
 for key in ['deadline_override_authority','deadline_override','capture_completed_at']:
  bad=dict(p);bad.pop(key)
  with pytest.raises((ValueError,TypeError)):validate_capture_proof(bad,captured_at=t,calendar=c)
