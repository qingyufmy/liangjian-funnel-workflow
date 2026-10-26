from datetime import datetime
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_capture import KEY,build_capture_proof
from liangjian_funnel.runtime.research_source_date import runtime_trade_date


def test_late_capture_records_friday_source_without_relabelling_capture():
    capture=datetime.fromisoformat('2026-10-24T02:48:10+08:00')
    proof=build_capture_proof(source_trade_date='2026-10-23',started_at=capture,captured_at=capture,
        completed_at=capture,calendar=ExchangeTradingCalendar())
    snapshot=SimpleNamespace(as_of=capture,data={KEY:proof})
    assert runtime_trade_date(snapshot,generated_at=capture,slot='close')=='2026-10-23'
    assert snapshot.as_of==capture and proof['capture_as_of'].startswith('2026-10-24')
    proof['source_trade_date']='2026-10-24'
    with pytest.raises(ValueError):runtime_trade_date(snapshot,generated_at=capture,slot='close')


def test_unproved_plan_target_cannot_choose_source_date():
    at=datetime.fromisoformat('2026-10-26T17:00:00+08:00')
    snapshot=SimpleNamespace(as_of=at,data={'target_trade_date':'2026-10-28'})
    assert runtime_trade_date(snapshot,generated_at=at,slot='close')=='2026-10-26'
