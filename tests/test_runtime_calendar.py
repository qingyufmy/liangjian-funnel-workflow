from datetime import date

import pytest

from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar, TradingCalendarError


def test_xshg_calendar_skips_weekend_and_official_2026_holidays():
    calendar = ExchangeTradingCalendar()
    assert calendar.is_trading_day(date(2026, 8, 24)) is True
    assert calendar.is_trading_day(date(2026, 8, 23)) is False
    assert calendar.is_trading_day(date(2026, 10, 1)) is False
    assert calendar.is_trading_day(date(2026, 10, 8)) is True
    assert calendar.is_trading_day(date(2026, 2, 17)) is False
    assert calendar.next_trading_day(date(2026, 8, 28)) == date(2026, 8, 31)
    assert calendar.next_trading_day(date(2026, 9, 30)) == date(2026, 10, 8)


def test_calendar_factory_failure_is_fail_closed():
    def broken(*_args, **_kwargs):
        raise RuntimeError("upstream detail")

    with pytest.raises(TradingCalendarError, match="TRADING_CALENDAR_UNAVAILABLE"):
        ExchangeTradingCalendar(factory=broken).is_trading_day(date(2026, 8, 24))
