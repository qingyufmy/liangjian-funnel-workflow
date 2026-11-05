"""Use the production calendar with a padded lower construction boundary.

Exchange calendars' is_session raises for New-Year holidays preceding its
first session when built from Jan1. A14-day preceding range keeps that date
inside the same explicit XSHG holiday table. No weekday fallback, future
calendar extension or changes to the production calendar module.
"""
from datetime import timedelta

from ..runtime.calendar import ExchangeTradingCalendar


def market_calendar():
    def factory(name, *, start, end):
        import exchange_calendars
        return exchange_calendars.get_calendar(name,start=start-timedelta(days=14),end=end)
    return ExchangeTradingCalendar(factory=factory)
