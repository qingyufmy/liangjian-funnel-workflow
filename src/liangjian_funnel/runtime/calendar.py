"""Fail-closed Shanghai/Shenzhen trading-session calendar."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any


class TradingCalendarError(RuntimeError):
    def __init__(self, reason_code: str = "TRADING_CALENDAR_UNAVAILABLE"):
        self.reason_code = reason_code
        super().__init__(reason_code)


CalendarFactory = Callable[..., Any]


class ExchangeTradingCalendar:
    """Versioned XSHG calendar used for both SH and SZ session dates.

    The dependency contains an explicit holiday table.  Calendar construction
    or an out-of-range date is a blocking error; it never falls back to a
    weekday approximation.
    """

    def __init__(self, factory: CalendarFactory | None = None):
        self._factory = factory
        self._cache: dict[int, Any] = {}

    def _calendar(self, year: int) -> Any:
        cached = self._cache.get(year)
        if cached is not None:
            return cached
        factory = self._factory
        if factory is None:
            try:
                import exchange_calendars as calendars
            except ImportError as exc:
                raise TradingCalendarError() from exc
            factory = calendars.get_calendar
        try:
            calendar = factory(
                "XSHG",
                start=date(year, 1, 1),
                end=date(year, 12, 31),
            )
        except Exception as exc:
            raise TradingCalendarError() from exc
        self._cache[year] = calendar
        return calendar

    def is_trading_day(self, value: date) -> bool:
        if not isinstance(value, date):
            raise TypeError("trading day must be a date")
        try:
            return bool(self._calendar(value.year).is_session(value.isoformat()))
        except TradingCalendarError:
            raise
        except Exception as exc:
            raise TradingCalendarError("TRADING_CALENDAR_DATE_UNSUPPORTED") from exc


__all__ = ["ExchangeTradingCalendar", "TradingCalendarError"]
