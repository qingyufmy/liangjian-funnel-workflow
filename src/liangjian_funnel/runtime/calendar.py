"""Fail-closed Shanghai/Shenzhen trading-session calendar."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from dataclasses import dataclass
from zoneinfo import ZoneInfo
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
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError("trading day must be a date")
        try:
            return bool(self._calendar(value.year).is_session(value.isoformat()))
        except TradingCalendarError:
            raise
        except Exception as exc:
            raise TradingCalendarError("TRADING_CALENDAR_DATE_UNSUPPORTED") from exc

    def next_trading_day(self, value: date) -> date:
        """Return the first exchange session strictly after ``value``.

        The workflow uses this for publication horizons and for preparing a
        non-trading-day session.  It deliberately delegates every candidate
        date to :meth:`is_trading_day`, so weekends and exchange holidays are
        handled by the versioned XSHG calendar rather than a weekday shortcut.
        """

        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError("trading day must be a date")
        candidate = value + timedelta(days=1)
        for _ in range(370):
            if self.is_trading_day(candidate):
                return candidate
            candidate += timedelta(days=1)
        raise TradingCalendarError("TRADING_CALENDAR_NEXT_SESSION_UNAVAILABLE")

    def previous_trading_day(self, value: date) -> date:
        """Return the last exchange session strictly before ``value``.

        This is intentionally the inverse of :meth:`next_trading_day`, rather
        than a weekday calculation.  It is used when a non-trading-day
        preparation run needs the latest completed market session (for
        example, Sunday 2026-08-30 resolves to Friday 2026-08-28).
        """

        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError("trading day must be a date")
        candidate = value - timedelta(days=1)
        for _ in range(370):
            if self.is_trading_day(candidate):
                return candidate
            candidate -= timedelta(days=1)
        raise TradingCalendarError("TRADING_CALENDAR_PREVIOUS_SESSION_UNAVAILABLE")


@dataclass(frozen=True)
class ClosedResearchSession:
    source_trade_date: date
    target_trade_date: date
    market_close: datetime
    publication_deadline: datetime
    wall_clock: datetime


def latest_closed_research_session(now: datetime, calendar) -> ClosedResearchSession:
    """Derive source identity only; this grants no publication authority."""
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError('CLOSE_RESEARCH_CLOCK_INVALID')
    current=now.astimezone(ZoneInfo('Asia/Shanghai'))
    source=(current.date() if calendar.is_trading_day(current.date())
            and current.time().replace(tzinfo=None)>=time(15)
            else calendar.previous_trading_day(current.date()))
    target=calendar.next_trading_day(source)
    deadline=datetime.combine(target,time(6,30),tzinfo=current.tzinfo)
    return ClosedResearchSession(source,target,
        datetime.combine(source,time(15),tzinfo=current.tzinfo),deadline,current)


def closed_research_session(now: datetime, calendar) -> ClosedResearchSession:
    """Keep the ordinary close/regeneration 06:30 publication gate."""
    session = latest_closed_research_session(now, calendar)
    if session.wall_clock >= session.publication_deadline:
        raise ValueError('CLOSE_RESEARCH_PUBLICATION_DEADLINE_REACHED')
    return session


__all__ = ["ExchangeTradingCalendar", "TradingCalendarError", "ClosedResearchSession",
    "closed_research_session", "latest_closed_research_session"]
