"""Separate the immutable capture clock from the last closed market session."""
from datetime import datetime
from zoneinfo import ZoneInfo


def research_times(snapshot, calendar):
    captured = snapshot.as_of
    if not isinstance(captured, datetime) or captured.utcoffset() is None:
        raise ValueError('TEAM_RESEARCH_SOURCE_TIME_REQUIRED')
    captured = captured.astimezone(ZoneInfo('Asia/Shanghai'))
    if calendar.is_trading_day(captured.date()) is True:
        if captured.hour < 15:
            raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
        # Keep the existing natural-close request contract byte-for-byte.
        return captured, captured

    # run-next-session-prep already binds this cutoff when it collects market
    # facts. Do not relabel the Sunday snapshot (including weekend news) or
    # invent a market timestamp when that binding is missing/stale.
    raw = snapshot.data.get('MARKET_DATA_AS_OF')
    try:
        market = datetime.fromisoformat(raw) if isinstance(raw, str) else raw
    except ValueError:
        market = None
    if not isinstance(market, datetime) or market.utcoffset() is None:
        raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
    market = market.astimezone(ZoneInfo('Asia/Shanghai'))
    if (market > captured or market.hour < 15
            or market.date() != calendar.previous_trading_day(captured.date())):
        raise ValueError('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
    return captured, market
