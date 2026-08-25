"""Deterministic Phase-2 market aggregates built only from frozen facts."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel


SHANGHAI = ZoneInfo("Asia/Shanghai")
EMOTION_ALGORITHM = "market-emotion/1.0.0"


def build_market_emotion(
    universe_records: Sequence[Any],
    facts: Mapping[str, Any],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    """Aggregate breadth and price-limit facts without silently using partial data."""

    cutoff = _aware(as_of)
    required = ("LIMIT_UP_POOL", "LIMIT_DOWN_POOL", "LIMIT_BREAK_POOL", "LIMIT_UP_LADDER")
    validated: dict[str, Mapping[str, Any]] = {}
    for key in required:
        value, reason = _available_fact(facts.get(key), cutoff)
        if value is None:
            return _unavailable("MARKET_EMOTION_SNAPSHOT", cutoff, reason or f"{key}_UNAVAILABLE")
        validated[key] = value

    changes = [_finite(_mapping(item).get("change_ratio_pct")) for item in universe_records]
    observed = [value for value in changes if value is not None]
    if not observed:
        return _unavailable("MARKET_EMOTION_SNAPSHOT", cutoff, "BREADTH_DATA_MISSING")
    advances = sum(value > 0 for value in observed)
    declines = sum(value < 0 for value in observed)
    flats = len(observed) - advances - declines
    coverage = len(observed) / len(universe_records) if universe_records else 0.0
    if coverage < 0.9:
        return {
            **_unavailable("MARKET_EMOTION_SNAPSHOT", cutoff, "BREADTH_COVERAGE_INSUFFICIENT"),
            "breadth_coverage": coverage,
        }

    up_records = _records(validated["LIMIT_UP_POOL"])
    down_records = _records(validated["LIMIT_DOWN_POOL"])
    break_records = _records(validated["LIMIT_BREAK_POOL"])
    ladder_records = _records(validated["LIMIT_UP_LADDER"])
    if any(value is None for value in (up_records, down_records, break_records, ladder_records)):
        return _unavailable("MARKET_EMOTION_SNAPSHOT", cutoff, "FACT_RECORDS_MALFORMED")
    up_count = len(up_records or ())
    down_count = len(down_records or ())
    break_count = len(break_records or ())
    breadth = advances / (advances + declines) if advances + declines else 0.5
    break_rate = break_count / (up_count + break_count) if up_count + break_count else None
    ladder_height, promotion_rate = _ladder_metrics(ladder_records or (), cutoff.date())
    temperature = _temperature(
        breadth=breadth,
        limit_up=up_count,
        limit_down=down_count,
        break_rate=break_rate,
        ladder_height=ladder_height,
    )
    missing_fields = []
    if promotion_rate is None:
        missing_fields.append("previous_day_promotion_rate")
    missing_fields.append("previous_limit_up_premium")
    return {
        "available": True,
        "reason_code": "OK",
        "source": "DETERMINISTIC_FROZEN_FACTS",
        "algorithm_version": EMOTION_ALGORITHM,
        "as_of": cutoff.isoformat(),
        "temperature": temperature,
        "advances": advances,
        "declines": declines,
        "flats": flats,
        "breadth": breadth,
        "breadth_coverage": coverage,
        "limit_up_count": up_count,
        "limit_down_count": down_count,
        "limit_break_count": break_count,
        "break_rate": break_rate,
        "ladder_height": ladder_height,
        "previous_day_promotion_rate": promotion_rate,
        "previous_limit_up_premium": None,
        "missing_fields": missing_fields,
    }


def build_crowding_snapshot(
    facts: Mapping[str, Any],
    symbols: Sequence[str],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    """Expose available attention proxies while refusing to call them full crowding."""

    cutoff = _aware(as_of)
    wanted = {str(symbol).upper() for symbol in symbols}
    dragon, dragon_reason = _available_fact(facts.get("DRAGON_TIGER_LIST"), cutoff)
    hot, hot_reason = _available_fact(facts.get("HOT_STOCK_LIST"), cutoff)
    dragon_rows = _records(dragon) if dragon else None
    hot_rows = _records(hot) if hot else None
    return {
        "available": False,
        "reason_code": "PARTIAL_PROXY_ONLY",
        "source": "DETERMINISTIC_FROZEN_FACTS",
        "as_of": cutoff.isoformat(),
        "scope_symbols": sorted(wanted),
        "dragon_tiger_component": {
            "available": dragon_rows is not None,
            "reason_code": "OK" if dragon_rows is not None else dragon_reason,
            "records": _filter_symbols(dragon_rows or (), wanted),
        },
        "market_attention_component": {
            "available": hot_rows is not None,
            "reason_code": "OK" if hot_rows is not None else hot_reason,
            "records": _filter_symbols(hot_rows or (), wanted),
        },
        "missing_components": ["FUND_HOLDINGS", "MARGIN_FINANCING", "TURNOVER_STRUCTURE"],
    }


def build_sector_cycle_and_permissions(
    facts: Mapping[str, Any],
    symbols: Sequence[str],
    *,
    as_of: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Measure sector persistence from frozen THS 881* index history.

    Turnover is labelled as a price-volume proxy, never as capital flow.  The
    regime may use Top-3 return overlap and persistence, while A2 still sees
    the missing capital-flow component and must degrade that score dimension.
    """

    cutoff = _aware(as_of)
    industry, industry_reason = _available_fact(facts.get("THS_INDUSTRY_CATALOG"), cutoff)
    catalog_rows = _records(industry) if industry else None
    membership, membership_reason = _available_fact(facts.get("THS_INDUSTRY_MEMBERSHIP"), cutoff)
    membership_rows = _records(membership) if membership else None
    history, history_reason = _available_fact(facts.get("THS_INDUSTRY_HISTORY"), cutoff)
    history_rows = _records(history) if history else None
    reason = "OK"
    if catalog_rows is None:
        reason = industry_reason or "THS_INDUSTRY_CATALOG_UNAVAILABLE"
    elif membership_rows is None:
        reason = membership_reason or "THS_INDUSTRY_MEMBERSHIP_UNAVAILABLE"
    elif history_rows is None:
        reason = history_reason or "THS_INDUSTRY_HISTORY_UNAVAILABLE"
    wanted = sorted({str(symbol).upper() for symbol in symbols})
    by_symbol = {
        str(row.get("thscode") or "").upper(): row
        for row in (membership_rows or ())
        if str(row.get("thscode") or "").upper() in wanted
    }
    mapped = {
        symbol
        for symbol, row in by_symbol.items()
        if row.get("mapping_status") == "MAPPED"
        and isinstance(row.get("memberships"), Sequence)
        and not isinstance(row.get("memberships"), (str, bytes, bytearray))
        and bool(row.get("memberships"))
    }
    coverage = len(mapped) / len(wanted) if wanted else 1.0
    history_metrics = _sector_history_metrics(history_rows or ()) if history_rows is not None else None
    if history_rows is not None and history_metrics is None:
        reason = "THS_INDUSTRY_HISTORY_MALFORMED"
    cycle_available = reason == "OK" and history_metrics is not None
    cycle = {
        "available": cycle_available,
        "reason_code": "OK" if cycle_available else reason,
        "source": "THS_PRIMARY_TAXONOMY",
        "as_of": cutoff.isoformat(),
        "industry_catalog_count": len(catalog_rows) if catalog_rows is not None else None,
        "membership_available": membership_rows is not None,
        "membership_coverage": coverage if membership_rows is not None else 0.0,
        "mapped_symbol_count": len(mapped),
        "taxonomy": "THS",
        "history_metrics": history_metrics,
        "capital_flow_available": False,
        "turnover_is_capital_flow": False,
        "missing_components": [
            item
            for item, missing in (
                ("CURRENT_MEMBERSHIP", membership_rows is None),
                ("INDEX_HISTORY", history_metrics is None),
                ("SECTOR_CAPITAL_FLOW", True),
            )
            if missing
        ],
    }
    membership_ready = membership_rows is not None and coverage >= 0.80
    mainline_codes = {
        str(item.get("industry_thscode") or "")
        for item in (history_metrics or {}).get("persistent_mainline_candidates", ())
        if isinstance(item, Mapping)
    }
    symbol_industries: dict[str, set[str]] = {}
    for symbol, row in by_symbol.items():
        memberships = row.get("memberships")
        symbol_industries[symbol] = {
            str(item.get("industry_thscode") or "")
            for item in memberships if isinstance(item, Mapping)
        } if isinstance(memberships, Sequence) and not isinstance(memberships, (str, bytes, bytearray)) else set()
    permissions = {
        "available": membership_ready,
        "reason_code": (
            "THS_MEMBERSHIP_AND_HISTORY_READY_CAPITAL_FLOW_MISSING"
            if membership_ready and cycle_available
            else "THS_MEMBERSHIP_READY_CYCLE_PARTIAL"
            if membership_ready
            else "THS_MEMBERSHIP_COVERAGE_INSUFFICIENT"
        ),
        "as_of": cutoff.isoformat(),
        "taxonomy": "THS",
        "default_permission": "PROBE_ONLY" if membership_ready else "RESEARCH_ONLY",
        "by_symbol": {
            symbol: (
                "STANDARD"
                if membership_ready and cycle_available and symbol_industries.get(symbol, set()).intersection(mainline_codes)
                else "PROBE_ONLY"
                if membership_ready and symbol in mapped
                else "RESEARCH_ONLY"
            )
            for symbol in wanted
        },
    }
    return cycle, permissions


def _sector_history_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    series: dict[str, dict[int, dict[str, float]]] = {}
    names: dict[str, str] = {}
    for raw in rows:
        code = str(raw.get("industry_thscode") or "")
        name = str(raw.get("industry_name") or "")
        bars = raw.get("bars")
        if not code.startswith("881") or not isinstance(bars, Sequence) or isinstance(bars, (str, bytes, bytearray)):
            continue
        parsed: dict[int, dict[str, float]] = {}
        for bar in bars:
            if not isinstance(bar, Mapping):
                continue
            day = _integer(bar.get("date_ms"))
            close = _finite(bar.get("close_price"))
            turnover = _finite(bar.get("turnover"))
            if day is None or close is None or close <= 0 or turnover is None or turnover < 0:
                continue
            parsed[day] = {"close": close, "turnover": turnover}
        if len(parsed) >= 5:
            series[code] = parsed
            names[code] = name
    if len(series) < 3:
        return None
    all_dates = sorted({day for values in series.values() for day in values})
    latest_dates = all_dates[-6:]
    daily_top3: list[dict[str, Any]] = []
    appearances: dict[str, int] = {}
    for previous_day, day in zip(latest_dates, latest_dates[1:]):
        returns = []
        for code, values in series.items():
            previous = values.get(previous_day)
            current = values.get(day)
            if previous is None or current is None or previous["close"] <= 0:
                continue
            returns.append((current["close"] / previous["close"] - 1.0, code))
        if len(returns) < 3:
            continue
        leaders = sorted(returns, key=lambda item: (-item[0], item[1]))[:3]
        codes = [code for _return, code in leaders]
        for code in codes:
            appearances[code] = appearances.get(code, 0) + 1
        daily_top3.append({
            "date_ms": day,
            "industries": [
                {
                    "industry_thscode": code,
                    "industry_name": names.get(code, ""),
                    "daily_return": value,
                }
                for value, code in leaders
            ],
        })
    if not daily_top3:
        return None
    overlaps = []
    for left, right in zip(daily_top3, daily_top3[1:]):
        left_codes = {item["industry_thscode"] for item in left["industries"]}
        right_codes = {item["industry_thscode"] for item in right["industries"]}
        overlaps.append(len(left_codes.intersection(right_codes)) / 3.0)
    overlap = sum(overlaps) / len(overlaps) if overlaps else 0.0
    candidates = []
    for code, count in sorted(appearances.items(), key=lambda item: (-item[1], item[0])):
        ordered = [series[code][day] for day in latest_dates if day in series[code]]
        if len(ordered) < 2:
            continue
        return_lookback = ordered[-1]["close"] / ordered[0]["close"] - 1.0
        recent_turnover = sum(item["turnover"] for item in ordered[-3:]) / min(3, len(ordered))
        prior_values = ordered[:-3]
        prior_turnover = (
            sum(item["turnover"] for item in prior_values) / len(prior_values)
            if prior_values else None
        )
        candidates.append({
            "industry_thscode": code,
            "industry_name": names.get(code, ""),
            "top3_appearance_count": count,
            "lookback_return": return_lookback,
            "recent_turnover": recent_turnover,
            "turnover_persistence_ratio": (
                recent_turnover / prior_turnover if prior_turnover and prior_turnover > 0 else None
            ),
        })
    persistent = [
        item for item in candidates
        if item["top3_appearance_count"] >= 2 and item["lookback_return"] > 0
    ]
    return {
        "lookback_trading_days": len(daily_top3),
        "top3_daily_overlap": overlap,
        "top3_by_day": daily_top3,
        "persistent_mainline_candidates": persistent,
        "turnover_metric_role": "PRICE_VOLUME_PROXY_ONLY",
    }


def build_news_heat_snapshot(
    fact_payload: Mapping[str, Any],
    symbols: Sequence[str],
    *,
    as_of: datetime,
    recent_hours: int = 168,
    max_items: int = 120,
) -> dict[str, Any]:
    """Build a deterministic news view without manufacturing sentiment.

    Media and RSS facts are T3 clues.  The aggregate exposes deduplicated
    counts, source/channel lineage and bounded headlines, while explicitly
    declaring that no sentiment classifier has run.
    """

    cutoff = _aware(as_of)
    lower = cutoff - timedelta(hours=recent_hours)
    wanted = {str(symbol).upper() for symbol in symbols}
    groups = fact_payload.get("fact_groups") if isinstance(fact_payload, Mapping) else None
    health = fact_payload.get("source_health") if isinstance(fact_payload, Mapping) else None
    if not isinstance(groups, Mapping):
        groups = {}
    items: list[dict[str, Any]] = []
    dropped_future = 0
    dropped_stale = 0
    for fact_type in ("MARKET_NEWS_FLASH", "STOCK_NEWS_ITEM", "INDUSTRY_RSS_ITEM"):
        records = groups.get(fact_type, ())
        if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
            continue
        for raw in records:
            if not isinstance(raw, Mapping) or raw.get("available") is False:
                continue
            published = _parse_datetime(raw.get("publish_time"))
            if published is None:
                continue
            if published > cutoff:
                dropped_future += 1
                continue
            if published < lower:
                dropped_stale += 1
                continue
            symbol = str(raw.get("symbol") or "").upper() or None
            if fact_type == "STOCK_NEWS_ITEM" and symbol not in wanted:
                continue
            items.append({
                "fact_id": raw.get("fact_id"),
                "fact_type": fact_type,
                "channel": raw.get("channel"),
                "symbol": symbol,
                "title": raw.get("title"),
                "summary": raw.get("summary"),
                "publish_time": published.isoformat(),
                "source_id": raw.get("source_id"),
                "source_name": raw.get("source_name"),
                "source_url": raw.get("source_url"),
                "original_sources": raw.get("original_sources") or [],
                "original_source_names": raw.get("original_source_names") or [],
                "industry_hint": raw.get("industry_hint"),
                "repost_count": int(raw.get("repost_count") or 0),
                "untrusted_text": True,
            })
    items.sort(key=lambda item: (str(item["publish_time"]), str(item.get("fact_id") or "")), reverse=True)
    by_channel: dict[str, int] = {}
    by_symbol: dict[str, int] = {}
    by_source: dict[str, int] = {}
    for item in items:
        channel = str(item.get("channel") or "UNKNOWN")
        by_channel[channel] = by_channel.get(channel, 0) + 1
        if item.get("symbol"):
            symbol = str(item["symbol"])
            by_symbol[symbol] = by_symbol.get(symbol, 0) + 1
        source = str(item.get("source_id") or "UNKNOWN")
        by_source[source] = by_source.get(source, 0) + 1
    news_health = [
        item
        for item in (health or ())
        if isinstance(item, Mapping) and str(item.get("source_id") or "").startswith("open_news.")
    ] if isinstance(health, Sequence) and not isinstance(health, (str, bytes, bytearray)) else []
    healthy_sources = sum(item.get("available") is True for item in news_health)
    return {
        "available": bool(news_health) and healthy_sources > 0,
        "reason_code": "OK" if news_health and healthy_sources > 0 else "OPEN_NEWS_SOURCES_UNAVAILABLE",
        "as_of": cutoff.isoformat(),
        "window_hours": recent_hours,
        "evidence_tier": "T3",
        "untrusted_text": True,
        "deduped_item_count": len(items),
        "repost_count_total": sum(int(item["repost_count"]) for item in items),
        "by_channel": dict(sorted(by_channel.items())),
        "by_symbol": dict(sorted(by_symbol.items())),
        "by_source": dict(sorted(by_source.items())),
        "source_health_count": len(news_health),
        "healthy_source_count": healthy_sources,
        "future_items_dropped": dropped_future,
        "stale_items_dropped": dropped_stale,
        "sentiment_available": False,
        "sentiment_reason_code": "NO_DETERMINISTIC_CLASSIFIER",
        "items": items[:max_items],
    }


def _available_fact(value: Any, as_of: datetime) -> tuple[Mapping[str, Any] | None, str | None]:
    if not isinstance(value, Mapping):
        return None, "SOURCE_NOT_CONFIGURED"
    if value.get("available") is not True:
        return None, str(value.get("reason_code") or "SOURCE_UNAVAILABLE")
    event_time = _parse_datetime(value.get("event_time"))
    fetch_time = _parse_datetime(value.get("fetch_time"))
    if event_time is None or fetch_time is None:
        return None, "FACT_TIME_MISSING"
    # Fetching immediately after a point-in-time cutoff is expected.  Only the
    # fact's market event time must not cross the frozen research boundary.
    if event_time > as_of:
        return None, "FUTURE_FACT_DETECTED"
    return value, None


def _records(value: Mapping[str, Any] | None) -> list[Mapping[str, Any]] | None:
    if value is None:
        return None
    records = value.get("records")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        return None
    normalized = [item for item in records if isinstance(item, Mapping)]
    if len(normalized) != len(records):
        return None
    declared = value.get("record_count")
    if isinstance(declared, int) and not isinstance(declared, bool) and declared != len(normalized):
        return None
    return normalized


def _ladder_metrics(records: Sequence[Mapping[str, Any]], as_of_date: date) -> tuple[int | None, float | None]:
    dated: list[tuple[date, Mapping[str, Any]]] = []
    for record in records:
        try:
            day = date.fromisoformat(str(record.get("date")))
        except ValueError:
            continue
        if day <= as_of_date:
            dated.append((day, record))
    if not dated:
        return None, None
    dated.sort(key=lambda item: item[0], reverse=True)
    latest_boards = dated[0][1].get("boards")
    height = _max_board(latest_boards)
    previous = next((item for item in dated if item[0] < dated[0][0]), None)
    if previous is None:
        return height, None
    outcomes: list[bool] = []
    boards = previous[1].get("boards")
    if isinstance(boards, Mapping):
        for rows in boards.values():
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)):
                for row in rows:
                    if isinstance(row, Mapping) and isinstance(row.get("seal_nextday"), bool):
                        outcomes.append(bool(row["seal_nextday"]))
    promotion = sum(outcomes) / len(outcomes) if outcomes else None
    return height, promotion


def _max_board(boards: Any) -> int | None:
    values: list[int] = []
    if isinstance(boards, Mapping):
        for rows in boards.values():
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)):
                for row in rows:
                    if isinstance(row, Mapping):
                        value = row.get("board_num")
                        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                            values.append(value)
    return max(values) if values else None


def _temperature(*, breadth: float, limit_up: int, limit_down: int, break_rate: float | None, ladder_height: int | None) -> str:
    rate = break_rate if break_rate is not None else 1.0
    height = ladder_height or 0
    if breadth <= 0.30 or limit_down >= max(10, limit_up):
        return "ICE"
    if breadth < 0.42 or limit_down > limit_up * 0.6:
        return "WEAK"
    if limit_up >= 50 and rate >= 0.35:
        return "DIVERGING_WEAK"
    if breadth >= 0.68 and limit_up >= 80 and height >= 5 and rate <= 0.20:
        return "OVERHEATED"
    if breadth >= 0.58 and limit_up >= 40 and rate <= 0.30:
        return "STRONG"
    return "RECOVERY"


def _filter_symbols(rows: Sequence[Mapping[str, Any]], wanted: set[str]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("thscode") or "").upper() in wanted]


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python")
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result >= 0 else None


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("aggregate timestamps must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _unavailable(name: str, as_of: datetime, reason: str) -> dict[str, Any]:
    return {
        "available": False,
        "reason_code": reason,
        "source": "DETERMINISTIC_FROZEN_FACTS",
        "aggregate": name,
        "as_of": as_of.isoformat(),
    }


__all__ = [
    "build_crowding_snapshot",
    "build_market_emotion",
    "build_news_heat_snapshot",
    "build_sector_cycle_and_permissions",
]
