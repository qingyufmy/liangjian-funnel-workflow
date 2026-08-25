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
    """Use THS as the primary taxonomy without inventing a sector-cycle score.

    Current THS membership is enough to establish auditable industry scope,
    but not enough to claim rotation, breadth or capital-flow persistence.
    Mapped symbols therefore become ``PROBE_ONLY`` until index history and
    sector-flow facts are frozen; unmapped symbols remain research-only.
    """

    cutoff = _aware(as_of)
    industry, industry_reason = _available_fact(facts.get("THS_INDUSTRY_CATALOG"), cutoff)
    catalog_rows = _records(industry) if industry else None
    membership, membership_reason = _available_fact(facts.get("THS_INDUSTRY_MEMBERSHIP"), cutoff)
    membership_rows = _records(membership) if membership else None
    reason = "THS_INDEX_HISTORY_AND_SECTOR_FLOW_MISSING"
    if catalog_rows is None:
        reason = industry_reason or "THS_INDUSTRY_CATALOG_UNAVAILABLE"
    elif membership_rows is None:
        reason = membership_reason or "THS_INDUSTRY_MEMBERSHIP_UNAVAILABLE"
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
    cycle = {
        "available": False,
        "reason_code": reason,
        "source": "THS_PRIMARY_TAXONOMY",
        "as_of": cutoff.isoformat(),
        "industry_catalog_count": len(catalog_rows) if catalog_rows is not None else None,
        "membership_available": membership_rows is not None,
        "membership_coverage": coverage if membership_rows is not None else 0.0,
        "mapped_symbol_count": len(mapped),
        "taxonomy": "THS",
        "missing_components": [
            item
            for item, missing in (
                ("CURRENT_MEMBERSHIP", membership_rows is None),
                ("INDEX_HISTORY", True),
                ("SECTOR_CAPITAL_FLOW", True),
            )
            if missing
        ],
    }
    membership_ready = membership_rows is not None and coverage >= 0.80
    permissions = {
        "available": membership_ready,
        "reason_code": "THS_MEMBERSHIP_READY_CYCLE_PARTIAL" if membership_ready else "THS_MEMBERSHIP_COVERAGE_INSUFFICIENT",
        "as_of": cutoff.isoformat(),
        "taxonomy": "THS",
        "default_permission": "PROBE_ONLY" if membership_ready else "RESEARCH_ONLY",
        "by_symbol": {
            symbol: "PROBE_ONLY" if membership_ready and symbol in mapped else "RESEARCH_ONLY"
            for symbol in wanted
        },
    }
    return cycle, permissions


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
