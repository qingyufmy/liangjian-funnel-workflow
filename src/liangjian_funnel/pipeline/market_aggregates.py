"""Deterministic Phase-2 market aggregates built only from frozen facts."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel


SHANGHAI = ZoneInfo("Asia/Shanghai")
EMOTION_ALGORITHM = "market-emotion/2.0.0"
SECTOR_CYCLE_ALGORITHM = "sector-cycle/2.0.0"
SECTOR_HEALTH_ALGORITHM = "sector-health/1.0.0"
_MONTHLY_OBSERVATION_BARS = 21  # 20 return periods require 21 closes.
_MONTHLY_TOP10_MIN_APPEARANCES = 2
_SECTOR_SEQUENCE_MIN_BARS = 4


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
    emotion_cycle = _emotion_cycle_contract(
        temperature=temperature,
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
        "emotion_cycle_stage": emotion_cycle["stage"],
        "emotion_cycle_stage_cn": emotion_cycle["stage_cn"],
        "new_long_permission": emotion_cycle["new_long_permission"],
        "emotion_cycle_reason_codes": emotion_cycle["reason_codes"],
        "emotion_cycle_evidence": emotion_cycle["evidence"],
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
    history_metrics = (
        _sector_history_metrics(history_rows or (), as_of=cutoff)
        if history_rows is not None
        else None
    )
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


def build_sector_health_snapshot(
    facts: Mapping[str, Any],
    g0_quotes: Sequence[Any] | Mapping[str, Any],
    *,
    as_of: datetime,
    symbols: Sequence[str] | None = None,
    board_capital_flow_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the deterministic, point-in-time A2 sector health view.

    This aggregate deliberately has a narrower contract than a stock selector:
    it describes the state of every THS industry/concept represented by the
    frozen G0 quote set.  It combines member breadth and current strength with
    available industry index history and the latest eligible limit-up facts.
    Missing history is represented as ``UNKNOWN``; it is never filled from
    today's value or from a future bar.  In particular, turnover/amount is
    retained only as a price-volume proxy and is never exposed as capital flow.

    ``facts`` may be either the ``facts`` mapping itself or the complete fact
    payload returned by ``manifest_projection``.  ``g0_quotes`` accepts the
    frozen ``SecurityRecord`` sequence as well as a symbol-keyed mapping, which
    keeps replay and production call sites on the same path.
    """

    cutoff = _aware(as_of)
    fact_map = _fact_mapping(facts)
    wanted = _normalise_symbols(symbols) if symbols is not None else None
    quotes = _quote_map(g0_quotes, wanted)
    scope_symbols = sorted(wanted if wanted is not None else set(quotes))

    membership_by_taxonomy: dict[str, dict[str, list[dict[str, str]]]] = {}
    taxonomy_status: dict[str, dict[str, Any]] = {}
    for taxonomy in ("industry", "concept"):
        membership_key = f"THS_{taxonomy.upper()}_MEMBERSHIP"
        catalog_key = f"THS_{taxonomy.upper()}_CATALOG"
        membership_value, membership_reason = _available_fact(fact_map.get(membership_key), cutoff)
        membership_rows = _records_any(membership_value)
        catalog_value, catalog_reason = _available_fact(fact_map.get(catalog_key), cutoff)
        catalog_rows = _records_any(catalog_value)
        catalog_names = _taxonomy_catalog_names(catalog_rows, taxonomy)
        member_map = _taxonomy_memberships(
            membership_rows,
            taxonomy=taxonomy,
            wanted=set(scope_symbols),
            catalog_names=catalog_names,
        )
        membership_by_taxonomy[taxonomy] = member_map
        taxonomy_status[taxonomy] = {
            "available": membership_rows is not None,
            "reason_code": "OK" if membership_rows is not None else membership_reason,
            "catalog_available": catalog_rows is not None,
            "catalog_reason_code": "OK" if catalog_rows is not None else catalog_reason,
            "catalog_count": len(catalog_rows) if catalog_rows is not None else None,
            "membership_row_count": len(membership_rows) if membership_rows is not None else None,
            "mapped_symbol_count": len(member_map),
            "membership_coverage": len(member_map) / len(scope_symbols) if scope_symbols else 1.0,
        }

    history_value, history_reason = _available_fact(fact_map.get("THS_INDUSTRY_HISTORY"), cutoff)
    history_rows = _records_any(history_value)
    history_by_code, future_history_dropped = _sector_history_series(history_rows or (), cutoff)
    history_available = history_rows is not None and bool(history_by_code)
    history_reason_code = "OK" if history_available else history_reason or "HISTORY_RECORDS_UNUSABLE"
    history_sequence_available = any(
        item.get("sequence_valid") is True for item in history_by_code.values()
    )

    pool_value, pool_reason = _available_fact(fact_map.get("LIMIT_UP_POOL"), cutoff)
    pool_rows = _records_any(pool_value)
    pool_symbols = {
        symbol
        for row in (pool_rows or ())
        if (symbol := _row_symbol(row)) is not None and symbol in set(scope_symbols)
    }
    ladder_value, ladder_reason = _available_fact(fact_map.get("LIMIT_UP_LADDER"), cutoff)
    ladder_rows = _records_any(ladder_value)
    ladder_by_symbol, ladder_meta = _latest_ladder_events(ladder_rows or (), cutoff)
    ladder_available = ladder_rows is not None and bool(ladder_meta.get("latest_date"))
    board_flow = _board_capital_flow_index(board_capital_flow_snapshot)

    result_by_taxonomy: dict[str, dict[str, Any]] = {}
    all_missing: list[str] = []
    if not quotes:
        all_missing.append("G0_QUOTES")
    mapped_symbols = {
        symbol
        for member_map in membership_by_taxonomy.values()
        for symbol in member_map
    }
    mapped_quote_coverage = (
        len(mapped_symbols.intersection(quotes)) / len(mapped_symbols)
        if mapped_symbols
        else 0.0
    )
    if mapped_symbols and mapped_quote_coverage < 0.80:
        all_missing.append("G0_QUOTE_COVERAGE")
    for taxonomy in ("industry", "concept"):
        member_map = membership_by_taxonomy[taxonomy]
        sectors = _build_sector_health_rows(
            taxonomy=taxonomy,
            member_map=member_map,
            quotes=quotes,
            history_by_code=history_by_code if taxonomy == "industry" else {},
            pool_symbols=pool_symbols,
            ladder_by_symbol=ladder_by_symbol,
            board_flow=board_flow.get(taxonomy, {}),
        )
        strength_values = [
            _finite(item.get("strength", {}).get("relative_strength_value"))
            for item in sectors
            if isinstance(item.get("strength"), Mapping)
        ]
        for item in sectors:
            strength = item.get("strength")
            relative_value = _finite(strength.get("relative_strength_value")) if isinstance(strength, Mapping) else None
            item["relative_strength_percentile"] = _cross_section_percentile(relative_value, strength_values)
            item["relative_strength"] = {
                "value": relative_value,
                "percentile": item["relative_strength_percentile"],
                "source": strength.get("relative_strength_source") if isinstance(strength, Mapping) else None,
            }
        healthy = [item for item in sectors if item.get("health_state") == "HEALTHY"]
        status = taxonomy_status[taxonomy]
        if status["available"] is not True:
            all_missing.append(f"{taxonomy.upper()}_MEMBERSHIP")
        elif scope_symbols and status["membership_coverage"] < 0.80:
            all_missing.append(f"{taxonomy.upper()}_MEMBERSHIP_COVERAGE")
        if taxonomy == "industry":
            if not history_available:
                all_missing.append("THS_INDUSTRY_HISTORY")
            elif not history_sequence_available:
                all_missing.append("THS_INDUSTRY_HISTORY_SEQUENCE")
        result_by_taxonomy[taxonomy] = {
            **status,
            "sectors": sectors,
            "sector_count": len(sectors),
            "healthy_sector_count": len(healthy),
            "healthy_sectors": [
                {
                    "taxonomy_code": item.get("taxonomy_code"),
                    "taxonomy_name": item.get("taxonomy_name"),
                    "health_state": item.get("health_state"),
                }
                for item in healthy
            ],
        }

    if pool_rows is None:
        all_missing.append("LIMIT_UP_POOL")
    if not ladder_available:
        all_missing.append("LIMIT_UP_LADDER")
    missing_components = list(dict.fromkeys(all_missing))
    sector_count = sum(int(item.get("sector_count") or 0) for item in result_by_taxonomy.values())
    healthy_count = sum(int(item.get("healthy_sector_count") or 0) for item in result_by_taxonomy.values())
    has_membership = any(item.get("mapped_symbol_count", 0) > 0 for item in taxonomy_status.values())
    available = bool(quotes) and has_membership
    mapped_flow_count = sum(
        1
        for taxonomy in result_by_taxonomy.values()
        for row in taxonomy.get("sectors", ())
        if isinstance(row, Mapping) and row.get("capital_flow_available") is True
    )
    capital_flow_available = mapped_flow_count > 0
    data_sufficiency_state = "SUFFICIENT" if available and not missing_components else "PARTIAL"
    return {
        "available": available,
        "reason_code": "OK" if available and not missing_components else "PARTIAL_FACTS" if available else "SECTOR_HEALTH_NOT_READY",
        "source": "THS_TAXONOMY_FROZEN_G0_QUOTES",
        "algorithm_version": SECTOR_HEALTH_ALGORITHM,
        "as_of": cutoff.isoformat(),
        "scope": {
            "symbol_count": len(scope_symbols),
            "symbols_hash": _symbols_digest(scope_symbols),
            "g0_only": True,
            "mapped_symbol_count": len(mapped_symbols),
            "mapped_quote_coverage": mapped_quote_coverage,
        },
        "data_sufficiency_state": data_sufficiency_state,
        "by_taxonomy": result_by_taxonomy,
        # Flat aliases make the contract convenient for existing prompt and
        # dashboard consumers while ``by_taxonomy`` remains canonical.
        "industry": result_by_taxonomy["industry"],
        "concept": result_by_taxonomy["concept"],
        "sector_count": sector_count,
        "healthy_sector_count": healthy_count,
        "limit_up_pool": {
            "available": pool_rows is not None,
            "reason_code": "OK" if pool_rows is not None else pool_reason,
            "member_count": len(pool_symbols),
            "symbols": sorted(pool_symbols),
        },
        "limit_up_ladder": {
            "available": ladder_available,
            "reason_code": "OK" if ladder_available else ladder_reason or "LADDER_DATE_MISSING",
            **ladder_meta,
        },
        "history": {
            "industry_available": history_available,
            "valid_sequence_available": history_sequence_available,
            "reason_code": history_reason_code,
            "future_bars_dropped": future_history_dropped,
            "return_flow_requires_valid_sequence": True,
        },
        "capital_flow_available": capital_flow_available,
        "capital_flow_reason_code": (
            "OK"
            if capital_flow_available
            else str((board_capital_flow_snapshot or {}).get("reason_code") or "SOURCE_NOT_INCLUDED")
        ),
        "capital_flow_mapped_sector_count": mapped_flow_count,
        "turnover_is_capital_flow": False,
        "turnover_metric_role": "PRICE_VOLUME_PROXY_ONLY",
        "missing_components": missing_components,
    }


# The longer name is useful to callers that treat all A2 aggregates as an
# explicit namespace.  Keep both names as aliases so replay code can migrate
# without changing its fact contract.
build_a2_sector_health_snapshot = build_sector_health_snapshot


def _board_capital_flow_index(
    snapshot: Mapping[str, Any] | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Normalize the persisted Eastmoney board ranks for sector joins.

    Eastmoney and THS use different board identifiers, so an exact normalized
    name is the authoritative cross-vendor join when codes do not match.  Rank
    percentiles are vendor-derived relative flow scores; raw amounts and
    percentages remain attached for audit and are never synthesized from
    turnover.
    """

    result: dict[str, dict[str, dict[str, Any]]] = {"industry": {}, "concept": {}}
    if not isinstance(snapshot, Mapping):
        return result
    by_taxonomy = snapshot.get("by_taxonomy")
    if not isinstance(by_taxonomy, Mapping):
        return result
    period_weights = {"today": 0.50, "5d": 0.30, "10d": 0.20}
    for taxonomy in ("industry", "concept"):
        periods = by_taxonomy.get(taxonomy)
        if not isinstance(periods, Mapping):
            continue
        aggregates: dict[str, dict[str, Any]] = {}
        for period, weight in period_weights.items():
            period_snapshot = periods.get(period)
            if not isinstance(period_snapshot, Mapping) or period_snapshot.get("available") is not True:
                continue
            records = period_snapshot.get("records")
            if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
                continue
            rows = [row for row in records if isinstance(row, Mapping)]
            count = len(rows)
            for ordinal, row in enumerate(rows, start=1):
                code = str(row.get("code") or "").strip().upper()
                name = str(row.get("name") or "").strip()
                name_key = _sector_name_key(name)
                identity = code or name_key
                if not identity:
                    continue
                rank = _integer(row.get("rank")) or ordinal
                rank_score = 50.0 if count <= 1 else max(0.0, min(100.0, (count - rank) / (count - 1) * 100.0))
                item = aggregates.setdefault(identity, {
                    "code": code,
                    "name": name,
                    "weighted_score": 0.0,
                    "available_weight": 0.0,
                    "windows": {},
                })
                item["weighted_score"] += rank_score * weight
                item["available_weight"] += weight
                item["windows"][period] = {
                    "rank": rank,
                    "rank_percentile": round(rank_score, 4),
                    "main_net_cny": _finite(row.get("main_net_cny")),
                    "main_pct": _finite(row.get("main_pct")),
                    "change_pct": _finite(row.get("change_pct")),
                    "leader": row.get("leader"),
                    "source_hash": period_snapshot.get("content_hash"),
                }
        for item in aggregates.values():
            weight = float(item["available_weight"])
            if weight <= 0:
                continue
            normalized = {
                "available": True,
                "availability_state": "OBSERVED_VALUE",
                "reason_code": "OK",
                "score": round(float(item["weighted_score"]) / weight, 4),
                "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
                "source_scope": "SECTOR",
                "provider_method": "VENDOR_DERIVED_RANK_PERCENTILE",
                "available_weight": round(weight, 4),
                "code": item["code"],
                "name": item["name"],
                "windows": item["windows"],
            }
            if item["code"]:
                result[taxonomy][str(item["code"])] = normalized
            if item["name"]:
                result[taxonomy][_sector_name_key(item["name"])] = normalized
    return result


def _sector_name_key(value: Any) -> str:
    return "".join(character.lower() for character in str(value or "").strip() if character.isalnum())


def _build_sector_health_rows(
    *,
    taxonomy: str,
    member_map: Mapping[str, Sequence[Mapping[str, str]]],
    quotes: Mapping[str, Mapping[str, Any]],
    history_by_code: Mapping[str, Mapping[str, Any]],
    pool_symbols: set[str],
    ladder_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    board_flow: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for symbol, memberships in member_map.items():
        quote = quotes.get(symbol)
        for membership in memberships:
            code = str(membership.get("taxonomy_code") or "").strip().upper()
            if not code:
                continue
            name = str(membership.get("taxonomy_name") or "").strip()
            group = grouped.setdefault(
                code,
                {
                    "taxonomy": taxonomy,
                    "taxonomy_code": code,
                    "taxonomy_name": name,
                    "members": set(),
                    "observed_changes": [],
                    "amounts": [],
                    "pool_members": set(),
                    "ladder_members": set(),
                    "boards": [],
                },
            )
            if not group["taxonomy_name"] and name:
                group["taxonomy_name"] = name
            group["members"].add(symbol)
            if symbol in pool_symbols:
                group["pool_members"].add(symbol)
            for event in ladder_by_symbol.get(symbol, ()):
                group["ladder_members"].add(symbol)
                board = _integer(event.get("board_num"))
                if board is not None and board > 0:
                    group["boards"].append(board)
            if quote is None:
                continue
            change = _quote_change(quote)
            if change is not None:
                group["observed_changes"].append(change)
            amount = _quote_amount(quote)
            if amount is not None and amount >= 0:
                group["amounts"].append(amount)

    rows: list[dict[str, Any]] = []
    for code, group in sorted(grouped.items(), key=lambda item: (str(item[1]["taxonomy_name"]), item[0])):
        members = group["members"]
        changes = group["observed_changes"]
        advances = sum(value > 0 for value in changes)
        declines = sum(value < 0 for value in changes)
        flats = len(changes) - advances - declines
        observed = len(changes)
        member_count = len(members)
        quote_coverage = observed / member_count if member_count else 0.0
        breadth = advances / observed if observed else None
        balance = (advances - declines) / observed if observed else None
        average_change = _mean(changes)
        median_change = _median(changes)
        history = dict(history_by_code.get(code, {}))
        historical_strength = _finite(history.get("return_5d"))
        if historical_strength is None:
            historical_strength = _finite(history.get("lookback_return"))
        relative_strength_value = historical_strength if historical_strength is not None else average_change
        relative_strength_source = (
            "THS_INDUSTRY_HISTORY"
            if historical_strength is not None
            else "G0_MEMBER_PRICE_CHANGE"
        )
        persistence = _persistence_from_history(history)
        return_flow_state = _return_flow_state(history)
        health_state = _sector_health_state(
            breadth=breadth,
            average_change=average_change,
            quote_coverage=quote_coverage,
            persistence=persistence,
        )
        flow = board_flow.get(code) or board_flow.get(_sector_name_key(group["taxonomy_name"]))
        flow = dict(flow) if isinstance(flow, Mapping) else None
        rows.append({
            "taxonomy": taxonomy,
            "taxonomy_code": code,
            "taxonomy_name": group["taxonomy_name"],
            "member_count": member_count,
            "quote_count": observed,
            "quote_coverage": quote_coverage,
            "advances": advances,
            "declines": declines,
            "flats": flats,
            "breadth": breadth,
            "breadth_balance": balance,
            "strength": {
                "average_change_pct": average_change,
                "median_change_pct": median_change,
                "historical_return_1d": history.get("return_1d"),
                "historical_return_5d": history.get("return_5d"),
                "historical_lookback_return": history.get("lookback_return"),
                "relative_strength_value": relative_strength_value,
                "relative_strength_source": relative_strength_source,
                "amount_total": sum(group["amounts"]) if group["amounts"] else None,
                "amount_is_price_volume_proxy": True,
            },
            "persistence": persistence,
            "return_flow_state": return_flow_state,
            "ladder_count": len(group["ladder_members"]),
            "max_board": max(group["boards"]) if group["boards"] else None,
            "limit_up_count": len(group["pool_members"]),
            "ladder_member_symbols": sorted(group["ladder_members"]),
            "limit_up_member_symbols": sorted(group["pool_members"]),
            "health_state": health_state,
            "capital_flow_available": flow is not None,
            "capital_flow_reason_code": "OK" if flow is not None else "SECTOR_NOT_IN_BOARD_FLOW_RANKING",
            "capital_flow": flow or {
                "available": False,
                "availability_state": "OBSERVED_ABSENT" if board_flow else "SOURCE_UNAVAILABLE",
                "reason_code": "SECTOR_NOT_IN_BOARD_FLOW_RANKING" if board_flow else "SOURCE_UNAVAILABLE",
                "score": None,
                "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
            },
            "turnover_is_capital_flow": False,
            "history": history,
        })
    return rows


def _sector_health_state(
    *,
    breadth: float | None,
    average_change: float | None,
    quote_coverage: float,
    persistence: Mapping[str, Any],
) -> str:
    if breadth is None or average_change is None or quote_coverage <= 0:
        return "UNKNOWN"
    # The deterministic health label is intentionally descriptive, not an
    # entry signal.  Low quote coverage remains visible as DEGRADED instead of
    # silently turning missing members into flat prices.
    if quote_coverage < 0.5:
        return "DEGRADED"
    if breadth >= 0.50 and average_change > 0:
        return "HEALTHY"
    if breadth >= 0.50 and average_change >= 0 and persistence.get("state") in {"PERSISTENT", "REPAIR", "UNKNOWN"}:
        return "REPAIR"
    return "WEAK"


def _persistence_from_history(history: Mapping[str, Any]) -> dict[str, Any]:
    if not history.get("sequence_valid"):
        return {
            "state": "UNKNOWN",
            "available": False,
            "observed_bars": int(history.get("observed_bars") or 0),
            "positive_day_rate": None,
            "consecutive_positive_bars": None,
        }
    positive_rate = _finite(history.get("positive_day_rate"))
    return {
        "state": "PERSISTENT" if positive_rate is not None and positive_rate >= 0.60 else "REPAIR" if positive_rate is not None and positive_rate >= 0.40 else "WEAK",
        "available": True,
        "observed_bars": int(history.get("observed_bars") or 0),
        "positive_day_rate": positive_rate,
        "consecutive_positive_bars": history.get("consecutive_positive_bars"),
    }


def _return_flow_state(history: Mapping[str, Any]) -> str:
    # No sequence means no directional reflow claim.  Current breadth or
    # turnover can never upgrade UNKNOWN to WEAK_TO_STRONG.
    if not history.get("sequence_valid"):
        return "UNKNOWN"
    returns = history.get("returns")
    if not isinstance(returns, Sequence) or isinstance(returns, (str, bytes, bytearray)) or len(returns) < 3:
        return "UNKNOWN"
    values = [value for value in (_finite(item) for item in returns) if value is not None]
    if len(values) < 3:
        return "UNKNOWN"
    split = max(1, len(values) // 2)
    prior = _mean(values[:split])
    recent = _mean(values[split:])
    if prior is None or recent is None:
        return "UNKNOWN"
    if prior <= 0 and recent > 0:
        return "WEAK_TO_STRONG"
    if prior >= 0 and recent < 0:
        return "STRONG_TO_WEAK"
    if recent > prior:
        return "IMPROVING"
    if recent < prior:
        return "DETERIORATING"
    return "STABLE"


def _sector_history_series(
    rows: Sequence[Mapping[str, Any]],
    cutoff: datetime,
) -> tuple[dict[str, dict[str, Any]], int]:
    cutoff_ms = int(cutoff.timestamp() * 1000)
    by_code: dict[str, dict[str, Any]] = {}
    future_dropped = 0
    for raw in rows:
        code = _taxonomy_code(raw, "industry")
        if not code:
            continue
        name = _taxonomy_name(raw, "industry")
        bars = raw.get("bars")
        if not isinstance(bars, Sequence) or isinstance(bars, (str, bytes, bytearray)):
            continue
        parsed: dict[int, float] = {}
        for bar in bars:
            if not isinstance(bar, Mapping):
                continue
            day = _bar_epoch_ms(bar)
            close = _finite(bar.get("close_price") if "close_price" in bar else bar.get("close"))
            if day is None or close is None or close <= 0:
                continue
            if day > cutoff_ms:
                future_dropped += 1
                continue
            parsed[day] = close
        ordered = sorted(parsed.items())
        if not ordered:
            continue
        closes = [close for _day, close in ordered]
        returns = [closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes)) if closes[index - 1] > 0]
        if not returns:
            sequence_valid = False
        else:
            sequence_valid = len(ordered) >= _SECTOR_SEQUENCE_MIN_BARS and all(
                ordered[index][0] > ordered[index - 1][0] for index in range(1, len(ordered))
            )
        positive = [value > 0 for value in returns]
        consecutive = 0
        for value in reversed(positive):
            if not value:
                break
            consecutive += 1
        by_code[code] = {
            "taxonomy_code": code,
            "taxonomy_name": name,
            "sequence_valid": sequence_valid,
            "observed_bars": len(ordered),
            "returns": returns,
            "return_1d": returns[-1] if returns else None,
            "return_5d": _period_return([{"close": close} for close in closes], 5),
            "lookback_return": closes[-1] / closes[0] - 1.0 if len(closes) >= 2 else None,
            "positive_day_rate": sum(positive) / len(positive) if positive else None,
            "consecutive_positive_bars": consecutive if returns else None,
        }
    return by_code, future_dropped


def _latest_ladder_events(
    rows: Sequence[Mapping[str, Any]],
    cutoff: datetime,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    cutoff_day = cutoff.date()
    dated: list[tuple[date, Mapping[str, Any]]] = []
    future_dropped = 0
    for record in rows:
        day = _record_day(record)
        if day is None:
            continue
        if day > cutoff_day:
            future_dropped += 1
            continue
        dated.append((day, record))
    if not dated:
        return {}, {"latest_date": None, "event_count": 0, "future_records_dropped": future_dropped}
    latest_day = max(day for day, _record in dated)
    events: dict[str, list[dict[str, Any]]] = {}
    for day, record in dated:
        if day != latest_day:
            continue
        for row in _ladder_rows(record):
            symbol = _row_symbol(row)
            if symbol is None:
                continue
            event = {
                "date": day.isoformat(),
                "board_num": _integer(row.get("board_num")),
                "seal_nextday": row.get("seal_nextday") if isinstance(row.get("seal_nextday"), bool) else None,
            }
            events.setdefault(symbol, []).append(event)
    return events, {
        "latest_date": latest_day.isoformat(),
        "event_count": sum(len(items) for items in events.values()),
        "symbol_count": len(events),
        "future_records_dropped": future_dropped,
    }


def _ladder_rows(record: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    if _row_symbol(record) is not None:
        rows.append(record)
    boards = record.get("boards")
    if isinstance(boards, Mapping):
        for board_rows in boards.values():
            if isinstance(board_rows, Mapping):
                board_rows = [board_rows]
            if not isinstance(board_rows, Sequence) or isinstance(board_rows, (str, bytes, bytearray)):
                continue
            for raw in board_rows:
                if isinstance(raw, Mapping):
                    rows.append(raw)
    nested = record.get("items") or record.get("records")
    if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
        for raw in nested:
            if isinstance(raw, Mapping):
                rows.extend(_ladder_rows(raw))
    return rows


def _taxonomy_memberships(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    taxonomy: str,
    wanted: set[str],
    catalog_names: Mapping[str, str],
) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}
    if rows is None:
        return result
    for raw in rows:
        symbol = _row_symbol(raw)
        if symbol is None or symbol not in wanted:
            continue
        memberships = raw.get("memberships")
        if isinstance(memberships, Mapping):
            memberships = [memberships]
        if not isinstance(memberships, Sequence) or isinstance(memberships, (str, bytes, bytearray)):
            # A reverse membership row normally has ``thscode`` for the stock
            # and no taxonomy code of its own.  Do not mistake that stock code
            # for a sector when the nested membership list is absent.
            raw_code = raw.get(f"{taxonomy}_thscode") or raw.get("taxonomy_code")
            memberships = [raw] if raw_code is not None and str(raw_code).strip() else []
        for membership in memberships:
            if not isinstance(membership, Mapping):
                continue
            code = _taxonomy_code(membership, taxonomy)
            if not code:
                continue
            name = _taxonomy_name(membership, taxonomy) or catalog_names.get(code, "")
            item = {"taxonomy_code": code, "taxonomy_name": name}
            existing = result.setdefault(symbol, [])
            if item not in existing:
                existing.append(item)
    for values in result.values():
        values.sort(key=lambda item: (item["taxonomy_code"], item["taxonomy_name"]))
    return result


def _taxonomy_catalog_names(rows: Sequence[Mapping[str, Any]] | None, taxonomy: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows or ():
        code = _taxonomy_code(row, taxonomy)
        if code:
            result[code] = _taxonomy_name(row, taxonomy)
    return result


def _taxonomy_code(row: Mapping[str, Any], taxonomy: str) -> str:
    keys = (
        f"{taxonomy}_thscode",
        "taxonomy_code",
        "thscode",
        "code",
    )
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().upper()
    return ""


def _taxonomy_name(row: Mapping[str, Any], taxonomy: str) -> str:
    for key in (f"{taxonomy}_name", "taxonomy_name", "name"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _fact_mapping(facts: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = facts.get("facts") if isinstance(facts, Mapping) else None
    return nested if isinstance(nested, Mapping) else facts


def _records_any(value: Mapping[str, Any] | None) -> list[Mapping[str, Any]] | None:
    if value is None:
        return None
    for key in ("records", "items", "memberships"):
        records = value.get(key)
        if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
            normalized = [item for item in records if isinstance(item, Mapping)]
            if len(normalized) == len(records):
                declared = value.get("record_count")
                if isinstance(declared, int) and not isinstance(declared, bool) and declared != len(normalized):
                    continue
                return normalized
    payload = value.get("payload")
    if isinstance(payload, Mapping):
        return _records_any(payload)
    return None


def _quote_map(
    values: Sequence[Any] | Mapping[str, Any],
    wanted: set[str] | None,
) -> dict[str, dict[str, Any]]:
    rows: list[tuple[str | None, Any]] = []
    if isinstance(values, Mapping):
        if any(key in values for key in ("records", "items")):
            raw_rows = values.get("records") or values.get("items") or []
            if isinstance(raw_rows, Sequence) and not isinstance(raw_rows, (str, bytes, bytearray)):
                rows.extend((None, item) for item in raw_rows)
        else:
            rows.extend((str(key), item) for key, item in values.items())
    elif isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        rows.extend((None, item) for item in values)
    result: dict[str, dict[str, Any]] = {}
    for fallback, raw in rows:
        row = _mapping(raw)
        symbol = _row_symbol(row) or _normalise_symbol(fallback)
        if symbol is None or (wanted is not None and symbol not in wanted):
            continue
        result[symbol] = row
    return result


def _row_symbol(row: Mapping[str, Any] | None) -> str | None:
    if not isinstance(row, Mapping):
        return None
    # ``code`` is also used for taxonomy codes by some normalized edge
    # payloads, so stock-specific keys must win before that generic fallback.
    for key in ("thscode", "symbol", "ticker", "member_thscode", "stock_code", "code"):
        symbol = _normalise_symbol(row.get(key))
        if symbol:
            return symbol
    return None


def _normalise_symbols(values: Sequence[str] | set[str] | None) -> set[str]:
    return {symbol for value in (values or ()) if (symbol := _normalise_symbol(value)) is not None}


def _normalise_symbol(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _quote_change(row: Mapping[str, Any]) -> float | None:
    for key in ("change_ratio_pct", "price_change_ratio_pct", "pct_chg", "change_pct", "change_ratio", "change"):
        value = _finite(row.get(key))
        if value is not None:
            return value
    return None


def _quote_amount(row: Mapping[str, Any]) -> float | None:
    for key in ("amount", "turnover", "turnover_amount", "成交额"):
        value = _finite(row.get(key))
        if value is not None:
            return value
    return None


def _bar_epoch_ms(row: Mapping[str, Any]) -> int | None:
    value = row.get("date_ms")
    if value is None:
        value = row.get("timestamp") or row.get("time_ms") or row.get("date")
    if isinstance(value, str):
        text = value.strip()
        for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return int(datetime.strptime(text, pattern).replace(tzinfo=SHANGHAI).timestamp() * 1000)
            except ValueError:
                continue
    integer = _integer(value)
    if integer is None:
        return None
    if 10_000_000 <= integer < 100_000_000:
        try:
            return int(datetime.strptime(str(integer), "%Y%m%d").replace(tzinfo=SHANGHAI).timestamp() * 1000)
        except ValueError:
            pass
    return integer if integer >= 10_000_000_000 else integer * 1000


def _record_day(row: Mapping[str, Any]) -> date | None:
    value = row.get("date") or row.get("trade_date") or row.get("trade_date_ms")
    if isinstance(value, datetime):
        return _aware(value).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(text[:10] if pattern != "%Y%m%d" else text[:8], pattern).date()
            except ValueError:
                continue
    integer = _integer(value)
    if integer is not None:
        try:
            if 10_000_000 <= integer < 100_000_000:
                return datetime.strptime(str(integer), "%Y%m%d").date()
            seconds = integer / 1000 if integer >= 10_000_000_000 else integer
            return datetime.fromtimestamp(seconds, tz=SHANGHAI).date()
        except (OSError, OverflowError, ValueError):
            return None
    return None


def _median(values: Sequence[Any]) -> float | None:
    numbers = sorted(value for value in (_finite(item) for item in values) if value is not None)
    if not numbers:
        return None
    middle = len(numbers) // 2
    if len(numbers) % 2:
        return numbers[middle]
    return (numbers[middle - 1] + numbers[middle]) / 2.0


def _symbols_digest(symbols: Sequence[str]) -> str:
    return hashlib.sha256("|".join(sorted(symbols)).encode("utf-8")).hexdigest()


def _sector_history_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any] | None:
    """Build short- and monthly-cycle metrics from point-in-time index bars.

    ``top3_by_day`` and ``persistent_mainline_candidates`` are retained for
    compatibility with the existing regime/A2 consumers.  The monthly view is
    intentionally calculated independently: it uses up to 21 closes (up to 20
    return periods), counts Top-10 appearances across the whole window, and
    only admits a sector after it appears on at least two ranking days.  That
    last gate prevents a one-day price pulse from becoming a monthly mainline.

    A bar is usable for price calculations even when its turnover is absent;
    turnover-dependent fields then become ``None``.  When ``as_of`` is given,
    bars whose epoch-millisecond timestamp is in the future are excluded.
    """

    series: dict[str, dict[int, dict[str, float | None]]] = {}
    names: dict[str, str] = {}
    cutoff_ms = int(_aware(as_of).timestamp() * 1000) if as_of is not None else None
    future_bars_dropped = 0
    for raw in rows:
        code = str(raw.get("industry_thscode") or "")
        name = str(raw.get("industry_name") or "")
        bars = raw.get("bars")
        if not code.startswith("881") or not isinstance(bars, Sequence) or isinstance(bars, (str, bytes, bytearray)):
            continue
        parsed: dict[int, dict[str, float | None]] = {}
        for bar in bars:
            if not isinstance(bar, Mapping):
                continue
            day = _integer(bar.get("date_ms"))
            close = _finite(bar.get("close_price"))
            if cutoff_ms is not None and day is not None and day > cutoff_ms:
                future_bars_dropped += 1
                continue
            turnover = _finite(bar.get("turnover"))
            if day is None or close is None or close <= 0:
                continue
            if turnover is not None and turnover < 0:
                turnover = None
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
        recent_turnover = _mean(item.get("turnover") for item in ordered[-3:])
        prior_values = ordered[:-3]
        prior_turnover = _mean(item.get("turnover") for item in prior_values)
        candidates.append({
            "industry_thscode": code,
            "industry_name": names.get(code, ""),
            "top3_appearance_count": count,
            "lookback_return": return_lookback,
            "recent_turnover": recent_turnover,
            "turnover_persistence_ratio": (
                recent_turnover / prior_turnover
                if recent_turnover is not None and prior_turnover is not None and prior_turnover > 0
                else None
            ),
        })
    persistent = [
        item for item in candidates
        if item["top3_appearance_count"] >= 2 and item["lookback_return"] > 0
    ]

    # Monthly rotation: retain the final 21 closes so that a complete series
    # has 5d/10d/20d return observations.  Missing bars are not filled or
    # forward-filled; each metric explicitly degrades to None when it lacks
    # enough point-in-time closes.
    monthly_dates = all_dates[-_MONTHLY_OBSERVATION_BARS:]
    monthly_top10: list[dict[str, Any]] = []
    monthly_appearances: dict[str, int] = {}
    for previous_day, day in zip(monthly_dates, monthly_dates[1:]):
        returns = []
        for code, values in series.items():
            previous = values.get(previous_day)
            current = values.get(day)
            if previous is None or current is None or previous["close"] is None or previous["close"] <= 0:
                continue
            returns.append((current["close"] / previous["close"] - 1.0, code))
        if len(returns) < 3:
            continue
        leaders = sorted(returns, key=lambda item: (-item[0], item[1]))[:10]
        for _return, code in leaders:
            monthly_appearances[code] = monthly_appearances.get(code, 0) + 1
        monthly_top10.append({
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

    monthly_returns: dict[str, dict[str, float | None]] = {}
    relative_strength_inputs: dict[str, float] = {}
    monthly_candidates: list[dict[str, Any]] = []
    for code, values in series.items():
        ordered = [values[day] for day in monthly_dates if day in values]
        return_5d = _period_return(ordered, 5)
        return_10d = _period_return(ordered, 10)
        return_20d = _period_return(ordered, 20)
        monthly_returns[code] = {
            "return_5d": return_5d,
            "return_10d": return_10d,
            "return_20d": return_20d,
        }
        if return_20d is not None:
            relative_strength_inputs[code] = return_20d

    monthly_min_appearances = (
        max(
            _MONTHLY_TOP10_MIN_APPEARANCES,
            math.ceil(len(monthly_top10) * 0.10),
        )
        if monthly_top10
        else _MONTHLY_TOP10_MIN_APPEARANCES
    )
    monthly_rank_days = len(monthly_top10)
    for code, count in monthly_appearances.items():
        returns = monthly_returns.get(code, {})
        recent_window = min(5, len([day for day in monthly_dates if day in series[code]]))
        ordered = [series[code][day] for day in monthly_dates if day in series[code]]
        recent_values = ordered[-recent_window:] if recent_window else []
        prior_values = ordered[:-recent_window] if recent_window else []
        recent_turnover = _mean(item.get("turnover") for item in recent_values)
        prior_turnover = _mean(item.get("turnover") for item in prior_values)
        relative_strength = _cross_section_percentile(
            relative_strength_inputs.get(code),
            tuple(relative_strength_inputs.values()),
        )
        candidate = {
            "industry_thscode": code,
            "industry_name": names.get(code, ""),
            "return_5d": returns.get("return_5d"),
            "return_10d": returns.get("return_10d"),
            "return_20d": returns.get("return_20d"),
            "relative_strength_percentile_20d": relative_strength,
            "top10_appearance_count": count,
            "top10_appearance_rate": count / monthly_rank_days if monthly_rank_days else None,
            "recent_turnover": recent_turnover,
            "turnover_persistence_ratio": (
                recent_turnover / prior_turnover
                if recent_turnover is not None and prior_turnover is not None and prior_turnover > 0
                else None
            ),
        }
        # A single daily pulse has count=1 and is deliberately not promoted to
        # the monthly candidate set.  No sector names or industry classes are
        # special-cased here; the same rule applies to every taxonomy member.
        if count >= monthly_min_appearances:
            monthly_candidates.append(candidate)

    monthly_candidates.sort(
        key=lambda item: (
            -int(item["top10_appearance_count"]),
            -float(item["relative_strength_percentile_20d"])
            if item["relative_strength_percentile_20d"] is not None
            else float("inf"),
            -float(item["return_20d"])
            if item["return_20d"] is not None
            else float("inf"),
            str(item["industry_thscode"]),
        )
    )
    return {
        "lookback_trading_days": len(daily_top3),
        "top3_daily_overlap": overlap,
        "top3_by_day": daily_top3,
        "persistent_mainline_candidates": persistent,
        "algorithm_version": SECTOR_CYCLE_ALGORITHM,
        "monthly_lookback_trading_days": max(0, len(monthly_dates) - 1),
        "monthly_observation_bars": len(monthly_dates),
        "monthly_rank_days": monthly_rank_days,
        "monthly_top10_by_day": monthly_top10,
        "monthly_min_top10_appearances": monthly_min_appearances,
        "monthly_rotation_candidates": monthly_candidates,
        "future_bars_dropped": future_bars_dropped,
        "turnover_metric_role": "PRICE_VOLUME_PROXY_ONLY",
    }


def _period_return(
    ordered: Sequence[Mapping[str, float | None]],
    periods: int,
) -> float | None:
    if periods <= 0 or len(ordered) < periods + 1:
        return None
    start = _finite(ordered[-(periods + 1)].get("close"))
    end = _finite(ordered[-1].get("close"))
    if start is None or end is None or start <= 0:
        return None
    return end / start - 1.0


def _mean(values: Sequence[Any] | Any) -> float | None:
    if isinstance(values, (str, bytes, bytearray)):
        return None
    try:
        numbers = [number for value in values if (number := _finite(value)) is not None]
    except TypeError:
        return None
    return sum(numbers) / len(numbers) if numbers else None


def _cross_section_percentile(value: float | None, population: Sequence[float]) -> float | None:
    if value is None or not population:
        return None
    finite = [item for item in population if _finite(item) is not None]
    if not finite:
        return None
    less = sum(item < value for item in finite)
    equal = sum(item == value for item in finite)
    # Mid-rank percentile, deterministic under ties and bounded to [0, 100].
    return 100.0 * (less + 0.5 * equal) / len(finite)


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


def _emotion_cycle_contract(
    *,
    temperature: str,
    breadth: float,
    limit_up: int,
    limit_down: int,
    break_rate: float | None,
    ladder_height: int | None,
) -> dict[str, Any]:
    """Translate existing market facts into an execution permission.

    This is intentionally not another score.  It reuses the already-audited
    market-temperature states and only distinguishes the points where the
    short-term emotion playbook changes materially: probe during ignition,
    follow the core during confirmation/acceleration, and stop creating new
    leader plans during climax, divergence, retreat or ice point.
    """

    height = ladder_height or 0
    mapping = {
        "ICE": ("ICE_POINT", "冰点期", "NO_NEW_ENTRY", ["MARKET_ICE_POINT"]),
        "DIVERGING_WEAK": (
            "DIVERGENCE",
            "分化退潮期",
            "NO_NEW_ENTRY",
            ["HIGH_BREAK_RATE_DIVERGENCE"],
        ),
        "OVERHEATED": (
            "CLIMAX",
            "情绪高潮期",
            "NO_NEW_ENTRY",
            ["OVERHEATED_CLIMAX_NO_NEW_LEADER"],
        ),
        "STRONG": (
            "ACCELERATION" if height >= 2 else "STARTUP",
            "加速期" if height >= 2 else "启动期",
            "ALLOW_CORE",
            ["HEALTHY_BREADTH_AND_LADDER"],
        ),
    }
    if temperature in mapping:
        stage, stage_cn, permission, reason_codes = mapping[temperature]
    elif temperature == "WEAK" and (
        (break_rate is not None and break_rate >= 0.30)
        or limit_down >= max(5, limit_up)
    ):
        stage, stage_cn, permission, reason_codes = (
            "DIVERGENCE",
            "分化退潮期",
            "NO_NEW_ENTRY",
            ["MARKET_RETREAT"],
        )
    elif temperature == "WEAK":
        stage, stage_cn, permission, reason_codes = (
            "LATENT",
            "潜伏期",
            "WATCH_ONLY",
            ["LATENT_TURNING_POINT_NOT_CONFIRMED"],
        )
    elif limit_up > limit_down and breadth >= 0.45 and height >= 1:
        stage, stage_cn, permission, reason_codes = (
            "STARTUP",
            "启动期",
            "PROBE_ONLY",
            ["RECOVERY_WITH_LADDER_SEED"],
        )
    else:
        stage, stage_cn, permission, reason_codes = (
            "LATENT",
            "潜伏期",
            "WATCH_ONLY",
            ["EMOTION_CYCLE_NOT_CONFIRMED"],
        )
    return {
        "stage": stage,
        "stage_cn": stage_cn,
        "new_long_permission": permission,
        "reason_codes": reason_codes,
        "evidence": {
            "temperature": temperature,
            "breadth": breadth,
            "limit_up_count": limit_up,
            "limit_down_count": limit_down,
            "break_rate": break_rate,
            "ladder_height": ladder_height,
            "scoring_used": False,
        },
    }


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
    "build_a2_sector_health_snapshot",
    "build_crowding_snapshot",
    "build_market_emotion",
    "build_news_heat_snapshot",
    "build_sector_health_snapshot",
    "build_sector_cycle_and_permissions",
]
