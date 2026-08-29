"""Deterministic A2 role, tier and chain-resonance materialization.

The module is intentionally pure.  External collection and feature-store
publication happen elsewhere, allowing the same frozen inputs to be replayed
without a network connection.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
A2_FEATURE_SCHEMA = "a2-features/3.0.0"


def build_a2_feature_snapshot(
    *,
    candidates: Sequence[Mapping[str, Any]],
    daily_bars: Mapping[str, Sequence[Mapping[str, Any]]],
    industry_membership: Mapping[str, Any] | None,
    concept_membership: Mapping[str, Any] | None,
    ladder_snapshot: Mapping[str, Any] | None,
    dragon_tiger_snapshot: Mapping[str, Any] | None,
    attention_snapshot: Mapping[str, Any] | None,
    sector_cycle_snapshot: Mapping[str, Any] | None,
    capital_flow_snapshot: Mapping[str, Any] | None,
    as_of: datetime,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    candidate_by_symbol = {
        symbol: dict(row)
        for row in candidates
        if (symbol := _symbol(row))
    }
    symbols = tuple(sorted(candidate_by_symbol))
    returns = {
        symbol: value
        for symbol in symbols
        if (value := _return_20d(daily_bars.get(symbol, ()), cutoff.date())) is not None
    }
    return_percentiles = _percentiles(returns)
    liquidity_values = {
        symbol: value
        for symbol, row in candidate_by_symbol.items()
        if (value := _number(row.get("amount") or row.get("turnover") or row.get("daily_turnover"))) is not None
        and value >= 0
    }
    liquidity_percentiles = _percentiles(liquidity_values)
    industry_by_symbol = _membership_by_symbol(industry_membership, taxonomy="INDUSTRY")
    concept_by_symbol = _membership_by_symbol(concept_membership, taxonomy="CONCEPT")
    ladder = _ladder_by_symbol(ladder_snapshot, cutoff.date())
    ladder_observed, ladder_state, ladder_reason = _dataset_observation(ladder_snapshot)
    dragon = _event_symbols(dragon_tiger_snapshot)
    attention = _event_symbols(attention_snapshot)
    capital_by_symbol = (
        capital_flow_snapshot.get("by_symbol", {})
        if isinstance(capital_flow_snapshot, Mapping)
        and isinstance(capital_flow_snapshot.get("by_symbol"), Mapping)
        else {}
    )

    trend_by_symbol: dict[str, dict[str, Any]] = {}
    tier_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        ladder_row = ladder.get(symbol)
        relative = return_percentiles.get(symbol)
        trend_by_symbol[symbol] = _factor(
            relative,
            source="LOCAL_POINT_IN_TIME_DAILY_BARS",
            availability_state="OBSERVED_VALUE" if relative is not None else "SOURCE_FAILED",
            reason_code="OK" if relative is not None else "A2_TREND_DAILY_BARS_MISSING",
            extra={"trend_percentile": relative},
        )
        if ladder_row is not None:
            height = int(_number(ladder_row.get("board_num")) or 1)
            score = min(100.0, 55.0 + height * 7.5)
            tier = "T3_PLUS" if height >= 3 else "T2" if height == 2 else "T1"
            tier_by_symbol[symbol] = _factor(
                score,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state="OBSERVED_VALUE",
                reason_code="OK",
                extra={"tier": tier, "ladder_height": height, "trend_percentile": relative},
            )
        elif ladder_observed:
            tier_by_symbol[symbol] = _factor(
                0.0,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state="OBSERVED_ABSENT",
                reason_code="NO_LIMIT_UP_EVENT",
                extra={"tier": "NONE", "ladder_height": 0, "trend_percentile": relative},
            )
        else:
            tier_by_symbol[symbol] = _factor(
                None,
                source="HITHINK_LIMIT_UP_LADDER",
                availability_state=ladder_state,
                reason_code=ladder_reason,
                extra={"tier": "UNKNOWN", "ladder_height": None, "trend_percentile": relative},
            )

    leader_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        components: list[tuple[float, float]] = []
        relative = return_percentiles.get(symbol)
        liquidity = liquidity_percentiles.get(symbol)
        tier_score = _number(tier_by_symbol[symbol].get("score"))
        capital_row = capital_by_symbol.get(symbol)
        capital_score = _number(capital_row.get("capital_flow_score")) if isinstance(capital_row, Mapping) else None
        if relative is not None:
            components.append((relative, 0.45))
        if liquidity is not None:
            components.append((liquidity, 0.30))
        if tier_score is not None:
            components.append((tier_score, 0.15))
        if capital_score is not None and isinstance(capital_row, Mapping) and capital_row.get("available") is True:
            components.append((capital_score, 0.10))
        score = _weighted(components)
        if score is not None:
            confirmation = int(symbol in dragon) * 6 + int(symbol in attention) * 4
            score = min(100.0, score + confirmation)
        height = int(_number(tier_by_symbol[symbol].get("ladder_height")) or 0)
        if height >= 2:
            role = "EMOTION_LEADER"
        elif relative is not None and relative >= 85:
            role = "TREND_LEADER"
        elif liquidity is not None and liquidity >= 80 and capital_score is not None and capital_score >= 60:
            role = "INSTITUTIONAL_CORE"
        elif liquidity is not None and liquidity >= 80:
            role = "CAPACITY_CORE"
        elif score is not None:
            role = "FOLLOWER"
        else:
            role = "UNCONFIRMED"
        leader_by_symbol[symbol] = _factor(
            score,
            source="LOCAL_THEME_CROSS_SECTION",
            availability_state="OBSERVED_VALUE" if score is not None else "SOURCE_FAILED",
            reason_code="OK" if score is not None else "A2_LEADER_COMPONENTS_MISSING",
            extra={
                "role": role,
                "relative_strength_percentile": relative,
                "liquidity_percentile": liquidity,
                "tier_score": tier_score,
                "capital_flow_score": capital_score,
                "dragon_tiger_confirmation": symbol in dragon,
                "attention_confirmation": symbol in attention,
            },
        )

    group_members: dict[str, set[str]] = defaultdict(set)
    group_meta: dict[str, dict[str, str]] = {}
    for symbol in symbols:
        for item in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ())):
            code = str(item.get("taxonomy_code") or "").strip().upper()
            if not code:
                continue
            key = f"{item.get('taxonomy')}:{code}"
            group_members[key].add(symbol)
            group_meta[key] = {
                "taxonomy": str(item.get("taxonomy") or ""),
                "taxonomy_code": code,
                "taxonomy_name": str(item.get("taxonomy_name") or ""),
            }
    theme_metrics: dict[str, dict[str, Any]] = {}
    for key, members in sorted(group_members.items()):
        observed_returns = [returns[symbol] for symbol in members if symbol in returns]
        observed_relative = [return_percentiles[symbol] for symbol in members if symbol in return_percentiles]
        observed_capital = [
            float(row["capital_flow_score"])
            for symbol in members
            if isinstance((row := capital_by_symbol.get(symbol)), Mapping)
            and row.get("available") is True
            and _number(row.get("capital_flow_score")) is not None
        ]
        ladder_count = sum(symbol in ladder for symbol in members)
        dragon_count = sum(symbol in dragon for symbol in members)
        breadth = sum(value > 0 for value in observed_returns) / len(observed_returns) if observed_returns else None
        relative_mean = sum(observed_relative) / len(observed_relative) if observed_relative else None
        capital_mean = sum(observed_capital) / len(observed_capital) if observed_capital else None
        ladder_ratio = ladder_count / len(members) if members else None
        dragon_ratio = dragon_count / len(members) if members else None
        cycle_score = _cycle_score(sector_cycle_snapshot, group_meta[key]["taxonomy_code"])
        components: list[tuple[float, float]] = []
        if breadth is not None:
            components.append((breadth * 100.0, 0.25))
        if relative_mean is not None:
            components.append((relative_mean, 0.25))
        if capital_mean is not None:
            components.append((capital_mean, 0.20))
        if ladder_ratio is not None:
            components.append((min(100.0, ladder_ratio * 500.0), 0.10))
        if dragon_ratio is not None:
            components.append((min(100.0, dragon_ratio * 500.0), 0.05))
        if cycle_score is not None:
            components.append((cycle_score, 0.15))
        score = _weighted(components)
        coverage = len(observed_returns) / len(members) if members else 0.0
        theme_metrics[key] = {
            **group_meta[key],
            "available": score is not None and coverage >= 0.80,
            "availability_state": "OBSERVED_VALUE" if score is not None and coverage >= 0.80 else "SOURCE_FAILED",
            "reason_code": "OK" if score is not None and coverage >= 0.80 else "A2_CHAIN_MEMBER_COVERAGE_INSUFFICIENT",
            "score": round(score, 4) if score is not None else None,
            "member_count": len(members),
            "return_coverage": round(coverage, 6),
            "breadth": round(breadth, 6) if breadth is not None else None,
            "relative_strength_mean": round(relative_mean, 4) if relative_mean is not None else None,
            "capital_flow_mean": round(capital_mean, 4) if capital_mean is not None else None,
            "capital_flow_coverage": round(len(observed_capital) / len(members), 6) if members else 0.0,
            "ladder_member_count": ladder_count,
            "dragon_tiger_member_count": dragon_count,
            "cycle_score": cycle_score,
        }

    chain_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        keys = []
        for item in (*industry_by_symbol.get(symbol, ()), *concept_by_symbol.get(symbol, ())):
            code = str(item.get("taxonomy_code") or "").strip().upper()
            if code:
                keys.append(f"{item.get('taxonomy')}:{code}")
        available_rows = [theme_metrics[key] for key in keys if key in theme_metrics and theme_metrics[key].get("available") is True]
        best = max(available_rows, key=lambda row: float(row.get("score") or 0.0), default=None)
        chain_by_symbol[symbol] = _factor(
            _number(best.get("score")) if best else None,
            source="POINT_IN_TIME_TAXONOMY_AGGREGATE",
            availability_state="OBSERVED_VALUE" if best else "SOURCE_FAILED",
            reason_code="OK" if best else "A2_CHAIN_MAPPING_OR_COVERAGE_MISSING",
            extra={
                "taxonomy": best.get("taxonomy") if best else None,
                "taxonomy_code": best.get("taxonomy_code") if best else None,
                "taxonomy_name": best.get("taxonomy_name") if best else None,
            },
        )

    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        capital = capital_by_symbol.get(symbol)
        capital_factor = (
            _factor(
                _number(capital.get("capital_flow_score")),
                source=str(capital_flow_snapshot.get("source_id") or "CAPITAL_FLOW_SNAPSHOT"),
                availability_state=str(capital.get("availability_state") or "OBSERVED_VALUE"),
                reason_code=str(capital.get("reason_code") or "OK"),
                extra={"source_refs": list(capital.get("source_refs") or ())},
            )
            if isinstance(capital, Mapping)
            else _factor(None, source="CAPITAL_FLOW_SNAPSHOT", availability_state="NOT_CONFIGURED", reason_code="A2_CAPITAL_FLOW_UNAVAILABLE")
        )
        factors = {
            "capital_flow": capital_factor,
            "tier_structure": tier_by_symbol[symbol],
            # Trend strength is useful for leader identification but is not a
            # substitute for an observed limit-up ladder.  It deliberately
            # remains outside the canonical A2 factor weights.
            "trend_strength_proxy": trend_by_symbol[symbol],
            "leader_structure": leader_by_symbol[symbol],
            "index_chain_resonance": chain_by_symbol[symbol],
        }
        by_symbol[symbol] = {
            "symbol": symbol,
            "factors": factors,
            **factors,
            "available_factor_count": sum(item.get("available") is True for item in factors.values()),
            "missing_factor_count": sum(item.get("available") is not True for item in factors.values()),
            "leader_role": leader_by_symbol[symbol].get("role"),
            "tier": tier_by_symbol[symbol].get("tier"),
        }

    symbol_count = len(symbols)
    daily_bar_coverage = sum(symbol in returns for symbol in symbols) / symbol_count if symbol_count else 0.0
    identity_coverage = (
        sum(bool(industry_by_symbol.get(symbol) or concept_by_symbol.get(symbol)) for symbol in symbols) / symbol_count
        if symbol_count
        else 0.0
    )
    factor_coverage = {
        name: (
            sum(by_symbol[symbol]["factors"][name].get("available") is True for symbol in symbols) / symbol_count
            if symbol_count
            else 0.0
        )
        for name in ("capital_flow", "tier_structure", "leader_structure", "index_chain_resonance")
    }
    critical_sufficient = bool(symbols) and all((
        daily_bar_coverage >= 0.95,
        identity_coverage >= 0.95,
        factor_coverage["capital_flow"] >= 0.90,
        factor_coverage["tier_structure"] >= 0.90,
        factor_coverage["leader_structure"] >= 0.90,
        factor_coverage["index_chain_resonance"] >= 0.90,
    ))
    payload: dict[str, Any] = {
        "schema_version": A2_FEATURE_SCHEMA,
        "available": critical_sufficient,
        "reason_code": "OK" if critical_sufficient else "A2_CRITICAL_DATA_INSUFFICIENT",
        "data_sufficiency_state": "SUFFICIENT" if critical_sufficient else "INSUFFICIENT",
        "as_of": cutoff.isoformat(),
        "symbol_count": symbol_count,
        "daily_bar_coverage": round(daily_bar_coverage, 6),
        "identity_coverage": round(identity_coverage, 6),
        "factor_coverage": {key: round(value, 6) for key, value in factor_coverage.items()},
        "coverage_thresholds": {
            "daily_bars": 0.95,
            "identity": 0.95,
            "capital_flow": 0.90,
            "tier_structure": 0.90,
            "leader_structure": 0.90,
            "index_chain_resonance": 0.90,
        },
        "ladder_dataset_state": ladder_state,
        "ladder_dataset_reason_code": ladder_reason,
        "capital_flow_available": bool(isinstance(capital_flow_snapshot, Mapping) and capital_flow_snapshot.get("available") is True),
        "capital_flow_method": capital_flow_snapshot.get("provider_method") if isinstance(capital_flow_snapshot, Mapping) else None,
        "by_symbol": by_symbol,
        "theme_metrics": theme_metrics,
    }
    payload["content_hash"] = _hash(payload)
    return payload


def _membership_by_symbol(value: Mapping[str, Any] | None, *, taxonomy: str) -> dict[str, tuple[dict[str, str], ...]]:
    records = _records(value)
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in records:
        symbol = _symbol(row)
        memberships = row.get("memberships")
        if not symbol or not isinstance(memberships, Sequence) or isinstance(memberships, (str, bytes, bytearray)):
            continue
        for item in memberships:
            if not isinstance(item, Mapping):
                continue
            code = str(
                item.get("taxonomy_code")
                or item.get("industry_thscode")
                or item.get("concept_thscode")
                or ""
            ).strip().upper()
            name = str(
                item.get("taxonomy_name")
                or item.get("industry_name")
                or item.get("concept_name")
                or ""
            ).strip()
            if code:
                result[symbol].append({"taxonomy": taxonomy, "taxonomy_code": code, "taxonomy_name": name})
    return {symbol: tuple(rows) for symbol, rows in result.items()}


def _ladder_by_symbol(value: Mapping[str, Any] | None, as_of: date) -> dict[str, dict[str, Any]]:
    latest: tuple[date, Mapping[str, Any]] | None = None
    for row in _records(value):
        day = _date(row.get("date"))
        if day is None or day > as_of or (latest is not None and day <= latest[0]):
            continue
        latest = (day, row)
    result: dict[str, dict[str, Any]] = {}
    if latest is None:
        return result
    boards = latest[1].get("boards")
    if not isinstance(boards, Mapping):
        return result
    for name, entries in boards.items():
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes, bytearray)):
            continue
        for item in entries:
            if not isinstance(item, Mapping) or not (symbol := _symbol(item)):
                continue
            height = int(_number(item.get("board_num")) or _board_height(name) or 1)
            result[symbol] = {**dict(item), "board_num": height, "trade_date": latest[0].isoformat()}
    return result


def _event_symbols(value: Mapping[str, Any] | None) -> set[str]:
    return {symbol for row in _records(value) if (symbol := _symbol(row))}


def _records(value: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Mapping):
        return ()
    direct = value.get("records")
    if isinstance(direct, Sequence) and not isinstance(direct, (str, bytes, bytearray)):
        return tuple(item for item in direct if isinstance(item, Mapping))
    payload = value.get("payload")
    if isinstance(payload, Mapping):
        nested = payload.get("records")
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
            return tuple(item for item in nested if isinstance(item, Mapping))
    return ()


def _dataset_observation(value: Mapping[str, Any] | None) -> tuple[bool, str, str]:
    """Return whether absence from a dataset is an observed fact.

    An empty, successfully collected full-market event set is valid evidence
    that a stock had no event.  A missing/malformed/failed source is not.
    """

    if not isinstance(value, Mapping):
        return False, "NOT_CONFIGURED", "A2_TIER_SOURCE_NOT_CONFIGURED"
    if value.get("available") is False:
        return False, str(value.get("availability_state") or "SOURCE_FAILED"), str(
            value.get("reason_code") or "A2_TIER_SOURCE_FAILED"
        )
    records = value.get("records")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        return False, "SOURCE_FAILED", "A2_TIER_RECORDS_MALFORMED"
    if any(not isinstance(item, Mapping) for item in records):
        return False, "SOURCE_FAILED", "A2_TIER_RECORDS_MALFORMED"
    return True, "OBSERVED_VALUE", "OK"


def _return_20d(bars: Sequence[Mapping[str, Any]], as_of: date) -> float | None:
    usable: list[tuple[int, float]] = []
    cutoff = int(datetime.combine(as_of, datetime.max.time(), tzinfo=SHANGHAI).timestamp() * 1000)
    for index, row in enumerate(bars):
        day = int(_number(row.get("date_ms") or row.get("timestamp") or row.get("time")) or index)
        close = _number(row.get("close_price") or row.get("close"))
        if close is not None and close > 0 and day <= cutoff:
            usable.append((day, close))
    usable.sort()
    if len(usable) < 21:
        return None
    first, last = usable[-21][1], usable[-1][1]
    return (last / first - 1.0) * 100.0 if first > 0 else None


def _cycle_score(value: Mapping[str, Any] | None, taxonomy_code: str) -> float | None:
    if not isinstance(value, Mapping):
        return None
    metrics = value.get("history_metrics")
    if not isinstance(metrics, Mapping):
        return None
    rows = metrics.get("monthly_rotation_candidates") or metrics.get("persistent_mainline_candidates")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None
    for row in rows:
        if not isinstance(row, Mapping) or str(row.get("industry_thscode") or "").upper() != taxonomy_code:
            continue
        for key in ("rotation_score", "relative_strength_percentile_20d", "persistence_score", "score"):
            if (number := _number(row.get(key))) is not None:
                return min(100.0, max(0.0, number))
    return None


def _factor(
    score: float | None,
    *,
    source: str,
    availability_state: str,
    reason_code: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "available": score is not None,
        "availability_state": availability_state,
        "reason_code": reason_code,
        "score": round(score, 4) if score is not None else None,
        "source": source,
    }
    if extra:
        result.update(extra)
    return result


def _weighted(values: Sequence[tuple[float, float]]) -> float | None:
    total = sum(weight for _value, weight in values)
    return sum(value * weight for value, weight in values) / total if total > 0 else None


def _percentiles(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted((float(value), symbol) for symbol, value in values.items() if math.isfinite(float(value)))
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][1]: 50.0}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        average_rank = (index + end - 1) / 2.0
        score = average_rank / (len(ordered) - 1) * 100.0
        for _value, symbol in ordered[index:end]:
            result[symbol] = round(score, 4)
        index = end
    return result


def _symbol(value: Mapping[str, Any] | Any) -> str:
    if isinstance(value, Mapping):
        raw = value.get("symbol") or value.get("thscode") or value.get("ts_code") or value.get("code")
    else:
        raw = value
    text = str(raw or "").strip().upper()
    if len(text) == 6 and text.isdigit():
        suffix = "SH" if text.startswith(("5", "6", "9")) else "BJ" if text.startswith(("4", "8")) else "SZ"
        return f"{text}.{suffix}"
    return text if len(text) == 9 and text[6] == "." else ""


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _date(value: Any) -> date | None:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _board_height(name: Any) -> int | None:
    text = str(name or "").lower()
    words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7}
    return next((height for word, height in words.items() if word in text), None)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A2 feature as_of must be timezone-aware")
    return value.astimezone(SHANGHAI)


def _hash(value: Mapping[str, Any]) -> str:
    body = dict(value)
    body.pop("content_hash", None)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = ["A2_FEATURE_SCHEMA", "build_a2_feature_snapshot"]
