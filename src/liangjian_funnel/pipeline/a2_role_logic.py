"""Auditable A2 stock-behaviour and route classification.

This module deliberately contains no ranking score and no provider calls.  It
turns point-in-time evidence into one of three mutually exclusive behaviour
types:

``EMOTION``
    A theme/ladder leader or a reliably observed first-board candidate.  The
    only downstream route is the intraday leader strategy; A3 decides whether
    first-board evidence is sufficiently confirmed for execution.
``TREND``
    A medium-term trend/core stock.  The downstream routes are the daily
    5-day-line trend strategy and the 5/20 swing strategy.
``UNRESOLVED``
    The facts are insufficient or contradictory.  It has no A4 route.

The result is intentionally verbose.  Missing facts are represented as data
gaps and are never converted into a negative observation or a zero score.
Callers can persist the returned object alongside the A2 snapshot to make the
decision reproducible for a particular ``as_of`` timestamp.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any


A2_ROLE_LOGIC_VERSION = "a2-role-logic/1.1.0"

EMOTION = "EMOTION"
TREND = "TREND"
UNRESOLVED = "UNRESOLVED"

EMOTION_MARKET_ROLE = "EMOTION_LEADER"
TREND_MARKET_ROLE = "TREND_CORE"
UNRESOLVED_MARKET_ROLE = "UNRESOLVED"

LEADER_INTRADAY = "LEADER_INTRADAY"
TREND_MA5 = "TREND_MA5"
MA520_SWING = "MA520_SWING"

CORE_EVIDENCE_DIMENSIONS = (
    "supply_chain_position",
    "capital_flow",
    "ladder_structure",
    "crowding",
    "index_chain_resonance",
    "identifiability_liquidity",
)

TREND_REQUIRED_FACETS = (
    "medium_term_trend",
    "relative_strength",
    "industry_logic",
)

_ALIASES: dict[str, tuple[str, ...]] = {
    "supply_chain_position": (
        "supply_chain_position",
        "supply_chain",
        "chain_position",
        "供应链位置",
        "产业链位置",
    ),
    "capital_flow": (
        "capital_flow",
        "capital_flow_support",
        "fund_flow",
        "资金流",
        "资金流向",
    ),
    "ladder_structure": (
        "ladder_structure",
        "tier_structure",
        "ladder",
        "theme_ladder",
        "梯队结构",
        "连板结构",
    ),
    "crowding": ("crowding", "crowdedness", "拥挤度"),
    "index_chain_resonance": (
        "index_chain_resonance",
        "industry_resonance",
        "theme_resonance",
        "指数产业链共振",
        "指数/产业链共振",
    ),
    "identifiability_liquidity": (
        "identifiability_liquidity",
        "identity_liquidity",
        "liquidity",
        "辨识度流动性",
        "辨识度/流动性",
    ),
    "medium_term_trend": (
        "medium_term_trend",
        "trend",
        "trend_structure",
        "weekly_daily_trend",
        "中期趋势",
        "中期趋势结构",
    ),
    "relative_strength": (
        "relative_strength",
        "relative_strength_vs_index",
        "rs",
        "相对强度",
        "相对强弱",
    ),
    "industry_logic": (
        "industry_logic",
        "industry_chain_logic",
        "theme_logic",
        "产业逻辑",
        "行业逻辑",
    ),
}

_EMOTION_ROLES = {
    "EMOTION",
    "EMOTION_LEADER",
    "LEADER",
    "情绪龙头",
    "题材龙头",
    "龙头",
}


def classify_a2_stock(
    *,
    symbol: str,
    name: str | None = None,
    evidence: Mapping[str, Any] | None = None,
    as_of: str | date | datetime | None = None,
    trend_candidate: bool = False,
) -> dict[str, Any]:
    """Classify one A2 candidate using explicit, point-in-time evidence.

    ``evidence`` accepts the canonical English dimension names, the Chinese
    labels used by the workflow, and a small set of backwards-compatible
    aliases.  A dimension value should be an object such as::

        {
            "available": True,
            "met": True,
            "value": {"board_num": 3, "theme": "贵金属"},
            "source_refs": ["hithink:limit-up:2026-08-29"],
            "as_of": "2026-08-29T15:00:00+08:00",
            "reason": "three-board theme leader",
        }

    ``available=False`` is a data gap.  ``available=True, met=False`` is a
    known negative and is kept distinct.  No numeric value is ever turned into
    a ranking score by this function.
    """

    raw = evidence if isinstance(evidence, Mapping) else {}
    normalized = {
        dimension: _normalize_evidence(
            _first_value(raw, _ALIASES[dimension]),
            default_as_of=as_of,
            dimension=dimension,
        )
        for dimension in CORE_EVIDENCE_DIMENSIONS
    }
    facets = _build_facets(raw, normalized, default_as_of=as_of)

    emotion_anchor = _emotion_anchor(normalized["ladder_structure"])
    first_board_observed = _first_board_observed(normalized["ladder_structure"])
    emotion_negative = normalized["ladder_structure"]["available"] is True and normalized["ladder_structure"]["met"] is False
    emotion_qualified = emotion_anchor and not emotion_negative

    # Trend facets are route-specific.  Once a clear emotional ladder anchor
    # exists, absent trend facets are not a defect in the emotion decision and
    # should not make the UI report the stock as globally data-incomplete.
    # They remain present in ``required_facets`` for auditability.
    data_gaps = _collect_data_gaps(normalized, facets, include_facets=not emotion_anchor)
    known_negatives = _collect_known_negatives(normalized, facets, include_facets=not emotion_anchor)

    trend_states = [facets[key] for key in TREND_REQUIRED_FACETS]
    trend_qualified = all(
        state["available"] is True and state["met"] is True
        for state in trend_states
    )
    # A2 first establishes a broad, auditable candidate universe and lets the
    # model make the quality decision.  For a monthly A1 member already
    # joined to a positive-flow TOP5 board, a single weak relative-strength
    # or industry-resonance facet must not turn the row into an invisible
    # UNRESOLVED row.  The medium-term trend remains a required anchor;
    # missing/negative medium-term evidence is not bypassed.  This opt-in
    # flag is intentionally restricted to the deterministic TOP5 route and
    # does not broaden the independent emotion contract.
    partial_trend_qualified = bool(trend_candidate) and _partial_trend_candidate(trend_states)

    conflicts: list[str] = []

    hinted_type = _hinted_behavior_type(raw)
    derived_type: str
    if emotion_qualified:
        # A ladder/leader observation describes the short-horizon trading
        # behaviour even when the same company also has a healthy medium-term
        # trend.  The shorter-horizon emotion contract takes precedence so the
        # stock cannot be routed simultaneously to leader and trend playbooks;
        # A3 retains the stronger confirmation gate for first-board rows.
        derived_type = EMOTION
    elif trend_qualified or partial_trend_qualified:
        derived_type = TREND
    else:
        derived_type = UNRESOLVED

    if hinted_type and hinted_type != derived_type:
        conflicts.append("DECLARED_BEHAVIOR_TYPE_CONFLICT")
        derived_type = UNRESOLVED

    reason_codes = _reason_codes(
        behavior_type=derived_type,
        emotion_anchor=emotion_anchor,
        first_board_observed=first_board_observed,
        trend_states=trend_states,
        conflicts=conflicts,
        data_gaps=data_gaps,
        known_negatives=known_negatives,
    )

    if derived_type == EMOTION:
        market_role = EMOTION_MARKET_ROLE
        route_permission = [LEADER_INTRADAY]
    elif derived_type == TREND:
        market_role = _trend_market_role(raw)
        route_permission = [TREND_MA5, MA520_SWING]
    else:
        market_role = UNRESOLVED_MARKET_ROLE
        route_permission = []

    return {
        "schema": A2_ROLE_LOGIC_VERSION,
        "symbol": str(symbol or "").strip(),
        "name": name,
        "stock_behavior_type": derived_type,
        "market_role": market_role,
        "route_permission": route_permission,
        "reason_codes": reason_codes,
        "data_gaps": data_gaps,
        "known_negatives": known_negatives,
        "conflicts": conflicts,
        "evidence": normalized,
        "required_facets": facets,
        "decision_basis": {
            "emotion_anchor": emotion_anchor,
            "first_board_observed": first_board_observed,
            "emotion_qualified": emotion_qualified,
            "trend_qualified": trend_qualified,
            "partial_trend_qualified": partial_trend_qualified,
            "trend_candidate_requested": bool(trend_candidate),
            "emotion_precedence_applied": emotion_qualified and trend_qualified,
            "hinted_behavior_type": hinted_type,
            "scoring_used": False,
        },
    }


def classify_stock_behavior(
    *,
    symbol: str,
    name: str | None = None,
    evidence: Mapping[str, Any] | None = None,
    as_of: str | date | datetime | None = None,
    trend_candidate: bool = False,
) -> dict[str, Any]:
    """Readable alias for callers that do not use the A2 prefix."""

    return classify_a2_stock(
        symbol=symbol,
        name=name,
        evidence=evidence,
        as_of=as_of,
        trend_candidate=trend_candidate,
    )


def _first_value(mapping: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    for key in aliases:
        if key in mapping:
            return mapping[key]
    return None


def _normalize_evidence(
    raw: Any,
    *,
    default_as_of: str | date | datetime | None,
    dimension: str,
) -> dict[str, Any]:
    """Normalize one fact without inferring a positive/negative score."""

    if raw is None:
        return {
            "available": False,
            "met": None,
            "value": None,
            "source_refs": [],
            "as_of": default_as_of,
            "reason": "DATA_GAP",
            "status": "DATA_GAP",
            "dimension": dimension,
        }

    if isinstance(raw, Mapping):
        available = raw.get("available")
        met = raw.get("met")
        value = raw.get("value")
        if value is None and "observed_value" in raw:
            value = raw.get("observed_value")
        # Explicit booleans are useful in small deterministic adapters.  They
        # are accepted only as a boolean fact; arbitrary numeric values do not
        # become evidence by themselves.
        if met is None and isinstance(value, bool):
            met = value
        if available is None:
            available = isinstance(met, bool)
        available = bool(available)
        if not available:
            met = None
            status = "DATA_GAP"
            reason = str(raw.get("reason") or "DATA_GAP")
        elif not isinstance(met, bool):
            available = False
            met = None
            status = "DATA_GAP"
            reason = str(raw.get("reason") or "EVIDENCE_MET_UNSPECIFIED")
        else:
            status = "OBSERVED_VALUE" if met else "KNOWN_NEGATIVE"
            reason = str(raw.get("reason") or ("OBSERVED_MET" if met else "OBSERVED_NOT_MET"))
        return {
            "available": available,
            "met": met,
            "value": value,
            "source_refs": _source_refs(raw.get("source_refs") or raw.get("sources")),
            "as_of": raw.get("as_of", default_as_of),
            "reason": reason,
            "status": status,
            "dimension": dimension,
        }

    if isinstance(raw, bool):
        return {
            "available": True,
            "met": raw,
            "value": raw,
            "source_refs": [],
            "as_of": default_as_of,
            "reason": "EXPLICIT_BOOLEAN_FACT",
            "status": "OBSERVED_VALUE" if raw else "KNOWN_NEGATIVE",
            "dimension": dimension,
        }

    # A scalar number/string is not silently interpreted as a pass.  Keeping
    # it in ``value`` makes the malformed input visible to the UI/audit log.
    return {
        "available": False,
        "met": None,
        "value": raw,
        "source_refs": [],
        "as_of": default_as_of,
        "reason": "EVIDENCE_MET_UNSPECIFIED",
        "status": "DATA_GAP",
        "dimension": dimension,
    }


def _source_refs(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        return [raw.decode() if isinstance(raw, bytes) else raw]
    if isinstance(raw, Sequence):
        return [item for item in raw if item not in (None, "")]
    return [raw]


def _build_facets(
    raw: Mapping[str, Any],
    normalized: Mapping[str, Mapping[str, Any]],
    *,
    default_as_of: str | date | datetime | None,
) -> dict[str, dict[str, Any]]:
    """Build trend facets from explicit facts or structured resonance data."""

    resonance_value = normalized["index_chain_resonance"].get("value")
    resonance = resonance_value if isinstance(resonance_value, Mapping) else {}
    supply_value = normalized["supply_chain_position"].get("value")
    supply = supply_value if isinstance(supply_value, Mapping) else {}

    facets: dict[str, dict[str, Any]] = {}
    for facet in TREND_REQUIRED_FACETS:
        explicit = _first_value(raw, _ALIASES[facet])
        nested = None
        if facet == "medium_term_trend":
            nested = _first_nested(resonance, ("medium_term_trend", "trend", "trend_state", "weekly_trend", "daily_trend"))
        elif facet == "relative_strength":
            nested = _first_nested(resonance, ("relative_strength", "relative_strength_vs_index", "rs"))
        elif facet == "industry_logic":
            nested = _first_nested(resonance, ("industry_logic", "industry_chain_logic", "theme_logic"))
            if nested is None:
                nested = _first_nested(supply, ("industry_logic", "industry_chain_logic", "theme_logic"))
        candidate = explicit if explicit is not None else nested
        if candidate is None and facet == "industry_logic":
            # A positive, explicit supply-chain-position fact is itself an
            # industry-logic observation.  No score is inferred.
            candidate = normalized["supply_chain_position"]
        facets[facet] = _normalize_evidence(
            candidate,
            default_as_of=default_as_of,
            dimension=facet,
        )
    return facets


def _first_nested(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _partial_trend_candidate(states: Sequence[Mapping[str, Any]]) -> bool:
    """Return whether the trend evidence is broad enough for A2 review.

    ``TREND`` here means *candidate for model review*, not a final quality
    verdict.  The medium-term trend is a required anchor and must be
    available and positive.  Relative strength and industry logic are
    complementary facts: one may be unavailable or a known negative while
    the other still supports a review candidate.  If the medium-term anchor
    is missing/negative, or both supporting facets are absent/negative, the
    deterministic layer has no objective trend evidence to hand to the model.
    """

    if len(states) < 3:
        return False
    medium, relative, industry = states[:3]
    # The medium-term anchor is a critical fact and cannot be bypassed by a
    # strong board or by the LLM broad-review contract.
    if medium.get("available") is not True or medium.get("met") is not True:
        return False
    supporting = (relative, industry)
    return any(
        state.get("available") is True and state.get("met") is True
        for state in supporting
    )


def _emotion_anchor(ladder: Mapping[str, Any]) -> bool:
    if ladder.get("available") is not True or ladder.get("met") is not True:
        return False
    value = ladder.get("value")
    if value is None or isinstance(value, bool):
        # ``met=True`` with no qualifier is an explicit adapter-level ladder
        # confirmation.  Detailed values below provide stronger audit text.
        return True
    if isinstance(value, str):
        token = value.strip().upper()
        return token in _EMOTION_ROLES or any(word in value for word in ("龙头", "连板", "情绪"))
    if not isinstance(value, Mapping):
        return False
    for key in ("emotion_leader", "is_emotion_leader", "theme_leader", "is_leader", "leader"):
        if value.get(key) is True:
            return True
    for key in ("market_role", "role", "leader_role", "leader_type"):
        token = str(value.get(key) or "").strip().upper()
        if token in _EMOTION_ROLES:
            return True
        if any(word in str(value.get(key) or "") for word in ("龙头", "连板", "情绪")):
            return True
    for key in ("ladder_height", "board_num", "continuous_boards", "board_count", "连板数", "梯队高度"):
        number = _number(value.get(key))
        # A reliable first-board observation is enough to enter the emotion /
        # leader candidate route.  A3 still owns the confirmation gate and
        # must keep first-board rows observation-only.  Requiring a positive
        # board number prevents arbitrary ``met=True`` mappings from becoming
        # emotion leaders.
        if number is not None and number >= 1:
            return True
    return False


def _first_board_observed(ladder: Mapping[str, Any]) -> bool:
    """Return whether the ladder fact is explicitly bounded to board one."""

    if ladder.get("available") is not True or ladder.get("met") is not True:
        return False
    value = ladder.get("value")
    if not isinstance(value, Mapping):
        return False
    if value.get("first_board_observed") is True:
        return True
    source = str(value.get("event_source") or value.get("source") or "").strip().upper()
    if source == "HITHINK_LIMIT_UP_POOL":
        return True
    for key in ("ladder_height", "board_num", "continuous_boards", "board_count", "连板数", "梯队高度"):
        number = _number(value.get(key))
        if number is not None and number == 1:
            return True
    return False


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _collect_data_gaps(
    evidence: Mapping[str, Mapping[str, Any]],
    facets: Mapping[str, Mapping[str, Any]],
    *,
    include_facets: bool = True,
) -> list[str]:
    gaps: list[str] = []
    items = tuple(evidence.items()) + (tuple(facets.items()) if include_facets else ())
    for key, item in items:
        if item.get("available") is not True or not isinstance(item.get("met"), bool):
            gaps.append(f"A2_DATA_GAP_{key.upper()}")
    return gaps


def _collect_known_negatives(
    evidence: Mapping[str, Mapping[str, Any]],
    facets: Mapping[str, Mapping[str, Any]],
    *,
    include_facets: bool = True,
) -> list[str]:
    negatives: list[str] = []
    items = tuple(evidence.items()) + (tuple(facets.items()) if include_facets else ())
    for key, item in items:
        if item.get("available") is True and item.get("met") is False:
            negatives.append(f"A2_KNOWN_NEGATIVE_{key.upper()}")
    return negatives


def _hinted_behavior_type(raw: Mapping[str, Any]) -> str | None:
    value = _first_value(raw, ("stock_behavior_type", "behavior_type", "类型", "行为类型"))
    if value is None:
        return None
    token = str(value).strip().upper()
    if token in {EMOTION, "情绪", "EMOTION_LEADER"}:
        return EMOTION
    if token in {TREND, "趋势", "TREND_CORE", "TREND_LEADER"}:
        return TREND
    if token in {UNRESOLVED, "UNKNOWN", "待确认", "未确定"}:
        return UNRESOLVED
    return token


def _trend_market_role(raw: Mapping[str, Any]) -> str:
    value = _first_value(raw, ("market_role", "leader_role", "role", "市场角色"))
    token = str(value or "").strip().upper()
    if token in {"TREND_LEADER", "INSTITUTIONAL_CORE", "CORE_ARMY", "TREND_CORE"}:
        return token
    return TREND_MARKET_ROLE


def _reason_codes(
    *,
    behavior_type: str,
    emotion_anchor: bool,
    first_board_observed: bool = False,
    trend_states: Sequence[Mapping[str, Any]],
    conflicts: Sequence[str],
    data_gaps: Sequence[str],
    known_negatives: Sequence[str],
) -> list[str]:
    reasons: list[str] = []
    if behavior_type == EMOTION:
        reasons.append(
            "A2_EMOTION_FIRST_BOARD_OBSERVED"
            if first_board_observed
            else "A2_EMOTION_LADDER_LEADER_CONFIRMED"
        )
        if all(item.get("available") is True and item.get("met") is True for item in trend_states):
            reasons.append("A2_EMOTION_PRECEDENCE_OVER_TREND")
        if not any(item.get("available") is True and item.get("met") is True for item in trend_states):
            reasons.append("A2_TREND_FACTS_NOT_REQUIRED_FOR_EMOTION_ROUTE")
    elif behavior_type == TREND:
        facet_codes = (
            ("A2_MEDIUM_TERM_TREND_CONFIRMED", "A2_WEAK_MEDIUM_TERM_TREND"),
            ("A2_RELATIVE_STRENGTH_CONFIRMED", "A2_WEAK_RELATIVE_STRENGTH"),
            ("A2_INDUSTRY_LOGIC_CONFIRMED", "A2_WEAK_INDUSTRY_LOGIC"),
        )
        for state, (confirmed_code, weak_code) in zip(trend_states, facet_codes):
            if state.get("available") is True and state.get("met") is True:
                reasons.append(confirmed_code)
            else:
                # Preserve the weak/data-gap fact alongside the positive
                # trend candidate.  The LLM decides whether this is focus,
                # watch or rejected; it is not silently discarded locally.
                reasons.append(weak_code)
        if not all(
            state.get("available") is True and state.get("met") is True
            for state in trend_states
        ):
            reasons.append("A2_TREND_PARTIAL_CONFIRMATION")
    else:
        if conflicts:
            reasons.append("A2_ROLE_EVIDENCE_CONFLICT")
        if data_gaps:
            reasons.append("A2_ROLE_EVIDENCE_INSUFFICIENT")
        if known_negatives and not conflicts:
            reasons.append("A2_ROLE_KNOWN_NEGATIVE")
        if not reasons:
            reasons.append("A2_ROLE_EVIDENCE_INSUFFICIENT")
    if emotion_anchor and behavior_type != EMOTION and not conflicts:
        reasons.append("A2_EMOTION_ANCHOR_NOT_ROUTEABLE")
    reasons.extend(conflicts)
    return _unique(reasons)


def _unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


__all__ = [
    "A2_ROLE_LOGIC_VERSION",
    "CORE_EVIDENCE_DIMENSIONS",
    "TREND_REQUIRED_FACETS",
    "classify_a2_stock",
    "classify_stock_behavior",
]
