"""Explicit research condition coverage; protective/data gates are never relaxed."""

_TREND_SEQUENCE = (
    "TREND_PULLBACK_VOLUME_CONTRACTION", "TREND_PRIOR_5M_REVERSAL",
    "TREND_SUBSEQUENT_5M_CONFIRMATION", "TREND_VWAP_RECLAIMED",
)
_PROFILES = {
    "TREND_MA5": (
        "TREND_15M_PRESSURE_EASING", *_TREND_SEQUENCE,
        "TREND_5M_REVERSAL_CONFIRMATION", "TREND_VOLUME_NOT_OVERHEATED",
        "A3_PULLBACK_ZONE", "A4_LIVE_NO_CHASE_ACCEPTED", "LIVE_MARKET_ENTRY_PERMISSION",
    ),
    "MA520_SWING": (
        "DAILY_MA5_ABOVE_MA20", "DAILY_CLOSE_ABOVE_MA20", "MA520_15M_STABLE",
        "MA520_5M_HIGHER_LOW", "MA520_5M_VWAP_RECLAIM",
        "MA520_TWO_CLOSED_5M_CONFIRMATIONS", "MA520_VOLUME_NOT_OVERHEATED",
        "A3_RIGHT_SIDE_CONFIRMATION", "A3_TRIGGER_ZONE", "A4_LIVE_NO_CHASE_ACCEPTED",
        "LIVE_MARKET_ENTRY_PERMISSION",
    ),
    "LEADER_INTRADAY": (
        "LEADER_15M_NOT_WEAK", "LEADER_5M_TRIGGER", "A3_TRIGGER_ZONE",
        "A4_LIVE_NO_CHASE_ACCEPTED", "LIVE_MARKET_ENTRY_PERMISSION",
        "LEADER_CONTEXT_CURRENT", "LEADER_CONTEXT_STRENGTH",
        "LEADER_BOARD_CONFIRMATION", "LEADER_BOARD_RISK",
        "LEADER_L1_DIVERGENCE_TO_STRENGTH", "LEADER_L2_RESEAL", "LEADER_L3_BREAKOUT",
    ),
}
_UNSUPPORTED = {
    "A3_RIGHT_SIDE_CONFIRMATION": "Frozen A3 right-side evidence cannot be manufactured or bypassed.",
    "LEADER_CONTEXT_CURRENT": "Missing/expired frozen leader evidence cannot be bypassed.",
    "LEADER_CONTEXT_STRENGTH": "This path includes ladder/theme invalidation; preserve protective exits.",
    "LEADER_BOARD_CONFIRMATION": "First-board route eligibility is outside the WP1 entry-timing experiment.",
    "LEADER_BOARD_RISK": "Climax risk veto is preserved.",
    "LEADER_L1_DIVERGENCE_TO_STRENGTH": "Production evaluator has no independent L1 route.",
    "LEADER_L2_RESEAL": "Production evaluator has no independent L2 route.",
    "LEADER_L3_BREAKOUT": "Production evaluator has no independent L3 route.",
}
_PROTECTIVE = (
    "HARD_STOP", "SELLABLE_POSITION", "EXECUTABLE_NOT_LOCKED_LIMIT_UP",
    "LIVE_MARKET_STATE_READY", "PLAN_DATA_READY", "A4_LIVE_ENTRY_GEOMETRY_READY",
    "A4_LIVE_STOP_DISTANCE_ACCEPTED", "A4_LIVE_REWARD_RISK_ACCEPTED",
)

ATTRIBUTION_VERSION = "ATOMIC_UNMET/2"
_GEOMETRY_CHILDREN = {
    "A4_LIVE_NO_CHASE_EXCEEDED": "A4_LIVE_NO_CHASE_ACCEPTED",
    "A4_LIVE_STOP_DISTANCE_TOO_WIDE": "A4_LIVE_STOP_DISTANCE_ACCEPTED",
    "A4_LIVE_REWARD_RISK_BELOW_MINIMUM": "A4_LIVE_REWARD_RISK_ACCEPTED",
}


def atomic_failures(decision: dict, *, versioned_trend=False) -> dict:
    """Expand computed AND-parent failures; never infer a missing child PASS.

    Geometry reasons are emitted together by the real helper after all three
    comparisons. Unknown parents remain visible and cannot claim unique kill.
    Legacy trend's composite predicate is deliberately retained on that route.
    """
    failed = list(dict.fromkeys(decision.get("unmet_conditions") or ()))
    reasons = set(decision.get("reason_codes") or ())
    suppressed, unresolved, evidence = [], [], {}
    parent = "A4_LIVE_ENTRY_GEOMETRY_ACCEPTED"
    children = [child for reason, child in _GEOMETRY_CHILDREN.items() if reason in reasons]
    if parent in failed:
        if children:
            failed.remove(parent)
            failed.extend(children)
            suppressed.append(parent)
            evidence[parent] = {reason:child for reason, child in _GEOMETRY_CHILDREN.items() if reason in reasons}
        else:
            unresolved.append(parent)
    if versioned_trend and "TREND_5M_REVERSAL_CONFIRMATION" in failed:
        parent = "TREND_5M_REVERSAL_CONFIRMATION"
        components = (*_TREND_SEQUENCE, "TREND_POST_CONFIRM_LOW_AND_ZONE_HELD")
        if set(failed) & set(components):
            failed.remove(parent)
            suppressed.append(parent)
            evidence[parent] = [name for name in components if name in failed]
        else:
            unresolved.append(parent)
    return {"version":ATTRIBUTION_VERSION, "failed_conditions":list(dict.fromkeys(failed)),
            "suppressed_parents":suppressed, "unresolved_parents":unresolved,
            "expansion_evidence":evidence}


def condition_catalog(profile: str) -> list[dict]:
    """Return IDs from the real evaluator, including unsupported coverage.

    ``derived`` is diagnostic: exclude it from unique-killer denominators.
    Legacy trend plans have the composite reversal gate; trend-ma5/2 also
    exposes its four atomic sequence gates, and the engine reports applicability.
    """
    profile = str(profile).strip().upper()
    result = []
    for condition in _PROFILES.get(profile, ()):
        derived = condition == "TREND_5M_REVERSAL_CONFIRMATION"
        reason = _UNSUPPORTED.get(condition, "Exact isolated production predicate substitution; all other gates retained.")
        if condition in _TREND_SEQUENCE:
            reason = "Only applicable to frozen trend-ma5/2 plans with at least four closed 5m bars."
        if derived:
            reason = "Legacy route only; on trend-ma5/2 this aggregate is diagnostic and cannot bypass four atomic gates."
        composite = condition == "LEADER_5M_TRIGGER"
        result.append({"id": condition, "supported": condition not in _UNSUPPORTED,
                       "reason": reason, "derived": derived,
                       "components": list(_TREND_SEQUENCE) if derived else
                       ["LEADER_5M_DIVERGENCE_TO_STRENGTH", "LEADER_5M_RESEAL", "LEADER_FIRST_15M_BREAKOUT"] if composite else [],
                       "kind": "derived" if derived else "composite_or" if composite else "atomic"})
    result.extend({"id": name, "supported": False, "reason": "Protective/data gate; never ablated.",
                   "derived": False, "components": [], "kind": "protected"} for name in _PROTECTIVE)
    result.append({"id":"A4_LIVE_ENTRY_GEOMETRY_ACCEPTED", "supported":False,
                   "reason":"AND diagnostic parent; attribute only actual computed geometry children.",
                   "derived":True, "components":list(_GEOMETRY_CHILDREN.values()), "kind":"derived"})
    if profile == "TREND_MA5":
        result.extend({"id":name, "supported":False, "derived":False, "components":[],
                       "kind":"research_variant", "reason":"Explicit variant only, not a removable production condition."}
                      for name in ("TREND_PULLBACK_LEN_K", "TREND_CONFIRM_WITHIN_N"))
        result.append({"id":"TREND_POST_CONFIRM_LOW_AND_ZONE_HELD", "supported":False,
                       "derived":False, "components":[], "kind":"protected",
                       "reason":"Within-N research protection; never removed."})
    return result
