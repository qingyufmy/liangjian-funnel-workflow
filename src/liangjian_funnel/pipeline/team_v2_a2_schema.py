"""Claude A2 3.0.1 response schema, separate from engine eligibility.

Caller supplies the frozen domains; no domain is inferred from model claims.
An invalid response is returned intact to the existing degradation policy.
This module performs no scoring, data access, plan publication or repair.
"""
from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from .team_v2_contract import TeamValidation, validate_team_output

_POLICIES = {"IGNITION": "PROBE_ONLY", "CONFIRMATION": "ALLOW", "ACCELERATION": "ALLOW",
             "CLIMAX": "WATCH_ONLY", "DIVERGENCE": "WATCH_ONLY", "RETREAT": "NO_NEW_ENTRY",
             "REPAIR": "ALLOW", "FADE": "NO_NEW_ENTRY"}
_ROOT = frozenset(("envelope", "reason_codes", "market_read", "theme_reviews", "focus_decisions",
                   "rotation_reviews", "reject_decisions", "model_nominations", "handoff_notes",
                   "what_would_change_my_mind", "unresolved_questions"))
_EVIDENCE = frozenset(("supporting_refs", "opposing_refs"))
_OPTIONAL = frozenset(("opposing_search_scope",))
_FIELDS = {
    "market_read": frozenset(("regime", "dominant_themes", "emotion_cycle")) | _EVIDENCE,
    "theme_reviews": frozenset(("theme_id", "stage", "new_entry_policy", "penalty_codes",
        "support_codes", "risk_codes", "what_would_change_my_mind")) | _EVIDENCE,
    "focus_decisions": frozenset(("symbol", "support_codes", "risk_codes")) | _EVIDENCE,
    "rotation_reviews": frozenset(("rotation_direction_id", "decision", "reviewed_symbols",
        "reason_codes", "explanation")) | _EVIDENCE,
    "reject_decisions": frozenset(("symbol", "risk_codes")) | _EVIDENCE,
    "model_nominations": frozenset(("symbol", "theme_id", "nomination_thesis", "quant_filter_reason",
        "why_i_disagree", "falsifiers")) | _EVIDENCE,
}


def _strings(value: Any, maximum: int, minimum: int = 0) -> bool:
    return (isinstance(value, list) and minimum <= len(value) <= maximum
            and all(isinstance(item, str) and item.strip() for item in value)
            and len(value) == len(set(value)))


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _same_json(left: Any, right: Any) -> bool:
    try:
        return json.dumps(left, sort_keys=True, allow_nan=False, ensure_ascii=False) == json.dumps(
            right, sort_keys=True, allow_nan=False, ensure_ascii=False)
    except (TypeError, ValueError):
        return False


def validate_a2_response(output: Any, catalog: Any, *, required_envelope: Mapping[str, Any],
                         candidate_ids: set[str], nomination_scope: set[str],
                         symbol_themes: Mapping[str, str], allowed_theme_ids: set[str],
                         direction_symbols: Mapping[str, set[str]]) -> TeamValidation:
    issues: list[str] = []
    if not isinstance(output, Mapping):
        return TeamValidation(False, ("A2_RESPONSE_OBJECT_REQUIRED",), (), 0)
    if set(output) != _ROOT:
        issues.append("A2_RESPONSE_ROOT_FIELDS_INVALID")
    if not _same_json(output.get("envelope"), required_envelope):
        issues.append("A2_ENVELOPE_BINDING_MISMATCH")
    if not _strings(output.get("reason_codes"), 4):
        issues.append("A2_REASON_CODES_INVALID")
    if not isinstance(output.get("unresolved_questions"), list) or any(
            not _nonempty(item) for item in output["unresolved_questions"]):
        issues.append("A2_UNRESOLVED_QUESTIONS_INVALID")
    notes = output.get("handoff_notes")
    if not isinstance(notes, Mapping) or set(notes) != {"to_a3"}:
        issues.append("A2_HANDOFF_RECIPIENT_INVALID")

    # Structural checks precede domain operations so malformed JSON never throws.
    pools: dict[str, list[Mapping[str, Any]]] = {}
    for name, fields in _FIELDS.items():
        rows = [output.get(name)] if name == "market_read" else output.get(name)
        if not isinstance(rows, list):
            issues.append("A2_POOL_INVALID:" + name); pools[name] = []; continue
        pools[name] = []
        for row in rows:
            if (not isinstance(row, Mapping) or not fields.issubset(row)
                    or not set(row).issubset(fields | _OPTIONAL)):
                issues.append("A2_ITEM_FIELDS_INVALID:" + name); continue
            pools[name].append(row)

    for row in pools["market_read"]:
        if row["emotion_cycle"] not in tuple(_POLICIES) or not _strings(row["dominant_themes"], 3):
            issues.append("A2_MARKET_READ_INVALID")
    focused: list[str] = []; rejected: list[str] = []; nominated: list[str] = []
    for name, ids in (("focus_decisions", focused), ("reject_decisions", rejected),
                      ("model_nominations", nominated)):
        for row in pools[name]:
            symbol = row["symbol"]
            domain = nomination_scope - candidate_ids if name == "model_nominations" else candidate_ids
            if not isinstance(symbol, str) or symbol not in domain:
                issues.append("A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:" + name)
            else:
                ids.append(symbol)
            if name == "focus_decisions":
                if not _strings(row["support_codes"], 2) or not _strings(row["risk_codes"], 2):
                    issues.append("A2_FOCUS_CODES_INVALID")
            elif name == "reject_decisions":
                if not _strings(row["risk_codes"], 3, 1): issues.append("A2_REJECT_CODES_INVALID")
            elif (not all(_nonempty(row[key]) for key in ("theme_id", "nomination_thesis",
                      "quant_filter_reason", "why_i_disagree")) or not _strings(row["falsifiers"], 2, 1)):
                issues.append("A2_NOMINATION_FIELDS_INVALID")
    all_ids = focused + rejected + nominated
    if len(all_ids) != len(set(all_ids)):
        issues.append("A2_DUPLICATE_SYMBOL_DECISION")

    theme_ids: list[str] = []
    for row in pools["theme_reviews"]:
        theme = row["theme_id"]
        if not isinstance(theme, str) or theme not in allowed_theme_ids:
            issues.append("A2_THEME_OUTSIDE_FROZEN_DOMAIN")
        else:
            theme_ids.append(theme)
        stage = row["stage"]
        if not isinstance(stage, str) or _POLICIES.get(stage) != row["new_entry_policy"]:
            issues.append("A2_STAGE_POLICY_MISMATCH")
        if (not _strings(row["penalty_codes"], 3) or not _strings(row["support_codes"], 2)
                or not _strings(row["risk_codes"], 2)
                or not _strings(row["what_would_change_my_mind"], 2, 1)):
            issues.append("A2_THEME_FIELDS_INVALID")
    focus_themes = {symbol_themes[symbol] for symbol in focused if symbol in symbol_themes}
    if (any(symbol not in symbol_themes for symbol in focused)
            or len(theme_ids) != len(set(theme_ids)) or set(theme_ids) != focus_themes):
        issues.append("A2_FOCUS_THEME_COVERAGE_MISMATCH")

    expected = {direction for direction, symbols in direction_symbols.items()
                if not set(focused).intersection(symbols)}
    reviewed: list[str] = []
    for row in pools["rotation_reviews"]:
        direction = row["rotation_direction_id"]
        if not isinstance(direction, str) or direction not in expected:
            issues.append("A2_ROTATION_DOMAIN_MISMATCH"); continue
        reviewed.append(direction)
        symbols = row["reviewed_symbols"]
        if (row["decision"] != "NO_FOCUS" or not _strings(symbols, 3, 1)
                or not set(symbols).issubset(direction_symbols[direction])
                or not _strings(row["reason_codes"], 4) or not _nonempty(row["explanation"])):
            issues.append("A2_ROTATION_REVIEW_INVALID")
    if len(reviewed) != len(set(reviewed)) or set(reviewed) != expected:
        issues.append("A2_ROTATION_COVERAGE_MISMATCH")
    common = validate_team_output("A2", output, catalog, nomination_scope=nomination_scope)
    issues.extend(common.issues)
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)
