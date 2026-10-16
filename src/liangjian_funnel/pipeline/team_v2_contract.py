"""Team V2 evidence contract. Pure validation: never repairs or grants execution."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

NO_OPPOSING = "NO_OPPOSING_EVIDENCE_FOUND"
_BLIND_KEYS = frozenset({"eligibility", "met_conditions", "unmet_conditions", "review_status",
                         "technical_score", "score_breakdown", "engine_verdict", "trigger_result"})
_POOLS = {
    "A1": ("structural_themes", "active_research_pool", "monitor_pool", "rejected_candidates", "industry_theme_mappings"),
    "A2": ("theme_reviews", "focus_decisions", "reject_decisions", "rotation_reviews", "model_nominations"),
    "A3": ("core_watch_pool", "secondary_watch_pool", "rejected_candidates", "slot_plan",
           "plan_allocations", "overnight_stances"),
    "A4": ("signals", "position_actions"),
    "A5": ("model_value_assessment", "signal_reviews", "missed_opportunity_reviews", "core_defects", "hypotheses"),
    "A6": ("envelope_adjustments", "exception_actions", "overnight_reviews", "risk_event_attribution"),
}
_SINGLE = {"A2": ("market_read",), "A3": ("deployment_target", "capacity_request"),
           "A5": ("a2_review", "a3_review", "a4_review", "a6_review", "trader_vs_deterministic", "risk_governor_value"),
           "A6": ("utilization_cap", "capacity_for_tomorrow")}


@dataclass(frozen=True)
class TeamValidation:
    valid: bool
    issues: tuple[str, ...]
    judgments: tuple[dict[str, Any], ...]
    sentinel_count: int


def _texts(value: Any, limit: int) -> bool:
    return (isinstance(value, list) and 1 <= len(value) <= limit
            and all(isinstance(x, str) and bool(x.strip()) and len(x) <= 2000 for x in value))


def blind_plan_facts(value: Any) -> Any:
    """Remove decision labels, including nested labels, not underlying measurements."""
    if isinstance(value, Mapping):
        return {key: blind_plan_facts(item) for key, item in value.items() if key not in _BLIND_KEYS}
    if isinstance(value, (list, tuple)):
        return [blind_plan_facts(item) for item in value]
    return deepcopy(value)


def validate_team_output(role: str, output: Any, catalog: Any, *, session: str | None = None,
                         expected_ids: set[str] | None = None,
                         nomination_scope: set[str] | None = None) -> TeamValidation:
    issues: list[str] = []; judgments: list[dict[str, Any]] = []
    role = role.upper() if isinstance(role, str) else ""
    if role not in _POOLS or not isinstance(output, Mapping):
        return TeamValidation(False, ("TEAM_ROLE_OR_OUTPUT_INVALID",), (), 0)
    if session is not None and output.get("session") != session:
        issues.append("TEAM_SESSION_MISMATCH")
    notes = output.get("handoff_notes")
    if role == "A1" and isinstance(notes, list):
        if not _texts(notes, 3): issues.append("TEAM_HANDOFF_NOTES_INVALID")
    elif (not isinstance(notes, Mapping) or not notes
            or not any(_texts(value, 3) for value in notes.values())
            or any(not isinstance(key, str) or not isinstance(value, list) or len(value) > 3
                   or (value and not _texts(value, 3)) for key, value in notes.items())):
        issues.append("TEAM_HANDOFF_NOTES_INVALID")
    if not _texts(output.get("what_would_change_my_mind"), 2):
        issues.append("TEAM_FALSIFIERS_INVALID")
    categories: set[str] = set(); refs: set[str] = set()
    if not isinstance(catalog, Mapping) or not isinstance(catalog.get("categories"), list) or not isinstance(catalog.get("items"), list):
        issues.append("TEAM_CATALOG_INVALID")
    else:
        raw_categories = catalog["categories"]
        if any(not isinstance(x, str) or not x.strip() for x in raw_categories) or len(set(map(str, raw_categories))) != len(raw_categories):
            issues.append("TEAM_CATALOG_INVALID")
        else:
            categories = set(raw_categories)
        for entry in catalog["items"]:
            if (not isinstance(entry, Mapping) or not isinstance(entry.get("id"), str)
                    or not entry["id"] or entry["id"] in refs
                    or not isinstance(entry.get("category"), str) or entry.get("category") not in categories):
                issues.append("TEAM_CATALOG_INVALID")
            else:
                refs.add(entry["id"])
    for pool in _POOLS[role]:
        if pool not in output:
            continue  # Session-specific required roots are checked by the session schema.
        rows = output[pool]
        if not isinstance(rows, list):
            issues.append("TEAM_DECISION_POOL_INVALID:" + pool); continue
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                issues.append("TEAM_JUDGMENT_INVALID:" + pool); continue
            judgments.append({**deepcopy(dict(row)), "path": f"{pool}[{index}]"})
    for name in _SINGLE.get(role, ()):
        if name in output:
            row = output[name]
            if not isinstance(row, Mapping):
                issues.append("TEAM_JUDGMENT_INVALID:" + name)
            else:
                judgments.append({**deepcopy(dict(row)), "path": name})
    if role == "A5" and isinstance(output.get("team_scorecard"), Mapping):
        attribution = output["team_scorecard"].get("attribution", [])
        if not isinstance(attribution,list):
            issues.append("TEAM_ATTRIBUTION_INVALID"); attribution=[]
        for row in attribution:
            if not isinstance(row, Mapping):
                issues.append("TEAM_ATTRIBUTION_INVALID")
            else:
                judgments.append({**deepcopy(dict(row)), "path": "team_scorecard.attribution"})
    sentinel_count = 0
    for row in judgments:
        aliases = (("supporting_source_refs", "contradicting_source_refs")
                   if role=="A1" and row["path"].startswith("industry_theme_mappings[")
                   else ("evidence_ids", "opposing_refs") if role=="A5"
                   else ("supporting_refs", "opposing_refs"))
        supporting = row.get("supporting_refs",row.get(aliases[0]))
        opposing = row.get("opposing_refs",row.get(aliases[1]))
        if ((aliases[0] in row and "supporting_refs" in row and row[aliases[0]] != supporting)
                or (aliases[1] in row and "opposing_refs" in row and row[aliases[1]] != opposing)):
            issues.append("TEAM_REF_ALIAS_CONFLICT:"+row["path"])
        if not _valid_refs(supporting, refs):
            issues.append("TEAM_SUPPORTING_REFS_INVALID:" + row["path"])
        if opposing == [NO_OPPOSING]:
            scope = row.get("opposing_search_scope")
            if (not isinstance(scope, list) or len(scope) < 2
                    or any(not isinstance(x, str) for x in scope)
                    or len(set(map(str, scope))) != len(scope)
                    or not set(map(str, scope)).issubset(categories)):
                issues.append("TEAM_OPPOSING_SEARCH_SCOPE_INVALID:" + row["path"])
            else:
                sentinel_count += 1
        elif not _valid_refs(opposing, refs):
            issues.append("TEAM_OPPOSING_REFS_INVALID:" + row["path"])
        if opposing != [NO_OPPOSING] and "opposing_search_scope" in row:
            issues.append("TEAM_UNEXPECTED_SEARCH_SCOPE:"+row["path"])
    if role == "A3" and output.get("session") == "PLAN_REVIEW" and expected_ids is not None:
        ids = [row.get("symbol") for row in judgments if row["path"].split("[", 1)[0] in
               {"core_watch_pool", "secondary_watch_pool", "rejected_candidates"}]
        if len(ids) != len(set(map(str, ids))) or set(map(str, ids)) != expected_ids:
            issues.append("TEAM_CANDIDATE_DOMAIN_MISMATCH")
    if role == "A4" and expected_ids is not None:
        ids = [row.get("plan_id") for row in judgments if row["path"].startswith("signals[")]
        if len(ids) != len(set(map(str, ids))) or set(map(str, ids)) != expected_ids:
            issues.append("TEAM_PLAN_DOMAIN_MISMATCH")
    if role == "A2":
        nominations = output.get("model_nominations", [])
        if (not isinstance(nominations, list) or len(nominations) > 5
                or any(not isinstance(row, Mapping) or not isinstance(row.get("symbol"), str)
                       or row["symbol"] not in (nomination_scope or set()) for row in nominations)):
            issues.append("TEAM_NOMINATION_DOMAIN_INVALID")
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), tuple(judgments), sentinel_count)


def _valid_refs(value: Any, refs: set[str]) -> bool:
    return (isinstance(value, list) and 1 <= len(value) <= 3
            and all(isinstance(x, str) and x in refs and x != NO_OPPOSING for x in value)
            and len(set(value)) == len(value))
