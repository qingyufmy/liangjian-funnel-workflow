"""The single, versioned contract for the A1 macro/industry discovery call.

The A1 discovery call is deliberately smaller than the A1 company-review
call.  The server owns the deterministic monthly industry ranking and
decision; a model may only add the semantic theme/industry mapping and the
evidence-backed structural themes/chain nodes.  Keeping that boundary in a
small module prevents the prompt, runtime envelope and validators from
drifting apart again.

This module is dependency-free by design.  It can therefore be used while
building a request, validating a model response, and reading old audit files
without importing the orchestration layer.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


A1_CONTRACT_VERSION = "a1-discovery-contract/3.1.0"
A1_MONTHLY_DECISION_COUNT = 20
A1_THEME_TARGET: tuple[int, int] = (8, 12)
A1_NODE_TARGET: tuple[int, int] = (40, 80)
A1_MINIMUM_READY_POLICY_DOCUMENTS = 1

# The model is not asked to copy the server-owned rows.  ``monthly_industry_decisions``
# remains an output field only for old result readers and for the
# server-side merged result produced by :func:`merge_a1_discovery_output`.
A1_DISCOVERY_MODEL_FIELDS: tuple[str, ...] = (
    "envelope",
    "analysis_summary",
    "macro_regime",
    "policy_dossiers",
    "policy_calendar",
    "structural_themes",
    "industry_chain_graph",
    "taxonomy_links",
    "industry_theme_mappings",
    "source_health",
    "unresolved_questions",
)
A1_DISCOVERY_SERVER_FIELDS: tuple[str, ...] = (
    "monthly_industry_decisions",
    "monthly_rotation_coverage",
    "canonical_monthly_decisions",
)
A1_DISCOVERY_LEGACY_FIELDS: tuple[str, ...] = ("monthly_industry_decisions",)

_DECISIONS = frozenset({"INCLUDE", "EXCLUDE", "DEFER"})
_MAPPING_STATUSES = frozenset({"MAPPED", "UNMAPPED", "PARTIAL"})


def canonical_json(value: Any) -> str:
    """Return the stable JSON representation used by contract hashes."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def stable_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CanonicalMonthlyIndustryDecision:
    """One server-owned monthly industry decision.

    ``decision`` is kept as the canonical public name because it is used by
    the existing deterministic selector.  ``base_decision`` and
    ``final_decision`` are additive aliases in the serialized view and make
    the authority boundary explicit for downstream consumers.
    """

    rank: int
    industry_thscode: str
    industry_name: str | None = None
    decision: str = "DEFER"
    reason_codes: tuple[str, ...] = ()
    supporting_source_refs: tuple[str, ...] = ()
    contradicting_source_refs: tuple[str, ...] = ()
    data_gaps: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)
    mapped_theme_ids: tuple[str, ...] = ()
    mapping_status: str = "UNMAPPED"
    mapping_source: str = "SERVER"
    final_decision: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], *, rank_default: int = 0) -> "CanonicalMonthlyIndustryDecision":
        rank = _positive_int(raw.get("rank"), rank_default)
        code = _code(raw.get("industry_thscode"))
        decision = _decision(raw.get("base_decision") or raw.get("decision"))
        return cls(
            rank=rank,
            industry_thscode=code,
            industry_name=_optional_text(raw.get("industry_name")),
            decision=decision,
            reason_codes=_text_tuple(raw.get("base_reason_codes") or raw.get("reason_codes")),
            supporting_source_refs=_text_tuple(raw.get("base_source_refs") or raw.get("supporting_source_refs")),
            contradicting_source_refs=_text_tuple(raw.get("contradicting_source_refs")),
            data_gaps=_text_tuple(raw.get("data_gaps")),
            metrics=dict(raw.get("metrics")) if isinstance(raw.get("metrics"), Mapping) else {},
            mapped_theme_ids=_text_tuple(raw.get("mapped_theme_ids")),
            mapping_status=_mapping_status(raw.get("mapping_status")),
            mapping_source=_optional_text(raw.get("mapping_source")) or "SERVER",
            final_decision=_decision_or_none(raw.get("final_decision")) or decision,
        )

    def as_dict(self) -> dict[str, Any]:
        decision = _decision(self.decision)
        final = _decision_or_none(self.final_decision) or decision
        return {
            "rank": self.rank,
            "industry_thscode": self.industry_thscode,
            "industry_name": self.industry_name,
            "decision": decision,
            "base_decision": decision,
            "mapped_theme_ids": list(self.mapped_theme_ids),
            "mapping_status": self.mapping_status,
            "mapping_source": self.mapping_source,
            "final_decision": final,
            "reason_codes": list(self.reason_codes),
            "base_reason_codes": list(self.reason_codes),
            "supporting_source_refs": list(self.supporting_source_refs),
            "base_source_refs": list(self.supporting_source_refs),
            "contradicting_source_refs": list(self.contradicting_source_refs),
            "data_gaps": list(self.data_gaps),
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True, slots=True)
class IndustryThemeMapping:
    """Model-owned semantic mapping for one canonical industry row."""

    industry_thscode: str
    mapped_theme_ids: tuple[str, ...] = ()
    mapping_status: str = "UNMAPPED"
    supporting_source_refs: tuple[str, ...] = ()
    contradicting_source_refs: tuple[str, ...] = ()
    data_gaps: tuple[str, ...] = ()
    confidence: float | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "IndustryThemeMapping":
        confidence = _finite_float(raw.get("confidence"))
        if confidence is not None:
            confidence = max(0.0, min(1.0, confidence))
        return cls(
            industry_thscode=_code(raw.get("industry_thscode")),
            mapped_theme_ids=_text_tuple(raw.get("mapped_theme_ids")),
            mapping_status=_mapping_status(raw.get("mapping_status")),
            supporting_source_refs=_text_tuple(raw.get("supporting_source_refs")),
            contradicting_source_refs=_text_tuple(raw.get("contradicting_source_refs")),
            data_gaps=_text_tuple(raw.get("data_gaps")),
            confidence=confidence,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "industry_thscode": self.industry_thscode,
            "mapped_theme_ids": list(self.mapped_theme_ids),
            "mapping_status": self.mapping_status,
            "supporting_source_refs": list(self.supporting_source_refs),
            "contradicting_source_refs": list(self.contradicting_source_refs),
            "data_gaps": list(self.data_gaps),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class A1DiscoveryValidation:
    """Machine-readable validation result for a discovery response."""

    valid: bool
    reason_codes: tuple[str, ...] = ()
    missing_industry_codes: tuple[str, ...] = ()
    unknown_industry_codes: tuple[str, ...] = ()
    duplicate_industry_codes: tuple[str, ...] = ()
    theme_count: int = 0
    node_count: int = 0
    mapping_count: int = 0
    expected_mapping_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "reason_codes": list(self.reason_codes),
            "missing_industry_codes": list(self.missing_industry_codes),
            "unknown_industry_codes": list(self.unknown_industry_codes),
            "duplicate_industry_codes": list(self.duplicate_industry_codes),
            "theme_count": self.theme_count,
            "node_count": self.node_count,
            "mapping_count": self.mapping_count,
            "expected_mapping_count": self.expected_mapping_count,
        }


def contract_values(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return one normalized set of A1 contract values.

    The config is optional so callers that only have a frozen snapshot can
    still render exactly the same default contract.  Nested ``policy_research``
    values are supported for the current YAML layout; top-level values are
    accepted for migration and testing.
    """

    root = config if isinstance(config, Mapping) else {}
    a1 = root.get("agent_1") if isinstance(root.get("agent_1"), Mapping) else root
    policy = a1.get("policy_research") if isinstance(a1, Mapping) and isinstance(a1.get("policy_research"), Mapping) else a1

    def pick(name: str, default: Any) -> Any:
        for source in (a1, policy, root):
            if isinstance(source, Mapping) and source.get(name) is not None:
                return source.get(name)
        return default

    theme_target = _target_pair(pick("monthly_theme_target", A1_THEME_TARGET), A1_THEME_TARGET)
    node_target = _target_pair(pick("node_count_target", A1_NODE_TARGET), A1_NODE_TARGET)
    return {
        "contract_version": str(pick("discovery_contract_version", A1_CONTRACT_VERSION)),
        "monthly_rotation_decision_top_n": max(1, int(pick("monthly_rotation_decision_top_n", A1_MONTHLY_DECISION_COUNT))),
        "monthly_theme_target": list(theme_target),
        "node_count_target": list(node_target),
        "minimum_ready_policy_documents": max(0, int(pick("minimum_ready_policy_documents", A1_MINIMUM_READY_POLICY_DOCUMENTS))),
        "strict_pit_required_for_replay": bool(pick("strict_pit_required_for_replay", True)),
        "evidence_aware_coverage": bool(pick("evidence_aware_coverage", True)),
    }


def render_runtime_contract(config: Mapping[str, Any] | None = None) -> str:
    """Render the contract text injected into the A1 discovery request.

    It intentionally contains no ``Top10`` completeness rule.  The only
    completeness rule is the server-owned configured top-N (normally 20).
    """

    values = contract_values(config)
    return (
        "A1_DISCOVERY_CONTRACT\n"
        f"contract_version={values['contract_version']}\n"
        f"monthly_rotation_decision_top_n={values['monthly_rotation_decision_top_n']}\n"
        f"monthly_theme_target={canonical_json(values['monthly_theme_target'])}\n"
        f"node_count_target={canonical_json(values['node_count_target'])}\n"
        "authority=SERVER_OWNS_CANONICAL_MONTHLY_DECISIONS;MODEL_OWNS_THEME_AND_CHAIN_MAPPING\n"
        "model_output=structural_themes,industry_chain_graph,taxonomy_links,industry_theme_mappings\n"
        "model_must_map_every_include_industry_or_declare_UNMAPPED\n"
        "model_must_not_return_or_rewrite_monthly_industry_decisions\n"
        "batch_is_transport_boundary_not_selection_quota\n"
    )


def canonicalize_monthly_decisions(
    decisions: Sequence[Mapping[str, Any]] | None,
    *,
    expected_count: int = A1_MONTHLY_DECISION_COUNT,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Normalize server decisions and report missing/duplicate ranks/codes."""

    limit = max(1, int(expected_count))
    rows: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    seen_ranks: set[int] = set()
    duplicate_codes: list[str] = []
    duplicate_ranks: list[int] = []
    for index, raw in enumerate(decisions or (), start=1):
        if not isinstance(raw, Mapping):
            continue
        row = CanonicalMonthlyIndustryDecision.from_mapping(raw, rank_default=index)
        if row.rank < 1 or row.rank > limit or not row.industry_thscode:
            continue
        if row.industry_thscode in seen_codes:
            duplicate_codes.append(row.industry_thscode)
            continue
        if row.rank in seen_ranks:
            duplicate_ranks.append(row.rank)
            continue
        seen_codes.add(row.industry_thscode)
        seen_ranks.add(row.rank)
        rows.append(row.as_dict())
    rows.sort(key=lambda item: (int(item["rank"]), str(item["industry_thscode"])))
    missing_ranks = [rank for rank in range(1, limit + 1) if rank not in seen_ranks]
    return rows, {
        "contract_version": A1_CONTRACT_VERSION,
        "requested_top_n": limit,
        "observed_count": len(rows),
        "status": "READY" if len(rows) == limit and not duplicate_codes and not duplicate_ranks else "INCOMPLETE",
        "missing_ranks": missing_ranks,
        "duplicate_codes": sorted(set(duplicate_codes)),
        "duplicate_ranks": sorted(set(duplicate_ranks)),
        "decision_counts": {
            decision: sum(str(item.get("decision")) == decision for item in rows)
            for decision in sorted(_DECISIONS)
        },
    }


def required_mapping_codes(decisions: Sequence[Mapping[str, Any]] | None) -> tuple[str, ...]:
    """Return INCLUDE industry codes that the model must map."""

    codes = {
        _code(item.get("industry_thscode"))
        for item in decisions or ()
        if isinstance(item, Mapping)
        and _decision(item.get("base_decision") or item.get("decision")) == "INCLUDE"
        and _code(item.get("industry_thscode"))
    }
    return tuple(sorted(codes))


def validate_discovery_output(
    output: Mapping[str, Any] | None,
    *,
    canonical_decisions: Sequence[Mapping[str, Any]] | None = None,
    theme_target: Sequence[int] = A1_THEME_TARGET,
    node_target: Sequence[int] = A1_NODE_TARGET,
    require_targets: bool = True,
) -> A1DiscoveryValidation:
    """Validate only the A1 discovery response, not company pool fields."""

    value = output if isinstance(output, Mapping) else {}
    reasons: list[str] = []
    themes = value.get("structural_themes")
    nodes = value.get("industry_chain_graph")
    mappings = value.get("industry_theme_mappings")
    theme_rows = themes if isinstance(themes, list) else []
    node_rows = nodes if isinstance(nodes, list) else []
    mapping_rows = mappings if isinstance(mappings, list) else []
    if not isinstance(themes, list) or not theme_rows:
        reasons.append("A1_DISCOVERY_THEMES_MISSING")
    if not isinstance(nodes, list) or not node_rows:
        reasons.append("A1_DISCOVERY_CHAIN_NODES_MISSING")
    theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in theme_rows
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    }
    node_ids = {
        str(item.get("node_id") or "").strip()
        for item in node_rows
        if isinstance(item, Mapping) and str(item.get("node_id") or "").strip()
    }
    if len(theme_ids) != len(theme_rows):
        reasons.append("A1_DISCOVERY_THEME_ID_INVALID")
    if len(node_ids) != len(node_rows):
        reasons.append("A1_DISCOVERY_NODE_ID_INVALID")
    if require_targets:
        minimum_themes, maximum_themes = _target_pair(theme_target, A1_THEME_TARGET)
        minimum_nodes, maximum_nodes = _target_pair(node_target, A1_NODE_TARGET)
        if len(theme_rows) < minimum_themes:
            reasons.append("A1_MONTHLY_THEME_COVERAGE_INSUFFICIENT")
        if len(theme_rows) > maximum_themes:
            reasons.append("A1_MONTHLY_THEME_COVERAGE_EXCEEDED")
        if len(node_rows) < minimum_nodes:
            reasons.append("A1_MONTHLY_CHAIN_COVERAGE_INSUFFICIENT")
        if len(node_rows) > maximum_nodes:
            reasons.append("A1_MONTHLY_CHAIN_COVERAGE_EXCEEDED")
    for node in node_rows:
        if not isinstance(node, Mapping):
            reasons.append("A1_DISCOVERY_NODE_INVALID")
            continue
        linked = node.get("theme_ids")
        if not isinstance(linked, list) or not linked or not theme_ids.intersection(str(item).strip() for item in linked):
            reasons.append("A1_DISCOVERY_NODE_THEME_LINK_INVALID")
    expected_by_code = {
        _code(item.get("industry_thscode")): item
        for item in canonical_decisions or ()
        if isinstance(item, Mapping) and _code(item.get("industry_thscode"))
    }
    required_codes = set(required_mapping_codes(canonical_decisions))
    observed_codes: set[str] = set()
    duplicate_codes: set[str] = set()
    unknown_codes: set[str] = set()
    if canonical_decisions is not None and not isinstance(mappings, list):
        reasons.append("A1_INDUSTRY_THEME_MAPPINGS_MISSING")
    for raw in mapping_rows:
        if not isinstance(raw, Mapping):
            reasons.append("A1_INDUSTRY_THEME_MAPPING_INVALID")
            continue
        mapping = IndustryThemeMapping.from_mapping(raw)
        code = mapping.industry_thscode
        if not code or code in observed_codes:
            if code:
                duplicate_codes.add(code)
            reasons.append("A1_INDUSTRY_THEME_MAPPING_DUPLICATE")
            continue
        observed_codes.add(code)
        if expected_by_code and code not in expected_by_code:
            unknown_codes.add(code)
            reasons.append("A1_INDUSTRY_THEME_MAPPING_UNKNOWN_INDUSTRY")
        raw_status = str(raw.get("mapping_status") or "UNMAPPED").strip().upper()
        if raw_status not in _MAPPING_STATUSES:
            reasons.append("A1_INDUSTRY_THEME_MAPPING_STATUS_INVALID")
        if mapping.mapping_status == "MAPPED" and not mapping.mapped_theme_ids:
            reasons.append("A1_INDUSTRY_THEME_MAPPING_THEME_MISSING")
        if mapping.mapping_status == "MAPPED" and not set(mapping.mapped_theme_ids).intersection(theme_ids):
            reasons.append("A1_INDUSTRY_THEME_MAPPING_THEME_UNKNOWN")
        if mapping.mapping_status == "MAPPED" and not mapping.supporting_source_refs:
            reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_MISSING")
        if mapping.confidence is not None and not 0.0 <= mapping.confidence <= 1.0:
            reasons.append("A1_INDUSTRY_THEME_MAPPING_CONFIDENCE_INVALID")
    missing_codes = sorted(required_codes.difference(observed_codes))
    if missing_codes:
        reasons.append("A1_INDUSTRY_THEME_MAPPING_INCOMPLETE")
    return A1DiscoveryValidation(
        valid=not reasons,
        reason_codes=tuple(dict.fromkeys(reasons)),
        missing_industry_codes=tuple(missing_codes),
        unknown_industry_codes=tuple(sorted(unknown_codes)),
        duplicate_industry_codes=tuple(sorted(duplicate_codes)),
        theme_count=len(theme_rows),
        node_count=len(node_rows),
        mapping_count=len(mapping_rows),
        expected_mapping_count=len(required_codes),
    )


def merge_a1_discovery_output(
    output: Mapping[str, Any],
    canonical_decisions: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Join model mappings onto immutable server-owned monthly decisions.

    Unknown, duplicate or malformed model mappings are ignored in the join;
    the validation result remains available in ``a1_contract.validation``.
    An EXCLUDE/DEFER decision can have no mapping.  No model field can change
    the server's decision or rank.
    """

    merged = dict(output) if isinstance(output, Mapping) else {}
    canonical, coverage = canonicalize_monthly_decisions(canonical_decisions)
    expected = {str(item["industry_thscode"]): item for item in canonical}
    themes = {
        str(item.get("theme_id") or "").strip()
        for item in merged.get("structural_themes", ())
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    }
    mappings: dict[str, dict[str, Any]] = {}
    for raw in merged.get("industry_theme_mappings", ()):
        if not isinstance(raw, Mapping):
            continue
        item = IndustryThemeMapping.from_mapping(raw).as_dict()
        code = item["industry_thscode"]
        if code not in expected or code in mappings:
            continue
        mapped = [theme for theme in item["mapped_theme_ids"] if theme in themes]
        item["mapped_theme_ids"] = mapped
        if mapped:
            item["mapping_status"] = "MAPPED"
        elif item["mapping_status"] == "MAPPED":
            item["mapping_status"] = "UNMAPPED"
        mappings[code] = item

    rows: list[dict[str, Any]] = []
    missing_include: list[str] = []
    for raw in canonical:
        code = str(raw["industry_thscode"])
        row = dict(raw)
        item = mappings.get(code)
        if item:
            row["mapped_theme_ids"] = list(item["mapped_theme_ids"])
            row["mapping_status"] = item["mapping_status"]
            row["mapping_source"] = "MODEL"
            row["mapping_source_refs"] = list(item.get("supporting_source_refs", ()))
            row["mapping_confidence"] = item.get("confidence")
            row["contradicting_source_refs"] = list(dict.fromkeys([
                *row.get("contradicting_source_refs", []),
                *item.get("contradicting_source_refs", []),
            ]))
            row["data_gaps"] = list(dict.fromkeys([
                *row.get("data_gaps", []),
                *item.get("data_gaps", []),
            ]))
        else:
            row["mapped_theme_ids"] = []
            row["mapping_status"] = "UNMAPPED"
            row["mapping_source"] = "SERVER"
            if row.get("decision") == "INCLUDE":
                missing_include.append(code)
        if row.get("decision") == "INCLUDE" and row.get("mapping_status") != "MAPPED" and code not in missing_include:
            # An explicit UNMAPPED response is auditable, but it remains a
            # degraded mapping gap until the industry can be linked.
            missing_include.append(code)
        row["final_decision"] = row.get("decision")
        rows.append(row)

    merged["monthly_industry_decisions"] = rows
    merged["canonical_monthly_decisions"] = rows
    merged["monthly_rotation_coverage"] = {
        **coverage,
        "mapped_count": sum(bool(item.get("mapped_theme_ids")) for item in rows),
        "missing_include_mapping_codes": sorted(missing_include),
        "mapping_status": "READY" if not missing_include else "DEGRADED_MAPPING_GAP",
    }
    merged["a1_contract"] = {
        "contract_version": A1_CONTRACT_VERSION,
        "authority": "SERVER_CANONICAL_MONTHLY_DECISIONS",
        "model_output_fields": list(A1_DISCOVERY_MODEL_FIELDS),
        "validation": validate_discovery_output(
            output,
            canonical_decisions=canonical,
            require_targets=False,
        ).as_dict(),
    }
    return merged


def migrate_legacy_discovery_output(
    output: Mapping[str, Any],
    *,
    expected_decisions: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read an old model result without treating it as a new-contract result.

    This explicit helper is intentionally not called by the new validator.
    It is for reports/checkpoint readers that need to display historical
    ``monthly_industry_decisions`` while the new runtime uses server-owned
    canonical rows.
    """

    migrated = dict(output) if isinstance(output, Mapping) else {}
    old_rows = output.get("monthly_industry_decisions") if isinstance(output, Mapping) else None
    if isinstance(old_rows, list):
        migrated["legacy_monthly_industry_decisions"] = [dict(item) for item in old_rows if isinstance(item, Mapping)]
    if expected_decisions is not None:
        migrated = merge_a1_discovery_output(
            {key: value for key, value in migrated.items() if key != "monthly_industry_decisions"},
            expected_decisions,
        )
    migrated["a1_contract"] = {
        "contract_version": A1_CONTRACT_VERSION,
        "migration": "LEGACY_MODEL_MONTHLY_DECISIONS_READ_ONLY",
    }
    return migrated


def _target_pair(value: Sequence[int] | Any, default: tuple[int, int]) -> tuple[int, int]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and len(value) >= 2:
        try:
            low, high = int(value[0]), int(value[1])
        except (TypeError, ValueError):
            return default
        return (min(low, high), max(low, high))
    return default


def _positive_int(value: Any, default: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = int(default)
    return result


def _code(value: Any) -> str:
    return str(value or "").strip().upper()


def _decision(value: Any) -> str:
    normalized = str(value or "DEFER").strip().upper()
    return normalized if normalized in _DECISIONS else "DEFER"


def _decision_or_none(value: Any) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in _DECISIONS else None


def _mapping_status(value: Any) -> str:
    normalized = str(value or "UNMAPPED").strip().upper()
    return normalized if normalized in _MAPPING_STATUSES else "UNMAPPED"


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _text_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = list(value)
    else:
        values = []
    return tuple(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


__all__ = [
    "A1_CONTRACT_VERSION",
    "A1_DISCOVERY_LEGACY_FIELDS",
    "A1_DISCOVERY_MODEL_FIELDS",
    "A1_DISCOVERY_SERVER_FIELDS",
    "A1_MINIMUM_READY_POLICY_DOCUMENTS",
    "A1_MONTHLY_DECISION_COUNT",
    "A1_NODE_TARGET",
    "A1_THEME_TARGET",
    "A1DiscoveryValidation",
    "CanonicalMonthlyIndustryDecision",
    "IndustryThemeMapping",
    "canonical_json",
    "canonicalize_monthly_decisions",
    "contract_values",
    "merge_a1_discovery_output",
    "migrate_legacy_discovery_output",
    "render_runtime_contract",
    "required_mapping_codes",
    "stable_digest",
    "validate_discovery_output",
]
