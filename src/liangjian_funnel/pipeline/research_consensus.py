"""Safe, point-in-time loading of hand-reviewed institutional strategy views.

The files consumed here are deliberately a small, non-trading evidence plane.
They are local inputs written and reviewed by an operator; this module never
fetches a URL and never turns a view into a stock recommendation.  The loader
projects every accepted document onto an explicit allow-list before exposing it
to the rest of the workflow.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


SCHEMA_VERSION = "institution-strategy-consensus/1.0.0"
# Keep the initial internal name readable for hand-authored fixtures while
# normalizing every accepted document to the public contract above.
_SUPPORTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION, "research-consensus/1.0.0"})
SOURCE_TYPE = "PUBLIC_INSTITUTION_STRATEGY_AGGREGATION"
PRIVATE_ATTACHMENT_SOURCE_TYPE = "PRIVATE_RESEARCH_ATTACHMENT"
EVIDENCE_TIER = "T2"

MAX_DOCUMENTS = 12
MAX_INSTITUTIONS = 64
MAX_AXES = 32
MAX_DISAGREEMENTS = 32
MAX_VERIFICATION_ITEMS = 32
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024

MAX_DOCUMENT_ID_LENGTH = 128
MAX_TITLE_LENGTH = 512
MAX_PUBLISHER_LENGTH = 256
MAX_SOURCE_URL_LENGTH = 2048
MAX_TEXT_LENGTH = 4096
MAX_SHORT_TEXT_LENGTH = 512
MAX_LIST_ITEMS = 64
MAX_INDUSTRY_TILTS = 32
MAX_POSITIONS = 16

_REQUIRED_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "document_id",
        "title",
        "publisher",
        "publish_time",
        "effective_month",
        "institutions",
        "consensus_axes",
        "disagreements",
        "verification_plan",
    }
)
_ALLOWED_DOCUMENT_FIELDS = _REQUIRED_DOCUMENT_FIELDS | {
    "source_type",
    "source_url",
    "source_ref",
    "source_label",
}
_INSTITUTION_FIELDS = frozenset(
    {
        "institution_id",
        "institution_name",
        "market_view",
        "style_tilts",
        "industry_tilts",
        "conditions",
        "risks",
    }
)
_INDUSTRY_TILT_FIELDS = frozenset({"theme", "stance", "conditions"})
_AXIS_FIELDS = frozenset(
    {"axis", "stance", "confidence", "supporting_institutions", "conditions", "counter_signals"}
)
_DISAGREEMENT_FIELDS = frozenset({"topic", "positions", "resolution_evidence"})
_POSITION_FIELDS = frozenset({"stance", "institutions"})
_VERIFICATION_FIELDS = frozenset({"question", "required_facts", "decision_rule"})

# These key fragments are intentionally broader than the exact input schema.
# A manually added ``stock_list`` or ``recommendation`` field must not survive
# projection merely because it is nested under an otherwise valid object.
_FORBIDDEN_KEY_FRAGMENTS = (
    "symbol",
    "stock",
    "recommend",
    "ticker",
    "security_code",
    "securitycode",
    "股票",
    "个股",
    "推荐",
    "标的",
    "证券代码",
)
_CODE_EXEMPT_KEYS = frozenset(
    {
        "schema_version",
        "document_id",
        "source_url",
        "source_ref",
        "source_label",
        "publish_time",
        "effective_month",
        "source_type",
        "institution_id",
        "supporting_institutions",
        "institutions",
    }
)
# Bare six-digit A-share codes are supported by a number of upstream formats;
# exchange-qualified forms are included too.  This check is only applied to
# research-content values, never to document IDs or URLs (which may contain a
# long article number such as 202608303859443558).
_A_SHARE_CODE_RE = re.compile(
    r"(?<!\d)(?:"
    r"\d{6}(?:[._-](?:SH|SZ|BJ))|"
    r"(?:SH|SZ|BJ)(?:SE)?[._-]?\d{6}|"
    r"\d{6}"
    r")(?!\d)",
    re.IGNORECASE,
)
_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


class ResearchConsensusValidationError(ValueError):
    """Internal validation error whose code is safe to expose to callers."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class _ValidatedDocument:
    payload: dict[str, Any]
    path: Path
    publish_time: datetime
    effective_month: str


def load_research_consensus(
    directory: Path | str | Any,
    as_of: datetime,
) -> dict[str, Any]:
    """Load eligible strategy-consensus JSON files from a local directory.

    ``directory`` may also be a Settings-like object carrying
    ``research_consensus_dir``.  The latter keeps the small public function
    convenient for callers that already have a settings object while avoiding
    an import cycle with :mod:`liangjian_funnel.settings`.

    A naive ``as_of`` is rejected rather than silently assuming a timezone.
    File-level errors are isolated and reported as safe reason codes; an
    unexpected directory/read failure is allowed to propagate to the workflow
    boundary, which converts it to an unavailable contract.
    """

    if hasattr(directory, "research_consensus_dir"):
        directory = getattr(directory, "research_consensus_dir")
    return ResearchConsensusLoader(Path(directory)).load(as_of=as_of)


# A descriptive alias for callers that prefer the returned contract's name.
load_research_consensus_bundle = load_research_consensus


def unavailable_research_consensus(
    *,
    as_of: datetime,
    source_dir: Path | str | None = None,
    reason_code: str = "RESEARCH_CONSENSUS_LOAD_FAILED",
) -> dict[str, Any]:
    """Return the fail-closed contract used when workflow loading fails."""

    cutoff = _aware_as_of(as_of)
    return _contract(
        cutoff=cutoff,
        available=False,
        reason_code=reason_code,
        documents=(),
        invalid_documents=(),
    )


def project_a2_research_hypotheses(bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Project T2 strategy views into a compact, non-scoring A2 input.

    A2 receives only theme hypotheses, conditions, counter-signals and
    verification questions.  It never receives authority to change the
    deterministic factor snapshot, create an out-of-A1 symbol, or treat a
    viewpoint as a fact.  Keeping this projection small also avoids repeating
    the full monthly research packet in every A2 model batch.
    """

    documents_raw = bundle.get("documents")
    documents = documents_raw if isinstance(documents_raw, list) else []
    projected_documents: list[dict[str, Any]] = []
    for document in documents[:MAX_DOCUMENTS]:
        if not isinstance(document, Mapping):
            continue
        hypotheses: list[dict[str, Any]] = []
        institutions = document.get("institutions")
        if isinstance(institutions, list):
            for institution in institutions:
                if not isinstance(institution, Mapping):
                    continue
                tilts = institution.get("industry_tilts")
                if not isinstance(tilts, list):
                    continue
                for tilt in tilts:
                    if not isinstance(tilt, Mapping) or len(hypotheses) >= MAX_INDUSTRY_TILTS:
                        continue
                    hypotheses.append(
                        {
                            "theme": tilt.get("theme"),
                            "stance": tilt.get("stance"),
                            "conditions": list(tilt.get("conditions") or ())[:MAX_LIST_ITEMS],
                        }
                    )
        projected_documents.append(
            {
                "document_id": document.get("document_id"),
                "publish_time": document.get("publish_time"),
                "effective_month": document.get("effective_month"),
                "source_type": document.get("source_type"),
                "source_ref": document.get("source_ref") or document.get("source_url"),
                "theme_hypotheses": hypotheses,
                "consensus_axes": list(document.get("consensus_axes") or ())[:MAX_AXES],
                "disagreements": list(document.get("disagreements") or ())[:MAX_DISAGREEMENTS],
                "verification_plan": list(document.get("verification_plan") or ())[:MAX_VERIFICATION_ITEMS],
            }
        )
    payload = {
        "schema_version": "a2-research-hypotheses/1.0.0",
        "available": bundle.get("available") is True and bool(projected_documents),
        "reason_code": bundle.get("reason_code"),
        "as_of": bundle.get("as_of"),
        "evidence_tier": EVIDENCE_TIER,
        "viewpoint_only": True,
        "untrusted_text": True,
        "deterministic_score_influence_allowed": False,
        "fact_substitution_allowed": False,
        "out_of_a1_selection_allowed": False,
        "documents": projected_documents,
    }
    payload["content_hash"] = _canonical_hash(payload)
    return payload


@dataclass(frozen=True, slots=True)
class ResearchConsensusLoader:
    """Read and validate the operator-maintained consensus directory."""

    directory: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "directory", Path(self.directory).expanduser().resolve())

    def safe_summary(self) -> dict[str, str]:
        """Expose only the configured path, never loaded document contents."""

        return {"research_consensus_dir": str(self.directory)}

    def load(self, as_of: datetime) -> dict[str, Any]:
        cutoff = _aware_as_of(as_of)
        window_start, window_end = _effective_month_window(cutoff)
        if not self.directory.is_dir():
            return _contract(
                cutoff=cutoff,
                available=False,
                reason_code="RESEARCH_CONSENSUS_DIRECTORY_MISSING",
                documents=(),
                invalid_documents=(),
                window_start=window_start,
                window_end=window_end,
            )

        # ``glob`` is deliberately limited to direct ``*.json`` children.  A
        # nested archive is not an active version and should not unexpectedly
        # enter the point-in-time view.
        paths = sorted((path for path in self.directory.glob("*.json") if path.is_file()), key=_path_sort_key)
        valid: list[_ValidatedDocument] = []
        invalid: list[dict[str, str]] = []
        for path in paths:
            try:
                raw = _read_json(path)
                candidate = _validate_document(raw, path)
            except ResearchConsensusValidationError as exc:
                invalid.append(_invalid_ref(path, exc.reason_code))
                continue
            except (OSError, UnicodeError):
                invalid.append(_invalid_ref(path, "DOCUMENT_READ_FAILED"))
                continue

            # A valid, reviewed file outside the requested PIT month window is
            # not a bad document.  Excluding it without incrementing invalid
            # count makes partial-document diagnostics meaningful.
            if candidate.publish_time > cutoff:
                continue
            if not (window_start <= candidate.effective_month <= window_end):
                continue
            valid.append(candidate)

        valid.sort(key=_document_sort_key)
        valid, excluded_document_count = _bound_documents(valid)

        documents = tuple(item.payload for item in valid)
        if documents:
            reason_code = "PARTIAL_DOCUMENTS" if invalid or excluded_document_count else "OK"
            available = True
        elif invalid:
            reason_code = "NO_VALID_DOCUMENTS"
            available = False
        else:
            reason_code = "NO_EFFECTIVE_DOCUMENTS"
            available = False
        return _contract(
            cutoff=cutoff,
            available=available,
            reason_code=reason_code,
            documents=documents,
            invalid_documents=tuple(invalid),
            window_start=window_start,
            window_end=window_end,
            excluded_document_count=excluded_document_count,
        )


def _contract(
    *,
    cutoff: datetime,
    available: bool,
    reason_code: str,
    documents: Sequence[Mapping[str, Any]],
    invalid_documents: Sequence[Mapping[str, str]],
    window_start: str | None = None,
    window_end: str | None = None,
    excluded_document_count: int = 0,
) -> dict[str, Any]:
    projected_documents = [dict(document) for document in documents]
    # Only public, reviewable provenance enters the model packet.  Local or VM
    # filesystem paths belong in operational configuration summaries, never
    # in research evidence.
    source_refs = list(dict.fromkeys(
        str(document.get("source_ref") or document.get("source_url"))
        for document in projected_documents
        if isinstance(document.get("source_ref") or document.get("source_url"), str)
    ))
    if window_start is None or window_end is None:
        window_start, window_end = _effective_month_window(cutoff)
    counts = _coverage(projected_documents)
    payload_for_hash = {
        "schema_version": SCHEMA_VERSION,
        "documents": projected_documents,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source_type": SOURCE_TYPE,
        "available": bool(available),
        "reason_code": reason_code,
        "as_of": cutoff.isoformat(),
        "effective_month_window": {"start": window_start, "end": window_end},
        "evidence_tier": EVIDENCE_TIER,
        "primary_evidence": False,
        "viewpoint_only": True,
        "untrusted_text": True,
        "direct_stock_selection_allowed": False,
        "documents": projected_documents,
        "source_refs": source_refs,
        "invalid_document_count": len(invalid_documents),
        "invalid_documents": [dict(item) for item in invalid_documents],
        "excluded_document_count": int(excluded_document_count),
        "coverage": counts,
        "content_hash": _canonical_hash(payload_for_hash),
    }


def _coverage(documents: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    institution_ids: set[str] = set()
    axis_count = 0
    disagreement_count = 0
    verification_count = 0
    effective_months: set[str] = set()
    for document in documents:
        effective_month = document.get("effective_month")
        if isinstance(effective_month, str):
            effective_months.add(effective_month)
        institutions = document.get("institutions")
        if isinstance(institutions, list):
            for institution in institutions:
                if isinstance(institution, Mapping) and isinstance(institution.get("institution_id"), str):
                    institution_ids.add(institution["institution_id"])
        axis_count += _list_len(document.get("consensus_axes"))
        disagreement_count += _list_len(document.get("disagreements"))
        verification_count += _list_len(document.get("verification_plan"))
    return {
        "document_count": len(documents),
        "institution_count": len(institution_ids),
        "axis_count": axis_count,
        "disagreement_count": disagreement_count,
        "verification_item_count": verification_count,
        "effective_months": sorted(effective_months),
    }


def _bound_documents(documents: Sequence[_ValidatedDocument]) -> tuple[list[_ValidatedDocument], int]:
    """Apply aggregate document/list limits without truncating a document."""

    selected: list[_ValidatedDocument] = []
    institution_ids: set[str] = set()
    axis_count = disagreement_count = verification_count = 0
    excluded = 0
    for candidate in documents:
        payload = candidate.payload
        raw_institutions = payload.get("institutions")
        candidate_ids = {
            str(item.get("institution_id"))
            for item in raw_institutions
            if isinstance(item, Mapping) and item.get("institution_id")
        } if isinstance(raw_institutions, list) else set()
        candidate_axes = _list_len(payload.get("consensus_axes"))
        candidate_disagreements = _list_len(payload.get("disagreements"))
        candidate_verification = _list_len(payload.get("verification_plan"))
        exceeds = (
            len(selected) >= MAX_DOCUMENTS
            or len(institution_ids | candidate_ids) > MAX_INSTITUTIONS
            or axis_count + candidate_axes > MAX_AXES
            or disagreement_count + candidate_disagreements > MAX_DISAGREEMENTS
            or verification_count + candidate_verification > MAX_VERIFICATION_ITEMS
        )
        if exceeds:
            excluded += 1
            continue
        selected.append(candidate)
        institution_ids.update(candidate_ids)
        axis_count += candidate_axes
        disagreement_count += candidate_disagreements
        verification_count += candidate_verification
    return selected, excluded


def _validate_document(raw: Any, path: Path) -> _ValidatedDocument:
    if not isinstance(raw, Mapping):
        raise ResearchConsensusValidationError("INVALID_DOCUMENT_SHAPE")
    forbidden = _forbidden_content_reason(raw)
    if forbidden is not None:
        raise ResearchConsensusValidationError(forbidden)
    missing = _REQUIRED_DOCUMENT_FIELDS.difference(raw)
    if missing:
        raise ResearchConsensusValidationError("MISSING_REQUIRED_FIELD")
    if raw.get("schema_version") not in _SUPPORTED_SCHEMA_VERSIONS:
        raise ResearchConsensusValidationError("INVALID_SCHEMA_VERSION")
    source_type = raw.get("source_type") or SOURCE_TYPE
    if source_type not in {SOURCE_TYPE, PRIVATE_ATTACHMENT_SOURCE_TYPE}:
        raise ResearchConsensusValidationError("INVALID_SOURCE_TYPE")

    document_id = _text(raw.get("document_id"), MAX_DOCUMENT_ID_LENGTH, "INVALID_DOCUMENT_ID")
    title = _text(raw.get("title"), MAX_TITLE_LENGTH, "INVALID_TITLE")
    publisher = _text(raw.get("publisher"), MAX_PUBLISHER_LENGTH, "INVALID_PUBLISHER")
    source_url: str | None = None
    source_label: str | None = None
    if source_type == SOURCE_TYPE:
        source_url = _https_url(raw.get("source_url"))
        source_ref = source_url
    else:
        source_ref = _attachment_source_ref(raw.get("source_ref"))
        source_label = _source_label(raw.get("source_label"))
    publish_time = _parse_aware_datetime(raw.get("publish_time"), "INVALID_PUBLISH_TIME")
    effective_month = _effective_month(raw.get("effective_month"))

    institutions_raw = raw.get("institutions")
    if not isinstance(institutions_raw, list) or not institutions_raw or len(institutions_raw) > MAX_INSTITUTIONS:
        raise ResearchConsensusValidationError("INVALID_INSTITUTIONS")
    institutions: list[dict[str, Any]] = []
    institution_ids: set[str] = set()
    for item in institutions_raw:
        institution = _validate_institution(item)
        institution_id = institution["institution_id"]
        if institution_id in institution_ids:
            raise ResearchConsensusValidationError("DUPLICATE_INSTITUTION_ID")
        institution_ids.add(institution_id)
        institutions.append(institution)

    axes_raw = raw.get("consensus_axes")
    if not isinstance(axes_raw, list) or not axes_raw or len(axes_raw) > MAX_AXES:
        raise ResearchConsensusValidationError("INVALID_CONSENSUS_AXES")
    axes = [_validate_axis(item, institution_ids) for item in axes_raw]

    disagreements_raw = raw.get("disagreements")
    if not isinstance(disagreements_raw, list) or len(disagreements_raw) > MAX_DISAGREEMENTS:
        raise ResearchConsensusValidationError("INVALID_DISAGREEMENTS")
    disagreements = [_validate_disagreement(item, institution_ids) for item in disagreements_raw]

    verification_raw = raw.get("verification_plan")
    if not isinstance(verification_raw, list) or not verification_raw or len(verification_raw) > MAX_VERIFICATION_ITEMS:
        raise ResearchConsensusValidationError("INVALID_VERIFICATION_PLAN")
    verification_plan = [_validate_verification(item) for item in verification_raw]

    payload = {
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "title": title,
        "source_ref": source_ref,
        "publisher": publisher,
        "publish_time": publish_time.isoformat(),
        "effective_month": effective_month,
        "source_type": source_type,
        "institutions": institutions,
        "consensus_axes": axes,
        "disagreements": disagreements,
        "verification_plan": verification_plan,
    }
    if source_url is not None:
        payload["source_url"] = source_url
    if source_label is not None:
        payload["source_label"] = source_label
    return _ValidatedDocument(payload, path, publish_time, effective_month)


def _validate_institution(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ResearchConsensusValidationError("INVALID_INSTITUTION")
    institution_id = _text(raw.get("institution_id"), MAX_DOCUMENT_ID_LENGTH, "INVALID_INSTITUTION")
    institution_name = _text(raw.get("institution_name"), MAX_PUBLISHER_LENGTH, "INVALID_INSTITUTION")
    market_view = _text(raw.get("market_view"), MAX_TEXT_LENGTH, "INVALID_INSTITUTION")
    style_tilts = _text_list(raw.get("style_tilts"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_INSTITUTION")
    industry_raw = raw.get("industry_tilts")
    if not isinstance(industry_raw, list) or len(industry_raw) > MAX_INDUSTRY_TILTS:
        raise ResearchConsensusValidationError("INVALID_INSTITUTION")
    industry_tilts: list[dict[str, Any]] = []
    for item in industry_raw:
        if not isinstance(item, Mapping):
            raise ResearchConsensusValidationError("INVALID_INDUSTRY_TILT")
        theme = _text(item.get("theme"), MAX_SHORT_TEXT_LENGTH, "INVALID_INDUSTRY_TILT")
        stance = _text(item.get("stance"), MAX_SHORT_TEXT_LENGTH, "INVALID_INDUSTRY_TILT")
        conditions = _text_list(item.get("conditions"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_INDUSTRY_TILT")
        industry_tilts.append({"theme": theme, "stance": stance, "conditions": conditions})
    conditions = _text_list(raw.get("conditions"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_INSTITUTION")
    risks = _text_list(raw.get("risks"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_INSTITUTION")
    return {
        "institution_id": institution_id,
        "institution_name": institution_name,
        "market_view": market_view,
        "style_tilts": style_tilts,
        "industry_tilts": industry_tilts,
        "conditions": conditions,
        "risks": risks,
    }


def _validate_axis(raw: Any, institution_ids: set[str]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ResearchConsensusValidationError("INVALID_CONSENSUS_AXIS")
    axis = _text(raw.get("axis"), MAX_SHORT_TEXT_LENGTH, "INVALID_CONSENSUS_AXIS")
    stance = _text(raw.get("stance"), MAX_SHORT_TEXT_LENGTH, "INVALID_CONSENSUS_AXIS")
    confidence = _confidence(raw.get("confidence"))
    supporting = _text_list(raw.get("supporting_institutions"), MAX_INSTITUTIONS, MAX_DOCUMENT_ID_LENGTH, "INVALID_CONSENSUS_AXIS")
    if any(item not in institution_ids for item in supporting):
        raise ResearchConsensusValidationError("UNKNOWN_INSTITUTION_REFERENCE")
    conditions = _text_list(raw.get("conditions"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_CONSENSUS_AXIS")
    counter_signals = _text_list(raw.get("counter_signals"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_CONSENSUS_AXIS")
    return {
        "axis": axis,
        "stance": stance,
        "confidence": confidence,
        "supporting_institutions": supporting,
        "conditions": conditions,
        "counter_signals": counter_signals,
    }


def _validate_disagreement(raw: Any, institution_ids: set[str]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ResearchConsensusValidationError("INVALID_DISAGREEMENT")
    topic = _text(raw.get("topic"), MAX_SHORT_TEXT_LENGTH, "INVALID_DISAGREEMENT")
    positions_raw = raw.get("positions")
    if not isinstance(positions_raw, list) or not positions_raw or len(positions_raw) > MAX_POSITIONS:
        raise ResearchConsensusValidationError("INVALID_DISAGREEMENT")
    positions: list[dict[str, Any]] = []
    for item in positions_raw:
        if not isinstance(item, Mapping):
            raise ResearchConsensusValidationError("INVALID_POSITION")
        stance = _text(item.get("stance"), MAX_SHORT_TEXT_LENGTH, "INVALID_POSITION")
        institutions = _text_list(item.get("institutions"), MAX_INSTITUTIONS, MAX_DOCUMENT_ID_LENGTH, "INVALID_POSITION")
        if any(value not in institution_ids for value in institutions):
            raise ResearchConsensusValidationError("UNKNOWN_INSTITUTION_REFERENCE")
        positions.append({"stance": stance, "institutions": institutions})
    evidence = _text_list(raw.get("resolution_evidence"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_DISAGREEMENT")
    return {"topic": topic, "positions": positions, "resolution_evidence": evidence}


def _validate_verification(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ResearchConsensusValidationError("INVALID_VERIFICATION_ITEM")
    question = _text(raw.get("question"), MAX_TEXT_LENGTH, "INVALID_VERIFICATION_ITEM")
    required_facts = _text_list(raw.get("required_facts"), MAX_LIST_ITEMS, MAX_SHORT_TEXT_LENGTH, "INVALID_VERIFICATION_ITEM")
    decision_rule = _text(raw.get("decision_rule"), MAX_TEXT_LENGTH, "INVALID_VERIFICATION_ITEM")
    return {"question": question, "required_facts": required_facts, "decision_rule": decision_rule}


def _read_json(path: Path) -> Any:
    if path.stat().st_size > MAX_DOCUMENT_BYTES:
        raise ResearchConsensusValidationError("DOCUMENT_TOO_LARGE")
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResearchConsensusValidationError("INVALID_JSON") from exc


def _forbidden_content_reason(value: Any) -> str | None:
    """Find forbidden fields/codes without scanning article IDs and URLs."""

    def walk(node: Any, key: str | None = None, *, scan_scalar: bool = True) -> str | None:
        if key is not None and _forbidden_key(key):
            return "FORBIDDEN_RESEARCH_FIELD"
        if isinstance(node, Mapping):
            for raw_key, child in node.items():
                child_key = raw_key if isinstance(raw_key, str) else str(raw_key)
                reason = walk(child, child_key, scan_scalar=child_key.casefold() not in _CODE_EXEMPT_KEYS)
                if reason:
                    return reason
            return None
        if isinstance(node, list):
            for child in node:
                reason = walk(child, key, scan_scalar=scan_scalar)
                if reason:
                    return reason
            return None
        if scan_scalar and isinstance(node, str) and _A_SHARE_CODE_RE.search(node):
            return "A_SHARE_CODE_FORBIDDEN"
        return None

    return walk(value)


def _forbidden_key(value: str) -> bool:
    normalized = re.sub(r"[\s._-]+", "_", value.casefold())
    return any(fragment in normalized for fragment in _FORBIDDEN_KEY_FRAGMENTS)


def _text(value: Any, limit: int, reason_code: str) -> str:
    if not isinstance(value, str):
        raise ResearchConsensusValidationError(reason_code)
    normalized = value.strip()
    if not normalized or len(normalized) > limit or "\x00" in normalized:
        raise ResearchConsensusValidationError(reason_code)
    return normalized


def _text_list(value: Any, max_items: int, item_limit: int, reason_code: str) -> list[str]:
    if not isinstance(value, list) or len(value) > max_items:
        raise ResearchConsensusValidationError(reason_code)
    return [_text(item, item_limit, reason_code) for item in value]


def _confidence(value: Any) -> float | str:
    # Hand-reviewed files commonly use LOW/MEDIUM/HIGH labels; machine-made
    # fixtures may use a bounded probability.  Both forms remain descriptive
    # evidence and are never interpreted as a trading score here.
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"LOW", "MEDIUM", "HIGH"}:
            return normalized
        raise ResearchConsensusValidationError("INVALID_CONFIDENCE")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResearchConsensusValidationError("INVALID_CONFIDENCE")
    confidence = float(value)
    if confidence != confidence or confidence in (float("inf"), float("-inf")) or not 0.0 <= confidence <= 1.0:
        raise ResearchConsensusValidationError("INVALID_CONFIDENCE")
    return confidence


def _https_url(value: Any) -> str:
    url = _text(value, MAX_SOURCE_URL_LENGTH, "INVALID_SOURCE_URL")
    parsed = urlparse(url)
    if parsed.scheme.casefold() != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ResearchConsensusValidationError("INVALID_SOURCE_URL")
    return url


def _attachment_source_ref(value: Any) -> str:
    source_ref = _text(value, 96, "INVALID_ATTACHMENT_SOURCE_REF").casefold()
    if not re.fullmatch(r"attachment-sha256:[0-9a-f]{64}", source_ref):
        raise ResearchConsensusValidationError("INVALID_ATTACHMENT_SOURCE_REF")
    return source_ref


def _source_label(value: Any) -> str:
    label = _text(value, MAX_TITLE_LENGTH, "INVALID_SOURCE_LABEL")
    if Path(label).name != label or "/" in label or "\\" in label:
        raise ResearchConsensusValidationError("INVALID_SOURCE_LABEL")
    return label


def _parse_aware_datetime(value: Any, reason_code: str) -> datetime:
    if not isinstance(value, str):
        raise ResearchConsensusValidationError(reason_code)
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResearchConsensusValidationError(reason_code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchConsensusValidationError(reason_code)
    return parsed


def _aware_as_of(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("AS_OF_TIMEZONE_REQUIRED")
    return value


def _effective_month(value: Any) -> str:
    if not isinstance(value, str):
        raise ResearchConsensusValidationError("INVALID_EFFECTIVE_MONTH")
    text = value.strip()
    match = _MONTH_RE.fullmatch(text)
    if not match:
        raise ResearchConsensusValidationError("INVALID_EFFECTIVE_MONTH")
    try:
        date(int(match.group(1)), int(match.group(2)), 1)
    except ValueError as exc:
        raise ResearchConsensusValidationError("INVALID_EFFECTIVE_MONTH") from exc
    return text


def _effective_month_window(as_of: datetime) -> tuple[str, str]:
    start = date(as_of.year, as_of.month, 1)
    end_year, end_month = start.year, start.month + 2
    if end_month > 12:
        end_year += (end_month - 1) // 12
        end_month = (end_month - 1) % 12 + 1
    return start.strftime("%Y-%m"), f"{end_year:04d}-{end_month:02d}"


def _path_sort_key(path: Path) -> str:
    return str(path.resolve()).casefold()


def _document_sort_key(document: _ValidatedDocument) -> tuple[str, datetime, str, str]:
    return (document.effective_month, document.publish_time, str(document.payload["document_id"]), _path_sort_key(document.path))


def _invalid_ref(path: Path, reason_code: str) -> dict[str, str]:
    return {"source_ref": path.name, "reason_code": reason_code}


def _list_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "EVIDENCE_TIER",
    "MAX_AXES",
    "MAX_DISAGREEMENTS",
    "MAX_DOCUMENTS",
    "MAX_INSTITUTIONS",
    "MAX_VERIFICATION_ITEMS",
    "PRIVATE_ATTACHMENT_SOURCE_TYPE",
    "ResearchConsensusLoader",
    "ResearchConsensusValidationError",
    "SCHEMA_VERSION",
    "SOURCE_TYPE",
    "load_research_consensus",
    "load_research_consensus_bundle",
    "project_a2_research_hypotheses",
    "unavailable_research_consensus",
]
