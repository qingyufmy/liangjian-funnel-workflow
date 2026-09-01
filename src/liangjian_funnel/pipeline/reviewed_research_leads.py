"""Point-in-time loader for hand-reviewed public research commentary.

This is deliberately separate from broker/institution consensus.  A public
premarket article is a T3 research lead: useful for framing questions, but it
cannot become a fact, alter a deterministic score, or introduce a stock into
the funnel.  Only a compact allow-listed projection is exposed to A1/A2.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "reviewed-public-research-lead/1.0.0"
CONTRACT_VERSION = "reviewed-public-research-leads/1.0.0"
EVIDENCE_TIER = "T3"
SHANGHAI = ZoneInfo("Asia/Shanghai")

MAX_DOCUMENTS = 16
MAX_DOCUMENT_BYTES = 512 * 1024
MAX_ITEMS = 32
MAX_TEXT = 2048
MAX_SHORT_TEXT = 512

_DOCUMENT_FIELDS = frozenset(
    {
        "schema_version",
        "document_id",
        "title",
        "publisher",
        "source_urls",
        "publish_time",
        "claimed_data_cutoff",
        "valid_until",
        "methodology_axes",
        "theme_hypotheses",
        "quality_flags",
    }
)
_METHODOLOGY_FIELDS = frozenset(
    {"axis", "method", "required_inputs", "failure_modes"}
)
_HYPOTHESIS_FIELDS = frozenset(
    {"theme", "stance", "rationale", "required_facts", "counter_signals"}
)
_FORBIDDEN_KEY_FRAGMENTS = (
    "symbol",
    "ticker",
    "security_code",
    "stock",
    "position_size",
    "entry_price",
    "stop_loss",
    "target_price",
    "股票",
    "个股",
    "标的",
    "仓位",
    "买入价",
    "止损",
    "目标价",
)
_A_SHARE_CODE_RE = re.compile(
    r"(?<!\d)(?:\d{6}(?:[._-](?:SH|SZ|BJ))|(?:SH|SZ|BJ)(?:SE)?[._-]?\d{6}|\d{6})(?!\d)",
    re.IGNORECASE,
)


class ReviewedResearchLeadError(ValueError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


def load_reviewed_research_leads(
    directory: Path | str,
    *,
    as_of: datetime,
) -> dict[str, Any]:
    cutoff = _aware(as_of)
    root = Path(directory).expanduser().resolve()
    if not root.is_dir():
        return unavailable_reviewed_research_leads(
            as_of=cutoff,
            reason_code="REVIEWED_RESEARCH_LEADS_DIRECTORY_MISSING",
        )

    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, str]] = []
    expired_count = 0
    future_count = 0
    for path in sorted(root.glob("*.json"), key=lambda item: item.name.casefold()):
        if not path.is_file():
            continue
        try:
            document = _validate_document(_read_json(path))
        except ReviewedResearchLeadError as exc:
            invalid.append({"source_ref": path.name, "reason_code": exc.reason_code})
            continue
        except (OSError, UnicodeError):
            invalid.append({"source_ref": path.name, "reason_code": "DOCUMENT_READ_FAILED"})
            continue
        published = datetime.fromisoformat(document["publish_time"])
        valid_until = datetime.fromisoformat(document["valid_until"])
        if published > cutoff:
            future_count += 1
            continue
        if cutoff > valid_until:
            expired_count += 1
            continue
        valid.append(document)

    valid.sort(key=lambda item: (item["publish_time"], item["document_id"]))
    excluded_count = max(0, len(valid) - MAX_DOCUMENTS)
    valid = valid[-MAX_DOCUMENTS:]
    if valid:
        reason_code = "PARTIAL_DOCUMENTS" if invalid or excluded_count else "OK"
        available = True
    elif invalid:
        reason_code = "NO_VALID_DOCUMENTS"
        available = False
    else:
        reason_code = "NO_ACTIVE_DOCUMENTS"
        available = False
    return _contract(
        as_of=cutoff,
        available=available,
        reason_code=reason_code,
        documents=valid,
        invalid_documents=invalid,
        future_document_count=future_count,
        expired_document_count=expired_count,
        excluded_document_count=excluded_count,
    )


def unavailable_reviewed_research_leads(
    *,
    as_of: datetime,
    reason_code: str = "REVIEWED_RESEARCH_LEADS_LOAD_FAILED",
) -> dict[str, Any]:
    return _contract(
        as_of=_aware(as_of),
        available=False,
        reason_code=reason_code,
        documents=(),
        invalid_documents=(),
    )


def _validate_document(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _DOCUMENT_FIELDS:
        raise ReviewedResearchLeadError("INVALID_DOCUMENT_FIELDS")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ReviewedResearchLeadError("INVALID_SCHEMA_VERSION")
    if _forbidden_content(raw):
        raise ReviewedResearchLeadError("FORBIDDEN_SELECTION_CONTENT")

    publish_time = _timestamp(raw.get("publish_time"), "INVALID_PUBLISH_TIME")
    claimed_cutoff = _timestamp(raw.get("claimed_data_cutoff"), "INVALID_CLAIMED_DATA_CUTOFF")
    valid_until = _timestamp(raw.get("valid_until"), "INVALID_VALID_UNTIL")
    if claimed_cutoff > publish_time or valid_until < publish_time:
        raise ReviewedResearchLeadError("INVALID_TIME_ORDER")

    source_urls_raw = raw.get("source_urls")
    if not isinstance(source_urls_raw, list) or not source_urls_raw or len(source_urls_raw) > MAX_ITEMS:
        raise ReviewedResearchLeadError("INVALID_SOURCE_URLS")
    source_urls = [_https_url(value) for value in source_urls_raw]

    methodology_raw = raw.get("methodology_axes")
    if not isinstance(methodology_raw, list) or not methodology_raw or len(methodology_raw) > MAX_ITEMS:
        raise ReviewedResearchLeadError("INVALID_METHODOLOGY_AXES")
    methodology_axes = [_methodology(item) for item in methodology_raw]

    hypotheses_raw = raw.get("theme_hypotheses")
    if not isinstance(hypotheses_raw, list) or len(hypotheses_raw) > MAX_ITEMS:
        raise ReviewedResearchLeadError("INVALID_THEME_HYPOTHESES")
    theme_hypotheses = [_hypothesis(item) for item in hypotheses_raw]

    quality_flags = _text_list(raw.get("quality_flags"), "INVALID_QUALITY_FLAGS")
    if claimed_cutoff.astimezone(SHANGHAI).time() < datetime.strptime("09:15", "%H:%M").time():
        reviewed_states = {
            "PREOPEN_NO_CURRENT_SESSION_QUOTES_CONFIRMED",
            "PREOPEN_CURRENT_SESSION_QUOTE_CONFLICT",
        }
        if not reviewed_states.intersection(quality_flags):
            raise ReviewedResearchLeadError("PREOPEN_QUOTE_REVIEW_REQUIRED")

    return {
        "schema_version": SCHEMA_VERSION,
        "document_id": _text(raw.get("document_id"), 128, "INVALID_DOCUMENT_ID"),
        "title": _text(raw.get("title"), MAX_SHORT_TEXT, "INVALID_TITLE"),
        "publisher": _text(raw.get("publisher"), 256, "INVALID_PUBLISHER"),
        "source_urls": list(dict.fromkeys(source_urls)),
        "publish_time": publish_time.isoformat(),
        "claimed_data_cutoff": claimed_cutoff.isoformat(),
        "valid_until": valid_until.isoformat(),
        "evidence_tier": EVIDENCE_TIER,
        "viewpoint_only": True,
        "original_fact_authority": False,
        "direct_stock_selection_allowed": False,
        "deterministic_score_influence_allowed": False,
        "independent_source_resolution_required": True,
        "methodology_axes": methodology_axes,
        "theme_hypotheses": theme_hypotheses,
        "quality_flags": quality_flags,
    }


def _methodology(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _METHODOLOGY_FIELDS:
        raise ReviewedResearchLeadError("INVALID_METHODOLOGY_ITEM")
    return {
        "axis": _text(raw.get("axis"), MAX_SHORT_TEXT, "INVALID_METHODOLOGY_ITEM"),
        "method": _text(raw.get("method"), MAX_TEXT, "INVALID_METHODOLOGY_ITEM"),
        "required_inputs": _text_list(raw.get("required_inputs"), "INVALID_METHODOLOGY_ITEM"),
        "failure_modes": _text_list(raw.get("failure_modes"), "INVALID_METHODOLOGY_ITEM"),
    }


def _hypothesis(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _HYPOTHESIS_FIELDS:
        raise ReviewedResearchLeadError("INVALID_HYPOTHESIS_ITEM")
    return {
        "theme": _text(raw.get("theme"), MAX_SHORT_TEXT, "INVALID_HYPOTHESIS_ITEM"),
        "stance": _text(raw.get("stance"), MAX_SHORT_TEXT, "INVALID_HYPOTHESIS_ITEM"),
        "rationale": _text_list(raw.get("rationale"), "INVALID_HYPOTHESIS_ITEM"),
        "required_facts": _text_list(raw.get("required_facts"), "INVALID_HYPOTHESIS_ITEM"),
        "counter_signals": _text_list(raw.get("counter_signals"), "INVALID_HYPOTHESIS_ITEM"),
    }


def _contract(
    *,
    as_of: datetime,
    available: bool,
    reason_code: str,
    documents: Sequence[Mapping[str, Any]],
    invalid_documents: Sequence[Mapping[str, str]],
    future_document_count: int = 0,
    expired_document_count: int = 0,
    excluded_document_count: int = 0,
) -> dict[str, Any]:
    projected = [dict(item) for item in documents]
    body = {"schema_version": CONTRACT_VERSION, "documents": projected}
    return {
        "schema_version": CONTRACT_VERSION,
        "available": bool(available),
        "reason_code": reason_code,
        "as_of": as_of.isoformat(),
        "evidence_tier": EVIDENCE_TIER,
        "viewpoint_only": True,
        "untrusted_text": True,
        "original_fact_authority": False,
        "direct_stock_selection_allowed": False,
        "deterministic_score_influence_allowed": False,
        "fact_substitution_allowed": False,
        "documents": projected,
        "source_refs": list(
            dict.fromkeys(
                url
                for document in projected
                for url in document.get("source_urls", ())
                if isinstance(url, str)
            )
        ),
        "invalid_document_count": len(invalid_documents),
        "invalid_documents": [dict(item) for item in invalid_documents],
        "future_document_count": int(future_document_count),
        "expired_document_count": int(expired_document_count),
        "excluded_document_count": int(excluded_document_count),
        "content_hash": hashlib.sha256(
            json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _read_json(path: Path) -> Any:
    if path.stat().st_size > MAX_DOCUMENT_BYTES:
        raise ReviewedResearchLeadError("DOCUMENT_TOO_LARGE")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ReviewedResearchLeadError("INVALID_JSON") from exc


def _forbidden_content(value: Any) -> bool:
    def walk(node: Any, key: str | None = None) -> bool:
        if key is not None:
            normalized = re.sub(r"[\s._-]+", "_", key.casefold())
            if any(fragment in normalized for fragment in _FORBIDDEN_KEY_FRAGMENTS):
                return True
        if isinstance(node, Mapping):
            return any(walk(child, str(raw_key)) for raw_key, child in node.items())
        if isinstance(node, list):
            return any(walk(child, key) for child in node)
        return isinstance(node, str) and _A_SHARE_CODE_RE.search(node) is not None

    return walk(value)


def _timestamp(value: Any, reason_code: str) -> datetime:
    if not isinstance(value, str):
        raise ReviewedResearchLeadError(reason_code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReviewedResearchLeadError(reason_code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReviewedResearchLeadError(reason_code)
    return parsed


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("AS_OF_TIMEZONE_REQUIRED")
    return value


def _https_url(value: Any) -> str:
    text = _text(value, 2048, "INVALID_SOURCE_URL")
    parsed = urlparse(text)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ReviewedResearchLeadError("INVALID_SOURCE_URL")
    return text


def _text(value: Any, limit: int, reason_code: str) -> str:
    if not isinstance(value, str):
        raise ReviewedResearchLeadError(reason_code)
    normalized = value.strip()
    if not normalized or len(normalized) > limit or "\x00" in normalized:
        raise ReviewedResearchLeadError(reason_code)
    return normalized


def _text_list(value: Any, reason_code: str) -> list[str]:
    if not isinstance(value, list) or len(value) > MAX_ITEMS:
        raise ReviewedResearchLeadError(reason_code)
    return [_text(item, MAX_SHORT_TEXT, reason_code) for item in value]


__all__ = [
    "CONTRACT_VERSION",
    "EVIDENCE_TIER",
    "SCHEMA_VERSION",
    "ReviewedResearchLeadError",
    "load_reviewed_research_leads",
    "unavailable_reviewed_research_leads",
]
