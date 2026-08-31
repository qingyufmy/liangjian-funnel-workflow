"""Governed A1 research-source registry and point-in-time availability view.

The registry is deliberately not a web scraper.  It records which sources may
be automated, which require a licensed API, and which may enter the workflow
only through a reviewed local export.  A source name or homepage is never
treated as evidence by itself: A1 may use a source only when a frozen snapshot
contract or a validated research-consensus document is actually present.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


REGISTRY_SCHEMA_VERSION = "a1-research-source-registry/1.0.0"
CONTEXT_SCHEMA_VERSION = "a1-research-source-context/1.0.0"
_ACCESS_MODES = frozenset({
    "OFFICIAL_PUBLIC",
    "PUBLIC_WEB",
    "LICENSED_API",
    "PAID_CONTENT",
})
_INGESTION_MODES = frozenset({
    "AUTOMATED_CONTRACT",
    "AUTHORIZED_API_ONLY",
    "MANUAL_REVIEWED_EXPORT",
    "REFERENCE_ONLY",
    "AUTOMATION_FORBIDDEN",
})
_EVIDENCE_TIERS = frozenset({"T1", "T2", "T3", "T4"})
_ALLOWED_ROLES = frozenset({
    "OFFICIAL_DISCLOSURE",
    "OFFICIAL_MACRO",
    "MACRO_POLICY",
    "INDUSTRY_ROTATION",
    "BROKER_RESEARCH",
    "FUND_VEHICLE",
    "FUNDAMENTAL_CROSS_CHECK",
    "MARKET_SENTIMENT",
    "CAPITAL_FLOW_CROSS_CHECK",
})
_MAX_SOURCES = 32


class A1SourceRegistryError(ValueError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


def load_a1_source_registry(path: Path | str) -> dict[str, Any]:
    """Load a strict, non-secret source policy from YAML."""

    registry_path = Path(path).expanduser().resolve()
    if not registry_path.is_file():
        raise A1SourceRegistryError("A1_SOURCE_REGISTRY_MISSING")
    try:
        raw = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise A1SourceRegistryError("A1_SOURCE_REGISTRY_READ_FAILED") from exc
    if not isinstance(raw, Mapping) or raw.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise A1SourceRegistryError("A1_SOURCE_REGISTRY_SCHEMA_INVALID")
    rows = raw.get("sources")
    if not isinstance(rows, list) or not rows or len(rows) > _MAX_SOURCES:
        raise A1SourceRegistryError("A1_SOURCE_REGISTRY_COUNT_INVALID")

    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_hosts: set[str] = set()
    for raw_row in rows:
        if not isinstance(raw_row, Mapping):
            raise A1SourceRegistryError("A1_SOURCE_REGISTRY_ROW_INVALID")
        source_id = _required_token(raw_row.get("source_id"), "A1_SOURCE_ID_INVALID")
        if source_id in seen_ids:
            raise A1SourceRegistryError("A1_SOURCE_ID_DUPLICATE")
        host = _host(raw_row.get("host"))
        aliases = [_host(value) for value in raw_row.get("host_aliases", ())] if isinstance(raw_row.get("host_aliases"), list) else []
        hosts = list(dict.fromkeys([host, *aliases]))
        if set(hosts).intersection(seen_hosts):
            raise A1SourceRegistryError("A1_SOURCE_HOST_DUPLICATE")
        access_mode = str(raw_row.get("access_mode") or "").strip().upper()
        ingestion_mode = str(raw_row.get("ingestion_mode") or "").strip().upper()
        evidence_tier = str(raw_row.get("evidence_tier") or "").strip().upper()
        if access_mode not in _ACCESS_MODES:
            raise A1SourceRegistryError("A1_SOURCE_ACCESS_MODE_INVALID")
        if ingestion_mode not in _INGESTION_MODES:
            raise A1SourceRegistryError("A1_SOURCE_INGESTION_MODE_INVALID")
        if evidence_tier not in _EVIDENCE_TIERS:
            raise A1SourceRegistryError("A1_SOURCE_EVIDENCE_TIER_INVALID")
        roles = _tokens(raw_row.get("roles"))
        if not roles or not set(roles).issubset(_ALLOWED_ROLES):
            raise A1SourceRegistryError("A1_SOURCE_ROLES_INVALID")
        contracts = _tokens(raw_row.get("snapshot_contracts"), upper=False)
        if raw_row.get("direct_stock_selection_allowed") is not False:
            raise A1SourceRegistryError("A1_SOURCE_SELECTION_AUTHORITY_INVALID")
        if ingestion_mode == "AUTOMATED_CONTRACT" and not contracts:
            raise A1SourceRegistryError("A1_SOURCE_AUTOMATION_CONTRACT_MISSING")
        if ingestion_mode in {"AUTOMATION_FORBIDDEN", "MANUAL_REVIEWED_EXPORT"} and contracts:
            raise A1SourceRegistryError("A1_SOURCE_MANUAL_CONTRACT_INVALID")
        normalized.append({
            "source_id": source_id,
            "label": _required_text(raw_row.get("label"), "A1_SOURCE_LABEL_INVALID"),
            "host": host,
            "host_aliases": aliases,
            "homepage": f"https://{host}/",
            "access_mode": access_mode,
            "ingestion_mode": ingestion_mode,
            "evidence_tier": evidence_tier,
            "roles": roles,
            "snapshot_contracts": contracts,
            "fact_authority": bool(raw_row.get("fact_authority") is True),
            "viewpoint_only": bool(raw_row.get("viewpoint_only") is True),
            "direct_stock_selection_allowed": False,
            "provenance_mode": str(raw_row.get("provenance_mode") or "DIRECT").strip().upper(),
            "contract_requires_matching_document": bool(
                raw_row.get("contract_requires_matching_document") is True
            ),
            "activation_requirement": _optional_text(raw_row.get("activation_requirement")),
        })
        seen_ids.add(source_id)
        seen_hosts.update(hosts)

    payload = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "sources": normalized,
    }
    payload["content_hash"] = _hash(payload)
    return payload


def unavailable_a1_source_context(reason_code: str) -> dict[str, Any]:
    payload = {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "available": False,
        "reason_code": str(reason_code or "A1_SOURCE_CONTEXT_UNAVAILABLE"),
        "source_count": 0,
        "usable_source_count": 0,
        "sources": [],
        "role_coverage": {},
        "governance": _governance(),
    }
    payload["content_hash"] = _hash(payload)
    return payload


def build_a1_source_context(
    registry: Mapping[str, Any],
    *,
    snapshot: Mapping[str, Any],
    research_consensus: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve configured sources against facts frozen for this A1 run."""

    rows = registry.get("sources")
    if registry.get("schema_version") != REGISTRY_SCHEMA_VERSION or not isinstance(rows, list):
        return unavailable_a1_source_context("A1_SOURCE_REGISTRY_SCHEMA_INVALID")
    consensus = research_consensus if isinstance(research_consensus, Mapping) else {}
    documents = consensus.get("documents")
    documents = documents if isinstance(documents, list) else []
    resolved: list[dict[str, Any]] = []
    role_coverage: Counter[str] = Counter()
    for configured in rows:
        if not isinstance(configured, Mapping):
            continue
        host = str(configured.get("host") or "").lower()
        hosts = [host, *[
            str(value).lower()
            for value in configured.get("host_aliases", ())
            if isinstance(value, str) and value
        ]]
        matching_documents = [
            document for document in documents
            if isinstance(document, Mapping) and _document_matches(document, hosts, str(configured.get("label") or ""))
        ]
        active_contracts = [
            contract for contract in configured.get("snapshot_contracts", ())
            if isinstance(contract, str) and _contract_available(snapshot.get(contract))
        ]
        if configured.get("contract_requires_matching_document") is True and not matching_documents:
            active_contracts = []
        evidence_available = bool(active_contracts or matching_documents)
        ingestion_mode = str(configured.get("ingestion_mode") or "")
        status = _status(
            ingestion_mode,
            active_contracts=active_contracts,
            matching_document_count=len(matching_documents),
            provenance_mode=str(configured.get("provenance_mode") or "DIRECT"),
        )
        usable = evidence_available and status not in {"AUTOMATION_FORBIDDEN", "AUTHORIZATION_REQUIRED"}
        roles = list(configured.get("roles") or ())
        if usable:
            role_coverage.update(str(role) for role in roles)
        resolved.append({
            "source_id": configured.get("source_id"),
            "label": configured.get("label"),
            "host": host,
            "host_aliases": list(configured.get("host_aliases") or ()),
            "evidence_tier": configured.get("evidence_tier"),
            "roles": roles,
            "access_mode": configured.get("access_mode"),
            "ingestion_mode": ingestion_mode,
            "status": status,
            "usable_for_a1": usable,
            "active_snapshot_contracts": active_contracts,
            "reviewed_document_count": len(matching_documents),
            "reviewed_document_refs": [
                document.get("source_ref") or document.get("source_url")
                for document in matching_documents[:12]
            ],
            "fact_authority": configured.get("fact_authority") is True,
            "viewpoint_only": configured.get("viewpoint_only") is True,
            "direct_stock_selection_allowed": False,
            "activation_requirement": configured.get("activation_requirement"),
        })
    payload = {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "available": True,
        "reason_code": "OK",
        "registry_hash": registry.get("content_hash"),
        "source_count": len(resolved),
        "usable_source_count": sum(item["usable_for_a1"] for item in resolved),
        "sources": resolved,
        "role_coverage": dict(sorted(role_coverage.items())),
        "governance": _governance(),
    }
    payload["content_hash"] = _hash(payload)
    return payload


def _status(
    ingestion_mode: str,
    *,
    active_contracts: Sequence[str],
    matching_document_count: int,
    provenance_mode: str,
) -> str:
    if active_contracts:
        return "ACTIVE_INDIRECT_TRANSPORT" if provenance_mode == "INDIRECT" else "ACTIVE_AUTOMATED"
    if matching_document_count:
        return "REVIEWED_EVIDENCE_AVAILABLE"
    if ingestion_mode == "AUTOMATION_FORBIDDEN":
        return "AUTOMATION_FORBIDDEN"
    if ingestion_mode == "AUTHORIZED_API_ONLY":
        return "AUTHORIZATION_REQUIRED"
    if ingestion_mode == "MANUAL_REVIEWED_EXPORT":
        return "MANUAL_EXPORT_REQUIRED"
    if ingestion_mode == "REFERENCE_ONLY":
        return "REFERENCE_ONLY"
    return "CONFIGURED_NO_EVIDENCE"


def _governance() -> dict[str, bool]:
    return {
        "homepage_is_not_evidence": True,
        "point_in_time_evidence_required": True,
        "paid_content_requires_authorized_export": True,
        "automated_access_must_be_explicitly_allowed": True,
        "viewpoints_cannot_replace_official_facts": True,
        "direct_stock_selection_from_source_forbidden": True,
    }


def _document_matches(document: Mapping[str, Any], hosts: Sequence[str], label: str) -> bool:
    raw_url = document.get("source_url") or document.get("source_ref")
    if isinstance(raw_url, str) and raw_url.startswith("https://"):
        document_host = (urlparse(raw_url).hostname or "").lower()
        if any(document_host == host or document_host.endswith(f".{host}") for host in hosts):
            return True
    source_label = str(document.get("source_label") or "").strip().casefold()
    return bool(source_label and label.strip() and label.strip().casefold() in source_label)


def _contract_available(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    if value.get("available") is True:
        return True
    for key in ("items", "records", "documents", "events", "series"):
        rows = value.get(key)
        if isinstance(rows, list) and rows:
            return True
    return False


def _required_token(value: Any, reason: str) -> str:
    token = str(value or "").strip().lower()
    if not token or len(token) > 64 or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_" for char in token):
        raise A1SourceRegistryError(reason)
    return token


def _required_text(value: Any, reason: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 128:
        raise A1SourceRegistryError(reason)
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:512] or None


def _tokens(value: Any, *, upper: bool = True) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for raw in value:
        token = str(raw or "").strip()
        token = token.upper() if upper else token
        if token and token not in result:
            result.append(token)
    return result


def _host(value: Any) -> str:
    raw = str(value or "").strip().lower().rstrip(".")
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.hostname or "").lower()
    if not host or parsed.scheme != "https" or parsed.path not in {"", "/"} or "*" in host:
        raise A1SourceRegistryError("A1_SOURCE_HOST_INVALID")
    return host


def _hash(value: Any) -> str:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


__all__ = [
    "A1SourceRegistryError",
    "CONTEXT_SCHEMA_VERSION",
    "REGISTRY_SCHEMA_VERSION",
    "build_a1_source_context",
    "load_a1_source_registry",
    "unavailable_a1_source_context",
]
