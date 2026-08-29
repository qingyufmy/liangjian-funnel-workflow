"""Deterministic readiness gates for the persisted research fact plane.

The research workflow must never discover missing full-market facts halfway
through an LLM request.  This module turns cache coverage and immutable source
watermarks into one small, serialisable decision that can be checked before a
research run starts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


READY = "READY"
READY_DEGRADED = "READY_DEGRADED"
BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class NamespaceReadiness:
    namespace: str
    required: bool
    status: str
    covered_symbols: int
    expected_symbols: int
    watermark: str | None = None
    reason_code: str | None = None

    @property
    def coverage_ratio(self) -> float:
        if self.expected_symbols <= 0:
            return 1.0
        return min(1.0, self.covered_symbols / self.expected_symbols)

    def as_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "required": self.required,
            "status": self.status,
            "covered_symbols": self.covered_symbols,
            "expected_symbols": self.expected_symbols,
            "coverage_ratio": round(self.coverage_ratio, 6),
            "watermark": self.watermark,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True, slots=True)
class DataReadinessReport:
    status: str
    as_of: str
    expected_symbols: int
    namespaces: tuple[NamespaceReadiness, ...]
    reason_codes: tuple[str, ...]
    version_hash: str

    @property
    def ready(self) -> bool:
        return self.status in {READY, READY_DEGRADED}

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "research-data-readiness/1.0.0",
            "status": self.status,
            "as_of": self.as_of,
            "expected_symbols": self.expected_symbols,
            "namespaces": [item.as_dict() for item in self.namespaces],
            "reason_codes": list(self.reason_codes),
            "version_hash": self.version_hash,
        }


def evaluate_data_readiness(
    coverage: Mapping[str, Any],
    *,
    expected_symbols: int,
    as_of: datetime,
    supplemental: Sequence[NamespaceReadiness] = (),
    minimum_daily_ratio: float = 0.98,
    minimum_financial_ratio: float = 0.90,
) -> DataReadinessReport:
    """Evaluate only persisted facts; this function performs no network IO.

    Daily bars are a hard gate.  Financial facts are a degraded-ready gate so
    newly listed companies or temporarily unavailable low-priority facts do
    not erase a valid full-market research run.  Every missing symbol remains
    visible in the deterministic stock-level reasons downstream.
    """

    if expected_symbols < 1:
        raise ValueError("expected_symbols must be positive")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    if not 0 < minimum_daily_ratio <= 1 or not 0 < minimum_financial_ratio <= 1:
        raise ValueError("coverage ratios must be in (0, 1]")

    daily = _mapping(coverage.get("daily"))
    financial = _mapping(coverage.get("financial"))
    rows = [
        _coverage_namespace(
            "daily_bars",
            required=True,
            covered=_count(daily.get("symbols")),
            expected=expected_symbols,
            minimum_ratio=minimum_daily_ratio,
            watermark=_text(daily.get("max_timestamp")),
        ),
        _coverage_namespace(
            "financial_facts",
            required=False,
            covered=_count(financial.get("symbols")),
            expected=expected_symbols,
            minimum_ratio=minimum_financial_ratio,
            watermark=_text(financial.get("max_published_at")),
        ),
        *supplemental,
    ]
    reasons = tuple(
        dict.fromkeys(item.reason_code for item in rows if item.reason_code)
    )
    if any(item.required and item.status == BLOCKED for item in rows):
        status = BLOCKED
    elif any(item.status != READY for item in rows):
        status = READY_DEGRADED
    else:
        status = READY
    as_of_text = as_of.astimezone(timezone.utc).isoformat(timespec="seconds")
    canonical = {
        "as_of": as_of_text,
        "expected_symbols": expected_symbols,
        "namespaces": [item.as_dict() for item in rows],
    }
    version_hash = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return DataReadinessReport(
        status=status,
        as_of=as_of_text,
        expected_symbols=expected_symbols,
        namespaces=tuple(rows),
        reason_codes=reasons,
        version_hash=version_hash,
    )


def namespace_readiness(
    namespace: str,
    *,
    covered_symbols: int,
    expected_symbols: int,
    required: bool,
    minimum_ratio: float,
    watermark: str | None = None,
    unavailable_reason: str | None = None,
) -> NamespaceReadiness:
    if unavailable_reason:
        return NamespaceReadiness(
            namespace=namespace,
            required=required,
            status=BLOCKED if required else READY_DEGRADED,
            covered_symbols=max(0, int(covered_symbols)),
            expected_symbols=max(0, int(expected_symbols)),
            watermark=watermark,
            reason_code=unavailable_reason,
        )
    return _coverage_namespace(
        namespace,
        required=required,
        covered=covered_symbols,
        expected=expected_symbols,
        minimum_ratio=minimum_ratio,
        watermark=watermark,
    )


def _coverage_namespace(
    namespace: str,
    *,
    required: bool,
    covered: int,
    expected: int,
    minimum_ratio: float,
    watermark: str | None,
) -> NamespaceReadiness:
    ratio = 1.0 if expected <= 0 else covered / expected
    if ratio >= minimum_ratio:
        status = READY
        reason = None
    else:
        status = BLOCKED if required else READY_DEGRADED
        reason = f"{namespace.upper()}_COVERAGE_BELOW_THRESHOLD"
    return NamespaceReadiness(
        namespace=namespace,
        required=required,
        status=status,
        covered_symbols=max(0, int(covered)),
        expected_symbols=max(0, int(expected)),
        watermark=watermark,
        reason_code=reason,
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


__all__ = [
    "BLOCKED",
    "READY",
    "READY_DEGRADED",
    "DataReadinessReport",
    "NamespaceReadiness",
    "evaluate_data_readiness",
    "namespace_readiness",
]
