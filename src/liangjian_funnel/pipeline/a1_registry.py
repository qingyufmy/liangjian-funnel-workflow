"""Persistent A1 generations and the fail-closed active pointer.

The deterministic feature store has its own lifecycle and is intentionally not
used as the source of truth for the A1 research result.  This module keeps the
small, immutable A1 contract separate: a maintenance run writes one staging
generation, seals it after validation, and swaps a single active pointer in a
SQLite transaction.  A failed or stale staging generation can therefore never
replace the last known-good A1 result.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


A1_REGISTRY_SCHEMA = "liangjian-a1-registry/1.0.0"
A1_REGISTRY_DOMAIN = "A1"
A1_FULL = "FULL"
A1_INCREMENTAL = "INCREMENTAL"
A1_GENERATION_STATUSES = frozenset({"STAGING", "SEALED", "FAILED"})
A1_GENERATION_MODES = frozenset({A1_FULL, A1_INCREMENTAL})
# A weekly maintenance slot normally refreshes this pointer before seven
# elapsed days.  A natural-days grace window is required for exchange
# holidays (notably the multi-day National Day break); the workflow exposes a
# degraded warning after the normal weekly cadence and fails closed only once
# this longer maximum age is exceeded.
DEFAULT_A1_DEGRADED_AFTER = timedelta(days=7)
DEFAULT_A1_MAX_AGE = timedelta(days=14)
A1_OUTPUT_PARTITIONS = (
    "active_research_pool",
    "monitor_pool",
    "rejected_candidates",
)
_SYMBOL_KEYS = frozenset({
    "symbol",
    "security_symbol",
    "ticker",
    "code",
    "thscode",
    "security_code",
})
_A1_STATIC_CANDIDATE_KEYS = frozenset({
    "symbol",
    "security_symbol",
    "ticker",
    "code",
    "exchange",
    "name",
    "research_eligible",
    "trade_eligible",
    "exclusion_reasons",
    "source",
    # Optional adapters may expose these stable identity fields.  Volatile
    # quote/turnover/price-limit fields are deliberately excluded below.
    "listing_date",
    "listed_date",
    "board",
    "security_type",
})
_A1_GLOBAL_INPUT_KEYS = (
    "MACRO_POLICY_FEED",
    "MACRO_ECONOMIC_DATA",
    "ASSET_ROTATION_SNAPSHOT",
    "GLOBAL_MACRO_SNAPSHOT",
    "CROSS_MARKET_LEAD_SNAPSHOT",
    "INDUSTRY_ACTIVITY_DATA",
    "INDUSTRY_PROFIT_DATA",
    "BROKER_RESEARCH_CONSENSUS",
    "BROKER_GOLD_COVERAGE_POOL",
    "RESEARCH_CONSENSUS",
    "A1_RESEARCH_SOURCE_CONTEXT",
    "A1_MONTHLY_STRATEGY_CONTEXT",
    "MONTHLY_STRATEGY_CONTEXT",
    "A1_WEEKLY_STRATEGY_CONTEXT",
    "WEEKLY_STRATEGY_CONTEXT",
    "EXISTING_CHAIN_GRAPH",
    "THEME_REGISTRY",
    "SECTOR_CYCLE_SNAPSHOT",
    "THS_INDUSTRY_MEMBERSHIP",
    "THS_CONCEPT_MEMBERSHIP",
    "A1_THEME_FINGERPRINTS",
)
_VOLATILE_GLOBAL_KEYS = frozenset({
    "as_of",
    "fetched_at",
    "retrieved_at",
    "updated_at",
    "generated_at",
    "snapshot_id",
    "snapshot_hash",
    "content_hash",
    "cache_status",
    "fetch_timestamp",
    "fetch_timestamps",
    "expires_at",
    "window_start",
    "window_end",
})


class _ClosingConnection(sqlite3.Connection):
    """Close short-lived SQLite handles at context exit (important on Windows)."""

    def __exit__(self, *args: Any) -> None:
        try:
            super().__exit__(*args)
        finally:
            self.close()


class A1RegistryError(RuntimeError):
    """Base error with a stable, safe reason code."""

    def __init__(self, reason_code: str, *, diagnostics: Mapping[str, Any] | None = None):
        self.reason_code = str(reason_code)
        self.diagnostics = dict(diagnostics or {})
        super().__init__(self.reason_code)


class A1ActivationError(A1RegistryError):
    """Raised when an immutable generation cannot become active."""


class A1GenerationNotFound(A1RegistryError):
    """Raised when a requested generation does not exist."""


@dataclass(frozen=True, slots=True)
class A1Generation:
    generation_id: str
    mode: str
    status: str
    snapshot_id: str
    snapshot_hash: str
    as_of: datetime
    base_generation_id: str | None
    manifest: Mapping[str, Any]
    payload: Mapping[str, Any]
    manifest_hash: str
    payload_hash: str
    created_at: datetime
    sealed_at: datetime | None = None
    failed_at: datetime | None = None
    failure_reason: str | None = None
    activated_at: datetime | None = None
    previous_generation_id: str | None = None

    @property
    def is_sealed(self) -> bool:
        return self.status == "SEALED"

    def as_dict(self, *, include_payload: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": A1_REGISTRY_SCHEMA,
            "generation_id": self.generation_id,
            "mode": self.mode,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "as_of": self.as_of.isoformat(),
            "base_generation_id": self.base_generation_id,
            "manifest": dict(self.manifest),
            "manifest_hash": self.manifest_hash,
            "payload_hash": self.payload_hash,
            "created_at": self.created_at.isoformat(),
            "sealed_at": self.sealed_at.isoformat() if self.sealed_at else None,
            "failed_at": self.failed_at.isoformat() if self.failed_at else None,
            "failure_reason": self.failure_reason,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "previous_generation_id": self.previous_generation_id,
        }
        if include_payload:
            result["payload"] = dict(self.payload)
        return result


@dataclass(frozen=True, slots=True)
class A1IncrementalScope:
    """The bounded set that an incremental A1 maintenance run may inspect."""

    symbols: tuple[str, ...]
    added_symbols: tuple[str, ...]
    changed_symbols: tuple[str, ...]
    theme_affected_symbols: tuple[str, ...]
    removed_symbols: tuple[str, ...]
    unchanged_symbols: tuple[str, ...]
    global_input_changed: bool = False
    current_global_input_hash: str = ""
    base_global_input_hash: str | None = None
    reason_codes: tuple[str, ...] = ()

    @property
    def processed_symbols(self) -> tuple[str, ...]:
        return self.symbols

    def as_dict(self) -> dict[str, Any]:
        return {
            "processed_symbols": list(self.symbols),
            "processed_count": len(self.symbols),
            "added_symbols": list(self.added_symbols),
            "added_count": len(self.added_symbols),
            "changed_symbols": list(self.changed_symbols),
            "changed_count": len(self.changed_symbols),
            "theme_affected_symbols": list(self.theme_affected_symbols),
            "theme_affected_count": len(self.theme_affected_symbols),
            "removed_symbols": list(self.removed_symbols),
            "removed_count": len(self.removed_symbols),
            "unchanged_count": len(self.unchanged_symbols),
            "global_input_changed": self.global_input_changed,
            "current_global_input_hash": self.current_global_input_hash or None,
            "base_global_input_hash": self.base_global_input_hash,
            "reason_codes": list(self.reason_codes),
        }


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _aware(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _text(value: Any) -> str:
    return str(value or "").strip()


def _symbols(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(sorted({str(item).strip().upper() for item in value if str(item).strip()}))


def _symbol_from_item(item: Mapping[str, Any]) -> str:
    for key in _SYMBOL_KEYS:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().upper()
    return ""


def _mapping_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _verified_outside_g0_symbols(
    output: Mapping[str, Any],
    g0_symbols: set[str],
) -> tuple[str, ...]:
    """Return research-only broker-gold rows that may extend an A1 partition.

    The exception is deliberately narrow: the symbol must be backed by the
    server-built institutional coverage pool, be a verified T2 broker-gold
    entry, and remain ineligible for downstream trading.  Merely emitting an
    unknown symbol in one of the three partitions never widens the registry
    domain.
    """

    verified: set[str] = set()
    for row in _mapping_list(output.get("institutional_coverage_pool")):
        symbol = _symbol_from_item(row)
        coverage = row.get("institutional_coverage")
        raw_reason_codes = row.get("reason_codes", ())
        reason_codes = {
            str(code).strip().upper()
            for code in raw_reason_codes
            if str(code).strip()
        } if isinstance(raw_reason_codes, Sequence) and not isinstance(
            raw_reason_codes, (str, bytes, bytearray)
        ) else set()
        if (
            symbol
            and symbol not in g0_symbols
            and str(row.get("autonomous_partition") or "").strip().upper() == "OUTSIDE_G0"
            and str(row.get("coverage_origin") or "").strip().upper() == "BROKER_GOLD_T2"
            and "A1_INSTITUTIONAL_DIRECT_ENTRY" in reason_codes
            and "A1_INSTITUTIONAL_OUTSIDE_G0" in reason_codes
            and isinstance(coverage, Mapping)
            and str(coverage.get("evidence_tier") or "").strip().upper() == "T2"
            and coverage.get("direct_research_entry") is True
        ):
            verified.add(symbol)
    return tuple(sorted(verified))


def _candidate_map(snapshot_data: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw = snapshot_data.get("g0_candidates", snapshot_data.get("universe_candidates", ()))
    if isinstance(raw, Mapping):
        return {
            str(key).strip().upper(): value
            for key, value in raw.items()
            if str(key).strip() and isinstance(value, Mapping)
        }
    result: dict[str, Mapping[str, Any]] = {}
    for item in raw if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) else ():
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol_from_item(item)
        if symbol:
            result[symbol] = item
    return result


def _candidate_static_projection(item: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only stable G0 identity/qualification fields for a symbol hash."""

    return {
        str(key): item[key]
        for key in sorted(_A1_STATIC_CANDIDATE_KEYS)
        if key in item
    }


def _symbol_mapping_value(value: Any, symbol: str) -> Any:
    """Resolve one symbol from either a flat map or a records envelope."""

    if not isinstance(value, Mapping):
        return None
    direct = value.get(symbol)
    if direct is not None:
        return direct
    for key, item in value.items():
        if str(key).strip().upper() == symbol:
            return item
    records = value.get("records")
    if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
        matching = [
            dict(item)
            for item in records
            if isinstance(item, Mapping) and symbol in _scan_symbols(item)
        ]
        if matching:
            return matching
    return None


def _symbol_event_projection(value: Any, symbol: str) -> Any:
    """Extract structural event/membership rows without global timestamps."""

    if isinstance(value, Mapping):
        resolved = _symbol_mapping_value(value, symbol)
        return resolved
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            dict(item)
            for item in value
            if isinstance(item, Mapping) and symbol in _scan_symbols(item)
        ]
    return None


def _symbol_value_index(value: Any, symbols: set[str]) -> dict[str, Any]:
    """Index one low-frequency source in one pass.

    The prior per-symbol resolver rescanned every ``records`` array for every
    G0 security, making manifest construction quadratic at full-market scale.
    Direct symbol-keyed values retain precedence; record envelopes are grouped
    once and preserve the same list shape as the legacy resolver.
    """

    direct: dict[str, Any] = {}
    rows: Sequence[Any] = ()
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().upper()
            if normalized in symbols:
                direct[normalized] = item
        candidate_rows = value.get("records")
        if isinstance(candidate_rows, Sequence) and not isinstance(candidate_rows, (str, bytes, bytearray)):
            rows = candidate_rows
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        rows = value

    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        matched = _scan_symbols(item).intersection(symbols)
        if not matched:
            continue
        row = dict(item)
        for symbol in matched:
            if symbol not in direct:
                grouped.setdefault(symbol, []).append(row)
    return {**grouped, **direct}


def _low_frequency_projection(value: Any) -> Any:
    """Canonicalize low-frequency A1 context while dropping fetch metadata."""

    if isinstance(value, Mapping):
        return {
            str(key): _low_frequency_projection(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key).strip().lower() not in _VOLATILE_GLOBAL_KEYS
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        projected = [_low_frequency_projection(item) for item in value]
        # Provider order is not part of the A1 contract.  Sorting makes the
        # global hash insensitive to harmless response ordering changes while
        # preserving rank fields inside each row.
        return sorted(projected, key=canonical_json)
    return value


def a1_global_input_hash(snapshot_data: Mapping[str, Any]) -> str:
    """Hash policy/strategy/industry context that can change A1 globally."""

    projection = {
        key: _low_frequency_projection(snapshot_data[key])
        for key in _A1_GLOBAL_INPUT_KEYS
        if key in snapshot_data
    }
    return content_hash(projection)


def _symbol_scoped(snapshot_data: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    """Build a stable, bounded fingerprint input for one security.

    Only source fields that can affect A1 identity/eligibility are included.
    Short-cycle quotes, turnover, technical factors and liquidity are owned by
    downstream stages and must never make a weekly A1 delta look like a full
    market refresh.
    """

    candidate = _candidate_map(snapshot_data).get(symbol)
    result: dict[str, Any] = {
        "candidate": _candidate_static_projection(candidate) if isinstance(candidate, Mapping) else {},
    }
    for key in (
        "COMPANY_FUNDAMENTALS",
        "MAIN_BUSINESS_EVIDENCE",
        "THS_INDUSTRY_MEMBERSHIP",
        "THS_CONCEPT_MEMBERSHIP",
    ):
        if key in snapshot_data:
            result[key] = _low_frequency_projection(
                _symbol_event_projection(snapshot_data.get(key), symbol)
            )
    for key in ("RISK_EVENTS", "DISCLOSURE_EVENTS"):
        if key in snapshot_data:
            result[key] = _low_frequency_projection(
                _symbol_event_projection(snapshot_data.get(key), symbol)
            )
    return result


def _symbol_fingerprints(
    snapshot_data: Mapping[str, Any],
    symbols: Sequence[str] | set[str],
) -> dict[str, str]:
    """Build all A1 candidate fingerprints with linear source indexing."""

    normalized_symbols = {
        str(symbol).strip().upper()
        for symbol in symbols
        if str(symbol).strip()
    }
    candidates = _candidate_map(snapshot_data)
    indexed_sources = {
        key: _symbol_value_index(snapshot_data.get(key), normalized_symbols)
        for key in (
            "COMPANY_FUNDAMENTALS",
            "MAIN_BUSINESS_EVIDENCE",
            "THS_INDUSTRY_MEMBERSHIP",
            "THS_CONCEPT_MEMBERSHIP",
            "RISK_EVENTS",
            "DISCLOSURE_EVENTS",
        )
        if key in snapshot_data
    }
    result: dict[str, str] = {}
    for symbol in normalized_symbols:
        candidate = candidates.get(symbol)
        projection: dict[str, Any] = {
            "candidate": _candidate_static_projection(candidate) if isinstance(candidate, Mapping) else {},
        }
        for key, index in indexed_sources.items():
            projection[key] = _low_frequency_projection(index.get(symbol))
        result[symbol] = content_hash(projection)
    return result


def _scan_symbols(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).strip().lower() in _SYMBOL_KEYS:
                text = str(item).strip().upper()
                if text:
                    found.add(text)
            found.update(_scan_symbols(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            found.update(_scan_symbols(item))
    return found


def _theme_ids_from_item(item: Mapping[str, Any]) -> set[str]:
    result: set[str] = set()
    for key in ("theme_id", "primary_theme", "theme"):
        value = item.get(key)
        if value is not None and str(value).strip():
            result.add(str(value).strip())
    for key in ("theme_ids", "primary_theme_ids"):
        value = item.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            result.update(str(item).strip() for item in value if str(item).strip())
    return result


def _manifest_symbol_fingerprints(manifest: Mapping[str, Any]) -> dict[str, str]:
    raw = manifest.get("candidate_fingerprints")
    if not isinstance(raw, Mapping):
        return {}
    return {str(key).strip().upper(): str(value) for key, value in raw.items() if str(key).strip()}


def _manifest_symbol_themes(manifest: Mapping[str, Any]) -> dict[str, set[str]]:
    raw = manifest.get("candidate_themes")
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, set[str]] = {}
    for key, value in raw.items():
        symbol = str(key).strip().upper()
        if not symbol:
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            result[symbol] = {str(item).strip() for item in value if str(item).strip()}
    return result


def _output_symbol_themes(output: Mapping[str, Any]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for partition in A1_OUTPUT_PARTITIONS:
        for item in _mapping_list(output.get(partition)):
            symbol = _symbol_from_item(item)
            if symbol:
                result.setdefault(symbol, set()).update(_theme_ids_from_item(item))
    return result


def build_a1_manifest(
    snapshot_data: Mapping[str, Any],
    outputs_by_lane: Mapping[str, Mapping[str, Any]],
    *,
    mode: str,
    snapshot_id: str,
    snapshot_hash: str,
    as_of: datetime | str,
    base_generation_id: str | None = None,
    delta: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the immutable audit manifest for a sealed A1 generation."""

    normalized_mode = str(mode or "").strip().upper()
    if normalized_mode not in A1_GENERATION_MODES:
        raise ValueError("A1 generation mode must be FULL or INCREMENTAL")
    symbols = _symbols(snapshot_data.get("g0_symbols", snapshot_data.get("g0", ())))
    candidates = _candidate_map(snapshot_data)
    fingerprints = _symbol_fingerprints(snapshot_data, symbols)
    candidate_themes: dict[str, list[str]] = {symbol: [] for symbol in symbols}
    theme_fingerprints: dict[str, str] = {}
    for output in outputs_by_lane.values():
        if not isinstance(output, Mapping):
            continue
        for symbol, themes in _output_symbol_themes(output).items():
            candidate_themes.setdefault(symbol, [])
            candidate_themes[symbol] = sorted(set(candidate_themes[symbol]).union(themes))
        for key in ("structural_themes", "industry_chain_graph"):
            for item in _mapping_list(output.get(key)):
                identifier = str(item.get("theme_id") or item.get("node_id") or "").strip()
                if identifier:
                    theme_fingerprints[identifier] = content_hash(item)
    # A snapshot can explicitly expose changed/new theme ids before the model
    # request.  Preserve these hints for the next incremental scope decision.
    for key in ("A1_THEME_FINGERPRINTS", "a1_theme_fingerprints"):
        raw = snapshot_data.get(key)
        if isinstance(raw, Mapping):
            theme_fingerprints.update({str(k): content_hash(v) for k, v in raw.items() if str(k).strip()})
    global_input_hash = a1_global_input_hash(snapshot_data)
    partition_symbols_by_lane: dict[str, list[str]] = {}
    outside_g0_research_symbols_by_lane: dict[str, list[str]] = {}
    g0_set = set(symbols)
    for lane_id, output in outputs_by_lane.items():
        outside = list(_verified_outside_g0_symbols(output, g0_set))
        normalized_lane = str(lane_id)
        outside_g0_research_symbols_by_lane[normalized_lane] = outside
        partition_symbols_by_lane[normalized_lane] = sorted(g0_set.union(outside))
    manifest: dict[str, Any] = {
        "schema_version": A1_REGISTRY_SCHEMA,
        "mode": normalized_mode,
        "snapshot_id": str(snapshot_id),
        "snapshot_hash": str(snapshot_hash),
        "as_of": _aware(as_of).isoformat(),
        "g0_symbols": list(symbols),
        "g0_count": len(symbols),
        "candidate_fingerprints": fingerprints,
        "candidate_themes": candidate_themes,
        "theme_fingerprints": theme_fingerprints,
        "global_input_hash": global_input_hash,
        "a1_global_input_hash": global_input_hash,
        "lane_ids": sorted(str(key) for key in outputs_by_lane),
        "partition_names": list(A1_OUTPUT_PARTITIONS),
        # A1 normally partitions G0 exactly.  Verified current-month broker
        # gold rows may additionally enter research outside G0, but are
        # explicitly research-only and are declared per lane for strict
        # registry validation.  Older manifests without these fields retain
        # the original G0-only contract.
        "partition_symbols_by_lane": partition_symbols_by_lane,
        "outside_g0_research_symbols_by_lane": outside_g0_research_symbols_by_lane,
        "base_generation_id": base_generation_id,
        "delta": dict(delta or {}),
    }
    # Keep the candidate count in the manifest even when a malformed adapter
    # supplied duplicate/omitted rows; the full g0 contract is authoritative.
    manifest["candidate_record_count"] = len(candidates)
    return manifest


def compute_incremental_scope(
    snapshot_data: Mapping[str, Any],
    base_manifest: Mapping[str, Any],
    *,
    base_output: Mapping[str, Any] | None = None,
    changed_theme_ids: Sequence[str] | None = None,
    new_theme_ids: Sequence[str] | None = None,
) -> A1IncrementalScope:
    """Determine added/changed/theme-affected symbols without a full A1 run."""

    current_symbols = set(_symbols(snapshot_data.get("g0_symbols", snapshot_data.get("g0", ()))))
    old_symbols = set(_symbols(base_manifest.get("g0_symbols", ())))
    old_fingerprints = _manifest_symbol_fingerprints(base_manifest)
    current_fingerprints = _symbol_fingerprints(snapshot_data, current_symbols)
    current_global_hash = a1_global_input_hash(snapshot_data)
    base_global_hash_raw = base_manifest.get("global_input_hash") or base_manifest.get("a1_global_input_hash")
    base_global_hash = str(base_global_hash_raw).strip() if base_global_hash_raw else None
    # A generation created before the global hash contract was introduced is
    # conservatively treated as needing one macro refresh, without widening
    # the candidate delta to the complete market.
    global_changed = base_global_hash != current_global_hash
    added = current_symbols - old_symbols
    removed = old_symbols - current_symbols
    changed = {
        symbol
        for symbol in current_symbols.intersection(old_symbols)
        if old_fingerprints.get(symbol) != current_fingerprints.get(symbol)
    }
    # Explicit theme hints are preferred.  In their absence, a candidate's
    # own theme binding is enough to scope rows affected by a changed theme;
    # no unrelated full-market rows are pulled into the weekly request.
    changed_themes = {
        str(item).strip()
        for item in (*tuple(changed_theme_ids or ()), *tuple(new_theme_ids or ()))
        if str(item).strip()
    }
    current_theme_fingerprints = snapshot_data.get("A1_THEME_FINGERPRINTS")
    base_theme_fingerprints = base_manifest.get("theme_fingerprints")
    if isinstance(current_theme_fingerprints, Mapping) and isinstance(base_theme_fingerprints, Mapping):
        changed_themes.update(
            str(theme_id).strip()
            for theme_id, fingerprint in current_theme_fingerprints.items()
            if str(theme_id).strip()
            and str(base_theme_fingerprints.get(theme_id) or "") != content_hash(fingerprint)
        )
    old_themes = _manifest_symbol_themes(base_manifest)
    current_themes: dict[str, set[str]] = {}
    candidates = _candidate_map(snapshot_data)
    for symbol, item in candidates.items():
        if isinstance(item, Mapping):
            current_themes[symbol] = _theme_ids_from_item(item)
    if base_output is not None:
        for symbol, themes in _output_symbol_themes(base_output).items():
            old_themes.setdefault(symbol, set()).update(themes)
    theme_affected = {
        symbol
        for symbol in current_symbols
        if symbol not in added
        and bool((current_themes.get(symbol, set()) | old_themes.get(symbol, set())).intersection(changed_themes))
    }
    processed = added | changed | theme_affected
    unchanged = current_symbols - processed
    reasons: list[str] = []
    if global_changed:
        reasons.append("A1_GLOBAL_INPUT_CHANGED")
    if not processed and not global_changed:
        reasons.append("A1_INCREMENTAL_NO_CHANGES")
    return A1IncrementalScope(
        symbols=tuple(sorted(processed)),
        added_symbols=tuple(sorted(added)),
        changed_symbols=tuple(sorted(changed)),
        theme_affected_symbols=tuple(sorted(theme_affected)),
        removed_symbols=tuple(sorted(removed)),
        unchanged_symbols=tuple(sorted(unchanged)),
        global_input_changed=global_changed,
        current_global_input_hash=current_global_hash,
        base_global_input_hash=base_global_hash,
        reason_codes=tuple(reasons),
    )


def merge_a1_partitions(
    base_output: Mapping[str, Any],
    delta_output: Mapping[str, Any],
    *,
    updated_symbols: Sequence[str],
    removed_symbols: Sequence[str] = (),
) -> dict[str, Any]:
    """Merge only changed A1 rows while retaining complete old partitions."""

    updated = {str(item).strip().upper() for item in (*tuple(updated_symbols), *tuple(removed_symbols)) if str(item).strip()}
    result = dict(base_output)
    for key, value in delta_output.items():
        if key not in A1_OUTPUT_PARTITIONS and key not in {"envelope", "analysis_summary"}:
            # Discovery/contract fields are generation-level metadata.  A
            # valid incremental response may replace them; absent fields keep
            # the previous immutable value.
            result[key] = value
    for partition in A1_OUTPUT_PARTITIONS:
        old_rows = _mapping_list(base_output.get(partition))
        new_rows = _mapping_list(delta_output.get(partition))
        kept = [row for row in old_rows if _symbol_from_item(row) not in updated]
        combined = kept + new_rows
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for row in combined:
            symbol = _symbol_from_item(row)
            key = symbol or content_hash(row)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        result[partition] = deduped
    # Preserve the server-owned envelope identity but expose the newest audit
    # metadata from the delta response when available.
    if isinstance(base_output.get("envelope"), Mapping):
        result["envelope"] = dict(base_output["envelope"])
        if isinstance(delta_output.get("envelope"), Mapping):
            for key in ("schema_version", "config_version", "prompt_version", "market_regime"):
                if key in delta_output["envelope"]:
                    result["envelope"][key] = delta_output["envelope"][key]
    # A summary belongs to the complete merged generation, not to the delta
    # request.  Retain any model-provided extra fields but refresh the
    # partition counts so consumers never see the old baseline totals.
    summary = result.get("analysis_summary")
    summary_data = dict(summary) if isinstance(summary, Mapping) else {}
    summary_data.update(
        {
            "outcome": "A1_INCREMENTAL_MERGED",
            "approved_count": len(result.get("active_research_pool", ())),
            "monitor_count": len(result.get("monitor_pool", ())),
            "rejected_count": len(result.get("rejected_candidates", ())),
        }
    )
    result["analysis_summary"] = summary_data
    return result


def validate_a1_generation_contract(
    manifest: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    generation_id: str | None = None,
    mode: str | None = None,
    snapshot_id: str | None = None,
    snapshot_hash: str | None = None,
) -> None:
    """Reject a generation that cannot safely become the daily A2 input.

    A1 is a complete partition of the deterministic G0 universe plus any
    explicitly declared, verified broker-gold research-only rows for every
    published lane.  Enforcing that invariant at the registry boundary keeps
    a partial model response, an interrupted incremental merge, a hallucinated
    symbol, or mismatched metadata from ever becoming the active pointer.
    """

    if manifest.get("schema_version") != A1_REGISTRY_SCHEMA:
        raise A1RegistryError("A1_MANIFEST_SCHEMA_INVALID")
    if payload.get("schema_version") != A1_REGISTRY_SCHEMA:
        raise A1RegistryError("A1_PAYLOAD_SCHEMA_INVALID")
    expected_mode = str(mode or manifest.get("mode") or "").strip().upper()
    if expected_mode not in A1_GENERATION_MODES or str(manifest.get("mode") or "").strip().upper() != expected_mode:
        raise A1RegistryError("A1_GENERATION_MODE_MISMATCH")
    if payload.get("mode") is not None and str(payload.get("mode") or "").strip().upper() != expected_mode:
        raise A1RegistryError("A1_GENERATION_MODE_MISMATCH")
    for field, expected in (("snapshot_id", snapshot_id), ("snapshot_hash", snapshot_hash)):
        manifest_value = str(manifest.get(field) or "").strip()
        payload_value = str(payload.get(field) or "").strip()
        expected_value = str(expected or manifest_value).strip()
        if not manifest_value or manifest_value != expected_value:
            raise A1RegistryError("A1_GENERATION_SNAPSHOT_MISMATCH")
        if payload_value and payload_value != expected_value:
            raise A1RegistryError("A1_GENERATION_SNAPSHOT_MISMATCH")
    if generation_id and payload.get("generation_id") is not None:
        if str(payload.get("generation_id") or "").strip() != str(generation_id).strip():
            raise A1RegistryError("A1_GENERATION_ID_MISMATCH")

    g0_symbols = _symbols(manifest.get("g0_symbols"))
    g0_set = set(g0_symbols)
    try:
        declared_g0_count = int(manifest.get("g0_count"))
    except (TypeError, ValueError):
        declared_g0_count = -1
    if not g0_symbols or declared_g0_count != len(g0_symbols):
        raise A1RegistryError("A1_G0_MANIFEST_INVALID")
    lanes = payload.get("lanes")
    if not isinstance(lanes, Mapping) or not lanes:
        raise A1RegistryError("A1_GENERATION_LANES_EMPTY")
    manifest_lanes = {
        str(item).strip()
        for item in manifest.get("lane_ids", ())
        if str(item).strip()
    }
    payload_lanes = {str(item).strip() for item in lanes if str(item).strip()}
    if manifest_lanes != payload_lanes:
        raise A1RegistryError("A1_GENERATION_LANES_MISMATCH")

    declared_partitions = manifest.get("partition_symbols_by_lane")
    declared_outside = manifest.get("outside_g0_research_symbols_by_lane")
    if declared_partitions is not None and not isinstance(declared_partitions, Mapping):
        raise A1RegistryError("A1_PARTITION_DOMAIN_INVALID")
    if declared_outside is not None and not isinstance(declared_outside, Mapping):
        raise A1RegistryError("A1_PARTITION_DOMAIN_INVALID")

    for lane_id, lane in lanes.items():
        if not isinstance(lane, Mapping) or not isinstance(lane.get("output"), Mapping):
            raise A1RegistryError("A1_LANE_OUTPUT_INVALID", diagnostics={"lane_id": str(lane_id)})
        if not str(lane.get("status") or "").strip().upper().startswith("VALIDATED"):
            raise A1RegistryError("A1_LANE_STATUS_INVALID", diagnostics={"lane_id": str(lane_id)})
        output = lane["output"]
        normalized_lane = str(lane_id).strip()
        verified_outside = set(_verified_outside_g0_symbols(output, g0_set))
        manifest_outside = (
            set(_symbols(declared_outside.get(normalized_lane)))
            if isinstance(declared_outside, Mapping)
            else set()
        )
        if manifest_outside != verified_outside:
            raise A1RegistryError(
                "A1_OUTSIDE_G0_RESEARCH_CONTRACT_INVALID",
                diagnostics={"lane_id": normalized_lane},
            )
        expected_symbols = g0_set.union(verified_outside)
        if isinstance(declared_partitions, Mapping):
            declared_symbols = set(_symbols(declared_partitions.get(normalized_lane)))
            if declared_symbols != expected_symbols:
                raise A1RegistryError(
                    "A1_PARTITION_DOMAIN_INVALID",
                    diagnostics={"lane_id": normalized_lane},
                )
        seen: set[str] = set()
        for partition in A1_OUTPUT_PARTITIONS:
            rows = output.get(partition)
            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                raise A1RegistryError(
                    "A1_PARTITION_INVALID",
                    diagnostics={"lane_id": str(lane_id), "partition": partition},
                )
            for row in rows:
                if not isinstance(row, Mapping):
                    raise A1RegistryError("A1_PARTITION_ROW_INVALID")
                symbol = _symbol_from_item(row)
                if not symbol or symbol not in expected_symbols:
                    raise A1RegistryError(
                        "A1_PARTITION_SYMBOL_INVALID",
                        diagnostics={"lane_id": str(lane_id), "partition": partition, "symbol": symbol},
                    )
                if symbol in verified_outside and (
                    partition != "active_research_pool"
                    or str(row.get("selection_basis") or row.get("research_route") or "").strip().upper()
                    != "BROKER_GOLD_DIRECT"
                    or row.get("downstream_trade_eligible") is not False
                ):
                    raise A1RegistryError(
                        "A1_OUTSIDE_G0_RESEARCH_CONTRACT_INVALID",
                        diagnostics={"lane_id": normalized_lane, "partition": partition, "symbol": symbol},
                    )
                if symbol in seen:
                    raise A1RegistryError(
                        "A1_PARTITION_DUPLICATE_SYMBOL",
                        diagnostics={"lane_id": str(lane_id), "symbol": symbol},
                    )
                seen.add(symbol)
        if seen != expected_symbols:
            raise A1RegistryError(
                "A1_PARTITION_COVERAGE_INCOMPLETE",
                diagnostics={
                    "lane_id": str(lane_id),
                    "expected_count": len(expected_symbols),
                    "actual_count": len(seen),
                    "missing_count": len(expected_symbols - seen),
                },
            )


class A1Registry:
    """SQLite-backed immutable generation registry with one active pointer."""

    def __init__(self, path: str | Path):
        candidate = Path(path)
        if candidate.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            candidate = candidate / "a1_registry.sqlite3"
        candidate.parent.mkdir(parents=True, exist_ok=True)
        self.path = candidate.resolve()
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
            factory=_ClosingConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        # Publishing A1 is infrequent and controls every daily A2/A3 run.
        # Prefer durable pointer commits over the marginal write-speed gain of
        # NORMAL; reads remain WAL-backed.
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS a1_generations (
                    generation_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL CHECK(mode IN ('FULL','INCREMENTAL')),
                    status TEXT NOT NULL CHECK(status IN ('STAGING','SEALED','FAILED')),
                    snapshot_id TEXT NOT NULL,
                    snapshot_hash TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    base_generation_id TEXT,
                    manifest_json TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    sealed_at TEXT,
                    failed_at TEXT,
                    failure_reason TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_a1_generations_as_of
                    ON a1_generations(as_of DESC, created_at DESC);
                CREATE TABLE IF NOT EXISTS a1_active_pointer (
                    pointer_name TEXT PRIMARY KEY CHECK(pointer_name='A1'),
                    generation_id TEXT NOT NULL,
                    activated_at TEXT NOT NULL,
                    previous_generation_id TEXT,
                    FOREIGN KEY(generation_id) REFERENCES a1_generations(generation_id),
                    FOREIGN KEY(previous_generation_id) REFERENCES a1_generations(generation_id)
                );
                """
            )

    @staticmethod
    def _new_id(mode: str, now: datetime) -> str:
        return f"a1-{mode.lower()}-{now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _row_to_generation(row: sqlite3.Row, pointer: sqlite3.Row | None = None) -> A1Generation:
        try:
            manifest = json.loads(str(row["manifest_json"]))
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise A1RegistryError("A1_GENERATION_CORRUPT") from exc
        if not isinstance(manifest, Mapping) or not isinstance(payload, Mapping):
            raise A1RegistryError("A1_GENERATION_CORRUPT")
        if content_hash(manifest) != str(row["manifest_hash"]) or content_hash(payload) != str(row["payload_hash"]):
            raise A1RegistryError("A1_GENERATION_HASH_MISMATCH")
        if str(row["status"]) == "SEALED":
            validate_a1_generation_contract(
                manifest,
                payload,
                generation_id=str(row["generation_id"]),
                mode=str(row["mode"]),
                snapshot_id=str(row["snapshot_id"]),
                snapshot_hash=str(row["snapshot_hash"]),
            )
        return A1Generation(
            generation_id=str(row["generation_id"]),
            mode=str(row["mode"]),
            status=str(row["status"]),
            snapshot_id=str(row["snapshot_id"]),
            snapshot_hash=str(row["snapshot_hash"]),
            as_of=_aware(str(row["as_of"])),
            base_generation_id=str(row["base_generation_id"]) if row["base_generation_id"] else None,
            manifest=dict(manifest),
            payload=dict(payload),
            manifest_hash=str(row["manifest_hash"]),
            payload_hash=str(row["payload_hash"]),
            created_at=_aware(str(row["created_at"])),
            sealed_at=_aware(str(row["sealed_at"])) if row["sealed_at"] else None,
            failed_at=_aware(str(row["failed_at"])) if row["failed_at"] else None,
            failure_reason=str(row["failure_reason"]) if row["failure_reason"] else None,
            activated_at=_aware(str(pointer["activated_at"])) if pointer is not None and pointer["activated_at"] else None,
            previous_generation_id=(
                str(pointer["previous_generation_id"])
                if pointer is not None and pointer["previous_generation_id"]
                else None
            ),
        )

    def create_generation(
        self,
        *,
        mode: str,
        snapshot_id: str,
        snapshot_hash: str,
        as_of: datetime | str,
        manifest: Mapping[str, Any],
        payload: Mapping[str, Any],
        base_generation_id: str | None = None,
        generation_id: str | None = None,
        created_at: datetime | str | None = None,
    ) -> A1Generation:
        normalized_mode = str(mode or "").strip().upper()
        if normalized_mode not in A1_GENERATION_MODES:
            raise ValueError("A1 generation mode must be FULL or INCREMENTAL")
        if not str(snapshot_id or "").strip() or not str(snapshot_hash or "").strip():
            raise ValueError("A1 snapshot identity is required")
        if not isinstance(manifest, Mapping) or not isinstance(payload, Mapping):
            raise TypeError("A1 manifest and payload must be mappings")
        current = _aware(created_at or as_of)
        identifier = str(generation_id or self._new_id(normalized_mode, current)).strip()
        manifest_data = dict(manifest)
        payload_data = dict(payload)
        with self._lock, self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO a1_generations(
                        generation_id,mode,status,snapshot_id,snapshot_hash,as_of,
                        base_generation_id,manifest_json,manifest_hash,payload_json,
                        payload_hash,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        identifier,
                        normalized_mode,
                        "STAGING",
                        str(snapshot_id),
                        str(snapshot_hash),
                        _aware(as_of).isoformat(),
                        base_generation_id,
                        canonical_json(manifest_data),
                        content_hash(manifest_data),
                        canonical_json(payload_data),
                        content_hash(payload_data),
                        current.isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise A1RegistryError("A1_GENERATION_ID_CONFLICT") from exc
            row = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
        if row is None:
            raise A1RegistryError("A1_GENERATION_CREATE_FAILED")
        return self._row_to_generation(row)

    begin_generation = create_generation

    def get_generation(self, generation_id: str) -> A1Generation | None:
        identifier = str(generation_id or "").strip()
        if not identifier:
            return None
        with self._lock, self._connect() as connection:
            row = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
        return self._row_to_generation(row) if row is not None else None

    def seal_generation(
        self,
        generation_id: str,
        *,
        manifest: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        sealed_at: datetime | str | None = None,
    ) -> A1Generation:
        identifier = str(generation_id or "").strip()
        current = _aware(sealed_at or datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            row = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
            if row is None:
                raise A1GenerationNotFound("A1_GENERATION_NOT_FOUND")
            if str(row["status"]) == "SEALED":
                return self._row_to_generation(row)
            if str(row["status"]) != "STAGING":
                raise A1RegistryError("A1_GENERATION_NOT_SEALABLE")
            manifest_data = dict(manifest) if isinstance(manifest, Mapping) else json.loads(str(row["manifest_json"]))
            payload_data = dict(payload) if isinstance(payload, Mapping) else json.loads(str(row["payload_json"]))
            if not isinstance(manifest_data, Mapping) or not isinstance(payload_data, Mapping):
                raise A1RegistryError("A1_GENERATION_CORRUPT")
            validate_a1_generation_contract(
                manifest_data,
                payload_data,
                generation_id=identifier,
                mode=str(row["mode"]),
                snapshot_id=str(row["snapshot_id"]),
                snapshot_hash=str(row["snapshot_hash"]),
            )
            connection.execute(
                """
                UPDATE a1_generations
                   SET status='SEALED', manifest_json=?, manifest_hash=?,
                       payload_json=?, payload_hash=?, sealed_at=?
                 WHERE generation_id=? AND status='STAGING'
                """,
                (
                    canonical_json(manifest_data),
                    content_hash(manifest_data),
                    canonical_json(payload_data),
                    content_hash(payload_data),
                    current.isoformat(),
                    identifier,
                ),
            )
            updated = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
        if updated is None:
            raise A1RegistryError("A1_GENERATION_SEAL_FAILED")
        return self._row_to_generation(updated)

    def fail_generation(
        self,
        generation_id: str,
        reason_code: str,
        *,
        failed_at: datetime | str | None = None,
    ) -> A1Generation:
        identifier = str(generation_id or "").strip()
        reason = str(reason_code or "A1_MAINTENANCE_FAILED").strip()
        current = _aware(failed_at or datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            row = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
            if row is None:
                raise A1GenerationNotFound("A1_GENERATION_NOT_FOUND")
            if str(row["status"]) == "SEALED":
                raise A1RegistryError("A1_GENERATION_IMMUTABLE")
            connection.execute(
                "UPDATE a1_generations SET status='FAILED', failed_at=?, failure_reason=? WHERE generation_id=? AND status='STAGING'",
                (current.isoformat(), reason, identifier),
            )
            updated = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
        if updated is None:
            raise A1RegistryError("A1_GENERATION_FAILURE_RECORD_FAILED")
        return self._row_to_generation(updated)

    mark_failed = fail_generation

    def get_active_generation(self) -> A1Generation | None:
        with self._lock, self._connect() as connection:
            pointer = connection.execute(
                "SELECT * FROM a1_active_pointer WHERE pointer_name='A1'"
            ).fetchone()
            if pointer is None:
                return None
            row = connection.execute(
                "SELECT * FROM a1_generations WHERE generation_id=? AND status='SEALED'",
                (pointer["generation_id"],),
            ).fetchone()
        if row is None:
            raise A1RegistryError("A1_ACTIVE_POINTER_CORRUPT")
        return self._row_to_generation(row, pointer)

    get_active = get_active_generation
    active_generation = get_active_generation

    def require_active(
        self,
        *,
        as_of: datetime | str,
        max_age: timedelta = DEFAULT_A1_MAX_AGE,
    ) -> A1Generation:
        generation = self.get_active_generation()
        if generation is None:
            raise A1RegistryError("A1_ACTIVE_MISSING")
        current = _aware(as_of)
        age = current - generation.as_of
        if age.total_seconds() < 0:
            raise A1RegistryError("A1_ACTIVE_AS_OF_IN_FUTURE")
        if age > max_age:
            raise A1RegistryError(
                "A1_ACTIVE_EXPIRED",
                diagnostics={"age_seconds": int(age.total_seconds()), "max_age_seconds": int(max_age.total_seconds())},
            )
        return generation

    def activate_generation(
        self,
        generation_id: str,
        *,
        expected_current_id: str | None = None,
        activated_at: datetime | str | None = None,
    ) -> A1Generation:
        """Atomically swap the active pointer using an optimistic CAS."""

        identifier = str(generation_id or "").strip()
        current = _aware(activated_at or datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                pointer = connection.execute(
                    "SELECT * FROM a1_active_pointer WHERE pointer_name='A1'"
                ).fetchone()
                current_id = str(pointer["generation_id"]) if pointer is not None else None
                if current_id != expected_current_id:
                    raise A1ActivationError(
                        "A1_ACTIVE_POINTER_CONFLICT",
                        diagnostics={"expected_current_id": expected_current_id, "actual_current_id": current_id},
                    )
                row = connection.execute(
                    "SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)
                ).fetchone()
                if row is None:
                    raise A1GenerationNotFound("A1_GENERATION_NOT_FOUND")
                if str(row["status"]) != "SEALED":
                    raise A1ActivationError("A1_GENERATION_NOT_SEALED")
                connection.execute(
                    """
                    INSERT INTO a1_active_pointer(pointer_name,generation_id,activated_at,previous_generation_id)
                    VALUES('A1',?,?,?)
                    ON CONFLICT(pointer_name) DO UPDATE SET
                        generation_id=excluded.generation_id,
                        activated_at=excluded.activated_at,
                        previous_generation_id=excluded.previous_generation_id
                    """,
                    (identifier, current.isoformat(), current_id),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            pointer = connection.execute(
                "SELECT * FROM a1_active_pointer WHERE pointer_name='A1'"
            ).fetchone()
            updated = connection.execute("SELECT * FROM a1_generations WHERE generation_id=?", (identifier,)).fetchone()
        if updated is None:
            raise A1ActivationError("A1_ACTIVE_POINTER_WRITE_FAILED")
        return self._row_to_generation(updated, pointer)


def default_a1_registry_path(settings: Any) -> Path:
    """Resolve a registry path without expanding the Settings contract."""

    configured = getattr(settings, "a1_registry_db_path", None)
    if configured:
        return Path(configured).resolve()
    state_db = getattr(settings, "state_db_path", None)
    if state_db:
        return Path(state_db).resolve().parent / "a1_registry.sqlite3"
    root = Path(getattr(settings, "root", Path.cwd())).resolve()
    return root / "state" / "a1_registry.sqlite3"


__all__ = [
    "A1ActivationError",
    "A1Generation",
    "A1GenerationNotFound",
    "A1IncrementalScope",
    "A1Registry",
    "A1RegistryError",
    "A1_FULL",
    "A1_INCREMENTAL",
    "A1_OUTPUT_PARTITIONS",
    "A1_REGISTRY_SCHEMA",
    "DEFAULT_A1_DEGRADED_AFTER",
    "DEFAULT_A1_MAX_AGE",
    "a1_global_input_hash",
    "build_a1_manifest",
    "canonical_json",
    "compute_incremental_scope",
    "content_hash",
    "default_a1_registry_path",
    "merge_a1_partitions",
    "validate_a1_generation_contract",
]
