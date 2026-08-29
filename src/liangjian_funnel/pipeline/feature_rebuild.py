"""Durable incremental and weekly feature-generation maintenance.

The research feature store is deliberately append-only by generation.  This
module provides the maintenance-plane consumer for ``dirty_entities`` and a
full-rebuild path with the same staging/validation/publish contract.  Builders
receive one entity at a time and must write only to the supplied staging
generation; raw fact storage is never opened or modified here.

The queue and generation lifecycle are independent of the research workflow:
the maintenance job can be stopped, restarted, or run from another process
without changing the generation bound to an active research run.
"""

from __future__ import annotations

import inspect
import json
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .feature_store import (
    DEFAULT_DIRTY_LEASE_SECONDS,
    FeatureGenerationError,
    ResearchFeatureStore,
    content_hash,
)


FEATURE_REBUILD_CONTRACT = "feature-rebuild/1.0.0"
FEATURE_REBUILD_ALGORITHM = "feature-rebuild-staging-v1"

# Every table below is a rebuildable projection with a generation_id.  Raw
# facts live in LocalFactCache and are intentionally absent from this list.
_GENERATION_TABLES: tuple[str, ...] = (
    "feature_generation_members",
    "taxonomy_membership_versions",
    "theme_registry_versions",
    "chain_node_versions",
    "theme_taxonomy_links",
    "business_exposure_facts",
    "stock_fundamental_features",
    "stock_market_role_features",
    "deterministic_stage_decisions",
)

# A maintenance snapshot owns durable stock inputs and taxonomy/business
# projections.  Theme, chain, market-role, and stage-decision rows are bound
# to a research run and must not be copied or counted as maintenance coverage.
_MAINTENANCE_GENERATION_TABLES: tuple[str, ...] = (
    "feature_generation_members",
    "taxonomy_membership_versions",
    "business_exposure_facts",
    "stock_fundamental_features",
)
_RUN_SCOPED_TABLES: tuple[str, ...] = (
    "theme_registry_versions",
    "chain_node_versions",
    "theme_taxonomy_links",
    "stock_market_role_features",
    "deterministic_stage_decisions",
)
FEATURE_VALIDATION_SCHEMA = "feature-validation/2.0.0"


class EntityBuilder(Protocol):
    """Build one entity into a supplied staging generation."""

    def __call__(
        self,
        entity: Mapping[str, Any],
        generation_id: str,
        store: ResearchFeatureStore,
    ) -> Mapping[str, Any] | None:
        ...


class GenerationValidator(Protocol):
    """Validate a staging generation and return safe validation metadata."""

    def __call__(
        self, store: ResearchFeatureStore, generation_id: str
    ) -> Mapping[str, Any] | None:
        ...


class DependencyExpander(Protocol):
    """Expand a leased queue batch to the entities it invalidates."""

    def __call__(
        self, items: Iterable[Mapping[str, Any]], max_depth: int = 8
    ) -> Iterable[Mapping[str, Any]]:
        ...


@dataclass(frozen=True, slots=True)
class FeatureRebuildResult:
    """Serializable maintenance outcome used by CLI/API adapters."""

    mode: str
    status: str
    generation_id: str | None = None
    previous_generation_id: str | None = None
    claimed_count: int = 0
    expanded_count: int = 0
    processed_count: int = 0
    resolved_count: int = 0
    retry_count: int = 0
    dead_count: int = 0
    validation: Mapping[str, Any] | None = None
    reason_code: str | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "status": self.status,
            "generation_id": self.generation_id,
            "previous_generation_id": self.previous_generation_id,
            "claimed_count": self.claimed_count,
            "expanded_count": self.expanded_count,
            "processed_count": self.processed_count,
            "resolved_count": self.resolved_count,
            "retry_count": self.retry_count,
            "dead_count": self.dead_count,
            "validation": None if self.validation is None else dict(self.validation),
            "reason_code": self.reason_code,
            "error": self.error,
        }

    def __getitem__(self, key: str) -> Any:
        """Allow compatibility with callers that expect a result mapping."""

        return self.as_dict()[key]


def _now(value: datetime | str | None = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise TypeError("now must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _entity_key(entity: Mapping[str, Any]) -> tuple[str, str]:
    entity_type = str(entity.get("entity_type") or entity.get("type") or "").strip().upper()
    entity_id = str(entity.get("entity_id") or entity.get("id") or "").strip()
    if not entity_type or not entity_id:
        raise ValueError("rebuild entity requires entity_type and entity_id")
    return entity_type, entity_id


def _normalize_entity(entity: Any) -> dict[str, Any]:
    if isinstance(entity, Mapping):
        value = dict(entity)
        entity_type, entity_id = _entity_key(value)
        value["entity_type"] = entity_type
        value["entity_id"] = entity_id
        return value
    if isinstance(entity, Sequence) and not isinstance(entity, (str, bytes)) and len(entity) >= 2:
        return {"entity_type": str(entity[0]).strip().upper(), "entity_id": str(entity[1]).strip()}
    raise TypeError("rebuild entities must be mappings or (entity_type, entity_id) pairs")


def _dedupe_entities(items: Iterable[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        normalized = _normalize_entity(item)
        key = _entity_key(normalized)
        if key in by_key:
            # Preserve queue metadata from the most specific record while
            # keeping the first occurrence's deterministic order.
            by_key[key].update({k: v for k, v in normalized.items() if v is not None})
            continue
        by_key[key] = normalized
        result.append(normalized)
    return result


def _invoke(callback: Callable[..., Any], *args: Any) -> Any:
    """Call a test/embedding callback without swallowing body TypeErrors."""

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args)
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )
    if has_varargs:
        return callback(*args)
    return callback(*args[: len(positional)])


def clone_feature_generation(
    store: ResearchFeatureStore,
    source_generation_id: str,
    target_generation_id: str,
    *,
    include_runtime_projections: bool = True,
) -> dict[str, int]:
    """Copy projections from a published generation into staging.

    This is an internal, generation-scoped copy.  It does not truncate or
    update the source generation and never touches LocalFactCache raw facts.
    A changed entity can then be replaced in the staging generation while
    unchanged entities retain their last published projection.
    """

    source = str(source_generation_id or "").strip()
    target = str(target_generation_id or "").strip()
    if not source or not target or source == target:
        raise ValueError("source and target generation ids must be distinct")
    counts: dict[str, int] = {}
    tables = (
        _GENERATION_TABLES
        if include_runtime_projections
        else _MAINTENANCE_GENERATION_TABLES
    )
    with store._connect() as connection:  # noqa: SLF001 - package-private lifecycle helper
        connection.execute("BEGIN IMMEDIATE")
        try:
            source_row = store._assert_generation(  # noqa: SLF001
                connection, source, strict=True, allow_legacy=False
            )
            store._assert_generation(  # noqa: SLF001
                connection, target, strict=False, allow_legacy=False, for_write=True
            )
            if str(source_row["status"]).upper() not in {"SEALED", "PUBLISHED"}:
                raise FeatureGenerationError("FEATURE_GENERATION_SOURCE_NOT_PUBLISHED")
            for table in tables:
                columns = [
                    str(row[1])
                    for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
                ]
                if "generation_id" not in columns:
                    continue
                copied_columns = ",".join(f'"{column}"' for column in columns)
                select_columns = ",".join(
                    "?" if column == "generation_id" else f'"{column}"'
                    for column in columns
                )
                cursor = connection.execute(
                    f'INSERT INTO "{table}" ({copied_columns}) '
                    f'SELECT {select_columns} FROM "{table}" WHERE generation_id=?',
                    (target, source),
                )
                counts[table] = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return counts


def _normalise_symbols(values: Iterable[Any] | None) -> tuple[str, ...] | None:
    if values is None:
        return None
    return tuple(
        dict.fromkeys(
            str(value).strip().upper()
            for value in values
            if str(value).strip()
        )
    )


def _coverage_config(coverage_contract: Mapping[str, Any] | None) -> tuple[dict[str, bool], dict[str, str], tuple[str, ...]]:
    """Normalize the optional purpose-aware maintenance coverage contract."""

    contract = dict(coverage_contract or {})
    required_raw = contract.get("required")
    required = {
        "members": False,
        "taxonomy": False,
        "business": False,
        "fundamental": False,
    }
    if isinstance(required_raw, Mapping):
        for key in required:
            required[key] = bool(required_raw.get(key, False))
    applicability_raw = contract.get("applicability")
    applicability = {
        "theme_registry": "RUN_SCOPED",
        "chain_node": "RUN_SCOPED",
        "theme_taxonomy_links": "RUN_SCOPED",
        "market_role": "RUN_SCOPED",
        "stage_decisions": "RUN_SCOPED",
    }
    if isinstance(applicability_raw, Mapping):
        for key in applicability:
            value = str(applicability_raw.get(key) or applicability[key]).upper()
            applicability[key] = value if value in {"NOT_APPLICABLE", "RUN_SCOPED"} else "RUN_SCOPED"
    taxonomy_raw = contract.get("taxonomy_types")
    taxonomy_types = tuple(
        dict.fromkeys(
            str(value).strip().upper()
            for value in (taxonomy_raw if isinstance(taxonomy_raw, (list, tuple, set, frozenset)) else ())
            if str(value).strip()
        )
    )
    return required, applicability, taxonomy_types


def _query_distinct_symbols(
    connection: Any,
    table: str,
    generation_id: str,
    *,
    symbol_column: str = "symbol",
    where: str = "",
    params: Sequence[Any] = (),
    required_payload_key: str | None = None,
) -> set[str]:
    selected_columns = f'"{symbol_column}"'
    if required_payload_key is not None:
        selected_columns += ',"payload_json"'
    rows = connection.execute(
        f'SELECT DISTINCT {selected_columns} FROM "{table}" WHERE generation_id=?' + where,
        (str(generation_id), *params),
    ).fetchall()
    result: set[str] = set()
    for row in rows:
        symbol = str(row[0] or "").strip().upper()
        if not symbol:
            continue
        if required_payload_key is not None:
            try:
                payload = json.loads(str(row[1] or "{}"))
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, Mapping):
                continue
            value = payload.get(required_payload_key)
            if value in (None, "", (), [], {}):
                continue
        result.add(symbol)
    return result


def build_validation_manifest(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    expected_entity_count: int | None = None,
    expected_symbols: Iterable[Any] | None = None,
    purpose: str | None = None,
    coverage_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a purpose-aware validation manifest without changing state.

    ``coverage_contract`` is supplied by snapshot maintenance.  Without it,
    the historical generic validator remains compatible: only an explicitly
    requested member/entity count is required, while runtime projections are
    reported as ``RUN_SCOPED`` rather than being counted as maintenance data.
    """

    generation = store.get_feature_generation(generation_id)
    if generation is None:
        raise FeatureGenerationError("FEATURE_GENERATION_NOT_FOUND")
    generation_status = str(generation.get("status") or "").upper()
    if generation_status not in {"STAGING", "VALIDATED"}:
        raise FeatureGenerationError("FEATURE_GENERATION_INVALID_VALIDATION_STATUS")
    expected = _normalise_symbols(expected_symbols)
    expected_set = set(expected or ())
    required, applicability, taxonomy_types = _coverage_config(coverage_contract)
    if coverage_contract is None:
        required["members"] = expected_entity_count is not None or expected is not None
    purpose_name = str(purpose or generation.get("purpose") or "UNKNOWN").strip().upper()

    table_counts: dict[str, int] = {}
    invalid_payloads: dict[str, int] = {}
    invalid_hashes = 0
    entity_count = 0
    member_symbols: set[str] = set()
    taxonomy_symbols: dict[str, set[str]] = {}
    business_symbols: set[str] = set()
    fundamental_symbols: set[str] = set()
    with store._connect() as connection:  # noqa: SLF001
        for table in _GENERATION_TABLES:
            row = connection.execute(
                f'SELECT COUNT(*) AS count FROM "{table}" WHERE generation_id=?',
                (str(generation_id),),
            ).fetchone()
            table_counts[table] = int(row["count"] if row is not None else 0)
            columns = {
                str(column[1])
                for column in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            }
            if "payload_json" in columns:
                invalid = connection.execute(
                    f'SELECT COUNT(*) AS count FROM "{table}" '
                    "WHERE generation_id=? AND (payload_json IS NULL OR json_valid(payload_json)=0)",
                    (str(generation_id),),
                ).fetchone()
                invalid_payloads[table] = int(invalid["count"] if invalid is not None else 0)
        rows = connection.execute(
            "SELECT content_hash FROM feature_generation_members WHERE generation_id=?",
            (str(generation_id),),
        ).fetchall()
        invalid_hashes = sum(
            1
            for row in rows
            if len(str(row["content_hash"] or "")) != 64
            or any(character not in "0123456789abcdefABCDEF" for character in str(row["content_hash"] or ""))
        )
        row = connection.execute(
            "SELECT COUNT(DISTINCT entity_type || char(0) || entity_id) AS count "
            "FROM feature_generation_members WHERE generation_id=?",
            (str(generation_id),),
        ).fetchone()
        entity_count = int(row["count"] if row is not None else 0)
        member_symbols = _query_distinct_symbols(
            connection,
            "feature_generation_members",
            generation_id,
            symbol_column="entity_id",
            where=" AND entity_type='STOCK'",
        )
        if "taxonomy_membership_versions" in table_counts:
            taxonomy_rows = connection.execute(
                "SELECT DISTINCT taxonomy,symbol FROM taxonomy_membership_versions "
                "WHERE generation_id=?",
                (str(generation_id),),
            ).fetchall()
            for row in taxonomy_rows:
                taxonomy_symbols.setdefault(str(row[0]).strip().upper(), set()).add(
                    str(row[1]).strip().upper()
                )
        business_symbols = _query_distinct_symbols(
            connection, "business_exposure_facts", generation_id
        )
        fundamental_symbols = _query_distinct_symbols(
            connection,
            "stock_fundamental_features",
            generation_id,
            where=" AND available=1",
            required_payload_key="financial_features",
        )

    def _coverage(
        name: str,
        actual_symbols: set[str],
        *,
        applicable: str = "REQUIRED",
        required_override: bool | None = None,
    ) -> dict[str, Any]:
        is_required = required_override if required_override is not None else required.get(name, False)
        if applicable == "NOT_APPLICABLE":
            return {
                "status": "NOT_APPLICABLE",
                "applicability": "NOT_APPLICABLE",
                "required": False,
                "expected_symbols": len(expected_set),
                "actual_symbols": len(actual_symbols),
                "missing_symbols": [],
            }
        if expected is None:
            expected_count = expected_entity_count if name == "members" else None
            missing: list[str] = []
        else:
            expected_count = len(expected_set)
            missing = sorted(expected_set - actual_symbols)
        complete = not is_required or (not missing and (expected_count is None or len(actual_symbols) >= expected_count))
        return {
            "status": "READY" if complete else "INCOMPLETE",
            "applicability": "REQUIRED" if is_required else applicable,
            "required": bool(is_required),
            "expected_symbols": expected_count,
            "actual_symbols": len(actual_symbols),
            "missing_symbols": missing[:200],
        }

    coverage: dict[str, Any] = {
        "members": _coverage("members", member_symbols),
        "business": _coverage(
            "business",
            business_symbols,
            applicable="REQUIRED" if required.get("business", False) else "NOT_APPLICABLE",
        ),
        "fundamental": _coverage("fundamental", fundamental_symbols),
    }
    taxonomy_required = required.get("taxonomy", False)
    if not taxonomy_types and taxonomy_required:
        taxonomy_types = tuple(sorted(taxonomy_symbols))
    taxonomy_by_type = {
        taxonomy: _coverage(
            "taxonomy",
            taxonomy_symbols.get(taxonomy, set()),
            required_override=taxonomy_required,
        )
        for taxonomy in taxonomy_types
    }
    taxonomy_missing = sorted(
        {
            symbol
            for item in taxonomy_by_type.values()
            for symbol in item["missing_symbols"]
        }
    )
    taxonomy_complete = bool(taxonomy_types) and all(
        item["status"] == "READY" for item in taxonomy_by_type.values()
    )
    coverage["taxonomy"] = {
        "status": (
            "INCOMPLETE" if taxonomy_required and not taxonomy_complete else
            "READY" if taxonomy_required else "NOT_APPLICABLE"
        ),
        "applicability": "REQUIRED" if taxonomy_required else "NOT_APPLICABLE",
        "required": taxonomy_required,
        "taxonomy_types": list(taxonomy_types),
        "expected_symbols": len(expected_set) if expected is not None else None,
        "actual_symbols": len(set().union(*(taxonomy_symbols.get(key, set()) for key in taxonomy_types))) if taxonomy_types else 0,
        "missing_symbols": taxonomy_missing[:200],
        "by_type": taxonomy_by_type,
    }

    runtime_tables = {
        "theme_registry": "theme_registry_versions",
        "chain_node": "chain_node_versions",
        "theme_taxonomy_links": "theme_taxonomy_links",
        "market_role": "stock_market_role_features",
        "stage_decisions": "deterministic_stage_decisions",
    }
    runtime_projection = {
        name: {
            "status": applicability.get(name, "RUN_SCOPED"),
            "applicability": applicability.get(name, "RUN_SCOPED"),
            "rows": table_counts.get(table, 0),
            "counts_as_maintenance_coverage": False,
        }
        for name, table in runtime_tables.items()
    }
    failures = [
        name for name, item in coverage.items()
        if item.get("required") and item.get("status") != "READY"
    ]
    invalid_total = sum(invalid_payloads.values()) + invalid_hashes
    if invalid_total:
        failures.append("INVALID_ROWS")
    if expected_entity_count is not None and entity_count < int(expected_entity_count):
        failures.append("ENTITY_COUNT")
    failures = list(dict.fromkeys(failures))
    ready = not failures
    return {
        "schema_version": FEATURE_VALIDATION_SCHEMA,
        "contract": FEATURE_REBUILD_CONTRACT,
        "generation_id": str(generation_id),
        "purpose": purpose_name,
        "status": "READY" if ready else "INCOMPLETE",
        "activation_eligible": bool(ready and purpose_name in {"LIVE_FULL", "LIVE_INCREMENTAL"}),
        "entity_count": entity_count,
        "expected_entity_count": expected_entity_count,
        "table_counts": table_counts,
        "invalid_payloads": invalid_payloads,
        "invalid_hashes": invalid_hashes,
        "coverage": coverage,
        "runtime_projections": runtime_projection,
        "failures": failures,
        "coverage_contract": dict(coverage_contract or {}),
    }


def validate_feature_generation(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    expected_entity_count: int | None = None,
    expected_symbols: Iterable[Any] | None = None,
    purpose: str | None = None,
    coverage_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run invariants and fail closed when required coverage is missing."""

    manifest = build_validation_manifest(
        store,
        generation_id,
        expected_entity_count=expected_entity_count,
        expected_symbols=expected_symbols,
        purpose=purpose,
        coverage_contract=coverage_contract,
    )
    failures = manifest["failures"]
    if failures:
        if "INVALID_ROWS" in failures:
            invalid_total = sum(manifest["invalid_payloads"].values()) + int(manifest["invalid_hashes"])
            raise FeatureGenerationError(
                f"FEATURE_GENERATION_VALIDATION_FAILED:INVALID_ROWS:{invalid_total}"
            )
        if "ENTITY_COUNT" in failures:
            raise FeatureGenerationError(
                "FEATURE_GENERATION_VALIDATION_FAILED:ENTITY_COUNT:"
                f"{manifest['entity_count']}<{expected_entity_count}"
            )
        raise FeatureGenerationError(
            "FEATURE_GENERATION_VALIDATION_FAILED:REQUIRED_COVERAGE:"
            + ",".join(str(item) for item in failures)
        )
    return manifest


class FeatureRebuildCoordinator:
    """Run leased dirty batches and weekly full rebuilds safely."""

    def __init__(
        self,
        store: ResearchFeatureStore,
        builder: EntityBuilder,
        *,
        validator: GenerationValidator | None = None,
        dependency_expander: DependencyExpander | None = None,
        clock: Callable[[], datetime] | None = None,
        include_runtime_projections: bool = True,
    ) -> None:
        self.store = store
        self.builder = builder
        self.validator = validator or validate_feature_generation
        self.dependency_expander = dependency_expander
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.include_runtime_projections = bool(include_runtime_projections)

    def _timestamp(self, value: datetime | str | None = None) -> str:
        return _now(value if value is not None else self.clock()).isoformat()

    @staticmethod
    def _new_generation_id(mode: str) -> str:
        return f"feature-{mode.lower()}-{uuid.uuid4().hex[:24]}"

    def _create_generation(
        self,
        *,
        mode: str,
        as_of: datetime | str,
        contract_version: str,
        algorithm_version: str,
        source_manifest_hash: str,
        worker_id: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        source_hash = str(source_manifest_hash or "").strip() or content_hash(
            {
                "mode": mode,
                "as_of": str(as_of),
                "algorithm_version": algorithm_version,
            }
        )
        details = {
            "rebuild_mode": mode,
            "worker_id": worker_id,
            "contract_version": contract_version,
            **dict(metadata or {}),
        }
        return self.store.create_feature_generation(
            generation_id=self._new_generation_id(mode),
            domain="RESEARCH",
            as_of=as_of,
            contract_version=contract_version,
            algorithm_version=algorithm_version,
            source_manifest_hash=source_hash,
            created_at=self._timestamp(),
            metadata=details,
            purpose=("LIVE_INCREMENTAL" if str(mode).upper() == "INCREMENTAL" else "LIVE_FULL"),
            activation_eligible=True,
        )

    def _failure(
        self,
        *,
        mode: str,
        generation_id: str | None,
        previous_generation_id: str | None,
        claimed: Sequence[Mapping[str, Any]],
        expanded_count: int,
        processed_count: int,
        error: BaseException,
        worker_id: str,
    ) -> FeatureRebuildResult:
        error_code = f"{type(error).__name__.upper()}"
        if generation_id:
            current = self.store.get_feature_generation(generation_id)
            if current is not None and str(current.get("status") or "").upper() not in {"SEALED", "PUBLISHED"}:
                try:
                    self.store.fail_feature_generation(
                        generation_id,
                        reason=f"{error_code}:{str(error)[:160]}",
                        failed_at=self._timestamp(),
                        diagnostics={
                            "mode": mode,
                            "processed_count": processed_count,
                            "expanded_count": expanded_count,
                        },
                    )
                except Exception:
                    # The original build error remains the useful diagnostic;
                    # a second database error must not hide it.
                    pass
        retry_count = 0
        dead_count = 0
        for item in claimed:
            try:
                updated = self.store.retry_dirty(
                    entity_type=str(item["entity_type"]),
                    entity_id=str(item["entity_id"]),
                    reason_code=str(item["reason_code"]),
                    source_version=str(item["source_version"]),
                    error_code=error_code,
                    now=self._timestamp(),
                    worker_id=worker_id,
                )
                if updated is not None:
                    state = str(updated.get("status") or "").upper()
                    retry_count += int(state == "RETRY")
                    dead_count += int(state == "DEAD")
            except Exception:
                # A worker crash must not prevent the caller from seeing that
                # the generation was kept off active.  Expired leases are
                # recoverable by the next claim.
                continue
        return FeatureRebuildResult(
            mode=mode,
            status="FAILED",
            generation_id=generation_id,
            previous_generation_id=previous_generation_id,
            claimed_count=len(claimed),
            expanded_count=expanded_count,
            processed_count=processed_count,
            retry_count=retry_count,
            dead_count=dead_count,
            reason_code=error_code,
            error=str(error)[:500],
        )

    def run_incremental(
        self,
        *,
        as_of: datetime | str,
        worker_id: str,
        limit: int = 100,
        lease_seconds: int = DEFAULT_DIRTY_LEASE_SECONDS,
        contract_version: str = FEATURE_REBUILD_CONTRACT,
        algorithm_version: str = FEATURE_REBUILD_ALGORITHM,
        source_manifest_hash: str = "",
        now: datetime | str | None = None,
        max_dependency_depth: int = 8,
    ) -> FeatureRebuildResult:
        """Rebuild one leased batch into a cloned staging generation."""

        owner = str(worker_id or "").strip()
        if not owner:
            raise ValueError("worker_id must not be empty")
        claimed = self.store.claim_dirty(
            worker_id=owner,
            limit=limit,
            now=now if now is not None else self._timestamp(),
            lease_seconds=lease_seconds,
        )
        if not claimed:
            return FeatureRebuildResult(mode="INCREMENTAL", status="NOOP")

        generation_id: str | None = None
        previous_generation_id: str | None = None
        expanded: list[dict[str, Any]] = []
        all_claimed: list[dict[str, Any]] = list(claimed)
        processed_count = 0
        try:
            if self.dependency_expander is None:
                expanded = _dedupe_entities(
                    self.store.expand_dirty_dependencies(
                        claimed, max_depth=max_dependency_depth
                    )
                )
            else:
                expanded = _dedupe_entities(
                    _invoke(self.dependency_expander, claimed, max_dependency_depth)
                )
            additional = self.store.claim_dirty_items(
                items=expanded,
                worker_id=owner,
                now=now if now is not None else self._timestamp(),
                lease_seconds=lease_seconds,
            )
            all_claimed = _dedupe_queue_items([*claimed, *additional])
            active = self.store.get_active_feature_generation("RESEARCH")
            previous_generation_id = (
                str(active["generation_id"]) if active is not None else None
            )
            generation_id = self._create_generation(
                mode="INCREMENTAL",
                as_of=as_of,
                contract_version=contract_version,
                algorithm_version=algorithm_version,
                source_manifest_hash=source_manifest_hash,
                worker_id=owner,
                metadata={
                    "claimed_count": len(all_claimed),
                    "expanded_count": len(expanded),
                    "previous_generation_id": previous_generation_id,
                },
            )
            if previous_generation_id:
                clone_feature_generation(
                    self.store,
                    previous_generation_id,
                    generation_id,
                    include_runtime_projections=self.include_runtime_projections,
                )
            for entity in expanded:
                _invoke(self.builder, entity, generation_id, self.store)
                processed_count += 1
            validation = _invoke(self.validator, self.store, generation_id)
            validation_metadata = dict(validation or {})
            if validation_metadata.get("activation_eligible") is False:
                raise FeatureGenerationError("FEATURE_GENERATION_VALIDATION_FAILED:REQUIRED_COVERAGE")
            self.store.validate_feature_generation(
                generation_id,
                validated_at=self._timestamp(),
                validation=validation_metadata,
            )
            self.store.seal_generation(
                generation_id,
                validation_manifest=validation_metadata,
                purpose="LIVE_INCREMENTAL",
                activation_eligible=True,
                sealed_at=self._timestamp(),
            )
            self.store.activate_generation(
                generation_id,
                expected_current_id=previous_generation_id,
                activation_reason="INCREMENTAL_FEATURE_REBUILD",
                domain="RESEARCH",
                actor=owner,
                activated_at=self._timestamp(),
            )
            resolved_count = sum(
                int(
                    self.store.complete_dirty(
                        entity_type=str(item["entity_type"]),
                        entity_id=str(item["entity_id"]),
                        reason_code=str(item["reason_code"]),
                        source_version=str(item["source_version"]),
                        resolved_at=self._timestamp(),
                        worker_id=owner,
                    )
                )
                for item in all_claimed
            )
            return FeatureRebuildResult(
                mode="INCREMENTAL",
                status="PUBLISHED",
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                claimed_count=len(all_claimed),
                expanded_count=len(expanded),
                processed_count=processed_count,
                resolved_count=resolved_count,
                validation=validation_metadata,
            )
        except Exception as error:
            return self._failure(
                mode="INCREMENTAL",
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                claimed=all_claimed,
                expanded_count=len(expanded),
                processed_count=processed_count,
                error=error,
                worker_id=owner,
            )

    def run_full(
        self,
        *,
        entities: Iterable[Mapping[str, Any] | Sequence[Any]],
        as_of: datetime | str,
        worker_id: str = "weekly-feature-rebuild",
        contract_version: str = FEATURE_REBUILD_CONTRACT,
        algorithm_version: str = FEATURE_REBUILD_ALGORITHM,
        source_manifest_hash: str = "",
    ) -> FeatureRebuildResult:
        """Rebuild a complete universe into fresh staging and publish on success."""

        normalized = _dedupe_entities(entities)
        active = self.store.get_active_feature_generation("RESEARCH")
        previous_generation_id = (
            str(active["generation_id"]) if active is not None else None
        )
        generation_id: str | None = None
        processed_count = 0
        try:
            generation_id = self._create_generation(
                mode="FULL",
                as_of=as_of,
                contract_version=contract_version,
                algorithm_version=algorithm_version,
                source_manifest_hash=source_manifest_hash,
                worker_id=worker_id,
                metadata={
                    "entity_count": len(normalized),
                    "previous_generation_id": previous_generation_id,
                },
            )
            for entity in normalized:
                _invoke(self.builder, entity, generation_id, self.store)
                processed_count += 1
            validation = _invoke(self.validator, self.store, generation_id)
            validation_metadata = dict(validation or {})
            if validation_metadata.get("activation_eligible") is False:
                raise FeatureGenerationError("FEATURE_GENERATION_VALIDATION_FAILED:REQUIRED_COVERAGE")
            self.store.validate_feature_generation(
                generation_id,
                validated_at=self._timestamp(),
                validation=validation_metadata,
            )
            self.store.seal_generation(
                generation_id,
                validation_manifest=validation_metadata,
                purpose="LIVE_FULL",
                activation_eligible=True,
                sealed_at=self._timestamp(),
            )
            self.store.activate_generation(
                generation_id,
                expected_current_id=previous_generation_id,
                activation_reason="FULL_FEATURE_REBUILD",
                domain="RESEARCH",
                actor=worker_id,
                activated_at=self._timestamp(),
            )
            return FeatureRebuildResult(
                mode="FULL",
                status="PUBLISHED",
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                expanded_count=len(normalized),
                processed_count=processed_count,
                validation=validation_metadata,
            )
        except Exception as error:
            return self._failure(
                mode="FULL",
                generation_id=generation_id,
                previous_generation_id=previous_generation_id,
                claimed=(),
                expanded_count=len(normalized),
                processed_count=processed_count,
                error=error,
                worker_id=worker_id,
            )


# Friendly aliases for maintenance scripts and future CLI integration.
FeatureRebuildWorker = FeatureRebuildCoordinator
FeatureStoreRebuilder = FeatureRebuildCoordinator


def _dedupe_queue_items(items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for raw in items:
        if not isinstance(raw, Mapping):
            continue
        key = (
            str(raw.get("entity_type") or "").strip().upper(),
            str(raw.get("entity_id") or "").strip(),
            str(raw.get("reason_code") or "").strip().upper(),
            str(raw.get("source_version") or "").strip(),
        )
        if not all(key) or key in seen:
            continue
        seen.add(key)
        result.append(dict(raw))
    return result


__all__ = [
    "DependencyExpander",
    "EntityBuilder",
    "FEATURE_REBUILD_ALGORITHM",
    "FEATURE_REBUILD_CONTRACT",
    "FeatureRebuildCoordinator",
    "FeatureRebuildResult",
    "FeatureRebuildWorker",
    "FeatureStoreRebuilder",
    "FEATURE_VALIDATION_SCHEMA",
    "GenerationValidator",
    "build_validation_manifest",
    "clone_feature_generation",
    "validate_feature_generation",
]
