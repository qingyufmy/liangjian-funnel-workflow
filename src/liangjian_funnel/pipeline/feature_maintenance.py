"""Safe maintenance entry points for the versioned research feature store.

The maintenance plane consumes an already frozen research snapshot.  It never
fetches market data, invokes an LLM, runs a research lane, or touches the
simulation broker.  A snapshot is accepted only after its content hash and
basic G0 contract have been verified.

The builder intentionally writes the expensive full-universe projections in
bulk on the first callback of a full rebuild.  The coordinator still counts
each G0 entity as processed, but taxonomy and business facts are never
rewritten once per stock.  Incremental runs write only the claimed STOCK
entity and preserve every other member copied from the active generation.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .feature_rebuild import (
    FEATURE_REBUILD_ALGORITHM,
    FEATURE_REBUILD_CONTRACT,
    FeatureRebuildCoordinator,
    FeatureRebuildResult,
    validate_feature_generation,
)
from .feature_store import ResearchFeatureStore, content_hash


SHANGHAI = ZoneInfo("Asia/Shanghai")
_SAFE_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SNAPSHOT_FILE = re.compile(r"^snapshot-[A-Za-z0-9+._-]{8,180}\.json$")


class FeatureMaintenanceError(RuntimeError):
    """Stable, non-sensitive maintenance failure."""

    def __init__(self, reason_code: str):
        self.reason_code = str(reason_code)
        super().__init__(self.reason_code)


@dataclass(frozen=True, slots=True)
class VerifiedFeatureSnapshot:
    """A point-in-time snapshot whose envelope and content hash are valid."""

    snapshot_id: str
    snapshot_hash: str
    as_of: datetime
    data: Mapping[str, Any]
    path: Path


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aware(value: datetime | str) -> datetime:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        value = datetime.fromisoformat(text)
    if value.tzinfo is None or value.utcoffset() is None:
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_AS_OF_INVALID")
    return value.astimezone(SHANGHAI)


def _load_snapshot(path: Path, snapshot_root: Path) -> VerifiedFeatureSnapshot:
    if not path.is_file() or not path.resolve().is_relative_to(snapshot_root):
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_NOT_FOUND")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_INVALID") from exc
    if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_INVALID")
    snapshot_id = str(payload.get("snapshot_id") or "")
    snapshot_hash = str(payload.get("snapshot_hash") or "")
    data = dict(payload["data"])
    if not snapshot_id or not snapshot_hash or snapshot_hash != _canonical_hash(data):
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_HASH_MISMATCH")
    try:
        as_of = _aware(str(payload.get("as_of") or ""))
    except (TypeError, ValueError) as exc:
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_AS_OF_INVALID") from exc
    symbols = data.get("g0_symbols")
    if not isinstance(symbols, list) or not symbols:
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_G0_INVALID")
    normalized = [str(symbol).strip().upper() for symbol in symbols]
    if len(normalized) != len(set(normalized)) or any(not _SAFE_SYMBOL.fullmatch(symbol) for symbol in normalized):
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_G0_INVALID")
    if snapshot_id != path.stem:
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_ID_MISMATCH")
    return VerifiedFeatureSnapshot(
        snapshot_id=snapshot_id,
        snapshot_hash=snapshot_hash,
        as_of=as_of,
        data=data,
        path=path.resolve(),
    )


def load_latest_verified_snapshot(snapshot_dir: str | Path) -> VerifiedFeatureSnapshot:
    """Return the newest valid top-level frozen snapshot.

    Invalid files are ignored while looking for an older valid snapshot.  If
    no valid file exists, the reason code from the newest candidate is exposed
    so callers fail closed without leaking filesystem or provider details.
    """

    root = Path(snapshot_dir).resolve()
    if not root.is_dir():
        raise FeatureMaintenanceError("FEATURE_SNAPSHOT_NOT_FOUND")
    candidates = sorted(
        (path for path in root.iterdir() if _SNAPSHOT_FILE.fullmatch(path.name)),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    last_error: FeatureMaintenanceError | None = None
    valid: list[VerifiedFeatureSnapshot] = []
    for path in candidates:
        try:
            valid.append(_load_snapshot(path, root))
        except FeatureMaintenanceError as exc:
            last_error = exc
    if not valid:
        raise last_error or FeatureMaintenanceError("FEATURE_SNAPSHOT_NOT_FOUND")
    return max(valid, key=lambda item: item.as_of)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _symbol_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key).strip().upper(): item for key, item in value.items()}


def _record_map(records: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(records, list):
        return {}
    result: dict[str, Mapping[str, Any]] = {}
    for item in records:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("thscode") or item.get("symbol") or "").strip().upper()
        if _SAFE_SYMBOL.fullmatch(symbol):
            result[symbol] = item
    return result


def _business_facts(symbol: str, raw: Any) -> list[dict[str, Any]]:
    value = _mapping(raw)
    evidence = value.get("evidence")
    if not isinstance(evidence, list):
        return []
    facts: list[dict[str, Any]] = []
    for index, item in enumerate(evidence):
        if not isinstance(item, Mapping):
            continue
        ref = str(item.get("source_ref") or item.get("evidence_ref") or "").strip()
        announcement = str(item.get("announcement_id") or "").strip()
        if not ref:
            ref = f"snapshot:{symbol}:{announcement or index}"
        title = str(item.get("business_name") or item.get("announcement_title") or "MAIN_BUSINESS").strip()
        if not title:
            title = "MAIN_BUSINESS"
        published = str(item.get("publish_time") or item.get("published_at") or "UNKNOWN")
        facts.append(
            {
                "symbol": symbol,
                "report_period": published[:80],
                "business_name": title[:240],
                "revenue_exposure_pct": item.get("revenue_exposure_pct"),
                "gross_profit_exposure_pct": item.get("gross_profit_exposure_pct"),
                "node_id": item.get("node_id"),
                "evidence_ref": ref[:500],
                "page_number": item.get("page_number"),
                "confidence": item.get("confidence", 0.5),
                "parser_version": "snapshot-main-business-v1",
                "content_hash": str(item.get("content_hash") or content_hash(item)),
                "source_payload": dict(item),
            }
        )
    return facts


class SnapshotFeatureBuilder:
    """Materialize one verified snapshot into a supplied generation."""

    def __init__(self, snapshot: VerifiedFeatureSnapshot, *, mode: str):
        self.snapshot = snapshot
        self.mode = str(mode).upper()
        self._bulk_generations: set[str] = set()

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(str(symbol).strip().upper() for symbol in self.snapshot.data["g0_symbols"])

    def __call__(self, entity: Mapping[str, Any], generation_id: str, store: ResearchFeatureStore) -> None:
        if self.mode == "FULL" and generation_id not in self._bulk_generations:
            self._write_bulk(generation_id, store)
            self._bulk_generations.add(generation_id)
        # Full mode is intentionally materialized in one bulk operation.  The
        # coordinator invokes the builder once per G0 entity for accounting,
        # but those callbacks must not repeat the same writes.
        if self.mode == "FULL":
            return
        self._write_entity(entity, generation_id, store)

    def _stock_payload(self, symbol: str) -> dict[str, Any]:
        data = self.snapshot.data
        candidates = {
            str(item.get("symbol") or "").strip().upper(): dict(item)
            for item in data.get("g0_candidates", ())
            if isinstance(item, Mapping) and item.get("symbol")
        }
        industries = _record_map(_mapping(data.get("THS_INDUSTRY_MEMBERSHIP")).get("records"))
        concepts = _record_map(_mapping(data.get("THS_CONCEPT_MEMBERSHIP")).get("records"))
        return {
            "schema_version": "feature-inputs/1.0.0",
            "symbol": symbol,
            "snapshot_id": self.snapshot.snapshot_id,
            "snapshot_hash": self.snapshot.snapshot_hash,
            "as_of": self.snapshot.as_of.isoformat(),
            "candidate": candidates.get(symbol, {"symbol": symbol}),
            "daily_bars": _symbol_map(data.get("RECENT_DAILY_BARS")).get(symbol, []),
            "fundamentals": _symbol_map(data.get("COMPANY_FUNDAMENTALS")).get(symbol),
            "factor": _symbol_map(data.get("FACTOR_SNAPSHOT")).get(symbol),
            "a2_factor": _symbol_map(data.get("A2_FACTOR_SNAPSHOT")).get(symbol),
            "liquidity": _symbol_map(data.get("LIQUIDITY_SNAPSHOT")).get(symbol),
            "tradability": _symbol_map(data.get("TRADABILITY_FLAGS")).get(symbol),
            "industry_membership": industries.get(symbol),
            "concept_membership": concepts.get(symbol),
            "main_business": _symbol_map(data.get("MAIN_BUSINESS_EVIDENCE")).get(symbol),
        }

    def _write_entity(self, entity: Mapping[str, Any], generation_id: str, store: ResearchFeatureStore) -> None:
        entity_type = str(entity.get("entity_type") or "").strip().upper()
        entity_id = str(entity.get("entity_id") or "").strip().upper()
        if entity_type == "STOCK" and _SAFE_SYMBOL.fullmatch(entity_id):
            payload = self._stock_payload(entity_id)
            store.record_feature_generation_members(
                generation_id=generation_id,
                members=[
                    {
                        "entity_type": "STOCK",
                        "entity_id": entity_id,
                        "partition_name": "snapshot-inputs",
                        "payload": payload,
                        "content_hash": content_hash(payload),
                        "row_count": len(payload.get("daily_bars") or []),
                    }
                ],
            )
        elif entity_type in {"INDUSTRY", "CONCEPT"}:
            self._write_taxonomy(entity_type, generation_id, store)
        elif entity_type in {"MAIN_BUSINESS", "BUSINESS"}:
            self._write_business(generation_id, store, symbols=(entity_id,))

    def _write_bulk(self, generation_id: str, store: ResearchFeatureStore) -> None:
        members = [
            {
                "entity_type": "STOCK",
                "entity_id": symbol,
                "partition_name": "snapshot-inputs",
                "payload": self._stock_payload(symbol),
            }
            for symbol in self.symbols
        ]
        store.record_feature_generation_members(generation_id=generation_id, members=members)
        self._write_taxonomy("INDUSTRY", generation_id, store)
        self._write_taxonomy("CONCEPT", generation_id, store)
        self._write_business(generation_id, store)

    def _write_taxonomy(self, taxonomy: str, generation_id: str, store: ResearchFeatureStore) -> None:
        key = "THS_INDUSTRY_MEMBERSHIP" if taxonomy == "INDUSTRY" else "THS_CONCEPT_MEMBERSHIP"
        snapshot = self.snapshot.data.get(key)
        if isinstance(snapshot, Mapping):
            store.replace_taxonomy_memberships(
                taxonomy=taxonomy,
                snapshot=snapshot,
                as_of=self.snapshot.as_of,
                generation_id=generation_id,
            )

    def _write_business(
        self,
        generation_id: str,
        store: ResearchFeatureStore,
        *,
        symbols: Iterable[str] | None = None,
    ) -> None:
        source = _symbol_map(self.snapshot.data.get("MAIN_BUSINESS_EVIDENCE"))
        selected = tuple(symbols) if symbols is not None else self.symbols
        facts = [fact for symbol in selected for fact in _business_facts(symbol, source.get(symbol))]
        # Incremental generations are cloned from the active generation.  A
        # changed business fact must replace the symbol's old rows rather than
        # leave contradictory revisions visible in the new generation.  Do
        # this even when the new source has no evidence, so stale evidence is
        # not silently retained.
        if symbols is not None:
            with store._connect() as connection:  # noqa: SLF001 - generation-scoped maintenance
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for symbol in selected:
                        connection.execute(
                            "DELETE FROM business_exposure_facts WHERE generation_id=? AND symbol=?",
                            (generation_id, str(symbol).strip().upper()),
                        )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
        if facts:
            store.replace_business_exposure_facts(facts, generation_id=generation_id)


def run_feature_maintenance(
    settings: Any,
    *,
    full: bool = False,
    now: datetime | None = None,
    worker_id: str | None = None,
) -> dict[str, Any]:
    """Run the safe CLI maintenance operation and return a JSON payload."""

    current = _aware(now or datetime.now(SHANGHAI))
    snapshot = load_latest_verified_snapshot(settings.snapshot_dir)
    if current.weekday() == 6:
        return {
            "status": "NOOP",
            "mode": "WEEKEND",
            "reason_code": "NON_MAINTENANCE_DAY",
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_hash": snapshot.snapshot_hash,
            "snapshot_path": str(snapshot.path),
        }
    mode = "FULL" if full or current.weekday() == 5 else "INCREMENTAL"
    store = ResearchFeatureStore(settings.feature_store_db_path)
    if mode == "INCREMENTAL" and store.get_active_feature_generation("RESEARCH") is None:
        raise FeatureMaintenanceError("FEATURE_ACTIVE_GENERATION_MISSING")
    builder = SnapshotFeatureBuilder(snapshot, mode=mode)
    coordinator = FeatureRebuildCoordinator(
        store,
        builder,
        validator=lambda feature_store, generation_id: validate_feature_generation(
            feature_store,
            generation_id,
            expected_entity_count=len(builder.symbols) if mode == "FULL" else None,
        ),
    )
    if mode == "FULL":
        entities = ({"entity_type": "STOCK", "entity_id": symbol} for symbol in builder.symbols)
        result = coordinator.run_full(
            entities=entities,
            as_of=snapshot.as_of,
            worker_id=worker_id or "feature-maintenance-full",
            source_manifest_hash=snapshot.snapshot_hash,
        )
    else:
        result = coordinator.run_incremental(
            as_of=snapshot.as_of,
            worker_id=worker_id or "feature-maintenance-incremental",
            source_manifest_hash=snapshot.snapshot_hash,
        )
    return {
        **result.as_dict(),
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_hash": snapshot.snapshot_hash,
        "snapshot_path": str(snapshot.path),
        "g0_count": len(builder.symbols),
        "maintenance_at": current.isoformat(),
        "llm_invoked": False,
        "external_orders": False,
    }


__all__ = [
    "FeatureMaintenanceError",
    "SnapshotFeatureBuilder",
    "VerifiedFeatureSnapshot",
    "load_latest_verified_snapshot",
    "run_feature_maintenance",
]
