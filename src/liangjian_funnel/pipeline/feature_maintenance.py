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
import os
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
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
from .feature_store import FeatureGenerationError, ResearchFeatureStore, content_hash
from ..runtime.storage_governance import evaluate_disk_watermark
from ..runtime.resource_guard import measure_resources
from ..runtime.progress import WorkflowProgress


SHANGHAI = ZoneInfo("Asia/Shanghai")
_SAFE_SYMBOL = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SNAPSHOT_FILE = re.compile(r"^snapshot-[A-Za-z0-9+._-]{8,180}\.json$")
LIVE_SOURCE_CONTRACT = "live-source-generation/1.0.0"
LIVE_SOURCE_ALGORITHM = "live-source-batched-v1"


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


@dataclass(frozen=True, slots=True)
class LiveSourceMaterializationResult:
    """Safe summary of a bounded in-memory source materialisation."""

    status: str
    generation_id: str | None
    snapshot_id: str
    snapshot_hash: str
    market_trade_date: str
    g0_count: int
    member_count: int
    fundamental_count: int
    taxonomy_count: int
    business_count: int
    reused: bool = False
    reason_code: str | None = None
    resources: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "generation_id": self.generation_id,
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "market_trade_date": self.market_trade_date,
            "g0_count": self.g0_count,
            "member_count": self.member_count,
            "fundamental_count": self.fundamental_count,
            "taxonomy_count": self.taxonomy_count,
            "business_count": self.business_count,
            "reused": self.reused,
            "reason_code": self.reason_code,
            "resources": dict(self.resources or {}),
        }


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


def _chunks(values: Sequence[str], size: int) -> Iterable[tuple[str, ...]]:
    for start in range(0, len(values), size):
        yield tuple(values[start : start + size])


def _date_token(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value or "").strip()
    if not text:
        return None
    compact = text[:10].replace("/", "-")
    if re.fullmatch(r"\d{8}", compact):
        compact = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    try:
        return datetime.fromisoformat(compact).date().isoformat()
    except ValueError:
        return None


def _date_ms_token(value: Any) -> str | None:
    """Project a provider Unix-millisecond timestamp onto the A-share day.

    Hithink daily bars encode Shanghai midnight as Unix milliseconds.  Taking
    the UTC date would therefore move every bar to the previous calendar day.
    Keep this parser deliberately strict so Unix seconds and malformed values
    cannot accidentally satisfy the market-freshness gate.
    """

    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        milliseconds = float(value)
    except (TypeError, ValueError):
        return None
    if not milliseconds.is_integer():
        return None
    # Millisecond timestamps for supported market history are at least 12
    # digits.  The upper bound rejects implausible values before conversion.
    if milliseconds < 100_000_000_000 or milliseconds >= 10_000_000_000_000:
        return None
    try:
        observed = datetime.fromtimestamp(milliseconds / 1000.0, tz=SHANGHAI)
    except (OverflowError, OSError, ValueError):
        return None
    return observed.date().isoformat()


def _latest_bar_date(raw: Any) -> str | None:
    if not isinstance(raw, list):
        return None
    latest: str | None = None
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        observed = None
        for key in ("trade_date", "date", "time", "datetime", "timestamp"):
            observed = _date_token(item.get(key))
            if observed:
                break
        if observed is None:
            observed = _date_ms_token(item.get("date_ms"))
        if observed and (latest is None or observed > latest):
            latest = observed
    return latest


def _staleness_explained(raw: Any) -> bool:
    if not isinstance(raw, Mapping):
        return False
    if raw.get("tradable") is False or raw.get("suspended") is True:
        return True
    reasons = raw.get("exclusion_reasons")
    if not isinstance(reasons, (list, tuple)):
        return False
    tokens = {str(item or "").strip().upper() for item in reasons}
    return bool(tokens & {"SUSPENDED", "SUSPENSION", "停牌", "NO_DAILY_BAR"})


def _market_freshness(
    data: Mapping[str, Any],
    symbols: Sequence[str],
    market_trade_date: str,
) -> dict[str, Any]:
    bars = _symbol_map(data.get("RECENT_DAILY_BARS"))
    tradability = _symbol_map(data.get("TRADABILITY_FLAGS"))
    fresh = 0
    explained = 0
    unexplained: list[str] = []
    observed_dates: dict[str, int] = {}
    for symbol in symbols:
        latest = _latest_bar_date(bars.get(symbol))
        observed_dates[latest or "MISSING"] = observed_dates.get(latest or "MISSING", 0) + 1
        if latest == market_trade_date:
            fresh += 1
        elif _staleness_explained(tradability.get(symbol)):
            explained += 1
        else:
            unexplained.append(symbol)
    return {
        "expected_trade_date": market_trade_date,
        "fresh_count": fresh,
        "explained_stale_count": explained,
        "unexplained_stale_count": len(unexplained),
        "unexplained_sample": unexplained[:20],
        "observed_date_counts": dict(sorted(observed_dates.items())),
        "status": "READY" if not unexplained else "STALE",
    }


def _member_root_hash(store: ResearchFeatureStore, generation_id: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    with store._connect() as connection:  # noqa: SLF001 - generation validation
        cursor = connection.execute(
            """
            SELECT entity_id,content_hash,row_count
            FROM feature_generation_members
            WHERE generation_id=? AND entity_type='STOCK' AND partition_name='snapshot-inputs'
            ORDER BY entity_id
            """,
            (generation_id,),
        )
        for row in cursor:
            digest.update(str(row["entity_id"]).encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(row["content_hash"]).encode("ascii", "ignore"))
            digest.update(b"\0")
            digest.update(str(int(row["row_count"] or 0)).encode("ascii"))
            digest.update(b"\n")
            count += 1
    return digest.hexdigest(), count


def _source_table_counts(store: ResearchFeatureStore, generation_id: str) -> dict[str, int]:
    tables = {
        "members": "feature_generation_members",
        "fundamental": "stock_fundamental_features",
        "taxonomy": "taxonomy_membership_versions",
        "business": "business_exposure_facts",
    }
    counts: dict[str, int] = {}
    with store._connect() as connection:  # noqa: SLF001 - bounded validation queries
        for name, table in tables.items():
            counts[name] = int(
                connection.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE generation_id=?',
                    (generation_id,),
                ).fetchone()[0]
            )
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    if quick_check.lower() != "ok":
        raise FeatureMaintenanceError("FEATURE_SOURCE_SQLITE_QUICK_CHECK_FAILED")
    return counts


def _write_taxonomy_source_batched(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    taxonomy: str,
    raw: Any,
    as_of: datetime,
    batch_size: int,
) -> int:
    snapshot = raw if isinstance(raw, Mapping) else {}
    records = snapshot.get("records")
    records = records if isinstance(records, list) else []
    normalized: list[tuple[str, Mapping[str, Any]]] = []
    version_digest = hashlib.sha256()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        symbol = str(record.get("thscode") or record.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        normalized.append((symbol, record))
        version_digest.update(symbol.encode("utf-8"))
        version_digest.update(content_hash(record).encode("ascii"))
    version_hash = version_digest.hexdigest()
    timestamp = as_of.isoformat()
    written = 0
    for start in range(0, len(normalized), batch_size):
        rows: list[tuple[Any, ...]] = []
        for symbol, record in normalized[start : start + batch_size]:
            memberships = record.get("memberships")
            if not isinstance(memberships, list):
                continue
            for membership in memberships:
                if not isinstance(membership, Mapping):
                    continue
                code = str(
                    membership.get("taxonomy_code")
                    or membership.get("industry_thscode")
                    or membership.get("concept_thscode")
                    or ""
                ).strip().upper()
                if not code:
                    continue
                name = str(
                    membership.get("taxonomy_name")
                    or membership.get("industry_name")
                    or membership.get("concept_name")
                    or ""
                ).strip()
                payload = {"symbol": symbol, **dict(membership)}
                rows.append(
                    (
                        generation_id,
                        taxonomy,
                        version_hash,
                        timestamp,
                        symbol,
                        code,
                        name,
                        content_hash(payload),
                        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str),
                    )
                )
        if not rows:
            continue
        with store._connect() as connection:  # noqa: SLF001 - bounded source materialisation
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO taxonomy_membership_versions(
                        generation_id,taxonomy,version_hash,as_of,symbol,taxonomy_code,
                        taxonomy_name,source_hash,payload_json
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        written += len(rows)
    return written


class SnapshotFeatureBuilder:
    """Materialize one verified snapshot into a supplied generation."""

    def __init__(self, snapshot: VerifiedFeatureSnapshot, *, mode: str):
        self.snapshot = snapshot
        self.mode = str(mode).upper()
        self._bulk_generations: set[str] = set()
        data = snapshot.data
        self._candidates = {
            str(item.get("symbol") or "").strip().upper(): dict(item)
            for item in data.get("g0_candidates", ())
            if isinstance(item, Mapping) and item.get("symbol")
        }
        self._daily_bars = _symbol_map(data.get("RECENT_DAILY_BARS"))
        self._fundamentals = _symbol_map(data.get("COMPANY_FUNDAMENTALS"))
        self._factors = _symbol_map(data.get("FACTOR_SNAPSHOT"))
        self._a2_factors = _symbol_map(data.get("A2_FACTOR_SNAPSHOT"))
        self._liquidity = _symbol_map(data.get("LIQUIDITY_SNAPSHOT"))
        self._tradability = _symbol_map(data.get("TRADABILITY_FLAGS"))
        self._industries = _record_map(_mapping(data.get("THS_INDUSTRY_MEMBERSHIP")).get("records"))
        self._concepts = _record_map(_mapping(data.get("THS_CONCEPT_MEMBERSHIP")).get("records"))
        self._main_business = _symbol_map(data.get("MAIN_BUSINESS_EVIDENCE"))
        self._taxonomy_types = tuple(
            taxonomy
            for taxonomy, key in (
                ("INDUSTRY", "THS_INDUSTRY_MEMBERSHIP"),
                ("CONCEPT", "THS_CONCEPT_MEMBERSHIP"),
            )
            if key in data
        )

    @property
    def validation_contract(self) -> dict[str, Any]:
        """Describe which durable projections this maintenance snapshot owns.

        Stock members and fundamentals are always required for a live
        maintenance generation.  Taxonomy and business are required only
        when the snapshot carries that source namespace.  Theme/chain/role
        projections are intentionally run-scoped and are never manufactured
        by maintenance.
        """

        return {
            "schema_version": "feature-maintenance-validation/1.0.0",
            "required": {
                "members": True,
                "taxonomy": bool(self._taxonomy_types),
                "business": "MAIN_BUSINESS_EVIDENCE" in self.snapshot.data,
                "fundamental": True,
            },
            "taxonomy_types": list(self._taxonomy_types),
            "applicability": {
                "theme_registry": "RUN_SCOPED",
                "chain_node": "RUN_SCOPED",
                "theme_taxonomy_links": "RUN_SCOPED",
                "market_role": "RUN_SCOPED",
                "stage_decisions": "RUN_SCOPED",
            },
        }

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
        return {
            "schema_version": "feature-inputs/1.0.0",
            "symbol": symbol,
            "snapshot_id": self.snapshot.snapshot_id,
            "snapshot_hash": self.snapshot.snapshot_hash,
            "as_of": self.snapshot.as_of.isoformat(),
            "candidate": self._candidates.get(symbol, {"symbol": symbol}),
            "daily_bars": self._daily_bars.get(symbol, []),
            "fundamentals": self._fundamentals.get(symbol),
            "factor": self._factors.get(symbol),
            "a2_factor": self._a2_factors.get(symbol),
            "liquidity": self._liquidity.get(symbol),
            "tradability": self._tradability.get(symbol),
            "industry_membership": self._industries.get(symbol),
            "concept_membership": self._concepts.get(symbol),
            "main_business": self._main_business.get(symbol),
        }

    def _fundamental_decision(self, symbol: str) -> dict[str, Any] | None:
        """Adapt one real snapshot fundamental into the Feature Store schema."""

        raw = self._fundamentals.get(symbol)
        if not isinstance(raw, Mapping) or not raw:
            return None
        financial_features = raw.get("financial_features")
        if not isinstance(financial_features, Mapping) or not financial_features:
            # Existing snapshots use a flat COMPANY_FUNDAMENTALS mapping.  It
            # is still a real source payload and must be materialized rather
            # than left only inside feature_generation_members.
            financial_features = dict(raw)
        source_hashes = raw.get("source_hashes")
        if not isinstance(source_hashes, Mapping) or not source_hashes:
            source_hashes = {
                "snapshot_hash": self.snapshot.snapshot_hash,
                "fundamental_payload_hash": content_hash(raw),
            }
        return {
            "symbol": symbol,
            "financial_features": dict(financial_features),
            "financial_quality_score": raw.get("financial_quality_score"),
            "data_quality_score": raw.get("data_quality_score", raw.get("quality_score")),
            "liquidity_score": raw.get("liquidity_score"),
            "score_breakdown": raw.get("score_breakdown"),
            "source_hashes": dict(source_hashes),
            "feature_version": str(raw.get("feature_version") or "snapshot-fundamental-v1"),
        }

    def _write_fundamental(self, generation_id: str, store: ResearchFeatureStore, symbol: str) -> None:
        """Replace one symbol's cloned fundamental projection atomically."""

        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return
        # The table key includes as_of, feature_version and source_hash.  A
        # simple INSERT OR REPLACE therefore leaves old cloned revisions in
        # place.  Delete this symbol first so an incremental generation can
        # never expose contradictory fundamental versions.
        with store._connect() as connection:  # noqa: SLF001 - generation-scoped maintenance
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "DELETE FROM stock_fundamental_features WHERE generation_id=? AND symbol=?",
                    (generation_id, normalized),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        decision = self._fundamental_decision(normalized)
        if decision is not None:
            store.record_fundamental_features(
                as_of=self.snapshot.as_of,
                decisions=[decision],
                generation_id=generation_id,
            )

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
            self._write_fundamental(generation_id, store, entity_id)
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
        decisions = [
            decision
            for symbol in self.symbols
            if (decision := self._fundamental_decision(symbol)) is not None
        ]
        if decisions:
            store.record_fundamental_features(
                as_of=self.snapshot.as_of,
                decisions=decisions,
                generation_id=generation_id,
            )
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
        selected = tuple(symbols) if symbols is not None else self.symbols
        facts = [
            fact
            for symbol in selected
            for fact in _business_facts(symbol, self._main_business.get(symbol))
        ]
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


def materialize_live_source(
    store: ResearchFeatureStore,
    *,
    snapshot_id: str,
    snapshot_hash: str,
    as_of: datetime,
    market_trade_date: str,
    data: Mapping[str, Any],
    batch_size: int = 50,
) -> LiveSourceMaterializationResult:
    """Persist the maintenance projection while the frozen data is in memory.

    This function never reads the top-level JSON snapshot.  It creates an
    immutable, non-activatable source generation and bounds every Python row
    collection by ``batch_size``.
    """

    if batch_size < 25 or batch_size > 200:
        raise ValueError("feature source batch_size must be between 25 and 200")
    symbols_raw = data.get("g0_symbols")
    if not isinstance(symbols_raw, list) or not symbols_raw:
        raise FeatureMaintenanceError("FEATURE_SOURCE_G0_INVALID")
    symbols = tuple(str(item or "").strip().upper() for item in symbols_raw)
    if len(symbols) != len(set(symbols)) or any(not _SAFE_SYMBOL.fullmatch(item) for item in symbols):
        raise FeatureMaintenanceError("FEATURE_SOURCE_G0_INVALID")
    market_date = _date_token(market_trade_date)
    if market_date is None:
        raise FeatureMaintenanceError("FEATURE_SOURCE_MARKET_DATE_INVALID")

    freshness = _market_freshness(data, symbols, market_date)
    namespace_contract = [
        name
        for name in (
            "g0_candidates",
            "RECENT_DAILY_BARS",
            "COMPANY_FUNDAMENTALS",
            "FACTOR_SNAPSHOT",
            "A2_FACTOR_SNAPSHOT",
            "LIQUIDITY_SNAPSHOT",
            "TRADABILITY_FLAGS",
            "THS_INDUSTRY_MEMBERSHIP",
            "THS_CONCEPT_MEMBERSHIP",
            "MAIN_BUSINESS_EVIDENCE",
        )
        if name in data
    ]
    relevant_symbols = set(symbols)
    source_version_set: set[str] = set()
    dependency_hash_set: set[str] = set()
    source_versions_by_entity: dict[str, set[str]] = {}
    dependency_hashes_by_entity: dict[str, set[str]] = {}
    # Stream the narrow queue identity columns instead of imposing a hidden
    # 10,000-row cap.  A cap could permanently exclude a valid dirty version
    # and make maintenance retry forever even though the source is current.
    with store._connect() as connection:  # noqa: SLF001 - source watermark
        cursor = connection.execute(
            "SELECT entity_id,source_version,dependency_hash FROM dirty_entities "
            "WHERE entity_type='STOCK' AND resolved_at IS NULL "
            "AND status IN ('PENDING','RETRY','LEASED')"
        )
        for row in cursor:
            entity_id = str(row["entity_id"] or "").strip().upper()
            if entity_id not in relevant_symbols:
                continue
            source_version = str(row["source_version"] or "").strip()
            dependency_hash = str(row["dependency_hash"] or "").strip()
            if source_version:
                source_version_set.add(source_version)
                source_versions_by_entity.setdefault(entity_id, set()).add(
                    source_version
                )
            if dependency_hash:
                dependency_hash_set.add(dependency_hash)
                dependency_hashes_by_entity.setdefault(entity_id, set()).add(
                    dependency_hash
                )
    source_versions = sorted(source_version_set)
    dependency_hashes = sorted(dependency_hash_set)
    metadata = {
        "schema_version": LIVE_SOURCE_CONTRACT,
        "snapshot_id": snapshot_id,
        "snapshot_hash": snapshot_hash,
        "as_of": as_of.isoformat(),
        "market_trade_date": market_date,
        "g0_count": len(symbols),
        "namespace_contract": namespace_contract,
        "namespace_freshness": {"RECENT_DAILY_BARS": freshness},
        "source_versions": source_versions,
        "dependency_hashes": dependency_hashes,
        "source_versions_by_entity": {
            symbol: sorted(values)
            for symbol, values in sorted(source_versions_by_entity.items())
        },
        "dependency_hashes_by_entity": {
            symbol: sorted(values)
            for symbol, values in sorted(dependency_hashes_by_entity.items())
        },
    }
    row = store.create_or_get_live_source(
        snapshot_hash=snapshot_hash,
        source_manifest_hash=snapshot_hash,
        as_of=as_of,
        contract_version=LIVE_SOURCE_CONTRACT,
        algorithm_version=LIVE_SOURCE_ALGORITHM,
        metadata=metadata,
    )
    generation_id = str(row["generation_id"])
    status = str(row.get("status") or "").upper()
    if status in {"SEALED", "PUBLISHED"}:
        counts = _source_table_counts(store, generation_id)
        return LiveSourceMaterializationResult(
            status="READY",
            generation_id=generation_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            market_trade_date=market_date,
            g0_count=len(symbols),
            member_count=counts["members"],
            fundamental_count=counts["fundamental"],
            taxonomy_count=counts["taxonomy"],
            business_count=counts["business"],
            reused=True,
            resources=measure_resources(store.path.parent).as_dict(),
        )
    if status == "FAILED":
        return LiveSourceMaterializationResult(
            status="BLOCKED_SOURCE_GENERATION",
            generation_id=generation_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            market_trade_date=market_date,
            g0_count=len(symbols),
            member_count=0,
            fundamental_count=0,
            taxonomy_count=0,
            business_count=0,
            reason_code="FEATURE_SOURCE_GENERATION_FAILED",
        )
    if freshness["status"] != "READY":
        store.fail_feature_generation(
            generation_id,
            reason="FEATURE_SOURCE_MARKET_DATA_STALE",
            diagnostics={"freshness": freshness},
        )
        return LiveSourceMaterializationResult(
            status="BLOCKED_SOURCE_GENERATION",
            generation_id=generation_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            market_trade_date=market_date,
            g0_count=len(symbols),
            member_count=0,
            fundamental_count=0,
            taxonomy_count=0,
            business_count=0,
            reason_code="FEATURE_SOURCE_MARKET_DATA_STALE",
            resources=measure_resources(store.path.parent).as_dict(),
        )

    snapshot = VerifiedFeatureSnapshot(
        snapshot_id=snapshot_id,
        snapshot_hash=snapshot_hash,
        as_of=as_of,
        data=data,
        path=Path(f"{snapshot_id}.json"),
    )
    builder = SnapshotFeatureBuilder(snapshot, mode="SOURCE")
    try:
        for batch in _chunks(symbols, batch_size):
            members = []
            decisions = []
            facts = []
            for symbol in batch:
                payload = builder._stock_payload(symbol)  # noqa: SLF001 - same source contract
                members.append(
                    {
                        "entity_type": "STOCK",
                        "entity_id": symbol,
                        "partition_name": "snapshot-inputs",
                        "payload": payload,
                        "content_hash": content_hash(payload),
                        "row_count": len(payload.get("daily_bars") or []),
                    }
                )
                decision = builder._fundamental_decision(symbol)  # noqa: SLF001
                if decision is not None:
                    decisions.append(decision)
                facts.extend(_business_facts(symbol, builder._main_business.get(symbol)))  # noqa: SLF001
            store.record_feature_generation_members_batched(
                generation_id=generation_id,
                members=members,
                batch_size=batch_size,
            )
            if decisions:
                store.record_fundamental_features(
                    as_of=as_of,
                    decisions=decisions,
                    generation_id=generation_id,
                )
            if facts:
                store.replace_business_exposure_facts(facts, generation_id=generation_id)
        taxonomy_count = _write_taxonomy_source_batched(
            store,
            generation_id,
            taxonomy="INDUSTRY",
            raw=data.get("THS_INDUSTRY_MEMBERSHIP"),
            as_of=as_of,
            batch_size=batch_size,
        )
        taxonomy_count += _write_taxonomy_source_batched(
            store,
            generation_id,
            taxonomy="CONCEPT",
            raw=data.get("THS_CONCEPT_MEMBERSHIP"),
            as_of=as_of,
            batch_size=batch_size,
        )
        root_hash, member_count = _member_root_hash(store, generation_id)
        counts = _source_table_counts(store, generation_id)
        if member_count != len(symbols) or counts["fundamental"] != len(symbols):
            raise FeatureMaintenanceError("FEATURE_SOURCE_REQUIRED_COVERAGE_FAILED")
        validation = {
            **metadata,
            "status": "READY",
            "purpose": "LIVE_SOURCE",
            "activation_eligible": False,
            "member_root_hash": root_hash,
            "counts": counts,
        }
        store.validate_feature_generation(
            generation_id,
            validation=validation,
        )
        store.seal_generation(
            generation_id,
            validation_manifest=validation,
            purpose="LIVE_SOURCE",
            activation_eligible=False,
        )
        return LiveSourceMaterializationResult(
            status="READY",
            generation_id=generation_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            market_trade_date=market_date,
            g0_count=len(symbols),
            member_count=member_count,
            fundamental_count=counts["fundamental"],
            taxonomy_count=taxonomy_count,
            business_count=counts["business"],
            resources=measure_resources(store.path.parent).as_dict(),
        )
    except Exception as exc:
        current = store.get_feature_generation(generation_id)
        if current is not None and str(current.get("status") or "").upper() not in {"SEALED", "PUBLISHED", "FAILED"}:
            store.fail_feature_generation(
                generation_id,
                reason=f"{type(exc).__name__.upper()}:{str(exc)[:160]}",
                diagnostics={"snapshot_id": snapshot_id, "market_trade_date": market_date},
            )
        return LiveSourceMaterializationResult(
            status="BLOCKED_SOURCE_GENERATION",
            generation_id=generation_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            market_trade_date=market_date,
            g0_count=len(symbols),
            member_count=0,
            fundamental_count=0,
            taxonomy_count=0,
            business_count=0,
            reason_code=(exc.reason_code if isinstance(exc, FeatureMaintenanceError) else type(exc).__name__.upper()),
            resources=measure_resources(store.path.parent).as_dict(),
        )


def _source_manifest(source: Mapping[str, Any]) -> dict[str, Any]:
    value = source.get("validation_manifest")
    if isinstance(value, Mapping):
        return dict(value)
    metadata = source.get("metadata")
    if isinstance(metadata, Mapping) and isinstance(metadata.get("validation"), Mapping):
        return dict(metadata["validation"])
    return {}


def _select_ready_live_source(
    store: ResearchFeatureStore,
    *,
    as_of: datetime,
) -> dict[str, Any] | None:
    cutoff = _aware(as_of)
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    with store._connect() as connection:  # noqa: SLF001 - strict latest-source selection
        rows = connection.execute(
            "SELECT generation_id,as_of,status,activation_eligible,created_at,"
            "source_manifest_hash,failure_reason,metadata_json,validation_manifest_json "
            "FROM feature_generations WHERE domain='RESEARCH' AND purpose='LIVE_SOURCE'"
        ).fetchall()
    for row in rows:
        try:
            observed = _aware(datetime.fromisoformat(str(row["as_of"])))
        except (TypeError, ValueError):
            continue
        if observed <= cutoff:
            candidates.append((observed, dict(row)))
    if not candidates:
        return None
    newest_as_of = max(item[0] for item in candidates)
    newest = [item[1] for item in candidates if item[0] == newest_as_of]
    source_hashes = {
        str(item.get("source_manifest_hash") or "").strip()
        for item in newest
        if str(item.get("source_manifest_hash") or "").strip()
    }
    if len(source_hashes) != 1:
        raise FeatureGenerationError("LIVE_SOURCE_AMBIGUOUS")
    ready: list[dict[str, Any]] = []
    for item in newest:
        manifest = ResearchFeatureStore._parse_json(  # noqa: SLF001
            item.get("validation_manifest_json"), {}
        )
        if (
            str(item.get("status") or "").upper() in {"SEALED", "PUBLISHED"}
            and not bool(item.get("activation_eligible"))
            and isinstance(manifest, Mapping)
            and str(manifest.get("status") or "").upper() == "READY"
            and not manifest.get("failures")
        ):
            ready.append(item)
    if not ready:
        failure_codes = {
            str(item.get("failure_reason") or "").split(":", 1)[0].strip().upper()
            for item in newest
            if str(item.get("failure_reason") or "").strip()
        }
        precise = next(
            (
                code
                for code in sorted(failure_codes)
                if code.startswith("FEATURE_SOURCE_")
            ),
            None,
        )
        raise FeatureGenerationError(precise or "LIVE_SOURCE_NOT_READY")
    ready.sort(
        key=lambda item: (
            str(item.get("created_at") or ""),
            str(item.get("generation_id") or ""),
        ),
        reverse=True,
    )
    source = store.get_feature_generation(str(ready[0]["generation_id"]))
    if source is None:
        raise FeatureGenerationError("LIVE_SOURCE_NOT_AVAILABLE")
    metadata = source.get("metadata")
    if isinstance(metadata, Mapping):
        source["snapshot_hash"] = metadata.get("snapshot_hash")
        source["source_hash"] = metadata.get("source_hash") or source.get(
            "source_manifest_hash"
        )
    manifest = _source_manifest(source)
    if manifest.get("status") != "READY" or manifest.get("activation_eligible") is not False:
        raise FeatureGenerationError("FEATURE_SOURCE_GENERATION_INVALID")
    freshness = manifest.get("namespace_freshness")
    daily = freshness.get("RECENT_DAILY_BARS") if isinstance(freshness, Mapping) else None
    if not isinstance(daily, Mapping) or daily.get("status") != "READY":
        raise FeatureGenerationError("FEATURE_SOURCE_MARKET_DATA_STALE")
    return source


def _source_symbols(store: ResearchFeatureStore, generation_id: str) -> tuple[str, ...]:
    with store._connect() as connection:  # noqa: SLF001 - bounded generation manifest
        rows = connection.execute(
            """
            SELECT entity_id FROM feature_generation_members
            WHERE generation_id=? AND entity_type='STOCK' AND partition_name='snapshot-inputs'
            ORDER BY entity_id
            """,
            (generation_id,),
        ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _maintenance_coverage_contract(source: Mapping[str, Any]) -> dict[str, Any]:
    manifest = _source_manifest(source)
    table_counts = manifest.get("table_counts")
    if not isinstance(table_counts, Mapping):
        table_counts = manifest.get("counts")
    if not isinstance(table_counts, Mapping):
        table_counts = {}
    namespaces = {
        str(item)
        for item in manifest.get("namespace_contract", ())
        if isinstance(item, str)
    }
    taxonomy_types = [
        taxonomy
        for taxonomy, namespace in (
            ("INDUSTRY", "THS_INDUSTRY_MEMBERSHIP"),
            ("CONCEPT", "THS_CONCEPT_MEMBERSHIP"),
        )
        if namespace in namespaces
        and int(
            table_counts.get("taxonomy_membership_versions")
            or table_counts.get("taxonomy")
            or 0
        ) > 0
    ]
    return {
        "schema_version": "feature-maintenance-validation/2.0.0",
        "required": {
            "members": True,
            # Not every listed company is a member of a THS concept and not
            # every source exposes structured main-business evidence.  These
            # projections are verified by exact source/target counts below;
            # requiring all G0 symbols here would turn a legitimate partial
            # namespace into a false maintenance failure.
            "taxonomy": False,
            "business": False,
            "fundamental": True,
        },
        "taxonomy_types": taxonomy_types,
        "applicability": {
            "theme_registry": "RUN_SCOPED",
            "chain_node": "RUN_SCOPED",
            "theme_taxonomy_links": "RUN_SCOPED",
            "market_role": "RUN_SCOPED",
            "stage_decisions": "RUN_SCOPED",
        },
    }


def _source_supports_dirty(batch: Any, source: Mapping[str, Any]) -> bool:
    manifest = _source_manifest(source)
    source_versions_raw = manifest.get("source_versions_by_entity")
    dependency_hashes_raw = manifest.get("dependency_hashes_by_entity")
    source_versions = (
        source_versions_raw if isinstance(source_versions_raw, Mapping) else {}
    )
    dependency_hashes = (
        dependency_hashes_raw if isinstance(dependency_hashes_raw, Mapping) else {}
    )
    for item in getattr(batch, "all_claimed", ()) or getattr(batch, "claimed", ()):
        entity_id = str(item.get("entity_id") or "").strip().upper()
        source_version = str(item.get("source_version") or "").strip()
        dependency_hash = str(item.get("dependency_hash") or "").strip()
        entity_versions = {
            str(value)
            for value in source_versions.get(entity_id, ())
            if str(value)
        }
        entity_dependencies = {
            str(value)
            for value in dependency_hashes.get(entity_id, ())
            if str(value)
        }
        if source_version and source_version not in entity_versions:
            return False
        if dependency_hash and dependency_hash not in entity_dependencies:
            return False
    return True


def _progress_for_maintenance(settings: Any, current: datetime) -> WorkflowProgress | None:
    path = getattr(settings, "workflow_progress_path", None)
    if path is None:
        return None
    return WorkflowProgress(
        Path(path),
        run_id=f"feature-maintenance-{current.strftime('%Y%m%dT%H%M%S%z')}",
        job="features",
        now=current,
    )


def _progress_phase(
    progress: WorkflowProgress | None,
    phase: str,
    *,
    resource_path: Path,
) -> None:
    if progress is None:
        return
    progress.set_phase(phase)
    progress.update_resources(measure_resources(resource_path).as_dict())


def _progress_finish(
    progress: WorkflowProgress | None,
    *,
    status: str,
    phase: str,
    reason_code: str,
    resource_path: Path,
) -> None:
    if progress is None:
        return
    progress.update_resources(measure_resources(resource_path).as_dict())
    progress.finish(status=status, phase=phase, reason_code=reason_code)


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


@contextmanager
def _maintenance_lock(path: Path) -> Iterable[bool]:
    """Acquire one host-local lock without holding a database write lock."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    token = f"{os.getpid()}:{uuid.uuid4().hex}"
    descriptor: int | None = None
    acquired = False
    for _attempt in range(2):
        try:
            descriptor = os.open(
                str(target),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            os.write(
                descriptor,
                json.dumps(
                    {"owner_pid": os.getpid(), "token": token},
                    separators=(",", ":"),
                ).encode("utf-8"),
            )
            os.close(descriptor)
            descriptor = None
            acquired = True
            break
        except FileExistsError:
            parsed_owner = False
            try:
                existing = json.loads(target.read_text(encoding="utf-8")[:4096])
                owner_pid = int(existing.get("owner_pid") or 0)
                parsed_owner = owner_pid > 0
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                owner_pid = 0
            if not parsed_owner:
                try:
                    age_seconds = max(
                        0.0,
                        datetime.now(timezone.utc).timestamp()
                        - target.stat().st_mtime,
                    )
                except OSError:
                    break
                # Another process may have created the file but not finished
                # its tiny owner write.  Only recover malformed locks after a
                # conservative stale interval.
                if age_seconds < 600:
                    break
            elif _process_alive(owner_pid):
                break
            try:
                target.unlink(missing_ok=True)
            except OSError:
                break
        finally:
            if descriptor is not None:
                os.close(descriptor)
                descriptor = None
    try:
        yield acquired
    finally:
        if not acquired:
            return
        try:
            existing = json.loads(target.read_text(encoding="utf-8")[:4096])
            if str(existing.get("token") or "") == token:
                target.unlink(missing_ok=True)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            # Never remove a lock whose ownership can no longer be proven.
            pass


def _run_feature_maintenance_locked(
    settings: Any,
    *,
    full: bool = False,
    now: datetime | None = None,
    worker_id: str | None = None,
) -> dict[str, Any]:
    """Run maintenance without reopening a frozen top-level JSON snapshot.

    The scheduled path deliberately separates the cheap control plane from
    source access.  Sunday and disabled runs return before opening the Feature
    Store; weekday incrementals claim the durable queue before selecting a
    source, so an empty queue is genuinely near-zero work.
    """

    current = _aware(now or datetime.now(SHANGHAI))
    mode = "FULL" if full or current.weekday() == 5 else "INCREMENTAL"
    storage_root = Path(
        getattr(settings, "root", Path(settings.feature_store_db_path).parent)
    )
    progress = _progress_for_maintenance(settings, current)
    _progress_phase(
        progress,
        "FEATURE_MAINTENANCE_PRECHECK",
        resource_path=storage_root,
    )

    def _base_payload(*, status: str, reason_code: str | None) -> dict[str, Any]:
        return {
            "status": status,
            "mode": mode,
            "reason_code": reason_code,
            "maintenance_at": current.isoformat(),
            "resources": measure_resources(storage_root).as_dict(),
            "llm_invoked": False,
            "external_orders": False,
        }

    if not bool(getattr(settings, "feature_maintenance_enabled", True)):
        reason = "FEATURE_MAINTENANCE_DISABLED"
        _progress_finish(
            progress,
            status="SUCCEEDED",
            phase="FEATURE_MAINTENANCE_NOOP",
            reason_code=reason,
            resource_path=storage_root,
        )
        return _base_payload(status="NOOP", reason_code=reason)

    if current.weekday() == 6:
        reason = "NON_MAINTENANCE_DAY"
        _progress_finish(
            progress,
            status="SUCCEEDED",
            phase="FEATURE_MAINTENANCE_NOOP",
            reason_code=reason,
            resource_path=storage_root,
        )
        return _base_payload(status="NOOP", reason_code=reason)

    watermark: Any | None = None
    if mode == "FULL":
        watermark = evaluate_disk_watermark(storage_root)
        if not watermark.full_rebuild_allowed:
            reason = "FEATURE_FULL_REBUILD_STORAGE_WATERMARK_BLOCKED"
            _progress_finish(
                progress,
                status="FAILED",
                phase="FEATURE_MAINTENANCE_PRECHECK",
                reason_code=reason,
                resource_path=storage_root,
            )
            return {
                **_base_payload(status="FAILED_RESOURCE", reason_code=reason),
                "storage_watermark": watermark.as_dict(),
            }

    store = ResearchFeatureStore(settings.feature_store_db_path)
    coordinator = FeatureRebuildCoordinator(
        store,
        lambda *_args: None,
        include_runtime_projections=False,
    )
    selected_source: dict[str, Any] | None = None
    source_symbols: tuple[str, ...] = ()
    batch_size = int(getattr(settings, "feature_source_batch_size", 50))

    def _select_source() -> dict[str, Any]:
        nonlocal selected_source, source_symbols
        selected_source = _select_ready_live_source(store, as_of=current)
        if selected_source is None:
            raise FeatureGenerationError("LIVE_SOURCE_NOT_AVAILABLE")
        source_symbols = _source_symbols(
            store,
            str(selected_source["generation_id"]),
        )
        if not source_symbols:
            raise FeatureGenerationError("FEATURE_SOURCE_GENERATION_INVALID")
        return selected_source

    def _validate_target(
        feature_store: ResearchFeatureStore,
        generation_id: str,
    ) -> Mapping[str, Any]:
        if selected_source is None or not source_symbols:
            raise FeatureGenerationError("LIVE_SOURCE_NOT_AVAILABLE")
        validation = validate_feature_generation(
            feature_store,
            generation_id,
            expected_entity_count=len(source_symbols),
            expected_symbols=source_symbols,
            purpose=("LIVE_FULL" if mode == "FULL" else "LIVE_INCREMENTAL"),
            coverage_contract=_maintenance_coverage_contract(selected_source),
        )
        source_counts_raw = _source_manifest(selected_source).get("counts")
        source_counts = (
            dict(source_counts_raw) if isinstance(source_counts_raw, Mapping) else {}
        )
        target_counts = validation.get("table_counts")
        target_counts = dict(target_counts) if isinstance(target_counts, Mapping) else {}
        count_mapping = {
            "members": "feature_generation_members",
            "fundamental": "stock_fundamental_features",
            "taxonomy": "taxonomy_membership_versions",
            "business": "business_exposure_facts",
        }
        mismatches = {
            key: {
                "source": int(source_counts.get(key) or 0),
                "target": int(target_counts.get(table) or 0),
            }
            for key, table in count_mapping.items()
            if int(source_counts.get(key) or 0) != int(target_counts.get(table) or 0)
        }
        if mismatches:
            raise FeatureGenerationError(
                "FEATURE_GENERATION_VALIDATION_FAILED:SOURCE_COUNT_MISMATCH"
            )
        validation["source_equivalence"] = {
            "status": "READY",
            "counts": {
                key: int(source_counts.get(key) or 0) for key in count_mapping
            },
        }
        return validation

    if mode == "INCREMENTAL":
        owner = worker_id or "feature-maintenance-incremental"
        batch = coordinator.claim_incremental_batch(
            worker_id=owner,
            now=current,
        )
        if batch.empty:
            reason = "NOOP_NO_DIRTY"
            _progress_finish(
                progress,
                status="SUCCEEDED",
                phase="FEATURE_MAINTENANCE_NOOP",
                reason_code=reason,
                resource_path=storage_root,
            )
            return _base_payload(status="NOOP", reason_code=reason)
        watermark = evaluate_disk_watermark(storage_root)
        if not watermark.incremental_write_allowed:
            def _blocked_incremental_source() -> Mapping[str, Any]:
                raise FeatureMaintenanceError(
                    "FEATURE_INCREMENTAL_STORAGE_WATERMARK_BLOCKED"
                )

            result = coordinator.run_incremental_claimed(
                batch,
                as_of=current,
                source_selector=_blocked_incremental_source,
            )
        else:
            _progress_phase(
                progress,
                "FEATURE_MAINTENANCE_SOURCE_SELECT",
                resource_path=storage_root,
            )
            try:
                source = _select_source()
            except Exception as exc:
                def _failed_source_selection(
                    error: Exception = exc,
                ) -> Mapping[str, Any]:
                    raise error

                result = coordinator.run_incremental_claimed(
                    batch,
                    as_of=current,
                    source_selector=_failed_source_selection,
                    copy_batch_size=batch_size,
                )
            else:
                _progress_phase(
                    progress,
                    "FEATURE_MAINTENANCE_BUILD",
                    resource_path=storage_root,
                )
                result = coordinator.run_incremental_claimed(
                    batch,
                    as_of=current,
                    source_selector=lambda: source,
                    validator=_validate_target,
                    source_compatibility_validator=_source_supports_dirty,
                    source_manifest_hash="",
                    copy_batch_size=batch_size,
                )
    else:
        _progress_phase(
            progress,
            "FEATURE_MAINTENANCE_SOURCE_SELECT",
            resource_path=storage_root,
        )
        try:
            source = _select_source()
            _progress_phase(
                progress,
                "FEATURE_MAINTENANCE_BUILD",
                resource_path=storage_root,
            )
            result = coordinator.run_full_from_live_source(
                as_of=source["as_of"],
                worker_id=worker_id or "feature-maintenance-full",
                source_generation_id=str(source["generation_id"]),
                validator=_validate_target,
                source_manifest_hash=str(
                    source.get("source_manifest_hash")
                    or source.get("source_hash")
                    or ""
                ),
                copy_batch_size=batch_size,
            )
        except Exception as exc:
            result = FeatureRebuildResult(
                mode="FULL",
                status="FAILED",
                reason_code=(
                    str(exc).split(":", 1)[0]
                    if str(exc).strip()
                    else type(exc).__name__.upper()
                ),
                error=str(exc)[:500],
            )

    reason = str(result.reason_code or "").strip() or None
    source_blocked = bool(
        reason
        and (
            reason.startswith("LIVE_SOURCE")
            or reason.startswith("FEATURE_SOURCE")
        )
    )
    resource_failed = bool(reason and "STORAGE_WATERMARK" in reason)
    if result.status == "PUBLISHED":
        public_status = "PUBLISHED"
        final_phase = "FEATURE_MAINTENANCE_PUBLISH"
        progress_status = "SUCCEEDED"
        final_reason = "PUBLISHED"
    elif result.status == "NOOP":
        public_status = "NOOP"
        final_phase = "FEATURE_MAINTENANCE_NOOP"
        progress_status = "SUCCEEDED"
        final_reason = reason or "NOOP_NO_DIRTY"
    elif source_blocked:
        public_status = "BLOCKED_SOURCE_GENERATION"
        final_phase = "FEATURE_MAINTENANCE_SOURCE_SELECT"
        progress_status = "FAILED"
        final_reason = reason or "LIVE_SOURCE_NOT_AVAILABLE"
    elif resource_failed:
        public_status = "FAILED_RESOURCE"
        final_phase = "FEATURE_MAINTENANCE_PRECHECK"
        progress_status = "FAILED"
        final_reason = reason or "FEATURE_STORAGE_WATERMARK_BLOCKED"
    else:
        public_status = "FAILED"
        final_phase = "FEATURE_MAINTENANCE_VALIDATE"
        progress_status = "FAILED"
        final_reason = reason or "FEATURE_MAINTENANCE_FAILED"
    _progress_finish(
        progress,
        status=progress_status,
        phase=final_phase,
        reason_code=final_reason,
        resource_path=storage_root,
    )

    source_metadata = (
        selected_source.get("metadata")
        if isinstance(selected_source, Mapping)
        and isinstance(selected_source.get("metadata"), Mapping)
        else {}
    )
    payload = {
        **result.as_dict(),
        **_base_payload(status=public_status, reason_code=final_reason),
        "source_generation_id": (
            str(selected_source.get("generation_id"))
            if isinstance(selected_source, Mapping)
            else None
        ),
        "snapshot_id": source_metadata.get("snapshot_id"),
        "snapshot_hash": (
            selected_source.get("snapshot_hash")
            if isinstance(selected_source, Mapping)
            else None
        ),
        "source_market_trade_date": source_metadata.get("market_trade_date"),
        "g0_count": len(source_symbols),
        "storage_watermark": (
            watermark.as_dict() if watermark is not None else None
        ),
    }
    return payload


def run_feature_maintenance(
    settings: Any,
    *,
    full: bool = False,
    now: datetime | None = None,
    worker_id: str | None = None,
) -> dict[str, Any]:
    """Run one host-wide maintenance operation with a crash-recoverable lock."""

    current = _aware(now or datetime.now(SHANGHAI))
    if (
        not bool(getattr(settings, "feature_maintenance_enabled", True))
        or current.weekday() == 6
    ):
        return _run_feature_maintenance_locked(
            settings,
            full=full,
            now=current,
            worker_id=worker_id,
        )
    lock_path = Path(settings.feature_store_db_path).with_suffix(
        ".maintenance.lock"
    )
    with _maintenance_lock(lock_path) as acquired:
        if acquired:
            return _run_feature_maintenance_locked(
                settings,
                full=full,
                now=current,
                worker_id=worker_id,
            )
        storage_root = Path(
            getattr(settings, "root", Path(settings.feature_store_db_path).parent)
        )
        progress = _progress_for_maintenance(settings, current)
        _progress_finish(
            progress,
            status="SUCCEEDED",
            phase="FEATURE_MAINTENANCE_NOOP",
            reason_code="FEATURE_MAINTENANCE_BUSY",
            resource_path=storage_root,
        )
        return {
            "status": "NOOP",
            "mode": "FULL" if full or current.weekday() == 5 else "INCREMENTAL",
            "reason_code": "FEATURE_MAINTENANCE_BUSY",
            "maintenance_at": current.isoformat(),
            "resources": measure_resources(storage_root).as_dict(),
            "llm_invoked": False,
            "external_orders": False,
        }


__all__ = [
    "FeatureMaintenanceError",
    "LiveSourceMaterializationResult",
    "SnapshotFeatureBuilder",
    "VerifiedFeatureSnapshot",
    "load_latest_verified_snapshot",
    "materialize_live_source",
    "run_feature_maintenance",
]
