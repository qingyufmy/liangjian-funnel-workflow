"""Field-level A1 coverage projection and fair incremental repair queue.

The ledger lives in the existing A1 registry SQLite file.  Raw facts remain
in their current immutable stores; this module records only a rebuildable
coverage projection and repair-task metadata.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from collections.abc import Sequence
from typing import Any, Callable, Iterable, Mapping


class A1GapReason(StrEnum):
    SOURCE_NOT_SUPPORTED = "SOURCE_NOT_SUPPORTED"
    ACCESS_NOT_AUTHORIZED = "ACCESS_NOT_AUTHORIZED"
    NOT_FETCHED = "NOT_FETCHED"
    BUDGET_DEFERRED = "BUDGET_DEFERRED"
    RATE_LIMITED = "RATE_LIMITED"
    TRANSIENT_FETCH_ERROR = "TRANSIENT_FETCH_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    FIELD_MISSING = "FIELD_MISSING"
    REPORT_NOT_PUBLISHED = "REPORT_NOT_PUBLISHED"
    HISTORICAL_PERIOD_NOT_AVAILABLE = "HISTORICAL_PERIOD_NOT_AVAILABLE"
    TIME_UNVERIFIED = "TIME_UNVERIFIED"
    STALE = "STALE"
    CROSS_SOURCE_CONFLICT = "CROSS_SOURCE_CONFLICT"
    FEATURE_BUILD_FAILED = "FEATURE_BUILD_FAILED"
    PACKET_OMITTED = "PACKET_OMITTED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


NON_RETRYABLE_GAPS = frozenset({
    A1GapReason.SOURCE_NOT_SUPPORTED.value,
    A1GapReason.ACCESS_NOT_AUTHORIZED.value,
    A1GapReason.NOT_APPLICABLE.value,
})


@dataclass(frozen=True)
class CoverageRequirement:
    symbol: str
    dataset: str
    field: str
    report_period: str
    as_of: datetime
    source_version: str
    required: bool = True
    applicable: bool = True
    requirement_reason: str = "A1_BASE_EVIDENCE"
    consumer_path: str = "A1_BASE"
    expected_unit: str | None = None
    expected_currency: str | None = None
    expected_scope: str | None = None

    @property
    def key(self) -> str:
        payload = "|".join((
            self.symbol,
            self.dataset,
            self.field,
            self.report_period,
            str(_iso(self.as_of)),
            self.source_version,
        ))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CoverageObservation:
    requested: bool = False
    raw_found: bool = False
    parsed: bool = False
    value: Any = None
    unit: str | None = None
    currency: str | None = None
    consolidation_scope: str | None = None
    announced_at: datetime | None = None
    available_at: datetime | None = None
    feature_ready: bool = False
    feature_generation: str | None = None
    packet_ready: bool = False
    evidence_ref: str | None = None
    gap_reason: str | None = None
    attempted_at: datetime | None = None


def _aware(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("A1 coverage timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | str | None) -> str | None:
    parsed = _aware(value)
    return parsed.isoformat() if parsed is not None else None


def _value_state(value: Any) -> str:
    if value is None:
        return "MISSING"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return "NON_FINITE"
        if numeric == 0:
            return "ZERO"
        if numeric < 0:
            return "NEGATIVE"
        return "POSITIVE"
    if isinstance(value, str) and not value.strip():
        return "MISSING"
    return "PRESENT"


def classify_coverage(
    requirement: CoverageRequirement,
    observation: CoverageObservation,
) -> dict[str, Any]:
    state = _value_state(observation.value)
    reason = str(observation.gap_reason or "").strip().upper() or None
    if not requirement.applicable:
        reason = A1GapReason.NOT_APPLICABLE.value
    elif not observation.requested:
        reason = reason or A1GapReason.NOT_FETCHED.value
    elif not observation.raw_found:
        reason = reason or A1GapReason.NOT_FETCHED.value
    elif not observation.parsed:
        reason = reason or A1GapReason.PARSE_ERROR.value
    elif state in {"MISSING", "NON_FINITE"}:
        reason = reason or A1GapReason.FIELD_MISSING.value
    elif requirement.expected_unit and observation.unit != requirement.expected_unit:
        reason = reason or A1GapReason.SCHEMA_CHANGED.value
    elif requirement.expected_currency and observation.currency != requirement.expected_currency:
        reason = reason or A1GapReason.CROSS_SOURCE_CONFLICT.value
    elif requirement.expected_scope and observation.consolidation_scope != requirement.expected_scope:
        reason = reason or A1GapReason.CROSS_SOURCE_CONFLICT.value
    else:
        announced = _aware(observation.announced_at)
        available = _aware(observation.available_at)
        cutoff = _aware(requirement.as_of)
        if announced is None or available is None:
            reason = reason or A1GapReason.TIME_UNVERIFIED.value
        elif announced > cutoff or available > cutoff:
            reason = reason or A1GapReason.TIME_UNVERIFIED.value
        elif not observation.feature_ready:
            reason = reason or A1GapReason.FEATURE_BUILD_FAILED.value
        elif not observation.packet_ready:
            reason = reason or A1GapReason.PACKET_OMITTED.value

    raw_found = requirement.applicable and observation.raw_found
    parsed = raw_found and observation.parsed and state not in {"MISSING", "NON_FINITE"}
    point_in_time_ready = parsed and reason not in {
        A1GapReason.SCHEMA_CHANGED.value,
        A1GapReason.CROSS_SOURCE_CONFLICT.value,
        A1GapReason.TIME_UNVERIFIED.value,
        A1GapReason.STALE.value,
    }
    feature_ready = point_in_time_ready and observation.feature_ready
    packet_ready = feature_ready and observation.packet_ready
    return {
        "coverage_key": requirement.key,
        "value_state": state,
        "gap_reason": None if packet_ready else reason,
        "requested": bool(observation.requested),
        "raw_found": bool(raw_found),
        "parsed": bool(parsed),
        "point_in_time_ready": bool(point_in_time_ready),
        "feature_ready": bool(feature_ready),
        "packet_ready": bool(packet_ready),
    }


class A1CoverageLedger:
    """Rebuildable projection stored beside A1 generations."""

    PRIORITY_ORDER = ("HOLDING", "CANDIDATE", "STALE", "UNIVERSE")
    PRIORITY_WEIGHTS = {"HOLDING": 35, "CANDIDATE": 30, "STALE": 20, "UNIVERSE": 15}

    def __init__(self, registry_path: str | Path):
        candidate = Path(registry_path)
        if candidate.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            candidate = candidate / "a1_registry.sqlite3"
        candidate.parent.mkdir(parents=True, exist_ok=True)
        self.path = candidate.resolve()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS a1_coverage_projection (
                    coverage_key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL, dataset TEXT NOT NULL, field TEXT NOT NULL,
                    report_period TEXT NOT NULL, as_of TEXT NOT NULL, source_version TEXT NOT NULL,
                    required INTEGER NOT NULL, applicable INTEGER NOT NULL,
                    requirement_reason TEXT NOT NULL, consumer_path TEXT NOT NULL,
                    requested INTEGER NOT NULL, raw_found INTEGER NOT NULL, parsed INTEGER NOT NULL,
                    point_in_time_ready INTEGER NOT NULL, feature_ready INTEGER NOT NULL,
                    packet_ready INTEGER NOT NULL, value_state TEXT NOT NULL,
                    unit TEXT, currency TEXT, consolidation_scope TEXT,
                    announced_at TEXT, available_at TEXT, feature_generation TEXT,
                    gap_reason TEXT, evidence_ref TEXT, attempted_at TEXT,
                    last_success_at TEXT, failure_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT, updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_a1_coverage_scope
                    ON a1_coverage_projection(consumer_path,symbol,dataset,field);
                CREATE TABLE IF NOT EXISTS a1_backfill_tasks (
                    task_key TEXT PRIMARY KEY,
                    coverage_key TEXT NOT NULL,
                    priority_class TEXT NOT NULL,
                    status TEXT NOT NULL,
                    queued_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    next_attempt_at TEXT, retry_after_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT, lease_until TEXT, fencing_token INTEGER NOT NULL DEFAULT 0,
                    last_reason TEXT,
                    FOREIGN KEY(coverage_key) REFERENCES a1_coverage_projection(coverage_key)
                );
                CREATE INDEX IF NOT EXISTS idx_a1_backfill_ready
                    ON a1_backfill_tasks(status,priority_class,next_attempt_at,queued_at);
            """)

    def record(
        self,
        requirement: CoverageRequirement,
        observation: CoverageObservation,
        *,
        recorded_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = _aware(recorded_at or datetime.now(timezone.utc))
        classified = classify_coverage(requirement, observation)
        success_at = now.isoformat() if classified["packet_ready"] else None
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT failure_count,last_success_at,attempted_at FROM a1_coverage_projection WHERE coverage_key=?",
                (requirement.key,),
            ).fetchone()
            failure_count = int(existing["failure_count"] if existing else 0)
            same_attempt = bool(
                existing is not None
                and _iso(observation.attempted_at)
                and str(existing["attempted_at"] or "") == _iso(observation.attempted_at)
            )
            if classified["packet_ready"]:
                failure_count = 0
            elif requirement.applicable and not same_attempt:
                failure_count += 1
            connection.execute("""
                INSERT INTO a1_coverage_projection(
                    coverage_key,symbol,dataset,field,report_period,as_of,source_version,
                    required,applicable,requirement_reason,consumer_path,requested,raw_found,
                    parsed,point_in_time_ready,feature_ready,packet_ready,value_state,unit,currency,
                    consolidation_scope,announced_at,available_at,feature_generation,gap_reason,
                    evidence_ref,attempted_at,last_success_at,failure_count,next_attempt_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(coverage_key) DO UPDATE SET
                    requested=excluded.requested,raw_found=excluded.raw_found,parsed=excluded.parsed,
                    point_in_time_ready=excluded.point_in_time_ready,feature_ready=excluded.feature_ready,
                    packet_ready=excluded.packet_ready,value_state=excluded.value_state,unit=excluded.unit,
                    currency=excluded.currency,consolidation_scope=excluded.consolidation_scope,
                    announced_at=excluded.announced_at,available_at=excluded.available_at,
                    feature_generation=excluded.feature_generation,gap_reason=excluded.gap_reason,
                    evidence_ref=excluded.evidence_ref,attempted_at=excluded.attempted_at,
                    last_success_at=COALESCE(excluded.last_success_at,a1_coverage_projection.last_success_at),
                    failure_count=excluded.failure_count,updated_at=excluded.updated_at
            """, (
                requirement.key, requirement.symbol, requirement.dataset, requirement.field,
                requirement.report_period, _iso(requirement.as_of), requirement.source_version,
                int(requirement.required), int(requirement.applicable), requirement.requirement_reason,
                requirement.consumer_path, int(classified["requested"]), int(classified["raw_found"]),
                int(classified["parsed"]), int(classified["point_in_time_ready"]),
                int(classified["feature_ready"]), int(classified["packet_ready"]),
                classified["value_state"], observation.unit, observation.currency,
                observation.consolidation_scope, _iso(observation.announced_at),
                _iso(observation.available_at), observation.feature_generation,
                classified["gap_reason"], observation.evidence_ref, _iso(observation.attempted_at),
                success_at, failure_count, None, now.isoformat(),
            ))
        return self.get(requirement.key) or {}

    def record_many(
        self,
        records: Iterable[tuple[CoverageRequirement, CoverageObservation]],
        *,
        recorded_at: datetime | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """Persist one snapshot projection in a single bounded transaction.

        Snapshot construction can cover thousands of field instances.  Opening
        one SQLite connection per field made the projection itself a new A1
        bottleneck, so the batch path preserves the exact ``record`` semantics
        while committing atomically.
        """

        values = tuple(records)
        if not values:
            return ()
        now = _aware(recorded_at or datetime.now(timezone.utc))
        keys: list[str] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for requirement, observation in values:
                    classified = classify_coverage(requirement, observation)
                    existing = connection.execute(
                        "SELECT failure_count,attempted_at FROM a1_coverage_projection WHERE coverage_key=?",
                        (requirement.key,),
                    ).fetchone()
                    failure_count = int(existing["failure_count"] if existing else 0)
                    same_attempt = bool(
                        existing is not None
                        and _iso(observation.attempted_at)
                        and str(existing["attempted_at"] or "") == _iso(observation.attempted_at)
                    )
                    if classified["packet_ready"]:
                        failure_count = 0
                    elif requirement.applicable and not same_attempt:
                        failure_count += 1
                    connection.execute("""
                        INSERT INTO a1_coverage_projection(
                            coverage_key,symbol,dataset,field,report_period,as_of,source_version,
                            required,applicable,requirement_reason,consumer_path,requested,raw_found,
                            parsed,point_in_time_ready,feature_ready,packet_ready,value_state,unit,currency,
                            consolidation_scope,announced_at,available_at,feature_generation,gap_reason,
                            evidence_ref,attempted_at,last_success_at,failure_count,next_attempt_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(coverage_key) DO UPDATE SET
                            requested=excluded.requested,raw_found=excluded.raw_found,parsed=excluded.parsed,
                            point_in_time_ready=excluded.point_in_time_ready,feature_ready=excluded.feature_ready,
                            packet_ready=excluded.packet_ready,value_state=excluded.value_state,unit=excluded.unit,
                            currency=excluded.currency,consolidation_scope=excluded.consolidation_scope,
                            announced_at=excluded.announced_at,available_at=excluded.available_at,
                            feature_generation=excluded.feature_generation,gap_reason=excluded.gap_reason,
                            evidence_ref=excluded.evidence_ref,attempted_at=excluded.attempted_at,
                            last_success_at=COALESCE(excluded.last_success_at,a1_coverage_projection.last_success_at),
                            failure_count=excluded.failure_count,updated_at=excluded.updated_at
                    """, (
                        requirement.key, requirement.symbol, requirement.dataset, requirement.field,
                        requirement.report_period, _iso(requirement.as_of), requirement.source_version,
                        int(requirement.required), int(requirement.applicable), requirement.requirement_reason,
                        requirement.consumer_path, int(classified["requested"]), int(classified["raw_found"]),
                        int(classified["parsed"]), int(classified["point_in_time_ready"]),
                        int(classified["feature_ready"]), int(classified["packet_ready"]),
                        classified["value_state"], observation.unit, observation.currency,
                        observation.consolidation_scope, _iso(observation.announced_at),
                        _iso(observation.available_at), observation.feature_generation,
                        classified["gap_reason"], observation.evidence_ref, _iso(observation.attempted_at),
                        now.isoformat() if classified["packet_ready"] else None,
                        failure_count, None, now.isoformat(),
                    ))
                    keys.append(requirement.key)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            placeholders = ",".join("?" for _ in keys)
            rows = connection.execute(
                f"SELECT * FROM a1_coverage_projection WHERE coverage_key IN ({placeholders}) "
                "ORDER BY symbol,dataset,field,report_period,source_version",
                keys,
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def get(self, coverage_key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM a1_coverage_projection WHERE coverage_key=?", (coverage_key,)
            ).fetchone()
        return dict(row) if row else None

    def rows(
        self,
        *,
        consumer_path: str | None = None,
        source_version: str | None = None,
        as_of: datetime | str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        sql = "SELECT * FROM a1_coverage_projection"
        clauses: list[str] = []
        params: list[Any] = []
        if consumer_path:
            clauses.append("consumer_path=?")
            params.append(consumer_path)
        if source_version:
            clauses.append("source_version=?")
            params.append(source_version)
        if as_of is not None:
            clauses.append("as_of<=?")
            params.append(_iso(as_of))
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY symbol,dataset,field,report_period,source_version"
        with self._connect() as connection:
            values = connection.execute(sql, params).fetchall()
        return tuple(dict(row) for row in values)

    def latest_source_version(
        self,
        *,
        consumer_path: str | None = None,
        as_of: datetime | str | None = None,
    ) -> str | None:
        clauses: list[str] = []
        params: list[Any] = []
        if consumer_path:
            clauses.append("consumer_path=?")
            params.append(consumer_path)
        if as_of is not None:
            clauses.append("as_of<=?")
            params.append(_iso(as_of))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT source_version,MAX(updated_at) latest_update "
                f"FROM a1_coverage_projection{where} "
                "GROUP BY source_version ORDER BY latest_update DESC,source_version DESC LIMIT 1",
                params,
            ).fetchone()
        return str(row["source_version"]) if row is not None else None

    def coverage_report(
        self,
        *,
        consumer_path: str | None = None,
        source_version: str | None = None,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any]:
        rows = self.rows(
            consumer_path=consumer_path,
            source_version=source_version,
            as_of=as_of,
        )
        applicable = [row for row in rows if row["applicable"] and row["required"]]
        layers = (
            ("REQUESTED", "requested"),
            ("RAW_FOUND", "raw_found"),
            ("PARSED", "parsed"),
            ("POINT_IN_TIME_READY", "point_in_time_ready"),
            ("FEATURE_READY", "feature_ready"),
            ("PACKET_READY", "packet_ready"),
        )
        layer_report: dict[str, Any] = {}
        prior = {row["coverage_key"] for row in applicable}
        for label, column in layers:
            current = {row["coverage_key"] for row in applicable if row[column]}
            lost = prior - current
            reasons: dict[str, int] = {}
            for row in applicable:
                if row["coverage_key"] in lost:
                    reason = str(row.get("gap_reason") or f"{label}_MISSING")
                    reasons[reason] = reasons.get(reason, 0) + 1
            layer_report[label] = {
                "count": len(current),
                "set_hash": hashlib.sha256("\n".join(sorted(current)).encode("utf-8")).hexdigest(),
                "lost_from_previous": len(lost),
                "loss_reasons": dict(sorted(reasons.items())),
            }
            prior = current
        denominator = len(applicable)
        ready = int(layer_report["PACKET_READY"]["count"])
        parsed = int(layer_report["PARSED"]["count"])
        point_in_time_ready = int(layer_report["POINT_IN_TIME_READY"]["count"])
        not_applicable = sum(1 for row in rows if not row["applicable"])
        denominator_keys = [
            "|".join((
                str(row["symbol"]), str(row["dataset"]), str(row["field"]),
                str(row["report_period"]), str(row["source_version"]),
                str(row["consumer_path"]), str(row["required"]), str(row["applicable"]),
            ))
            for row in rows
        ]
        denominator_version = hashlib.sha256(
            "\n".join(sorted(denominator_keys)).encode("utf-8")
        ).hexdigest()
        symbols = sorted({str(row["symbol"]) for row in applicable})
        ready_by_symbol = {
            symbol: all(bool(row["packet_ready"]) for row in applicable if row["symbol"] == symbol)
            for symbol in symbols
        }
        gaps = [
            {
                "coverage_key": row["coverage_key"], "symbol": row["symbol"],
                "dataset": row["dataset"], "field": row["field"],
                "report_period": row["report_period"], "gap_reason": row["gap_reason"],
            }
            for row in applicable if not row["packet_ready"]
        ]
        return {
            "schema_version": "a1-coverage-report/1.0.0",
            "consumer_path": consumer_path or "ALL",
            "source_version": source_version or "ALL",
            "as_of": _iso(as_of),
            "denominator_version": denominator_version,
            "denominator": denominator,
            "packet_ready": ready,
            "required_field_coverage": (ready / denominator if denominator else None),
            "base_fact_coverage": (parsed / denominator if denominator else None),
            "point_in_time_coverage": (point_in_time_ready / denominator if denominator else None),
            "path_symbol_denominator": len(symbols),
            "path_research_ready": sum(1 for value in ready_by_symbol.values() if value),
            "path_research_rate": (
                sum(1 for value in ready_by_symbol.values() if value) / len(symbols)
                if symbols else None
            ),
            "not_applicable": not_applicable,
            "status": "N_A" if not denominator else "READY" if ready == denominator else "INCOMPLETE",
            "layers": layer_report,
            "gaps": gaps,
        }

    def enqueue_gap(
        self,
        coverage_key: str,
        *,
        priority_class: str = "UNIVERSE",
        now: datetime | None = None,
    ) -> bool:
        priority = str(priority_class).upper()
        if priority not in self.PRIORITY_ORDER:
            raise ValueError("invalid A1 backfill priority")
        current = _aware(now or datetime.now(timezone.utc))
        row = self.get(coverage_key)
        if row is None:
            raise KeyError("A1_COVERAGE_KEY_NOT_FOUND")
        reason = str(row.get("gap_reason") or "")
        status = "PERMANENT" if reason in NON_RETRYABLE_GAPS else "QUEUED"
        with self._connect() as connection:
            before = connection.total_changes
            connection.execute("""
                INSERT OR IGNORE INTO a1_backfill_tasks(
                    task_key,coverage_key,priority_class,status,queued_at,updated_at,last_reason
                ) VALUES(?,?,?,?,?,?,?)
            """, (coverage_key, coverage_key, priority, status, current.isoformat(), current.isoformat(), reason))
            return connection.total_changes > before

    def enqueue_gaps(
        self,
        coverage_keys: Iterable[str],
        *,
        priority_class: str = "UNIVERSE",
        now: datetime | None = None,
    ) -> int:
        """Idempotently enqueue a bounded set without per-field connections."""

        priority = str(priority_class).upper()
        if priority not in self.PRIORITY_ORDER:
            raise ValueError("invalid A1 backfill priority")
        keys = tuple(dict.fromkeys(str(key) for key in coverage_keys if str(key)))
        if not keys:
            return 0
        current = _aware(now or datetime.now(timezone.utc))
        inserted = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for key in keys:
                    row = connection.execute(
                        "SELECT gap_reason FROM a1_coverage_projection WHERE coverage_key=?",
                        (key,),
                    ).fetchone()
                    if row is None:
                        raise KeyError("A1_COVERAGE_KEY_NOT_FOUND")
                    reason = str(row["gap_reason"] or "")
                    status = "PERMANENT" if reason in NON_RETRYABLE_GAPS else "QUEUED"
                    cursor = connection.execute("""
                        INSERT OR IGNORE INTO a1_backfill_tasks(
                            task_key,coverage_key,priority_class,status,queued_at,updated_at,last_reason
                        ) VALUES(?,?,?,?,?,?,?)
                    """, (key, key, priority, status, current.isoformat(), current.isoformat(), reason))
                    inserted += max(0, int(cursor.rowcount))
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return inserted

    def plan_backfill(
        self,
        *,
        limit: int,
        now: datetime | None = None,
        consumer_path: str | None = None,
        source_version: str | None = None,
        as_of: datetime | str | None = None,
        retry_budget: int | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if limit <= 0:
            return ()
        current = _aware(now or datetime.now(timezone.utc))
        clauses = [
            "t.status IN ('QUEUED','DEFERRED')",
            "(t.next_attempt_at IS NULL OR t.next_attempt_at<=?)",
            "(t.retry_after_at IS NULL OR t.retry_after_at<=?)",
        ]
        params: list[Any] = [current.isoformat(), current.isoformat()]
        if consumer_path:
            clauses.append("p.consumer_path=?")
            params.append(consumer_path)
        if source_version:
            clauses.append("p.source_version=?")
            params.append(source_version)
        if as_of is not None:
            clauses.append("p.as_of<=?")
            params.append(_iso(as_of))
        if retry_budget is not None:
            clauses.append("t.attempt_count<?")
            params.append(max(0, int(retry_budget)))
        with self._connect() as connection:
            rows = [dict(row) for row in connection.execute(f"""
                SELECT t.*,p.symbol,p.dataset,p.field,p.report_period,p.as_of,p.source_version,
                       p.required,p.applicable,p.requirement_reason,p.consumer_path,p.gap_reason
                FROM a1_backfill_tasks t
                JOIN a1_coverage_projection p ON p.coverage_key=t.coverage_key
                WHERE {' AND '.join(clauses)}
                ORDER BY t.queued_at,t.task_key
            """, params).fetchall()]
        by_priority = {priority: [] for priority in self.PRIORITY_ORDER}
        for row in rows:
            by_priority[str(row["priority_class"])].append(row)
        selected: list[dict[str, Any]] = []
        if limit >= len(self.PRIORITY_ORDER):
            for priority in self.PRIORITY_ORDER:
                quota = max(1, limit * self.PRIORITY_WEIGHTS[priority] // 100)
                selected.extend(by_priority[priority][:quota])
                by_priority[priority] = by_priority[priority][quota:]
        while len(selected) < limit and any(by_priority.values()):
            for priority in self.PRIORITY_ORDER:
                if by_priority[priority] and len(selected) < limit:
                    selected.append(by_priority[priority].pop(0))
        return tuple(selected[:limit])

    def run_backfill(
        self,
        worker: Callable[[Mapping[str, Any]], CoverageObservation],
        *,
        owner: str,
        limit: int,
        now: datetime | None = None,
        consumer_path: str | None = None,
        source_version: str | None = None,
        as_of: datetime | str | None = None,
        retry_budget: int | None = None,
    ) -> dict[str, Any]:
        """Run one bounded slice with an injected, governed source adapter.

        No adapter is selected implicitly.  The caller owns network authority;
        this method owns leases, fencing, observation persistence and retry
        transitions.  Tests inject a local worker, while the CLI stays blocked
        until an operator-configured adapter is registered.
        """

        current = _aware(now or datetime.now(timezone.utc))
        plan = self.plan_backfill(
            limit=limit,
            now=current,
            consumer_path=consumer_path,
            source_version=source_version,
            as_of=as_of,
            retry_budget=retry_budget,
        )
        counts = {"planned": len(plan), "succeeded": 0, "deferred": 0, "skipped": 0}
        results: list[dict[str, Any]] = []
        for task in plan:
            task_key = str(task["task_key"])
            token = self.claim(task_key, owner=owner, now=current)
            if token is None:
                counts["skipped"] += 1
                continue
            try:
                observation = worker(task)
                if not isinstance(observation, CoverageObservation):
                    raise TypeError("A1 backfill worker must return CoverageObservation")
                requirement = CoverageRequirement(
                    symbol=str(task["symbol"]),
                    dataset=str(task["dataset"]),
                    field=str(task["field"]),
                    report_period=str(task["report_period"]),
                    as_of=_aware(str(task["as_of"])) or current,
                    source_version=str(task["source_version"]),
                    required=bool(task["required"]),
                    applicable=bool(task["applicable"]),
                    requirement_reason=str(task["requirement_reason"]),
                    consumer_path=str(task["consumer_path"]),
                )
                row = self.record(requirement, observation, recorded_at=current)
                success = bool(row.get("packet_ready"))
                reason = str(row.get("gap_reason") or "OK")
                self.complete(
                    task_key,
                    owner=owner,
                    fencing_token=token,
                    success=success,
                    reason_code=reason,
                    now=current,
                )
                counts["succeeded" if success else "deferred"] += 1
                results.append({
                    "task_key": task_key,
                    "status": "SUCCEEDED" if success else "DEFERRED",
                    "reason_code": reason,
                })
            except Exception as exc:
                self.complete(
                    task_key,
                    owner=owner,
                    fencing_token=token,
                    success=False,
                    reason_code="TRANSIENT_FETCH_ERROR",
                    now=current,
                )
                counts["deferred"] += 1
                results.append({
                    "task_key": task_key,
                    "status": "DEFERRED",
                    "reason_code": "TRANSIENT_FETCH_ERROR",
                    "error_type": type(exc).__name__,
                })
        return {
            "schema_version": "a1-backfill-run/1.0.0",
            "owner": owner,
            "counts": counts,
            "results": results,
        }

    def claim(
        self,
        task_key: str,
        *,
        owner: str,
        lease_seconds: int = 60,
        now: datetime | None = None,
    ) -> int | None:
        current = _aware(now or datetime.now(timezone.utc))
        lease_until = current + timedelta(seconds=max(1, lease_seconds))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM a1_backfill_tasks WHERE task_key=?", (task_key,)
            ).fetchone()
            if row is None or str(row["status"]) in {"SUCCEEDED", "PERMANENT"}:
                connection.rollback()
                return None
            if row["retry_after_at"] and str(row["retry_after_at"]) > current.isoformat():
                connection.rollback()
                return None
            if row["lease_until"] and str(row["lease_until"]) > current.isoformat() and row["lease_owner"] != owner:
                connection.rollback()
                return None
            token = int(row["fencing_token"]) + 1
            connection.execute("""
                UPDATE a1_backfill_tasks
                SET status='LEASED',lease_owner=?,lease_until=?,fencing_token=?,
                    attempt_count=attempt_count+1,updated_at=?
                WHERE task_key=?
            """, (owner, lease_until.isoformat(), token, current.isoformat(), task_key))
            connection.commit()
            return token

    def complete(
        self,
        task_key: str,
        *,
        owner: str,
        fencing_token: int,
        success: bool,
        reason_code: str = "OK",
        retry_after: datetime | None = None,
        now: datetime | None = None,
    ) -> bool:
        current = _aware(now or datetime.now(timezone.utc))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM a1_backfill_tasks WHERE task_key=?", (task_key,)
            ).fetchone()
            if row is None or row["lease_owner"] != owner or int(row["fencing_token"]) != fencing_token:
                return False
            permanent = str(reason_code).upper() in NON_RETRYABLE_GAPS
            status = "SUCCEEDED" if success else "PERMANENT" if permanent else "DEFERRED"
            attempts = int(row["attempt_count"])
            next_attempt = None
            if not success and not permanent:
                next_attempt = current + timedelta(minutes=min(24 * 60, 2 ** min(attempts, 10)))
            retry = _aware(retry_after)
            if retry is not None and (next_attempt is None or retry > next_attempt):
                next_attempt = retry
            connection.execute("""
                UPDATE a1_backfill_tasks
                SET status=?,next_attempt_at=?,retry_after_at=?,lease_owner=NULL,lease_until=NULL,
                    last_reason=?,updated_at=?
                WHERE task_key=? AND lease_owner=? AND fencing_token=?
            """, (
                status, _iso(next_attempt), _iso(retry), str(reason_code), current.isoformat(),
                task_key, owner, fencing_token,
            ))
            return connection.total_changes > 0

    def task_report(self, *, per_run_quota: int | None = None) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status,COUNT(*) count,MIN(queued_at) oldest FROM a1_backfill_tasks GROUP BY status"
            ).fetchall()
        result = {
            "by_status": {str(row["status"]): int(row["count"]) for row in rows},
            "oldest_by_status": {str(row["status"]): row["oldest"] for row in rows},
        }
        if per_run_quota is not None:
            pending = sum(
                int(row["count"]) for row in rows
                if str(row["status"]) in {"QUEUED", "DEFERRED", "LEASED"}
            )
            quota = max(1, int(per_run_quota))
            result["estimated_runs_to_clear"] = math.ceil(pending / quota)
            result["per_run_quota"] = quota
        return result


def minimum_evidence_contract(
    rows: Iterable[Mapping[str, Any]],
    *,
    required_periods: int = 3,
) -> dict[str, Any]:
    values = list(rows)
    pit = [row for row in values if row.get("point_in_time_ready")]
    periods = {str(row.get("report_period")) for row in pit if row.get("report_period")}
    if any(not row.get("announced_at") or not row.get("available_at") for row in values):
        return {"status": "INCOMPLETE", "reason_code": A1GapReason.TIME_UNVERIFIED.value, "period_count": len(periods)}
    if len(periods) < required_periods:
        return {
            "status": "INCOMPLETE",
            "reason_code": A1GapReason.HISTORICAL_PERIOD_NOT_AVAILABLE.value,
            "period_count": len(periods),
        }
    return {"status": "READY", "reason_code": "MINIMUM_EVIDENCE_READY", "period_count": len(periods)}


_INDICATOR_ALIASES: Mapping[str, tuple[str, ...]] = {
    "roe": ("roe", "index_weighted_avg_roe"),
    "revenue_growth": (
        "calculate_operating_income_yoy_growth_ratio", "operating_income_yoy", "revenue_yoy",
    ),
    "profit_growth": (
        "calculate_parent_holder_net_profit_yoy_growth_ratio", "net_profit_yoy",
    ),
    "cashflow_quality": (
        "net_profit_cash_content", "cashflow_net_income_ratio",
        "operating_cash_flow_net_divide_income",
    ),
}


def _coverage_time(value: Any, *, timezone_hint: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 100_000_000_000:
            numeric /= 1000.0
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone_hint)
    return parsed.astimezone(timezone.utc)


def _latest_statement(fundamental: Mapping[str, Any], dataset: str) -> Mapping[str, Any] | None:
    statements = fundamental.get("statements")
    rows = statements.get(dataset) if isinstance(statements, Mapping) else None
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return None
    mapped = [row for row in rows if isinstance(row, Mapping)]
    return max(
        mapped,
        key=lambda row: (
            float(row.get("report_date_ms") or 0),
            float(row.get("period_end_ms") or 0),
        ),
        default=None,
    )


def _statement_period(row: Mapping[str, Any] | None) -> str:
    if not isinstance(row, Mapping):
        return "LATEST"
    year = row.get("fiscal_year")
    period = row.get("fiscal_period")
    if year not in (None, "") and period not in (None, ""):
        return f"{year}{period}"
    return str(row.get("period_end_ms") or row.get("report_date_ms") or "LATEST")


def materialize_snapshot_coverage(
    ledger: A1CoverageLedger,
    snapshot: Mapping[str, Any],
    *,
    as_of: datetime,
    source_version: str,
    feature_generation: str | None = None,
    enqueue_missing: bool = True,
) -> dict[str, Any]:
    """Project the fields actually consumed by the current A1 snapshot.

    This is deliberately a projection over frozen data, not another fetcher.
    Missing timestamps stay ``TIME_UNVERIFIED`` and absent facts stay in the
    denominator.  A future source adapter may repair the queued keys without
    changing the denominator or the original frozen snapshot.
    """

    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("A1 coverage as_of must be timezone-aware")
    symbols_value = snapshot.get("g0_symbols")
    symbols = tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols_value
            if str(symbol).strip()
        )
    ) if isinstance(symbols_value, Sequence) and not isinstance(symbols_value, (str, bytes, bytearray)) else ()
    fundamentals_value = snapshot.get("COMPANY_FUNDAMENTALS")
    fundamentals = fundamentals_value if isinstance(fundamentals_value, Mapping) else {}
    business_value = snapshot.get("MAIN_BUSINESS_EVIDENCE")
    businesses = business_value if isinstance(business_value, Mapping) else {}
    rows: list[tuple[CoverageRequirement, CoverageObservation]] = []
    for symbol in symbols:
        raw_fundamental = fundamentals.get(symbol)
        fundamental = raw_fundamental if isinstance(raw_fundamental, Mapping) else {}
        indicators_value = fundamental.get("indicators")
        indicators = indicators_value if isinstance(indicators_value, Sequence) and not isinstance(indicators_value, (str, bytes, bytearray)) else ()
        indicator_map = {
            str(item.get("index_id") or item.get("name") or "").strip().lower(): item.get("value")
            for item in indicators if isinstance(item, Mapping)
        }
        latest_income = _latest_statement(fundamental, "INCOME")
        latest_cash = _latest_statement(fundamental, "CASH_FLOW")
        statement_specs = (
            ("INCOME", "operating_income", latest_income),
            ("INCOME", "parent_holder_net_profit", latest_income),
            ("CASH_FLOW", "act_cash_flow_net", latest_cash),
        )
        for dataset, field, statement in statement_specs:
            value = statement.get(field) if isinstance(statement, Mapping) else None
            published = _coverage_time(
                statement.get("report_date_ms") if isinstance(statement, Mapping) else None,
                timezone_hint=as_of.tzinfo,
            )
            requirement = CoverageRequirement(
                symbol=symbol,
                dataset=dataset,
                field=field,
                report_period=_statement_period(statement),
                as_of=as_of,
                source_version=source_version,
                requirement_reason="A1_FINANCIAL_QUALITY",
                consumer_path="A1_BASE",
                expected_unit="CNY",
                expected_currency="CNY",
                expected_scope="CONSOLIDATED",
            )
            rows.append((requirement, CoverageObservation(
                requested=True,
                raw_found=bool(fundamental),
                parsed=value is not None,
                value=value,
                unit="CNY" if value is not None else None,
                currency="CNY" if value is not None else None,
                consolidation_scope="CONSOLIDATED" if value is not None else None,
                announced_at=published,
                # The compact legacy snapshot does not retain fetched_at.
                # Never infer strict point-in-time availability from presence.
                available_at=None,
                feature_ready=value is not None,
                feature_generation=feature_generation,
                packet_ready=value is not None,
                evidence_ref=(f"snapshot:{source_version}:{symbol}:{dataset}:{_statement_period(statement)}" if value is not None else None),
                gap_reason=(
                    A1GapReason.TIME_UNVERIFIED.value
                    if value is not None
                    else A1GapReason.FIELD_MISSING.value
                    if fundamental
                    else A1GapReason.NOT_FETCHED.value
                ),
                attempted_at=as_of,
            )))
        indicator_period = _statement_period(latest_income)
        for field, aliases in _INDICATOR_ALIASES.items():
            selected = next((indicator_map[alias] for alias in aliases if alias in indicator_map), None)
            requirement = CoverageRequirement(
                symbol=symbol,
                dataset="INDICATORS",
                field=field,
                report_period=indicator_period,
                as_of=as_of,
                source_version=source_version,
                requirement_reason="A1_FINANCIAL_QUALITY",
                consumer_path="A1_BASE",
            )
            rows.append((requirement, CoverageObservation(
                requested=True,
                raw_found=bool(fundamental),
                parsed=selected is not None,
                value=selected,
                announced_at=None,
                available_at=None,
                feature_ready=selected is not None,
                feature_generation=feature_generation,
                packet_ready=selected is not None,
                evidence_ref=(f"snapshot:{source_version}:{symbol}:INDICATORS:{field}" if selected is not None else None),
                gap_reason=(
                    A1GapReason.TIME_UNVERIFIED.value
                    if selected is not None
                    else A1GapReason.FIELD_MISSING.value
                    if fundamental
                    else A1GapReason.NOT_FETCHED.value
                ),
                attempted_at=as_of,
            )))
        raw_business = businesses.get(symbol)
        business = raw_business if isinstance(raw_business, Mapping) else {}
        evidence_value = business.get("evidence")
        evidence = next(
            (item for item in evidence_value if isinstance(item, Mapping)),
            None,
        ) if isinstance(evidence_value, Sequence) and not isinstance(evidence_value, (str, bytes, bytearray)) else None
        published = _coverage_time(
            evidence.get("publish_time") if isinstance(evidence, Mapping) else None,
            timezone_hint=as_of.tzinfo,
        )
        available = _coverage_time(
            evidence.get("evidence_fetched_at") if isinstance(evidence, Mapping) else None,
            timezone_hint=as_of.tzinfo,
        )
        business_ref = str(evidence.get("source_ref") or "") if isinstance(evidence, Mapping) else ""
        requirement = CoverageRequirement(
            symbol=symbol,
            dataset="MAIN_BUSINESS_EVIDENCE",
            field="disclosed_business_evidence",
            report_period=(str(business.get("latest_full_report_publish_time") or "LATEST")[:10]),
            as_of=as_of,
            source_version=source_version,
            requirement_reason="A1_MAIN_BUSINESS_EXPOSURE",
            consumer_path="A1_BASE",
        )
        rows.append((requirement, CoverageObservation(
            requested=True,
            raw_found=isinstance(raw_business, Mapping),
            parsed=evidence is not None and bool(business_ref),
            value=business_ref or None,
            announced_at=published,
            available_at=available,
            feature_ready=evidence is not None and bool(business_ref),
            feature_generation=feature_generation,
            packet_ready=evidence is not None and bool(business_ref),
            evidence_ref=business_ref or None,
            gap_reason=(None if evidence is not None and business_ref else A1GapReason.FIELD_MISSING.value),
            attempted_at=as_of,
        )))
    projected = ledger.record_many(rows, recorded_at=as_of)
    if enqueue_missing:
        ledger.enqueue_gaps(
            (str(row["coverage_key"]) for row in projected if not row.get("packet_ready")),
            priority_class="CANDIDATE",
            now=as_of,
        )
    return ledger.coverage_report(
        consumer_path="A1_BASE",
        source_version=source_version,
        as_of=as_of,
    )


def packet_coverage_projection(report: Mapping[str, Any], *, gap_limit: int = 200) -> dict[str, Any]:
    gaps = list(report.get("gaps") or [])
    return {
        "schema_version": "a1-packet-coverage/1.0.0",
        "status": report.get("status"),
        "denominator": report.get("denominator"),
        "packet_ready": report.get("packet_ready"),
        "required_field_coverage": report.get("required_field_coverage"),
        "layers": report.get("layers") or {},
        "critical_gaps": gaps[:gap_limit],
        "projected_out_gap_count": max(0, len(gaps) - gap_limit),
        "projection_reason": "PACKET_GAP_LIMIT" if len(gaps) > gap_limit else None,
    }


__all__ = [
    "A1CoverageLedger",
    "A1GapReason",
    "CoverageObservation",
    "CoverageRequirement",
    "classify_coverage",
    "minimum_evidence_contract",
    "materialize_snapshot_coverage",
    "packet_coverage_projection",
]
