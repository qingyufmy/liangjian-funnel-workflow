"""Versioned local feature and deterministic-decision storage.

The raw fact cache remains the source of truth.  This database contains only
rebuildable projections and stage decisions, so a schema or scoring change can
invalidate derived rows without touching immutable market facts.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
FEATURE_SCHEMA = "liangjian-research-feature-store/1.0.0"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class ResearchFeatureStore:
    """Small WAL SQLite store for rebuildable research projections."""

    def __init__(self, path: str | Path):
        candidate = Path(path)
        if candidate.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            candidate = candidate / "research_feature_store.sqlite3"
        candidate.parent.mkdir(parents=True, exist_ok=True)
        self.path = candidate.resolve()
        self._schema_lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS feature_store_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS taxonomy_membership_versions (
                    taxonomy TEXT NOT NULL,
                    version_hash TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    taxonomy_code TEXT NOT NULL,
                    taxonomy_name TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (taxonomy, version_hash, symbol, taxonomy_code)
                );
                CREATE INDEX IF NOT EXISTS idx_taxonomy_symbol
                    ON taxonomy_membership_versions(taxonomy, symbol, as_of);

                CREATE TABLE IF NOT EXISTS theme_registry_versions (
                    run_id TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    version_hash TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, lane_id, version_hash)
                );

                CREATE TABLE IF NOT EXISTS chain_node_versions (
                    run_id TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    version_hash TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, lane_id, node_id, version_hash)
                );

                CREATE TABLE IF NOT EXISTS theme_taxonomy_links (
                    run_id TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    taxonomy TEXT NOT NULL,
                    taxonomy_code TEXT NOT NULL,
                    taxonomy_name TEXT,
                    match_method TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    source_hash TEXT NOT NULL,
                    PRIMARY KEY (run_id, lane_id, node_id, taxonomy, taxonomy_code)
                );

                CREATE TABLE IF NOT EXISTS business_exposure_facts (
                    symbol TEXT NOT NULL,
                    report_period TEXT NOT NULL,
                    business_name TEXT NOT NULL,
                    revenue_exposure_pct REAL,
                    gross_profit_exposure_pct REAL,
                    node_id TEXT,
                    evidence_ref TEXT NOT NULL,
                    page_number INTEGER,
                    confidence REAL NOT NULL,
                    parser_version TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (symbol, report_period, business_name, evidence_ref, parser_version)
                );

                CREATE TABLE IF NOT EXISTS stock_fundamental_features (
                    symbol TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    feature_version TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    quality_score REAL NOT NULL,
                    available INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (symbol, as_of, feature_version, source_hash)
                );

                CREATE TABLE IF NOT EXISTS stock_market_role_features (
                    run_id TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    theme_id TEXT NOT NULL,
                    feature_version TEXT NOT NULL,
                    role_score REAL NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, lane_id, symbol, theme_id, feature_version)
                );

                CREATE TABLE IF NOT EXISTS deterministic_stage_decisions (
                    run_id TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    score REAL,
                    node_id TEXT,
                    theme_id TEXT,
                    node_rank INTEGER,
                    sent_to_llm INTEGER NOT NULL,
                    reason_codes_json TEXT NOT NULL,
                    source_hashes_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, lane_id, stage, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_stage_decisions_summary
                    ON deterministic_stage_decisions(run_id, lane_id, stage, status);

                CREATE TABLE IF NOT EXISTS dirty_entities (
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    source_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    PRIMARY KEY (entity_type, entity_id, reason_code, source_version)
                );
                """
            )
            connection.execute(
                "INSERT INTO feature_store_meta(key, value) VALUES('schema', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (FEATURE_SCHEMA,),
            )
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def replace_stage_decisions(
        self,
        *,
        run_id: str,
        lane_id: str,
        stage: str,
        decisions: Sequence[Mapping[str, Any]],
        updated_at: datetime | str,
    ) -> int:
        """Atomically replace one stage projection for a lane."""

        timestamp = updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at)
        rows = []
        seen: set[str] = set()
        for raw in decisions:
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol or symbol in seen:
                raise ValueError("stage decisions require unique non-empty symbols")
            seen.add(symbol)
            reasons = tuple(dict.fromkeys(str(item) for item in raw.get("reason_codes", ()) if str(item)))
            source_hashes = raw.get("source_hashes") if isinstance(raw.get("source_hashes"), Mapping) else {}
            score = _optional_float(raw.get("score"))
            node_rank = _optional_int(raw.get("node_rank"))
            rows.append(
                (
                    run_id,
                    lane_id,
                    stage,
                    symbol,
                    str(raw.get("status") or "UNKNOWN"),
                    score,
                    _optional_text(raw.get("node_id")),
                    _optional_text(raw.get("theme_id")),
                    node_rank,
                    int(bool(raw.get("sent_to_llm"))),
                    canonical_json(reasons),
                    canonical_json(source_hashes),
                    canonical_json(raw),
                    timestamp,
                )
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM deterministic_stage_decisions WHERE run_id=? AND lane_id=? AND stage=?",
                (run_id, lane_id, stage),
            )
            connection.executemany(
                """
                INSERT INTO deterministic_stage_decisions(
                    run_id,lane_id,stage,symbol,status,score,node_id,theme_id,node_rank,
                    sent_to_llm,reason_codes_json,source_hashes_json,payload_json,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            connection.commit()
        return len(rows)

    def replace_taxonomy_memberships(
        self,
        *,
        taxonomy: str,
        snapshot: Mapping[str, Any],
        as_of: datetime | str,
    ) -> int:
        """Persist one immutable taxonomy projection version."""

        taxonomy_name = str(taxonomy).strip().upper()
        if taxonomy_name not in {"INDUSTRY", "CONCEPT"}:
            raise ValueError("taxonomy must be INDUSTRY or CONCEPT")
        records = snapshot.get("records")
        records = records if isinstance(records, list) else []
        timestamp = as_of.isoformat() if isinstance(as_of, datetime) else str(as_of)
        version_hash = content_hash({"taxonomy": taxonomy_name, "records": records})
        rows: list[tuple[Any, ...]] = []
        for raw in records:
            if not isinstance(raw, Mapping):
                continue
            symbol = str(raw.get("thscode") or raw.get("symbol") or "").strip().upper()
            memberships = raw.get("memberships")
            if not symbol or not isinstance(memberships, list):
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
                name = str(
                    membership.get("taxonomy_name")
                    or membership.get("industry_name")
                    or membership.get("concept_name")
                    or ""
                ).strip()
                if not code:
                    continue
                payload = {"symbol": symbol, **dict(membership)}
                rows.append((
                    taxonomy_name,
                    version_hash,
                    timestamp,
                    symbol,
                    code,
                    name,
                    content_hash(payload),
                    canonical_json(payload),
                ))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM taxonomy_membership_versions WHERE taxonomy=? AND version_hash=?",
                (taxonomy_name, version_hash),
            )
            connection.executemany(
                "INSERT INTO taxonomy_membership_versions(taxonomy,version_hash,as_of,symbol,taxonomy_code,taxonomy_name,source_hash,payload_json) "
                "VALUES(?,?,?,?,?,?,?,?)",
                rows,
            )
            connection.commit()
        return len(rows)

    def replace_business_exposure_facts(self, facts: Sequence[Mapping[str, Any]]) -> int:
        rows: list[tuple[Any, ...]] = []
        for fact in facts:
            symbol = str(fact.get("symbol") or "").strip().upper()
            evidence_ref = str(fact.get("evidence_ref") or "").strip()
            business_name = str(fact.get("business_name") or "").strip()
            parser_version = str(fact.get("parser_version") or "").strip()
            if not symbol or not evidence_ref or not business_name or not parser_version:
                continue
            rows.append((
                symbol,
                str(fact.get("report_period") or "UNKNOWN"),
                business_name,
                _optional_float(fact.get("revenue_exposure_pct")),
                _optional_float(fact.get("gross_profit_exposure_pct")),
                _optional_text(fact.get("node_id")),
                evidence_ref,
                _optional_int(fact.get("page_number")),
                max(0.0, min(1.0, _optional_float(fact.get("confidence")) or 0.0)),
                parser_version,
                str(fact.get("content_hash") or content_hash(fact)),
                canonical_json(fact),
            ))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT OR REPLACE INTO business_exposure_facts(symbol,report_period,business_name,revenue_exposure_pct,gross_profit_exposure_pct,node_id,evidence_ref,page_number,confidence,parser_version,content_hash,payload_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            connection.commit()
        return len(rows)

    def record_fundamental_features(
        self,
        *,
        as_of: datetime | str,
        decisions: Sequence[Mapping[str, Any]],
    ) -> int:
        timestamp = as_of.isoformat() if isinstance(as_of, datetime) else str(as_of)
        rows: list[tuple[Any, ...]] = []
        for decision in decisions:
            symbol = str(decision.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            payload = {
                "financial_features": decision.get("financial_features"),
                "financial_quality_score": decision.get("financial_quality_score"),
                "data_quality_score": decision.get("data_quality_score"),
                "liquidity_score": decision.get("liquidity_score"),
                "score_breakdown": decision.get("score_breakdown"),
            }
            rows.append((
                symbol,
                timestamp,
                str(decision.get("feature_version") or "UNKNOWN"),
                content_hash(decision.get("source_hashes") or {}),
                _optional_float(decision.get("data_quality_score")) or 0.0,
                int(bool(payload.get("financial_features"))),
                canonical_json(payload),
            ))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT OR REPLACE INTO stock_fundamental_features(symbol,as_of,feature_version,source_hash,quality_score,available,payload_json) "
                "VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            connection.commit()
        return len(rows)

    def record_market_role_features(
        self,
        *,
        run_id: str,
        lane_id: str,
        decisions: Sequence[Mapping[str, Any]],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for decision in decisions:
            symbol = str(decision.get("symbol") or "").strip().upper()
            theme_id = str(decision.get("theme_id") or "UNMAPPED")
            if not symbol:
                continue
            rows.append((
                run_id,
                lane_id,
                symbol,
                theme_id,
                str(decision.get("feature_version") or "UNKNOWN"),
                _optional_float(decision.get("score")) or 0.0,
                canonical_json(decision),
            ))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT OR REPLACE INTO stock_market_role_features(run_id,lane_id,symbol,theme_id,feature_version,role_score,payload_json) "
                "VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            connection.commit()
        return len(rows)

    def mark_dirty(
        self,
        *,
        entity_type: str,
        entity_id: str,
        reason_code: str,
        source_version: str,
        created_at: datetime | str,
    ) -> None:
        timestamp = created_at.isoformat() if isinstance(created_at, datetime) else str(created_at)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO dirty_entities(entity_type,entity_id,reason_code,source_version,created_at,resolved_at) VALUES(?,?,?,?,?,NULL)",
                (entity_type, entity_id, reason_code, source_version, timestamp),
            )

    def resolve_dirty(self, *, entity_type: str, entity_id: str, resolved_at: datetime | str) -> int:
        timestamp = resolved_at.isoformat() if isinstance(resolved_at, datetime) else str(resolved_at)
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE dirty_entities SET resolved_at=? WHERE entity_type=? AND entity_id=? AND resolved_at IS NULL",
                (timestamp, entity_type, entity_id),
            )
        return int(cursor.rowcount)

    def stage_summary(self, run_id: str, lane_id: str, stage: str) -> dict[str, Any]:
        with self._connect() as connection:
            grouped = connection.execute(
                """
                SELECT status, COUNT(*) AS count, SUM(sent_to_llm) AS sent
                FROM deterministic_stage_decisions
                WHERE run_id=? AND lane_id=? AND stage=?
                GROUP BY status ORDER BY status
                """,
                (run_id, lane_id, stage),
            ).fetchall()
        counts = {str(row["status"]): int(row["count"]) for row in grouped}
        return {
            "run_id": run_id,
            "lane_id": lane_id,
            "stage": stage,
            "evaluated_count": sum(counts.values()),
            "sent_to_llm_count": sum(int(row["sent"] or 0) for row in grouped),
            "status_counts": counts,
        }

    def stage_decisions(
        self,
        run_id: str,
        lane_id: str,
        stage: str,
        *,
        status: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = ["run_id=?", "lane_id=?", "stage=?"]
        parameters: list[Any] = [run_id, lane_id, stage]
        if status:
            clauses.append("status=?")
            parameters.append(status)
        parameters.extend((max(1, min(int(limit), 5000)), max(0, int(offset))))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM deterministic_stage_decisions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY COALESCE(node_id,''), COALESCE(node_rank,2147483647), symbol LIMIT ? OFFSET ?",
                parameters,
            ).fetchall()
        return [json.loads(str(row["payload_json"])) for row in rows]

    def record_theme_registry(
        self,
        *,
        run_id: str,
        lane_id: str,
        as_of: datetime | str,
        themes: Sequence[Mapping[str, Any]],
        nodes: Sequence[Mapping[str, Any]],
    ) -> str:
        timestamp = as_of.isoformat() if isinstance(as_of, datetime) else str(as_of)
        version_hash = content_hash({"themes": themes, "nodes": nodes})
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT OR REPLACE INTO theme_registry_versions(run_id,lane_id,version_hash,as_of,payload_json) "
                "VALUES(?,?,?,?,?)",
                (run_id, lane_id, version_hash, timestamp, canonical_json(themes)),
            )
            connection.executemany(
                "INSERT OR REPLACE INTO chain_node_versions(run_id,lane_id,node_id,version_hash,as_of,payload_json) "
                "VALUES(?,?,?,?,?,?)",
                [
                    (
                        run_id,
                        lane_id,
                        str(node.get("node_id") or f"node-{index}"),
                        version_hash,
                        timestamp,
                        canonical_json(node),
                    )
                    for index, node in enumerate(nodes)
                ],
            )
            connection.commit()
        return version_hash

    def latest_theme_registry(
        self,
        *,
        lane_id: str,
        before: datetime | str,
    ) -> dict[str, Any] | None:
        """Return the latest point-in-time theme graph before ``before``."""

        cutoff = before.isoformat() if isinstance(before, datetime) else str(before)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT run_id,version_hash,as_of,payload_json FROM theme_registry_versions "
                "WHERE lane_id=? AND as_of<? ORDER BY as_of DESC, rowid DESC LIMIT 1",
                (lane_id, cutoff),
            ).fetchone()
            if row is None:
                return None
            nodes = connection.execute(
                "SELECT payload_json FROM chain_node_versions "
                "WHERE run_id=? AND lane_id=? AND version_hash=? ORDER BY node_id",
                (str(row["run_id"]), lane_id, str(row["version_hash"])),
            ).fetchall()
        try:
            themes = json.loads(str(row["payload_json"]))
            parsed_nodes = [json.loads(str(item["payload_json"])) for item in nodes]
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(themes, list) or any(not isinstance(item, dict) for item in themes):
            return None
        return {
            "run_id": str(row["run_id"]),
            "lane_id": lane_id,
            "version_hash": str(row["version_hash"]),
            "as_of": str(row["as_of"]),
            "themes": themes,
            "nodes": parsed_nodes,
        }

    def record_taxonomy_links(
        self,
        *,
        run_id: str,
        lane_id: str,
        links: Iterable[Mapping[str, Any]],
        source_hash: str,
    ) -> int:
        rows = []
        for link in links:
            code = str(link.get("taxonomy_code") or "").strip().upper()
            node_id = str(link.get("node_id") or "").strip()
            taxonomy = str(link.get("taxonomy") or "").strip().upper()
            if not code or not node_id or taxonomy not in {"INDUSTRY", "CONCEPT"}:
                continue
            rows.append(
                (
                    run_id,
                    lane_id,
                    node_id,
                    taxonomy,
                    code,
                    _optional_text(link.get("taxonomy_name")),
                    str(link.get("match_method") or "UNKNOWN"),
                    max(0.0, min(1.0, _optional_float(link.get("confidence")) or 0.0)),
                    source_hash,
                )
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM theme_taxonomy_links WHERE run_id=? AND lane_id=?",
                (run_id, lane_id),
            )
            connection.executemany(
                "INSERT INTO theme_taxonomy_links(run_id,lane_id,node_id,taxonomy,taxonomy_code,taxonomy_name,match_method,confidence,source_hash) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                rows,
            )
            connection.commit()
        return len(rows)


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = ["FEATURE_SCHEMA", "ResearchFeatureStore", "canonical_json", "content_hash"]
