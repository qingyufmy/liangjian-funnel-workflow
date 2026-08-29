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
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
FEATURE_SCHEMA = "liangjian-research-feature-store/2.0.0"
LEGACY_GENERATION_ID = "legacy-v1"
DEFAULT_FEATURE_DOMAIN = "RESEARCH"
GENERATION_PURPOSES = frozenset(
    {
        "LIVE_FULL",
        "LIVE_INCREMENTAL",
        "RUN_SNAPSHOT",
        "HISTORICAL_REPLAY",
        "TEST_FIXTURE",
        "UNKNOWN",
    }
)
GENERATION_STATUSES = frozenset(
    {"STAGING", "VALIDATED", "SEALED", "PUBLISHED", "FAILED", "LEGACY"}
)
# PUBLISHED is retained for stores created by an older v2 build.  New
# generations always become SEALED and are read through the same strict path.
_STRICT_GENERATION_STATUSES = frozenset({"SEALED", "PUBLISHED"})
DIRTY_STATUSES = frozenset({"PENDING", "LEASED", "RETRY", "DEAD", "RESOLVED"})
DEFAULT_DIRTY_MAX_ATTEMPTS = 5
DEFAULT_DIRTY_LEASE_SECONDS = 300
DEFAULT_DIRTY_BACKOFF_SECONDS = 30


class FeatureStoreError(RuntimeError):
    """Base exception for feature-store contract violations."""


class FeatureGenerationError(FeatureStoreError):
    """Raised when a feature generation cannot be read or transitioned."""


class _ClosingConnection(sqlite3.Connection):
    """Connection context manager that also closes the SQLite handle.

    ``sqlite3.Connection.__exit__`` commits or rolls back but deliberately
    leaves the connection open.  The feature store creates short-lived
    connections, so closing at the context boundary prevents descriptor/WAL
    leakage and makes Windows file replacement and backup operations safe.
    """

    def __exit__(self, *args: Any) -> None:
        try:
            super().__exit__(*args)
        finally:
            self.close()


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
        # Changing the journal mode takes a brief schema lock.  Multiple
        # workers can construct a store during process start-up, so retry the
        # pragma rather than failing a healthy second initializer with a
        # transient ``database is locked`` error.
        for attempt in range(600):
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 599:
                    connection.close()
                    raise
                time.sleep(0.05)
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as connection:
            # A v1 store contains the same table names but no generation
            # column.  Rename those tables before creating v2 so the old rows
            # remain available for audit and cannot accidentally be selected by
            # a generation-bound read.
            legacy_tables = self._legacy_tables(connection)
            old_schema = self._meta_value(connection, "schema")
            if legacy_tables:
                self._migrate_legacy_tables(connection, legacy_tables)
            # v2 originally combined validation, publication, and activation
            # in one state transition.  Rebuild just the generation table when
            # opening such a database so SEALED plus lifecycle metadata are
            # enforced by SQLite as well as by the service layer.  The active
            # pointer and all generation-scoped rows are preserved in place.
            self._migrate_generation_lifecycle(connection)
            self._create_schema_v2(connection)
            self._ensure_legacy_generation(connection)
            if old_schema and old_schema != FEATURE_SCHEMA:
                connection.execute(
                    "INSERT INTO feature_store_meta(key, value) VALUES('schema_previous', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (old_schema,),
                )
            connection.execute(
                "INSERT INTO feature_store_meta(key, value) VALUES('schema', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (FEATURE_SCHEMA,),
            )
            connection.execute(
                "INSERT INTO feature_store_meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
            connection.execute(
                "INSERT INTO feature_store_meta(key, value) VALUES('generation_lifecycle', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("3.0.0",),
            )
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return row is not None

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
        if not ResearchFeatureStore._table_exists(connection, table):
            return set()
        return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}

    @staticmethod
    def _meta_value(connection: sqlite3.Connection, key: str) -> str | None:
        if not ResearchFeatureStore._table_exists(connection, "feature_store_meta"):
            return None
        row = connection.execute("SELECT value FROM feature_store_meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row is not None else None

    @classmethod
    def _legacy_tables(cls, connection: sqlite3.Connection) -> list[str]:
        tables = [
            "taxonomy_membership_versions",
            "theme_registry_versions",
            "chain_node_versions",
            "theme_taxonomy_links",
            "business_exposure_facts",
            "stock_fundamental_features",
            "stock_market_role_features",
            "deterministic_stage_decisions",
        ]
        return [table for table in tables if cls._table_exists(connection, table) and "generation_id" not in cls._table_columns(connection, table)]

    @staticmethod
    def _rename_legacy_table(connection: sqlite3.Connection, table: str) -> str:
        legacy = f"{table}_legacy_v1"
        if ResearchFeatureStore._table_exists(connection, legacy):
            # A prior interrupted migration may have left the audit copy.  Do
            # not overwrite it; retain the current table as a second, clearly
            # named audit copy and let the new table be authoritative.
            suffix = 2
            while ResearchFeatureStore._table_exists(connection, f"{legacy}_{suffix}"):
                suffix += 1
            legacy = f"{legacy}_{suffix}"
        connection.execute(f'ALTER TABLE "{table}" RENAME TO "{legacy}"')
        return legacy

    @classmethod
    def _migrate_legacy_tables(cls, connection: sqlite3.Connection, tables: Sequence[str]) -> None:
        renamed = {table: cls._rename_legacy_table(connection, table) for table in tables}
        cls._create_schema_v2(connection)
        cls._ensure_legacy_generation(connection)
        source_columns: dict[str, tuple[str, ...]] = {
            "taxonomy_membership_versions": (
                "taxonomy", "version_hash", "as_of", "symbol", "taxonomy_code",
                "taxonomy_name", "source_hash", "payload_json",
            ),
            "theme_registry_versions": ("run_id", "lane_id", "version_hash", "as_of", "payload_json"),
            "chain_node_versions": ("run_id", "lane_id", "node_id", "version_hash", "as_of", "payload_json"),
            "theme_taxonomy_links": (
                "run_id", "lane_id", "node_id", "taxonomy", "taxonomy_code", "taxonomy_name",
                "match_method", "confidence", "source_hash",
            ),
            "business_exposure_facts": (
                "symbol", "report_period", "business_name", "revenue_exposure_pct",
                "gross_profit_exposure_pct", "node_id", "evidence_ref", "page_number",
                "confidence", "parser_version", "content_hash", "payload_json",
            ),
            "stock_fundamental_features": (
                "symbol", "as_of", "feature_version", "source_hash", "quality_score", "available", "payload_json",
            ),
            "stock_market_role_features": (
                "run_id", "lane_id", "symbol", "theme_id", "feature_version", "role_score", "payload_json",
            ),
            "deterministic_stage_decisions": (
                "run_id", "lane_id", "stage", "symbol", "status", "score", "node_id", "theme_id",
                "node_rank", "sent_to_llm", "reason_codes_json", "source_hashes_json", "payload_json", "updated_at",
            ),
        }
        for table, legacy in renamed.items():
            columns = source_columns[table]
            target = ",".join(("generation_id", *columns))
            source = ",".join(("?", *columns))
            connection.execute(
                f'INSERT INTO "{table}" ({target}) SELECT {source} FROM "{legacy}"',
                (LEGACY_GENERATION_ID,),
            )

    @classmethod
    def _migrate_generation_lifecycle(cls, connection: sqlite3.Connection) -> None:
        """Upgrade the original v2 generation table without moving active data.

        SQLite cannot add a value to an existing CHECK constraint.  A small
        table rebuild is therefore required for stores created before the
        seal/bind/activate split.  ``legacy_alter_table`` keeps foreign-key
        declarations pointing at the original table name while the old table
        is copied and removed.  The active pointer, bindings, and all
        generation projections remain untouched and keep their identifiers.
        """

        if not cls._table_exists(connection, "feature_generations"):
            return
        columns = cls._table_columns(connection, "feature_generations")
        sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='feature_generations'"
        ).fetchone()
        create_sql = str(sql_row[0] or "").upper() if sql_row is not None else ""
        required = {
            "purpose",
            "activation_eligible",
            "sealed_at",
            "validation_manifest_json",
        }
        if (
            required.issubset(columns)
            and "'SEALED'" in create_sql
            and "'RUN_SNAPSHOT'" in create_sql
        ):
            return

        old_name = "feature_generations_legacy_v2"
        suffix = 2
        while cls._table_exists(connection, old_name):
            old_name = f"feature_generations_legacy_v2_{suffix}"
            suffix += 1
        rows = connection.execute("SELECT * FROM feature_generations").fetchall()

        # This connection is outside an explicit transaction at initialization
        # time.  Commit before changing PRAGMA foreign_keys, then restore it
        # even if a malformed legacy database causes the rebuild to fail.
        connection.commit()
        connection.execute("PRAGMA legacy_alter_table=ON")
        connection.execute("PRAGMA foreign_keys=OFF")
        try:
            connection.execute(
                f'ALTER TABLE "feature_generations" RENAME TO "{old_name}"'
            )
            cls._create_generation_table(connection)
            for row in rows:
                record = {key: row[key] for key in row.keys()}
                metadata = cls._parse_json(record.get("metadata_json"), {})
                if not isinstance(metadata, dict):
                    metadata = {}
                purpose = cls._infer_generation_purpose(record, metadata)
                status = str(record.get("status") or "UNKNOWN").upper()
                # PUBLISHED was the old name for a generation that had already
                # been validated and activated.  Preserve its identity while
                # moving it to the immutable SEALED state.
                if status == "PUBLISHED":
                    status = "SEALED"
                elif status not in {"STAGING", "VALIDATED", "SEALED", "FAILED", "LEGACY"}:
                    status = "FAILED"
                sealed_at = record.get("sealed_at")
                if not sealed_at and str(record.get("status") or "").upper() == "PUBLISHED":
                    sealed_at = record.get("published_at") or record.get("validated_at")
                manifest = cls._parse_json(record.get("validation_manifest_json"), {})
                if not isinstance(manifest, dict):
                    manifest = {}
                if not manifest and isinstance(metadata.get("validation"), Mapping):
                    manifest = dict(metadata["validation"])
                if not manifest and status == "SEALED":
                    manifest = {"migrated_from": "v2"}
                activation_eligible = bool(
                    status == "SEALED" and purpose in {"LIVE_FULL", "LIVE_INCREMENTAL"}
                )
                # A legacy active row is kept exactly as-is, but unknown or
                # replay generations are deliberately not made eligible.
                connection.execute(
                    """
                    INSERT INTO feature_generations(
                        generation_id,domain,as_of,contract_version,algorithm_version,
                        source_manifest_hash,status,created_at,validated_at,published_at,
                        failed_at,failure_reason,metadata_json,purpose,activation_eligible,
                        sealed_at,validation_manifest_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        record.get("generation_id"),
                        record.get("domain") or DEFAULT_FEATURE_DOMAIN,
                        record.get("as_of") or "1970-01-01T00:00:00+00:00",
                        record.get("contract_version") or "unknown",
                        record.get("algorithm_version") or "unknown",
                        record.get("source_manifest_hash") or "unknown",
                        status,
                        record.get("created_at") or record.get("as_of") or "1970-01-01T00:00:00+00:00",
                        record.get("validated_at"),
                        record.get("published_at"),
                        record.get("failed_at"),
                        record.get("failure_reason"),
                        record.get("metadata_json") or "{}",
                        purpose,
                        int(activation_eligible),
                        sealed_at,
                        canonical_json(manifest),
                    ),
                )
            # The old index has the same name.  Remove it so the idempotent
            # schema creator can attach a fresh index to the new table.
            connection.execute("DROP INDEX IF EXISTS idx_feature_generations_domain_status")
            connection.execute(f'DROP TABLE "{old_name}"')
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys=ON")

    @staticmethod
    def _infer_generation_purpose(
        record: Mapping[str, Any], metadata: Mapping[str, Any]
    ) -> str:
        explicit = str(record.get("purpose") or metadata.get("purpose") or "").strip().upper()
        if explicit in GENERATION_PURPOSES:
            return explicit
        mode = str(metadata.get("rebuild_mode") or "").strip().upper()
        if mode == "FULL":
            return "LIVE_FULL"
        if mode == "INCREMENTAL":
            return "LIVE_INCREMENTAL"
        # Research generations carry a snapshot id but maintenance fixtures
        # need not.  Treat those as historical because activating one would
        # be the unsafe behavior this migration is designed to eliminate.
        if metadata.get("snapshot_id") or metadata.get("historical_replay"):
            return "HISTORICAL_REPLAY"
        return "UNKNOWN"

    @staticmethod
    def _create_generation_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_generations (
                generation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                as_of TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('STAGING','VALIDATED','SEALED','PUBLISHED','FAILED','LEGACY')),
                created_at TEXT NOT NULL,
                validated_at TEXT,
                published_at TEXT,
                failed_at TEXT,
                failure_reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                purpose TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK(purpose IN ('LIVE_FULL','LIVE_INCREMENTAL','RUN_SNAPSHOT','HISTORICAL_REPLAY','TEST_FIXTURE','UNKNOWN')),
                activation_eligible INTEGER NOT NULL DEFAULT 0 CHECK(activation_eligible IN (0,1)),
                sealed_at TEXT,
                validation_manifest_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )

    @staticmethod
    def _create_schema_v2(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS feature_store_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS feature_generations (
                generation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                as_of TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('STAGING','VALIDATED','SEALED','PUBLISHED','FAILED','LEGACY')),
                created_at TEXT NOT NULL,
                validated_at TEXT,
                published_at TEXT,
                failed_at TEXT,
                failure_reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                purpose TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK(purpose IN ('LIVE_FULL','LIVE_INCREMENTAL','RUN_SNAPSHOT','HISTORICAL_REPLAY','TEST_FIXTURE','UNKNOWN')),
                activation_eligible INTEGER NOT NULL DEFAULT 0 CHECK(activation_eligible IN (0,1)),
                sealed_at TEXT,
                validation_manifest_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_feature_generations_domain_status
                ON feature_generations(domain, status, created_at);

            CREATE TABLE IF NOT EXISTS feature_generation_members (
                generation_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                partition_name TEXT NOT NULL DEFAULT '',
                content_hash TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (generation_id, entity_type, entity_id, partition_name),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_feature_generation_members_entity
                ON feature_generation_members(entity_type, entity_id, generation_id);

            CREATE TABLE IF NOT EXISTS active_feature_generations (
                domain TEXT PRIMARY KEY,
                generation_id TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                previous_generation_id TEXT,
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id),
                FOREIGN KEY (previous_generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS feature_generation_activation_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                generation_id TEXT NOT NULL,
                previous_generation_id TEXT,
                expected_current_id TEXT,
                activated_at TEXT NOT NULL,
                actor TEXT NOT NULL,
                activation_reason TEXT NOT NULL,
                generation_as_of TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                activation_hash TEXT NOT NULL,
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id),
                FOREIGN KEY (previous_generation_id) REFERENCES feature_generations(generation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_feature_generation_activation_audit_generation
                ON feature_generation_activation_audit(generation_id, activated_at);

            CREATE TRIGGER IF NOT EXISTS trg_feature_active_generation_insert_guard
            BEFORE INSERT ON active_feature_generations
            WHEN NOT EXISTS (
                SELECT 1 FROM feature_generations
                WHERE generation_id=NEW.generation_id
                  AND activation_eligible=1
                  AND status IN ('SEALED','PUBLISHED')
                  AND purpose IN ('LIVE_FULL','LIVE_INCREMENTAL')
            )
            BEGIN
                SELECT RAISE(ABORT, 'FEATURE_GENERATION_NOT_ACTIVATION_ELIGIBLE');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_feature_active_generation_update_guard
            BEFORE UPDATE ON active_feature_generations
            WHEN NOT EXISTS (
                SELECT 1 FROM feature_generations
                WHERE generation_id=NEW.generation_id
                  AND activation_eligible=1
                  AND status IN ('SEALED','PUBLISHED')
                  AND purpose IN ('LIVE_FULL','LIVE_INCREMENTAL')
            )
            BEGIN
                SELECT RAISE(ABORT, 'FEATURE_GENERATION_NOT_ACTIVATION_ELIGIBLE');
            END;

            CREATE TABLE IF NOT EXISTS run_feature_bindings (
                run_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                generation_id TEXT NOT NULL,
                contract_hash TEXT NOT NULL,
                bound_at TEXT NOT NULL,
                PRIMARY KEY (run_id, domain),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_run_feature_bindings_generation
                ON run_feature_bindings(generation_id, domain);

            CREATE TABLE IF NOT EXISTS taxonomy_membership_versions (
                generation_id TEXT NOT NULL,
                taxonomy TEXT NOT NULL,
                version_hash TEXT NOT NULL,
                as_of TEXT NOT NULL,
                symbol TEXT NOT NULL,
                taxonomy_code TEXT NOT NULL,
                taxonomy_name TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (generation_id, taxonomy, version_hash, symbol, taxonomy_code),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_taxonomy_symbol_v2
                ON taxonomy_membership_versions(generation_id, taxonomy, symbol, as_of);

            CREATE TABLE IF NOT EXISTS theme_registry_versions (
                generation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                version_hash TEXT NOT NULL,
                as_of TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (generation_id, run_id, lane_id, version_hash),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS chain_node_versions (
                generation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                version_hash TEXT NOT NULL,
                as_of TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (generation_id, run_id, lane_id, node_id, version_hash),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS theme_taxonomy_links (
                generation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                taxonomy TEXT NOT NULL,
                taxonomy_code TEXT NOT NULL,
                taxonomy_name TEXT,
                match_method TEXT NOT NULL,
                confidence REAL NOT NULL,
                source_hash TEXT NOT NULL,
                PRIMARY KEY (generation_id, run_id, lane_id, node_id, taxonomy, taxonomy_code),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS business_exposure_facts (
                generation_id TEXT NOT NULL,
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
                PRIMARY KEY (generation_id, symbol, report_period, business_name, evidence_ref, parser_version),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS stock_fundamental_features (
                generation_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                as_of TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                quality_score REAL NOT NULL,
                available INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (generation_id, symbol, as_of, feature_version, source_hash),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS stock_market_role_features (
                generation_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                theme_id TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                role_score REAL NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (generation_id, run_id, lane_id, symbol, theme_id, feature_version),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );

            CREATE TABLE IF NOT EXISTS deterministic_stage_decisions (
                generation_id TEXT NOT NULL,
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
                PRIMARY KEY (generation_id, run_id, lane_id, stage, symbol),
                FOREIGN KEY (generation_id) REFERENCES feature_generations(generation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_stage_decisions_summary_v2
                ON deterministic_stage_decisions(generation_id, run_id, lane_id, stage, status);

            CREATE TABLE IF NOT EXISTS dirty_entities (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                source_version TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                PRIMARY KEY (entity_type, entity_id, reason_code, source_version)
            );

            CREATE TABLE IF NOT EXISTS dirty_entity_dependencies (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                dependency_type TEXT NOT NULL,
                dependency_id TEXT NOT NULL,
                relation TEXT NOT NULL DEFAULT 'depends_on',
                created_at TEXT NOT NULL,
                PRIMARY KEY (
                    entity_type, entity_id, dependency_type, dependency_id, relation
                )
            );
            CREATE INDEX IF NOT EXISTS idx_dirty_dependencies_parent
                ON dirty_entity_dependencies(entity_type, entity_id);
            CREATE INDEX IF NOT EXISTS idx_dirty_dependencies_child
                ON dirty_entity_dependencies(dependency_type, dependency_id);
            """
        )
        # These columns are intentionally additive for old dirty queues.  The
        # queue lifecycle itself is consumed by the incremental-maintenance
        # worker; adding them here makes migration forward compatible without
        # rewriting or dropping unresolved rows.
        dirty_columns = {
            "status": "TEXT NOT NULL DEFAULT 'PENDING'",
            "priority": "INTEGER NOT NULL DEFAULT 0",
            "attempts": "INTEGER NOT NULL DEFAULT 0",
            "max_attempts": "INTEGER NOT NULL DEFAULT 5",
            "next_retry_at": "TEXT",
            "lease_owner": "TEXT",
            "lease_expires_at": "TEXT",
            "dependency_hash": "TEXT",
            "last_error_code": "TEXT",
            "last_error_at": "TEXT",
            "updated_at": "TEXT",
        }
        existing = ResearchFeatureStore._table_columns(connection, "dirty_entities")
        for name, definition in dirty_columns.items():
            if name not in existing:
                try:
                    connection.execute(f'ALTER TABLE dirty_entities ADD COLUMN "{name}" {definition}')
                except sqlite3.OperationalError as exc:
                    # Two process/thread initializers may both observe the
                    # pre-column schema.  SQLite serializes the ALTER; the
                    # second initializer can safely accept the resulting
                    # duplicate-column error after the first has committed.
                    if "duplicate column name" not in str(exc).lower():
                        raise
                existing.add(name)
        # Older v2 databases were created before dependency expansion was
        # persisted.  The table/index DDL above is intentionally idempotent,
        # so opening one of those stores upgrades it without touching facts or
        # any existing generation.
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_dirty_claim "
            "ON dirty_entities(status, next_retry_at, priority DESC, created_at)"
        )

    @staticmethod
    def _ensure_legacy_generation(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            INSERT INTO feature_generations(
                generation_id,domain,as_of,contract_version,algorithm_version,
                source_manifest_hash,status,created_at,metadata_json
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(generation_id) DO NOTHING
            """,
            (
                LEGACY_GENERATION_ID,
                "LEGACY",
                "1970-01-01T00:00:00+00:00",
                "legacy-v1",
                "legacy-v1",
                "legacy-v1",
                "LEGACY",
                "1970-01-01T00:00:00+00:00",
                canonical_json({"read_only": True, "reason": "migrated_from_v1"}),
            ),
        )

    @staticmethod
    def _timestamp(value: datetime | str) -> str:
        return value.isoformat() if isinstance(value, datetime) else str(value)

    @staticmethod
    def _normalise_domain(value: str | None) -> str:
        domain = str(value or DEFAULT_FEATURE_DOMAIN).strip().upper()
        if not domain:
            raise ValueError("feature domain must not be empty")
        return domain

    @staticmethod
    def _normalise_purpose(value: str | None, *, metadata: Mapping[str, Any] | None = None) -> str:
        explicit = str(value or "").strip().upper()
        if explicit in GENERATION_PURPOSES:
            return explicit
        if explicit:
            raise ValueError(f"invalid feature generation purpose: {explicit}")
        details = metadata if isinstance(metadata, Mapping) else {}
        mode = str(details.get("rebuild_mode") or "").strip().upper()
        if mode == "FULL":
            return "LIVE_FULL"
        if mode == "INCREMENTAL":
            return "LIVE_INCREMENTAL"
        # Existing callers that create a generation without lifecycle
        # metadata historically expected the compatibility publish wrapper to
        # activate it.  Keep that behavior explicit as a live full build;
        # migrated old rows are handled conservatively by
        # _infer_generation_purpose above.
        return "LIVE_FULL"

    @staticmethod
    def _parse_timestamp(value: Any, *, field: str = "timestamp") -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or "").strip()
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except (TypeError, ValueError) as exc:
                raise FeatureGenerationError(f"FEATURE_GENERATION_{field.upper()}_INVALID") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _parse_json(value: Any, fallback: Any) -> Any:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return fallback
        return parsed

    def _generation_row(self, connection: sqlite3.Connection, generation_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM feature_generations WHERE generation_id=?", (generation_id,)
        ).fetchone()

    def _assert_generation(
        self,
        connection: sqlite3.Connection,
        generation_id: str,
        *,
        strict: bool = False,
        allow_legacy: bool = False,
        for_write: bool = False,
        allow_published_write: bool = False,
    ) -> sqlite3.Row:
        generation = str(generation_id or "").strip()
        if not generation:
            raise FeatureGenerationError("FEATURE_GENERATION_MISSING")
        row = self._generation_row(connection, generation)
        if row is None:
            raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_FOUND:{generation}")
        status = str(row["status"] or "").upper()
        if status == "LEGACY" and not allow_legacy:
            raise FeatureGenerationError("FEATURE_GENERATION_LEGACY_READ_DISABLED")
        if strict and status not in _STRICT_GENERATION_STATUSES:
            raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_PUBLISHED:{generation}")
        if for_write and status in {"FAILED", "LEGACY"} and not (status == "LEGACY" and allow_legacy):
            raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_WRITABLE:{generation}")
        if for_write and status in {"SEALED", "PUBLISHED"} and not allow_published_write:
            raise FeatureGenerationError(f"FEATURE_GENERATION_IMMUTABLE:{generation}")
        return row

    def _bound_generation(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str | None,
        domain: str,
    ) -> str | None:
        if not run_id:
            return None
        row = connection.execute(
            "SELECT generation_id FROM run_feature_bindings WHERE run_id=? AND domain=?",
            (str(run_id), domain),
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _query_generation(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str | None,
        generation_id: str | None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        strict: bool = False,
    ) -> str | None:
        """Resolve a query generation without weakening old API behavior.

        Existing callers that do not know about generations may continue to
        query their run.  Once a run has a binding, however, even that legacy
        call is automatically narrowed to the bound generation.  New strict
        callers must provide a generation or a bound run and only published
        generations are accepted.
        """

        domain_name = self._normalise_domain(domain)
        explicit = str(generation_id or "").strip() or None
        bound = self._bound_generation(connection, run_id=run_id, domain=domain_name)
        selected = explicit or bound
        if selected:
            self._assert_generation(
                connection,
                selected,
                strict=strict,
                allow_legacy=not strict,
            )
            if explicit and bound and explicit != bound:
                raise FeatureGenerationError("FEATURE_GENERATION_RUN_BINDING_MISMATCH")
            return selected
        if strict:
            raise FeatureGenerationError("FEATURE_GENERATION_NOT_BOUND")
        # Unbound historical reads retain the v1 API's all-generations view.
        return None

    def _write_generation(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str | None,
        generation_id: str | None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> str:
        explicit = str(generation_id or "").strip() or None
        bound = self._bound_generation(
            connection,
            run_id=run_id,
            domain=self._normalise_domain(domain),
        )
        selected = explicit or bound or LEGACY_GENERATION_ID
        if explicit and bound and explicit != bound:
            raise FeatureGenerationError("FEATURE_GENERATION_RUN_BINDING_MISMATCH")
        # Legacy writes are retained for old callers but are never accepted by
        # the strict read path.  New generation writes may target STAGING or
        # VALIDATED; FAILED/LEGACY are rejected by this guard.
        self._assert_generation(
            connection,
            selected,
            strict=False,
            allow_legacy=(selected == LEGACY_GENERATION_ID),
            for_write=True,
            # Run-scoped decision rows may be appended to the immutable source
            # generation selected when the run was created.  Unbound source
            # materialisation (taxonomy, exposure, members) must never mutate
            # a generation after publication.
            allow_published_write=bool(run_id and bound),
        )
        return selected

    def create_feature_generation(
        self,
        *,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        as_of: datetime | str,
        contract_version: str,
        algorithm_version: str,
        source_manifest_hash: str,
        generation_id: str | None = None,
        created_at: datetime | str | None = None,
        metadata: Mapping[str, Any] | None = None,
        purpose: str | None = None,
        activation_eligible: bool | None = None,
    ) -> str:
        """Create an isolated STAGING generation and return its identifier."""

        domain_name = self._normalise_domain(domain)
        metadata_dict = dict(metadata or {})
        purpose_name = self._normalise_purpose(purpose, metadata=metadata_dict)
        eligible = (
            bool(activation_eligible)
            if activation_eligible is not None
            else purpose_name in {"LIVE_FULL", "LIVE_INCREMENTAL"}
        )
        if purpose_name not in {"LIVE_FULL", "LIVE_INCREMENTAL"}:
            eligible = False
        generation = str(generation_id or "").strip()
        if not generation:
            seed = {
                "domain": domain_name,
                "as_of": self._timestamp(as_of),
                "contract_version": str(contract_version),
                "algorithm_version": str(algorithm_version),
                "source_manifest_hash": str(source_manifest_hash),
                "created_at": self._timestamp(created_at or as_of),
            }
            generation = f"feature-{content_hash(seed)[:24]}"
        created = self._timestamp(created_at or as_of)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT INTO feature_generations(
                        generation_id,domain,as_of,contract_version,algorithm_version,
                        source_manifest_hash,status,created_at,metadata_json
                        ,purpose,activation_eligible
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        generation,
                        domain_name,
                        self._timestamp(as_of),
                        str(contract_version),
                        str(algorithm_version),
                        str(source_manifest_hash),
                        "STAGING",
                        created,
                        canonical_json(metadata_dict),
                        purpose_name,
                        int(eligible),
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise FeatureGenerationError(f"FEATURE_GENERATION_ALREADY_EXISTS:{generation}") from exc
            except Exception:
                connection.rollback()
                raise
        return generation

    # Short aliases make the lifecycle API convenient for maintenance scripts
    # while keeping the descriptive names used by the public contract.
    begin_feature_generation = create_feature_generation
    begin_generation = create_feature_generation

    def get_feature_generation(self, generation_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = self._generation_row(connection, str(generation_id))
        if row is None:
            return None
        return _generation_dict(row)

    def list_feature_generations(
        self,
        *,
        domain: str | None = None,
        statuses: Iterable[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if domain:
            clauses.append("domain=?")
            params.append(self._normalise_domain(domain))
        status_values = tuple(str(item).upper() for item in (statuses or ()) if str(item).strip())
        if status_values:
            clauses.append("status IN (" + ",".join("?" for _ in status_values) + ")")
            params.extend(status_values)
        params.append(max(1, min(int(limit), 1000)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM feature_generations" + where + " ORDER BY created_at DESC, generation_id DESC LIMIT ?",
                params,
            ).fetchall()
        return [_generation_dict(row) for row in rows]

    def validate_feature_generation(
        self,
        generation_id: str,
        *,
        validated_at: datetime | str | None = None,
        validation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark a staging generation VALIDATED after external invariants pass."""

        timestamp = self._timestamp(validated_at or datetime.utcnow())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._assert_generation(connection, generation_id, allow_legacy=False)
                status = str(row["status"] or "").upper()
                if status == "FAILED":
                    raise FeatureGenerationError("FEATURE_GENERATION_FAILED_NOT_VALIDATABLE")
                if status in {"SEALED", "PUBLISHED"}:
                    connection.commit()
                    return _generation_dict(row)
                if status not in {"STAGING", "VALIDATED"}:
                    raise FeatureGenerationError(f"FEATURE_GENERATION_INVALID_STATUS:{status}")
                metadata = self._parse_json(row["metadata_json"], {})
                if not isinstance(metadata, dict):
                    metadata = {}
                validation_manifest = None
                if validation:
                    validation_manifest = dict(validation)
                    metadata["validation"] = validation_manifest
                connection.execute(
                    """
                    UPDATE feature_generations
                    SET status='VALIDATED',
                        validated_at=?,
                        metadata_json=?,
                        validation_manifest_json=COALESCE(?, validation_manifest_json)
                    WHERE generation_id=?
                    """,
                    (
                        timestamp,
                        canonical_json(metadata),
                        canonical_json(validation_manifest) if validation_manifest is not None else None,
                        str(generation_id),
                    ),
                )
                connection.commit()
                updated = self._generation_row(connection, str(generation_id))
            except Exception:
                connection.rollback()
                raise
        if updated is None:  # pragma: no cover - protected by the update above
            raise FeatureGenerationError("FEATURE_GENERATION_NOT_FOUND")
        return _generation_dict(updated)

    validate_generation = validate_feature_generation

    def fail_feature_generation(
        self,
        generation_id: str,
        *,
        reason: str,
        failed_at: datetime | str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fail an unpublished generation without changing the active pointer."""

        timestamp = self._timestamp(failed_at or datetime.utcnow())
        reason_text = str(reason or "FEATURE_GENERATION_VALIDATION_FAILED").strip()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._assert_generation(connection, generation_id, allow_legacy=False)
                status = str(row["status"] or "").upper()
                if status in {"SEALED", "PUBLISHED"}:
                    raise FeatureGenerationError("FEATURE_GENERATION_PUBLISHED_NOT_FAILABLE")
                metadata = self._parse_json(row["metadata_json"], {})
                if not isinstance(metadata, dict):
                    metadata = {}
                if diagnostics:
                    metadata["failure_diagnostics"] = dict(diagnostics)
                connection.execute(
                    "UPDATE feature_generations SET status='FAILED', failed_at=?, failure_reason=?, metadata_json=? WHERE generation_id=?",
                    (timestamp, reason_text, canonical_json(metadata), str(generation_id)),
                )
                connection.commit()
                updated = self._generation_row(connection, str(generation_id))
            except Exception:
                connection.rollback()
                raise
        if updated is None:  # pragma: no cover
            raise FeatureGenerationError("FEATURE_GENERATION_NOT_FOUND")
        return _generation_dict(updated)

    fail_generation = fail_feature_generation

    def seal_generation(
        self,
        generation_id: str,
        validation_manifest: Mapping[str, Any] | None = None,
        *,
        sealed_at: datetime | str | None = None,
        purpose: str | None = None,
        activation_eligible: bool | None = None,
    ) -> dict[str, Any]:
        """Seal a validated generation without changing the active pointer.

        Sealing is the durable boundary for a generation.  A sealed
        historical replay is safe to bind to its run, but remains ineligible
        for activation.  Only ``activate_generation`` can mutate the active
        pointer.
        """

        timestamp = self._timestamp(sealed_at or datetime.now(timezone.utc))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._assert_generation(connection, generation_id, allow_legacy=False)
                generation = str(row["generation_id"])
                status = str(row["status"] or "").upper()
                if status == "FAILED":
                    raise FeatureGenerationError("FEATURE_GENERATION_FAILED_NOT_SEALABLE")
                if status == "PUBLISHED":
                    # A pre-lifecycle database can still be opened while a
                    # process is holding the old row.  Normalize it in place;
                    # migration handles the normal startup path.
                    status = "SEALED"
                elif status not in {"VALIDATED", "SEALED"}:
                    raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_VALIDATED:{generation}")
                metadata = self._parse_json(row["metadata_json"], {})
                if not isinstance(metadata, dict):
                    metadata = {}
                existing_purpose = str(row["purpose"] or "UNKNOWN").upper()
                existing_eligible = bool(row["activation_eligible"])
                if status == "SEALED":
                    if purpose is not None and str(purpose).strip().upper() != existing_purpose:
                        raise FeatureGenerationError("FEATURE_GENERATION_SEALED_IMMUTABLE")
                    if activation_eligible is not None and bool(activation_eligible) != existing_eligible:
                        raise FeatureGenerationError("FEATURE_GENERATION_SEALED_IMMUTABLE")
                purpose_name = self._normalise_purpose(
                    purpose or row["purpose"], metadata=metadata
                )
                if purpose_name == "HISTORICAL_REPLAY":
                    eligible = False
                elif activation_eligible is None:
                    eligible = bool(row["activation_eligible"]) and purpose_name in {
                        "LIVE_FULL",
                        "LIVE_INCREMENTAL",
                    }
                else:
                    eligible = bool(activation_eligible)
                if purpose_name not in {"LIVE_FULL", "LIVE_INCREMENTAL"}:
                    eligible = False
                manifest = validation_manifest
                if manifest is None:
                    existing = self._parse_json(row["validation_manifest_json"], {})
                    manifest = existing if isinstance(existing, Mapping) else {}
                    if not manifest and isinstance(metadata.get("validation"), Mapping):
                        manifest = metadata["validation"]
                manifest_dict = dict(manifest or {})
                metadata["purpose"] = purpose_name
                metadata["activation_eligible"] = bool(eligible)
                metadata["sealed_at"] = timestamp
                connection.execute(
                    """
                    UPDATE feature_generations
                    SET status='SEALED',
                        purpose=?, activation_eligible=?, sealed_at=?,
                        published_at=COALESCE(published_at, ?),
                        metadata_json=?, validation_manifest_json=?
                    WHERE generation_id=?
                    """,
                    (
                        purpose_name,
                        int(eligible),
                        timestamp,
                        timestamp,
                        canonical_json(metadata),
                        canonical_json(manifest_dict),
                        generation,
                    ),
                )
                connection.commit()
                updated = self._generation_row(connection, generation)
            except Exception:
                connection.rollback()
                raise
        if updated is None:  # pragma: no cover
            raise FeatureGenerationError("FEATURE_GENERATION_NOT_FOUND")
        return _generation_dict(updated)

    seal_feature_generation = seal_generation

    def activate_generation(
        self,
        generation_id: str,
        expected_current_id: str | None,
        activation_reason: str,
        *,
        domain: str | None = None,
        actor: str = "system",
        activated_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        """CAS-activate one eligible sealed live generation.

        The compare-and-swap check and monotonic ``as_of`` check happen in a
        single IMMEDIATE transaction.  SQLite triggers provide a second line
        of defense against direct insertion of replay/unknown generations
        into ``active_feature_generations``.
        """

        reason = str(activation_reason or "").strip()
        if not reason:
            raise ValueError("activation_reason must not be empty")
        actor_name = str(actor or "system").strip() or "system"
        timestamp = self._timestamp(activated_at or datetime.now(timezone.utc))
        expected = str(expected_current_id or "").strip() or None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._assert_generation(
                    connection, generation_id, strict=True, allow_legacy=False
                )
                generation = str(row["generation_id"])
                generation_domain = self._normalise_domain(domain or row["domain"])
                if str(row["domain"]).upper() != generation_domain:
                    raise FeatureGenerationError("FEATURE_GENERATION_DOMAIN_MISMATCH")
                purpose = str(row["purpose"] or "UNKNOWN").upper()
                if purpose not in {"LIVE_FULL", "LIVE_INCREMENTAL"}:
                    raise FeatureGenerationError("FEATURE_GENERATION_PURPOSE_NOT_ACTIVATABLE")
                if not bool(row["activation_eligible"]):
                    raise FeatureGenerationError("FEATURE_GENERATION_NOT_ACTIVATION_ELIGIBLE")
                if str(row["status"] or "").upper() not in {"SEALED", "PUBLISHED"}:
                    raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_SEALED:{generation}")
                active = connection.execute(
                    "SELECT generation_id FROM active_feature_generations WHERE domain=?",
                    (generation_domain,),
                ).fetchone()
                actual = str(active[0]) if active is not None else None
                if actual != expected:
                    raise FeatureGenerationError(
                        f"FEATURE_GENERATION_ACTIVE_CAS_MISMATCH:{expected or 'NONE'}:{actual or 'NONE'}"
                    )
                if actual is not None:
                    active_generation = self._generation_row(connection, actual)
                    if active_generation is None:
                        raise FeatureGenerationError("FEATURE_GENERATION_ACTIVE_NOT_FOUND")
                    if self._parse_timestamp(row["as_of"], field="as_of") < self._parse_timestamp(
                        active_generation["as_of"], field="as_of"
                    ):
                        raise FeatureGenerationError("FEATURE_GENERATION_AS_OF_REGRESSION")
                activation_hash = content_hash(
                    {
                        "domain": generation_domain,
                        "generation_id": generation,
                        "previous_generation_id": actual,
                        "expected_current_id": expected,
                        "activated_at": timestamp,
                        "actor": actor_name,
                        "activation_reason": reason,
                        "as_of": row["as_of"],
                        "source_manifest_hash": row["source_manifest_hash"],
                    }
                )
                # The trigger on this table re-checks eligibility in SQLite.
                connection.execute(
                    """
                    INSERT INTO active_feature_generations(domain,generation_id,activated_at,previous_generation_id)
                    VALUES(?,?,?,?)
                    ON CONFLICT(domain) DO UPDATE SET
                        generation_id=excluded.generation_id,
                        activated_at=excluded.activated_at,
                        previous_generation_id=excluded.previous_generation_id
                    """,
                    (generation_domain, generation, timestamp, actual),
                )
                connection.execute(
                    """
                    INSERT INTO feature_generation_activation_audit(
                        domain,generation_id,previous_generation_id,expected_current_id,
                        activated_at,actor,activation_reason,generation_as_of,
                        source_manifest_hash,activation_hash
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        generation_domain,
                        generation,
                        actual,
                        expected,
                        timestamp,
                        actor_name,
                        reason,
                        str(row["as_of"]),
                        str(row["source_manifest_hash"]),
                        activation_hash,
                    ),
                )
                connection.commit()
                updated = self._generation_row(connection, generation)
            except Exception:
                connection.rollback()
                raise
        if updated is None:  # pragma: no cover
            raise FeatureGenerationError("FEATURE_GENERATION_NOT_FOUND")
        result = _generation_dict(updated)
        result["activation_hash"] = activation_hash
        return result

    activate_feature_generation = activate_generation

    def publish_feature_generation(
        self,
        generation_id: str,
        *,
        domain: str | None = None,
        activated_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Deprecated compatibility wrapper for seal then CAS activation.

        New callers must use ``seal_generation`` and ``activate_generation``
        separately.  Keeping this wrapper avoids breaking older maintenance
        integrations while ensuring even legacy calls pass the new purpose,
        eligibility, CAS, and monotonicity checks.
        """

        row = self.get_feature_generation(generation_id)
        if row is None:
            raise FeatureGenerationError(f"FEATURE_GENERATION_NOT_FOUND:{generation_id}")
        if str(row.get("status") or "").upper() == "FAILED":
            # Preserve the legacy exception contract for callers still using
            # publish_feature_generation while the new seal API exposes its
            # more precise FAILED_NOT_SEALABLE code.
            raise FeatureGenerationError("FEATURE_GENERATION_FAILED_NOT_PUBLISHABLE")
        existing_manifest = row.get("validation_manifest")
        sealed = self.seal_generation(
            generation_id,
            validation_manifest=existing_manifest
            if isinstance(existing_manifest, Mapping) and existing_manifest
            else None,
            purpose=str(row.get("purpose") or "LIVE_FULL"),
            sealed_at=activated_at,
        )
        generation_domain = domain or str(sealed.get("domain") or "RESEARCH")
        # This legacy helper owns discovery of the current generation, so it
        # must also absorb a race between that read and the strict CAS write.
        # Keep ``activate_generation`` fail-closed for explicit callers; only
        # the compatibility wrapper refreshes its internally-derived expected
        # value and retries.
        last_cas_error: FeatureGenerationError | None = None
        for _attempt in range(16):
            active = self.get_active_feature_generation(generation_domain)
            try:
                return self.activate_generation(
                    generation_id,
                    expected_current_id=(str(active["generation_id"]) if active else None),
                    activation_reason="COMPAT_PUBLISH_FEATURE_GENERATION",
                    domain=domain,
                    actor="compat.publish_feature_generation",
                    activated_at=activated_at,
                )
            except FeatureGenerationError as exc:
                if not str(exc).startswith("FEATURE_GENERATION_ACTIVE_CAS_MISMATCH:"):
                    raise
                last_cas_error = exc
        raise FeatureGenerationError(
            "FEATURE_GENERATION_COMPAT_PUBLISH_RETRY_EXHAUSTED"
        ) from last_cas_error

    publish_generation = publish_feature_generation

    def get_active_feature_generation(self, domain: str = DEFAULT_FEATURE_DOMAIN) -> dict[str, Any] | None:
        domain_name = self._normalise_domain(domain)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT g.*, a.activated_at, a.previous_generation_id
                FROM active_feature_generations a
                JOIN feature_generations g ON g.generation_id=a.generation_id
                WHERE a.domain=?
                """,
                (domain_name,),
            ).fetchone()
        if row is None:
            return None
        return _generation_dict(row)

    active_feature_generation = get_active_feature_generation

    def list_generation_activation_audit(
        self,
        *,
        domain: str | None = None,
        generation_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return append-only active-pointer transition evidence."""

        clauses: list[str] = []
        params: list[Any] = []
        if domain:
            clauses.append("domain=?")
            params.append(self._normalise_domain(domain))
        if generation_id:
            clauses.append("generation_id=?")
            params.append(str(generation_id))
        params.append(max(1, min(int(limit), 1000)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM feature_generation_activation_audit"
                + where
                + " ORDER BY audit_id DESC LIMIT ?",
                params,
            ).fetchall()
        return [{key: row[key] for key in row.keys()} for row in rows]

    generation_activation_audit = list_generation_activation_audit

    def bind_run_feature_generation(
        self,
        *,
        run_id: str,
        generation_id: str,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        contract_hash: str = "",
        bound_at: datetime | str | None = None,
        allow_unpublished: bool = False,
    ) -> dict[str, Any]:
        """Bind a run once; a different generation cannot replace its binding."""

        run = str(run_id or "").strip()
        if not run:
            raise ValueError("run_id must not be empty")
        domain_name = self._normalise_domain(domain)
        timestamp = self._timestamp(bound_at or datetime.utcnow())
        generation = str(generation_id or "").strip()
        if not generation:
            raise FeatureGenerationError("FEATURE_GENERATION_MISSING")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                generation_row = self._assert_generation(
                    connection,
                    generation,
                    strict=not allow_unpublished,
                    allow_legacy=False,
                )
                if str(generation_row["domain"]).upper() != domain_name:
                    raise FeatureGenerationError("FEATURE_GENERATION_DOMAIN_MISMATCH")
                existing = connection.execute(
                    "SELECT * FROM run_feature_bindings WHERE run_id=? AND domain=?",
                    (run, domain_name),
                ).fetchone()
                if existing is not None:
                    if str(existing["generation_id"]) != generation:
                        raise FeatureGenerationError("FEATURE_GENERATION_RUN_ALREADY_BOUND")
                    if str(existing["contract_hash"] or "") != str(contract_hash or ""):
                        raise FeatureGenerationError("FEATURE_GENERATION_RUN_CONTRACT_MISMATCH")
                    connection.commit()
                    return _binding_dict(existing)
                connection.execute(
                    "INSERT INTO run_feature_bindings(run_id,domain,generation_id,contract_hash,bound_at) VALUES(?,?,?,?,?)",
                    (run, domain_name, generation, str(contract_hash or ""), timestamp),
                )
                connection.commit()
                row = connection.execute(
                    "SELECT * FROM run_feature_bindings WHERE run_id=? AND domain=?",
                    (run, domain_name),
                ).fetchone()
            except Exception:
                connection.rollback()
                raise
        if row is None:  # pragma: no cover
            raise FeatureGenerationError("FEATURE_GENERATION_BINDING_NOT_WRITTEN")
        return _binding_dict(row)

    bind_run_to_generation = bind_run_feature_generation

    def bind_run_generation(
        self,
        run_id: str,
        generation_id: str,
        contract_hash: str = "",
        *,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        bound_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Bind a run to a sealed generation using the lifecycle API name."""

        return self.bind_run_feature_generation(
            run_id=run_id,
            generation_id=generation_id,
            domain=domain,
            contract_hash=contract_hash,
            bound_at=bound_at,
        )

    def bind_run_to_active_generation(
        self,
        *,
        run_id: str,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        contract_hash: str = "",
        bound_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        active = self.get_active_feature_generation(domain)
        if active is None:
            raise FeatureGenerationError("FEATURE_GENERATION_ACTIVE_NOT_FOUND")
        return self.bind_run_feature_generation(
            run_id=run_id,
            generation_id=str(active["generation_id"]),
            domain=domain,
            contract_hash=contract_hash,
            bound_at=bound_at,
        )

    def get_run_feature_binding(
        self,
        *,
        run_id: str,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        expected_contract_hash: str | None = None,
        strict: bool = False,
    ) -> dict[str, Any] | None:
        domain_name = self._normalise_domain(domain)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM run_feature_bindings WHERE run_id=? AND domain=?",
                (str(run_id), domain_name),
            ).fetchone()
            if row is not None:
                self._assert_generation(connection, str(row["generation_id"]), strict=strict)
                if expected_contract_hash is not None and str(row["contract_hash"] or "") != str(expected_contract_hash):
                    raise FeatureGenerationError("FEATURE_GENERATION_RUN_CONTRACT_MISMATCH")
        return _binding_dict(row) if row is not None else None

    run_feature_binding = get_run_feature_binding

    def record_feature_generation_members(
        self,
        *,
        generation_id: str,
        members: Sequence[Mapping[str, Any]],
    ) -> int:
        """Write generation manifest rows without touching another generation."""

        rows: list[tuple[Any, ...]] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._assert_generation(connection, generation_id, for_write=True)
                for item in members:
                    entity_type = str(item.get("entity_type") or "").strip().upper()
                    entity_id = str(item.get("entity_id") or "").strip()
                    if not entity_type or not entity_id:
                        continue
                    payload = item.get("payload") if isinstance(item.get("payload"), Mapping) else dict(item)
                    rows.append(
                        (
                            str(generation_id),
                            entity_type,
                            entity_id,
                            str(item.get("partition") or item.get("partition_name") or ""),
                            str(item.get("content_hash") or content_hash(payload)),
                            max(0, _optional_int(item.get("row_count")) or 0),
                            canonical_json(payload),
                        )
                    )
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO feature_generation_members(
                        generation_id,entity_type,entity_id,partition_name,content_hash,row_count,payload_json
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    record_generation_members = record_feature_generation_members

    def feature_generation_members(
        self,
        generation_id: str,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        with self._connect() as connection:
            self._assert_generation(
                connection,
                generation_id,
                strict=strict,
                allow_legacy=not strict,
            )
            clauses = ["generation_id=?"]
            params: list[Any] = [str(generation_id)]
            if entity_type:
                clauses.append("entity_type=?")
                params.append(str(entity_type).upper())
            if entity_id:
                clauses.append("entity_id=?")
                params.append(str(entity_id))
            rows = connection.execute(
                "SELECT * FROM feature_generation_members WHERE " + " AND ".join(clauses)
                + " ORDER BY entity_type,entity_id,partition_name",
                params,
            ).fetchall()
        return [_member_dict(row) for row in rows]

    generation_members = feature_generation_members

    def replace_stage_decisions(
        self,
        *,
        run_id: str,
        lane_id: str,
        stage: str,
        decisions: Sequence[Mapping[str, Any]],
        updated_at: datetime | str,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> int:
        """Atomically replace one stage projection for a lane and generation.

        Omitting ``generation_id`` preserves the v1 call contract.  A bound
        run is still narrowed to its binding; an entirely unbound legacy call
        writes to the read-disabled ``legacy-v1`` generation.
        """

        timestamp = self._timestamp(updated_at)
        rows = []
        seen: set[str] = set()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
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
                            selected_generation,
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
                connection.execute(
                    "DELETE FROM deterministic_stage_decisions WHERE generation_id=? AND run_id=? AND lane_id=? AND stage=?",
                    (selected_generation, run_id, lane_id, stage),
                )
                connection.executemany(
                    """
                    INSERT INTO deterministic_stage_decisions(
                        generation_id,run_id,lane_id,stage,symbol,status,score,node_id,theme_id,node_rank,
                        sent_to_llm,reason_codes_json,source_hashes_json,payload_json,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def replace_taxonomy_memberships(
        self,
        *,
        taxonomy: str,
        snapshot: Mapping[str, Any],
        as_of: datetime | str,
        generation_id: str | None = None,
        run_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> int:
        """Persist one immutable taxonomy projection version."""

        taxonomy_name = str(taxonomy).strip().upper()
        if taxonomy_name not in {"INDUSTRY", "CONCEPT"}:
            raise ValueError("taxonomy must be INDUSTRY or CONCEPT")
        records = snapshot.get("records")
        records = records if isinstance(records, list) else []
        timestamp = self._timestamp(as_of)
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
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.execute(
                    "DELETE FROM taxonomy_membership_versions WHERE generation_id=? AND taxonomy=? AND version_hash=?",
                    (selected_generation, taxonomy_name, version_hash),
                )
                connection.executemany(
                    "INSERT INTO taxonomy_membership_versions(generation_id,taxonomy,version_hash,as_of,symbol,taxonomy_code,taxonomy_name,source_hash,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    [(selected_generation, *row) for row in rows],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def replace_business_exposure_facts(
        self,
        facts: Sequence[Mapping[str, Any]],
        *,
        generation_id: str | None = None,
        run_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> int:
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
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.executemany(
                    "INSERT OR REPLACE INTO business_exposure_facts(generation_id,symbol,report_period,business_name,revenue_exposure_pct,gross_profit_exposure_pct,node_id,evidence_ref,page_number,confidence,parser_version,content_hash,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(selected_generation, *row) for row in rows],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def record_fundamental_features(
        self,
        *,
        as_of: datetime | str,
        decisions: Sequence[Mapping[str, Any]],
        generation_id: str | None = None,
        run_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> int:
        timestamp = self._timestamp(as_of)
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
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.executemany(
                    "INSERT OR REPLACE INTO stock_fundamental_features(generation_id,symbol,as_of,feature_version,source_hash,quality_score,available,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    [(selected_generation, *row) for row in rows],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def record_market_role_features(
        self,
        *,
        run_id: str,
        lane_id: str,
        decisions: Sequence[Mapping[str, Any]],
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
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
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.executemany(
                    "INSERT OR REPLACE INTO stock_market_role_features(generation_id,run_id,lane_id,symbol,theme_id,feature_version,role_score,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    [(selected_generation, *row) for row in rows],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def mark_dirty(
        self,
        *,
        entity_type: str,
        entity_id: str,
        reason_code: str,
        source_version: str,
        created_at: datetime | str,
        priority: int = 0,
        max_attempts: int = DEFAULT_DIRTY_MAX_ATTEMPTS,
        dependency_hash: str | None = None,
    ) -> None:
        """Idempotently enqueue one rebuildable entity.

        The original v1 contract returned ``None`` and accepted only the first
        five keyword arguments; the additional queue controls are optional so
        existing synchronizers continue to work unchanged.  Re-marking a
        leased or retrying item does not reset its attempt counter.  A resolved
        or dead item is explicitly re-opened, which lets a new source version
        be retried without deleting audit history.
        """

        normalized_type = str(entity_type or "").strip().upper()
        normalized_id = str(entity_id or "").strip()
        normalized_reason = str(reason_code or "").strip().upper()
        normalized_source = str(source_version or "").strip()
        if not normalized_type or not normalized_id or not normalized_reason or not normalized_source:
            raise ValueError("dirty entity fields must not be empty")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        timestamp = _queue_timestamp(created_at)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO dirty_entities(
                    entity_type,entity_id,reason_code,source_version,created_at,
                    resolved_at,status,priority,attempts,max_attempts,next_retry_at,
                    lease_owner,lease_expires_at,dependency_hash,last_error_code,
                    last_error_at,updated_at
                ) VALUES(?,?,?,?,?,NULL,'PENDING',?,0,?,NULL,NULL,NULL,?,NULL,NULL,?)
                ON CONFLICT(entity_type,entity_id,reason_code,source_version) DO UPDATE SET
                    created_at=excluded.created_at,
                    resolved_at=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.resolved_at
                    END,
                    status=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN 'PENDING'
                        ELSE dirty_entities.status
                    END,
                    priority=MAX(dirty_entities.priority, excluded.priority),
                    max_attempts=excluded.max_attempts,
                    attempts=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN 0
                        ELSE dirty_entities.attempts
                    END,
                    next_retry_at=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.next_retry_at
                    END,
                    lease_owner=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.lease_owner
                    END,
                    lease_expires_at=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.lease_expires_at
                    END,
                    dependency_hash=COALESCE(excluded.dependency_hash, dirty_entities.dependency_hash),
                    last_error_code=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.last_error_code
                    END,
                    last_error_at=CASE
                        WHEN dirty_entities.status IN ('RESOLVED','DEAD') THEN NULL
                        ELSE dirty_entities.last_error_at
                    END,
                    updated_at=excluded.updated_at
                """,
                (
                    normalized_type,
                    normalized_id,
                    normalized_reason,
                    normalized_source,
                    timestamp,
                    priority,
                    max_attempts,
                    None if dependency_hash is None else str(dependency_hash),
                    timestamp,
                ),
            )

    def resolve_dirty(self, *, entity_type: str, entity_id: str, resolved_at: datetime | str) -> int:
        """Resolve every unresolved reason/version for an entity.

        This preserves the historical API while also closing leases and
        clearing retry metadata.  New workers should prefer
        :meth:`complete_dirty`, which resolves one exact queue item and can
        enforce the lease owner.
        """

        timestamp = _queue_timestamp(resolved_at)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE dirty_entities
                SET resolved_at=?, status='RESOLVED', lease_owner=NULL,
                    lease_expires_at=NULL, next_retry_at=NULL, updated_at=?
                WHERE entity_type=? AND entity_id=?
                    AND resolved_at IS NULL AND status <> 'RESOLVED'
                """,
                (timestamp, timestamp, str(entity_type or "").strip().upper(), str(entity_id or "").strip()),
            )
        return int(cursor.rowcount)

    def claim_dirty(
        self,
        *,
        worker_id: str,
        limit: int = 100,
        now: datetime | str | None = None,
        lease_seconds: int = DEFAULT_DIRTY_LEASE_SECONDS,
        entity_types: Iterable[str] | None = None,
        entity_keys: Iterable[tuple[str, str, str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically lease a bounded set of ready dirty items.

        Expired leases are made available in the same write transaction.  The
        method is safe for multiple processes sharing the WAL database: the
        ``BEGIN IMMEDIATE`` lock is held only while selecting and marking rows,
        never while a feature builder runs.
        """

        owner = str(worker_id or "").strip()
        if not owner:
            raise ValueError("worker_id must not be empty")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        now_dt, now_text = _queue_now(now)
        expires_text = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        type_values = (entity_types,) if isinstance(entity_types, str) else (entity_types or ())
        normalized_types = tuple(
            sorted({str(item or "").strip().upper() for item in type_values if str(item or "").strip()})
        )
        normalized_keys = tuple(
            (
                str(item[0] or "").strip().upper(),
                str(item[1] or "").strip(),
                str(item[2] or "").strip().upper(),
                str(item[3] or "").strip(),
            )
            for item in (entity_keys or ())
        )
        if entity_keys is not None and not normalized_keys:
            return []

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                # A process may have died after incrementing attempts.  Put
                # those leases back into the queue, and permanently dead-letter
                # rows that have exhausted their configured budget.
                connection.execute(
                    """
                    UPDATE dirty_entities
                    SET status=CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END,
                        lease_owner=NULL, lease_expires_at=NULL,
                        next_retry_at=CASE WHEN attempts >= max_attempts THEN NULL ELSE ? END,
                        updated_at=?
                    WHERE status='LEASED' AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?
                    """,
                    (now_text, now_text, now_text),
                )
                connection.execute(
                    """
                    UPDATE dirty_entities
                    SET status='DEAD', next_retry_at=NULL, updated_at=?
                    WHERE resolved_at IS NULL AND status IN ('PENDING','RETRY')
                        AND attempts >= max_attempts
                    """,
                    (now_text,),
                )
                clauses = [
                    "resolved_at IS NULL",
                    "status IN ('PENDING','RETRY')",
                    "(status='PENDING' OR next_retry_at IS NULL OR next_retry_at <= ?)",
                ]
                params: list[Any] = [now_text]
                if normalized_types:
                    clauses.append("entity_type IN (" + ",".join("?" for _ in normalized_types) + ")")
                    params.extend(normalized_types)
                if normalized_keys:
                    key_clauses: list[str] = []
                    for key in normalized_keys:
                        key_clauses.append(
                            "(entity_type=? AND entity_id=? AND reason_code=? AND source_version=?)"
                        )
                        params.extend(key)
                    clauses.append("(" + " OR ".join(key_clauses) + ")")
                params.append(max(1, min(limit, 10000)))
                rows = connection.execute(
                    "SELECT * FROM dirty_entities WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY priority DESC, created_at ASC, entity_type ASC, entity_id ASC LIMIT ?",
                    params,
                ).fetchall()
                for row in rows:
                    connection.execute(
                        """
                        UPDATE dirty_entities
                        SET status='LEASED', attempts=attempts+1,
                            lease_owner=?, lease_expires_at=?, updated_at=?
                        WHERE entity_type=? AND entity_id=? AND reason_code=?
                            AND source_version=?
                        """,
                        (
                            owner,
                            expires_text,
                            now_text,
                            row["entity_type"],
                            row["entity_id"],
                            row["reason_code"],
                            row["source_version"],
                        ),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

            leased_rows = []
            for row in rows:
                updated = connection.execute(
                    """
                    SELECT * FROM dirty_entities
                    WHERE entity_type=? AND entity_id=? AND reason_code=? AND source_version=?
                    """,
                    (row["entity_type"], row["entity_id"], row["reason_code"], row["source_version"]),
                ).fetchone()
                if updated is not None:
                    leased_rows.append(_dirty_dict(updated))
        return leased_rows

    def claim_dirty_items(
        self,
        *,
        items: Iterable[Mapping[str, Any]],
        worker_id: str,
        now: datetime | str | None = None,
        lease_seconds: int = DEFAULT_DIRTY_LEASE_SECONDS,
    ) -> list[dict[str, Any]]:
        """Lease exact queue keys, used after dependency expansion."""

        keys: list[tuple[str, str, str, str]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            values = (
                str(item.get("entity_type") or "").strip().upper(),
                str(item.get("entity_id") or "").strip(),
                str(item.get("reason_code") or "").strip().upper(),
                str(item.get("source_version") or "").strip(),
            )
            if all(values):
                keys.append(values)
        if not keys:
            return []
        return self.claim_dirty(
            worker_id=worker_id,
            limit=len(keys),
            now=now,
            lease_seconds=lease_seconds,
            entity_keys=keys,
        )

    def complete_dirty(
        self,
        *,
        entity_type: str,
        entity_id: str,
        reason_code: str,
        source_version: str,
        resolved_at: datetime | str,
        worker_id: str | None = None,
    ) -> bool:
        """Resolve one exact item, optionally requiring its lease owner."""

        timestamp = _queue_timestamp(resolved_at)
        clauses = [
            "entity_type=?", "entity_id=?", "reason_code=?", "source_version=?",
            "resolved_at IS NULL", "status='LEASED'",
        ]
        params: list[Any] = [
            str(entity_type or "").strip().upper(),
            str(entity_id or "").strip(),
            str(reason_code or "").strip().upper(),
            str(source_version or "").strip(),
        ]
        if worker_id is not None:
            clauses.append("lease_owner=?")
            params.append(str(worker_id).strip())
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE dirty_entities
                SET status='RESOLVED', resolved_at=?, updated_at=?,
                    lease_owner=NULL, lease_expires_at=NULL, next_retry_at=NULL
                WHERE """ + " AND ".join(clauses),
                [timestamp, timestamp, *params],
            )
        return bool(cursor.rowcount)

    def retry_dirty(
        self,
        *,
        entity_type: str,
        entity_id: str,
        reason_code: str,
        source_version: str,
        error_code: str,
        now: datetime | str | None = None,
        worker_id: str | None = None,
        base_delay_seconds: int = DEFAULT_DIRTY_BACKOFF_SECONDS,
        max_delay_seconds: int = 3600,
    ) -> dict[str, Any] | None:
        """Move a leased item to RETRY with bounded exponential backoff."""

        if isinstance(base_delay_seconds, bool) or not isinstance(base_delay_seconds, int) or base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be a positive integer")
        if isinstance(max_delay_seconds, bool) or not isinstance(max_delay_seconds, int) or max_delay_seconds <= 0:
            raise ValueError("max_delay_seconds must be a positive integer")
        now_dt, now_text = _queue_now(now)
        key = (
            str(entity_type or "").strip().upper(),
            str(entity_id or "").strip(),
            str(reason_code or "").strip().upper(),
            str(source_version or "").strip(),
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT * FROM dirty_entities
                    WHERE entity_type=? AND entity_id=? AND reason_code=? AND source_version=?
                    """,
                    key,
                ).fetchone()
                if row is None or str(row["status"]).upper() == "RESOLVED":
                    connection.commit()
                    return None
                if worker_id is not None and str(row["lease_owner"] or "") != str(worker_id).strip():
                    connection.commit()
                    return None
                attempts = int(row["attempts"] or 0)
                max_attempts = max(1, int(row["max_attempts"] or DEFAULT_DIRTY_MAX_ATTEMPTS))
                if attempts >= max_attempts:
                    next_status = "DEAD"
                    next_retry = None
                else:
                    next_status = "RETRY"
                    delay = min(max_delay_seconds, base_delay_seconds * (2 ** max(0, attempts - 1)))
                    next_retry = (now_dt + timedelta(seconds=delay)).isoformat()
                safe_error = str(error_code or "UNKNOWN_ERROR").strip()[:200] or "UNKNOWN_ERROR"
                connection.execute(
                    """
                    UPDATE dirty_entities
                    SET status=?, next_retry_at=?, last_error_code=?, last_error_at=?,
                        lease_owner=NULL, lease_expires_at=NULL, updated_at=?
                    WHERE entity_type=? AND entity_id=? AND reason_code=? AND source_version=?
                    """,
                    (next_status, next_retry, safe_error, now_text, now_text, *key),
                )
                connection.commit()
                updated = connection.execute(
                    """
                    SELECT * FROM dirty_entities
                    WHERE entity_type=? AND entity_id=? AND reason_code=? AND source_version=?
                    """,
                    key,
                ).fetchone()
            except Exception:
                connection.rollback()
                raise
        return _dirty_dict(updated) if updated is not None else None

    def release_dirty(
        self,
        *,
        entity_type: str,
        entity_id: str,
        reason_code: str,
        source_version: str,
        now: datetime | str | None = None,
        worker_id: str | None = None,
    ) -> bool:
        """Release a lease for graceful shutdown without consuming a retry."""

        timestamp = _queue_timestamp(now)
        clauses = [
            "entity_type=?", "entity_id=?", "reason_code=?", "source_version=?",
            "status='LEASED'",
        ]
        params: list[Any] = [
            str(entity_type or "").strip().upper(), str(entity_id or "").strip(),
            str(reason_code or "").strip().upper(), str(source_version or "").strip(),
        ]
        if worker_id is not None:
            clauses.append("lease_owner=?")
            params.append(str(worker_id).strip())
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE dirty_entities
                SET status='RETRY', next_retry_at=?, lease_owner=NULL,
                    lease_expires_at=NULL, updated_at=?
                WHERE """ + " AND ".join(clauses),
                [timestamp, timestamp, *params],
            )
        return bool(cursor.rowcount)

    def list_dirty(
        self,
        *,
        statuses: Iterable[str] | None = None,
        entity_type: str | None = None,
        limit: int = 1000,
        include_resolved: bool = False,
    ) -> list[dict[str, Any]]:
        """Return queue rows for observability and maintenance tooling."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        clauses: list[str] = []
        params: list[Any] = []
        status_values = (statuses,) if isinstance(statuses, str) else (statuses or ())
        normalized_statuses = tuple(
            sorted({str(item or "").strip().upper() for item in status_values if str(item or "").strip()})
        )
        invalid = set(normalized_statuses) - DIRTY_STATUSES
        if invalid:
            raise ValueError(f"unknown dirty status: {sorted(invalid)[0]}")
        if normalized_statuses:
            clauses.append("status IN (" + ",".join("?" for _ in normalized_statuses) + ")")
            params.extend(normalized_statuses)
        elif not include_resolved:
            clauses.append("status <> 'RESOLVED'")
        if entity_type:
            clauses.append("entity_type=?")
            params.append(str(entity_type).strip().upper())
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 10000)))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM dirty_entities" + where
                + " ORDER BY priority DESC, created_at ASC, entity_type ASC, entity_id ASC LIMIT ?",
                params,
            ).fetchall()
        return [_dirty_dict(row) for row in rows]

    def dirty_stats(self) -> dict[str, int]:
        """Return counts by lifecycle state, including zero-count states."""

        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM dirty_entities GROUP BY status"
            ).fetchall()
        result = {status: 0 for status in DIRTY_STATUSES}
        result.update({str(row["status"]).upper(): int(row["count"]) for row in rows})
        return result

    def register_dirty_dependency(
        self,
        *,
        entity_type: str,
        entity_id: str,
        dependency_type: str,
        dependency_id: str,
        relation: str = "depends_on",
        created_at: datetime | str,
    ) -> None:
        """Persist one directed dependency edge for incremental expansion."""

        timestamp = _queue_timestamp(created_at)
        values = (
            str(entity_type or "").strip().upper(), str(entity_id or "").strip(),
            str(dependency_type or "").strip().upper(), str(dependency_id or "").strip(),
            str(relation or "depends_on").strip() or "depends_on", timestamp,
        )
        if not all(values[:4]):
            raise ValueError("dirty dependency fields must not be empty")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO dirty_entity_dependencies(
                    entity_type,entity_id,dependency_type,dependency_id,relation,created_at
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(entity_type,entity_id,dependency_type,dependency_id,relation)
                DO UPDATE SET created_at=excluded.created_at
                """,
                values,
            )

    def list_dirty_dependencies(
        self, *, entity_type: str, entity_id: str, relation: str | None = None
    ) -> list[dict[str, Any]]:
        clauses = ["entity_type=?", "entity_id=?"]
        params: list[Any] = [str(entity_type or "").strip().upper(), str(entity_id or "").strip()]
        if relation is not None:
            clauses.append("relation=?")
            params.append(str(relation).strip())
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM dirty_entity_dependencies WHERE " + " AND ".join(clauses)
                + " ORDER BY dependency_type, dependency_id",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def expand_dirty_dependencies(
        self,
        items: Iterable[Mapping[str, Any]],
        *,
        max_depth: int = 8,
    ) -> list[dict[str, Any]]:
        """Expand persisted edges breadth-first without looping on cycles.

        Returned rows contain the original queue item fields when available;
        synthetic dependency rows use ``reason_code=DEPENDENCY`` and a stable
        ``source_version=dependency`` marker.  A coordinator may lease exact
        queue rows again after expansion, while still rebuilding dependencies
        that have not yet received their own dirty event.
        """

        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")
        initial: list[dict[str, Any]] = []
        frontier: list[tuple[str, str, int]] = []
        seen: set[tuple[str, str]] = set()
        for raw in items:
            if not isinstance(raw, Mapping):
                continue
            entity_type = str(raw.get("entity_type") or "").strip().upper()
            entity_id = str(raw.get("entity_id") or "").strip()
            if not entity_type or not entity_id:
                continue
            key = (entity_type, entity_id)
            if key in seen:
                continue
            seen.add(key)
            value = dict(raw)
            value.setdefault("dependency", False)
            initial.append(value)
            frontier.append((entity_type, entity_id, 0))

        expanded = list(initial)
        with self._connect() as connection:
            while frontier:
                entity_type, entity_id, depth = frontier.pop(0)
                if depth >= max_depth:
                    continue
                rows = connection.execute(
                    """
                    SELECT dependency_type,dependency_id,relation,created_at
                    FROM dirty_entity_dependencies
                    WHERE entity_type=? AND entity_id=?
                    ORDER BY dependency_type,dependency_id,relation
                    """,
                    (entity_type, entity_id),
                ).fetchall()
                for row in rows:
                    key = (str(row["dependency_type"]), str(row["dependency_id"]))
                    if key in seen:
                        continue
                    seen.add(key)
                    dependency = {
                        "entity_type": key[0],
                        "entity_id": key[1],
                        "reason_code": "DEPENDENCY",
                        "source_version": "dependency",
                        "dependency": True,
                        "dependency_of": f"{entity_type}:{entity_id}",
                        "relation": row["relation"],
                        "created_at": row["created_at"],
                    }
                    expanded.append(dependency)
                    frontier.append((key[0], key[1], depth + 1))
        return expanded

    def stage_summary(
        self,
        run_id: str,
        lane_id: str,
        stage: str,
        *,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        strict: bool = False,
    ) -> dict[str, Any]:
        with self._connect() as connection:
            selected_generation = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            generation_clause = ""
            params: list[Any] = [run_id, lane_id, stage]
            if selected_generation is not None:
                generation_clause = " AND generation_id=?"
                params.append(selected_generation)
            grouped = connection.execute(
                """
                SELECT status, COUNT(*) AS count, SUM(sent_to_llm) AS sent
                FROM deterministic_stage_decisions
                WHERE run_id=? AND lane_id=? AND stage=?""" + generation_clause + """
                GROUP BY status ORDER BY status
                """,
                params,
            ).fetchall()
        counts = {str(row["status"]): int(row["count"]) for row in grouped}
        return {
            "run_id": run_id,
            "lane_id": lane_id,
            "stage": stage,
            "generation_id": selected_generation,
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
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        clauses = ["run_id=?", "lane_id=?", "stage=?"]
        parameters: list[Any] = [run_id, lane_id, stage]
        if status:
            clauses.append("status=?")
            parameters.append(status)
        parameters.extend((max(1, min(int(limit), 5000)), max(0, int(offset))))
        with self._connect() as connection:
            selected_generation = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            if selected_generation is not None:
                clauses.insert(0, "generation_id=?")
                parameters.insert(0, selected_generation)
            rows = connection.execute(
                "SELECT payload_json FROM deterministic_stage_decisions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY COALESCE(node_id,''), COALESCE(node_rank,2147483647), symbol LIMIT ? OFFSET ?",
                parameters,
            ).fetchall()
        return [json.loads(str(row["payload_json"])) for row in rows]

    def strict_stage_summary(
        self,
        run_id: str,
        lane_id: str,
        stage: str,
        *,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> dict[str, Any]:
        """Read a stage only from a published, run-bound generation."""

        return self.stage_summary(
            run_id,
            lane_id,
            stage,
            generation_id=generation_id,
            domain=domain,
            strict=True,
        )

    def strict_stage_decisions(
        self,
        run_id: str,
        lane_id: str,
        stage: str,
        *,
        status: str | None = None,
        limit: int = 500,
        offset: int = 0,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> list[dict[str, Any]]:
        """Read stage decisions only from a published, run-bound generation."""

        return self.stage_decisions(
            run_id,
            lane_id,
            stage,
            status=status,
            limit=limit,
            offset=offset,
            generation_id=generation_id,
            domain=domain,
            strict=True,
        )

    def record_theme_registry(
        self,
        *,
        run_id: str,
        lane_id: str,
        as_of: datetime | str,
        themes: Sequence[Mapping[str, Any]],
        nodes: Sequence[Mapping[str, Any]],
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
    ) -> str:
        timestamp = self._timestamp(as_of)
        version_hash = content_hash({"themes": themes, "nodes": nodes})
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.execute(
                    "INSERT OR REPLACE INTO theme_registry_versions(generation_id,run_id,lane_id,version_hash,as_of,payload_json) "
                    "VALUES(?,?,?,?,?,?)",
                    (selected_generation, run_id, lane_id, version_hash, timestamp, canonical_json(themes)),
                )
                connection.executemany(
                    "INSERT OR REPLACE INTO chain_node_versions(generation_id,run_id,lane_id,node_id,version_hash,as_of,payload_json) "
                    "VALUES(?,?,?,?,?,?,?)",
                    [
                        (
                            selected_generation,
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
            except Exception:
                connection.rollback()
                raise
        return version_hash

    def latest_theme_registry(
        self,
        *,
        lane_id: str,
        before: datetime | str,
        run_id: str | None = None,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        strict: bool = False,
    ) -> dict[str, Any] | None:
        """Return the latest point-in-time theme graph before ``before``."""

        cutoff = self._timestamp(before)
        with self._connect() as connection:
            selected_generation = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            clauses = ["lane_id=?", "as_of<?"]
            params: list[Any] = [lane_id, cutoff]
            if selected_generation is not None:
                clauses.insert(0, "generation_id=?")
                params.insert(0, selected_generation)
            row = connection.execute(
                "SELECT generation_id,run_id,version_hash,as_of,payload_json FROM theme_registry_versions "
                "WHERE " + " AND ".join(clauses) + " ORDER BY as_of DESC, rowid DESC LIMIT 1",
                params,
            ).fetchone()
            if row is None:
                return None
            node_clauses = [
                "generation_id=?", "run_id=?", "lane_id=?", "version_hash=?",
            ]
            nodes = connection.execute(
                "SELECT payload_json FROM chain_node_versions WHERE " + " AND ".join(node_clauses) + " ORDER BY node_id",
                (str(row["generation_id"]), str(row["run_id"]), lane_id, str(row["version_hash"])),
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
            "generation_id": str(row["generation_id"]),
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
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
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
            try:
                selected_generation = self._write_generation(
                    connection,
                    run_id=run_id,
                    generation_id=generation_id,
                    domain=domain,
                )
                connection.execute(
                    "DELETE FROM theme_taxonomy_links WHERE generation_id=? AND run_id=? AND lane_id=?",
                    (selected_generation, run_id, lane_id),
                )
                connection.executemany(
                    "INSERT INTO theme_taxonomy_links(generation_id,run_id,lane_id,node_id,taxonomy,taxonomy_code,taxonomy_name,match_method,confidence,source_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    [(selected_generation, *row) for row in rows],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return len(rows)

    def get_taxonomy_memberships(
        self,
        taxonomy: str,
        *,
        generation_id: str | None = None,
        run_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        symbol: str | None = None,
        strict: bool = True,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        taxonomy_name = str(taxonomy).strip().upper()
        if taxonomy_name not in {"INDUSTRY", "CONCEPT"}:
            raise ValueError("taxonomy must be INDUSTRY or CONCEPT")
        with self._connect() as connection:
            selected = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            clauses = ["taxonomy=?"]
            params: list[Any] = [taxonomy_name]
            if selected is not None:
                clauses.insert(0, "generation_id=?")
                params.insert(0, selected)
            if symbol:
                clauses.append("symbol=?")
                params.append(str(symbol).strip().upper())
            params.append(max(1, min(int(limit), 100_000)))
            rows = connection.execute(
                "SELECT * FROM taxonomy_membership_versions WHERE " + " AND ".join(clauses)
                + " ORDER BY as_of DESC,symbol,taxonomy_code LIMIT ?",
                params,
            ).fetchall()
        return [_taxonomy_dict(row) for row in rows]

    taxonomy_memberships = get_taxonomy_memberships

    def get_fundamental_features(
        self,
        *,
        generation_id: str | None = None,
        run_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        symbols: Iterable[str] | None = None,
        strict: bool = True,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        requested = tuple(dict.fromkeys(str(item).strip().upper() for item in (symbols or ()) if str(item).strip()))
        with self._connect() as connection:
            selected = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            clauses: list[str] = []
            params: list[Any] = []
            if selected is not None:
                clauses.append("generation_id=?")
                params.append(selected)
            if requested:
                clauses.append("symbol IN (" + ",".join("?" for _ in requested) + ")")
                params.extend(requested)
            if not clauses:
                clauses.append("1=1")
            params.append(max(1, min(int(limit), 100_000)))
            rows = connection.execute(
                "SELECT * FROM stock_fundamental_features WHERE " + " AND ".join(clauses)
                + " ORDER BY as_of DESC,symbol LIMIT ?",
                params,
            ).fetchall()
        return [_payload_with_columns(row, ("generation_id", "symbol", "as_of", "feature_version", "source_hash", "quality_score", "available")) for row in rows]

    fundamental_features = get_fundamental_features

    def get_market_role_features(
        self,
        *,
        run_id: str,
        lane_id: str,
        generation_id: str | None = None,
        domain: str = DEFAULT_FEATURE_DOMAIN,
        symbols: Iterable[str] | None = None,
        strict: bool = True,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        requested = tuple(dict.fromkeys(str(item).strip().upper() for item in (symbols or ()) if str(item).strip()))
        with self._connect() as connection:
            selected = self._query_generation(
                connection,
                run_id=run_id,
                generation_id=generation_id,
                domain=domain,
                strict=strict,
            )
            clauses = ["run_id=?", "lane_id=?"]
            params: list[Any] = [run_id, lane_id]
            if selected is not None:
                clauses.insert(0, "generation_id=?")
                params.insert(0, selected)
            if requested:
                clauses.append("symbol IN (" + ",".join("?" for _ in requested) + ")")
                params.extend(requested)
            params.append(max(1, min(int(limit), 100_000)))
            rows = connection.execute(
                "SELECT * FROM stock_market_role_features WHERE " + " AND ".join(clauses)
                + " ORDER BY theme_id,role_score DESC,symbol LIMIT ?",
                params,
            ).fetchall()
        return [_payload_with_columns(row, ("generation_id", "run_id", "lane_id", "symbol", "theme_id", "feature_version", "role_score")) for row in rows]

    market_role_features = get_market_role_features

    def assert_generation_usable(self, generation_id: str) -> dict[str, Any]:
        """Return a generation only when it is safe for production reads."""

        with self._connect() as connection:
            row = self._assert_generation(connection, generation_id, strict=True)
        return _generation_dict(row)


def _generation_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = {key: row[key] for key in row.keys()}
    metadata = result.pop("metadata_json", "{}")
    result["metadata"] = ResearchFeatureStore._parse_json(metadata, {})
    if "activation_eligible" in result:
        result["activation_eligible"] = bool(result["activation_eligible"])
    if "validation_manifest_json" in result:
        result["validation_manifest"] = ResearchFeatureStore._parse_json(
            result["validation_manifest_json"], {}
        )
    return result


def _binding_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _member_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = {key: row[key] for key in row.keys()}
    payload = result.pop("payload_json", "{}")
    result["payload"] = ResearchFeatureStore._parse_json(payload, {})
    return result


def _taxonomy_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = {key: row[key] for key in row.keys()}
    payload = result.pop("payload_json", "{}")
    result["payload"] = ResearchFeatureStore._parse_json(payload, {})
    return result


def _payload_with_columns(row: sqlite3.Row, columns: Sequence[str]) -> dict[str, Any]:
    result = ResearchFeatureStore._parse_json(row["payload_json"], {})
    if not isinstance(result, dict):
        result = {}
    for column in columns:
        result[column] = row[column]
    return result


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


def _queue_timestamp(value: datetime | str | None) -> str:
    """Normalize queue timestamps to comparable UTC ISO-8601 strings."""

    if value is None:
        return datetime.now(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp must not be empty")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid timestamp: {value!r}") from exc
    else:
        raise TypeError("timestamp must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        # Legacy callers occasionally supplied a naive timestamp.  Treating it
        # as UTC retains ordering while making cross-process lease comparisons
        # deterministic; new callers should pass an aware timestamp.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _queue_now(value: datetime | str | None) -> tuple[datetime, str]:
    normalized = _queue_timestamp(value)
    parsed = datetime.fromisoformat(normalized)
    return parsed, normalized


def _dirty_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a dirty queue row without exposing a live SQLite object."""

    return {key: row[key] for key in row.keys()}


__all__ = [
    "DEFAULT_FEATURE_DOMAIN",
    "DEFAULT_DIRTY_BACKOFF_SECONDS",
    "DEFAULT_DIRTY_LEASE_SECONDS",
    "DEFAULT_DIRTY_MAX_ATTEMPTS",
    "DIRTY_STATUSES",
    "FEATURE_SCHEMA",
    "GENERATION_PURPOSES",
    "FeatureGenerationError",
    "FeatureStoreError",
    "GENERATION_STATUSES",
    "LEGACY_GENERATION_ID",
    "ResearchFeatureStore",
    "SCHEMA_VERSION",
    "canonical_json",
    "content_hash",
]
