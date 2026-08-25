"""Durable runtime state for research lanes, A4 and paper simulation.

This module is intentionally self-contained.  SQLite is the source of truth
for state transitions and idempotency; callers never need to use a raw
connection.  All writes use ``BEGIN IMMEDIATE`` with WAL/FULL durability.
No table in this module contains model reasoning text.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..contracts import RunStatus, StageStatus


SHANGHAI = ZoneInfo("Asia/Shanghai")


class RuntimeStateError(RuntimeError):
    """Base class with a safe, stable public reason code."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


class StateTransitionError(RuntimeStateError):
    pass


class PersistenceError(RuntimeStateError):
    pass


class PersistenceBlockedError(PersistenceError):
    pass


class PlanStatus(StrEnum):
    DRAFT_CLOSE = "DRAFT_CLOSE"
    PENDING_MORNING_REVIEW = "PENDING_MORNING_REVIEW"
    ACTIVE_TODAY = "ACTIVE_TODAY"
    INVALIDATED = "INVALIDATED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class MonitorAction(StrEnum):
    NO_ACTION = "NO_ACTION"
    START_CONFIRMATION = "START_CONFIRMATION"
    BUY_SIGNAL = "BUY_SIGNAL"
    ADD_SIGNAL = "ADD_SIGNAL"
    SELL_SIGNAL = "SELL_SIGNAL"
    REDUCE_SIGNAL = "REDUCE_SIGNAL"
    CANCEL_SIGNAL = "CANCEL_SIGNAL"
    BLOCK = "BLOCK"
    LLM_VETO = "LLM_VETO"
    PLAN_INVALIDATED = "PLAN_INVALIDATED"
    DATA_BLOCK = "DATA_BLOCK"
    FORCED_RISK_EXIT = "FORCED_RISK_EXIT"
    EMPTY_SCOPE = "EMPTY_SCOPE"
    MONITOR_OVERRUN = "MONITOR_OVERRUN"


EFFECTIVE_ACTIONS = frozenset(
    {
        MonitorAction.BUY_SIGNAL.value,
        MonitorAction.ADD_SIGNAL.value,
        MonitorAction.SELL_SIGNAL.value,
        MonitorAction.REDUCE_SIGNAL.value,
        MonitorAction.LLM_VETO.value,
        MonitorAction.PLAN_INVALIDATED.value,
        MonitorAction.DATA_BLOCK.value,
        MonitorAction.FORCED_RISK_EXIT.value,
    }
)


RUN_TRANSITIONS: dict[str, frozenset[str]] = {
    RunStatus.CREATED.value: frozenset({RunStatus.DATA_PREPARING.value}),
    RunStatus.DATA_PREPARING.value: frozenset(
        {RunStatus.DATA_BOUND.value, RunStatus.BLOCKED.value, RunStatus.FAILED.value, RunStatus.EXPIRED.value}
    ),
    RunStatus.DATA_BOUND.value: frozenset(
        {RunStatus.RUNNING.value, RunStatus.BLOCKED.value, RunStatus.FAILED.value, RunStatus.EXPIRED.value}
    ),
    RunStatus.RUNNING.value: frozenset(
        {RunStatus.READY_TO_PUBLISH.value, RunStatus.BLOCKED.value, RunStatus.FAILED.value, RunStatus.EXPIRED.value}
    ),
    RunStatus.READY_TO_PUBLISH.value: frozenset(
        {RunStatus.PUBLISHED.value, RunStatus.BLOCKED.value, RunStatus.FAILED.value, RunStatus.EXPIRED.value}
    ),
}

STAGE_TRANSITIONS: dict[str, frozenset[str]] = {
    StageStatus.PENDING.value: frozenset({StageStatus.RUNNING.value, StageStatus.BLOCKED.value, StageStatus.EXPIRED.value}),
    StageStatus.RUNNING.value: frozenset(
        {StageStatus.VALIDATED.value, StageStatus.BLOCKED.value, StageStatus.FAILED.value, StageStatus.EXPIRED.value}
    ),
}


def _now() -> datetime:
    return datetime.now(SHANGHAI)


def _iso(value: datetime | date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(SHANGHAI).isoformat()
    return value.isoformat()


def _json(value: Mapping[str, Any] | None) -> str:
    if value is None:
        return "{}"
    # The payload is structural metadata only.  Callers must not put natural
    # language/model reasoning here; the store also rejects common fields.
    forbidden = {"reasoning", "thinking", "thoughts", "analysis", "response_text"}
    if forbidden.intersection(str(key).lower() for key in value):
        raise ValueError("model reasoning text is not persistable")
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _row_dict(row: sqlite3.Row | tuple[Any, ...] | None, columns: tuple[str, ...] | None = None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    if columns is None:
        return dict(enumerate(row))
    return dict(zip(columns, row))


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, declaration: str) -> None:
    existing = {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


class RuntimeStore:
    """SQLite-backed state store with durable fail-closed semantics."""

    def __init__(self, path: str | Path):
        requested = Path(path)
        if requested.exists() and requested.is_dir():
            requested = requested / "runtime.sqlite3"
        elif requested.suffix == "" and not requested.exists():
            requested = requested / "runtime.sqlite3"
        self.path = requested.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._persistence_failed = False
        self._lock = threading.RLock()
        self._initialize()

    @property
    def persistence_failed(self) -> bool:
        return self._persistence_failed

    @property
    def healthy(self) -> bool:
        if self._persistence_failed:
            return False
        try:
            with self._connect() as connection:
                result = connection.execute("PRAGMA quick_check").fetchone()
            return bool(result and result[0] == "ok")
        except Exception:
            self._persistence_failed = True
            return False

    def assert_writable(self) -> None:
        if self._persistence_failed:
            raise PersistenceBlockedError("PERSISTENCE_FAILED")
        if not self.healthy:
            raise PersistenceBlockedError("PERSISTENCE_FAILED")

    def mark_persistence_failed(self) -> None:
        self._persistence_failed = True

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        try:
            with self._connect() as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS runtime_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS research_runs (
                        run_id TEXT PRIMARY KEY,
                        trade_date TEXT NOT NULL,
                        slot TEXT NOT NULL,
                        model TEXT NOT NULL,
                        status TEXT NOT NULL,
                        snapshot_hash TEXT,
                        prompt_hash TEXT,
                        config_hash TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(trade_date, slot, model)
                    );
                    CREATE TABLE IF NOT EXISTS lane_stages (
                        run_id TEXT NOT NULL REFERENCES research_runs(run_id),
                        stage TEXT NOT NULL CHECK(stage IN ('A1','A2','A3')),
                        status TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(run_id, stage)
                    );
                    CREATE TABLE IF NOT EXISTS execution_plans (
                        plan_id TEXT PRIMARY KEY,
                        lane_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        status TEXT NOT NULL,
                        plan_version INTEGER NOT NULL DEFAULT 1,
                        valid_from TEXT,
                        expires_at TEXT,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(lane_id, plan_id)
                    );
                    CREATE TABLE IF NOT EXISTS monitor_events (
                        event_id TEXT PRIMARY KEY,
                        event_key TEXT NOT NULL UNIQUE,
                        lane_id TEXT NOT NULL,
                        minute_end TEXT NOT NULL,
                        action TEXT NOT NULL,
                        reason_code TEXT,
                        effective INTEGER NOT NULL CHECK(effective IN (0,1)),
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS virtual_accounts (
                        account_id TEXT PRIMARY KEY,
                        model TEXT NOT NULL UNIQUE,
                        initial_cash REAL NOT NULL CHECK(initial_cash >= 0),
                        cash REAL NOT NULL CHECK(cash >= 0),
                        equity REAL NOT NULL CHECK(equity >= 0),
                        status TEXT NOT NULL DEFAULT 'ACTIVE',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS virtual_positions (
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        symbol TEXT NOT NULL,
                        total_qty INTEGER NOT NULL CHECK(total_qty >= 0),
                        sellable_qty INTEGER NOT NULL CHECK(sellable_qty >= 0),
                        avg_cost REAL NOT NULL CHECK(avg_cost >= 0),
                        stop_level REAL,
                        plan_id TEXT,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(account_id, symbol),
                        CHECK(sellable_qty <= total_qty)
                    );
                    CREATE TABLE IF NOT EXISTS position_risk_plans (
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        symbol TEXT NOT NULL,
                        source_plan_id TEXT,
                        status TEXT NOT NULL,
                        entry_price REAL NOT NULL CHECK(entry_price > 0),
                        stop_level REAL NOT NULL CHECK(stop_level > 0),
                        max_adds INTEGER NOT NULL DEFAULT 1 CHECK(max_adds >= 0),
                        adds_used INTEGER NOT NULL DEFAULT 0 CHECK(adds_used >= 0),
                        corporate_action_version TEXT,
                        unresolved_corporate_action INTEGER NOT NULL DEFAULT 0 CHECK(unresolved_corporate_action IN (0,1)),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(account_id, symbol)
                    );
                    CREATE TABLE IF NOT EXISTS portfolio_marks (
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        symbol TEXT NOT NULL,
                        price REAL NOT NULL CHECK(price > 0),
                        bar_end TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(account_id, symbol)
                    );
                    CREATE TABLE IF NOT EXISTS account_trading_days (
                        account_id TEXT PRIMARY KEY REFERENCES virtual_accounts(account_id),
                        trade_date TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS applied_corporate_actions (
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        symbol TEXT NOT NULL,
                        action_id TEXT NOT NULL,
                        quantity_factor REAL NOT NULL CHECK(quantity_factor > 0),
                        price_factor REAL NOT NULL CHECK(price_factor > 0),
                        effective_at TEXT NOT NULL,
                        applied_at TEXT NOT NULL,
                        PRIMARY KEY(account_id, symbol, action_id)
                    );
                    CREATE TABLE IF NOT EXISTS simulation_intents (
                        intent_id TEXT PRIMARY KEY,
                        intent_key TEXT NOT NULL UNIQUE,
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        signal_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        action TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS virtual_fills (
                        fill_id TEXT PRIMARY KEY,
                        intent_id TEXT NOT NULL REFERENCES simulation_intents(intent_id),
                        fill_sequence INTEGER NOT NULL,
                        account_id TEXT NOT NULL REFERENCES virtual_accounts(account_id),
                        signal_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        action TEXT NOT NULL,
                        qty INTEGER NOT NULL CHECK(qty > 0),
                        price REAL NOT NULL CHECK(price > 0),
                        fee REAL NOT NULL CHECK(fee >= 0),
                        bar_end TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        UNIQUE(intent_id, fill_sequence)
                    );
                    CREATE TABLE IF NOT EXISTS scheduler_leases (
                        lease_name TEXT PRIMARY KEY,
                        owner TEXT NOT NULL,
                        acquired_at TEXT NOT NULL,
                        heartbeat_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        generation INTEGER NOT NULL DEFAULT 1,
                        last_dispatch_key TEXT
                    );
                    CREATE TABLE IF NOT EXISTS workflow_runs (
                        run_id TEXT NOT NULL,
                        lane_id TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        slot TEXT NOT NULL,
                        model TEXT NOT NULL,
                        status TEXT NOT NULL,
                        snapshot_hash TEXT NOT NULL,
                        prompt_hash TEXT,
                        config_hash TEXT,
                        reason_codes_json TEXT NOT NULL DEFAULT '[]',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(run_id, lane_id)
                    );
                    CREATE TABLE IF NOT EXISTS workflow_stages (
                        run_id TEXT NOT NULL,
                        lane_id TEXT NOT NULL,
                        stage TEXT NOT NULL CHECK(stage IN ('A1','A2','A3')),
                        status TEXT NOT NULL,
                        reason_codes_json TEXT NOT NULL DEFAULT '[]',
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY(run_id, lane_id, stage),
                        FOREIGN KEY(run_id, lane_id) REFERENCES workflow_runs(run_id, lane_id)
                    );
                    """
                )
                _ensure_column(connection, "scheduler_leases", "state", "TEXT NOT NULL DEFAULT 'ACTIVE'")
                _ensure_column(connection, "scheduler_leases", "completed_at", "TEXT")
        except Exception:
            self._persistence_failed = True
            raise PersistenceError("PERSISTENCE_FAILED") from None

    def _write(self, operation):
        self.assert_writable()
        connection: sqlite3.Connection | None = None
        with self._lock:
            try:
                connection = self._connect()
                connection.execute("BEGIN IMMEDIATE")
                result = operation(connection)
                connection.commit()
                return result
            except (StateTransitionError, ValueError, KeyError, PersistenceBlockedError):
                if connection is not None:
                    connection.rollback()
                raise
            except sqlite3.IntegrityError:
                if connection is not None:
                    connection.rollback()
                raise StateTransitionError("STATE_CONFLICT") from None
            except Exception:
                if connection is not None:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                self._persistence_failed = True
                raise PersistenceError("PERSISTENCE_FAILED") from None
            finally:
                if connection is not None:
                    connection.close()

    def _read(self, operation):
        if self._persistence_failed:
            raise PersistenceBlockedError("PERSISTENCE_FAILED")
        try:
            with self._connect() as connection:
                return operation(connection)
        except PersistenceBlockedError:
            raise
        except Exception:
            self._persistence_failed = True
            raise PersistenceError("PERSISTENCE_FAILED") from None

    # ------------------------------------------------------------------
    # Research run and stage state machines
    # ------------------------------------------------------------------
    def create_research_run(self, run_id: str, trade_date: str, slot: str, model: str) -> dict[str, Any]:
        now = _iso(_now())

        def operation(connection):
            existing = connection.execute(
                "SELECT * FROM research_runs WHERE trade_date=? AND slot=? AND model=?",
                (trade_date, str(slot), model),
            ).fetchone()
            if existing is not None:
                if existing["run_id"] != run_id:
                    raise StateTransitionError("DUPLICATE_RESEARCH_RUN")
                return _row_dict(existing)
            connection.execute(
                "INSERT INTO research_runs(run_id,trade_date,slot,model,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, trade_date, str(slot), model, RunStatus.CREATED.value, now, now),
            )
            return _row_dict(connection.execute("SELECT * FROM research_runs WHERE run_id=?", (run_id,)).fetchone())

        return self._write(operation)

    def get_research_run(self, run_id: str) -> dict[str, Any] | None:
        return self._read(lambda connection: _row_dict(connection.execute("SELECT * FROM research_runs WHERE run_id=?", (run_id,)).fetchone()))

    def transition_research_run(
        self,
        run_id: str,
        status: str | RunStatus,
        *,
        snapshot_hash: str | None = None,
        prompt_hash: str | None = None,
        config_hash: str | None = None,
    ) -> dict[str, Any]:
        target = str(status)
        now = _iso(_now())

        def operation(connection):
            current = connection.execute("SELECT * FROM research_runs WHERE run_id=?", (run_id,)).fetchone()
            if current is None:
                raise StateTransitionError("RUN_NOT_FOUND")
            current_status = current["status"]
            if target == current_status:
                return _row_dict(current)
            if target not in RUN_TRANSITIONS.get(current_status, frozenset()):
                raise StateTransitionError("ILLEGAL_RUN_TRANSITION")
            hashes = (snapshot_hash, prompt_hash, config_hash)
            if target == RunStatus.DATA_BOUND.value and not all(hashes):
                raise StateTransitionError("DATA_BOUND_HASHES_REQUIRED")
            if current_status not in {RunStatus.CREATED.value, RunStatus.DATA_PREPARING.value}:
                for name, value in zip(("snapshot_hash", "prompt_hash", "config_hash"), hashes):
                    if value is not None and current[name] != value:
                        raise StateTransitionError("IMMUTABLE_RUN_HASH")
            if target != RunStatus.DATA_BOUND.value:
                hashes = (current["snapshot_hash"], current["prompt_hash"], current["config_hash"])
            connection.execute(
                """
                UPDATE research_runs
                SET status=?, snapshot_hash=?, prompt_hash=?, config_hash=?, updated_at=?
                WHERE run_id=? AND status=?
                """,
                (target, *hashes, now, run_id, current_status),
            )
            return _row_dict(connection.execute("SELECT * FROM research_runs WHERE run_id=?", (run_id,)).fetchone())

        return self._write(operation)

    def create_lane_stage(self, run_id: str, stage: str, status: str | StageStatus = StageStatus.PENDING) -> dict[str, Any]:
        stage = str(stage)
        status = str(status)
        if stage not in {"A1", "A2", "A3"}:
            raise ValueError("invalid lane stage")
        if status not in {item.value for item in StageStatus}:
            raise ValueError("invalid lane stage status")
        now = _iso(_now())

        def operation(connection):
            connection.execute(
                "INSERT OR IGNORE INTO lane_stages(run_id,stage,status,updated_at) VALUES(?,?,?,?)",
                (run_id, stage, status, now),
            )
            return _row_dict(
                connection.execute("SELECT * FROM lane_stages WHERE run_id=? AND stage=?", (run_id, stage)).fetchone()
            )

        return self._write(operation)

    def get_lane_stage(self, run_id: str, stage: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute("SELECT * FROM lane_stages WHERE run_id=? AND stage=?", (run_id, stage)).fetchone()
            )
        )

    def transition_lane_stage(self, run_id: str, stage: str, status: str | StageStatus) -> dict[str, Any]:
        stage = str(stage)
        target = str(status)
        now = _iso(_now())

        def operation(connection):
            current = connection.execute(
                "SELECT * FROM lane_stages WHERE run_id=? AND stage=?", (run_id, stage)
            ).fetchone()
            if current is None:
                raise StateTransitionError("STAGE_NOT_FOUND")
            if target == current["status"]:
                return _row_dict(current)
            if target not in STAGE_TRANSITIONS.get(current["status"], frozenset()):
                raise StateTransitionError("ILLEGAL_STAGE_TRANSITION")
            if target == StageStatus.RUNNING.value and stage in {"A2", "A3"}:
                previous = "A1" if stage == "A2" else "A2"
                row = connection.execute(
                    "SELECT status FROM lane_stages WHERE run_id=? AND stage=?", (run_id, previous)
                ).fetchone()
                if row is None or row["status"] != StageStatus.VALIDATED.value:
                    raise StateTransitionError("STAGE_PREREQUISITE_NOT_VALIDATED")
            connection.execute(
                "UPDATE lane_stages SET status=?,updated_at=? WHERE run_id=? AND stage=? AND status=?",
                (target, now, run_id, stage, current["status"]),
            )
            return _row_dict(
                connection.execute("SELECT * FROM lane_stages WHERE run_id=? AND stage=?", (run_id, stage)).fetchone()
            )

        return self._write(operation)

    def record_workflow_run(
        self,
        *,
        run_id: str,
        lane_id: str,
        trade_date: str,
        slot: str,
        model: str,
        status: str,
        snapshot_hash: str,
        prompt_hash: str | None = None,
        config_hash: str | None = None,
        reason_codes: Sequence[str] = (),
    ) -> dict[str, Any]:
        now = _iso(_now())
        reasons = json.dumps(list(dict.fromkeys(str(item) for item in reason_codes)), ensure_ascii=False)

        def operation(connection):
            connection.execute(
                """
                INSERT INTO workflow_runs(
                    run_id,lane_id,trade_date,slot,model,status,snapshot_hash,prompt_hash,config_hash,
                    reason_codes_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id,lane_id) DO UPDATE SET
                    status=excluded.status,prompt_hash=COALESCE(excluded.prompt_hash,workflow_runs.prompt_hash),
                    config_hash=COALESCE(excluded.config_hash,workflow_runs.config_hash),
                    reason_codes_json=excluded.reason_codes_json,updated_at=excluded.updated_at
                """,
                (
                    run_id,
                    lane_id,
                    trade_date,
                    slot,
                    model,
                    status,
                    snapshot_hash,
                    prompt_hash,
                    config_hash,
                    reasons,
                    now,
                    now,
                ),
            )
            return _row_dict(
                connection.execute(
                    "SELECT * FROM workflow_runs WHERE run_id=? AND lane_id=?",
                    (run_id, lane_id),
                ).fetchone()
            )

        return self._write(operation)

    def record_workflow_stage(
        self,
        *,
        run_id: str,
        lane_id: str,
        stage: str,
        status: str,
        reason_codes: Sequence[str] = (),
    ) -> dict[str, Any]:
        if stage not in {"A1", "A2", "A3"}:
            raise ValueError("invalid workflow stage")
        now = _iso(_now())
        reasons = json.dumps(list(dict.fromkeys(str(item) for item in reason_codes)), ensure_ascii=False)

        def operation(connection):
            connection.execute(
                """
                INSERT INTO workflow_stages(run_id,lane_id,stage,status,reason_codes_json,updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(run_id,lane_id,stage) DO UPDATE SET
                    status=excluded.status,reason_codes_json=excluded.reason_codes_json,updated_at=excluded.updated_at
                """,
                (run_id, lane_id, stage, status, reasons, now),
            )
            return _row_dict(
                connection.execute(
                    "SELECT * FROM workflow_stages WHERE run_id=? AND lane_id=? AND stage=?",
                    (run_id, lane_id, stage),
                ).fetchone()
            )

        return self._write(operation)

    def list_workflow_runs(self, *, limit: int = 20) -> tuple[dict[str, Any], ...]:
        bounded = max(1, min(int(limit), 200))
        return self._read(
            lambda connection: tuple(
                _row_dict(row)
                for row in connection.execute(
                    "SELECT * FROM workflow_runs ORDER BY updated_at DESC,run_id,lane_id LIMIT ?",
                    (bounded,),
                ).fetchall()
            )
        )

    def mark_workflow_runs_published(self, run_id: str, lane_ids: Sequence[str]) -> int:
        lanes = tuple(dict.fromkeys(str(item) for item in lane_ids))
        if not lanes:
            return 0
        now = _iso(_now())

        def operation(connection):
            updated = 0
            for lane_id in lanes:
                cursor = connection.execute(
                    """
                    UPDATE workflow_runs SET status='PUBLISHED',updated_at=?
                    WHERE run_id=? AND lane_id=? AND status='READY_TO_PUBLISH'
                    """,
                    (now, run_id, lane_id),
                )
                updated += int(cursor.rowcount)
            return updated

        return int(self._write(operation))

    # ------------------------------------------------------------------
    # Execution plans and monitor event ledger
    # ------------------------------------------------------------------
    def create_execution_plan(
        self,
        plan_id: str,
        lane_id: str,
        symbol: str,
        *,
        status: str | PlanStatus = PlanStatus.DRAFT_CLOSE,
        plan_version: int = 1,
        valid_from: datetime | str | None = None,
        expires_at: datetime | str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _iso(_now())
        payload_json = _json(payload)
        if str(status) not in {item.value for item in PlanStatus}:
            raise ValueError("invalid plan status")

        def operation(connection):
            existing = connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if existing is not None:
                return _row_dict(existing)
            connection.execute(
                """
                INSERT INTO execution_plans(
                    plan_id,lane_id,symbol,status,plan_version,valid_from,expires_at,payload_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (plan_id, lane_id, symbol, str(status), int(plan_version), _iso(valid_from), _iso(expires_at), payload_json, now, now),
            )
            return _row_dict(connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone())

        return self._write(operation)

    def get_execution_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self._read(lambda connection: _row_dict(connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone()))

    def list_execution_plans(
        self,
        *,
        lane_id: str | None = None,
        status: str | PlanStatus | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """Read-only plan listing used by publication/recovery checks."""

        status_value = str(status) if status is not None else None
        if status_value is not None and status_value not in {item.value for item in PlanStatus}:
            raise ValueError("invalid plan status")

        def operation(connection):
            clauses: list[str] = []
            args: list[Any] = []
            if lane_id is not None:
                clauses.append("lane_id=?")
                args.append(lane_id)
            if status_value is not None:
                clauses.append("status=?")
                args.append(status_value)
            sql = "SELECT * FROM execution_plans" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY lane_id,symbol,plan_id"
            return tuple(_row_dict(row) for row in connection.execute(sql, args).fetchall())

        return self._read(operation)

    def activate_plan(self, plan_id: str, *, valid_from: datetime | str | None = None) -> dict[str, Any]:
        return self._transition_plan(plan_id, PlanStatus.ACTIVE_TODAY.value, valid_from=valid_from)

    def invalidate_plan(self, plan_id: str, *, status: str | PlanStatus = PlanStatus.INVALIDATED) -> dict[str, Any]:
        target = str(status)
        if target not in {PlanStatus.INVALIDATED.value, PlanStatus.CANCELLED.value, PlanStatus.EXPIRED.value}:
            raise ValueError("invalid terminal plan status")
        return self._transition_plan(plan_id, target)

    def _transition_plan(self, plan_id: str, target: str, *, valid_from: datetime | str | None = None) -> dict[str, Any]:
        now = _iso(_now())

        def operation(connection):
            current = connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if current is None:
                raise StateTransitionError("PLAN_NOT_FOUND")
            if current["status"] == target:
                return _row_dict(current)
            allowed = {
                PlanStatus.DRAFT_CLOSE.value: {PlanStatus.PENDING_MORNING_REVIEW.value, PlanStatus.INVALIDATED.value},
                PlanStatus.PENDING_MORNING_REVIEW.value: {
                    PlanStatus.ACTIVE_TODAY.value,
                    PlanStatus.INVALIDATED.value,
                    PlanStatus.EXPIRED.value,
                },
                PlanStatus.ACTIVE_TODAY.value: {
                    PlanStatus.INVALIDATED.value,
                    PlanStatus.CANCELLED.value,
                    PlanStatus.EXPIRED.value,
                },
            }
            if target not in allowed.get(current["status"], set()):
                raise StateTransitionError("ILLEGAL_PLAN_TRANSITION")
            connection.execute(
                "UPDATE execution_plans SET status=?,valid_from=?,updated_at=? WHERE plan_id=? AND status=?",
                (target, _iso(valid_from) if valid_from is not None else current["valid_from"], now, plan_id, current["status"]),
            )
            return _row_dict(connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone())

        return self._write(operation)

    def set_plan_pending_morning_review(self, plan_id: str) -> dict[str, Any]:
        return self._transition_plan(plan_id, PlanStatus.PENDING_MORNING_REVIEW.value)

    def publish_plan_batch(
        self,
        plans: Sequence[Mapping[str, Any]],
        *,
        expire_active_lanes: Sequence[str] = (),
    ) -> tuple[dict[str, Any], ...]:
        """Publish a validated multi-lane plan set in one SQLite transaction."""

        normalized: list[dict[str, Any]] = []
        allowed_statuses = {PlanStatus.PENDING_MORNING_REVIEW.value, PlanStatus.ACTIVE_TODAY.value}
        for raw in plans:
            item = dict(raw)
            required = {"plan_id", "lane_id", "symbol", "status", "expires_at", "payload"}
            if not required.issubset(item):
                raise ValueError("plan batch item incomplete")
            status = str(item["status"])
            if status not in allowed_statuses:
                raise ValueError("plan batch status invalid")
            item["status"] = status
            item["payload_json"] = _json(item["payload"])
            normalized.append(item)
        now = _iso(_now())
        lanes = tuple(dict.fromkeys(str(item) for item in expire_active_lanes))

        def operation(connection):
            for lane_id in lanes:
                connection.execute(
                    "UPDATE execution_plans SET status=?,updated_at=? WHERE lane_id=? AND status=?",
                    (PlanStatus.EXPIRED.value, now, lane_id, PlanStatus.ACTIVE_TODAY.value),
                )
            rows: list[dict[str, Any]] = []
            for item in normalized:
                existing = connection.execute(
                    "SELECT * FROM execution_plans WHERE plan_id=?",
                    (str(item["plan_id"]),),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO execution_plans(
                            plan_id,lane_id,symbol,status,plan_version,valid_from,expires_at,
                            payload_json,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(item["plan_id"]),
                            str(item["lane_id"]),
                            str(item["symbol"]),
                            str(item["status"]),
                            int(item.get("plan_version", 1)),
                            _iso(item.get("valid_from")),
                            _iso(item["expires_at"]),
                            item["payload_json"],
                            now,
                            now,
                        ),
                    )
                else:
                    immutable = (
                        existing["lane_id"],
                        existing["symbol"],
                        existing["payload_json"],
                    )
                    proposed = (
                        str(item["lane_id"]),
                        str(item["symbol"]),
                        item["payload_json"],
                    )
                    if immutable != proposed:
                        raise StateTransitionError("PLAN_ID_CONTENT_CONFLICT")
                parent = item.get("parent_plan_id")
                if parent:
                    connection.execute(
                        "UPDATE execution_plans SET status=?,updated_at=? WHERE plan_id=? AND status=?",
                        (
                            PlanStatus.INVALIDATED.value,
                            now,
                            str(parent),
                            PlanStatus.PENDING_MORNING_REVIEW.value,
                        ),
                    )
                rows.append(
                    _row_dict(
                        connection.execute(
                            "SELECT * FROM execution_plans WHERE plan_id=?",
                            (str(item["plan_id"]),),
                        ).fetchone()
                    )
                )
            return tuple(rows)

        return self._write(operation)

    def activate_pending_plan_batch(
        self,
        plan_ids: Sequence[str],
        *,
        valid_from: datetime,
    ) -> tuple[dict[str, Any], ...]:
        """Atomically activate a fully validated morning-review plan set."""

        ids = tuple(dict.fromkeys(str(item) for item in plan_ids))
        if not ids:
            return ()
        stamp = _iso(valid_from)
        now = _iso(_now())

        def operation(connection):
            rows = []
            for plan_id in ids:
                row = connection.execute(
                    "SELECT * FROM execution_plans WHERE plan_id=?",
                    (plan_id,),
                ).fetchone()
                if row is None:
                    raise StateTransitionError("PLAN_NOT_FOUND")
                if row["status"] != PlanStatus.PENDING_MORNING_REVIEW.value:
                    raise StateTransitionError("PLAN_NOT_PENDING_MORNING_REVIEW")
                rows.append(row)
            for plan_id in ids:
                connection.execute(
                    "UPDATE execution_plans SET status=?,valid_from=?,updated_at=? WHERE plan_id=?",
                    (PlanStatus.ACTIVE_TODAY.value, stamp, now, plan_id),
                )
            return tuple(
                _row_dict(
                    connection.execute(
                        "SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)
                    ).fetchone()
                )
                for plan_id in ids
            )

        return self._write(operation)

    def list_active_plans(self, lane_id: str, *, at: datetime | None = None) -> tuple[dict[str, Any], ...]:
        stamp = _iso(at or _now())

        def operation(connection):
            rows = connection.execute(
                """
                SELECT * FROM execution_plans
                WHERE lane_id=? AND status=?
                  AND (valid_from IS NULL OR valid_from<=?)
                  AND (expires_at IS NULL OR expires_at>=?)
                ORDER BY symbol, plan_id
                """,
                (lane_id, PlanStatus.ACTIVE_TODAY.value, stamp, stamp),
            ).fetchall()
            return tuple(_row_dict(row) for row in rows)

        return self._read(operation)

    def record_monitor_event(
        self,
        *,
        event_key: str,
        lane_id: str,
        minute_end: datetime,
        action: str | MonitorAction,
        reason_code: str | None = None,
        effective: bool = False,
        payload: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        action = str(action)
        if effective and action not in EFFECTIVE_ACTIONS:
            raise ValueError("invalid effective monitor action")
        now = _iso(_now())
        minute = _iso(minute_end)
        payload_json = _json(payload)
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-monitor:{event_key}"))

        def operation(connection):
            existing = connection.execute("SELECT * FROM monitor_events WHERE event_key=?", (event_key,)).fetchone()
            if existing is not None:
                return _row_dict(existing), False
            connection.execute(
                """
                INSERT INTO monitor_events(event_id,event_key,lane_id,minute_end,action,reason_code,effective,payload_json,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (event_id, event_key, lane_id, minute, action, reason_code, int(effective), payload_json, now),
            )
            return _row_dict(connection.execute("SELECT * FROM monitor_events WHERE event_id=?", (event_id,)).fetchone()), True

        return self._write(operation)

    def list_monitor_events(self, *, lane_id: str | None = None, effective_only: bool = False) -> tuple[dict[str, Any], ...]:
        def operation(connection):
            where: list[str] = []
            args: list[Any] = []
            if lane_id is not None:
                where.append("lane_id=?")
                args.append(lane_id)
            if effective_only:
                where.append("effective=1")
            sql = "SELECT * FROM monitor_events" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY minute_end,event_id"
            return tuple(_row_dict(row) for row in connection.execute(sql, args).fetchall())

        return self._read(operation)

    # ------------------------------------------------------------------
    # Virtual account/position/fill ledger
    # ------------------------------------------------------------------
    def ensure_virtual_account(self, account_id: str, model: str, initial_cash: float = 1_000_000.0) -> dict[str, Any]:
        if initial_cash < 0:
            raise ValueError("initial cash must be non-negative")
        now = _iso(_now())

        def operation(connection):
            existing = connection.execute("SELECT * FROM virtual_accounts WHERE account_id=?", (account_id,)).fetchone()
            if existing is not None:
                return _row_dict(existing)
            connection.execute(
                "INSERT INTO virtual_accounts(account_id,model,initial_cash,cash,equity,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (account_id, model, float(initial_cash), float(initial_cash), float(initial_cash), "ACTIVE", now, now),
            )
            return _row_dict(connection.execute("SELECT * FROM virtual_accounts WHERE account_id=?", (account_id,)).fetchone())

        return self._write(operation)

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        return self._read(lambda connection: _row_dict(connection.execute("SELECT * FROM virtual_accounts WHERE account_id=?", (account_id,)).fetchone()))

    def list_accounts(self) -> tuple[dict[str, Any], ...]:
        return self._read(lambda connection: tuple(_row_dict(row) for row in connection.execute("SELECT * FROM virtual_accounts ORDER BY model").fetchall()))

    def get_position(self, account_id: str, symbol: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute("SELECT * FROM virtual_positions WHERE account_id=? AND symbol=?", (account_id, symbol)).fetchone()
            )
        )

    def list_positions(self, account_id: str) -> tuple[dict[str, Any], ...]:
        return self._read(
            lambda connection: tuple(
                _row_dict(row)
                for row in connection.execute("SELECT * FROM virtual_positions WHERE account_id=? ORDER BY symbol", (account_id,)).fetchall()
            )
        )

    def get_position_risk_plan(self, account_id: str, symbol: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    "SELECT * FROM position_risk_plans WHERE account_id=? AND symbol=?",
                    (account_id, symbol),
                ).fetchone()
            )
        )

    def start_account_trading_day(self, account_id: str, trade_date: date) -> bool:
        """Idempotently release T+1 quantities once per real trading date."""

        day = trade_date.isoformat()
        now = _iso(_now())

        def operation(connection):
            current = connection.execute(
                "SELECT trade_date FROM account_trading_days WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if current is not None and str(current["trade_date"]) == day:
                return False
            if current is not None and str(current["trade_date"]) > day:
                raise StateTransitionError("TRADING_DAY_REGRESSION")
            connection.execute(
                "UPDATE virtual_positions SET sellable_qty=total_qty,updated_at=? WHERE account_id=? AND sellable_qty<total_qty",
                (now, account_id),
            )
            connection.execute(
                """
                INSERT INTO account_trading_days(account_id,trade_date,updated_at) VALUES(?,?,?)
                ON CONFLICT(account_id) DO UPDATE SET trade_date=excluded.trade_date,updated_at=excluded.updated_at
                """,
                (account_id, day, now),
            )
            return True

        return bool(self._write(operation))

    def upsert_market_mark(self, account_id: str, symbol: str, price: float, bar_end: datetime) -> dict[str, Any]:
        if price <= 0:
            raise ValueError("mark price must be positive")
        stamp = _iso(bar_end)
        now = _iso(_now())

        def operation(connection):
            existing = connection.execute(
                "SELECT * FROM portfolio_marks WHERE account_id=? AND symbol=?",
                (account_id, symbol),
            ).fetchone()
            if existing is not None and str(existing["bar_end"]) > str(stamp):
                raise StateTransitionError("MARK_TIME_REGRESSION")
            connection.execute(
                """
                INSERT INTO portfolio_marks(account_id,symbol,price,bar_end,updated_at) VALUES(?,?,?,?,?)
                ON CONFLICT(account_id,symbol) DO UPDATE SET
                    price=excluded.price,bar_end=excluded.bar_end,updated_at=excluded.updated_at
                """,
                (account_id, symbol, float(price), stamp, now),
            )
            return _row_dict(
                connection.execute(
                    "SELECT * FROM portfolio_marks WHERE account_id=? AND symbol=?",
                    (account_id, symbol),
                ).fetchone()
            )

        return self._write(operation)

    def get_market_mark(self, account_id: str, symbol: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    "SELECT * FROM portfolio_marks WHERE account_id=? AND symbol=?",
                    (account_id, symbol),
                ).fetchone()
            )
        )

    def mark_account_to_market(self, account_id: str) -> float:
        now = _iso(_now())

        def operation(connection):
            account = connection.execute(
                "SELECT cash FROM virtual_accounts WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if account is None:
                raise StateTransitionError("ACCOUNT_NOT_FOUND")
            positions = connection.execute(
                "SELECT * FROM virtual_positions WHERE account_id=?",
                (account_id,),
            ).fetchall()
            equity = float(account["cash"])
            for position in positions:
                mark = connection.execute(
                    "SELECT price FROM portfolio_marks WHERE account_id=? AND symbol=?",
                    (account_id, position["symbol"]),
                ).fetchone()
                price = float(mark["price"]) if mark is not None else float(position["avg_cost"])
                equity += int(position["total_qty"]) * price
            connection.execute(
                "UPDATE virtual_accounts SET equity=?,updated_at=? WHERE account_id=?",
                (equity, now, account_id),
            )
            return equity

        return float(self._write(operation))

    def apply_corporate_action(
        self,
        *,
        account_id: str,
        symbol: str,
        action_id: str,
        quantity_factor: float,
        price_factor: float,
        effective_at: datetime,
    ) -> bool:
        """Idempotently adjust a simulated position for a verified action."""

        if quantity_factor <= 0 or price_factor <= 0:
            raise ValueError("corporate action factors must be positive")
        now = _iso(_now())

        def operation(connection):
            existing = connection.execute(
                "SELECT 1 FROM applied_corporate_actions WHERE account_id=? AND symbol=? AND action_id=?",
                (account_id, symbol, action_id),
            ).fetchone()
            if existing is not None:
                return False
            position = connection.execute(
                "SELECT * FROM virtual_positions WHERE account_id=? AND symbol=?",
                (account_id, symbol),
            ).fetchone()
            if position is None:
                raise StateTransitionError("POSITION_NOT_FOUND")
            total = int(round(int(position["total_qty"]) * quantity_factor))
            sellable = int(round(int(position["sellable_qty"]) * quantity_factor))
            if total <= 0 or sellable < 0 or sellable > total:
                raise StateTransitionError("CORPORATE_ACTION_QUANTITY_INVALID")
            connection.execute(
                """
                UPDATE virtual_positions SET total_qty=?,sellable_qty=?,avg_cost=?,stop_level=?,updated_at=?
                WHERE account_id=? AND symbol=?
                """,
                (
                    total,
                    sellable,
                    float(position["avg_cost"]) * price_factor,
                    float(position["stop_level"]) * price_factor if position["stop_level"] is not None else None,
                    now,
                    account_id,
                    symbol,
                ),
            )
            connection.execute(
                """
                UPDATE position_risk_plans SET entry_price=entry_price*?,stop_level=stop_level*?,
                    corporate_action_version=?,unresolved_corporate_action=0,updated_at=?
                WHERE account_id=? AND symbol=?
                """,
                (price_factor, price_factor, action_id, now, account_id, symbol),
            )
            connection.execute(
                "INSERT INTO applied_corporate_actions VALUES(?,?,?,?,?,?,?)",
                (
                    account_id,
                    symbol,
                    action_id,
                    float(quantity_factor),
                    float(price_factor),
                    _iso(effective_at),
                    now,
                ),
            )
            return True

        return bool(self._write(operation))

    def commit_fill(
        self,
        *,
        intent_id: str,
        intent_key: str,
        account_id: str,
        signal_id: str,
        symbol: str,
        action: str,
        qty: int,
        price: float,
        fee: float,
        bar_end: datetime,
        cash_after: float,
        position: Mapping[str, Any] | None,
        equity_after: float | None = None,
        stop_level: float | None = None,
        plan_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        if qty <= 0 or price <= 0 or fee < 0 or cash_after < 0:
            raise ValueError("invalid fill accounting values")
        if position is not None:
            required = {"total_qty", "sellable_qty", "avg_cost"}
            if not required.issubset(position):
                raise ValueError("position fields are incomplete")
            if int(position["total_qty"]) < 0 or int(position["sellable_qty"]) < 0:
                raise ValueError("position quantity cannot be negative")
            if int(position["sellable_qty"]) > int(position["total_qty"]):
                raise ValueError("sellable quantity cannot exceed total quantity")
        now = _iso(_now())
        bar_stamp = _iso(bar_end)

        def operation(connection):
            account = connection.execute("SELECT * FROM virtual_accounts WHERE account_id=?", (account_id,)).fetchone()
            if account is None:
                raise StateTransitionError("ACCOUNT_NOT_FOUND")
            existing_intent = connection.execute("SELECT * FROM simulation_intents WHERE intent_key=?", (intent_key,)).fetchone()
            if existing_intent is not None:
                existing_fill = connection.execute(
                    "SELECT * FROM virtual_fills WHERE intent_id=? ORDER BY fill_sequence LIMIT 1",
                    (existing_intent["intent_id"],),
                ).fetchone()
                if existing_fill is None:
                    raise StateTransitionError("INTENT_ALREADY_RESERVED")
                return _row_dict(existing_fill), False
            connection.execute(
                "INSERT INTO simulation_intents(intent_id,intent_key,account_id,signal_id,symbol,action,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (intent_id, intent_key, account_id, signal_id, symbol, action, "PENDING", now, now),
            )
            current = connection.execute(
                "SELECT COALESCE(MAX(fill_sequence),0) AS sequence FROM virtual_fills WHERE intent_id=?", (intent_id,)
            ).fetchone()["sequence"]
            fill_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-fill:{intent_key}:1"))
            connection.execute(
                """
                INSERT INTO virtual_fills(
                    fill_id,intent_id,fill_sequence,account_id,signal_id,symbol,action,qty,price,fee,bar_end,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (fill_id, intent_id, int(current) + 1, account_id, signal_id, symbol, action, int(qty), float(price), float(fee), bar_stamp, now),
            )
            connection.execute(
                "UPDATE simulation_intents SET status='FILLED',updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            connection.execute(
                "UPDATE virtual_accounts SET cash=?,equity=?,updated_at=? WHERE account_id=?",
                (float(cash_after), float(equity_after if equity_after is not None else cash_after), now, account_id),
            )
            if position is None or int(position["total_qty"]) == 0:
                connection.execute("DELETE FROM virtual_positions WHERE account_id=? AND symbol=?", (account_id, symbol))
                connection.execute(
                    "UPDATE position_risk_plans SET status='CLOSED',updated_at=? WHERE account_id=? AND symbol=?",
                    (now, account_id, symbol),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO virtual_positions(account_id,symbol,total_qty,sellable_qty,avg_cost,stop_level,plan_id,updated_at)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(account_id,symbol) DO UPDATE SET
                      total_qty=excluded.total_qty,sellable_qty=excluded.sellable_qty,avg_cost=excluded.avg_cost,
                      stop_level=excluded.stop_level,plan_id=excluded.plan_id,updated_at=excluded.updated_at
                    """,
                    (
                        account_id,
                        symbol,
                        int(position["total_qty"]),
                        int(position["sellable_qty"]),
                        float(position["avg_cost"]),
                        stop_level if stop_level is not None else position.get("stop_level"),
                        plan_id if plan_id is not None else position.get("plan_id"),
                        now,
                    ),
                )
                existing_risk = connection.execute(
                    "SELECT * FROM position_risk_plans WHERE account_id=? AND symbol=?",
                    (account_id, symbol),
                ).fetchone()
                if action in {"BUY", "ADD"}:
                    effective_stop = stop_level if stop_level is not None else position.get("stop_level")
                    if effective_stop is None:
                        raise StateTransitionError("POSITION_RISK_STOP_REQUIRED")
                    adds_used = int(existing_risk["adds_used"]) + 1 if action == "ADD" and existing_risk else 0
                    connection.execute(
                        """
                        INSERT INTO position_risk_plans(
                            account_id,symbol,source_plan_id,status,entry_price,stop_level,max_adds,adds_used,
                            corporate_action_version,unresolved_corporate_action,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(account_id,symbol) DO UPDATE SET
                            source_plan_id=excluded.source_plan_id,status='ACTIVE',entry_price=excluded.entry_price,
                            stop_level=excluded.stop_level,adds_used=excluded.adds_used,updated_at=excluded.updated_at
                        """,
                        (
                            account_id,
                            symbol,
                            plan_id,
                            "ACTIVE",
                            float(position["avg_cost"]),
                            float(effective_stop),
                            1,
                            adds_used,
                            None,
                            0,
                            now,
                            now,
                        ),
                    )
            return _row_dict(connection.execute("SELECT * FROM virtual_fills WHERE fill_id=?", (fill_id,)).fetchone()), True

        return self._write(operation)

    def release_t1(self, account_id: str) -> int:
        """Release all quantities bought on prior sessions for a new day."""

        now = _iso(_now())

        def operation(connection):
            cursor = connection.execute(
                "UPDATE virtual_positions SET sellable_qty=total_qty,updated_at=? WHERE account_id=? AND sellable_qty<total_qty",
                (now, account_id),
            )
            return int(cursor.rowcount)

        return int(self._write(operation))

    def get_fill_by_intent_key(self, intent_key: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    "SELECT f.* FROM virtual_fills f JOIN simulation_intents i ON i.intent_id=f.intent_id WHERE i.intent_key=? ORDER BY f.fill_sequence LIMIT 1",
                    (intent_key,),
                ).fetchone()
            )
        )

    def list_fills(self, account_id: str | None = None) -> tuple[dict[str, Any], ...]:
        def operation(connection):
            if account_id is None:
                rows = connection.execute("SELECT * FROM virtual_fills ORDER BY bar_end,fill_id").fetchall()
            else:
                rows = connection.execute("SELECT * FROM virtual_fills WHERE account_id=? ORDER BY bar_end,fill_id", (account_id,)).fetchall()
            return tuple(_row_dict(row) for row in rows)

        return self._read(operation)

    # ------------------------------------------------------------------
    # Scheduler leases
    # ------------------------------------------------------------------
    def acquire_lease(
        self,
        lease_name: str,
        owner: str,
        *,
        now: datetime | None = None,
        ttl_seconds: float = 90.0,
        dispatch_key: str | None = None,
    ) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("lease ttl must be positive")
        now = now or _now()
        now_text = _iso(now)
        expires_text = _iso(now + timedelta(seconds=ttl_seconds))

        def operation(connection):
            existing = connection.execute("SELECT * FROM scheduler_leases WHERE lease_name=?", (lease_name,)).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO scheduler_leases(lease_name,owner,acquired_at,heartbeat_at,expires_at,generation,last_dispatch_key,state,completed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (lease_name, owner, now_text, now_text, expires_text, 1, dispatch_key, "ACTIVE", None),
                )
                return True
            same_dispatch = dispatch_key is not None and existing["last_dispatch_key"] == dispatch_key
            if same_dispatch and existing["state"] == "COMPLETED":
                return False
            active = existing["expires_at"] > now_text
            if active and existing["state"] == "ACTIVE":
                return False
            generation = int(existing["generation"]) + 1
            connection.execute(
                "UPDATE scheduler_leases SET owner=?,acquired_at=?,heartbeat_at=?,expires_at=?,generation=?,last_dispatch_key=?,state='ACTIVE',completed_at=NULL WHERE lease_name=?",
                (owner, now_text, now_text, expires_text, generation, dispatch_key, lease_name),
            )
            return True

        return bool(self._write(operation))

    def heartbeat_lease(self, lease_name: str, owner: str, *, now: datetime | None = None, ttl_seconds: float = 90.0) -> bool:
        now = now or _now()
        now_text = _iso(now)
        expires_text = _iso(now + timedelta(seconds=ttl_seconds))

        def operation(connection):
            cursor = connection.execute(
                "UPDATE scheduler_leases SET heartbeat_at=?,expires_at=? WHERE lease_name=? AND owner=? AND expires_at>?",
                (now_text, expires_text, lease_name, owner, now_text),
            )
            return cursor.rowcount == 1

        return bool(self._write(operation))

    def release_lease(self, lease_name: str, owner: str) -> bool:
        def operation(connection):
            cursor = connection.execute("DELETE FROM scheduler_leases WHERE lease_name=? AND owner=?", (lease_name, owner))
            return cursor.rowcount == 1

        return bool(self._write(operation))

    def complete_lease(
        self,
        lease_name: str,
        owner: str,
        *,
        dispatch_key: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        stamp = _iso(now or _now())

        def operation(connection):
            clauses = "lease_name=? AND owner=? AND state='ACTIVE'"
            args: list[Any] = [stamp, stamp, lease_name, owner]
            if dispatch_key is not None:
                clauses += " AND last_dispatch_key=?"
                args.append(dispatch_key)
            cursor = connection.execute(
                f"UPDATE scheduler_leases SET state='COMPLETED',completed_at=?,expires_at=? WHERE {clauses}",
                args,
            )
            return cursor.rowcount == 1

        return bool(self._write(operation))

    def get_lease(self, lease_name: str) -> dict[str, Any] | None:
        return self._read(lambda connection: _row_dict(connection.execute("SELECT * FROM scheduler_leases WHERE lease_name=?", (lease_name,)).fetchone()))

    def list_leases(self) -> tuple[dict[str, Any], ...]:
        return self._read(
            lambda connection: tuple(
                _row_dict(row)
                for row in connection.execute(
                    "SELECT * FROM scheduler_leases ORDER BY lease_name"
                ).fetchall()
            )
        )

    def effective_events(self, *, lane_id: str | None = None) -> tuple[dict[str, Any], ...]:
        return self.list_monitor_events(lane_id=lane_id, effective_only=True)

    def close(self) -> None:
        """Compatibility no-op; connections are scoped to each operation."""

    def __enter__(self) -> "RuntimeStore":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()


__all__ = [
    "EFFECTIVE_ACTIONS",
    "MonitorAction",
    "PersistenceBlockedError",
    "PersistenceError",
    "PlanStatus",
    "RuntimeStateError",
    "RuntimeStore",
    "StateTransitionError",
]
