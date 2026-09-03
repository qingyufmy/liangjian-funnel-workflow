"""Durable runtime state for research lanes, A4 and paper simulation.

This module is intentionally self-contained.  SQLite is the source of truth
for state transitions and idempotency; callers never need to use a raw
connection.  All writes use ``BEGIN IMMEDIATE`` with WAL/FULL durability.
No table in this module contains model reasoning text.
"""

from __future__ import annotations

import json
import math
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


class OutcomeLabelConflictError(StateTransitionError):
    """An outcome label tried to change an immutable decision fact.

    Forward-performance fields are append-only measurements, but the
    decision identity, source hashes and decision itself are immutable.  A
    separate public exception makes that boundary visible to offline
    evaluators without weakening the normal state conflict contract.
    """

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


class A4SignalStatus(StrEnum):
    """Durable lifecycle of one A4 entry signal.

    This is deliberately separate from :class:`MonitorAction`: an event is
    an immutable observation, while this status describes the paper-trade
    lifecycle that may span many events and fills.
    """

    SIGNALLED = "SIGNALLED"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSED = "CLOSED"
    UNFILLED = "UNFILLED"
    CANCELLED = "CANCELLED"
    INVALIDATED = "INVALIDATED"
    DATA_ERROR = "DATA_ERROR"


A4_SIGNAL_TERMINAL_STATUSES = frozenset(
    {
        A4SignalStatus.CLOSED.value,
        A4SignalStatus.UNFILLED.value,
        A4SignalStatus.CANCELLED.value,
        A4SignalStatus.INVALIDATED.value,
        A4SignalStatus.DATA_ERROR.value,
    }
)

# Transitions are intentionally conservative.  Replaying an already reached
# state is handled as an idempotent no-op, but a terminal state can never be
# reopened or changed to another terminal state.
A4_SIGNAL_TRANSITIONS: dict[str, frozenset[str]] = {
    A4SignalStatus.SIGNALLED.value: frozenset(
        {
            A4SignalStatus.OPEN.value,
            A4SignalStatus.EXIT_PENDING.value,
            A4SignalStatus.UNFILLED.value,
            A4SignalStatus.CANCELLED.value,
            A4SignalStatus.INVALIDATED.value,
            A4SignalStatus.DATA_ERROR.value,
        }
    ),
    A4SignalStatus.OPEN.value: frozenset(
        {
            A4SignalStatus.EXIT_PENDING.value,
            A4SignalStatus.PARTIALLY_CLOSED.value,
            A4SignalStatus.CLOSED.value,
            A4SignalStatus.DATA_ERROR.value,
            A4SignalStatus.INVALIDATED.value,
        }
    ),
    A4SignalStatus.EXIT_PENDING.value: frozenset(
        {
            A4SignalStatus.OPEN.value,
            A4SignalStatus.PARTIALLY_CLOSED.value,
            A4SignalStatus.CLOSED.value,
            A4SignalStatus.DATA_ERROR.value,
            A4SignalStatus.INVALIDATED.value,
        }
    ),
    A4SignalStatus.PARTIALLY_CLOSED.value: frozenset(
        {
            A4SignalStatus.EXIT_PENDING.value,
            A4SignalStatus.PARTIALLY_CLOSED.value,
            A4SignalStatus.CLOSED.value,
            A4SignalStatus.DATA_ERROR.value,
        }
    ),
}


class ResearchStageStatus(StrEnum):
    """Detailed outcomes for a research stage.

    ``contracts.StageStatus`` predates the deterministic funnel and only has
    coarse outcomes.  These additive values are kept as strings in SQLite so
    old control planes can display them without rewriting historical rows.
    """

    VALIDATED_NO_OPPORTUNITY = "VALIDATED_NO_OPPORTUNITY"
    VALIDATED_NO_ACTION = "VALIDATED_NO_ACTION"
    DEGRADED_UNDERFILLED_DATA_GAP = "DEGRADED_UNDERFILLED_DATA_GAP"
    VALIDATED_UNDERFILLED_MARKET = "VALIDATED_UNDERFILLED_MARKET"
    VALIDATED_NO_SETUP = "VALIDATED_NO_SETUP"
    NOT_RUN_UPSTREAM_BLOCKED = "NOT_RUN_UPSTREAM_BLOCKED"
    BLOCKED_DATA_COVERAGE = "BLOCKED_DATA_COVERAGE"
    BLOCKED_EVIDENCE_GAP = "BLOCKED_EVIDENCE_GAP"
    BLOCKED_MODEL = "BLOCKED_MODEL"
    BLOCKED_TECHNICAL_DATA = "BLOCKED_TECHNICAL_DATA"


STAGE_STATUS_VALUES = frozenset(
    {
        *(item.value for item in StageStatus),
        *(item.value for item in ResearchStageStatus),
    }
)

# These outcomes completed their own stage.  A data-gap result is allowed to
# continue to a technical audit, but callers must keep it ineligible for
# publication/simulation.
STAGE_COMPLETED_VALUES = frozenset(
    {
        StageStatus.VALIDATED.value,
        ResearchStageStatus.VALIDATED_NO_OPPORTUNITY.value,
        ResearchStageStatus.VALIDATED_NO_ACTION.value,
        ResearchStageStatus.DEGRADED_UNDERFILLED_DATA_GAP.value,
        ResearchStageStatus.VALIDATED_UNDERFILLED_MARKET.value,
        ResearchStageStatus.VALIDATED_NO_SETUP.value,
    }
)


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

# Lark card header templates accepted by the interactive-card webhook.  The
# sequence is intentionally stable: notification colors are derived from a
# durable SQLite sequence rather than process memory, so a restart cannot
# accidentally reuse the previous card color.
NOTIFICATION_CARD_COLORS: tuple[str, ...] = (
    "blue",
    "wathet",
    "turquoise",
    "green",
    "yellow",
    "orange",
    "red",
    "carmine",
    "violet",
    "purple",
    "indigo",
    "grey",
)

NOTIFICATION_STATUSES = frozenset({"SENT", "FAILED"})

OUTCOME_STAGES = frozenset({"G0", "A1", "A2", "A3", "A4"})
OUTCOME_DECISIONS = frozenset({"PASSED", "REJECTED", "NOT_SENT_TO_LLM"})
OUTCOME_SELECTION_BASES = frozenset(
    {
        "LLM_REVIEWED",
        "DETERMINISTIC_SCORE",
        "QUOTA_FILL",
        "BROKER_GOLD_DIRECT",
        "FUNDAMENTAL_BASELINE",
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
    StageStatus.PENDING.value: frozenset(
        {
            StageStatus.RUNNING.value,
            StageStatus.BLOCKED.value,
            StageStatus.EXPIRED.value,
            ResearchStageStatus.NOT_RUN_UPSTREAM_BLOCKED.value,
        }
    ),
    StageStatus.RUNNING.value: frozenset(
        {
            StageStatus.VALIDATED.value,
            StageStatus.BLOCKED.value,
            StageStatus.FAILED.value,
            StageStatus.EXPIRED.value,
            ResearchStageStatus.VALIDATED_NO_OPPORTUNITY.value,
            ResearchStageStatus.VALIDATED_NO_ACTION.value,
            ResearchStageStatus.DEGRADED_UNDERFILLED_DATA_GAP.value,
            ResearchStageStatus.VALIDATED_UNDERFILLED_MARKET.value,
            ResearchStageStatus.VALIDATED_NO_SETUP.value,
            ResearchStageStatus.NOT_RUN_UPSTREAM_BLOCKED.value,
            ResearchStageStatus.BLOCKED_DATA_COVERAGE.value,
            ResearchStageStatus.BLOCKED_EVIDENCE_GAP.value,
            ResearchStageStatus.BLOCKED_MODEL.value,
            ResearchStageStatus.BLOCKED_TECHNICAL_DATA.value,
        }
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


def _notify_stamp(value: datetime | str | None = None) -> str:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("notification timestamp must be ISO-8601") from None
    if value is not None and (not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None):
        raise ValueError("notification timestamp must be timezone-aware")
    return _iso(value or _now()) or ""


def _notify_text(value: str, field: str, limit: int = 512) -> str:
    text = value.strip() if isinstance(value, str) else ""
    if not text or len(text) > limit:
        raise ValueError(f"notification {field} invalid")
    return text


def _notify_payload(value: Mapping[str, Any] | None) -> str:
    if value is not None and not isinstance(value, Mapping):
        raise ValueError("notification payload invalid")
    forbidden = {"analysis", "api_key", "kline", "model_output", "prompt", "raw", "response", "secret", "token", "webhook"}
    if value is not None and any(
        marker in str(key).lower()
        for key in value
        for marker in forbidden
    ):
        raise ValueError("unsafe notification payload")
    result = _json(value)
    if len(result.encode("utf-8")) > 32 * 1024:
        raise ValueError("notification payload too large")
    return result


def _notify_reason(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip().upper()
    return text if text and len(text) <= 64 and all(char.isalnum() or char == "_" for char in text) else "LARK_DELIVERY_FAILED"


def _row_dict(row: sqlite3.Row | tuple[Any, ...] | None, columns: tuple[str, ...] | None = None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    if columns is None:
        return dict(enumerate(row))
    return dict(zip(columns, row))


def _a4_mapping(value: Any) -> dict[str, Any]:
    """Decode a structural A4 payload without retaining model prose."""

    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _a4_first(mapping_values: Sequence[Mapping[str, Any]], *keys: str) -> Any:
    for mapping in mapping_values:
        for key in keys:
            value = mapping.get(key)
            if value is not None and value != "":
                return value
    return None


def _a4_number(value: Any, *, allow_none: bool = True, positive: bool = False) -> float | None:
    if value is None or value == "":
        if allow_none:
            return None
        raise ValueError("A4 numeric value required")
    if isinstance(value, bool):
        raise ValueError("A4 numeric value invalid")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("A4 numeric value invalid") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise ValueError("A4 numeric value invalid")
    return result


def _a4_nonnegative_int(value: Any, *, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ValueError("A4 quantity invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("A4 quantity invalid") from exc
    if result < 0:
        raise ValueError("A4 quantity invalid")
    return result


def _a4_bar_stamp(value: Any) -> str:
    if isinstance(value, datetime):
        return _iso(value) or ""
    stamp = str(value or "").strip()
    if not stamp:
        raise ValueError("A4 timestamp required")
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise ValueError("A4 timestamp invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("A4 timestamp must be timezone-aware")
    return _iso(parsed) or ""


def _a4_json_list(value: Any) -> list[str]:
    parsed = _a4_mapping(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        try:
            parsed = json.loads(str(value or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = []
    return [str(item) for item in parsed if str(item)]


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, declaration: str) -> None:
    existing = {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


def _migrate_outcome_label_identity(connection: sqlite3.Connection) -> None:
    """Migrate an early date-only outcome table without losing rows.

    The first draft of T0 used ``(trade_date, stage, symbol)`` as its unique
    key.  That cannot represent same-day reruns or comparison lanes.  If such
    a table is encountered, preserve every row under a stable legacy run/lane
    identity before the new composite index is created.  Fresh databases do
    not enter this branch.
    """

    table_info = connection.execute("PRAGMA table_info(astock_outcome_labels)").fetchall()
    columns = {str(row[1]) for row in table_info}
    if not columns:
        return

    # The first draft used a date-only unique key.  A later draft added the
    # identity columns but could still have the old table-level constraint.
    # Detect both forms before the indexes are created below; otherwise an old
    # database would either fail on the new index (missing columns) or keep
    # rejecting same-day reruns (stale UNIQUE(trade_date, stage, symbol)).
    has_bad_unique = False
    for index in connection.execute("PRAGMA index_list(astock_outcome_labels)").fetchall():
        index_name = str(index[1])
        is_unique = bool(index[2])
        if not is_unique:
            continue
        index_columns = tuple(
            str(item[2])
            for item in connection.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
        if set(index_columns) == {"trade_date", "stage", "symbol"} and len(index_columns) == 3:
            has_bad_unique = True
            break
    if {"run_id", "lane_id"}.issubset(columns) and not has_bad_unique:
        return

    # Keep any prior migration backup instead of silently deleting it.  This
    # matters when an interrupted/manual migration left a table with that
    # name; source rows remain inspectable and the renamed table cannot block
    # the new composite identity.
    legacy_table = "astock_outcome_labels_legacy_migration"
    suffix = 1
    existing_tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    while legacy_table in existing_tables:
        suffix += 1
        legacy_table = f"astock_outcome_labels_legacy_migration_{suffix}"
    connection.execute(f"ALTER TABLE astock_outcome_labels RENAME TO {legacy_table}")
    connection.executescript(
        """
        CREATE TABLE astock_outcome_labels (
            label_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            lane_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            stage TEXT NOT NULL CHECK(stage IN ('G0','A1','A2','A3','A4')),
            symbol TEXT NOT NULL,
            decision TEXT NOT NULL CHECK(decision IN ('PASSED','REJECTED','NOT_SENT_TO_LLM')),
            reason_codes TEXT NOT NULL DEFAULT '[]',
            selection_basis TEXT,
            score REAL,
            snapshot_id TEXT NOT NULL,
            config_hash TEXT NOT NULL,
            fwd_return_1d REAL,
            fwd_return_3d REAL,
            fwd_return_5d REAL,
            fwd_return_10d REAL,
            mfe_5d REAL,
            mae_5d REAL,
            benchmark_return_5d REAL,
            excess_return_5d REAL,
            labeled_at TEXT,
            baseline_status TEXT NOT NULL DEFAULT 'PENDING',
            baseline_sample_size INTEGER,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(run_id, lane_id, stage, symbol)
        )
        """
    )
    rows = connection.execute(f"SELECT rowid,* FROM {legacy_table} ORDER BY rowid").fetchall()
    copied_identities: set[tuple[str, str, str, str]] = set()
    for row in rows:
        data = {key: row[key] for key in row.keys()}
        label_id = str(data.get("label_id") or "").strip()
        if not label_id:
            label_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"liangjian-outcome:legacy:{data.get('rowid')}",
            ).hex
        snapshot_id = str(data.get("snapshot_id") or "legacy").strip() or "legacy"
        run_id = str(data.get("run_id") or f"legacy:{snapshot_id}").strip()
        lane_id = str(data.get("lane_id") or f"legacy:{data.get('rowid')}").strip()
        stage = str(data.get("stage") or "").strip().upper()
        decision = str(data.get("decision") or "").strip().upper()
        if stage not in OUTCOME_STAGES or decision not in OUTCOME_DECISIONS:
            # The old draft had no checks.  Invalid legacy rows remain
            # inspectable in the migration backup table and are excluded from
            # the typed ledger rather than blocking initialization.
            continue
        identity = (run_id, lane_id, stage, str(data.get("symbol") or "").upper())
        if identity in copied_identities:
            # A partially migrated table may have populated run/lane columns
            # with the same default for every legacy row.  Preserve all rows
            # by assigning only the colliding lane a stable row-id suffix.
            lane_id = f"{lane_id}:legacy:{data.get('rowid')}"
            identity = (run_id, lane_id, stage, identity[3])
            while identity in copied_identities:
                lane_id = f"{lane_id}:x"
                identity = (run_id, lane_id, stage, identity[3])
        copied_identities.add(identity)
        connection.execute(
            """
            INSERT INTO astock_outcome_labels(
                label_id,run_id,lane_id,trade_date,stage,symbol,decision,reason_codes,
                selection_basis,score,snapshot_id,config_hash,fwd_return_1d,fwd_return_3d,
                fwd_return_5d,fwd_return_10d,mfe_5d,mae_5d,benchmark_return_5d,
                excess_return_5d,labeled_at,baseline_status,baseline_sample_size,metadata_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                label_id,
                run_id,
                lane_id,
                str(data.get("trade_date") or ""),
                stage,
                str(data.get("symbol") or "").upper(),
                decision,
                str(data.get("reason_codes") or "[]"),
                data.get("selection_basis"),
                data.get("score"),
                snapshot_id,
                str(data.get("config_hash") or "legacy"),
                data.get("fwd_return_1d"),
                data.get("fwd_return_3d"),
                data.get("fwd_return_5d"),
                data.get("fwd_return_10d"),
                data.get("mfe_5d"),
                data.get("mae_5d"),
                data.get("benchmark_return_5d"),
                data.get("excess_return_5d"),
                data.get("labeled_at"),
                data.get("baseline_status") or "PENDING",
                data.get("baseline_sample_size"),
                data.get("metadata_json") or "{}",
            ),
        )
    # Deliberately retain the renamed source table as a migration archive.
    # It is outside the live table/index namespace and can be removed by a
    # separately reviewed storage-retention operation after verification.


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
                    CREATE TABLE IF NOT EXISTS a4_signal_lifecycles (
                        lifecycle_id TEXT PRIMARY KEY,
                        signal_id TEXT NOT NULL UNIQUE,
                        entry_event_key TEXT NOT NULL UNIQUE,
                        entry_event_id TEXT NOT NULL UNIQUE,
                        plan_id TEXT NOT NULL,
                        lane_id TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        source_run_id TEXT NOT NULL,
                        name TEXT NOT NULL DEFAULT '',
                        stock_behavior_type TEXT NOT NULL,
                        strategy_profile TEXT NOT NULL,
                        strategy_snapshot_json TEXT NOT NULL DEFAULT '{}',
                        plan_snapshot_json TEXT NOT NULL DEFAULT '{}',
                        status TEXT NOT NULL CHECK(status IN (
                            'SIGNALLED','OPEN','EXIT_PENDING','PARTIALLY_CLOSED','CLOSED',
                            'UNFILLED','CANCELLED','INVALIDATED','DATA_ERROR'
                        )),
                        signal_time TEXT NOT NULL,
                        signal_price REAL,
                        entry_time TEXT,
                        entry_price REAL,
                        entry_qty INTEGER NOT NULL DEFAULT 0 CHECK(entry_qty >= 0),
                        exit_signal_time TEXT,
                        exit_signal_price REAL,
                        exit_signal_qty INTEGER NOT NULL DEFAULT 0 CHECK(exit_signal_qty >= 0),
                        exit_time TEXT,
                        exit_price REAL,
                        exit_qty INTEGER NOT NULL DEFAULT 0 CHECK(exit_qty >= 0),
                        remaining_qty INTEGER NOT NULL DEFAULT 0 CHECK(remaining_qty >= 0),
                        entry_fee REAL NOT NULL DEFAULT 0 CHECK(entry_fee >= 0),
                        exit_fee REAL NOT NULL DEFAULT 0 CHECK(exit_fee >= 0),
                        total_fee REAL NOT NULL DEFAULT 0 CHECK(total_fee >= 0),
                        gross_pnl REAL,
                        net_pnl REAL,
                        realized_pnl REAL,
                        gross_return REAL,
                        net_return REAL,
                        max_price REAL,
                        min_price REAL,
                        mfe REAL,
                        mae REAL,
                        holding_minutes INTEGER,
                        last_bar_end TEXT,
                        last_close REAL,
                        exit_reason TEXT,
                        last_fill_key TEXT,
                        applied_fill_keys_json TEXT NOT NULL DEFAULT '[]',
                        applied_exit_event_keys_json TEXT NOT NULL DEFAULT '[]',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS a5_daily_reviews (
                        review_id TEXT PRIMARY KEY,
                        review_key TEXT NOT NULL UNIQUE,
                        trade_date TEXT NOT NULL,
                        review_kind TEXT NOT NULL CHECK(review_kind IN ('MIDDAY','POST_CLOSE')),
                        cutoff_at TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('COMPLETED','DEGRADED')),
                        model TEXT NOT NULL,
                        source_run_ids_json TEXT NOT NULL DEFAULT '[]',
                        input_hash TEXT NOT NULL,
                        prompt_hash TEXT NOT NULL,
                        output_hash TEXT NOT NULL,
                        latency_ms INTEGER NOT NULL DEFAULT 0 CHECK(latency_ms >= 0),
                        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                        thinking_variant TEXT,
                        fact_snapshot_json TEXT NOT NULL,
                        report_json TEXT NOT NULL,
                        markdown_path TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        UNIQUE(trade_date, review_kind, input_hash)
                    );
                    CREATE INDEX IF NOT EXISTS idx_a4_signal_lifecycles_account_symbol
                        ON a4_signal_lifecycles(account_id, symbol, status, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_a4_signal_lifecycles_plan
                        ON a4_signal_lifecycles(plan_id, trade_date, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_a4_signal_lifecycles_status
                        ON a4_signal_lifecycles(status, updated_at DESC);
                    CREATE TABLE IF NOT EXISTS notification_deliveries (
                        delivery_id TEXT PRIMARY KEY,
                        delivery_key TEXT NOT NULL UNIQUE,
                        kind TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('SENT','FAILED')),
                        title TEXT NOT NULL,
                        color TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 1 CHECK(attempt_count >= 0),
                        last_reason_code TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        sent_at TEXT,
                        payload_json TEXT NOT NULL DEFAULT '{}'
                    );
                    CREATE INDEX IF NOT EXISTS idx_notification_deliveries_created
                        ON notification_deliveries(created_at DESC, delivery_id DESC);
                    CREATE INDEX IF NOT EXISTS idx_notification_deliveries_status
                        ON notification_deliveries(status, updated_at DESC);
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
                    CREATE TABLE IF NOT EXISTS astock_outcome_labels (
                        label_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        lane_id TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        stage TEXT NOT NULL CHECK(stage IN ('G0','A1','A2','A3','A4')),
                        symbol TEXT NOT NULL,
                        decision TEXT NOT NULL CHECK(decision IN ('PASSED','REJECTED','NOT_SENT_TO_LLM')),
                        reason_codes TEXT NOT NULL DEFAULT '[]',
                        selection_basis TEXT,
                        score REAL,
                        snapshot_id TEXT NOT NULL,
                        config_hash TEXT NOT NULL,
                        fwd_return_1d REAL,
                        fwd_return_3d REAL,
                        fwd_return_5d REAL,
                        fwd_return_10d REAL,
                        mfe_5d REAL,
                        mae_5d REAL,
                        benchmark_return_5d REAL,
                        excess_return_5d REAL,
                        labeled_at TEXT,
                        baseline_status TEXT NOT NULL DEFAULT 'PENDING',
                        baseline_sample_size INTEGER,
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        UNIQUE(run_id, lane_id, stage, symbol)
                    );
                    """
                )
                _migrate_outcome_label_identity(connection)
                _ensure_column(connection, "scheduler_leases", "state", "TEXT NOT NULL DEFAULT 'ACTIVE'")
                _ensure_column(connection, "scheduler_leases", "completed_at", "TEXT")
                _ensure_column(connection, "workflow_runs", "outcome_json", "TEXT NOT NULL DEFAULT '{}'")
                _ensure_column(connection, "workflow_stages", "outcome_json", "TEXT NOT NULL DEFAULT '{}'")
                _ensure_column(connection, "astock_outcome_labels", "run_id", "TEXT NOT NULL DEFAULT ''")
                _ensure_column(connection, "astock_outcome_labels", "lane_id", "TEXT NOT NULL DEFAULT ''")
                _ensure_column(connection, "astock_outcome_labels", "baseline_status", "TEXT NOT NULL DEFAULT 'PENDING'")
                _ensure_column(connection, "astock_outcome_labels", "baseline_sample_size", "INTEGER")
                _ensure_column(connection, "astock_outcome_labels", "metadata_json", "TEXT NOT NULL DEFAULT '{}'")
                # Create secondary indexes only after migrations have added
                # the identity columns.  This keeps first-open upgrades from
                # failing on the original date-only table.
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_astock_outcome_labels_stage_date_v2 "
                    "ON astock_outcome_labels(stage, trade_date, decision, run_id, lane_id)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_astock_outcome_labels_unlabeled_v2 "
                    "ON astock_outcome_labels(labeled_at, trade_date)"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_astock_outcome_labels_identity_v2 "
                    "ON astock_outcome_labels(run_id, lane_id, stage, symbol)"
                )
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
        if status not in STAGE_STATUS_VALUES:
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
                if row is None or row["status"] not in STAGE_COMPLETED_VALUES:
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
        outcome: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _iso(_now())
        reasons = json.dumps(list(dict.fromkeys(str(item) for item in reason_codes)), ensure_ascii=False)
        outcome_json = _json(outcome)

        def operation(connection):
            connection.execute(
                """
                INSERT INTO workflow_runs(
                    run_id,lane_id,trade_date,slot,model,status,snapshot_hash,prompt_hash,config_hash,
                    reason_codes_json,outcome_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id,lane_id) DO UPDATE SET
                    status=excluded.status,prompt_hash=COALESCE(excluded.prompt_hash,workflow_runs.prompt_hash),
                    config_hash=COALESCE(excluded.config_hash,workflow_runs.config_hash),
                    reason_codes_json=excluded.reason_codes_json,outcome_json=excluded.outcome_json,
                    updated_at=excluded.updated_at
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
                    outcome_json,
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
        outcome: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in {"A1", "A2", "A3"}:
            raise ValueError("invalid workflow stage")
        now = _iso(_now())
        reasons = json.dumps(list(dict.fromkeys(str(item) for item in reason_codes)), ensure_ascii=False)
        outcome_json = _json(outcome)

        def operation(connection):
            connection.execute(
                """
                INSERT INTO workflow_stages(run_id,lane_id,stage,status,reason_codes_json,outcome_json,updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(run_id,lane_id,stage) DO UPDATE SET
                    status=excluded.status,reason_codes_json=excluded.reason_codes_json,
                    outcome_json=excluded.outcome_json,updated_at=excluded.updated_at
                """,
                (run_id, lane_id, stage, status, reasons, outcome_json, now),
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
    # Deterministic outcome-label ledger
    # ------------------------------------------------------------------
    def record_outcome_labels(self, labels: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
        """Insert immutable stage decisions, idempotently.

        The natural identity is ``(run_id, lane_id, stage, symbol)``.  Replaying
        the same decision is a no-op; attempting to change any decision fact
        (including its source hashes) raises ``OUTCOME_LABEL_IMMUTABLE_CONFLICT``.
        Performance measurements are deliberately not accepted on this path;
        they are appended later by :meth:`update_outcome_label_metrics`.
        """

        if isinstance(labels, (str, bytes, bytearray)):
            raise TypeError("outcome labels must be a sequence of mappings")
        prepared: list[dict[str, Any]] = []
        for raw in labels:
            if not isinstance(raw, Mapping):
                raise TypeError("outcome label must be a mapping")
            run_id = str(raw.get("run_id") or raw.get("decision_run_id") or "default").strip()
            lane_id = str(raw.get("lane_id") or "default").strip()
            if not run_id or not lane_id:
                raise ValueError("outcome label run_id and lane_id must not be empty")
            trade_date = str(raw.get("trade_date") or "").strip()
            try:
                date.fromisoformat(trade_date)
            except ValueError as exc:
                raise ValueError("outcome label trade_date must be YYYY-MM-DD") from exc
            stage = str(raw.get("stage") or "").strip().upper()
            if stage not in OUTCOME_STAGES:
                raise ValueError("invalid outcome label stage")
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                raise ValueError("outcome label symbol must not be empty")
            decision = str(raw.get("decision") or "").strip().upper()
            if decision not in OUTCOME_DECISIONS:
                raise ValueError("invalid outcome label decision")
            snapshot_id = str(raw.get("snapshot_id") or "").strip()
            config_hash = str(raw.get("config_hash") or "").strip()
            if not snapshot_id or not config_hash:
                raise ValueError("outcome label snapshot_id and config_hash are required")
            selection_basis = raw.get("selection_basis")
            if selection_basis is not None:
                selection_basis = str(selection_basis).strip().upper() or None
                if selection_basis not in OUTCOME_SELECTION_BASES:
                    raise ValueError("invalid outcome label selection_basis")
            score = raw.get("score")
            if score is not None:
                if isinstance(score, bool):
                    raise ValueError("outcome label score must be finite")
                try:
                    score = float(score)
                except (TypeError, ValueError) as exc:
                    raise ValueError("outcome label score must be finite") from exc
                if not math.isfinite(score):
                    raise ValueError("outcome label score must be finite")
            reasons = raw.get("reason_codes", raw.get("reasons", ()))
            if reasons is None:
                reasons = []
            elif isinstance(reasons, str):
                reasons = [reasons] if reasons.strip() else []
            elif isinstance(reasons, Mapping):
                reasons = dict(reasons)
            elif isinstance(reasons, Sequence):
                reasons = list(reasons)
            else:
                reasons = [str(reasons)]
            reason_json = json.dumps(
                reasons,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            metadata = raw.get("metadata", raw.get("context", {}))
            if metadata is None:
                metadata = {}
            if not isinstance(metadata, Mapping):
                raise TypeError("outcome label metadata must be a mapping")
            metadata_json = _json(metadata)
            label_id = str(raw.get("label_id") or "").strip()
            expected_label_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"liangjian-outcome:{run_id}:{lane_id}:{trade_date}:{stage}:{symbol}",
            ).hex
            if label_id and label_id != expected_label_id:
                raise ValueError("outcome label_id does not match natural identity")
            prepared.append(
                {
                    "label_id": expected_label_id,
                    "run_id": run_id,
                    "lane_id": lane_id,
                    "trade_date": trade_date,
                    "stage": stage,
                    "symbol": symbol,
                    "decision": decision,
                    "reason_codes": reason_json,
                    "selection_basis": selection_basis,
                    "score": score,
                    "snapshot_id": snapshot_id,
                    "config_hash": config_hash,
                    "metadata_json": metadata_json,
                }
            )
        if not prepared:
            return ()
        immutable_fields = (
            "label_id",
            "run_id",
            "lane_id",
            "trade_date",
            "stage",
            "symbol",
            "decision",
            "reason_codes",
            "selection_basis",
            "score",
            "snapshot_id",
            "config_hash",
            "metadata_json",
        )

        def operation(connection):
            result: list[dict[str, Any]] = []
            for item in prepared:
                existing = connection.execute(
                    "SELECT * FROM astock_outcome_labels WHERE run_id=? AND lane_id=? AND stage=? AND symbol=?",
                    (item["run_id"], item["lane_id"], item["stage"], item["symbol"]),
                ).fetchone()
                if existing is not None:
                    for field in immutable_fields:
                        if existing[field] != item[field]:
                            raise OutcomeLabelConflictError("OUTCOME_LABEL_IMMUTABLE_CONFLICT")
                    result.append(_row_dict(existing) or {})
                    continue
                connection.execute(
                    """
                    INSERT INTO astock_outcome_labels(
                        label_id,run_id,lane_id,trade_date,stage,symbol,decision,reason_codes,
                        selection_basis,score,snapshot_id,config_hash,metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        item["label_id"],
                        item["run_id"],
                        item["lane_id"],
                        item["trade_date"],
                        item["stage"],
                        item["symbol"],
                        item["decision"],
                        item["reason_codes"],
                        item["selection_basis"],
                        item["score"],
                        item["snapshot_id"],
                        item["config_hash"],
                        item["metadata_json"],
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM astock_outcome_labels WHERE label_id=?",
                    (item["label_id"],),
                ).fetchone()
                result.append(_row_dict(row) or {})
            return tuple(result)

        return tuple(self._write(operation))

    def record_outcome_label(self, label: Mapping[str, Any]) -> dict[str, Any]:
        """Singular convenience wrapper around :meth:`record_outcome_labels`."""

        rows = self.record_outcome_labels((label,))
        return rows[0]

    def update_outcome_label_metrics(
        self,
        updates: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], ...]:
        """Append forward measurements without rewriting immutable facts.

        A non-null measurement cannot be replaced by a different value.  A
        null incoming value never erases a previously observed value.  This
        makes retries safe while still allowing a short window (1/3/5 days)
        to be completed before the 10-day label is closed.
        """

        if isinstance(updates, (str, bytes, bytearray)):
            raise TypeError("outcome metric updates must be a sequence of mappings")
        fields = (
            "fwd_return_1d",
            "fwd_return_3d",
            "fwd_return_5d",
            "fwd_return_10d",
            "mfe_5d",
            "mae_5d",
            "benchmark_return_5d",
            "excess_return_5d",
            "labeled_at",
            "baseline_status",
            "baseline_sample_size",
        )
        prepared: list[dict[str, Any]] = []
        for raw in updates:
            if not isinstance(raw, Mapping):
                raise TypeError("outcome metric update must be a mapping")
            item = {field: raw.get(field) for field in fields if field in raw}
            if not item:
                continue
            if raw.get("label_id"):
                item["label_id"] = str(raw["label_id"])
                item["identity"] = ("label_id", item["label_id"])
            else:
                identity = (
                    str(raw.get("run_id") or raw.get("decision_run_id") or "").strip(),
                    str(raw.get("lane_id") or "").strip(),
                    str(raw.get("trade_date") or "").strip(),
                    str(raw.get("stage") or "").strip().upper(),
                    str(raw.get("symbol") or "").strip().upper(),
                )
                # The run/lane pair is mandatory whenever the natural key is
                # used.  A legacy three-field lookup is retained only when it
                # is unambiguous; same-day reruns must never silently update
                # an arbitrary lane.
                if bool(identity[0]) != bool(identity[1]):
                    raise ValueError("outcome metric run_id and lane_id must be supplied together")
                if not all(identity):
                    if all(identity[2:]):
                        item["identity"] = ("legacy_natural", *identity[2:])
                    else:
                        raise ValueError("outcome metric update identity is required")
                else:
                    item["identity"] = ("natural", *identity)
            for field in fields:
                if field not in item or item[field] is None or field in {"labeled_at", "baseline_status"}:
                    continue
                if field == "baseline_sample_size":
                    if isinstance(item[field], bool) or not isinstance(item[field], int) or item[field] < 0:
                        raise ValueError("baseline_sample_size must be a non-negative integer")
                    continue
                try:
                    number = float(item[field])
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{field} must be finite") from exc
                if not math.isfinite(number):
                    raise ValueError(f"{field} must be finite")
                item[field] = number
            if item.get("baseline_status") is not None:
                item["baseline_status"] = str(item["baseline_status"]).strip().upper()
                if not item["baseline_status"]:
                    raise ValueError("baseline_status must not be empty")
            prepared.append(item)
        if not prepared:
            return ()

        def operation(connection):
            output: list[dict[str, Any]] = []
            for item in prepared:
                if item["identity"][0] == "label_id":
                    current = connection.execute(
                        "SELECT * FROM astock_outcome_labels WHERE label_id=?",
                        (item["identity"][1],),
                    ).fetchone()
                elif item["identity"][0] == "legacy_natural":
                    matches = connection.execute(
                        "SELECT * FROM astock_outcome_labels WHERE trade_date=? AND stage=? AND symbol=?",
                        item["identity"][1:],
                    ).fetchall()
                    if len(matches) > 1:
                        raise OutcomeLabelConflictError("OUTCOME_LABEL_IDENTITY_REQUIRED")
                    current = matches[0] if matches else None
                else:
                    current = connection.execute(
                        "SELECT * FROM astock_outcome_labels "
                        "WHERE run_id=? AND lane_id=? AND trade_date=? AND stage=? AND symbol=?",
                        item["identity"][1:],
                    ).fetchone()
                if current is None:
                    raise StateTransitionError("OUTCOME_LABEL_NOT_FOUND")
                assignments: list[str] = []
                values: list[Any] = []
                for field in fields:
                    if field not in item or item[field] is None:
                        continue
                    old = current[field]
                    new = item[field]
                    # Baseline availability can improve as the same
                    # snapshot's peer rows arrive.  It is an observation
                    # status, not an immutable decision fact.
                    if field in {"baseline_status", "baseline_sample_size"}:
                        if old != new:
                            assignments.append(f"{field}=?")
                            values.append(new)
                        continue
                    if old is not None and old != new:
                        raise OutcomeLabelConflictError("OUTCOME_LABEL_METRIC_CONFLICT")
                    if old is None:
                        assignments.append(f"{field}=?")
                        values.append(new)
                if assignments:
                    values.append(current["label_id"])
                    connection.execute(
                        f"UPDATE astock_outcome_labels SET {','.join(assignments)} WHERE label_id=?",
                        values,
                    )
                row = connection.execute(
                    "SELECT * FROM astock_outcome_labels WHERE label_id=?",
                    (current["label_id"],),
                ).fetchone()
                output.append(_row_dict(row) or {})
            return tuple(output)

        return tuple(self._write(operation))

    def update_outcome_label(self, label_id: str, **metrics: Any) -> dict[str, Any]:
        """Singular convenience wrapper for deterministic backfill callers."""

        rows = self.update_outcome_label_metrics(({"label_id": label_id, **metrics},))
        return rows[0]

    def get_outcome_label(self, label_id: str) -> dict[str, Any] | None:
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    "SELECT * FROM astock_outcome_labels WHERE label_id=?",
                    (str(label_id),),
                ).fetchone()
            )
        )

    def list_outcome_labels(
        self,
        *,
        run_id: str | None = None,
        lane_id: str | None = None,
        trade_date: str | None = None,
        stage: str | None = None,
        symbol: str | None = None,
        decision: str | None = None,
        labeled_only: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        """Read labels in deterministic identity order for offline evaluation."""

        clauses: list[str] = []
        args: list[Any] = []
        if run_id is not None:
            clauses.append("run_id=?")
            args.append(str(run_id).strip())
        if lane_id is not None:
            clauses.append("lane_id=?")
            args.append(str(lane_id).strip())
        if trade_date is not None:
            clauses.append("trade_date=?")
            args.append(str(trade_date).strip())
        if stage is not None:
            value = str(stage).strip().upper()
            if value not in OUTCOME_STAGES:
                raise ValueError("invalid outcome label stage")
            clauses.append("stage=?")
            args.append(value)
        if symbol is not None:
            clauses.append("symbol=?")
            args.append(str(symbol).strip().upper())
        if decision is not None:
            value = str(decision).strip().upper()
            if value not in OUTCOME_DECISIONS:
                raise ValueError("invalid outcome label decision")
            clauses.append("decision=?")
            args.append(value)
        if labeled_only:
            clauses.append("labeled_at IS NOT NULL")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._read(
            lambda connection: tuple(
                _row_dict(row)
                for row in connection.execute(
                    "SELECT * FROM astock_outcome_labels"
                    f"{where} ORDER BY trade_date, CASE stage WHEN 'G0' THEN 0 WHEN 'A1' THEN 1 "
                    "WHEN 'A2' THEN 2 WHEN 'A3' THEN 3 WHEN 'A4' THEN 4 END, run_id, lane_id, symbol",
                    args,
                ).fetchall()
            )
        )

    def count_outcome_labels(self, **filters: Any) -> int:
        return len(self.list_outcome_labels(**filters))

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
        invalidate_pending_lanes: Sequence[str] = (),
    ) -> tuple[dict[str, Any], ...]:
        """Publish a validated multi-lane plan set in one SQLite transaction.

        ``invalidate_pending_lanes`` is used by a close publication to make
        the new A3 result authoritative for the next morning.  Only older
        ``PENDING_MORNING_REVIEW`` rows in those lanes are retired; the plan
        ids in the current batch are explicitly preserved so an idempotent
        re-publication cannot invalidate itself.  No ``ACTIVE_TODAY`` row is
        touched by this replacement operation.
        """

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
        pending_lanes = tuple(dict.fromkeys(str(item) for item in invalidate_pending_lanes))
        current_plan_ids_by_lane: dict[str, set[str]] = {}
        for item in normalized:
            current_plan_ids_by_lane.setdefault(str(item["lane_id"]), set()).add(str(item["plan_id"]))

        def operation(connection):
            for lane_id in lanes:
                connection.execute(
                    "UPDATE execution_plans SET status=?,updated_at=? WHERE lane_id=? AND status=?",
                    (PlanStatus.EXPIRED.value, now, lane_id, PlanStatus.ACTIVE_TODAY.value),
                )
            # A close result is a complete replacement for the next-session
            # pending scope, including the valid-empty case.  Fetching the
            # ids first lets us exclude every plan id in the current batch
            # without constructing unbounded SQL placeholders.  The whole
            # operation remains inside this BEGIN IMMEDIATE transaction.
            for lane_id in pending_lanes:
                protected_ids = current_plan_ids_by_lane.get(lane_id, set())
                pending_rows = connection.execute(
                    "SELECT plan_id FROM execution_plans WHERE lane_id=? AND status=?",
                    (lane_id, PlanStatus.PENDING_MORNING_REVIEW.value),
                ).fetchall()
                for row in pending_rows:
                    if str(row["plan_id"]) in protected_ids:
                        continue
                    connection.execute(
                        "UPDATE execution_plans SET status=?,updated_at=? WHERE plan_id=? AND status=?",
                        (
                            PlanStatus.INVALIDATED.value,
                            now,
                            str(row["plan_id"]),
                            PlanStatus.PENDING_MORNING_REVIEW.value,
                        ),
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

    def activate_latest_a3_plan_batch(
        self,
        plan_ids: Sequence[str],
        *,
        invalidated_plan_ids: Sequence[str] = (),
        valid_from: datetime,
        as_of: datetime | None = None,
        session_expires_at: datetime | None = None,
        source_run_id: str,
    ) -> tuple[dict[str, Any], ...]:
        """Idempotently activate one already-published A3 plan set.

        This is a narrow recovery/entry point for the current A4 session.  A
        caller must identify the published A3 source run and perform the
        provider quote, stop and no-chase checks before calling it.  The
        method itself still enforces the immutable source lineage, current
        validity horizon and atomic state transition.  Existing active rows
        are returned unchanged, so repeated monitor ticks cannot create a
        second activation or re-run the morning transition.
        """

        activation_ids = tuple(dict.fromkeys(str(item) for item in plan_ids))
        invalidation_ids = tuple(dict.fromkeys(str(item) for item in invalidated_plan_ids))
        ids = tuple(dict.fromkeys((*activation_ids, *invalidation_ids)))
        source = str(source_run_id).strip()
        if not ids:
            return ()
        if not source:
            raise ValueError("A3 source run id is required")
        if valid_from.tzinfo is None or valid_from.utcoffset() is None:
            raise ValueError("valid_from must be timezone-aware")
        effective_at = as_of or valid_from
        if effective_at.tzinfo is None or effective_at.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if session_expires_at is not None and (
            session_expires_at.tzinfo is None or session_expires_at.utcoffset() is None
        ):
            raise ValueError("session_expires_at must be timezone-aware")
        stamp = _iso(valid_from)
        check_at = _iso(effective_at)
        session_expiry = _iso(session_expires_at)
        if session_expiry is not None and session_expiry < check_at:
            raise ValueError("session_expires_at must not precede as_of")
        now = _iso(_now())

        def operation(connection):
            rows: list[sqlite3.Row] = []
            for plan_id in ids:
                row = connection.execute(
                    "SELECT * FROM execution_plans WHERE plan_id=?",
                    (plan_id,),
                ).fetchone()
                if row is None:
                    raise StateTransitionError("PLAN_NOT_FOUND")
                if row["status"] == PlanStatus.ACTIVE_TODAY.value:
                    if plan_id in invalidation_ids:
                        raise StateTransitionError("A3_ACTIVE_PLAN_CANNOT_INVALIDATE")
                    if row["expires_at"] is not None and row["expires_at"] < check_at:
                        raise StateTransitionError("A3_PLAN_EXPIRED")
                    try:
                        payload = json.loads(str(row["payload_json"] or "{}"))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise StateTransitionError("A3_PLAN_PAYLOAD_INVALID") from None
                    if not isinstance(payload, Mapping) or str(payload.get("source_run_id") or "") != source:
                        raise StateTransitionError("A3_PLAN_SOURCE_MISMATCH")
                    rows.append(row)
                    continue
                if row["status"] == PlanStatus.INVALIDATED.value:
                    if plan_id not in invalidation_ids:
                        raise StateTransitionError("A3_PLAN_NOT_PENDING_MORNING_REVIEW")
                    try:
                        payload = json.loads(str(row["payload_json"] or "{}"))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise StateTransitionError("A3_PLAN_PAYLOAD_INVALID") from None
                    if not isinstance(payload, Mapping) or str(payload.get("source_run_id") or "") != source:
                        raise StateTransitionError("A3_PLAN_SOURCE_MISMATCH")
                    rows.append(row)
                    continue
                if row["status"] != PlanStatus.PENDING_MORNING_REVIEW.value:
                    raise StateTransitionError("PLAN_NOT_PENDING_MORNING_REVIEW")
                if row["expires_at"] is None or row["expires_at"] < check_at:
                    raise StateTransitionError("A3_PLAN_EXPIRED")
                try:
                    payload = json.loads(str(row["payload_json"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    raise StateTransitionError("A3_PLAN_PAYLOAD_INVALID") from None
                if not isinstance(payload, Mapping) or str(payload.get("source_run_id") or "") != source:
                    raise StateTransitionError("A3_PLAN_SOURCE_MISMATCH")
                rows.append(row)
            for row in rows:
                if row["status"] != PlanStatus.PENDING_MORNING_REVIEW.value:
                    continue
                target = (
                    PlanStatus.INVALIDATED.value
                    if row["plan_id"] in invalidation_ids
                    else PlanStatus.ACTIVE_TODAY.value
                )
                connection.execute(
                    "UPDATE execution_plans SET status=?,valid_from=?,expires_at=COALESCE(?,expires_at),updated_at=? WHERE plan_id=? AND status=?",
                    (
                        target,
                        stamp if target == PlanStatus.ACTIVE_TODAY.value else row["valid_from"],
                        session_expiry if target == PlanStatus.ACTIVE_TODAY.value else None,
                        now,
                        row["plan_id"],
                        PlanStatus.PENDING_MORNING_REVIEW.value,
                    ),
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
        terminal_plan_id: str | None = None,
        sync_a4_lifecycle: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        action = str(action)
        if effective and action not in EFFECTIVE_ACTIONS:
            raise ValueError("invalid effective monitor action")
        if terminal_plan_id is not None:
            terminal_plan_id = str(terminal_plan_id).strip()
            if not terminal_plan_id:
                raise ValueError("terminal plan id must not be empty")
            if not effective or action != MonitorAction.PLAN_INVALIDATED.value:
                raise ValueError("terminal plan id requires effective PLAN_INVALIDATED event")
        now = _iso(_now())
        minute = _iso(minute_end)
        payload_json = _json(payload)
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-monitor:{event_key}"))

        def operation(connection):
            existing = connection.execute("SELECT * FROM monitor_events WHERE event_key=?", (event_key,)).fetchone()
            if existing is not None:
                if terminal_plan_id is not None:
                    if existing["action"] != MonitorAction.PLAN_INVALIDATED.value or not bool(existing["effective"]):
                        raise StateTransitionError("TERMINAL_PLAN_EVENT_CONFLICT")
                    plan = connection.execute(
                        "SELECT status FROM execution_plans WHERE plan_id=?", (terminal_plan_id,)
                    ).fetchone()
                    if plan is None:
                        raise StateTransitionError("PLAN_NOT_FOUND")
                    if plan["status"] == PlanStatus.INVALIDATED.value:
                        pass
                    elif plan["status"] in {
                        PlanStatus.ACTIVE_TODAY.value,
                        PlanStatus.PENDING_MORNING_REVIEW.value,
                    }:
                        connection.execute(
                            "UPDATE execution_plans SET status=?,updated_at=? WHERE plan_id=? AND status IN (?,?)",
                            (
                                PlanStatus.INVALIDATED.value,
                                now,
                                terminal_plan_id,
                                PlanStatus.ACTIVE_TODAY.value,
                                PlanStatus.PENDING_MORNING_REVIEW.value,
                            ),
                        )
                    else:
                        raise StateTransitionError("TERMINAL_PLAN_STATE_CONFLICT")
                if sync_a4_lifecycle and effective:
                    self._sync_a4_lifecycle_for_event(
                        connection,
                        event_id=str(existing["event_id"]),
                        event_key=str(existing["event_key"]),
                        lane_id=str(existing["lane_id"]),
                        minute_end=str(existing["minute_end"]),
                        action=str(existing["action"]),
                        reason_code=existing["reason_code"],
                        payload=_a4_mapping(existing["payload_json"]),
                        terminal_plan_id=terminal_plan_id,
                        now=now,
                    )
                return _row_dict(existing), False
            if terminal_plan_id is not None:
                plan = connection.execute(
                    "SELECT status FROM execution_plans WHERE plan_id=?", (terminal_plan_id,)
                ).fetchone()
                if plan is None:
                    raise StateTransitionError("PLAN_NOT_FOUND")
                if plan["status"] in {
                    PlanStatus.ACTIVE_TODAY.value,
                    PlanStatus.PENDING_MORNING_REVIEW.value,
                }:
                    connection.execute(
                        "UPDATE execution_plans SET status=?,updated_at=? WHERE plan_id=? AND status IN (?,?)",
                        (
                            PlanStatus.INVALIDATED.value,
                            now,
                            terminal_plan_id,
                            PlanStatus.ACTIVE_TODAY.value,
                            PlanStatus.PENDING_MORNING_REVIEW.value,
                        ),
                    )
                elif plan["status"] != PlanStatus.INVALIDATED.value:
                    raise StateTransitionError("TERMINAL_PLAN_STATE_CONFLICT")
            connection.execute(
                """
                INSERT INTO monitor_events(event_id,event_key,lane_id,minute_end,action,reason_code,effective,payload_json,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (event_id, event_key, lane_id, minute, action, reason_code, int(effective), payload_json, now),
            )
            if sync_a4_lifecycle and effective:
                self._sync_a4_lifecycle_for_event(
                    connection,
                    event_id=event_id,
                    event_key=event_key,
                    lane_id=lane_id,
                    minute_end=minute_end,
                    action=action,
                    reason_code=reason_code,
                    payload=payload,
                    terminal_plan_id=terminal_plan_id,
                )
            return _row_dict(connection.execute("SELECT * FROM monitor_events WHERE event_id=?", (event_id,)).fetchone()), True

        return self._write(operation)

    def list_monitor_events(
        self,
        *,
        lane_id: str | None = None,
        effective_only: bool = False,
        from_time: datetime | str | None = None,
        to_time: datetime | str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        def operation(connection):
            where: list[str] = []
            args: list[Any] = []
            if lane_id is not None:
                where.append("lane_id=?")
                args.append(lane_id)
            if effective_only:
                where.append("effective=1")
            if from_time is not None:
                where.append("minute_end>=?")
                args.append(_iso(from_time))
            if to_time is not None:
                where.append("minute_end<=?")
                args.append(_iso(to_time))
            sql = "SELECT * FROM monitor_events" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY minute_end,event_id"
            return tuple(_row_dict(row) for row in connection.execute(sql, args).fetchall())

        return self._read(operation)

    # ------------------------------------------------------------------
    # A4 signal lifecycle ledger
    # ------------------------------------------------------------------
    @staticmethod
    def _a4_row_mapping(value: Any) -> dict[str, Any]:
        if isinstance(value, sqlite3.Row):
            return {key: value[key] for key in value.keys()}
        if isinstance(value, Mapping):
            return dict(value)
        raise TypeError("A4 row must be a mapping")

    @staticmethod
    def _a4_transition(current: str, target: str) -> None:
        if current == target:
            return
        if current in A4_SIGNAL_TERMINAL_STATUSES:
            raise StateTransitionError("A4_TERMINAL_STATE_CONFLICT")
        if target not in A4_SIGNAL_TRANSITIONS.get(current, frozenset()):
            raise StateTransitionError("ILLEGAL_A4_LIFECYCLE_TRANSITION")

    @staticmethod
    def _a4_decode_payload(row: Mapping[str, Any] | None) -> dict[str, Any]:
        if not row:
            return {}
        decoded = _a4_mapping(row.get("payload_json"))
        if decoded:
            return decoded
        return _a4_mapping(row.get("payload"))

    @staticmethod
    def _a4_scalar(mapping_values: Sequence[Mapping[str, Any]], *keys: str) -> Any:
        value = _a4_first(mapping_values, *keys)
        return None if isinstance(value, (Mapping, list, tuple, set)) else value

    @staticmethod
    def _a4_resolve_on_connection(
        connection: sqlite3.Connection,
        signal_event_key: str | None = None,
        *,
        event_payload: Mapping[str, Any] | None = None,
        plan_id: str | None = None,
        account_id: str | None = None,
        symbol: str | None = None,
        include_terminal: bool = True,
    ) -> sqlite3.Row | None:
        payload = _a4_mapping(event_payload)
        keys = (
            signal_event_key,
            payload.get("entry_event_key"),
            payload.get("signal_event_key"),
            payload.get("entry_event_id"),
            payload.get("signal_id"),
            payload.get("lifecycle_id"),
        )
        for key in keys:
            text = str(key or "").strip()
            if not text:
                continue
            row = connection.execute(
                """
                SELECT * FROM a4_signal_lifecycles
                WHERE entry_event_key=? OR entry_event_id=? OR signal_id=? OR lifecycle_id=?
                LIMIT 1
                """,
                (text, text, text, text),
            ).fetchone()
            if row is not None:
                return row
        clauses: list[str] = []
        args: list[Any] = []
        if plan_id:
            clauses.append("plan_id=?")
            args.append(str(plan_id))
        if account_id:
            clauses.append("account_id=?")
            args.append(str(account_id))
        if symbol:
            clauses.append("symbol=?")
            args.append(str(symbol))
        if not include_terminal:
            placeholders = ",".join("?" for _ in A4_SIGNAL_TERMINAL_STATUSES)
            clauses.append(f"status NOT IN ({placeholders})")
            args.extend(sorted(A4_SIGNAL_TERMINAL_STATUSES))
        if clauses:
            return connection.execute(
                "SELECT * FROM a4_signal_lifecycles WHERE "
                + " AND ".join(clauses)
                + " ORDER BY updated_at DESC,lifecycle_id DESC LIMIT 1",
                args,
            ).fetchone()
        return None

    def _record_a4_entry_on_connection(
        self,
        connection: sqlite3.Connection,
        *,
        event: Mapping[str, Any],
        plan: Mapping[str, Any],
        now: str,
    ) -> tuple[dict[str, Any], bool]:
        event_data = dict(event)
        plan_data = dict(plan)
        event_payload = self._a4_decode_payload(event_data)
        plan_payload = self._a4_decode_payload(plan_data)
        event_strategy = _a4_mapping(event_payload.get("strategy") or event_payload.get("strategy_snapshot"))
        scopes = (event_payload, event_strategy, plan_payload, plan_data, event_data)

        event_key = str(event_data.get("event_key") or "").strip()
        event_id = str(event_data.get("event_id") or "").strip()
        if not event_key or not event_id:
            raise ValueError("A4 entry event key and id are required")
        if str(event_data.get("action") or "").strip().upper() not in {"BUY_SIGNAL", "BUY"}:
            raise ValueError("A4 entry event must be BUY_SIGNAL")

        plan_id = str(self._a4_scalar(scopes, "plan_id") or "").strip()
        lane_id = str(event_data.get("lane_id") or self._a4_scalar(scopes, "lane_id") or "").strip()
        symbol = str(self._a4_scalar(scopes, "symbol", "stock_code", "code") or "").strip().upper()
        if not plan_id or not lane_id or not symbol:
            raise ValueError("A4 entry plan, lane and symbol are required")
        account_raw = self._a4_scalar(scopes, "account_id", "account")
        account_id = f"paper:{lane_id}"
        if account_raw is not None and str(account_raw).strip() != account_id:
            raise StateTransitionError("A4_ACCOUNT_IDENTITY_CONFLICT")
        trade_raw = self._a4_scalar(scopes, "trade_date", "market_trade_date", "signal_date")
        if trade_raw is None:
            trade_raw = event_data.get("minute_end")
        trade_date = str(trade_raw or "").strip().replace(" ", "T", 1).split("T", 1)[0]
        try:
            date.fromisoformat(trade_date)
        except ValueError as exc:
            raise ValueError("A4 trade date must be YYYY-MM-DD") from exc
        source_run_id = str(
            self._a4_scalar(scopes, "source_run_id", "run_id", "research_run_id") or ""
        ).strip()
        # A few legacy monitor tests/old plans predate source lineage and the
        # strategy-type contract.  Keep their event history auditable without
        # dropping the lifecycle: the explicit legacy marker makes the gap
        # visible to downstream reports and never masquerades as a real run.
        if not source_run_id:
            source_run_id = f"legacy:{plan_id}"
        name = str(self._a4_scalar(scopes, "name", "stock_name", "security_name") or symbol).strip() or symbol
        behavior = str(
            self._a4_scalar(scopes, "stock_behavior_type", "behavior_type", "stock_type", "category") or ""
        ).strip().upper()
        strategy = str(
            self._a4_scalar(scopes, "strategy_profile", "deterministic_strategy_profile", "strategy_route") or ""
        ).strip().upper()
        behavior = behavior or "UNKNOWN"
        strategy = strategy or "LEGACY"
        signal_time = _a4_bar_stamp(event_data.get("minute_end"))
        signal_price = _a4_number(
            self._a4_scalar(scopes, "signal_price", "entry_signal_price", "entry_reference", "reference_price", "price")
        )
        strategy_snapshot = event_strategy or _a4_mapping(event_payload.get("strategy_snapshot"))
        if not strategy_snapshot:
            strategy_snapshot = {
                key: event_payload[key]
                for key in ("strategy_profile", "stock_behavior_type", "reason_codes", "met_conditions", "unmet_conditions", "veto_conditions")
                if key in event_payload
            }
        plan_snapshot = plan_payload or {
            key: plan_data[key]
            for key in ("plan_id", "lane_id", "symbol", "status", "plan_version", "valid_from", "expires_at")
            if key in plan_data
        }
        strategy_json = _json(strategy_snapshot)
        plan_json = _json(plan_snapshot)
        lifecycle_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-a4-lifecycle:{event_key}"))
        existing = connection.execute(
            "SELECT * FROM a4_signal_lifecycles WHERE entry_event_key=?", (event_key,)
        ).fetchone()
        if existing is not None:
            immutable = (
                existing["lifecycle_id"], existing["entry_event_id"], existing["plan_id"], existing["lane_id"],
                existing["account_id"], existing["symbol"], existing["trade_date"], existing["source_run_id"],
                existing["name"], existing["stock_behavior_type"], existing["strategy_profile"],
                existing["strategy_snapshot_json"], existing["plan_snapshot_json"], existing["signal_price"],
            )
            proposed = (
                lifecycle_id, event_id, plan_id, lane_id, account_id, symbol, trade_date, source_run_id,
                name, behavior, strategy, strategy_json, plan_json, signal_price,
            )
            if immutable != proposed:
                raise StateTransitionError("A4_SIGNAL_IDENTITY_CONFLICT")
            return _row_dict(existing) or {}, False
        duplicate_id = connection.execute(
            "SELECT 1 FROM a4_signal_lifecycles WHERE entry_event_id=? OR signal_id=? OR lifecycle_id=? LIMIT 1",
            (event_id, lifecycle_id, lifecycle_id),
        ).fetchone()
        if duplicate_id is not None:
            raise StateTransitionError("A4_SIGNAL_IDENTITY_CONFLICT")
        connection.execute(
            """
            INSERT INTO a4_signal_lifecycles(
                lifecycle_id,signal_id,entry_event_key,entry_event_id,plan_id,lane_id,account_id,symbol,
                trade_date,source_run_id,name,stock_behavior_type,strategy_profile,
                strategy_snapshot_json,plan_snapshot_json,status,signal_time,signal_price,
                entry_qty,exit_signal_qty,exit_qty,remaining_qty,entry_fee,exit_fee,total_fee,
                applied_fill_keys_json,applied_exit_event_keys_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                lifecycle_id, lifecycle_id, event_key, event_id, plan_id, lane_id, account_id, symbol,
                trade_date, source_run_id, name, behavior, strategy, strategy_json, plan_json,
                A4SignalStatus.SIGNALLED.value, signal_time, signal_price,
                0, 0, 0, 0, 0.0, 0.0, 0.0, "[]", "[]", now, now,
            ),
        )
        return _row_dict(
            connection.execute("SELECT * FROM a4_signal_lifecycles WHERE lifecycle_id=?", (lifecycle_id,)).fetchone()
        ) or {}, True

    def _record_a4_exit_on_connection(
        self,
        connection: sqlite3.Connection,
        *,
        event: Mapping[str, Any],
        now: str,
        required: bool = False,
    ) -> tuple[dict[str, Any] | None, bool]:
        event_data = dict(event)
        action = str(event_data.get("action") or "").strip().upper()
        if action not in {"SELL_SIGNAL", "SELL", "REDUCE_SIGNAL", "REDUCE", "FORCED_RISK_EXIT", "PLAN_INVALIDATED", "CANCEL_SIGNAL", "DATA_BLOCK"}:
            raise ValueError("A4 exit event action invalid")
        event_key = str(event_data.get("event_key") or "").strip()
        if not event_key:
            raise ValueError("A4 exit event key is required")
        event_payload = self._a4_decode_payload(event_data)
        strategy = _a4_mapping(event_payload.get("strategy"))
        scopes = (event_payload, strategy, event_data)
        plan_id = str(self._a4_scalar(scopes, "plan_id") or "").strip() or None
        account_id = str(self._a4_scalar(scopes, "account_id", "account") or "").strip() or None
        symbol = str(self._a4_scalar(scopes, "symbol", "stock_code", "code") or "").strip().upper() or None
        row = self._a4_resolve_on_connection(
            connection,
            event_payload.get("entry_event_key"),
            event_payload=event_payload,
            plan_id=plan_id,
            account_id=account_id,
            symbol=symbol,
            include_terminal=True,
        )
        if row is None:
            if required:
                raise StateTransitionError("A4_LIFECYCLE_NOT_FOUND")
            return None, False
        applied_keys = _a4_json_list(row["applied_exit_event_keys_json"])
        if event_key in applied_keys:
            return _row_dict(row), False
        current = str(row["status"])
        if current in A4_SIGNAL_TERMINAL_STATUSES:
            raise StateTransitionError("A4_TERMINAL_STATE_CONFLICT")
        if action in {"PLAN_INVALIDATED"}:
            # Invalidating an unfilled plan is terminal. If a paper position
            # already exists, the plan may stop authorizing new entries while
            # that position still needs an auditable exit lifecycle.
            target = (
                A4SignalStatus.EXIT_PENDING.value
                if int(row["remaining_qty"] or 0) > 0
                else A4SignalStatus.INVALIDATED.value
            )
        elif action == "CANCEL_SIGNAL":
            target = A4SignalStatus.CANCELLED.value
        elif action == "DATA_BLOCK":
            target = A4SignalStatus.DATA_ERROR.value
        else:
            target = A4SignalStatus.EXIT_PENDING.value
        if target in A4_SIGNAL_TERMINAL_STATUSES and int(row["remaining_qty"] or 0) > 0:
            raise StateTransitionError("A4_OPEN_QUANTITY_NOT_TERMINAL")
        self._a4_transition(current, target)
        applied_keys.append(event_key)
        price = _a4_number(self._a4_scalar(scopes, "signal_price", "exit_signal_price", "price"))
        qty = _a4_nonnegative_int(self._a4_scalar(scopes, "qty", "quantity", "exit_signal_qty"), default=0)
        reason = str(event_data.get("reason_code") or self._a4_scalar(scopes, "exit_reason", "reason") or "").strip() or None
        connection.execute(
            """
            UPDATE a4_signal_lifecycles
            SET status=?,exit_signal_time=?,exit_signal_price=?,exit_signal_qty=?,exit_reason=?,
                applied_exit_event_keys_json=?,updated_at=?
            WHERE lifecycle_id=? AND status=?
            """,
            (
                target,
                _a4_bar_stamp(event_data.get("minute_end")),
                price,
                qty,
                reason,
                json.dumps(applied_keys, ensure_ascii=False, separators=(",", ":")),
                now,
                row["lifecycle_id"],
                current,
            ),
        )
        return _row_dict(
            connection.execute("SELECT * FROM a4_signal_lifecycles WHERE lifecycle_id=?", (row["lifecycle_id"],)).fetchone()
        ), True

    def _sync_a4_lifecycle_for_event(
        self,
        connection: sqlite3.Connection,
        *,
        event_id: str,
        event_key: str,
        lane_id: str,
        minute_end: datetime | str,
        action: str,
        reason_code: str | None,
        payload: Mapping[str, Any] | None,
        terminal_plan_id: str | None,
        now: str | None = None,
    ) -> None:
        """Synchronize the lifecycle while the monitor event transaction is open."""

        stamp = now or _iso(_now()) or ""
        event = {
            "event_id": event_id,
            "event_key": event_key,
            "lane_id": lane_id,
            "minute_end": minute_end,
            "action": action,
            "reason_code": reason_code,
            "payload": dict(payload or {}),
        }
        if action == MonitorAction.BUY_SIGNAL.value:
            plan_id = str(_a4_first((_a4_mapping(payload),), "plan_id") or "").strip()
            if not plan_id:
                raise StateTransitionError("A4_PLAN_NOT_FOUND")
            plan_row = connection.execute("SELECT * FROM execution_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan_row is None:
                raise StateTransitionError("A4_PLAN_NOT_FOUND")
            self._record_a4_entry_on_connection(
                connection,
                event=event,
                plan=self._a4_row_mapping(plan_row),
                now=stamp,
            )
            return
        if action in {
            MonitorAction.SELL_SIGNAL.value,
            MonitorAction.REDUCE_SIGNAL.value,
            MonitorAction.FORCED_RISK_EXIT.value,
            MonitorAction.PLAN_INVALIDATED.value,
            MonitorAction.CANCEL_SIGNAL.value,
        }:
            if action == MonitorAction.PLAN_INVALIDATED.value and terminal_plan_id:
                event["payload"] = {**_a4_mapping(payload), "plan_id": terminal_plan_id}
            self._record_a4_exit_on_connection(connection, event=event, now=stamp, required=False)

    def record_a4_entry_signal(
        self,
        event: Mapping[str, Any] | sqlite3.Row | None = None,
        plan: Mapping[str, Any] | sqlite3.Row | None = None,
        *,
        event_row: Mapping[str, Any] | sqlite3.Row | None = None,
        plan_row: Mapping[str, Any] | sqlite3.Row | None = None,
        plan_payload: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Persist one immutable BUY_SIGNAL lifecycle, idempotently."""

        raw_event = event_row if event_row is not None else event
        raw_plan = plan_row if plan_row is not None else plan
        if raw_event is None or raw_plan is None:
            raise ValueError("A4 entry event and plan are required")
        event_data = self._a4_row_mapping(raw_event)
        plan_data = self._a4_row_mapping(raw_plan)
        if plan_payload is not None:
            plan_data["payload"] = dict(plan_payload)
        now = _iso(_now()) or ""
        return self._write(lambda connection: self._record_a4_entry_on_connection(connection, event=event_data, plan=plan_data, now=now))

    def record_a4_exit_signal(
        self,
        event: Mapping[str, Any] | sqlite3.Row | None = None,
        *,
        event_row: Mapping[str, Any] | sqlite3.Row | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Record a SELL/REDUCE/invalidating event against an existing signal."""

        raw_event = event_row if event_row is not None else event
        if raw_event is None:
            raise ValueError("A4 exit event is required")
        event_data = self._a4_row_mapping(raw_event)
        now = _iso(_now()) or ""
        return self._write(lambda connection: self._record_a4_exit_on_connection(connection, event=event_data, now=now, required=True))

    def apply_a4_fill(
        self,
        signal_event_key: str,
        fill_row: Mapping[str, Any] | sqlite3.Row | None = None,
        remaining_qty: int | None = None,
        *,
        fill: Mapping[str, Any] | sqlite3.Row | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Apply one paper fill to a lifecycle, with fill-key idempotency."""

        if fill_row is not None and fill is not None:
            raise ValueError("provide only one A4 fill row")
        raw_fill = fill if fill is not None else fill_row
        if raw_fill is None:
            raise ValueError("A4 fill row is required")
        fill_data = self._a4_row_mapping(raw_fill)
        key = str(
            fill_data.get("fill_key")
            or fill_data.get("fill_id")
            or fill_data.get("intent_key")
            or fill_data.get("event_key")
            or ""
        ).strip()
        if not key:
            key = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    "liangjian-a4-fill:" + json.dumps(fill_data, ensure_ascii=False, sort_keys=True, default=str),
                )
            )
        action = str(fill_data.get("action") or "").strip().upper()
        action = {
            "BUY_SIGNAL": "BUY",
            "ADD_SIGNAL": "ADD",
            "SELL_SIGNAL": "SELL",
            "REDUCE_SIGNAL": "REDUCE",
            "FORCED_RISK_EXIT": "SELL",
        }.get(action, action)
        if action not in {"BUY", "ADD", "SELL", "REDUCE"}:
            raise ValueError("A4 fill action invalid")
        qty = _a4_nonnegative_int(fill_data.get("qty", fill_data.get("quantity")))
        if qty <= 0:
            raise ValueError("A4 fill quantity must be positive")
        price = _a4_number(fill_data.get("price"), allow_none=False, positive=True)
        fee = _a4_number(fill_data.get("fee", fill_data.get("commission", 0.0)), allow_none=False)
        if fee is None or fee < 0:
            raise ValueError("A4 fill fee invalid")
        bar_end = _a4_bar_stamp(fill_data.get("bar_end", fill_data.get("filled_at", fill_data.get("timestamp"))))
        if remaining_qty is None and fill_data.get("remaining_qty") is not None:
            remaining_qty = _a4_nonnegative_int(fill_data.get("remaining_qty"))
        account_id = str(fill_data.get("account_id") or "").strip() or None
        symbol = str(fill_data.get("symbol") or "").strip().upper() or None
        now = _iso(_now()) or ""

        def operation(connection):
            row = self._a4_resolve_on_connection(connection, signal_event_key, account_id=account_id, symbol=symbol)
            if row is None:
                raise StateTransitionError("A4_LIFECYCLE_NOT_FOUND")
            applied_keys = _a4_json_list(row["applied_fill_keys_json"])
            if key in applied_keys:
                return _row_dict(row) or {}, False
            if row["status"] in A4_SIGNAL_TERMINAL_STATUSES:
                raise StateTransitionError("A4_TERMINAL_STATE_CONFLICT")
            if account_id is not None and account_id != row["account_id"]:
                raise StateTransitionError("A4_ACCOUNT_IDENTITY_CONFLICT")
            if symbol is not None and symbol != row["symbol"]:
                raise StateTransitionError("A4_SIGNAL_IDENTITY_CONFLICT")
            current_remaining = int(row["remaining_qty"])
            current_entry_qty = int(row["entry_qty"])
            current_exit_qty = int(row["exit_qty"])
            entry_fee = float(row["entry_fee"] or 0.0)
            exit_fee = float(row["exit_fee"] or 0.0)
            old_entry_price = float(row["entry_price"]) if row["entry_price"] is not None else None
            old_exit_price = float(row["exit_price"]) if row["exit_price"] is not None else None
            target_status: str
            entry_time = row["entry_time"]
            exit_time = row["exit_time"]
            exit_price = old_exit_price
            exit_qty = current_exit_qty
            if action in {"BUY", "ADD"}:
                if row["status"] not in {A4SignalStatus.SIGNALLED.value, A4SignalStatus.OPEN.value, A4SignalStatus.EXIT_PENDING.value}:
                    raise StateTransitionError("ILLEGAL_A4_LIFECYCLE_TRANSITION")
                if action == "BUY" and current_entry_qty > 0:
                    raise StateTransitionError("A4_DUPLICATE_ENTRY_FILL")
                new_entry_qty = current_entry_qty + qty
                new_remaining = current_remaining + qty
                if remaining_qty is not None and int(remaining_qty) != new_remaining:
                    raise StateTransitionError("A4_REMAINING_QTY_CONFLICT")
                new_entry_price = price if old_entry_price is None else ((old_entry_price * current_entry_qty) + (price * qty)) / new_entry_qty
                entry_fee += float(fee)
                entry_time = entry_time or bar_end
                target_status = A4SignalStatus.OPEN.value
                connection.execute(
                    """
                    UPDATE a4_signal_lifecycles
                    SET status=?,entry_time=?,entry_price=?,entry_qty=?,remaining_qty=?,entry_fee=?,total_fee=?,
                        last_fill_key=?,applied_fill_keys_json=?,updated_at=?
                    WHERE lifecycle_id=? AND status=?
                    """,
                    (
                        target_status, entry_time, new_entry_price, new_entry_qty, new_remaining, entry_fee,
                        entry_fee + exit_fee, key,
                        json.dumps([*applied_keys, key], ensure_ascii=False, separators=(",", ":")), now,
                        row["lifecycle_id"], row["status"],
                    ),
                )
            else:
                if current_entry_qty <= 0 or current_remaining <= 0:
                    raise StateTransitionError("A4_NO_OPEN_QUANTITY")
                if qty > current_remaining:
                    raise StateTransitionError("A4_EXIT_QTY_EXCEEDS_REMAINING")
                new_remaining = current_remaining - qty
                if remaining_qty is not None and int(remaining_qty) != new_remaining:
                    raise StateTransitionError("A4_REMAINING_QTY_CONFLICT")
                entry_price = old_entry_price
                if entry_price is None or entry_price <= 0:
                    raise StateTransitionError("A4_ENTRY_PRICE_REQUIRED")
                exit_qty += qty
                new_exit_price = price if old_exit_price is None else ((old_exit_price * current_exit_qty) + (price * qty)) / exit_qty
                gross_pnl = float(row["gross_pnl"] or 0.0) + (price - entry_price) * qty
                exit_fee += float(fee)
                total_fee = entry_fee + exit_fee
                net_pnl = gross_pnl - total_fee
                cost_basis = entry_price * current_entry_qty
                target_status = A4SignalStatus.CLOSED.value if new_remaining == 0 else A4SignalStatus.PARTIALLY_CLOSED.value
                if target_status == A4SignalStatus.CLOSED.value:
                    exit_time = bar_end
                connection.execute(
                    """
                    UPDATE a4_signal_lifecycles
                    SET status=?,exit_time=?,exit_price=?,exit_qty=?,remaining_qty=?,exit_fee=?,total_fee=?,
                        gross_pnl=?,net_pnl=?,realized_pnl=?,gross_return=?,net_return=?,last_fill_key=?,
                        applied_fill_keys_json=?,updated_at=?
                    WHERE lifecycle_id=? AND status=?
                    """,
                    (
                        target_status, exit_time, new_exit_price, exit_qty, new_remaining, exit_fee, total_fee,
                        gross_pnl, net_pnl, net_pnl,
                        gross_pnl / cost_basis if cost_basis else None,
                        net_pnl / cost_basis if cost_basis else None,
                        key, json.dumps([*applied_keys, key], ensure_ascii=False, separators=(",", ":")), now,
                        row["lifecycle_id"], row["status"],
                    ),
                )
            return _row_dict(
                connection.execute("SELECT * FROM a4_signal_lifecycles WHERE lifecycle_id=?", (row["lifecycle_id"],)).fetchone()
            ) or {}, True

        return self._write(operation)

    def observe_a4_lifecycle(
        self,
        account: str | None = None,
        symbol: str | None = None,
        bar_end: datetime | str | None = None,
        high: float | None = None,
        low: float | None = None,
        close: float | None = None,
        *,
        account_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Update aggregate extrema only; minute bars are not copied."""

        resolved_account = str(account_id or account or "").strip()
        resolved_symbol = str(symbol or "").strip().upper()
        if not resolved_account or not resolved_symbol:
            raise ValueError("A4 account and symbol are required")
        if bar_end is None or high is None or low is None or close is None:
            raise ValueError("A4 observation values are required")
        stamp = _a4_bar_stamp(bar_end)
        high_value = _a4_number(high, allow_none=False, positive=True)
        low_value = _a4_number(low, allow_none=False, positive=True)
        close_value = _a4_number(close, allow_none=False, positive=True)
        if high_value is None or low_value is None or close_value is None or high_value < low_value:
            raise ValueError("A4 OHLC values invalid")
        now = _iso(_now()) or ""

        def operation(connection):
            row = self._a4_resolve_on_connection(
                connection, account_id=resolved_account, symbol=resolved_symbol, include_terminal=False
            )
            if row is None:
                return None
            max_price = max(float(row["max_price"]) if row["max_price"] is not None else high_value, high_value)
            min_price = min(float(row["min_price"]) if row["min_price"] is not None else low_value, low_value)
            baseline = row["entry_price"] if row["entry_price"] is not None else row["signal_price"]
            mfe = (max_price / float(baseline)) - 1.0 if baseline and float(baseline) > 0 else None
            mae = (min_price / float(baseline)) - 1.0 if baseline and float(baseline) > 0 else None
            holding_minutes = None
            if row["entry_time"]:
                try:
                    entry_dt = datetime.fromisoformat(str(row["entry_time"]))
                    bar_dt = datetime.fromisoformat(stamp)
                    holding_minutes = max(0, int((bar_dt - entry_dt).total_seconds() // 60))
                except (TypeError, ValueError):
                    holding_minutes = None
            connection.execute(
                """
                UPDATE a4_signal_lifecycles
                SET max_price=?,min_price=?,mfe=?,mae=?,holding_minutes=?,last_bar_end=?,last_close=?,updated_at=?
                WHERE lifecycle_id=? AND status NOT IN (?,?,?,?,?)
                """,
                (
                    max_price, min_price, mfe, mae, holding_minutes, stamp, close_value, now, row["lifecycle_id"],
                    *sorted(A4_SIGNAL_TERMINAL_STATUSES),
                ),
            )
            return _row_dict(
                connection.execute("SELECT * FROM a4_signal_lifecycles WHERE lifecycle_id=?", (row["lifecycle_id"],)).fetchone()
            )

        return self._write(operation)

    def mark_a4_signal_terminal(
        self,
        signal_event_key: str | None = None,
        status: str | A4SignalStatus | None = None,
        *,
        event_key: str | None = None,
        reason_code: str | None = None,
        at: datetime | str | None = None,
        exit_price: float | None = None,
        exit_qty: int | None = None,
        fee: float | None = None,
    ) -> dict[str, Any]:
        signal_event_key = str(signal_event_key or event_key or "").strip()
        if not signal_event_key:
            raise ValueError("A4 lifecycle identity is required")
        if status is None:
            raise ValueError("A4 terminal status is required")
        target = str(status).strip().upper()
        if target not in A4_SIGNAL_TERMINAL_STATUSES:
            raise ValueError("A4 terminal status invalid")
        stamp = _a4_bar_stamp(at) if at is not None else (_iso(_now()) or "")
        price = _a4_number(exit_price)
        qty = _a4_nonnegative_int(exit_qty, default=0)
        fee_value = 0.0 if fee is None else _a4_number(fee, allow_none=False)
        if fee_value is None or fee_value < 0:
            raise ValueError("A4 terminal fee invalid")
        now = _iso(_now()) or ""

        def operation(connection):
            row = self._a4_resolve_on_connection(connection, signal_event_key)
            if row is None:
                raise StateTransitionError("A4_LIFECYCLE_NOT_FOUND")
            current = str(row["status"])
            if current == target:
                return _row_dict(row) or {}
            self._a4_transition(current, target)
            if target in {
                A4SignalStatus.UNFILLED.value,
                A4SignalStatus.CANCELLED.value,
                A4SignalStatus.INVALIDATED.value,
                A4SignalStatus.DATA_ERROR.value,
            } and int(row["remaining_qty"] or 0) > 0:
                raise StateTransitionError("A4_OPEN_QUANTITY_NOT_TERMINAL")
            exit_time = stamp if target == A4SignalStatus.CLOSED.value else row["exit_time"]
            exit_qty_value = qty if qty else int(row["exit_qty"] or 0)
            exit_fee = float(row["exit_fee"] or 0.0) + float(fee_value)
            connection.execute(
                """
                UPDATE a4_signal_lifecycles
                SET status=?,exit_time=?,exit_price=COALESCE(?,exit_price),exit_qty=?,remaining_qty=?,
                    exit_fee=?,total_fee=entry_fee+?,exit_reason=?,updated_at=?
                WHERE lifecycle_id=? AND status=?
                """,
                (
                    target, exit_time, price, exit_qty_value,
                    0 if target in A4_SIGNAL_TERMINAL_STATUSES else int(row["remaining_qty"]),
                    exit_fee, exit_fee, reason_code, now, row["lifecycle_id"], current,
                ),
            )
            return _row_dict(
                connection.execute("SELECT * FROM a4_signal_lifecycles WHERE lifecycle_id=?", (row["lifecycle_id"],)).fetchone()
            ) or {}

        return self._write(operation)

    def get_a4_lifecycle(
        self,
        signal_event_key: str | None = None,
        *,
        entry_event_key: str | None = None,
        lifecycle_id: str | None = None,
        signal_id: str | None = None,
    ) -> dict[str, Any] | None:
        key = signal_event_key or entry_event_key or lifecycle_id or signal_id
        if not key:
            raise ValueError("A4 lifecycle identity is required")
        text = str(key).strip()
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    """
                    SELECT * FROM a4_signal_lifecycles
                    WHERE entry_event_key=? OR entry_event_id=? OR lifecycle_id=? OR signal_id=?
                    LIMIT 1
                    """,
                    (text, text, text, text),
                ).fetchone()
            )
        )

    def list_a4_lifecycles(
        self,
        *,
        account_id: str | None = None,
        symbol: str | None = None,
        lane_id: str | None = None,
        plan_id: str | None = None,
        trade_date: str | None = None,
        status: str | Sequence[str] | None = None,
        limit: int = 1000,
    ) -> tuple[dict[str, Any], ...]:
        bounded = max(1, min(int(limit), 10000))

        def operation(connection):
            clauses: list[str] = []
            args: list[Any] = []
            for column, value in (("account_id", account_id), ("symbol", symbol), ("lane_id", lane_id), ("plan_id", plan_id), ("trade_date", trade_date)):
                if value is not None:
                    clauses.append(f"{column}=?")
                    args.append(str(value).strip().upper() if column == "symbol" else str(value).strip())
            if status is not None:
                statuses = [str(item).strip().upper() for item in status] if isinstance(status, Sequence) and not isinstance(status, str) else [str(status).strip().upper()]
                statuses = [item for item in statuses if item]
                if statuses:
                    clauses.append("status IN (" + ",".join("?" for _ in statuses) + ")")
                    args.extend(statuses)
            sql = "SELECT * FROM a4_signal_lifecycles" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY signal_time DESC,lifecycle_id DESC LIMIT ?"
            args.append(bounded)
            return tuple(_row_dict(row) for row in connection.execute(sql, args).fetchall())

        return self._read(operation)

    # Naming aliases keep callers readable while retaining one SQL source of truth.
    get_a4_signal_lifecycle = get_a4_lifecycle
    list_a4_signal_lifecycles = list_a4_lifecycles

    # ------------------------------------------------------------------
    # A5 twice-daily evidence review ledger
    # ------------------------------------------------------------------
    def record_a5_review(
        self,
        *,
        trade_date: str,
        review_kind: str,
        cutoff_at: datetime | str,
        status: str,
        model: str,
        source_run_ids: Sequence[str],
        input_hash: str,
        prompt_hash: str,
        output_hash: str,
        latency_ms: int,
        attempts: int,
        thinking_variant: str | None,
        fact_snapshot: Mapping[str, Any],
        report: Mapping[str, Any],
        markdown_path: str,
    ) -> tuple[dict[str, Any], bool]:
        """Persist one immutable A5 review, idempotent for identical facts."""

        kind = str(review_kind).strip().upper()
        if kind not in {"MIDDAY", "POST_CLOSE"}:
            raise ValueError("invalid A5 review kind")
        state = str(status).strip().upper()
        if state not in {"COMPLETED", "DEGRADED"}:
            raise ValueError("invalid A5 review status")
        day = date.fromisoformat(str(trade_date)).isoformat()
        cutoff = _iso(cutoff_at)
        if cutoff is None:
            raise ValueError("A5 cutoff is required")
        identity = f"{day}:{kind}:{input_hash}"
        review_key = f"a5:{identity}"
        review_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-{review_key}"))
        created_at = _iso(_now())
        source_json = json.dumps(
            list(dict.fromkeys(str(item) for item in source_run_ids if str(item))),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        fact_json = _json(fact_snapshot)
        report_json = _json(report)

        def operation(connection):
            existing = connection.execute(
                "SELECT * FROM a5_daily_reviews WHERE review_key=?", (review_key,)
            ).fetchone()
            if existing is not None:
                immutable = (
                    existing["cutoff_at"], existing["model"], existing["prompt_hash"],
                    existing["output_hash"], existing["fact_snapshot_json"], existing["report_json"],
                )
                proposed = (cutoff, str(model), str(prompt_hash), str(output_hash), fact_json, report_json)
                if immutable != proposed:
                    raise StateTransitionError("A5_REVIEW_CONTENT_CONFLICT")
                return _row_dict(existing), False
            connection.execute(
                """
                INSERT INTO a5_daily_reviews(
                    review_id,review_key,trade_date,review_kind,cutoff_at,status,model,
                    source_run_ids_json,input_hash,prompt_hash,output_hash,latency_ms,attempts,
                    thinking_variant,fact_snapshot_json,report_json,markdown_path,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    review_id, review_key, day, kind, cutoff, state, str(model), source_json,
                    str(input_hash), str(prompt_hash), str(output_hash), max(0, int(latency_ms)),
                    max(0, int(attempts)), thinking_variant, fact_json, report_json,
                    str(markdown_path), created_at,
                ),
            )
            return _row_dict(
                connection.execute("SELECT * FROM a5_daily_reviews WHERE review_id=?", (review_id,)).fetchone()
            ), True

        return self._write(operation)

    def list_a5_reviews(
        self,
        *,
        trade_date: str | None = None,
        review_kind: str | None = None,
        limit: int = 20,
    ) -> tuple[dict[str, Any], ...]:
        bounded = max(1, min(int(limit), 200))
        kind = str(review_kind).strip().upper() if review_kind is not None else None
        if kind is not None and kind not in {"MIDDAY", "POST_CLOSE"}:
            raise ValueError("invalid A5 review kind")

        def operation(connection):
            clauses: list[str] = []
            args: list[Any] = []
            if trade_date is not None:
                clauses.append("trade_date=?")
                args.append(date.fromisoformat(str(trade_date)).isoformat())
            if kind is not None:
                clauses.append("review_kind=?")
                args.append(kind)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            rows = connection.execute(
                "SELECT * FROM a5_daily_reviews" + where + " ORDER BY cutoff_at DESC,created_at DESC LIMIT ?",
                (*args, bounded),
            ).fetchall()
            return tuple(_row_dict(row) for row in rows)

        return self._read(operation)

    # ------------------------------------------------------------------
    # Lark notification delivery ledger
    # ------------------------------------------------------------------
    def next_notification_color(self) -> str:
        """Return the next durable card color without creating a row.

        The workflow sends synchronously and records the outcome afterwards.
        This read lets the caller build the exact card that will be recorded;
        the single scheduler lease serializes normal production callers.  The
        ledger itself also prevents duplicate keys from creating a second
        color slot.
        """

        def operation(connection):
            row = connection.execute(
                "SELECT color FROM notification_deliveries ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return NOTIFICATION_CARD_COLORS[0]
            previous = str(row["color"] or "")
            try:
                index = NOTIFICATION_CARD_COLORS.index(previous)
            except ValueError:
                index = -1
            return NOTIFICATION_CARD_COLORS[(index + 1) % len(NOTIFICATION_CARD_COLORS)]

        return str(self._read(operation))

    def record_delivery(
        self,
        *,
        delivery_key: str,
        kind: str,
        source_id: str,
        title: str,
        status: str,
        color: str | None = None,
        attempt_count: int = 1,
        last_reason_code: str | None = None,
        payload: Mapping[str, Any] | None = None,
        created_at: datetime | str | None = None,
        sent_at: datetime | str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Record one synchronous Lark delivery outcome idempotently.

        ``delivery_key`` is the business idempotency key (for example,
        ``premarket:<date>:<run>:<page>`` or ``a4:<event_id>``).  The caller
        checks the existing row before sending; this method is the durable
        last line of defence and never creates a second row for the same key.
        Only a safe, pre-built summary is accepted as ``payload``.
        """

        key = _notify_text(delivery_key, field="delivery key", limit=512)
        kind_value = _notify_text(kind, field="kind", limit=64)
        source = _notify_text(source_id, field="source id", limit=256)
        title_value = _notify_text(title, field="title", limit=512)
        status_value = str(status).strip().upper()
        if status_value not in NOTIFICATION_STATUSES:
            raise ValueError("invalid notification delivery status")
        try:
            attempts = int(attempt_count)
        except (TypeError, ValueError):
            raise ValueError("notification attempt count must be an integer") from None
        if attempts < 0:
            raise ValueError("notification attempt count must be non-negative")
        payload_json = _notify_payload(payload)
        reason = _notify_reason(last_reason_code)
        stamp = _notify_stamp(created_at)
        sent_stamp = _notify_stamp(sent_at) if sent_at is not None else None
        delivery_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"liangjian-lark-delivery:{key}"))

        def operation(connection):
            existing = connection.execute(
                "SELECT * FROM notification_deliveries WHERE delivery_key=?", (key,)
            ).fetchone()
            if existing is not None:
                return _row_dict(existing), False

            latest = connection.execute(
                "SELECT color FROM notification_deliveries ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
            previous = str(latest["color"] or "") if latest is not None else ""
            requested = str(color or "").strip().lower()
            if requested not in NOTIFICATION_CARD_COLORS:
                requested = ""
            # A supplied color is allowed for semantic grouping, but adjacent
            # cards must still be visually distinguishable.  Invalid or
            # repeated colors use the same durable rotation as the default.
            if not requested or requested == previous:
                try:
                    index = NOTIFICATION_CARD_COLORS.index(previous)
                except ValueError:
                    index = -1
                requested = NOTIFICATION_CARD_COLORS[(index + 1) % len(NOTIFICATION_CARD_COLORS)]
            if status_value == "SENT":
                effective_sent_at = sent_stamp or stamp
            else:
                effective_sent_at = None
            connection.execute(
                """
                INSERT INTO notification_deliveries(
                    delivery_id,delivery_key,kind,source_id,status,title,color,
                    attempt_count,last_reason_code,created_at,updated_at,sent_at,payload_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    delivery_id,
                    key,
                    kind_value,
                    source,
                    status_value,
                    title_value,
                    requested,
                    attempts,
                    reason,
                    stamp,
                    stamp,
                    effective_sent_at,
                    payload_json,
                ),
            )
            return (
                _row_dict(
                    connection.execute(
                        "SELECT * FROM notification_deliveries WHERE delivery_id=?", (delivery_id,)
                    ).fetchone()
                ),
                True,
            )

        return self._write(operation)

    def get_delivery_by_key(self, delivery_key: str) -> dict[str, Any] | None:
        key = _notify_text(delivery_key, field="delivery key", limit=512)
        return self._read(
            lambda connection: _row_dict(
                connection.execute(
                    "SELECT * FROM notification_deliveries WHERE delivery_key=?", (key,)
                ).fetchone()
            )
        )

    def list_notification_deliveries(
        self,
        *,
        limit: int = 50,
        kind: str | None = None,
        status: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        bounded = max(1, min(int(limit), 200))
        kind_value = _notify_text(kind, field="kind", limit=64) if kind is not None else None
        status_value = str(status).strip().upper() if status is not None else None
        if status_value is not None and status_value not in NOTIFICATION_STATUSES:
            raise ValueError("invalid notification delivery status")

        def operation(connection):
            clauses: list[str] = []
            args: list[Any] = []
            if kind_value is not None:
                clauses.append("kind=?")
                args.append(kind_value)
            if status_value is not None:
                clauses.append("status=?")
                args.append(status_value)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            rows = connection.execute(
                "SELECT * FROM notification_deliveries"
                + where
                + " ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (*args, bounded),
            ).fetchall()
            return tuple(_row_dict(row) for row in rows)

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
    "A4_SIGNAL_TERMINAL_STATUSES",
    "A4SignalStatus",
    "EFFECTIVE_ACTIONS",
    "MonitorAction",
    "NOTIFICATION_CARD_COLORS",
    "NOTIFICATION_STATUSES",
    "OUTCOME_DECISIONS",
    "OUTCOME_SELECTION_BASES",
    "OUTCOME_STAGES",
    "OutcomeLabelConflictError",
    "PersistenceBlockedError",
    "PersistenceError",
    "PlanStatus",
    "ResearchStageStatus",
    "RuntimeStateError",
    "RuntimeStore",
    "STAGE_COMPLETED_VALUES",
    "STAGE_STATUS_VALUES",
    "StateTransitionError",
]
