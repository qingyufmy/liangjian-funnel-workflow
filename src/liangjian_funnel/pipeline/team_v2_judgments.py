"""Separate research journal for model judgments, not trade/outcome statistics.

The journal preserves rejected output too. ``validated`` means schema/evidence
validation only, never engine eligibility or an executable trading permission.
No connection to RuntimeStore, execution_plans, brokers or notifications.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
from contextlib import closing
from pathlib import Path
import sqlite3
from typing import Any

from .model_client import ModelCallResult
from .team_v2_contract import TeamValidation


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ModelJudgmentJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS model_calls (
                    call_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, role TEXT NOT NULL,
                    session TEXT NOT NULL, source_trade_date TEXT NOT NULL,
                    execution_trade_date TEXT NOT NULL, captured_at TEXT NOT NULL,
                    model TEXT NOT NULL, prompt_hash TEXT NOT NULL, input_hash TEXT NOT NULL,
                    catalog_hash TEXT NOT NULL, output_hash TEXT NOT NULL,
                    latency_ms INTEGER NOT NULL, provider_attempts INTEGER NOT NULL,
                    validated INTEGER NOT NULL, issues_json TEXT NOT NULL,
                    sentinel_count INTEGER NOT NULL, output_json TEXT NOT NULL,
                    binding_sha256 TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS model_judgments (
                    call_id TEXT NOT NULL REFERENCES model_calls(call_id),
                    ordinal INTEGER NOT NULL, judgment_path TEXT NOT NULL,
                    validated INTEGER NOT NULL, judgment_json TEXT NOT NULL,
                    judgment_sha256 TEXT NOT NULL, decision_id TEXT NOT NULL UNIQUE,
                    plan_id TEXT, symbol TEXT, t1_return TEXT, t3_return TEXT,
                    PRIMARY KEY(call_id, ordinal));
                CREATE INDEX IF NOT EXISTS model_calls_cohort
                    ON model_calls(source_trade_date, role, session);
                CREATE TABLE IF NOT EXISTS model_call_claims (
                    call_id TEXT PRIMARY KEY, request_binding TEXT NOT NULL,
                    state TEXT NOT NULL, failure_code TEXT);
            """)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=2)
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def claim(self, call_id: str, request_binding: str) -> None:
        """Claim before network I/O. A crash/timeout cannot cause an implicit retry."""
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM model_calls WHERE call_id=?", (call_id,)).fetchone():
                raise ValueError("MODEL_CALL_ALREADY_RECORDED")
            existing = db.execute("SELECT request_binding FROM model_call_claims WHERE call_id=?", (call_id,)).fetchone()
            if existing:
                code = "MODEL_CALL_OUTCOME_UNKNOWN_NO_RETRY" if existing[0] == request_binding else "MODEL_CALL_BINDING_CONFLICT"
                raise ValueError(code)
            db.execute("INSERT INTO model_call_claims VALUES (?,?,'CLAIMED',NULL)", (call_id, request_binding))

    def mark_failed(self, call_id: str, code: str) -> None:
        # Code is chosen by our transport, never copied from exception text.
        if code not in {"TEAM_MODEL_CALL_FAILED", "TEAM_MODEL_RESULT_INVALID", "TEAM_VALIDATOR_FAILED"}:
            raise ValueError("MODEL_FAILURE_CODE_INVALID")
        with closing(self._connect()) as db, db:
            db.execute("UPDATE model_call_claims SET state='FAILED',failure_code=? WHERE call_id=? AND state='CLAIMED'",
                       (code, call_id))

    def capture(self, *, call_id: str, run_id: str, role: str, session: str,
                source_trade_date: str, execution_trade_date: str, captured_at: datetime,
                catalog_hash: str, response: ModelCallResult, validation: TeamValidation) -> dict:
        if (not isinstance(captured_at, datetime) or captured_at.tzinfo is None
                or captured_at.utcoffset() is None):
            raise ValueError("MODEL_CAPTURE_TIME_INVALID")
        if (role not in {"A1", "A2", "A3", "A4", "A5", "A6"}
                or any(not isinstance(value, str) or not value.strip()
                       for value in (call_id, run_id, session, catalog_hash,
                                     response.model, response.prompt_hash, response.input_hash))):
            raise ValueError("MODEL_CAPTURE_IDENTITY_INVALID")
        for day in (source_trade_date, execution_trade_date):
            if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
                raise ValueError("MODEL_CAPTURE_DATE_INVALID")
        if (type(response.latency_ms) is not int or response.latency_ms < 0
                or type(response.attempts) is not int or response.attempts < 1):
            raise ValueError("MODEL_CAPTURE_METRICS_INVALID")
        output_json = _json(response.output)
        if _hash(output_json) != response.output_hash:
            raise ValueError("MODEL_OUTPUT_HASH_MISMATCH")
        judgments = []
        for row in validation.judgments:
            if not isinstance(row, dict) or not isinstance(row.get("path"), str) or not row["path"]:
                raise ValueError("MODEL_JUDGMENT_PATH_INVALID")
            if any(row.get(key) is not None and not isinstance(row[key], str) for key in ("plan_id", "symbol")):
                raise ValueError("MODEL_JUDGMENT_IDENTITY_INVALID")
            judgments.append((row["path"], _json(row)))
        body = {"run_id": run_id, "role": role, "session": session,
                "source_trade_date": source_trade_date, "execution_trade_date": execution_trade_date,
                "model": response.model, "prompt_hash": response.prompt_hash,
                "input_hash": response.input_hash, "catalog_hash": catalog_hash,
                "output_hash": response.output_hash, "latency_ms": response.latency_ms,
                "provider_attempts": response.attempts, "validated": int(validation.valid),
                "issues_json": _json(validation.issues), "sentinel_count": validation.sentinel_count,
                "output_json": output_json}
        binding = _hash(_json({**body, "judgments": judgments}))
        db = self._connect()
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                existing = db.execute("SELECT binding_sha256 FROM model_calls WHERE call_id=?", (call_id,)).fetchone()
                if existing:
                    if existing[0] != binding:
                        raise ValueError("MODEL_CALL_BINDING_CONFLICT")
                    return {"call_id": call_id, "judgment_count": len(judgments), "reused": True}
                fields = {"call_id": call_id, **body, "captured_at": captured_at.isoformat(), "binding_sha256": binding}
                db.execute("INSERT INTO model_calls (" + ",".join(fields) + ") VALUES ("
                           + ",".join("?" for _ in fields) + ")", tuple(fields.values()))
                db.executemany("""INSERT INTO model_judgments
                    (call_id,ordinal,judgment_path,validated,judgment_json,judgment_sha256,
                     decision_id,plan_id,symbol) VALUES (?,?,?,?,?,?,?,?,?)""",
                    [(call_id, index, path, int(validation.valid), text, _hash(text),
                      _hash(_json([call_id, index, path])), json.loads(text).get("plan_id"),
                      json.loads(text).get("symbol"))
                     for index, (path, text) in enumerate(judgments)])
                db.execute("UPDATE model_call_claims SET state='CAPTURED' WHERE call_id=? AND state='CLAIMED'", (call_id,))
            return {"call_id": call_id, "judgment_count": len(judgments), "reused": False}
        finally:
            db.close()

    def summary(self) -> list[dict]:
        db = self._connect()
        db.row_factory = sqlite3.Row
        try:
            # Count calls before joining judgment rows: one response may contain
            # many decisions, while network retries belong to provider_attempts.
            return [dict(row) for row in db.execute("""
                SELECT role,session,count(*) logical_calls,sum(provider_attempts) provider_attempts,
                    sum(latency_ms) latency_ms,sum(validated) valid_calls,
                    sum((SELECT count(*) FROM model_judgments j WHERE j.call_id=c.call_id)) judgments
                FROM model_calls c GROUP BY role,session ORDER BY role,session
            """)]
        finally:
            db.close()
