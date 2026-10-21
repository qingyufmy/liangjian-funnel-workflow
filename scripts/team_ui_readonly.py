"""UI-only bounded SQLite projection. Standard library; no runtime/business imports.

Called by Node with fixed arguments, never a shell or a SQL statement supplied by HTTP.
This module does not create directories, open a writable database or request external data.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

ROLE = {"a1": "A1", "a2": "A2", "pm": "PM", "tr": "A4", "rk": "A6", "rv": "A5"}
KINDS = {"tasks", "feed", "plans", "fills", "role-today", "role-stats", "review/scoreboard", "review/lessons", "settings"}


def scalar(value, *, identifier=False):
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if not isinstance(value, str):
        return None
    # No free prose, URLs, filesystem paths, provider errors or secret-bearing JSON.
    pattern = r"[A-Za-z0-9_.:+\-]{1,240}" if identifier else r"[\w\s.()+\-\u3400-\u9fff]{0,64}"
    return value if re.fullmatch(pattern, value) else None


def source(table, row_id, raw):
    return {"table": table, "id": scalar(row_id, identifier=True),
            "sha256": hashlib.sha256(raw.encode()).hexdigest()}


def connect(root, relative):
    path = (root / relative).resolve(strict=True)
    if not path.is_relative_to(root):
        raise ValueError("SOURCE_OUTSIDE_PROJECT")
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=.05)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    db.execute("PRAGMA mmap_size=0")
    db.execute("PRAGMA cache_size=-1024")
    deadline = time.monotonic() + .12
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    return db


def project(root, query):
    kind, day = query["kind"], query["trade_date"]
    date.fromisoformat(day)
    if kind not in KINDS or len(day) != 10:
        raise ValueError("INVALID_QUERY")
    limit, offset = query["limit"], query["offset"]
    if not (type(limit) is int and 1 <= limit <= 500 and type(offset) is int and 0 <= offset <= 1_000_000):
        raise ValueError("INVALID_QUERY")
    if query.get("role") is not None and query["role"] not in ROLE:
        raise ValueError("INVALID_QUERY")
    out = {"schema": "team-ui-readonly/1", "data_mode": "PERSISTED_READ_ONLY",
           "trade_date": day, "observed_at": datetime.now(timezone(timedelta(hours=8))).isoformat(),
           "coverage_kind": "BOUNDED_PERSISTED_SOURCE", "status": "MISSING_SOURCE",
           "rows": None, "total": None, "next_offset": None,
           "missing_reason": "NO_APPROVED_DISPLAY_SOURCE"}
    # Source/metric contracts for conversations, lessons and settings are not yet
    # established. Do not scrape raw prompts, arbitrary JSON or .env as a substitute.
    if kind in {"feed", "review/scoreboard", "review/lessons", "settings", "role-stats"}:
        return out
    rows = []
    if kind == "plans":
        db = connect(root, "state/workflow.sqlite3")
        try:
            for row in db.execute("SELECT plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json FROM execution_plans WHERE valid_from>=? AND valid_from<? ORDER BY plan_id LIMIT ? OFFSET ?",
                                  (day, (date.fromisoformat(day) + timedelta(days=1)).isoformat(), limit + 1, offset)):
                payload = json.loads(row["payload_json"])
                rows.append({"id": scalar(row["plan_id"], identifier=True), "code": scalar(row["symbol"], identifier=True),
                             "name": scalar(payload.get("name")), "status": scalar(row["status"], identifier=True),
                             "strat": scalar(payload.get("strategy_profile"), identifier=True),
                             "zone": [scalar(payload.get("trigger_low")), scalar(payload.get("trigger_high"))],
                             "stop": scalar(payload.get("stop_level")), "cluster": scalar(payload.get("board_group")),
                             "risk_unit": scalar(payload.get("risk_unit"), identifier=True),
                             "updated_at": scalar(row["updated_at"], identifier=True),
                             "source": source("execution_plans", row["plan_id"], row["payload_json"])})
            # This table holds latest plan state, not a historical status-transition journal.
            out["state_scope"] = "CURRENT_PERSISTED_STATUS_NOT_HISTORICAL_RECONSTRUCTION"
        finally:
            db.close()
    elif kind == "fills":
        db = connect(root, "state/team_v2/paper.db")
        try:
            for row in db.execute("SELECT fill_id,order_id,symbol,plan_id,side,qty,price,fee,minute_end FROM paper_fills WHERE minute_end>=? AND minute_end<? ORDER BY minute_end,fill_id LIMIT ? OFFSET ?",
                                  (day, (date.fromisoformat(day) + timedelta(days=1)).isoformat(), limit + 1, offset)):
                item = {key: scalar(row[key], identifier=key not in {"qty", "price", "fee"}) for key in row.keys()}
                item["source"] = source("paper_fills", row["fill_id"], json.dumps(dict(row), sort_keys=True))
                rows.append(item)
        finally:
            db.close()
    else:
        db = connect(root, "state/team_v2/model.db")
        try:
            role = ROLE.get(query.get("role"))
            clause = " AND (CASE WHEN session LIKE '%PM%' THEN 'PM' ELSE role END)=?" if role else ""
            params = [day, (date.fromisoformat(day) + timedelta(days=1)).isoformat()]
            if role:
                params.append(role)
            for row in db.execute("SELECT call_id,role,session,captured_at,latency_ms,provider_attempts,validated,output_hash FROM model_calls WHERE captured_at>=? AND captured_at<?" + clause + " ORDER BY captured_at,call_id LIMIT ? OFFSET ?", (*params, limit + 1, offset)):
                item = {key: scalar(row[key], identifier=isinstance(row[key], str)) for key in row.keys()}
                item["source"] = source("model_calls", row["call_id"], json.dumps(dict(row), sort_keys=True))
                rows.append(item)
            out["coverage_kind"] = "MODEL_CALL_JOURNAL_ONLY_NOT_ALL_SCHEDULER_TASKS"
        finally:
            db.close()
    has_more = len(rows) > limit
    out.update(status="AVAILABLE", rows=rows[:limit], next_offset=offset + limit if has_more else None,
               missing_reason=None, total=None)
    # A page count is not a full-day total. Avoid unbounded COUNT scans on live tables.
    out["page_count"] = len(rows[:limit])
    return out


def main():
    try:
        root = Path(sys.argv[1]).resolve(strict=True)
        query = json.loads(sys.argv[2])
        print(json.dumps(project(root, query), ensure_ascii=False, allow_nan=False))
    except (OSError, ValueError, KeyError, sqlite3.Error):
        # Stable public code only. Never disclose the database path or raw exception.
        print(json.dumps({"error": "TEAM_UI_SOURCE_UNAVAILABLE"}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
