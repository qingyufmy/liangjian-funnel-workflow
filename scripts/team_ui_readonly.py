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
import math
from datetime import date, datetime, timedelta, timezone

ROLE = {"a1": "A1", "a2": "A2", "pm": "PM", "tr": "A4", "rk": "A6", "rv": "A5"}
KINDS = {"dashboard", "summary", "system/health", "events", "tasks", "feed", "plans", "fills", "role-today", "role-stats", "review/scoreboard", "review/lessons", "settings", "account/positions", "account/lots", "account/orders"}


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


def number(value):
    """Persisted decimal text is a number; missing/invalid is never a zero."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    if isinstance(value, str) and not re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return None
    try:
        result = float(value)
    except (ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def data_limits(payload):
    detail = payload.get('source_data_limits')
    detail = detail if isinstance(detail, dict) else {}
    codes = detail.get('reason_codes')
    # Only stored flags/details are projected. Absence does not prove complete data.
    flagged = payload.get('DATA_LIMITED')
    return {'data_limited': flagged if isinstance(flagged, bool) else None,
            'missing_reason': None if isinstance(flagged, bool) else 'SOURCE_GAP_STATUS_NOT_RECORDED',
            'reason_codes': [v for v in (codes if isinstance(codes, list) else [])
                             if isinstance(v, str) and re.fullmatch(r'[A-Z0-9_:.\-]{1,120}', v)][:30],
            'a6_explicit_review_required': detail.get('a6_explicit_review_required')
                if isinstance(detail.get('a6_explicit_review_required'), bool) else None,
            'source_as_of': scalar(detail.get('source_as_of'), identifier=True)}


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
    if query.get('plan_id') is not None and (kind!='plans' or offset!=0 or not isinstance(query['plan_id'],str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,240}',query['plan_id'])):
        raise ValueError('INVALID_PLAN')
    out = {"schema": "team-ui-readonly/1", "data_mode": "PERSISTED_READ_ONLY",
           "trade_date": day, "observed_at": datetime.now(timezone(timedelta(hours=8))).isoformat(),
           "coverage_kind": "BOUNDED_PERSISTED_SOURCE", "status": "MISSING_SOURCE",
           "rows": None, "total": None, "next_offset": None,
           "missing_reason": "NO_APPROVED_DISPLAY_SOURCE"}
    if kind in {'dashboard','summary','system/health'}:
        from team_ui_dashboard import project_dashboard
        return project_dashboard(root,query,out,project=project,connect=connect,source=source,number=number,scalar=scalar)
    # Source/metric contracts for conversations, lessons and settings are not yet
    # established. Do not scrape raw prompts, arbitrary JSON or .env as a substitute.
    if kind in {"feed", "review/scoreboard", "review/lessons", "settings", "role-stats"}:
        return out
    if kind.startswith('account/'):
        from team_ui_account import project_account
        return project_account(root, query, out, connect=connect, source=source, scalar=scalar, number=number)
    rows = []
    if kind == "events":
        since = query.get('since_id', 0)
        if type(since) is not int or not 0 <= since <= 9007199254740991 or offset:
            raise ValueError('INVALID_CURSOR')
        db = connect(root, 'state/team_v2/model.db')
        try:
            for row in db.execute('SELECT rowid AS cursor_id,call_id,role,session,captured_at,latency_ms,provider_attempts,validated,output_hash FROM model_calls WHERE rowid>? AND captured_at>=? AND captured_at<? ORDER BY rowid LIMIT ?',
                                  (since, day, (date.fromisoformat(day)+timedelta(days=1)).isoformat(), limit+1)):
                original = dict(row)
                rows.append({'cursor_id': row['cursor_id'], 'type':'task', 'id':scalar(row['call_id'],identifier=True),
                             'label':'PM' if 'PM' in row['session'] else scalar(row['role'],identifier=True),
                             'status':'VALIDATED' if row['validated'] else 'VALIDATION_FAILED',
                             'at':scalar(row['captured_at'],identifier=True), 'latency_ms':row['latency_ms'],
                             'provenance':source('model_calls',row['call_id'],json.dumps(original,sort_keys=True))})
        finally:
            db.close()
        page = rows[:limit]
        out.update(status='AVAILABLE',rows=page,next_since_id=page[-1]['cursor_id'] if page else since,
                   has_more=len(rows)>limit,missing_reason=None,page_count=len(page),
                   coverage_kind='MODEL_CALL_JOURNAL_ONLY_NOT_PLAN_TRANSITIONS_OR_FULL_EVENT_STREAM')
        return out
    if kind == "plans":
        db = connect(root, "state/workflow.sqlite3")
        try:
            clause=' AND plan_id=?' if query.get('plan_id') else ''
            next_day=(date.fromisoformat(day)+timedelta(days=1)).isoformat()
            params=[day,next_day,day,next_day]
            if query.get('plan_id'):params.append(query['plan_id'])
            for row in db.execute("SELECT plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json FROM execution_plans WHERE ((valid_from>=? AND valid_from<?) OR (valid_from IS NULL AND expires_at>=? AND expires_at<?))"+clause+" ORDER BY plan_id LIMIT ? OFFSET ?",(*params,1 if query.get('plan_id') else limit+1,0 if query.get('plan_id') else offset)):
                payload = json.loads(row["payload_json"])
                rows.append({"id": scalar(row["plan_id"], identifier=True), "code": scalar(row["symbol"], identifier=True),
                             "name": scalar(payload.get("company_name")) or scalar(payload.get("name")), "status": scalar(row["status"], identifier=True),
                             "strat": scalar(payload.get("strategy_profile"), identifier=True),
                             "zone": [number(payload.get("trigger_low")), number(payload.get("trigger_high"))],
                             "stop": number(payload.get("stop_level")), "cluster": scalar(payload.get("board_group")),
                             "target": next((number(payload[k]) for k in ('first_resistance','pressure_reduce_price','observation_target') if number(payload.get(k)) is not None),None),
                             "minimum_rr": number(payload.get('minimum_reward_risk')),
                             "risk_unit": scalar(payload.get("risk_unit"), identifier=True),
                             "planned_rr": number(payload.get('reward_risk')),
                             "valid_from": scalar(row['valid_from'],identifier=True),"expires_at":scalar(row['expires_at'],identifier=True),
                             "data_limits": data_limits(payload),
                             "day_scope_basis": 'ACTIVATION_DATE' if row['valid_from'] is not None else 'UNACTIVATED_EXPIRY_DATE',
                             "updated_at": scalar(row["updated_at"], identifier=True),
                             "source": source("execution_plans", row["plan_id"], row["payload_json"])})
            if query.get('plan_id') and rows:
                from team_ui_plan_detail import project_detail
                rows[0].update(project_detail(db,day,query['plan_id'],root=root,connect=connect))
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
        # ASCII transport preserves Chinese across Windows CP936 and VM UTF-8.
        print(json.dumps(project(root, query), ensure_ascii=True, allow_nan=False))
    except (OSError, ValueError, KeyError, sqlite3.Error):
        # Stable public code only. Never disclose the database path or raw exception.
        print(json.dumps({"error": "TEAM_UI_SOURCE_UNAVAILABLE"}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
