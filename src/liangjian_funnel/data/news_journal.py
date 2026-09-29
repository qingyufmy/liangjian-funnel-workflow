"""Isolated news page/revision journal. Never writes the production fact store.

Page evidence and cursor advance commit together. A missing article is not a
withdrawal. Source success is not proof that a historical window is complete.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

from .open_news import OpenNewsFetchResult


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _utc(value):
    if value.utcoffset() is None:
        raise ValueError("NEWS_JOURNAL_TIMEZONE_REQUIRED")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


class NewsJournal:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS pages (
              page_hash TEXT PRIMARY KEY, source TEXT NOT NULL, received TEXT NOT NULL,
              cursor_in TEXT NOT NULL, cursor_out TEXT, status TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observations (
              source TEXT NOT NULL, identity TEXT NOT NULL, revision TEXT NOT NULL,
              received TEXT NOT NULL, page_hash TEXT NOT NULL, payload TEXT NOT NULL,
              PRIMARY KEY(page_hash,identity));
            CREATE TABLE IF NOT EXISTS cursors (
              source TEXT PRIMARY KEY, cursor TEXT NOT NULL, last_page TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS news_point_in_time ON observations(source,identity,received);
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def cursor(self, source: str) -> str:
        with self._db() as db:
            row = db.execute("SELECT cursor FROM cursors WHERE source=?", (source,)).fetchone()
        return row[0] if row else ""

    def record_page(self, result: OpenNewsFetchResult, *, received_at: datetime,
                    cursor_in: str = "", next_cursor: str | None = None,
                    end_verified: bool = False) -> dict:
        received = _utc(received_at)
        if received_at < result.fetched_at:
            raise ValueError("NEWS_JOURNAL_RECEIPT_BEFORE_FETCH")
        if any(item.source_id != result.source_id or item.publish_time > received_at for item in result.items):
            raise ValueError("NEWS_JOURNAL_ITEM_SOURCE_OR_TIME_INVALID")
        clean = result.ok and not result.dropped_missing_time and not result.dropped_invalid_items
        status = ("PAGE_FAILED" if not result.ok else "PAGE_PARTIAL" if not clean
                  else "END_VERIFIED" if end_verified else "CONTINUE" if next_cursor and next_cursor != cursor_in
                  else "CURSOR_UNAVAILABLE_OR_STALLED")
        value = {"result": result.model_dump(mode="json"), "received_at": received,
                 "cursor_in": cursor_in, "next_cursor": next_cursor, "status": status}
        digest = _hash(value)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM pages WHERE page_hash=?", (digest,)).fetchone():
                return {"page_hash": digest, "status": status, "reused": True}
            current = db.execute("SELECT cursor FROM cursors WHERE source=?", (result.source_id,)).fetchone()
            if cursor_in != (current[0] if current else ""):
                raise ValueError("NEWS_JOURNAL_CURSOR_CONFLICT")
            if status == "CONTINUE" and db.execute(
                "SELECT 1 FROM pages WHERE source=? AND cursor_in=? LIMIT 1",
                (result.source_id, next_cursor),
            ).fetchone():
                status = "CURSOR_CYCLE"
                value["status"] = status
                digest = _hash(value)
                if db.execute("SELECT 1 FROM pages WHERE page_hash=?", (digest,)).fetchone():
                    return {"page_hash": digest, "status": status, "reused": True}
            db.execute("INSERT INTO pages VALUES(?,?,?,?,?,?,?)", (
                digest, result.source_id, received, cursor_in, next_cursor, status, _json(value)))
            for item in result.items if result.ok else ():
                payload = item.model_dump(mode="json")
                identity = item.provider_item_id or item.url
                revision = _hash({k: payload[k] for k in ("title", "summary", "publish_time", "url", "symbol")})
                db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?)", (
                    item.source_id, identity, revision, received, digest, _json(payload)))
            if status == "CONTINUE":
                db.execute("INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,last_page=excluded.last_page",
                           (result.source_id, next_cursor, digest))
        return {"page_hash": digest, "status": status, "reused": False}

    def visible(self, *, as_of: datetime) -> list[dict]:
        """Latest observed version at cutoff; conflicting same-time revisions stay explicit."""
        with self._db() as db:
            rows = db.execute("""SELECT source,identity,revision,received,page_hash,payload FROM observations
                WHERE received<=? ORDER BY received,page_hash""", (_utc(as_of),)).fetchall()
        groups = {}
        for source, identity, revision, received, page, payload in rows:
            key = (source, identity)
            item = {"source_id": source, "identity": identity, "revision_hash": revision,
                    "received_at": received, "page_hash": page, "payload": json.loads(payload)}
            previous = groups.get(key)
            if previous and previous["received_at"] == received:
                item["same_time_conflict"] = previous.get("same_time_conflict", False) or previous["revision_hash"] != revision
            groups[key] = item
        return [groups[k] for k in sorted(groups)]


def collect_pages(journal: NewsJournal, source: str, fetch_page, *, now, max_pages=3):
    """Adapter supplies verified next cursor/end; never infer end from a short page."""
    if not 1 <= max_pages <= 20:
        raise ValueError("NEWS_PAGE_BUDGET_INVALID")
    seen = set()
    records = []
    for _ in range(max_pages):
        cursor = journal.cursor(source)
        if cursor in seen:
            return {"status": "CURSOR_CYCLE", "pages": records, "complete": False}
        seen.add(cursor)
        result, next_cursor, end_verified = fetch_page(cursor)
        if result.source_id != source:
            raise ValueError("NEWS_SOURCE_MISMATCH")
        record = journal.record_page(result, received_at=now(), cursor_in=cursor,
                                     next_cursor=next_cursor, end_verified=end_verified)
        records.append(record)
        if record["status"] != "CONTINUE":
            return {"status": record["status"], "pages": records,
                    "complete": record["status"] == "END_VERIFIED"}
    return {"status": "PAGE_BUDGET_EXHAUSTED", "pages": records, "complete": False}
