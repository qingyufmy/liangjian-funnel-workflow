"""Durable SQLite cache for normalized market and fundamental facts.

The cache is intentionally provider-neutral.  Adapters should pass normalized
dictionaries rather than persisting provider response envelopes.  It uses only
the Python standard library and is safe for short-lived connections from
multiple threads or processes.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import RLock
from typing import Any


SCHEMA_VERSION = 3
DEFAULT_BATCH_SIZE = 500
_MISSING = object()

_SECRET_KEY_RE = re.compile(
    r"(?:^|[_\-.])(?:api[_\-.]?key|access[_\-.]?token|auth(?:orization)?|"
    r"bearer|secret|password|passwd|private[_\-.]?key|credential|cookie|"
    r"session|token|signature|sign)(?:$|[_\-.])",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)(?P<prefix>bearer\s+|(?:sk|key|token|secret)(?:[_=-]|:\s*))"
    r"(?P<secret>[A-Za-z0-9._~+:\-/=]{8,})"
)
_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&](?:api[_\-.]?key|access[_\-.]?token|token|secret|signature)="
    r")[^&#\s]+"
)

_DAILY_META = frozenset(
    {
        "symbol", "code", "timestamp", "bar_timestamp", "bar_time", "bar_end",
        "time", "datetime", "date", "date_ms", "observed_at",
        "adjust", "adjust_mode", "adjustment",
        "fetched_at", "fetch_time", "fetchTime", "content_hash", "payload",
        "raw", "raw_response", "response", "headers", "request",
    }
)
_FINANCIAL_META = frozenset(
    {
        "symbol", "code", "dataset", "data_set", "report_period", "period",
        "published_at", "publish_time", "publishedAt", "fetched_at",
        "fetch_time", "fetchTime", "content_hash", "version", "revision",
        "payload", "raw", "raw_response", "response", "headers", "request",
    }
)


def _sanitize(value: Any) -> Any:
    """Remove credential-bearing fields and redact credential-like strings."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            name = str(key)
            normalized = name.replace("-", "_").replace(".", "_").lower()
            if (
                _SECRET_KEY_RE.search(normalized)
                or normalized in {
                    "raw", "raw_response", "response_body", "authorization",
                    "headers", "request_headers", "cookies",
                }
            ):
                continue
            result[name] = _sanitize(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_sanitize(item) for item in value]
    if isinstance(value, str):
        query_redacted = _QUERY_SECRET_RE.sub(r"\1[REDACTED]", value)
        return _SECRET_VALUE_RE.sub(r"\g<prefix>[REDACTED]", query_redacted)
    return value


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("JSON values must be finite")
        return value
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise TypeError(f"value of type {type(value).__name__} is not JSON-compatible")


def canonical_json(value: Any) -> str:
    """Encode a value with stable key ordering and no insignificant spaces."""

    return json.dumps(
        _json_value(_sanitize(value)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_hash(value: Any) -> str:
    """Return the SHA-256 hash of canonical JSON."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _timestamp(value: datetime | str | int | float) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Hithink quote responses expose epoch milliseconds as date_ms.
        number = float(value)
        if number >= 100_000_000_000:
            return datetime.fromtimestamp(number / 1000, timezone.utc).isoformat(
                timespec="microseconds"
            )
        if number >= 100_000_000:
            return datetime.fromtimestamp(number, timezone.utc).isoformat(
                timespec="microseconds"
            )
        raise ValueError("numeric timestamp must be epoch seconds or milliseconds")
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid timestamp: {value!r}") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError("timestamp must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _optional_timestamp(value: Any) -> str | None:
    return None if value is None else _timestamp(value)


def _value(row: Mapping[str, Any], *names: str, default: Any = _MISSING) -> Any:
    for name in names:
        if name in row:
            return row[name]
    if default is not _MISSING:
        return default
    raise ValueError(f"missing required field: {names[0]}")


def _symbol(row: Mapping[str, Any]) -> str:
    value = str(_value(row, "symbol", "code")).strip().upper()
    if not value:
        raise ValueError("symbol must not be empty")
    if re.fullmatch(r"\d{6}", value):
        value = f"{value}.SH" if value.startswith(("5", "6")) else f"{value}.SZ"
    return value


def _payload(row: Mapping[str, Any], metadata: frozenset[str]) -> dict[str, Any]:
    supplied = row.get("payload", _MISSING)
    if supplied is not _MISSING:
        if not isinstance(supplied, Mapping):
            raise TypeError("payload must be a mapping")
        value = dict(supplied)
    else:
        value = {key: item for key, item in row.items() if key not in metadata}
    normalized = _json_value(_sanitize(value))
    if not isinstance(normalized, dict):
        raise TypeError("payload must normalize to an object")
    return normalized


def _batch_size(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("batch_size must be a positive integer")
    return value


def _chunks(values: Iterable[Mapping[str, Any]], size: int):
    chunk: list[Mapping[str, Any]] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class LocalFactCache:
    """WAL/FULL SQLite cache with immutable financial revisions."""

    def __init__(self, path: str | Path):
        candidate = Path(path)
        if candidate.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            candidate.mkdir(parents=True, exist_ok=True)
            candidate = candidate / "local_facts.sqlite3"
        else:
            candidate.parent.mkdir(parents=True, exist_ok=True)
        self.path = candidate.resolve()
        self._schema_lock = RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        for attempt in range(7):
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 6:
                    connection.close()
                    raise
                time.sleep(0.05 * (2**attempt))
        return connection

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current > SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema version {current} is newer than supported {SCHEMA_VERSION}"
                )
            if current == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.executescript(
                        """
                        CREATE TABLE IF NOT EXISTS daily_bars (
                            symbol TEXT NOT NULL,
                            bar_timestamp TEXT NOT NULL,
                            adjust TEXT NOT NULL,
                            fetched_at TEXT NOT NULL,
                            content_hash TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            PRIMARY KEY (
                                symbol, bar_timestamp, adjust, content_hash, fetched_at
                            )
                        ) WITHOUT ROWID;
                        CREATE INDEX IF NOT EXISTS idx_daily_incremental
                            ON daily_bars(symbol, adjust, bar_timestamp, fetched_at);
                        CREATE INDEX IF NOT EXISTS idx_daily_fetched
                            ON daily_bars(fetched_at);
                        CREATE TABLE IF NOT EXISTS financial_facts (
                            symbol TEXT NOT NULL,
                            dataset TEXT NOT NULL,
                            report_period TEXT NOT NULL,
                            published_at TEXT NOT NULL,
                            fetched_at TEXT NOT NULL,
                            content_hash TEXT NOT NULL,
                            version TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            PRIMARY KEY(
                                symbol, dataset, report_period, published_at,
                                content_hash, version
                            )
                        ) WITHOUT ROWID;
                        CREATE INDEX IF NOT EXISTS idx_financial_as_of
                            ON financial_facts(symbol, dataset, published_at, fetched_at);
                        CREATE INDEX IF NOT EXISTS idx_financial_period
                            ON financial_facts(symbol, dataset, report_period);
                        CREATE TABLE IF NOT EXISTS sync_state (
                            endpoint TEXT NOT NULL,
                            symbol TEXT NOT NULL,
                            last_success TEXT,
                            cursor_json TEXT,
                            status TEXT NOT NULL,
                            reason TEXT,
                            updated_at TEXT NOT NULL,
                            PRIMARY KEY(endpoint, symbol)
                        ) WITHOUT ROWID;
                        CREATE INDEX IF NOT EXISTS idx_sync_status
                            ON sync_state(status, updated_at);
                        CREATE TABLE IF NOT EXISTS cached_results (
                            namespace TEXT NOT NULL,
                            cache_key TEXT NOT NULL,
                            fetched_at TEXT NOT NULL,
                            expires_at TEXT NOT NULL,
                            content_hash TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            PRIMARY KEY(
                                namespace, cache_key, content_hash, fetched_at
                            )
                        ) WITHOUT ROWID;
                        CREATE INDEX IF NOT EXISTS idx_cached_results_lookup
                            ON cached_results(namespace, cache_key, expires_at, fetched_at);
                        """
                    )
                    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
                    connection.execute("PRAGMA user_version=3")
                    connection.execute(
                        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                        "VALUES(3, ?)",
                        (now,),
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            elif current == 1:
                # Version 1 used the natural bar key alone and consequently
                # overwrote a corrected observation.  Rebuild only that table;
                # all existing rows remain valid revision-1 observations.
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute("DROP INDEX IF EXISTS idx_daily_incremental")
                    connection.execute("DROP INDEX IF EXISTS idx_daily_fetched")
                    connection.execute("ALTER TABLE daily_bars RENAME TO daily_bars_v1")
                    connection.execute(
                        """
                        CREATE TABLE daily_bars (
                            symbol TEXT NOT NULL,
                            bar_timestamp TEXT NOT NULL,
                            adjust TEXT NOT NULL,
                            fetched_at TEXT NOT NULL,
                            content_hash TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            PRIMARY KEY(
                                symbol, bar_timestamp, adjust, content_hash, fetched_at
                            )
                        ) WITHOUT ROWID
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO daily_bars(
                            symbol, bar_timestamp, adjust, fetched_at, content_hash, payload_json
                        )
                        SELECT symbol, bar_timestamp, adjust, fetched_at, content_hash, payload_json
                        FROM daily_bars_v1
                        """
                    )
                    connection.execute("DROP TABLE daily_bars_v1")
                    connection.execute(
                        "CREATE INDEX idx_daily_incremental "
                        "ON daily_bars(symbol, adjust, bar_timestamp, fetched_at)"
                    )
                    connection.execute(
                        "CREATE INDEX idx_daily_fetched ON daily_bars(fetched_at)"
                    )
                    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
                    connection.execute(
                        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                        "VALUES(2, ?)",
                        (now,),
                    )
                    connection.execute("PRAGMA user_version=2")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            if current in {1, 2}:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.executescript(
                        """
                        CREATE TABLE IF NOT EXISTS cached_results (
                            namespace TEXT NOT NULL,
                            cache_key TEXT NOT NULL,
                            fetched_at TEXT NOT NULL,
                            expires_at TEXT NOT NULL,
                            content_hash TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            PRIMARY KEY(
                                namespace, cache_key, content_hash, fetched_at
                            )
                        ) WITHOUT ROWID;
                        CREATE INDEX IF NOT EXISTS idx_cached_results_lookup
                            ON cached_results(namespace, cache_key, expires_at, fetched_at);
                        """
                    )
                    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
                    connection.execute(
                        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                        "VALUES(3, ?)",
                        (now,),
                    )
                    connection.execute("PRAGMA user_version=3")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

    @staticmethod
    def _daily_row(row: Mapping[str, Any]) -> dict[str, Any]:
        payload = _payload(row, _DAILY_META)
        supplied_hash = _value(row, "content_hash", default=None)
        content_hash = str(supplied_hash).strip() if supplied_hash else canonical_json_hash(payload)
        if not re.fullmatch(r"[0-9a-fA-F]{64}", content_hash):
            raise ValueError("content_hash must be a SHA-256 hex digest")
        fetched = _optional_timestamp(
            _value(
                row, "fetched_at", "fetch_time", "fetchTime",
                "observed_at", default=None
            )
        ) or datetime.now(timezone.utc).isoformat(timespec="microseconds")
        timestamp = _timestamp(
            _value(
                row, "timestamp", "bar_timestamp", "bar_time", "bar_end",
                "time", "datetime", "date", "date_ms", "observed_at"
            )
        )
        adjust = str(
            _value(row, "adjust", "adjust_mode", "adjustment", default="none")
        ).strip().lower()
        if not adjust:
            raise ValueError("adjust must not be empty")
        return {
            "symbol": _symbol(row), "timestamp": timestamp, "adjust": adjust,
            "fetched_at": fetched, "content_hash": content_hash.lower(),
            "payload": payload,
        }

    @staticmethod
    def _financial_row(row: Mapping[str, Any]) -> dict[str, Any]:
        payload = _payload(row, _FINANCIAL_META)
        supplied_hash = _value(row, "content_hash", default=None)
        content_hash = str(supplied_hash).strip() if supplied_hash else canonical_json_hash(payload)
        if not re.fullmatch(r"[0-9a-fA-F]{64}", content_hash):
            raise ValueError("content_hash must be a SHA-256 hex digest")
        published_at = _timestamp(_value(row, "published_at", "publish_time", "publishedAt"))
        fetched_at = _optional_timestamp(
            _value(row, "fetched_at", "fetch_time", "fetchTime", default=None)
        ) or datetime.now(timezone.utc).isoformat(timespec="microseconds")
        report_period = str(_value(row, "report_period", "period")).strip()
        dataset = str(_value(row, "dataset", "data_set")).strip()
        version = str(_value(row, "version", "revision", default="0")).strip() or "0"
        if not report_period or not dataset:
            raise ValueError("dataset and report_period must not be empty")
        return {
            "symbol": _symbol(row), "dataset": dataset,
            "report_period": report_period, "published_at": published_at,
            "fetched_at": fetched_at, "content_hash": content_hash.lower(),
            "version": version, "payload": payload,
        }

    @staticmethod
    def _json_loads(value: str) -> Any:
        return json.loads(canonical_json(json.loads(value)))

    @staticmethod
    def _daily_output(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "symbol": row["symbol"], "timestamp": row["bar_timestamp"],
            "adjust": row["adjust"], "fetched_at": row["fetched_at"],
            "content_hash": row["content_hash"],
            "payload": LocalFactCache._json_loads(row["payload_json"]),
        }

    @staticmethod
    def _financial_output(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "symbol": row["symbol"], "dataset": row["dataset"],
            "report_period": row["report_period"], "published_at": row["published_at"],
            "fetched_at": row["fetched_at"], "content_hash": row["content_hash"],
            "version": row["version"],
            "payload": LocalFactCache._json_loads(row["payload_json"]),
        }

    def upsert_daily_bars(
        self, rows: Iterable[Mapping[str, Any]], *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> dict[str, int]:
        """Insert bar observations keyed by natural key, hash and observed time.

        Each bounded batch is validated before BEGIN IMMEDIATE and committed
        independently.  A correction is retained as a new revision; replaying
        the same observation is counted as unchanged.
        """

        size = _batch_size(batch_size)
        result = {"inserted": 0, "updated": 0, "unchanged": 0, "batches": 0}
        for raw_batch in _chunks(rows, size):
            normalized: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
            for raw in raw_batch:
                row = self._daily_row(raw)
                normalized[
                    (
                        row["symbol"], row["timestamp"], row["adjust"],
                        row["content_hash"], row["fetched_at"],
                    )
                ] = row
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                for row in normalized.values():
                    key = (
                        row["symbol"], row["timestamp"], row["adjust"],
                        row["content_hash"], row["fetched_at"],
                    )
                    payload_json = canonical_json(row["payload"])
                    existing = connection.execute(
                        "SELECT 1 FROM daily_bars WHERE "
                        "symbol=? AND bar_timestamp=? AND adjust=? "
                        "AND content_hash=? AND fetched_at=?",
                        key,
                    ).fetchone()
                    connection.execute(
                        """
                        INSERT INTO daily_bars(
                            symbol, bar_timestamp, adjust, fetched_at,
                            content_hash, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(
                            symbol, bar_timestamp, adjust, content_hash, fetched_at
                        ) DO NOTHING
                        """,
                        (
                            row["symbol"], row["timestamp"], row["adjust"],
                            row["fetched_at"], row["content_hash"], payload_json,
                        ),
                    )
                    if existing is None:
                        result["inserted"] += 1
                    else:
                        result["unchanged"] += 1
                connection.commit()
                result["batches"] += 1
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        return result

    write_daily_bars = upsert_daily_bars

    def query_daily_bars(
        self,
        symbol: str | None = None,
        *,
        adjust: str | None = None,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        after: datetime | str | None = None,
        as_of: datetime | str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Query bars; start is inclusive and end/after are exclusive."""

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer")
        clauses: list[str] = []
        params: list[Any] = []
        if symbol is not None:
            clauses.append("symbol=?")
            params.append(_symbol({"symbol": symbol}))
        if adjust is not None:
            clauses.append("adjust=?")
            params.append(str(adjust).strip().lower())
        if start is not None:
            clauses.append("bar_timestamp>=?")
            params.append(_timestamp(start))
        if end is not None:
            clauses.append("bar_timestamp<?")
            params.append(_timestamp(end))
        if after is not None:
            clauses.append("bar_timestamp>?")
            params.append(_timestamp(after))
        if as_of is not None:
            clauses.append("fetched_at<=?")
            params.append(_timestamp(as_of))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = "DESC" if descending else "ASC"
        query = (
            "WITH ranked AS ("
            " SELECT symbol, bar_timestamp, adjust, fetched_at, content_hash, payload_json,"
            " ROW_NUMBER() OVER ("
            " PARTITION BY symbol, bar_timestamp, adjust"
            " ORDER BY fetched_at DESC, content_hash DESC"
            f" ) AS revision_rank FROM daily_bars{where}"
            " ) SELECT symbol, bar_timestamp, adjust, fetched_at, content_hash, payload_json"
            f" FROM ranked WHERE revision_rank=1"
            f" ORDER BY bar_timestamp {order}, symbol {order}"
        )
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._daily_output(row) for row in rows]

    def incremental_daily_bars(
        self, symbol: str, *, adjust: str = "none",
        after: datetime | str | None = None, as_of: datetime | str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return bars after a supplied or persisted high-water timestamp."""

        return self.query_daily_bars(
            symbol, adjust=adjust, after=after, as_of=as_of, limit=limit
        )

    def latest_daily_bar(
        self, symbol: str, *, adjust: str = "none",
        as_of: datetime | str | None = None,
    ) -> dict[str, Any] | None:
        """Return the latest bar available under the knowledge cut-off."""

        rows = self.query_daily_bars(
            symbol, adjust=adjust, as_of=as_of, limit=1, descending=True
        )
        return rows[0] if rows else None

    def upsert_financial_facts(
        self, rows: Iterable[Mapping[str, Any]], *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> dict[str, int]:
        """Insert immutable financial observations and retain every revision."""

        size = _batch_size(batch_size)
        result = {"inserted": 0, "unchanged": 0, "batches": 0}
        for raw_batch in _chunks(rows, size):
            normalized: dict[
                tuple[str, str, str, str, str, str], dict[str, Any]
            ] = {}
            for raw in raw_batch:
                row = self._financial_row(raw)
                key = (
                    row["symbol"], row["dataset"], row["report_period"],
                    row["published_at"], row["content_hash"], row["version"],
                )
                normalized[key] = row
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                for key, row in normalized.items():
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO financial_facts(
                            symbol, dataset, report_period, published_at, fetched_at,
                            content_hash, version, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            *key[:4], row["fetched_at"], key[4], key[5],
                            canonical_json(row["payload"]),
                        ),
                    )
                    if cursor.rowcount == 1:
                        result["inserted"] += 1
                    else:
                        result["unchanged"] += 1
                connection.commit()
                result["batches"] += 1
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        return result

    write_financial_facts = upsert_financial_facts

    def query_financial_facts(
        self,
        symbol: str | None = None,
        *,
        dataset: str | None = None,
        report_period: str | None = None,
        as_of: datetime | str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Query filings, applying published_at and fetched_at cut-offs."""

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer")
        clauses: list[str] = []
        params: list[Any] = []
        if symbol is not None:
            clauses.append("symbol=?")
            params.append(_symbol({"symbol": symbol}))
        if dataset is not None:
            clauses.append("dataset=?")
            params.append(str(dataset).strip())
        if report_period is not None:
            clauses.append("report_period=?")
            params.append(str(report_period).strip())
        if as_of is not None:
            cutoff = _timestamp(as_of)
            clauses.extend(("published_at<=?", "fetched_at<=?"))
            params.extend((cutoff, cutoff))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = "DESC" if descending else "ASC"
        query = (
            "SELECT symbol, dataset, report_period, published_at, fetched_at, "
            f"content_hash, version, payload_json FROM financial_facts{where} "
            f"ORDER BY published_at {order}, fetched_at {order}, version {order}, "
            f"content_hash {order}, symbol {order}, dataset {order}"
        )
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._financial_output(row) for row in rows]

    def latest_financial_fact(
        self, symbol: str, dataset: str, *, report_period: str | None = None,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any] | None:
        """Return the latest published revision known at the cut-off."""

        rows = self.query_financial_facts(
            symbol, dataset=dataset, report_period=report_period,
            as_of=as_of, limit=1, descending=True,
        )
        return rows[0] if rows else None

    def update_sync_state(
        self,
        endpoint: str,
        symbol: str | None = None,
        *,
        last_success: datetime | str | None | object = _MISSING,
        cursor: Any = _MISSING,
        status: str = "ok",
        reason: str | None | object = _MISSING,
    ) -> dict[str, Any]:
        """Commit endpoint/symbol cursor and status, retaining omitted fields."""

        endpoint = _sanitize(str(endpoint).strip())
        normalized_symbol = "" if symbol is None else str(symbol).strip().upper()
        status = str(status).strip()
        if not endpoint or not status:
            raise ValueError("endpoint and status must not be empty")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT last_success, cursor_json, status, reason FROM sync_state "
                "WHERE endpoint=? AND symbol=?",
                (endpoint, normalized_symbol),
            ).fetchone()
            previous = existing
            last_value = (
                previous["last_success"] if last_success is _MISSING
                and previous is not None else (
                    None if last_success is _MISSING else _optional_timestamp(last_success)
                )
            )
            cursor_value = (
                previous["cursor_json"] if cursor is _MISSING and previous is not None
                else (None if cursor is _MISSING or cursor is None else canonical_json(cursor))
            )
            reason_value = (
                previous["reason"] if reason is _MISSING and previous is not None
                else (
                    None
                    if reason is _MISSING or reason is None
                    else _sanitize(str(reason))
                )
            )
            now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            connection.execute(
                """
                INSERT INTO sync_state(
                    endpoint, symbol, last_success, cursor_json, status, reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(endpoint, symbol) DO UPDATE SET
                    last_success=excluded.last_success, cursor_json=excluded.cursor_json,
                    status=excluded.status, reason=excluded.reason, updated_at=excluded.updated_at
                """,
                (
                    endpoint, normalized_symbol, last_value, cursor_value,
                    status, reason_value, now,
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_sync_state(endpoint, symbol)

    def put_cached_result(
        self,
        namespace: str,
        cache_key: str,
        payload: Mapping[str, Any],
        *,
        fetched_at: datetime | str,
        expires_at: datetime | str,
    ) -> dict[str, Any]:
        """Persist one immutable, redacted provider-result revision."""

        normalized_namespace = str(_sanitize(namespace)).strip()
        normalized_key = str(_sanitize(cache_key)).strip()
        if not normalized_namespace or not normalized_key:
            raise ValueError("namespace and cache_key must not be empty")
        fetched = _timestamp(fetched_at)
        expires = _timestamp(expires_at)
        if expires < fetched:
            raise ValueError("expires_at must not precede fetched_at")
        normalized_payload = _json_value(_sanitize(payload))
        if not isinstance(normalized_payload, dict):
            raise TypeError("payload must normalize to an object")
        digest = canonical_json_hash(normalized_payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO cached_results(
                        namespace, cache_key, fetched_at, expires_at,
                        content_hash, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_namespace,
                        normalized_key,
                        fetched,
                        expires,
                        digest,
                        canonical_json(normalized_payload),
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return {
            "namespace": normalized_namespace,
            "cache_key": normalized_key,
            "fetched_at": fetched,
            "expires_at": expires,
            "content_hash": digest,
            "payload": normalized_payload,
        }

    def get_cached_result(
        self,
        namespace: str,
        cache_key: str,
        *,
        as_of: datetime | str | None = None,
        fresh_at: datetime | str | None = None,
    ) -> dict[str, Any] | None:
        """Return the latest revision known at ``as_of`` and still fresh."""

        clauses = ["namespace=?", "cache_key=?"]
        params: list[Any] = [
            str(_sanitize(namespace)).strip(),
            str(_sanitize(cache_key)).strip(),
        ]
        if as_of is not None:
            clauses.append("fetched_at<=?")
            params.append(_timestamp(as_of))
        if fresh_at is not None:
            clauses.append("expires_at>=?")
            params.append(_timestamp(fresh_at))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT namespace, cache_key, fetched_at, expires_at, "
                "content_hash, payload_json FROM cached_results WHERE "
                + " AND ".join(clauses)
                + " ORDER BY fetched_at DESC, content_hash DESC LIMIT 1",
                params,
            ).fetchone()
        if row is None:
            return None
        return {
            "namespace": row["namespace"],
            "cache_key": row["cache_key"],
            "fetched_at": row["fetched_at"],
            "expires_at": row["expires_at"],
            "content_hash": row["content_hash"],
            "payload": self._json_loads(row["payload_json"]),
        }

    def get_cached_results(
        self,
        namespace: str,
        cache_keys: Iterable[str],
        *,
        as_of: datetime | str | None = None,
        fresh_at: datetime | str | None = None,
        chunk_size: int = 500,
    ) -> dict[str, dict[str, Any]]:
        """Return the newest valid revision for many keys in bounded queries.

        SQLite limits the number of bound parameters per statement.  The
        caller may therefore pass the whole document universe; this method
        chunks the ``IN`` predicates and merges the rows into one key mapping.
        Filters are applied before ranking, matching :meth:`get_cached_result`:
        an expired revision cannot mask an older still-fresh revision.
        Duplicate keys are queried once and the returned mapping is
        deterministic.
        """

        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or not 1 <= chunk_size <= 900:
            raise ValueError("chunk_size must be between 1 and 900")
        normalized_namespace = str(_sanitize(namespace)).strip()
        if not normalized_namespace:
            raise ValueError("namespace must not be empty")
        normalized_keys: list[str] = []
        seen: set[str] = set()
        for raw_key in cache_keys:
            key = str(_sanitize(raw_key)).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            normalized_keys.append(key)
        if not normalized_keys:
            return {}
        as_of_value = None if as_of is None else _timestamp(as_of)
        fresh_at_value = None if fresh_at is None else _timestamp(fresh_at)
        result: dict[str, dict[str, Any]] = {}
        with self._connect() as connection:
            # Namespace and both optional timestamps consume three parameters;
            # keep the default comfortably below SQLite's common 999 limit.
            for offset in range(0, len(normalized_keys), chunk_size):
                chunk = normalized_keys[offset : offset + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                clauses = [f"namespace=?", f"cache_key IN ({placeholders})"]
                params: list[Any] = [normalized_namespace, *chunk]
                if as_of_value is not None:
                    clauses.append("fetched_at<=?")
                    params.append(as_of_value)
                if fresh_at_value is not None:
                    clauses.append("expires_at>=?")
                    params.append(fresh_at_value)
                rows = connection.execute(
                    "WITH ranked AS ("
                    " SELECT namespace, cache_key, fetched_at, expires_at, content_hash, payload_json,"
                    " ROW_NUMBER() OVER ("
                    " PARTITION BY cache_key ORDER BY fetched_at DESC, content_hash DESC"
                    ") AS revision_rank"
                    " FROM cached_results WHERE "
                    + " AND ".join(clauses)
                    + ") SELECT namespace, cache_key, fetched_at, expires_at, content_hash, payload_json"
                    " FROM ranked WHERE revision_rank=1",
                    params,
                ).fetchall()
                for row in rows:
                    result[row["cache_key"]] = {
                        "namespace": row["namespace"],
                        "cache_key": row["cache_key"],
                        "fetched_at": row["fetched_at"],
                        "expires_at": row["expires_at"],
                        "content_hash": row["content_hash"],
                        "payload": self._json_loads(row["payload_json"]),
                    }
        # Dict insertion order follows caller order even if SQLite returns a
        # different row order, which makes downstream cache accounting stable.
        return {key: result[key] for key in normalized_keys if key in result}

    def get_sync_state(
        self, endpoint: str, symbol: str | None = None
    ) -> dict[str, Any] | None:
        """Read one synchronization state as a plain dictionary."""

        normalized_symbol = "" if symbol is None else str(symbol).strip().upper()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT endpoint, symbol, last_success, cursor_json, status, reason, updated_at "
                "FROM sync_state WHERE endpoint=? AND symbol=?",
            (_sanitize(str(endpoint).strip()), normalized_symbol),
            ).fetchone()
        return self._sync_output(row) if row is not None else None

    def list_sync_state(
        self, endpoint: str | None = None, *, symbol: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """List endpoint/symbol synchronization records."""

        clauses: list[str] = []
        params: list[Any] = []
        if endpoint is not None:
            clauses.append("endpoint=?")
            params.append(_sanitize(str(endpoint).strip()))
        if symbol is not None:
            clauses.append("symbol=?")
            params.append(str(symbol).strip().upper())
        if status is not None:
            clauses.append("status=?")
            params.append(str(status).strip())
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT endpoint, symbol, last_success, cursor_json, status, reason, updated_at "
                f"FROM sync_state{where} ORDER BY endpoint ASC, symbol ASC",
                params,
            ).fetchall()
        return [self._sync_output(row) for row in rows]

    @staticmethod
    def _sync_output(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "endpoint": row["endpoint"], "symbol": row["symbol"] or None,
            "last_success": row["last_success"],
            "cursor": (
                None if row["cursor_json"] is None
                else json.loads(canonical_json(json.loads(row["cursor_json"])))
            ),
            "status": row["status"], "reason": row["reason"],
            "updated_at": row["updated_at"],
        }

    def get_coverage(
        self,
        *,
        symbol: str | None = None,
        dataset: str | None = None,
        adjust: str | None = None,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Return rows, symbols, datasets and time ranges for readiness checks."""

        daily_clauses: list[str] = []
        daily_params: list[Any] = []
        financial_clauses: list[str] = []
        financial_params: list[Any] = []
        if symbol is not None:
            normalized_symbol = _symbol({"symbol": symbol})
            daily_clauses.append("symbol=?")
            daily_params.append(normalized_symbol)
            financial_clauses.append("symbol=?")
            financial_params.append(normalized_symbol)
        if adjust is not None:
            daily_clauses.append("adjust=?")
            daily_params.append(str(adjust).strip().lower())
        if dataset is not None:
            financial_clauses.append("dataset=?")
            financial_params.append(str(dataset).strip())
        if as_of is not None:
            cutoff = _timestamp(as_of)
            daily_clauses.append("fetched_at<=?")
            daily_params.append(cutoff)
            financial_clauses.extend(("published_at<=?", "fetched_at<=?"))
            financial_params.extend((cutoff, cutoff))
        daily_where = f" WHERE {' AND '.join(daily_clauses)}" if daily_clauses else ""
        financial_where = (
            f" WHERE {' AND '.join(financial_clauses)}" if financial_clauses else ""
        )
        with self._connect() as connection:
            daily = connection.execute(
                "SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols, "
                "MIN(bar_timestamp) AS min_timestamp, MAX(bar_timestamp) AS max_timestamp "
                "FROM ("
                " SELECT symbol, bar_timestamp, adjust FROM daily_bars"
                f"{daily_where} GROUP BY symbol, bar_timestamp, adjust"
                ") AS distinct_bars",
                daily_params,
            ).fetchone()
            financial = connection.execute(
                "SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols, "
                "COUNT(DISTINCT dataset) AS datasets, MIN(published_at) AS min_published_at, "
                "MAX(published_at) AS max_published_at "
                f"FROM financial_facts{financial_where}",
                financial_params,
            ).fetchone()
        return {
            "schema_version": SCHEMA_VERSION,
            "as_of": None if as_of is None else _timestamp(as_of),
            "daily": {
                "rows": int(daily["rows"]), "symbols": int(daily["symbols"]),
                "min_timestamp": daily["min_timestamp"], "max_timestamp": daily["max_timestamp"],
            },
            "financial": {
                "rows": int(financial["rows"]), "symbols": int(financial["symbols"]),
                "datasets": int(financial["datasets"]),
                "min_published_at": financial["min_published_at"],
                "max_published_at": financial["max_published_at"],
            },
        }

    coverage = get_coverage

    def stats(self, **filters: Any) -> dict[str, Any]:
        """Alias for get_coverage used by synchronizer readiness checks."""

        return self.get_coverage(**filters)


__all__ = [
    "DEFAULT_BATCH_SIZE", "LocalFactCache", "SCHEMA_VERSION",
    "canonical_json", "canonical_json_hash",
]
