from __future__ import annotations

import sqlite3
import hashlib
import json
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from .mootdx import MinuteBar, map_symbol


SHANGHAI = ZoneInfo("Asia/Shanghai")


_MARKET_FIELD_NAMES = (
    "symbol",
    "interval",
    "bar_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjust_mode",
)


class CacheConflictError(RuntimeError):
    reason_code = "MINUTE_CACHE_CONFLICT"

    def __init__(
        self,
        *,
        symbol: str | None = None,
        interval: str | None = None,
        bar_end: str | None = None,
        differing_fields: Iterable[str] = (),
    ) -> None:
        # Keep the exception message stable and free of market payloads.  The
        # bounded fields below are diagnostics only; values are never included.
        self.symbol = symbol
        self.interval = interval
        self.bar_end = bar_end
        self.differing_fields = tuple(
            field for field in differing_fields if field in _MARKET_FIELD_NAMES
        )
        super().__init__(self.reason_code)

    @property
    def diagnostics(self) -> dict[str, object]:
        """Return safe conflict metadata without exposing either bar payload."""

        result: dict[str, object] = {"reason_code": self.reason_code}
        if self.symbol is not None:
            result["symbol"] = self.symbol
        if self.interval is not None:
            result["interval"] = self.interval
        if self.bar_end is not None:
            result["bar_end"] = self.bar_end
        if self.differing_fields:
            result["differing_fields"] = self.differing_fields
        return result


class CacheWriteResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    inserted: int = Field(ge=0)
    unchanged: int = Field(ge=0)
    revised: int = Field(default=0, ge=0)
    overlap_conflicts: int = Field(default=0, ge=0)
    skipped_future: int = Field(default=0, ge=0)


class MinuteBarStore:
    """Transactional SQLite cache with explicit live/replay write modes.

    ``write`` is the strict, point-in-time path used by offline/replay data:
    an already persisted market observation is immutable and a conflicting
    observation raises :class:`CacheConflictError`.  Live providers commonly
    return an overlapping window and may revise the last forming bar or use a
    different node for the overlap.  ``write_live`` therefore appends only
    new, closed observations, retains the local canonical value for overlap
    conflicts, and records a safe diagnostic without poisoning the monitor.
    """

    def __init__(self, directory: Path):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "minute_bars.sqlite3"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15)
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS minute_bars (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL CHECK (interval IN ('1m', '5m')),
                    bar_end TEXT NOT NULL,
                    open_value TEXT NOT NULL,
                    high_value TEXT NOT NULL,
                    low_value TEXT NOT NULL,
                    close_value TEXT NOT NULL,
                    volume_value TEXT NOT NULL,
                    amount_value TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    adjust_mode TEXT NOT NULL CHECK (adjust_mode IN ('none', 'raw')),
                    PRIMARY KEY (symbol, interval, bar_end)
                ) WITHOUT ROWID
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS minute_bar_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL CHECK (interval IN ('1m', '5m')),
                    bar_end TEXT NOT NULL,
                    event_type TEXT NOT NULL CHECK (event_type IN ('REVISION', 'OVERLAP_CONFLICT', 'SKIPPED_FUTURE')),
                    revision_number INTEGER,
                    observed_at TEXT NOT NULL,
                    old_digest TEXT,
                    new_digest TEXT,
                    source_id TEXT NOT NULL,
                    differing_fields TEXT NOT NULL DEFAULT '[]',
                    dedupe_key TEXT NOT NULL DEFAULT ''
                )
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(minute_bar_audit)").fetchall()
            }
            if "dedupe_key" not in columns:
                connection.execute(
                    "ALTER TABLE minute_bar_audit ADD COLUMN dedupe_key TEXT NOT NULL DEFAULT ''"
                )
                connection.execute(
                    """
                    UPDATE minute_bar_audit
                    SET dedupe_key = event_type || '|' || symbol || '|' || interval || '|' || bar_end || '|' ||
                      COALESCE(old_digest, '-') || '|' || COALESCE(new_digest, '-') || '|' || audit_id
                    WHERE dedupe_key=''
                    """
                )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_minute_bar_audit_dedupe
                ON minute_bar_audit(dedupe_key)
                """
            )

    def write(
        self,
        bars: Iterable[MinuteBar],
        *,
        allow_revisions_for: date | datetime | None = None,
        observed_at: datetime | None = None,
    ) -> CacheWriteResult:
        """Write strict observations, optionally allowing current-day revisions.

        The optional revision date is deliberately explicit.  It is intended
        for a caller that has independently established the current trading
        date; the default remains immutable so historical/replay writes keep
        detecting source drift.  Every accepted revision is recorded in the
        audit ledger with digests and field names, never raw market values.
        """
        values = tuple(bars)
        if not values:
            return CacheWriteResult(inserted=0, unchanged=0)
        revision_date = _revision_date(allow_revisions_for)
        observed = _observed_at(observed_at)
        inserted = 0
        unchanged = 0
        revised = 0
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for bar in values:
                payload = _payload(bar)
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO minute_bars (
                        symbol, interval, bar_end, open_value, high_value, low_value,
                        close_value, volume_value, amount_value, source_id, adjust_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                if cursor.rowcount == 1:
                    inserted += 1
                    continue
                existing = connection.execute(
                    """
                    SELECT symbol, interval, bar_end, open_value, high_value, low_value,
                           close_value, volume_value, amount_value, source_id, adjust_mode
                    FROM minute_bars
                    WHERE symbol=? AND interval=? AND bar_end=?
                    """,
                    (payload[0], payload[1], payload[2]),
                ).fetchone()
                if existing is None:
                    differing_fields = ("row_missing_after_insert_ignore",)
                else:
                    differing_fields = _differing_fields(existing, payload)
                if existing is None or differing_fields:
                    if (
                        existing is not None
                        and revision_date is not None
                        and _bar_date(payload) == revision_date
                    ):
                        self._replace_existing(
                            connection,
                            existing,
                            payload,
                            observed_at=observed,
                            differing_fields=differing_fields,
                        )
                        revised += 1
                        continue
                    raise CacheConflictError(
                        symbol=payload[0],
                        interval=payload[1],
                        bar_end=payload[2],
                        differing_fields=differing_fields,
                    )
                unchanged += 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return CacheWriteResult(inserted=inserted, unchanged=unchanged, revised=revised)

    def write_live(
        self,
        bars: Iterable[MinuteBar],
        *,
        as_of: datetime,
    ) -> CacheWriteResult:
        """Append a live closed-bar snapshot without failing on overlaps.

        The provider window is intentionally not allowed to rewrite the local
        canonical value.  This makes a Tencent/MootDX overlap safe even when
        the source revises a recent row or two public nodes disagree.  A
        caller that needs an explicit current-day correction can use
        ``write(..., allow_revisions_for=...)`` instead.

        Rows later than ``as_of`` are not persisted.  The adapter normally
        performs this filter too, but keeping the invariant in the cache is
        important because provider responses can race the minute boundary.
        """
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        values = tuple(bars)
        if not values:
            return CacheWriteResult(inserted=0, unchanged=0)
        cutoff = as_of
        observed = _observed_at(as_of)
        inserted = unchanged = overlap_conflicts = skipped_future = 0
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for bar in values:
                payload = _payload(bar)
                if bar.bar_end > cutoff:
                    self._insert_audit(
                        connection,
                        payload,
                        event_type="SKIPPED_FUTURE",
                        observed_at=observed,
                        old_digest=None,
                        new_digest=_market_digest(payload),
                        differing_fields=(),
                    )
                    skipped_future += 1
                    continue
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO minute_bars (
                        symbol, interval, bar_end, open_value, high_value, low_value,
                        close_value, volume_value, amount_value, source_id, adjust_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                if cursor.rowcount == 1:
                    inserted += 1
                    continue
                existing = connection.execute(
                    """
                    SELECT symbol, interval, bar_end, open_value, high_value, low_value,
                           close_value, volume_value, amount_value, source_id, adjust_mode
                    FROM minute_bars
                    WHERE symbol=? AND interval=? AND bar_end=?
                    """,
                    (payload[0], payload[1], payload[2]),
                ).fetchone()
                differing_fields = (
                    ("row_missing_after_insert_ignore",)
                    if existing is None
                    else _differing_fields(existing, payload)
                )
                if existing is None:
                    raise CacheConflictError(
                        symbol=payload[0],
                        interval=payload[1],
                        bar_end=payload[2],
                        differing_fields=differing_fields,
                    )
                if differing_fields:
                    self._insert_audit(
                        connection,
                        payload,
                        event_type="OVERLAP_CONFLICT",
                        observed_at=observed,
                        old_digest=_market_digest(existing),
                        new_digest=_market_digest(payload),
                        differing_fields=differing_fields,
                    )
                    overlap_conflicts += 1
                else:
                    unchanged += 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return CacheWriteResult(
            inserted=inserted,
            unchanged=unchanged,
            overlap_conflicts=overlap_conflicts,
            skipped_future=skipped_future,
        )

    def _replace_existing(
        self,
        connection: sqlite3.Connection,
        existing: tuple[str, ...] | sqlite3.Row,
        payload: tuple[str, ...],
        *,
        observed_at: str,
        differing_fields: Iterable[str],
    ) -> None:
        revision_number = int(
            connection.execute(
                """
                SELECT COALESCE(MAX(revision_number), 0) + 1
                FROM minute_bar_audit
                WHERE symbol=? AND interval=? AND bar_end=? AND event_type='REVISION'
                """,
                (payload[0], payload[1], payload[2]),
            ).fetchone()[0]
        )
        connection.execute(
            """
            UPDATE minute_bars SET open_value=?,high_value=?,low_value=?,close_value=?,
              volume_value=?,amount_value=?,source_id=?,adjust_mode=?
            WHERE symbol=? AND interval=? AND bar_end=?
            """,
            (
                payload[3], payload[4], payload[5], payload[6], payload[7],
                payload[8], payload[9], payload[10], payload[0], payload[1], payload[2],
            ),
        )
        self._insert_audit(
            connection,
            payload,
            event_type="REVISION",
            revision_number=revision_number,
            observed_at=observed_at,
            old_digest=_market_digest(existing),
            new_digest=_market_digest(payload),
            differing_fields=differing_fields,
        )

    @staticmethod
    def _insert_audit(
        connection: sqlite3.Connection,
        payload: tuple[str, ...],
        *,
        event_type: str,
        observed_at: str,
        revision_number: int | None = None,
        old_digest: str | None,
        new_digest: str | None,
        differing_fields: Iterable[str],
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO minute_bar_audit(
              symbol,interval,bar_end,event_type,revision_number,observed_at,
              old_digest,new_digest,source_id,differing_fields,dedupe_key
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                payload[0], payload[1], payload[2], event_type, revision_number,
                observed_at, old_digest, new_digest, payload[9],
                json.dumps(tuple(differing_fields), ensure_ascii=False, separators=(",", ":")),
                "|".join(
                    (
                        event_type,
                        payload[0],
                        payload[1],
                        payload[2],
                        old_digest or "-",
                        new_digest or "-",
                    )
                ),
            ),
        )

    def load_latest(self, symbol: str, interval: str, *, limit: int) -> tuple[MinuteBar, ...]:
        canonical = map_symbol(symbol).canonical
        if interval not in {"1m", "5m"}:
            raise ValueError("interval must be 1m or 5m")
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT symbol, interval, bar_end, open_value, high_value, low_value,
                       close_value, volume_value, amount_value, source_id, adjust_mode
                FROM minute_bars
                WHERE symbol=? AND interval=?
                ORDER BY bar_end DESC
                LIMIT ?
                """,
                (canonical, interval, limit),
            ).fetchall()
        return tuple(_from_row(row) for row in reversed(rows))


def _number_text(value: float) -> str:
    return repr(float(value))


def _payload(bar: MinuteBar) -> tuple[str, ...]:
    return (
        bar.symbol,
        bar.interval,
        bar.bar_end.isoformat(),
        _number_text(bar.open),
        _number_text(bar.high),
        _number_text(bar.low),
        _number_text(bar.close),
        _number_text(bar.volume),
        _number_text(bar.amount),
        bar.source_id,
        bar.adjust_mode,
    )


def _market_payload(payload: tuple[str, ...]) -> tuple[str, ...]:
    return payload[:9] + payload[10:]


def _market_digest(payload: tuple[str, ...] | sqlite3.Row) -> str:
    """Hash market fields only; source identity is deliberately excluded."""

    values = tuple(payload[index] for index in (*range(9), 10))
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _bar_date(payload: tuple[str, ...]) -> date:
    return datetime.fromisoformat(payload[2]).astimezone(SHANGHAI).date()


def _revision_date(value: date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("revision date must be timezone-aware")
        return value.astimezone(SHANGHAI).date()
    if isinstance(value, date):
        return value
    raise ValueError("revision date must be a date")


def _observed_at(value: datetime | None) -> str:
    if value is None:
        value = datetime.now(SHANGHAI)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(SHANGHAI).isoformat()


def _differing_fields(existing: tuple[str, ...], incoming: tuple[str, ...]) -> tuple[str, ...]:
    """Compare only immutable market fields; source identity is metadata."""

    indexes = (*range(9), 10)
    return tuple(
        name
        for index, name in zip(indexes, _MARKET_FIELD_NAMES)
        if existing[index] != incoming[index]
    )


def _from_row(row: tuple[str, ...]) -> MinuteBar:
    return MinuteBar(
        symbol=row[0],
        interval=row[1],
        bar_end=datetime.fromisoformat(row[2]),
        open=float(row[3]),
        high=float(row[4]),
        low=float(row[5]),
        close=float(row[6]),
        volume=float(row[7]),
        amount=float(row[8]),
        source_id=row[9],
        adjust_mode=row[10],
    )
