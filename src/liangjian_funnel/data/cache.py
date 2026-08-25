from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from .mootdx import MinuteBar, map_symbol


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


class MinuteBarStore:
    """Transactional SQLite cache with immutable source observations."""

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

    def write(self, bars: Iterable[MinuteBar]) -> CacheWriteResult:
        values = tuple(bars)
        if not values:
            return CacheWriteResult(inserted=0, unchanged=0)
        inserted = 0
        unchanged = 0
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
        return CacheWriteResult(inserted=inserted, unchanged=unchanged)

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
