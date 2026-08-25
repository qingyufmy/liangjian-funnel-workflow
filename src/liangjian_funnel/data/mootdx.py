"""Fail-closed minute-bar adapter for the standard TongDaXin (mootdx) feed.

The adapter deliberately keeps the mootdx import lazy.  This makes capability
and contract tests independent of the optional ``mootdx``/``pandas`` runtime
dependencies and gives the caller a structured result when they are absent.

Only unadjusted minute bars are exposed.  Corporate-action adjustment is a
separate data contract owned by the HiThink source, so this module never
silently applies an adjustment.
"""

from __future__ import annotations

import ipaddress
import math
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from typing import Any, Callable, Literal, TypeAlias
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SHANGHAI = ZoneInfo("Asia/Shanghai")
Interval: TypeAlias = Literal["1m", "5m"]
AdjustMode: TypeAlias = Literal["none", "raw"]

PAGE_SIZE = 800
FREQUENCY_BY_INTERVAL: dict[str, int] = {"1m": 8, "5m": 0}

# These are public TongDaXin HQ servers.  The list is intentionally explicit
# and conservative; callers can provide their own ordered list without
# changing the adapter.  No server is contacted while this module is
# imported.
DEFAULT_NODES: tuple[tuple[str, int], ...] = (
    ("110.41.147.114", 7709),
    ("8.129.13.54", 7709),
    ("120.24.149.49", 7709),
    ("47.113.94.204", 7709),
    ("124.70.176.52", 7709),
    ("47.100.236.28", 7709),
)

_CODE = re.compile(r"^\d{6}$")
_EXCHANGE = re.compile(r"^(SH|SZ|XSHG|XSHE)$")
_FORBIDDEN_HOST = re.compile(r"[\x00-\x20/\\:@?#\[\]]")
_FORBIDDEN_SOURCE = re.compile(r"[\x00-\x1f/\\?#\[\]]")


class MootdxError(RuntimeError):
    """A safe, structured adapter error.

    Error messages intentionally contain only a stable reason code and an
    optional public node identifier.  Exception text from mootdx is never
    copied into a public result.
    """

    def __init__(self, reason_code: str, *, server: str | None = None):
        self.reason_code = reason_code
        self.server = server
        suffix = f" server={server}" if server else ""
        super().__init__(f"mootdx {reason_code}{suffix}")


class SymbolError(ValueError):
    """Invalid or unsupported A-share symbol with a stable reason code."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class SymbolMapping:
    code: str
    exchange: Literal["SH", "SZ"]

    @property
    def canonical(self) -> str:
        return f"{self.code}.{self.exchange}"


class MootdxNode(BaseModel):
    """One public HQ node in the explicit rotation list."""

    model_config = ConfigDict(frozen=True)

    host: str
    port: int = Field(default=7709, ge=1, le=65535)

    @field_validator("host")
    @classmethod
    def safe_host(cls, value: str) -> str:
        value = str(value).strip()
        if not value or len(value) > 253 or _FORBIDDEN_HOST.search(value):
            raise ValueError("invalid public mootdx node host")
        # Accept IPs and test/dedicated hostnames, but never a URL or path.
        try:
            ipaddress.ip_address(value)
        except ValueError:
            if not re.fullmatch(r"[A-Za-z0-9.-]+", value):
                raise ValueError("invalid public mootdx node host")
        return value

    @property
    def server(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def source_id(self) -> str:
        return f"MOOTDX:{self.server}"


class MinuteBar(BaseModel):
    """Validated, immutable, Asia/Shanghai-aware minute bar."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    interval: Interval
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    source_id: str
    adjust_mode: AdjustMode = "none"

    @field_validator("symbol", mode="before")
    @classmethod
    def canonical_symbol(cls, value: str) -> str:
        return map_symbol(value).canonical

    @field_validator("bar_end")
    @classmethod
    def shanghai_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("bar_end must be timezone-aware")
        if value.second or value.microsecond:
            raise ValueError("bar_end must have minute precision")
        return value.astimezone(SHANGHAI)

    @field_validator("source_id")
    @classmethod
    def safe_source_id(cls, value: str) -> str:
        value = str(value).strip()
        if not value or len(value) > 300 or _FORBIDDEN_SOURCE.search(value):
            raise ValueError("invalid public source_id")
        return value

    @model_validator(mode="after")
    def valid_values(self) -> "MinuteBar":
        prices = (self.open, self.high, self.low, self.close)
        if not all(math.isfinite(value) for value in (*prices, self.volume, self.amount)):
            raise ValueError("minute bar values must be finite")
        if any(value <= 0 for value in prices):
            raise ValueError("minute bar prices must be positive")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("minute bar OHLC relationship is invalid")
        if self.low > self.high:
            raise ValueError("minute bar low must not exceed high")
        if self.volume < 0 or self.amount < 0:
            raise ValueError("minute bar volume and amount must be non-negative")
        return self


class NodeAttempt(BaseModel):
    """Safe public evidence for one node attempt."""

    model_config = ConfigDict(frozen=True)

    server: str
    pages: int = Field(default=0, ge=0)
    returned_bars: int = Field(default=0, ge=0)
    reason_code: str


class FetchResult(BaseModel):
    """Structured fetch outcome; expected data shortages do not raise."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    interval: Interval | str
    requested_bars: int = Field(ge=0)
    returned_bars: int = Field(ge=0)
    bars: tuple[MinuteBar, ...] = ()
    server: str | None = None
    attempts: tuple[NodeAttempt, ...] = ()
    reason_code: str
    complete: bool = False

    @model_validator(mode="after")
    def count_matches(self) -> "FetchResult":
        if self.returned_bars != len(self.bars):
            raise ValueError("returned_bars must match bars length")
        if self.complete and self.returned_bars < self.requested_bars:
            raise ValueError("complete result must satisfy requested_bars")
        return self


class BarGap(BaseModel):
    """One expected closed bar that is absent between two observed bars."""

    model_config = ConfigDict(frozen=True)

    interval: Interval
    previous_end: datetime
    expected_end: datetime
    next_observed_end: datetime


ClientFactory: TypeAlias = Callable[[MootdxNode], Any]


def map_symbol(symbol: str) -> SymbolMapping:
    """Map a bare/common exchange-qualified A-share code to mootdx input.

    The standard six-digit bare code is preferred.  ``SH.600519``,
    ``600519.SH``, ``XSHG``/``XSHE`` and ``SH600519`` forms are accepted for
    integration convenience.  Beijing codes or mixed exchange prefixes fail
    closed and are never passed to mootdx.
    """

    if not isinstance(symbol, str):
        raise SymbolError("INVALID_SYMBOL")
    raw = symbol.strip().upper()
    if not raw:
        raise SymbolError("INVALID_SYMBOL")

    code: str
    exchange_hint: str | None = None
    if "." in raw:
        parts = raw.split(".")
        if len(parts) != 2:
            raise SymbolError("INVALID_SYMBOL")
        left, right = parts
        if _CODE.fullmatch(left):
            code, exchange_hint = left, right
        elif _CODE.fullmatch(right):
            exchange_hint, code = left, right
        else:
            if left in {"BJ", "XBEJ"} or right in {"BJ", "XBEJ"}:
                raise SymbolError("UNSUPPORTED_EXCHANGE")
            raise SymbolError("INVALID_SYMBOL")
    elif len(raw) > 2 and raw[:2] in {"SH", "SZ", "BJ"}:
        exchange_hint, code = raw[:2], raw[2:]
    else:
        code = raw

    if exchange_hint in {"BJ", "XBEJ"}:
        raise SymbolError("UNSUPPORTED_EXCHANGE")
    if not _CODE.fullmatch(code):
        raise SymbolError("INVALID_SYMBOL")

    if code.startswith(("4", "8", "43", "83", "87", "88", "92")):
        raise SymbolError("UNSUPPORTED_EXCHANGE")

    if code.startswith(("600", "601", "603", "605", "688", "689")):
        inferred: Literal["SH", "SZ"] = "SH"
    elif code.startswith(("000", "001", "002", "003", "300", "301")):
        inferred = "SZ"
    else:
        raise SymbolError("UNMAPPABLE_SYMBOL")

    if exchange_hint:
        exchange = {"XSHG": "SH", "XSHE": "SZ"}.get(exchange_hint, exchange_hint)
        if exchange not in {"SH", "SZ"}:
            if exchange in {"BJ", "XBEJ"}:
                raise SymbolError("UNSUPPORTED_EXCHANGE")
            if not _EXCHANGE.fullmatch(exchange_hint):
                raise SymbolError("INVALID_SYMBOL")
        if exchange != inferred:
            raise SymbolError("SYMBOL_EXCHANGE_MISMATCH")
    else:
        exchange = inferred
    return SymbolMapping(code=code, exchange=exchange)  # type: ignore[arg-type]


def _normalise_node(node: MootdxNode | Sequence[Any] | str) -> MootdxNode:
    if isinstance(node, MootdxNode):
        return node
    if isinstance(node, str):
        return MootdxNode(host=node)
    if isinstance(node, Sequence) and not isinstance(node, (bytes, bytearray)) and len(node) == 2:
        return MootdxNode(host=str(node[0]), port=int(node[1]))
    if isinstance(node, Mapping):
        return MootdxNode(host=str(node.get("host") or node.get("addr")), port=int(node.get("port", 7709)))
    raise ValueError("invalid mootdx node")


def _records(value: Any) -> list[Mapping[str, Any]]:
    """Convert mootdx's DataFrame/list response to safe row mappings."""

    if value is None:
        return []
    if hasattr(value, "empty") and bool(getattr(value, "empty")):
        return []
    if hasattr(value, "to_dict"):
        try:
            rows = value.to_dict(orient="records")
        except TypeError:
            rows = value.to_dict()
        if isinstance(rows, list):
            records = [row for row in rows if isinstance(row, Mapping)]
            # DataFrame records omit the index.  Mootdx supplies datetime in
            # normal responses, but preserve a named/unnamed datetime index
            # for compatible injected clients.
            try:
                columns = {str(key).lower() for row in records for key in row}
                if not (columns & _TIME_KEYS) and hasattr(value, "index"):
                    indices = list(value.index)
                    records = [dict(row, datetime=index) for row, index in zip(records, indices)]
            except Exception:
                pass
            return records
        if isinstance(rows, Mapping):
            return [rows]
    if isinstance(value, Mapping):
        # A single row mapping is the useful mootdx-compatible shape.  A
        # mapping of column lists is also accepted when all lists align.
        if any(key in value for key in _TIME_KEYS | _OPEN_KEYS | _HIGH_KEYS | _LOW_KEYS | _CLOSE_KEYS):
            return [value]
        return []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return [row for row in value if isinstance(row, Mapping)]
    return []


_TIME_KEYS = {"datetime", "date", "time", "timestamp", "bar_end", "dt", "日期", "时间"}
_OPEN_KEYS = {"open", "open_price", "开盘"}
_HIGH_KEYS = {"high", "high_price", "最高"}
_LOW_KEYS = {"low", "low_price", "最低"}
_CLOSE_KEYS = {"close", "close_price", "收盘"}
_VOLUME_KEYS = {"volume", "vol", "成交量"}
_AMOUNT_KEYS = {"amount", "turnover", "money", "成交额", "成交金额"}


def _value(row: Mapping[str, Any], aliases: set[str]) -> Any:
    folded = {str(key).lower(): value for key, value in row.items()}
    for key in aliases:
        if key in folded:
            return folded[key]
    return None


def _parse_timestamp(value: Any) -> datetime:
    if value is None:
        raise ValueError("missing bar timestamp")
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, date):
        result = datetime.combine(value, datetime_time.min)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("invalid bar timestamp")
        # Timestamps from market libraries are commonly milliseconds or
        # seconds.  Refuse implausibly small/large values instead of guessing.
        if abs(number) >= 1e11:
            result = datetime.fromtimestamp(number / 1000, tz=SHANGHAI)
        elif abs(number) >= 1e9:
            result = datetime.fromtimestamp(number, tz=SHANGHAI)
        else:
            raise ValueError("invalid bar timestamp")
    elif isinstance(value, str):
        text = value.strip().replace("T", " ")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        result = datetime.fromisoformat(text)
    elif hasattr(value, "to_pydatetime"):
        result = value.to_pydatetime()
    else:
        raise ValueError("invalid bar timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        result = result.replace(tzinfo=SHANGHAI)
    # Do not silently floor a malformed timestamp.  MinuteBar's validator
    # rejects non-minute precision so an incomplete/ambiguous bar cannot enter
    # the immutable data contract.
    return result.astimezone(SHANGHAI)


def _number(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError("missing numeric bar field")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite numeric bar field")
    return number


def normalize_bars(
    rows: Any,
    *,
    symbol: str,
    interval: Interval,
    source_id: str,
    adjust_mode: AdjustMode = "none",
) -> tuple[MinuteBar, ...]:
    """Normalize and validate one page of mootdx rows.

    A page may be monotonically ascending or descending (both are ordered
    responses used by market libraries), but arbitrary order and duplicate
    timestamps are rejected.  Cross-page duplicates are handled by
    :class:`MootdxAdapter` before the final ascending sort.
    """

    if interval not in FREQUENCY_BY_INTERVAL:
        raise MootdxError("INVALID_INTERVAL")
    records = _records(rows)
    if not records:
        raise MootdxError("EMPTY_DATA")
    try:
        mapping = map_symbol(symbol)
        bars = tuple(
            MinuteBar(
                symbol=mapping.canonical,
                interval=interval,
                bar_end=_parse_timestamp(_value(row, _TIME_KEYS)),
                open=_number(_value(row, _OPEN_KEYS)),
                high=_number(_value(row, _HIGH_KEYS)),
                low=_number(_value(row, _LOW_KEYS)),
                close=_number(_value(row, _CLOSE_KEYS)),
                volume=_number(_value(row, _VOLUME_KEYS)),
                amount=_number(_value(row, _AMOUNT_KEYS)),
                source_id=source_id,
                adjust_mode=adjust_mode,
            )
            for row in records
        )
    except (ValueError, TypeError):
        raise MootdxError("INVALID_BAR_DATA") from None

    timestamps = [bar.bar_end for bar in bars]
    if len(set(timestamps)) != len(timestamps):
        raise MootdxError("DUPLICATE_BAR_TIME")
    if len(timestamps) > 1:
        deltas = [(right - left).total_seconds() for left, right in zip(timestamps, timestamps[1:])]
        if not (all(delta > 0 for delta in deltas) or all(delta < 0 for delta in deltas)):
            raise MootdxError("UNORDERED_BAR_DATA")
    return bars


class _FetchFailure(Exception):
    def __init__(
        self,
        reason_code: str,
        *,
        pages: int = 0,
        returned_bars: int = 0,
        bars: tuple[MinuteBar, ...] = (),
    ):
        self.reason_code = reason_code
        self.pages = pages
        self.returned_bars = returned_bars
        self.bars = bars


class MootdxAdapter:
    """Paged, fail-closed adapter over the mootdx standard HQ market."""

    def __init__(
        self,
        nodes: Sequence[MootdxNode | Sequence[Any] | str] = DEFAULT_NODES,
        *,
        client_factory: ClientFactory | None = None,
        factory: ClientFactory | None = None,
        page_size: int = PAGE_SIZE,
        max_pages: int = 64,
        timeout_seconds: float = 15.0,
        adjust_mode: AdjustMode = "none",
    ):
        if client_factory is not None and factory is not None:
            raise ValueError("provide only one client factory")
        if not 1 <= page_size <= PAGE_SIZE:
            raise ValueError("page_size must be between 1 and 800")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if adjust_mode not in {"none", "raw"}:
            raise ValueError("adjust_mode must be none or raw")
        normalized = tuple(_normalise_node(node) for node in nodes)
        if not normalized:
            raise ValueError("at least one mootdx node is required")
        self.nodes = normalized
        self.client_factory = client_factory or factory or self._default_factory
        self.page_size = page_size
        self.max_pages = max_pages
        self.timeout_seconds = timeout_seconds
        self.adjust_mode = adjust_mode

    def _default_factory(self, node: MootdxNode) -> Any:
        try:
            from mootdx.quotes import Quotes  # type: ignore[import-not-found]
        except ImportError:
            raise MootdxError("MOOTDX_NOT_INSTALLED", server=node.server) from None
        return Quotes.factory(
            market="std",
            server=(node.host, node.port),
            timeout=self.timeout_seconds,
            auto_retry=False,
            raise_exception=True,
        )

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        required_bars: int,
        *,
        as_of: datetime | None = None,
    ) -> FetchResult:
        """Fetch exactly the latest ``required_bars`` after validation.

        A successful result has ``complete=True`` and ``reason_code=OK``.
        Invalid symbols, empty responses, malformed rows, node failures and
        insufficient history return ``complete=False`` with a stable reason
        code.  ``bar_end`` is the closing timestamp of a bar, so a bar is
        eligible only when it has ended by ``as_of``.  The adapter fetches at
        least one additional historical bar when capacity allows, which keeps
        the requested count stable when the newest feed row is still forming.
        The adapter never falls back to a different exchange or to adjusted
        prices.
        """

        requested = int(required_bars) if isinstance(required_bars, int) and not isinstance(required_bars, bool) else -1
        if requested <= 0:
            return self._result(symbol, interval, 0, (), None, (), "INVALID_REQUIRED_BARS", False)
        if interval not in FREQUENCY_BY_INTERVAL:
            return self._result(symbol, interval, requested, (), None, (), "INVALID_INTERVAL", False)
        if as_of is not None and (
            not isinstance(as_of, datetime)
            or as_of.tzinfo is None
            or as_of.utcoffset() is None
        ):
            return self._result(symbol, interval, requested, (), None, (), "INVALID_AS_OF", False)
        effective_as_of = (as_of or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
        max_capacity = self.page_size * self.max_pages
        if requested > max_capacity:
            return self._result(symbol, interval, requested, (), None, (), "REQUEST_TOO_LARGE", False)
        # One extra closed bar is the normal safety margin.  At the absolute
        # page capacity there is no room for an additional row; in that case
        # the adapter still accepts the request and correctly reports
        # INSUFFICIENT_BARS if the only spare row was an in-progress bar.
        fetch_target = min(requested + 1, max_capacity)
        try:
            mapping = map_symbol(symbol)
        except SymbolError as exc:
            return self._result(symbol, interval, requested, (), None, (), exc.reason_code, False)

        attempts: list[NodeAttempt] = []
        partial_bars: tuple[MinuteBar, ...] = ()
        partial_server: str | None = None
        for node in self.nodes:
            client: Any = None
            try:
                client = self.client_factory(node)
                bars, pages = self._fetch_node(
                    client,
                    mapping.canonical,
                    mapping.code,
                    interval,
                    requested,
                    fetch_target,
                    effective_as_of,
                    node,
                )
                attempt = NodeAttempt(server=node.server, pages=pages, returned_bars=len(bars), reason_code="OK")
                attempts.append(attempt)
                return self._result(mapping.canonical, interval, requested, bars, node.server, tuple(attempts), "OK", True)
            except _FetchFailure as exc:
                if exc.bars and len(exc.bars) > len(partial_bars):
                    partial_bars = exc.bars
                    partial_server = node.server
                attempts.append(
                    NodeAttempt(
                        server=node.server,
                        pages=exc.pages,
                        returned_bars=exc.returned_bars,
                        reason_code=exc.reason_code,
                    )
                )
            except MootdxError as exc:
                attempts.append(NodeAttempt(server=node.server, reason_code=exc.reason_code))
            except (ConnectionError, TimeoutError, OSError):
                attempts.append(NodeAttempt(server=node.server, reason_code="NODE_REQUEST_FAILED"))
            except Exception:
                # Do not expose third-party exception text or local paths.
                attempts.append(NodeAttempt(server=node.server, reason_code="NODE_REQUEST_FAILED"))
            finally:
                self._close_client(client)

        reason = _final_reason(attempts)
        # Partial history is diagnostic only: complete remains false and the
        # workflow must not consume it as a ready snapshot.  For malformed or
        # conflicting data, do not return any potentially poisoned rows.
        diagnostic_bars = partial_bars if reason == "INSUFFICIENT_BARS" else ()
        return self._result(
            symbol,
            interval,
            requested,
            diagnostic_bars,
            partial_server if diagnostic_bars else None,
            tuple(attempts),
            reason,
            False,
        )

    def _fetch_node(
        self,
        client: Any,
        canonical_symbol: str,
        mootdx_code: str,
        interval: Interval,
        required_bars: int,
        fetch_target: int,
        as_of: datetime,
        node: MootdxNode,
    ) -> tuple[tuple[MinuteBar, ...], int]:
        frequency = FREQUENCY_BY_INTERVAL[interval]
        by_time: dict[datetime, MinuteBar] = {}
        pages = 0
        for page_number in range(self.max_pages):
            start = page_number * self.page_size
            try:
                # mootdx.StdQuotes.bars uses (symbol, frequency, start,
                # offset); positional arguments keep this compatible with
                # small injected fakes while matching the upstream API.
                raw = client.bars(mootdx_code, frequency, start, self.page_size)
            except MootdxError:
                raise
            except (ConnectionError, TimeoutError, OSError):
                raise _FetchFailure("NODE_REQUEST_FAILED", pages=pages, returned_bars=len(by_time)) from None
            except Exception:
                raise _FetchFailure("NODE_REQUEST_FAILED", pages=pages, returned_bars=len(by_time)) from None
            pages += 1
            records = _records(raw)
            if not records:
                if pages == 1:
                    raise _FetchFailure("EMPTY_DATA", pages=pages, returned_bars=0)
                break
            try:
                page_bars = normalize_bars(
                    records,
                    symbol=canonical_symbol,
                    interval=interval,
                    source_id=node.source_id,
                    adjust_mode=self.adjust_mode,
                )
            except MootdxError as exc:
                raise _FetchFailure(exc.reason_code, pages=pages, returned_bars=len(by_time)) from None
            if len(page_bars) > self.page_size:
                raise _FetchFailure("PAGE_TOO_LARGE", pages=pages, returned_bars=len(by_time))
            for bar in page_bars:
                # A mootdx row can be the currently forming bar.  Do not add
                # it to the deduplication set: it is not eligible for this
                # snapshot and a later update must never become a cache
                # conflict with an earlier, incomplete value.
                if bar.bar_end > as_of:
                    continue
                previous = by_time.get(bar.bar_end)
                if previous is not None:
                    # Same timestamp from overlapping pages is expected.  A
                    # conflicting OHLC record is not safe to choose silently.
                    if previous != bar:
                        raise _FetchFailure("DUPLICATE_BAR_CONFLICT", pages=pages, returned_bars=len(by_time))
                    continue
                by_time[bar.bar_end] = bar
            if len(by_time) >= fetch_target:
                ordered = tuple(sorted(by_time.values(), key=lambda bar: bar.bar_end))
                return ordered[-required_bars:], pages
        if len(by_time) < required_bars:
            ordered = tuple(sorted(by_time.values(), key=lambda bar: bar.bar_end))
            raise _FetchFailure(
                "INSUFFICIENT_BARS",
                pages=pages,
                returned_bars=len(by_time),
                bars=ordered,
            )
        ordered = tuple(sorted(by_time.values(), key=lambda bar: bar.bar_end))
        return ordered[-required_bars:], pages

    @staticmethod
    def _close_client(client: Any) -> None:
        if client is None:
            return
        try:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        except Exception:
            pass

    @staticmethod
    def _result(
        symbol: str,
        interval: Interval | str,
        requested: int,
        bars: tuple[MinuteBar, ...],
        server: str | None,
        attempts: tuple[NodeAttempt, ...],
        reason_code: str,
        complete: bool,
    ) -> FetchResult:
        return FetchResult(
            symbol=str(symbol).strip().upper(),
            interval=interval,
            requested_bars=max(0, requested),
            returned_bars=len(bars),
            bars=bars,
            server=server,
            attempts=attempts,
            reason_code=reason_code,
            complete=complete,
        )


def _final_reason(attempts: Sequence[NodeAttempt]) -> str:
    if not attempts:
        return "NODE_LIST_EMPTY"
    priority = (
        "INVALID_BAR_DATA",
        "DUPLICATE_BAR_TIME",
        "UNORDERED_BAR_DATA",
        "DUPLICATE_BAR_CONFLICT",
        "EMPTY_DATA",
        "INSUFFICIENT_BARS",
        "MOOTDX_NOT_INSTALLED",
        "NODE_REQUEST_FAILED",
    )
    reasons = {attempt.reason_code for attempt in attempts}
    for reason in priority:
        if reason in reasons:
            return reason
    return "NODE_REQUEST_FAILED"


def _session_time(value: datetime) -> datetime_time:
    return value.timetz().replace(tzinfo=None)


def _in_session(value: datetime, interval_minutes: int) -> bool:
    current = _session_time(value)
    # mootdx timestamps are bar-end timestamps: the first closed 1m/5m bar
    # ends at 09:31/09:35 and the first afternoon bar at 13:01/13:05.
    morning_start = datetime_time(9, 30 + interval_minutes)
    afternoon_start = datetime_time(13, interval_minutes)
    return morning_start <= current <= datetime_time(11, 30) or afternoon_start <= current <= datetime_time(15, 0)


def _next_expected(value: datetime, interval_minutes: int) -> datetime | None:
    candidate = value + timedelta(minutes=interval_minutes)
    if value.date() != candidate.date():
        return None
    current = _session_time(value)
    if current <= datetime_time(11, 30) and _session_time(candidate) > datetime_time(11, 30):
        candidate = candidate.replace(hour=13, minute=interval_minutes)
    if _session_time(candidate) > datetime_time(15, 0):
        return None
    if _session_time(candidate) < datetime_time(9, 30 + interval_minutes):
        return None
    return candidate


def _floor_complete(value: datetime, interval_minutes: int) -> datetime:
    value = value.astimezone(SHANGHAI)
    minute = value.minute - (value.minute % interval_minutes)
    return value.replace(minute=minute, second=0, microsecond=0)


def detect_missing_bars(
    bars: Iterable[MinuteBar],
    interval: Interval,
    *,
    as_of: datetime | None = None,
) -> tuple[BarGap, ...]:
    """Find missing complete bars while excluding the midday break.

    Only gaps between two observed bars are considered.  A date boundary is
    not treated as a gap because this function does not own the exchange
    holiday calendar.  If ``as_of`` is supplied, bars after the completed
    interval containing ``as_of`` are ignored so an in-progress bar cannot
    produce a false alarm.
    """

    if interval not in FREQUENCY_BY_INTERVAL:
        raise ValueError("interval must be 1m or 5m")
    if as_of is not None:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        cutoff = _floor_complete(as_of, int(interval[:-1]))
    else:
        cutoff = None
    interval_minutes = int(interval[:-1])
    ordered = sorted(
        (bar for bar in bars if isinstance(bar, MinuteBar) and (cutoff is None or bar.bar_end <= cutoff)),
        key=lambda bar: bar.bar_end,
    )
    if len(ordered) < 2:
        return ()
    gaps: list[BarGap] = []
    for previous, following in zip(ordered, ordered[1:]):
        if following.bar_end <= previous.bar_end or previous.bar_end.date() != following.bar_end.date():
            continue
        if not (_in_session(previous.bar_end, interval_minutes) and _in_session(following.bar_end, interval_minutes)):
            continue
        candidate = _next_expected(previous.bar_end, interval_minutes)
        while candidate is not None and candidate < following.bar_end:
            if _in_session(candidate, interval_minutes) and (cutoff is None or candidate <= cutoff):
                gaps.append(
                    BarGap(
                        interval=interval,
                        previous_end=previous.bar_end,
                        expected_end=candidate,
                        next_observed_end=following.bar_end,
                    )
                )
            candidate = _next_expected(candidate, interval_minutes)
    return tuple(gaps)


__all__ = [
    "AdjustMode",
    "BarGap",
    "ClientFactory",
    "DEFAULT_NODES",
    "FetchResult",
    "FREQUENCY_BY_INTERVAL",
    "Interval",
    "MinuteBar",
    "MootdxAdapter",
    "MootdxError",
    "MootdxNode",
    "NodeAttempt",
    "SymbolError",
    "SymbolMapping",
    "detect_missing_bars",
    "map_symbol",
    "normalize_bars",
]
