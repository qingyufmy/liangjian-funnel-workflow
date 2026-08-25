"""Fail-closed HiThink data access used by the frozen pipeline.

The public HiThink API has a small envelope (``code``/``data.item``), but
different endpoints expose slightly different row fields.  This module keeps
the transport contract strict while leaving field interpretation to the
snapshot and factor layers.  A failed page is never silently converted into
an empty successful page and response bodies are deliberately not retained in
failure objects.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..redaction import safe_error
from ..settings import Settings


_KEY_WORDS = re.compile(r"(?:api[_-]?key|authorization|password|secret|token)", re.I)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ENDPOINT_TICKERS = "/api/meta/tickers/list"
_ENDPOINT_SNAPSHOT = "/api/a-share/prices/snapshot"
_ENDPOINT_HISTORY = "/api/a-share/prices/historical"
_ENDPOINT_INDICATORS = "/api/a-share/financials/indicators"
_ENDPOINT_INCOME = "/api/a-share/financials/income-statements"
_ENDPOINT_BALANCE = "/api/a-share/financials/balance-sheets"
_ENDPOINT_CASH_FLOW = "/api/a-share/financials/cash-flow-statements"
_ENDPOINT_THS_INDEX_CATALOG = "/api/a-share-index/catalog/ths-index-list"
_ENDPOINT_THS_INDEX_CONSTITUENTS = "/api/a-share-index/constituents/ths-stock-list"
_ENDPOINT_INDEX_SNAPSHOT = "/api/a-share-index/prices/snapshot"
_ENDPOINT_INDEX_HISTORY = "/api/a-share-index/prices/historical"
_ENDPOINT_AUCTION = "/api/a-share/auction/snapshot"
_ENDPOINT_LIMIT_UP = "/api/a-share/special-data/limit-up-pool"
_ENDPOINT_LIMIT_DOWN = "/api/a-share/special-data/limit-down-pool"
_ENDPOINT_LIMIT_BREAK = "/api/a-share/special-data/limit-break-pool"
_ENDPOINT_LIMIT_LADDER = "/api/a-share/special-data/limit-up-ladder"
_ENDPOINT_DRAGON_TIGER = "/api/a-share/special-data/dragon-tiger-list"
_ENDPOINT_HOT_STOCK = "/api/a-share/special-data/hot-stock-list"
_ENDPOINT_SKYROCKET = "/api/a-share/special-data/skyrocket-list"

_INDEX_TAGS = {"cn_concept", "region", "tszs", "industry"}
_POOL_SORT_FIELDS = {
    _ENDPOINT_LIMIT_UP: {"last_price", "continue_day_cnt", "seal_money", "limit_up_time"},
    _ENDPOINT_LIMIT_DOWN: {
        "last_limit_time", "first_limit_time", "last_price", "price_change_ratio_pct", "turnover_ratio_pct"
    },
    _ENDPOINT_LIMIT_BREAK: {"price_change_ratio_pct", "open_times", "last_price", "turnover_ratio_pct", "turnover"},
}


class HithinkRow(BaseModel):
    """An immutable, sanitized row from a successful HiThink response."""

    model_config = ConfigDict(frozen=True, extra="allow")


class HithinkFetchResult(BaseModel):
    """Structured outcome for one endpoint or paginated collection.

    ``items`` may contain rows from earlier successful pages when a later page
    fails.  Consumers must check ``complete``/``ok`` before using them; this is
    useful for audit lineage without making partial data look ready.
    """

    model_config = ConfigDict(frozen=True)

    endpoint: str
    ok: bool
    complete: bool
    reason_code: str
    items: tuple[HithinkRow, ...] = ()
    pages: int = Field(default=0, ge=0)
    total: int | None = Field(default=None, ge=0)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=0, ge=0)
    fetch_time: datetime
    http_status: int | None = Field(default=None, ge=100, le=599)
    business_code: int | str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("fetch_time")
    @classmethod
    def timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fetch_time must be timezone-aware")
        return value.astimezone(_SHANGHAI)

    @property
    def records(self) -> tuple[HithinkRow, ...]:
        """Compatibility alias for callers that call rows records."""

        return self.items

    @property
    def status(self) -> Literal["PASS", "BLOCKED"]:
        return "PASS" if self.ok and self.complete else "BLOCKED"


# A shorter name is convenient for integrations and hidden contract tests.
FetchResult = HithinkFetchResult


class _Page(BaseModel):
    model_config = ConfigDict(frozen=True)

    items: tuple[HithinkRow, ...]
    fetch_time: datetime
    total: int | None = Field(default=None, ge=0)
    has_more: bool | None = None
    next_offset: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("fetch_time")
    @classmethod
    def timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("page fetch_time must be timezone-aware")
        return value.astimezone(_SHANGHAI)


class HithinkClient:
    """Small HTTP client for the HiThink endpoints used by the workflow.

    The client owns an ``httpx.Client`` by default.  Tests can inject a
    ``MockTransport`` or an already-created client; neither changes the
    request contract.  Authentication is attached only to the request header
    and is never put into an exception, result, or report.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self._sleep = sleep
        self._last_request = 0.0
        self._owns_client = http_client is None
        if http_client is not None:
            self._client = http_client
        else:
            key = settings.hithink_api_key.get_secret_value() if settings.hithink_api_key else ""
            self._client = httpx.Client(
                base_url=settings.hithink_base_url,
                timeout=settings.timeout_seconds,
                transport=transport,
                trust_env=False,
                headers={"X-api-key": key, "Accept": "application/json"},
            )

    def __enter__(self) -> "HithinkClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass

    def ticker_catalog(
        self,
        *,
        exchange: str = "SH,SZ,BJ",
        asset_type: str = "a-share",
        limit: int = 100,
        max_pages: int = 100,
    ) -> HithinkFetchResult:
        return self._paginate(
            _ENDPOINT_TICKERS,
            {"exchange": exchange, "asset_type": asset_type},
            limit=limit,
            max_pages=max_pages,
        )

    # Explicit aliases keep the method name readable at call sites.
    list_tickers = ticker_catalog
    fetch_ticker_catalog = ticker_catalog

    def market_snapshot(self, *, limit: int = 100, max_pages: int = 100) -> HithinkFetchResult:
        return self._paginate(_ENDPOINT_SNAPSHOT, {}, limit=limit, max_pages=max_pages)

    fetch_market_snapshot = market_snapshot
    full_market_snapshot = market_snapshot

    def history_1d(
        self,
        symbol: str,
        *,
        start: int | str | None = None,
        end: int | str | None = None,
        adjust: Literal["none", "raw", "forward", "backward"] = "none",
        limit: int = 1000,
        max_pages: int = 100,
    ) -> HithinkFetchResult:
        if adjust not in {"none", "raw", "forward", "backward"}:
            return self._failure(_ENDPOINT_HISTORY, "INVALID_ADJUST_MODE")
        params: dict[str, Any] = {"thscode": _public_symbol(symbol), "interval": "1d", "adjust": "none" if adjust == "raw" else adjust}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end
        # Historical 1d accepts a bounded start/end range and currently
        # returns that range in one response; repeating an ignored offset
        # would duplicate rows and create a false pagination failure.
        result = self._paginate(_ENDPOINT_HISTORY, params, limit=limit, max_pages=1)
        return result.model_copy(update={"metadata": {**result.metadata, "adjust": adjust, "symbol": _public_symbol(symbol)}})

    fetch_history_1d = history_1d
    historical_1d = history_1d

    def financial_indicators(
        self,
        symbol: str,
        *,
        report: str | None = None,
        period: str | None = None,
        limit: int = 20,
        max_pages: int = 100,
    ) -> HithinkFetchResult:
        # The A-share indicators endpoint is report-based and returns nested
        # ``abilities`` rather than a paged ``item`` list.  ``period`` is
        # accepted as a compatibility alias when a caller already supplies a
        # report string.
        selected_report = report or (period if period and re.fullmatch(r"\d{4}-[1-4]", period) else None)
        if selected_report is None:
            selected_report = f"{self._now().year - 1}-4"
        return self._paginate(
            _ENDPOINT_INDICATORS,
            {"thscode": _public_symbol(symbol), "report": selected_report},
            limit=limit,
            max_pages=1,
        )

    fetch_financial_indicators = financial_indicators

    def income_statements(
        self,
        symbol: str,
        *,
        period: str = "quarterly",
        limit: int = 20,
        max_pages: int = 100,
    ) -> HithinkFetchResult:
        return self._paginate(
            _ENDPOINT_INCOME,
            {"thscode": _public_symbol(symbol), "period": period},
            limit=limit,
            # This endpoint currently ignores offset and always returns its
            # latest bounded sequence; repeating pages would be unsafe.
            max_pages=1,
        )

    fetch_income_statements = income_statements
    financial_income_statements = income_statements

    def balance_sheets(
        self,
        symbol: str,
        *,
        period: str = "quarterly",
        limit: int = 20,
    ) -> HithinkFetchResult:
        return self._paginate(
            _ENDPOINT_BALANCE,
            {"thscode": _public_symbol(symbol), "period": period},
            limit=limit,
            max_pages=1,
        )

    fetch_balance_sheets = balance_sheets

    def cash_flow_statements(
        self,
        symbol: str,
        *,
        period: str = "quarterly",
        limit: int = 20,
    ) -> HithinkFetchResult:
        return self._paginate(
            _ENDPOINT_CASH_FLOW,
            {"thscode": _public_symbol(symbol), "period": period},
            limit=limit,
            max_pages=1,
        )

    fetch_cash_flow_statements = cash_flow_statements

    def ths_index_catalog(self, *, tag: str = "cn_concept") -> HithinkFetchResult:
        normalized = str(tag).strip().lower()
        if normalized not in _INDEX_TAGS:
            return self._failure(_ENDPOINT_THS_INDEX_CATALOG, "INVALID_INDEX_TAG")
        return self._fetch_once(_ENDPOINT_THS_INDEX_CATALOG, {"tag": normalized})

    fetch_ths_index_catalog = ths_index_catalog

    def ths_index_constituents(self, thscode: str) -> HithinkFetchResult:
        symbol = _public_symbol(thscode)
        if not _valid_qualified_symbol(symbol, suffixes={"SH", "SZ", "TI"}):
            return self._failure(_ENDPOINT_THS_INDEX_CONSTITUENTS, "INVALID_INDEX_SYMBOL")
        return self._fetch_once(_ENDPOINT_THS_INDEX_CONSTITUENTS, {"thscode": symbol})

    fetch_ths_index_constituents = ths_index_constituents

    def index_snapshot(self, thscodes: Sequence[str]) -> HithinkFetchResult:
        symbols = _qualified_symbol_list(thscodes, suffixes={"SH", "SZ", "TI"})
        if symbols is None:
            return self._failure(_ENDPOINT_INDEX_SNAPSHOT, "INVALID_INDEX_SYMBOLS")
        return self._fetch_once(_ENDPOINT_INDEX_SNAPSHOT, {"thscodes": ",".join(symbols)})

    fetch_index_snapshot = index_snapshot

    def index_history_1d(
        self,
        thscode: str,
        *,
        start: int,
        end: int,
        limit: int = 1000,
    ) -> HithinkFetchResult:
        symbol = _public_symbol(thscode)
        if not _valid_qualified_symbol(symbol, suffixes={"SH", "SZ", "TI"}):
            return self._failure(_ENDPOINT_INDEX_HISTORY, "INVALID_INDEX_SYMBOL")
        if (
            isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or start < 0
            or end <= start
        ):
            return self._failure(_ENDPOINT_INDEX_HISTORY, "INVALID_TIME_RANGE")
        return self._paginate(
            _ENDPOINT_INDEX_HISTORY,
            {"thscode": symbol, "interval": "1d", "start": start, "end": end},
            limit=limit,
            max_pages=1,
        )

    fetch_index_history_1d = index_history_1d

    def auction_snapshot(
        self,
        thscodes: Sequence[str],
        *,
        stage: Literal["live", "final"] = "final",
    ) -> HithinkFetchResult:
        symbols = _qualified_symbol_list(thscodes, suffixes={"SH", "SZ", "BJ"})
        if symbols is None:
            return self._failure(_ENDPOINT_AUCTION, "INVALID_AUCTION_SYMBOLS")
        if stage not in {"live", "final"}:
            return self._failure(_ENDPOINT_AUCTION, "INVALID_AUCTION_STAGE")
        return self._fetch_once(
            _ENDPOINT_AUCTION,
            {"thscodes": ",".join(symbols), "stage": stage},
            metadata_keys=("timestamp", "auction_phase", "data_status", "total"),
        )

    fetch_auction_snapshot = auction_snapshot

    def limit_up_pool(
        self,
        *,
        date_ms: int | None = None,
        size: int = 200,
        max_pages: int = 20,
        sort_field: str = "continue_day_cnt",
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> HithinkFetchResult:
        return self._numbered_pool(
            _ENDPOINT_LIMIT_UP,
            date_ms=date_ms,
            size=size,
            max_pages=max_pages,
            sort_field=sort_field,
            sort_dir=sort_dir,
        )

    fetch_limit_up_pool = limit_up_pool

    def limit_down_pool(
        self,
        *,
        date_ms: int | None = None,
        size: int = 200,
        max_pages: int = 20,
        sort_field: str = "last_limit_time",
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> HithinkFetchResult:
        return self._numbered_pool(
            _ENDPOINT_LIMIT_DOWN,
            date_ms=date_ms,
            size=size,
            max_pages=max_pages,
            sort_field=sort_field,
            sort_dir=sort_dir,
        )

    fetch_limit_down_pool = limit_down_pool

    def limit_break_pool(
        self,
        *,
        date_ms: int | None = None,
        size: int = 200,
        max_pages: int = 20,
        sort_field: str = "price_change_ratio_pct",
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> HithinkFetchResult:
        return self._numbered_pool(
            _ENDPOINT_LIMIT_BREAK,
            date_ms=date_ms,
            size=size,
            max_pages=max_pages,
            sort_field=sort_field,
            sort_dir=sort_dir,
        )

    fetch_limit_break_pool = limit_break_pool

    def limit_up_ladder(self) -> HithinkFetchResult:
        return self._fetch_once(
            _ENDPOINT_LIMIT_LADDER,
            {},
            metadata_keys=("timestamp", "window"),
        )

    fetch_limit_up_ladder = limit_up_ladder

    def dragon_tiger_list(
        self,
        *,
        board_type: Literal["all", "org", "hot_money"] = "all",
        date: str | None = None,
    ) -> HithinkFetchResult:
        if board_type not in {"all", "org", "hot_money"}:
            return self._failure(_ENDPOINT_DRAGON_TIGER, "INVALID_BOARD_TYPE")
        if date is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(date)):
            return self._failure(_ENDPOINT_DRAGON_TIGER, "INVALID_TRADE_DATE")
        params: dict[str, Any] = {"board_type": board_type}
        if date is not None:
            params["date"] = date
        return self._fetch_once(
            _ENDPOINT_DRAGON_TIGER,
            params,
            item_keys=("stock_items", "hot_money_items"),
            metadata_keys=("timestamp", "board_type", "trade_date", "count", "stock_count"),
            annotate_collection=True,
            allow_empty=True,
        )

    fetch_dragon_tiger_list = dragon_tiger_list

    def hot_stock_list(self, *, period: Literal["day", "hour"] = "day") -> HithinkFetchResult:
        if period not in {"day", "hour"}:
            return self._failure(_ENDPOINT_HOT_STOCK, "INVALID_HOT_PERIOD")
        return self._fetch_once(_ENDPOINT_HOT_STOCK, {"period": period}, allow_empty=True)

    fetch_hot_stock_list = hot_stock_list

    def skyrocket_list(self, *, period: Literal["day", "hour"] = "day") -> HithinkFetchResult:
        if period not in {"day", "hour"}:
            return self._failure(_ENDPOINT_SKYROCKET, "INVALID_HOT_PERIOD")
        return self._fetch_once(_ENDPOINT_SKYROCKET, {"period": period}, allow_empty=True)

    fetch_skyrocket_list = skyrocket_list

    def _numbered_pool(
        self,
        endpoint: str,
        *,
        date_ms: int | None,
        size: int,
        max_pages: int,
        sort_field: str,
        sort_dir: str,
    ) -> HithinkFetchResult:
        fetched_at = self._now()
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= 200:
            return self._failure(endpoint, "INVALID_PAGE_SIZE", fetch_time=fetched_at)
        if not isinstance(max_pages, int) or isinstance(max_pages, bool) or not 1 <= max_pages <= 1000:
            return self._failure(endpoint, "INVALID_MAX_PAGES", fetch_time=fetched_at)
        if sort_field not in _POOL_SORT_FIELDS[endpoint]:
            return self._failure(endpoint, "INVALID_SORT_FIELD", fetch_time=fetched_at)
        if sort_dir not in {"asc", "desc"}:
            return self._failure(endpoint, "INVALID_SORT_DIRECTION", fetch_time=fetched_at)
        if date_ms is not None and (isinstance(date_ms, bool) or not isinstance(date_ms, int) or date_ms < 0):
            return self._failure(endpoint, "INVALID_DATE_MS", fetch_time=fetched_at)

        collected: list[HithinkRow] = []
        seen: set[str] = set()
        last_metadata: dict[str, Any] = {}
        total: int | None = None
        for page_number in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "page": page_number,
                "size": size,
                "sort_field": sort_field,
                "sort_dir": sort_dir,
            }
            if date_ms is not None:
                params["date_ms"] = date_ms
            page = self._fetch_once(
                endpoint,
                params,
                allow_empty=True,
                metadata_keys=("timestamp", "pagination"),
            )
            if not page.ok:
                return page.model_copy(update={"items": tuple(collected), "pages": page_number})
            last_metadata = page.metadata
            pagination = last_metadata.get("pagination")
            pages_expected = _first_int(pagination, "pages") if isinstance(pagination, Mapping) else None
            page_total = _first_int(pagination, "total") if isinstance(pagination, Mapping) else None
            total = page_total if page_total is not None else total
            for index, item in enumerate(page.items):
                identity = _row_identity(item, len(collected) + index)
                if identity not in seen:
                    seen.add(identity)
                    collected.append(item)
            if not page.items or (pages_expected is not None and page_number >= pages_expected) or len(page.items) < size:
                return HithinkFetchResult(
                    endpoint=endpoint,
                    ok=True,
                    complete=True,
                    reason_code="OK",
                    items=tuple(collected),
                    pages=page_number,
                    total=total if total is not None else len(collected),
                    offset=page_number,
                    limit=size,
                    fetch_time=self._now(),
                    metadata=last_metadata,
                )
        return self._failure(
            endpoint,
            "PAGINATION_LIMIT",
            items=tuple(collected),
            pages=max_pages,
            total=total,
            offset=max_pages,
            limit=size,
            fetch_time=self._now(),
            metadata=last_metadata,
        )

    def _fetch_once(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        *,
        item_keys: Sequence[str] = ("item", "items"),
        metadata_keys: Sequence[str] = ("timestamp",),
        annotate_collection: bool = False,
        allow_empty: bool = False,
    ) -> HithinkFetchResult:
        fetched_at = self._now()
        if self.settings.hithink_api_key is None:
            return self._failure(endpoint, "HITHINK_API_KEY_MISSING", fetch_time=fetched_at)
        self._throttle()
        try:
            response = self._client.get(endpoint, params=dict(params))
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            return self._failure(endpoint, "REQUEST_FAILED", fetch_time=fetched_at, metadata={"error": safe_error(exc)})
        except Exception:
            return self._failure(endpoint, "REQUEST_FAILED", fetch_time=fetched_at)
        status = int(response.status_code)
        if status == 429:
            return self._failure(endpoint, "RATE_LIMITED", http_status=status, fetch_time=fetched_at)
        if status >= 400:
            return self._failure(endpoint, "HTTP_ERROR", http_status=status, fetch_time=fetched_at)
        try:
            envelope = response.json()
        except (TypeError, ValueError):
            return self._failure(endpoint, "INVALID_JSON", http_status=status, fetch_time=fetched_at)
        if not isinstance(envelope, Mapping) or "code" not in envelope:
            return self._failure(endpoint, "INVALID_ENVELOPE", http_status=status, fetch_time=fetched_at)
        business_code = envelope.get("code")
        if business_code not in (0, "0"):
            safe_code = business_code if isinstance(business_code, (int, str)) and not isinstance(business_code, bool) else type(business_code).__name__
            return self._failure(
                endpoint,
                "BUSINESS_ERROR",
                http_status=status,
                business_code=safe_code,
                fetch_time=fetched_at,
            )
        data = envelope.get("data")
        if not isinstance(data, Mapping):
            return self._failure(endpoint, "MALFORMED_DATA", http_status=status, fetch_time=fetched_at)

        found_collection = False
        raw_rows: list[dict[str, Any]] = []
        for collection in item_keys:
            value = data.get(collection)
            if value is None:
                continue
            found_collection = True
            if not isinstance(value, list):
                return self._failure(endpoint, "MALFORMED_DATA", http_status=status, fetch_time=fetched_at)
            for raw in value:
                if not isinstance(raw, Mapping):
                    return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, fetch_time=fetched_at)
                row = _sanitize_row(raw)
                if annotate_collection:
                    row = {"collection": collection, **row}
                raw_rows.append(row)
        if not found_collection:
            return self._failure(endpoint, "MALFORMED_DATA", http_status=status, fetch_time=fetched_at)
        if not raw_rows and not allow_empty:
            return self._failure(endpoint, "EMPTY_DATA", http_status=status, fetch_time=fetched_at)
        try:
            rows = tuple(HithinkRow.model_validate(row) for row in raw_rows)
        except Exception:
            return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, fetch_time=fetched_at)
        metadata = {
            str(key): _sanitize_json_value(data[key])
            for key in metadata_keys
            if key in data and not _KEY_WORDS.search(str(key))
        }
        return HithinkFetchResult(
            endpoint=endpoint,
            ok=True,
            complete=True,
            reason_code="OK",
            items=rows,
            pages=1,
            total=len(rows),
            fetch_time=fetched_at,
            metadata=metadata,
        )

    def _paginate(
        self,
        endpoint: str,
        base_params: Mapping[str, Any],
        *,
        limit: int,
        max_pages: int,
    ) -> HithinkFetchResult:
        fetched_at = self._now()
        if self.settings.hithink_api_key is None:
            return self._failure(endpoint, "HITHINK_API_KEY_MISSING", fetch_time=fetched_at)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            return self._failure(endpoint, "INVALID_PAGE_LIMIT", fetch_time=fetched_at)
        if not isinstance(max_pages, int) or isinstance(max_pages, bool) or not 1 <= max_pages <= 1000:
            return self._failure(endpoint, "INVALID_MAX_PAGES", fetch_time=fetched_at)

        collected: list[HithinkRow] = []
        seen: set[str] = set()
        offset = 0
        total: int | None = None
        pages = 0
        previous_signature: tuple[str, ...] | None = None
        last_metadata: dict[str, Any] = {}
        while pages < max_pages:
            params = {**dict(base_params), "limit": limit, "offset": offset}
            page = self._request_page(endpoint, params, limit=limit, offset=offset)
            pages += 1
            if isinstance(page, HithinkFetchResult) and not page.ok:
                return self._failure(
                    endpoint,
                    page.reason_code,
                    items=tuple(collected),
                    pages=pages,
                    total=total if total is not None else page.total,
                    offset=offset,
                    limit=limit,
                    fetch_time=page.fetch_time,
                    http_status=page.http_status,
                    business_code=page.business_code,
                    metadata=page.metadata,
                )
            total = page.total if page.total is not None else total
            last_metadata = page.metadata
            signature = tuple(_row_identity(item, index) for index, item in enumerate(page.items))
            if not page.items:
                if not collected:
                    return self._failure(endpoint, "EMPTY_DATA", pages=pages, total=total, offset=offset, limit=limit, fetch_time=page.fetch_time, metadata=last_metadata)
                break
            if previous_signature == signature and not any(_row_identity(item, len(collected) + i) not in seen for i, item in enumerate(page.items)):
                return self._failure(
                    endpoint,
                    "PAGINATION_STALLED",
                    items=tuple(collected),
                    pages=pages,
                    total=total,
                    offset=offset,
                    limit=limit,
                    fetch_time=page.fetch_time,
                    metadata=last_metadata,
                )
            previous_signature = signature
            before = len(collected)
            for index, item in enumerate(page.items):
                identity = _row_identity(item, offset + index)
                if identity not in seen:
                    seen.add(identity)
                    collected.append(item)
            if len(collected) == before and len(page.items) >= limit:
                return self._failure(
                    endpoint,
                    "PAGINATION_STALLED",
                    items=tuple(collected), pages=pages, total=total, offset=offset, limit=limit,
                    fetch_time=page.fetch_time, metadata=last_metadata,
                )
            if endpoint in {
                _ENDPOINT_HISTORY,
                _ENDPOINT_INDEX_HISTORY,
                _ENDPOINT_INCOME,
                _ENDPOINT_BALANCE,
                _ENDPOINT_CASH_FLOW,
                _ENDPOINT_INDICATORS,
            }:
                # Financial endpoints expose a bounded latest sequence (and
                # the indicators endpoint is nested, not offset-paged).
                break
            next_offset = page.next_offset
            if page.has_more is False or (total is not None and len(collected) >= total):
                break
            if page.has_more is None and next_offset is None and len(page.items) < limit:
                break
            proposed = next_offset if next_offset is not None else offset + len(page.items)
            if proposed <= offset:
                return self._failure(
                    endpoint,
                    "PAGINATION_STALLED",
                    items=tuple(collected), pages=pages, total=total, offset=offset, limit=limit,
                    fetch_time=page.fetch_time, metadata=last_metadata,
                )
            offset = proposed
        else:
            return self._failure(
                endpoint,
                "PAGINATION_LIMIT",
                items=tuple(collected), pages=pages, total=total, offset=offset, limit=limit,
                fetch_time=self._now(), metadata=last_metadata,
            )
        if not collected:
            return self._failure(endpoint, "EMPTY_DATA", pages=pages, total=total, offset=offset, limit=limit, fetch_time=self._now(), metadata=last_metadata)
        return HithinkFetchResult(
            endpoint=endpoint,
            ok=True,
            complete=True,
            reason_code="OK",
            items=tuple(collected),
            pages=pages,
            total=total if total is not None else len(collected),
            offset=offset,
            limit=limit,
            fetch_time=self._now(),
            metadata=last_metadata,
        )

    def _request_page(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        *,
        limit: int,
        offset: int,
    ) -> HithinkFetchResult | _Page:
        self._throttle()
        fetched_at = self._now()
        try:
            response = self._client.get(endpoint, params=dict(params))
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            return self._failure(endpoint, "REQUEST_FAILED", offset=offset, limit=limit, fetch_time=fetched_at, metadata={"error": safe_error(exc)})
        except Exception:
            return self._failure(endpoint, "REQUEST_FAILED", offset=offset, limit=limit, fetch_time=fetched_at)
        status = int(response.status_code)
        if status == 429:
            return self._failure(endpoint, "RATE_LIMITED", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        if status >= 400:
            return self._failure(endpoint, "HTTP_ERROR", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        try:
            envelope = response.json()
        except (ValueError, TypeError):
            return self._failure(endpoint, "INVALID_JSON", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        if not isinstance(envelope, dict) or "code" not in envelope:
            return self._failure(endpoint, "INVALID_ENVELOPE", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        business_code = envelope.get("code")
        if business_code not in (0, "0"):
            safe_code: int | str
            if isinstance(business_code, (int, str)) and not isinstance(business_code, bool):
                safe_code = business_code
            else:
                safe_code = type(business_code).__name__
            return self._failure(endpoint, "BUSINESS_ERROR", http_status=status, business_code=safe_code, offset=offset, limit=limit, fetch_time=fetched_at)
        data = envelope.get("data")
        if not isinstance(data, dict):
            return self._failure(endpoint, "MALFORMED_DATA", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        raw_items = data.get("item", data.get("items"))
        if endpoint == _ENDPOINT_INDICATORS and raw_items is None:
            abilities = data.get("abilities")
            if not isinstance(abilities, list):
                return self._failure(endpoint, "MALFORMED_DATA", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
            flattened: list[dict[str, Any]] = []
            for ability in abilities:
                if not isinstance(ability, Mapping):
                    return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
                ability_name = ability.get("ability")
                indicators = ability.get("indicators")
                if not isinstance(indicators, list):
                    return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
                for indicator in indicators:
                    if not isinstance(indicator, Mapping):
                        return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
                    flattened.append({"ability": ability_name, **dict(indicator)})
            raw_items = flattened
        if not isinstance(raw_items, list):
            return self._failure(endpoint, "MALFORMED_DATA", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        rows: list[HithinkRow] = []
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
            try:
                rows.append(HithinkRow.model_validate(_sanitize_row(raw)))
            except Exception:
                return self._failure(endpoint, "MALFORMED_ITEM", http_status=status, offset=offset, limit=limit, fetch_time=fetched_at)
        metadata = _safe_page_metadata(data)
        return _Page(
            items=tuple(rows),
            fetch_time=fetched_at,
            total=_first_int(data, "total", "total_count", "count"),
            has_more=_first_bool(data, "has_more", "hasMore", "more"),
            next_offset=_first_int(data, "next_offset", "nextOffset"),
            metadata=metadata,
        )

    def _throttle(self) -> None:
        interval = float(self.settings.hithink_min_request_interval_seconds)
        now = time.monotonic()
        wait = interval - (now - self._last_request)
        if self._last_request and wait > 0:
            self._sleep(wait)
        self._last_request = time.monotonic()

    def _now(self) -> datetime:
        return datetime.now(ZoneInfo(self.settings.timezone)).astimezone(_SHANGHAI)

    @staticmethod
    def _failure(endpoint: str, reason_code: str, **kwargs: Any) -> HithinkFetchResult:
        kwargs.setdefault("fetch_time", datetime.now(_SHANGHAI))
        return HithinkFetchResult(
            endpoint=endpoint,
            ok=False,
            complete=False,
            reason_code=reason_code,
            **kwargs,
        )


def _sanitize_row(raw: Mapping[Any, Any]) -> dict[str, Any]:
    """Copy only JSON-like row values and drop accidental credential fields."""

    result: dict[str, Any] = {}
    for key, value in raw.items():
        key_text = str(key)
        if _KEY_WORDS.search(key_text):
            continue
        if isinstance(value, Mapping):
            result[key_text] = _sanitize_row(value)
        elif isinstance(value, (list, tuple)):
            result[key_text] = [
                _sanitize_row(item) if isinstance(item, Mapping) else item
                for item in value
            ]
        else:
            result[key_text] = value
    return result


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_row(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_json_value(item) for item in value]
    return value


def _row_identity(row: HithinkRow, fallback: int) -> str:
    data = row.model_dump(mode="json")
    disambiguator = ""
    for key in ("date_ms", "timestamp_ms", "period_end_ms", "period", "report", "date", "trade_date", "index_id"):
        value = data.get(key)
        if value not in (None, ""):
            disambiguator = f"|{key}:{str(value).strip().upper()}"
            break
    for key in ("thscode", "ths_code", "symbol", "ticker", "code", "id"):
        value = data.get(key)
        if value not in (None, ""):
            return f"{key}:{str(value).strip().upper()}{disambiguator}"
    # A page without an identity is still accepted as a raw endpoint result,
    # but position-based identity keeps pagination deterministic.
    return f"row:{fallback}:{repr(sorted(data.items(), key=lambda item: item[0]))}"


def _safe_page_metadata(data: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"timestamp", "request_id", "has_more", "hasMore", "more", "next_offset", "nextOffset"}
    return {str(key): data[key] for key in data if str(key) in allowed and not isinstance(data[key], (dict, list))}


def _first_int(data: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, bool):
            continue
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError, OverflowError):
            continue
    return None


def _first_bool(data: Mapping[str, Any], *keys: str) -> bool | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.lower() == "true"
    return None


def _public_symbol(symbol: str) -> str:
    value = str(symbol).strip().upper()
    return value


def _valid_qualified_symbol(symbol: str, *, suffixes: set[str]) -> bool:
    match = re.fullmatch(r"\d{6}[.]([A-Z]{2})", symbol)
    return bool(match and match.group(1) in suffixes)


def _qualified_symbol_list(values: Sequence[str], *, suffixes: set[str]) -> tuple[str, ...] | None:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence) or not values:
        return None
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        symbol = _public_symbol(raw)
        if not _valid_qualified_symbol(symbol, suffixes=suffixes):
            return None
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return tuple(result)


__all__ = ["FetchResult", "HithinkClient", "HithinkFetchResult", "HithinkRow"]
