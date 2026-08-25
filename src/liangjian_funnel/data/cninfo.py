"""Fail-closed adapter for the public CNINFO announcement endpoint.

CNINFO's public announcement query is a web endpoint rather than a versioned
data API.  This module therefore owns the request contract, strict response
shape checks, bounded retries, and untrusted-text handling in one place.  It
never stores a response body in an exception or a failed fetch result.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator


SHANGHAI = ZoneInfo("Asia/Shanghai")
CNINFO_ENDPOINT = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_ORG_SEARCH_ENDPOINT = "https://www.cninfo.com.cn/new/information/topSearch/detailOfQuery"
CNINFO_REFERER = "https://www.cninfo.com.cn/new/index"
CNINFO_SOURCE_ID = "cninfo_public"
CNINFO_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
PAGE_SIZE_DEFAULT = 30
PAGE_SIZE_MAX = 100
MAX_PAGES_DEFAULT = 20
MAX_PAGES_MAX = 100
MAX_RETRIES = 3

_CANONICAL_SYMBOL = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>SH|SZ)$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CODE = re.compile(r"^\d{6}$")
_WHITESPACE = re.compile(r"\s+")
_SECRET = re.compile(
    r"(?ix)"
    r"(?:\bsk-[a-z0-9][a-z0-9_-]{7,}\b)"
    r"|(?:\bbearer\s+[a-z0-9._~+/=-]{8,})"
    r"|(?:(?:api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|"
    r"client[_-]?secret|private[_-]?key|password|secret)\s*[:=]\s*[^\s,;]+)"
)
_INJECTION = re.compile(
    r"(?ix)"
    r"(?:ignore\s+(?:all\s+)?(?:previous|prior|above|earlier)\s+"
    r"(?:instructions?|messages?|prompts?))"
    r"|(?:disregard\s+(?:all\s+)?(?:previous|prior|above)\s+"
    r"(?:instructions?|messages?|prompts?))"
    r"|(?:system\s+prompt|developer\s+message)"
    r"|(?:忽略(?:前述|之前|以上|先前|此前)(?:的)?(?:指令|提示词|提示|内容|消息)?)"
    r"|(?:系统(?:提示词|指令))"
    r"|(?:不要遵循(?:以上|前述|系统)?(?:指令|提示))"
)
_MISSING = object()


class _TitleParser(HTMLParser):
    """Collect visible title text while dropping script/style contents."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._ignored += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._ignored:
            self._ignored -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored:
            self.parts.append(data)


class CninfoContractError(ValueError):
    """Internal error carrying only a stable public reason code."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _safe_text(value: Any, *, limit: int, field: str) -> str:
    if not isinstance(value, str):
        if value is None:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        value = str(value)
    value = value.strip()
    if not value or len(value) > limit:
        # Long titles are explicitly truncated by the contract; identifiers
        # and names are rejected rather than silently changed.
        if field == "announcement_title":
            value = value[:limit]
        else:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    if _SECRET.search(value):
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    return value


def _clean_title(value: Any) -> tuple[str, bool]:
    if value is None:
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    raw = str(value)
    parser = _TitleParser()
    try:
        parser.feed(raw)
        parser.close()
        cleaned = "".join(parser.parts)
    except Exception:
        # A malformed HTML fragment is still untrusted text.  Removing tags
        # gives a deterministic fallback without exposing parser exceptions.
        cleaned = re.sub(r"<[^>]*>", " ", raw)
    cleaned = _WHITESPACE.sub(" ", html.unescape(cleaned)).strip()[:500]
    if not cleaned:
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    return cleaned, bool(_INJECTION.search(cleaned))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _parse_time(value: Any) -> datetime:
    if value is None or value == "":
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not (number == number and abs(number) != float("inf")):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        if abs(number) >= 100_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(number, tz=SHANGHAI)
        except (OSError, OverflowError, ValueError):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED") from None
    if not isinstance(value, str):
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    text = value.strip()
    if not text:
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text.replace("T", " "))
    except ValueError:
        parsed = None
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED") from None
    return _aware(parsed)


def _safe_url(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    raw = value.strip()
    # CNINFO returns adjunctUrl as a root-relative path in some responses and
    # as an absolute static.cninfo.com.cn URL in others.
    if raw.startswith("//"):
        raw = "https:" + raw
    elif not re.match(r"^[a-z][a-z0-9+.-]*://", raw, re.I):
        raw = urljoin("https://static.cninfo.com.cn/", raw)
    parts = urlsplit(raw)
    try:
        port = parts.port
    except ValueError:
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED") from None
    if (
        parts.scheme.lower() != "https"
        or parts.hostname is None
        or parts.hostname.lower() != "static.cninfo.com.cn"
        or port is not None
        or parts.username is not None
        or parts.password is not None
        or not parts.path
        or parts.fragment
        or _SECRET.search(parts.query)
    ):
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    return raw


def _normalise_symbol(value: Any) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise CninfoContractError("INVALID_SYMBOL")
    symbol = value.strip().upper()
    match = _CANONICAL_SYMBOL.fullmatch(symbol)
    if not match:
        if ".BJ" in symbol or symbol.startswith("BJ."):
            raise CninfoContractError("UNSUPPORTED_EXCHANGE")
        raise CninfoContractError("INVALID_SYMBOL")
    code = match.group("code")
    exchange = match.group("exchange")
    # Do not infer exchange from a bare code here: callers must provide the
    # canonical form so an accidental cross-market request cannot occur.
    stock = f"{code},gss{'h' if exchange == 'SH' else 'z'}0{code}"
    column = "sse" if exchange == "SH" else "szse"
    return symbol, column, stock


def _normalise_date(value: Any) -> str:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise CninfoContractError("INVALID_DATE")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise CninfoContractError("INVALID_DATE") from None
    return value


def _integer(value: Any, *, allow_zero: bool = True) -> int:
    if isinstance(value, bool):
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        number = int(value.strip())
    else:
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    if number < 0 or (not allow_zero and number == 0):
        raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
    return number


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    raise CninfoContractError("CNINFO_CONTRACT_CHANGED")


def _content_hash(values: Mapping[str, Any]) -> str:
    encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class CninfoAnnouncement(BaseModel):
    """Immutable, normalized metadata for one public announcement."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    announcement_id: str
    sec_code: str
    sec_name: str
    announcement_title: str
    adjunct_url: str
    publish_time: datetime
    storage_time: datetime | None = None
    org_id: str | None = None
    untrusted_text: bool = True
    prompt_injection_suspected: bool = False
    content_hash: str | None = None

    @model_validator(mode="after")
    def validate_and_hash(self) -> "CninfoAnnouncement":
        if not self.announcement_id.strip() or len(self.announcement_id) > 128 or _SECRET.search(self.announcement_id):
            raise ValueError("invalid announcement_id")
        if not _CODE.fullmatch(self.sec_code):
            raise ValueError("invalid sec_code")
        if not self.sec_name or len(self.sec_name) > 200:
            raise ValueError("invalid sec_name")
        if not self.announcement_title or len(self.announcement_title) > 500:
            raise ValueError("invalid announcement_title")
        if not self.untrusted_text:
            raise ValueError("announcement text must remain untrusted")
        publish = _aware(self.publish_time)
        storage = _aware(self.storage_time) if self.storage_time is not None else None
        if self.content_hash is None:
            digest = _content_hash(
                {
                    "announcement_id": self.announcement_id,
                    "sec_code": self.sec_code,
                    "sec_name": self.sec_name,
                    "announcement_title": self.announcement_title,
                    "adjunct_url": self.adjunct_url,
                    "publish_time": publish.isoformat(),
                    "storage_time": storage.isoformat() if storage is not None else None,
                    "org_id": self.org_id,
                }
            )
            object.__setattr__(self, "content_hash", digest)
        object.__setattr__(self, "publish_time", publish)
        object.__setattr__(self, "storage_time", storage)
        return self

    @property
    def announcement_time(self) -> datetime:
        """Compatibility alias for CNINFO's ``announcementTime`` field."""

        return self.publish_time

    @property
    def pdf_url(self) -> str:
        return self.adjunct_url


class CninfoFetchResult(BaseModel):
    """Structured outcome for one bounded CNINFO announcement query."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: str
    start_date: str
    end_date: str
    ok: bool
    complete: bool
    reason_code: str
    announcements: tuple[CninfoAnnouncement, ...] = ()
    total: int | None = Field(default=None, ge=0)
    total_pages: int | None = Field(default=None, ge=0)
    pages: int = Field(default=0, ge=0)
    attempts: int = Field(default=0, ge=0)
    page_size: int = Field(default=PAGE_SIZE_DEFAULT, ge=0)
    max_pages: int = Field(default=MAX_PAGES_DEFAULT, ge=0)
    fetched_at: datetime
    http_status: int | None = Field(default=None, ge=100, le=599)
    source_id: str = CNINFO_SOURCE_ID
    source_url: str = CNINFO_ENDPOINT
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_result(self) -> "CninfoFetchResult":
        if self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")
        if self.ok and not self.complete:
            raise ValueError("ok result must be complete")
        if self.complete and not self.ok:
            raise ValueError("complete result must be ok")
        if self.reason_code == "OK" and not self.complete:
            raise ValueError("OK result must be complete")
        return self

    @property
    def records(self) -> tuple[CninfoAnnouncement, ...]:
        return self.announcements


class _Page:
    __slots__ = ("announcements", "total", "total_pages", "has_more")

    def __init__(self, announcements: tuple[CninfoAnnouncement, ...], total: int, total_pages: int, has_more: bool) -> None:
        self.announcements = announcements
        self.total = total
        self.total_pages = total_pages
        self.has_more = has_more


class _PageOutcome:
    __slots__ = ("page", "reason_code", "http_status", "attempts")

    def __init__(self, *, page: _Page | None, reason_code: str | None, http_status: int | None, attempts: int) -> None:
        self.page = page
        self.reason_code = reason_code
        self.http_status = http_status
        self.attempts = attempts


class CninfoClient:
    """Context-managed, bounded client for CNINFO's public endpoint."""

    def __init__(
        self,
        *,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 15.0,
        base_url: str = "https://www.cninfo.com.cn",
        min_request_interval_seconds: float = 0.0,
    ) -> None:
        if http_client is not None and client is not None:
            raise ValueError("provide only one http client")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        parts = urlsplit(base_url.rstrip("/"))
        if (
            parts.scheme != "https"
            or parts.hostname != "www.cninfo.com.cn"
            or parts.port is not None
            or parts.path not in {"", "/"}
            or parts.query
            or parts.fragment
            or parts.username is not None
            or parts.password is not None
        ):
            raise ValueError("CNINFO base URL must be the approved HTTPS host")
        if not 0 <= min_request_interval_seconds <= 10:
            raise ValueError("CNINFO request interval is invalid")
        self._owns_client = http_client is None and client is None
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request = 0.0
        self._min_request_interval = float(min_request_interval_seconds)
        self._endpoint = f"{base_url.rstrip('/')}/new/hisAnnouncement/query"
        self._org_search_endpoint = f"{base_url.rstrip('/')}/new/information/topSearch/detailOfQuery"
        self._org_id_cache: dict[str, str] = {}
        self._now_fn = now or (lambda: datetime.now(SHANGHAI))
        self._client = http_client or client
        if self._client is None:
            self._client = httpx.Client(
                timeout=timeout_seconds,
                transport=transport,
                trust_env=False,
                headers=self._headers(),
            )
        else:
            # An injected httpx client may not have been configured with the
            # browser-like headers required by the public endpoint.  Updating
            # its default headers keeps MockTransport and real clients alike.
            headers = getattr(self._client, "headers", None)
            if headers is not None:
                try:
                    headers.update(self._headers())
                except Exception:
                    pass

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "User-Agent": CNINFO_USER_AGENT,
            "Referer": CNINFO_REFERER,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

    def __enter__(self) -> "CninfoClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass

    def fetch_announcements(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        page_size: int = PAGE_SIZE_DEFAULT,
        max_pages: int = MAX_PAGES_DEFAULT,
        search_keyword: str = "",
        _resolved_stock: str | None = None,
    ) -> CninfoFetchResult:
        fetched_at = self._now()
        raw_symbol = str(symbol) if isinstance(symbol, str) else ""
        raw_start = str(start_date) if isinstance(start_date, str) else ""
        raw_end = str(end_date) if isinstance(end_date, str) else ""
        try:
            canonical, column, stock = _normalise_symbol(symbol)
        except CninfoContractError as exc:
            return self._failure(raw_symbol, raw_start, raw_end, exc.reason_code, fetched_at=fetched_at)
        try:
            start = _normalise_date(start_date)
            end = _normalise_date(end_date)
            if start > end:
                raise CninfoContractError("INVALID_DATE_RANGE")
        except CninfoContractError as exc:
            return self._failure(canonical, raw_start, raw_end, exc.reason_code, fetched_at=fetched_at)
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= PAGE_SIZE_MAX:
            return self._failure(canonical, start, end, "INVALID_PAGE_SIZE", fetched_at=fetched_at)
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 1 <= max_pages <= MAX_PAGES_MAX:
            return self._failure(canonical, start, end, "INVALID_MAX_PAGES", fetched_at=fetched_at)
        keyword = str(search_keyword).strip()
        if len(keyword) > 40 or _SECRET.search(keyword) or _INJECTION.search(keyword):
            return self._failure(canonical, start, end, "INVALID_SEARCH_KEYWORD", fetched_at=fetched_at)

        if _resolved_stock is not None:
            expected_prefix = f"{canonical.split('.', 1)[0]},"
            if not _resolved_stock.startswith(expected_prefix) or not re.fullmatch(r"\d{6},[A-Za-z0-9]{3,32}", _resolved_stock):
                return self._failure(canonical, start, end, "INVALID_RESOLVED_STOCK", fetched_at=fetched_at)
            stock = _resolved_stock

        form_base: dict[str, str | int] = {
            "pageSize": page_size,
            "column": column,
            "tabName": "fulltext",
            "plate": "",
            "stock": stock,
            "searchkey": keyword,
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start}~{end}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        announcements: dict[str, CninfoAnnouncement] = {}
        total: int | None = None
        total_pages: int | None = None
        pages = 0
        attempts = 0
        http_status: int | None = None
        for page_number in range(1, max_pages + 1):
            form = {**form_base, "pageNum": page_number}
            outcome = self._request_page(form, canonical)
            attempts += outcome.attempts
            http_status = outcome.http_status or http_status
            if outcome.page is None:
                return self._failure(
                    canonical,
                    start,
                    end,
                    outcome.reason_code or "CNINFO_REQUEST_FAILED",
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                )
            page = outcome.page
            pages += 1
            if total is None:
                total, total_pages = page.total, page.total_pages
            elif page.total != total or page.total_pages != total_pages:
                return self._failure(
                    canonical,
                    start,
                    end,
                    "CNINFO_CONTRACT_CHANGED",
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                )
            for item in page.announcements:
                announcements.setdefault(item.announcement_id, item)
            if not page.has_more:
                if total_pages is not None and total_pages not in (0, page_number):
                    return self._failure(
                        canonical,
                        start,
                        end,
                        "CNINFO_PAGINATION_INCOMPLETE",
                        announcements=tuple(announcements.values()),
                        total=total,
                        total_pages=total_pages,
                        pages=pages,
                        attempts=attempts,
                        page_size=page_size,
                        max_pages=max_pages,
                        fetched_at=fetched_at,
                        http_status=http_status,
                    )
                reason = "NO_RECORDS" if total == 0 else "OK"
                result = self._success(
                    canonical,
                    start,
                    end,
                    reason,
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                )
                if reason == "NO_RECORDS" and _resolved_stock is None:
                    resolved_stock = self._resolve_stock(canonical, column)
                    if resolved_stock is not None and resolved_stock != stock:
                        resolved = self.fetch_announcements(
                            canonical,
                            start,
                            end,
                            page_size=page_size,
                            max_pages=max_pages,
                            search_keyword=keyword,
                            _resolved_stock=resolved_stock,
                        )
                        return resolved.model_copy(
                            update={
                                "metadata": {
                                    **resolved.metadata,
                                    "org_id_source": "CNINFO_TOP_SEARCH",
                                }
                            }
                        )
                return result
            if total_pages is not None and page_number >= total_pages:
                return self._failure(
                    canonical,
                    start,
                    end,
                    "CNINFO_CONTRACT_CHANGED",
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                )
        return self._failure(
            canonical,
            start,
            end,
            "CNINFO_PAGINATION_INCOMPLETE",
            announcements=tuple(announcements.values()),
            total=total,
            total_pages=total_pages,
            pages=pages,
            attempts=attempts,
            page_size=page_size,
            max_pages=max_pages,
            fetched_at=fetched_at,
            http_status=http_status,
        )

    def _request_page(self, form: Mapping[str, str | int], symbol: str) -> _PageOutcome:
        attempts = 0
        last_status: int | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            attempts += 1
            try:
                self._throttle()
                response = self._client.post(self._endpoint, data=dict(form))
            except (httpx.HTTPError, TimeoutError, OSError):
                return _PageOutcome(page=None, reason_code="CNINFO_REQUEST_FAILED", http_status=None, attempts=attempts)
            except Exception:
                return _PageOutcome(page=None, reason_code="CNINFO_REQUEST_FAILED", http_status=None, attempts=attempts)
            status = int(getattr(response, "status_code", 0))
            last_status = status if 100 <= status <= 599 else None
            if status == 429:
                if attempt < MAX_RETRIES:
                    self._sleep(self._retry_after(response, attempt))
                    continue
                return _PageOutcome(page=None, reason_code="CNINFO_RATE_LIMITED", http_status=status, attempts=attempts)
            if 500 <= status <= 599:
                if attempt < MAX_RETRIES:
                    self._sleep(0.5 * (2 ** (attempt - 1)))
                    continue
                return _PageOutcome(page=None, reason_code="CNINFO_HTTP_5XX", http_status=status, attempts=attempts)
            if 400 <= status <= 499:
                return _PageOutcome(page=None, reason_code="CNINFO_HTTP_4XX", http_status=status, attempts=attempts)
            if status < 200 or status >= 300:
                return _PageOutcome(page=None, reason_code="CNINFO_HTTP_ERROR", http_status=last_status, attempts=attempts)
            try:
                payload = response.json()
            except (ValueError, TypeError):
                return _PageOutcome(page=None, reason_code="CNINFO_INVALID_JSON", http_status=status, attempts=attempts)
            try:
                page = self._parse_page(payload, symbol)
            except CninfoContractError as exc:
                return _PageOutcome(page=None, reason_code=exc.reason_code, http_status=status, attempts=attempts)
            return _PageOutcome(page=page, reason_code=None, http_status=status, attempts=attempts)
        return _PageOutcome(page=None, reason_code="CNINFO_REQUEST_FAILED", http_status=last_status, attempts=attempts)

    def _resolve_stock(self, canonical: str, column: str) -> str | None:
        code = canonical.split(".", 1)[0]
        cached = self._org_id_cache.get(code)
        if cached is not None:
            return f"{code},{cached}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                response = self._client.post(
                    self._org_search_endpoint,
                    data={"keyWord": code, "maxSecNum": 10, "maxListNum": 5},
                )
            except (httpx.HTTPError, TimeoutError, OSError):
                return None
            except Exception:
                return None
            status = int(getattr(response, "status_code", 0))
            if status == 429 or 500 <= status <= 599:
                if attempt < MAX_RETRIES:
                    self._sleep(self._retry_after(response, attempt))
                    continue
                return None
            if status < 200 or status >= 300:
                return None
            try:
                payload = response.json()
            except (ValueError, TypeError):
                return None
            rows = payload.get("keyBoardList") if isinstance(payload, Mapping) else None
            if not isinstance(rows, list):
                return None
            matches = [
                row
                for row in rows
                if isinstance(row, Mapping)
                and str(row.get("code") or "") == code
                and str(row.get("plate") or "").lower() == column
                and re.fullmatch(r"[A-Za-z0-9]{3,32}", str(row.get("orgId") or ""))
            ]
            if len(matches) != 1:
                return None
            org_id = str(matches[0]["orgId"])
            self._org_id_cache[code] = org_id
            return f"{code},{org_id}"
        return None

    def _parse_page(self, payload: Any, symbol: str) -> _Page:
        if not isinstance(payload, Mapping):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        if payload.get("code") not in (None, 0, "0") or payload.get("success") is False:
            raise CninfoContractError("CNINFO_BUSINESS_ERROR")
        required = ("totalAnnouncement", "totalpages", "hasMore")
        if any(key not in payload for key in required):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        total = _integer(payload["totalAnnouncement"])
        total_pages = _integer(payload["totalpages"])
        has_more = _boolean(payload["hasMore"])
        raw_announcements = payload.get("announcements", _MISSING)
        if total == 0 and raw_announcements is None:
            if has_more or total_pages not in (0, 1):
                raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
            return _Page((), total, total_pages, False)
        if raw_announcements is _MISSING or raw_announcements is None or not isinstance(raw_announcements, list):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        if total == 0 and raw_announcements:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        if total > 0 and not raw_announcements:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        parsed: list[CninfoAnnouncement] = []
        for raw in raw_announcements:
            if not isinstance(raw, Mapping):
                raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
            parsed.append(self._parse_announcement(raw, symbol))
        return _Page(tuple(parsed), total, total_pages, has_more)

    @staticmethod
    def _parse_announcement(raw: Mapping[str, Any], symbol: str) -> CninfoAnnouncement:
        announcement_id = raw.get("announcementId")
        sec_code = raw.get("secCode")
        sec_name = raw.get("secName")
        title = raw.get("announcementTitle")
        adjunct_url = raw.get("adjunctUrl")
        publish_raw = raw.get("announcementTime", raw.get("publishTime"))
        storage_raw = raw.get("storageTime")
        if announcement_id is None or sec_code is None or sec_name is None or title is None or adjunct_url is None:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        announcement_id_text = _safe_text(announcement_id, limit=128, field="announcement_id")
        sec_code_text = str(sec_code).strip()
        if not _CODE.fullmatch(sec_code_text):
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        # A query is scoped to one security.  If the provider returns a
        # different security, keeping it would contaminate the frozen input.
        expected_code = symbol.split(".", 1)[0]
        if sec_code_text != expected_code:
            raise CninfoContractError("CNINFO_CONTRACT_CHANGED")
        sec_name_text = _safe_text(sec_name, limit=200, field="sec_name")
        title_text, injection = _clean_title(title)
        return CninfoAnnouncement(
            announcement_id=announcement_id_text,
            sec_code=sec_code_text,
            sec_name=sec_name_text,
            announcement_title=title_text,
            adjunct_url=_safe_url(adjunct_url),
            publish_time=_parse_time(publish_raw),
            storage_time=None if storage_raw in (None, "") else _parse_time(storage_raw),
            org_id=None if raw.get("orgId") in (None, "") else _safe_text(raw.get("orgId"), limit=128, field="org_id"),
            untrusted_text=True,
            prompt_injection_suspected=injection,
        )

    def _retry_after(self, response: Any, attempt: int) -> float:
        headers = getattr(response, "headers", {}) or {}
        value = headers.get("Retry-After") if hasattr(headers, "get") else None
        if value is not None:
            try:
                delay = float(str(value).strip())
                if delay >= 0:
                    return min(delay, 60.0)
            except (TypeError, ValueError):
                pass
            try:
                retry_at = parsedate_to_datetime(str(value))
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                delay = (retry_at - self._now()).total_seconds()
                return min(max(0.0, delay), 60.0)
            except (TypeError, ValueError, OverflowError):
                pass
        return min(0.5 * (2 ** (attempt - 1)), 60.0)

    def _now(self) -> datetime:
        try:
            value = self._now_fn()
        except Exception:
            value = datetime.now(SHANGHAI)
        if not isinstance(value, datetime):
            value = datetime.now(SHANGHAI)
        return _aware(value)

    def _throttle(self) -> None:
        current = self._monotonic()
        wait = self._min_request_interval - (current - self._last_request)
        if self._last_request and wait > 0:
            self._sleep(wait)
        self._last_request = self._monotonic()

    @staticmethod
    def _failure(symbol: str, start: str, end: str, reason_code: str, **kwargs: Any) -> CninfoFetchResult:
        kwargs.setdefault("fetched_at", datetime.now(SHANGHAI))
        return CninfoFetchResult(
            symbol=symbol[:64],
            start_date=start[:32],
            end_date=end[:32],
            ok=False,
            complete=False,
            reason_code=reason_code,
            **kwargs,
        )

    @staticmethod
    def _success(symbol: str, start: str, end: str, reason_code: str, **kwargs: Any) -> CninfoFetchResult:
        return CninfoFetchResult(
            symbol=symbol,
            start_date=start,
            end_date=end,
            ok=True,
            complete=True,
            reason_code=reason_code,
            **kwargs,
        )


__all__ = [
    "CNINFO_ENDPOINT",
    "CNINFO_ORG_SEARCH_ENDPOINT",
    "CNINFO_REFERER",
    "CNINFO_SOURCE_ID",
    "CNINFO_USER_AGENT",
    "CninfoAnnouncement",
    "CninfoClient",
    "CninfoContractError",
    "CninfoFetchResult",
]
