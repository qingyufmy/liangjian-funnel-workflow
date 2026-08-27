"""Fail-closed adapter for the Beijing Stock Exchange announcement page.

The BSE web site exposes company announcements through a JSONP endpoint used
by its public page.  JSONP is not an API contract, so this module deliberately
keeps the transport, response validation, pagination and untrusted-text
handling together.  It returns the existing :class:`CninfoFetchResult` shape
so the research pipeline can consume both official announcement sources while
``source_id`` and ``metadata`` retain the provenance distinction.
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
from threading import Lock
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

import httpx

from .cninfo import CninfoAnnouncement, CninfoFetchResult


SHANGHAI = ZoneInfo("Asia/Shanghai")
BSE_BASE_URL = "https://www.bse.cn"
BSE_ENDPOINT = f"{BSE_BASE_URL}/disclosureInfoController/companyAnnouncement.do"
BSE_REFERER = f"{BSE_BASE_URL}/disclosure/announcement.html"
BSE_SOURCE_ID = "bse_official"
BSE_CALLBACK = "__liangjian_bse_callback"
BSE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
PAGE_SIZE_DEFAULT = 20
MAX_PAGES_DEFAULT = 100
MAX_PAGES_MAX = 500
MAX_RETRIES = 3

_CANONICAL_SYMBOL = re.compile(r"^(?P<code>\d{6})\.BJ$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CODE = re.compile(r"^\d{6}$")
_SPACE = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]*>")
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


class BseContractError(ValueError):
    """Internal error carrying only a stable public reason code."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


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


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _normalise_symbol(value: object) -> tuple[str, str]:
    if not isinstance(value, str):
        raise BseContractError("INVALID_SYMBOL")
    symbol = value.strip().upper()
    match = _CANONICAL_SYMBOL.fullmatch(symbol)
    if match is None:
        if ".SH" in symbol or ".SZ" in symbol:
            raise BseContractError("UNSUPPORTED_EXCHANGE")
        raise BseContractError("INVALID_SYMBOL")
    return symbol, match.group("code")


def _normalise_date(value: object) -> str:
    if not isinstance(value, str) or _DATE.fullmatch(value) is None:
        raise BseContractError("INVALID_DATE")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise BseContractError("INVALID_DATE") from None
    return value


def _integer(value: object, *, allow_zero: bool = True) -> int:
    if isinstance(value, bool):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        number = int(value.strip())
    else:
        raise BseContractError("BSE_CONTRACT_CHANGED")
    if number < 0 or (not allow_zero and number == 0):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    return number


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    return value


def _clean_text(value: object, *, limit: int, field: str, required: bool = True) -> str:
    if value is None:
        if required:
            raise BseContractError("BSE_CONTRACT_CHANGED")
        return ""
    if not isinstance(value, str):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    if _SECRET.search(value):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    parser = _TitleParser()
    try:
        parser.feed(value)
        parser.close()
        visible = "".join(parser.parts)
    except Exception:
        visible = _TAG.sub(" ", value)
    cleaned = _SPACE.sub(" ", html.unescape(visible)).strip()
    if required and not cleaned:
        raise BseContractError("BSE_CONTRACT_CHANGED")
    if field != "announcement_title" and len(cleaned) > limit:
        raise BseContractError("BSE_CONTRACT_CHANGED")
    return cleaned[:limit]


def _publish_time(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 64:
        raise BseContractError("BSE_CONTRACT_CHANGED")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(text.replace("T", " "))
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        raise BseContractError("BSE_CONTRACT_CHANGED")
    return _aware(parsed)


def _announcement_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BseContractError("BSE_CONTRACT_CHANGED")
    raw = value.strip()
    if raw.startswith("/"):
        path = raw
    else:
        parts = urlsplit(raw)
        try:
            port = parts.port
        except ValueError:
            raise BseContractError("BSE_CONTRACT_CHANGED") from None
        if (
            parts.scheme.lower() != "https"
            or parts.hostname is None
            or parts.hostname.lower() != "www.bse.cn"
            or port is not None
            or parts.username is not None
            or parts.password is not None
            or not parts.path
            or parts.query
            or parts.fragment
        ):
            raise BseContractError("BSE_CONTRACT_CHANGED")
        path = parts.path
    decoded_path = unquote(path)
    if (
        not path.startswith("/disclosure/")
        or "\\" in path
        or "?" in path
        or "#" in path
        or "/../" in f"{decoded_path}/"
        or _SECRET.search(path)
        or any(ord(char) < 0x20 for char in path)
    ):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    if any(part in {"", ".", ".."} for part in decoded_path.split("/", 2)[2].split("/")):
        raise BseContractError("BSE_CONTRACT_CHANGED")
    return f"{BSE_BASE_URL}{path}"


def _content_hash(values: Mapping[str, object]) -> str:
    encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class _Page:
    __slots__ = (
        "records",
        "row_keys",
        "total",
        "total_pages",
        "number",
        "size",
        "last_page",
        "number_of_elements",
    )

    def __init__(
        self,
        *,
        records: tuple[tuple[str, CninfoAnnouncement], ...],
        row_keys: tuple[str, ...],
        total: int,
        total_pages: int,
        number: int,
        size: int,
        last_page: bool,
        number_of_elements: int,
    ) -> None:
        self.records = records
        self.row_keys = row_keys
        self.total = total
        self.total_pages = total_pages
        self.number = number
        self.size = size
        self.last_page = last_page
        self.number_of_elements = number_of_elements


class _PageOutcome:
    __slots__ = ("page", "reason_code", "http_status", "attempts")

    def __init__(
        self,
        *,
        page: _Page | None,
        reason_code: str | None,
        http_status: int | None,
        attempts: int,
    ) -> None:
        self.page = page
        self.reason_code = reason_code
        self.http_status = http_status
        self.attempts = attempts


class BseClient:
    """Context-managed client for BSE's official company-announcement JSONP."""

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
        base_url: str = BSE_BASE_URL,
        min_request_interval_seconds: float = 0.0,
    ) -> None:
        if http_client is not None and client is not None:
            raise ValueError("provide only one http client")
        if timeout_seconds <= 0 or not 0 <= min_request_interval_seconds <= 10:
            raise ValueError("invalid BSE client timing")
        parts = urlsplit(base_url.rstrip("/"))
        if (
            parts.scheme.lower() != "https"
            or parts.hostname is None
            or parts.hostname.lower() != "www.bse.cn"
            or parts.port is not None
            or parts.path not in {"", "/"}
            or parts.query
            or parts.fragment
            or parts.username is not None
            or parts.password is not None
        ):
            raise ValueError("BSE base URL must be the approved HTTPS host")
        injected = http_client or client
        self._owns_client = injected is None
        self._timeout_seconds = float(timeout_seconds)
        self._client = injected or httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            trust_env=False,
            follow_redirects=True,
        )
        headers = getattr(self._client, "headers", None)
        if headers is not None:
            try:
                headers.update(self._headers())
            except Exception:
                pass
        self._endpoint = f"{base_url.rstrip('/')}/disclosureInfoController/companyAnnouncement.do"
        self._sleep = sleep
        self._monotonic = monotonic
        self._min_request_interval = float(min_request_interval_seconds)
        self._last_request: float | None = None
        self._throttle_lock = Lock()
        self._now_fn = now or (lambda: datetime.now(SHANGHAI))

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "User-Agent": BSE_USER_AGENT,
            "Referer": BSE_REFERER,
            "Accept": "application/javascript, application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

    def __enter__(self) -> "BseClient":
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
        max_pages: int = MAX_PAGES_DEFAULT,
        search_keyword: str = "",
        keyword: str | None = None,
    ) -> CninfoFetchResult:
        fetched_at = self._now()
        raw_symbol = symbol if isinstance(symbol, str) else ""
        raw_start = start_date if isinstance(start_date, str) else ""
        raw_end = end_date if isinstance(end_date, str) else ""
        try:
            canonical, company_code = _normalise_symbol(symbol)
        except BseContractError as exc:
            return self._failure(raw_symbol, raw_start, raw_end, exc.reason_code, fetched_at=fetched_at)
        try:
            start = _normalise_date(start_date)
            end = _normalise_date(end_date)
            if start > end:
                raise BseContractError("INVALID_DATE_RANGE")
        except BseContractError as exc:
            return self._failure(canonical, raw_start, raw_end, exc.reason_code, fetched_at=fetched_at)
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 1 <= max_pages <= MAX_PAGES_MAX:
            return self._failure(canonical, start, end, "INVALID_MAX_PAGES", fetched_at=fetched_at)
        if keyword is not None:
            if search_keyword and keyword != search_keyword:
                return self._failure(canonical, start, end, "INVALID_SEARCH_KEYWORD", fetched_at=fetched_at)
            search_keyword = keyword
        if (
            not isinstance(search_keyword, str)
            or len(search_keyword) > 40
            or _SECRET.search(search_keyword)
            or _INJECTION.search(search_keyword)
        ):
            return self._failure(canonical, start, end, "INVALID_SEARCH_KEYWORD", fetched_at=fetched_at)

        announcements: dict[str, CninfoAnnouncement] = {}
        row_keys: set[str] = set()
        seen_rows = 0
        total: int | None = None
        total_pages: int | None = None
        page_size = PAGE_SIZE_DEFAULT
        pages = 0
        attempts = 0
        http_status: int | None = None
        pagination_metadata_inconsistent = False

        def metadata() -> dict[str, object]:
            result: dict[str, object] = {
                "source_system": "bse",
                "target_company_code": company_code,
                "date_filter": "client_local_inclusive",
                "date_filter_start": start,
                "date_filter_end": end,
                "returned_count": len(announcements),
            }
            if pagination_metadata_inconsistent:
                result["pagination_metadata_inconsistent"] = True
            return result

        for page_number in range(max_pages):
            form = self._form(company_code, start, end, page_number, search_keyword)
            outcome = self._request_page(form, canonical, page_number, start, end)
            attempts += outcome.attempts
            http_status = outcome.http_status or http_status
            if outcome.page is None:
                return self._failure(
                    canonical,
                    start,
                    end,
                    outcome.reason_code or "BSE_REQUEST_FAILED",
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                    metadata=metadata(),
                )
            page = outcome.page
            pages += 1
            seen_rows += page.number_of_elements
            if total is None:
                total, total_pages, page_size = page.total, page.total_pages, page.size
            else:
                if page.total != total or page.size != page_size:
                    return self._failure(
                        canonical,
                        start,
                        end,
                        "BSE_CONTRACT_CHANGED",
                        announcements=tuple(announcements.values()),
                        total=total,
                        total_pages=total_pages,
                        pages=pages,
                        attempts=attempts,
                        page_size=page_size,
                        max_pages=max_pages,
                        fetched_at=fetched_at,
                        http_status=http_status,
                        metadata=metadata(),
                    )
                if page.total_pages != total_pages:
                    pagination_metadata_inconsistent = True

            before_rows = len(row_keys)
            for row_key in page.row_keys:
                row_keys.add(row_key)
            for announcement_id, item in page.records:
                announcements.setdefault(announcement_id, item)
            new_rows = len(row_keys) - before_rows

            if page.total_pages > 0 and page.number >= page.total_pages:
                # An inconsistent totalPages value must not make us address an
                # invalid page; lastPage remains the termination signal.
                if not (page.last_page and page.number == page.total_pages - 1):
                    pagination_metadata_inconsistent = True

            if page.last_page:
                # numberOfElements is checked per page; cumulative row count
                # must still cover the provider's advertised total.  We do
                # not require unique announcement IDs to equal totalElements,
                # because the public endpoint can repeat a row across pages.
                if total is not None and seen_rows < total:
                    return self._failure(
                        canonical,
                        start,
                        end,
                        "BSE_PAGINATION_INCOMPLETE",
                        announcements=tuple(announcements.values()),
                        total=total,
                        total_pages=total_pages,
                        pages=pages,
                        attempts=attempts,
                        page_size=page_size,
                        max_pages=max_pages,
                        fetched_at=fetched_at,
                        http_status=http_status,
                        metadata=metadata(),
                    )
                ordered = tuple(announcements.values())
                reason = "NO_RECORDS" if not ordered else "OK"
                return self._success(
                    canonical,
                    start,
                    end,
                    reason,
                    announcements=ordered,
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                    metadata=metadata(),
                )

            if new_rows == 0:
                return self._failure(
                    canonical,
                    start,
                    end,
                    "BSE_PAGINATION_STALLED",
                    announcements=tuple(announcements.values()),
                    total=total,
                    total_pages=total_pages,
                    pages=pages,
                    attempts=attempts,
                    page_size=page_size,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                    http_status=http_status,
                    metadata=metadata(),
                )

        return self._failure(
            canonical,
            start,
            end,
            "BSE_PAGINATION_INCOMPLETE",
            announcements=tuple(announcements.values()),
            total=total,
            total_pages=total_pages,
            pages=max_pages,
            attempts=attempts,
            page_size=page_size,
            max_pages=max_pages,
            fetched_at=fetched_at,
            http_status=http_status,
            metadata=metadata(),
        )

    @staticmethod
    def _form(company_code: str, start: str, end: str, page: int, keyword: str = "") -> list[tuple[str, str]]:
        fields = (
            "companyCd",
            "companyName",
            "disclosureTitle",
            "disclosurePostTitle",
            "destFilePath",
            "publishDate",
            "xxfcbj",
            "fileExt",
            "xxzrlx",
        )
        form: list[tuple[str, str]] = [
            ("disclosureType[]", "5"),
            ("disclosureSubtype[]", ""),
            ("page", str(page)),
            ("companyCd", company_code),
            ("isNewThree", "1"),
            ("startTime", start),
            ("endTime", end),
            ("keyword", keyword),
            ("xxfcbj[]", "2"),
        ]
        form.extend(("needFields[]", field) for field in fields)
        form.extend((("sortfield", "xxssdq"), ("sorttype", "asc")))
        return form

    def _request_page(
        self,
        form: list[tuple[str, str]],
        symbol: str,
        page_number: int,
        start: str,
        end: str,
    ) -> _PageOutcome:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                # httpx accepts a mapping whose values are lists for
                # repeated application/x-www-form-urlencoded fields.  A raw
                # list of pairs is interpreted as multipart data by some
                # supported httpx versions, so normalize it explicitly.
                data: dict[str, list[str]] = {}
                for key, value in form:
                    data.setdefault(key, []).append(value)
                response = self._client.post(
                    self._endpoint,
                    params={"callback": BSE_CALLBACK},
                    data=data,
                    headers=self._headers(),
                    timeout=self._timeout,
                    follow_redirects=True,
                )
            except (httpx.HTTPError, TimeoutError, OSError):
                if attempt < MAX_RETRIES:
                    self._sleep(min(0.5 * 2 ** (attempt - 1), 60.0))
                    continue
                return _PageOutcome(page=None, reason_code="BSE_NETWORK_ERROR", http_status=None, attempts=attempt)
            except Exception:
                # An injected transport can expose a non-httpx network
                # exception.  Keep the public contract stable and never leak
                # provider/client exception text into the result.
                if attempt < MAX_RETRIES:
                    self._sleep(min(0.5 * 2 ** (attempt - 1), 60.0))
                    continue
                return _PageOutcome(page=None, reason_code="BSE_NETWORK_ERROR", http_status=None, attempts=attempt)
            status = response.status_code
            final_url = urlsplit(str(response.url))
            if (
                final_url.scheme.lower() != "https"
                or final_url.hostname is None
                or final_url.hostname.lower() != "www.bse.cn"
                or final_url.port is not None
                or final_url.username is not None
                or final_url.password is not None
            ):
                return _PageOutcome(page=None, reason_code="BSE_REDIRECT_REJECTED", http_status=status, attempts=attempt)
            if status == 429:
                if attempt < MAX_RETRIES:
                    self._sleep(self._retry_after(response, attempt))
                    continue
                return _PageOutcome(page=None, reason_code="BSE_RATE_LIMITED", http_status=status, attempts=attempt)
            if 500 <= status <= 599:
                if attempt < MAX_RETRIES:
                    self._sleep(self._retry_after(response, attempt))
                    continue
                return _PageOutcome(page=None, reason_code="BSE_HTTP_5XX", http_status=status, attempts=attempt)
            if 400 <= status <= 499:
                return _PageOutcome(page=None, reason_code="BSE_HTTP_4XX", http_status=status, attempts=attempt)
            if not 200 <= status < 300:
                return _PageOutcome(page=None, reason_code="BSE_HTTP_ERROR", http_status=status, attempts=attempt)
            try:
                page = self._parse_jsonp(response, symbol, page_number, start, end)
            except BseContractError as exc:
                return _PageOutcome(page=None, reason_code=exc.reason_code, http_status=status, attempts=attempt)
            return _PageOutcome(page=page, reason_code=None, http_status=status, attempts=attempt)
        return _PageOutcome(page=None, reason_code="BSE_NETWORK_ERROR", http_status=None, attempts=MAX_RETRIES)

    @classmethod
    def _parse_jsonp(
        cls,
        response: httpx.Response,
        symbol: str,
        page_number: int,
        start: str,
        end: str,
    ) -> _Page:
        try:
            body = response.content.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise BseContractError("BSE_INVALID_JSONP") from None
        match = re.fullmatch(
            rf"\s*{re.escape(BSE_CALLBACK)}\s*\((?P<payload>.*)\)\s*",
            body,
            flags=re.DOTALL,
        )
        if match is None:
            raise BseContractError("BSE_INVALID_JSONP")
        try:
            payload = json.loads(match.group("payload"))
        except (TypeError, ValueError):
            raise BseContractError("BSE_INVALID_JSONP") from None
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], Mapping):
            raise BseContractError("BSE_CONTRACT_CHANGED")
        envelope = payload[0]
        status = envelope.get("status")
        if not isinstance(status, int) or isinstance(status, bool):
            raise BseContractError("BSE_CONTRACT_CHANGED")
        if status != 0:
            raise BseContractError("BSE_BUSINESS_ERROR")
        info = envelope.get("listInfo")
        if not isinstance(info, Mapping):
            raise BseContractError("BSE_CONTRACT_CHANGED")
        content = info.get("content")
        if not isinstance(content, list):
            raise BseContractError("BSE_CONTRACT_CHANGED")
        number = _integer(info.get("number"))
        if number != page_number:
            raise BseContractError("BSE_PAGINATION_OUT_OF_RANGE")
        first_page = _boolean(info.get("firstPage"))
        last_page = _boolean(info.get("lastPage"))
        if first_page != (page_number == 0):
            raise BseContractError("BSE_PAGINATION_OUT_OF_RANGE")
        number_of_elements = _integer(info.get("numberOfElements"))
        size = _integer(info.get("size"), allow_zero=False)
        total = _integer(info.get("totalElements"))
        total_pages = _integer(info.get("totalPages"))
        if number_of_elements != len(content) or len(content) > size:
            raise BseContractError("BSE_CONTRACT_CHANGED")
        if total > 0 and total_pages == 0:
            raise BseContractError("BSE_CONTRACT_CHANGED")
        if total == 0 and content:
            raise BseContractError("BSE_CONTRACT_CHANGED")
        if total_pages > 0 and page_number >= total_pages and not last_page:
            raise BseContractError("BSE_PAGINATION_OUT_OF_RANGE")

        records: list[tuple[str, CninfoAnnouncement]] = []
        row_keys: list[str] = []
        target_code = symbol.split(".", 1)[0]
        for raw in content:
            if not isinstance(raw, Mapping):
                raise BseContractError("BSE_CONTRACT_CHANGED")
            company_value = raw.get("companyCd")
            if not isinstance(company_value, str) or _CODE.fullmatch(company_value.strip()) is None:
                raise BseContractError("BSE_CONTRACT_CHANGED")
            company = company_value.strip()
            title = _clean_text(raw.get("disclosureTitle"), limit=500, field="announcement_title")
            post_title = _clean_text(raw.get("disclosurePostTitle"), limit=500, field="announcement_title", required=False)
            publish = _publish_time(raw.get("publishDate"))
            url = _announcement_url(raw.get("destFilePath"))
            # The row key includes all server-provided identity fields.  It is
            # used for pagination progress; the URL-derived ID deduplicates
            # repeat rows in the returned result.
            row_key = _content_hash(
                {
                    "companyCd": company,
                    "publishDate": publish.isoformat(),
                    "destFilePath": url,
                    "disclosureTitle": title,
                    "disclosurePostTitle": post_title,
                }
            )
            row_keys.append(row_key)
            if company != target_code:
                continue
            combined_title = _SPACE.sub(" ", f"{title}{post_title}").strip()[:500]
            suspected = bool(_INJECTION.search(combined_title))
            announcement_id = hashlib.sha256(f"{company}|{url}".encode("utf-8")).hexdigest()
            item = CninfoAnnouncement(
                announcement_id=announcement_id,
                sec_code=company,
                sec_name=_clean_text(raw.get("companyName"), limit=200, field="company_name"),
                announcement_title=combined_title,
                adjunct_url=url,
                publish_time=publish,
                org_id=company,
                untrusted_text=True,
                prompt_injection_suspected=suspected,
                content_hash=_content_hash(
                    {
                        "announcement_id": announcement_id,
                        "sec_code": company,
                        "sec_name": _clean_text(raw.get("companyName"), limit=200, field="company_name"),
                        "announcement_title": combined_title,
                        "adjunct_url": url,
                        "publish_time": publish.isoformat(),
                        "storage_time": None,
                        "org_id": company,
                    }
                ),
            )
            if start <= publish.date().isoformat() <= end:
                records.append((announcement_id, item))
        return _Page(
            records=tuple(records),
            row_keys=tuple(row_keys),
            total=total,
            total_pages=total_pages,
            number=number,
            size=size,
            last_page=last_page,
            number_of_elements=number_of_elements,
        )

    @property
    def _timeout(self) -> float:
        # httpx.Client already owns its configured timeout.  A finite timeout
        # argument is supplied for clients injected by tests/callers as well.
        return self._timeout_seconds

    def _throttle(self) -> None:
        with self._throttle_lock:
            current = self._monotonic()
            if self._last_request is not None:
                wait = self._min_request_interval - (current - self._last_request)
                if wait > 0:
                    self._sleep(wait)
            self._last_request = self._monotonic()

    def _retry_after(self, response: httpx.Response, attempt: int) -> float:
        value = response.headers.get("Retry-After")
        if value:
            try:
                return min(max(float(value), 0.0), 60.0)
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(value)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return min(max((retry_at - self._now()).total_seconds(), 0.0), 60.0)
                except (TypeError, ValueError, OverflowError):
                    pass
        return min(0.5 * 2 ** (attempt - 1), 60.0)

    def _now(self) -> datetime:
        value = self._now_fn()
        return _aware(value if isinstance(value, datetime) else datetime.now(SHANGHAI))

    def _failure(self, symbol: str, start: str, end: str, reason: str, **kwargs: object) -> CninfoFetchResult:
        kwargs.setdefault("fetched_at", self._now())
        kwargs.setdefault("source_id", BSE_SOURCE_ID)
        kwargs.setdefault("source_url", self._endpoint)
        return CninfoFetchResult(
            symbol=symbol[:32],
            start_date=start[:32],
            end_date=end[:32],
            ok=False,
            complete=False,
            reason_code=reason,
            **kwargs,
        )

    def _success(self, symbol: str, start: str, end: str, reason: str, **kwargs: object) -> CninfoFetchResult:
        kwargs.setdefault("fetched_at", self._now())
        kwargs.setdefault("source_id", BSE_SOURCE_ID)
        kwargs.setdefault("source_url", self._endpoint)
        return CninfoFetchResult(
            symbol=symbol,
            start_date=start,
            end_date=end,
            ok=True,
            complete=True,
            reason_code=reason,
            **kwargs,
        )


__all__ = [
    "BSE_BASE_URL",
    "BSE_CALLBACK",
    "BSE_ENDPOINT",
    "BSE_REFERER",
    "BSE_SOURCE_ID",
    "BSE_USER_AGENT",
    "BseClient",
    "BseContractError",
]
