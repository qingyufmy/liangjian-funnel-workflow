"""Official SSE and SZSE public-announcement adapters.

Both web endpoints are undocumented presentation services, not contracted
APIs.  The adapters therefore validate every response, paginate completely,
retain provenance, and fail closed when the observed shape changes.
"""

from __future__ import annotations

import hashlib
import html
import re
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime
from threading import Lock
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx

from .cninfo import CninfoAnnouncement, CninfoFetchResult


SHANGHAI = ZoneInfo("Asia/Shanghai")
SSE_ENDPOINT = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
SSE_REFERER = "https://www.sse.com.cn/assortment/stock/list/info/announcement/"
SSE_SOURCE_ID = "sse_official"
SZSE_ENDPOINT = "https://www.szse.cn/api/disc/announcement/annList"
SZSE_REFERER = "https://www.szse.cn/disclosure/listed/notice/index.html"
SZSE_SOURCE_ID = "szse_official"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "Chrome/126.0.0.0 Safari/537.36"
)
PAGE_SIZE = 30
MAX_PAGES = 30
MAX_RETRIES = 3

_SYMBOL = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>SH|SZ)$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SPACE = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]*>")
_SECRET = re.compile(
    r"(?i)(?:\bsk-[a-z0-9_-]{8,}|\bbearer\s+[a-z0-9._~+/=-]{8,}|"
    r"(?:api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,;]+)"
)
_INJECTION = re.compile(
    r"(?i)(?:ignore\s+(?:all\s+)?(?:previous|prior)\s+instructions|"
    r"system\s+prompt|developer\s+message|忽略.{0,12}(?:指令|提示词)|系统提示词)"
)


class ExchangeDisclosureContractError(ValueError):
    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _validate_inputs(symbol: str, start: str, end: str, exchange: str) -> tuple[str, str, str]:
    match = _SYMBOL.fullmatch(str(symbol).strip().upper())
    if match is None:
        raise ExchangeDisclosureContractError("INVALID_SYMBOL")
    if match.group("exchange") != exchange:
        raise ExchangeDisclosureContractError("UNSUPPORTED_EXCHANGE")
    for value in (start, end):
        if not isinstance(value, str) or _DATE.fullmatch(value) is None:
            raise ExchangeDisclosureContractError("INVALID_DATE")
        try:
            date.fromisoformat(value)
        except ValueError:
            raise ExchangeDisclosureContractError("INVALID_DATE") from None
    if start > end:
        raise ExchangeDisclosureContractError("INVALID_DATE_RANGE")
    return match.group("code"), start, end


def _text(value: object, *, required: bool = True, limit: int = 500) -> str:
    if not isinstance(value, str):
        if required:
            raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")
        return ""
    if _SECRET.search(value):
        raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")
    cleaned = _SPACE.sub(" ", html.unescape(_TAG.sub(" ", value))).strip()
    if required and not cleaned:
        raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")
    return cleaned[:limit]


def _timestamp(value: object) -> datetime:
    text = _text(value, limit=64)
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
            return _aware(parsed)
        except ValueError:
            continue
    raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")


def _safe_url(host: str, path: object, *, prefix: str) -> str:
    if not isinstance(path, str) or not path.startswith(prefix) or ".." in path or "?" in path or "#" in path:
        raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")
    url = urljoin(f"https://{host}/", path)
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != host:
        raise ExchangeDisclosureContractError("EXCHANGE_DISCLOSURE_CONTRACT_CHANGED")
    return url


class _BaseClient:
    source_id = ""
    endpoint = ""
    referer = ""
    exchange = ""

    def __init__(
        self,
        *,
        timeout_seconds: float = 30,
        min_request_interval_seconds: float = 0.5,
        http_client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if timeout_seconds <= 0 or not 0 <= min_request_interval_seconds <= 10:
            raise ValueError("invalid exchange disclosure client bounds")
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now(SHANGHAI))
        self._interval = min_request_interval_seconds
        self._last_request: float | None = None
        self._lock = Lock()
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            headers={
                "User-Agent": USER_AGENT,
                "Referer": self.referer,
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _throttle(self) -> None:
        with self._lock:
            current = self._monotonic()
            scheduled = current
            if self._last_request is not None:
                scheduled = max(scheduled, self._last_request + self._interval)
                if scheduled > current:
                    self._sleep(scheduled - current)
            self._last_request = max(scheduled, self._monotonic())

    def _request(self, *, params: Mapping[str, object] | None = None, json_body: Mapping[str, object] | None = None) -> tuple[Mapping[str, object] | None, str, int | None, int]:
        status: int | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                response = self._client.get(self.endpoint, params=params) if json_body is None else self._client.post(self.endpoint, params={"random": "0.6180339887"}, json=json_body)
            except (httpx.HTTPError, TimeoutError, OSError):
                return None, "EXCHANGE_DISCLOSURE_REQUEST_FAILED", None, attempt
            status = int(response.status_code)
            if status == 429 or 500 <= status <= 599:
                if attempt < MAX_RETRIES:
                    self._sleep(0.5 * (2 ** (attempt - 1)))
                    continue
                reason = "EXCHANGE_DISCLOSURE_RATE_LIMITED" if status == 429 else "EXCHANGE_DISCLOSURE_HTTP_5XX"
                return None, reason, status, attempt
            if not 200 <= status < 300:
                return None, "EXCHANGE_DISCLOSURE_HTTP_4XX", status, attempt
            try:
                payload = response.json()
            except (ValueError, TypeError):
                return None, "EXCHANGE_DISCLOSURE_INVALID_JSON", status, attempt
            if not isinstance(payload, Mapping):
                return None, "EXCHANGE_DISCLOSURE_CONTRACT_CHANGED", status, attempt
            return payload, "OK", status, attempt
        return None, "EXCHANGE_DISCLOSURE_REQUEST_FAILED", status, MAX_RETRIES

    def _failure(self, symbol: str, start: str, end: str, reason: str, *, attempts: int = 0, status: int | None = None, metadata: Mapping[str, object] | None = None) -> CninfoFetchResult:
        return CninfoFetchResult(
            symbol=symbol,
            start_date=start,
            end_date=end,
            ok=False,
            complete=False,
            reason_code=reason,
            attempts=attempts,
            fetched_at=_aware(self._now()),
            http_status=status,
            source_id=self.source_id,
            source_url=self.endpoint,
            metadata=dict(metadata or {}),
        )

    def _success(self, symbol: str, start: str, end: str, announcements: list[CninfoAnnouncement], *, pages: int, attempts: int, status: int | None, metadata: Mapping[str, object] | None = None) -> CninfoFetchResult:
        ordered = tuple(sorted({row.announcement_id: row for row in announcements}.values(), key=lambda row: (row.publish_time, row.announcement_id), reverse=True))
        return CninfoFetchResult(
            symbol=symbol,
            start_date=start,
            end_date=end,
            ok=True,
            complete=True,
            reason_code="OK" if ordered else "NO_RECORDS",
            announcements=ordered,
            total=len(ordered),
            pages=pages,
            attempts=attempts,
            fetched_at=_aware(self._now()),
            http_status=status,
            source_id=self.source_id,
            source_url=self.endpoint,
            metadata=dict(metadata or {}),
        )


class SseDisclosureClient(_BaseClient):
    source_id = SSE_SOURCE_ID
    endpoint = SSE_ENDPOINT
    referer = SSE_REFERER
    exchange = "SH"

    def fetch_announcements(self, symbol: str, start_date: str, end_date: str, *, search_keyword: str = "") -> CninfoFetchResult:
        try:
            code, start, end = _validate_inputs(symbol, start_date, end_date, self.exchange)
        except ExchangeDisclosureContractError as exc:
            return self._failure(str(symbol)[:64], str(start_date)[:32], str(end_date)[:32], exc.reason_code)
        rows: list[CninfoAnnouncement] = []
        attempts = 0
        status = None
        expected_total: int | None = None
        for page in range(1, MAX_PAGES + 1):
            payload, reason, status, used = self._request(params={
                "isPagination": "true",
                "productId": code,
                "keyWord": search_keyword,
                "securityType": "0101,120100,020100,020200,120200",
                "beginDate": start,
                "endDate": end,
                "pageHelp.pageSize": PAGE_SIZE,
                "pageHelp.pageNo": page,
                "pageHelp.beginPage": page,
                "pageHelp.endPage": page,
                "pageHelp.cacheSize": 1,
            })
            attempts += used
            if payload is None:
                return self._failure(symbol, start, end, f"SSE_{reason}", attempts=attempts, status=status)
            page_help = payload.get("pageHelp")
            if not isinstance(page_help, Mapping) or not isinstance(page_help.get("data"), list):
                return self._failure(symbol, start, end, "SSE_CONTRACT_CHANGED", attempts=attempts, status=status)
            try:
                total = int(page_help.get("total", 0))
            except (TypeError, ValueError):
                return self._failure(symbol, start, end, "SSE_CONTRACT_CHANGED", attempts=attempts, status=status)
            if total < 0 or (expected_total is not None and total != expected_total):
                return self._failure(symbol, start, end, "SSE_CONTRACT_CHANGED", attempts=attempts, status=status)
            expected_total = total
            for raw in page_help["data"]:
                try:
                    if not isinstance(raw, Mapping) or str(raw.get("SECURITY_CODE")) != code:
                        raise ExchangeDisclosureContractError("SSE_CONTRACT_CHANGED")
                    title = _text(raw.get("TITLE"))
                    publish = _timestamp(raw.get("SSEDATE"))
                    path = raw.get("URL")
                    url = _safe_url("static.sse.com.cn", path, prefix="/disclosure/")
                    identifier = hashlib.sha256(f"{code}|{url}".encode()).hexdigest()
                    rows.append(CninfoAnnouncement(
                        announcement_id=identifier,
                        sec_code=code,
                        sec_name=_text(raw.get("SECURITY_NAME"), limit=200),
                        announcement_title=title,
                        adjunct_url=url,
                        publish_time=publish,
                        untrusted_text=True,
                        prompt_injection_suspected=bool(_INJECTION.search(title)),
                    ))
                except ExchangeDisclosureContractError as exc:
                    return self._failure(symbol, start, end, exc.reason_code, attempts=attempts, status=status)
            if len(rows) >= total or not page_help["data"]:
                return self._success(symbol, start, end, rows, pages=page, attempts=attempts, status=status, metadata={"provider_total": total})
        return self._failure(symbol, start, end, "SSE_PAGINATION_INCOMPLETE", attempts=attempts, status=status, metadata={"provider_total": expected_total, "records": len(rows)})


class SzseDisclosureClient(_BaseClient):
    source_id = SZSE_SOURCE_ID
    endpoint = SZSE_ENDPOINT
    referer = SZSE_REFERER
    exchange = "SZ"

    def fetch_announcements(self, symbol: str, start_date: str, end_date: str, *, search_keyword: str = "") -> CninfoFetchResult:
        try:
            code, start, end = _validate_inputs(symbol, start_date, end_date, self.exchange)
        except ExchangeDisclosureContractError as exc:
            return self._failure(str(symbol)[:64], str(start_date)[:32], str(end_date)[:32], exc.reason_code)
        rows: list[CninfoAnnouncement] = []
        attempts = 0
        status = None
        expected_total: int | None = None
        for page in range(1, MAX_PAGES + 1):
            payload, reason, status, used = self._request(json_body={
                "channelCode": ["listedNotice_disc"],
                "pageSize": PAGE_SIZE,
                "pageNum": page,
                "stock": [code],
                "seDate": [start, end],
            })
            attempts += used
            if payload is None:
                return self._failure(symbol, start, end, f"SZSE_{reason}", attempts=attempts, status=status)
            raw_rows = payload.get("data")
            try:
                total = int(payload.get("announceCount", 0))
            except (TypeError, ValueError):
                total = -1
            if not isinstance(raw_rows, list) or total < 0 or (expected_total is not None and total != expected_total):
                return self._failure(symbol, start, end, "SZSE_CONTRACT_CHANGED", attempts=attempts, status=status)
            expected_total = total
            for raw in raw_rows:
                try:
                    if not isinstance(raw, Mapping):
                        raise ExchangeDisclosureContractError("SZSE_CONTRACT_CHANGED")
                    codes = raw.get("secCode")
                    names = raw.get("secName")
                    if not isinstance(codes, list) or code not in [str(v) for v in codes] or not isinstance(names, list) or not names:
                        raise ExchangeDisclosureContractError("SZSE_CONTRACT_CHANGED")
                    title = _text(raw.get("title"))
                    if search_keyword and search_keyword not in title:
                        continue
                    url = _safe_url("disc.static.szse.cn", f"/download{raw.get('attachPath')}", prefix="/download/disc/")
                    identifier = str(raw.get("annId") or raw.get("id") or "").strip()
                    if not identifier or len(identifier) > 128 or _SECRET.search(identifier):
                        identifier = hashlib.sha256(f"{code}|{url}".encode()).hexdigest()
                    rows.append(CninfoAnnouncement(
                        announcement_id=identifier,
                        sec_code=code,
                        sec_name=_text(names[0], limit=200),
                        announcement_title=title,
                        adjunct_url=url,
                        publish_time=_timestamp(raw.get("publishTime")),
                        untrusted_text=True,
                        prompt_injection_suspected=bool(_INJECTION.search(title)),
                    ))
                except ExchangeDisclosureContractError as exc:
                    return self._failure(symbol, start, end, exc.reason_code, attempts=attempts, status=status)
            if page * PAGE_SIZE >= total or not raw_rows:
                return self._success(symbol, start, end, rows, pages=page, attempts=attempts, status=status, metadata={"provider_total": total, "search_keyword": search_keyword})
        return self._failure(symbol, start, end, "SZSE_PAGINATION_INCOMPLETE", attempts=attempts, status=status, metadata={"provider_total": expected_total, "records": len(rows)})


__all__ = [
    "SSE_ENDPOINT", "SSE_REFERER", "SSE_SOURCE_ID",
    "SZSE_ENDPOINT", "SZSE_REFERER", "SZSE_SOURCE_ID",
    "ExchangeDisclosureContractError", "SseDisclosureClient", "SzseDisclosureClient",
]
