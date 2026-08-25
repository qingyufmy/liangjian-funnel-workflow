"""Fail-closed reader for the State Council policy-library web interface.

The official site exposes a JSON endpoint used by its own search page, but it
is not a versioned data API.  This adapter therefore validates the complete
request/response contract and treats any drift as unavailable data.
"""

from __future__ import annotations

import hashlib
import html
import re
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator


SHANGHAI = ZoneInfo("Asia/Shanghai")
GOV_POLICY_ENDPOINT = "https://sousuo.www.gov.cn/search-gov/data"
GOV_POLICY_SOURCE_ID = "gov_policy_library"
MAX_RETRIES = 3
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HTML = re.compile(r"<[^>]*>")
_SPACE = re.compile(r"\s+")
_INJECTION = re.compile(
    r"(?ix)(?:ignore\s+(?:all\s+)?(?:previous|prior|above)\s+(?:instructions?|prompts?))"
    r"|(?:system\s+prompt|developer\s+message)"
    r"|(?:忽略(?:前述|之前|以上|先前)(?:的)?(?:指令|提示词|提示|消息)?)"
    r"|(?:系统(?:提示词|指令))"
)


class GovPolicyContractError(ValueError):
    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _clean_text(value: Any, *, limit: int, required: bool = True) -> str:
    if value is None:
        if required:
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        return ""
    text = _SPACE.sub(" ", html.unescape(_HTML.sub(" ", str(value)))).strip()
    if required and not text:
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    return text[:limit]


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    if result < 0:
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    return result


def _date(value: Any) -> str:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise GovPolicyContractError("INVALID_DATE")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise GovPolicyContractError("INVALID_DATE") from None
    return value


def _publish_time(value: Any) -> datetime | None:
    # The official result occasionally emits 0.  Missing publication time is
    # preserved as None and must never be replaced with fetch time.
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    number = float(value)
    if number <= 0 or number != number or abs(number) == float("inf"):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    if number >= 100_000_000_000:
        number /= 1000
    try:
        return datetime.fromtimestamp(number, tz=SHANGHAI)
    except (OSError, OverflowError, ValueError):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED") from None


def _url(value: Any) -> str:
    if not isinstance(value, str):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    raw = value.strip()
    parts = urlsplit(raw)
    try:
        port = parts.port
    except ValueError:
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED") from None
    if (
        parts.scheme != "https"
        or parts.hostname != "www.gov.cn"
        or port is not None
        or parts.username is not None
        or parts.password is not None
        or not parts.path
        or parts.fragment
    ):
        raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
    return raw


class GovPolicyDocument(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    document_id: str
    category: Literal["gongwen", "bumenfile"]
    title: str
    summary: str = ""
    url: str
    publish_time: datetime | None = None
    issuing_body: str | None = None
    document_number: str | None = None
    untrusted_text: bool = True
    prompt_injection_suspected: bool = False

    @property
    def pubtime(self) -> datetime | None:
        return self.publish_time

    @model_validator(mode="after")
    def validate_document(self) -> "GovPolicyDocument":
        if not self.document_id or len(self.document_id) > 128:
            raise ValueError("invalid document_id")
        if not self.title or len(self.title) > 500 or len(self.summary) > 1500:
            raise ValueError("invalid policy text")
        if not self.untrusted_text:
            raise ValueError("policy text must remain untrusted")
        if self.publish_time is not None:
            object.__setattr__(self, "publish_time", _aware(self.publish_time))
        return self


class GovPolicyFetchResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    start_date: str
    end_date: str
    ok: bool
    complete: bool
    reason_code: str
    documents: tuple[GovPolicyDocument, ...] = ()
    category_totals: dict[str, int] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=lambda: {
        "excluded_categories": ["otherfile", "gongbao"]
    })
    pages: int = Field(default=0, ge=0)
    attempts: int = Field(default=0, ge=0)
    fetched_at: datetime
    http_status: int | None = Field(default=None, ge=100, le=599)
    source_id: str = GOV_POLICY_SOURCE_ID
    source_url: str = GOV_POLICY_ENDPOINT

    @model_validator(mode="after")
    def validate_result(self) -> "GovPolicyFetchResult":
        if self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")
        if self.ok != self.complete:
            raise ValueError("policy result success must be complete")
        return self

    @property
    def total_counts(self) -> dict[str, int]:
        return self.category_totals

    @property
    def total(self) -> int:
        return sum(self.category_totals.values())

    @property
    def source_health(self) -> dict[str, Any]:
        return {
            "available": self.ok and self.complete,
            "reason_code": self.reason_code,
            "checked_at": self.fetched_at.isoformat(),
        }


class GovPolicyClient:
    def __init__(
        self,
        *,
        transport: httpx.BaseTransport | None = None,
        client: httpx.Client | None = None,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 15,
        base_url: str = "https://sousuo.www.gov.cn",
        min_request_interval_seconds: float = 0,
    ) -> None:
        parts = urlsplit(base_url.rstrip("/"))
        if (
            parts.scheme != "https"
            or parts.hostname != "sousuo.www.gov.cn"
            or parts.port is not None
            or parts.path not in {"", "/"}
            or parts.query
            or parts.fragment
        ):
            raise ValueError("policy base URL must be the approved HTTPS host")
        if timeout_seconds <= 0 or not 0 <= min_request_interval_seconds <= 10:
            raise ValueError("invalid policy client timing")
        if client is not None and http_client is not None:
            raise ValueError("provide only one HTTP client")
        injected = client or http_client
        self._owns = injected is None
        self._client = injected or httpx.Client(timeout=timeout_seconds, transport=transport, trust_env=False)
        self._endpoint = f"{base_url.rstrip('/')}/search-gov/data"
        self._sleep = sleep
        self._monotonic = monotonic
        self._last_request = 0.0
        self._min_interval = min_request_interval_seconds
        self._now_fn = now or (lambda: datetime.now(SHANGHAI))

    def __enter__(self) -> "GovPolicyClient":
        return self

    def __exit__(self, *_args: object) -> None:
        if self._owns:
            self._client.close()

    def fetch_documents(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        query: str = "",
        page_size: int = 50,
        max_pages: int = 10,
    ) -> GovPolicyFetchResult:
        fetched_at = self._now()
        default_end = fetched_at.date()
        default_start = default_end - timedelta(days=6)
        start_date = start_date or default_start.isoformat()
        end_date = end_date or default_end.isoformat()
        try:
            start, end = _date(start_date), _date(end_date)
            if start > end:
                raise GovPolicyContractError("INVALID_DATE_RANGE")
        except GovPolicyContractError as exc:
            return self._failure(str(start_date), str(end_date), exc.reason_code, fetched_at=fetched_at)
        if not isinstance(query, str) or len(query) > 200:
            return self._failure(start, end, "INVALID_QUERY", fetched_at=fetched_at)
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
            return self._failure(start, end, "INVALID_PAGE_SIZE", fetched_at=fetched_at)
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 1 <= max_pages <= 50:
            return self._failure(start, end, "INVALID_MAX_PAGES", fetched_at=fetched_at)

        documents: dict[str, GovPolicyDocument] = {}
        totals: dict[str, int] | None = None
        attempts = 0
        last_status: int | None = None
        for page in range(1, max_pages + 1):
            params = {
                "t": "zhengcelibrary",
                "q": query,
                "timetype": "timezd",
                "mintime": start,
                "maxtime": end,
                "sort": "pubtime",
                "sortType": 1,
                "searchfield": "title:content:summary",
                "pcodeJiguan": "",
                "childtype": "",
                "subchildtype": "",
                "tsbq": "",
                "pubtimeyear": "",
                "puborg": "",
                "pcodeYear": "",
                "pcodeNum": "",
                "filetype": "",
                "p": page,
                "n": page_size,
                "inpro": "",
                "bmfl": "",
                "dup": "",
                "orpro": "",
                "bmpubyear": "",
                "type": "gwyzcwjk",
            }
            outcome, used, status, reason = self._request(params, start, end, page, page_size)
            attempts += used
            last_status = status or last_status
            if reason == "NO_RECORDS":
                return self._success(start, end, reason, documents=(), category_totals={
                    "gongwen": 0, "bumenfile": 0
                }, pages=page, attempts=attempts, fetched_at=fetched_at, http_status=status)
            if outcome is None:
                return self._failure(start, end, reason or "GOV_POLICY_REQUEST_FAILED", documents=tuple(documents.values()), category_totals=totals or {}, pages=page - 1, attempts=attempts, fetched_at=fetched_at, http_status=last_status)
            page_documents, page_totals = outcome
            if totals is None:
                totals = page_totals
            elif totals != page_totals:
                return self._failure(start, end, "GOV_POLICY_CONTRACT_CHANGED", documents=tuple(documents.values()), category_totals=totals, pages=page, attempts=attempts, fetched_at=fetched_at, http_status=last_status)
            for item in page_documents:
                documents.setdefault(item.document_id, item)
            if page * page_size >= max(totals.values(), default=0):
                ordered = tuple(sorted(documents.values(), key=lambda x: (x.publish_time or datetime.min.replace(tzinfo=SHANGHAI), x.document_id), reverse=True))
                return self._success(start, end, "NO_RECORDS" if not ordered else "OK", documents=ordered, category_totals=totals, pages=page, attempts=attempts, fetched_at=fetched_at, http_status=last_status)
        return self._failure(start, end, "GOV_POLICY_PAGINATION_INCOMPLETE", documents=tuple(documents.values()), category_totals=totals or {}, pages=max_pages, attempts=attempts, fetched_at=fetched_at, http_status=last_status)

    def _request(self, params: Mapping[str, Any], start: str, end: str, page: int, page_size: int) -> tuple[tuple[tuple[GovPolicyDocument, ...], dict[str, int]] | None, int, int | None, str | None]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                response = self._client.get(self._endpoint, params=dict(params), headers={"Accept": "application/json, text/plain, */*", "Referer": "https://sousuo.www.gov.cn/zcwjk/"})
            except (httpx.HTTPError, TimeoutError, OSError):
                return None, attempt, None, "GOV_POLICY_REQUEST_FAILED"
            status = response.status_code
            if status == 429:
                if attempt < MAX_RETRIES:
                    self._sleep(self._retry_after(response, attempt))
                    continue
                return None, attempt, status, "GOV_POLICY_RATE_LIMITED"
            if 500 <= status <= 599:
                if attempt < MAX_RETRIES:
                    self._sleep(0.5 * 2 ** (attempt - 1))
                    continue
                return None, attempt, status, "GOV_POLICY_HTTP_5XX"
            if 400 <= status <= 499:
                return None, attempt, status, "GOV_POLICY_HTTP_4XX"
            if not 200 <= status < 300:
                return None, attempt, status, "GOV_POLICY_HTTP_ERROR"
            try:
                payload = response.json()
                parsed = self._parse(payload, start, end, page, page_size)
            except GovPolicyContractError as exc:
                if exc.reason_code == "NO_RECORDS":
                    return None, attempt, status, "NO_RECORDS"
                return None, attempt, status, exc.reason_code
            except (ValueError, TypeError):
                return None, attempt, status, "GOV_POLICY_INVALID_JSON"
            return parsed, attempt, status, None
        return None, MAX_RETRIES, None, "GOV_POLICY_REQUEST_FAILED"

    @staticmethod
    def _parse(payload: Any, start: str, end: str, page: int, page_size: int) -> tuple[tuple[GovPolicyDocument, ...], dict[str, int]]:
        if not isinstance(payload, Mapping):
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        if payload.get("code") == 1001:
            if payload.get("data") == [] and payload.get("searchVO") is None and payload.get("paramsVO") is None:
                raise GovPolicyContractError("NO_RECORDS")
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        if payload.get("code") != 200:
            raise GovPolicyContractError("GOV_POLICY_BUSINESS_ERROR")
        params = payload.get("paramsVO")
        search = payload.get("searchVO")
        if not isinstance(params, Mapping) or not isinstance(search, Mapping):
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        if params.get("mintime") != start or params.get("maxtime") != end or _integer(params.get("p")) != page or _integer(params.get("n")) != page_size:
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        cat_map = search.get("catMap")
        if not isinstance(cat_map, Mapping):
            raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
        categories = {
            "gongwen": "gongwen",
            "bumenfile": "bumenfile",
        }
        totals: dict[str, int] = {}
        documents: list[GovPolicyDocument] = []
        for raw_category, canonical in categories.items():
            bucket = cat_map.get(raw_category)
            if bucket is None:
                totals[raw_category] = 0
                continue
            if not isinstance(bucket, Mapping):
                raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
            total = _integer(bucket.get("totalCount"))
            raw_items = bucket.get("listVO")
            if raw_items is None and total == 0:
                raw_items = []
            if not isinstance(raw_items, list) or (total == 0 and raw_items):
                raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
            totals[raw_category] = total
            for raw in raw_items:
                if not isinstance(raw, Mapping):
                    raise GovPolicyContractError("GOV_POLICY_CONTRACT_CHANGED")
                title = _clean_text(raw.get("title"), limit=500)
                summary = _clean_text(raw.get("summary"), limit=1500, required=False)
                url = _url(raw.get("url"))
                raw_id = str(raw.get("id") or "").strip()
                document_id = raw_id[:128] if raw_id else hashlib.sha256(url.encode("utf-8")).hexdigest()
                issuing = _clean_text(raw.get("puborg"), limit=300, required=False) or None
                number = _clean_text(raw.get("pcode"), limit=200, required=False) or None
                documents.append(GovPolicyDocument(document_id=document_id, category=canonical, title=title, summary=summary, url=url, publish_time=_publish_time(raw.get("pubtime")), issuing_body=issuing, document_number=number, prompt_injection_suspected=bool(_INJECTION.search(f"{title} {summary}"))))
        return tuple(documents), totals

    def _now(self) -> datetime:
        value = self._now_fn()
        return _aware(value if isinstance(value, datetime) else datetime.now(SHANGHAI))

    def _throttle(self) -> None:
        current = self._monotonic()
        wait = self._min_interval - (current - self._last_request)
        if self._last_request and wait > 0:
            self._sleep(wait)
        self._last_request = self._monotonic()

    def _retry_after(self, response: httpx.Response, attempt: int) -> float:
        value = response.headers.get("Retry-After")
        if value:
            try:
                return min(max(float(value), 0), 60)
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(value)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return min(max((retry_at - self._now()).total_seconds(), 0), 60)
                except (TypeError, ValueError, OverflowError):
                    pass
        return min(0.5 * 2 ** (attempt - 1), 60)

    @staticmethod
    def _failure(start: str, end: str, reason: str, **kwargs: Any) -> GovPolicyFetchResult:
        kwargs.setdefault("fetched_at", datetime.now(SHANGHAI))
        return GovPolicyFetchResult(start_date=start[:32], end_date=end[:32], ok=False, complete=False, reason_code=reason, **kwargs)

    @staticmethod
    def _success(start: str, end: str, reason: str, **kwargs: Any) -> GovPolicyFetchResult:
        return GovPolicyFetchResult(start_date=start, end_date=end, ok=True, complete=True, reason_code=reason, **kwargs)


__all__ = ["GOV_POLICY_ENDPOINT", "GOV_POLICY_SOURCE_ID", "GovPolicyClient", "GovPolicyContractError", "GovPolicyDocument", "GovPolicyFetchResult"]
