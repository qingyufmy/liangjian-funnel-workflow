"""Fail-closed adapters for the public news feeds used by the funnel.

The upstream Vibe-Research project documents four useful public feeds:
财联社滚动电报, 东方财富 7x24, 东方财富个股新闻 and one RSS/Atom source.
This module keeps the useful endpoint knowledge, but gives the rest of this
workflow a small, immutable and auditable contract.  In particular, raw
response bodies are never retained and an item without a trustworthy
publication time is not returned as a news item.

The endpoints are public web endpoints rather than versioned APIs.  Every
parser is therefore deliberately strict: a transport, HTTP, JSON/JSONP or
XML contract failure returns a structured OpenNewsFetchResult with a
reason code.  One source can fail without making another source appear
empty-and-healthy.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SHANGHAI = ZoneInfo("Asia/Shanghai")

CLS_ROLL_ENDPOINT = "https://www.cls.cn/v1/roll/get_roll_list"
EASTMONEY_7X24_ENDPOINT = "https://np-weblist.eastmoney.com/comm/web/getFastNewsList"
EASTMONEY_STOCK_NEWS_ENDPOINT = "https://search-api-web.eastmoney.com/search/jsonp"
OPEN_NEWS_SOURCE_ID = "open_news"
CLS_SOURCE_ID = "open_news.cls_roll"
EASTMONEY_7X24_SOURCE_ID = "open_news.eastmoney_7x24"
MAX_RETRIES = 3
SUMMARY_LIMIT = 300
TITLE_LIMIT = 500

_TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "spm",
    "share_token",
    "fbclid",
    "gclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "_hsenc",
    "_hsmi",
}
_DUP_TITLE_WINDOW = timedelta(hours=48)
_TAG = re.compile(r"<[^>]*>")
_SPACE = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)
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
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/+\-]{0,127}$")
_URL_SCHEME = re.compile(r"^https?://", re.IGNORECASE)


class OpenNewsContractError(ValueError):
    """Internal parser error carrying a stable public reason code only."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _reject_secret(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).lower().replace("_", "").replace("-", "")
            if key_text in {
                "apikey",
                "authorization",
                "accesstoken",
                "refreshtoken",
                "clientsecret",
                "privatekey",
                "password",
                "secret",
                "token",
            }:
                raise OpenNewsContractError("OPEN_NEWS_SECRET_VALUE")
            _reject_secret(key)
            _reject_secret(item)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            _reject_secret(item)
        return
    if isinstance(value, str) and _SECRET.search(value):
        raise OpenNewsContractError("OPEN_NEWS_SECRET_VALUE")


def _clean_text(value: Any, *, limit: int, required: bool = False) -> str:
    if value is None:
        if required:
            raise OpenNewsContractError("OPEN_NEWS_INVALID_ITEM")
        return ""
    if not isinstance(value, str):
        value = str(value)
    text = _SPACE.sub(" ", html.unescape(_TAG.sub(" ", value))).strip()
    if required and not text:
        raise OpenNewsContractError("OPEN_NEWS_INVALID_ITEM")
    if _SECRET.search(text):
        raise OpenNewsContractError("OPEN_NEWS_SECRET_VALUE")
    return text[:limit]


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def parse_publish_time(value: Any) -> datetime | None:
    """Parse provider timestamps without substituting fetch time.

    A missing or malformed value returns None.  The caller must count and
    drop that item; this is intentionally different from using now as a
    fabricated publication time.
    """

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number != number or abs(number) == float("inf") or number <= 0:
            return None
        if number >= 100_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(number, tz=SHANGHAI)
        except (OSError, OverflowError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(text.replace("T", " "))
    except ValueError:
        for pattern in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            try:
                parsed = parsedate_to_datetime(text)
            except (TypeError, ValueError, OverflowError):
                return None
    return _aware(parsed) if parsed is not None else None


def normalize_url(value: str, *, base_url: str | None = None) -> str:
    """Normalize only known tracking parameters.

    Query parameters that may identify an article are retained.  This keeps
    deduplication conservative and avoids silently merging two distinct
    articles.
    """

    if not isinstance(value, str) or not value.strip():
        raise OpenNewsContractError("OPEN_NEWS_INVALID_URL")
    raw = value.strip()
    if base_url is not None:
        raw = urljoin(base_url, raw)
    if not _URL_SCHEME.match(raw):
        raise OpenNewsContractError("OPEN_NEWS_INVALID_URL")
    try:
        parts = urlsplit(raw)
        if (
            parts.hostname is None
            or parts.username is not None
            or parts.password is not None
            or _SECRET.search(parts.query)
        ):
            raise OpenNewsContractError("OPEN_NEWS_INVALID_URL")
        kept = [
            (key, val)
            for key, val in parse_qsl(parts.query, keep_blank_values=True)
            if key.lower() not in _TRACKING_PARAMS
        ]
        return urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path.rstrip("/") or "/",
                urlencode(kept),
                "",
            )
        )
    except (TypeError, ValueError):
        raise OpenNewsContractError("OPEN_NEWS_INVALID_URL") from None


def normalize_title(value: str) -> str:
    """Normalize whitespace and punctuation for conservative repost matching."""

    text = _clean_text(value, limit=TITLE_LIMIT, required=False).lower()
    return _PUNCT.sub("", text)


def build_cls_signature(params: Mapping[str, Any]) -> str:
    """Return the v1 roll signature documented by Vibe-Research.

    The signature excludes sign itself and uses sorted keys exactly as the
    upstream endpoint expects: md5(sha1(query).hexdigest()).
    """

    _reject_secret(params)
    clean = {str(key): str(value) for key, value in params.items() if str(key) != "sign"}
    query = "&".join(f"{key}={clean[key]}" for key in sorted(clean))
    return hashlib.md5(hashlib.sha1(query.encode("utf-8")).hexdigest().encode("utf-8")).hexdigest()


def parse_jsonp(text: str, *, callback: str | None = None) -> Any:
    """Parse one JSONP wrapper and reject executable/non-JSON content."""

    if not isinstance(text, str):
        raise OpenNewsContractError("OPEN_NEWS_JSONP_PARSE_FAILED")
    match = re.match(r"^\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*\((.*)\)\s*;?\s*$", text, re.DOTALL)
    if match is None or (callback is not None and match.group(1) != callback):
        raise OpenNewsContractError("OPEN_NEWS_JSONP_PARSE_FAILED")
    try:
        value = json.loads(match.group(2))
    except (TypeError, ValueError, json.JSONDecodeError):
        raise OpenNewsContractError("OPEN_NEWS_JSONP_PARSE_FAILED") from None
    _reject_secret(value)
    return value


def _identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value or not _IDENTIFIER.fullmatch(value):
        raise OpenNewsContractError(f"OPEN_NEWS_INVALID_{field.upper()}")
    _reject_secret(value)
    return value


def _label(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 128:
        raise OpenNewsContractError(f"OPEN_NEWS_INVALID_{field.upper()}")
    text = value.strip()
    _reject_secret(text)
    return text


def _symbol(value: str) -> str:
    if not isinstance(value, str):
        raise OpenNewsContractError("OPEN_NEWS_INVALID_SYMBOL")
    raw = value.strip().upper()
    match = re.fullmatch(r"(?:(SH|SZ|BJ))?(\d{6})(?:\.(SH|SZ|BJ))?", raw)
    if match is None or (match.group(1) and match.group(3) and match.group(1) != match.group(3)):
        raise OpenNewsContractError("OPEN_NEWS_INVALID_SYMBOL")
    code = match.group(2)
    exchange = match.group(1) or match.group(3)
    if exchange is None:
        if code.startswith(("92", "4", "8")):
            exchange = "BJ"
        elif code.startswith(("6", "9")):
            exchange = "SH"
        else:
            exchange = "SZ"
    return f"{code}.{exchange}"


class OpenNewsItem(BaseModel):
    """One bounded news item; provider prose remains explicitly untrusted."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    provider_item_id: str | None = None
    title: str
    summary: str = ""
    publish_time: datetime
    url: str
    symbol: str | None = None
    channel: str
    fetched_at: datetime
    prompt_injection_suspected: bool = False
    untrusted_text: bool = True
    original_sources: tuple[str, ...] = ()
    repost_count: int = Field(default=1, ge=1)

    @field_validator("source_id")
    @classmethod
    def validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, field=info.field_name)

    @field_validator("channel")
    @classmethod
    def validate_channel(cls, value: str) -> str:
        return _label(value, field="channel")

    @field_validator("provider_item_id")
    @classmethod
    def validate_provider_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or len(value) > 128 or _SECRET.search(value):
            raise ValueError("invalid provider_item_id")
        return value

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        return _clean_text(value, limit=TITLE_LIMIT, required=True)

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        return _clean_text(value, limit=SUMMARY_LIMIT)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return normalize_url(value)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str | None) -> str | None:
        return None if value is None else _symbol(value)

    @field_validator("publish_time", "fetched_at")
    @classmethod
    def validate_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("news timestamps must be timezone-aware")
        return _aware(value)

    @field_validator("original_sources")
    @classmethod
    def validate_sources(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        unique = []
        for source in value:
            _identifier(source, field="original_sources")
            if source not in unique:
                unique.append(source)
        return tuple(sorted(unique))

    @model_validator(mode="after")
    def validate_item(self) -> "OpenNewsItem":
        if not self.untrusted_text:
            raise ValueError("news text must remain untrusted")
        if self.publish_time > self.fetched_at:
            raise ValueError("publish_time cannot be after fetched_at")
        if _INJECTION.search(f"{self.title} {self.summary}"):
            object.__setattr__(self, "prompt_injection_suspected", True)
        if self.source_id not in self.original_sources:
            object.__setattr__(
                self,
                "original_sources",
                tuple(sorted(set(self.original_sources) | {self.source_id})),
            )
        _reject_secret(self.model_dump(mode="python"))
        return self

    @property
    def pubtime(self) -> datetime:
        return self.publish_time


class OpenNewsFetchResult(BaseModel):
    """Structured result for one provider/source request."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    channel: str
    source_url: str
    ok: bool
    complete: bool
    reason_code: str
    items: tuple[OpenNewsItem, ...] = ()
    fetched_at: datetime
    http_status: int | None = Field(default=None, ge=100, le=599)
    attempts: int = Field(default=0, ge=0)
    dropped_missing_time: int = Field(default=0, ge=0)
    dropped_invalid_items: int = Field(default=0, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_id", "reason_code")
    @classmethod
    def validate_result_identifiers(cls, value: str, info: Any) -> str:
        return _identifier(value, field=info.field_name)

    @field_validator("channel")
    @classmethod
    def validate_result_channel(cls, value: str) -> str:
        return _label(value, field="channel")

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str) -> str:
        return normalize_url(value)

    @field_validator("fetched_at")
    @classmethod
    def validate_fetched_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")
        return _aware(value)

    @model_validator(mode="after")
    def validate_result(self) -> "OpenNewsFetchResult":
        if self.ok != self.complete:
            raise ValueError("ok and complete must agree")
        if self.reason_code == "OK" and not self.complete:
            raise ValueError("failed result cannot have OK reason")
        _reject_secret(self.metadata)
        return self

    @property
    def fetch_time(self) -> datetime:
        return self.fetched_at

    @property
    def records(self) -> tuple[OpenNewsItem, ...]:
        return self.items


def _source_for_rss(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return f"open_news.rss.{digest}"


def _provider_item_id(raw: Mapping[str, Any]) -> str | None:
    for key in ("id", "roll_id", "news_id", "article_id", "art_code", "code"):
        value = raw.get(key)
        if value not in (None, ""):
            text = str(value).strip()
            if text and len(text) <= 128 and not _SECRET.search(text):
                return text
    return None


def _fallback_item_url(endpoint: str, provider_item_id: str | None) -> str:
    if provider_item_id and endpoint == CLS_ROLL_ENDPOINT:
        return f"https://www.cls.cn/detail/{provider_item_id}"
    if provider_item_id and endpoint == EASTMONEY_7X24_ENDPOINT:
        # Fast-news rows usually omit ``url`` but expose a stable article
        # code.  Reusing the collection endpoint would collapse every row as
        # the same URL during deduplication.
        return f"https://finance.eastmoney.com/a/{provider_item_id}.html"
    return endpoint


def _make_item(
    raw: Mapping[str, Any],
    *,
    source_id: str,
    channel: str,
    fetched_at: datetime,
    time_value: Any,
    symbol: str | None = None,
    endpoint: str,
    title_value: Any = None,
    summary_value: Any = None,
    url_value: Any = None,
) -> OpenNewsItem:
    provider_id = _provider_item_id(raw)
    title = _clean_text(
        raw.get("title") if title_value is None else title_value,
        limit=TITLE_LIMIT,
        required=True,
    )
    summary = _clean_text(
        (raw.get("summary") or raw.get("content") or raw.get("brief"))
        if summary_value is None
        else summary_value,
        limit=SUMMARY_LIMIT,
    )
    publish_time = parse_publish_time(time_value)
    if publish_time is None:
        raise OpenNewsContractError("OPEN_NEWS_MISSING_PUBLISH_TIME")
    if publish_time > fetched_at:
        raise OpenNewsContractError("OPEN_NEWS_FUTURE_PUBLISH_TIME")
    raw_url = raw.get("url") if url_value is None else url_value
    if not raw_url:
        raw_url = _fallback_item_url(endpoint, provider_id)
    url = normalize_url(str(raw_url), base_url=endpoint)
    injection = bool(_INJECTION.search(f"{title} {summary}"))
    try:
        return OpenNewsItem(
            source_id=source_id,
            provider_item_id=provider_id,
            title=title,
            summary=summary,
            publish_time=publish_time,
            url=url,
            symbol=symbol,
            channel=channel,
            fetched_at=fetched_at,
            prompt_injection_suspected=injection,
            original_sources=(source_id,),
        )
    except (TypeError, ValueError):
        raise OpenNewsContractError("OPEN_NEWS_INVALID_ITEM") from None


def _partial_reason(*, missing_time: int, invalid_items: int) -> str:
    if missing_time:
        return "PARTIAL_TIMESTAMP_COVERAGE"
    if invalid_items:
        return "PARTIAL_ITEM_COVERAGE"
    return "OK"


def _parse_cls_payload(
    payload: Any,
    *,
    source_id: str,
    channel: str,
    endpoint: str,
    fetched_at: datetime,
) -> tuple[list[OpenNewsItem], int, int]:
    if not isinstance(payload, Mapping):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    errno = payload.get("errno")
    if errno not in (None, 0, "0"):
        raise OpenNewsContractError("OPEN_NEWS_BUSINESS_ERROR")
    data = payload.get("data")
    if not isinstance(data, Mapping) or not isinstance(data.get("roll_data"), list):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    items: list[OpenNewsItem] = []
    missing = invalid = 0
    for raw in data["roll_data"]:
        if not isinstance(raw, Mapping):
            invalid += 1
            continue
        try:
            item = _make_item(
                raw,
                source_id=source_id,
                channel=channel,
                fetched_at=fetched_at,
                time_value=raw.get("ctime"),
                endpoint=endpoint,
                title_value=raw.get("title") or raw.get("brief"),
                summary_value=raw.get("content") or raw.get("brief"),
                url_value=raw.get("url") or raw.get("share_url"),
            )
        except OpenNewsContractError as exc:
            if exc.reason_code == "OPEN_NEWS_MISSING_PUBLISH_TIME":
                missing += 1
            else:
                invalid += 1
            continue
        items.append(item)
    return items, missing, invalid


def _parse_global_payload(
    payload: Any,
    *,
    source_id: str,
    channel: str,
    endpoint: str,
    fetched_at: datetime,
) -> tuple[list[OpenNewsItem], int, int]:
    if not isinstance(payload, Mapping):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    data = payload.get("data")
    if not isinstance(data, Mapping) or not isinstance(data.get("fastNewsList"), list):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    items: list[OpenNewsItem] = []
    missing = invalid = 0
    for raw in data["fastNewsList"]:
        if not isinstance(raw, Mapping):
            invalid += 1
            continue
        try:
            item = _make_item(
                raw,
                source_id=source_id,
                channel=channel,
                fetched_at=fetched_at,
                time_value=raw.get("showTime") or raw.get("publishTime") or raw.get("time"),
                endpoint=endpoint,
                title_value=raw.get("title"),
                summary_value=raw.get("summary") or raw.get("content"),
                url_value=raw.get("url") or raw.get("newsUrl"),
            )
        except OpenNewsContractError as exc:
            if exc.reason_code == "OPEN_NEWS_MISSING_PUBLISH_TIME":
                missing += 1
            else:
                invalid += 1
            continue
        items.append(item)
    return items, missing, invalid


def _parse_stock_payload(
    payload: Any,
    *,
    source_id: str,
    channel: str,
    endpoint: str,
    fetched_at: datetime,
    symbol: str,
) -> tuple[list[OpenNewsItem], int, int]:
    if not isinstance(payload, Mapping):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    result = payload.get("result")
    if not isinstance(result, Mapping) or not isinstance(result.get("cmsArticleWebOld"), list):
        raise OpenNewsContractError("OPEN_NEWS_CONTRACT_CHANGED")
    items: list[OpenNewsItem] = []
    missing = invalid = 0
    for raw in result["cmsArticleWebOld"]:
        if not isinstance(raw, Mapping):
            invalid += 1
            continue
        try:
            item = _make_item(
                raw,
                source_id=source_id,
                channel=channel,
                fetched_at=fetched_at,
                time_value=raw.get("date") or raw.get("publishTime") or raw.get("time"),
                endpoint=endpoint,
                symbol=symbol,
                title_value=raw.get("title"),
                summary_value=raw.get("content") or raw.get("summary"),
                url_value=raw.get("url"),
            )
        except OpenNewsContractError as exc:
            if exc.reason_code == "OPEN_NEWS_MISSING_PUBLISH_TIME":
                missing += 1
            else:
                invalid += 1
            continue
        items.append(item)
    return items, missing, invalid


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _xml_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return _SPACE.sub(" ", "".join(node.itertext())).strip()


def _xml_child(node: ET.Element, names: set[str]) -> ET.Element | None:
    for child in list(node):
        if _local_tag(child.tag) in names:
            return child
    return None


def _parse_rss(
    raw: bytes,
    *,
    source_id: str,
    channel: str,
    endpoint: str,
    fetched_at: datetime,
    page_size: int,
) -> tuple[list[OpenNewsItem], int, int]:
    try:
        root = ET.fromstring(raw)
    except (ET.ParseError, ValueError, TypeError):
        raise OpenNewsContractError("OPEN_NEWS_XML_PARSE_FAILED") from None
    nodes = [
        node
        for node in root.iter()
        if _local_tag(node.tag) in {"item", "entry"}
    ]
    items: list[OpenNewsItem] = []
    missing = invalid = 0
    for node in nodes[:page_size]:
        title = _xml_text(_xml_child(node, {"title"}))
        link_node = _xml_child(node, {"link"})
        link = (link_node.get("href") if link_node is not None else "") or _xml_text(link_node)
        time_node = _xml_child(node, {"pubdate", "published", "updated", "date"})
        summary_node = _xml_child(node, {"description", "summary", "content", "encoded"})
        raw_item = {
            "title": title,
            "url": link,
            "summary": _xml_text(summary_node),
        }
        try:
            item = _make_item(
                raw_item,
                source_id=source_id,
                channel=channel,
                fetched_at=fetched_at,
                time_value=_xml_text(time_node),
                endpoint=endpoint,
                title_value=title,
                summary_value=_xml_text(summary_node),
                url_value=link,
            )
        except OpenNewsContractError as exc:
            if exc.reason_code == "OPEN_NEWS_MISSING_PUBLISH_TIME":
                missing += 1
            else:
                invalid += 1
            continue
        items.append(item)
    return items, missing, invalid


def _dedup_source_names(*items: OpenNewsItem) -> tuple[str, ...]:
    names = {source for item in items for source in item.original_sources}
    names.update(item.source_id for item in items)
    return tuple(sorted(names))


def deduplicate_news(items: Iterable[OpenNewsItem]) -> tuple[OpenNewsItem, ...]:
    """Deduplicate URL repeats and 48-hour same-title reposts.

    Items are sorted newest-first, so the group representative is always the
    latest item.  Repost provenance is retained in original_sources and
    repost_count.
    """

    ordered = sorted(
        tuple(items),
        key=lambda item: (
            -item.publish_time.timestamp(),
            item.source_id,
            item.provider_item_id or "",
            item.url,
        ),
    )
    groups: list[OpenNewsItem] = []
    url_groups: dict[str, int] = {}
    title_groups: dict[str, tuple[int, datetime]] = {}
    for item in ordered:
        url_key = normalize_url(item.url)
        title_key = normalize_title(item.title)
        group_index = url_groups.get(url_key)
        if group_index is None and title_key:
            candidate = title_groups.get(title_key)
            if candidate is not None and abs(item.publish_time - candidate[1]) <= _DUP_TITLE_WINDOW:
                group_index = candidate[0]
        if group_index is None:
            groups.append(item)
            group_index = len(groups) - 1
        else:
            current = groups[group_index]
            groups[group_index] = current.model_copy(
                update={
                    "original_sources": _dedup_source_names(current, item),
                    "repost_count": current.repost_count + item.repost_count,
                }
            )
        url_groups[url_key] = group_index
        if title_key:
            title_groups[title_key] = (group_index, groups[group_index].publish_time)
    return tuple(groups)


class OpenNewsClient:
    """Context-managed client for four independent public news sources."""

    def __init__(
        self,
        *,
        transport: httpx.BaseTransport | None = None,
        client: httpx.Client | None = None,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 15,
        max_retries: int = MAX_RETRIES,
        base_cls_url: str = CLS_ROLL_ENDPOINT,
        base_eastmoney_7x24_url: str = EASTMONEY_7X24_ENDPOINT,
        base_eastmoney_stock_url: str = EASTMONEY_STOCK_NEWS_ENDPOINT,
    ) -> None:
        if client is not None and http_client is not None:
            raise ValueError("provide only one HTTP client")
        if timeout_seconds <= 0 or not 1 <= max_retries <= 5:
            raise ValueError("invalid open news client timing")
        injected = client or http_client
        self._owns = injected is None
        self._client = injected or httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            follow_redirects=True,
            trust_env=False,
        )
        self._sleep = sleep
        self._now_fn = now or (lambda: datetime.now(SHANGHAI))
        self._max_retries = max_retries
        self._endpoints = {
            "cls": normalize_url(base_cls_url),
            "global": normalize_url(base_eastmoney_7x24_url),
            "stock": normalize_url(base_eastmoney_stock_url),
        }

    def __enter__(self) -> "OpenNewsClient":
        return self

    def __exit__(self, *_args: object) -> None:
        if self._owns:
            self.close()

    def close(self) -> None:
        if self._owns:
            self._client.close()

    def _now(self) -> datetime:
        value = self._now_fn()
        if not isinstance(value, datetime):
            return datetime.now(SHANGHAI)
        return _aware(value)

    def _get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str],
    ) -> tuple[httpx.Response | None, int, str | None]:
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._client.get(url, params=dict(params or {}), headers=dict(headers))
            except httpx.TimeoutException:
                return None, attempt, "OPEN_NEWS_TIMEOUT"
            except (httpx.HTTPError, TimeoutError, OSError):
                return None, attempt, "OPEN_NEWS_REQUEST_FAILED"
            except Exception:
                return None, attempt, "OPEN_NEWS_REQUEST_FAILED"
            if response.status_code == 429:
                if attempt < self._max_retries:
                    self._sleep(min(0.5 * (2 ** (attempt - 1)), 5))
                    continue
                return response, attempt, "OPEN_NEWS_RATE_LIMITED"
            if 500 <= response.status_code <= 599:
                if attempt < self._max_retries:
                    self._sleep(min(0.5 * (2 ** (attempt - 1)), 5))
                    continue
                return response, attempt, "OPEN_NEWS_HTTP_5XX"
            if 400 <= response.status_code <= 499:
                return response, attempt, "OPEN_NEWS_HTTP_4XX"
            if not 200 <= response.status_code < 300:
                return response, attempt, "OPEN_NEWS_HTTP_ERROR"
            return response, attempt, None
        return None, self._max_retries, "OPEN_NEWS_REQUEST_FAILED"

    def _failure(
        self,
        *,
        source_id: str,
        channel: str,
        endpoint: str,
        fetched_at: datetime,
        reason_code: str,
        attempts: int = 0,
        http_status: int | None = None,
        **metadata: Any,
    ) -> OpenNewsFetchResult:
        return OpenNewsFetchResult(
            source_id=source_id,
            channel=channel,
            source_url=endpoint,
            ok=False,
            complete=False,
            reason_code=reason_code,
            items=(),
            fetched_at=fetched_at,
            attempts=attempts,
            http_status=http_status,
            metadata=metadata,
        )

    def _json_result(
        self,
        *,
        source_id: str,
        channel: str,
        endpoint: str,
        params: Mapping[str, Any],
        fetched_at: datetime,
        parser: Callable[[Any], tuple[list[OpenNewsItem], int, int]],
        jsonp_callback: str | None = None,
    ) -> OpenNewsFetchResult:
        response, attempts, reason = self._get(
            endpoint,
            params=params,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.cls.cn/" if source_id == CLS_SOURCE_ID else "https://so.eastmoney.com/",
            },
        )
        if reason is not None:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=reason,
                attempts=attempts,
                http_status=response.status_code if response is not None else None,
            )
        assert response is not None
        try:
            if jsonp_callback is None:
                payload = response.json()
            else:
                payload = parse_jsonp(response.text, callback=jsonp_callback)
            items, missing, invalid = parser(payload)
        except OpenNewsContractError as exc:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=exc.reason_code,
                attempts=attempts,
                http_status=response.status_code,
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_JSON",
                attempts=attempts,
                http_status=response.status_code,
            )
        except Exception:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_PARSE_FAILED",
                attempts=attempts,
                http_status=response.status_code,
            )
        return OpenNewsFetchResult(
            source_id=source_id,
            channel=channel,
            source_url=endpoint,
            ok=True,
            complete=True,
            reason_code=_partial_reason(missing_time=missing, invalid_items=invalid),
            items=tuple(items),
            fetched_at=fetched_at,
            attempts=attempts,
            http_status=response.status_code,
            dropped_missing_time=missing,
            dropped_invalid_items=invalid,
            metadata={"raw_item_count": len(items) + missing + invalid},
        )

    def fetch_cls_roll(self, *, page_size: int = 50, last_time: str = "") -> OpenNewsFetchResult:
        fetched_at = self._now()
        source_id, channel, endpoint = CLS_SOURCE_ID, "cls_roll", self._endpoints["cls"]
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_PAGE_SIZE",
            )
        if not isinstance(last_time, str) or len(last_time) > 32 or _SECRET.search(last_time):
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_CURSOR",
            )
        params: dict[str, Any] = {
            "appName": "CailianpressWeb",
            "os": "web",
            "sv": "7.7.5",
            "last_time": last_time,
            "refresh_type": "1",
            "rn": str(page_size),
        }
        params["sign"] = build_cls_signature(params)
        return self._json_result(
            source_id=source_id,
            channel=channel,
            endpoint=endpoint,
            params=params,
            fetched_at=fetched_at,
            parser=lambda payload: _parse_cls_payload(
                payload,
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
            ),
        )

    def fetch_cls(self, **kwargs: Any) -> OpenNewsFetchResult:
        return self.fetch_cls_roll(**kwargs)

    def fetch_eastmoney_7x24(self, *, page_size: int = 50, sort_end: str = "") -> OpenNewsFetchResult:
        fetched_at = self._now()
        source_id, channel, endpoint = EASTMONEY_7X24_SOURCE_ID, "eastmoney_7x24", self._endpoints["global"]
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_PAGE_SIZE",
            )
        if not isinstance(sort_end, str) or len(sort_end) > 64 or _SECRET.search(sort_end):
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_CURSOR",
            )
        params = {
            "client": "web",
            "biz": "web_724",
            "fastColumn": "102",
            "sortEnd": sort_end,
            "pageSize": str(page_size),
            "req_trace": str(uuid.uuid4()),
        }
        return self._json_result(
            source_id=source_id,
            channel=channel,
            endpoint=endpoint,
            params=params,
            fetched_at=fetched_at,
            parser=lambda payload: _parse_global_payload(
                payload,
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
            ),
        )

    def fetch_eastmoney_global(self, **kwargs: Any) -> OpenNewsFetchResult:
        return self.fetch_eastmoney_7x24(**kwargs)

    def fetch_eastmoney_stock_news(self, symbol: str, *, page_size: int = 20) -> OpenNewsFetchResult:
        fetched_at = self._now()
        endpoint = self._endpoints["stock"]
        try:
            canonical_symbol = _symbol(symbol)
        except OpenNewsContractError as exc:
            return self._failure(
                source_id="open_news.eastmoney_stock.invalid",
                channel="eastmoney_stock",
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=exc.reason_code,
            )
        source_id = f"open_news.eastmoney_stock.{canonical_symbol.replace('.', '_')}"
        channel = "eastmoney_stock"
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
            return self._failure(
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_PAGE_SIZE",
            )
        code = canonical_symbol.split(".", 1)[0]
        callback = "jQuery_news"
        inner_params = json.dumps(
            {
                "uid": "",
                "keyword": code,
                "type": ["cmsArticleWebOld"],
                "client": "web",
                "clientType": "web",
                "clientVersion": "curr",
                "param": {
                    "cmsArticleWebOld": {
                        "searchScope": "default",
                        "sort": "default",
                        "pageIndex": 1,
                        "pageSize": page_size,
                        "preTag": "",
                        "postTag": "",
                    }
                },
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
        params = {"cb": callback, "param": inner_params}
        return self._json_result(
            source_id=source_id,
            channel=channel,
            endpoint=endpoint,
            params=params,
            fetched_at=fetched_at,
            jsonp_callback=callback,
            parser=lambda payload: _parse_stock_payload(
                payload,
                source_id=source_id,
                channel=channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                symbol=canonical_symbol,
            ),
        )

    def fetch_stock_news(self, symbol: str, **kwargs: Any) -> OpenNewsFetchResult:
        return self.fetch_eastmoney_stock_news(symbol, **kwargs)

    def fetch_rss(
        self,
        source_url: str,
        *,
        source_id: str | None = None,
        channel: str | None = None,
        page_size: int = 50,
    ) -> OpenNewsFetchResult:
        fetched_at = self._now()
        try:
            endpoint = normalize_url(source_url)
            rss_source_id = source_id or _source_for_rss(endpoint)
            _identifier(rss_source_id, field="source_id")
            rss_channel = _label(channel or rss_source_id, field="channel")
        except OpenNewsContractError as exc:
            fallback_source = source_id if isinstance(source_id, str) and source_id else "open_news.rss.invalid"
            fallback_channel = channel if isinstance(channel, str) and channel else "rss"
            try:
                fallback_source = _identifier(fallback_source, field="source_id")
            except OpenNewsContractError:
                fallback_source = "open_news.rss.invalid"
            try:
                fallback_channel = _identifier(fallback_channel, field="channel")
            except OpenNewsContractError:
                fallback_channel = "rss"
            endpoint = normalize_url(EASTMONEY_7X24_ENDPOINT)
            return self._failure(
                source_id=fallback_source,
                channel=fallback_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=exc.reason_code,
            )
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 200:
            return self._failure(
                source_id=rss_source_id,
                channel=rss_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_INVALID_PAGE_SIZE",
            )
        response, attempts, reason = self._get(
            endpoint,
            params=None,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                ),
                "Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
                "Referer": endpoint,
            },
        )
        if reason is not None:
            return self._failure(
                source_id=rss_source_id,
                channel=rss_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=reason,
                attempts=attempts,
                http_status=response.status_code if response is not None else None,
            )
        assert response is not None
        try:
            items, missing, invalid = _parse_rss(
                response.content,
                source_id=rss_source_id,
                channel=rss_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                page_size=page_size,
            )
        except OpenNewsContractError as exc:
            return self._failure(
                source_id=rss_source_id,
                channel=rss_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code=exc.reason_code,
                attempts=attempts,
                http_status=response.status_code,
            )
        except Exception:
            return self._failure(
                source_id=rss_source_id,
                channel=rss_channel,
                endpoint=endpoint,
                fetched_at=fetched_at,
                reason_code="OPEN_NEWS_PARSE_FAILED",
                attempts=attempts,
                http_status=response.status_code,
            )
        return OpenNewsFetchResult(
            source_id=rss_source_id,
            channel=rss_channel,
            source_url=endpoint,
            ok=True,
            complete=True,
            reason_code=_partial_reason(missing_time=missing, invalid_items=invalid),
            items=tuple(items),
            fetched_at=fetched_at,
            attempts=attempts,
            http_status=response.status_code,
            dropped_missing_time=missing,
            dropped_invalid_items=invalid,
            metadata={"raw_item_count": len(items) + missing + invalid},
        )


def collect_open_news(
    results: Mapping[str, OpenNewsFetchResult] | Iterable[OpenNewsFetchResult],
) -> tuple[OpenNewsItem, ...]:
    """Merge successful source results and apply deterministic repost dedup."""

    values = results.values() if isinstance(results, Mapping) else results
    items = [
        item
        for result in values
        if result.ok and result.complete
        for item in result.items
    ]
    return deduplicate_news(items)


def collect_open_news_results(
    client: OpenNewsClient,
    *,
    symbols: Iterable[str] = (),
    rss_sources: Iterable[str | Mapping[str, str]] = (),
    page_size: int = 50,
) -> dict[str, OpenNewsFetchResult]:
    """Collect all configured sources independently.

    rss_sources accepts URLs or {"url", "source_id", "channel"} mappings.  A
    malformed stock/RSS source becomes its own structured failure and never
    prevents the remaining providers from running.
    """

    results: dict[str, OpenNewsFetchResult] = {}
    cls_result = client.fetch_cls_roll(page_size=page_size)
    results[cls_result.source_id] = cls_result
    global_result = client.fetch_eastmoney_7x24(page_size=page_size)
    results[global_result.source_id] = global_result
    for raw_symbol in symbols:
        result = client.fetch_eastmoney_stock_news(raw_symbol, page_size=min(page_size, 100))
        results[result.source_id] = result
    for source in rss_sources:
        if isinstance(source, str):
            result = client.fetch_rss(source, page_size=page_size)
        elif isinstance(source, Mapping) and isinstance(source.get("url"), str):
            result = client.fetch_rss(
                source["url"],
                source_id=source.get("source_id"),
                channel=source.get("channel") or source.get("hint") or "rss",
                page_size=page_size,
            )
        else:
            result = client.fetch_rss(
                "",
                source_id="open_news.rss.invalid",
                channel="rss",
                page_size=page_size,
            )
        results[result.source_id] = result
    return results


collect_news_results = collect_open_news_results
normalize_news_url = normalize_url
normalize_news_title = normalize_title


__all__ = [
    "CLS_ROLL_ENDPOINT",
    "CLS_SOURCE_ID",
    "EASTMONEY_7X24_ENDPOINT",
    "EASTMONEY_7X24_SOURCE_ID",
    "EASTMONEY_STOCK_NEWS_ENDPOINT",
    "MAX_RETRIES",
    "OpenNewsClient",
    "OpenNewsContractError",
    "OpenNewsFetchResult",
    "OpenNewsItem",
    "build_cls_signature",
    "collect_news_results",
    "collect_open_news",
    "collect_open_news_results",
    "deduplicate_news",
    "normalize_news_title",
    "normalize_news_url",
    "normalize_title",
    "normalize_url",
    "parse_jsonp",
    "parse_publish_time",
]
