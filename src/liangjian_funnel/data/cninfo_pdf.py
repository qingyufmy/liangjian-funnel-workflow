"""Bounded, auditable PDF evidence extraction for public CNINFO filings."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
import pypdf
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .cninfo import CNINFO_REFERER, CNINFO_USER_AGENT, CninfoAnnouncement


SHANGHAI = ZoneInfo("Asia/Shanghai")
CNINFO_PDF_HOST = "static.cninfo.com.cn"
MAX_PDF_BYTES = 20 * 1024 * 1024
MAX_PDF_PAGES = 200
MAX_EXTRACTED_CHARS = 200_000
MAX_EVIDENCE_SNIPPETS = 12
MAX_SNIPPET_CHARS = 500
_CONTENT_TYPES = {"application/pdf", "application/octet-stream"}
_EVIDENCE_KEYWORDS = (
    "中标", "合同", "订单", "金额", "收入", "营收", "产能", "投产", "客户",
    "减持", "质押", "冻结", "诉讼", "仲裁", "处罚", "调查", "审计", "退市",
    "风险", "现金流", "毛利率", "净利润", "同比", "报告期",
)
_INJECTION = re.compile(
    r"(?:ignore\s+(?:all\s+)?(?:previous|prior)\s+instructions|system\s+prompt|"
    r"developer\s+message|reveal\s+(?:the\s+)?prompt|忽略.{0,12}(?:指令|规则)|"
    r"系统提示词|开发者消息|泄露.{0,12}提示词)",
    re.IGNORECASE,
)
_SECRET_LIKE = re.compile(
    r"(?ix)(?:\bsk-[a-z0-9][a-z0-9_-]{7,}\b|\bbearer\s+[a-z0-9._~+/=-]{8,}|"
    r"(?:api[_-]?key|access[_-]?token|client[_-]?secret|password)\s*[:=]\s*[^\s,;]+)"
)


class PdfEvidenceSnippet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    page_number: int = Field(ge=1)
    text: str = Field(min_length=1, max_length=MAX_SNIPPET_CHARS)
    matched_keywords: tuple[str, ...] = ()
    prompt_injection_suspected: bool = False


class CninfoPdfEvidence(BaseModel):
    """No full document text: only bounded page-addressable evidence."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    announcement_id: str = Field(min_length=1, max_length=128)
    pdf_url: str
    available: bool
    reason_code: str
    fetched_at: datetime
    http_status: int | None = Field(default=None, ge=100, le=599)
    attempts: int = Field(default=0, ge=0)
    cache_hit: bool = False
    pdf_sha256: str | None = None
    cache_relative_path: str | None = None
    content_type: str | None = None
    byte_size: int | None = Field(default=None, ge=0)
    parser: str = f"pypdf/{pypdf.__version__}"
    page_count: int | None = Field(default=None, ge=0)
    pages_scanned: int = Field(default=0, ge=0)
    extracted_chars: int = Field(default=0, ge=0)
    truncated: bool = False
    prompt_injection_suspected: bool = False
    snippets: tuple[PdfEvidenceSnippet, ...] = ()

    @field_validator("pdf_url")
    @classmethod
    def validate_pdf_url(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("empty CNINFO PDF URL")
        return value

    @field_validator("fetched_at")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:
        return _aware(value)

    @field_validator("pdf_sha256")
    @classmethod
    def validate_sha256(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("invalid PDF SHA-256")
        return value

    @field_validator("cache_relative_path")
    @classmethod
    def validate_relative_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("raw",):
            raise ValueError("invalid PDF cache relative path")
        return path.as_posix()

    @model_validator(mode="after")
    def validate_outcome(self) -> "CninfoPdfEvidence":
        if not _approved_url(self.pdf_url) and self.reason_code != "CNINFO_PDF_URL_REJECTED":
            raise ValueError("unapproved CNINFO PDF URL")
        if self.content_type is not None and self.content_type not in _CONTENT_TYPES:
            raise ValueError("invalid cached PDF content type")
        if self.available:
            if self.reason_code != "OK" or self.pdf_sha256 is None or self.cache_relative_path is None:
                raise ValueError("available PDF evidence requires an OK hash-bound cache record")
            if not self.page_count or self.extracted_chars < 1 or not self.snippets:
                raise ValueError("available PDF evidence requires extracted page evidence")
        elif self.reason_code == "OK" or self.snippets:
            raise ValueError("unavailable PDF evidence cannot carry OK snippets")
        return self


class CninfoPdfClient:
    def __init__(
        self,
        cache_dir: str | os.PathLike[str],
        *,
        timeout_seconds: float = 30.0,
        max_attempts: int = 3,
        max_bytes: int = MAX_PDF_BYTES,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout_seconds <= 0 or max_attempts < 1 or max_bytes < 5:
            raise ValueError("invalid CNINFO PDF client bounds")
        self.cache_dir = Path(cache_dir).resolve()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.max_bytes = max_bytes
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(SHANGHAI))
        self._monotonic = monotonic
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout_seconds, follow_redirects=False)

    def __enter__(self) -> "CninfoPdfClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch_evidence(self, announcement: CninfoAnnouncement) -> CninfoPdfEvidence:
        url = announcement.pdf_url
        if not _approved_url(url):
            return self._failure(announcement, "CNINFO_PDF_URL_REJECTED")
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        pdf_path = self._cache_path("raw", f"{key}.pdf")
        sidecar_path = self._cache_path("metadata", f"{key}.json")
        cached = self._valid_cache(pdf_path, sidecar_path, url)
        if cached is not None:
            digest, size, content_type = cached
            return self._extract(
                announcement,
                pdf_path,
                digest=digest,
                size=size,
                content_type=content_type,
                attempts=0,
                http_status=200,
                cache_hit=True,
            )

        downloaded = self._download(url)
        if downloaded[0] is None:
            _, reason, status, attempts = downloaded
            return self._failure(announcement, reason, http_status=status, attempts=attempts)
        body, content_type, status, attempts = downloaded
        assert isinstance(body, bytes)
        digest = hashlib.sha256(body).hexdigest()
        try:
            self._atomic_write(pdf_path, body)
        except OSError:
            return self._failure(
                announcement,
                "CNINFO_PDF_CACHE_WRITE_FAILED",
                http_status=status,
                attempts=attempts,
                digest=digest,
                size=len(body),
                content_type=content_type,
            )
        sidecar = {
            "schema_version": "liangjian-cninfo-pdf-cache/1.0.0",
            "url": url,
            "sha256": digest,
            "byte_size": len(body),
            "content_type": content_type,
        }
        try:
            self._atomic_write(
                sidecar_path,
                json.dumps(sidecar, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
        except OSError:
            return self._failure(
                announcement,
                "CNINFO_PDF_CACHE_WRITE_FAILED",
                http_status=status,
                attempts=attempts,
                digest=digest,
                size=len(body),
                content_type=content_type,
                path=pdf_path,
            )
        return self._extract(
            announcement,
            pdf_path,
            digest=digest,
            size=len(body),
            content_type=content_type,
            attempts=attempts,
            http_status=status,
            cache_hit=False,
        )

    def _download(self, url: str) -> tuple[bytes | None, str, int | None, int]:
        last_status: int | None = None
        for attempt in range(1, self.max_attempts + 1):
            deadline = self._monotonic() + self.timeout_seconds
            try:
                with self._client.stream(
                    "GET",
                    url,
                    headers={"User-Agent": CNINFO_USER_AGENT, "Referer": CNINFO_REFERER},
                    timeout=self.timeout_seconds,
                    follow_redirects=False,
                ) as response:
                    status = response.status_code
                    last_status = status
                    if 300 <= status < 400:
                        return None, "CNINFO_PDF_REDIRECT_REJECTED", status, attempt
                    if status == 429 or 500 <= status <= 599:
                        if attempt < self.max_attempts:
                            self._sleep(_retry_delay(response.headers.get("Retry-After"), attempt))
                            continue
                        reason = "CNINFO_PDF_RATE_LIMITED" if status == 429 else "CNINFO_PDF_HTTP_5XX"
                        return None, reason, status, attempt
                    if status < 200 or status >= 300:
                        return None, "CNINFO_PDF_HTTP_4XX", status, attempt
                    if not _approved_url(str(response.url)):
                        return None, "CNINFO_PDF_URL_REJECTED", status, attempt
                    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                    if content_type not in _CONTENT_TYPES:
                        return None, "CNINFO_PDF_CONTENT_TYPE_INVALID", status, attempt
                    chunks: list[bytes] = []
                    size = 0
                    for chunk in response.iter_bytes():
                        if self._monotonic() >= deadline:
                            return None, "CNINFO_PDF_TIMEOUT", status, attempt
                        size += len(chunk)
                        if size > self.max_bytes:
                            return None, "CNINFO_PDF_TOO_LARGE", status, attempt
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    if not body.startswith(b"%PDF"):
                        return None, "CNINFO_PDF_MAGIC_INVALID", status, attempt
                    return body, content_type, status, attempt
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError):
                if attempt < self.max_attempts:
                    self._sleep(float(2 ** (attempt - 1)))
                    continue
                return None, "CNINFO_PDF_REQUEST_FAILED", last_status, attempt
        return None, "CNINFO_PDF_REQUEST_FAILED", last_status, self.max_attempts

    def _extract(
        self,
        announcement: CninfoAnnouncement,
        path: Path,
        *,
        digest: str,
        size: int,
        content_type: str,
        attempts: int,
        http_status: int | None,
        cache_hit: bool,
    ) -> CninfoPdfEvidence:
        try:
            from pypdf import PdfReader
            from pypdf.errors import PdfReadError
        except ImportError:
            return self._failure(
                announcement, "CNINFO_PDF_PARSER_UNAVAILABLE", http_status=http_status, attempts=attempts
            )
        try:
            reader = PdfReader(str(path), strict=False)
            if reader.is_encrypted:
                return self._failure(
                    announcement, "CNINFO_PDF_ENCRYPTED", http_status=http_status, attempts=attempts,
                    cache_hit=cache_hit, digest=digest, size=size, content_type=content_type, path=path,
                )
            page_count = len(reader.pages)
            pages_scanned = min(page_count, MAX_PDF_PAGES)
            page_texts: list[tuple[int, str]] = []
            extracted_chars = 0
            truncated = page_count > MAX_PDF_PAGES
            for page_number in range(1, pages_scanned + 1):
                text = _clean_text(reader.pages[page_number - 1].extract_text() or "")
                remaining = MAX_EXTRACTED_CHARS - extracted_chars
                if remaining <= 0:
                    truncated = True
                    break
                if len(text) > remaining:
                    text = text[:remaining]
                    truncated = True
                extracted_chars += len(text)
                if text:
                    page_texts.append((page_number, text))
            if extracted_chars == 0:
                return self._failure(
                    announcement, "CNINFO_PDF_TEXT_EMPTY", http_status=http_status, attempts=attempts,
                    cache_hit=cache_hit, digest=digest, size=size, content_type=content_type, path=path,
                    page_count=page_count, pages_scanned=pages_scanned,
                )
            snippets = _build_snippets(page_texts)
            suspected = any(item.prompt_injection_suspected for item in snippets)
            return CninfoPdfEvidence(
                announcement_id=announcement.announcement_id,
                pdf_url=announcement.pdf_url,
                available=True,
                reason_code="OK",
                fetched_at=_aware(self._now()),
                http_status=http_status,
                attempts=attempts,
                cache_hit=cache_hit,
                pdf_sha256=digest,
                cache_relative_path=path.relative_to(self.cache_dir).as_posix(),
                content_type=content_type,
                byte_size=size,
                page_count=page_count,
                pages_scanned=pages_scanned,
                extracted_chars=extracted_chars,
                truncated=truncated,
                prompt_injection_suspected=suspected,
                snippets=snippets,
            )
        except (PdfReadError, OSError, ValueError, TypeError, KeyError, IndexError, RuntimeError):
            return self._failure(
                announcement, "CNINFO_PDF_PARSE_FAILED", http_status=http_status, attempts=attempts,
                cache_hit=cache_hit, digest=digest, size=size, content_type=content_type, path=path,
            )

    def _failure(
        self,
        announcement: CninfoAnnouncement,
        reason: str,
        *,
        http_status: int | None = None,
        attempts: int = 0,
        cache_hit: bool = False,
        digest: str | None = None,
        size: int | None = None,
        content_type: str | None = None,
        path: Path | None = None,
        page_count: int | None = None,
        pages_scanned: int = 0,
    ) -> CninfoPdfEvidence:
        return CninfoPdfEvidence(
            announcement_id=announcement.announcement_id,
            pdf_url=announcement.pdf_url,
            available=False,
            reason_code=reason,
            fetched_at=_aware(self._now()),
            http_status=http_status,
            attempts=attempts,
            cache_hit=cache_hit,
            pdf_sha256=digest,
            cache_relative_path=path.relative_to(self.cache_dir).as_posix() if path else None,
            content_type=content_type,
            byte_size=size,
            page_count=page_count,
            pages_scanned=pages_scanned,
        )

    def _valid_cache(self, pdf_path: Path, sidecar_path: Path, url: str) -> tuple[str, int, str] | None:
        if not pdf_path.is_file() or not sidecar_path.is_file():
            return None
        try:
            metadata = json.loads(sidecar_path.read_text(encoding="utf-8"))
            body = pdf_path.read_bytes()
            digest = hashlib.sha256(body).hexdigest()
            content_type = str(metadata["content_type"])
            if (
                metadata.get("url") != url
                or metadata.get("sha256") != digest
                or metadata.get("byte_size") != len(body)
                or len(body) > self.max_bytes
                or not body.startswith(b"%PDF")
                or content_type not in _CONTENT_TYPES
            ):
                return None
            return digest, len(body), content_type
        except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError):
            return None

    def _cache_path(self, folder: str, name: str) -> Path:
        path = (self.cache_dir / folder / name).resolve()
        if path == self.cache_dir or self.cache_dir not in path.parents:
            raise ValueError("CNINFO PDF cache path escaped its root")
        return path

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()


def _approved_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "https" and parsed.hostname == CNINFO_PDF_HOST and not parsed.username and not parsed.password


def _retry_delay(value: str | None, attempt: int) -> float:
    if value:
        try:
            return min(30.0, max(0.0, float(value)))
        except ValueError:
            pass
    return float(2 ** (attempt - 1))


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _build_snippets(page_texts: list[tuple[int, str]]) -> tuple[PdfEvidenceSnippet, ...]:
    candidates: list[tuple[int, int, int, PdfEvidenceSnippet]] = []
    for page_number, page_text in page_texts:
        units = [item.strip() for item in re.split(r"(?<=[。！？；])|\n+", page_text) if item.strip()]
        if not units:
            units = [page_text]
        for index, unit in enumerate(units):
            matched = tuple(keyword for keyword in _EVIDENCE_KEYWORDS if keyword in unit)
            if not matched:
                continue
            raw_snippet = unit[:MAX_SNIPPET_CHARS]
            secret_suspected = bool(_SECRET_LIKE.search(raw_snippet))
            snippet_text = _SECRET_LIKE.sub("[SECRET_LIKE_TEXT_BLOCKED]", raw_snippet)
            snippet = PdfEvidenceSnippet(
                page_number=page_number,
                text=snippet_text,
                matched_keywords=matched,
                prompt_injection_suspected=secret_suspected or bool(_INJECTION.search(snippet_text)),
            )
            candidates.append((-len(matched), page_number, index, snippet))
    if not candidates and page_texts:
        page_number, page_text = page_texts[0]
        raw_snippet = page_text[:MAX_SNIPPET_CHARS]
        secret_suspected = bool(_SECRET_LIKE.search(raw_snippet))
        snippet_text = _SECRET_LIKE.sub("[SECRET_LIKE_TEXT_BLOCKED]", raw_snippet)
        candidates.append((0, page_number, 0, PdfEvidenceSnippet(
            page_number=page_number,
            text=snippet_text,
            prompt_injection_suspected=secret_suspected or bool(_INJECTION.search(snippet_text)),
        )))
    candidates.sort(key=lambda item: item[:3])
    return tuple(item[3] for item in candidates[:MAX_EVIDENCE_SNIPPETS])


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("CNINFO PDF timestamp must be timezone-aware")
    return value.astimezone(SHANGHAI)


__all__ = [
    "CNINFO_PDF_HOST",
    "MAX_PDF_BYTES",
    "CninfoPdfClient",
    "CninfoPdfEvidence",
    "PdfEvidenceSnippet",
]
