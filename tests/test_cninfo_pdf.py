from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pypdf
import pytest

from liangjian_funnel.data.cninfo import CninfoAnnouncement
from liangjian_funnel.data.cninfo_pdf import CninfoPdfClient


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 25, 15, 10, tzinfo=TZ)


def announcement(url: str = "https://static.cninfo.com.cn/finalpage/a.pdf") -> CninfoAnnouncement:
    return CninfoAnnouncement(
        announcement_id="a1",
        sec_code="600519",
        sec_name="贵州茅台",
        announcement_title="重大合同公告",
        adjunct_url=url,
        publish_time=NOW,
    )


class Page:
    def __init__(self, text: str):
        self.text = text

    def extract_text(self) -> str:
        return self.text


class Reader:
    is_encrypted = False

    def __init__(self, _path: str, strict: bool = False):
        assert strict is False
        self.pages = [Page("公司中标重大项目，合同金额10亿元。报告期收入同比增长。")]


def make_client(tmp_path: Path, handler, *, sleeps: list[float] | None = None, max_bytes: int = 1024):
    transport = httpx.MockTransport(handler)
    http = httpx.Client(transport=transport, follow_redirects=False)
    return CninfoPdfClient(
        tmp_path,
        client=http,
        max_bytes=max_bytes,
        sleep=(sleeps.append if sleeps is not None else lambda _seconds: None),
        now=lambda: NOW,
    )


def test_download_extract_and_hash_validated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pypdf, "PdfReader", Reader)
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, headers={"Content-Type": "application/pdf"}, content=b"%PDF-test")

    client = make_client(tmp_path, handler)
    first = client.fetch_evidence(announcement())
    second = client.fetch_evidence(announcement())

    assert first.available is True
    assert first.cache_hit is False
    assert first.snippets[0].page_number == 1
    assert "合同" in first.snippets[0].matched_keywords
    assert second.cache_hit is True
    assert second.pdf_sha256 == first.pdf_sha256
    assert calls == 1
    sidecar = next((tmp_path / "metadata").glob("*.json"))
    assert "公司中标" not in sidecar.read_text(encoding="utf-8")
    assert json.loads(sidecar.read_text(encoding="utf-8"))["url"] == announcement().pdf_url


def test_corrupt_cache_is_refetched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pypdf, "PdfReader", Reader)
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, headers={"Content-Type": "application/pdf"}, content=b"%PDF-good")

    client = make_client(tmp_path, handler)
    assert client.fetch_evidence(announcement()).available
    next((tmp_path / "raw").glob("*.pdf")).write_bytes(b"%PDF-corrupt")
    result = client.fetch_evidence(announcement())

    assert result.available
    assert result.cache_hit is False
    assert calls == 2


def test_rejects_unapproved_host_without_request(tmp_path: Path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("must not request unapproved host")

    result = make_client(tmp_path, handler).fetch_evidence(announcement("https://evil.example/a.pdf"))
    assert result.available is False
    assert result.reason_code == "CNINFO_PDF_URL_REJECTED"


@pytest.mark.parametrize(
    "response,reason",
    [
        (httpx.Response(302, headers={"Location": "https://evil.example/a.pdf"}), "CNINFO_PDF_REDIRECT_REJECTED"),
        (httpx.Response(200, headers={"Content-Type": "text/html"}, content=b"%PDF-x"), "CNINFO_PDF_CONTENT_TYPE_INVALID"),
        (httpx.Response(200, headers={"Content-Type": "application/pdf"}, content=b"not-pdf"), "CNINFO_PDF_MAGIC_INVALID"),
        (httpx.Response(404), "CNINFO_PDF_HTTP_4XX"),
    ],
)
def test_download_contract_failures_are_stable(tmp_path: Path, response: httpx.Response, reason: str) -> None:
    result = make_client(tmp_path, lambda _request: response).fetch_evidence(announcement())
    assert result.available is False
    assert result.reason_code == reason


def test_429_retries_and_does_not_persist_error_body(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pypdf, "PdfReader", Reader)
    statuses = iter((429, 200))
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        if status == 429:
            return httpx.Response(429, headers={"Retry-After": "0"}, content=b"secret upstream body")
        return httpx.Response(200, headers={"Content-Type": "application/octet-stream"}, content=b"%PDF-ok")

    result = make_client(tmp_path, handler, sleeps=sleeps).fetch_evidence(announcement())
    assert result.available is True
    assert result.attempts == 2
    assert sleeps == [0.0]
    assert "secret upstream body" not in repr(result)


def test_size_and_empty_text_degrade_explicitly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    too_large = make_client(
        tmp_path / "large",
        lambda _request: httpx.Response(200, headers={"Content-Type": "application/pdf"}, content=b"%PDF-123456"),
        max_bytes=8,
    ).fetch_evidence(announcement())
    assert too_large.reason_code == "CNINFO_PDF_TOO_LARGE"

    class EmptyReader(Reader):
        def __init__(self, _path: str, strict: bool = False):
            self.pages = [Page("")]

    monkeypatch.setattr(pypdf, "PdfReader", EmptyReader)
    empty = make_client(
        tmp_path / "empty",
        lambda _request: httpx.Response(200, headers={"Content-Type": "application/pdf"}, content=b"%PDF-empty"),
    ).fetch_evidence(announcement())
    assert empty.available is False
    assert empty.reason_code == "CNINFO_PDF_TEXT_EMPTY"
    assert empty.page_count == 1


def test_stream_has_total_wall_clock_deadline(tmp_path: Path) -> None:
    ticks = iter((0.0, 31.0))
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-slow",
        )
    )
    client = CninfoPdfClient(
        tmp_path,
        client=httpx.Client(transport=transport, follow_redirects=False),
        timeout_seconds=30,
        monotonic=lambda: next(ticks),
        now=lambda: NOW,
    )

    result = client.fetch_evidence(announcement())

    assert result.available is False
    assert result.reason_code == "CNINFO_PDF_TIMEOUT"


def test_secret_like_untrusted_text_is_blocked_before_fact_layer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SecretReader(Reader):
        def __init__(self, _path: str, strict: bool = False):
            self.pages = [Page("重大合同 api_key=source-secret-value-123 已签署。")]

    monkeypatch.setattr(pypdf, "PdfReader", SecretReader)
    client = make_client(
        tmp_path,
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-secret",
        ),
    )

    result = client.fetch_evidence(announcement())

    assert result.available is True
    assert result.prompt_injection_suspected is True
    assert "source-secret-value-123" not in result.snippets[0].text
    assert "[SECRET_LIKE_TEXT_BLOCKED]" in result.snippets[0].text
