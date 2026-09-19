from __future__ import annotations

from datetime import datetime
from urllib.parse import parse_qs
from zoneinfo import ZoneInfo

import httpx

from liangjian_funnel.data.exchange_disclosure import (
    SSE_ENDPOINT,
    SZSE_ENDPOINT,
    SseDisclosureClient,
    SzseDisclosureClient,
)


NOW = datetime(2026, 9, 19, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_sse_official_query_applies_date_symbol_keyword_and_paginates() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        query = parse_qs(request.url.query.decode())
        assert str(request.url).startswith(SSE_ENDPOINT)
        assert query["productId"] == ["600001"]
        assert query["beginDate"] == ["2026-09-01"]
        assert query["endDate"] == ["2026-09-19"]
        assert query["keyWord"] == ["半年度报告"]
        page = int(query["pageHelp.pageNo"][0])
        item = {
            "SECURITY_CODE": "600001",
            "SECURITY_NAME": "测试公司",
            "SSEDATE": f"2026-09-0{page}",
            "TITLE": f"测试公司2026年半年度报告{page}",
            "URL": f"/disclosure/listedinfo/announcement/c/new/2026-09-0{page}/a{page}.pdf",
        }
        return httpx.Response(200, json={"pageHelp": {"data": [item], "total": 2}})

    with SseDisclosureClient(
        transport=httpx.MockTransport(handler),
        min_request_interval_seconds=0,
        now=lambda: NOW,
    ) as client:
        result = client.fetch_announcements(
            "600001.SH", "2026-09-01", "2026-09-19", search_keyword="半年度报告"
        )

    assert result.ok and result.complete and result.source_id == "sse_official"
    assert result.pages == 2 and len(calls) == 2
    assert result.announcements[0].pdf_url.startswith("https://static.sse.com.cn/")


def test_szse_official_query_posts_json_filters_keyword_and_builds_pdf_url() -> None:
    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith(SZSE_ENDPOINT)
        body = __import__("json").loads(request.content)
        captured.append(body)
        return httpx.Response(
            200,
            json={
                "announceCount": 2,
                "data": [
                    {
                        "annId": 1001,
                        "title": "测试公司：2026年半年度报告",
                        "publishTime": "2026-09-17 00:00:00",
                        "attachPath": "/disc/disk03/finalpage/2026-09-17/a.PDF",
                        "secCode": ["000001"],
                        "secName": ["测试公司"],
                    },
                    {
                        "annId": 1002,
                        "title": "测试公司：董事会决议公告",
                        "publishTime": "2026-09-17 00:00:00",
                        "attachPath": "/disc/disk03/finalpage/2026-09-17/b.PDF",
                        "secCode": ["000001"],
                        "secName": ["测试公司"],
                    },
                ],
            },
        )

    with SzseDisclosureClient(
        transport=httpx.MockTransport(handler),
        min_request_interval_seconds=0,
        now=lambda: NOW,
    ) as client:
        result = client.fetch_announcements(
            "000001.SZ", "2026-09-01", "2026-09-19", search_keyword="半年度报告"
        )

    assert result.ok and result.source_id == "szse_official"
    assert len(result.announcements) == 1
    assert result.announcements[0].announcement_id == "1001"
    assert result.announcements[0].pdf_url == (
        "https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-09-17/a.PDF"
    )
    assert captured[0]["stock"] == ["000001"]
    assert captured[0]["seDate"] == ["2026-09-01", "2026-09-19"]


def test_contract_mismatch_fails_closed_without_partial_success() -> None:
    with SseDisclosureClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"pageHelp": {"data": [{}], "total": 1}})
        ),
        min_request_interval_seconds=0,
        now=lambda: NOW,
    ) as client:
        result = client.fetch_announcements("600001.SH", "2026-09-01", "2026-09-19")
    assert not result.ok and not result.complete
    assert result.reason_code == "SSE_CONTRACT_CHANGED"
