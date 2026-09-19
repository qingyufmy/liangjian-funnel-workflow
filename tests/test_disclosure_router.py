from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoFetchResult
from liangjian_funnel.data.disclosure_router import OfficialDisclosureRouter
from liangjian_funnel.facts.cninfo import normalize_cninfo_results
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.workflow import WorkflowApplication, _merge_cninfo_query_results


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 19, 12, 0, tzinfo=TZ)


def result(
    source: str,
    *,
    ok: bool,
    reason: str,
    announcements: tuple[CninfoAnnouncement, ...] = (),
    fetched_at: datetime = NOW,
) -> CninfoFetchResult:
    return CninfoFetchResult(
        symbol="600001.SH",
        start_date="2026-09-01",
        end_date="2026-09-19",
        ok=ok,
        complete=ok,
        reason_code=reason,
        announcements=announcements,
        total=len(announcements) if ok else None,
        fetched_at=fetched_at,
        source_id=source,
        source_url=f"https://{source}.example/announcements",
    )


class Client:
    def __init__(self, value: CninfoFetchResult) -> None:
        self.value = value
        self.calls = 0

    def fetch_announcements(self, *_args, **_kwargs) -> CninfoFetchResult:
        self.calls += 1
        return self.value


def announcement() -> CninfoAnnouncement:
    return CninfoAnnouncement(
        announcement_id="annual-1",
        sec_code="600001",
        sec_name="测试公司",
        announcement_title="2026年半年度报告",
        adjunct_url="https://static.cninfo.com.cn/finalpage/annual-1.pdf",
        publish_time=NOW - timedelta(days=20),
    )


def test_router_uses_exchange_only_when_cninfo_is_unavailable() -> None:
    primary = Client(result("cninfo_public", ok=False, reason="CNINFO_HTTP_4XX"))
    exchange = Client(
        result("sse_official", ok=True, reason="OK", announcements=(announcement(),))
    )
    routed = OfficialDisclosureRouter(primary, sse=exchange, now=lambda: NOW).fetch_announcements(
        "600001.SH", "2026-09-01", "2026-09-19"
    )
    assert routed.ok and routed.source_id == "sse_official"
    assert routed.metadata["fallback_used"] is True
    assert routed.metadata["fallback_succeeded"] is True
    assert [row["source_id"] for row in routed.metadata["provider_attempts"]] == [
        "cninfo_public",
        "sse_official",
    ]


def test_router_does_not_fallback_after_confirmed_empty_query() -> None:
    primary = Client(result("cninfo_public", ok=True, reason="NO_RECORDS"))
    exchange = Client(result("sse_official", ok=True, reason="NO_RECORDS"))
    routed = OfficialDisclosureRouter(primary, sse=exchange).fetch_announcements(
        "600001.SH", "2026-09-01", "2026-09-19"
    )
    assert routed.ok and routed.reason_code == "NO_RECORDS"
    assert exchange.calls == 0


def test_business_query_reuses_bounded_stale_complete_cache_on_live_failure(
    tmp_path: Path,
) -> None:
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    old = result(
        "cninfo_public",
        ok=True,
        reason="OK",
        announcements=(announcement(),),
        fetched_at=NOW - timedelta(days=8),
    )
    app.fact_cache.put_cached_result(
        "CNINFO_ANNOUNCEMENTS",
        "600001.SH:ANNUAL_REPORT_450D",
        old.model_dump(mode="json"),
        fetched_at=old.fetched_at,
        expires_at=old.fetched_at + timedelta(days=7),
    )
    failed = Client(result("cninfo_public", ok=False, reason="CNINFO_HTTP_4XX"))
    recovered, hit = app._cached_cninfo_result(
        failed,
        symbol="600001.SH",
        start_date="2025-06-01",
        end_date="2026-09-19",
        semantic_key="ANNUAL_REPORT_450D",
        ttl=timedelta(days=7),
        search_keyword="年度报告",
        stale_if_error=timedelta(days=45),
    )
    assert hit and recovered.ok
    assert recovered.metadata["cache_status"] == "STALE_VERIFIED_FALLBACK"
    assert recovered.metadata["live_failure_reason_code"] == "CNINFO_HTTP_4XX"


def test_business_stale_cache_precedes_expensive_exchange_history_scan(
    tmp_path: Path,
) -> None:
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    old = result(
        "cninfo_public",
        ok=True,
        reason="OK",
        announcements=(announcement(),),
        fetched_at=NOW - timedelta(days=8),
    )
    app.fact_cache.put_cached_result(
        "CNINFO_ANNOUNCEMENTS",
        "600001.SH:ANNUAL_REPORT_450D",
        old.model_dump(mode="json"),
        fetched_at=old.fetched_at,
        expires_at=old.fetched_at + timedelta(days=7),
    )
    primary = Client(result("cninfo_public", ok=False, reason="CNINFO_HTTP_4XX"))
    exchange = Client(
        result("sse_official", ok=True, reason="OK", announcements=(announcement(),))
    )
    router = OfficialDisclosureRouter(primary, sse=exchange, now=lambda: NOW)
    recovered, hit = app._cached_cninfo_result(
        router,
        symbol="600001.SH",
        start_date="2025-06-01",
        end_date="2026-09-19",
        semantic_key="ANNUAL_REPORT_450D",
        ttl=timedelta(days=7),
        search_keyword="年度报告",
        stale_if_error=timedelta(days=45),
    )
    assert hit and recovered.metadata["cache_status"] == "STALE_VERIFIED_FALLBACK"
    assert primary.calls == 1
    assert exchange.calls == 0


def test_recent_failure_preserves_business_facts_but_marks_source_degraded() -> None:
    recent = result("cninfo_public", ok=False, reason="CNINFO_HTTP_4XX")
    business = result(
        "cninfo_public", ok=True, reason="OK", announcements=(announcement(),)
    )
    merged = _merge_cninfo_query_results(recent, business)
    assert merged.ok and merged.complete
    assert merged.reason_code == "BUSINESS_EVIDENCE_READY_RECENT_QUERY_UNAVAILABLE"
    assert merged.metadata["recent_query_complete"] is False

    manifest = normalize_cninfo_results({"600001.SH": merged}, as_of=NOW)
    assert len(manifest.facts) == 1
    assert manifest.source_health[0].status.value == "DEGRADED"
    assert manifest.source_health[0].details["recent_query_complete"] is False
