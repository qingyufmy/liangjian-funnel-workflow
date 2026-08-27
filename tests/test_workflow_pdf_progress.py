from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
import time
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoFetchResult
from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.runtime.progress import WorkflowProgress
from liangjian_funnel.workflow import WorkflowApplication, _build_cninfo_pdf_tasks, _deduplicate_cninfo_pdf_tasks


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 26, 15, 10, tzinfo=TZ)


def _announcement(identifier: str, title: str, *, minutes_ago: int = 1) -> CninfoAnnouncement:
    return CninfoAnnouncement(
        announcement_id=identifier,
        sec_code="600519",
        sec_name="贵州茅台",
        announcement_title=title,
        adjunct_url=f"https://static.cninfo.com.cn/finalpage/{identifier}.pdf",
        publish_time=NOW - timedelta(minutes=minutes_ago),
        storage_time=NOW - timedelta(minutes=max(minutes_ago - 1, 0)),
    )


def _result(symbol: str, *announcements: CninfoAnnouncement) -> CninfoFetchResult:
    return CninfoFetchResult(
        symbol=symbol,
        start_date="2026-08-20",
        end_date="2026-08-26",
        ok=True,
        complete=True,
        reason_code="OK",
        announcements=announcements,
        total=len(announcements),
        pages=1,
        fetched_at=NOW,
    )


def test_cninfo_pdf_progress_uses_prebuilt_document_total_and_tracks_outcomes(tmp_path):
    results = {
        "600519.SH": _result(
            "600519.SH",
            _announcement("risk-1", "关于股东减持风险的公告"),
            _announcement("annual-1", "2026年半年度报告"),
        ),
        "000001.SZ": _result(
            "000001.SZ",
            _announcement("order-1", "重大项目中标公告"),
        ),
    }

    tasks = _build_cninfo_pdf_tasks(results, limit=2)
    assert len(tasks) == 3
    assert [(symbol, announcement.announcement_id) for symbol, announcement in tasks] == [
        ("000001.SZ", "order-1"),
        ("600519.SH", "risk-1"),
        ("600519.SH", "annual-1"),
    ]

    progress = WorkflowProgress(
        tmp_path / "workflow_progress.json",
        run_id="close-pdf",
        job="close",
        now=NOW,
    )
    progress.set_phase("CNINFO_SYNC", now=NOW)
    progress.update_data(
        processed=3,
        total=3,
        cache_hits=2,
        cache_misses=4,
        failures=1,
        current_symbol="600519.SH",
        now=NOW,
    )
    progress.set_phase("CNINFO_PDF_SYNC", now=NOW)
    progress.update_data(
        processed=0,
        total=len(tasks),
        cache_hits=0,
        cache_misses=0,
        failures=0,
        documents_succeeded=0,
        documents_failed=0,
        now=NOW,
    )
    initial = progress.snapshot()
    assert initial["phase"] == "CNINFO_PDF_SYNC"
    assert initial["data"]["processed"] == 0
    assert initial["data"]["total"] == 3
    assert initial["data"]["cache_hits"] == 0
    assert initial["data"]["cache_misses"] == 0
    assert initial["data"]["failures"] == 0
    assert initial["data"]["documents_succeeded"] == 0
    assert initial["data"]["documents_failed"] == 0

    progress.update_data(
        processed=0,
        total=len(tasks),
        cache_hits=0,
        cache_misses=0,
        failures=0,
        current_symbol=tasks[0][0],
        current_document=tasks[0][1].announcement_id,
        documents_succeeded=0,
        documents_failed=0,
        now=NOW + timedelta(seconds=10),
    )
    in_progress = progress.snapshot()
    assert in_progress["data"]["processed"] == 0
    assert in_progress["data"]["current_symbol"] == "000001.SZ"
    assert in_progress["data"]["current_document"] == "ORDER-1"
    assert in_progress["data"]["documents_succeeded"] == 0

    progress.update_data(
        processed=1,
        total=len(tasks),
        cache_hits=1,
        cache_misses=0,
        failures=0,
        current_symbol=tasks[1][0],
        current_document=tasks[1][1].announcement_id,
        documents_succeeded=1,
        documents_failed=0,
        now=NOW + timedelta(seconds=20),
    )
    in_progress = progress.snapshot()
    assert in_progress["data"]["processed"] == 1
    assert in_progress["data"]["current_symbol"] == "600519.SH"
    assert in_progress["data"]["current_document"] == "RISK-1"

    progress.update_data(
        processed=2,
        total=len(tasks),
        cache_hits=1,
        cache_misses=2,
        failures=1,
        current_symbol=tasks[2][0],
        current_document=tasks[2][1].announcement_id,
        documents_succeeded=1,
        documents_failed=1,
        now=NOW + timedelta(seconds=30),
    )
    progress.update_data(
        processed=len(tasks),
        total=len(tasks),
        cache_hits=1,
        cache_misses=2,
        failures=1,
        current_symbol=tasks[-1][0],
        current_document=tasks[-1][1].announcement_id,
        documents_succeeded=2,
        documents_failed=1,
        now=NOW + timedelta(seconds=40),
    )

    final = progress.snapshot()
    assert final["phase"] == "CNINFO_PDF_SYNC"
    assert final["data"]["processed"] == final["data"]["total"] == 3
    assert final["data"]["cache_hits"] == 1
    assert final["data"]["cache_misses"] == 2
    assert final["data"]["failures"] == 1
    assert final["data"]["documents_succeeded"] == 2
    assert final["data"]["documents_failed"] == 1
    assert final["data"]["current_symbol"] == "600519.SH"
    assert final["data"]["current_document"] == "ANNUAL-1"


def test_cninfo_pdf_duplicate_announcement_ids_are_downloaded_once_but_keep_symbol_mapping():
    duplicate_for_first = _announcement("same", "重大合同公告")
    duplicate_for_second = _announcement("same", "重大合同公告")
    tasks = (
        ("000001.SZ", duplicate_for_first),
        ("600519.SH", duplicate_for_second),
        ("600519.SH", _announcement("different", "定期报告")),
    )

    unique = _deduplicate_cninfo_pdf_tasks(tasks)

    assert [(symbol, announcement.announcement_id) for symbol, announcement in unique] == [
        ("000001.SZ", "same"),
        ("600519.SH", "different"),
    ]
    # The original task list remains the deterministic expansion source for
    # the two per-symbol evidence keys.
    assert [(symbol, announcement.announcement_id) for symbol, announcement in tasks] == [
        ("000001.SZ", "same"),
        ("600519.SH", "same"),
        ("600519.SH", "different"),
    ]


def test_pdf_workers_are_bounded_and_completion_order_does_not_change_mapping(tmp_path: Path):
    first = _announcement("same", "重大合同公告")
    second = _announcement("different", "定期报告")
    tasks = (
        ("000001.SZ", first),
        ("600519.SH", first),
        ("600519.SH", second),
    )
    unique = _deduplicate_cninfo_pdf_tasks(tasks)
    calls: list[str] = []
    calls_lock = Lock()

    class FakePdfClient:
        def fetch_evidence(self, item: CninfoAnnouncement) -> CninfoPdfEvidence:
            # Force the second completion to arrive first.
            if item.announcement_id == "same":
                time.sleep(0.03)
            with calls_lock:
                calls.append(item.announcement_id)
            return CninfoPdfEvidence(
                announcement_id=item.announcement_id,
                pdf_url=item.pdf_url,
                available=False,
                reason_code="CNINFO_PDF_TEXT_EMPTY",
                fetched_at=NOW,
            )

    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    app.settings = SimpleNamespace(
        cninfo_pdf_retain_raw=False,
        cninfo_pdf_cache_dir=tmp_path / "pdf",
    )
    outcomes: dict[int, CninfoPdfEvidence] = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(app._fetch_and_cache_cninfo_pdf_evidence, FakePdfClient(), announcement): index
            for index, (_, announcement) in enumerate(unique)
        }
        for future in as_completed(futures):
            outcomes[futures[future]] = future.result()

    assert calls == ["different", "same"]
    ordered = [outcomes[index].announcement_id for index in range(len(unique))]
    assert ordered == ["same", "different"]
    expanded = []
    for _, announcement in tasks:
        index = next(
            index
            for index, (_, item) in enumerate(unique)
            if item.announcement_id == announcement.announcement_id
        )
        expanded.append(outcomes[index])
    assert [item.announcement_id for item in expanded] == ["same", "same", "different"]


def test_pdf_failure_cache_uses_seven_day_ttl_and_is_reported_as_a_cache_hit(tmp_path: Path):
    announcement = _announcement("failed", "定期报告")
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    raw_path = tmp_path / "pdf" / "raw" / "failed.pdf"
    metadata_path = tmp_path / "pdf" / "metadata" / "failed.json"
    raw_path.parent.mkdir(parents=True)
    metadata_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"%PDF-failed")
    metadata_path.write_text("{}", encoding="utf-8")
    app = object.__new__(WorkflowApplication)
    app.fact_cache = cache
    app.settings = SimpleNamespace(
        cninfo_pdf_retain_raw=False,
        cninfo_pdf_cache_dir=tmp_path / "pdf",
    )
    evidence = CninfoPdfEvidence(
        announcement_id=announcement.announcement_id,
        pdf_url=announcement.pdf_url,
        available=False,
        reason_code="CNINFO_PDF_TEXT_EMPTY",
        fetched_at=NOW,
        pdf_sha256="a" * 64,
        cache_relative_path="raw/failed.pdf",
        content_type="application/pdf",
        byte_size=len(b"%PDF-failed"),
    )

    app._persist_cninfo_pdf_evidence(evidence)
    assert not raw_path.exists()
    assert not metadata_path.exists()
    cached = cache.get_cached_result(
        "CNINFO_PDF_EVIDENCE",
        "failed",
        fresh_at=NOW + timedelta(days=6, hours=23),
    )
    assert cached is not None
    hit = app._cached_cninfo_pdf_evidence_from_record(announcement, cached)
    assert hit is not None
    assert hit.cache_hit is True
    assert hit.reason_code == "CNINFO_PDF_TEXT_EMPTY"
    assert cache.get_cached_result(
        "CNINFO_PDF_EVIDENCE",
        "failed",
        fresh_at=NOW + timedelta(days=7, seconds=1),
    ) is None


def test_bse_cdn_failure_cache_uses_transient_ttl(tmp_path: Path):
    cache = LocalFactCache(tmp_path / "facts.sqlite3")
    app = object.__new__(WorkflowApplication)
    app.fact_cache = cache
    app.settings = SimpleNamespace(
        cninfo_pdf_retain_raw=False,
        cninfo_pdf_cache_dir=tmp_path / "pdf",
    )
    evidence = CninfoPdfEvidence(
        announcement_id="bse-cdn-blocked",
        pdf_url="https://www.bse.cn/disclosure/2026/2026-08-20/example.pdf",
        available=False,
        reason_code="CNINFO_PDF_BSE_CDN_BLOCKED",
        fetched_at=NOW,
        http_status=403,
        attempts=3,
    )

    app._persist_cninfo_pdf_evidence(evidence)

    assert cache.get_cached_result(
        "CNINFO_PDF_EVIDENCE",
        evidence.announcement_id,
        fresh_at=NOW + timedelta(minutes=14),
    ) is not None
    assert cache.get_cached_result(
        "CNINFO_PDF_EVIDENCE",
        evidence.announcement_id,
        fresh_at=NOW + timedelta(minutes=16),
    ) is None


def test_cninfo_candidate_workers_rebuild_results_in_selected_order():
    symbols = ("000001.SZ", "000002.SZ", "600519.SH")
    app = object.__new__(WorkflowApplication)

    def cached(_client, *, symbol, **_kwargs):
        if symbol == "000002.SZ":
            time.sleep(0.03)
        return _result(symbol, _announcement(f"{symbol}-announcement", "定期报告")), False

    app._cached_cninfo_result = cached
    completed = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                app._fetch_cninfo_candidate_queries,
                object(),
                symbol,
                "2026-08-20",
                "2026-08-26",
                "2025-06-01",
            ): index
            for index, symbol in enumerate(symbols)
        }
        for future in as_completed(futures):
            completed[futures[future]] = future.result()

    assert next(iter(completed.values()))[0] != "000002.SZ"
    assert [completed[index][0] for index in range(len(symbols))] == list(symbols)
