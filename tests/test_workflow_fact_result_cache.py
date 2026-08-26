from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cninfo import CninfoFetchResult
from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence, PdfEvidenceSnippet
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.pipeline.research import FrozenInputSnapshot
from liangjian_funnel.workflow import (
    PreparedSnapshot,
    WorkflowApplication,
    _hash_json,
    _merge_news_heat_snapshots,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeCninfoClient:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_announcements(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        search_keyword: str = "",
    ) -> CninfoFetchResult:
        self.calls += 1
        now = datetime.now(SHANGHAI)
        return CninfoFetchResult(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            ok=True,
            complete=True,
            reason_code="OK",
            fetched_at=now,
            attempts=1,
            metadata={"search_keyword": search_keyword},
        )


def test_cninfo_semantic_cache_avoids_repeated_company_request(tmp_path: Path):
    application = object.__new__(WorkflowApplication)
    application.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    client = FakeCninfoClient()

    first, first_hit = application._cached_cninfo_result(
        client,
        symbol="600519.SH",
        start_date="2025-06-01",
        end_date="2026-08-26",
        semantic_key="ANNUAL_REPORT_450D",
        ttl=timedelta(days=7),
        search_keyword="年度报告",
    )
    second, second_hit = application._cached_cninfo_result(
        client,
        symbol="600519.SH",
        start_date="2025-06-02",
        end_date="2026-08-27",
        semantic_key="ANNUAL_REPORT_450D",
        ttl=timedelta(days=7),
        search_keyword="年度报告",
    )

    assert first_hit is False
    assert second_hit is True
    assert client.calls == 1
    assert second.model_dump(mode="json") == first.model_dump(mode="json")


def test_news_heat_merge_is_deduplicated_bounded_and_keeps_market_items():
    duplicate = {"content_hash": "same", "title": "stock", "publish_time": "2026-08-26T10:00:00+08:00"}
    merged = _merge_news_heat_snapshots(
        {
            "available": True,
            "items": [
                {"content_hash": "market", "title": "market", "publish_time": "2026-08-26T09:00:00+08:00"},
                duplicate,
            ],
        },
        {"available": True, "items": [duplicate]},
    )

    assert merged["available"] is True
    assert merged["reason_code"] == "OK"
    assert merged["item_count"] == 2
    assert {item["content_hash"] for item in merged["items"]} == {"market", "same"}


def test_research_resume_marker_reuses_only_untampered_same_day_snapshot(tmp_path: Path):
    application = object.__new__(WorkflowApplication)
    application.settings = SimpleNamespace(
        snapshot_dir=tmp_path / "snapshots",
        research_checkpoint_dir=tmp_path / "checkpoints",
    )
    application.settings.snapshot_dir.mkdir(parents=True)
    as_of = datetime(2026, 8, 26, 15, 10, tzinfo=SHANGHAI)
    data = {
        "G0_SCOPE_CONTRACT": "CONFIGURED_RESEARCH_UNIVERSE_V1",
        "g0_symbols": ["600519.SH"],
        "value": 1,
    }
    digest = _hash_json(data)
    path = application.settings.snapshot_dir / "snapshot.json"
    path.write_text(
        __import__("json").dumps(
            {
                "snapshot_id": "snapshot-one",
                "snapshot_hash": digest,
                "as_of": as_of.isoformat(),
                "data": data,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    prepared = PreparedSnapshot(
        snapshot=FrozenInputSnapshot(
            snapshot_id="snapshot-one",
            snapshot_hash=digest,
            as_of=as_of,
            data=data,
        ),
        path=path,
        full_universe_count=3,
        research_universe_count=1,
        trade_universe_count=1,
        selected_count=1,
        factor_ready_count=0,
    )

    application._write_research_resume_marker("close", prepared, status="RETRYABLE")
    resumed = application._load_research_resume_snapshot("close", as_of)
    assert resumed is not None
    assert resumed.snapshot.snapshot_hash == digest
    assert resumed.selected_count == 1

    path.write_text(path.read_text(encoding="utf-8").replace('"value": 1', '"value": 2'), encoding="utf-8")
    assert application._load_research_resume_snapshot("close", as_of) is None


def test_pdf_raw_cache_is_pruned_only_inside_configured_generated_directories(tmp_path: Path):
    root = tmp_path / "cninfo"
    raw = root / "raw" / "abc.pdf"
    metadata = root / "metadata" / "abc.json"
    raw.parent.mkdir(parents=True)
    metadata.parent.mkdir(parents=True)
    raw.write_bytes(b"%PDF-test")
    metadata.write_text("{}", encoding="utf-8")
    application = object.__new__(WorkflowApplication)
    application.settings = SimpleNamespace(cninfo_pdf_cache_dir=root)
    evidence = CninfoPdfEvidence(
        announcement_id="announcement-1",
        pdf_url="https://static.cninfo.com.cn/finalpage/test.pdf",
        available=True,
        reason_code="OK",
        fetched_at=datetime.now(SHANGHAI),
        pdf_sha256="a" * 64,
        cache_relative_path="raw/abc.pdf",
        content_type="application/pdf",
        byte_size=9,
        page_count=1,
        pages_scanned=1,
        extracted_chars=4,
        snippets=(PdfEvidenceSnippet(page_number=1, text="主营业务"),),
    )

    application._prune_cninfo_pdf_raw(evidence)

    assert not raw.exists()
    assert not metadata.exists()
