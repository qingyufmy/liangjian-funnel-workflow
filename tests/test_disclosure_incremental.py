from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import pytest

from liangjian_funnel.data.cninfo import CninfoFetchResult, CninfoAnnouncement
from liangjian_funnel.data.disclosure_incremental import compose_disclosure_delta
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.workflow import WorkflowApplication

NOW = datetime.now(ZoneInfo("Asia/Shanghai"))
END = NOW.date().isoformat()
START = (NOW.date() - timedelta(days=450)).isoformat()


def announcement(identifier, days, title="年度报告"):
    return CninfoAnnouncement(announcement_id=identifier, sec_code="600519",
                             sec_name="贵州茅台", announcement_title=title,
                             adjunct_url="https://static.cninfo.com.cn/test.pdf",
                             publish_time=NOW - timedelta(days=days))


def inputs():
    base = CninfoFetchResult(symbol="600519.SH", start_date=(NOW.date()-timedelta(days=458)).isoformat(),
        end_date=(NOW.date()-timedelta(days=8)).isoformat(), ok=True, complete=True,
        reason_code="OK", fetched_at=NOW-timedelta(days=8),
        announcements=(announcement("old", 30),), metadata={"search_keyword":"年度报告"})
    recent = CninfoFetchResult(symbol="600519.SH", start_date=(NOW.date()-timedelta(days=10)).isoformat(),
        end_date=END, ok=True, complete=True, reason_code="OK", fetched_at=NOW,
        announcements=(announcement("new", 1), announcement("risk", 1, "风险提示")),
        metadata={"search_keyword":""})
    return base, recent


def compose(base, recent):
    return compose_disclosure_delta(base, recent, symbol="600519.SH", start=START,
        end=END, keyword="年度报告", now=NOW, max_age=timedelta(days=45))


def test_complete_delta_includes_new_reports_preserves_input_and_hashes():
    base, recent = inputs()
    before = base.model_dump(mode="json"), recent.model_dump(mode="json")
    result = compose(base, recent)
    assert result is not None and result.complete
    assert result.start_date == START and result.end_date == END
    assert {r.announcement_id for r in result.announcements} == {"old", "new", "risk"}
    assert result.source_id == "official_disclosure_incremental_projection"
    assert result.metadata["result_semantics"] == "HISTORICAL_LOCATOR_UNION_UNFILTERED_RECENT"
    assert len(result.metadata["incremental_evidence"]) == 2
    assert all(len(r["content_hash"]) == 64 for r in result.metadata["incremental_evidence"])
    assert before == (base.model_dump(mode="json"), recent.model_dump(mode="json"))


@pytest.mark.parametrize("side,update", [
    ("base", {"symbol":"000001.SZ"}),
    ("base", {"complete":False,"ok":False,"reason_code":"EMPTY_DATA"}),
    ("base", {"start_date": END}),
    ("base", {"end_date":(NOW.date()-timedelta(days=12)).isoformat()}),
    ("base", {"fetched_at":NOW-timedelta(days=46)}),
    ("base", {"fetched_at":NOW+timedelta(seconds=1)}),
    ("base", {"metadata":{"search_keyword":"报告"}}),
    ("recent", {"symbol":"000001.SZ"}),
    ("recent", {"complete":False,"ok":False,"reason_code":"EMPTY_DATA"}),
    ("recent", {"end_date":(NOW.date()-timedelta(days=1)).isoformat()}),
    ("recent", {"metadata":{"search_keyword":"年度报告"}}),
    ("recent", {"fetched_at":NOW+timedelta(seconds=1)}),
])
def test_gaps_wrong_identity_partial_future_or_filtered_delta_fall_back(side, update):
    base, recent = inputs()
    if side == "base": base = base.model_copy(update=update)
    else: recent = recent.model_copy(update=update)
    assert compose(base, recent) is None


def test_conflicting_document_revision_is_not_silently_merged():
    base, recent = inputs()
    recent = recent.model_copy(update={"announcements":(announcement("old", 1),)})
    assert compose(base, recent) is None


def test_expired_business_query_can_use_complete_recent_delta_without_network(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    base, recent = inputs()
    app.fact_cache.put_cached_result("CNINFO_ANNOUNCEMENTS", "600519.SH:ANNUAL_REPORT_450D",
        base.model_dump(mode="json"), fetched_at=base.fetched_at,
        expires_at=base.fetched_at+timedelta(days=7))
    class NoNetwork:
        def fetch_announcements(self, *a, **k): raise AssertionError("redundant full history query")
    result, hit = app._cached_cninfo_result(NoNetwork(), symbol="600519.SH", start_date=START,
        end_date=END, semantic_key="ANNUAL_REPORT_450D", ttl=timedelta(days=7),
        search_keyword="年度报告", stale_if_error=timedelta(days=45), recent_delta=recent)
    assert hit and result.end_date == END
    assert result.metadata["cache_status"] == "VERIFIED_HISTORY_WITH_COMPLETE_DELTA"


def test_same_query_fresh_cache_still_avoids_network(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    _, recent = inputs()
    app.fact_cache.put_cached_result("CNINFO_ANNOUNCEMENTS", "600519.SH:RECENT_10D",
        recent.model_dump(mode="json"), fetched_at=recent.fetched_at,
        expires_at=recent.fetched_at+timedelta(hours=6))
    class NoNetwork:
        def fetch_announcements(self, *a, **k): raise AssertionError("same complete query should reuse")
    result, hit = app._cached_cninfo_result(NoNetwork(), symbol="600519.SH",
        start_date=recent.start_date, end_date=END, semantic_key="RECENT_10D", ttl=timedelta(hours=6))
    assert hit and result == recent


def test_corrupt_cache_cannot_be_reblessed_by_complete_delta(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / "facts.sqlite3")
    base, recent = inputs()
    app.fact_cache.get_cached_result = lambda *a, **k: {
        "payload":base.model_dump(mode="json"), "content_hash":"wrong", "fetched_at":base.fetched_at.isoformat()}
    class FullQuery:
        calls = 0
        def fetch_announcements(self, *a, **k):
            self.calls += 1
            return base.model_copy(update={"start_date":START,"end_date":END,"fetched_at":NOW})
    client = FullQuery()
    result, hit = app._cached_cninfo_result(client, symbol="600519.SH", start_date=START,
        end_date=END, semantic_key="ANNUAL_REPORT_450D", ttl=timedelta(days=7),
        search_keyword="年度报告", stale_if_error=timedelta(days=45), recent_delta=recent)
    assert not hit and client.calls == 1 and result.end_date == END


@pytest.mark.parametrize('end_days', [1, 0])
def test_recent_window_uses_complete_overlapping_delta_not_full_rescan(tmp_path: Path, end_days):
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / 'recent.sqlite3')
    base, recent = inputs()
    base = base.model_copy(update={'start_date': recent.start_date,
                                  'end_date': (NOW.date() - timedelta(days=end_days)).isoformat(),
                                  'fetched_at': NOW - timedelta(hours=7),
                                  'metadata': {'search_keyword': ''}, 'announcements': ()})
    app.fact_cache.put_cached_result('CNINFO_ANNOUNCEMENTS', '600519.SH:RECENT_10D',
        base.model_dump(mode='json'), fetched_at=base.fetched_at,
        expires_at=base.fetched_at + timedelta(hours=6))
    class Delta:
        calls = []
        def fetch_announcements(self, symbol, start, end, **kwargs):
            self.calls.append((start, end))
            return recent.model_copy(update={'start_date': start, 'announcements': ()})
    client = Delta()
    result, hit = app._cached_cninfo_result(client, symbol='600519.SH', start_date=recent.start_date,
        end_date=END, semantic_key='RECENT_10D', ttl=timedelta(hours=6))
    assert client.calls == [(base.end_date, END)]
    assert hit and result.metadata['cache_status'] == 'VERIFIED_HISTORY_WITH_COMPLETE_DELTA'
    assert result.end_date == END and result.start_date == recent.start_date


def test_partial_recent_delta_requires_full_authoritative_query(tmp_path: Path):
    app = object.__new__(WorkflowApplication)
    app.fact_cache = LocalFactCache(tmp_path / 'partial.sqlite3')
    _, recent = inputs()
    base = recent.model_copy(update={'end_date': (NOW.date()-timedelta(days=1)).isoformat(),
                                    'fetched_at': NOW-timedelta(hours=12), 'announcements': ()})
    app.fact_cache.put_cached_result('CNINFO_ANNOUNCEMENTS', '600519.SH:RECENT_10D',
        base.model_dump(mode='json'), fetched_at=base.fetched_at, expires_at=base.fetched_at+timedelta(hours=6))
    class Partial:
        calls = []
        def fetch_announcements(self, symbol, start, end, **kwargs):
            self.calls.append((start, end))
            return recent.model_copy(update={'start_date': start, 'complete': len(self.calls) == 2})
    client = Partial()
    result, hit = app._cached_cninfo_result(client, symbol='600519.SH', start_date=recent.start_date,
        end_date=END, semantic_key='RECENT_10D', ttl=timedelta(hours=6))
    assert not hit and result.complete
    assert client.calls == [(base.end_date, END), (recent.start_date, END)]
