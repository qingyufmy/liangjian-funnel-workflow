from datetime import datetime, timedelta, timezone
import pytest

from liangjian_funnel.data.open_news import OpenNewsItem, OpenNewsFetchResult
from liangjian_funnel.data.news_journal import NewsJournal, collect_pages

NOW = datetime(2026, 9, 29, 8, tzinfo=timezone.utc)
SOURCE = "open_news.test"


def page(title="订单签订", ok=True, dropped=0):
    item = OpenNewsItem(source_id=SOURCE, provider_item_id="one", title=title, summary=title,
        publish_time=NOW - timedelta(hours=1), fetched_at=NOW, url="https://example.test/1", channel="test")
    return OpenNewsFetchResult(source_id=SOURCE, source_url="https://example.test/feed", channel="test",
        ok=ok, complete=ok, reason_code="OK" if ok else "HTTP_FAILED", fetched_at=NOW,
        items=(item,) if ok else (), dropped_invalid_items=dropped)


def test_failure_does_not_advance_and_resume_reuses_cursor(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    calls = []
    def fetch(cursor):
        calls.append(cursor)
        return (page(), "p2", False) if not cursor else (page(ok=False), "p3", False)
    result = collect_pages(journal, SOURCE, fetch, now=lambda: NOW)
    assert result["status"] == "PAGE_FAILED" and not result["complete"]
    assert journal.cursor(SOURCE) == "p2"
    resumed = collect_pages(journal, SOURCE, lambda c: (page(), None, True), now=lambda: NOW)
    assert resumed["complete"] and calls == ["", "p2"]


def test_budget_is_incomplete_and_cursor_persisted(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    result = collect_pages(journal, SOURCE, lambda c: (page(), c + "x", False), now=lambda: NOW, max_pages=2)
    assert result["status"] == "PAGE_BUDGET_EXHAUSTED"
    assert journal.cursor(SOURCE) == "xx"


@pytest.mark.parametrize("dropped,status", [(0, "CURSOR_UNAVAILABLE_OR_STALLED"), (1, "PAGE_PARTIAL")])
def test_short_or_partial_page_not_complete(tmp_path, dropped, status):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    result = collect_pages(journal, SOURCE, lambda c: (page(dropped=dropped), None, False), now=lambda: NOW)
    assert result["status"] == status and not result["complete"]
    assert journal.cursor(SOURCE) == ""


def test_revision_is_point_in_time_and_missing_does_not_withdraw(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    journal.record_page(page(), received_at=NOW)
    journal.record_page(page("订单取消"), received_at=NOW + timedelta(minutes=5))
    assert journal.visible(as_of=NOW)[0]["payload"]["title"] == "订单签订"
    assert journal.visible(as_of=NOW + timedelta(minutes=6))[0]["payload"]["title"] == "订单取消"
    journal.record_page(page(), received_at=NOW + timedelta(minutes=10))
    assert journal.visible(as_of=NOW + timedelta(minutes=11))[0]["payload"]["title"] == "订单签订"
    journal.record_page(page().model_copy(update={"items": ()}), received_at=NOW + timedelta(minutes=12))
    assert len(journal.visible(as_of=NOW + timedelta(minutes=13))) == 1
    assert journal.visible(as_of=NOW - timedelta(seconds=1)) == []


def test_idempotent_page_and_stale_cursor_conflict(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    first = journal.record_page(page(), received_at=NOW, next_cursor="p2")
    second = journal.record_page(page(), received_at=NOW, next_cursor="p2")
    assert first["page_hash"] == second["page_hash"] and second["reused"]
    with pytest.raises(ValueError, match="CURSOR_CONFLICT"):
        journal.record_page(page(), received_at=NOW + timedelta(seconds=1), next_cursor="p2")


def test_simultaneous_conflicting_revisions_remain_flagged(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    journal.record_page(page(), received_at=NOW)
    journal.record_page(page("更正"), received_at=NOW)
    assert journal.visible(as_of=NOW)[0]["same_time_conflict"]


def test_cursor_cycle_across_resumed_runs_never_advances(tmp_path):
    journal = NewsJournal(tmp_path / "shadow.sqlite")
    journal.record_page(page(), received_at=NOW, next_cursor="a")
    journal.record_page(page(), received_at=NOW, cursor_in="a", next_cursor="b")
    result = journal.record_page(page(), received_at=NOW, cursor_in="b", next_cursor="a")
    assert result["status"] == "CURSOR_CYCLE"
    assert journal.cursor(SOURCE) == "b"
    assert journal.record_page(page(), received_at=NOW, cursor_in="b", next_cursor="a")["reused"]
