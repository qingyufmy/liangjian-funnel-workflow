from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoFetchResult
from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence, PdfEvidenceSnippet, BUSINESS_EXTRACTION_VERSION
from liangjian_funnel.pipeline.disclosure_cache_worker import DisclosureCacheWorker


def worker(tmp_path, monkeypatch):
    from liangjian_funnel import workflow
    monkeypatch.setattr(workflow.WorkflowApplication, '__init__',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('full app forbidden')))
    settings = SimpleNamespace(fact_cache_db_path=tmp_path/'local-test-cache.sqlite',
        cninfo_pdf_cache_dir=tmp_path/'pdf', cninfo_pdf_retain_raw=False,
        cninfo_pdf_max_documents_per_symbol=3)
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    ann = CninfoAnnouncement(announcement_id='test-ann', sec_code='600001', sec_name='test',
        announcement_title='2026年半年度报告', adjunct_url='https://static.cninfo.com.cn/finalpage/test.pdf',
        publish_time=now-timedelta(days=20))
    class Router:
        calls = 0
        fail = False
        def fetch_announcements(self, symbol, start, end, search_keyword=''):
            self.calls += 1
            return CninfoFetchResult(symbol=symbol, start_date=start, end_date=end,
                ok=not self.fail, complete=not self.fail, reason_code='REQUEST_FAILED' if self.fail else 'OK',
                announcements=(ann,) if search_keyword and not self.fail else (),
                fetched_at=datetime.now(ZoneInfo('Asia/Shanghai')),
                metadata={'search_keyword': search_keyword})
    class Pdf:
        calls = 0
        def fetch_evidence(self, announcement):
            self.calls += 1
            return CninfoPdfEvidence(announcement_id=announcement.announcement_id,
                pdf_url=announcement.pdf_url, available=True, reason_code='OK',
                fetched_at=datetime.now(ZoneInfo('Asia/Shanghai')), pdf_sha256='a'*64,
                cache_relative_path='raw/test.pdf', byte_size=1,
                page_count=1, pages_scanned=1, extracted_chars=30,
                extraction_version=BUSINESS_EXTRACTION_VERSION,
                snippets=(PdfEvidenceSnippet(page_number=1,
                    text='公司主要从事精密光学仪器的研发、生产和销售。'),))
    value = DisclosureCacheWorker(settings, Router(), Pdf())
    return value, now


def query(value, now):
    return value.collect('600001.SH', (now.date()-timedelta(days=10)).isoformat(),
        now.date().isoformat(), (now.date()-timedelta(days=450)).isoformat())


def test_cache_only_worker_reuses_formal_queries_and_business_pdf(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    first = query(value, now)
    assert first['ok'] is True and first['business_available'] is True
    assert value.router.calls == 1 and value.pdf_client.calls == 1
    second = query(value, now)
    assert second['ok'] is True
    assert value.router.calls == 1 and value.pdf_client.calls == 1
    assert first['recent_reason_code'] == 'RECENT_NOT_REQUESTED'
    assert not value.app.fact_cache.get_cached_result('CNINFO_ANNOUNCEMENTS', '600001.SH:RECENT_10D')
    assert value.can_reuse(second, datetime.now(ZoneInfo('Asia/Shanghai'))) is True
    assert not hasattr(value.app, 'runtime_store')
    assert not hasattr(value.app, 'a1_registry')


def test_failed_wider_business_query_preserves_cache_and_never_marks_ready(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    row = query(value, now)
    assert row['ok']
    cache = value.app.fact_cache
    key = '600001.SH:ANNUAL_REPORT_450D'
    saved = cache.get_cached_result('CNINFO_ANNOUNCEMENTS', key)
    cache.put_cached_result('CNINFO_ANNOUNCEMENTS', key, saved['payload'],
        fetched_at=now-timedelta(days=1), expires_at=now-timedelta(minutes=1))
    value.router.fail = True
    # A wider start-date query cannot be covered by the existing narrower cache.
    failed = value.collect('600001.SH', (now.date()-timedelta(days=30)).isoformat(),
        now.date().isoformat(), (now.date()-timedelta(days=460)).isoformat())
    assert failed['ok'] is False and failed['business_complete'] is False
    assert failed['reason_code'] == 'CACHE_COVERAGE_NOT_CURRENT'
    assert cache.get_cached_result('CNINFO_ANNOUNCEMENTS', key)['content_hash'] == saved['content_hash']
    # The old narrower business window is not poisoned by a failed wider
    # request; it is NOT proof of the wider window or of current risk news.
    assert value.can_reuse(row, datetime.now(ZoneInfo('Asia/Shanghai'))) is True
    assert value.can_reuse(row, datetime.now(ZoneInfo('Asia/Shanghai'))+timedelta(days=8)) is False


def test_no_business_filings_is_complete_cache_work_not_fabricated_business_evidence(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    original = value.router.fetch_announcements
    def empty(*args, **kwargs):
        return original(*args, **kwargs).model_copy(update={'announcements': (), 'total': 0})
    value.router.fetch_announcements = empty
    row = query(value, now)
    assert row['ok'] and row['business_complete'] and row['pdf_complete']
    assert row['business_available'] is False
    assert row['expected_pdf_count'] == 0 and row['pdf_records'] == []
    assert value.pdf_client.calls == 0
    assert value.can_reuse(row, datetime.now(ZoneInfo('Asia/Shanghai'))) is True
    assert value.can_reuse({**row, 'expected_pdf_count': 1}, now) is False


def test_business_cache_survives_six_hour_recent_ttl_without_certifying_next_day_news(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    row = query(value, now)
    assert value.can_reuse(row, now+timedelta(hours=17)) is True
    assert all('RECENT_10D' not in r['key'] for r in row['query_records'])
    assert row['recent_complete'] is False and row['recent_required'] is False


def test_resume_rechecks_pdf_content_not_just_old_success_receipt(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    row = query(value, now)
    cache = value.app.fact_cache
    saved = cache.get_cached_result('CNINFO_PDF_EVIDENCE', 'test-ann')
    changed = {**saved['payload'], 'available': False, 'reason_code': 'CNINFO_PDF_TIMEOUT', 'snippets': []}
    cache.put_cached_result('CNINFO_PDF_EVIDENCE', 'test-ann', changed,
        fetched_at=datetime.now(ZoneInfo('Asia/Shanghai')), expires_at=now+timedelta(minutes=15))
    assert value.can_reuse(row, datetime.now(ZoneInfo('Asia/Shanghai'))) is False


def test_catalog_failure_does_not_turn_missing_identity_into_empty_success(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    class Catalog:
        def warm_org_catalog(self, symbols):
            raise TimeoutError('secret-provider-url')
    receipt = value.warm_catalog(Catalog(), ['600001.SH'])
    assert receipt == {'status': 'UNAVAILABLE', 'reason_code': 'TimeoutError'}
    row = query(value, now)
    assert row['ok'] is True  # Per-symbol official queries actually completed.
    assert row['org_catalog_receipt'] == receipt


def test_catalog_warmup_is_lazy_and_inside_the_maintenance_deadline(tmp_path, monkeypatch):
    from threading import Event
    import time
    from test_wp5_disclosure_maintenance import queue, NOW
    from liangjian_funnel.pipeline.disclosure_maintenance import run_maintenance
    value, _ = worker(tmp_path, monkeypatch)
    entered, release, done = Event(), Event(), Event()
    class Catalog:
        def warm_org_catalog(self, symbols):
            entered.set()
            release.wait(2)
            raise TimeoutError('secret-url')
    value.configure_catalog(Catalog(), ['000000.SZ'])
    assert not entered.is_set()
    def collect(*args):
        try:
            return value.collect(*args)
        finally:
            done.set()
    started = time.monotonic()
    try:
        report = run_maintenance(queue(tmp_path), output_dir=tmp_path/'reports', now=NOW,
            execute=True, collect=collect, can_reuse=value.can_reuse, budget_seconds=0.05)
        assert entered.is_set() and time.monotonic()-started < 0.5
        assert report['rows'][0]['reason_code'] == 'DEADLINE_EXCEEDED'
    finally:
        release.set()
        assert done.wait(1)


def test_future_pdf_evidence_is_not_resume_proof(tmp_path, monkeypatch):
    value, now = worker(tmp_path, monkeypatch)
    row = query(value, now)
    cache = value.app.fact_cache
    saved = cache.get_cached_result('CNINFO_PDF_EVIDENCE', 'test-ann')
    future = datetime.now(ZoneInfo('Asia/Shanghai'))+timedelta(hours=1)
    payload = {**saved['payload'], 'fetched_at': future.isoformat()}
    current = cache.put_cached_result('CNINFO_PDF_EVIDENCE', 'test-ann', payload,
        fetched_at=future, expires_at=future+timedelta(days=1))
    # Even a matching receipt hash cannot grant access to future knowledge.
    row['pdf_records'][0]['content_hash'] = current['content_hash']
    assert value.can_reuse(row, datetime.now(ZoneInfo('Asia/Shanghai'))) is False
