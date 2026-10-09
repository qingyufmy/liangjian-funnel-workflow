"""Small cache-only adapter reusing the formal official disclosure contracts."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..data.cninfo import CninfoAnnouncement, CninfoFetchResult
from ..data.disclosure_incremental import covers_query
from ..facts.contracts import canonical_json_hash
from ..facts.cninfo import _pdf_payload, compact_cninfo_pdf_evidence
from .local_fact_cache import LocalFactCache

SHANGHAI = ZoneInfo('Asia/Shanghai')


class DisclosureCacheWorker:
    def __init__(self, settings: Any, router: Any, pdf_client: Any):
        self.app = DisclosureCacheContext(settings)
        self.router, self.pdf_client = router, pdf_client

    def warm_catalog(self, client, symbols):
        try:
            self.catalog_receipt = self.app._warm_cninfo_org_catalog(client, symbols)
        except Exception as exc:
            # Catalogue failure must preserve the existing per-symbol official
            # fallback path. It is not proof of an empty disclosure query.
            self.catalog_receipt = {'status': 'UNAVAILABLE', 'reason_code': type(exc).__name__}
        return self.catalog_receipt

    def configure_catalog(self, client, symbols):
        self.catalog_request = (client, list(symbols))

    def collect(self, symbol, start, end, business_start):
        if hasattr(self, 'catalog_request') and not hasattr(self, 'catalog_receipt'):
            self.warm_catalog(*self.catalog_request)
        from ..workflow import _build_cninfo_pdf_tasks, _main_business_evidence
        _, recent, recent_hit, business, business_hit = self.app._fetch_cninfo_candidate_queries(
            self.router, symbol, start, end, business_start, include_recent=False)
        tasks = _build_cninfo_pdf_tasks({symbol: business},
            limit=self.app.settings.cninfo_pdf_max_documents_per_symbol)
        evidence_rows, pdf_records, failures = [], [], []
        for _, announcement in tasks:
            now = datetime.now(SHANGHAI)
            cached = self.app.fact_cache.get_cached_result('CNINFO_PDF_EVIDENCE',
                announcement.announcement_id, fresh_at=now)
            evidence = self.app._cached_cninfo_pdf_evidence_from_record(announcement, cached)
            if evidence is None:
                evidence = self.pdf_client.fetch_evidence(announcement)
                # A retryable refresh failure must not hide usable old proof.
                # Failure is in the task journal, not a replacement cache row.
                if evidence.available:
                    self.app._persist_cninfo_pdf_evidence(evidence)
            if not evidence.available:
                failures.append(evidence.reason_code)
            evidence_rows.append({**_pdf_payload(compact_cninfo_pdf_evidence(evidence)),
                'announcement_id': announcement.announcement_id, 'content_hash': evidence.pdf_sha256,
                'announcement_title': announcement.announcement_title,
                'publish_time': announcement.publish_time.isoformat(), 'source_url': announcement.pdf_url})
            current = self.app.fact_cache.get_cached_result('CNINFO_PDF_EVIDENCE',
                announcement.announcement_id, fresh_at=datetime.now(SHANGHAI))
            if evidence.available and current:
                pdf_records.append({'announcement': announcement.model_dump(mode='json'),
                                    'content_hash': current['content_hash']})
        business_available = _main_business_evidence({'by_symbol': {symbol: evidence_rows}}, [symbol])[symbol]['available']
        cache_records = []
        queries = [('ANNUAL_REPORT_450D', business_start, '年度报告')]
        semantics = {'报告': 'BUSINESS_REPORT_450D_V3', '招股说明书': 'BUSINESS_PROSPECTUS_450D_V3'}
        for query in business.metadata.get('supplemental_queries', []):
            queries.append((semantics[query['search_keyword']], business_start, query['search_keyword']))
        for semantic, query_start, keyword in queries:
            key = f'{symbol}:{semantic}'
            cached = self.app.fact_cache.get_cached_result('CNINFO_ANNOUNCEMENTS', key,
                fresh_at=datetime.now(SHANGHAI))
            if cached:
                cache_records.append({'key': key, 'content_hash': cached['content_hash'],
                    'start': query_start, 'end': end, 'keyword': keyword})
        pdf_complete = not failures and len(pdf_records) == len(tasks)
        row = {'symbol': symbol, 'ok': bool(business.ok and business.complete and pdf_complete),
            'maintenance_scope': 'BUSINESS_AND_PDF_ONLY', 'recent_required': False,
            'recent_complete': False,
            'business_complete': bool(business.ok and business.complete),
            'pdf_complete': pdf_complete, 'expected_pdf_count': len(tasks),
            'business_available': business_available,
            'reason_code': ('BUSINESS_QUERY_INCOMPLETE' if not business.ok or not business.complete
                else 'PDF_INCOMPLETE' if not pdf_complete else 'CACHE_WARMED'),
            'recent_reason_code': recent.reason_code, 'business_reason_code': business.reason_code,
            'pdf_reason_codes': failures, 'recent_cache_hit': recent_hit, 'business_cache_hit': business_hit,
            'query_records': cache_records, 'expected_query_count': len(queries), 'pdf_records': pdf_records,
            'recent_result_hash': canonical_json_hash(recent.model_dump(mode='json')),
            'business_result_hash': canonical_json_hash(business.model_dump(mode='json')),
            'org_catalog_receipt': getattr(self, 'catalog_receipt', None)}
        # A stale fallback can be useful proof, but is not current query
        # coverage and cannot certify the night work as complete.
        if row['ok'] and not self.can_reuse(row, datetime.now(SHANGHAI)):
            row.update(ok=False, business_complete=False, reason_code='CACHE_COVERAGE_NOT_CURRENT')
        return row

    def can_reuse(self, row, now):
        try:
            return self._can_reuse(row, now)
        except (ValueError, KeyError, TypeError):
            return False

    def _can_reuse(self, row, now):
        queries = row.get('query_records', [])
        if (not row.get('ok') or row.get('maintenance_scope') != 'BUSINESS_AND_PDF_ONLY'
                or len(queries) != row.get('expected_query_count') or len(queries) < 1):
            return False
        for query in queries:
            cached = self.app.fact_cache.get_cached_result('CNINFO_ANNOUNCEMENTS', query['key'], fresh_at=now)
            if not cached or cached['content_hash'] != query['content_hash']:
                return False
            if canonical_json_hash(cached['payload']) != cached['content_hash']:
                return False
            result = CninfoFetchResult.model_validate(cached['payload'])
            if not covers_query(result, symbol=row['symbol'], start=query['start'], end=query['end'],
                    keyword=query['keyword'], now=now, cache_keyword=query['keyword']):
                return False
        if (not isinstance(row.get('expected_pdf_count'), int)
                or row.get('expected_pdf_count') != len(row.get('pdf_records', []))):
            return False
        for record in row['pdf_records']:
            ann = CninfoAnnouncement.model_validate(record['announcement'])
            if ann.sec_code != row['symbol'].split('.')[0] or ann.publish_time > now:
                return False
            cached = self.app.fact_cache.get_cached_result('CNINFO_PDF_EVIDENCE', ann.announcement_id, fresh_at=now)
            if not cached or cached['content_hash'] != record['content_hash']:
                return False
            evidence = self.app._cached_cninfo_pdf_evidence_from_record(ann, cached)
            if evidence is None or not evidence.available or evidence.fetched_at > now:
                return False
        return True


class DisclosureCacheContext:
    """Explicit cache-only receiver for the existing formal query helpers.

    No partially initialized WorkflowApplication is constructed. This small
    interface is integration-tested using real helpers and fake transports.
    """
    def __init__(self, settings):
        self.settings = settings
        self.fact_cache = LocalFactCache(settings.fact_cache_db_path)

    def _warm_cninfo_org_catalog(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._warm_cninfo_org_catalog(self, *args, **kwargs)

    def _fetch_cninfo_candidate_queries(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._fetch_cninfo_candidate_queries(self, *args, **kwargs)

    def _cached_cninfo_result(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._cached_cninfo_result(self, *args, **kwargs)

    def _cached_cninfo_pdf_evidence_from_record(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._cached_cninfo_pdf_evidence_from_record(self, *args, **kwargs)

    def _persist_cninfo_pdf_evidence(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._persist_cninfo_pdf_evidence(self, *args, **kwargs)

    def _prune_cninfo_pdf_raw(self, *args, **kwargs):
        from ..workflow import WorkflowApplication
        return WorkflowApplication._prune_cninfo_pdf_raw(self, *args, **kwargs)
