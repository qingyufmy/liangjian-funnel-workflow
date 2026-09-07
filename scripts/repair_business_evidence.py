"""Isolated, resumable business-evidence repair. Never publishes A1/A2/A3 plans."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib
import re
from types import SimpleNamespace

from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoClient, CninfoFetchResult
from liangjian_funnel.data.bse import BseClient
from liangjian_funnel.data.cninfo_pdf import CninfoPdfClient, CninfoPdfEvidence
from liangjian_funnel.facts.cninfo import (
    _pdf_payload, compact_cninfo_pdf_evidence, is_full_periodic_report, is_final_prospectus,
    select_cninfo_pdf_candidates,
)
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.workflow import WorkflowApplication, _main_business_evidence, _hash_json
from liangjian_funnel.pipeline.deterministic import screen_a1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--download', action='store_true')
    parser.add_argument('--query-missing', action='store_true')
    parser.add_argument('--reextract', action='store_true', help='Re-extract unresolved documents from verified local PDFs')
    parser.add_argument('--refresh-stale', action='store_true', help='Also try a newer full report when current proof uses an older filing')
    parser.add_argument('--verify-selector', action='store_true', help='Replay formal selection and bounded fallback from exported evidence only; no network downloads')
    parser.add_argument('--workers', type=int, choices=range(1, 5), default=2)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    root = args.root.resolve()
    read = lambda p: json.loads(p.read_text(encoding='utf-8'))
    snapshot = read(root/'source-snapshot.json')
    data = snapshot['data']
    if _hash_json(data) != snapshot['snapshot_hash']:
        raise SystemExit('SOURCE_SNAPSHOT_HASH_MISMATCH')
    cache = read(root/'pdf-cache-records.json')
    baseline = {r['symbol'] for r in read(root/'gaps-full-cache.json')['rows']}
    events = data['DISCLOSURE_EVENTS']['by_symbol']
    for p in (root/'query-results').glob('*.json'):
        q = read(p)
        if q['ok']:
            known = {r['announcement_id'] for r in events.get(q['symbol'], [])}
            events.setdefault(q['symbol'], []).extend(r for r in q['records'] if r['announcement_id'] not in known)
    updates = {p.stem: read(p) for p in (root/'evidence').glob('*.json')}

    def project(symbol):
        rows = []
        for raw in events.get(symbol, []):
            payload = updates.get(raw['announcement_id']) or cache.get(raw['announcement_id'], {}).get('payload')
            row = dict(raw)
            if payload:
                evidence = CninfoPdfEvidence.model_validate(payload)
                if evidence.announcement_id != raw['announcement_id'] or evidence.pdf_url != raw.get('source_url'):
                    raise ValueError('BUSINESS_REPAIR_SOURCE_IDENTITY_MISMATCH')
                row.update(_pdf_payload(compact_cninfo_pdf_evidence(evidence)))
                row['content_hash'] = evidence.pdf_sha256 or raw.get('content_hash')
                row['evidence_fetched_at'] = evidence.fetched_at.isoformat()
            rows.append(row)
        return _main_business_evidence({'by_symbol': {symbol: rows}}, [symbol])[symbol]

    symbols = sorted(data['g0_symbols'])
    before = {s: project(s) for s in symbols}
    missing = [s for s in symbols if not before[s]['available']]
    def newer_filing(symbol, business):
        latest = max((r['publish_time'] for r in events.get(symbol, []) if is_full_periodic_report(r['announcement_title'])), default='')
        proof_time = max((p.get('publish_time') or '' for p in business['evidence'] if is_full_periodic_report(p.get('announcement_title') or '')), default='')
        return bool(latest and latest > proof_time)
    stale = [s for s in symbols if before[s]['available'] and newer_filing(s, before[s])]
    selected_symbols = missing + (stale if args.refresh_stale else [])
    print(json.dumps({'phase': 'cache_projection', 'total': len(symbols), 'missing': len(missing),
        'baseline_missing': len(baseline), 'baseline_recovered': sum(before[s]['available'] for s in baseline), 'older_filing_fallbacks': len(stale)}), flush=True)

    def repair(symbol):
        rows = events.get(symbol, [])
        candidates = [r for r in rows if is_full_periodic_report(r['announcement_title']) or is_final_prospectus(r['announcement_title'])]
        if args.query_missing and not candidates:
            factory = BseClient if symbol.endswith('.BJ') else CninfoClient
            records, query_audit = [], []
            with factory(timeout_seconds=10, min_request_interval_seconds=0.25) as client:
                for keyword in ('报告', '招股说明书'):
                    query = client.fetch_announcements(symbol, '2025-01-01', datetime.now(ZoneInfo('Asia/Shanghai')).date().isoformat(), search_keyword=keyword, max_pages=10)
                    query_audit.append({'keyword': keyword, 'ok': query.ok, 'complete': query.complete, 'reason_code': query.reason_code})
                    if query.ok and query.complete:
                        records.extend({
                            'announcement_id': a.announcement_id, 'announcement_title': a.announcement_title,
                            'sec_name': a.sec_name, 'source_url': a.pdf_url, 'publish_time': a.publish_time.isoformat(),
                            'content_hash': a.content_hash, 'storage_time': a.storage_time.isoformat() if a.storage_time else None,
                        } for a in query.announcements)
                    if any(is_full_periodic_report(r['announcement_title']) or is_final_prospectus(r['announcement_title']) for r in records):
                        break
            records = list({r['announcement_id']: r for r in records}.values())
            atomic_write_json(root/'query-results'/f'{symbol}.json', {'symbol': symbol, 'ok': bool(records),
                'reason_code': query.reason_code, 'fetched_at': query.fetched_at.isoformat(), 'records': records, 'queries': query_audit})
            if records:
                known = {r['announcement_id'] for r in rows}
                rows = rows + [r for r in records if r['announcement_id'] not in known]
                events[symbol] = rows
                candidates = [r for r in rows if is_full_periodic_report(r['announcement_title']) or is_final_prospectus(r['announcement_title'])]
        candidates.sort(key=lambda r: (r['publish_time'], r['announcement_id']), reverse=True)
        attempted = []
        for row in candidates[:3]:
            identifier = row['announcement_id']
            if identifier in updates and not args.reextract:
                attempted.append({'announcement_id': identifier, 'reason': updates[identifier]['reason_code'], 'reused': True})
                continue
            ann = CninfoAnnouncement(announcement_id=identifier, sec_code=symbol[:6], sec_name=row['sec_name'],
                announcement_title=row['announcement_title'], adjunct_url=row['source_url'], publish_time=row['publish_time'], storage_time=row.get('storage_time'))
            with CninfoPdfClient(root/'pdfs', timeout_seconds=25, max_attempts=2, max_bytes=64*1024*1024) as client:
                evidence = client.fetch_evidence(ann)
            payload = evidence.model_dump(mode='json')
            atomic_write_json(root/'evidence'/f'{identifier}.json', payload)
            updates[identifier] = payload
            attempted.append({'announcement_id': identifier, 'reason': evidence.reason_code, 'reused': False})
            if project(symbol)['available']:
                break
        return {'symbol': symbol, 'available': project(symbol)['available'], 'attempts': attempted,
                'candidate_count': len(candidates)}

    outcomes = []
    if args.download:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            jobs = {pool.submit(repair, s): s for s in selected_symbols[:args.limit]}
            for job in as_completed(jobs):
                try:
                    row = job.result()
                except Exception as exc:
                    row = {'symbol': jobs[job], 'available': False, 'error': type(exc).__name__}
                outcomes.append(row)
                atomic_write_json(root/'repair-progress.json', {'completed': len(outcomes), 'selected': len(jobs), 'rows': outcomes})
                print(json.dumps({'completed': len(outcomes), 'selected': len(jobs), **row}, ensure_ascii=False), flush=True)
    projection = {s: project(s) for s in symbols}
    a1 = next(s['output'] for s in read(root/'source-research.json')['stages'] if s['stage'] == 'A1')
    gate = screen_a1({**data, 'MAIN_BUSINESS_EVIDENCE': projection}, a1)
    previous = data['MAIN_BUSINESS_EVIDENCE']
    verification_failures, report_cohorts, parser_counts = [], Counter(), Counter()
    raw_pdf_verified = set()
    for symbol, business in projection.items():
        for proof in business['evidence']:
            aid = proof['announcement_id']
            payload = updates.get(aid) or cache.get(aid, {}).get('payload')
            record = next((r for r in events[symbol] if r['announcement_id'] == aid), None)
            if not payload or not record:
                verification_failures.append({'symbol': symbol, 'reason': 'SOURCE_RECORD_MISSING'})
                continue
            page = proof['page_number']
            valid = (proof['content_hash'] == payload['pdf_sha256']
                and record['source_url'] == payload['pdf_url']
                and isinstance(page, int) and 1 <= page <= payload['page_count']
                and any(s['page_number'] == page and s['text'] == proof['text'] for s in payload['snippets']))
            if not valid:
                verification_failures.append({'symbol': symbol, 'reason': 'HASH_PAGE_TEXT_IDENTITY_MISMATCH'})
            # New repairs retain original local bytes; legacy cache proofs
            # are validated against the exported hash-bound source record.
            if aid in updates and aid not in raw_pdf_verified:
                path = root/'pdfs'/payload['cache_relative_path']
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != payload['pdf_sha256']:
                    verification_failures.append({'symbol': symbol, 'reason': 'RAW_PDF_HASH_MISMATCH'})
                raw_pdf_verified.add(aid)
        if business['evidence']:
            first = business['evidence'][0]
            title = first.get('announcement_title') or ''
            period = re.search(r'20\d{2}', title)
            cohort = 'PROSPECTUS' if '招股说明书' in title else (
                (period.group() + ('_HALF_YEAR' if '半' in title else '_ANNUAL')) if period else 'OTHER_DISCLOSURE')
            report_cohorts[cohort] += 1
            parser_counts[first.get('parser') or 'legacy_unrecorded'] += 1
    report = {
        'captured_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        'source_snapshot_id': snapshot['snapshot_id'], 'source_snapshot_hash': snapshot['snapshot_hash'],
        'production_plans_changed': False, 'historical_replay': False,
        'baseline_missing': len(baseline), 'baseline_recovered': sum(projection[s]['available'] for s in baseline),
        'baseline_remaining': [s for s in sorted(baseline) if not projection[s]['available']],
        'full_count': len(symbols), 'full_available': sum(p['available'] for p in projection.values()),
        'full_missing': [s for s in symbols if not projection[s]['available']],
        'previous_available_now_rejected': [s for s in symbols if previous.get(s, {}).get('available') and not projection[s]['available']],
        'older_filing_fallback_symbols': [s for s in symbols if projection[s]['available'] and newer_filing(s, projection[s])],
        'verification': {'passed': not verification_failures, 'failures': verification_failures,
            'new_raw_pdf_hashes_verified': len(raw_pdf_verified), 'primary_evidence_periods': dict(report_cohorts),
            'primary_evidence_parsers': dict(parser_counts)},
        'quant_only_counts': dict(Counter(r['status'] for r in gate.decisions)),
        'business_projection': projection,
    }
    if args.verify_selector:
        report['formal_selection_replay'] = verify_formal_selection(symbols, events, cache, updates, root)
    report['content_hash'] = _hash_json(report)
    atomic_write_json(root/'repair-report.json', report)
    print(json.dumps({k:v for k,v in report.items() if k != 'business_projection'}, ensure_ascii=False), flush=True)
    if verification_failures:
        raise SystemExit('BUSINESS_EVIDENCE_VERIFICATION_FAILED')
    if args.verify_selector and report['formal_selection_replay']['missing']:
        raise SystemExit('FORMAL_SELECTION_BUSINESS_GAP')


def verify_formal_selection(symbols, events, cache, updates, root):
    """Use real selectors/orchestration with verified exported PDF payloads.

    Does not verify live cache TTLs or delivery; never claims to run production.
    """
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    results, lookup, evidence, ids, calls = {}, {}, {}, {}, []
    def exported_evidence(_client, announcement):
        aid = announcement.announcement_id
        calls.append(aid)
        payload = updates.get(aid) or cache.get(aid, {}).get('payload')
        if not payload:
            return CninfoPdfEvidence(announcement_id=aid, pdf_url=announcement.pdf_url,
                available=False, reason_code='OFFLINE_CACHE_MISS', fetched_at=now)
        item = CninfoPdfEvidence.model_validate(payload)
        if item.announcement_id != aid or item.pdf_url != announcement.pdf_url:
            raise ValueError('FORMAL_REPLAY_SOURCE_IDENTITY_MISMATCH')
        return compact_cninfo_pdf_evidence(item)
    for symbol in symbols:
        announcements = []
        for row in events.get(symbol, []):
            a = CninfoAnnouncement(announcement_id=row['announcement_id'], sec_code=symbol[:6],
                sec_name=row['sec_name'], announcement_title=row['announcement_title'],
                adjunct_url=row['source_url'], publish_time=row['publish_time'])
            announcements.append(a)
            lookup[(symbol, a.announcement_id)] = row
        result = CninfoFetchResult(symbol=symbol, start_date='2025-01-01', end_date=now.date().isoformat(),
            ok=True, complete=True, reason_code='OK', announcements=tuple(announcements), fetched_at=now)
        results[symbol] = result
        for a in select_cninfo_pdf_candidates(result, limit=3):
            evidence[a.announcement_id] = exported_evidence(None, a)
            ids.setdefault(symbol, []).append(a.announcement_id)
    def project():
        rows = {symbol: [{**lookup[(symbol, aid)], **_pdf_payload(evidence[aid])}
            for aid in ids.get(symbol, [])] for symbol in symbols}
        return _main_business_evidence({'by_symbol': rows}, symbols)
    before = project()
    calls.clear()
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(cninfo_pdf_cache_dir=root/'pdfs', timeout_seconds=1, cninfo_pdf_workers=3)
    # An in-memory exported-source adapter, deliberately not the live cache or downloader.
    app._cached_cninfo_pdf_evidence = exported_evidence
    app._supplement_cninfo_business_evidence(results, evidence, ids, {})
    after = project()
    return {'production_selector_offline_replay': True, 'network_downloads': 0,
        'before_available': sum(v['available'] for v in before.values()),
        'after_available': sum(v['available'] for v in after.values()),
        'fallback_exported_cache_reads': len(calls),
        'missing': [s for s, v in after.items() if not v['available']]}


if __name__ == '__main__':
    main()
