"""Recover only failed documents referenced by one frozen snapshot, into an isolated folder."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3

from liangjian_funnel.settings import Settings
from liangjian_funnel.data.cninfo import CninfoAnnouncement
from liangjian_funnel.data.cninfo_pdf import CninfoPdfClient
from liangjian_funnel.reporting import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=16)
    args = parser.parse_args()
    settings = Settings.from_env(root=Path.cwd())
    source = json.loads(args.snapshot.read_text(encoding='utf-8'))
    data = source.get('data', source)
    manifest = data['snapshot_manifest']
    facts = json.loads((settings.fact_store_dir / manifest['fact_store_relative_path']).read_text())['facts']
    announcements = {r.get('payload', {}).get('announcement_id'): r for r in facts if r.get('fact_type') == 'DISCLOSURE_EVENT'}
    rows = []
    with sqlite3.connect(settings.fact_cache_db_path.resolve().as_uri() + '?mode=ro', uri=True) as db, CninfoPdfClient(args.output_dir / 'pdfs', timeout_seconds=15, max_attempts=2) as client:
        for symbol, failures in sorted(manifest['source_failures'].items()):
            for failure in failures:
                if not failure.startswith('CNINFO_PDF:') or len(rows) >= args.limit:
                    continue
                identifier = failure.split(':')[1]
                record = announcements.get(identifier)
                old = db.execute("SELECT payload_json FROM cached_results WHERE namespace='CNINFO_PDF_EVIDENCE' AND cache_key=? ORDER BY fetched_at DESC LIMIT 1", (identifier,)).fetchone()
                old = json.loads(old[0]) if old else {}
                if record:
                    payload = record['payload']
                    announcement = CninfoAnnouncement(announcement_id=identifier, sec_code=symbol[:6],
                        sec_name=payload['sec_name'], announcement_title=payload['announcement_title'],
                        adjunct_url=record['source_url'], publish_time=datetime.fromisoformat(record['publish_time']))
                else:
                    announcement = None
                    for cached in db.execute("SELECT payload_json FROM cached_results WHERE namespace='CNINFO_ANNOUNCEMENTS' AND cache_key LIKE ? ORDER BY fetched_at DESC", (symbol + ':%',)):
                        for candidate in json.loads(cached[0]).get('announcements', []):
                            if candidate.get('announcement_id') == identifier:
                                announcement = CninfoAnnouncement.model_validate(candidate)
                                break
                        if announcement:
                            break
                    if announcement is None or announcement.pdf_url != old.get('pdf_url'):
                        raise ValueError(f'Original announcement URL cannot be verified: {identifier}')
                evidence = client.fetch_evidence(announcement)
                row = {'symbol': symbol, 'announcement_id': identifier, 'title': announcement.announcement_title,
                       'original_failure': failure, 'original_pdf_sha256': old.get('pdf_sha256'),
                       'same_original_bytes': old.get('pdf_sha256') == evidence.pdf_sha256,
                       'original_raw_retained': bool(old.get('cache_relative_path') and (settings.cninfo_pdf_cache_dir / old['cache_relative_path']).is_file()),
                       'recovery': evidence.model_dump(mode='json')}
                rows.append(row)
                atomic_write_json(args.output_dir / 'pdf-recovery.json', {'schema_version': 'pdf-recovery/1.0',
                    'original_snapshot': str(args.snapshot), 'production_mutation': False,
                    'scope': 'POST_HOC_RECOVERY_NOT_ORIGINAL_KNOWLEDGE', 'documents': rows})
                print(json.dumps({k: row[k] for k in ('symbol','announcement_id','title','same_original_bytes')} | {'status':evidence.reason_code,'pages':evidence.page_count,'characters':evidence.extracted_chars}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
