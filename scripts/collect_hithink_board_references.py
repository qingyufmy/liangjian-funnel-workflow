"""Bounded slow-reference refresh; independent output, no production workflow."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from liangjian_funnel.data.board_reference import digest, load_reference, write_reference
from liangjian_funnel.data.hithink_board_reference import SOURCE, binding_components, collect_catalog, collect_members, load_rotation_references
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env-root', required=True, type=Path)
    p.add_argument('--output-dir', required=True, type=Path)
    p.add_argument('--bindings', type=Path, default=Path('config/rotation_reference_bindings_v1.json'))
    p.add_argument('--registry', type=Path, default=Path('config/rotation_themes_v1.yaml'))
    p.add_argument('--refresh-days', type=int, default=7)
    p.add_argument('--max-requests', type=int, default=40)
    p.add_argument('--deadline-seconds', type=int, default=180)
    p.add_argument('--catalog-only', action='store_true')
    args = p.parse_args()
    if not 1 <= args.refresh_days <= 14 or not 1 <= args.max_requests <= 100 or args.deadline_seconds <= 0:
        p.error('invalid bounded refresh policy')
    bindings = json.loads(args.bindings.read_text(encoding='utf-8'))
    if not isinstance(bindings, list) or any(not isinstance(b, dict) for b in bindings):
        p.error('bindings must be an object list')
    themes = load_rotation_theme_config(args.registry)
    known = {t.theme_id for t in themes.themes}
    if any(b.get('theme_id') not in known or b.get('source_id') != SOURCE for b in bindings):
        p.error('unknown theme or wrong source')
    now = lambda: datetime.now(ZoneInfo('Asia/Shanghai'))
    deadline = time.monotonic() + args.deadline_seconds
    calls = 0
    audits = []
    catalogs = {}
    settings = Settings.from_env(root=args.env_root).model_copy(update={'timeout_seconds': 10})
    def refresh(category, code=None):
        nonlocal calls
        identity, kind = (code, 'members') if code else (category, 'catalog')
        cached = load_reference(args.output_dir, SOURCE, identity, now=now(), kind=kind)
        identity_valid = not code or (cached.get('source_catalog_hash') == catalogs[category].get('content_hash')
            and cached.get('board_name') == next((r['name'] for r in catalogs[category].get('records',[]) if r['board_id'] == code), None))
        if cached.get('available') and cached['age_days'] < args.refresh_days and identity_valid:
            audits.append({'board_id': identity, 'cache_reused': True, 'content_hash': cached['content_hash']})
            return cached
        if calls >= args.max_requests or time.monotonic() >= deadline:
            audits.append({'board_id': identity, 'failure': 'REFERENCE_REFRESH_BUDGET'})
            return cached
        calls += 1
        value = collect_members(client, catalogs[category], code, now=now) if code else collect_catalog(client, category, now=now)
        write_reference(args.output_dir, value)
        audits.append({'board_id': identity, 'available': value['available'], 'reason_code': value['reason_code'], 'content_hash': value['content_hash']})
        return value if value['available'] else cached
    with HithinkClient(settings) as client:
        for category in ('concept', 'industry'):
            catalogs[category] = refresh(category)
        if not args.catalog_only:
            selected = {(c['category'], c['board_id']) for b in bindings if b.get('approved') is True
                        for c in binding_components(b)}
            for category, code in sorted(selected):
                if catalogs.get(category, {}).get('available'):
                    refresh(category, code)
                else:
                    audits.append({'board_id': code, 'failure': 'REFERENCE_CATALOG_UNAVAILABLE'})
    projected = load_rotation_references(args.output_dir, bindings, sorted(known), as_of=now())
    report = {'observed_at': now().isoformat(), 'execution_scope': 'SHADOW',
        'production_publish_forbidden': True, 'requests': calls, 'attempts': audits,
        'bindings_hash': digest(bindings), 'catalog_counts': {k: len(v.get('records', [])) for k,v in catalogs.items()},
        'theme_count': len(known), 'qualified_theme_count': sum(v['available'] for v in projected.values()),
        'unavailable_themes': {k: v['reason_code'] for k,v in projected.items() if not v['available']},
        'projections': projected, 'provider_revision_date_known': False}
    report['market_mapping_complete'] = report['qualified_theme_count'] == len(known)
    report['requested_scope'] = 'CATALOG_ONLY' if args.catalog_only else 'REVIEWED_BINDINGS_ONLY'
    report['content_hash'] = digest(report)
    output = args.output_dir / f"refresh-audit-{report['content_hash']}.json"
    atomic_write_json(output, report)
    print(json.dumps({'report': str(output), 'requests': calls, 'catalog_counts': report['catalog_counts'],
                      'qualified_themes': report['qualified_theme_count'], 'theme_count': len(known)}, ensure_ascii=True))
    return 2 if any(a.get('failure') or a.get('available') is False for a in audits) else 0


if __name__ == '__main__':
    raise SystemExit(main())
