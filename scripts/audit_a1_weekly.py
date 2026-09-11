"""Read the sealed A1 and optionally compare fresh full-universe quant facts."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.a1_registry import A1Registry
from liangjian_funnel.pipeline.deterministic import screen_a1
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import default_a1_registry_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE_AUDIT')
    settings = Settings.from_env(root=Path.cwd())
    active = A1Registry(default_a1_registry_path(settings)).get_active_generation()
    if active is None:
        raise SystemExit('A1_ACTIVE_MISSING')
    lane = active.payload['lanes'][settings.research_primary_lane_id]
    output = lane['output']
    pools = ('active_research_pool', 'monitor_pool', 'rejected_candidates')
    rows = {key: [{k: row.get(k) for k in ('symbol', 'name', 'primary_theme', 'theme_id',
        'company_name', 'monthly_direction_name', 'monthly_direction_matches', 'sector_index_name',
        'business_exposure', 'disclosed_business_match', 'reason_codes', 'half_year_support',
        'fundamental_support', 'selection_basis')}
        for row in output.get(key, [])] for key in pools}
    current_symbols = {row['symbol'] for row in rows['active_research_pool']}
    report = {'schema': 'a1-weekly-audit/1', 'generated_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        'generation_id': active.generation_id, 'mode': active.mode, 'as_of': active.as_of.isoformat(),
        'snapshot_id': active.snapshot_id, 'manifest_counts': {k:v for k,v in active.manifest.items() if 'count' in k},
        'pool_counts': {k:len(v) for k,v in rows.items()},
        'active_theme_counts': dict(Counter(r.get('theme_id') or r.get('primary_theme') or 'UNMAPPED' for r in rows['active_research_pool'])),
        'all_matched_theme_counts': dict(Counter(theme for r in rows['active_research_pool']
            for theme in {m.get('monthly_direction_id') for m in r.get('monthly_direction_matches') or []
                if m.get('monthly_direction_id')})),
        'structural_themes': output.get('structural_themes', []),
        'partitions': rows, 'production_decisions_changed': False}
    if args.baseline:
        baseline = json.loads(args.baseline.read_text(encoding='utf8'))
        prior = {r['symbol'] for r in baseline['partitions']['active_research_pool']}
        report['delta'] = {'baseline_generation_id': baseline['generation_id'],
            'added': sorted(current_symbols-prior), 'removed': sorted(prior-current_symbols),
            'retained_count': len(prior & current_symbols)}
    if args.snapshot:
        raw = json.loads(args.snapshot.read_text(encoding='utf8'))
        digest = hashlib.sha256(json.dumps(raw['data'],ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
        if raw['snapshot_hash'] != digest:
            raise SystemExit('SNAPSHOT_HASH_MISMATCH')
        gate = screen_a1(raw['data'], output)
        symbols = set(raw['data']['g0_symbols'])
        business = raw['data'].get('MAIN_BUSINESS_EVIDENCE', {})
        missing_business = sorted(s for s in symbols if not business.get(s, {}).get('available'))
        report['fresh_quant_comparison'] = {'basis': 'FRESH_FACTS_WITH_SEALED_MONTHLY_DIRECTIONS_NOT_NEW_LLM_SELECTION',
            'snapshot_id': raw['snapshot_id'], 'snapshot_hash': digest,
            'g0_count': len(symbols), 'status_counts': dict(Counter(r['status'] for r in gate.decisions)),
            'universe_exclusions': [{k: r.get(k) for k in
                ('symbol', 'name', 'research_eligible', 'trade_eligible', 'exclusion_reasons')}
                for r in raw['data'].get('universe_candidates', [])
                if not r.get('research_eligible')],
            'business_available_count': len(symbols) - len(missing_business),
            'business_missing_symbols': missing_business,
            'business_evidence': {s: {'available': business.get(s, {}).get('available', False),
                'reason_code': business.get(s, {}).get('reason_code'),
                'latest_full_report_publish_time': business.get(s, {}).get('latest_full_report_publish_time'),
                'uses_older_filing': business.get(s, {}).get('uses_older_filing'),
                'references': [{k:e.get(k) for k in ('source_ref','source_url','announcement_title',
                    'publish_time','page_number','content_hash','extraction_version')}
                    for e in business.get(s, {}).get('evidence', [])]} for s in sorted(symbols)},
            'reason_counts': dict(Counter(code for r in gate.decisions for code in r.get('reason_codes',[]))),
            'decisions': [{k:r.get(k) for k in ('symbol','name','status','theme_id','industry','reason_codes',
                'selection_basis','half_year_support','fundamental_support','sector_index_name')} for r in gate.decisions]}
    atomic_write_json(args.output, report)
    print(json.dumps({k:v for k,v in report.items() if k not in ('partitions','fresh_quant_comparison')},ensure_ascii=False))


if __name__ == '__main__':
    main()
