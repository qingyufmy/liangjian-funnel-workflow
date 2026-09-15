"""Read frozen production inputs; compare only the A2 stock-structure predicate."""
import argparse
import gc
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings


def load_candidate(directory, name):
    path = directory / (name + '.py')
    key = 'liangjian_funnel.pipeline.' + name + '_audit'
    spec = importlib.util.spec_from_file_location(key, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[key] = module
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate-module-dir', type=Path, default=Path('src/liangjian_funnel/pipeline'))
    parser.add_argument('--output', type=Path, default=Path('outputs/recovery/a2-structure-shadow-v2-2026-09-15.json'))
    args = parser.parse_args()
    features = load_candidate(args.candidate_module_dir, 'a2_features')
    deterministic = load_candidate(args.candidate_module_dir, 'deterministic')
    settings = Settings.from_env()
    results = []
    for day, snap, run in [
        ('2026-09-14', 'snapshot-20260914T183000+0800-72257d014f07', '2026-09-14-close-research-permission-final-181241'),
        ('2026-09-15', 'snapshot-20260915T154405+0800-525b59d35f5a', '2026-09-15-close-525b59d35f5a'),
    ]:
        path = Path('storage/snapshots') / (snap + '.json')
        raw = json.loads(path.read_text())
        frozen = raw['data']
        actual_hash = hashlib.sha256(json.dumps(frozen,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        assert actual_hash == raw['snapshot_hash']
        audit = json.loads((Path('outputs/research') / ('research_'+run+'_lane_1.json')).read_text())
        a1 = next(r['output'] for r in audit['stages'] if r['stage']=='A1')
        kwargs = dict(minimum_identifiability_score=float(frozen.get('MIN_IDENTIFIABILITY_SCORE') or 60),
            llm_top_n_per_theme=settings.a2_llm_top_n_per_theme,
            review_all_eligible=settings.a2_review_all_eligible,
            rotation_theme_count=int(frozen.get('A2_ROTATION_THEME_COUNT') or 5))
        old = deterministic.screen_a2(frozen, a1, **kwargs)
        factors = frozen['A2_FACTOR_SNAPSHOT']['by_symbol']
        for symbol, row in factors.items():
            evidence = features.stock_trend_structure(frozen['RECENT_DAILY_BARS'].get(symbol, []), date.fromisoformat(day))
            row['stock_trend_structure'] = evidence
            row.setdefault('factors', {})['stock_trend_structure'] = evidence
        new = deterministic.screen_a2(frozen, a1, **kwargs)
        before = {r['symbol']:r for r in old.decisions}
        changes = []
        for row in new.decisions:
            prev = before[row['symbol']]
            if (prev['status'],prev.get('stock_behavior_type')) != (row['status'],row.get('stock_behavior_type')):
                changes.append({'symbol': row['symbol'], 'before': [prev['status'],prev.get('stock_behavior_type')],
                    'after': [row['status'],row.get('stock_behavior_type')],
                    'structure': factors[row['symbol']]['stock_trend_structure'],
                    'previous_medium': prev['behavior_type_decision']['required_facets']['medium_term_trend'],
                    'reasons':row['reason_codes']})
        result={'day':day,'base_snapshot_hash':actual_hash,'candidate_count':len(new.decisions),
            'old_review':len(old.review_symbols),'new_review':len(new.review_symbols),
            'added_review':sorted(set(new.review_symbols)-set(old.review_symbols)),
            'removed_review':sorted(set(old.review_symbols)-set(new.review_symbols)),
            'old_types':dict(Counter(r.get('stock_behavior_type') for r in old.decisions)),
            'new_types':dict(Counter(r.get('stock_behavior_type') for r in new.decisions)),
            'structure_states':dict(Counter(r['stock_trend_structure']['reason_code'] for r in factors.values())),
            'changes':changes,'scope':'DETERMINISTIC_SHADOW_NO_MODEL_NO_PLAN_PUBLICATION'}
        results.append(result)
        print(json.dumps({k:v for k,v in result.items() if k!='changes'},ensure_ascii=False),flush=True)
        del raw,frozen,audit,a1,old,new,before,factors
        gc.collect()
    atomic_write_json(args.output,results)


if __name__ == '__main__':
    main()
