"""Index existing frozen A2/A3 set and field diffs; no model or publication."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'artifacts/team-v2-20261016'


def read(path):
    raw = path.read_bytes()
    return json.loads(raw), dict(path=path.relative_to(ROOT).as_posix(),
                                sha256=hashlib.sha256(raw).hexdigest())


def sets(before, after):
    if not isinstance(before, list) or not isinstance(after, list):
        raise ValueError('ORIGINAL_AND_NEW_SET_REQUIRED')
    if any(not isinstance(item, str) for item in before + after):
        raise ValueError('SYMBOL_SET_INVALID')
    if len(set(before)) != len(before) or len(set(after)) != len(after):
        raise ValueError('DUPLICATE_SYMBOL_NOT_SILENTLY_DEDUPED')
    left, right = set(before), set(after)
    return dict(original_count=len(left), new_count=len(right),
                retained=sorted(left & right), added=sorted(right - left),
                removed=sorted(left - right))


def index(name):
    directory = BASE / name
    result, proof = read(directory / 'RESULT.json')
    diff, diff_proof = read(directory / 'FIELD_DIFF.json')
    fields = diff['fields']
    if not isinstance(fields, list):
        raise ValueError('FIELD_DIFF_INVALID')
    return result, dict(result=proof, field_diff=diff_proof,
        field_count=len(fields), kinds=dict(Counter(row['kind'] for row in fields)),
        root_fields=dict(Counter(row['path'].split('.')[1].split('[')[0]
            if row['path'].startswith('$.') else '$' for row in fields)),
        original_basis={key: value for key, value in diff.items() if key != 'fields'},
        actual_finished_at=result.get('actual_finished_at'), status=result['status'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or not output.is_relative_to(BASE.resolve()):
        raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    inheritance, inheritance_proof = read(BASE / 'author315-gates-20261017T1534/GATES.json')
    if inheritance['a3_plan_review']['inheritance_allowed_by_0315'] is not True:
        raise ValueError('A3_INHERITANCE_NOT_PROVEN')
    manifest, manifest_proof = read(ROOT / 'prompts/team_v2/manifest.json')
    prompt_proofs = {}
    for filename in ('agent_2_theme_sentiment_v3.txt', 'agent_3_portfolio_planner_v4.txt'):
        raw = (ROOT / 'prompts/team_v2' / filename).read_bytes()
        proof = dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        if proof != manifest['files'][filename]:
            raise ValueError('PROMPT_MANIFEST_MISMATCH')
        prompt_proofs[filename] = proof
    a2, a3 = {}, {}
    for day in ('1014', '1015', '1016'):
        name = 'a2-author305-' + ('feedback308-' if day == '1016' else '') + day + '-real'
        result, item = index(name)
        item['pools'] = {pool: sets(result['original_pools'][pool], result['new_pools'][pool])
            for pool in ('focus_pool', 'watch_only_pool', 'rejected_candidates', 'model_nominations')}
        pools = ('focus_pool', 'watch_only_pool', 'rejected_candidates')
        original_symbols = sorted({symbol for pool in pools for symbol in result['original_pools'][pool]})
        new_symbols = sorted({symbol for pool in pools for symbol in result['new_pools'][pool]})
        item['explicit_pool_coverage'] = sets(original_symbols, new_symbols)
        item['coverage_note'] = 'ABSENT_FROM_ALL_EXPLICIT_POOLS_IS_NOT_AN_EXPLICIT_MODEL_REJECT_OR_STRATEGY_FAILURE'
        a2[day] = item
        name = ('a3-author406-1016-inherit-confirm-real' if day == '1016'
                else 'a3-author403-' + day + '-real')
        result, item = index(name)
        item['pass_set'] = sets(result['original_core'], result['new_core'])
        before, after = item['pass_set']['original_count'], item['pass_set']['new_count']
        item['pass_count_decline_gt20pct'] = (before - after) * 5 > before
        if item['pass_count_decline_gt20pct'] != result['pass_count_decline_gt20pct']:
            raise ValueError('A3_DECLINE_RECEIPT_MISMATCH')
        item['original_upstream'] = result['original_upstream']
        item['scope'] = 'PLAN_REVIEW_ONLY_0315_0319_SECTION_INHERITANCE_NOT_COMPOSED_NEW_A1_A2_CHAIN'
        a3[day] = item
    body = dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        prompt_manifest=manifest_proof, prompt_proofs=prompt_proofs,
        inheritance=inheritance_proof, a2=a2, a3=a3,
        interpretation='SET_AND_FIELD_DIFF_INDEX_NOT_STRATEGY_OR_RELEASE_ACCEPTANCE',
        model_calls=0, production_writes=0, release_approved=False)
    output.mkdir()
    with (output / 'DIFF_INDEX.json').open('x', encoding='utf-8') as handle:
        json.dump(body, handle, ensure_ascii=False, indent=2)
    print(json.dumps(dict(path=str(output / 'DIFF_INDEX.json'),
        sha256=hashlib.sha256((output / 'DIFF_INDEX.json').read_bytes()).hexdigest(),
        a2={day: {pool: [row['original_count'], row['new_count']]
            for pool, row in item['pools'].items()} for day, item in a2.items()},
        a3={day: item['pass_set'] for day, item in a3.items()}, model_calls=0)))


if __name__ == '__main__':
    main()
