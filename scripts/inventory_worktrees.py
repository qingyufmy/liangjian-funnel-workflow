"""Archive refs and inspect ancestry; never delete, move, merge or push."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

NAMES = ('liangjian_funnel_workflow', 'liangjian_funnel_iteration',
         'liangjian_sources_20260923', 'liangjian_funnel_workflow-a1-snapshot-availability',
         'liangjian_funnel_workflow-a5-coverage-ledger')


def git(root: Path, *args: str, check: bool = True):
    return subprocess.run(['git', '-C', str(root), *args], capture_output=True,
                          text=True, encoding='utf-8', check=check)


def inventory(parent: Path, primary: Path, stamp: str, *, tag: bool = False):
    target = git(primary, 'rev-parse', 'HEAD').stdout.strip()
    results = []
    for name in NAMES:
        root = parent / name
        head = git(root, 'rev-parse', 'HEAD').stdout.strip()
        status = git(root, 'status', '--porcelain=v1', '--untracked-files=all').stdout.splitlines()
        ancestry = git(primary, 'merge-base', '--is-ancestor', head, target, check=False)
        if ancestry.returncode not in (0, 1):
            raise RuntimeError('ANCESTRY_CHECK_FAILED')
        missing = git(primary, 'log', '--format=%H %s', f'{target}..{head}').stdout.splitlines()
        ref = f'archive/{name}-{stamp}'
        previous = git(root, 'rev-parse', '--verify', f'{ref}^{{commit}}', check=False)
        if previous.returncode == 0 and previous.stdout.strip() != head:
            raise RuntimeError(f'ARCHIVE_TAG_CONFLICT:{ref}')
        if tag and previous.returncode != 0:
            git(root, 'tag', '-a', ref, head, '-m',
                f'WP0 preserve {name} at {head}; uncommitted files are NOT archived')
        results.append({'root': str(root.resolve()), 'head': head,
            'branch': git(root, 'branch', '--show-current').stdout.strip(),
            'archive_tag': ref, 'tag_created_or_verified': tag,
            'head_is_ancestor': ancestry.returncode == 0,
            'missing_commits': missing, 'uncommitted_paths': status,
            'deletion_authorized': False,
            'deletion_ready': ancestry.returncode == 0 and not status,
            'ignored_files_preserved': True})
    return {'schema_version': 'wp0-worktree-inventory/1',
        'captured_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        'primary': str(primary.resolve()), 'primary_head': target,
        'mode': 'LOCAL_TAG_ONLY' if tag else 'READ_ONLY', 'worktrees': results,
        'important': 'Tags preserve commits only; dirty and ignored files require separate review. No directory deletion or rename is authorized.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parent', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--primary', type=Path, required=True)
    parser.add_argument('--stamp', default='20261009')
    parser.add_argument('--tag', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    result = inventory(args.parent, args.primary, args.stamp, tag=args.tag)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(json.dumps({'output': str(args.output),
        'worktrees': len(result['worktrees']),
        'not_merged': [r['root'] for r in result['worktrees'] if not r['head_is_ancestor']],
        'dirty': [r['root'] for r in result['worktrees'] if r['uncommitted_paths']]}, ensure_ascii=False))


if __name__ == '__main__':
    main()
