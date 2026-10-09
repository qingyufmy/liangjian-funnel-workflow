"""Promote only a complete reviewed local membership graph, never price facts."""
from datetime import datetime, time
import argparse
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.hithink_board_reference import load_rotation_references
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.settings import load_dotenv


def promote(root: Path, references: Path, *, execute=False, now=None):
    current = now or datetime.now(ZoneInfo('Asia/Shanghai'))
    root, references = root.resolve(), references.resolve()
    binding_path = root / 'config/rotation_reference_bindings_v1.json'
    bindings = json.loads(binding_path.read_text(encoding='utf-8'))
    themes = load_rotation_theme_config(root / 'config/rotation_themes_v1.yaml').active(current.date())
    projected = load_rotation_references(references, bindings, [t.theme_id for t in themes], as_of=current)
    gaps = {k: v['reason_code'] for k, v in projected.items() if not v['available']}
    if not themes or gaps:
        raise ValueError(f'ROTATION_PROMOTION_INCOMPLETE:{sorted(gaps)}')
    report = {'observed_at': current.isoformat(), 'source': 'LOCAL_REFERENCE',
              'complete_themes': len(projected), 'membership_only': True,
              'daily_quote_flow_coverage': 'NOT_PROVEN_BY_MEMBERSHIP_PROMOTION',
              'reference_hashes': {k: v['content_hash'] for k, v in projected.items()},
              'executed': False}
    if not execute:
        return report
    if current.weekday() < 5 and time(9) <= current.time().replace(tzinfo=None) < time(16, 30):
        raise ValueError('ROTATION_PROMOTION_TRADING_WINDOW_PROTECTED')
    env = root / '.env'
    if env.is_symlink() or not env.is_file():
        raise ValueError('ROTATION_PROMOTION_ENV_INVALID')
    before = env.read_bytes()
    values = load_dotenv(env)  # rejects malformed/duplicate keys without logging secrets
    keys = {'LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE': 'LOCAL_REFERENCE',
            'LIANGJIAN_ROTATION_REFERENCE_DIR': str(references),
            'LIANGJIAN_ROTATION_REFERENCE_BINDINGS_PATH': str(binding_path)}
    if any(k in os.environ and os.environ[k] != v for k, v in keys.items()):
        raise ValueError('ROTATION_PROMOTION_ENV_OVERRIDDEN')
    lines = []
    remaining = dict(keys)
    for line in before.decode('utf-8-sig').splitlines():
        key = line.strip().split('=', 1)[0] if '=' in line and not line.lstrip().startswith('#') else None
        if key in remaining:
            lines.append(f'{key}={remaining.pop(key)}')
        else:
            lines.append(line)
    lines.extend(f'{k}={v}' for k, v in remaining.items())
    backup = root / f'.env.rotation-backup-{current.strftime("%Y%m%dT%H%M%S%f")}'
    with backup.open('xb') as stream:
        os.chmod(backup, env.stat().st_mode & 0o777)
        stream.write(before)
    pending = root / f'.env.rotation-pending-{current.strftime("%Y%m%dT%H%M%S%f")}'
    with pending.open('xb') as stream:
        os.chmod(pending, env.stat().st_mode & 0o777)
        stream.write(('\n'.join(lines)+'\n').encode('utf-8'))
    if env.read_bytes() != before:
        pending.unlink()
        raise ValueError('ROTATION_PROMOTION_ENV_CHANGED_CONCURRENTLY')
    os.replace(pending, env)
    assert all(load_dotenv(env).get(k) == v for k, v in keys.items())
    report.update(executed=True, previous_source=values.get('LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE', 'EASTMONEY'),
                  env_before_sha256=hashlib.sha256(before).hexdigest(), backup_name=backup.name)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--references', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    print(json.dumps(promote(args.root, args.references, execute=args.execute), ensure_ascii=False))
