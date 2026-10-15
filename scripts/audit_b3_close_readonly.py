"""Date-bound B3 close observer; reuse pinned RO probe without executing old driver."""
from __future__ import annotations

import argparse
import ast
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo


def remote_code(root: Path) -> str:
    original = root / 'artifacts/close-observation-20261012/audit_close_stage_readonly.py'
    raw = original.read_bytes()
    if hashlib.sha256(raw).hexdigest() != 'f70102f353122d2e1946ff93a74a6df5165434c29b70464e538091ea51e201ac':
        raise ValueError('ORIGINAL_READONLY_PROBE_CHANGED')
    tree = ast.parse(raw.decode('utf-8-sig'))
    matches = [node.value for node in tree.body if isinstance(node, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == 'CODE' for t in node.targets)]
    if len(matches) != 1:
        raise ValueError('ORIGINAL_PROBE_CODE_NOT_UNIQUE')
    code = ast.literal_eval(matches[0]).replace('2026-10-12', '2026-10-15')
    code = code.replace("c=sqlite3.connect(", "assert now.date().isoformat()=='2026-10-15' and (now.hour,now.minute)>=(15,10)\nassert subprocess.check_output(['hostname'],text=True).strip()=='debian'\nassert pathlib.Path.cwd()==r\nc=sqlite3.connect(", 1)
    # A stale global progress file is evidence, not this run's budget.
    code = code.replace("if parent and isinstance(budget,(float,int)):",
        "parent_matches_node=bool(parent and payload.get('job')=='close' and str(payload.get('run_id','')).startswith('2026-10-15-close') and any(x.get('job')=='close' and abs((datetime.fromisoformat(x['startedAt'].replace('Z','+00:00'))-datetime.fromisoformat(parent)).total_seconds())<1 for x in jobs))\nif parent_matches_node and isinstance(budget,(float,int)):")
    code = code.replace("'historical_log_reconstruction_not_contemporaneous_poll':True",
        "'historical_log_reconstruction_not_contemporaneous_poll':False,'budget_parent_matches_node':parent_matches_node")
    compile(code, '<B3 date-bound remote RO probe>', 'exec')
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--check', action='store_true', help='Compile locally; no SSH or files written.')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    code = remote_code(root)
    if args.check:
        assert '2026-10-12' not in code
        print('LOCAL_CHECK_OK; SSH_CALLS=0; PRODUCTION_WRITES=0')
        return 0
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    if now.date().isoformat() != '2026-10-15' or (now.hour, now.minute) < (15, 10):
        parser.error('OUTSIDE_AUTHORIZED_20261015_CLOSE_OBSERVATION_WINDOW')
    if args.output is None:
        parser.error('--output required for a new evidence directory')
    args.output.mkdir(parents=True, exist_ok=False)
    cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
           '-o', 'ConnectTimeout=8', 'aurum-vm',
           'cd /www/wwwroot/Agu/liangjian-funnel-workflow && .venv/bin/python -B -']
    result = subprocess.run(cmd, input=code.encode(), capture_output=True, timeout=30)
    for name, value in [('stdout.log', result.stdout), ('stderr.log', result.stderr)]:
        (args.output / name).write_bytes(value)
    (args.output / 'COMMAND.json').write_text(json.dumps({
        'command': cmd, 'exit_code': result.returncode, 'actual_local_at': now.isoformat(),
        'remote_code_sha256': hashlib.sha256(code.encode()).hexdigest(),
        'production_writes': 0, 'new_model_calls': 0}, indent=2), encoding='utf-8')
    if result.returncode:
        print(f'READONLY_PROBE_FAILED:{result.returncode}; see stderr.log')
        return result.returncode
    observation = json.loads(result.stdout)
    (args.output / 'CLOSE_STAGE_READONLY.json').write_text(
        json.dumps(observation, ensure_ascii=False, indent=2), encoding='utf-8')
    progress = observation.get('progress', {}).get('payload', {})
    print(json.dumps({key: observation.get(key) for key in (
        'captured_at', 'head', 'jobs', 'budget_parent_matches_node', 'remaining_global_minutes',
        'receipt_hash_mismatch_log_count')}, ensure_ascii=False))
    print(json.dumps({'progress_run_id': progress.get('run_id'),
                      'phase': progress.get('phase'), 'output': str(args.output)}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
