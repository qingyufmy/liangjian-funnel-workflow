#!/usr/bin/env python3
"""One-shot resumable sync; stop by 17:50, never compete with the 18:00 A1."""
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

root = Path(__file__).resolve().parents[2]
now = datetime.now(ZoneInfo('Asia/Shanghai'))
deadline = now.replace(hour=17, minute=50, second=0, microsecond=0)
if now >= deadline:
    raise SystemExit('Before-A1 sync window closed; resume sync-data after A1 completes')
stamp = now.strftime('%Y%m%dT%H%M%S')
log = root / 'outputs' / f'task1-sync-data-{stamp}.log'
receipt = root / 'outputs' / f'task1-sync-supervisor-{stamp}.json'
env = {**os.environ, 'TZ': 'Asia/Shanghai', 'LIANGJIAN_BROKER': 'local_paper',
       'LIANGJIAN_WORKFLOW_PROGRESS_PATH': str(root / 'outputs/task1-sync-progress.json')}
with log.open('x') as stream:
    child = subprocess.Popen([str(root / '.venv/bin/python'), '-m', 'liangjian_funnel', 'sync-data'],
                             cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    reason = 'PROCESS_EXIT'
    try:
        code = child.wait(timeout=max(1, (deadline-now).total_seconds()))
    except subprocess.TimeoutExpired:
        reason = 'STOPPED_BEFORE_A1_WINDOW_RESUMABLE'
        child.terminate()
        try:
            code = child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            child.kill()
            code = child.wait()
with receipt.open('x') as stream:
    json.dump(dict(started_at=now.isoformat(), ended_at=datetime.now(now.tzinfo).isoformat(),
                   exit_code=code, reason=reason, log=str(log)), stream, indent=2)
