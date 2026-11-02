#!/usr/bin/env python3
"""launchd entry point; secrets stay in the owner-readable .env."""
import os
from pathlib import Path
from liangjian_funnel.settings import Settings, load_dotenv

root = Path(__file__).resolve().parents[2]
os.chdir(root)
env = {**os.environ, **load_dotenv(root / '.env')}
env.update(NODE_ENV='production', HOST='127.0.0.1', PORT='3210',
           TZ='Asia/Shanghai', LIANGJIAN_SCHEDULER_ENABLED='true',
           LIANGJIAN_PYTHON_BIN=str(root / '.venv/bin/python'),
           LIANGJIAN_WEB_DIST=str(root / 'dist/web'),
           LIANGJIAN_BROKER='local_paper')
settings = Settings.from_env(env, root=root)
if settings.broker_adapter != 'local_paper' or not env.get('LIANGJIAN_DASHBOARD_TOKEN'):
    raise SystemExit('Local paper adapter and dashboard token required')
node = Path.home() / '.nvm/versions/node/v22.23.4/bin/node'
env['PATH'] = f"{node.parent}:{root / '.venv/bin'}:/usr/bin:/bin:/usr/sbin:/sbin"
os.execve('/usr/bin/caffeinate', ['caffeinate', '-is', str(node), 'dist/server/index.js'], env)
