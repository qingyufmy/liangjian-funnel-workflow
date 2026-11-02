#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
export PATH="$HOME/.nvm/versions/node/v22.23.4/bin:$HOME/.local/bin:$PATH"
[[ "$(node -p 'process.versions.node.split(".")[0]')" == 22 ]]
[[ -f .env ]] || { echo 'Existing .env required; refusing to create it'; exit 1; }
chmod 600 .env
mkdir -p outputs/node outputs/scheduler state storage cache
if [[ ! -x .venv/bin/python ]]; then
  [[ ! -e .venv ]] || { echo 'Existing incomplete .venv preserved'; exit 1; }
  uv venv -p 3.11 .venv
fi
.venv/bin/python -c 'import sys; assert sys.version_info >= (3,11)'
uv pip install -p .venv/bin/python -e '.[dev]'
# Preserve an existing install; npm ci is only used for an absent directory.
if [[ ! -d node_modules ]]; then npm ci; fi
npm run typecheck
# Build is an explicitly requested regeneration of dist, never a data cleanup.
npm run build
echo 'Environment built. start.sh requires LIANGJIAN_VM_STOP_CONFIRMED=true and LIANGJIAN_RELEASE_CONFIRMED=true after the production revision is released (T1 M5).'
