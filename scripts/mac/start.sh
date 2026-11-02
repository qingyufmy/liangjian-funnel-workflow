#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LABEL=com.tonyyu.liangjian-funnel
DOMAIN="gui/$(id -u)"
[[ "${LIANGJIAN_VM_STOP_CONFIRMED:-}" == true ]] || { echo 'Confirm old scheduler is stopped: LIANGJIAN_VM_STOP_CONFIRMED=true'; exit 1; }
[[ "${LIANGJIAN_RELEASE_CONFIRMED:-}" == true ]] || { echo 'Production revision requires explicit release confirmation (T1 M5); scheduler remains disabled'; exit 1; }
[[ -f "$ROOT/dist/server/index.js" && -f "$ROOT/dist/web/index.html" ]]
if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
  echo 'LaunchAgent already loaded; no second instance started'; exit 0
fi
if /usr/sbin/lsof -nP -iTCP:3210 -sTCP:LISTEN >/dev/null 2>&1; then
  echo 'Port 3210 already occupied; refusing a second instance'; exit 1
fi
"$ROOT/.venv/bin/python" - "$ROOT" <<'PY'
import os, plistlib, sys
from pathlib import Path
root=Path(sys.argv[1]); home=Path.home()
template=root/'scripts/mac/com.tonyyu.liangjian-funnel.plist'
body=template.read_text().replace('__ROOT__',str(root))
plistlib.loads(body.encode())
target=home/'Library/LaunchAgents/com.tonyyu.liangjian-funnel.plist'
target.parent.mkdir(parents=True,exist_ok=True)
if target.exists():
    if target.read_text()!=body: raise SystemExit('Existing LaunchAgent differs; preserved for operator review')
else:
    with target.open('x') as f: f.write(body)
    os.chmod(target,0o600)
PY
launchctl enable "$DOMAIN/$LABEL"
launchctl bootstrap "$DOMAIN" "$HOME/Library/LaunchAgents/$LABEL.plist"
echo 'LaunchAgent loaded; check outputs/scheduler and /api/health'
