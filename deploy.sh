#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="/www/wwwroot/Agu/liangjian-funnel-workflow"
PROJECT_NAME="量见-A股-工作流"
BOOTSTRAP_UNIT="liangjian-research-g0-bootstrap-20260826.service"
DEPLOY_LOCK="/tmp/liangjian-funnel-node-deploy.lock"

exec 9>"${DEPLOY_LOCK}"
if ! flock -n 9; then
  echo "[deploy] Another Node deployment is already running."
  exit 1
fi

cd "${PROJECT_ROOT}"
bootstrap_pid_before="$(systemctl show "${BOOTSTRAP_UNIT}" --property=MainPID --value 2>/dev/null || true)"
bootstrap_state_before="$(systemctl is-active "${BOOTSTRAP_UNIT}" 2>/dev/null || true)"
echo "[deploy] G0 bootstrap before deploy: state=${bootstrap_state_before:-unknown} pid=${bootstrap_pid_before:-0}"

echo "[deploy] Fetching origin/main..."
runuser -u www -- git fetch origin main
current_commit="$(runuser -u www -- git rev-parse HEAD)"
target_commit="$(runuser -u www -- git rev-parse origin/main)"

if [[ "${bootstrap_state_before}" == "active" && "${current_commit}" != "${target_commit}" ]]; then
  changed_files="$(runuser -u www -- git diff --name-only "${current_commit}..${target_commit}")"
  if grep -Eq '^(src/liangjian_funnel/|config/|prompts/|pyproject\.toml$)' <<<"${changed_files}"; then
    echo "[deploy] Refusing a Python workflow hot-swap while ${BOOTSTRAP_UNIT} is active."
    exit 2
  fi
fi

lock_hash_before="$(sha256sum package-lock.json | awk '{print $1}')"
python_project_hash_before="$(sha256sum pyproject.toml | awk '{print $1}')"
echo "[deploy] Pulling latest Node/UI code..."
runuser -u www -- git pull --ff-only origin main
lock_hash_after="$(sha256sum package-lock.json | awk '{print $1}')"
python_project_hash_after="$(sha256sum pyproject.toml | awk '{print $1}')"

if [[ ! -x .venv/bin/python ]]; then
  echo "[deploy] Python virtual environment is missing."
  exit 6
fi

if [[ "${python_project_hash_before}" != "${python_project_hash_after}" ]]; then
  echo "[deploy] Python project metadata changed; updating virtual environment..."
  runuser -u www -- .venv/bin/python -m pip install ".[dev]"
fi

# The project uses a regular (non-editable) site-packages installation in
# production.  Its version is intentionally stable, so a plain `pip install .`
# may leave an older wheel installed after a source-only Git update.  Always
# rebuild and replace the local package to keep scheduled Python jobs on the
# exact same source as the checked-out commit.
echo "[deploy] Reinstalling current Python source package..."
runuser -u www -- .venv/bin/python -m pip install \
  --no-build-isolation \
  --no-deps \
  --force-reinstall \
  .

if [[ ! -d node_modules || "${lock_hash_before}" != "${lock_hash_after}" ]]; then
  echo "[deploy] Installing Node dependencies..."
  runuser -u www -- npm install --include=dev
else
  echo "[deploy] package-lock.json unchanged; reusing installed dependencies."
fi

echo "[deploy] Building production assets..."
runuser -u www -- npm run build

baota_action() {
  local action="$1"
  cd /www/server/panel
  PROJECT_NAME="${PROJECT_NAME}" BAOTA_ACTION="${action}" python3 - <<'PY'
import json
import os
import sys

sys.path.insert(0, "/www/server/panel/class")
from projectModel.nodejsModel import main as nodejsModel


class FakeGet(dict):
    def __getattr__(self, name):
        return self.get(name)

    def __setattr__(self, name, value):
        self[name] = value


model = nodejsModel()
method = model.restart_project if os.environ["BAOTA_ACTION"] == "restart" else model.start_project
result = method(FakeGet(project_name=os.environ["PROJECT_NAME"]))
print(json.dumps(result, ensure_ascii=False))
if not isinstance(result, dict) or not result.get("status"):
    raise SystemExit(1)
PY
}

wait_for_health() {
  for _ in $(seq 1 20); do
    if curl --fail --silent --show-error http://127.0.0.1:3210/api/health >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "[deploy] Restarting BaoTa Node project only..."
baota_action restart

# BaoTa restarts asynchronously. The previous process can remain healthy for
# several seconds, so let the handoff settle before probing the replacement.
sleep 12

echo "[deploy] Verifying Node health..."
if ! wait_for_health; then
  echo "[deploy] Restart did not produce a healthy replacement; retrying BaoTa start once..."
  baota_action start
  if ! wait_for_health; then
    echo "[deploy] Node recovery start failed."
    exit 3
  fi
fi
sleep 5
if ! curl --fail --silent --show-error http://127.0.0.1:3210/api/health >/dev/null; then
  echo "[deploy] Node exited after the initial health check; retrying BaoTa start once..."
  baota_action start
  if ! wait_for_health; then
    echo "[deploy] Node recovery start failed."
    exit 4
  fi
  sleep 5
  if ! curl --fail --silent --show-error http://127.0.0.1:3210/api/health >/dev/null; then
    echo "[deploy] Node did not remain healthy after recovery start."
    exit 5
  fi
fi

cd "${PROJECT_ROOT}"
deployed_commit="$(runuser -u www -- git rev-parse HEAD)"
bootstrap_pid_after="$(systemctl show "${BOOTSTRAP_UNIT}" --property=MainPID --value 2>/dev/null || true)"
bootstrap_state_after="$(systemctl is-active "${BOOTSTRAP_UNIT}" 2>/dev/null || true)"
echo "[deploy] Deployed commit: ${deployed_commit}"
echo "[deploy] G0 bootstrap after deploy: state=${bootstrap_state_after:-unknown} pid=${bootstrap_pid_after:-0}"
if [[ "${bootstrap_state_before}" == "active" && "${bootstrap_state_after}" == "active" && "${bootstrap_pid_before}" != "${bootstrap_pid_after}" ]]; then
  echo "[deploy] WARNING: bootstrap PID changed outside this script."
fi
echo "[deploy] Done."
