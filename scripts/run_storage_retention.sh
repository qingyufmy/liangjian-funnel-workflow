#!/usr/bin/env bash
set -euo pipefail

project_root="${LIANGJIAN_PROJECT_ROOT:-/www/wwwroot/Agu/liangjian-funnel-workflow}"
cutoff_hours="${LIANGJIAN_RETENTION_CUTOFF_HOURS:-72}"
policy="${LIANGJIAN_RETENTION_POLICY:-scheduled-gzip-v1}"

project_root="$(realpath "$project_root")"
[[ "$project_root" == "/www/wwwroot/Agu/liangjian-funnel-workflow" ]]

# Retention is maintenance, never part of the research critical path.  Skip a
# busy window and let the persistent timer try again the next day.
if pgrep -f 'python.*-m liangjian_funnel (run-a1-maintenance|run-research|run-close|run-next-session-prep)' >/dev/null; then
  echo '{"status":"SKIPPED","reason_code":"RESEARCH_TASK_ACTIVE"}'
  exit 0
fi

cd "$project_root"
stamp="$(date +%Y%m%dT%H%M%S%z)"
plan_path="state/storage-retention-plan-${stamp}.json"
plan_log="outputs/node/storage-retention-plan-${stamp}.log"
execute_log="outputs/node/storage-retention-execute-${stamp}.log"
python_bin=".venv/bin/python"

install -o www -g www -m 0640 /dev/null "$plan_log"
install -o www -g www -m 0640 /dev/null "$execute_log"

runuser -u www -- "$python_bin" -m liangjian_funnel storage-cleanup \
  --root "$project_root" \
  --policy "$policy" \
  --cutoff-hours "$cutoff_hours" \
  --manifest "$plan_path" >"$plan_log"

plan_id="$(runuser -u www -- "$python_bin" -c \
  'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["plan_id"])' \
  "$plan_path")"

runuser -u www -- "$python_bin" -m liangjian_funnel storage-cleanup \
  --execute \
  --root "$project_root" \
  --policy "$policy" \
  --manifest "$plan_path" \
  --confirm-token "$plan_id" >"$execute_log"

runuser -u www -- "$python_bin" -c \
  'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8")); print(json.dumps({"status":p.get("status"),"plan_id":p.get("plan_id"),"archive_count":p.get("archive_count"),"raw_bytes":sum(int(x.get("raw_size_bytes",0)) for x in p.get("items",[])),"compressed_bytes":sum(int(x.get("compressed_size_bytes",0)) for x in p.get("items",[]))}, ensure_ascii=False))' \
  "$execute_log"

