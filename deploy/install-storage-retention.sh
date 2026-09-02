#!/usr/bin/env bash
set -euo pipefail

project_root="/www/wwwroot/Agu/liangjian-funnel-workflow"
project_root="$(realpath "$project_root")"
[[ "$project_root" == "/www/wwwroot/Agu/liangjian-funnel-workflow" ]]

install -o root -g root -m 0755 \
  "$project_root/scripts/run_storage_retention.sh" \
  /usr/local/sbin/liangjian-storage-retention
install -o root -g root -m 0644 \
  "$project_root/deploy/liangjian-storage-retention.service" \
  /etc/systemd/system/liangjian-storage-retention.service
install -o root -g root -m 0644 \
  "$project_root/deploy/liangjian-storage-retention.timer" \
  /etc/systemd/system/liangjian-storage-retention.timer

systemctl daemon-reload
systemctl enable --now liangjian-storage-retention.timer
systemctl is-enabled liangjian-storage-retention.timer
systemctl list-timers liangjian-storage-retention.timer --no-pager

