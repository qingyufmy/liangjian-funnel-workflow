#!/bin/bash
set -euo pipefail
TARGET="gui/$(id -u)/com.tonyyu.liangjian-funnel"
if launchctl print "$TARGET" >/dev/null 2>&1; then
  launchctl bootout "$TARGET"
  echo 'LaunchAgent stopped (30s launchd exit grace); all files retained'
else
  echo 'LaunchAgent is not loaded'
fi
