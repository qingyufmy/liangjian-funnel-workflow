#!/usr/bin/env bash
set -euo pipefail
test_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_python="${LIANGJIAN_TEST_PYTHON:-${test_root}/.venv/bin/python}"
exec "${test_python}" "${test_root}/scripts/full_test_baseline.py" --python "${test_python}" "$@"
