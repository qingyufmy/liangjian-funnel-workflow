"""Execute extracted budget guards with fake commands, never deploy anything."""
from pathlib import Path
import shutil
import subprocess

import pytest

SOURCE = (Path(__file__).parents[1] / 'deploy.sh').read_text(encoding='utf-8')
PREFIX = SOURCE.split('# Keep the whole pre-open/intraday window', 1)[0]


def shell(body):
    bash = shutil.which('bash')
    if not bash:
        pytest.skip('bash not installed')
    return subprocess.run([bash, '-c', PREFIX + '\n' + body], capture_output=True,
                          text=True, timeout=10)


@pytest.mark.parametrize('elapsed,expected', [(0,1200),(420,1200),(421,1199),
                                            (1500,120),(1619,1)])
def test_remaining_budget_is_single_total_minus_health_reserve(elapsed,expected):
    result=shell(f'SECONDS=$((deployment_started_seconds+{elapsed}))\ndeployment_integrity_timeout_seconds')
    assert result.returncode == 0, result.stderr
    assert int(result.stdout) == expected


@pytest.mark.parametrize('elapsed', [1620,1621,1799,1800,1801])
def test_insufficient_budget_refuses_integrity_before_any_stop(elapsed):
    result=shell(f'SECONDS=$((deployment_started_seconds+{elapsed}))\ndeployment_integrity_timeout_seconds\necho MUST_NOT_CONTINUE')
    assert result.returncode == 9
    assert 'MUST_NOT_CONTINUE' not in result.stdout


@pytest.mark.parametrize('elapsed,expected', [(1620,0),(1621,9),(1800,9)])
def test_pre_stop_rechecks_actual_remaining_after_integrity(elapsed,expected):
    result=shell(f'SECONDS=$((deployment_started_seconds+{elapsed}))\nassert_deployment_start_budget')
    assert result.returncode == expected


def test_actual_verify_block_passes_one_shared_budget_and_rechecks_before_stop():
    block=SOURCE.split('integrity_timeout_seconds="$(deployment_integrity_timeout_seconds)"',1)[1]
    block='integrity_timeout_seconds="$(deployment_integrity_timeout_seconds)"'+block.split('# BaoTa stop is asynchronous.',1)[0]
    result=shell('''
runuser() { printf 'CHILD_ARGS:%s\n' "$*"; SECONDS=$((deployment_started_seconds+1621)); }
pgrep() { printf '123\n'; }
baota_action() { echo MUST_NOT_STOP; }
''' + block)
    assert result.returncode == 9, result.stdout+result.stderr
    assert 'verify-runtime-integrity --timeout-seconds 1200' in result.stdout
    assert result.stdout.count('CHILD_ARGS:') == 1
    assert 'MUST_NOT_STOP' not in result.stdout


def test_expired_script_cannot_print_done_and_existing_window_and_scans_remain():
    final=SOURCE[SOURCE.rindex('deployment_remaining_seconds >/dev/null'):]
    result=shell('SECONDS=$((deployment_started_seconds+1800))\n'+final)
    assert result.returncode == 9
    assert '[deploy] Done.' not in result.stdout
    assert SOURCE.count('-m liangjian_funnel verify-runtime-integrity ') == 1
    assert SOURCE.index('assert_deployment_window "$@"') < SOURCE.index('git pull --ff-only')
    assert SOURCE.index('assert_deployment_start_budget\necho') < SOURCE.index('baota_action stop')
