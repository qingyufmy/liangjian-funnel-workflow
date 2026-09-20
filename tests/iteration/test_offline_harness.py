from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "run_iteration_acceptance.py"


def test_iteration_fixtures_are_injected_and_state_is_temporary(
    fake_clock, fake_provider, fake_llm, runtime_store
):
    assert fake_clock.now().isoformat() == "2026-09-18T10:00:00+08:00"
    fake_provider.values["quote"] = {"price": 10.0}
    assert fake_provider.fetch("quote") == {"price": 10.0}
    assert fake_llm.invoke({"symbol": "600000.SH"})["llm_veto"] is True
    assert "pytest" in str(runtime_store.path).lower()


def test_iteration_offline_guard_blocks_network():
    sock = socket.socket()
    with pytest.raises(AssertionError, match="network access is forbidden"):
        sock.connect(("127.0.0.1", 9))


def test_acceptance_entry_help_is_executable():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--profile" in result.stdout


def test_non_offline_profile_never_claims_pass_without_evidence(tmp_path: Path):
    output = tmp_path / "replay"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--profile",
            "replay",
            "--output-dir",
            str(output),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 3
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "PENDING_EVIDENCE"
    assert summary["missing_evidence"]
    assert summary["test_counts"] == {"passed": 0, "failed": 0, "skipped": 0}
