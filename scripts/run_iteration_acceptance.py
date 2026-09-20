#!/usr/bin/env python3
"""Run isolated iteration acceptance checks and emit machine-readable evidence.

The offline profile is intentionally small enough for a development loop.  It
does not replace the repository CI suite; the baseline/full-suite evidence is
recorded separately in docs/iteration/ITERATION_STATE.json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import yaml


EXIT_PASS = 0
EXIT_FAILURE = 1
EXIT_ARGUMENT = 2
EXIT_MISSING_EVIDENCE = 3
EXIT_SAFETY_BLOCK = 4

REPO_ROOT = Path(__file__).resolve().parents[1]
PROTECTED_OUTPUT_ROOTS = {
    (REPO_ROOT / name).resolve()
    for name in ("state", "storage", "outputs", "cache", "config")
}


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: str
    detail: str
    command: list[str] | None = None
    exit_code: int | None = None
    passed: int | None = None
    failed: int | None = None
    skipped: int | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "UNAVAILABLE"


def _safe_output_dir(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = REPO_ROOT / path
    resolved = path.resolve()
    if resolved == REPO_ROOT.resolve():
        raise RuntimeError("output directory cannot be the repository root")
    if any(resolved == root or root in resolved.parents for root in PROTECTED_OUTPUT_ROOTS):
        raise PermissionError(f"protected output directory: {resolved}")
    return resolved


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected mapping in {path}")
    return value


def _config_checks() -> list[CheckResult]:
    checks: list[CheckResult] = []
    try:
        runtime = _load_yaml(REPO_ROOT / "config" / "runtime.yaml")
        exchange = _load_yaml(REPO_ROOT / "config" / "exchange_rules.yaml")
        funnel = _load_yaml(REPO_ROOT / "config" / "funnel_config_v2.yaml")
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return [CheckResult("S00-CONFIG-LOAD", "FAIL", f"{type(exc).__name__}: {exc}")]

    permissions = runtime.get("permissions") or {}
    runtime_contract = funnel.get("runtime") or {}
    assertions = {
        "S00-SAFE-01": (
            permissions.get("external_orders") is False
            and permissions.get("live_trading") is False,
            "runtime external orders and live trading are disabled",
        ),
        "S00-SAFE-02": (
            exchange.get("simulation_only") is True
            and exchange.get("external_orders") is False,
            "exchange rules are simulation-only",
        ),
        "S00-SAFE-03": (
            runtime_contract.get("simulation_only") is True
            and runtime_contract.get("order_permission") == "DISABLED"
            and runtime_contract.get("mode") == "SHADOW",
            "funnel runtime is shadow-only with order permission disabled",
        ),
    }
    for check_id, (passed, detail) in assertions.items():
        checks.append(CheckResult(check_id, "PASS" if passed else "FAIL", detail))
    return checks


def _parse_pytest_counts(output: str) -> tuple[int | None, int | None, int | None]:
    passed = re.search(r"(\d+) passed", output)
    failed = re.search(r"(\d+) failed", output)
    skipped = re.search(r"(\d+) skipped", output)
    return (
        int(passed.group(1)) if passed else None,
        int(failed.group(1)) if failed else 0,
        int(skipped.group(1)) if skipped else 0,
    )


def _run_command(
    check_id: str,
    command: Sequence[str],
    output_dir: Path,
    env: dict[str, str],
) -> CheckResult:
    started = _utc_now()
    completed = subprocess.run(
        list(command),
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    combined = completed.stdout + completed.stderr
    (output_dir / "test-results").mkdir(parents=True, exist_ok=True)
    (output_dir / "test-results" / f"{check_id}.log").write_text(combined, encoding="utf-8")
    with (output_dir / "commands.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "check_id": check_id,
                    "command": list(command),
                    "started_at": started,
                    "finished_at": _utc_now(),
                    "exit_code": completed.returncode,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
    passed, failed, skipped = _parse_pytest_counts(combined)
    return CheckResult(
        check_id=check_id,
        status="PASS" if completed.returncode == 0 else "FAIL",
        detail=f"log=test-results/{check_id}.log",
        command=list(command),
        exit_code=completed.returncode,
        passed=passed,
        failed=failed,
        skipped=skipped,
    )


def _offline_environment() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    # Acceptance must never inherit a working model or notification credential.
    for key in tuple(env):
        upper = key.upper()
        if any(token in upper for token in ("WEBHOOK", "API_KEY", "ACCESS_TOKEN", "SECRET_KEY")):
            env.pop(key, None)
    env.update(
        {
            "LIANGJIAN_ITERATION_OFFLINE": "1",
            "LIANGJIAN_DISABLE_NETWORK": "1",
            "LIANGJIAN_DISABLE_NOTIFICATIONS": "1",
            "LIANGJIAN_EXTERNAL_ORDERS": "0",
        }
    )
    return env


def _write_summary(
    *,
    output_dir: Path,
    profile: str,
    started_at: str,
    status: str,
    exit_code: int,
    checks: list[CheckResult],
    missing_evidence: list[str],
) -> None:
    requirement_map = {
        "S00-SAFE": ["S00-SAFE-01", "S00-SAFE-02", "S00-SAFE-03"],
        "S00-OFFLINE": ["S00-OFFLINE-TESTS"],
        "OBS-01": ["tests/iteration/test_decision_observability.py::test_external_failure_has_independent_job_data_opportunity_and_eligibility_axes"],
        "OBS-02": ["tests/iteration/test_decision_observability.py::test_noncritical_degradation_remains_visible_without_blocking_trade"],
        "OBS-03": ["tests/iteration/test_decision_observability.py::test_observability_redaction_removes_secret_and_url_query_values"],
        "OBS-04": ["tests/iteration/test_decision_observability.py::test_observation_hash_excludes_wall_clock_and_timing_but_tracks_frozen_input"],
        "OBS-05": ["tests/iteration/test_decision_observability.py::test_timing_percentiles_are_measured_from_spans_and_missing_is_explicit"],
        "SRC-01": ["tests/iteration/test_provider_governance.py::test_src_01_fifty_concurrent_identical_requests_are_singleflight"],
        "SRC-02": ["tests/iteration/test_provider_governance.py::test_src_02_quota_and_cooldown_survive_restart_and_share_scope"],
        "SRC-03": ["tests/iteration/test_provider_governance.py::test_src_03_rate_limit_timeout_then_recovery_has_bounded_attempts"],
        "SRC-04": ["tests/iteration/test_provider_governance.py::test_src_04_terminal_source_states_are_distinct_and_not_retried"],
        "SRC-05": ["tests/iteration/test_provider_governance.py::test_src_05_bad_response_keeps_last_good_and_stale_is_not_tradable"],
        "SRC-06": ["tests/iteration/test_provider_governance.py::test_src_06_fallback_requires_semantic_match_and_independent_upstream"],
        "SRC-07": ["tests/iteration/test_provider_governance.py::test_src_07_total_deadline_bounds_nonresponsive_adapter"],
        "SRC-08": ["tests/iteration/test_provider_governance.py::test_src_08_expired_lease_recovers_and_stale_owner_cannot_publish"],
    }
    counted = [item for item in checks if item.passed is not None]
    test_counts = {
        "passed": sum(item.passed or 0 for item in counted),
        "failed": sum(item.failed or 0 for item in counted),
        "skipped": sum(item.skipped or 0 for item in counted),
    }
    summary = {
        "schema_version": "liangjian-iteration-acceptance/1.0.0",
        "profile": profile,
        "status": status,
        "exit_code": exit_code,
        "baseline_commit": _git("rev-parse", "HEAD"),
        "test_commit": _git("rev-parse", "HEAD"),
        "worktree_status": _git("status", "--short"),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "offline": profile == "offline",
        },
        "input_hashes": {
            str(path.relative_to(REPO_ROOT)): _sha256(path)
            for path in (
                REPO_ROOT / "config" / "runtime.yaml",
                REPO_ROOT / "config" / "exchange_rules.yaml",
                REPO_ROOT / "config" / "funnel_config_v2.yaml",
                REPO_ROOT / "config" / "capability_specs.yaml",
                REPO_ROOT / "docs" / "iteration" / "LIANGJIAN_CODEX_IMPLEMENTATION_MASTER.md",
            )
            if path.exists()
        },
        "started_at": started_at,
        "finished_at": _utc_now(),
        "artifact_dir": str(output_dir),
        "checks": [asdict(item) for item in checks],
        "test_counts": test_counts,
        "requirements_to_tests": requirement_map,
        "failed_items": [item.check_id for item in checks if item.status == "FAIL"],
        "missing_evidence": missing_evidence,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "liangjian-iteration-artifact/1.0.0",
        "summary_sha256": _sha256(output_dir / "summary.json"),
        "files": sorted(
            str(path.relative_to(output_dir)).replace("\\", "/")
            for path in output_dir.rglob("*")
            if path.is_file()
        ),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run isolated Liangjian iteration acceptance checks.")
    parser.add_argument(
        "--profile",
        choices=("offline", "replay", "stress", "shadow-report"),
        required=True,
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest", help="Read-only replay/shadow evidence manifest.")
    parser.add_argument("--scenario", help="Named stress scenario.")
    parser.add_argument(
        "--inject-failure",
        action="store_true",
        help="Append a controlled failing check to validate failure propagation.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        output_dir = _safe_output_dir(args.output_dir)
    except PermissionError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_SAFETY_BLOCK
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_ARGUMENT

    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now()
    checks = _config_checks()
    missing_evidence: list[str] = []

    if args.profile != "offline":
        if args.profile in {"replay", "shadow-report"} and not args.manifest:
            missing_evidence.append("readonly manifest is required")
        if args.profile == "stress" and not args.scenario:
            missing_evidence.append("stress scenario is required")
        if not missing_evidence:
            missing_evidence.append(f"{args.profile} profile is not implemented before its dependent stage")
        _write_summary(
            output_dir=output_dir,
            profile=args.profile,
            started_at=started_at,
            status="PENDING_EVIDENCE",
            exit_code=EXIT_MISSING_EVIDENCE,
            checks=checks,
            missing_evidence=missing_evidence,
        )
        return EXIT_MISSING_EVIDENCE

    command = [
        sys.executable,
        "-m",
        "pytest",
        "tests/iteration",
        "tests/test_runtime_calendar.py",
        "tests/test_runtime_scheduler.py",
        "tests/test_runtime_simulation.py",
        "tests/test_a1_packet.py",
        "tests/test_a1_registry.py",
    ]
    checks.append(_run_command("S00-OFFLINE-TESTS", command, output_dir, _offline_environment()))
    if args.inject_failure:
        checks.append(
            _run_command(
                "S00-INJECTED-FAILURE",
                [sys.executable, "-c", "import sys; sys.exit(1)"],
                output_dir,
                _offline_environment(),
            )
        )

    failed = [item.check_id for item in checks if item.status == "FAIL"]
    exit_code = EXIT_FAILURE if failed else EXIT_PASS
    _write_summary(
        output_dir=output_dir,
        profile=args.profile,
        started_at=started_at,
        status="FAIL" if failed else "CODE_ACCEPTED",
        exit_code=exit_code,
        checks=checks,
        missing_evidence=missing_evidence,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
