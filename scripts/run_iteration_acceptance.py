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
sys.path.insert(0, str(REPO_ROOT / "src"))

from liangjian_funnel.runtime.iteration_acceptance import (  # noqa: E402
    AcceptanceContractError,
    validate_evidence_package,
)

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
        "A4-01": ["tests/iteration/test_a4_orchestration.py::test_a4_01_hung_auxiliary_is_bounded_and_required_lane_stays_available"],
        "A4-02": ["tests/iteration/test_a4_orchestration.py::test_a4_02_monitor_never_fetches_native_5m_or_archive_only_symbol"],
        "A4-03": ["tests/test_runtime_monitor.py::test_symbol_data_block_does_not_stop_healthy_plan"],
        "A4-04": ["tests/iteration/test_a4_orchestration.py::test_a4_04_position_hard_stop_does_not_need_market_or_llm"],
        "A4-05": ["tests/iteration/test_a4_orchestration.py::test_a4_05_model_completion_after_absolute_deadline_cannot_publish_buy"],
        "A4-06": ["tests/iteration/test_a4_orchestration.py::test_a4_06_late_bounded_result_cannot_replace_terminal_timeout"],
        "A4-07": ["tests/test_execution_data_contract.py::test_expected_clock"],
        "A4-08": ["tests/test_runtime_strategies.py::test_locked_limit_up_cannot_buy"],
        "A4-09": ["tests/test_runtime_scheduler.py::test_expired_active_lease_can_recover_same_dispatch_after_crash"],
        "A4-10": ["tests/iteration/test_a4_orchestration.py::test_a4_10_absolute_deadline_wrapper_preserves_valid_deterministic_output"],
        "A4-11": ["tests/iteration/test_a4_orchestration.py::test_a4_11_stale_position_quote_is_data_block_not_success"],
        "A4-12": ["tests/iteration/test_a4_orchestration.py::test_a4_12_archive_sqlite_writer_cannot_block_risk_intent_store"],
        "A1-01": ["tests/iteration/test_a1_coverage.py::test_a1_01_parse_mapping_gap_then_fix_reaches_real_packet"],
        "A1-02": ["tests/iteration/test_a1_coverage.py::test_a1_02_value_and_gap_states_are_not_collapsed"],
        "A1-03": ["tests/iteration/test_a1_coverage.py::test_a1_03_thousand_symbol_backfill_is_fair_under_new_high_priority_work"],
        "A1-04": ["tests/iteration/test_a1_coverage.py::test_a1_04_restart_resumes_only_deferred_task_and_keeps_success"],
        "A1-05": ["tests/iteration/test_a1_coverage.py::test_a1_05_late_announcement_cannot_enter_historical_cutoff"],
        "A1-06": ["tests/iteration/test_a1_coverage.py::test_a1_06_one_f10_period_cannot_claim_multi_period_or_strict_pit"],
        "A1-07": ["tests/iteration/test_a1_coverage.py::test_a1_07_negative_is_valid_but_missing_is_unknown"],
        "A1-08": ["tests/iteration/test_a1_coverage.py::test_a1_08_enqueue_is_idempotent_and_retry_after_survives_restart"],
        "A1-09": ["tests/iteration/test_a1_coverage.py::test_a1_09_packet_budget_never_hides_critical_gap_projection"],
        "A1-10": ["tests/iteration/test_a1_coverage.py::test_a1_10_arbitrary_821_scope_reconciles_every_layer"],
        "A1-11": ["tests/test_a1_registry.py::test_a1_11_incomplete_coverage_generation_cannot_replace_active"],
        "A1-12": ["tests/iteration/test_a1_coverage.py::test_a1_12_failed_fields_remain_in_denominator_and_empty_group_is_na"],
        "EX-01": ["tests/iteration/test_execution_causality.py::test_ex_01_fee_components_minimum_rounding_split_and_order_boundary"],
        "EX-02": ["tests/iteration/test_execution_causality.py::test_ex_02_future_close_does_not_change_frozen_order_quantity"],
        "EX-03": ["tests/iteration/test_execution_causality.py::test_ex_03_mid_bar_review_uses_only_following_complete_bar"],
        "EX-04": ["tests/iteration/test_execution_causality.py::test_ex_04_lunch_and_expiry_are_session_aware_and_never_backfilled"],
        "EX-05": ["tests/iteration/test_execution_causality.py::test_ex_05_capacity_caps_fill_and_records_remaining_quantity"],
        "EX-06": ["tests/iteration/test_execution_causality.py::test_ex_06_security_rules_come_from_versioned_provider"],
        "EX-07": ["tests/iteration/test_execution_causality.py::test_ex_07_accounting_is_atomic_and_concurrent_orders_do_not_double_spend"],
        "EX-08": ["tests/iteration/test_execution_causality.py::test_ex_08_legacy_fill_history_is_not_overwritten_by_new_replay"],
        "EX-09": ["tests/iteration/test_execution_causality.py::test_ex_09_synthetic_quote_cannot_supply_fill_or_capacity"],
        "EX-10": ["tests/iteration/test_execution_causality.py::test_ex_10_workflow_freezes_order_contract_and_rejects_risk_quote"],
        "RISK-01": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_01_concurrent_reservations_atomically_enforce_cash_and_total_budget"],
        "RISK-02": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_02_forced_exit_wins_conflict_without_increasing_risk"],
        "RISK-03": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_03_same_day_hard_stop_stays_pending_until_t1_release"],
        "RISK-04": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_04_old_sellable_lot_and_new_locked_add_survive_restart"],
        "RISK-05": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_05_duplicate_fill_is_idempotent_and_new_exit_revision_can_continue"],
        "RISK-06": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_06_release_and_partial_consumption_zero_out_reservation"],
        "RISK-07": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_07_new_reduce_episode_is_distinct_but_same_episode_is_idempotent"],
        "RISK-08": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_08_plan_expiry_does_not_close_position_risk_plan"],
        "RISK-09": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_09_gap_exit_uses_current_window_and_locked_bar_does_not_fake_fill"],
        "RISK-10": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_10_corporate_action_is_versioned_and_unresolved_blocks_add"],
        "RISK-11": ["tests/iteration/test_portfolio_risk_lifecycle.py::test_risk_11_failed_commit_leaves_no_half_written_fill_or_position"],
        "LLM-01": ["tests/iteration/test_llm_review_contract.py::test_llm_01_requires_exact_candidate_set_and_strict_boolean"],
        "LLM-02": ["tests/iteration/test_llm_review_contract.py::test_llm_02_action_price_quantity_symbol_and_unknown_fields_are_forbidden"],
        "LLM-03": [
            "tests/iteration/test_llm_review_contract.py::test_llm_03_rejects_late_or_wrong_decision_and_snapshot",
            "tests/test_pipeline_models.py::test_continuous_sse_is_bounded_by_total_wall_clock",
            "tests/test_pipeline_models.py::test_silent_socket_is_interrupted_by_remaining_wall_clock_budget",
        ],
        "LLM-04": ["tests/iteration/test_llm_review_contract.py::test_llm_04_model_failure_cannot_block_deterministic_position_exit"],
        "LLM-05": ["tests/iteration/test_llm_review_contract.py::test_llm_05_untrusted_text_is_data_and_fake_evidence_is_rejected"],
        "LLM-06": ["tests/iteration/test_llm_review_contract.py::test_llm_06_shadow_experiment_requires_separate_store_account_and_output"],
        "LLM-07": [
            "tests/iteration/test_llm_review_contract.py::test_llm_07_workflow_callback_returns_bound_transport_audit",
            "tests/iteration/test_llm_review_contract.py::test_llm_07_strict_monitor_persists_reason_reference_and_identity",
        ],
        "UI-01": [
            "tests/iteration/test_research_presentation.py::test_ui_01_a3_daily_setup_is_not_immediate_entry_and_target_claim_is_bounded",
            "test/server/research-presentation.test.ts::UI-01 and UI-02 keep A3 entry pending and A2 stock facts separate",
        ],
        "UI-02": [
            "tests/iteration/test_research_presentation.py::test_ui_02_a2_theme_strength_is_not_a_fake_stock_total_score",
            "test/server/research-presentation.test.ts::UI-01 and UI-02 keep A3 entry pending and A2 stock facts separate",
        ],
        "UI-03": [
            "tests/iteration/test_research_presentation.py::test_ui_03_unified_status_keeps_job_data_opportunity_and_unknown_separate",
            "test/server/research-presentation.test.ts::UI-03 keeps missing coverage unknown and critical/noncritical data distinct",
        ],
        "UI-04": [
            "tests/iteration/test_research_presentation.py::test_ui_04_attached_projection_is_the_single_persisted_stage_projection",
            "test/server/research-presentation.test.ts::UI-04 persisted projection is accepted but unknown fields and credentials are removed",
        ],
        "UI-05": ["test/server/research-presentation.test.ts::UI-05 100 dashboard reads consume local capability snapshots without remote fetch"],
        "UI-06": ["test/server/research-presentation.test.ts::UI-06 A4 dispatch exposes schedule, valid-through and independent scope states"],
        "SRC-09": ["tests/iteration/test_supplemental_sidecar.py::test_src_09_sidecar_enablement_never_mutates_formal_candidates_or_orders"],
        "SRC-10": ["tests/iteration/test_supplemental_sidecar.py::test_src_10_taxonomy_identities_cannot_be_interchanged"],
        "SRC-11": ["tests/iteration/test_supplemental_sidecar.py::test_src_11_http_file_time_without_trade_date_is_not_tradable"],
        "SRC-12": ["tests/iteration/test_supplemental_sidecar.py::test_src_12_reposts_form_one_source_chain_and_expired_opinion_has_no_authority"],
        "SRC-13": ["tests/iteration/test_supplemental_sidecar.py::test_src_13_business_parse_failure_keeps_industry_as_classification_only"],
        "SRC-14": ["tests/iteration/test_supplemental_sidecar.py::test_src_14_unlicensed_or_unverified_source_cannot_become_fallback"],
        "EVAL-01": ["tests/iteration/test_layered_evaluation.py::test_eval_01_future_revisions_do_not_change_frozen_decision_identity"],
        "EVAL-02": ["tests/iteration/test_layered_evaluation.py::test_eval_02_observed_and_repaired_runs_are_strictly_separated"],
        "EVAL-03": ["tests/iteration/test_layered_evaluation.py::test_eval_03_replay_is_idempotent_and_nonfills_are_not_profitable_trades"],
        "EVAL-04": ["tests/iteration/test_layered_evaluation.py::test_eval_04_versioned_cost_fill_and_strategy_reports_never_overwrite"],
        "EVAL-05": ["tests/iteration/test_layered_evaluation.py::test_eval_05_time_split_is_fixed_and_detects_overlapping_label_windows"],
        "EVAL-06": ["tests/iteration/test_layered_evaluation.py::test_eval_06_llm_veto_signal_and_account_effects_are_not_added_together"],
        "EVAL-07": ["tests/iteration/test_layered_evaluation.py::test_eval_07_known_future_and_cost_leakage_counterexamples_fail"],
        "EVAL-08": ["tests/iteration/test_layered_evaluation.py::test_eval_08_small_samples_and_contaminated_benchmarks_are_not_evidence"],
        "EVAL-09": ["tests/iteration/test_layered_evaluation.py::test_eval_09_a1_a2_reconciliation_keeps_rejected_and_missing_rows"],
        "OPS-01": ["tests/iteration/test_integrated_acceptance.py::test_ops_01_acceptance_entry_returns_missing_evidence_code"],
        "OPS-02": ["tests/iteration/test_integrated_acceptance.py::test_ops_02_migration_is_idempotent_and_failed_attempt_keeps_rollback_copy"],
        "OPS-03": ["tests/iteration/test_integrated_acceptance.py::test_ops_03_shadow_report_detects_faults_and_does_not_call_them_stable"],
        "OPS-04": ["tests/iteration/test_integrated_acceptance.py::test_ops_04_shadow_paths_reject_overlap_parent_child_and_symlinks"],
        "OPS-05": ["tests/iteration/test_integrated_acceptance.py::test_ops_05_requirement_bindings_name_real_python_tests"],
        "OPS-06": ["tests/iteration/test_integrated_acceptance.py::test_ops_06_documented_commands_have_working_help"],
        "OPS-07": ["tests/iteration/test_integrated_acceptance.py::test_ops_07_complete_package_generates_report_only_from_hashed_files"],
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
                REPO_ROOT / "config" / "a1_evidence_contracts.yaml",
                REPO_ROOT / "config" / "supplemental_sources.yaml",
                REPO_ROOT / "config" / "evaluation_experiments.yaml",
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
        if args.profile == "stress" and args.scenario not in {None, "a4-s03"}:
            missing_evidence.append(f"unsupported stress scenario: {args.scenario}")
        if args.profile == "stress" and not missing_evidence:
            checks.append(_run_command(
                "S03-A4-STRESS",
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "run_a4_stress.py"),
                    "--output-dir",
                    str(output_dir / "stress-data"),
                ],
                output_dir,
                _offline_environment(),
            ))
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
        if args.profile in {"replay", "shadow-report"} and not missing_evidence:
            manifest_input = Path(str(args.manifest)).expanduser().resolve()
            package_root = manifest_input.parent if manifest_input.is_file() else manifest_input
            try:
                evidence = validate_evidence_package(package_root)
            except AcceptanceContractError as exc:
                checks.append(CheckResult("OPS-07-EVIDENCE-PACKAGE", "FAIL", exc.reason_code))
                _write_summary(
                    output_dir=output_dir,
                    profile=args.profile,
                    started_at=started_at,
                    status="FAIL",
                    exit_code=EXIT_FAILURE,
                    checks=checks,
                    missing_evidence=missing_evidence,
                )
                return EXIT_FAILURE
            checks.append(CheckResult("OPS-07-EVIDENCE-PACKAGE", "PASS", evidence["status"]))
            if evidence["status"] != "EVIDENCE_VALID":
                missing_evidence.extend(f"missing evidence category: {item}" for item in evidence["missing_categories"])
            if evidence["test_only"]:
                missing_evidence.append("production replay/operations acceptance requires non-test evidence")
            category = "layered_evaluation_input" if args.profile == "replay" else "shadow_sessions"
            relative = evidence["files_by_category"].get(category)
            if not relative:
                missing_evidence.append(f"missing evidence category: {category}")
            if not missing_evidence:
                if args.profile == "replay":
                    command = [
                        sys.executable,
                        str(REPO_ROOT / "scripts" / "run_layered_evaluation.py"),
                        "--manifest",
                        str(package_root / relative),
                        "--output-dir",
                        str(output_dir / "replay-report"),
                    ]
                    check = _run_command("S10-LAYERED-REPLAY", command, output_dir, _offline_environment())
                    checks.append(check)
                    exit_code = EXIT_PASS if check.status == "PASS" else EXIT_FAILURE
                    status = "REPLAY_ACCEPTED" if exit_code == EXIT_PASS else "FAIL"
                else:
                    command = [
                        sys.executable,
                        str(REPO_ROOT / "scripts" / "build_shadow_stability_report.py"),
                        "--input",
                        str(package_root / relative),
                        "--output",
                        str(output_dir / "shadow-stability.json"),
                    ]
                    check = _run_command("S11-SHADOW-REPORT", command, output_dir, _offline_environment())
                    if check.exit_code == EXIT_MISSING_EVIDENCE:
                        check = CheckResult(**{**asdict(check), "status": "MISSING_EVIDENCE"})
                        missing_evidence.append("shadow observation window or fault evidence is incomplete")
                        exit_code = EXIT_MISSING_EVIDENCE
                        status = "PENDING_EVIDENCE"
                    else:
                        exit_code = EXIT_PASS if check.status == "PASS" else EXIT_FAILURE
                        status = "OPERATIONS_ACCEPTED" if exit_code == EXIT_PASS else "FAIL"
                    checks.append(check)
                _write_summary(
                    output_dir=output_dir,
                    profile=args.profile,
                    started_at=started_at,
                    status=status,
                    exit_code=exit_code,
                    checks=checks,
                    missing_evidence=missing_evidence,
                )
                return exit_code
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
