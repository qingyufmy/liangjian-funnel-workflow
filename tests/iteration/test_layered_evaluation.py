from __future__ import annotations

from copy import deepcopy
from datetime import date
from pathlib import Path
import subprocess
import sys

import pytest

from liangjian_funnel.evaluation.layered_evaluation import (
    EvaluationContractError,
    build_layered_evaluation,
    decision_identity,
    validate_config_change_proposal,
)


def _decision(index: int = 1, *, theme: str = "液冷") -> dict:
    day = 2 + index
    return {
        "decision_id": f"decision-{index}",
        "symbol": f"600{index:03d}.SH",
        "trade_date": f"2026-09-{day:02d}",
        "decision_at": f"2026-09-{day:02d}T10:00:00+08:00",
        "selected_at": f"2026-09-{day:02d}T10:00:00+08:00",
        "strategy_profile": "TREND_MA5",
        "technical_eligible": True,
        "a1_decision": "PASSED",
        "a2_decision": "PASSED",
        "a3_decision": "PASSED",
        "a4_decision": "BUY",
        "llm_decision": "ALLOW",
        "llm_used": True,
        "server_override": "NONE",
        "a1_path": "BUSINESS_AND_EARNINGS",
        "a2_theme": theme,
        "a2_role": "CORE",
        "a4_conditions": ["MA5_RECLAIM", "VOLUME_OK"],
        "market_regime": "ROTATION",
        "entry_mode": "PULLBACK_RECLAIM",
        "target_source": "STRUCTURE",
        "holding_window_days": 5,
        "theme_snapshot_date": f"2026-09-{day:02d}",
        "announcement_first_known_at": f"2026-09-{day:02d}T09:00:00+08:00",
        "adjustment_state": "RAW_PLUS_EXPLICIT_FACTOR",
        "evidence_complete": True,
        "frozen_evidence": {
            "snapshot_id": f"snapshot-{index}",
            "snapshot_hash": f"{index:064x}",
            "first_known_at": f"2026-09-{day:02d}T09:59:00+08:00",
        },
        "label": {
            "labeled_at": "2026-09-18T16:10:00+08:00",
            "fwd_return_1d": 0.01 * index,
            "fwd_return_3d": 0.02 * index,
            "fwd_return_5d": 0.03 * index,
            "fwd_return_10d": None,
            "mfe_5d": 0.05 * index,
            "mae_5d": -0.01 * index,
        },
    }


def _manifest(*, mode: str = "AS_OBSERVED", count: int = 4) -> dict:
    decisions = [_decision(i) for i in range(1, count + 1)]
    return {
        "schema_version": "liangjian-layered-evaluation-input/1.0.0",
        "run_id": f"run-{mode.lower()}",
        "dataset_mode": mode,
        "generated_at": "2026-09-20T10:00:00+08:00",
        "data_cutoff": "2026-09-18T15:00:00+08:00",
        "input_hash": "a" * 64,
        "config_version": "config-v1",
        "strategy_version": "strategy-v1",
        "fill_model_version": "fill-v2",
        "fee_model_version": "fee-cn-a-v1",
        "costs_complete": True,
        "random_seed": 20260920,
        "split": {"method": "TIME_WALK_FORWARD", "purge_days": 5, "embargo_days": 1},
        "decisions": decisions,
        "orders": [
            {
                "order_id": "order-1",
                "decision_id": "decision-1",
                "status": "FILLED",
                "filled_quantity": 100,
                "gross_pnl": 120.0,
                "fees": 10.0,
            },
            {
                "order_id": "order-2",
                "decision_id": "decision-2",
                "status": "BLOCKED",
                "filled_quantity": 0,
                "gross_pnl": 9999.0,
                "fees": 0.0,
            },
            {
                "order_id": "order-3",
                "decision_id": "decision-3",
                "status": "UNFILLED",
                "filled_quantity": 0,
                "gross_pnl": 9999.0,
                "fees": 0.0,
            },
        ][:count],
        "universe_reconciliation": {
            "A1": {"input": [d["decision_id"] for d in decisions], "passed": [d["decision_id"] for d in decisions], "rejected": [], "missing": []},
            "A2": {"input": [d["decision_id"] for d in decisions], "passed": [d["decision_id"] for d in decisions], "rejected": [], "missing": []},
        },
        "benchmark": {"kind": "HELD_OUT", "sample_ids": ["benchmark-1", "benchmark-2"]},
        "repairs": [],
    }


def test_eval_01_future_revisions_do_not_change_frozen_decision_identity() -> None:
    original = _decision()
    revised = deepcopy(original)
    revised["label"]["fwd_return_5d"] = 0.99
    revised["future_financial_revision"] = {"published_at": "2026-10-01T09:00:00+08:00"}
    assert decision_identity(original) == decision_identity(revised)


def test_eval_02_observed_and_repaired_runs_are_strictly_separated() -> None:
    observed = _manifest()
    observed["decisions"][0]["evidence_complete"] = False
    with pytest.raises(EvaluationContractError, match="AS_OBSERVED_EVIDENCE_INCOMPLETE"):
        build_layered_evaluation(observed)

    repaired = deepcopy(observed)
    repaired["dataset_mode"] = "REPAIRED_DATA"
    repaired["run_id"] = "run-repaired"
    repaired["repairs"] = [{
        "field": "minute_bars", "reason": "provider_gap", "repaired_at": "2026-09-20T09:00:00+08:00",
        "originally_available_at": None,
    }]
    report = build_layered_evaluation(repaired)
    assert report["dataset_mode"] == "REPAIRED_DATA"
    assert report["tradability_claim"] == "NOT_HISTORICALLY_TRADABLE"
    assert report["report_id"] != build_layered_evaluation(_manifest())["report_id"]


def test_eval_03_replay_is_idempotent_and_nonfills_are_not_profitable_trades() -> None:
    manifest = _manifest()
    manifest["orders"].append(deepcopy(manifest["orders"][0]))
    first = build_layered_evaluation(manifest)
    second = build_layered_evaluation(deepcopy(manifest))
    assert first == second
    account = first["scorecards"]["account"]["PLUS_LLM_VETO"]
    assert account["filled_trade_count"] == 1
    assert account["net_pnl"] == 110.0
    assert account["blocked_count"] == 1
    assert account["unfilled_count"] == 1


def test_eval_04_versioned_cost_fill_and_strategy_reports_never_overwrite() -> None:
    first = build_layered_evaluation(_manifest())
    changed = _manifest()
    changed["fee_model_version"] = "fee-cn-a-v2"
    second = build_layered_evaluation(changed)
    assert first["report_id"] != second["report_id"]
    assert first["versions"]["fee_model"] == "fee-cn-a-v1"
    assert second["versions"]["fee_model"] == "fee-cn-a-v2"


def test_eval_05_time_split_is_fixed_and_detects_overlapping_label_windows() -> None:
    report = build_layered_evaluation(_manifest(count=8))
    split = report["split"]
    assert split["method"] == "TIME_WALK_FORWARD"
    assert split["random_seed"] == 20260920
    assert split["purged_ids"]
    assert set(split["train_ids"]).isdisjoint(split["validation_ids"])


def test_eval_06_llm_veto_signal_and_account_effects_are_not_added_together() -> None:
    manifest = _manifest()
    manifest["decisions"][1]["llm_decision"] = "VETO"
    report = build_layered_evaluation(manifest)
    effect = report["llm_veto_evaluation"]
    assert effect["signal_level"]["vetoed_count"] == 1
    assert effect["account_level"]["basis"] == "ACTUAL_FILLS_ONLY"
    assert effect["account_level"]["counterfactual_addition_allowed"] is False


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda m: m["decisions"][0].update(announcement_first_known_at="2026-10-01T09:00:00+08:00"), "FUTURE_ANNOUNCEMENT_LEAKAGE"),
        (lambda m: m["decisions"][0].update(theme_snapshot_date="2026-09-04"), "THEME_SNAPSHOT_DATE_MISMATCH"),
        (lambda m: m["decisions"][0].update(adjustment_state="UNKNOWN"), "ADJUSTMENT_STATE_UNVERIFIED"),
        (lambda m: m.update(costs_complete=False), "COST_MODEL_INCOMPLETE"),
    ],
)
def test_eval_07_known_future_and_cost_leakage_counterexamples_fail(mutate, reason: str) -> None:
    manifest = _manifest()
    mutate(manifest)
    with pytest.raises(EvaluationContractError, match=reason):
        build_layered_evaluation(manifest)


def test_eval_08_small_samples_and_contaminated_benchmarks_are_not_evidence() -> None:
    manifest = _manifest(count=2)
    report = build_layered_evaluation(manifest)
    assert report["strategy_evidence_status"] == "INSUFFICIENT_EVIDENCE"
    manifest["benchmark"]["sample_ids"] = ["decision-1"]
    with pytest.raises(EvaluationContractError, match="BENCHMARK_SELECTION_CONTAMINATION"):
        build_layered_evaluation(manifest)


def test_eval_09_a1_a2_reconciliation_keeps_rejected_and_missing_rows() -> None:
    manifest = _manifest()
    manifest["universe_reconciliation"]["A2"] = {
        "input": ["decision-1", "decision-2", "decision-3", "decision-4"],
        "passed": ["decision-1", "decision-2"],
        "rejected": ["decision-3"],
        "missing": ["decision-4"],
    }
    report = build_layered_evaluation(manifest)
    assert report["universe_reconciliation"]["A2"]["balanced"] is True
    assert report["universe_reconciliation"]["A2"]["rejected"] == ["decision-3"]
    assert report["universe_reconciliation"]["A2"]["missing"] == ["decision-4"]


def test_config_change_proposal_is_advisory_and_never_auto_applies() -> None:
    proposal = {
        "artifact_type": "CONFIG_CHANGE_PROPOSAL",
        "proposal_id": "proposal-1",
        "hypothesis": "MA5 confirmation reduces false entries",
        "impact_scope": ["TREND_MA5"],
        "frozen_versions": {"strategy": "strategy-v1"},
        "training_window": ["2026-01-01", "2026-06-30"],
        "validation_window": ["2026-07-01", "2026-08-31"],
        "all_experiments": ["control", "candidate"],
        "control": "control",
        "costs_and_risks": ["turnover"],
        "acceptance_thresholds": {"minimum_trades": 30},
        "rollback": "restore strategy-v1",
        "automatic_apply": False,
    }
    result = validate_config_change_proposal(proposal)
    assert result["status"] == "PROPOSED_NOT_APPLIED"
    assert result["production_config_changed"] is False
    proposal["automatic_apply"] = True
    with pytest.raises(EvaluationContractError, match="CONFIG_CHANGE_AUTO_APPLY_FORBIDDEN"):
        validate_config_change_proposal(proposal)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda m: m.update(schema_version="wrong"), "EVALUATION_INPUT_SCHEMA_INVALID"),
        (lambda m: m.update(dataset_mode="UNKNOWN"), "DATASET_MODE_INVALID"),
        (lambda m: m.update(dataset_mode="REPAIRED_DATA"), "REPAIRED_DATA_LEDGER_MISSING"),
        (lambda m: m.update(dataset_mode="RETROSPECTIVE_MODEL_RESEARCH"), "RETROSPECTIVE_MODEL_RISK_UNACKNOWLEDGED"),
        (lambda m: m["decisions"][0]["frozen_evidence"].update(first_known_at="2026-10-01T09:00:00+08:00"), "FUTURE_EVIDENCE_LEAKAGE"),
        (lambda m: m["decisions"][0].update(llm_used="yes"), "MODEL_USE_TRACE_MISSING"),
        (lambda m: m["decisions"][0].update(holding_window_days=0), "HOLDING_WINDOW_INVALID"),
        (lambda m: m.update(strategy_version=""), "EVALUATION_VERSION_MISSING"),
        (lambda m: m.update(random_seed=True), "RANDOM_SEED_INVALID"),
        (lambda m: m["decisions"].append(deepcopy(m["decisions"][0])), "DUPLICATE_DECISION_ID"),
        (lambda m: m.update(benchmark={"kind": "NOT_HELD_OUT", "sample_ids": []}), "BENCHMARK_NOT_HELD_OUT"),
        (lambda m: m["universe_reconciliation"]["A2"].update(rejected=["decision-1"]), "A2_RECONCILIATION_OVERLAP"),
    ],
)
def test_evaluation_contract_rejects_malformed_or_leaking_inputs(mutate, reason: str) -> None:
    manifest = _manifest()
    mutate(manifest)
    with pytest.raises(EvaluationContractError, match=reason):
        build_layered_evaluation(manifest)


def test_repaired_and_retrospective_modes_preserve_nontradable_boundary() -> None:
    repaired = _manifest(mode="REPAIRED_DATA")
    repaired["repairs"] = [{
        "field": "daily_bar", "reason": "late_correction", "repaired_at": "2026-09-20T09:00:00+08:00"
    }]
    assert build_layered_evaluation(repaired)["tradability_claim"] == "NOT_HISTORICALLY_TRADABLE"
    retrospective = _manifest(mode="RETROSPECTIVE_MODEL_RESEARCH")
    retrospective["retrospective_model_risk_acknowledged"] = True
    assert build_layered_evaluation(retrospective)["tradability_claim"] == "NOT_HISTORICALLY_TRADABLE"


def test_broker_gold_cannot_be_admission_and_heldout_benchmark() -> None:
    manifest = _manifest()
    manifest["decisions"][0]["a1_path"] = "BROKER_GOLD"
    manifest["benchmark"]["provenance"] = "BROKER_GOLD"
    with pytest.raises(EvaluationContractError, match="BROKER_GOLD_ADMISSION_BENCHMARK_REUSE"):
        build_layered_evaluation(manifest)


def test_post_close_selection_cannot_claim_same_day_fill() -> None:
    manifest = _manifest()
    manifest["decisions"][0]["selected_at"] = "2026-09-03T15:10:00+08:00"
    manifest["orders"][0]["fill_at"] = "2026-09-03T15:11:00+08:00"
    with pytest.raises(EvaluationContractError, match="POST_CLOSE_SAME_DAY_FILL"):
        build_layered_evaluation(manifest)


def test_empty_dataset_is_explicitly_insufficient_not_profitable() -> None:
    report = build_layered_evaluation(_manifest(count=0))
    assert report["strategy_evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert all(card["filled_trade_count"] == 0 for card in report["scorecards"]["account"].values())


def test_layered_evaluation_cli_runs_only_into_isolated_output(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "run_layered_evaluation.py"),
            "--manifest",
            str(root / "tests" / "fixtures" / "layered_evaluation_observed.json"),
            "--output-dir",
            str(tmp_path / "report"),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "report" / "layered-evaluation.json").exists()


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda m: m["decisions"][0].update(decision_id=""), "DECISION_ID_MISSING"),
        (lambda m: m["decisions"][0].update(trade_date="not-a-date"), "DECISION_TRADE_DATE_INVALID"),
        (lambda m: m["decisions"][0].update(decision_at="2026-09-04T10:00:00+08:00"), "DECISION_TRADE_DATE_MISMATCH"),
        (lambda m: m["decisions"][0].pop("frozen_evidence"), "FROZEN_EVIDENCE_MISSING"),
        (lambda m: m["decisions"][0]["frozen_evidence"].update(snapshot_hash="short"), "FROZEN_EVIDENCE_IDENTITY_INVALID"),
        (lambda m: m["decisions"][0].update(decision_at="not-a-time"), "DECISION_TIME_INVALID"),
        (lambda m: m["decisions"][0].update(decision_at="2026-09-03T10:00:00"), "DECISION_TIME_INVALID"),
        (lambda m: m.update(run_id=""), "EVALUATION_RUN_IDENTITY_INVALID"),
        (lambda m: m.update(decisions="bad"), "DECISION_SET_INVALID"),
        (lambda m: m.update(split=None), "SPLIT_SPEC_MISSING"),
        (lambda m: m["split"].update(method="RANDOM"), "SPLIT_METHOD_INVALID"),
        (lambda m: m["split"].update(purge_days=-1), "SPLIT_WINDOW_INVALID"),
        (lambda m: m.update(universe_reconciliation=None), "UNIVERSE_RECONCILIATION_MISSING"),
        (lambda m: m["universe_reconciliation"]["A2"].update(passed=["decision-1", "decision-2", "decision-3"]), "A2_RECONCILIATION_UNBALANCED"),
        (lambda m: m["orders"][0].update(decision_id="unknown"), "ORDER_DECISION_UNKNOWN"),
    ],
)
def test_additional_fail_closed_contract_edges(mutate, reason: str) -> None:
    manifest = _manifest()
    mutate(manifest)
    with pytest.raises(EvaluationContractError, match=reason):
        build_layered_evaluation(manifest)


@pytest.mark.parametrize(
    ("orders", "reason"),
    [
        (None, "ORDER_LEDGER_INVALID"),
        (["bad"], "ORDER_LEDGER_INVALID"),
        ([{"decision_id": "decision-1"}], "ORDER_ID_MISSING"),
        ([
            {"order_id": "same", "decision_id": "decision-1", "status": "BLOCKED"},
            {"order_id": "same", "decision_id": "decision-1", "status": "UNFILLED"},
        ], "ORDER_ID_CONFLICT"),
    ],
)
def test_order_ledger_identity_edges(orders, reason: str) -> None:
    manifest = _manifest()
    manifest["orders"] = orders
    with pytest.raises(EvaluationContractError, match=reason):
        build_layered_evaluation(manifest)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("filled_quantity", 0, "FILLED_QUANTITY_INVALID"),
        ("filled_quantity", None, "FILLED_QUANTITY_MISSING"),
        ("gross_pnl", "bad", "GROSS_PNL_MISSING"),
        ("fees", float("inf"), "FEE_DATA_MISSING"),
    ],
)
def test_filled_orders_require_complete_finite_accounting(field: str, value, reason: str) -> None:
    manifest = _manifest()
    manifest["orders"][0][field] = value
    with pytest.raises(EvaluationContractError, match=reason):
        build_layered_evaluation(manifest)


def test_layer_filters_preserve_every_rejected_route_and_invalid_labels() -> None:
    manifest = _manifest()
    manifest["decisions"][0]["technical_eligible"] = False
    manifest["decisions"][1]["a1_decision"] = "REJECTED"
    manifest["decisions"][2]["a2_decision"] = "REJECTED"
    manifest["decisions"][3]["a3_decision"] = "REJECTED"
    manifest["decisions"][3]["label"]["fwd_return_3d"] = "not-a-number"
    report = build_layered_evaluation(manifest)
    assert report["scorecards"]["research"]["TECHNICAL_BASELINE"]["selected_count"] == 3
    assert report["scorecards"]["research"]["PLUS_A1"]["selected_count"] == 2
    assert report["scorecards"]["research"]["PLUS_A2"]["selected_count"] == 1
    assert report["scorecards"]["research"]["FULL_A3_A4"]["selected_count"] == 0


def test_repair_and_change_proposal_malformed_records_fail_closed() -> None:
    repaired = _manifest(mode="REPAIRED_DATA")
    repaired["repairs"] = [{"field": "", "reason": "gap", "repaired_at": "2026-09-20T09:00:00+08:00"}]
    with pytest.raises(EvaluationContractError, match="REPAIRED_DATA_LEDGER_INVALID"):
        build_layered_evaluation(repaired)
    observed = _manifest()
    observed["repairs"] = [{"field": "x"}]
    with pytest.raises(EvaluationContractError, match="AS_OBSERVED_REPAIR_FORBIDDEN"):
        build_layered_evaluation(observed)
    with pytest.raises(EvaluationContractError, match="CONFIG_CHANGE_PROPOSAL_INCOMPLETE"):
        validate_config_change_proposal({})
    complete = {
        "proposal_id": "p", "hypothesis": "h", "impact_scope": ["A4"], "frozen_versions": {"v": 1},
        "training_window": [1], "validation_window": [2], "all_experiments": ["c"], "control": "c",
        "costs_and_risks": ["r"], "acceptance_thresholds": {"n": 1}, "rollback": "r", "automatic_apply": False,
    }
    with pytest.raises(EvaluationContractError, match="CONFIG_CHANGE_PROPOSAL_TYPE_INVALID"):
        validate_config_change_proposal(complete)
