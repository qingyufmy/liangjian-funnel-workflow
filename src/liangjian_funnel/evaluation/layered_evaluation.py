"""Causal, versioned scorecards over the existing replay and outcome ledgers.

This module is deliberately not a backtest engine.  It consumes immutable
decision/outcome/order exports produced by the existing evaluation and runtime
paths and checks that a comparison is point-in-time eligible before computing
research and account scorecards.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo


LAYERED_EVALUATION_SCHEMA = "liangjian-layered-evaluation/1.0.0"
INPUT_SCHEMA = "liangjian-layered-evaluation-input/1.0.0"
SHANGHAI = ZoneInfo("Asia/Shanghai")
EXPERIMENT_LAYERS = (
    "TECHNICAL_BASELINE",
    "PLUS_A1",
    "PLUS_A2",
    "FULL_A3_A4",
    "PLUS_LLM_VETO",
)


class DatasetMode(StrEnum):
    AS_OBSERVED = "AS_OBSERVED"
    REPAIRED_DATA = "REPAIRED_DATA"
    RETROSPECTIVE_MODEL_RESEARCH = "RETROSPECTIVE_MODEL_RESEARCH"


class EvaluationContractError(ValueError):
    """The evaluation input cannot support the requested causal claim."""

    def __init__(self, reason_code: str, message: str | None = None) -> None:
        self.reason_code = reason_code
        super().__init__(message or reason_code)


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _timestamp(value: Any, reason: str) -> datetime:
    raw = _text(value)
    if not raw:
        raise EvaluationContractError(reason)
    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise EvaluationContractError(reason) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise EvaluationContractError(reason)
    return result.astimezone(SHANGHAI)


def _date(value: Any, reason: str) -> date:
    raw = _text(value)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise EvaluationContractError(reason) from exc


def _finite(value: Any, *, reason: str) -> float:
    if value is None or isinstance(value, bool):
        raise EvaluationContractError(reason)
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise EvaluationContractError(reason) from exc
    if not math.isfinite(result):
        raise EvaluationContractError(reason)
    return result


def decision_identity(decision: Mapping[str, Any]) -> str:
    """Hash only facts that existed when the decision was frozen.

    Forward labels, later repairs and retrospective annotations are excluded so
    they can be appended without rewriting the decision's identity.
    """

    frozen = decision.get("frozen_evidence")
    frozen_map = dict(frozen) if isinstance(frozen, Mapping) else {}
    payload = {
        "decision_id": _text(decision.get("decision_id")),
        "symbol": _text(decision.get("symbol")).upper(),
        "trade_date": _text(decision.get("trade_date")),
        "decision_at": _text(decision.get("decision_at")),
        "strategy_profile": _text(decision.get("strategy_profile")),
        "technical_eligible": decision.get("technical_eligible"),
        "a1_decision": _text(decision.get("a1_decision")).upper(),
        "a2_decision": _text(decision.get("a2_decision")).upper(),
        "a3_decision": _text(decision.get("a3_decision")).upper(),
        "a4_decision": _text(decision.get("a4_decision")).upper(),
        "llm_decision": _text(decision.get("llm_decision")).upper(),
        "llm_used": decision.get("llm_used"),
        "server_override": _text(decision.get("server_override")).upper(),
        "snapshot_id": _text(frozen_map.get("snapshot_id")),
        "snapshot_hash": _text(frozen_map.get("snapshot_hash")).lower(),
        "evidence_first_known_at": _text(frozen_map.get("first_known_at")),
    }
    return _canonical_hash(payload)


def _validate_decision(decision: Mapping[str, Any], *, mode: DatasetMode) -> dict[str, Any]:
    decision_id = _text(decision.get("decision_id"))
    if not decision_id:
        raise EvaluationContractError("DECISION_ID_MISSING")
    trade_date = _date(decision.get("trade_date"), "DECISION_TRADE_DATE_INVALID")
    decision_at = _timestamp(decision.get("decision_at"), "DECISION_TIME_INVALID")
    if decision_at.date() != trade_date:
        raise EvaluationContractError("DECISION_TRADE_DATE_MISMATCH")

    if mode is DatasetMode.AS_OBSERVED and decision.get("evidence_complete") is not True:
        raise EvaluationContractError("AS_OBSERVED_EVIDENCE_INCOMPLETE")
    frozen = decision.get("frozen_evidence")
    if not isinstance(frozen, Mapping):
        raise EvaluationContractError("FROZEN_EVIDENCE_MISSING")
    if not _text(frozen.get("snapshot_id")) or len(_text(frozen.get("snapshot_hash"))) != 64:
        raise EvaluationContractError("FROZEN_EVIDENCE_IDENTITY_INVALID")
    first_known = _timestamp(frozen.get("first_known_at"), "EVIDENCE_FIRST_KNOWN_AT_MISSING")
    if first_known > decision_at:
        raise EvaluationContractError("FUTURE_EVIDENCE_LEAKAGE")

    announcement = _timestamp(
        decision.get("announcement_first_known_at"),
        "ANNOUNCEMENT_FIRST_KNOWN_AT_MISSING",
    )
    if announcement > decision_at:
        raise EvaluationContractError("FUTURE_ANNOUNCEMENT_LEAKAGE")
    if _date(decision.get("theme_snapshot_date"), "THEME_SNAPSHOT_DATE_INVALID") != trade_date:
        raise EvaluationContractError("THEME_SNAPSHOT_DATE_MISMATCH")
    if _text(decision.get("adjustment_state")) != "RAW_PLUS_EXPLICIT_FACTOR":
        raise EvaluationContractError("ADJUSTMENT_STATE_UNVERIFIED")
    if not isinstance(decision.get("llm_used"), bool) or not _text(decision.get("server_override")):
        raise EvaluationContractError("MODEL_USE_TRACE_MISSING")

    holding = decision.get("holding_window_days")
    if isinstance(holding, bool) or not isinstance(holding, int) or holding <= 0:
        raise EvaluationContractError("HOLDING_WINDOW_INVALID")

    return {
        **dict(decision),
        "decision_id": decision_id,
        "trade_date_parsed": trade_date,
        "decision_at_parsed": decision_at,
        "decision_identity": decision_identity(decision),
    }


def _passes_layer(decision: Mapping[str, Any], layer: str) -> bool:
    if decision.get("technical_eligible") is not True:
        return False
    if layer == "TECHNICAL_BASELINE":
        return True
    if _text(decision.get("a1_decision")).upper() != "PASSED":
        return False
    if layer == "PLUS_A1":
        return True
    if _text(decision.get("a2_decision")).upper() != "PASSED":
        return False
    if layer == "PLUS_A2":
        return True
    if _text(decision.get("a3_decision")).upper() != "PASSED" or _text(decision.get("a4_decision")).upper() not in {
        "BUY", "ADD", "ALLOW", "PASSED"
    }:
        return False
    if layer == "FULL_A3_A4":
        return True
    return _text(decision.get("llm_decision")).upper() == "ALLOW"


def _mean(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 8) if values else None


def _research_scorecard(decisions: Sequence[Mapping[str, Any]], layer: str, minimum_sample: int) -> dict[str, Any]:
    selected = [item for item in decisions if _passes_layer(item, layer)]
    windows: dict[str, float | None] = {}
    for key in ("fwd_return_1d", "fwd_return_3d", "fwd_return_5d", "fwd_return_10d", "mfe_5d", "mae_5d"):
        values: list[float] = []
        for item in selected:
            label = item.get("label")
            raw = label.get(key) if isinstance(label, Mapping) else None
            if raw is not None and not isinstance(raw, bool):
                try:
                    parsed = float(raw)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(parsed):
                    values.append(parsed)
        windows[key] = _mean(values)
    labeled_5d = sum(
        1
        for item in selected
        if isinstance(item.get("label"), Mapping) and item["label"].get("fwd_return_5d") is not None
    )
    return {
        "selected_count": len(selected),
        "labeled_5d_count": labeled_5d,
        "status": "EVIDENCE_AVAILABLE" if labeled_5d >= minimum_sample else "INSUFFICIENT_EVIDENCE",
        "metrics": windows,
        "group_dimensions": [
            "strategy_profile", "market_regime", "entry_mode", "target_source",
            "holding_window_days", "a1_path", "a2_theme", "a2_role", "a4_conditions",
        ],
    }


def _group_scorecards(
    decisions: Sequence[Mapping[str, Any]],
    *,
    layer: str,
    minimum_sample: int,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in decisions:
        profile = _text(item.get("strategy_profile")) or "UNKNOWN"
        grouped.setdefault(profile, []).append(item)
    return {
        profile: _research_scorecard(rows, layer, minimum_sample)
        for profile, rows in sorted(grouped.items())
    }


def _dedupe_orders(orders: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(orders, Sequence) or isinstance(orders, (str, bytes, bytearray)):
        raise EvaluationContractError("ORDER_LEDGER_INVALID")
    unique: dict[str, dict[str, Any]] = {}
    for raw in orders:
        if not isinstance(raw, Mapping):
            raise EvaluationContractError("ORDER_LEDGER_INVALID")
        order_id = _text(raw.get("order_id"))
        if not order_id:
            raise EvaluationContractError("ORDER_ID_MISSING")
        value = dict(raw)
        previous = unique.get(order_id)
        if previous is not None and _canonical_hash(previous) != _canonical_hash(value):
            raise EvaluationContractError("ORDER_ID_CONFLICT")
        unique[order_id] = value
    return tuple(unique[key] for key in sorted(unique))


def _account_scorecard(
    decisions_by_id: Mapping[str, Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
    layer: str,
) -> dict[str, Any]:
    relevant = [
        item for item in orders
        if (decision := decisions_by_id.get(_text(item.get("decision_id")))) is not None
        and _passes_layer(decision, layer)
    ]
    filled = [item for item in relevant if _text(item.get("status")).upper() in {"FILLED", "PARTIALLY_FILLED"}]
    net_pnl = 0.0
    for item in filled:
        quantity = _finite(item.get("filled_quantity"), reason="FILLED_QUANTITY_MISSING")
        if quantity <= 0:
            raise EvaluationContractError("FILLED_QUANTITY_INVALID")
        gross = _finite(item.get("gross_pnl"), reason="GROSS_PNL_MISSING")
        fees = _finite(item.get("fees"), reason="FEE_DATA_MISSING")
        net_pnl += gross - fees
    return {
        "basis": "ACTUAL_FILLS_ONLY",
        "filled_trade_count": len(filled),
        "blocked_count": sum(_text(item.get("status")).upper() == "BLOCKED" for item in relevant),
        "unfilled_count": sum(_text(item.get("status")).upper() in {"UNFILLED", "EXPIRED", "CANCELLED"} for item in relevant),
        "net_pnl": round(net_pnl, 8),
        "counterfactual_addition_allowed": False,
    }


def _walk_forward_split(decisions: Sequence[Mapping[str, Any]], split: Mapping[str, Any], seed: int) -> dict[str, Any]:
    if _text(split.get("method")) != "TIME_WALK_FORWARD":
        raise EvaluationContractError("SPLIT_METHOD_INVALID")
    purge_days = split.get("purge_days")
    embargo_days = split.get("embargo_days")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (purge_days, embargo_days)):
        raise EvaluationContractError("SPLIT_WINDOW_INVALID")
    ordered = sorted(decisions, key=lambda item: (item["trade_date_parsed"], item["decision_id"]))
    if len(ordered) < 2:
        return {
            "method": "TIME_WALK_FORWARD", "random_seed": seed,
            "train_ids": [], "validation_ids": [item["decision_id"] for item in ordered],
            "purged_ids": [], "embargoed_ids": [], "purge_days": purge_days, "embargo_days": embargo_days,
        }
    boundary = max(1, min(len(ordered) - 1, int(len(ordered) * 0.6)))
    raw_train = ordered[:boundary]
    raw_validation = ordered[boundary:]
    train_last = raw_train[-1]["trade_date_parsed"]
    embargo_cutoff = train_last + timedelta(days=embargo_days)
    embargoed = [item for item in raw_validation if item["trade_date_parsed"] <= embargo_cutoff]
    validation = [item for item in raw_validation if item["trade_date_parsed"] > embargo_cutoff]
    validation_start = validation[0]["trade_date_parsed"] if validation else raw_validation[-1]["trade_date_parsed"]
    purged = [
        item for item in raw_train
        if item["trade_date_parsed"] + timedelta(days=int(item["holding_window_days"])) >= validation_start
    ]
    purged_ids = {item["decision_id"] for item in purged}
    train = [item for item in raw_train if item["decision_id"] not in purged_ids]
    return {
        "method": "TIME_WALK_FORWARD",
        "random_seed": seed,
        "train_ids": [item["decision_id"] for item in train],
        "validation_ids": [item["decision_id"] for item in validation],
        "purged_ids": sorted(purged_ids),
        "embargoed_ids": [item["decision_id"] for item in embargoed],
        "purge_days": purge_days,
        "embargo_days": embargo_days,
    }


def _reconcile(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise EvaluationContractError("UNIVERSE_RECONCILIATION_MISSING")
    result: dict[str, Any] = {}
    for stage in ("A1", "A2"):
        item = raw.get(stage)
        if not isinstance(item, Mapping):
            raise EvaluationContractError(f"{stage}_RECONCILIATION_MISSING")
        groups = {
            key: [_text(value) for value in item.get(key, ())]
            if isinstance(item.get(key), Sequence) and not isinstance(item.get(key), (str, bytes, bytearray))
            else []
            for key in ("input", "passed", "rejected", "missing")
        }
        input_ids = set(groups["input"])
        partitions = [set(groups[key]) for key in ("passed", "rejected", "missing")]
        if any(partitions[i] & partitions[j] for i in range(3) for j in range(i + 1, 3)):
            raise EvaluationContractError(f"{stage}_RECONCILIATION_OVERLAP")
        if set().union(*partitions) != input_ids:
            raise EvaluationContractError(f"{stage}_RECONCILIATION_UNBALANCED")
        result[stage] = {**groups, "balanced": True}
    return result


def _validate_mode(manifest: Mapping[str, Any]) -> DatasetMode:
    try:
        mode = DatasetMode(_text(manifest.get("dataset_mode")))
    except ValueError as exc:
        raise EvaluationContractError("DATASET_MODE_INVALID") from exc
    repairs = manifest.get("repairs")
    repairs_seq = repairs if isinstance(repairs, Sequence) and not isinstance(repairs, (str, bytes, bytearray)) else []
    if mode is DatasetMode.AS_OBSERVED and repairs_seq:
        raise EvaluationContractError("AS_OBSERVED_REPAIR_FORBIDDEN")
    if mode is DatasetMode.REPAIRED_DATA and not repairs_seq:
        raise EvaluationContractError("REPAIRED_DATA_LEDGER_MISSING")
    if mode is DatasetMode.REPAIRED_DATA:
        for repair in repairs_seq:
            if not isinstance(repair, Mapping) or not _text(repair.get("field")) or not _text(repair.get("reason")):
                raise EvaluationContractError("REPAIRED_DATA_LEDGER_INVALID")
            _timestamp(repair.get("repaired_at"), "REPAIR_TIME_MISSING")
    if mode is DatasetMode.RETROSPECTIVE_MODEL_RESEARCH and manifest.get("retrospective_model_risk_acknowledged") is not True:
        raise EvaluationContractError("RETROSPECTIVE_MODEL_RISK_UNACKNOWLEDGED")
    return mode


def validate_config_change_proposal(proposal: Mapping[str, Any]) -> dict[str, Any]:
    """Validate an advisory proposal without applying it to production."""

    required = (
        "proposal_id", "hypothesis", "impact_scope", "frozen_versions",
        "training_window", "validation_window", "all_experiments", "control",
        "costs_and_risks", "acceptance_thresholds", "rollback",
    )
    missing = [field for field in required if not proposal.get(field)]
    if missing:
        raise EvaluationContractError(
            "CONFIG_CHANGE_PROPOSAL_INCOMPLETE",
            f"CONFIG_CHANGE_PROPOSAL_INCOMPLETE:{','.join(missing)}",
        )
    if _text(proposal.get("artifact_type")) != "CONFIG_CHANGE_PROPOSAL":
        raise EvaluationContractError("CONFIG_CHANGE_PROPOSAL_TYPE_INVALID")
    if proposal.get("automatic_apply") is not False:
        raise EvaluationContractError("CONFIG_CHANGE_AUTO_APPLY_FORBIDDEN")
    return {
        **dict(proposal),
        "status": "PROPOSED_NOT_APPLIED",
        "production_config_changed": False,
    }


def build_layered_evaluation(
    manifest: Mapping[str, Any],
    *,
    minimum_sample_size: int = 30,
) -> dict[str, Any]:
    """Validate one immutable evaluation export and build separated scorecards."""

    if _text(manifest.get("schema_version")) != INPUT_SCHEMA:
        raise EvaluationContractError("EVALUATION_INPUT_SCHEMA_INVALID")
    mode = _validate_mode(manifest)
    if manifest.get("costs_complete") is not True:
        raise EvaluationContractError("COST_MODEL_INCOMPLETE")
    versions = {
        "config": _text(manifest.get("config_version")),
        "strategy": _text(manifest.get("strategy_version")),
        "fill_model": _text(manifest.get("fill_model_version")),
        "fee_model": _text(manifest.get("fee_model_version")),
    }
    if not all(versions.values()):
        raise EvaluationContractError("EVALUATION_VERSION_MISSING")
    run_id = _text(manifest.get("run_id"))
    input_hash = _text(manifest.get("input_hash")).lower()
    if not run_id or len(input_hash) != 64:
        raise EvaluationContractError("EVALUATION_RUN_IDENTITY_INVALID")
    seed = manifest.get("random_seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise EvaluationContractError("RANDOM_SEED_INVALID")

    raw_decisions = manifest.get("decisions")
    if not isinstance(raw_decisions, Sequence) or isinstance(raw_decisions, (str, bytes, bytearray)):
        raise EvaluationContractError("DECISION_SET_INVALID")
    decisions = tuple(_validate_decision(item, mode=mode) for item in raw_decisions if isinstance(item, Mapping))
    if len(decisions) != len(raw_decisions):
        raise EvaluationContractError("DECISION_SET_INVALID")
    identities = [item["decision_id"] for item in decisions]
    if len(set(identities)) != len(identities):
        raise EvaluationContractError("DUPLICATE_DECISION_ID")
    decisions_by_id = {item["decision_id"]: item for item in decisions}

    benchmark = manifest.get("benchmark")
    if not isinstance(benchmark, Mapping) or _text(benchmark.get("kind")) != "HELD_OUT":
        raise EvaluationContractError("BENCHMARK_NOT_HELD_OUT")
    benchmark_ids = {_text(value) for value in benchmark.get("sample_ids", ())}
    if benchmark_ids & set(identities):
        raise EvaluationContractError("BENCHMARK_SELECTION_CONTAMINATION")
    if _text(benchmark.get("provenance")).upper() == "BROKER_GOLD" and any(
        _text(item.get("a1_path")).upper() == "BROKER_GOLD" for item in decisions
    ):
        raise EvaluationContractError("BROKER_GOLD_ADMISSION_BENCHMARK_REUSE")

    orders = _dedupe_orders(manifest.get("orders"))
    for order in orders:
        decision = decisions_by_id.get(_text(order.get("decision_id")))
        if decision is None:
            raise EvaluationContractError("ORDER_DECISION_UNKNOWN")
        selected_at = _timestamp(decision.get("selected_at"), "SELECTED_AT_MISSING")
        fill_at_raw = order.get("fill_at")
        if fill_at_raw:
            fill_at = _timestamp(fill_at_raw, "FILL_AT_INVALID")
            if selected_at.time() > time(15, 0) and fill_at.date() == selected_at.date():
                raise EvaluationContractError("POST_CLOSE_SAME_DAY_FILL")

    split_raw = manifest.get("split")
    if not isinstance(split_raw, Mapping):
        raise EvaluationContractError("SPLIT_SPEC_MISSING")
    split = _walk_forward_split(decisions, split_raw, seed)
    reconciliation = _reconcile(manifest.get("universe_reconciliation"))
    research = {layer: _research_scorecard(decisions, layer, minimum_sample_size) for layer in EXPERIMENT_LAYERS}
    account = {layer: _account_scorecard(decisions_by_id, orders, layer) for layer in EXPERIMENT_LAYERS}
    strategy_breakdown = {
        layer: _group_scorecards(decisions, layer=layer, minimum_sample=minimum_sample_size)
        for layer in EXPERIMENT_LAYERS
    }
    vetoed = [item for item in decisions if _text(item.get("llm_decision")).upper() == "VETO"]

    report_identity = {
        "run_id": run_id,
        "dataset_mode": mode.value,
        "input_hash": input_hash,
        "versions": versions,
        "seed": seed,
        "split": dict(split_raw),
        "layers": EXPERIMENT_LAYERS,
    }
    sufficient = any(item["status"] == "EVIDENCE_AVAILABLE" for item in research.values())
    return {
        "schema_version": LAYERED_EVALUATION_SCHEMA,
        "report_id": _canonical_hash(report_identity),
        "run_id": run_id,
        "dataset_mode": mode.value,
        "tradability_claim": "HISTORICAL_EXECUTION_ELIGIBLE" if mode is DatasetMode.AS_OBSERVED else "NOT_HISTORICALLY_TRADABLE",
        "versions": versions,
        "input_hash": input_hash,
        "registered_layers": list(EXPERIMENT_LAYERS),
        "reported_layers": list(EXPERIMENT_LAYERS),
        "decision_identities": {item["decision_id"]: item["decision_identity"] for item in decisions},
        "split": split,
        "scorecards": {"research": research, "account": account},
        "strategy_breakdown": strategy_breakdown,
        "llm_veto_evaluation": {
            "signal_level": {"vetoed_count": len(vetoed), "vetoed_ids": [item["decision_id"] for item in vetoed]},
            "account_level": {
                "basis": "ACTUAL_FILLS_ONLY",
                "counterfactual_addition_allowed": False,
                "note": "capital opportunity substitution requires a portfolio replay and is not added to signal returns",
            },
        },
        "universe_reconciliation": reconciliation,
        "strategy_evidence_status": "STRATEGY_EVIDENCE_SUFFICIENT" if sufficient else "INSUFFICIENT_EVIDENCE",
        "production_config_change": False,
    }


__all__ = [
    "DatasetMode",
    "EvaluationContractError",
    "EXPERIMENT_LAYERS",
    "INPUT_SCHEMA",
    "LAYERED_EVALUATION_SCHEMA",
    "build_layered_evaluation",
    "decision_identity",
    "validate_config_change_proposal",
]
