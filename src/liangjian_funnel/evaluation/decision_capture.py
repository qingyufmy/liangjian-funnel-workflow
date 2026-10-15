"""Compact current-run gate evidence, independent of the optional factor store.

This projection is observation-only. It never builds a candidate domain from
plans and never translates an unobserved decision into a strategy rejection.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

CAPTURE_VERSION = "stage-outcome-capture/1"
_FIELDS = (
    "symbol", "status", "reason_codes", "all_failed_gates", "first_blocking_gate",
    "selection_basis", "source_basis", "score", "structural_score", "theme_score", "a3_score",
    "industry", "industry_code", "industry_name", "ths_industry", "market_cap", "market_value",
    "volatility", "volatility_20d", "candidate_origin", "sent_to_llm",
)


def _copy(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def capture_gate(*, run_id: str, lane_id: str, stage: str, decisions: Sequence[Mapping[str, Any]],
                 snapshot_id: str, snapshot_hash: str, as_of: str | None) -> dict[str, Any]:
    rows = []
    for source in decisions:
        row = {key: _copy(source[key]) for key in _FIELDS if key in source}
        checks = source.get("gate_results")
        if isinstance(checks, Mapping):
            row["gate_results"] = {
                str(key): {k: _copy(value[k]) for k in ("available", "met", "reason", "status") if k in value}
                if isinstance(value, Mapping) else _copy(value)
                for key, value in checks.items()
            }
            row["gate_results_source_hash"] = _hash(checks)
        row["gate_row_hash"] = _hash(row)
        rows.append(row)
    return {"schema_version": CAPTURE_VERSION, "run_id": run_id, "lane_id": lane_id,
        "stage": stage, "snapshot_id": snapshot_id, "snapshot_hash": snapshot_hash,
        "as_of": as_of, "rows": rows, "rows_hash": _hash(rows)}


def label_rows(capture: Mapping[str, Any], *, stage: str, output: Mapping[str, Any],
               approved_symbols: set[str], a3_eligible: set[str],
               stage_status: str) -> tuple[list[dict[str, Any]], list[str]]:
    pools: dict[str, str] = {}
    # First pool wins; this agrees with focus-over-watch canonical handoff.
    names = {"A1": ("active_research_pool", "monitor_pool", "rejected_candidates"),
        "A2": ("focus_pool", "watch_only_pool", "crowded_pool", "low_identity_pool",
               "outside_rotation_pool", "rejected_candidates"),
        "A3": ("core_watch_pool", "secondary_watch_pool", "rejected_candidates")}[stage]
    for name in names:
        for item in output.get(name, ()) if isinstance(output.get(name), list) else ():
            if isinstance(item, Mapping) and item.get("symbol"):
                pools.setdefault(str(item["symbol"]).strip().upper(), name)
    decisions, unknown = [], []
    output_hash = _hash(output)
    for source in capture["rows"]:
        row = _copy(source)
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        pool = pools.get(symbol)
        status = str(row.get("status") or "").strip().upper()
        reasons = row.get("reason_codes") or ()
        failed = row.get("all_failed_gates") or ()
        reasons = [reasons] if isinstance(reasons, str) else reasons
        failed = [failed] if isinstance(failed, str) else failed
        reasons = {str(value).strip().upper() for value in reasons}
        failed = {str(value).strip().upper() for value in failed}
        truncated = "SENT_TO_LLM" in failed or bool({"A2_NOT_SENT_TO_LLM", "A1_NOT_SENT_TO_LLM"} & set(reasons))
        if pool is None and status not in {"LOCAL_REJECTED", "REJECTED", "OUTSIDE_G0", "OUTSIDE_THEME"} and not truncated:
            unknown.append(symbol)
            continue
        passed = pool in {"focus_pool", "watch_only_pool"} if stage == "A2" else symbol in approved_symbols
        row["decision"] = "PASSED" if passed else "NOT_SENT_TO_LLM" if truncated else "REJECTED"
        row["metadata"] = {
            "label_contract_version": CAPTURE_VERSION,
            "stage_review_status": stage_status,
            "stage_pass_semantics": "RETAINED_FOCUS_OR_WATCH" if stage == "A2" else "PRIMARY_STAGE_SELECTION",
            "stage_disposition": pool or ("NOT_SENT_TO_LLM" if truncated else status),
            "a2_focus": pool == "focus_pool" if stage == "A2" else None,
            "a2_watch_only": pool == "watch_only_pool" if stage == "A2" else None,
            "a3_eligible": symbol in a3_eligible if stage == "A2" else None,
            "a3_core_watch": pool == "core_watch_pool" if stage == "A3" else None,
            "a3_secondary_watch": pool == "secondary_watch_pool" if stage == "A3" else None,
            "gate_run_id": capture["run_id"], "gate_lane_id": capture["lane_id"],
            "gate_stage": capture["stage"], "gate_snapshot_hash": capture["snapshot_hash"],
            "gate_rows_hash": capture["rows_hash"], "gate_row_hash": source["gate_row_hash"],
            "gate_as_of": capture["as_of"], "stage_output_hash": output_hash,
        }
        decisions.append(row)
    return decisions, sorted(set(unknown))
