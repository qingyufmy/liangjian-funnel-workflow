"""Research-only aggregates with explicit minute/plan/trade denominators."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from statistics import mean, median
from typing import Any

MIN_SAMPLE_SIZE = 20
BUY_ACTIONS = {"BUY", "BUY_SIGNAL", "PROBE_BUY"}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (ValueError, TypeError):
        return None
    return parsed if math.isfinite(parsed) else None


def _conditions(row: Mapping[str, Any], field: str) -> tuple[str, ...]:
    value = row.get(field) or []
    if isinstance(value, str):
        value = [value]
    return tuple(sorted({str(item) for item in value
                         if field != "removed_conditions" or str(item) not in {"DATA_BLOCK", "DATA_BLOCKED"}}))


def _trigger(row: Mapping[str, Any]) -> bool:
    return _valid_scenario(row) and not row.get("data_block") and str(row.get("action")) in BUY_ACTIONS


def _scenario_status(row: Mapping[str, Any]) -> str:
    ablation = row.get("ablation")
    return str(ablation.get("status", "UNKNOWN")) if isinstance(ablation, Mapping) else "OK"


def _valid_scenario(row: Mapping[str, Any]) -> bool:
    return _scenario_status(row) == "OK"


def _attribution_version(row):
    return (row.get("attribution") or {}).get("version", "LEGACY_UNVERSIONED")


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    return {"sample_count": len(values), "mean": mean(values) if values else None,
            "median": median(values) if values else None, "min": min(values) if values else None,
            "max": max(values) if values else None}


def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Group rows without promoting correlated minutes to independent trades.

    Input ``failed_conditions`` must contain engine-owned atomic IDs: derived
    composite parent gates are intentionally not inferred from reason text.
    Each plan/scenario uses its earliest trigger, even if its outcome is absent.
    """
    by_group = defaultdict(list)
    for row in rows:
        removed = _conditions(row, "removed_conditions")
        by_group[(str(row.get("profile", "UNKNOWN")), str(row.get("scenario", "BASELINE")), removed,
                  _attribution_version(row))].append(row)
    groups = []
    for (profile, scenario, removed, attribution_version), items in sorted(by_group.items()):
        first: dict[str, Mapping[str, Any]] = {}
        minute_keys = set()
        plans = {str(item.get("plan_id")) for item in items if _valid_scenario(item)}
        status_windows: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for item in items:
            status_windows[_scenario_status(item)].add((str(item.get("plan_id")), str(item.get("minute"))))
        for item in sorted(items, key=lambda item: str(item.get("minute", ""))):
            if _trigger(item):
                pid = str(item.get("plan_id"))
                minute_keys.add((pid, str(item.get("minute"))))
                first.setdefault(pid, item)
        trades = [item["outcome"] for item in first.values()
                  if isinstance(item.get("outcome"), Mapping)
                  and item["outcome"].get("fill_status") == "FILLED"
                  and item["outcome"].get("return_kind") == "RESEARCH_COUNTERFACTUAL"]
        metrics = {key: [_number(trade.get(key)) for trade in trades]
                   for key in ("stop_r", "forward_r_t1", "forward_r_t3", "forward_r_t5",
                               "t1_close_return", "t3_close_return", "t5_close_return", "mfe", "mae")}
        metrics = {key: [value for value in values if value is not None] for key, values in metrics.items()}
        primary_returns = metrics["t5_close_return"]
        executed_sequences = {(str(item.get("plan_id")), str(item.get("minute"))):
            (item.get("ablation") or {}).get("sequence_candidate_count") for item in items
            if (item.get("ablation") or {}).get("sequence_evidence_scope") == "ACTUAL_ISOLATED_STRATEGY_HELPER_CALL"}
        opportunity: dict[str, float] = {}
        for item in items:
            pid = str(item.get("plan_id"))
            value = _number(item.get("opportunity_cost"))
            if _valid_scenario(item) and pid not in first and value is not None:
                opportunity.setdefault(pid, value)
        groups.append({
            "profile": profile, "scenario": scenario,
            "attribution_version":attribution_version,
            "variant":items[0].get("variant"),
            "scan_contract":{key:(items[0].get("ablation") or {}).get(key) for key in (
                "sequence_scan_model","sequence_window","sequence_pullback_length",
                "sequence_confirmation_anchor")},
            "sequence_helper_executed_window_count":len(executed_sequences),
            "sequence_actual_candidate_count_distribution":dict(sorted(Counter(
                str(count) for count in executed_sequences.values()).items())),
            "condition": "+".join(removed) if removed else str(items[0].get("condition") or "BASELINE"),
            "removed_conditions": list(removed), "independent_plan_count": len(plans),
            "input_plan_count": len({str(item.get("plan_id")) for item in items}),
            "valid_window_count": len(status_windows["OK"]),
            "unsupported_window_count": len(status_windows["UNSUPPORTED"]),
            "data_limited_window_count": len(status_windows["DATA_LIMITED"]),
            "other_invalid_window_count": sum(len(windows) for status, windows in status_windows.items()
                                              if status not in {"OK", "UNSUPPORTED", "DATA_LIMITED"}),
            "minute_trigger_count": len(minute_keys), "triggered_plan_count": len(first),
            "trade_sample_count": len(trades), "return_sample_count": len(primary_returns),
            "status": "SUFFICIENT_SAMPLE" if len(primary_returns) >= MIN_SAMPLE_SIZE else "INSUFFICIENT_EVIDENCE",
            "win_rate": sum(value > 0 for value in primary_returns) / len(primary_returns) if len(primary_returns) >= MIN_SAMPLE_SIZE else None,
            "win_rate_metric": "t5_close_return",
            "win_rate_denominator": "FIRST_TRIGGER_PER_PLAN_SCENARIO_WITH_FILLED_ENTRY_AND_OBSERVED_T5_CLOSE",
            "return_distributions": {key: {**_distribution(values),
                "status": "SUFFICIENT_SAMPLE" if len(values) >= MIN_SAMPLE_SIZE else "INSUFFICIENT_EVIDENCE"}
                for key, values in metrics.items()},
            "untriggered_opportunity_cost": _distribution(list(opportunity.values())),
            "return_kind": "RESEARCH_COUNTERFACTUAL",
        })
    baseline: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in rows:
        if str(row.get("scenario")) == "BASELINE":
            baseline.setdefault((str(row.get("profile", "UNKNOWN")), str(row.get("plan_id")),
                                 str(row.get("minute")), _attribution_version(row)), row)
    by_profile: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    unresolved_counts = defaultdict(int)
    for (profile, _, _, version), row in baseline.items():
        if (_valid_scenario(row) and not row.get("data_block")
                and str(row.get("action")) not in {"DATA_BLOCK", "DATA_BLOCKED"}):
            if (row.get("attribution") or {}).get("unresolved_parents"):
                unresolved_counts[(profile, version)] += 1
            else:
                by_profile[(profile,version)].append(row)
    rates = []
    for (profile, version), observations in sorted(by_profile.items()):
        conditions = {condition for row in observations for condition in _conditions(row, "failed_conditions")
                      if condition not in {"DATA_BLOCK", "DATA_BLOCKED"}}
        conditions.update(condition for row in rows if str(row.get("profile", "UNKNOWN")) == profile
                          and _attribution_version(row) == version
                          for condition in _conditions(row, "removed_conditions"))
        for condition in sorted(conditions):
            numerator = sum(_conditions(row, "failed_conditions") == (condition,) for row in observations)
            denominator = len(observations)
            rates.append({"profile": profile, "condition": condition, "numerator": numerator,
                "attribution_version":version,
                "interpretation":"UNIQUE_UNMET_OBSERVATION_NOT_CAUSAL_STRATEGY_KILL_RATE",
                "unresolved_parent_window_count":unresolved_counts[(profile,version)],
                "denominator": denominator, "denominator_basis": "UNIQUE_VALID_BASELINE_PLAN_MINUTES_EXCLUDING_DATA_BLOCK_AND_NON_OK_ABLATION",
                "kill_rate": numerator / denominator if denominator else None,
                "failed_condition_basis": "ENGINE_ATOMIC_IDS_DEDUPLICATED_NO_COMPOSITE_PARENT_GATES" if version != "LEGACY_UNVERSIONED"
                else "LEGACY_UNVERSIONED_INPUT_NOT_NEW_ATOMIC_EVIDENCE"})
    return {"return_kind": "RESEARCH_COUNTERFACTUAL", "sample_minimum": MIN_SAMPLE_SIZE,
            "groups": groups, "condition_kill_rates": rates,
            "unresolved_atomic_attribution":[{"profile":profile, "attribution_version":version,
                "window_count":count} for (profile,version),count in sorted(unresolved_counts.items())],
            "data_block_policy": "EVIDENCE_FAILURE_NOT_A_RELAXABLE_CONDITION"}


__all__ = ["summarize"]
