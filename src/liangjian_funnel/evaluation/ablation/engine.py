"""Counterfactual evaluation in isolated production-function namespaces.

No global monkeypatches, signal fabrication, external I/O, or production
threshold writes. AST substitutions are scoped to exact function/expression
locations and fail closed if the production implementation changes.
"""
from __future__ import annotations

import ast
import hashlib
import inspect
import itertools
import math
import textwrap
from copy import deepcopy
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from types import FunctionType

from ...runtime import strategies as production
from .conditions import _TREND_SEQUENCE, atomic_failures, condition_catalog


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


class _Replacements(ast.NodeTransformer):
    def __init__(self, replacements):
        self.replacements = replacements
        self.matches = {key: 0 for key in replacements}

    def visit(self, node):
        key = ast.dump(node, include_attributes=False)
        if key in self.replacements:
            self.matches[key] += 1
            return ast.copy_location(deepcopy(self.replacements[key]), node)
        return super().visit(node)


def _rewrite(function, namespace, changes):
    """Replace exactly one AST location per supplied original expression."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    replacements = {ast.dump(ast.parse(old, mode="eval").body, include_attributes=False):
                    ast.parse(new, mode="eval").body for old, new in changes}
    transform = _Replacements(replacements)
    tree = transform.visit(tree)
    if any(count != 1 for count in transform.matches.values()):
        raise RuntimeError(f"PRODUCTION_PREDICATE_DRIFT:{function.__name__}:{transform.matches}")
    ast.fix_missing_locations(tree)
    exec(compile(tree, inspect.getsourcefile(function) or "<ablation>", "exec"), namespace)


def _isolated_namespace():
    namespace = dict(vars(production))
    # Every module function must resolve helpers in this new mapping, including
    # evaluate_strategy -> evaluate_a4_plan -> protective/entry helpers.
    for name, value in vars(production).items():
        if isinstance(value, FunctionType) and value.__module__ == production.__name__:
            clone = FunctionType(value.__code__, namespace, value.__name__, value.__defaults__, value.__closure__)
            clone.__kwdefaults__ = value.__kwdefaults__
            clone.__annotations__ = value.__annotations__
            namespace[name] = clone
    return namespace


_PREDICATES = {
    "TREND_15M_PRESSURE_EASING": ("_evaluate_trend", "prior15 is not None and latest15.low + _EPSILON >= prior15.low if versioned_entry else _fifteen_not_weak(latest15, prior15)"),
    "TREND_5M_REVERSAL_CONFIRMATION": ("_evaluate_trend", "prior5 is not None and latest5.close >= latest5.open and latest5.close >= prior5.close and latest5.low + _EPSILON >= prior5.low and (vwap is None or latest5.close >= vwap)"),
    "DAILY_MA5_ABOVE_MA20": ("_evaluate_520", "ma5 >= ma20"),
    "DAILY_CLOSE_ABOVE_MA20": ("_evaluate_520", "daily_close is None or daily_close > ma20"),
    "MA520_15M_STABLE": ("_evaluate_520", "_fifteen_not_weak(latest15, prior15)"),
    "MA520_5M_HIGHER_LOW": ("_evaluate_520", "prior5 is not None and latest5.low + _EPSILON >= prior5.low"),
    "MA520_5M_VWAP_RECLAIM": ("_evaluate_520", "vwap is not None and latest5.close + _EPSILON >= vwap"),
    "MA520_TWO_CLOSED_5M_CONFIRMATIONS": ("_evaluate_520", "confirmations >= 2"),
    "LEADER_15M_NOT_WEAK": ("_evaluate_leader", "_fifteen_not_weak(latest15, prior15)"),
    "LEADER_5M_TRIGGER": ("_evaluate_leader", "divergence or reseal or breakout"),
}


@lru_cache(maxsize=256)
def _build_evaluator(disabled, sequence_window, sequence_scan_model):
    namespace = _isolated_namespace()
    changed = {}
    for condition in disabled:
        if condition in _PREDICATES:
            function, original = _PREDICATES[condition]
            changed.setdefault(function, []).append((original, "True"))
    # The leader trigger expression occurs twice (diagnostics + decision).
    # Match each separately by the enclosing bool expression instead.
    if "LEADER_5M_TRIGGER" in disabled:
        changed["_evaluate_leader"] = [item for item in changed["_evaluate_leader"] if item[0] != "divergence or reseal or breakout"]
        changed["_evaluate_leader"].extend([
            ("not (divergence or reseal or breakout)", "False"),
            ("leader_valid and stable and (divergence or reseal or breakout) and not any(item in veto for item in ('LEADER_CLIMAX_NO_NEW_ENTRY',)) and not (board_count is not None and board_count <= 1)",
             "leader_valid and stable and True and not any(item in veto for item in ('LEADER_CLIMAX_NO_NEW_ENTRY',)) and not (board_count is not None and board_count <= 1)"),
        ])
    if "TREND_5M_REVERSAL_CONFIRMATION" in disabled:
        changed.setdefault("_evaluate_trend", []).append(("all(confirmation.values())", "True"))
    if "A4_LIVE_NO_CHASE_ACCEPTED" in disabled:
        changed.setdefault("_apply_live_entry_geometry", []).append(("no_chase is not None and current_price > no_chase + _EPSILON", "False"))
    for name, changes in changed.items():
        _rewrite(getattr(production, name), namespace, changes)
    original_sequence = namespace["_trend_entry_sequence"]

    def sequence(five, vwap):
        if sequence_scan_model == "ORDERED_SUBSEQUENCE_LATEST_CONFIRM":
            window = five[-sequence_window:]
            choices = [original_sequence([window[a], window[b], window[c], window[-1]], vwap)
                       for a, b, c in itertools.combinations(range(len(window)-1), 3)]
            if not choices:
                choices = [original_sequence(five, vwap)]
        else:
            # All four phase bars are adjacent in the production aggregate
            # series. Requiring phase four to be the latest closed bar leaves
            # exactly the last-four window, independently of lookback N.
            window = five[-sequence_window:]
            choices = [original_sequence(window[start:start+4], vwap)
                       for start in range(len(window)-3)
                       if window[start+3].end == five[-1].end]
            if not choices:
                choices = [original_sequence(five, vwap)]
        for result in choices:
            if len(five) >= 4:
                for condition in disabled:
                    if condition in _TREND_SEQUENCE:
                        result[condition] = True
        # A complete match must come from one sequence, never combine passes
        # from different candidate sequences. Latest closed bar is confirmation.
        return next((result for result in choices if all(result.values())),
                    max(choices, key=lambda result: sum(result.values())))
    namespace["_trend_entry_sequence"] = sequence
    for condition in ("TREND_VOLUME_NOT_OVERHEATED", "MA520_VOLUME_NOT_OVERHEATED"):
        if condition in disabled:
            namespace["_volume_not_overheated"] = lambda plan, five: (True, "VOLUME_NOT_OVERHEATED")
    if "A3_PULLBACK_ZONE" in disabled or "A3_TRIGGER_ZONE" in disabled:
        namespace["_price_zone_met"] = lambda plan, price: True
    if "LIVE_MARKET_ENTRY_PERMISSION" in disabled:
        original_apply = namespace["_apply_live_market_gate"]
        def apply(decision, gate):
            # This changes only permission; the original readiness, date,
            # freshness and future-evidence checks already ran unchanged.
            research_gate = dict(gate)
            if gate.get("status") == "READY" and gate.get("state_status") == "READY" and gate.get("decision") == "BLOCK_NEW_ENTRY":
                research_gate["decision"] = "ALLOW"
            return original_apply(decision, research_gate)
        namespace["_apply_live_market_gate"] = apply
    return namespace["evaluate_strategy"]


@lru_cache(maxsize=1)
def _source_fingerprint():
    return hashlib.sha256(Path(inspect.getsourcefile(production)).read_bytes()).hexdigest()


def _zone_bounds(plan):
    zone = production._lookup(plan, ("trigger_zone",), ("entry_reference_zone",),
                              ("entry_zone",), ("pullback_zone",),
                              ("strategy_facts", "entry_reference_zone"))
    if isinstance(zone, dict):
        low = production._lookup(zone, ("low",), ("min",))
        high = production._lookup(zone, ("high",), ("max",))
    else:
        low, high = plan.get("trigger_low"), plan.get("trigger_high")
    return _number(low), _number(high)


def _research_sequence(five, vwap, *, model, k, n, zone, minutes, disabled):
    """One whole adjacent candidate owns every predicate and its protection.

    Confirm-within-N chooses the most recent *complete* passing candidate.
    Diagnostics for a failed request come from one candidate, never an OR of
    atomic passes at different times. Minute evidence is checked before entry.
    """
    window = list(five[-n:])
    size = k+3 if model == "TREND_PULLBACK_LEN_K" else 4
    starts = [len(window)-size] if model == "TREND_PULLBACK_LEN_K" else range(len(window)-size, -1, -1)
    candidates = []
    for start in starts:
        phases = window[start:start+size]
        baseline, reversal, confirm = phases[0], phases[-2], phases[-1]
        pullbacks = phases[1:-2]
        confirmation_vwap = (production._vwap([bar for bar in five if bar.end <= confirm.end])
                             if model == "TREND_CONFIRM_WITHIN_N" else vwap)
        conditions = production._trend_entry_sequence([baseline, pullbacks[-1], reversal, confirm], confirmation_vwap)
        contraction = all(0 < current.volume < previous.volume and current.close <= previous.close
                          for previous, current in zip([baseline]+pullbacks[:-1], pullbacks))
        conditions["TREND_PULLBACK_VOLUME_CONTRACTION"] = contraction
        for name in disabled:
            if name in _TREND_SEQUENCE:
                conditions[name] = True
        after = [bar for bar in minutes if bar.end > confirm.end]
        held = all(bar.low >= reversal.low and zone[0] <= bar.close <= zone[1] for bar in after)
        # Confirmation itself must be in the effective zone; otherwise a past
        # out-of-zone confirmation cannot be retroactively legitimised.
        held = held and zone[0] <= confirm.close <= zone[1]
        if model == "TREND_CONFIRM_WITHIN_N":
            conditions["TREND_POST_CONFIRM_LOW_AND_ZONE_HELD"] = held
        candidates.append({"phase_ends":[bar.end.isoformat() for bar in phases],
            "confirmation_end":confirm.end.isoformat(), "reversal_low":reversal.low,
            "confirmation_vwap":confirmation_vwap,
            "pullback_count":len(pullbacks), "conditions":conditions,
            "post_confirmation_minute_count":len(after),
            "post_confirmation_held":held})
    selected = next((candidate for candidate in candidates if all(candidate["conditions"].values())),
                    max(candidates, key=lambda candidate:sum(candidate["conditions"].values())))
    return dict(selected["conditions"]), {"model":model, "k":k, "n":n,
        "candidates":candidates, "selected_candidate":selected,
        "matched":all(selected["conditions"].values()),
        "evidence_scope":"ACTUAL_ISOLATED_STRATEGY_HELPER_CALL",
        "post_confirmation_policy":"EVERY_CLOSED_1M_LOW_GE_REVERSAL_LOW_AND_CLOSE_INSIDE_ZONE"}


def _condition_state(condition, result):
    """Project only computed predicates; a short-circuited gate is unknown."""
    if condition in result.get("unmet_conditions", ()):
        return "FAIL"
    if condition in result.get("met_conditions", ()):
        return "PASS"
    if condition == "LEADER_5M_TRIGGER" and set(result.get("met_conditions", ())) & {
        "LEADER_5M_DIVERGENCE_TO_STRENGTH", "LEADER_5M_RESEAL", "LEADER_FIRST_15M_BREAKOUT"
    }:
        return "PASS"
    if condition == "A4_LIVE_NO_CHASE_ACCEPTED" and result.get("live_no_chase_price") is not None:
        if any(str(reason).endswith("GEOMETRY_MISSING") for reason in result.get("reason_codes", ())):
            return "NOT_REACHED"
        return "FAIL" if "A4_LIVE_NO_CHASE_EXCEEDED" in result.get("reason_codes", ()) else "PASS"
    if condition == "LIVE_MARKET_ENTRY_PERMISSION" and result.get("action") in ("BUY_SIGNAL", "ADD_SIGNAL"):
        return "PASS"
    return "NOT_REACHED"


def evaluate_window(plan: dict, bars, *, now: datetime, decision_time: datetime,
                    market_context: dict, disabled: tuple[str, ...] = (),
                    variant: dict | None = None) -> dict:
    """Evaluate one frozen minute; failures retain the unmodified baseline.

    The dictionary contains the official decision fields plus ``counterfactual``
    and ``ablation``. Only status OK counterfactuals are valid research rows.
    Unsupported/data-limited requests are never reported as ablated samples.
    """
    bars = list(bars)
    baseline = production.evaluate_strategy(plan, bars, now=now, decision_time=decision_time,
                                             market_context=market_context).model_dump(mode="json")
    return _evaluate_with_baseline(plan, bars, now=now, decision_time=decision_time,
                                   market_context=market_context, baseline=baseline,
                                   disabled=disabled, variant=variant)


def _evaluate_with_baseline(plan, bars, *, now, decision_time, market_context,
                            baseline, disabled=(), variant=None):
    """Internal runner path: reuse an already audited formal baseline.

    The public evaluate_window always computes its own production baseline.
    This helper avoids repeating that identical pure call for each scenario.
    """
    disabled = tuple(dict.fromkeys(disabled))
    variant = dict(variant or {})
    sequence_window = variant.get("sequence_window", 4)
    sequence_scan_model = variant.get("sequence_scan_model", "EXACT_LAST4" if sequence_window == 4
                                     else "CONTIGUOUS4_LATEST_CONFIRM")
    aggregate_count = baseline.get("closed_5m_count", 0)
    sequence_candidates = 0
    if isinstance(sequence_window, int) and not isinstance(sequence_window, bool) and aggregate_count >= 4:
        sequence_candidates = math.comb(min(sequence_window, aggregate_count)-1, 3) if (
            sequence_scan_model == "ORDERED_SUBSEQUENCE_LATEST_CONFIRM" and sequence_window >= 4
        ) else 1
    catalog = {row["id"]: row for row in condition_catalog(plan.get("strategy_profile", ""))}
    metadata = {
        "status": "OK", "disabled": list(disabled), "variant": variant,
        "sequence_scan_model": sequence_scan_model,
        "sequence_window": sequence_window,
        "sequence_candidate_count": sequence_candidates,
        "sequence_confirmation_anchor": "LATEST_CLOSED_5M",
        "sequence_window_boundary": "Four adjacent phases with latest confirmation leave only the last-four candidate."
        if sequence_scan_model != "ORDERED_SUBSEQUENCE_LATEST_CONFIRM"
        else "Ordered phase indices may contain gaps; this is not a contiguous four-bar scan.",
        "sequence_scan_applicable": str(plan.get("strategy_profile", "")).upper() == "TREND_MA5"
        and plan.get("trend_entry_rule_version") == "trend-ma5/2",
        "source_sha256": _source_fingerprint(),
        "baseline_action": baseline["action"], "baseline_state": baseline["state"],
        "changes": {}, "predicate_substitutions": [],
        "evaluation_mode": "FORMAL_BASELINE" if not disabled and not variant else "ISOLATED_REEVALUATION",
    }
    requested_model = variant.get("research_sequence_model")
    if requested_model in ("TREND_PULLBACK_LEN_K", "TREND_CONFIRM_WITHIN_N"):
        requested_k = variant.get("pullback_length", 1)
        requested_n = (requested_k+3 if isinstance(requested_k, int) and not isinstance(requested_k, bool)
                       else None) if requested_model == "TREND_PULLBACK_LEN_K" else variant.get("confirmation_window")
        metadata.update(sequence_scan_model=requested_model, sequence_window=requested_n,
            sequence_pullback_length=requested_k, sequence_candidate_count=0,
            sequence_confirmation_anchor="LATEST_CLOSED_5M" if requested_model == "TREND_PULLBACK_LEN_K" else "MOST_RECENT_VALID_CONTIGUOUS_CONFIRM_WITHIN_N",
            sequence_window_boundary="Adjacent phases only; post-confirmation protection cannot be removed.",
            sequence_evidence={"evidence_scope":"NOT_REACHED", "candidates":[], "matched":None})
    def finish(result, status="OK", reason=None):
        metadata["status"] = status
        if reason:
            metadata["reason"] = reason
        if "condition_results" not in metadata:
            metadata["condition_results"] = {
                name: {"baseline": _condition_state(name, baseline),
                       "counterfactual": _condition_state(name, result)} for name in catalog
            }
        result = dict(result)
        metadata["atomic_attribution"] = atomic_failures(result, versioned_trend=(
            str(plan.get("strategy_profile", "")).upper() == "TREND_MA5"
            and plan.get("trend_entry_rule_version") == "trend-ma5/2"))
        result["counterfactual"] = bool(disabled or variant)
        result["ablation"] = metadata
        return result
    if not disabled and not variant:
        return finish(baseline)
    invalid = [name for name in disabled if name not in catalog or not catalog[name]["supported"]]
    if invalid or len(disabled) > 2:
        return finish(baseline, "UNSUPPORTED", "Unsupported conditions or more than two removals: " + ",".join(invalid))
    if baseline["state"] == "DATA_BLOCKED":
        return finish(baseline, "DATA_LIMITED", "Baseline data block is never ablated")
    if set(variant) - {"sequence_window", "sequence_scan_model", "zone_model", "atr_multiplier",
                       "research_sequence_model", "pullback_length", "confirmation_window"}:
        return finish(baseline, "UNSUPPORTED", "Unknown variant keys")
    window = variant.get("sequence_window", 4)
    if not isinstance(window, int) or isinstance(window, bool) or window not in (4, 6, 8):
        return finish(baseline, "UNSUPPORTED", "sequence_window must be 4, 6, or 8")
    if sequence_scan_model not in ("EXACT_LAST4", "CONTIGUOUS4_LATEST_CONFIRM", "ORDERED_SUBSEQUENCE_LATEST_CONFIRM"):
        return finish(baseline, "UNSUPPORTED", "Unknown sequence_scan_model")
    if sequence_scan_model == "EXACT_LAST4" and window != 4:
        return finish(baseline, "UNSUPPORTED", "EXACT_LAST4 requires sequence_window=4")
    versioned = str(plan.get("strategy_profile", "")).upper() == "TREND_MA5" and plan.get("trend_entry_rule_version") == "trend-ma5/2"
    research_model = variant.get("research_sequence_model")
    if research_model is not None and research_model not in ("TREND_PULLBACK_LEN_K", "TREND_CONFIRM_WITHIN_N"):
        return finish(baseline, "UNSUPPORTED", "Unknown research_sequence_model")
    if research_model and (not versioned or set(variant) & {"sequence_window", "sequence_scan_model"}):
        return finish(baseline, "UNSUPPORTED", "Research sequence requires trend-ma5/2 and its own explicit window contract")
    k, n = variant.get("pullback_length", 1), variant.get("confirmation_window", 4)
    if research_model == "TREND_PULLBACK_LEN_K":
        if isinstance(k, bool) or k not in (1,2,3) or not isinstance(k, int) or "confirmation_window" in variant:
            return finish(baseline, "UNSUPPORTED", "TREND_PULLBACK_LEN_K requires integer k=1/2/3 only")
        n = k+3
    elif research_model == "TREND_CONFIRM_WITHIN_N":
        if isinstance(n, bool) or n not in (6,8) or not isinstance(n, int) or "pullback_length" in variant:
            return finish(baseline, "UNSUPPORTED", "TREND_CONFIRM_WITHIN_N requires integer N=6/8 only")
    elif set(variant) & {"pullback_length", "confirmation_window"}:
        return finish(baseline, "UNSUPPORTED", "K/N requires its explicit research_sequence_model")
    if research_model:
        metadata.update(sequence_scan_model=research_model, sequence_window=n,
            sequence_pullback_length=k, sequence_candidate_count=0,
            sequence_confirmation_anchor="LATEST_CLOSED_5M" if research_model == "TREND_PULLBACK_LEN_K" else "MOST_RECENT_VALID_CONTIGUOUS_CONFIRM_WITHIN_N",
            sequence_window_boundary="Adjacent phases only; no ordered-subsequence gaps. Post-confirmation minute protection cannot be removed.",
            sequence_evidence={"evidence_scope":"NOT_REACHED", "candidates":[], "matched":None})
    if versioned and "TREND_5M_REVERSAL_CONFIRMATION" in disabled:
        return finish(baseline, "UNSUPPORTED", "Derived sequence aggregate cannot replace four atomic removals")
    if (set(disabled) & set(_TREND_SEQUENCE) or set(variant) & {"sequence_window", "sequence_scan_model"}) and not versioned:
        return finish(baseline, "UNSUPPORTED", "Sequence experiment requires frozen trend-ma5/2 route")
    if set(disabled) & set(_TREND_SEQUENCE) and baseline.get("closed_5m_count", 0) < 4:
        return finish(baseline, "DATA_LIMITED", "At least four closed 5m bars required")
    # Missing preceding bars/VWAP are evidence limits, not failed predicates.
    if set(disabled) & {"MA520_5M_HIGHER_LOW", "MA520_TWO_CLOSED_5M_CONFIRMATIONS",
                        "TREND_5M_REVERSAL_CONFIRMATION"} and baseline.get("closed_5m_count", 0) < 2:
        return finish(baseline, "DATA_LIMITED", "Missing preceding closed 5m evidence")
    if set(disabled) & {"MA520_5M_VWAP_RECLAIM", "TREND_VWAP_RECLAIMED"}:
        normalized = production._normalize_bars(bars, plan)
        five, _ = production._aggregate_sessions(normalized, as_of=now)
        if production._vwap(five) is None:
            return finish(baseline, "DATA_LIMITED", "Missing actual session VWAP evidence")
    if "TREND_15M_PRESSURE_EASING" in disabled and versioned and baseline.get("closed_15m_count", 0) < 2:
        return finish(baseline, "DATA_LIMITED", "Versioned trend requires preceding closed 15m evidence")
    if "LIVE_MARKET_ENTRY_PERMISSION" in disabled:
        payload = {**plan, "market_context": market_context}
        gate = production._live_market_gate(payload, decision_time)
        if gate.get("status") != "READY" or gate.get("state_status") != "READY":
            return finish(baseline, "DATA_LIMITED", "Market permission experiment requires valid frozen READY evidence")
    low, high = _zone_bounds(plan)
    if ("A3_PULLBACK_ZONE" in disabled or "A3_TRIGGER_ZONE" in disabled) and (low is None or high is None or high < low):
        return finish(baseline, "DATA_LIMITED", "Missing/invalid frozen zone cannot be removed")
    daily = production._daily_context(plan)
    if set(disabled) & {"DAILY_MA5_ABOVE_MA20", "DAILY_CLOSE_ABOVE_MA20"}:
        needed = ("ma5", "ma20", "close")
        if any(_number(daily.get(name)) is None for name in needed):
            return finish(baseline, "DATA_LIMITED", "Missing frozen daily price/averages")
    if "A4_LIVE_NO_CHASE_ACCEPTED" in disabled and production._number(production._lookup(plan, ("no_chase",), ("no_chase_price",), ("max_chase_price",))) is None:
        return finish(baseline, "DATA_LIMITED", "Missing frozen no-chase price")
    research_plan = deepcopy(plan)
    zone_model = variant.get("zone_model", "CURRENT")
    if zone_model not in ("CURRENT", "MA5_ATR", "REALTIME_MA5_SHIFT"):
        return finish(baseline, "UNSUPPORTED", "Unknown zone_model")
    if "atr_multiplier" in variant and zone_model != "MA5_ATR":
        return finish(baseline, "UNSUPPORTED", "atr_multiplier only applies to MA5_ATR")
    if zone_model == "MA5_ATR":
        ma5, atr = _number(daily.get("ma5")), _number(daily.get("atr14"))
        multiplier = variant.get("atr_multiplier")
        if isinstance(multiplier, bool) or multiplier not in (.5, 1.0, 1.5):
            return finish(baseline, "UNSUPPORTED", "atr_multiplier must be 0.5, 1.0, or 1.5")
        if ma5 is None or atr is None or ma5 <= 0 or atr <= 0:
            return finish(baseline, "DATA_LIMITED", "Missing valid frozen daily MA5/ATR14")
        research_plan["trigger_zone"] = {"low": ma5-multiplier*atr, "high": ma5+multiplier*atr}
    elif zone_model == "REALTIME_MA5_SHIFT":
        prior = daily.get("previous_daily_closes")
        if not isinstance(prior, (list, tuple)) or len(prior) < 4 or any(_number(v) is None or _number(v) <= 0 for v in prior[-4:]):
            return finish(baseline, "DATA_LIMITED", "Real-time daily MA5 needs four frozen preceding daily closes")
        if low is None or high is None or low > high or baseline.get("reference_price") is None:
            return finish(baseline, "DATA_LIMITED", "Missing original zone/current closed-minute price")
        center = (sum(float(v) for v in prior[-4:]) + baseline["reference_price"])/5
        width = (high-low)/2
        research_plan["trigger_zone"] = {"low": center-width, "high": center+width}
    if zone_model != "CURRENT":
        metadata["effective_zone"] = research_plan["trigger_zone"]
    research_minutes = None
    if research_model:
        research_zone = _zone_bounds(research_plan)
        if any(value is None for value in research_zone) or research_zone[0] > research_zone[1]:
            return finish(baseline, "DATA_LIMITED", "Research sequence requires valid effective zone")
        research_minutes = production._normalize_bars(bars, research_plan)
        research_five, _ = production._aggregate_sessions(research_minutes, as_of=now)
        if len(research_five) < n:
            return finish(baseline, "DATA_LIMITED", f"Research sequence requires {n} complete closed 5m bars")
        ends = [bar.end for bar in research_five[-n:]]
        if any(right-left != timedelta(minutes=5) for left,right in zip(ends, ends[1:])):
            return finish(baseline, "DATA_LIMITED", "Missing/nonadjacent 5m evidence; no gap or session-boundary stitching")
        start = ends[0]-timedelta(minutes=4)
        latest = research_minutes[-1].end
        present = {bar.end for bar in research_minutes if start <= bar.end <= latest}
        required = int((latest-start).total_seconds()/60)+1
        if len(present) != required:
            return finish(baseline, "DATA_LIMITED", "Missing post-confirmation closed 1m evidence")
        metadata["sequence_potential_candidate_count"] = 1 if research_model == "TREND_PULLBACK_LEN_K" else n-3
    first_unmet = next(iter(baseline.get("unmet_conditions", ())), None)
    equivalent = (disabled and not variant
                  and all(_condition_state(name, baseline) == "PASS" for name in disabled)
                  and (baseline.get("action") in ("BUY_SIGNAL", "ADD_SIGNAL")
                       or first_unmet is not None and first_unmet not in disabled))
    try:
        if equivalent:
            result = {key:value for key,value in baseline.items() if key not in ("ablation", "counterfactual")}
            metadata["evaluation_mode"] = "FORMAL_BASELINE_EQUIVALENT_NO_FAILED_TARGET"
            metadata["equivalence_evidence"] = {"targets":{name:"COMPUTED_PASS" for name in disabled},
                "first_unmet_condition":first_unmet, "other_inputs":"IDENTICAL_NO_PARAMETER_VARIANT"}
        else:
            evaluator = (_build_evaluator.__wrapped__(disabled, window, sequence_scan_model)
                         if research_model else _build_evaluator(disabled, window, sequence_scan_model))
            if research_model:
                # Fresh namespace and call-local trace: no cached mutable queue
                # or production-global monkeypatch, including concurrent runs.
                def research_sequence(five, vwap):
                    conditions, trace = _research_sequence(five, vwap, model=research_model,
                        k=k, n=n, zone=research_zone, minutes=research_minutes, disabled=disabled)
                    metadata["sequence_evidence"] = trace
                    metadata["sequence_candidate_count"] = len(trace["candidates"])
                    return conditions
                evaluator.__globals__["_trend_entry_sequence"] = research_sequence
            result = evaluator(research_plan, bars, now=now, decision_time=decision_time,
                               market_context=deepcopy(market_context)).model_dump(mode="json")
    except RuntimeError as exc:
        return finish(baseline, "UNSUPPORTED", str(exc))
    metadata["predicate_substitutions"] = [] if equivalent else list(disabled)
    metadata["changes"] = {key: {"baseline": baseline.get(key), "counterfactual": value}
                           for key, value in result.items() if baseline.get(key) != value}
    metadata["condition_applicability"] = {
        name: "NOT_REACHED" if _condition_state(name, baseline) == "NOT_REACHED" else "EVALUATED"
        for name in disabled
    }
    metadata["condition_results"] = {
        name: {"baseline": _condition_state(name, baseline),
               "counterfactual": _condition_state(name, result)} for name in catalog
    }
    # The OR trigger has no production met-label for the combined predicate.
    # Record the actual isolated override only when its route was reached.
    if "LEADER_5M_TRIGGER" in disabled and metadata["condition_results"]["LEADER_5M_TRIGGER"]["baseline"] != "NOT_REACHED":
        metadata["condition_results"]["LEADER_5M_TRIGGER"]["counterfactual"] = "PASS"
    return finish(result)
