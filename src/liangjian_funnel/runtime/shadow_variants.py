"""Isolated shadow evaluation. No storage, provider, activation or notification."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
import math
import re
import threading
import time
import uuid

from ..evaluation.ablation import engine

VARIANT_SET_VERSION = "a4-shadow-variants/1"
SHADOW_INPUT_SCHEMA = "a4-shadow-inputs/1"


@dataclass(frozen=True)
class ShadowVariant:
    variant_id: str
    zone_model: str
    matrix_scenario: str
    atr_multiplier: float | None = None
    research_sequence_model: str | None = None
    sequence_parameter: int | None = None

    def parameters(self):
        result = {"zone_model": self.zone_model}
        if self.atr_multiplier is not None:
            result["atr_multiplier"] = self.atr_multiplier
        if self.research_sequence_model:
            result["research_sequence_model"] = self.research_sequence_model
            result["confirmation_window" if self.research_sequence_model == "TREND_CONFIRM_WITHIN_N" else "pullback_length"] = self.sequence_parameter
        return result


SHADOW_VARIANTS_V1 = (
    ShadowVariant("V1", "MA5_ATR", "SCAN:MA5_ATR:1", 1.),
    ShadowVariant("V2", "MA5_ATR", "SCAN:MA5_ATR:1.5", 1.5),
    ShadowVariant("V3", "REALTIME_MA5_SHIFT", "SCAN:REALTIME_MA5_SHIFT"),
    ShadowVariant("V4", "MA5_ATR", "SCAN:CROSS:MA5_ATR:1:TREND_CONFIRM_WITHIN_N:6", 1., "TREND_CONFIRM_WITHIN_N", 6),
    ShadowVariant("V5", "MA5_ATR", "SCAN:CROSS:MA5_ATR:1:TREND_PULLBACK_LEN_K:2", 1., "TREND_PULLBACK_LEN_K", 2),
    ShadowVariant("V6", "REALTIME_MA5_SHIFT", "SCAN:CROSS:REALTIME_MA5_SHIFT:TREND_CONFIRM_WITHIN_N:6", None, "TREND_CONFIRM_WITHIN_N", 6),
)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def _positive(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _stamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("AWARE_TIMESTAMP_REQUIRED")
    return result


def prepare_shadow_plan(plan, *, decision_time):
    """Validate A3 append-only evidence, normalize only an isolated input copy."""
    facts = plan.get("strategy_facts")
    inputs = facts.get("shadow_inputs") if isinstance(facts, dict) else None
    if not isinstance(inputs, dict):
        return None, None, "SHADOW_INPUTS_MISSING"
    checksum = _digest(inputs)
    try:
        at = _stamp(decision_time)
        if inputs.get("schema_version") != SHADOW_INPUT_SCHEMA:
            raise ValueError("SHADOW_INPUT_SCHEMA_UNSUPPORTED")
        if not all(_positive(inputs.get(key)) for key in ("daily_ma5", "atr14")):
            raise ValueError("SHADOW_MA5_ATR_MISSING_OR_INVALID")
        closes, dates = inputs.get("previous_daily_closes"), inputs.get("previous_daily_close_dates")
        if not isinstance(closes, list) or len(closes) != 4 or not all(_positive(v) for v in closes):
            raise ValueError("FOUR_FROZEN_CLOSES_REQUIRED")
        if not isinstance(dates, list) or len(dates) != 4:
            raise ValueError("FOUR_FROZEN_CLOSE_DATES_REQUIRED")
        parsed = [date.fromisoformat(value) for value in dates]
        if parsed != sorted(set(parsed)) or any(value >= at.date() for value in parsed):
            raise ValueError("PRECEDING_CLOSE_DATES_INVALID_OR_FUTURE")
        as_of = _stamp(inputs.get("daily_as_of"))
        # Actual source observation can be this morning; closes remain strictly
        # from preceding sessions. Never disguise observation as yesterday.
        if as_of > at or parsed[-1] > as_of.date():
            raise ValueError("DAILY_EVIDENCE_INVALID_OR_FUTURE")
        if not re.fullmatch(r"[0-9a-f]{64}", str(inputs.get("atr_source_hash", ""))):
            raise ValueError("ATR_SOURCE_HASH_REQUIRED")
        daily = engine.production._daily_context(plan)
        if not _positive(daily.get("ma5")) or not math.isclose(float(daily["ma5"]), inputs["daily_ma5"], rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError("SHADOW_MA5_DIFFERS_FROM_FROZEN_PRODUCTION_MA5")
        if daily.get("atr14") is not None and (not _positive(daily["atr14"]) or not math.isclose(float(daily["atr14"]), inputs["atr14"], rel_tol=1e-12, abs_tol=1e-12)):
            raise ValueError("SHADOW_ATR_DIFFERS_FROM_FROZEN_ATR")
        research = deepcopy(plan)
        # Only geometry research inputs; baseline is never recomputed on this copy.
        research["atr14"] = inputs["atr14"]
        research["previous_daily_closes"] = deepcopy(closes)
        return research, checksum, None
    except (TypeError, ValueError) as exc:
        return None, checksum, str(exc)


def _primary(evaluation):
    reasons = evaluation.get("reason_codes")
    return reasons[0] if isinstance(reasons, (list, tuple)) and reasons else None


def _trace_summary(trace):
    if not isinstance(trace, dict):
        return None
    selected = trace.get("selected_candidate")
    return {"matched": trace.get("matched"), "evidence_scope": trace.get("evidence_scope"),
        "candidate_count": len(trace["candidates"]) if isinstance(trace.get("candidates"), list) else None,
        "selected_candidate": deepcopy(selected), "trace_sha256": _digest(trace)}


def _field_status(value, name):
    if name not in value:
        return "MISSING"
    if value[name] is None:
        return "NULL"
    if value[name] in ([], {}):
        return "EMPTY"
    return "PRESENT"


def _baseline_records(items, minute):
    """Every caller baseline, even when no variant event or admission occurred.

    These are original caller evidence projections, NOT a second evaluation.
    In particular the outer warmup projection is never inferred from inner.
    """
    records, seen = [], set()
    for item in items:
        pid = item.get("plan_id")
        if pid in seen:
            continue
        seen.add(pid)
        value = item.get("actual_outer_baseline")
        outer = value if isinstance(value, dict) else {}
        primary = _primary(outer) if "reason_codes" in outer else outer.get("reason")
        inner = item.get("baseline")
        records.append({"plan_id": pid, "symbol": item.get("plan", {}).get("symbol"),
            "minute": minute.isoformat(), "action": deepcopy(outer.get("action")),
            "first_cause": deepcopy(primary),
            "first_cause_source": "OUTER_REASON_CODES" if "reason_codes" in outer else
                "OUTER_REASON" if "reason" in outer else "MISSING",
            "event_id": deepcopy(item.get("baseline_event_id")),
            "event_sha256": deepcopy(item.get("baseline_event_sha256")),
            "outer_baseline_sha256": _digest(outer) if isinstance(value, dict) else None,
            "inner_baseline_sha256": _digest(inner) if isinstance(inner, dict) else None,
            "source_scope": "CALLER_SUPPLIED_NOT_REEVALUATED",
            "field_status": {"outer_baseline": _field_status(item, "actual_outer_baseline"),
                **{name: _field_status(outer,name) for name in ("action","state","reason_codes","reason")},
                "event_id": _field_status(item,"baseline_event_id"),
                "event_sha256": _field_status(item,"baseline_event_sha256")}})
    return records


@dataclass(frozen=True)
class TentativeShadowEvaluation:
    """Opaque engine-owned reservation; result mutation cannot authorize commit."""
    token: str | None
    result: dict
    state_version: int


class ShadowVariantEngine:
    """Single capacity, no queue; timed-out workers cannot emit or write anything."""
    def __init__(self, *, evaluator=None, clock=time.monotonic):
        self.evaluator = evaluator or engine._evaluate_with_baseline
        self.clock = clock
        self._lock = threading.Lock()
        self._active = None
        self._day = None
        self._last_minute = {}
        self._states = {}
        self._triggered = set()
        self._state_version = 0
        self._pending = None

    def evaluate_minute(self, items, *, minute, budget_seconds=5.0):
        # Existing direct callers retain immediate in-memory state commitment.
        return self._evaluate_minute(items,minute=minute,budget_seconds=budget_seconds,tentative=False)

    def evaluate_tentative(self, items, *, minute, budget_seconds=5.0):
        return self._evaluate_minute(items,minute=minute,budget_seconds=budget_seconds,tentative=True)

    def commit_tentative(self, evaluation, *, accepted_keys=None):
        if not self._lock.acquire(blocking=False):
            return {'ok':False,'error_code':'SHADOW_COMMIT_BUSY'}
        try:
            pending=self._pending
            if pending is None or evaluation is not pending['handle']:
                return {'ok':False,'error_code':'SHADOW_TRANSACTION_NOT_CURRENT'}
            if evaluation.state_version!=self._state_version:
                return {'ok':False,'error_code':'SHADOW_TRANSACTION_VERSION_CONFLICT'}
            if _digest(evaluation.result)!=pending['result_sha256']:
                return {'ok':False,'error_code':'SHADOW_TRANSACTION_RESULT_CHANGED'}
            keys=pending['touched'] if accepted_keys is None else set(accepted_keys)
            if not keys.issubset(pending['touched']):
                return {'ok':False,'error_code':'SHADOW_TRANSACTION_KEYS_INVALID'}
            if self._day!=pending['day']:
                self._day=pending['day']; self._states.clear(); self._triggered.clear(); self._last_minute.clear()
            for key in keys:
                self._states[key]=pending['states'][key]
                self._last_minute[key]=pending['last_minute'][key]
                if key in pending['triggered']:
                    self._triggered.add(key)
            self._state_version+=1
            self._pending=None
            return {'ok':True,'state_version':self._state_version,
                'status':'COMMITTED' if keys==pending['touched'] else 'PARTIALLY_COMMITTED',
                'committed_key_count':len(keys)}
        finally:
            self._lock.release()

    def discard_tentative(self, evaluation):
        if not self._lock.acquire(blocking=False):
            return {'ok':False,'error_code':'SHADOW_DISCARD_BUSY'}
        try:
            if self._pending is None or evaluation is not self._pending['handle']:
                return {'ok':False,'error_code':'SHADOW_TRANSACTION_NOT_CURRENT'}
            self._pending=None
            return {'ok':True,'status':'DISCARDED','state_version':self._state_version}
        finally:
            self._lock.release()

    def _evaluate_minute(self, items, *, minute, budget_seconds, tentative):
        if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)) or not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 5:
            raise ValueError("SHADOW_BUDGET_MUST_BE_GT_ZERO_LE_5")
        at = _stamp(minute)
        started = self.clock()
        items = tuple(items)
        summary = {"schema_version": "a4-shadow-minute/2", "cohort": "REALTIME_SHADOW", "minute": at.isoformat(), "variant_set_version": VARIANT_SET_VERSION, "status": "OK", "budget_seconds": budget_seconds, "evaluated_plan_count": 0, "evaluated_variant_count": 0, "requested_variant_count": 0, "skipped_variant_count": 0, "first_trigger_count": 0, "shadow_budget_exceeded_count": 0, "baseline_records": _baseline_records(items, at)}
        result = {"signals": [], "minute_summary": summary, "errors": []}
        def unavailable():
            return TentativeShadowEvaluation(None,result,self._state_version) if tentative else result
        if not self._lock.acquire(blocking=False):
            summary["status"] = "SHADOW_REENTRY_IN_FLIGHT"
            summary["elapsed_ms"] = (self.clock()-started)*1000
            return unavailable()
        staged=None
        try:
            if self._pending is not None:
                summary['status']='SHADOW_TRANSACTION_PENDING'
                return unavailable()
            if self._active is not None and self._active.is_alive():
                summary["status"] = "SHADOW_WORKER_BUSY"
                return unavailable()
            if self._day is not None and at.date()<self._day:
                summary['status']='SHADOW_CLOCK_REGRESSED'
                return unavailable()
            same_day=self._day==at.date()
            last_minute=dict(self._last_minute) if same_day else {}
            states=dict(self._states) if same_day else {}
            triggered=set(self._triggered) if same_day else set()
            touched=set()
            jobs = []
            evaluated_plans = set()
            summary["requested_plan_count"] = 0
            for item in items:
                profile = str(item.get("plan", {}).get("strategy_profile", "")).upper()
                specs = SHADOW_VARIANTS_V1 if profile == "TREND_MA5" else SHADOW_VARIANTS_V1[:3] if profile == "MA520_SWING" else ()
                jobs.extend((item, spec) for spec in specs)
                summary["requested_plan_count"] += bool(specs)
            summary["requested_variant_count"] = len(jobs)
            previous_clock = started
            for index, (item, spec) in enumerate(jobs):
                current_clock = self.clock()
                if current_clock < previous_clock:
                    summary["status"] = "SHADOW_CLOCK_REGRESSED"
                    summary["skipped_variant_count"] = len(jobs)-index
                    break
                previous_clock = current_clock
                remaining = started+budget_seconds-current_clock
                if remaining <= 0:
                    summary.update(status="SHADOW_BUDGET_EXCEEDED", shadow_budget_exceeded_count=1, skipped_variant_count=len(jobs)-index)
                    break
                key = (item.get("plan_id"), spec.variant_id)
                if last_minute.get(key, at-timedelta(minutes=1)) >= at:
                    continue
                frozen = deepcopy(item)
                plan, checksum, limitation = prepare_shadow_plan(frozen["plan"], decision_time=frozen["decision_time"])
                if _stamp(frozen["decision_time"]) != at:
                    limitation = "SHADOW_MINUTE_DIFFERS_FROM_DECISION_TIME"
                baseline = frozen.get("baseline")
                if not isinstance(baseline, dict) or any(name not in baseline for name in ("action", "state")):
                    limitation, baseline = "FORMAL_BASELINE_EVIDENCE_MISSING", {}
                evaluation, error = None, None
                evaluation_started = self.clock()
                if limitation is None:
                    box = {}
                    evaluator = self.evaluator
                    def work():
                        try:
                            box["evaluation"] = evaluator(plan, frozen["bars"], now=frozen["now"], decision_time=frozen["decision_time"], market_context=frozen["market_context"], baseline=deepcopy(baseline), disabled=(), variant=spec.parameters())
                        except Exception as exc:
                            box["error"] = f"{type(exc).__name__}:{exc}"
                    self._active = threading.Thread(target=work, daemon=True, name="a4-shadow-readonly")
                    self._active.start()
                    self._active.join(timeout=max(0, started+budget_seconds-self.clock()))
                    finished = self.clock()
                    if self._active.is_alive() or finished > started+budget_seconds:
                        summary.update(status="SHADOW_BUDGET_EXCEEDED", shadow_budget_exceeded_count=1, skipped_variant_count=len(jobs)-index)
                        break
                    if finished < evaluation_started:
                        summary.update(status="SHADOW_CLOCK_REGRESSED", skipped_variant_count=len(jobs)-index)
                        break
                    evaluation, error = box.get("evaluation"), box.get("error")
                elapsed = (self.clock()-evaluation_started)*1000
                if self.clock() > started+budget_seconds:
                    summary.update(status="SHADOW_BUDGET_EXCEEDED", shadow_budget_exceeded_count=1, skipped_variant_count=len(jobs)-index)
                    break
                if evaluation is not None and not isinstance(evaluation, dict):
                    error, evaluation = "INVALID_SHADOW_EVALUATION_RESULT", None
                meta = evaluation.get("ablation", {}) if evaluation else {}
                status = "DATA_LIMITED" if limitation else "ERROR" if error or evaluation is None else meta.get("status", "DATA_LIMITED")
                row = {"schema_version": "a4-shadow-signal/1", "cohort": "REALTIME_SHADOW", "variant_set_version": VARIANT_SET_VERSION, "variant_id": spec.variant_id, "matrix_scenario": spec.matrix_scenario, "parameters": spec.parameters(), "plan_id": frozen.get("plan_id"), "symbol": frozen["plan"].get("symbol"), "profile": frozen["plan"].get("strategy_profile"), "minute": at.isoformat(), "observation_time": _stamp(frozen["now"]).isoformat(), "reference_price": evaluation.get("reference_price") if evaluation else baseline.get("reference_price"), "baseline_action": baseline.get("action"), "baseline_first_cause": _primary(baseline), "baseline_reason_codes": deepcopy(baseline.get("reason_codes")), "baseline_state": baseline.get("state"), "variant_action": evaluation.get("action") if evaluation else None, "variant_state": evaluation.get("state") if evaluation else None, "status": status, "reason": limitation or error or meta.get("reason"), "variant_conditions": {name: deepcopy(evaluation.get(name)) if evaluation else None for name in ("reason_codes", "met_conditions", "unmet_conditions", "veto_conditions")}, "effective_zone": deepcopy(meta.get("effective_zone")), "sequence_evidence": _trace_summary(meta.get("sequence_evidence")), "shadow_inputs_hash": checksum, "elapsed_ms": elapsed, "counterfactual": True, "actual_execution_authorized": False}
                summary["evaluated_variant_count"] += 1
                evaluated_plans.add(key[0])
                summary["evaluated_plan_count"] = len(evaluated_plans)
                if error: result["errors"].append({"plan_id": key[0], "variant_id": key[1], "error": error})
                trigger = status == "OK" and row["variant_action"] in ("BUY_SIGNAL", "ADD_SIGNAL")
                state = _digest({name: row[name] for name in ("status", "variant_action", "variant_state", "variant_conditions", "reason")})
                first = trigger and key not in triggered
                if first:
                    row["event_kind"] = "FIRST_TRIGGER"
                    triggered.add(key)
                    summary["first_trigger_count"] += 1
                elif key not in states: row["event_kind"] = "INITIAL_STATE"
                elif states[key] != state: row["event_kind"] = "STATE_CHANGE"
                else: row["event_kind"] = None
                if row["event_kind"]: result["signals"].append(row)
                states[key] = state
                last_minute[key] = at
                touched.add(key)
            if tentative:
                staged=TentativeShadowEvaluation(uuid.uuid4().hex,result,self._state_version)
                self._pending={'handle':staged,'day':at.date(),'states':states,'last_minute':last_minute,
                    'triggered':triggered,'touched':touched}
                return staged
            self._day=at.date()
            self._last_minute,self._states,self._triggered=last_minute,states,triggered
            self._state_version+=1
            return result
        finally:
            summary["elapsed_ms"] = (self.clock()-started)*1000
            if staged is not None:
                self._pending['result_sha256']=_digest(result)
            self._lock.release()


def emit_shadow_signals(result, record_signal, *, log_error=None):
    """Optional caller-side sink bridge; storage implementation belongs to W2."""
    receipts = []
    for row in result.get("signals", []):
        try:
            receipts.append(record_signal(deepcopy(row)))
        except Exception as exc:
            message = f"SHADOW_LEDGER_WRITE_FAILED:{type(exc).__name__}:{exc}"
            receipts.append({"ok": False, "error_code": message})
            if log_error:
                try: log_error(message)
                except Exception: pass
    return receipts
