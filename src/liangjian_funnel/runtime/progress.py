"""Persistent, redacted workflow progress for the local control plane."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json
from ..pipeline.outcomes import RunOutcome, StageOutcome


SHANGHAI = ZoneInfo("Asia/Shanghai")
WORKFLOW_PROGRESS_SCHEMA_VERSION = "workflow-progress/3.0.0"

# Diagnostics are intentionally a separate, much narrower contract than the
# model/result payload.  Keep this list local to the writer so a future caller
# cannot make an arbitrary output field appear in the durable progress file.
_SAFE_PROGRESS_OUTPUT_FIELDS = frozenset(
    {
        "envelope",
        "analysis_summary",
        "structural_themes",
        "industry_chain_graph",
        "taxonomy_links",
        "industry_theme_mappings",
        "canonical_monthly_decisions",
        "monthly_industry_decisions",
        "monthly_rotation_coverage",
        "a1_contract",
        "local_screen_summary",
        "active_research_pool",
        "monitor_pool",
        "active_themes",
        "focus_pool",
        "watch_only_pool",
        "core_watch_pool",
        "secondary_watch_pool",
        "rejected_candidates",
        "source_health",
        "unresolved_questions",
    }
)
_SAFE_PROGRESS_SHAPE_TYPES = frozenset(
    {
        "object",
        "array",
        "list",
        "dict",
        "tuple",
        "string",
        "str",
        "number",
        "int",
        "float",
        "boolean",
        "bool",
        "null",
        "none",
        "nonetype",
    }
)
_MAX_PROGRESS_DIAGNOSTIC_COUNT = 10_000_000
_MAX_PROGRESS_DIAGNOSTIC_FIELDS = 20
_MAX_PROGRESS_DIAGNOSTIC_ITEMS = 10_000
_TIMING_PHASES = frozenset({
    "STARTING", "UNIVERSE_SYNC", "DATA_SYNC", "MARKET_FACT_SYNC", "COMPANY_FACT_SYNC",
    "CNINFO_SYNC", "CNINFO_PDF_SYNC", "FACT_MANIFEST_SYNC", "OPEN_MACRO_SYNC", "SNAPSHOT",
    "SNAPSHOT_READY", "SNAPSHOT_REUSE", "SNAPSHOT_RESUMED", "FEATURE_SOURCE_GENERATION",
    "EARLY_DISCOVERY_DAILY_SYNC", "RESEARCH", "PERSIST", "BOOTSTRAP", "UNKNOWN",
    "MACRO_DISCOVERY", "A1", "A2", "A3", "A1_LOCAL_SCREEN", "A1_LLM_REVIEW",
    "A2_LOCAL_ROLE", "A2_LLM_REVIEW", "A3_LOCAL_TECHNICAL", "A3_LLM_REVIEW",
    "MARKET_FACT_THS_INDUSTRY_CATALOG", "MARKET_FACT_THS_CONCEPT_CATALOG",
    "MARKET_FACT_LIMIT_UP_POOL", "MARKET_FACT_LIMIT_DOWN_POOL", "MARKET_FACT_LIMIT_BREAK_POOL",
    "MARKET_FACT_LIMIT_UP_LADDER", "MARKET_FACT_DRAGON_TIGER_LIST", "MARKET_FACT_HOT_STOCK_LIST",
})
_TIMING_STATUSES = {"RUNNING", "LEFT_PHASE", "LEFT_STAGE", "COMPLETED", "FAILED", "INTERRUPTED", "RUN_ENDED"}
_MAX_TIMING_VISITS = 384


class WorkflowProgress:
    """Atomically publish bounded workflow progress across process restarts.

    The progress file is presentation state, not an execution authority.  It
    deliberately contains counters and stable reason codes plus an explicit,
    bounded diagnostic schema; prompts, provider responses, API credentials
    and model reasoning never enter it.
    """

    def __init__(self, path: Path, *, run_id: str, job: str, now: datetime | None = None) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        started = _aware(now or datetime.now(SHANGHAI))
        self._state: dict[str, Any] = {
            "schema_version": WORKFLOW_PROGRESS_SCHEMA_VERSION,
            "run_id": str(run_id)[:200],
            "run_id_sha256": hashlib.sha256(str(run_id).encode("utf-8")).hexdigest(),
            "job": str(job)[:40],
            "status": "RUNNING",
            "job_status": "RUNNING",
            "phase": "STARTING",
            "started_at": started.isoformat(),
            "phase_started_at": started.isoformat(),
            "updated_at": started.isoformat(),
            "elapsed_seconds": 0,
            "eta_seconds": None,
            "data": {
                "processed": 0,
                "total": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "failures": 0,
            },
            "lanes": {},
            "resources": {},
            "reason_code": None,
        }
        self._timing_active: dict[str, dict[str, Any]] = {}
        self._timing_terminal = False
        self._timing_finished_at: datetime | None = None
        self._timing_session_started = started
        self._timing_prior_python_seconds = 0.0
        self._state["timing"] = {
            "schema_version": "workflow-timing/1.0", "stage_times_are_additive": False,
            "python_elapsed_seconds": 0.0, "run_wall_elapsed_seconds": 0.0,
            "budget_seconds": None, "budget_source": "UNKNOWN",
            "parent_started_at": None, "parent_elapsed_seconds": None, "budget_used_ratio": None,
            "visits": [], "totals": [], "visits_dropped_count": 0,
        }
        self._restore_timing(started)
        budget = _timing_number(os.environ.get("LIANGJIAN_PARENT_JOB_BUDGET_MS"))
        parent_ms = _timing_number(os.environ.get("LIANGJIAN_PARENT_JOB_STARTED_MS"))
        if budget is not None and budget > 0 and parent_ms is not None:
            try:
                parent = datetime.fromtimestamp(parent_ms / 1000, SHANGHAI)
                if parent <= started:
                    self._state["timing"].update(budget_seconds=budget / 1000,
                        budget_source="NODE_TIMEOUT_FOR_JOB", parent_started_at=parent.isoformat())
            except (ValueError, OSError, OverflowError):
                pass
        self._timing_enter("PHASE", "STARTING", None, started)
        self._touch(started)

    def set_phase(
        self,
        phase: str,
        *,
        status: str = "RUNNING",
        eta_seconds: int | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            if self._timing_terminal:
                return
            next_phase = _token(phase, 80)
            current = self._timing_now(now)
            self._timing_enter("PHASE", _timing_phase(next_phase), None, current)
            if next_phase != self._state.get("phase"):
                self._state["phase_started_at"] = _aware(
                    now or datetime.now(SHANGHAI)
                ).isoformat()
            self._state["phase"] = next_phase
            self._state["status"] = _token(status, 40)
            self._state["eta_seconds"] = _non_negative(eta_seconds)
            self._touch(now)

    def update_data(
        self,
        *,
        processed: int,
        total: int,
        cache_hits: int,
        cache_misses: int,
        failures: int,
        current_symbol: str | None = None,
        current_document: str | None = None,
        documents_succeeded: int | None = None,
        documents_failed: int | None = None,
        daily_updates: int | None = None,
        financial_refreshes: int | None = None,
        deferred_financial_refreshes: int | None = None,
        eta_seconds: int | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            payload: dict[str, Any] = {
                "processed": _non_negative(processed) or 0,
                "total": _non_negative(total) or 0,
                "cache_hits": _non_negative(cache_hits) or 0,
                "cache_misses": _non_negative(cache_misses) or 0,
                "failures": _non_negative(failures) or 0,
            }
            if current_symbol:
                payload["current_symbol"] = _token(current_symbol, 32)
            if current_document:
                payload["current_document"] = _token(current_document, 200)
            if documents_succeeded is not None:
                payload["documents_succeeded"] = _non_negative(documents_succeeded) or 0
            if documents_failed is not None:
                payload["documents_failed"] = _non_negative(documents_failed) or 0
            if daily_updates is not None:
                payload["daily_updates"] = _non_negative(daily_updates) or 0
            if financial_refreshes is not None:
                payload["financial_refreshes"] = _non_negative(financial_refreshes) or 0
            if deferred_financial_refreshes is not None:
                payload["deferred_financial_refreshes"] = (
                    _non_negative(deferred_financial_refreshes) or 0
                )
            self._state["data"] = payload
            computed_eta = _non_negative(eta_seconds)
            if computed_eta is None and payload["processed"] > 0 and payload["total"] > payload["processed"]:
                current = _aware(now or datetime.now(SHANGHAI))
                phase_started = datetime.fromisoformat(str(self._state["phase_started_at"]))
                elapsed = max(0.0, (current - phase_started).total_seconds())
                computed_eta = int(
                    elapsed * (payload["total"] - payload["processed"]) / payload["processed"]
                )
            self._state["eta_seconds"] = computed_eta
            self._touch(now)

    def research_event(self, event: Mapping[str, Any], *, now: datetime | None = None) -> None:
        """Consume one safe batch event from ``ResearchPipeline``."""

        lane = _token(event.get("lane") or event.get("lane_id") or "unknown", 40)
        stage = _token(event.get("stage") or "unknown", 40)
        with self._lock:
            if self._timing_terminal:
                return
            current = self._timing_now(now)
            self._timing_enter("PHASE", "RESEARCH", None, current)
            self._timing_research(event, current)
            lanes = self._state.setdefault("lanes", {})
            lane_state = lanes.setdefault(lane, {"model": None, "status": "RUNNING", "stages": {}})
            if event.get("model"):
                lane_state["model"] = _token(event["model"], 120)
            lane_state["status"] = _token(event.get("lane_status") or "RUNNING", 40)
            lane_state["job_status"] = _job_status(event.get("lane_job_status") or lane_state["status"])
            lane_state["current_stage"] = stage
            stages = lane_state.setdefault("stages", {})
            stage_state = {
                "status": _token(event.get("status") or "RUNNING", 40),
                "completed_batches": _non_negative(
                    event.get("completed_batches", event.get("completed", 0))
                ) or 0,
                "total_batches": _non_negative(event.get("total_batches", event.get("total", 0))) or 0,
                "attempts": _non_negative(event.get("attempts", 0)) or 0,
            }
            stage_state["job_status"] = _job_status(event.get("job_status") or stage_state["status"])
            reason_codes = _reason_codes(event.get("reason_codes"))
            if reason_codes:
                stage_state["reason_codes"] = reason_codes
            diagnostics = _progress_diagnostics(event.get("diagnostics"))
            if diagnostics:
                stage_state["diagnostics"] = diagnostics
            if isinstance(event.get("outcome"), str) and event["outcome"].strip():
                stage_state["outcome"] = _token(event["outcome"], 80)
            raw_outcome = event.get("outcome_v3")
            if not isinstance(raw_outcome, Mapping):
                raw_outcome = event.get("outcome") if isinstance(event.get("outcome"), Mapping) else None
            if isinstance(raw_outcome, Mapping):
                safe_outcome = _safe_stage_outcome(raw_outcome, stage)
                if safe_outcome is not None:
                    stage_state["outcome_v3"] = safe_outcome
            if isinstance(event.get("checkpoint_reused"), bool):
                stage_state["checkpoint_reused"] = event["checkpoint_reused"]
            if event.get("checkpoint_batch_index") is not None:
                checkpoint_batch = _non_negative(event.get("checkpoint_batch_index"))
                if checkpoint_batch is not None:
                    stage_state["checkpoint_batch_index"] = checkpoint_batch
            for target, *sources in (
                ("processed_symbols", "processed_symbols", "processed"),
                ("total_symbols", "total_symbols", "universe_total"),
                ("selected_symbols", "selected_symbols"),
                ("monitor_symbols", "monitor_symbols"),
                ("rejected_symbols", "rejected_symbols"),
                ("industry_count", "industry_count"),
                ("monthly_decision_count", "monthly_decision_count"),
                ("theme_count", "theme_count"),
                ("node_count", "node_count"),
                ("mapping_count", "mapping_count"),
            ):
                value = next((_non_negative(event.get(source)) for source in sources if event.get(source) is not None), None)
                if value is not None:
                    stage_state[target] = value
            stages[stage] = stage_state
            self._state["phase"] = f"RESEARCH_{stage}"[:80]
            self._touch(now)

    def update_resources(self, values: Mapping[str, Any], *, now: datetime | None = None) -> None:
        """Publish only numeric host metrics; never process commands or paths."""

        with self._lock:
            resources: dict[str, Any] = {}
            for key in (
                "rss_current_mb",
                "rss_peak_mb",
                "system_mem_available_mb",
                "swap_used_mb",
                "disk_free_mb",
                "disk_free_ratio",
                "open_file_descriptors",
            ):
                raw = values.get(key)
                if raw is None or isinstance(raw, bool):
                    continue
                try:
                    number = float(raw)
                except (TypeError, ValueError):
                    continue
                if number < 0 or number != number or number in {float("inf"), float("-inf")}:
                    continue
                resources[key] = round(number, 6)
            self._state["resources"] = resources
            self._touch(now)

    def finish(
        self,
        *,
        status: str,
        phase: str = "COMPLETED",
        reason_code: str | None = None,
        job_status: str | None = None,
        outcome: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._lock:
            current = self._timing_now(now)
            self._timing_refresh(current)
            for visit in list(self._timing_active.values()):
                self._timing_close(visit, current, "INTERRUPTED" if phase == "FAILED" else "RUN_ENDED")
            self._timing_terminal = True
            self._timing_finished_at = current
            self._state["status"] = _token(status, 40)
            self._state["phase"] = _token(phase, 80)
            self._state["eta_seconds"] = 0
            self._state["reason_code"] = _token(reason_code, 120) if reason_code else None
            self._state["job_status"] = _terminal_job_status(
                job_status or self._state["status"],
                phase=self._state["phase"],
            )
            safe_outcome: dict[str, Any] | None = None
            if isinstance(outcome, Mapping):
                safe_outcome = _safe_run_outcome(outcome)
                if safe_outcome is not None:
                    self._state["outcome_v3"] = safe_outcome
                    # The canonical outcome is authoritative for the job
                    # lifecycle when it is supplied.  A legacy ``status``
                    # such as BLOCKED describes business quality, not a
                    # process crash.
                    self._state["job_status"] = _terminal_job_status(
                        safe_outcome["job_status"],
                        phase=self._state["phase"],
                    )
            self._finalize_lanes(safe_outcome)
            self._touch(now)

    def _finalize_lanes(
        self,
        outcome: Mapping[str, Any] | None,
    ) -> None:
        """Close the lane lifecycle without rewriting detailed stage facts."""

        canonical_lanes: dict[str, Mapping[str, Any]] = {}
        raw_outcomes = outcome.get("lanes") if isinstance(outcome, Mapping) else None
        if isinstance(raw_outcomes, list):
            for item in raw_outcomes:
                if isinstance(item, Mapping):
                    key = _lane_key(item.get("lane_id") or item.get("lane"))
                    if key:
                        canonical_lanes[key] = item
        lanes = self._state.get("lanes")
        if not isinstance(lanes, dict):
            return
        for lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            canonical = canonical_lanes.get(_lane_key(lane_id) or "")
            if canonical is not None:
                lane["status"] = _token(
                    canonical.get("legacy_status")
                    or canonical.get("status")
                    or canonical.get("quality_state")
                    or self._state["status"],
                    40,
                )
                lane["job_status"] = _terminal_job_status(
                    canonical.get("job_status"), phase=self._state["phase"]
                )
            elif _job_status(lane.get("job_status") or lane.get("status")) in {"RUNNING", "QUEUED"}:
                lane["status"] = _token(self._state["status"], 40)
                lane["job_status"] = self._state["job_status"]
            lane["current_stage"] = None

    def set_outcome(self, outcome: Mapping[str, Any], *, now: datetime | None = None) -> None:
        """Persist a bounded canonical run outcome without changing lane state."""

        with self._lock:
            safe_outcome = _safe_run_outcome(outcome)
            if safe_outcome is not None:
                self._state["outcome_v3"] = safe_outcome
            self._touch(now)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._state)

    def _touch(self, now: datetime | None) -> None:
        current = self._timing_now(now)
        self._timing_refresh(current)
        started = datetime.fromisoformat(str(self._state["started_at"]))
        self._state["updated_at"] = current.isoformat()
        self._state["elapsed_seconds"] = max(0, int((current - started).total_seconds()))
        self._write()

    def _timing_now(self, now: datetime | None) -> datetime:
        if self._timing_finished_at is not None:
            return self._timing_finished_at
        current = _aware(now or datetime.now(SHANGHAI))
        return max(current, datetime.fromisoformat(self._state["updated_at"]))

    def _timing_refresh(self, current: datetime) -> None:
        timing = self._state["timing"]
        if self._timing_terminal:
            return
        timing["run_wall_elapsed_seconds"] = round(max(0.0,
            (current - datetime.fromisoformat(self._state["started_at"])).total_seconds()), 6)
        timing["python_elapsed_seconds"] = round(self._timing_prior_python_seconds + max(0.0,
            (current - self._timing_session_started).total_seconds()), 6)
        parent = timing.get("parent_started_at")
        if parent and timing.get("budget_seconds"):
            timing["parent_elapsed_seconds"] = round(max(0.0,
                (current - datetime.fromisoformat(parent)).total_seconds()), 6)
            timing["budget_used_ratio"] = round(timing["parent_elapsed_seconds"] / timing["budget_seconds"], 9)
        for visit in self._timing_active.values():
            elapsed = round(max(visit["elapsed_seconds"],
                (current - datetime.fromisoformat(visit["started_at"])).total_seconds()), 6)
            total = self._timing_total(visit)
            total["elapsed_seconds"] = round(total["elapsed_seconds"] + elapsed - visit["elapsed_seconds"], 6)
            total["current_invocation_elapsed_seconds"] = round(
                total["current_invocation_elapsed_seconds"] + elapsed - visit["elapsed_seconds"], 6)
            visit["elapsed_seconds"] = elapsed

    def _timing_total(self, visit: Mapping[str, Any]) -> dict:
        totals = self._state["timing"]["totals"]
        key = (visit["kind"], visit["phase"], visit["lane_id"])
        for item in totals:
            if (item["kind"], item["phase"], item["lane_id"]) == key:
                return item
        item = {"kind": key[0], "phase": key[1], "lane_id": key[2],
                "elapsed_seconds": 0.0, "current_invocation_elapsed_seconds": 0.0, "visits_count": 0}
        totals.append(item)
        return item

    def _timing_enter(self, kind: str, phase: str, lane: str | None, current: datetime) -> None:
        if self._timing_terminal:
            return
        self._timing_refresh(current)
        scope = lane if kind == "RESEARCH_STAGE" else "__PHASE__"
        previous = self._timing_active.get(scope)
        if previous and previous["phase"] == phase:
            return
        if previous:
            self._timing_close(previous, current, "LEFT_STAGE" if lane else "LEFT_PHASE")
        visit = {"kind": kind, "phase": phase, "lane_id": lane, "status": "RUNNING",
                 "started_at": current.isoformat(), "ended_at": None, "elapsed_seconds": 0.0,
                 "invocation_started_at": self._timing_session_started.isoformat(),
                 "budget_seconds": self._state["timing"]["budget_seconds"],
                 "parent_started_at": self._state["timing"]["parent_started_at"]}
        self._state["timing"]["visits"].append(visit)
        self._timing_active[scope] = visit
        self._timing_total(visit)["visits_count"] += 1
        visits = self._state["timing"]["visits"]
        while len(visits) > _MAX_TIMING_VISITS:
            closed = next((i for i, item in enumerate(visits) if item["status"] != "RUNNING"), None)
            if closed is None:
                break
            visits.pop(closed)
            self._state["timing"]["visits_dropped_count"] += 1

    def _timing_close(self, visit: dict, current: datetime, status: str) -> None:
        visit.update(status=status, ended_at=current.isoformat())
        self._timing_active.pop(visit["lane_id"] if visit["kind"] == "RESEARCH_STAGE" else "__PHASE__", None)

    def _timing_research(self, event: Mapping[str, Any], current: datetime) -> None:
        if self._timing_terminal:
            return
        lane = _timing_lane(event.get("lane") or event.get("lane_id"))
        phase = _timing_phase(event.get("stage"))
        status = str(event.get("status") or "RUNNING").upper()
        active = self._timing_active.get(lane)
        prior = next((v for v in reversed(self._state["timing"]["visits"])
                      if v["lane_id"] == lane and v["kind"] == "RESEARCH_STAGE"), None)
        if active is None and prior and prior["phase"] == phase and status not in {"RUNNING", "RETRYING", "STARTED"}:
            return  # Duplicate terminal callback is not a new zero-length visit.
        self._timing_enter("RESEARCH_STAGE", phase, lane, current)
        failed = status in {"FAILED", "MODEL_FAILED", "MODEL_CALL_FAILED", "BLOCKED_MODEL", "CANCELLED"}
        completed = _non_negative(event.get("completed_batches", event.get("completed")))
        total = _non_negative(event.get("total_batches", event.get("total")))
        incomplete = total is not None and total > 0 and (completed is None or completed < total)
        terminal = status in {"COMPLETED", "READY", "READY_DEGRADED", "SUCCEEDED", "REUSED", "NOT_RUN",
            "VALIDATED", "VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP",
            "DEGRADED_UNDERFILLED_DATA_GAP", "VALIDATED_UNDERFILLED_MARKET", "BLOCKED"}
        if failed or (terminal and not incomplete):
            self._timing_close(self._timing_active[lane], current, "FAILED" if failed else "COMPLETED")

    def _restore_timing(self, current: datetime) -> None:
        """Same-run recovery imports only this bounded numeric/date contract.

        The unobserved interval is run wall time, NOT active stage time.
        A last RUNNING visit becomes INTERRUPTED at its last recorded heartbeat.
        """
        try:
            if self.path.stat().st_size > 256_000:
                return
            prior = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(prior, dict) or prior.get("run_id") != self._state["run_id"] or prior.get("job") != self._state["job"]:
                return
            if prior.get("run_id_sha256") is not None and prior["run_id_sha256"] != self._state["run_id_sha256"]:
                return
            timing = prior.get("timing")
            if not isinstance(timing, dict) or timing.get("schema_version") != "workflow-timing/1.0":
                return
            started = _aware(datetime.fromisoformat(prior["started_at"]))
            updated = _aware(datetime.fromisoformat(prior["updated_at"]))
            if not started <= updated <= current:
                return
            safe_visits = []
            for raw in timing.get("visits", [])[-_MAX_TIMING_VISITS:]:
                if not isinstance(raw, dict) or raw.get("kind") not in {"PHASE", "RESEARCH_STAGE"}:
                    continue
                began = _aware(datetime.fromisoformat(raw["started_at"]))
                ended = _aware(datetime.fromisoformat(raw["ended_at"])) if raw.get("ended_at") else updated
                elapsed = _timing_number(raw.get("elapsed_seconds"))
                if elapsed is None or not started <= began <= ended <= updated:
                    continue
                safe_visits.append({"kind": raw["kind"], "phase": _timing_phase(raw.get("phase")),
                    "lane_id": _timing_lane(raw.get("lane_id")) if raw["kind"] == "RESEARCH_STAGE" else None,
                    "started_at": began.isoformat(), "ended_at": ended.isoformat(), "elapsed_seconds": elapsed,
                    "invocation_started_at": _aware(datetime.fromisoformat(raw.get("invocation_started_at") or raw["started_at"])).isoformat(),
                    "budget_seconds": _timing_number(raw.get("budget_seconds")),
                    "parent_started_at": _aware(datetime.fromisoformat(raw["parent_started_at"])).isoformat() if raw.get("parent_started_at") else None,
                    "status": "INTERRUPTED" if raw.get("status") == "RUNNING" else
                    raw.get("status") if raw.get("status") in _TIMING_STATUSES else "INTERRUPTED"})
            safe_totals = []
            for raw in timing.get("totals", [])[:512]:
                if not isinstance(raw, dict) or raw.get("kind") not in {"PHASE", "RESEARCH_STAGE"}:
                    continue
                elapsed, count = _timing_number(raw.get("elapsed_seconds")), _non_negative(raw.get("visits_count"))
                if elapsed is None or count is None:
                    continue
                safe_totals.append({"kind": raw["kind"], "phase": _timing_phase(raw.get("phase")),
                    "lane_id": _timing_lane(raw.get("lane_id")) if raw["kind"] == "RESEARCH_STAGE" else None,
                    "elapsed_seconds": elapsed, "current_invocation_elapsed_seconds": 0.0, "visits_count": count})
            self._state["started_at"] = started.isoformat()
            self._timing_prior_python_seconds = _timing_number(timing.get("python_elapsed_seconds")) or 0.0
            self._state["timing"].update(visits=safe_visits, totals=safe_totals,
                visits_dropped_count=_non_negative(timing.get("visits_dropped_count")) or 0)
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            return

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent_stat = self.path.parent.stat()
        atomic_write_json(
            self.path,
            self._state,
            mode=0o640,
            group_id=getattr(parent_stat, "st_gid", None),
        )
        # A later run can replace the presentation file; its last observed
        # RUNNING timing must still survive without pretending it completed.
        # The human-readable prefix is not identity: punctuation replacement
        # and truncation can collide. Hash the ORIGINAL caller ID before the
        # presentation field's 200-character bound, with room for atomic temp
        # names on Windows. Existing unhashed receipts are not migrated.
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", self._state["run_id"])[:64] or "unknown"
        receipt_name = f"{safe_run_id}-{self._state['run_id_sha256'][:16]}.json"
        receipt_dir = self.path.parent / "run_timing"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(receipt_dir / receipt_name, {
            "schema_version": "workflow-timing-receipt/1.0", "run_id": self._state["run_id"],
            "run_id_sha256": self._state["run_id_sha256"],
            "job": self._state["job"], "status": self._state["status"],
            "updated_at": self._state["updated_at"], "timing": self._state["timing"],
        }, mode=0o640, group_id=getattr(parent_stat, "st_gid", None))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI)
    return value.astimezone(SHANGHAI)


def _timing_phase(value: Any) -> str:
    token = str(value or "UNKNOWN").upper()
    return token if token in _TIMING_PHASES else "UNKNOWN"


def _timing_lane(value: Any) -> str:
    token = str(value or "UNKNOWN").upper()
    return token if re.fullmatch(r"LANE_[1-9]", token) else "UNKNOWN"


def _timing_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and 0 <= number <= 100_000_000_000_000 else None


def _token(value: Any, limit: int) -> str:
    text = str(value or "UNKNOWN").strip().upper()
    retained = "".join(character for character in text if character.isalnum() or character in "._:/+_-")
    return (retained or "UNKNOWN")[:limit]


def _non_negative(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, number)


def _reason_codes(value: Any) -> list[str]:
    """Keep only bounded, stable reason codes in the progress file."""

    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
    else:
        return []
    result: list[str] = []
    for raw in values:
        if not isinstance(raw, str):
            continue
        token = raw.strip().upper()
        if not token or len(token) > 120:
            continue
        if not all(character.isalnum() or character in "_:.-" for character in token):
            continue
        if token not in result:
            result.append(token)
        if len(result) >= 20:
            break
    return result


def _progress_diagnostics(value: Any) -> dict[str, Any]:
    """Return only the durable progress diagnostic contract.

    The research pipeline already redacts its callback payload, but the
    progress writer is a second trust boundary: callers can be added later or
    invoke ``research_event`` directly.  Never copy arbitrary diagnostic keys,
    field names, mapping codes, or response content into the progress file.
    """

    if not isinstance(value, Mapping):
        return {}

    result: dict[str, Any] = {}
    shape = value.get("last_invalid_output_shape")
    if isinstance(shape, Mapping):
        safe_shape: dict[str, Any] = {}
        shape_type = shape.get("type")
        if isinstance(shape_type, str):
            normalized_type = shape_type.strip().lower()
            if normalized_type in _SAFE_PROGRESS_SHAPE_TYPES:
                safe_shape["type"] = normalized_type

        raw_fields = shape.get("fields")
        if isinstance(raw_fields, (list, tuple, set, frozenset)):
            fields: list[str] = []
            for raw_field in raw_fields:
                if not isinstance(raw_field, str):
                    continue
                field = raw_field.strip()
                if field not in _SAFE_PROGRESS_OUTPUT_FIELDS or field in fields:
                    continue
                fields.append(field)
                if len(fields) >= _MAX_PROGRESS_DIAGNOSTIC_FIELDS:
                    break
            if fields:
                safe_shape["fields"] = fields

        for key in ("unknown_field_count", "envelope_unknown_field_count"):
            count = _diagnostic_count(shape.get(key))
            if count is not None:
                safe_shape[key] = count
        if safe_shape:
            result["last_invalid_output_shape"] = safe_shape

    for key in (
        "semantic_attempts",
        "theme_count",
        "node_count",
        "mapping_count",
        "expected_mapping_count",
        "missing_mapping_count",
    ):
        count = _diagnostic_count(value.get(key))
        if count is not None:
            result[key] = count

    # Older pipeline callbacks provide mapping codes, while newer callbacks
    # may provide the already-counted form.  Persist the count only; never the
    # codes themselves.  Limit iteration as a final guard against a hostile
    # or accidentally unbounded callback value.
    if "missing_mapping_count" not in result:
        missing = value.get("missing_mapping_codes")
        if isinstance(missing, (list, tuple, set, frozenset)):
            count = 0
            for index, item in enumerate(missing):
                if index >= _MAX_PROGRESS_DIAGNOSTIC_ITEMS:
                    break
                if isinstance(item, str) and item.strip():
                    count += 1
            result["missing_mapping_count"] = min(count, _MAX_PROGRESS_DIAGNOSTIC_COUNT)
    return result


def _diagnostic_count(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return min(max(0, number), _MAX_PROGRESS_DIAGNOSTIC_COUNT)


def _lane_key(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().upper().replace("-", "_")


def _terminal_job_status(value: Any, *, phase: Any) -> str:
    status = _job_status(value)
    phase_token = str(phase or "").strip().upper().replace("-", "_")
    if phase_token == "FAILED" and status == "SUCCEEDED":
        return "FAILED"
    if status in {"RUNNING", "QUEUED"}:
        return "FAILED" if phase_token == "FAILED" else "SUCCEEDED"
    return status


def _job_status(value: Any) -> str:
    """Normalize legacy progress statuses onto the v3 job lifecycle."""

    token = str(value or "UNKNOWN").strip().upper().replace("-", "_")
    if token in {"PENDING", "CREATED", "DATA_PREPARING", "DATA_BOUND", "QUEUED"}:
        return "QUEUED"
    if token in {"RUNNING", "RETRYING", "STARTED", "IN_PROGRESS"}:
        return "RUNNING"
    if token in {"CANCELLED", "CANCELED"}:
        return "CANCELLED"
    if token in {"FAILED", "MODEL_FAILED", "BLOCKED_MODEL", "MODEL_CALL_FAILED"}:
        return "FAILED"
    if token == "STALE":
        return "STALE"
    # READY/COMPLETED/READY_DEGRADED and a completed business gate all mean
    # that the process reached a terminal point.  Business quality is carried
    # by outcome_v3, never by this job axis.
    return "SUCCEEDED"


def _safe_stage_outcome(value: Mapping[str, Any], stage: str) -> dict[str, Any] | None:
    try:
        payload = dict(value)
        payload.setdefault("stage", stage)
        return StageOutcome.from_mapping(payload).as_dict()
    except (TypeError, ValueError, KeyError):
        return None


def _safe_run_outcome(value: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        return RunOutcome.from_mapping(dict(value)).as_dict()
    except (TypeError, ValueError, KeyError):
        return None


__all__ = ["WORKFLOW_PROGRESS_SCHEMA_VERSION", "WorkflowProgress"]
