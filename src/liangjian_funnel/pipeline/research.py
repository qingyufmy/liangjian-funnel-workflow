"""Independent A1 → A2 → A3 research lanes.

This module owns orchestration and audit boundaries only.  It does not attempt
to validate the complete, prompt-defined business schemas; it validates the
generic envelope, permissions, strict JSON and symbol lineage that must hold
before a stage can be consumed by its downstream stage.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from zoneinfo import ZoneInfo

from ..redaction import digest_text, safe_error
from ..reporting import atomic_write_json, atomic_write_text
from ..settings import RESEARCH_MODELS, Settings
from ..runtime.state import RuntimeStore
from .model_client import ModelCallResult, ModelClientError, OpenAICompatibleModelClient
from .prompts import PromptBundle, PromptRepository, PromptRepositoryError


STAGES: tuple[str, ...] = ("A1", "A2", "A3")
AGENT_BY_STAGE: Mapping[str, str] = {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}
_SYMBOL = re.compile(
    r"(?<![A-Za-z0-9_])(?:(?P<code>\d{6})\.(?P<suffix>SH|SZ|BJ)|"
    r"(?P<prefix>SHSE|SZSE|BJSE|XSHG|XSHE)\.(?P<prefix_code>\d{6}))(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_EXCHANGE_CANONICAL = {
    "SH": "SH",
    "SZ": "SZ",
    "BJ": "BJ",
    "SHSE": "SH",
    "XSHG": "SH",
    "SZSE": "SZ",
    "XSHE": "SZ",
    "BJSE": "BJ",
}
_CANDIDATE_KEYS = {
    "active_research_pool",
    "monitor_pool",
    "focus_pool",
    "watch_only_pool",
    "crowded_pool",
    "low_identity_pool",
    "core_watch_pool",
    "secondary_watch_pool",
    "rejected_candidates",
    "candidates",
    "candidate_pool",
}
_REASONING_KEYS = {"reasoning", "reasoning_content", "thinking", "chain_of_thought", "cot"}
_PERMISSION_KEYS = {
    "live_trading",
    "external_orders",
    "real_trading",
    "send_order",
    "broker_order",
    "order_permission",
}
_ALLOWED_DISABLED = {False, None, "", "DISABLED", "DISABLE", "OFF", "SHADOW", "SIMULATION"}
_PROMPT_PROJECTION_VERSION = "research-prompt-projection/1.0.0"
_PROMPT_MAX_CHARS = 180_000
_STAGE_OUTPUT_BUDGETS: Mapping[str, Mapping[str, int]] = {
    "A1": {"approved_pool": 5, "secondary_pool": 5, "themes": 8, "chain_nodes": 12, "evidence_per_item": 3},
    "A2": {"approved_pool": 5, "secondary_pool": 5, "themes": 8, "chain_nodes": 0, "evidence_per_item": 3},
    "A3": {"approved_pool": 5, "secondary_pool": 5, "themes": 0, "chain_nodes": 0, "evidence_per_item": 3},
}
_FUNDAMENTAL_FIELDS: Mapping[str, tuple[str, ...]] = {
    "INCOME": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "operating_income", "operating_costs", "operating_profit", "parent_holder_net_profit",
        "basic_eps", "research_and_development_expenses", "sales_fee", "manage_fee",
    ),
    "BALANCE": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "assets_total", "total_debt", "holder_equity_total", "cash", "accounts_receivable",
        "total_current_assets", "non_current_nets_total",
    ),
    "CASH_FLOW": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "act_cash_flow_net", "invest_cash_flow_net", "financing_cash_flow_net",
        "cash_equivalents_net_addition", "pay_fixed_assets_etc_cash",
    ),
    "INDICATORS": ("_dataset", "ability", "index_id", "value"),
}
_FUNDAMENTAL_PERIOD_LIMITS: Mapping[str, int] = {
    # The latest disclosed statements plus provider-computed YoY indicators
    # are sufficient for this screening layer. Full history remains in the
    # immutable snapshot for deterministic recalculation and audit.
    "INCOME": 1,
    "BALANCE": 1,
    "CASH_FLOW": 1,
    "INDICATORS": 40,
}
_FUNDAMENTAL_INDICATORS = {
    "calculate_operating_income_yoy_growth_ratio",
    "calculate_parent_holder_net_profit_yoy_growth_ratio",
    "sale_gross_margin",
    "index_weighted_avg_roe",
    "assets_debt_ratio",
    "receive_account_turnover_ratio",
    "net_profit_cash_content",
    "operating_cash_flow_net_divide_income",
}


class ResearchPipelineError(RuntimeError):
    """Safe pipeline error for invalid construction or snapshot shape."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(f"research pipeline {reason_code}")


@dataclass(frozen=True, slots=True)
class FrozenInputSnapshot(Mapping[str, Any]):
    """Small immutable wrapper accepted by :class:`ResearchPipeline`.

    The workflow also accepts ordinary mappings, which is useful for tests and
    for adapters that already provide a frozen snapshot contract.
    """

    snapshot_id: str
    data: Mapping[str, Any]
    snapshot_hash: str = ""
    as_of: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))
        if not self.snapshot_hash:
            object.__setattr__(self, "snapshot_hash", _sha256_json(self.data))

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)


@dataclass(frozen=True, slots=True)
class StageAudit:
    lane: str
    model: str
    stage: str
    status: str
    snapshot_id: str
    prompt_hash: str | None
    input_hash: str | None
    output_hash: str | None
    latency_ms: int | None
    attempts: int
    thinking_variant: str | None
    symbols: tuple[str, ...]
    reason_codes: tuple[str, ...]
    output: Mapping[str, Any] | None = None
    diagnostics: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "model": self.model,
            "stage": self.stage,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "prompt_hash": self.prompt_hash,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "latency_ms": self.latency_ms,
            "attempts": self.attempts,
            "thinking_variant": self.thinking_variant,
            "symbols": list(self.symbols),
            "reason_codes": list(self.reason_codes),
            "output": dict(self.output) if isinstance(self.output, Mapping) else None,
            "diagnostics": dict(self.diagnostics) if isinstance(self.diagnostics, Mapping) else None,
        }


@dataclass(frozen=True, slots=True)
class LaneResult:
    lane: str
    model: str
    status: str
    stages: tuple[StageAudit, ...]
    final_output: Mapping[str, Any] | None = None
    audit_path: Path | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "model": self.model,
            "status": self.status,
            "stages": [stage.as_dict() for stage in self.stages],
            "final_output": dict(self.final_output) if isinstance(self.final_output, Mapping) else None,
        }


@dataclass(frozen=True, slots=True)
class ResearchRunResult:
    run_id: str
    generated_at: datetime
    snapshot_id: str
    snapshot_hash: str | None
    status: str
    lanes: tuple[LaneResult, ...]
    audit_paths: tuple[Path, ...]
    markdown_path: Path | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generated_at": self.generated_at.isoformat(),
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "status": self.status,
            "lanes": [lane.as_dict() for lane in self.lanes],
            "audit_paths": [str(path) for path in self.audit_paths],
            "markdown_path": str(self.markdown_path) if self.markdown_path else None,
        }


class ResearchPipeline:
    """Run three independent research-model lanes with fail-closed gates."""

    def __init__(
        self,
        settings: Settings,
        *,
        prompt_repository: PromptRepository | str | Path | None = None,
        model_client: Any | None = None,
        output_dir: str | Path | None = None,
        now: Callable[[], datetime] | None = None,
        parallel_lanes: bool = False,
        runtime_store: RuntimeStore | None = None,
        slot: str | None = None,
    ):
        self.settings = settings
        if prompt_repository is None:
            configured = os.environ.get("LIANGJIAN_PROMPT_DIR")
            prompt_repository = Path(configured) if configured else settings.root / "prompts"
        self.prompts = (
            prompt_repository
            if isinstance(prompt_repository, PromptRepository)
            else PromptRepository(prompt_repository)
        )
        self.model_client = model_client or OpenAICompatibleModelClient(settings)
        self.output_dir = Path(output_dir or settings.root / "outputs" / "research").resolve()
        self.now = now or (lambda: datetime.now(ZoneInfo(settings.timezone)))
        self.parallel_lanes = bool(parallel_lanes)
        self.runtime_store = runtime_store
        self.slot = str(slot or "UNSPECIFIED").upper()

    def run(
        self,
        snapshot: FrozenInputSnapshot | Mapping[str, Any] | Any,
        *,
        run_id: str | None = None,
        generated_at: datetime | None = None,
    ) -> ResearchRunResult:
        current = generated_at or self.now()
        if current.tzinfo is None or current.utcoffset() is None:
            current = current.replace(tzinfo=ZoneInfo(self.settings.timezone))
        try:
            frozen = _coerce_snapshot(snapshot)
            g0 = _extract_g0(frozen.data)
        except ResearchPipelineError as exc:
            frozen = FrozenInputSnapshot(snapshot_id="UNKNOWN", data={})
            g0 = set()
            global_reason = exc.reason_code
        else:
            global_reason = None if frozen.snapshot_id else "SNAPSHOT_ID_MISSING"
            if not g0:
                global_reason = global_reason or "G0_UNPROVABLE"

        effective_run_id = _safe_run_id(run_id or _default_run_id(current, frozen.snapshot_id))
        try:
            bundle = self.prompts.load()
        except PromptRepositoryError:
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"
        except (OSError, UnicodeError, ValueError):
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"

        models = tuple(self.settings.research_models)
        if models != RESEARCH_MODELS:
            global_reason = global_reason or "RESEARCH_MODEL_CONFIG_INVALID"
        def execute_lane(index: int, model: str) -> LaneResult:
            lane_id = f"lane_{index}"
            lane = self._run_lane(
                lane_id=lane_id,
                model=model,
                snapshot=frozen,
                g0=g0,
                bundle=bundle,
                run_id=effective_run_id,
                global_reason=global_reason,
            )
            audit_path = self._write_lane_audit(effective_run_id, lane)
            return LaneResult(
                lane=lane.lane,
                model=lane.model,
                status=lane.status,
                stages=lane.stages,
                final_output=lane.final_output,
                audit_path=audit_path,
            )

        indexed_models = tuple(enumerate(models, start=1))
        if self.parallel_lanes:
            with ThreadPoolExecutor(max_workers=3, thread_name_prefix="liangjian-lane") as executor:
                lanes = list(executor.map(lambda item: execute_lane(*item), indexed_models))
        else:
            lanes = [execute_lane(index, model) for index, model in indexed_models]

        if self.runtime_store is not None:
            config_hash = str(frozen.data.get("config_hash") or "") or None
            for lane in lanes:
                reasons = tuple(
                    reason
                    for stage in lane.stages
                    for reason in stage.reason_codes
                )
                prompt_hash = next(
                    (stage.prompt_hash for stage in reversed(lane.stages) if stage.prompt_hash),
                    None,
                )
                self.runtime_store.record_workflow_run(
                    run_id=effective_run_id,
                    lane_id=lane.lane,
                    trade_date=current.date().isoformat(),
                    slot=self.slot,
                    model=lane.model,
                    status="READY_TO_PUBLISH" if lane.status == "READY" else "BLOCKED",
                    snapshot_hash=frozen.snapshot_hash,
                    prompt_hash=prompt_hash,
                    config_hash=config_hash,
                    reason_codes=reasons,
                )
                for stage in lane.stages:
                    self.runtime_store.record_workflow_stage(
                        run_id=effective_run_id,
                        lane_id=lane.lane,
                        stage=stage.stage,
                        status=stage.status,
                        reason_codes=stage.reason_codes,
                    )

        ready_count = sum(lane.status == "READY" for lane in lanes)
        overall = "READY" if lanes and ready_count == len(lanes) else "PARTIAL" if ready_count else "BLOCKED"
        result = ResearchRunResult(
            run_id=effective_run_id,
            generated_at=current,
            snapshot_id=frozen.snapshot_id,
            snapshot_hash=frozen.snapshot_hash if frozen.snapshot_id != "UNKNOWN" else None,
            status=overall,
            lanes=tuple(lanes),
            audit_paths=tuple(lane.audit_path for lane in lanes if lane.audit_path is not None),
            markdown_path=None,
        )
        markdown_path = self._write_markdown(result)
        return ResearchRunResult(
            run_id=result.run_id,
            generated_at=result.generated_at,
            snapshot_id=result.snapshot_id,
            snapshot_hash=result.snapshot_hash,
            status=result.status,
            lanes=result.lanes,
            audit_paths=result.audit_paths,
            markdown_path=markdown_path,
        )

    def _run_lane(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        g0: set[str],
        bundle: PromptBundle | None,
        run_id: str,
        global_reason: str | None,
    ) -> LaneResult:
        audits: list[StageAudit] = []
        upstream_output: Mapping[str, Any] | None = None
        upstream_symbols = set(g0)
        for stage in STAGES:
            if global_reason:
                audits.append(self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, global_reason))
                continue
            previous = audits[-1] if audits else None
            if previous is not None and previous.status != "VALIDATED":
                audits.append(
                    self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
                )
                continue
            if previous is not None and not previous.symbols:
                audits.append(
                    self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_POOL_EMPTY")
                )
                continue
            if stage == "A1" and len(upstream_symbols) > self.settings.research_a1_batch_size:
                audit = self._run_a1_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=snapshot,
                    g0=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            else:
                audit = self._run_stage(
                    lane_id=lane_id,
                    model=model,
                    stage=stage,
                    snapshot=snapshot,
                    upstream_output=upstream_output,
                    upstream_symbols=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            audits.append(audit)
            if audit.status == "VALIDATED":
                upstream_output = audit.output
                upstream_symbols = set(audit.symbols)

        status = "READY" if len(audits) == 3 and all(item.status == "VALIDATED" for item in audits) else "BLOCKED"
        final_output = audits[-1].output if status == "READY" else None
        return LaneResult(lane=lane_id, model=model, status=status, stages=tuple(audits), final_output=final_output)

    def _run_a1_batched(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        g0: set[str],
        bundle: PromptBundle | None,
        run_id: str,
    ) -> StageAudit:
        ordered = sorted(g0)
        size = self.settings.research_a1_batch_size
        batches = [set(ordered[index:index + size]) for index in range(0, len(ordered), size)]
        audits: list[StageAudit] = []
        for batch in batches:
            audit = self._run_stage(
                lane_id=lane_id,
                model=model,
                stage="A1",
                snapshot=snapshot,
                upstream_output=None,
                upstream_symbols=batch,
                bundle=bundle,
                run_id=run_id,
                projection_symbols=batch,
            )
            audits.append(audit)
            if audit.status != "VALIDATED":
                reasons = tuple(f"A1_BATCH_BLOCKED:{reason}" for reason in audit.reason_codes)
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage="A1",
                    status="BLOCKED",
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=_combined_digest(item.prompt_hash for item in audits),
                    input_hash=_combined_digest(item.input_hash for item in audits),
                    output_hash=None,
                    latency_ms=sum(item.latency_ms or 0 for item in audits),
                    attempts=sum(item.attempts for item in audits),
                    thinking_variant=_common_variant(audits),
                    symbols=(),
                    reason_codes=reasons or ("A1_BATCH_BLOCKED",),
                    diagnostics={
                        "batch_count": len(batches),
                        "completed_batches": len(audits),
                        "blocked_batch_output_shape": _output_shape(audit.output),
                    },
                )

        merged = _merge_a1_outputs([audit.output for audit in audits if isinstance(audit.output, Mapping)])
        reasons = _validate_output(
            merged,
            stage="A1",
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=g0,
            snapshot_data=snapshot.data,
        )
        symbols = tuple(sorted(_approved_symbols(merged, "A1")))
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A1",
            status="VALIDATED" if not reasons else "BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=_sha256_json(merged),
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=symbols,
            reason_codes=tuple(reasons),
            output=merged,
            diagnostics={"batch_count": len(batches), "completed_batches": len(audits)},
        )

    def _run_stage(
        self,
        *,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any] | None,
        upstream_symbols: set[str],
        bundle: PromptBundle | None,
        run_id: str,
        projection_symbols: set[str] | None = None,
    ) -> StageAudit:
        if bundle is None:
            return self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "PROMPT_REPOSITORY_BLOCKED")
        try:
            replacements = _prompt_replacements(
                bundle,
                stage,
                snapshot,
                upstream_output,
                projection_symbols=projection_symbols,
            )
            shared = bundle.render("00_shared_system_v2.txt", replacements)
            stage_prompt = bundle.render_stage(stage, replacements)
            effective_scope = projection_symbols if projection_symbols is not None else upstream_symbols
            execution_budget = _stage_execution_budget(stage, len(effective_scope))
            system_content = shared + "\n\n" + stage_prompt + "\n\n" + execution_budget
            prompt_hash = digest_text(system_content)
            runtime = _runtime_input(
                snapshot,
                lane_id,
                model,
                stage,
                upstream_output,
                upstream_symbols,
                scope_symbols=projection_symbols,
            )
            runtime["prompt_projection_version"] = _PROMPT_PROJECTION_VERSION
            runtime["output_budget"] = dict(_STAGE_OUTPUT_BUDGETS[stage])
            input_hash = _sha256_json(runtime)
            # Snapshot fields are already rendered into the immutable stage
            # prompt placeholders.  Sending the complete snapshot again in
            # the user message duplicated hundreds of kilobytes and could
            # exceed gateway context/body limits.  Keep it in ``runtime`` for
            # the lineage hash, but send only the compact execution envelope.
            model_runtime = {key: value for key, value in runtime.items() if key != "snapshot_data"}
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": "RUNTIME_INPUT\n" + _canonical_json(model_runtime)},
            ]
            prompt_chars = sum(len(str(message.get("content", ""))) for message in messages)
            if prompt_chars > _PROMPT_MAX_CHARS:
                raise ResearchPipelineError("MODEL_PROMPT_TOO_LARGE")
        except (PromptRepositoryError, TypeError, ValueError):
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=None,
                input_hash=None,
                output_hash=None,
                latency_ms=None,
                attempts=0,
                thinking_variant=None,
                symbols=(),
                reason_codes=("PROMPT_RENDER_BLOCKED",),
            )
        except ResearchPipelineError as exc:
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=locals().get("prompt_hash"),
                input_hash=locals().get("input_hash"),
                output_hash=None,
                latency_ms=0,
                attempts=0,
                thinking_variant=None,
                symbols=(),
                reason_codes=(exc.reason_code,),
                diagnostics={"prompt_chars": locals().get("prompt_chars", 0), "limit_chars": _PROMPT_MAX_CHARS},
            )

        model_started = time.perf_counter()
        try:
            result = self._call_model(
                model,
                messages,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                snapshot_id=snapshot.snapshot_id,
                stage=stage,
            )
        except ModelClientError as exc:
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                output_hash=None,
                latency_ms=int((time.perf_counter() - model_started) * 1000),
                attempts=max(0, _safe_int(getattr(exc, "attempts", 0))),
                thinking_variant=None,
                symbols=(),
                reason_codes=(exc.reason_code,),
                diagnostics=getattr(exc, "diagnostics", None),
            )
        except (OSError, TypeError, ValueError):
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                output_hash=None,
                latency_ms=int((time.perf_counter() - model_started) * 1000),
                attempts=0,
                thinking_variant=None,
                symbols=(),
                reason_codes=("MODEL_CALL_FAILED",),
            )

        output = _strip_reasoning(result.output)
        canonicalized_price_items = 0
        trend_veto_items = 0
        if stage == "A3":
            output, canonicalized_price_items, trend_veto_items = _canonicalize_a3_price_fields(output, snapshot.data)
        output_hash = _sha256_json(output)
        symbols = tuple(sorted(_approved_symbols(output, stage)))
        reasons = _validate_output(
            output,
            stage=stage,
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=upstream_symbols,
            snapshot_data=snapshot.data,
        )
        envelope = output.get("envelope") if isinstance(output, Mapping) else None
        model_status = envelope.get("status") if isinstance(envelope, Mapping) else None
        status = "VALIDATED" if not reasons and model_status != "BLOCKED" else "BLOCKED"
        if model_status == "BLOCKED" and "MODEL_DECLARED_BLOCKED" not in reasons:
            reasons.append("MODEL_DECLARED_BLOCKED")
        return StageAudit(
            lane=lane_id,
            model=model,
            stage=stage,
            status=status,
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=prompt_hash,
            input_hash=input_hash,
            output_hash=output_hash,
            latency_ms=result.latency_ms,
            attempts=result.attempts,
            thinking_variant=result.thinking_variant,
            symbols=symbols,
            reason_codes=tuple(dict.fromkeys(reasons)),
            output=output,
            diagnostics=(
                {
                    "output_shape": _output_shape(output),
                    "canonicalized_price_items": canonicalized_price_items,
                    "trend_veto_items": trend_veto_items,
                }
                if reasons
                else {
                    "canonicalized_price_items": canonicalized_price_items,
                    "trend_veto_items": trend_veto_items,
                }
                if canonicalized_price_items or trend_veto_items
                else None
            ),
        )

    def _call_model(self, model: str, messages: Sequence[Mapping[str, Any]], **metadata: Any) -> ModelCallResult:
        operation = getattr(self.model_client, "complete", None) or getattr(self.model_client, "call", None)
        if operation is None:
            raise ModelClientError("MODEL_CLIENT_INVALID")
        try:
            raw = operation(model, messages, **metadata)
        except TypeError as exc:
            # Permit tiny offline test doubles that expose only (model,
            # messages), while never hiding non-TypeError model failures.
            message = str(exc).lower()
            if "unexpected keyword" not in message and "positional argument" not in message:
                raise ModelClientError("MODEL_CLIENT_CALL_TYPE_ERROR") from exc
            try:
                raw = operation(model, messages)
            except TypeError:
                raise ModelClientError("MODEL_CLIENT_SIGNATURE_INVALID") from exc
        if isinstance(raw, ModelCallResult):
            return raw
        if isinstance(raw, Mapping):
            output = raw.get("output", raw.get("json", raw.get("data", raw)))
            if not isinstance(output, Mapping):
                raise ModelClientError("MODEL_OUTPUT_NOT_OBJECT")
            return ModelCallResult(
                model=str(raw.get("model", model)),
                output=dict(output),
                prompt_hash=raw.get("prompt_hash", metadata.get("prompt_hash")),
                input_hash=raw.get("input_hash", metadata.get("input_hash")),
                latency_ms=_safe_int(raw.get("latency_ms", 0)),
                attempts=max(1, _safe_int(raw.get("attempts", 1))),
                thinking_variant=str(raw.get("thinking_variant", "unknown")),
                reasoning_tokens=_safe_optional_int(raw.get("reasoning_tokens")),
            )
        output = getattr(raw, "output", None)
        if isinstance(output, Mapping):
            return ModelCallResult(
                model=str(getattr(raw, "model", model)),
                output=dict(output),
                prompt_hash=getattr(raw, "prompt_hash", metadata.get("prompt_hash")),
                input_hash=getattr(raw, "input_hash", metadata.get("input_hash")),
                latency_ms=_safe_int(getattr(raw, "latency_ms", 0)),
                attempts=max(1, _safe_int(getattr(raw, "attempts", 1))),
                thinking_variant=str(getattr(raw, "thinking_variant", "unknown")),
                reasoning_tokens=_safe_optional_int(getattr(raw, "reasoning_tokens", None)),
            )
        raise ModelClientError("MODEL_OUTPUT_NOT_OBJECT")

    def _blocked_stage(self, lane: str, model: str, stage: str, snapshot_id: str, reason: str) -> StageAudit:
        return StageAudit(
            lane=lane,
            model=model,
            stage=stage,
            status="BLOCKED",
            snapshot_id=snapshot_id,
            prompt_hash=None,
            input_hash=None,
            output_hash=None,
            latency_ms=None,
            attempts=0,
            thinking_variant=None,
            symbols=(),
            reason_codes=(reason,),
        )

    def _write_lane_audit(self, run_id: str, lane: LaneResult) -> Path:
        path = self.output_dir / f"research_{run_id}_{_safe_run_id(lane.lane)}.json"
        return atomic_write_json(path, lane.as_dict())

    def _write_markdown(self, result: ResearchRunResult) -> Path:
        lines = [
            "# 量见 A 股三模型研究漏斗报告",
            "",
            "> 内部模拟、非投资建议。该报告不连接真实证券账户，不发送真实委托。",
            "",
            f"- 运行时间：`{result.generated_at.isoformat()}`",
            f"- run_id：`{result.run_id}`",
            f"- snapshot_id：`{result.snapshot_id}`",
            f"- snapshot_hash：`{result.snapshot_hash or '-'}`",
            f"- 总状态：`{result.status}`",
            "",
            "| Lane | Model | A1 | A2 | A3 | Lane 状态 |",
            "|---|---|---|---|---|---|",
        ]
        for lane in result.lanes:
            by_stage = {stage.stage: stage.status for stage in lane.stages}
            lines.append(
                f"| {lane.lane} | {lane.model} | {by_stage.get('A1', 'BLOCKED')} | "
                f"{by_stage.get('A2', 'BLOCKED')} | {by_stage.get('A3', 'BLOCKED')} | {lane.status} |"
            )
        lines.extend(["", "## 阶段审计", ""])
        for lane in result.lanes:
            lines.append(f"### {lane.lane} · {lane.model}")
            lines.append("")
            for stage in lane.stages:
                reasons = ", ".join(stage.reason_codes) if stage.reason_codes else "-"
                latency = f"{stage.latency_ms} ms" if stage.latency_ms is not None else "-"
                lines.append(
                    f"- {stage.stage}：`{stage.status}`；原因：`{reasons}`；"
                    f"请求次数：`{stage.attempts}`；耗时：`{latency}`"
                )
            lines.append("")
        lines.extend(["", "## 最终候选/计划 JSON", ""])
        for lane in result.lanes:
            lines.extend(
                [
                    f"### {lane.lane} · {lane.model}",
                    "",
                    "```json",
                    json.dumps(lane.final_output or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
                    "```",
                    "",
                ]
            )
        path = self.output_dir / f"research_{result.run_id}.md"
        return atomic_write_text(path, "\n".join(lines))


def _coerce_snapshot(snapshot: FrozenInputSnapshot | Mapping[str, Any] | Any) -> FrozenInputSnapshot:
    if isinstance(snapshot, FrozenInputSnapshot):
        if not snapshot.snapshot_id:
            raise ResearchPipelineError("SNAPSHOT_ID_MISSING")
        return snapshot
    if isinstance(snapshot, Mapping):
        raw = dict(snapshot)
    elif hasattr(snapshot, "model_dump"):
        try:
            raw = dict(snapshot.model_dump(mode="python"))
        except (TypeError, ValueError):
            raw = {}
    else:
        raw = dict(vars(snapshot)) if hasattr(snapshot, "__dict__") else {}
    snapshot_id = raw.get("snapshot_id") or raw.get("id")
    nested = raw.get("data")
    data = dict(nested) if isinstance(nested, Mapping) else raw
    if not snapshot_id:
        raise ResearchPipelineError("SNAPSHOT_ID_MISSING")
    provided_hash = raw.get("snapshot_hash") or data.get("snapshot_hash") or ""
    return FrozenInputSnapshot(
        snapshot_id=str(snapshot_id),
        data=data,
        snapshot_hash=str(provided_hash) if provided_hash else _sha256_json(data),
        as_of=raw.get("as_of") or data.get("as_of"),
    )


def _extract_g0(data: Mapping[str, Any]) -> set[str]:
    candidates: list[Any] = []
    # The canonical frozen snapshot exposes these typed candidate collections.
    # ``trade_candidates`` is preferred because it is the deterministic,
    # tradable G0 passed to the research chain; the generic mapping form still
    # accepts the broader universe aliases below.
    preferred = data.get("trade_candidates")
    if preferred is not None:
        candidates.append(preferred)
    else:
        for key, value in data.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in {
                "g0",
                "g0_symbols",
                "g0_universe",
                "universe",
                "universe_symbols",
                "research_universe",
                "research_universe_symbols",
                "tradable_universe",
                "research_candidates",
                "universe_candidates",
            }:
                candidates.append(value)
    symbols: set[str] = set()
    for value in candidates:
        symbols.update(_scan_symbols(value))
    return symbols


def _prompt_replacements(
    bundle: PromptBundle,
    stage: str,
    snapshot: FrozenInputSnapshot,
    upstream_output: Mapping[str, Any] | None,
    *,
    projection_symbols: set[str] | None = None,
) -> dict[str, Any]:
    names = set(bundle.shared.placeholders)
    names.update(bundle.document({"A1": "agent_1_macro_chain_v2.txt", "A2": "agent_2_theme_sentiment_v2.txt", "A3": "agent_3_technical_planner_v2.txt"}[stage]).placeholders)
    if projection_symbols is not None:
        allowed_symbols: set[str] | None = set(projection_symbols)
    elif stage == "A1":
        allowed_symbols: set[str] | None = _extract_g0(snapshot.data)
    elif stage == "A2":
        allowed_symbols = _approved_symbols(upstream_output or {}, "A1")
    else:
        allowed_symbols = _approved_symbols(upstream_output or {}, "A2")
    replacements: dict[str, Any] = {}
    for name in names:
        if name == "UPSTREAM_ACTIVE_POOL" or name == "UPSTREAM_FOCUS_POOL":
            replacements[name] = upstream_output if upstream_output is not None else None
            continue
        if name == "SNAPSHOT_MANIFEST":
            manifest = snapshot.data.get("snapshot_manifest", snapshot.data)
            replacements[name] = _with_projection_metadata(manifest, snapshot, allowed_symbols)
            continue
        found, value = _lookup_field(snapshot.data, name)
        replacements[name] = _project_prompt_value(name, value, allowed_symbols) if found else None
    return replacements


def _with_projection_metadata(
    value: Any,
    snapshot: FrozenInputSnapshot,
    symbols: set[str] | None,
) -> Any:
    if not isinstance(value, Mapping):
        return value
    result = dict(value)
    result["prompt_projection"] = {
        "version": _PROMPT_PROJECTION_VERSION,
        "full_snapshot_hash": snapshot.snapshot_hash,
        "symbol_count": len(symbols or ()),
        "full_snapshot_retained_for_audit": True,
    }
    return result


def _project_prompt_value(name: str, value: Any, symbols: set[str] | None) -> Any:
    """Return a bounded, deterministic model view without mutating evidence.

    The persisted snapshot and its input hash remain full fidelity.  This view
    removes repetitive historical columns and raw feed volume that make the
    provider spend its entire deadline ingesting input.  A2/A3 symbol-indexed
    evidence is also restricted to the already approved upstream domain.
    """

    if name == "COMPANY_FUNDAMENTALS":
        return _project_fundamentals(value, symbols)
    if name == "DISCLOSURE_EVENTS":
        return _project_disclosures(value, symbols)
    if name == "INDUSTRY_NEWS_FEED":
        return _project_news(value, item_limit=8)
    if name == "NEWS_HEAT_SNAPSHOT":
        return _project_news(value, item_limit=40, symbols=symbols)
    if name in {
        "FACTOR_SNAPSHOT",
        "KLINE_PATTERNS",
        "PRICE_LEVELS",
        "LIQUIDITY_SNAPSHOT",
        "TRADABILITY_FLAGS",
        "COMPANY_FUNDAMENTALS",
    }:
        return _filter_symbol_mapping(value, symbols)
    if name == "RISK_EVENTS":
        return _project_disclosures(value, symbols)
    if name == "THS_INDUSTRY_MEMBERSHIP":
        return _project_membership(value, symbols)
    if name in {"CROWDING_SNAPSHOT", "FUND_HOLDINGS"}:
        return _filter_nested_symbol_data(value, symbols)
    return value


def _project_fundamentals(value: Any, symbols: set[str] | None) -> Any:
    if not isinstance(value, Mapping):
        return value
    projected: dict[str, Any] = {}
    for raw_symbol, raw_rows in sorted(value.items(), key=lambda item: str(item[0])):
        symbol = str(raw_symbol)
        if symbols is not None and symbol not in symbols:
            continue
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
            projected[symbol] = raw_rows
            continue
        by_dataset: dict[str, list[Mapping[str, Any]]] = {}
        for row in raw_rows:
            if isinstance(row, Mapping):
                dataset = str(row.get("_dataset") or "UNKNOWN")
                if dataset == "INDICATORS" and str(row.get("index_id") or "") not in _FUNDAMENTAL_INDICATORS:
                    continue
                by_dataset.setdefault(dataset, []).append(row)
        latest_statements: dict[str, dict[str, Any]] = {}
        indicators: dict[str, dict[str, Any]] = {}
        for dataset in ("INCOME", "BALANCE", "CASH_FLOW", "INDICATORS"):
            ordered = sorted(
                by_dataset.get(dataset, ()),
                key=lambda row: (
                    _safe_int(row.get("report_date_ms")),
                    _safe_int(row.get("period_end_ms")),
                    str(row.get("index_id") or ""),
                ),
                reverse=True,
            )[: _FUNDAMENTAL_PERIOD_LIMITS[dataset]]
            fields = _FUNDAMENTAL_FIELDS[dataset]
            compact_rows = [{key: row.get(key) for key in fields if key in row} for row in ordered]
            if dataset == "INDICATORS":
                indicators.update(
                    {
                        str(row.get("index_id")): {
                            "ability": row.get("ability"),
                            "value": row.get("value"),
                        }
                        for row in compact_rows
                        if row.get("index_id")
                    }
                )
            elif compact_rows:
                latest_statements[dataset.lower()] = compact_rows[0]
        projected[symbol] = {"latest_statements": latest_statements, "indicators": indicators}
    return projected


def _project_disclosures(value: Any, symbols: set[str] | None) -> Any:
    if not isinstance(value, Mapping):
        return value
    result = dict(value)
    by_symbol = value.get("by_symbol")
    if not isinstance(by_symbol, Mapping):
        return result
    allowed_fields = {
        "announcement_id", "announcement_title", "event_tags", "event_time", "publish_time",
        "source_url", "symbol", "sec_name", "pdf_evidence_available", "pdf_reason_code",
        "pdf_evidence_snippets", "prompt_injection_suspected", "reason_code", "content_hash",
    }
    projected: dict[str, Any] = {}
    for raw_symbol, raw_items in sorted(by_symbol.items(), key=lambda item: str(item[0])):
        symbol = str(raw_symbol)
        if symbols is not None and symbol not in symbols:
            continue
        items = raw_items if isinstance(raw_items, list) else []
        compact_items: list[dict[str, Any]] = []
        for item in items[:1]:
            if not isinstance(item, Mapping):
                continue
            compact = {key: item.get(key) for key in allowed_fields if key in item}
            snippets = compact.get("pdf_evidence_snippets")
            if isinstance(snippets, list):
                compact["pdf_evidence_snippets"] = [_truncate_nested(entry, 1_500) for entry in snippets[:2]]
            compact_items.append(compact)
        projected[symbol] = compact_items
    result["by_symbol"] = projected
    if isinstance(result.get("query_confirmed_symbols"), list) and symbols is not None:
        result["query_confirmed_symbols"] = sorted(set(result["query_confirmed_symbols"]).intersection(symbols))
    return result


def _project_membership(value: Any, symbols: set[str] | None) -> Any:
    if symbols is None or not isinstance(value, Mapping):
        return value
    result = dict(value)
    records = value.get("records")
    if isinstance(records, list):
        result["records"] = [
            record
            for record in records
            if isinstance(record, Mapping) and str(record.get("thscode") or record.get("symbol") or "") in symbols
        ]
        result["prompt_record_count"] = len(result["records"])
        result["full_record_count"] = len(records)
    return result


def _project_news(value: Any, *, item_limit: int, symbols: set[str] | None = None) -> Any:
    if not isinstance(value, Mapping):
        return value
    result = dict(value)
    items = value.get("items")
    if isinstance(items, list):
        relevant: list[Any] = []
        other: list[Any] = []
        for item in items:
            item_symbols = _scan_symbols(item)
            (relevant if symbols and item_symbols.intersection(symbols) else other).append(item)
        selected = (relevant + other)[:item_limit]
        result["items"] = [_truncate_nested(item, 800) for item in selected]
        result["prompt_item_count"] = len(selected)
        result["full_item_count"] = len(items)
    if isinstance(result.get("by_symbol"), Mapping) and symbols is not None:
        result["by_symbol"] = _filter_symbol_mapping(result["by_symbol"], symbols)
    return result


def _stage_execution_budget(stage: str, input_symbol_count: int) -> str:
    budget = _STAGE_OUTPUT_BUDGETS[stage]
    stage_contract = {
        "A1": (
            "Required top-level keys: envelope, analysis_summary, active_research_pool, monitor_pool, "
            "rejected_candidates. Each approved item needs symbol, company_name, primary_theme, "
            "industry_chain_node, core_thesis, bear_case, structural_score, status, source_refs."
        ),
        "A2": (
            "Required top-level keys: envelope, analysis_summary, active_themes, focus_pool, "
            "watch_only_pool, rejected_candidates. Each focus item needs symbol, theme_id, "
            "theme_stage, market_role, theme_score, supporting_evidence, contradicting_evidence, risk_flags."
        ),
        "A3": (
            "Required top-level keys: envelope, analysis_summary, core_watch_pool, secondary_watch_pool, "
            "rejected_candidates. Each core item must copy deterministic PRICE_LEVELS values for symbol, "
            "risk_unit, trigger_zone, invalidation_level, stop_distance_pct, first_resistance and reward_risk, "
            "then add concise scenarios and confirmation_conditions."
        ),
    }[stage]
    return (
        "RUNTIME_EXECUTION_BUDGET (overrides generic target counts, never overrides evidence gates):\n"
        f"- supplied_symbol_count={max(0, int(input_symbol_count))}; analyze only supplied symbols.\n"
        f"- approved pool <= {budget['approved_pool']}; secondary/watch pool <= {budget['secondary_pool']}.\n"
        f"- themes <= {budget['themes']}; industry_chain_graph nodes <= {budget['chain_nodes']}.\n"
        f"- each evidence/source/reference array <= {budget['evidence_per_item']}; use concise strings.\n"
        "- Do not expand empty or missing evidence. Return empty arrays and DEGRADED/BLOCKED where required.\n"
        "- Copy RUNTIME_INPUT.required_envelope exactly as the complete envelope; omit no field.\n"
        f"- {stage_contract}\n"
        "- This compact contract permits omission of all other large sections in the generic report schema.\n"
        "- Finish one valid JSON object within the response budget; no markdown or commentary."
    )


def _merge_a1_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not outputs:
        return {}
    merged: dict[str, Any] = {}
    list_values: dict[str, list[Any]] = {}
    for output in outputs:
        for key, value in output.items():
            if key == "envelope":
                if key not in merged and isinstance(value, Mapping):
                    merged[key] = dict(value)
                elif isinstance(value, Mapping) and isinstance(merged.get(key), dict):
                    if value.get("status") == "DEGRADED":
                        merged[key]["status"] = "DEGRADED"
                continue
            if isinstance(value, list):
                list_values.setdefault(str(key), []).extend(value)
            elif key not in merged:
                merged[str(key)] = value
    for key, values in list_values.items():
        merged[key] = _deduplicate_stage_items(key, values)

    active = merged.get("active_research_pool")
    if isinstance(active, list):
        normalized_active = [_normalize_pool_symbol(item) for item in active]
        merged["active_research_pool"] = sorted(
            normalized_active,
            key=lambda item: (
                -_safe_float(item.get("structural_score")) if isinstance(item, Mapping) else 0.0,
                _first_symbol(item) if isinstance(item, Mapping) else "",
            ),
        )[: _STAGE_OUTPUT_BUDGETS["A1"]["approved_pool"]]
    monitor = merged.get("monitor_pool")
    if isinstance(monitor, list):
        rejected_symbols = _scan_symbols(merged.get("rejected_candidates", ()))
        normalized_monitor = [_normalize_pool_symbol(item) for item in monitor]
        normalized_monitor = [item for item in normalized_monitor if not _scan_symbols(item).intersection(rejected_symbols)]
        merged["monitor_pool"] = sorted(
            normalized_monitor,
            key=lambda item: _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )[: _STAGE_OUTPUT_BUDGETS["A1"]["secondary_pool"]]
    for key, limit in (
        ("structural_themes", _STAGE_OUTPUT_BUDGETS["A1"]["themes"]),
        ("industry_chain_graph", _STAGE_OUTPUT_BUDGETS["A1"]["chain_nodes"]),
    ):
        if isinstance(merged.get(key), list):
            merged[key] = merged[key][:limit]
    if isinstance(merged.get("rejected_candidates"), list):
        merged["rejected_candidates"] = [_normalize_pool_symbol(item) for item in merged["rejected_candidates"]]
    summary = merged.get("analysis_summary")
    if isinstance(summary, Mapping):
        normalized_summary = dict(summary)
        normalized_summary["approved_count"] = len(merged.get("active_research_pool", ()))
        normalized_summary["monitor_count"] = len(merged.get("monitor_pool", ()))
        normalized_summary["rejected_count"] = len(merged.get("rejected_candidates", ()))
        merged["analysis_summary"] = normalized_summary
    return merged


def _deduplicate_stage_items(key: str, values: Sequence[Any]) -> list[Any]:
    identity_fields = {
        "active_research_pool": ("symbol",),
        "monitor_pool": ("symbol",),
        "rejected_candidates": ("symbol",),
        "invalidated_theses": ("symbol",),
        "policy_dossiers": ("policy_id", "title"),
        "policy_calendar": ("date", "event"),
        "structural_themes": ("theme_id",),
        "industry_chain_graph": ("node_id",),
    }
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        identity = ""
        if isinstance(value, Mapping):
            parts = [str(value.get(field) or "") for field in identity_fields.get(key, ())]
            if any(parts):
                identity = "|".join(parts)
        token = identity or _canonical_json(value)
        if token in seen:
            continue
        seen.add(token)
        result.append(value)
    return result


def _combined_digest(values: Any) -> str | None:
    retained = [str(value) for value in values if value]
    return digest_text("|".join(retained)) if retained else None


def _common_variant(audits: Sequence[StageAudit]) -> str | None:
    variants = {audit.thinking_variant for audit in audits if audit.thinking_variant}
    if not variants:
        return None
    return next(iter(variants)) if len(variants) == 1 else "mixed"


def _safe_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _first_symbol(value: Any) -> str:
    symbols = sorted(_scan_symbols(value))
    return symbols[0] if symbols else ""


def _normalize_pool_symbol(value: Any) -> Any:
    symbol = _first_symbol(value)
    if not symbol:
        return value
    if isinstance(value, Mapping):
        result = dict(value)
        result["symbol"] = symbol
        return result
    if isinstance(value, str):
        return symbol
    return value


def _output_shape(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"type": type(value).__name__}
    keys = sorted(str(key) for key in value)[:30]
    return {
        "type": "object",
        "fields": keys,
        "field_types": {key: type(value.get(key)).__name__ for key in keys},
        "array_lengths": {
            key: len(value.get(key))
            for key in keys
            if isinstance(value.get(key), list)
        },
        "envelope_fields": sorted(str(key) for key in value.get("envelope", {}))[:20]
        if isinstance(value.get("envelope"), Mapping)
        else [],
    }


def _filter_symbol_mapping(value: Any, symbols: set[str] | None) -> Any:
    if symbols is None or not isinstance(value, Mapping):
        return value
    return {str(key): item for key, item in value.items() if str(key) in symbols}


def _filter_nested_symbol_data(value: Any, symbols: set[str] | None) -> Any:
    if symbols is None or not isinstance(value, Mapping):
        return value
    result = dict(value)
    for key in ("by_symbol", "symbols", "holdings"):
        if isinstance(result.get(key), Mapping):
            result[key] = _filter_symbol_mapping(result[key], symbols)
    return result


def _truncate_nested(value: Any, max_string_chars: int) -> Any:
    if isinstance(value, str):
        return value if len(value) <= max_string_chars else value[:max_string_chars] + "…"
    if isinstance(value, Mapping):
        return {str(key): _truncate_nested(item, max_string_chars) for key, item in value.items()}
    if isinstance(value, list):
        return [_truncate_nested(item, max_string_chars) for item in value]
    return value


def _runtime_input(
    snapshot: FrozenInputSnapshot,
    lane: str,
    model: str,
    stage: str,
    upstream_output: Mapping[str, Any] | None,
    upstream_symbols: set[str],
    *,
    scope_symbols: set[str] | None = None,
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_hash": snapshot.snapshot_hash,
        "as_of": snapshot.as_of,
        "lane": lane,
        "model_name": model,
        "stage": stage,
        "required_envelope": {
            "stage_id": AGENT_BY_STAGE[stage],
            "model_name": model,
            "input_snapshot_ids": [snapshot.snapshot_id],
            "config_version": snapshot.data.get("config_version", "funnel-config-v2"),
            "prompt_version": "research-runtime-contract-v2",
            "market_regime": (
                snapshot.data.get("MARKET_REGIME_SNAPSHOT", {}).get("regime", "ROTATION_NO_MAINLINE")
                if isinstance(snapshot.data.get("MARKET_REGIME_SNAPSHOT"), Mapping)
                else "ROTATION_NO_MAINLINE"
            ),
            "status": "DEGRADED",
        },
        "g0_symbols": sorted(scope_symbols if scope_symbols is not None else _extract_g0(snapshot.data)),
        "upstream_symbols": sorted(upstream_symbols),
        "upstream_output": upstream_output,
        "snapshot_data": snapshot.data,
    }


def _lookup_field(data: Mapping[str, Any], name: str) -> tuple[bool, Any]:
    target = name.lower().replace("-", "_")
    for key, value in data.items():
        if str(key).lower().replace("-", "_") == target:
            return True, value
    return False, None


def _validate_output(
    output: Mapping[str, Any],
    *,
    stage: str,
    model: str,
    snapshot_id: str,
    upstream_symbols: set[str],
    snapshot_data: Mapping[str, Any] | None = None,
) -> list[str]:
    reasons: list[str] = []
    envelope = output.get("envelope")
    if not isinstance(envelope, Mapping):
        reasons.append("ENVELOPE_MISSING")
        return reasons
    required = {"stage_id", "status", "input_snapshot_ids", "model_name", "config_version", "prompt_version", "market_regime"}
    reasons.extend(f"ENVELOPE_FIELD_MISSING:{key}" for key in sorted(required.difference(envelope)))
    if envelope.get("stage_id") != AGENT_BY_STAGE[stage]:
        reasons.append("STAGE_ID_MISMATCH")
    if envelope.get("model_name") != model:
        reasons.append("MODEL_NAME_MISMATCH")
    if envelope.get("status") not in {"OK", "DEGRADED", "BLOCKED"}:
        reasons.append("ENVELOPE_STATUS_INVALID")
    input_snapshot_ids = envelope.get("input_snapshot_ids")
    if not isinstance(input_snapshot_ids, list) or snapshot_id not in input_snapshot_ids:
        reasons.append("SNAPSHOT_LINEAGE_MISSING")
    reasons.extend(_permission_violations(output))
    symbols = _scan_symbols(output)
    outside = sorted(symbols.difference(upstream_symbols))
    if outside:
        reasons.append("POOL_OUTSIDE_G0" if stage == "A1" else "POOL_OUTSIDE_UPSTREAM")
    if _unprovable_candidate_pool(output):
        reasons.append("CANDIDATE_LINEAGE_UNPROVABLE")
    reasons.extend(_validate_approved_pool(output, stage))
    if stage == "A3":
        reasons.extend(_validate_a3_provenance(output, snapshot_data or {}))
    return list(dict.fromkeys(reasons))


def _validate_approved_pool(output: Mapping[str, Any], stage: str) -> list[str]:
    key = {"A1": "active_research_pool", "A2": "focus_pool", "A3": "core_watch_pool"}[stage]
    pool = output.get(key)
    if not isinstance(pool, list):
        return ["APPROVED_POOL_SCHEMA_INVALID"]
    reasons: list[str] = []
    for item in pool:
        if not isinstance(item, Mapping):
            reasons.append("APPROVED_POOL_ITEM_INVALID")
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or len(_scan_symbols(symbol)) != 1:
            reasons.append("APPROVED_POOL_SYMBOL_INVALID")
    return reasons


def _validate_a3_provenance(output: Mapping[str, Any], snapshot_data: Mapping[str, Any]) -> list[str]:
    pool = output.get("core_watch_pool")
    if not isinstance(pool, list) or not pool:
        return []
    raw_levels = snapshot_data.get("PRICE_LEVELS")
    # Lightweight/offline callers may not provide computed price levels.  In
    # the production workflow the key is always present; only validate exact
    # A3 lineage when that deterministic evidence set was supplied.
    if not isinstance(raw_levels, Mapping):
        return []
    levels = raw_levels
    reasons: list[str] = []
    for item in pool:
        if not isinstance(item, Mapping):
            continue
        scanned = _scan_symbols(item.get("symbol"))
        if len(scanned) != 1:
            continue
        symbol = next(iter(scanned))
        expected = levels.get(symbol)
        if not isinstance(expected, Mapping) or expected.get("available") is not True:
            reasons.append("A3_PRICE_LEVELS_UNAVAILABLE")
            continue
        risk_unit = item.get("risk_unit")
        if risk_unit not in {"PROBE", "STANDARD", "NO_ENTRY"}:
            reasons.append("A3_RISK_UNIT_INVALID")
        actual_zone = item.get("trigger_zone")
        expected_zone = expected.get("trigger_zone")
        if not isinstance(actual_zone, Mapping) or not isinstance(expected_zone, Mapping):
            reasons.append("A3_TRIGGER_ZONE_PROVENANCE_MISMATCH")
        else:
            for key in ("low", "high"):
                if not _same_number(actual_zone.get(key), expected_zone.get(key)):
                    reasons.append("A3_TRIGGER_ZONE_PROVENANCE_MISMATCH")
                    break
        for actual_key, expected_key, reason in (
            ("invalidation_level", "invalidation", "A3_INVALIDATION_PROVENANCE_MISMATCH"),
            ("stop_distance_pct", "stop_distance_pct", "A3_STOP_DISTANCE_PROVENANCE_MISMATCH"),
            ("first_resistance", "first_resistance", "A3_RESISTANCE_PROVENANCE_MISMATCH"),
            ("reward_risk", "reward_risk", "A3_REWARD_RISK_PROVENANCE_MISMATCH"),
        ):
            if not _same_number(item.get(actual_key), expected.get(expected_key)):
                reasons.append(reason)
    return reasons


def _canonicalize_a3_price_fields(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int, int]:
    """Replace model-rounded A3 prices with the frozen deterministic values."""

    result = dict(output)
    pool = output.get("core_watch_pool")
    levels = snapshot_data.get("PRICE_LEVELS")
    if not isinstance(pool, list) or not isinstance(levels, Mapping):
        return result, 0, 0
    canonical_pool: list[Any] = []
    count = 0
    trend_veto_count = 0
    for raw_item in pool:
        if not isinstance(raw_item, Mapping):
            canonical_pool.append(raw_item)
            continue
        item = dict(raw_item)
        symbols = _scan_symbols(item.get("symbol"))
        symbol = next(iter(symbols)) if len(symbols) == 1 else ""
        expected = levels.get(symbol)
        if not isinstance(expected, Mapping) or expected.get("available") is not True:
            canonical_pool.append(item)
            continue
        replacements = {
            "trigger_zone": expected.get("trigger_zone"),
            "invalidation_level": expected.get("invalidation"),
            "stop_distance_pct": expected.get("stop_distance_pct"),
            "first_resistance": expected.get("first_resistance"),
            "reward_risk": expected.get("reward_risk"),
        }
        for key, value in replacements.items():
            item[key] = value
        if _major_trend_repair_required(symbol, snapshot_data):
            item["risk_unit"] = "NO_ENTRY"
            codes = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            item["reason_codes"] = list(dict.fromkeys([*codes, "MAJOR_TREND_REPAIR_REQUIRED"]))
            scenarios = item.get("scenarios")
            if isinstance(scenarios, Mapping):
                suspended: dict[str, Any] = {}
                for name, raw_scenario in scenarios.items():
                    if not isinstance(raw_scenario, Mapping) or name == "invalidation_plan":
                        suspended[str(name)] = raw_scenario
                        continue
                    scenario = dict(raw_scenario)
                    scenario["action"] = "NO_ENTRY"
                    if "risk_unit" in scenario:
                        scenario["risk_unit"] = "NO_ENTRY"
                    suspended[str(name)] = scenario
                item["scenarios"] = suspended
            trend_veto_count += 1
        canonical_pool.append(item)
        count += 1
    result["core_watch_pool"] = canonical_pool
    return result, count, trend_veto_count


def _major_trend_repair_required(symbol: str, snapshot_data: Mapping[str, Any]) -> bool:
    factors = snapshot_data.get("FACTOR_SNAPSHOT")
    factor = factors.get(symbol) if isinstance(factors, Mapping) else None
    summary = factor.get("technical_summary") if isinstance(factor, Mapping) else None
    timeframes = summary.get("timeframes") if isinstance(summary, Mapping) else None
    if not isinstance(timeframes, Mapping):
        return False

    def below_ma255(timeframe: str) -> bool:
        payload = timeframes.get(timeframe)
        averages = payload.get("ma") if isinstance(payload, Mapping) else None
        if not isinstance(payload, Mapping) or not isinstance(averages, Mapping):
            return False
        return _safe_float(payload.get("latest_close")) < _safe_float(averages.get("ma255")) and _safe_float(
            averages.get("ma255")
        ) > 0

    return below_ma255("daily") and below_ma255("120m")


def _same_number(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return False
    try:
        first = float(left)
        second = float(right)
    except (TypeError, ValueError, OverflowError):
        return False
    tolerance = max(1e-9, abs(second) * 1e-9)
    return abs(first - second) <= tolerance


def _approved_symbols(output: Mapping[str, Any], stage: str) -> set[str]:
    key = {"A1": "active_research_pool", "A2": "focus_pool", "A3": "core_watch_pool"}[stage]
    return _scan_symbols(output.get(key, ()))


def _permission_violations(value: Any) -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            disabled = item is None or (
                isinstance(item, (bool, str, int, float)) and item in _ALLOWED_DISABLED
            )
            if normalized in _PERMISSION_KEYS and not disabled:
                if isinstance(item, str) and item.upper() in {"FALSE", "NO", "NONE", "DISABLED", "OFF"}:
                    pass
                else:
                    violations.append("PERMISSION_ESCALATION")
            violations.extend(_permission_violations(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            violations.extend(_permission_violations(item))
    return list(dict.fromkeys(violations))


def _unprovable_candidate_pool(value: Any, *, parent_key: str = "") -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _CANDIDATE_KEYS and isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
                if item and not _scan_symbols(item):
                    return True
            if _unprovable_candidate_pool(item, parent_key=normalized):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_unprovable_candidate_pool(item, parent_key=parent_key) for item in value)
    return False


def _scan_symbols(value: Any) -> set[str]:
    symbols: set[str] = set()
    if isinstance(value, Mapping):
        for item in value.values():
            symbols.update(_scan_symbols(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            symbols.update(_scan_symbols(item))
    elif isinstance(value, str):
        for match in _SYMBOL.finditer(value):
            raw_exchange = (match.group("suffix") or match.group("prefix") or "").upper()
            code = match.group("code") or match.group("prefix_code")
            exchange = _EXCHANGE_CANONICAL[raw_exchange]
            symbols.add(f"{code}.{exchange}")
    return symbols


def _strip_reasoning(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_reasoning(item)
            for key, item in value.items()
            if str(key).lower() not in _REASONING_KEYS
        }
    if isinstance(value, list):
        return [_strip_reasoning(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_reasoning(item) for item in value)
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        try:
            return _jsonable(value.model_dump(mode="json"))
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _default_run_id(current: datetime, snapshot_id: str) -> str:
    stamp = current.strftime("%Y%m%dT%H%M%S%z")
    return f"{stamp}_{_sha256_json(snapshot_id)[:12]}"


def _safe_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return cleaned or "run"


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _safe_optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "FrozenInputSnapshot",
    "LaneResult",
    "ResearchPipeline",
    "ResearchPipelineError",
    "ResearchRunResult",
    "StageAudit",
]
