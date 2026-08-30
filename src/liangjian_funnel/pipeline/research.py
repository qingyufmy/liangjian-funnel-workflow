"""Independent A1 → A2 → A3 research lanes.

This module owns orchestration and audit boundaries only.  It does not attempt
to validate the complete, prompt-defined business schemas; it validates the
generic envelope, permissions, strict JSON and symbol lineage that must hold
before a stage can be consumed by its downstream stage.
"""

from __future__ import annotations

import gc
import hashlib
import inspect
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from zoneinfo import ZoneInfo

from ..redaction import digest_text, safe_error
from ..reporting import atomic_write_json, atomic_write_text
from .result_index import snapshot_name_catalog, write_lane_result_index
from ..settings import RESEARCH_MODELS, Settings
from ..runtime.state import RuntimeStore
from .bottleneck import (
    EVIDENCE_STRENGTHS,
    FACTOR_WEIGHTS as BOTTLENECK_FACTOR_WEIGHTS,
    MARKET_CORE_ROUTE,
    SUPPLY_CHAIN_ALPHA_ROUTE,
    SUPPLY_CHAIN_ROLES,
    canonicalize_model_scorecard,
)
from .deterministic import (
    PIPELINE_MODE as DETERMINISTIC_PIPELINE_MODE,
    DeterministicGateResult,
    local_active_items,
    local_monitor_items,
    local_rejected_items,
    screen_a1,
    screen_a2,
    screen_a3,
)
from .business_exposure import extract_business_exposure_facts
from .a1_contract import (
    A1_CONTRACT_VERSION,
    A1_MONTHLY_DECISION_COUNT,
    A1_NODE_TARGET,
    A1_THEME_TARGET,
    merge_a1_discovery_output,
    render_runtime_contract,
    validate_discovery_output,
)
from .a1_packet import A1_PACKET_TOKEN_BUDGET, A1PacketSizeError, build_a1_research_packet
from .feature_store import FeatureGenerationError, ResearchFeatureStore
from .feature_rebuild import validate_feature_generation as validate_generation_projection
from .candidate_catalog import enrich_candidate_metadata
from .model_client import (
    ModelCallResult,
    ModelClientError,
    OpenAICompatibleModelClient,
    mechanical_repair_output,
)
from .monthly_strategy import build_monthly_strategy_context
from .prompts import PromptBundle, PromptRepository, PromptRepositoryError
from .research_checkpoint import (
    FileResearchCheckpointStore,
    InMemoryResearchCheckpointStore,
    ResearchCheckpointKey,
    ResearchCheckpointStore,
)
from .outcomes import (
    LaneOutcome,
    RunOutcome,
    StageOutcome,
    aggregate_lane_outcome,
    aggregate_run_outcome,
    stage_outcome_from_legacy,
)


STAGES: tuple[str, ...] = ("A1", "A2", "A3")
AGENT_BY_STAGE: Mapping[str, str] = {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}

# Stage outcomes are deliberately represented as strings in the audit JSON so
# the runtime can evolve additively without rewriting historical snapshots.
STATUS_VALIDATED = "VALIDATED"
STATUS_VALIDATED_NO_OPPORTUNITY = "VALIDATED_NO_OPPORTUNITY"
STATUS_VALIDATED_NO_ACTION = "VALIDATED_NO_ACTION"
STATUS_DEGRADED_UNDERFILLED_DATA_GAP = "DEGRADED_UNDERFILLED_DATA_GAP"
STATUS_VALIDATED_UNDERFILLED_MARKET = "VALIDATED_UNDERFILLED_MARKET"
STATUS_VALIDATED_NO_SETUP = "VALIDATED_NO_SETUP"
STATUS_NOT_RUN_UPSTREAM_BLOCKED = "NOT_RUN_UPSTREAM_BLOCKED"
STATUS_BLOCKED_DATA_COVERAGE = "BLOCKED_DATA_COVERAGE"
STATUS_BLOCKED_EVIDENCE_GAP = "BLOCKED_EVIDENCE_GAP"
STATUS_BLOCKED_MODEL = "BLOCKED_MODEL"
STATUS_BLOCKED_TECHNICAL_DATA = "BLOCKED_TECHNICAL_DATA"

_COMPLETED_STAGE_STATUSES = frozenset(
    {
        STATUS_VALIDATED,
        STATUS_VALIDATED_NO_OPPORTUNITY,
        STATUS_VALIDATED_NO_ACTION,
        STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
        STATUS_VALIDATED_UNDERFILLED_MARKET,
        STATUS_VALIDATED_NO_SETUP,
    }
)
_PUBLISHABLE_STAGE_STATUSES = frozenset({STATUS_VALIDATED, STATUS_VALIDATED_NO_SETUP})
_A2_EVIDENCE_GAP_REASONS = frozenset(
    {
        "A2_BOTTLENECK_CONTEXT_MISSING",
        "A2_BOTTLENECK_SCORECARD_MISSING",
        "A2_BOTTLENECK_FACTORS_INVALID",
        "A2_BOTTLENECK_PENALTIES_INVALID",
        "A2_BOTTLENECK_EVIDENCE_INSUFFICIENT",
        "A2_BOTTLENECK_STRONG_EVIDENCE_MISSING",
        "A2_BOTTLENECK_MISSING_PROOF_UNDECLARED",
        "A2_BOTTLENECK_KILL_SWITCH_MISSING",
        "BOTTLENECK_UNKNOWN_FACTORS",
        "A2_CAPITAL_FLOW_SCORE_INVENTED",
        "A2_FACTOR_COVERAGE_BELOW_MINIMUM",
        "A2_CRITICAL_DATA_INSUFFICIENT",
        "A2_DATA_GAP",
        "A2_CAPITAL_FLOW_UNAVAILABLE",
        "A2_MARKET_FACTS_INSUFFICIENT",
        "A2_ROUTE_MISSING_OR_INVALID",
        "A2_EVIDENCE_COVERAGE_BELOW_MINIMUM",
    }
)
_A3_TECHNICAL_DATA_REASONS = frozenset(
    {
        "A3_TECHNICAL_FACTORS_NOT_READY",
        "A3_PRICE_LEVELS_NOT_READY",
        "A3_SYMBOL_NOT_TRADABLE",
        "A3_FACTOR_SNAPSHOT_NOT_READY",
        "A3_PRICE_LEVELS_UNAVAILABLE",
    }
)
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
_SAFE_OUTPUT_FIELDS = {
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
_SAFE_ENVELOPE_FIELDS = {
    "schema_version",
    "stage_id",
    "status",
    "input_snapshot_ids",
    "model_name",
    "config_version",
    "prompt_version",
    "market_regime",
}
_PERMISSION_KEYS = {
    "live_trading",
    "external_orders",
    "real_trading",
    "send_order",
    "broker_order",
    "order_permission",
}
_ALLOWED_DISABLED = {False, None, "", "DISABLED", "DISABLE", "OFF", "SHADOW", "SIMULATION"}
_PROMPT_PROJECTION_VERSION = "research-prompt-projection/2.0.0"
_DEFAULT_MODEL_MAX_INPUT_TOKENS = 1_000_000
_A3_BATCH_SIZE = 16
_STAGE_OUTPUT_BUDGETS: Mapping[str, Mapping[str, int]] = {
    "A1": {"approved_pool": 5, "secondary_pool": 5, "themes": 12, "chain_nodes": 80, "evidence_per_item": 3},
    "A2": {"approved_pool": 5, "secondary_pool": 5, "themes": 20, "chain_nodes": 0, "evidence_per_item": 3},
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

    def __init__(
        self,
        reason_code: str,
        *,
        diagnostics: Mapping[str, Any] | None = None,
    ):
        self.reason_code = reason_code
        self.diagnostics = dict(diagnostics or {})
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

    def outcome(self) -> StageOutcome:
        return _stage_outcome(self)

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
            "outcome_v2": self.outcome().as_dict(),
        }


@dataclass(frozen=True, slots=True)
class LaneResult:
    lane: str
    model: str
    status: str
    stages: tuple[StageAudit, ...]
    final_output: Mapping[str, Any] | None = None
    audit_path: Path | None = None

    def outcome(self) -> LaneOutcome:
        return aggregate_lane_outcome(
            tuple(stage.outcome() for stage in self.stages),
            lane_id=self.lane,
            model=self.model,
            legacy_status=self.status,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "model": self.model,
            "status": self.status,
            "stages": [stage.as_dict() for stage in self.stages],
            "final_output": dict(self.final_output) if isinstance(self.final_output, Mapping) else None,
            "outcome_v2": self.outcome().as_dict(),
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
    primary_lane_ids: tuple[str, ...] = ("lane_1",)

    def outcome(self) -> RunOutcome:
        return aggregate_run_outcome(
            tuple(lane.outcome() for lane in self.lanes),
            run_id=self.run_id,
            primary_lane_ids=self.primary_lane_ids,
            expected_lane_count=len(self.lanes),
        )

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
            "primary_lane_ids": list(self.primary_lane_ids),
            "outcome_v2": self.outcome().as_dict(),
        }


@dataclass(frozen=True, slots=True)
class _PreparedStageRequest:
    """Rendered request metadata shared by checkpoint lookup and model call."""

    prompt_hash: str
    input_hash: str
    messages: tuple[Mapping[str, Any], ...]
    prompt_chars: int
    estimated_input_tokens: int
    input_token_limit: int


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
        batch_workers: int = 1,
        progress_callback: Callable[[Mapping[str, Any]], Any] | None = None,
        checkpoint_store: Any | None = None,
        stage_snapshot_enricher: Callable[..., Any] | None = None,
    ):
        if isinstance(batch_workers, bool) or not isinstance(batch_workers, int) or batch_workers < 1:
            raise ValueError("batch_workers must be a positive integer")
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
        self.batch_workers = batch_workers
        self.progress_callback = progress_callback
        self.checkpoint_store = checkpoint_store
        self.stage_snapshot_enricher = stage_snapshot_enricher
        self.feature_store = (
            ResearchFeatureStore(settings.feature_store_db_path)
            if settings.research_pipeline_mode == DETERMINISTIC_PIPELINE_MODE
            else None
        )
        self._feature_generation_id = ""
        self._feature_generation_owned = False
        self._pipeline_contract_hash = _sha256_json({
            "pipeline_mode": settings.research_pipeline_mode,
            "stages": STAGES,
            "primary_lane": settings.research_primary_lane_id,
        })
        self._feature_contract_hash = _sha256_json({
            "schema": "feature-store/2.0.0",
            "a2_factors": (
                "capital_flow",
                "tier_structure",
                "leader_structure",
                "index_chain_resonance",
            ),
        })
        self._code_commit = str(os.environ.get("LIANGJIAN_CODE_COMMIT") or "").strip()
        self._deadline_started_monotonic: float | None = None

    def run(
        self,
        snapshot: FrozenInputSnapshot | Mapping[str, Any] | Any,
        *,
        run_id: str | None = None,
        generated_at: datetime | None = None,
        historical_replay: bool = False,
        models: Sequence[str] | None = None,
        lane_start_index: int = 1,
        primary_lane_ids: Sequence[str] | None = None,
    ) -> ResearchRunResult:
        """Run an explicitly selected set of model lanes.

        The historical/default contract remains a three-model run.  Callers
        that need the fast primary lane or the optional comparison lanes must
        pass the model set explicitly; lane numbering is kept stable so a
        comparison result can be joined to its parent without rewriting the
        primary run.
        """
        self._deadline_started_monotonic = time.monotonic()
        self._feature_generation_id = ""
        self._feature_generation_owned = False
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
        if self.feature_store is not None and frozen.snapshot_id != "UNKNOWN":
            try:
                # A research run owns a private, append-only projection.  It
                # must never replace the maintenance plane's active feature
                # generation, and two runs over the same snapshot must not
                # share mutable deterministic decisions.
                run_digest = hashlib.sha256(effective_run_id.encode("utf-8")).hexdigest()[:12]
                generation_id = f"run-{frozen.snapshot_hash[:12]}-{run_digest}"
                generation = self.feature_store.get_feature_generation(generation_id)
                if generation is None:
                    self.feature_store.create_feature_generation(
                        generation_id=generation_id,
                        as_of=frozen.as_of or current,
                        contract_version="feature-store/2.0.0",
                        algorithm_version=self.settings.research_pipeline_mode,
                        source_manifest_hash=frozen.snapshot_hash,
                        metadata={
                            "snapshot_id": frozen.snapshot_id,
                            "run_id": effective_run_id,
                            "historical_replay": bool(historical_replay),
                        },
                        purpose=(
                            "HISTORICAL_REPLAY" if historical_replay else "RUN_SNAPSHOT"
                        ),
                        activation_eligible=False,
                    )
                generation = self.feature_store.get_feature_generation(generation_id)
                generation_status = str((generation or {}).get("status") or "").upper()
                if generation_status == "SEALED":
                    # A checkpoint-resumed run can legitimately encounter its
                    # already sealed private generation.  Reuse it for reads,
                    # but never try to bind or seal it a second time.
                    self._feature_generation_id = generation_id
                    self._feature_generation_owned = False
                else:
                    if generation_status == "STAGING":
                        for taxonomy, field in (
                            ("INDUSTRY", "THS_INDUSTRY_MEMBERSHIP"),
                            ("CONCEPT", "THS_CONCEPT_MEMBERSHIP"),
                        ):
                            value = frozen.data.get(field)
                            if isinstance(value, Mapping):
                                self.feature_store.replace_taxonomy_memberships(
                                    taxonomy=taxonomy,
                                    snapshot=value,
                                    as_of=frozen.as_of or current,
                                    generation_id=generation_id,
                                )
                        self.feature_store.replace_business_exposure_facts(
                            extract_business_exposure_facts(frozen.data.get("MAIN_BUSINESS_EVIDENCE")),
                            generation_id=generation_id,
                        )
                    if generation_status not in {"STAGING", "VALIDATED"}:
                        raise FeatureGenerationError(
                            f"FEATURE_RUN_GENERATION_NOT_WRITABLE:{generation_id}"
                        )
                    # Bind while STAGING so deterministic A1/A2/A3 projections
                    # are written only to this run.  The binding is immutable;
                    # the generation itself is sealed after all lanes finish.
                    self.feature_store.bind_run_feature_generation(
                        run_id=effective_run_id,
                        generation_id=generation_id,
                        contract_hash=self._feature_contract_hash,
                        allow_unpublished=True,
                    )
                    self._feature_generation_id = generation_id
                    self._feature_generation_owned = True
            except (FeatureGenerationError, OSError, sqlite3.Error, ValueError):
                global_reason = global_reason or "FEATURE_STORE_MATERIALIZATION_FAILED"

        try:
            bundle = self.prompts.load()
        except PromptRepositoryError:
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"
        except (OSError, UnicodeError, ValueError):
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"

        selected_models = tuple(
            str(model).strip()
            for model in (self.settings.research_models if models is None else models)
        )
        configured_models = tuple(self.settings.research_models)
        if (
            not selected_models
            or any(not model for model in selected_models)
            or len(set(selected_models)) != len(selected_models)
            or any(model not in configured_models for model in selected_models)
            or not isinstance(lane_start_index, int)
            or isinstance(lane_start_index, bool)
            or lane_start_index < 1
            or lane_start_index + len(selected_models) - 1 > len(RESEARCH_MODELS)
        ):
            global_reason = global_reason or "RESEARCH_MODEL_CONFIG_INVALID"
        if primary_lane_ids is None:
            selected_primary_lane_ids = (self.settings.research_primary_lane_id,)
        else:
            selected_primary_lane_ids = tuple(str(item).strip() for item in primary_lane_ids if str(item).strip())
        expected_lane_ids = {
            f"lane_{lane_start_index + offset}"
            for offset in range(len(selected_models))
        } if isinstance(lane_start_index, int) and not isinstance(lane_start_index, bool) else set()
        if (
            not selected_primary_lane_ids
            or any(item not in expected_lane_ids for item in selected_primary_lane_ids)
        ):
            global_reason = global_reason or "PRIMARY_LANE_CONFIG_INVALID"

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
            enriched_stage_rows: list[StageAudit] = []
            for stage in lane.stages:
                if isinstance(stage.output, Mapping):
                    enriched_output = enrich_candidate_metadata(stage.output, frozen.data)
                    enriched_stage_rows.append(
                        replace(
                            stage,
                            output=enriched_output,
                            output_hash=_sha256_json(enriched_output),
                        )
                    )
                else:
                    enriched_stage_rows.append(stage)
            enriched_stages = tuple(enriched_stage_rows)
            lane = replace(
                lane,
                stages=enriched_stages,
                final_output=(
                    enriched_stages[-1].output
                    if lane.final_output is not None and enriched_stages
                    else lane.final_output
                ),
            )
            audit_path = self._write_lane_audit(effective_run_id, lane, snapshot=frozen)
            return LaneResult(
                lane=lane.lane,
                model=lane.model,
                status=lane.status,
                stages=lane.stages,
                final_output=lane.final_output,
                audit_path=audit_path,
            )

        indexed_models = (
            tuple(
                (lane_start_index + offset, model)
                for offset, model in enumerate(selected_models)
            )
            if isinstance(lane_start_index, int) and not isinstance(lane_start_index, bool)
            else ()
        )
        if self.parallel_lanes and indexed_models:
            with ThreadPoolExecutor(
                max_workers=min(3, len(indexed_models)),
                thread_name_prefix="liangjian-lane",
            ) as executor:
                lanes = list(executor.map(lambda item: execute_lane(*item), indexed_models))
        else:
            lanes = []
            for index, model in indexed_models:
                lanes.append(execute_lane(index, model))
                # Prompt projections can leave large temporary object graphs.
                # Collect them before the next model lane starts.
                gc.collect()

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
                    status=(
                        "READY_TO_PUBLISH"
                        if lane.status in {"READY", "READY_DEGRADED"}
                        else "BLOCKED"
                    ),
                    snapshot_hash=frozen.snapshot_hash,
                    prompt_hash=prompt_hash,
                    config_hash=config_hash,
                    reason_codes=reasons,
                    outcome=lane.outcome().as_dict(),
                )
                for stage in lane.stages:
                    self.runtime_store.record_workflow_stage(
                        run_id=effective_run_id,
                        lane_id=lane.lane,
                        stage=stage.stage,
                        status=stage.status,
                        reason_codes=stage.reason_codes,
                        outcome=stage.outcome().as_dict(),
                    )

        if self.feature_store is not None and self._feature_generation_id and self._feature_generation_owned:
            try:
                validation_manifest = validate_generation_projection(
                    self.feature_store,
                    self._feature_generation_id,
                )
                validation_manifest = {
                    **validation_manifest,
                    "snapshot_hash": frozen.snapshot_hash,
                    "run_id": effective_run_id,
                    "historical_replay": bool(historical_replay),
                    "lane_statuses": {lane.lane: lane.status for lane in lanes},
                }
                self.feature_store.validate_feature_generation(
                    self._feature_generation_id,
                    validation=validation_manifest,
                )
                self.feature_store.seal_generation(
                    self._feature_generation_id,
                    validation_manifest=validation_manifest,
                    purpose=("HISTORICAL_REPLAY" if historical_replay else "RUN_SNAPSHOT"),
                    activation_eligible=False,
                )
            except (FeatureGenerationError, OSError, sqlite3.Error, ValueError) as exc:
                raise ResearchPipelineError("FEATURE_RUN_GENERATION_FINALIZATION_FAILED") from exc

        run_outcome = aggregate_run_outcome(
            tuple(lane.outcome() for lane in lanes),
            run_id=effective_run_id,
            primary_lane_ids=selected_primary_lane_ids,
            expected_lane_count=len(selected_models),
        )
        overall = run_outcome.legacy_status
        result = ResearchRunResult(
            run_id=effective_run_id,
            generated_at=current,
            snapshot_id=frozen.snapshot_id,
            snapshot_hash=frozen.snapshot_hash if frozen.snapshot_id != "UNKNOWN" else None,
            status=overall,
            lanes=tuple(lanes),
            audit_paths=tuple(lane.audit_path for lane in lanes if lane.audit_path is not None),
            markdown_path=None,
            primary_lane_ids=selected_primary_lane_ids,
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
            primary_lane_ids=result.primary_lane_ids,
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
        if (
            self.settings.research_pipeline_mode == DETERMINISTIC_PIPELINE_MODE
            and snapshot.data.get("DETERMINISTIC_RESEARCH_V2_ENABLED") is True
        ):
            return self._run_lane_v2(
                lane_id=lane_id,
                model=model,
                snapshot=snapshot,
                g0=g0,
                bundle=bundle,
                run_id=run_id,
                global_reason=global_reason,
            )
        audits: list[StageAudit] = []
        upstream_output: Mapping[str, Any] | None = None
        upstream_symbols = set(g0)
        for stage in STAGES:
            if global_reason:
                audits.append(self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, global_reason))
                self._emit_progress(
                    run_id=run_id,
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    completed=0,
                    total=1,
                    status="FAILED",
                    attempts=0,
                    batch_index=1,
                )
                continue
            previous = audits[-1] if audits else None
            if previous is not None and previous.status != "VALIDATED":
                audits.append(
                    self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
                )
                self._emit_progress(
                    run_id=run_id,
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    completed=0,
                    total=1,
                    status="FAILED",
                    attempts=0,
                    batch_index=1,
                )
                continue
            if previous is not None and not previous.symbols:
                audit = self._empty_stage(
                    lane_id=lane_id,
                    model=model,
                    stage=stage,
                    snapshot=snapshot,
                    upstream_output=upstream_output,
                )
                audits.append(audit)
                upstream_output = audit.output
                upstream_symbols = set()
                self._emit_progress(
                    run_id=run_id,
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    completed=1,
                    total=1,
                    status="SKIPPED",
                    attempts=0,
                    batch_index=1,
                )
                continue
            stage_snapshot = snapshot
            if stage in {"A2", "A3"} and self.stage_snapshot_enricher is not None:
                try:
                    stage_snapshot = self._enrich_stage_snapshot(
                        stage=stage,
                        lane_id=lane_id,
                        model=model,
                        upstream_symbols=upstream_symbols,
                        snapshot=snapshot,
                    )
                except Exception:
                    # Enrichment is an optional data boundary.  A malformed or
                    # failed enrichment must never fall back to a different
                    # lane's data or to an unscoped full-universe snapshot.
                    audits.append(
                        self._blocked_stage(
                            lane_id,
                            model,
                            stage,
                            snapshot.snapshot_id,
                            "STAGE_SNAPSHOT_ENRICHMENT_FAILED",
                        )
                    )
                    self._emit_progress(
                        run_id=run_id,
                        lane=lane_id,
                        model=model,
                        stage=stage,
                        completed=0,
                        total=1,
                        status="FAILED",
                        attempts=0,
                        batch_index=1,
                    )
                    continue
            if stage == "A1" and len(upstream_symbols) > self.settings.research_a1_batch_size:
                audit = self._run_a1_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=stage_snapshot,
                    g0=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            elif stage == "A2" and len(upstream_symbols) > self.settings.research_a2_batch_size:
                audit = self._run_a2_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=stage_snapshot,
                    upstream_output=upstream_output or {},
                    upstream_symbols=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            elif stage == "A3" and len(upstream_symbols) > _A3_BATCH_SIZE:
                audit = self._run_a3_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=stage_snapshot,
                    upstream_output=upstream_output or {},
                    upstream_symbols=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            else:
                audit = self._run_stage_with_checkpoint(
                    lane_id=lane_id,
                    model=model,
                    stage=stage,
                    snapshot=stage_snapshot,
                    upstream_output=upstream_output,
                    upstream_symbols=upstream_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
                self._emit_progress(
                    run_id=run_id,
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    completed=1,
                    total=1,
                    status=_progress_status(audit),
                    attempts=audit.attempts,
                    batch_index=1,
                )
            audits.append(audit)
            if audit.status == "VALIDATED":
                upstream_output = audit.output
                upstream_symbols = set(audit.symbols)

        status = "READY" if len(audits) == 3 and all(item.status == "VALIDATED" for item in audits) else "BLOCKED"
        final_output = audits[-1].output if status == "READY" else None
        return LaneResult(lane=lane_id, model=model, status=status, stages=tuple(audits), final_output=final_output)

    def _run_lane_v2(
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
        """Run local full-market gates followed by bounded LLM reviews."""

        if global_reason:
            stages = tuple(
                self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, global_reason)
                for stage in STAGES
            )
            return LaneResult(lane=lane_id, model=model, status="BLOCKED", stages=stages, final_output=None)
        if bundle is None:
            stages = tuple(
                self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "PROMPT_REPOSITORY_BLOCKED")
                for stage in STAGES
            )
            return LaneResult(lane=lane_id, model=model, status="BLOCKED", stages=stages, final_output=None)
        if self._deadline_exceeded():
            stages = tuple(
                self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "RESEARCH_DEADLINE_EXCEEDED")
                for stage in STAGES
            )
            return LaneResult(lane=lane_id, model=model, status="BLOCKED", stages=stages, final_output=None)

        prior_registry = (
            self.feature_store.latest_theme_registry(
                lane_id=lane_id,
                before=snapshot.as_of or self.now(),
            )
            if self.feature_store is not None
            else None
        )
        monthly_strategy_context = build_monthly_strategy_context(
            snapshot.data,
            as_of=snapshot.as_of or self.now(),
            prior_registry=prior_registry,
            policy_lookback_days=self.settings.a1_policy_lookback_days,
            policy_document_limit=self.settings.a1_policy_document_limit,
        )
        # Publish the in-flight discovery stage before the potentially
        # multi-minute model request. Otherwise the dashboard remains on
        # SNAPSHOT_RESUMED until the response finishes, which looks stalled
        # even though all three lanes are actively working.
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage="MACRO_DISCOVERY",
            completed=0,
            total=1,
            status="RUNNING",
            attempts=0,
            batch_index=1,
            processed_symbols=0,
            total_symbols=0,
            industry_count=len(monthly_strategy_context.get("monthly_industry_rotation") or ()),
            monthly_decision_count=len(monthly_strategy_context.get("monthly_industry_decisions") or ()),
        )
        discovery = self._run_stage_with_checkpoint(
            lane_id=lane_id,
            model=model,
            stage="A1",
            snapshot=snapshot,
            upstream_output=None,
            upstream_symbols=set(),
            bundle=bundle,
            run_id=run_id,
            projection_symbols=set(),
            a1_discovery_context={
                "mode": "POLICY_MACRO_DISCOVERY",
                "monthly_strategy_context": monthly_strategy_context,
            },
        )
        if self._deadline_exceeded():
            discovery = self._blocked_stage(
                lane_id, model, "A1", snapshot.snapshot_id, "RESEARCH_DEADLINE_EXCEEDED"
            )
        discovery_progress_output = discovery.output if isinstance(discovery.output, Mapping) else {}
        discovery_diagnostics = _discovery_progress_diagnostics(
            discovery.diagnostics,
            monthly_strategy_context,
        )
        discovery_theme_count = len(discovery_progress_output.get("structural_themes") or ())
        discovery_node_count = len(discovery_progress_output.get("industry_chain_graph") or ())
        discovery_mapping_count = len(discovery_progress_output.get("industry_theme_mappings") or ())
        if not discovery_theme_count:
            discovery_theme_count = _safe_int(discovery_diagnostics.get("theme_count"))
        if not discovery_node_count:
            discovery_node_count = _safe_int(discovery_diagnostics.get("node_count"))
        if not discovery_mapping_count:
            discovery_mapping_count = _safe_int(discovery_diagnostics.get("mapping_count"))
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage="MACRO_DISCOVERY",
            completed=1 if discovery.status == "VALIDATED" else 0,
            total=1,
            status=_progress_status(discovery),
            attempts=discovery.attempts,
            batch_index=1,
            processed_symbols=0,
            total_symbols=0,
            industry_count=len(monthly_strategy_context.get("monthly_industry_rotation") or ()),
            monthly_decision_count=len(monthly_strategy_context.get("monthly_industry_decisions") or ()),
            theme_count=discovery_theme_count,
            node_count=discovery_node_count,
            mapping_count=discovery_mapping_count,
            reason_codes=discovery.reason_codes,
            diagnostics=discovery_diagnostics,
        )
        discovery_output = discovery.output if isinstance(discovery.output, Mapping) else {}
        monthly_discovery_reasons = _monthly_discovery_reasons(
            discovery_output,
            monthly_strategy_context,
        )
        if (
            discovery.status != "VALIDATED"
            or not _valid_a1_discovery_output(discovery_output)
            or monthly_discovery_reasons
        ):
            discovery_reasons = tuple(dict.fromkeys([
                "A1_DISCOVERY_BLOCKED",
                *(discovery.reason_codes or ()),
                *(() if _valid_a1_discovery_output(discovery_output) else ("A1_DISCOVERY_OUTPUT_INVALID",)),
                *monthly_discovery_reasons,
            ]))
            blocked = StageAudit(
                lane=discovery.lane,
                model=discovery.model,
                stage="A1",
                status="BLOCKED",
                snapshot_id=discovery.snapshot_id,
                prompt_hash=discovery.prompt_hash,
                input_hash=discovery.input_hash,
                output_hash=discovery.output_hash,
                latency_ms=discovery.latency_ms,
                attempts=discovery.attempts,
                thinking_variant=discovery.thinking_variant,
                symbols=(),
                reason_codes=discovery_reasons,
                output=discovery.output,
                diagnostics={**dict(discovery.diagnostics or {}), "pipeline_mode": DETERMINISTIC_PIPELINE_MODE},
            )
            downstream = tuple(
                self._not_run_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
                for stage in ("A2", "A3")
            )
            return LaneResult(
                lane=lane_id,
                model=model,
                status="BLOCKED",
                stages=(blocked, *downstream),
                final_output=None,
            )

        # The model response contains semantic mappings only.  Reattach the
        # immutable server-owned monthly decisions before any deterministic
        # A1 screen or downstream stage consumes the discovery output.
        discovery_output = merge_a1_discovery_output(
            discovery_output,
            monthly_strategy_context.get("monthly_industry_decisions")
            if isinstance(monthly_strategy_context, Mapping)
            else None,
        )
        themes = [item for item in discovery_output.get("structural_themes", ()) if isinstance(item, Mapping)]
        nodes = [item for item in discovery_output.get("industry_chain_graph", ()) if isinstance(item, Mapping)]
        if self.feature_store is not None:
            self.feature_store.record_theme_registry(
                run_id=run_id,
                lane_id=lane_id,
                as_of=snapshot.as_of or self.now(),
                themes=themes,
                nodes=nodes,
            )

        a1_gate = screen_a1(
            snapshot.data,
            discovery_output,
            local_top_n_per_node=self.settings.a1_local_top_n_per_node,
            llm_top_n_per_theme=self.settings.a1_llm_representatives_per_theme,
        )
        self._persist_gate(run_id, lane_id, a1_gate, snapshot)
        self._emit_gate_progress(run_id, lane_id, model, a1_gate)
        if self.feature_store is not None:
            self.feature_store.record_taxonomy_links(
                run_id=run_id,
                lane_id=lane_id,
                links=a1_gate.taxonomy_links,
                source_hash=snapshot.snapshot_hash,
            )
        frozen_discovery_context = {
            "mode": "COMPANY_MAPPING",
            "structural_themes": discovery_output["structural_themes"],
            "industry_chain_graph": discovery_output["industry_chain_graph"],
            "taxonomy_links": list(a1_gate.taxonomy_links),
            "monthly_strategy_context": monthly_strategy_context,
            "local_candidates": {
                str(item["symbol"]): item
                for item in a1_gate.decisions
                if item.get("sent_to_llm") is True
            },
        }
        a1_audit = self._run_v2_a1_review(
            lane_id=lane_id,
            model=model,
            snapshot=snapshot,
            g0=g0,
            bundle=bundle,
            run_id=run_id,
            discovery=discovery,
            discovery_output=discovery_output,
            discovery_context=frozen_discovery_context,
            gate=a1_gate,
        )
        if not _stage_completed(a1_audit.status):
            downstream = tuple(
                self._not_run_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
                for stage in ("A2", "A3")
            )
            return LaneResult(
                lane=lane_id,
                model=model,
                status="BLOCKED",
                stages=(a1_audit, *downstream),
                final_output=None,
            )

        a1_output = a1_audit.output if isinstance(a1_audit.output, Mapping) else {}
        a1_symbols = set(a1_audit.symbols)
        if not a1_symbols:
            a2_audit = self._empty_stage(
                run_id=run_id,
                lane_id=lane_id,
                model=model,
                stage="A2",
                snapshot=snapshot,
                upstream_output=a1_output,
                status=STATUS_VALIDATED_NO_OPPORTUNITY,
                outcome="NO_OPPORTUNITY_A1_POOL_EMPTY",
            )
            a3_audit = self._empty_stage(
                run_id=run_id,
                lane_id=lane_id,
                model=model,
                stage="A3",
                snapshot=snapshot,
                upstream_output=a2_audit.output,
                status=STATUS_VALIDATED_NO_ACTION,
                outcome="NO_ACTION_UPSTREAM_NO_OPPORTUNITY",
            )
            return LaneResult(
                lane=lane_id,
                model=model,
                status="READY",
                stages=(a1_audit, a2_audit, a3_audit),
                final_output=a3_audit.output,
            )

        try:
            a2_snapshot = self._enrich_stage_snapshot(
                stage="A2",
                lane_id=lane_id,
                model=model,
                upstream_symbols=a1_symbols,
                snapshot=snapshot,
            ) if self.stage_snapshot_enricher is not None else snapshot
        except Exception:
            blocked = self._blocked_stage(
                lane_id, model, "A2", snapshot.snapshot_id, "STAGE_SNAPSHOT_ENRICHMENT_FAILED"
            )
            return LaneResult(
                lane=lane_id,
                model=model,
                status="BLOCKED",
                stages=(a1_audit, blocked, self._not_run_stage(lane_id, model, "A3", snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")),
                final_output=None,
            )
        a2_gate = screen_a2(
            a2_snapshot.data,
            a1_output,
            minimum_identifiability_score=float(
                a2_snapshot.data.get("MIN_IDENTIFIABILITY_SCORE") or 60.0
            ),
            llm_top_n_per_theme=self.settings.a2_llm_top_n_per_theme,
        )
        a2_snapshot = _with_a2_bottleneck_context(a2_snapshot, a2_gate)
        self._persist_gate(run_id, lane_id, a2_gate, a2_snapshot)
        self._emit_gate_progress(run_id, lane_id, model, a2_gate)
        a2_audit = self._run_v2_downstream_review(
            lane_id=lane_id,
            model=model,
            stage="A2",
            snapshot=a2_snapshot,
            upstream_output=a1_output,
            full_upstream_symbols=a1_symbols,
            gate=a2_gate,
            bundle=bundle,
            run_id=run_id,
        )
        if not _stage_completed(a2_audit.status):
            blocked = self._not_run_stage(lane_id, model, "A3", snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
            return LaneResult(
                lane=lane_id,
                model=model,
                status="BLOCKED",
                stages=(a1_audit, a2_audit, blocked),
                final_output=None,
            )

        a2_output = a2_audit.output if isinstance(a2_audit.output, Mapping) else {}
        a2_symbols = set(a2_audit.symbols)
        if not a2_symbols:
            if a2_audit.status in {
                STATUS_BLOCKED_EVIDENCE_GAP,
                STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
            }:
                a3_audit = self._not_run_stage(
                    lane_id, model, "A3", snapshot.snapshot_id, "UPSTREAM_DATA_INSUFFICIENT"
                )
                lane_status = "READY_DEGRADED"
            else:
                a3_audit = self._empty_stage(
                    run_id=run_id,
                    lane_id=lane_id,
                    model=model,
                    stage="A3",
                    snapshot=a2_snapshot,
                    upstream_output=a2_output,
                    status=STATUS_VALIDATED_NO_ACTION,
                    outcome="NO_ACTION_UPSTREAM_NO_OPPORTUNITY",
                )
                lane_status = "READY"
            return LaneResult(
                lane=lane_id,
                model=model,
                status=lane_status,
                stages=(a1_audit, a2_audit, a3_audit),
                final_output=(
                    a3_audit.output if lane_status == "READY" else a2_output
                    if lane_status == "READY_DEGRADED"
                    else None
                ),
            )

        try:
            a3_snapshot = self._enrich_stage_snapshot(
                stage="A3",
                lane_id=lane_id,
                model=model,
                upstream_symbols=a2_symbols,
                snapshot=a2_snapshot,
            ) if self.stage_snapshot_enricher is not None else a2_snapshot
        except Exception:
            a3_audit = self._blocked_stage(
                lane_id, model, "A3", snapshot.snapshot_id, "STAGE_SNAPSHOT_ENRICHMENT_FAILED"
            )
            return LaneResult(
                lane=lane_id,
                model=model,
                status="BLOCKED",
                stages=(a1_audit, a2_audit, a3_audit),
                final_output=None,
            )
        a3_gate = screen_a3(a3_snapshot.data, a2_output)
        self._persist_gate(run_id, lane_id, a3_gate, a3_snapshot)
        self._emit_gate_progress(run_id, lane_id, model, a3_gate)
        a3_audit = self._run_v2_downstream_review(
            lane_id=lane_id,
            model=model,
            stage="A3",
            snapshot=a3_snapshot,
            upstream_output=a2_output,
            full_upstream_symbols=a2_symbols,
            gate=a3_gate,
            bundle=bundle,
            run_id=run_id,
        )
        status = _lane_status_from_stages((a1_audit, a2_audit, a3_audit))
        return LaneResult(
            lane=lane_id,
            model=model,
            status=status,
            stages=(a1_audit, a2_audit, a3_audit),
            final_output=a3_audit.output if status in {"READY", "READY_DEGRADED"} else None,
        )

    def _run_v2_a1_review(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        g0: set[str],
        bundle: PromptBundle,
        run_id: str,
        discovery: StageAudit,
        discovery_output: Mapping[str, Any],
        discovery_context: Mapping[str, Any],
        gate: DeterministicGateResult,
    ) -> StageAudit:
        batches = _chunk_symbol_sets(gate.review_symbols, self.settings.research_a1_batch_size)
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage="A1_LLM_REVIEW",
            completed=0,
            total=len(batches),
            status="RUNNING",
            attempts=0,
            processed_symbols=0,
            total_symbols=len(gate.review_symbols),
        )
        request_audits: list[StageAudit] = []
        valid_audits: list[StageAudit] = []
        split_count = 0
        blocked: StageAudit | None = None
        if batches:
            request_audits, valid_audits, split_count, blocked, _ = self._execute_batch_plan(
                batches=batches,
                lane_id=lane_id,
                model=model,
                stage="A1",
                progress_stage="A1_LLM_REVIEW",
                run_id=run_id,
                snapshot_id=snapshot.snapshot_id,
                runner=lambda batch: self._run_stage_with_checkpoint(
                    lane_id=lane_id,
                    model=model,
                    stage="A1",
                    snapshot=snapshot,
                    upstream_output=None,
                    upstream_symbols=batch,
                    bundle=bundle,
                    run_id=run_id,
                    projection_symbols=batch,
                    a1_discovery_context=discovery_context,
                ),
                splittable=_a1_batch_is_splittable,
            )
        if blocked is not None:
            self._emit_progress(
                run_id=run_id,
                lane=lane_id,
                model=model,
                stage="A1_LLM_REVIEW",
                completed=len(valid_audits),
                total=len(batches),
                status="FAILED",
                attempts=sum(item.attempts for item in request_audits),
                processed_symbols=sum(len(item.symbols) for item in valid_audits),
                total_symbols=len(gate.review_symbols),
            )
            return StageAudit(
                lane=lane_id,
                model=model,
                stage="A1",
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=_combined_digest(item.prompt_hash for item in (discovery, *request_audits)),
                input_hash=_combined_digest(item.input_hash for item in (discovery, *request_audits)),
                output_hash=None,
                latency_ms=sum(item.latency_ms or 0 for item in (discovery, *request_audits)),
                attempts=sum(item.attempts for item in (discovery, *request_audits)),
                thinking_variant=_common_variant((discovery, *request_audits)),
                symbols=(),
                reason_codes=tuple(f"A1_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes) or ("A1_BATCH_BLOCKED",),
                diagnostics={"pipeline_mode": DETERMINISTIC_PIPELINE_MODE, "local_screen": gate.summary},
            )
        outputs = [discovery_output, *(audit.output for audit in valid_audits if isinstance(audit.output, Mapping))]
        merged = _merge_a1_outputs(outputs)
        if isinstance(discovery_output.get("monthly_industry_decisions"), list):
            # Monthly industry decisions belong to the one macro-discovery
            # call.  Company mapping batches may repeat the prompt schema but
            # cannot rewrite the frozen monthly ranking decisions.
            merged["monthly_industry_decisions"] = [
                dict(item)
                for item in discovery_output["monthly_industry_decisions"]
                if isinstance(item, Mapping)
            ]
        merged["taxonomy_links"] = list(gate.taxonomy_links)
        merged["active_research_pool"] = _deduplicate_stage_items(
            "active_research_pool",
            [*merged.get("active_research_pool", []), *local_active_items(gate)],
        )
        merged["monitor_pool"] = _deduplicate_stage_items(
            "monitor_pool",
            [*merged.get("monitor_pool", []), *local_monitor_items(gate)],
        )
        merged["rejected_candidates"] = _deduplicate_stage_items(
            "rejected_candidates",
            [*merged.get("rejected_candidates", []), *local_rejected_items(gate)],
        )
        merged["local_screen_summary"] = gate.summary
        merged = _refresh_analysis_counts(merged, "A1")
        merged = _annotate_a1_pool_target(merged, snapshot.data)
        reasons = _validate_output(
            merged,
            stage="A1",
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=g0,
            snapshot_data=snapshot.data,
        )
        approved_symbols = tuple(sorted(_approved_symbols(merged, "A1")))
        stage_status, outcome_reasons = _classify_stage_outcome(
            "A1", merged, reasons=reasons, gate=gate
        )
        reasons = list(dict.fromkeys([*reasons, *outcome_reasons]))
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
                stage="A1_LLM_REVIEW",
                completed=len(valid_audits),
                total=len(batches),
                status=_progress_status_for_stage_status(stage_status),
                attempts=sum(item.attempts for item in request_audits),
                processed_symbols=len(gate.review_symbols),
                total_symbols=len(gate.review_symbols),
                selected_symbols=len(approved_symbols),
                reason_codes=reasons,
                outcome=stage_status,
        )
        audits = (discovery, *request_audits)
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A1",
            status=stage_status,
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=_sha256_json(merged),
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=approved_symbols,
            reason_codes=tuple(reasons),
            output=merged,
            diagnostics={
                "pipeline_mode": DETERMINISTIC_PIPELINE_MODE,
                "local_screen": gate.summary,
                "monthly_strategy_status": (
                    discovery_context.get("monthly_strategy_context", {}).get("status")
                    if isinstance(discovery_context.get("monthly_strategy_context"), Mapping)
                    else None
                ),
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "split_count": split_count,
                "pool_counts": _stage_pool_counts(merged, "A1"),
            },
        )

    def _run_v2_downstream_review(
        self,
        *,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any],
        full_upstream_symbols: set[str],
        gate: DeterministicGateResult,
        bundle: PromptBundle,
        run_id: str,
    ) -> StageAudit:
        review_symbols = set(gate.review_symbols)
        progress_stage = f"{stage}_LLM_REVIEW"
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage=progress_stage,
            completed=0,
            total=1 if review_symbols else 0,
            status="RUNNING",
            attempts=0,
            processed_symbols=0,
            total_symbols=len(review_symbols),
        )
        if self._deadline_exceeded():
            return self._blocked_stage(
                lane_id, model, stage, snapshot.snapshot_id, "RESEARCH_DEADLINE_EXCEEDED"
            )
        if review_symbols:
            if stage == "A2" and len(review_symbols) > self.settings.research_a2_batch_size:
                audit = self._run_a2_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=snapshot,
                    upstream_output=upstream_output,
                    upstream_symbols=review_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            elif stage == "A3" and len(review_symbols) > _A3_BATCH_SIZE:
                audit = self._run_a3_batched(
                    lane_id=lane_id,
                    model=model,
                    snapshot=snapshot,
                    upstream_output=upstream_output,
                    upstream_symbols=review_symbols,
                    bundle=bundle,
                    run_id=run_id,
                )
            else:
                audit = self._run_stage_with_checkpoint(
                    lane_id=lane_id,
                    model=model,
                    stage=stage,
                    snapshot=snapshot,
                    upstream_output=upstream_output,
                    upstream_symbols=review_symbols,
                    bundle=bundle,
                    run_id=run_id,
                    projection_symbols=review_symbols,
                )
            if self._deadline_exceeded():
                audit = self._blocked_stage(
                    lane_id, model, stage, snapshot.snapshot_id, "RESEARCH_DEADLINE_EXCEEDED"
                )
            if not _stage_completed(audit.status):
                self._emit_progress(
                    run_id=run_id,
                    lane=lane_id,
                    model=model,
                    stage=progress_stage,
                    completed=0,
                    total=1,
                    status="FAILED",
                    attempts=audit.attempts,
                    processed_symbols=0,
                    total_symbols=len(review_symbols),
                    reason_codes=audit.reason_codes,
                )
                return audit
            output = dict(audit.output or {})
        else:
            runtime = _runtime_input(snapshot, lane_id, model, stage, upstream_output, set())
            output = {"envelope": runtime["required_envelope"], "analysis_summary": {"outcome": "NO_LOCAL_REVIEW_CANDIDATES"}}
            if stage == "A2":
                output.update({"active_themes": [], "focus_pool": [], "watch_only_pool": []})
            else:
                output.update({"core_watch_pool": [], "secondary_watch_pool": [], "rejected_candidates": []})
            audit = StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="VALIDATED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=None,
                input_hash=_sha256_json(runtime),
                output_hash=None,
                latency_ms=0,
                attempts=0,
                thinking_variant="deterministic_noop",
                symbols=(),
                reason_codes=(),
                output=output,
                diagnostics={},
            )
        # Keep the provider-reviewed partition separate from deterministic
        # local rows appended below.  A2 local monitor rows intentionally
        # carry evidence-gap reasons (for example
        # ``A2_FACTOR_COVERAGE_BELOW_MINIMUM``), but those rows were not
        # reviewed by the model and must not turn a valid zero-focus model
        # response into a stage-level evidence block.
        reviewed_output = dict(output)
        if stage == "A2":
            output["watch_only_pool"] = _deduplicate_stage_items(
                "watch_only_pool",
                [*output.get("watch_only_pool", []), *_gate_secondary_items(gate, stage)],
            )
            output.setdefault("crowded_pool", [])
            output.setdefault("low_identity_pool", [])
            output.setdefault("rejected_candidates", [])
        else:
            output["rejected_candidates"] = _deduplicate_stage_items(
                "rejected_candidates",
                [*output.get("rejected_candidates", []), *_gate_secondary_items(gate, stage)],
            )
            output.setdefault("secondary_watch_pool", [])
        output["local_screen_summary"] = gate.summary
        output = _refresh_analysis_counts(output, stage)
        reasons = _validate_output(
            output,
            stage=stage,
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=full_upstream_symbols,
            snapshot_data=snapshot.data,
        )
        approved_symbols = tuple(sorted(_approved_symbols(output, stage)))
        stage_status, outcome_reasons = _classify_stage_outcome(
            stage,
            output,
            reasons=reasons,
            gate=gate,
            reviewed_output=reviewed_output,
        )
        reasons = list(dict.fromkeys([*reasons, *outcome_reasons]))
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage=progress_stage,
            completed=1 if review_symbols else 0,
            total=1 if review_symbols else 0,
            status=_progress_status_for_stage_status(stage_status),
            attempts=audit.attempts,
            processed_symbols=len(review_symbols),
            total_symbols=len(review_symbols),
            selected_symbols=len(approved_symbols),
            reason_codes=reasons,
            outcome=stage_status,
        )
        return StageAudit(
            lane=audit.lane,
            model=audit.model,
            stage=audit.stage,
            status=stage_status,
            snapshot_id=audit.snapshot_id,
            prompt_hash=audit.prompt_hash,
            input_hash=audit.input_hash,
            output_hash=_sha256_json(output),
            latency_ms=audit.latency_ms,
            attempts=audit.attempts,
            thinking_variant=audit.thinking_variant,
            symbols=approved_symbols,
            reason_codes=tuple(reasons),
            output=output,
            diagnostics={**dict(audit.diagnostics or {}), "pipeline_mode": DETERMINISTIC_PIPELINE_MODE, "local_screen": gate.summary},
        )

    def _persist_gate(
        self,
        run_id: str,
        lane_id: str,
        gate: DeterministicGateResult,
        snapshot: FrozenInputSnapshot,
    ) -> None:
        if self.feature_store is None:
            return
        try:
            self.feature_store.replace_stage_decisions(
                run_id=run_id,
                lane_id=lane_id,
                stage=gate.stage,
                decisions=gate.decisions,
                updated_at=snapshot.as_of or self.now(),
            )
            if gate.stage == "A1_LOCAL_SCREEN":
                self.feature_store.record_fundamental_features(
                    as_of=snapshot.as_of or self.now(),
                    decisions=gate.decisions,
                    run_id=run_id,
                )
            elif gate.stage == "A2_LOCAL_ROLE":
                self.feature_store.record_market_role_features(
                    run_id=run_id,
                    lane_id=lane_id,
                    decisions=gate.decisions,
                )
        except (FeatureGenerationError, OSError, sqlite3.Error, ValueError) as exc:
            raise ResearchPipelineError("FEATURE_STORE_WRITE_FAILED") from exc

    def _emit_gate_progress(
        self,
        run_id: str,
        lane_id: str,
        model: str,
        gate: DeterministicGateResult,
    ) -> None:
        summary = gate.summary
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage=gate.stage,
            completed=1,
            total=1,
            status="COMPLETED",
            attempts=0,
            batch_index=1,
            processed_symbols=int(summary["evaluated_count"]),
            total_symbols=int(summary["evaluated_count"]),
            selected_symbols=int(summary["sent_to_llm_count"]),
            monitor_symbols=int(summary["monitor_count"]),
            rejected_symbols=int(summary["rejected_count"]),
        )

    def _deadline_exceeded(self) -> bool:
        started = self._deadline_started_monotonic
        if started is None:
            return False
        return time.monotonic() - started >= float(self.settings.research_close_deadline_seconds)

    def _emit_progress(
        self,
        *,
        run_id: str,
        lane: str,
        model: str | None = None,
        stage: str,
        completed: int,
        total: int,
        status: str,
        attempts: int,
        batch_index: int | None = None,
        processed_symbols: int | None = None,
        total_symbols: int | None = None,
        selected_symbols: int | None = None,
        monitor_symbols: int | None = None,
        rejected_symbols: int | None = None,
        industry_count: int | None = None,
        monthly_decision_count: int | None = None,
        theme_count: int | None = None,
        node_count: int | None = None,
        mapping_count: int | None = None,
        reason_codes: Sequence[str] | None = None,
        outcome: str | None = None,
        checkpoint_reused: bool | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        """Send a redacted, stable progress event to the optional observer."""

        if self.progress_callback is None:
            return
        completed_value = max(0, int(completed))
        total_value = max(0, int(total))
        event: dict[str, Any] = {
            "run_id": str(run_id),
            "lane": str(lane),
            "stage": str(stage),
            "batch": {"completed": completed_value, "total": total_value},
            # Keep flat aliases for lightweight loggers and API adapters.
            "completed": completed_value,
            "total": total_value,
            "batch_completed": completed_value,
            "batch_total": total_value,
            "completed_batches": completed_value,
            "total_batches": total_value,
            "status": str(status),
            "attempts": max(0, int(attempts)),
        }
        if model:
            event["model"] = str(model)
        if batch_index is not None:
            event["batch_index"] = max(1, int(batch_index))
        safe_reason_codes = _safe_progress_reason_codes(reason_codes)
        if safe_reason_codes:
            event["reason_codes"] = safe_reason_codes
        if outcome:
            safe_outcome = _safe_progress_reason_codes((outcome,))
            if safe_outcome:
                event["outcome"] = safe_outcome[0]
        safe_diagnostics = _safe_progress_diagnostics(diagnostics)
        if safe_diagnostics:
            event["diagnostics"] = safe_diagnostics
        reused = checkpoint_reused if checkpoint_reused is not None else str(status).upper() == "REUSED"
        if reused:
            event["checkpoint_reused"] = True
            if batch_index is not None:
                event["checkpoint_batch_index"] = max(1, int(batch_index))
        for key, value in (
            ("processed_symbols", processed_symbols),
            ("total_symbols", total_symbols),
            ("selected_symbols", selected_symbols),
            ("monitor_symbols", monitor_symbols),
            ("rejected_symbols", rejected_symbols),
            ("industry_count", industry_count),
            ("monthly_decision_count", monthly_decision_count),
            ("theme_count", theme_count),
            ("node_count", node_count),
            ("mapping_count", mapping_count),
        ):
            if value is not None:
                event[key] = max(0, int(value))
        try:
            self.progress_callback(event)
        except Exception:
            # Observability must not change the model or fail-closed contract.
            return

    def _enrich_stage_snapshot(
        self,
        *,
        stage: str,
        lane_id: str,
        model: str,
        upstream_symbols: set[str],
        snapshot: FrozenInputSnapshot,
    ) -> FrozenInputSnapshot:
        """Load an immutable, lane-scoped snapshot for A2 or A3.

        The callback receives a frozen base snapshot and a ``frozenset`` of
        the current upstream pool.  A returned mapping is interpreted as a
        data overlay; a returned ``FrozenInputSnapshot`` is used verbatim.
        Several compatible callback signatures are accepted so integrations
        can remain small without weakening the lane/scope boundary.
        """

        callback = self.stage_snapshot_enricher
        if callback is None:
            return snapshot
        symbols = frozenset(upstream_symbols)
        callback_snapshot = _freeze_snapshot(snapshot)
        result = _invoke_stage_snapshot_enricher(
            callback,
            stage=stage,
            lane_id=lane_id,
            model=model,
            upstream_symbols=symbols,
            snapshot=callback_snapshot,
        )
        if result is None:
            return snapshot
        if isinstance(result, FrozenInputSnapshot):
            return _freeze_snapshot(result)
        if isinstance(result, Mapping):
            # A full wrapper can be returned by a data adapter.  Otherwise the
            # mapping is an immutable overlay on the base stage snapshot.
            nested = result.get("data")
            if isinstance(nested, Mapping):
                data = dict(nested)
                snapshot_id = str(result.get("snapshot_id") or "")
                snapshot_hash = str(result.get("snapshot_hash") or "")
                as_of = result.get("as_of", snapshot.as_of)
                if not snapshot_id:
                    snapshot_id = f"{snapshot.snapshot_id}:{stage.lower()}"
                return FrozenInputSnapshot(
                    snapshot_id=snapshot_id,
                    data=data,
                    snapshot_hash=snapshot_hash,
                    as_of=as_of,
                )
            merged = dict(snapshot.data)
            merged.update(dict(result))
            # The base snapshot is already content-addressed.  Hash only the
            # stage overlay here; traversing the full-market evidence tree for
            # every lane would erase the performance benefit of enrichment.
            overlay_hash = _sha256_json({
                "base_snapshot_hash": snapshot.snapshot_hash,
                "stage": stage,
                "overlay": result,
            })
            return FrozenInputSnapshot(
                snapshot_id=f"{snapshot.snapshot_id}:{stage.lower()}:{overlay_hash[:12]}",
                data=merged,
                snapshot_hash=overlay_hash,
                as_of=snapshot.as_of,
            )
        # Accept adapters that return a canonical snapshot object, but do not
        # let arbitrary model/data objects cross the immutable boundary.
        try:
            coerced = _coerce_snapshot(result)
        except Exception as exc:
            raise ResearchPipelineError("STAGE_SNAPSHOT_ENRICHMENT_INVALID") from exc
        if not isinstance(coerced, FrozenInputSnapshot):
            raise ResearchPipelineError("STAGE_SNAPSHOT_ENRICHMENT_INVALID")
        return _freeze_snapshot(coerced)

    def _checkpoint_key(
        self,
        *,
        run_id: str,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any] | None,
        upstream_symbols: set[str],
        bundle: PromptBundle | None,
        projection_symbols: set[str] | None,
        a1_discovery_context: Mapping[str, Any] | None,
    ) -> tuple[ResearchCheckpointKey | None, _PreparedStageRequest | None]:
        if self.checkpoint_store is None or bundle is None:
            return None, None
        try:
            prepared = self._prepare_stage_request(
                lane_id=lane_id,
                model=model,
                stage=stage,
                snapshot=snapshot,
                upstream_output=upstream_output,
                upstream_symbols=upstream_symbols,
                bundle=bundle,
                projection_symbols=projection_symbols,
                a1_discovery_context=a1_discovery_context,
            )
        except Exception:
            return None, None
        return (
            ResearchCheckpointKey(
                run_id=_safe_run_id(run_id),
                lane=lane_id,
                stage=stage,
                model=model,
                prompt_hash=prepared.prompt_hash,
                snapshot_hash=snapshot.snapshot_hash,
                batch_symbols_hash=_sha256_json(sorted(upstream_symbols)),
                generation_id=self._feature_generation_id,
                pipeline_contract_hash=self._pipeline_contract_hash,
                feature_contract_hash=self._feature_contract_hash,
                code_commit=self._code_commit,
                provider_contract_hash=_sha256_json({
                    "model": model,
                    "base_url": self.settings.model_base_url,
                }),
            ),
            prepared,
        )

    def _load_checkpoint(
        self,
        *,
        key: ResearchCheckpointKey,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_symbols: set[str],
        a1_discovery_context: Mapping[str, Any] | None = None,
    ) -> StageAudit | None:
        store = self.checkpoint_store
        if store is None:
            return None
        try:
            if isinstance(store, Mapping):
                record = store.get(key.digest)
            else:
                operation = getattr(store, "load_strict", None) if key.has_v2_identity else None
                operation = operation or getattr(store, "load", None) or getattr(store, "get", None)
                if operation is None:
                    return None
                try:
                    record = operation(key)
                except (KeyError, TypeError):
                    record = operation(key.digest)
        except Exception:
            return None
        if not isinstance(record, Mapping):
            return None
        stored_key = record.get("key")
        if isinstance(stored_key, Mapping) and dict(stored_key) != key.as_dict():
            return None
        if str(record.get("status") or "") not in _COMPLETED_STAGE_STATUSES:
            return None
        raw = record.get("audit", record)
        if not isinstance(raw, Mapping):
            return None
        authorized_discovery_refs: tuple[str, ...] = (
            tuple(a1_discovery_context.get("authorized_discovery_source_refs", ()))
            if isinstance(a1_discovery_context, Mapping)
            else ()
        )
        try:
            output = raw.get("output")
            if not isinstance(output, Mapping):
                return None
            output = _strip_reasoning(output)
            output, _ = mechanical_repair_output(output)
            output, _ = _normalize_server_envelope(
                output,
                _required_envelope(snapshot, lane_id, model, stage),
            )
            if (
                stage == "A1"
                and isinstance(a1_discovery_context, Mapping)
                and a1_discovery_context.get("mode") == "POLICY_MACRO_DISCOVERY"
            ):
                output, _ = _normalize_a1_discovery_source_refs(
                    output,
                    tuple(a1_discovery_context.get("authorized_discovery_source_refs", ())),
                )
            reasons = _validate_output(
                output,
                stage=stage,
                model=model,
                snapshot_id=snapshot.snapshot_id,
                upstream_symbols=upstream_symbols,
                snapshot_data=snapshot.data,
            )
            # A discovery checkpoint is only reusable when it still satisfies
            # the frozen monthly context.  The generic output validator cannot
            # see the canonical decision rows and would otherwise accept an
            # incomplete/old mapping response after a process restart.
            if stage == "A1" and isinstance(a1_discovery_context, Mapping):
                reasons.extend(_a1_discovery_context_reasons(output, a1_discovery_context))
                if (
                    a1_discovery_context.get("mode") == "POLICY_MACRO_DISCOVERY"
                    and _a1_discovery_evidence_required(a1_discovery_context)
                ):
                    reasons.extend(
                        _a1_discovery_evidence_reasons(
                            output,
                            snapshot.data,
                            authorized_source_refs=authorized_discovery_refs,
                        )
                    )
                    monthly_context = a1_discovery_context.get("monthly_strategy_context")
                    if isinstance(monthly_context, Mapping):
                        reasons.extend(_monthly_discovery_reasons(output, monthly_context))
            if reasons:
                return None
            symbols = tuple(sorted(_approved_symbols(output, stage)))
            if tuple(sorted(str(item) for item in (raw.get("symbols") or ()))) != symbols:
                return None
            audit = StageAudit(
                lane=str(raw.get("lane") or lane_id),
                model=str(raw.get("model") or model),
                stage=str(raw.get("stage") or stage),
                status=str(raw.get("status") or ""),
                snapshot_id=str(raw.get("snapshot_id") or ""),
                prompt_hash=str(raw.get("prompt_hash") or ""),
                input_hash=str(raw.get("input_hash") or "") or None,
                output_hash=str(raw.get("output_hash") or "") or None,
                latency_ms=_safe_optional_int(raw.get("latency_ms")),
                attempts=_safe_int(raw.get("attempts")),
                thinking_variant=str(raw.get("thinking_variant") or "unknown"),
                symbols=symbols,
                reason_codes=tuple(str(item) for item in (raw.get("reason_codes") or ())),
                output=output,
                diagnostics=raw.get("diagnostics") if isinstance(raw.get("diagnostics"), Mapping) else None,
            )
        except (TypeError, ValueError):
            return None
        if (
            audit.lane != lane_id
            or audit.model != model
            or audit.stage != stage
            or not _stage_completed(audit.status)
            or audit.snapshot_id != snapshot.snapshot_id
            or audit.prompt_hash != key.prompt_hash
            or audit.output_hash != _sha256_json(output)
            or audit.reason_codes
        ):
            return None
        diagnostics = dict(audit.diagnostics or {})
        diagnostics["checkpoint_reused"] = True
        return StageAudit(
            lane=audit.lane,
            model=audit.model,
            stage=audit.stage,
            status=audit.status,
            snapshot_id=audit.snapshot_id,
            prompt_hash=audit.prompt_hash,
            input_hash=audit.input_hash,
            output_hash=audit.output_hash,
            latency_ms=audit.latency_ms,
            attempts=audit.attempts,
            thinking_variant=audit.thinking_variant,
            symbols=audit.symbols,
            reason_codes=audit.reason_codes,
            output=audit.output,
            diagnostics=diagnostics,
        )

    def _save_checkpoint(self, *, key: ResearchCheckpointKey, audit: StageAudit) -> bool:
        store = self.checkpoint_store
        if store is None or not _stage_checkpointable(audit.status):
            return False
        record = {
            "key": key.as_dict(),
            "status": audit.status,
            "audit": _strip_reasoning(audit.as_dict()),
        }
        try:
            if isinstance(store, Mapping):
                # A plain mapping is accepted for tiny test doubles only when
                # it is mutable; immutable mappings remain read-only.
                store[key.digest] = record  # type: ignore[index]
            else:
                operation = getattr(store, "save", None) or getattr(store, "put", None)
                if operation is None:
                    return False
                try:
                    operation(key, record)
                except (KeyError, TypeError):
                    operation(key.digest, record)
            return True
        except Exception:
            return False

    def _run_stage_with_checkpoint(
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
        a1_discovery_context: Mapping[str, Any] | None = None,
    ) -> StageAudit:
        key, prepared_request = self._checkpoint_key(
            run_id=run_id,
            lane_id=lane_id,
            model=model,
            stage=stage,
            snapshot=snapshot,
            upstream_output=upstream_output,
            upstream_symbols=upstream_symbols,
            bundle=bundle,
            projection_symbols=projection_symbols,
            a1_discovery_context=a1_discovery_context,
        )
        if key is not None:
            reused = self._load_checkpoint(
                key=key,
                lane_id=lane_id,
                model=model,
                stage=stage,
                snapshot=snapshot,
                upstream_symbols=upstream_symbols,
                a1_discovery_context=a1_discovery_context,
            )
            if reused is not None:
                return reused
        audit = self._run_stage(
            lane_id=lane_id,
            model=model,
            stage=stage,
            snapshot=snapshot,
            upstream_output=upstream_output,
            upstream_symbols=upstream_symbols,
            bundle=bundle,
            run_id=run_id,
            projection_symbols=projection_symbols,
            a1_discovery_context=a1_discovery_context,
            prepared_request=prepared_request,
        )
        if key is not None and _stage_checkpointable(audit.status):
            self._save_checkpoint(key=key, audit=audit)
        return audit

    def _empty_stage(
        self,
        *,
        run_id: str | None = None,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any] | None,
        status: str = STATUS_VALIDATED,
        outcome: str = "NO_ACTION",
    ) -> StageAudit:
        runtime = _runtime_input(
            snapshot,
            lane_id,
            model,
            stage,
            upstream_output,
            set(),
        )
        output: dict[str, Any] = {
            "envelope": runtime["required_envelope"],
            "analysis_summary": {
                "outcome": outcome,
                "reason_codes": ["UPSTREAM_POOL_EMPTY"],
            },
            "rejected_candidates": [],
        }
        if stage == "A2":
            output.update({"active_themes": [], "focus_pool": [], "watch_only_pool": []})
        elif stage == "A3":
            output.update({"core_watch_pool": [], "secondary_watch_pool": []})
        else:
            raise ResearchPipelineError("EMPTY_STAGE_UNSUPPORTED")
        reasons = _validate_output(
            output,
            stage=stage,
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=set(),
            snapshot_data=snapshot.data,
        )
        if reasons:
            return self._blocked_stage(
                lane_id,
                model,
                stage,
                snapshot.snapshot_id,
                "EMPTY_STAGE_CONTRACT_INVALID",
            )
        audit = StageAudit(
            lane=lane_id,
            model=model,
            stage=stage,
            status=status,
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=None,
            input_hash=_sha256_json(runtime),
            output_hash=_sha256_json(output),
            latency_ms=0,
            attempts=0,
            thinking_variant="deterministic_noop",
            symbols=(),
            reason_codes=(),
            output=output,
            diagnostics=(
                {"outcome_code": "NO_ACTION_UPSTREAM_POOL_EMPTY"}
                if status == STATUS_VALIDATED and outcome == "NO_ACTION"
                else {"outcome_code": outcome, "upstream_pool_empty": True}
            ),
        )
        if run_id is not None:
            self._emit_progress(
                run_id=run_id,
                lane=lane_id,
                model=model,
                stage=f"{stage}_LLM_REVIEW",
                completed=0,
                total=0,
                status=_progress_status_for_stage_status(status),
                attempts=0,
                processed_symbols=0,
                total_symbols=0,
                selected_symbols=0,
                outcome=status,
            )
        return audit

    def _execute_batch_plan(
        self,
        *,
        batches: Sequence[set[str]],
        lane_id: str,
        model: str,
        stage: str,
        progress_stage: str | None = None,
        run_id: str,
        snapshot_id: str,
        runner: Callable[[set[str]], StageAudit],
        splittable: Callable[[Sequence[str]], bool],
    ) -> tuple[list[StageAudit], list[StageAudit], int, StageAudit | None, int]:
        """Execute a deterministic batch plan with optional bounded workers.

        The returned audits are ordered by their logical batch path, not by
        completion time.  A failed splittable batch is retried as two smaller
        batches; any non-splittable failure remains fail-closed.  This keeps
        the old serial semantics when ``batch_workers=1`` while making the
        parallel case deterministic at merge time.
        """

        emitted_stage = progress_stage or stage
        pending: list[tuple[tuple[int, ...], set[str]]] = [
            ((index,), set(batch)) for index, batch in enumerate(batches)
        ]
        initial_total = len(pending)
        total = initial_total
        request_audits: list[tuple[tuple[int, ...], StageAudit]] = []
        valid_audits: dict[tuple[int, ...], StageAudit] = {}
        split_count = 0
        completed = 0
        blocked: StageAudit | None = None
        def invoke(batch: set[str]) -> StageAudit:
            if self._deadline_exceeded():
                return self._blocked_stage(
                    lane_id,
                    model,
                    stage,
                    snapshot_id,
                    "RESEARCH_DEADLINE_EXCEEDED",
                )
            try:
                audit = runner(batch)
                if self._deadline_exceeded():
                    return self._blocked_stage(
                        lane_id,
                        model,
                        stage,
                        snapshot_id,
                        "RESEARCH_DEADLINE_EXCEEDED",
                    )
                return audit
            except Exception:
                return self._blocked_stage(
                    lane_id,
                    model,
                    stage,
                    snapshot_id,
                    "BATCH_EXECUTION_FAILED",
                )
        executor = (
            ThreadPoolExecutor(max_workers=self.batch_workers, thread_name_prefix="liangjian-batch")
            if self.batch_workers > 1
            else None
        )
        try:
            while pending:
                if executor is None or split_count > 0:
                    current = [pending.pop(0)]
                    results = [(current[0][0], invoke(current[0][1]))]
                else:
                    current = pending[: self.batch_workers]
                    del pending[: self.batch_workers]
                    # executor.map preserves the input order even when model
                    # responses complete out of order.
                    results = [
                        (item[0], audit)
                        for item, audit in zip(current, executor.map(lambda item: invoke(item[1]), current))
                    ]
                children: list[tuple[tuple[int, ...], set[str]]] = []
                for order, batch_audit in results:
                    request_audits.append((order, batch_audit))
                    if _stage_completed(batch_audit.status):
                        valid_audits[order] = batch_audit
                        completed += 1
                        self._emit_progress(
                            run_id=run_id,
                            lane=lane_id,
                            model=model,
                            stage=emitted_stage,
                            completed=completed,
                            total=total,
                            status=_progress_status(batch_audit),
                            attempts=batch_audit.attempts,
                            batch_index=order[0] + 1,
                            reason_codes=batch_audit.reason_codes,
                        )
                        continue
                    self._emit_progress(
                        run_id=run_id,
                        lane=lane_id,
                        model=model,
                        stage=emitted_stage,
                        completed=completed,
                        total=total,
                        status="FAILED",
                        attempts=batch_audit.attempts,
                        batch_index=order[0] + 1,
                        reason_codes=batch_audit.reason_codes,
                    )
                    batch = next((candidate for candidate_order, candidate in current if candidate_order == order), set())
                    if len(batch) > 1 and splittable(batch_audit.reason_codes):
                        midpoint = max(1, len(batch) // 2)
                        ordered_symbols = sorted(batch)
                        children.extend(
                            [
                                (order + (0,), set(ordered_symbols[:midpoint])),
                                (order + (1,), set(ordered_symbols[midpoint:])),
                            ]
                        )
                        split_count += 1
                        total += 1
                    else:
                        blocked = batch_audit
                        break
                if blocked is not None:
                    break
                if children:
                    children.sort(key=lambda item: item[0])
                    pending = [*children, *pending]
        finally:
            if executor is not None:
                executor.shutdown(wait=True)
        request_audits.sort(key=lambda item: item[0])
        ordered_valid = [audit for _, audit in sorted(valid_audits.items(), key=lambda item: item[0])]
        return [audit for _, audit in request_audits], ordered_valid, split_count, blocked, total

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
        size = self.settings.research_a1_batch_size
        batches = _build_a1_node_batches(g0, snapshot.data, size)
        discovery_output: Mapping[str, Any] = {}
        frozen_discovery_context: Mapping[str, Any] | None = None
        audits: list[StageAudit] = []
        if snapshot.data.get("A1_DRIVER_LINEAGE_REQUIRED") is True:
            discovery = self._run_stage(
                lane_id=lane_id,
                model=model,
                stage="A1",
                snapshot=snapshot,
                upstream_output=None,
                upstream_symbols=set(),
                bundle=bundle,
                run_id=run_id,
                projection_symbols=set(),
                a1_discovery_context={"mode": "POLICY_MACRO_DISCOVERY"},
            )
            discovery_output = discovery.output if isinstance(discovery.output, Mapping) else {}
            if discovery.status != "VALIDATED" or not _valid_a1_discovery_output(discovery_output):
                reasons = tuple(
                    f"A1_DISCOVERY_BLOCKED:{reason}"
                    for reason in (discovery.reason_codes or ("A1_DISCOVERY_OUTPUT_INVALID",))
                )
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage="A1",
                    status=STATUS_BLOCKED_MODEL,
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=discovery.prompt_hash,
                    input_hash=discovery.input_hash,
                    output_hash=None,
                    latency_ms=discovery.latency_ms,
                    attempts=discovery.attempts,
                    thinking_variant=discovery.thinking_variant,
                    symbols=(),
                    reason_codes=reasons,
                    diagnostics={"discovery_output_shape": _output_shape(discovery.output)},
                )
            frozen_discovery_context = {
                "mode": "COMPANY_MAPPING",
                "structural_themes": discovery_output["structural_themes"],
                "industry_chain_graph": discovery_output["industry_chain_graph"],
            }
            audits.append(discovery)
        batch_audits, valid_audits, split_count, blocked, _total_batches = self._execute_batch_plan(
            batches=batches,
            lane_id=lane_id,
            model=model,
            stage="A1",
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            runner=lambda batch: self._run_stage_with_checkpoint(
                lane_id=lane_id,
                model=model,
                stage="A1",
                snapshot=snapshot,
                upstream_output=None,
                upstream_symbols=batch,
                bundle=bundle,
                run_id=run_id,
                projection_symbols=batch,
                a1_discovery_context=frozen_discovery_context,
            ),
            splittable=_a1_batch_is_splittable,
        )
        audits.extend(batch_audits)
        if blocked is not None:
            reasons = tuple(f"A1_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes)
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
                    "completed_batches": len(valid_audits),
                    "request_groups": len(audits),
                    "split_count": split_count,
                    "blocked_batch_output_shape": _output_shape(blocked.output),
                    "blocked_batch_diagnostics": blocked.diagnostics,
                },
            )

        merge_outputs = [
            audit.output for audit in valid_audits if isinstance(audit.output, Mapping)
        ]
        if discovery_output:
            merge_outputs.insert(0, discovery_output)
        merged = _merge_a1_outputs(merge_outputs)
        merged = _annotate_a1_pool_target(merged, snapshot.data)
        reasons = _validate_output(
            merged,
            stage="A1",
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=g0,
            snapshot_data=snapshot.data,
        )
        symbols = tuple(sorted(_approved_symbols(merged, "A1")))
        diagnostics = {
            "batch_count": len(batches),
            "completed_batches": len(valid_audits),
            "request_groups": len(audits),
            "split_count": split_count,
            "pool_counts": _stage_pool_counts(merged, "A1"),
        }
        if discovery_output:
            diagnostics.update({
                "discovery_theme_count": len(discovery_output.get("structural_themes", ())),
                "discovery_node_count": len(discovery_output.get("industry_chain_graph", ())),
            })
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
            diagnostics=diagnostics,
        )

    def _run_a2_batched(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any],
        upstream_symbols: set[str],
        bundle: PromptBundle | None,
        run_id: str,
    ) -> StageAudit:
        """Run A2 in theme-preserving transport batches, then rank globally."""

        batches = _build_a2_theme_batches(
            upstream_output,
            upstream_symbols,
            self.settings.research_a2_batch_size,
        )
        audits, valid_audits, split_count, blocked, _total_batches = self._execute_batch_plan(
            batches=batches,
            lane_id=lane_id,
            model=model,
            stage="A2",
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            runner=lambda batch: self._run_stage_with_checkpoint(
                lane_id=lane_id,
                model=model,
                stage="A2",
                snapshot=snapshot,
                upstream_output=upstream_output,
                upstream_symbols=batch,
                bundle=bundle,
                run_id=run_id,
                projection_symbols=batch,
            ),
            splittable=_a2_batch_is_splittable,
        )
        if blocked is not None:
            return StageAudit(
                lane=lane_id,
                model=model,
                stage="A2",
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=_combined_digest(item.prompt_hash for item in audits),
                input_hash=_combined_digest(item.input_hash for item in audits),
                output_hash=None,
                latency_ms=sum(item.latency_ms or 0 for item in audits),
                attempts=sum(item.attempts for item in audits),
                thinking_variant=_common_variant(audits),
                symbols=(),
                reason_codes=tuple(f"A2_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes),
                diagnostics={
                    "batch_count": len(batches),
                    "completed_batches": len(valid_audits),
                    "request_groups": len(audits),
                    "split_count": split_count,
                    "blocked_batch_diagnostics": blocked.diagnostics,
                },
            )

        merged = _merge_a2_outputs([
            audit.output for audit in valid_audits if isinstance(audit.output, Mapping)
        ])
        merged, canonicalized_scores = _canonicalize_stage_scores(merged, "A2", snapshot.data)
        merged, canonicalized_bottleneck_scores = _canonicalize_a2_bottleneck_scorecards(
            merged, snapshot.data
        )
        merged, threshold_demotions = _apply_stage_threshold_policy(merged, "A2", snapshot.data)
        if snapshot.data.get("STRICT_AGENT_RULES") is True:
            merged, lineage_demotions = _apply_a2_lineage_policy(
                merged, upstream_output, snapshot.data
            )
        else:
            lineage_demotions = 0
        merged = _annotate_a2_pool_target(merged, snapshot.data)
        reasons = _validate_output(
            merged,
            stage="A2",
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=upstream_symbols,
            snapshot_data=snapshot.data,
        )
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A2",
            status="VALIDATED" if not reasons else "BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=_sha256_json(merged),
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=tuple(sorted(_approved_symbols(merged, "A2"))),
            reason_codes=tuple(reasons),
            output=merged,
            diagnostics={
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "request_groups": len(audits),
                "split_count": split_count,
                "canonicalized_score_items": canonicalized_scores,
                "canonicalized_bottleneck_scorecards": canonicalized_bottleneck_scores,
                "policy_demotions": threshold_demotions + lineage_demotions,
                "pool_counts": _stage_pool_counts(merged, "A2"),
            },
        )

    def _run_a3_batched(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any],
        upstream_symbols: set[str],
        bundle: PromptBundle | None,
        run_id: str,
    ) -> StageAudit:
        ordered = sorted(upstream_symbols)
        batches = [set(ordered[offset:offset + _A3_BATCH_SIZE]) for offset in range(0, len(ordered), _A3_BATCH_SIZE)]
        audits, valid_audits, _split_count, blocked, _total_batches = self._execute_batch_plan(
            batches=batches,
            lane_id=lane_id,
            model=model,
            stage="A3",
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            runner=lambda batch: self._run_stage_with_checkpoint(
                lane_id=lane_id,
                model=model,
                stage="A3",
                snapshot=snapshot,
                upstream_output=upstream_output,
                upstream_symbols=batch,
                bundle=bundle,
                run_id=run_id,
                projection_symbols=batch,
            ),
            # A3 transport failures are intentionally not split: the stage's
            # fixed technical projection is already bounded and its semantic
            # contract is fail-closed.
            splittable=lambda _reasons: False,
        )
        if blocked is not None:
            return StageAudit(
                lane=lane_id,
                model=model,
                stage="A3",
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=_combined_digest(item.prompt_hash for item in audits),
                input_hash=_combined_digest(item.input_hash for item in audits),
                output_hash=None,
                latency_ms=sum(item.latency_ms or 0 for item in audits),
                attempts=sum(item.attempts for item in audits),
                thinking_variant=_common_variant(audits),
                symbols=(),
                reason_codes=tuple(f"A3_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes),
                diagnostics={
                    "batch_count": len(batches),
                    "completed_batches": len(valid_audits),
                    "blocked_batch_diagnostics": blocked.diagnostics,
                },
            )
        merged = _merge_stage_outputs(
            "A3",
            [audit.output for audit in valid_audits if isinstance(audit.output, Mapping)],
        )
        merged, _ = _apply_a3_pool_limits(merged, snapshot.data)
        reasons = _validate_output(
            merged,
            stage="A3",
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=upstream_symbols,
            snapshot_data=snapshot.data,
        )
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A3",
            status="VALIDATED" if not reasons else "BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=_sha256_json(merged),
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=tuple(sorted(_approved_symbols(merged, "A3"))),
            reason_codes=tuple(reasons),
            output=merged,
            diagnostics={
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "pool_counts": _stage_pool_counts(merged, "A3"),
            },
        )

    def _prepare_stage_request(
        self,
        *,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any] | None,
        upstream_symbols: set[str],
        bundle: PromptBundle,
        projection_symbols: set[str] | None = None,
        a1_discovery_context: Mapping[str, Any] | None = None,
    ) -> _PreparedStageRequest:
        policy_macro_discovery = (
            stage == "A1"
            and str((a1_discovery_context or {}).get("mode") or "") == "POLICY_MACRO_DISCOVERY"
        )
        replacements = _prompt_replacements(
            bundle,
            stage,
            snapshot,
            upstream_output,
            projection_symbols=projection_symbols,
            a1_discovery_context=a1_discovery_context,
        )
        shared = bundle.render("00_shared_system_v2.txt", replacements)
        stage_prompt = bundle.render_stage(stage, replacements)
        effective_scope = projection_symbols if projection_symbols is not None else upstream_symbols
        execution_budget = _stage_execution_budget(
            stage,
            len(effective_scope),
            snapshot.data,
            discovery_mode=(
                str((a1_discovery_context or {}).get("mode") or "")
                if stage == "A1"
                else None
            ),
        )
        system_content = shared + "\n\n" + stage_prompt + "\n\n" + execution_budget
        if policy_macro_discovery:
            system_content += "\n\n" + render_runtime_contract()
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
        if policy_macro_discovery:
            packet = replacements.get("A1_RESEARCH_PACKET")
            if isinstance(packet, Mapping):
                runtime["a1_contract_version"] = A1_CONTRACT_VERSION
                runtime["a1_packet_hash"] = packet.get("packet_hash")
                runtime["a1_packet_diagnostics"] = packet.get("diagnostics")
                runtime["a1_canonical_decision_count"] = len(packet.get("canonical_monthly_decisions", ()))
            rendered_batch_context = replacements.get("A1_BATCH_CONTEXT")
            authorized_refs = (
                tuple(
                    value
                    for value in rendered_batch_context.get("allowed_primary_source_refs", ())
                    if isinstance(value, str) and value.strip()
                )
                if isinstance(rendered_batch_context, Mapping)
                else ()
            )
            # Keep the exact packet/snapshot intersection in the stage context
            # so initial prompt, semantic retry, and final validation cannot
            # independently rebuild divergent evidence domains.
            if isinstance(a1_discovery_context, dict):
                a1_discovery_context["authorized_discovery_source_refs"] = authorized_refs
            runtime["a1_discovery_context"] = {
                "allowed_primary_source_refs": list(authorized_refs),
                "authorized_source_refs": list(authorized_refs),
            }
        # Bind the request to the immutable snapshot/upstream digests without
        # serialising a second 200+ MiB snapshot copy merely to compute a hash.
        # The checkpoint key separately includes snapshot_hash as well.
        runtime_for_hash = {
            **runtime,
            "snapshot_data": {"snapshot_hash": snapshot.snapshot_hash},
            "upstream_output": (
                {"output_hash": _sha256_json(upstream_output)}
                if isinstance(upstream_output, Mapping)
                else None
            ),
        }
        input_hash = _sha256_json(runtime_for_hash)
        # Snapshot fields and upstream output are already rendered into
        # immutable stage-prompt placeholders. Sending either again in the
        # user message duplicates evidence and can exceed provider limits.
        model_runtime = {
            key: value
            for key, value in runtime.items()
            if key not in {"snapshot_data", "upstream_output"}
        }
        model_runtime["upstream_output"] = None
        messages: tuple[Mapping[str, Any], ...] = (
            {"role": "system", "content": system_content},
            {"role": "user", "content": "RUNTIME_INPUT\n" + _canonical_json(model_runtime)},
        )
        prompt_chars = sum(len(str(message.get("content", ""))) for message in messages)
        estimated_input_tokens = _estimate_message_tokens(messages)
        input_token_limit = int(
            getattr(self.settings, "model_max_input_tokens", _DEFAULT_MODEL_MAX_INPUT_TOKENS)
        )
        if estimated_input_tokens > input_token_limit:
            raise ResearchPipelineError(
                "MODEL_PROMPT_TOO_LARGE",
                diagnostics={
                    "prompt_chars": prompt_chars,
                    "estimated_input_tokens": estimated_input_tokens,
                    "input_token_limit": input_token_limit,
                },
            )
        return _PreparedStageRequest(
            prompt_hash=prompt_hash,
            input_hash=input_hash,
            messages=messages,
            prompt_chars=prompt_chars,
            estimated_input_tokens=estimated_input_tokens,
            input_token_limit=input_token_limit,
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
        a1_discovery_context: Mapping[str, Any] | None = None,
        prepared_request: _PreparedStageRequest | None = None,
    ) -> StageAudit:
        if bundle is None:
            return self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "PROMPT_REPOSITORY_BLOCKED")
        try:
            prepared = prepared_request or self._prepare_stage_request(
                lane_id=lane_id,
                model=model,
                stage=stage,
                snapshot=snapshot,
                upstream_output=upstream_output,
                upstream_symbols=upstream_symbols,
                bundle=bundle,
                projection_symbols=projection_symbols,
                a1_discovery_context=a1_discovery_context,
            )
            prompt_hash = prepared.prompt_hash
            input_hash = prepared.input_hash
            messages = list(prepared.messages)
            prompt_chars = prepared.prompt_chars
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
                diagnostics=exc.diagnostics,
            )

        semantic_limit = 2
        semantic_deadline = time.perf_counter() + self.settings.model_timeout_seconds
        aggregate_latency_ms = 0
        aggregate_attempts = 0
        variants: list[str] = []
        last_reasons: list[str] = []
        last_missing_mapping_codes: tuple[str, ...] = ()
        last_shape: dict[str, Any] = {"type": "NoneType"}
        authorized_discovery_refs: tuple[str, ...] = (
            tuple(a1_discovery_context.get("authorized_discovery_source_refs", ()))
            if isinstance(a1_discovery_context, Mapping)
            else ()
        )
        for semantic_attempt in range(1, semantic_limit + 1):
            active_messages = list(messages)
            if semantic_attempt > 1:
                active_messages.append(
                    {
                        "role": "user",
                        "content": _semantic_retry_instruction(
                            stage,
                            last_reasons,
                            missing_mapping_codes=last_missing_mapping_codes,
                            authorized_source_refs=authorized_discovery_refs,
                        ),
                    }
                )
            model_started = time.perf_counter()
            remaining_seconds = semantic_deadline - model_started
            if remaining_seconds <= 0:
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    status=STATUS_BLOCKED_MODEL,
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    output_hash=None,
                    latency_ms=aggregate_latency_ms,
                    attempts=aggregate_attempts,
                    thinking_variant=_common_text(variants),
                    symbols=(),
                    reason_codes=("MODEL_TOTAL_DEADLINE_EXCEEDED",),
                    diagnostics={"semantic_attempts": semantic_attempt - 1},
                )
            try:
                result = self._call_model(
                    model,
                    active_messages,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    snapshot_id=snapshot.snapshot_id,
                    stage=stage,
                    timeout_seconds=remaining_seconds,
                )
            except ModelClientError as exc:
                aggregate_latency_ms += int((time.perf_counter() - model_started) * 1000)
                aggregate_attempts += max(0, _safe_int(getattr(exc, "attempts", 0)))
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    status=STATUS_BLOCKED_MODEL,
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    output_hash=None,
                    latency_ms=aggregate_latency_ms,
                    attempts=aggregate_attempts,
                    thinking_variant=_common_text(variants),
                    symbols=(),
                    reason_codes=(exc.reason_code,),
                    diagnostics={
                        "semantic_attempts": semantic_attempt,
                        "last_invalid_output_shape": last_shape,
                        "missing_mapping_codes": list(last_missing_mapping_codes),
                        "client_diagnostics": _safe_diagnostics({
                            **dict(getattr(exc, "diagnostics", None) or {}),
                            "status_code": getattr(exc, "status_code", None),
                        }),
                    },
                )
            except (OSError, TypeError, ValueError):
                aggregate_latency_ms += int((time.perf_counter() - model_started) * 1000)
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    status=STATUS_BLOCKED_MODEL,
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    output_hash=None,
                    latency_ms=aggregate_latency_ms,
                    attempts=aggregate_attempts,
                    thinking_variant=_common_text(variants),
                    symbols=(),
                    reason_codes=("MODEL_CALL_FAILED",),
                    diagnostics={"semantic_attempts": semantic_attempt},
                )

            aggregate_latency_ms += result.latency_ms
            aggregate_attempts += result.attempts
            variants.append(result.thinking_variant)
            output = _strip_reasoning(result.output)
            output, canonicalized_analysis_summary = mechanical_repair_output(output)
            canonicalized_envelope = 0
            canonicalized_discovery_refs = 0
            required_envelope = _required_envelope(snapshot, lane_id, model, stage)
            output, canonicalized_envelope = _normalize_server_envelope(output, required_envelope)
            if (
                stage == "A1"
                and isinstance(a1_discovery_context, Mapping)
                and a1_discovery_context.get("mode") == "POLICY_MACRO_DISCOVERY"
            ):
                output, canonicalized_discovery_refs = _normalize_a1_discovery_source_refs(
                    output,
                    authorized_discovery_refs,
                )
            canonicalized_driver_context = 0
            if (
                stage == "A1"
                and isinstance(a1_discovery_context, Mapping)
                and a1_discovery_context.get("mode") == "COMPANY_MAPPING"
            ):
                output, canonicalized_driver_context = _canonicalize_a1_driver_context(
                    output, a1_discovery_context
                )
            output, canonicalized_pool_fields = _canonicalize_stage_pool_fields(output, stage)
            output, canonicalized_score_items = _canonicalize_stage_scores(
                output, stage, snapshot.data
            )
            canonicalized_bottleneck_scores = 0
            if stage == "A2":
                output, canonicalized_bottleneck_scores = _canonicalize_a2_bottleneck_scorecards(
                    output, snapshot.data
                )
            canonicalized_price_items = 0
            trend_veto_items = 0
            if stage == "A3":
                output, canonicalized_price_items, trend_veto_items = _canonicalize_a3_price_fields(
                    output, snapshot.data
                )
            output, policy_demotions = _apply_stage_threshold_policy(output, stage, snapshot.data)
            if stage == "A1" and projection_symbols is None:
                output = _annotate_a1_pool_target(output, snapshot.data)
            if stage == "A2" and snapshot.data.get("STRICT_AGENT_RULES") is True:
                output, a2_demotions = _apply_a2_lineage_policy(output, upstream_output or {}, snapshot.data)
                policy_demotions += a2_demotions
            if stage == "A2" and projection_symbols is None:
                output = _annotate_a2_pool_target(output, snapshot.data)
            reasons = _validate_output(
                output,
                stage=stage,
                model=model,
                snapshot_id=snapshot.snapshot_id,
                upstream_symbols=upstream_symbols,
                snapshot_data=snapshot.data,
            )
            if stage == "A1" and a1_discovery_context:
                reasons.extend(_a1_discovery_context_reasons(output, a1_discovery_context))
                if (
                    a1_discovery_context.get("mode") == "POLICY_MACRO_DISCOVERY"
                    and _a1_discovery_evidence_required(a1_discovery_context)
                ):
                    reasons.extend(
                        _a1_discovery_evidence_reasons(
                            output,
                            snapshot.data,
                            authorized_source_refs=tuple(
                                a1_discovery_context.get("authorized_discovery_source_refs", ())
                            ),
                        )
                    )
                    monthly_context = a1_discovery_context.get("monthly_strategy_context")
                    if isinstance(monthly_context, Mapping):
                        reasons.extend(_monthly_discovery_reasons(output, monthly_context))
                        last_missing_mapping_codes = _a1_missing_mapping_codes(
                            output,
                            monthly_context,
                        )
            envelope = output.get("envelope") if isinstance(output, Mapping) else None
            model_status = envelope.get("status") if isinstance(envelope, Mapping) else None
            if model_status == "BLOCKED":
                reasons.append("MODEL_DECLARED_BLOCKED")
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    status=STATUS_BLOCKED_MODEL,
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    output_hash=_sha256_json(output),
                    latency_ms=aggregate_latency_ms,
                    attempts=aggregate_attempts,
                    thinking_variant=_common_text(variants),
                    symbols=tuple(sorted(_approved_symbols(output, stage))),
                    reason_codes=tuple(dict.fromkeys(reasons)),
                    output=output,
                    diagnostics={"semantic_attempts": semantic_attempt},
                )
            if not reasons:
                diagnostics = {
                    "semantic_attempts": semantic_attempt,
                    "canonicalized_price_items": canonicalized_price_items,
                    "trend_veto_items": trend_veto_items,
                    "pool_counts": _stage_pool_counts(output, stage),
                }
                if canonicalized_analysis_summary:
                    diagnostics["canonicalized_analysis_summary"] = canonicalized_analysis_summary
                if canonicalized_discovery_refs:
                    diagnostics["canonicalized_discovery_refs"] = canonicalized_discovery_refs
                if canonicalized_score_items:
                    diagnostics["canonicalized_score_items"] = canonicalized_score_items
                if canonicalized_bottleneck_scores:
                    diagnostics["canonicalized_bottleneck_scorecards"] = canonicalized_bottleneck_scores
                if canonicalized_driver_context:
                    diagnostics["canonicalized_driver_context"] = canonicalized_driver_context
                if canonicalized_pool_fields:
                    diagnostics["canonicalized_pool_fields"] = canonicalized_pool_fields
                if policy_demotions:
                    diagnostics["policy_demotions"] = policy_demotions
                return StageAudit(
                    lane=lane_id,
                    model=model,
                    stage=stage,
                    status="VALIDATED",
                    snapshot_id=snapshot.snapshot_id,
                    prompt_hash=prompt_hash,
                    input_hash=input_hash,
                    output_hash=_sha256_json(output),
                    latency_ms=aggregate_latency_ms,
                    attempts=aggregate_attempts,
                    thinking_variant=_common_text(variants),
                    symbols=tuple(sorted(_approved_symbols(output, stage))),
                    reason_codes=(),
                    output=output,
                    diagnostics=diagnostics if any(diagnostics.values()) else None,
                )
            last_reasons = list(dict.fromkeys(reasons))
            last_shape = _output_shape(output)

        return StageAudit(
            lane=lane_id,
            model=model,
            stage=stage,
            status="BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=prompt_hash,
            input_hash=input_hash,
            output_hash=None,
            latency_ms=aggregate_latency_ms,
            attempts=aggregate_attempts,
            thinking_variant=_common_text(variants),
            symbols=(),
            reason_codes=tuple(last_reasons),
            output=None,
            diagnostics={
                "semantic_attempts": semantic_limit,
                "last_invalid_output_shape": last_shape,
                "missing_mapping_codes": list(last_missing_mapping_codes),
            },
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

    def _not_run_stage(self, lane: str, model: str, stage: str, snapshot_id: str, reason: str) -> StageAudit:
        """Persist a downstream stage that was deliberately not executed."""

        return StageAudit(
            lane=lane,
            model=model,
            stage=stage,
            status=STATUS_NOT_RUN_UPSTREAM_BLOCKED,
            snapshot_id=snapshot_id,
            prompt_hash=None,
            input_hash=None,
            output_hash=None,
            latency_ms=0,
            attempts=0,
            thinking_variant=None,
            symbols=(),
            reason_codes=(reason,),
            diagnostics={"outcome_code": "UPSTREAM_BLOCKED", "executed": False},
        )

    def _write_lane_audit(
        self,
        run_id: str,
        lane: LaneResult,
        *,
        snapshot: FrozenInputSnapshot | None = None,
    ) -> Path:
        path = self.output_dir / f"research_{run_id}_{_safe_run_id(lane.lane)}.json"
        written = atomic_write_json(path, lane.as_dict())
        write_lane_result_index(
            self.output_dir,
            run_id=run_id,
            lane_id=lane.lane,
            stages=lane.stages,
            model=lane.model,
            a1_input_count=len(snapshot.data.get("g0_symbols", ())) if snapshot is not None else None,
            name_catalog=(snapshot_name_catalog(snapshot.data) if snapshot is not None else None),
        )
        return written

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
        lines.extend(
            [
                "## 分阶段明细文件",
                "",
                f"- A1：`research_{result.run_id}_A1.md`",
                f"- A2：`research_{result.run_id}_A2.md`",
                f"- A3：`research_{result.run_id}_A3.md`",
                "",
                "分阶段文件由工作流在本报告后原子生成；lane JSON仍是完整审计来源。",
                "",
            ]
        )
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
    # Formal snapshots declare the complete A1 research domain explicitly.
    # Trade eligibility is a later publication/execution gate and must never
    # shrink G0 before the research funnel starts.
    for preferred_key in ("g0_symbols", "g0_candidates"):
        preferred = data.get(preferred_key)
        if preferred is not None:
            candidates.append(preferred)
            break
    if not candidates:
        preferred = data.get("trade_candidates")
        if preferred is not None:
            candidates.append(preferred)
    if not candidates:
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
    a1_discovery_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    names = set(bundle.shared.placeholders)
    names.update(bundle.document({"A1": "agent_1_macro_chain_v2.txt", "A2": "agent_2_theme_sentiment_v2.txt", "A3": "agent_3_technical_planner_v2.txt"}[stage]).placeholders)
    discovery_mode = str((a1_discovery_context or {}).get("mode") or "") if stage == "A1" else ""
    policy_macro_discovery = discovery_mode == "POLICY_MACRO_DISCOVERY"
    if projection_symbols is not None:
        allowed_symbols: set[str] | None = set(projection_symbols)
    elif stage == "A1":
        allowed_symbols: set[str] | None = _extract_g0(snapshot.data)
    elif stage == "A2":
        allowed_symbols = _approved_symbols(upstream_output or {}, "A1")
    else:
        allowed_symbols = _approved_symbols(upstream_output or {}, "A2")
    replacements: dict[str, Any] = {}
    a1_packet: Mapping[str, Any] | None = None
    if policy_macro_discovery:
        try:
            a1_packet = build_a1_research_packet(
                snapshot,
                monthly_strategy_context=(a1_discovery_context or {}).get("monthly_strategy_context"),
                prior_theme_registry=(a1_discovery_context or {}).get("prior_theme_registry"),
                max_estimated_tokens=A1_PACKET_TOKEN_BUDGET,
                raise_on_budget=True,
            )
        except A1PacketSizeError as exc:
            # The caller turns this into a fail-closed ResearchPipelineError
            # with the section-level diagnostics.  Do not silently truncate
            # the complete research packet to satisfy a provider limit.
            raise ResearchPipelineError(
                "A1_PACKET_TOO_LARGE",
                diagnostics=exc.diagnostics,
            ) from exc
    for name in names:
        if name == "A1_RESEARCH_PACKET":
            replacements[name] = dict(a1_packet or {})
            continue
        if policy_macro_discovery and name in {
            "MACRO_POLICY_FEED",
            "MACRO_ECONOMIC_DATA",
            "ASSET_ROTATION_SNAPSHOT",
            "GLOBAL_MACRO_SNAPSHOT",
            "CROSS_MARKET_LEAD_SNAPSHOT",
            "BROKER_RESEARCH_CONSENSUS",
            "INDUSTRY_NEWS_FEED",
            "INDUSTRY_PROFIT_DATA",
            "INDUSTRY_ACTIVITY_DATA",
            "THS_INDUSTRY_MEMBERSHIP",
            "EXISTING_CHAIN_GRAPH",
            "THEME_REGISTRY",
            "COMPANY_FUNDAMENTALS",
            "MAIN_BUSINESS_EVIDENCE",
            "DISCLOSURE_EVENTS",
            "RISK_EVENTS",
            "RESEARCH_CONSENSUS",
            "FUND_HOLDINGS",
        }:
            # Raw macro/industry histories and company evidence belong to the
            # immutable snapshot, not to the discovery model view.  The
            # compact packet is the sole source for POLICY_MACRO_DISCOVERY.
            replacements[name] = None
            continue
        if name == "UPSTREAM_ACTIVE_POOL" or name == "UPSTREAM_FOCUS_POOL":
            replacements[name] = (
                _project_upstream_output(upstream_output, allowed_symbols)
                if upstream_output is not None
                else None
            )
            continue
        if name == "SNAPSHOT_MANIFEST":
            if policy_macro_discovery:
                packet = a1_packet or {}
                manifest = {
                    "snapshot_id": snapshot.snapshot_id,
                    "snapshot_hash": snapshot.snapshot_hash,
                    "as_of": snapshot.as_of,
                    "contract_version": A1_CONTRACT_VERSION,
                    "packet_hash": packet.get("packet_hash"),
                    "quality_summary": packet.get("quality_summary", {}),
                    "coverage": packet.get("coverage", {}),
                    "full_snapshot_retained_for_audit": True,
                }
            else:
                manifest = snapshot.data.get("snapshot_manifest", snapshot.data)
            replacements[name] = _with_projection_metadata(manifest, snapshot, allowed_symbols)
            continue
        if name == "A1_POOL_TARGETS":
            if policy_macro_discovery:
                # Stock-pool capacity is a COMPANY_MAPPING concern.  Sending
                # it to discovery invites the model to turn a structural scan
                # into an implicit stock-selection task.
                replacements[name] = None
                continue
            found, value = _lookup_field(snapshot.data, name)
            # Snapshots frozen before this prompt parameter was introduced stay
            # replayable. New snapshots carry the configured value explicitly.
            replacements[name] = value if found else {
                "pool_min": 300,
                "pool_max": 1000,
                "clue_pool_target": [300, 800],
                "active_research_target": [100, 250],
                "node_count_target": [40, 80],
                "quota_forbidden": True,
            }
            continue
        if name == "A2_POOL_TARGETS":
            found, value = _lookup_field(snapshot.data, name)
            replacements[name] = value if found else {
                "pool_min": 100,
                "pool_max": 200,
                "quota_forbidden": True,
            }
            continue
        if name == "A1_BATCH_CONTEXT":
            batch_mode = str((a1_discovery_context or {}).get("mode") or "COMPANY_MAPPING")
            if policy_macro_discovery:
                packet = a1_packet or {}
                authorized_refs = _authorized_discovery_source_refs(snapshot.data, packet)
                monthly = (a1_discovery_context or {}).get("monthly_strategy_context")
                monthly = monthly if isinstance(monthly, Mapping) else {}
                replacements[name] = {
                    "mode": batch_mode,
                    "symbols": [],
                    "batch_is_transport_boundary": True,
                    "allowed_primary_source_refs": list(authorized_refs),
                    "canonical_monthly_decision_count": len(packet.get("canonical_monthly_decisions", ())),
                    "monthly_strategy_context": {
                        "strategy_month": monthly.get("strategy_month"),
                        "status": monthly.get("status"),
                        "macro_asset_quadrant": packet.get("macro_asset_quadrant", {}),
                        "canonical_monthly_decisions": packet.get("canonical_monthly_decisions", []),
                        "prior_theme_registry": packet.get("prior_theme_registry", {}),
                    },
                    "allowed_taxonomy_catalog": {
                        "industry": _taxonomy_catalog_projection(snapshot.data).get("industry", []),
                        "concept": [],
                    },
                }
                continue
            replacements[name] = {
                "mode": batch_mode,
                "symbols": sorted(allowed_symbols or ()),
                "node_by_symbol": _a1_node_by_symbol(snapshot.data, set(allowed_symbols or ())),
                "batch_is_transport_boundary": True,
                "allowed_primary_source_refs": (
                    sorted(_snapshot_discovery_evidence_refs(snapshot.data))[:512]
                    if batch_mode == "POLICY_MACRO_DISCOVERY"
                    else []
                ),
                "frozen_discovery": {
                    "structural_themes": (a1_discovery_context or {}).get("structural_themes", []),
                    "industry_chain_graph": (a1_discovery_context or {}).get("industry_chain_graph", []),
                    "taxonomy_links": (a1_discovery_context or {}).get("taxonomy_links", []),
                },
                "monthly_strategy_context": (
                    (a1_discovery_context or {}).get("monthly_strategy_context", {})
                ),
                "allowed_taxonomy_catalog": _taxonomy_catalog_projection(snapshot.data),
                "local_candidate_decisions": {
                    symbol: value
                    for symbol, value in (
                        (a1_discovery_context or {}).get("local_candidates", {})
                    ).items()
                    if symbol in set(allowed_symbols or ())
                } if isinstance((a1_discovery_context or {}).get("local_candidates"), Mapping) else {},
            }
            continue
        if name in {"A1_MINIMUMS", "MIN_THEME_SCORE", "MIN_TECHNICAL_SCORE"}:
            if policy_macro_discovery and name == "A1_MINIMUMS":
                replacements[name] = None
                continue
            defaults: dict[str, Any] = {
                "A1_MINIMUMS": {
                    "structural_score": 65,
                    "data_quality_score": 75,
                    "evidence_confidence": 0.70,
                },
                "MIN_THEME_SCORE": 60,
                "MIN_TECHNICAL_SCORE": 70,
            }
            found, value = _lookup_field(snapshot.data, name)
            replacements[name] = value if found else defaults[name]
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
    if name == "MACRO_POLICY_FEED":
        return _project_macro_policy(value, item_limit=24)
    if name == "FACTOR_SNAPSHOT":
        return _project_factor_snapshot(value, symbols)
    if name == "DISCLOSURE_EVENTS":
        return _project_disclosures(value, symbols)
    if name == "INDUSTRY_NEWS_FEED":
        return _project_news(value, item_limit=8)
    if name == "NEWS_HEAT_SNAPSHOT":
        return _project_news(value, item_limit=40, symbols=symbols)
    if name in {
        "KLINE_PATTERNS",
        "PRICE_LEVELS",
        "LIQUIDITY_SNAPSHOT",
        "TRADABILITY_FLAGS",
        "COMPANY_FUNDAMENTALS",
        "MAIN_BUSINESS_EVIDENCE",
        "A2_BOTTLENECK_CONTEXT",
    }:
        return _filter_symbol_mapping(value, symbols)
    if name == "RISK_EVENTS":
        return _project_disclosures(value, symbols)
    if name == "THS_INDUSTRY_MEMBERSHIP":
        return _project_membership(value, symbols)
    if name in {"CROWDING_SNAPSHOT", "FUND_HOLDINGS"}:
        return _filter_nested_symbol_data(value, symbols)
    return value


def _project_factor_snapshot(value: Any, symbols: set[str] | None) -> Any:
    if not isinstance(value, Mapping):
        return value
    projected: dict[str, Any] = {}
    for raw_symbol, raw_factor in sorted(value.items(), key=lambda item: str(item[0])):
        symbol = str(raw_symbol)
        if symbols is not None and symbol not in symbols:
            continue
        if not isinstance(raw_factor, Mapping):
            projected[symbol] = raw_factor
            continue
        raw_frames = raw_factor.get("timeframes")
        frames: dict[str, Any] = {}
        if isinstance(raw_frames, Mapping):
            for name, raw_frame in raw_frames.items():
                if not isinstance(raw_frame, Mapping):
                    continue
                latest = raw_frame.get("latest")
                compact_latest = {
                    key: latest.get(key)
                    for key in ("open", "high", "low", "close", "end", "closed", "timeframe")
                    if isinstance(latest, Mapping) and key in latest
                }
                frames[str(name)] = {
                    "latest": compact_latest,
                    "moving_averages": raw_frame.get("moving_averages"),
                    "ma_alignment": raw_frame.get("ma_alignment"),
                    "ma_event": raw_frame.get("ma_event"),
                    "ma_bias": raw_frame.get("ma_bias"),
                    "vwap": raw_frame.get("vwap"),
                    "ready": raw_frame.get("ready"),
                    "reasons": raw_frame.get("reasons"),
                }
        projected[symbol] = {
            "symbol": raw_factor.get("symbol", symbol),
            "as_of": raw_factor.get("as_of"),
            "ready": raw_factor.get("ready"),
            "reasons": raw_factor.get("reasons"),
            "timeframes": frames,
        }
    return projected


_POLICY_RESEARCH_TERMS: tuple[str, ...] = (
    "产业", "工业", "制造", "科技", "技术", "创新", "数字", "人工智能", "算力",
    "半导体", "机器人", "能源", "电力", "储能", "资源", "材料", "设备更新",
    "消费", "投资", "财政", "货币", "金融", "资本市场", "监管", "改革",
    "出口", "进口", "贸易", "关税", "制裁", "供应链", "专项债", "补贴", "招标",
)


def _project_macro_policy(value: Any, *, item_limit: int) -> Any:
    """Keep a bounded, auditable official-policy view for model research."""

    if not isinstance(value, Mapping):
        return value
    result = dict(value)
    documents = value.get("official_documents")
    if not isinstance(documents, list):
        return result

    def priority(item: Any) -> tuple[int, str, str]:
        if not isinstance(item, Mapping):
            return 0, "", ""
        text = " ".join(str(item.get(key) or "") for key in ("title", "summary", "issuing_body"))
        relevance = sum(1 for term in _POLICY_RESEARCH_TERMS if term in text)
        published = str(item.get("publish_time") or item.get("event_time") or "")
        fact_id = str(item.get("fact_id") or "")
        return relevance, published, fact_id

    eligible = [
        item for item in documents
        if isinstance(item, Mapping)
        and item.get("prompt_injection_suspected") is not True
        and isinstance(item.get("fact_id"), str)
        and item.get("fact_id")
    ]
    selected = sorted(eligible, key=priority, reverse=True)[:item_limit]
    result["official_documents"] = [_truncate_nested(item, 2_000) for item in selected]
    result["prompt_document_count"] = len(selected)
    result["full_document_count"] = len(documents)
    result["projection_method"] = "OFFICIAL_POLICY_RELEVANCE_THEN_RECENCY"
    return result


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
        ordered_items = sorted(
            (item for item in items if isinstance(item, Mapping)),
            key=_disclosure_prompt_priority,
        )
        compact_items: list[dict[str, Any]] = []
        for item in ordered_items[:3]:
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


def _disclosure_prompt_priority(item: Mapping[str, Any]) -> tuple[int, int, str]:
    title = re.sub(r"\s+", "", str(item.get("announcement_title") or ""))
    tags = {str(value) for value in item.get("event_tags", ())} if isinstance(item.get("event_tags"), list) else set()
    has_pdf = item.get("pdf_evidence_available") is True
    full_report = bool(re.search(r"(?:19|20)\d{2}年(?:半年度|年度)报告(?:全文)?$", title))
    if has_pdf and full_report:
        rank = 0
    elif has_pdf and "EARNINGS" in tags:
        rank = 1
    elif has_pdf:
        rank = 2
    elif "RISK" in tags:
        rank = 3
    elif "ORDER_OR_CAPACITY" in tags:
        rank = 4
    else:
        rank = 5
    published = str(item.get("publish_time") or item.get("event_time") or "")
    digits = re.sub(r"\D", "", published)
    return rank, -int(digits or 0), str(item.get("announcement_id") or "")


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


def _taxonomy_catalog_projection(snapshot_data: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for taxonomy, key in (
        ("industry", "THS_INDUSTRY_CATALOG"),
        ("concept", "THS_CONCEPT_CATALOG"),
    ):
        value = snapshot_data.get(key)
        records = value.get("records") if isinstance(value, Mapping) else None
        projected: list[dict[str, str]] = []
        if isinstance(records, list):
            for item in records:
                if not isinstance(item, Mapping):
                    continue
                code = str(item.get("thscode") or "").strip().upper()
                name = str(item.get("name") or "").strip()
                if code and name:
                    projected.append({"thscode": code, "name": name})
        result[taxonomy] = sorted(projected, key=lambda item: (item["thscode"], item["name"]))[:1000]
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


def _stage_execution_budget(
    stage: str,
    input_symbol_count: int,
    snapshot_data: Mapping[str, Any],
    *,
    discovery_mode: str | None = None,
) -> str:
    supplied = max(0, int(input_symbol_count))
    budget = dict(_STAGE_OUTPUT_BUDGETS[stage])
    regime_parameters = snapshot_data.get("REGIME_PARAM_SET")
    regime = regime_parameters if isinstance(regime_parameters, Mapping) else {}
    if stage == "A1":
        # A1 is a broad macro/fundamental eligibility layer. The batch size is
        # a transport boundary, not a global stock-selection quota.
        budget["approved_pool"] = supplied
        budget["secondary_pool"] = supplied
    elif stage == "A2":
        agent = regime.get("agent_2") if isinstance(regime.get("agent_2"), Mapping) else {}
        focus_max = max(0, _safe_int(agent.get("focus_pool_max", supplied)))
        budget["approved_pool"] = min(supplied, focus_max)
        budget["secondary_pool"] = supplied
    elif stage == "A3":
        agent = regime.get("agent_3") if isinstance(regime.get("agent_3"), Mapping) else {}
        core_max = max(0, _safe_int(agent.get("core_watch_max", supplied)))
        total_max = max(core_max, _safe_int(agent.get("total_watch_max", supplied)))
        budget["approved_pool"] = min(supplied, core_max)
        budget["secondary_pool"] = min(supplied, max(0, total_max - core_max))
    stage_contract = {
        "A1": (
            (
                "POLICY_MACRO_DISCOVERY contract: the server owns the complete canonical_monthly_decisions "
                "array (exactly 20 by the A1 contract). Return only envelope, analysis_summary, macro_regime, "
                "policy_dossiers, policy_calendar, structural_themes, industry_chain_graph, taxonomy_links, "
                "industry_theme_mappings, source_health and unresolved_questions. Do not return or rewrite "
                "monthly_industry_decisions; map every server base_decision=INCLUDE row or declare "
                "mapping_status=UNMAPPED with data_gaps. The canonical rows are read-only and the model cannot "
                "change their rank, decision, reason_codes or source_refs."
                if discovery_mode == "POLICY_MACRO_DISCOVERY"
                else (
                    "Required top-level keys: envelope, analysis_summary, structural_themes, industry_chain_graph, "
                    "taxonomy_links, active_research_pool, monitor_pool, rejected_candidates. Each approved item needs symbol, "
                    "candidate_id, company_name, primary_theme, industry_chain_node, core_thesis, bear_case, "
                    "structural_score, data_quality_score, evidence_confidence, status, source_refs, "
                    "business_exposure with revenue_exposure_pct and a snapshot-valid source_ref, and score_breakdown "
                    "containing every exact key from SCORE_WEIGHTS. structural_score must equal that configured weighted sum. "
                    "primary_theme must exactly match a theme_id or display_name in structural_themes; "
                    "industry_chain_node must exactly match a node_id in industry_chain_graph. Both records need "
                    "snapshot-bound source_refs; unsupported narrative is MONITOR, never ACTIVE. Every supplied symbol "
                    "must appear exactly once across active_research_pool, monitor_pool, and rejected_candidates; "
                    "the batch boundary is not a selection quota."
                )
            )
        ),
        "A2": (
            "Required top-level keys: envelope, analysis_summary, active_themes, focus_pool, "
            "watch_only_pool, rejected_candidates. Each active theme needs theme_id, stage, new_entry_policy, "
            "theme_score, score_breakdown containing every exact key from THEME_SCORE_WEIGHTS, penalties, "
            "supporting_evidence, contradicting_evidence, and rotation_overlap_ratio. Each focus item needs "
            "symbol, upstream_candidate_id, theme_id, theme_stage, a2_route, bottleneck_status, "
            "market_role, role_evidence, "
            "identifiability_score, identifiability_breakdown, theme_score inherited from its active theme, "
            "selection_reasons, risk_reasons and risk_flags. MARKET_CORE requires frozen market-role evidence, "
            "factor_coverage and bottleneck_status=NOT_REQUIRED_FOR_MARKET_CORE; it must not invent a scarcity "
            "scorecard. SUPPLY_CHAIN_ALPHA additionally requires supply_chain_role, scarce_layer, "
            "value_chain_position, a complete bottleneck_scorecard, at least two bottleneck_evidence items, "
            "missing_proof and kill_switches. Rank scarce layers before companies; unknown supply-chain facts "
            "must be sent to watch_only, never scored as zero. Every supplied symbol must appear exactly once "
            "across focus_pool, watch_only_pool, and rejected_candidates."
        ),
        "A3": (
            "Required top-level keys: envelope, analysis_summary, core_watch_pool, secondary_watch_pool, "
            "rejected_candidates. Each core item must copy deterministic PRICE_LEVELS values for symbol, "
            "risk_unit, trigger_zone, invalidation_level, stop_distance_pct, first_resistance and reward_risk, "
            "then add concise scenarios and confirmation_conditions. score_breakdown must contain every "
            "exact key from TECHNICAL_SCORE_WEIGHTS, and technical_score must equal that weighted sum."
        ),
    }[stage]
    return (
        "RUNTIME_EXECUTION_BUDGET (overrides generic target counts, never overrides evidence gates):\n"
        f"- supplied_symbol_count={supplied}; analyze only supplied symbols.\n"
        f"- approved pool <= {budget['approved_pool']}; secondary/watch pool <= {budget['secondary_pool']}.\n"
        f"- themes <= {budget['themes']}; industry_chain_graph nodes <= {budget['chain_nodes']}.\n"
        f"- each evidence/source/reference array <= {budget['evidence_per_item']}; use concise strings.\n"
        "- Do not expand empty or missing evidence. Return empty arrays and DEGRADED/BLOCKED where required.\n"
        "- Copy RUNTIME_INPUT.required_envelope exactly as the complete envelope; omit no field.\n"
        f"- {stage_contract}\n"
        "- This compact contract permits omission of all other large sections in the generic report schema.\n"
        "- Finish one valid JSON object within the response budget; no markdown or commentary."
    )


def _build_a1_node_batches(
    symbols: set[str],
    snapshot_data: Mapping[str, Any],
    batch_size: int,
) -> list[set[str]]:
    """Pack deterministic industry-node groups into bounded A1 calls."""

    if batch_size < 1:
        raise ResearchPipelineError("A1_BATCH_SIZE_INVALID")
    node_by_symbol = _a1_node_by_symbol(snapshot_data, symbols)
    raw_liquidity = snapshot_data.get("LIQUIDITY_SNAPSHOT")
    liquidity = raw_liquidity if isinstance(raw_liquidity, Mapping) else {}
    groups: dict[str, list[str]] = {}
    for symbol in symbols:
        groups.setdefault(node_by_symbol.get(symbol, f"UNMAPPED:{symbol}"), []).append(symbol)

    def turnover(symbol: str) -> float:
        value = liquidity.get(symbol)
        return _safe_float(value.get("turnover")) if isinstance(value, Mapping) else 0.0

    for members in groups.values():
        members.sort(key=lambda symbol: (-turnover(symbol), symbol))
    ordered_nodes = sorted(
        groups,
        key=lambda node: (-sum(turnover(symbol) for symbol in groups[node]), node),
    )
    batches: list[set[str]] = []
    current: list[str] = []
    for node in ordered_nodes:
        members = groups[node]
        for offset in range(0, len(members), batch_size):
            chunk = members[offset:offset + batch_size]
            if current and len(current) + len(chunk) > batch_size:
                batches.append(set(current))
                current = []
            current.extend(chunk)
            if len(current) == batch_size:
                batches.append(set(current))
                current = []
    if current:
        batches.append(set(current))
    return batches


def _chunk_symbol_sets(symbols: Sequence[str], size: int) -> list[set[str]]:
    ordered = tuple(dict.fromkeys(str(symbol) for symbol in symbols if str(symbol)))
    if size < 1:
        raise ValueError("batch size must be positive")
    return [set(ordered[index : index + size]) for index in range(0, len(ordered), size)]


def _gate_secondary_items(gate: DeterministicGateResult, stage: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for decision in gate.decisions:
        if decision.get("status") == "REVIEW_CANDIDATE":
            continue
        item = {
            "symbol": decision.get("symbol"),
            "company_name": decision.get("name"),
            "theme_id": decision.get("theme_id"),
            "industry_chain_node": decision.get("node_id"),
            "status": "WATCH_ONLY" if stage == "A2" else "REJECTED",
            "reason_codes": list(decision.get("reason_codes", ())),
            "local_decision": True,
            "sent_to_llm": False,
            "source_refs": [],
            "data_sufficiency_state": decision.get("data_sufficiency_state"),
            "factor_coverage": decision.get("factor_coverage"),
            "critical_factor_coverage": decision.get("critical_factor_coverage"),
            "local_partition": decision.get("status"),
        }
        if stage == "A2":
            item.update({
                "theme_score": decision.get("score"),
                "identifiability_score": decision.get("score"),
                "market_role": decision.get("role", "LOW_IDENTITY"),
            })
        items.append(item)
    return items


def _build_a2_theme_batches(
    upstream_output: Mapping[str, Any],
    symbols: set[str],
    batch_size: int,
) -> list[set[str]]:
    """Pack A1-approved companies by structural theme for A2 role review."""

    if batch_size < 1:
        raise ResearchPipelineError("A2_BATCH_SIZE_INVALID")
    groups: dict[str, list[tuple[float, str]]] = {}
    active = upstream_output.get("active_research_pool")
    if isinstance(active, list):
        for item in active:
            if not isinstance(item, Mapping):
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                continue
            symbol = next(iter(scanned))
            if symbol not in symbols:
                continue
            theme = str(item.get("primary_theme") or "UNMAPPED").strip() or "UNMAPPED"
            groups.setdefault(theme, []).append((_safe_float(item.get("structural_score")), symbol))
    assigned = {symbol for members in groups.values() for _score, symbol in members}
    for symbol in sorted(symbols.difference(assigned)):
        groups.setdefault("UNMAPPED", []).append((0.0, symbol))

    ordered_groups = sorted(
        groups.items(),
        key=lambda entry: (-max((score for score, _symbol in entry[1]), default=0.0), entry[0]),
    )
    batches: list[set[str]] = []
    current: list[str] = []
    for _theme, members in ordered_groups:
        ordered_members = [symbol for _score, symbol in sorted(members, key=lambda item: (-item[0], item[1]))]
        for offset in range(0, len(ordered_members), batch_size):
            chunk = ordered_members[offset:offset + batch_size]
            if current and len(current) + len(chunk) > batch_size:
                batches.append(set(current))
                current = []
            current.extend(chunk)
            if len(current) == batch_size:
                batches.append(set(current))
                current = []
    if current:
        batches.append(set(current))
    return batches


def _a1_node_by_symbol(
    snapshot_data: Mapping[str, Any],
    symbols: set[str],
) -> dict[str, str]:
    raw_membership = snapshot_data.get("THS_INDUSTRY_MEMBERSHIP")
    if not isinstance(raw_membership, Mapping):
        return {}
    records = raw_membership.get("records")
    if not isinstance(records, list):
        return {}
    result: dict[str, str] = {}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        symbol = str(record.get("thscode") or record.get("symbol") or "")
        memberships = record.get("memberships")
        if symbol not in symbols or not isinstance(memberships, list):
            continue
        valid = [
            item
            for item in memberships
            if isinstance(item, Mapping) and str(item.get("industry_thscode") or "")
        ]
        if not valid:
            continue
        specific = max(
            valid,
            key=lambda item: (
                str(item.get("industry_thscode") or "").startswith("884"),
                str(item.get("industry_thscode") or ""),
            ),
        )
        result[symbol] = str(specific.get("industry_thscode"))
    return result


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
        )
    monitor = merged.get("monitor_pool")
    if isinstance(monitor, list):
        rejected_symbols = _scan_symbols(merged.get("rejected_candidates", ()))
        active_symbols = _scan_symbols(merged.get("active_research_pool", ()))
        normalized_monitor = [_normalize_pool_symbol(item) for item in monitor]
        normalized_monitor = [
            item
            for item in normalized_monitor
            if not _scan_symbols(item).intersection(rejected_symbols | active_symbols)
        ]
        merged["monitor_pool"] = sorted(
            normalized_monitor,
            key=lambda item: _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )
    if isinstance(merged.get("rejected_candidates"), list):
        merged["rejected_candidates"] = [_normalize_pool_symbol(item) for item in merged["rejected_candidates"]]
    # A provider summary describes only one transport batch and becomes
    # misleading after merge (for example, "all five candidates"). Publish a
    # deterministic aggregate summary instead; the normalized pools and their
    # combined hashes remain the authoritative merged result.
    merged["analysis_summary"] = {
        "outcome": "A1_BATCHES_MERGED",
        "batch_count": len(outputs),
        "approved_count": len(merged.get("active_research_pool", ())),
        "monitor_count": len(merged.get("monitor_pool", ())),
        "rejected_count": len(merged.get("rejected_candidates", ())),
    }
    return merged


def _merge_a2_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Merge theme-preserving A2 batches into one globally ranked partition."""

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
    for pool in ("focus_pool", "watch_only_pool", "rejected_candidates"):
        values = merged.get(pool)
        if isinstance(values, list):
            merged[pool] = [_normalize_pool_symbol(item) for item in values]

    focus_symbols = _scan_symbols(merged.get("focus_pool", ()))
    if isinstance(merged.get("watch_only_pool"), list):
        merged["watch_only_pool"] = [
            item for item in merged["watch_only_pool"]
            if not _scan_symbols(item).intersection(focus_symbols)
        ]
    selected_symbols = focus_symbols | _scan_symbols(merged.get("watch_only_pool", ()))
    if isinstance(merged.get("rejected_candidates"), list):
        merged["rejected_candidates"] = [
            item for item in merged["rejected_candidates"]
            if not _scan_symbols(item).intersection(selected_symbols)
        ]

    def ranking(item: Any) -> tuple[float, float, str]:
        return (
            -_safe_float(item.get("theme_score")) if isinstance(item, Mapping) else 0.0,
            -_safe_float(item.get("identifiability_score")) if isinstance(item, Mapping) else 0.0,
            _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )

    for pool in ("focus_pool", "watch_only_pool"):
        if isinstance(merged.get(pool), list):
            merged[pool] = sorted(merged[pool], key=ranking)
    merged["analysis_summary"] = {
        "outcome": "A2_BATCHES_MERGED",
        "batch_count": len(outputs),
        **_stage_pool_counts(merged, "A2"),
    }
    return merged


def _merge_stage_outputs(stage: str, outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if stage != "A3" or not outputs:
        return {}
    merged: dict[str, Any] = {}
    list_values: dict[str, list[Any]] = {}
    for output in outputs:
        for key, value in output.items():
            if key == "envelope":
                if key not in merged and isinstance(value, Mapping):
                    merged[key] = dict(value)
                continue
            if isinstance(value, list):
                list_values.setdefault(str(key), []).extend(value)
            elif key not in merged:
                merged[str(key)] = value
    for key, values in list_values.items():
        merged[key] = _deduplicate_stage_items(key, values)
    merged["analysis_summary"] = {
        "outcome": "A3_BATCHES_MERGED",
        "batch_count": len(outputs),
        **_stage_pool_counts(merged, "A3"),
    }
    return merged


def _valid_a1_discovery_output(output: Mapping[str, Any]) -> bool:
    """Check the structural shape shared by discovery and company mapping.

    Target counts and canonical industry mapping completeness are evaluated by
    ``_monthly_discovery_reasons`` because that function has the frozen
    monthly context.  Keeping this predicate structural preserves compatibility
    with small pre-v3 fixtures and old read-only audit files.
    """

    validation = validate_discovery_output(
        output,
        require_targets=False,
        canonical_decisions=None,
    )
    return not any(
        reason in {
            "A1_DISCOVERY_THEMES_MISSING",
            "A1_DISCOVERY_CHAIN_NODES_MISSING",
            "A1_DISCOVERY_THEME_ID_INVALID",
            "A1_DISCOVERY_NODE_ID_INVALID",
            "A1_DISCOVERY_NODE_INVALID",
            "A1_DISCOVERY_NODE_THEME_LINK_INVALID",
        }
        for reason in validation.reason_codes
    )


def _monthly_discovery_reasons(
    output: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[str, ...]:
    """Reject a schema-valid but commercially unusable monthly theme map."""

    if _safe_int(context.get("g0_symbol_count")) < 500:
        return ()
    if str(context.get("status") or "BLOCKED") == "BLOCKED":
        return ("A1_MONTHLY_STRATEGY_INPUT_BLOCKED",)
    themes = output.get("structural_themes")
    nodes = output.get("industry_chain_graph")
    theme_count = len(themes) if isinstance(themes, list) else 0
    node_count = len(nodes) if isinstance(nodes, list) else 0
    reasons: list[str] = []
    # New-contract responses are held to the configured business target.  A
    # legacy response without mappings keeps the old structural minimum so
    # historical fixtures remain readable; it is never accepted as a new
    # contract response by the production prompt.
    is_new_mapping_response = isinstance(output.get("industry_theme_mappings"), list)
    if is_new_mapping_response:
        theme_minimum, theme_maximum = A1_THEME_TARGET
        node_minimum, node_maximum = A1_NODE_TARGET
    else:
        theme_minimum, theme_maximum = 6, A1_THEME_TARGET[1]
        node_minimum, node_maximum = 12, A1_NODE_TARGET[1]
    if theme_count < theme_minimum:
        reasons.append("A1_MONTHLY_THEME_COVERAGE_INSUFFICIENT")
    if theme_count > theme_maximum:
        reasons.append("A1_MONTHLY_THEME_COVERAGE_EXCEEDED")
    if node_count < node_minimum:
        reasons.append("A1_MONTHLY_CHAIN_COVERAGE_INSUFFICIENT")
    if node_count > node_maximum:
        reasons.append("A1_MONTHLY_CHAIN_COVERAGE_EXCEEDED")

    rotations = context.get("monthly_industry_rotation")
    rotations = rotations if isinstance(rotations, list) else []
    expected_codes = {
        str(item.get("industry_thscode") or "")
        for item in rotations[:10]
        if isinstance(item, Mapping) and str(item.get("industry_thscode") or "")
    }
    covered_codes: set[str] = set()
    def collect_codes(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key in {"industry_thscodes", "industry_codes"} and isinstance(nested, list):
                    covered_codes.update(str(item) for item in nested if str(item))
                elif key == "taxonomy_links":
                    collect_codes(nested)
        elif isinstance(value, list):
            for nested in value:
                collect_codes(nested)

    collect_codes(output.get("taxonomy_links"))
    collect_codes(nodes)
    # Contract v3 replaces the old implicit "codes must be embedded in
    # taxonomy/node objects" rule with an explicit per-industry mapping
    # array.  Applying both rules would reject a valid response merely
    # because it used the new canonical field.  Legacy responses continue to
    # use the historical 40% top-ten coverage check.
    if not is_new_mapping_response and len(expected_codes) >= 3:
        coverage = len(expected_codes.intersection(covered_codes)) / len(expected_codes)
        if coverage < 0.40:
            reasons.append("A1_MONTHLY_CYCLE_COVERAGE_INSUFFICIENT")

    expected_decisions = context.get("monthly_industry_decisions")
    decision_contract = context.get("monthly_rotation_coverage")
    if isinstance(output.get("industry_theme_mappings"), list) and isinstance(expected_decisions, list):
        # The complete canonical set is a prerequisite for a v3 discovery
        # response.  A model cannot repair a source-side shortfall by
        # returning mappings for the few rows that happened to be present;
        # accepting that would silently turn a top-20 run into top-3 (or any
        # other partial ranking).  Keep this separate from mapping validation
        # so the diagnostic points at the earliest broken layer.
        requested = (
            _safe_int(decision_contract.get("requested_top_n"))
            if isinstance(decision_contract, Mapping)
            else 0
        )
        observed = (
            _safe_int(decision_contract.get("observed_count"))
            if isinstance(decision_contract, Mapping)
            else len(expected_decisions)
        )
        coverage_status = (
            str(decision_contract.get("status") or "")
            if isinstance(decision_contract, Mapping)
            else ""
        )
        if (
            requested != A1_MONTHLY_DECISION_COUNT
            or observed != A1_MONTHLY_DECISION_COUNT
            or len(expected_decisions) != A1_MONTHLY_DECISION_COUNT
            or coverage_status != "READY"
        ):
            reasons.append("A1_CANONICAL_MONTHLY_DECISIONS_INCOMPLETE")
        validation = validate_discovery_output(
            output,
            canonical_decisions=expected_decisions,
            theme_target=A1_THEME_TARGET,
            node_target=A1_NODE_TARGET,
            require_targets=True,
        )
        reasons.extend(validation.reason_codes)
    elif isinstance(expected_decisions, list) and isinstance(decision_contract, Mapping):
        expected_by_code = {
            str(item.get("industry_thscode") or "").strip().upper(): int(item.get("rank") or 0)
            for item in expected_decisions
            if isinstance(item, Mapping) and str(item.get("industry_thscode") or "").strip()
        }
        raw_decisions = output.get("monthly_industry_decisions")
        if not isinstance(raw_decisions, list):
            reasons.append("A1_MONTHLY_ROTATION_DECISIONS_MISSING")
        else:
            observed: dict[str, Mapping[str, Any]] = {}
            duplicate = False
            invalid = False
            theme_ids = {
                str(item.get("theme_id") or "").strip()
                for item in themes or ()
                if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
            }
            for item in raw_decisions:
                if not isinstance(item, Mapping):
                    invalid = True
                    continue
                code = str(item.get("industry_thscode") or "").strip().upper()
                decision = str(item.get("decision") or "").strip().upper()
                if code in observed:
                    duplicate = True
                if code not in expected_by_code or decision not in {"INCLUDE", "EXCLUDE", "DEFER"}:
                    invalid = True
                mapped = item.get("mapped_theme_ids")
                if decision == "INCLUDE" and (
                    not isinstance(mapped, list)
                    or not {str(value).strip() for value in mapped}.intersection(theme_ids)
                ):
                    invalid = True
                observed[code] = item
            missing_codes = set(expected_by_code).difference(observed)
            if missing_codes:
                reasons.append("A1_MONTHLY_ROTATION_DECISIONS_INCOMPLETE")
            if duplicate:
                reasons.append("A1_MONTHLY_ROTATION_DECISIONS_DUPLICATE")
            if invalid:
                reasons.append("A1_MONTHLY_ROTATION_DECISIONS_INVALID")
    return tuple(dict.fromkeys(reasons))


def _a1_discovery_context_reasons(
    output: Mapping[str, Any],
    context: Mapping[str, Any],
) -> list[str]:
    mode = str(context.get("mode") or "")
    if mode == "POLICY_MACRO_DISCOVERY":
        return [] if _valid_a1_discovery_output(output) else ["A1_DISCOVERY_OUTPUT_INVALID"]
    if mode != "COMPANY_MAPPING":
        return []
    frozen_themes = context.get("structural_themes")
    frozen_nodes = context.get("industry_chain_graph")
    allowed_theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in frozen_themes
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    } if isinstance(frozen_themes, list) else set()
    allowed_node_ids = {
        str(item.get("node_id") or "").strip()
        for item in frozen_nodes
        if isinstance(item, Mapping) and str(item.get("node_id") or "").strip()
    } if isinstance(frozen_nodes, list) else set()
    output_theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in output.get("structural_themes", ())
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    } if isinstance(output.get("structural_themes"), list) else set()
    output_node_ids = {
        str(item.get("node_id") or "").strip()
        for item in output.get("industry_chain_graph", ())
        if isinstance(item, Mapping) and str(item.get("node_id") or "").strip()
    } if isinstance(output.get("industry_chain_graph"), list) else set()
    reasons: list[str] = []
    if output_theme_ids.difference(allowed_theme_ids):
        reasons.append("A1_BATCH_THEME_OUTSIDE_DISCOVERY")
    if output_node_ids.difference(allowed_node_ids):
        reasons.append("A1_BATCH_NODE_OUTSIDE_DISCOVERY")
    return reasons


def _a1_discovery_evidence_required(context: Mapping[str, Any]) -> bool:
    """Apply the strict packet evidence gate only to full discovery runs.

    Tiny pre-v2 fixtures and historical checkpoints may exercise the discovery
    shape without carrying a monthly packet. They remain readable, while any
    real/full-market run (or a context with canonical monthly decisions) must
    use the immutable packet/snapshot intersection and fail closed on gaps.
    """

    monthly = context.get("monthly_strategy_context")
    source = monthly if isinstance(monthly, Mapping) else context
    return (
        _safe_int(source.get("g0_symbol_count")) >= 500
        or bool(source.get("monthly_industry_decisions"))
    )


def _a1_discovery_evidence_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    authorized_source_refs: Sequence[str] | None = None,
) -> list[str]:
    # The discovery request and its final validator must share the exact same
    # immutable allowlist.  A caller that has rendered the compact packet passes
    # the packet/snapshot intersection; the fallback is retained for old
    # read-only callers that only have a snapshot and is intentionally limited to
    # the discovery-source projection below.
    valid_refs = {
        str(value)
        for value in (
            authorized_source_refs
            if authorized_source_refs is not None
            else _snapshot_discovery_evidence_refs(snapshot_data)
        )
        if isinstance(value, str) and value.strip()
    }
    reasons: list[str] = []
    for field, reason in (
        ("structural_themes", "A1_DISCOVERY_THEME_EVIDENCE_INVALID"),
        ("industry_chain_graph", "A1_DISCOVERY_NODE_EVIDENCE_INVALID"),
    ):
        records = output.get(field)
        if not isinstance(records, list):
            continue
        for record in records:
            refs, malformed = _discovery_record_source_refs(record)
            if malformed or not refs or not refs.issubset(valid_refs):
                reasons.append(reason)
                break
    mappings = output.get("industry_theme_mappings")
    if isinstance(mappings, list):
        for mapping in mappings:
            if not isinstance(mapping, Mapping):
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
            if str(mapping.get("mapping_status") or "").strip().upper() != "MAPPED":
                continue
            raw_refs = mapping.get("supporting_source_refs")
            if not isinstance(raw_refs, list) or not raw_refs:
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
            refs = {
                value.strip()
                for value in raw_refs
                if isinstance(value, str) and value.strip()
            }
            if len(refs) != len(raw_refs) or not refs.issubset(valid_refs):
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
    return reasons


def _discovery_record_source_refs(record: Any) -> tuple[set[str], bool]:
    """Read a discovery record's source refs without accepting lossy values."""

    if not isinstance(record, Mapping):
        return set(), True
    if "source_refs" in record:
        raw_refs = record.get("source_refs")
    elif "source_ref" in record:
        raw_refs = record.get("source_ref")
    else:
        return set(), True
    if isinstance(raw_refs, str):
        clean = raw_refs.strip()
        return ({clean} if clean else set()), not bool(clean) or clean != raw_refs
    if not isinstance(raw_refs, list) or not raw_refs:
        return set(), True
    refs = {value.strip() for value in raw_refs if isinstance(value, str) and value.strip()}
    return refs, len(refs) != len(raw_refs)


def _normalize_a1_discovery_source_refs(
    output: Mapping[str, Any],
    authorized_source_refs: Sequence[str],
) -> tuple[dict[str, Any], int]:
    """Normalize only authorized scalar refs; never invent discovery evidence."""

    authorized = frozenset(
        str(value)
        for value in authorized_source_refs
        if isinstance(value, str) and value.strip()
    )
    result = dict(output)
    changed = 0
    for field in ("structural_themes", "industry_chain_graph"):
        rows = result.get(field)
        if not isinstance(rows, list):
            continue
        normalized_rows: list[Any] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                normalized_rows.append(raw)
                continue
            item = dict(raw)
            if "source_refs" in item and isinstance(item.get("source_refs"), str):
                value = item["source_refs"]
                if value == value.strip() and value in authorized:
                    item["source_refs"] = [value]
                    changed += 1
            elif "source_ref" in item and isinstance(item.get("source_ref"), str):
                value = item["source_ref"]
                if value == value.strip() and value in authorized:
                    # Retain the original singular field for losslessness while
                    # supplying the canonical plural field expected by v3.
                    item["source_refs"] = [value]
                    changed += 1
            normalized_rows.append(item)
        result[field] = normalized_rows
    return result, changed


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
        "taxonomy_links": ("node_id", "taxonomy", "taxonomy_code"),
        "active_themes": ("theme_id",),
        "focus_pool": ("symbol",),
        "watch_only_pool": ("symbol",),
        "core_watch_pool": ("symbol",),
        "secondary_watch_pool": ("symbol",),
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


def _estimate_message_tokens(messages: Sequence[Mapping[str, Any]]) -> int:
    """Conservatively estimate mixed Chinese/JSON input without a model tokenizer.

    The three configured gateways do not expose a shared tokenizer.  ASCII
    JSON is approximated at four characters per token, while non-ASCII text is
    charged at two tokens per code point so CJK text and emoji fail closed.
    A small per-message allowance covers role and transport framing.
    """

    total = 0
    for message in messages:
        content = str(message.get("content", ""))
        ascii_chars = sum(ord(char) < 128 for char in content)
        non_ascii_chars = len(content) - ascii_chars
        total += (ascii_chars + 3) // 4
        total += non_ascii_chars * 2
        total += 8
    return total


def _a1_batch_is_splittable(reasons: Sequence[str]) -> bool:
    retryable_prefixes = (
        "MODEL_PROMPT_TOO_LARGE",
        "OUTPUT_BUDGET_",
        "NETWORK_",
        "MODEL_TOTAL_DEADLINE_",
        "STRICT_JSON_",
        "STREAM_",
        "RESPONSE_",
        "JSON_",
        "ENVELOPE_",
        "STAGE_ID_",
        "MODEL_NAME_",
        "SNAPSHOT_LINEAGE_",
        "APPROVED_POOL_",
        "A1_POOL_",
    )
    return any(str(reason).startswith(retryable_prefixes) for reason in reasons)


def _a2_batch_is_splittable(reasons: Sequence[str]) -> bool:
    retryable_prefixes = (
        "MODEL_PROMPT_TOO_LARGE",
        "OUTPUT_BUDGET_",
        "NETWORK_",
        "MODEL_TOTAL_DEADLINE_",
        "STRICT_JSON_",
        "STREAM_",
        "RESPONSE_",
        "JSON_",
        "ENVELOPE_",
        "STAGE_ID_",
        "MODEL_NAME_",
        "SNAPSHOT_LINEAGE_",
        "APPROVED_POOL_",
        "A2_POOL_",
    )
    return any(str(reason).startswith(retryable_prefixes) for reason in reasons)


def _common_variant(audits: Sequence[StageAudit]) -> str | None:
    variants = {audit.thinking_variant for audit in audits if audit.thinking_variant}
    if not variants:
        return None
    return next(iter(variants)) if len(variants) == 1 else "mixed"


def _common_text(values: Sequence[str]) -> str | None:
    retained = {str(value) for value in values if value}
    if not retained:
        return None
    return next(iter(retained)) if len(retained) == 1 else "mixed"


def _a1_missing_mapping_codes(
    output: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return exact missing INCLUDE codes for a field-level semantic retry."""

    expected = context.get("monthly_industry_decisions")
    if not isinstance(expected, list) or not isinstance(output.get("industry_theme_mappings"), list):
        return ()
    validation = validate_discovery_output(
        output,
        canonical_decisions=expected,
        require_targets=False,
    )
    return tuple(validation.missing_industry_codes)


def _semantic_retry_instruction(
    stage: str,
    reasons: Sequence[str],
    *,
    missing_mapping_codes: Sequence[str] = (),
    authorized_source_refs: Sequence[str] = (),
) -> str:
    safe_reasons = [
        reason
        for reason in dict.fromkeys(str(item) for item in reasons)
        if re.fullmatch(r"[A-Z0-9_:.-]{1,120}", reason)
    ][:20]
    discovery_requirements: list[str] = []
    if "A1_MONTHLY_THEME_COVERAGE_INSUFFICIENT" in safe_reasons:
        discovery_requirements.append(
            "Return 8-12 structural_themes with unique valid theme_id values."
        )
    if "A1_MONTHLY_CHAIN_COVERAGE_INSUFFICIENT" in safe_reasons:
        discovery_requirements.append(
            "Return 40-80 industry_chain_graph nodes with unique valid node_id values; "
            "each node must reference an existing theme_id."
        )
    if {
        "A1_DISCOVERY_THEME_EVIDENCE_INVALID",
        "A1_DISCOVERY_NODE_EVIDENCE_INVALID",
        "A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID",
    }.intersection(safe_reasons):
        discovery_requirements.append(
            "For every structural theme, every industry-chain node, and every MAPPED industry-theme "
            "mapping, copy at least one source_ref "
            "verbatim from RUNTIME_INPUT.a1_discovery_context.allowed_primary_source_refs. "
            "Use only that supplied structured context; do not invent, shorten, rewrite, or substitute "
            "source references."
        )
        discovery_requirements.append(
            "authorized_discovery_source_refs="
            + _canonical_json(list(dict.fromkeys(
                value for value in authorized_source_refs if isinstance(value, str) and value.strip()
            )))
        )
    discovery_retry = "\n".join(discovery_requirements)
    return (
        "PREVIOUS_RESPONSE_REJECTED\n"
        f"stage={stage}\n"
        f"reason_codes={_canonical_json(safe_reasons)}\n"
        + (
            f"missing_industry_theme_mapping_codes={_canonical_json(list(missing_mapping_codes)[:50])}\n"
            "Repair only the missing industry_theme_mappings; preserve valid mappings and do not return "
            "or rewrite server-owned monthly_industry_decisions.\n"
            if missing_mapping_codes
            else ""
        )
        + (f"DISCOVERY_CONTRACT_REPAIR\n{discovery_retry}\n" if discovery_retry else "")
        + (
            "Regenerate the discovery response from the original RUNTIME_INPUT. Copy required_envelope exactly, "
            "preserve valid mappings, and return one JSON object only. Do not return server-owned monthly decisions."
            if missing_mapping_codes
            else (
                "Regenerate the complete response from the original RUNTIME_INPUT. "
                "Copy required_envelope exactly, include every required top-level pool, "
                "and return one JSON object only. Do not discuss or repair the previous response."
            )
        )
    )


def _safe_diagnostics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    result = {
        "type": "object",
        "field_count": min(len(value), 100),
        "has_event_index": isinstance(value.get("event_index"), int),
    }
    for key in (
        "status_code",
        "content_type",
        "content_chars",
        "starts_with_object",
        "ends_with_object",
        "starts_with_fence",
        "ends_with_fence",
        "parsed_type",
    ):
        raw = value.get(key)
        if isinstance(raw, (str, int, bool)) and not isinstance(raw, float):
            result[key] = raw
    return result


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


def _stage_pool_counts(output: Mapping[str, Any], stage: str) -> dict[str, int]:
    fields = {
        "A1": ("active_research_pool", "monitor_pool", "rejected_candidates"),
        "A2": ("focus_pool", "watch_only_pool", "rejected_candidates"),
        "A3": ("core_watch_pool", "secondary_watch_pool", "rejected_candidates"),
    }[stage]
    return {
        field: len(output.get(field)) if isinstance(output.get(field), list) else 0
        for field in fields
    }


def _stage_outcome(audit: StageAudit) -> StageOutcome:
    """Project one audit without mistaking missing counts for zero stocks."""

    counts: dict[str, int] = {"selected": len(audit.symbols)}
    diagnostics = audit.diagnostics if isinstance(audit.diagnostics, Mapping) else {}
    local = diagnostics.get("local_screen")
    local = local if isinstance(local, Mapping) else {}
    evaluated = _safe_int(local.get("evaluated_count"))
    if evaluated > 0 or "evaluated_count" in local:
        counts["input"] = max(0, evaluated)
        counts["evaluated"] = max(0, evaluated)
    data_coverage: dict[str, float | int | str | None] = {}
    coverage = local.get("coverage_ratio")
    if isinstance(coverage, (int, float)) and not isinstance(coverage, bool):
        data_coverage["actual"] = float(coverage)
    required = local.get("minimum_coverage")
    if isinstance(required, (int, float)) and not isinstance(required, bool):
        data_coverage["required"] = float(required)
    sufficiency = local.get("data_sufficiency_state")
    if isinstance(sufficiency, str) and sufficiency:
        data_coverage["sufficiency_state"] = sufficiency
    critical = local.get("critical_factor_coverage")
    if isinstance(critical, Mapping):
        for name, value in critical.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                data_coverage[f"critical_{name}"] = float(value)
    minimum_critical = local.get("minimum_critical_factor_coverage")
    if isinstance(minimum_critical, (int, float)) and not isinstance(minimum_critical, bool):
        data_coverage["minimum_critical"] = float(minimum_critical)
    return stage_outcome_from_legacy(
        audit.status,
        stage=audit.stage,
        reason_codes=audit.reason_codes,
        counts=counts,
        data_coverage=data_coverage,
    )


def _refresh_analysis_counts(output: Mapping[str, Any], stage: str) -> dict[str, Any]:
    """Refresh aggregate counts after deterministic local rows are attached."""

    result = dict(output)
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    counts = _stage_pool_counts(result, stage)
    summary.update(counts)
    if stage == "A1":
        summary.update({
            "approved_count": counts["active_research_pool"],
            "monitor_count": counts["monitor_pool"],
            "rejected_count": counts["rejected_candidates"],
        })
    elif stage == "A2":
        summary.update({
            "approved_count": counts["focus_pool"],
            "monitor_count": counts["watch_only_pool"],
            "rejected_count": counts["rejected_candidates"],
        })
    else:
        summary.update({
            "approved_count": counts["core_watch_pool"],
            "monitor_count": counts["secondary_watch_pool"],
            "rejected_count": counts["rejected_candidates"],
        })
    result["analysis_summary"] = summary
    return result


def _canonicalize_stage_pool_fields(
    output: Mapping[str, Any],
    stage: str,
) -> tuple[dict[str, Any], int]:
    """Normalize harmless provider pool omissions/aliases to the runtime schema."""

    result = dict(output)
    changed = 0
    if stage == "A2":
        required = ("active_themes", "focus_pool", "watch_only_pool", "rejected_candidates")
    elif stage == "A3":
        if "secondary_watch_pool" not in result and isinstance(result.get("watch_only_pool"), list):
            result["secondary_watch_pool"] = result.pop("watch_only_pool")
            changed += 1
        required = ("core_watch_pool", "secondary_watch_pool", "rejected_candidates")
    else:
        return result, changed
    for field in required:
        if field not in result:
            result[field] = []
            changed += 1
    return result, changed


def _canonicalize_a1_driver_context(
    output: Mapping[str, Any],
    discovery_context: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Keep company batches read-only with respect to frozen A1 drivers."""

    result = dict(output)
    changed = 0
    for field in ("structural_themes", "industry_chain_graph"):
        frozen = discovery_context.get(field)
        if not isinstance(frozen, list):
            continue
        canonical = [dict(item) if isinstance(item, Mapping) else item for item in frozen]
        if result.get(field) != canonical:
            result[field] = canonical
            changed += 1
    return result, changed


def _canonicalize_a2_bottleneck_scorecards(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Recompute scorecards only for the strict supply-chain route.

    MARKET_CORE is a market-structure route and must not be rejected merely
    because it has no scarcity scorecard.  When a provider omits ``a2_route``,
    the server may fill it only from the frozen deterministic route context.
    """

    result = dict(output)
    changed = 0
    for pool in ("focus_pool", "watch_only_pool"):
        raw_items = result.get(pool)
        if not isinstance(raw_items, list):
            continue
        normalized: list[Any] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                normalized.append(raw_item)
                continue
            item = dict(raw_item)
            symbol = _first_symbol(item)
            route = _a2_item_route(item, snapshot_data, symbol)
            if route and item.get("a2_route") != route:
                item["a2_route"] = route
                changed += 1
            if route == MARKET_CORE_ROUTE:
                if item.get("bottleneck_status") != "NOT_REQUIRED_FOR_MARKET_CORE":
                    item["bottleneck_status"] = "NOT_REQUIRED_FOR_MARKET_CORE"
                    changed += 1
                normalized.append(item)
                continue
            scorecard, reasons = canonicalize_model_scorecard(item.get("bottleneck_scorecard"))
            if scorecard is not None:
                if item.get("bottleneck_scorecard") != scorecard:
                    changed += 1
                item["bottleneck_scorecard"] = scorecard
                item["bottleneck_score"] = scorecard["final_score"]
            elif reasons:
                existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*existing, *reasons]))
            normalized.append(item)
        result[pool] = normalized
    return result, changed


def _canonicalize_stage_scores(
    output: Mapping[str, Any],
    stage: str,
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Make configured weighted arithmetic deterministic and auditable.

    Models interpret evidence and assign component scores; the server owns the
    arithmetic. Older/provider-specific responses sometimes return already
    weighted point contributions instead of raw 0-100 component scores. That
    representation is normalized only when every value is within its exact
    contribution cap and the contribution sum matches the supplied total.
    """

    result = dict(output)
    specs = {
        "A1": ("SCORE_WEIGHTS", "structural_score", ("active_research_pool",), False),
        "A2": ("THEME_SCORE_WEIGHTS", "theme_score", ("active_themes",), True),
        "A3": (
            "TECHNICAL_SCORE_WEIGHTS",
            "technical_score",
            ("core_watch_pool", "secondary_watch_pool"),
            False,
        ),
    }
    weight_field, score_field, pools, include_penalties = specs[stage]
    raw_weights = snapshot_data.get(weight_field)
    if not isinstance(raw_weights, Mapping) or not raw_weights:
        return result, 0
    weights = {str(key): _safe_float(value) for key, value in raw_weights.items()}
    if (
        any(value <= 0 or value > 1 for value in weights.values())
        or abs(sum(weights.values()) - 1.0) > 1e-6
    ):
        return result, 0

    changed = 0
    for pool in pools:
        raw_items = result.get(pool)
        if not isinstance(raw_items, list):
            continue
        normalized_items: list[Any] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                normalized_items.append(raw_item)
                continue
            item = dict(raw_item)
            breakdown = item.get("score_breakdown")
            if not isinstance(breakdown, Mapping) or set(breakdown) != set(weights):
                normalized_items.append(item)
                continue
            if any(
                isinstance(breakdown.get(key), bool)
                or breakdown.get(key) is None
                or _safe_float(breakdown.get(key)) < 0
                or _safe_float(breakdown.get(key)) > 100
                for key in weights
            ):
                normalized_items.append(item)
                continue
            values = {key: _safe_float(breakdown.get(key)) for key in weights}
            penalty_points = 0.0
            if include_penalties:
                penalties = item.get("penalties")
                penalty_points = sum(
                    _safe_float(penalty.get("points"))
                    for penalty in penalties
                    if isinstance(penalty, Mapping)
                ) if isinstance(penalties, list) else 0.0

            contribution_total = sum(values.values()) + penalty_points
            contribution_mode = (
                all(values[key] <= weights[key] * 100 + 1e-6 for key in weights)
                and abs(contribution_total - _safe_float(item.get(score_field))) <= 0.51
            )
            if contribution_mode:
                values = {key: values[key] / weights[key] for key in weights}
                item["score_breakdown"] = {
                    key: round(values[key], 6) for key in weights
                }
            computed = max(
                0.0,
                min(100.0, sum(values[key] * weights[key] for key in weights) + penalty_points),
            )
            canonical_score = round(computed, 2)
            if contribution_mode or abs(canonical_score - _safe_float(item.get(score_field))) > 0.005:
                item[score_field] = canonical_score
                changed += 1
            normalized_items.append(item)
        result[pool] = normalized_items

    if stage == "A2":
        canonical_theme_scores = {
            str(item.get("theme_id") or "").strip(): _safe_float(item.get("theme_score"))
            for item in result.get("active_themes", ())
            if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
        }
        for pool in ("focus_pool", "watch_only_pool"):
            raw_items = result.get(pool)
            if not isinstance(raw_items, list):
                continue
            normalized_items: list[Any] = []
            for raw_item in raw_items:
                if not isinstance(raw_item, Mapping):
                    normalized_items.append(raw_item)
                    continue
                item = dict(raw_item)
                theme_id = str(item.get("theme_id") or "").strip()
                if theme_id in canonical_theme_scores and abs(
                    _safe_float(item.get("theme_score")) - canonical_theme_scores[theme_id]
                ) > 0.005:
                    item["theme_score"] = canonical_theme_scores[theme_id]
                    changed += 1
                normalized_items.append(item)
            result[pool] = normalized_items
    return result, changed


def _apply_stage_threshold_policy(
    output: Mapping[str, Any],
    stage: str,
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Enforce configured funnel thresholds after model interpretation."""

    result = dict(output)
    if stage == "A1":
        raw_minimums = snapshot_data.get("A1_MINIMUMS")
        minimums = raw_minimums if isinstance(raw_minimums, Mapping) else {}
        thresholds = (
            ("structural_score", _safe_float(minimums.get("structural_score", 65)), "A1_SCORE_BELOW_MINIMUM"),
            (
                "data_quality_score",
                _safe_float(minimums.get("data_quality_score", 75)),
                "A1_DATA_QUALITY_BELOW_MINIMUM",
            ),
            (
                "evidence_confidence",
                _safe_float(minimums.get("evidence_confidence", 0.70)),
                "A1_EVIDENCE_CONFIDENCE_BELOW_MINIMUM",
            ),
        )
        active = result.get("active_research_pool")
        monitor = list(result.get("monitor_pool")) if isinstance(result.get("monitor_pool"), list) else []
        if not isinstance(active, list):
            return result, 0
        structural_evidence_refs = _snapshot_primary_evidence_refs(snapshot_data)
        retained: list[Any] = []
        changed = 0
        for raw_item in active:
            if not isinstance(raw_item, Mapping):
                retained.append(raw_item)
                continue
            item = dict(raw_item)
            reason_codes = [
                reason
                for field, minimum, reason in thresholds
                if _safe_float(item.get(field)) < minimum
            ]
            reason_codes.extend(_a1_business_evidence_reasons(item, snapshot_data))
            if snapshot_data.get("A1_DRIVER_LINEAGE_REQUIRED") is True:
                reason_codes.extend(_a1_structural_lineage_reasons(item, result, structural_evidence_refs))
            reason_codes.extend(_a1_score_breakdown_reasons(item, snapshot_data))
            if not reason_codes:
                retained.append(item)
                continue
            existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            item["reason_codes"] = list(dict.fromkeys([*existing, *reason_codes]))
            item["status"] = "MONITOR"
            monitor.append(item)
            changed += 1
        result["active_research_pool"] = retained
        result["monitor_pool"] = _deduplicate_stage_items("monitor_pool", monitor)
        if changed:
            result["analysis_summary"] = _policy_summary(result, stage, changed)
        return result, changed

    if stage == "A2":
        minimum = _safe_float(snapshot_data.get("MIN_THEME_SCORE", 60))
        focus = result.get("focus_pool")
        watch = list(result.get("watch_only_pool")) if isinstance(result.get("watch_only_pool"), list) else []
        if not isinstance(focus, list):
            return result, 0
        retained = []
        changed = 0
        for raw_item in focus:
            if not isinstance(raw_item, Mapping) or _safe_float(raw_item.get("theme_score")) >= minimum:
                retained.append(raw_item)
                continue
            item = dict(raw_item)
            existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            item["reason_codes"] = list(dict.fromkeys([*existing, "A2_THEME_SCORE_BELOW_MINIMUM"]))
            watch.append(item)
            changed += 1
        result["focus_pool"] = retained
        result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
        if changed:
            result["analysis_summary"] = _policy_summary(result, stage, changed)
        return result, changed

    if stage == "A3":
        minimum_technical = _safe_float(snapshot_data.get("MIN_TECHNICAL_SCORE", 70))
        minimum_reward_risk = _safe_float(snapshot_data.get("MIN_REWARD_RISK", 2.0))
        maximum_stop = _safe_float(snapshot_data.get("MAX_STOP_DISTANCE", 0.06))
        core = result.get("core_watch_pool")
        secondary = (
            list(result.get("secondary_watch_pool"))
            if isinstance(result.get("secondary_watch_pool"), list)
            else []
        )
        rejected = (
            list(result.get("rejected_candidates"))
            if isinstance(result.get("rejected_candidates"), list)
            else []
        )
        if not isinstance(core, list):
            return result, 0
        retained = []
        changed = 0
        for raw_item in core:
            if not isinstance(raw_item, Mapping):
                retained.append(raw_item)
                continue
            item = dict(raw_item)
            hard_reasons: list[str] = []
            if _safe_float(item.get("reward_risk")) < minimum_reward_risk:
                hard_reasons.append("A3_REWARD_RISK_BELOW_MINIMUM")
            stop_distance = _safe_float(item.get("stop_distance_pct"))
            if stop_distance <= 0 or stop_distance > maximum_stop:
                hard_reasons.append("A3_STOP_DISTANCE_OUTSIDE_LIMIT")
            if hard_reasons:
                rejected.append(
                    {
                        "symbol": _first_symbol(item),
                        "parent_candidate_id": item.get("parent_candidate_id"),
                        "reason_codes": hard_reasons,
                        "veto_triggered": "SERVER_THRESHOLD_POLICY",
                    }
                )
                changed += 1
                continue
            score_reasons = _weighted_score_reasons(
                item,
                weights=snapshot_data.get("TECHNICAL_SCORE_WEIGHTS"),
                score_field="technical_score",
                missing_reason="A3_SCORE_BREAKDOWN_MISSING",
                invalid_reason="A3_SCORE_BREAKDOWN_INVALID",
                mismatch_reason="A3_TECHNICAL_SCORE_MISMATCH",
            )
            if score_reasons:
                existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*existing, *score_reasons]))
                item["risk_unit"] = "NO_ENTRY"
                secondary.append(item)
                changed += 1
                continue
            if _safe_float(item.get("technical_score")) < minimum_technical:
                existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*existing, "A3_TECHNICAL_SCORE_BELOW_MINIMUM"]))
                item["risk_unit"] = "NO_ENTRY"
                secondary.append(item)
                changed += 1
                continue
            if item.get("risk_unit") == "NO_ENTRY":
                existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*existing, "A3_NO_ENTRY_IS_SECONDARY_ONLY"]))
                secondary.append(item)
                changed += 1
                continue
            retained.append(item)
        result["core_watch_pool"] = retained
        result["secondary_watch_pool"] = _deduplicate_stage_items("secondary_watch_pool", secondary)
        result["rejected_candidates"] = _deduplicate_stage_items("rejected_candidates", rejected)
        result, limit_changes = _apply_a3_pool_limits(result, snapshot_data)
        changed += limit_changes
        if changed:
            result["analysis_summary"] = _policy_summary(result, stage, changed)
        return result, changed

    return result, 0


def _apply_a3_pool_limits(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    result = dict(output)
    params = snapshot_data.get("REGIME_PARAM_SET")
    agent = params.get("agent_3") if isinstance(params, Mapping) else None
    if not isinstance(agent, Mapping):
        return result, 0
    core_max = max(0, _safe_int(agent.get("core_watch_max", 0)))
    total_max = max(core_max, _safe_int(agent.get("total_watch_max", core_max)))
    core = list(result.get("core_watch_pool")) if isinstance(result.get("core_watch_pool"), list) else []
    secondary = list(result.get("secondary_watch_pool")) if isinstance(result.get("secondary_watch_pool"), list) else []
    rejected = list(result.get("rejected_candidates")) if isinstance(result.get("rejected_candidates"), list) else []

    def ranking(item: Any) -> tuple[float, str]:
        return (
            -_safe_float(item.get("technical_score")) if isinstance(item, Mapping) else 0.0,
            _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )

    ordered_core = sorted(core, key=ranking)
    retained_core = ordered_core[:core_max]
    overflow_core: list[Any] = []
    for raw_item in ordered_core[core_max:]:
        if not isinstance(raw_item, Mapping):
            overflow_core.append(raw_item)
            continue
        item = dict(raw_item)
        codes = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
        item["reason_codes"] = list(dict.fromkeys([*codes, "A3_GLOBAL_CORE_LIMIT"]))
        item["risk_unit"] = "NO_ENTRY"
        overflow_core.append(item)
    ordered_secondary = sorted([*secondary, *overflow_core], key=ranking)
    secondary_max = max(0, total_max - len(retained_core))
    retained_secondary = ordered_secondary[:secondary_max]
    overflow_secondary = ordered_secondary[secondary_max:]
    for raw_item in overflow_secondary:
        symbol = _first_symbol(raw_item) if isinstance(raw_item, Mapping) else ""
        rejected.append({
            "symbol": symbol,
            "parent_candidate_id": raw_item.get("parent_candidate_id") if isinstance(raw_item, Mapping) else None,
            "reason_codes": ["A3_GLOBAL_WATCH_LIMIT"],
            "veto_triggered": "SERVER_POOL_LIMIT",
        })
    result["core_watch_pool"] = retained_core
    result["secondary_watch_pool"] = _deduplicate_stage_items("secondary_watch_pool", retained_secondary)
    result["rejected_candidates"] = _deduplicate_stage_items("rejected_candidates", rejected)
    changed_tokens = {
        _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item)
        for item in [*overflow_core, *overflow_secondary]
    }
    return result, len(changed_tokens)


def _a1_business_evidence_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    if "MAIN_BUSINESS_EVIDENCE" not in snapshot_data:
        # Backward-compatible replay/test snapshots predate the deterministic
        # evidence contract. New production snapshots always include it.
        return []
    symbol = _first_symbol(item)
    raw_evidence = snapshot_data.get("MAIN_BUSINESS_EVIDENCE")
    symbol_evidence = raw_evidence.get(symbol) if isinstance(raw_evidence, Mapping) else None
    if not isinstance(symbol_evidence, Mapping) or symbol_evidence.get("available") is not True:
        return ["A1_MAIN_BUSINESS_EVIDENCE_MISSING"]

    exposure = item.get("business_exposure")
    if not isinstance(exposure, Mapping):
        return ["A1_REVENUE_EXPOSURE_UNCONFIRMED", "A1_BUSINESS_SOURCE_REF_INVALID"]
    revenue_exposure = _safe_float(exposure.get("revenue_exposure_pct"))
    reasons: list[str] = []
    if revenue_exposure <= 0 or revenue_exposure > 100:
        reasons.append("A1_REVENUE_EXPOSURE_UNCONFIRMED")

    valid_refs = {
        str(evidence.get("source_ref"))
        for evidence in symbol_evidence.get("evidence", ())
        if isinstance(evidence, Mapping) and evidence.get("source_ref")
    }
    if str(exposure.get("source_ref") or "") not in valid_refs:
        reasons.append("A1_BUSINESS_SOURCE_REF_INVALID")
    return reasons


def _a1_structural_lineage_reasons(
    item: Mapping[str, Any],
    output: Mapping[str, Any],
    valid_refs: set[str],
) -> list[str]:
    """Reject free-form A1 narratives that are not bound to frozen evidence."""

    themes = output.get("structural_themes")
    nodes = output.get("industry_chain_graph")
    if not isinstance(themes, list):
        return ["A1_STRUCTURAL_THEME_LINEAGE_MISSING", "A1_CHAIN_NODE_LINEAGE_MISSING"]
    if not isinstance(nodes, list):
        return ["A1_CHAIN_NODE_LINEAGE_MISSING"]
    primary_theme = str(item.get("primary_theme") or "").strip()
    chain_node = str(item.get("industry_chain_node") or "").strip()
    matched_theme = next((
        theme for theme in themes
        if isinstance(theme, Mapping)
        and primary_theme in {
            str(theme.get("theme_id") or "").strip(),
            str(theme.get("display_name") or "").strip(),
        }
    ), None)
    matched_node = next((
        node for node in nodes
        if isinstance(node, Mapping) and chain_node == str(node.get("node_id") or "").strip()
    ), None)
    reasons: list[str] = []
    if matched_theme is None:
        reasons.append("A1_STRUCTURAL_THEME_LINEAGE_MISSING")
    if matched_node is None:
        reasons.append("A1_CHAIN_NODE_LINEAGE_MISSING")
    if matched_theme is not None and matched_node is not None:
        theme_id = str(matched_theme.get("theme_id") or "").strip()
        node_theme_ids = matched_node.get("theme_ids")
        if not isinstance(node_theme_ids, list) or theme_id not in {
            str(value).strip() for value in node_theme_ids if isinstance(value, str)
        }:
            reasons.append("A1_CHAIN_NODE_THEME_LINK_INVALID")
    for matched, reason in (
        (matched_theme, "A1_THEME_DRIVER_EVIDENCE_INVALID"),
        (matched_node, "A1_CHAIN_NODE_EVIDENCE_INVALID"),
    ):
        if matched is None:
            continue
        raw_refs = matched.get("source_refs")
        refs = {
            str(value).strip()
            for value in raw_refs
            if isinstance(value, str) and value.strip()
        } if isinstance(raw_refs, list) else set()
        if not refs.intersection(valid_refs):
            reasons.append(reason)
    return reasons


def _a1_score_breakdown_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Ensure the model used configured A1 weights instead of an ad-hoc score."""

    raw_weights = snapshot_data.get("SCORE_WEIGHTS")
    if not isinstance(raw_weights, Mapping) or not raw_weights:
        return []
    weights = {
        str(key): _safe_float(value)
        for key, value in raw_weights.items()
        if isinstance(key, str) and 0 < _safe_float(value) <= 1
    }
    if not weights or abs(sum(weights.values()) - 1.0) > 1e-6:
        return ["A1_SCORE_WEIGHTS_INVALID"]
    breakdown = item.get("score_breakdown")
    if not isinstance(breakdown, Mapping):
        return ["A1_SCORE_BREAKDOWN_MISSING"]
    if set(breakdown) != set(weights):
        return ["A1_SCORE_BREAKDOWN_INVALID"]
    values: dict[str, float] = {}
    for key in weights:
        raw_value = breakdown.get(key)
        value = _safe_float(raw_value)
        if isinstance(raw_value, bool) or raw_value is None or value < 0 or value > 100:
            return ["A1_SCORE_BREAKDOWN_INVALID"]
        values[key] = value
    computed = sum(values[key] * weights[key] for key in weights)
    if abs(computed - _safe_float(item.get("structural_score"))) > 0.51:
        return ["A1_STRUCTURAL_SCORE_MISMATCH"]
    return []


def _weighted_score_reasons(
    item: Mapping[str, Any],
    *,
    weights: Any,
    score_field: str,
    missing_reason: str,
    invalid_reason: str,
    mismatch_reason: str,
) -> list[str]:
    if not isinstance(weights, Mapping) or not weights:
        return []
    resolved_weights = {str(key): _safe_float(value) for key, value in weights.items()}
    if abs(sum(resolved_weights.values()) - 1.0) > 1e-6:
        return [invalid_reason]
    breakdown = item.get("score_breakdown")
    if not isinstance(breakdown, Mapping) or set(breakdown) != set(resolved_weights):
        return [missing_reason]
    values = {key: _safe_float(breakdown.get(key)) for key in resolved_weights}
    if any(value < 0 or value > 100 for value in values.values()):
        return [invalid_reason]
    computed = sum(values[key] * resolved_weights[key] for key in resolved_weights)
    if abs(computed - _safe_float(item.get(score_field))) > 0.51:
        return [mismatch_reason]
    return []


def _snapshot_primary_evidence_refs(snapshot_data: Mapping[str, Any]) -> set[str]:
    """Return refs allowed to prove A1 policy/macro-to-business lineage.

    Open-news/RSS material is deliberately excluded: it is T3 discovery input
    and may corroborate a thesis, but cannot by itself promote a company into
    the A1 ACTIVE pool.
    """

    refs: set[str] = set()
    relevant = (
        "MACRO_POLICY_FEED",
        "DISCLOSURE_EVENTS",
        "RISK_EVENTS",
        "MAIN_BUSINESS_EVIDENCE",
        "INDUSTRY_ACTIVITY_DATA",
        "INDUSTRY_PROFIT_DATA",
    )

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) in {"fact_id", "source_ref", "source_url", "content_hash"} and isinstance(item, str):
                    clean = item.strip()
                    if clean:
                        refs.add(clean)
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for key in relevant:
        visit(snapshot_data.get(key))
    return refs


def _snapshot_discovery_evidence_refs(snapshot_data: Mapping[str, Any]) -> set[str]:
    """Return a bounded-source domain suitable for macro/chain discovery.

    Discovery should cite policy or formal industry-operating evidence, not a
    random company PDF from the much larger company-mapping snapshot.  The
    returned identifiers are copied verbatim into the prompt as an allowlist;
    the ordinary primary-evidence validator remains the final authority.
    """

    refs: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) in {"fact_id", "source_ref", "source_url", "content_hash"} and isinstance(item, str):
                    clean = item.strip()
                    if clean:
                        refs.add(clean)
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for key in ("MACRO_POLICY_FEED", "INDUSTRY_ACTIVITY_DATA", "INDUSTRY_PROFIT_DATA"):
        visit(snapshot_data.get(key))
    # An empty discovery-source domain is meaningful: allowing a fallback to
    # company disclosures would let a model manufacture a macro/industry theme
    # from evidence that was never supplied for discovery.
    return refs


def _authorized_discovery_source_refs(
    snapshot_data: Mapping[str, Any],
    packet: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return the immutable packet-source ∩ snapshot-discovery allowlist."""

    packet_index = packet.get("source_index") if isinstance(packet, Mapping) else None
    if isinstance(packet_index, Mapping):
        packet_refs = {
            str(value).strip()
            for value in packet_index
            if isinstance(value, str) and value.strip()
        }
    elif isinstance(packet_index, (list, tuple, set, frozenset)):
        packet_refs = {
            str(value).strip()
            for value in packet_index
            if isinstance(value, str) and value.strip()
        }
    else:
        packet_refs = set()
    return tuple(sorted(packet_refs.intersection(_snapshot_discovery_evidence_refs(snapshot_data))))


def _a2_bottleneck_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Require a source-backed scarce-layer thesis for every A2 focus item."""

    reasons: list[str] = []
    symbol = _first_symbol(item)
    raw_context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    # Pre-v2/legacy snapshots remain replayable. New deterministic-v2 runs
    # always attach this context after the local A2 gate.
    if not isinstance(raw_context, Mapping):
        return reasons
    context = raw_context.get(symbol) if isinstance(raw_context, Mapping) else None
    if not isinstance(context, Mapping):
        reasons.append("A2_BOTTLENECK_CONTEXT_MISSING")
    route = _a2_item_route(item, snapshot_data, symbol)
    if route == MARKET_CORE_ROUTE:
        if str(item.get("bottleneck_status") or "").strip().upper() != "NOT_REQUIRED_FOR_MARKET_CORE":
            reasons.append("A2_MARKET_CORE_STATUS_INVALID")
        return list(dict.fromkeys(reasons))
    if route != SUPPLY_CHAIN_ALPHA_ROUTE:
        reasons.append("A2_ROUTE_MISSING_OR_INVALID")
        return list(dict.fromkeys(reasons))
    role = str(item.get("supply_chain_role") or "").strip()
    if role not in SUPPLY_CHAIN_ROLES or role == "STORY_ONLY":
        reasons.append("A2_SUPPLY_CHAIN_ROLE_NOT_FOCUS_ELIGIBLE")
    if not str(item.get("scarce_layer") or "").strip():
        reasons.append("A2_SCARCE_LAYER_MISSING")
    if not str(item.get("value_chain_position") or "").strip():
        reasons.append("A2_VALUE_CHAIN_POSITION_MISSING")
    scorecard, scorecard_reasons = canonicalize_model_scorecard(item.get("bottleneck_scorecard"))
    reasons.extend(scorecard_reasons)
    if scorecard is not None and role in {"CONTROLS_SCARCE_LAYER", "SUPPLIES_SCARCE_LAYER"}:
        factors = scorecard["factors"]
        if (
            _safe_float(factors.get("chokepoint_severity")) < 3.0
            or max(
                _safe_float(factors.get("supplier_concentration")),
                _safe_float(factors.get("expansion_difficulty")),
            ) < 2.0
        ):
            reasons.append("A2_SCARCE_LAYER_SCORE_UNSUPPORTED")

    evidence = item.get("bottleneck_evidence")
    evidence = evidence if isinstance(evidence, list) else []
    allowed_refs = _snapshot_primary_evidence_refs(snapshot_data)
    if isinstance(context, Mapping):
        allowed_refs.update(
            str(value).strip()
            for value in context.get("source_refs", ())
            if isinstance(value, str) and value.strip()
        )
    allowed_refs.update(
        str(value).strip()
        for value in item.get("source_refs", ())
        if isinstance(value, str) and value.strip()
    ) if isinstance(item.get("source_refs"), list) else None
    valid_evidence = 0
    stronger_evidence = 0
    for raw in evidence:
        if not isinstance(raw, Mapping):
            continue
        strength = str(raw.get("strength") or "").strip().upper()
        source_ref = str(raw.get("source_ref") or "").strip()
        claim = str(raw.get("claim") or "").strip()
        if not claim or strength not in EVIDENCE_STRENGTHS or not source_ref:
            continue
        if allowed_refs and source_ref not in allowed_refs:
            continue
        valid_evidence += 1
        if strength in {"STRONG", "MEDIUM"}:
            stronger_evidence += 1
    if valid_evidence < 2:
        reasons.append("A2_BOTTLENECK_EVIDENCE_INSUFFICIENT")
    if stronger_evidence < 1:
        reasons.append("A2_BOTTLENECK_STRONG_EVIDENCE_MISSING")
    if not str(item.get("missing_proof") or "").strip():
        reasons.append("A2_BOTTLENECK_MISSING_PROOF_UNDECLARED")
    kill_switches = item.get("kill_switches")
    if not isinstance(kill_switches, list) or not any(str(value).strip() for value in kill_switches):
        reasons.append("A2_BOTTLENECK_KILL_SWITCH_MISSING")
    return list(dict.fromkeys(reasons))


def _a2_item_route(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    symbol: str,
) -> str | None:
    """Return an explicit or deterministically inferred A2 selection route."""

    explicit = str(item.get("a2_route") or item.get("selection_route") or item.get("route") or "").strip().upper()
    if explicit in {MARKET_CORE_ROUTE, SUPPLY_CHAIN_ALPHA_ROUTE}:
        return explicit
    raw_context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    context = raw_context.get(symbol) if isinstance(raw_context, Mapping) else None
    eligible = context.get("eligible_routes") if isinstance(context, Mapping) else None
    if isinstance(eligible, Sequence) and not isinstance(eligible, (str, bytes, bytearray)):
        normalized = [str(value).strip().upper() for value in eligible]
        if MARKET_CORE_ROUTE in normalized:
            return MARKET_CORE_ROUTE
        if SUPPLY_CHAIN_ALPHA_ROUTE in normalized:
            return SUPPLY_CHAIN_ALPHA_ROUTE
    if isinstance(context, Mapping):
        # Historical deterministic-v2 snapshots predate the dual-route field
        # and represented only the strict bottleneck path.  Keep those frozen
        # replays auditable instead of silently treating them as MARKET_CORE.
        return SUPPLY_CHAIN_ALPHA_ROUTE
    return None


def _apply_a2_lineage_policy(
    output: Mapping[str, Any],
    upstream_output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Demote A2 focus items that rewrite A1 themes or invent missing facts."""

    result = dict(output)
    upstream_themes = upstream_output.get("structural_themes")
    allowed_theme_ids: set[str] = set()
    if isinstance(upstream_themes, list):
        for theme in upstream_themes:
            if not isinstance(theme, Mapping):
                continue
            theme_id = str(theme.get("theme_id") or "").strip()
            if theme_id:
                allowed_theme_ids.add(theme_id)

    active_themes = result.get("active_themes")
    valid_active_themes: set[str] = set()
    if isinstance(active_themes, list):
        normalized_themes: list[Any] = []
        for raw_theme in active_themes:
            if not isinstance(raw_theme, Mapping):
                normalized_themes.append(raw_theme)
                continue
            theme = dict(raw_theme)
            theme_id = str(theme.get("theme_id") or "").strip()
            theme_reasons = _a2_theme_reasons(theme, snapshot_data)
            if theme_id not in allowed_theme_ids:
                theme_reasons.append("A2_THEME_OUTSIDE_A1")
            if theme_reasons:
                existing = theme.get("reason_codes") if isinstance(theme.get("reason_codes"), list) else []
                theme["reason_codes"] = list(dict.fromkeys([*existing, *theme_reasons]))
            else:
                valid_active_themes.add(theme_id)
            normalized_themes.append(theme)
        result["active_themes"] = normalized_themes

    focus = result.get("focus_pool")
    watch = list(result.get("watch_only_pool")) if isinstance(result.get("watch_only_pool"), list) else []
    if not isinstance(focus, list):
        return result, 0
    minimum_identity = _safe_float(snapshot_data.get("MIN_IDENTIFIABILITY_SCORE", 60))
    retained: list[Any] = []
    changed = 0
    for raw_item in focus:
        if not isinstance(raw_item, Mapping):
            retained.append(raw_item)
            continue
        item = dict(raw_item)
        reasons: list[str] = []
        theme_id = str(item.get("theme_id") or "").strip()
        if theme_id not in valid_active_themes:
            reasons.append("A2_THEME_LINEAGE_INVALID")
        role = str(item.get("market_role") or "").strip()
        focus_roles = {"LEADER", "CORE_ARMY", "TREND_CORE", "CHAIN_RESONANCE", "FIRST_MOVER"}
        if role not in focus_roles:
            reasons.append("A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE")
        if _safe_float(item.get("identifiability_score")) < minimum_identity:
            reasons.append("A2_IDENTIFIABILITY_BELOW_MINIMUM")
        reasons.extend(_a2_bottleneck_reasons(item, snapshot_data))
        active_theme = next((
            theme for theme in result.get("active_themes", ())
            if isinstance(theme, Mapping) and str(theme.get("theme_id") or "").strip() == theme_id
        ), None)
        if isinstance(active_theme, Mapping):
            if abs(_safe_float(item.get("theme_score")) - _safe_float(active_theme.get("theme_score"))) > 0.51:
                reasons.append("A2_THEME_SCORE_LINEAGE_MISMATCH")
            stage = str(active_theme.get("stage") or "")
            policy = str(active_theme.get("new_entry_policy") or "")
            if stage in {"CLIMAX", "DIVERGENCE", "RETREAT", "FADE"} or policy in {"WATCH_ONLY", "NO_NEW_ENTRY"}:
                reasons.append("A2_THEME_STAGE_NOT_FOCUS_ELIGIBLE")
        if not reasons:
            retained.append(item)
            continue
        existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
        item["reason_codes"] = list(dict.fromkeys([*existing, *reasons]))
        watch.append(item)
        changed += 1
    result["focus_pool"] = retained
    result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
    params = snapshot_data.get("REGIME_PARAM_SET")
    agent = params.get("agent_2") if isinstance(params, Mapping) else None
    if isinstance(agent, Mapping):
        focus_max = max(0, _safe_int(agent.get("focus_pool_max", len(retained))))
        ordered_focus = sorted(
            retained,
            key=lambda candidate: (
                -_safe_float(candidate.get("theme_score")) if isinstance(candidate, Mapping) else 0.0,
                -_safe_float(candidate.get("identifiability_score")) if isinstance(candidate, Mapping) else 0.0,
                _first_symbol(candidate) if isinstance(candidate, Mapping) else _canonical_json(candidate),
            ),
        )
        retained = ordered_focus[:focus_max]
        for raw_item in ordered_focus[focus_max:]:
            if not isinstance(raw_item, Mapping):
                watch.append(raw_item)
                continue
            item = dict(raw_item)
            existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            item["reason_codes"] = list(dict.fromkeys([*existing, "A2_GLOBAL_FOCUS_LIMIT"]))
            watch.append(item)
            changed += 1
        result["focus_pool"] = retained
        result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
    if changed:
        result["analysis_summary"] = _policy_summary(result, "A2", changed)
    return result, changed


def _annotate_a2_pool_target(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Report institutional pool capacity without manufacturing candidates."""

    result = dict(output)
    raw_targets = snapshot_data.get("A2_POOL_TARGETS")
    targets = raw_targets if isinstance(raw_targets, Mapping) else {}
    minimum = max(0, _safe_int(targets.get("pool_min", 100)))
    maximum = max(minimum, _safe_int(targets.get("pool_max", 200)))
    focus_count = len(result.get("focus_pool")) if isinstance(result.get("focus_pool"), list) else 0
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    reason_codes = summary.get("reason_codes") if isinstance(summary.get("reason_codes"), list) else []
    if focus_count < minimum:
        reason_codes = list(dict.fromkeys([*reason_codes, "POOL_TARGET_UNDERFILLED"]))
    summary.update({
        "focus_pool_count": focus_count,
        "pool_target": {"minimum": minimum, "maximum": maximum, "quota_forbidden": True},
        "pool_target_underfilled_by": max(0, minimum - focus_count),
        "reason_codes": reason_codes,
    })
    result["analysis_summary"] = summary
    return result


def _annotate_a1_pool_target(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose A1 research coverage gaps without changing the classification."""

    result = dict(output)
    raw_targets = snapshot_data.get("A1_POOL_TARGETS")
    targets = raw_targets if isinstance(raw_targets, Mapping) else {}
    active_min, active_max = _target_pair(targets.get("active_research_target"), (100, 250))
    clue_min, clue_max = _target_pair(targets.get("clue_pool_target"), (300, 800))
    active_count = len(result.get("active_research_pool")) if isinstance(result.get("active_research_pool"), list) else 0
    monitor_count = len(result.get("monitor_pool")) if isinstance(result.get("monitor_pool"), list) else 0
    clue_count = active_count + monitor_count
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    reason_codes = summary.get("reason_codes") if isinstance(summary.get("reason_codes"), list) else []
    if active_count < active_min:
        reason_codes = [*reason_codes, "A1_ACTIVE_TARGET_UNDERFILLED"]
    if clue_count < clue_min:
        reason_codes = [*reason_codes, "A1_CLUE_TARGET_UNDERFILLED"]
    summary.update({
        "institutional_pool_role": "P2_CLUE_TO_P3_RESEARCH_COVERAGE",
        "active_research_count": active_count,
        "clue_pool_count": clue_count,
        "active_research_target": {"minimum": active_min, "maximum": active_max},
        "clue_pool_target": {"minimum": clue_min, "maximum": clue_max},
        "quota_forbidden": True,
        "reason_codes": list(dict.fromkeys(reason_codes)),
    })
    result["analysis_summary"] = summary
    return result


def _target_pair(value: Any, default: tuple[int, int]) -> tuple[int, int]:
    if (
        isinstance(value, list)
        and len(value) == 2
        and all(isinstance(item, int) and not isinstance(item, bool) for item in value)
        and 0 <= value[0] <= value[1]
    ):
        return value[0], value[1]
    return default


def _a2_theme_reasons(theme: Mapping[str, Any], snapshot_data: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if str(theme.get("stage") or "") not in {
        "IGNITION", "CONFIRMATION", "ACCELERATION", "CLIMAX",
        "DIVERGENCE", "RETREAT", "REPAIR", "FADE",
    }:
        reasons.append("A2_THEME_STAGE_INVALID")
    if str(theme.get("new_entry_policy") or "") not in {
        "ALLOW", "PROBE_ONLY", "WATCH_ONLY", "NO_NEW_ENTRY",
    }:
        reasons.append("A2_NEW_ENTRY_POLICY_INVALID")
    for field, reason in (
        ("supporting_evidence", "A2_SUPPORTING_EVIDENCE_MISSING"),
        ("contradicting_evidence", "A2_CONTRADICTING_EVIDENCE_MISSING"),
    ):
        value = theme.get(field)
        if not isinstance(value, list) or not value:
            reasons.append(reason)
    raw_weights = snapshot_data.get("THEME_SCORE_WEIGHTS")
    breakdown = theme.get("score_breakdown")
    if isinstance(raw_weights, Mapping) and raw_weights:
        weights = {str(key): _safe_float(value) for key, value in raw_weights.items()}
        if not isinstance(breakdown, Mapping):
            reasons.append("A2_SCORE_BREAKDOWN_MISSING")
        elif set(breakdown) != set(weights):
            reasons.append("A2_SCORE_BREAKDOWN_INVALID")
        else:
            values = {key: _safe_float(breakdown.get(key)) for key in weights}
            if any(value < 0 or value > 100 for value in values.values()):
                reasons.append("A2_SCORE_BREAKDOWN_INVALID")
            else:
                penalties = theme.get("penalties")
                penalty_points = sum(
                    _safe_float(item.get("points"))
                    for item in penalties
                    if isinstance(item, Mapping)
                ) if isinstance(penalties, list) else 0.0
                computed = max(0.0, min(100.0, sum(values[key] * weights[key] for key in weights) + penalty_points))
                if abs(computed - _safe_float(theme.get("theme_score"))) > 0.51:
                    reasons.append("A2_THEME_SCORE_MISMATCH")
                capital_flow = snapshot_data.get("CAPITAL_FLOW_SNAPSHOT")
                if (
                    isinstance(capital_flow, Mapping)
                    and capital_flow.get("available") is not True
                    and values.get("capital_flow", 0.0) != 0.0
                ):
                    reasons.append("A2_CAPITAL_FLOW_SCORE_INVENTED")
    history = snapshot_data.get("SECTOR_CYCLE_SNAPSHOT")
    metrics = history.get("history_metrics") if isinstance(history, Mapping) else None
    if isinstance(metrics, Mapping) and metrics.get("available") is True:
        expected_overlap = _safe_float(metrics.get("top3_daily_overlap"))
        if abs(_safe_float(theme.get("rotation_overlap_ratio")) - expected_overlap) > 0.001:
            reasons.append("A2_ROTATION_OVERLAP_MISMATCH")
    return reasons


def _policy_summary(output: Mapping[str, Any], stage: str, changed: int) -> dict[str, Any]:
    return {
        "outcome": "SERVER_THRESHOLD_POLICY_APPLIED",
        "policy_demotions": changed,
        "pool_counts": _stage_pool_counts(output, stage),
    }


def _output_shape(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"type": type(value).__name__}
    raw_keys = {str(key) for key in value}
    keys = sorted(raw_keys.intersection(_SAFE_OUTPUT_FIELDS))
    envelope = value.get("envelope")
    envelope_keys = {str(key) for key in envelope} if isinstance(envelope, Mapping) else set()
    return {
        "type": "object",
        "fields": keys,
        "unknown_field_count": len(raw_keys.difference(_SAFE_OUTPUT_FIELDS)),
        "field_types": {key: type(value.get(key)).__name__ for key in keys},
        "array_lengths": {
            key: len(value.get(key))
            for key in keys
            if isinstance(value.get(key), list)
        },
        "envelope_fields": sorted(envelope_keys.intersection(_SAFE_ENVELOPE_FIELDS)),
        "envelope_unknown_field_count": len(envelope_keys.difference(_SAFE_ENVELOPE_FIELDS)),
    }


def _filter_symbol_mapping(value: Any, symbols: set[str] | None) -> Any:
    if symbols is None or not isinstance(value, Mapping):
        return value
    return {str(key): item for key, item in value.items() if str(key) in symbols}


def _project_upstream_output(value: Mapping[str, Any], symbols: set[str] | None) -> dict[str, Any]:
    if symbols is None:
        return dict(value)
    result = dict(value)
    for key in _CANDIDATE_KEYS:
        pool = result.get(key)
        if isinstance(pool, list):
            result[key] = [
                item for item in pool
                if isinstance(item, Mapping) and bool(_scan_symbols(item).intersection(symbols))
            ]
    return result


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
        "required_envelope": _required_envelope(snapshot, lane, model, stage),
        "g0_symbols": sorted(scope_symbols if scope_symbols is not None else _extract_g0(snapshot.data)),
        "upstream_symbols": sorted(upstream_symbols),
        "upstream_output": upstream_output,
        "snapshot_data": snapshot.data,
    }


def _required_envelope(
    snapshot: FrozenInputSnapshot,
    lane: str,
    model: str,
    stage: str,
) -> dict[str, Any]:
    """Build the server-owned envelope metadata for one immutable request."""

    del lane  # The lane is persisted in the outer runtime input, not the envelope.
    return {
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
    }


def _normalize_server_envelope(
    output: Mapping[str, Any],
    required_envelope: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Normalize only server-owned envelope metadata supplied by the runtime.

    The model may communicate a valid status, including an explicit BLOCKED;
    all other envelope fields come from the immutable request context.  Missing
    or malformed envelopes are left untouched so the normal validator can fail
    closed.  No discovery, mapping, or stock-pool field is created here.
    """

    result = dict(output)
    raw_envelope = output.get("envelope")
    if not isinstance(raw_envelope, Mapping) or not isinstance(required_envelope, Mapping):
        return result, 0
    # Keep model-owned envelope extensions visible to the generic permission
    # validator. Only server-owned fields are overwritten below; dropping
    # unknown fields here would turn an escalation such as
    # ``external_orders=true`` into a silently accepted response.
    normalized = dict(raw_envelope)
    normalized.update(dict(required_envelope))
    if "status" in raw_envelope:
        status = raw_envelope.get("status")
        if isinstance(status, str) and status.strip().upper() in {"OK", "DEGRADED", "BLOCKED"}:
            normalized["status"] = status.strip().upper()
        else:
            # Keep an explicit invalid status visible to the validator rather
            # than silently turning it into a successful/degraded response.
            normalized["status"] = status
    if normalized == dict(raw_envelope):
        return result, 0
    result["envelope"] = normalized
    return result, 1


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
    symbols = _candidate_pool_symbols(output)
    outside = sorted(symbols.difference(upstream_symbols))
    if outside:
        reasons.append("POOL_OUTSIDE_G0" if stage == "A1" else "POOL_OUTSIDE_UPSTREAM")
    if _unprovable_candidate_pool(output):
        reasons.append("CANDIDATE_LINEAGE_UNPROVABLE")
    # POLICY_MACRO_DISCOVERY is a semantic discovery response and deliberately
    # has no stock pools.  The presence of mappings plus the absence of pool
    # keys is the backwards-compatible marker used by checkpoint readers,
    # which do not carry the discovery context separately.
    a1_discovery_only = (
        stage == "A1"
        and isinstance(output.get("industry_theme_mappings"), list)
        and not any(key in output for key in ("active_research_pool", "monitor_pool", "rejected_candidates"))
    )
    if not a1_discovery_only:
        reasons.extend(_validate_approved_pool(output, stage))
    if stage == "A1" and not a1_discovery_only:
        reasons.extend(_validate_a1_partition(output, upstream_symbols))
    elif stage == "A2" and (snapshot_data or {}).get("STRICT_AGENT_RULES") is True:
        reasons.extend(_validate_partition(
            output,
            upstream_symbols,
            ("focus_pool", "watch_only_pool", "crowded_pool", "low_identity_pool", "rejected_candidates"),
            "A2",
        ))
    elif stage == "A3" and (snapshot_data or {}).get("STRICT_AGENT_RULES") is True:
        reasons.extend(_validate_partition(
            output,
            upstream_symbols,
            ("core_watch_pool", "secondary_watch_pool", "rejected_candidates"),
            "A3",
        ))
    if stage == "A3":
        reasons.extend(_validate_a3_provenance(output, snapshot_data or {}))
    return list(dict.fromkeys(reasons))


def _candidate_pool_symbols(output: Mapping[str, Any]) -> set[str]:
    symbols: set[str] = set()
    for key in _CANDIDATE_KEYS:
        if key in output:
            symbols.update(_scan_symbols(output.get(key)))
    return symbols


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


def _validate_a1_partition(output: Mapping[str, Any], upstream_symbols: set[str]) -> list[str]:
    reasons: list[str] = []
    pools: dict[str, set[str]] = {}
    for key in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        value = output.get(key)
        if not isinstance(value, list):
            reasons.append("A1_POOL_SCHEMA_INVALID")
            pools[key] = set()
            continue
        declared: set[str] = set()
        for item in value:
            if not isinstance(item, Mapping):
                reasons.append("A1_POOL_ITEM_INVALID")
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                reasons.append("A1_POOL_SYMBOL_INVALID")
                continue
            declared.update(scanned)
        pools[key] = declared
    if any(pools[left].intersection(pools[right]) for left, right in (
        ("active_research_pool", "monitor_pool"),
        ("active_research_pool", "rejected_candidates"),
        ("monitor_pool", "rejected_candidates"),
    )):
        reasons.append("A1_POOL_PARTITION_OVERLAP")
    covered = set().union(*pools.values())
    if covered != upstream_symbols:
        reasons.append("A1_POOL_PARTITION_INCOMPLETE")
    return reasons


def _validate_partition(
    output: Mapping[str, Any],
    upstream_symbols: set[str],
    keys: Sequence[str],
    stage: str,
) -> list[str]:
    pools: dict[str, set[str]] = {}
    reasons: list[str] = []
    for key in keys:
        value = output.get(key, [])
        if not isinstance(value, list):
            reasons.append(f"{stage}_POOL_SCHEMA_INVALID")
            pools[key] = set()
            continue
        declared: set[str] = set()
        for item in value:
            if not isinstance(item, Mapping):
                reasons.append(f"{stage}_POOL_ITEM_INVALID")
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                reasons.append(f"{stage}_POOL_SYMBOL_INVALID")
                continue
            declared.update(scanned)
        pools[key] = declared
    seen: set[str] = set()
    for key in keys:
        if seen.intersection(pools[key]):
            reasons.append(f"{stage}_POOL_PARTITION_OVERLAP")
        seen.update(pools[key])
    if seen != upstream_symbols:
        reasons.append(f"{stage}_POOL_PARTITION_INCOMPLETE")
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
        if snapshot_data.get("STRICT_AGENT_RULES") is True:
            reasons.extend(_a3_factor_contract_reasons(symbol, snapshot_data))
            scenarios = item.get("scenarios")
            required_scenarios = {
                "normal_open_plan", "weak_open_plan", "high_gap_no_chase_plan", "invalidation_plan"
            }
            if not isinstance(scenarios, Mapping) or not required_scenarios.issubset(str(key) for key in scenarios):
                reasons.append("A3_SCENARIO_SET_INCOMPLETE")
    return reasons


def _a3_factor_contract_reasons(symbol: str, snapshot_data: Mapping[str, Any]) -> list[str]:
    factors = snapshot_data.get("FACTOR_SNAPSHOT")
    factor = factors.get(symbol) if isinstance(factors, Mapping) else None
    frames = factor.get("timeframes") if isinstance(factor, Mapping) else None
    if not isinstance(factor, Mapping) or factor.get("ready") is not True or not isinstance(frames, Mapping):
        return ["A3_FACTOR_SNAPSHOT_NOT_READY"]
    reasons: list[str] = []
    for timeframe in ("weekly", "daily", "120m", "15m", "5m"):
        frame = frames.get(timeframe)
        if not isinstance(frame, Mapping) or frame.get("ready") is not True:
            reasons.append(f"A3_FACTOR_FRAME_NOT_READY:{timeframe}")
            continue
        if frame.get("ma_alignment") not in {
            "BULL_STACK", "BULL_PARTIAL", "ENTANGLED", "BEAR_PARTIAL", "BEAR_STACK"
        }:
            reasons.append(f"A3_MA_ALIGNMENT_MISSING:{timeframe}")
        if not isinstance(frame.get("ma_event"), str):
            reasons.append(f"A3_MA_EVENT_MISSING:{timeframe}")
        if not isinstance(frame.get("ma_bias"), Mapping):
            reasons.append(f"A3_MA_BIAS_MISSING:{timeframe}")
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
        factor_snapshot = snapshot_data.get("FACTOR_SNAPSHOT")
        factor = factor_snapshot.get(symbol) if isinstance(factor_snapshot, Mapping) else None
        if isinstance(factor, Mapping):
            replacements["ma_analysis"] = _canonical_ma_analysis(factor)
            replacements["factor_snapshot_hash"] = _sha256_json(factor)
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


def _canonical_ma_analysis(factor: Mapping[str, Any]) -> dict[str, Any]:
    frames = factor.get("timeframes")
    if not isinstance(frames, Mapping):
        return {}
    aliases = {"weekly": "weekly", "daily": "daily", "m120": "120m", "m15": "15m", "m5": "5m"}
    result: dict[str, Any] = {}
    for output_name, timeframe in aliases.items():
        frame = frames.get(timeframe)
        if not isinstance(frame, Mapping):
            continue
        moving = frame.get("moving_averages")
        result[output_name] = {
            **(dict(moving) if isinstance(moving, Mapping) else {}),
            "alignment": frame.get("ma_alignment"),
            "event": frame.get("ma_event"),
            "bias": frame.get("ma_bias"),
        }
    return result


def _major_trend_repair_required(symbol: str, snapshot_data: Mapping[str, Any]) -> bool:
    factors = snapshot_data.get("FACTOR_SNAPSHOT")
    factor = factors.get(symbol) if isinstance(factors, Mapping) else None
    summary = factor.get("technical_summary") if isinstance(factor, Mapping) else None
    timeframes = summary.get("timeframes") if isinstance(summary, Mapping) else None
    compact_timeframes = factor.get("timeframes") if isinstance(factor, Mapping) else None
    if not isinstance(timeframes, Mapping):
        timeframes = compact_timeframes
    if not isinstance(timeframes, Mapping):
        return False

    def below_ma255(timeframe: str) -> bool:
        payload = timeframes.get(timeframe)
        averages = payload.get("ma") if isinstance(payload, Mapping) else None
        if not isinstance(averages, Mapping) and isinstance(payload, Mapping):
            averages = payload.get("moving_averages")
        if not isinstance(payload, Mapping) or not isinstance(averages, Mapping):
            return False
        latest_close = payload.get("latest_close")
        latest = payload.get("latest")
        if latest_close is None and isinstance(latest, Mapping):
            latest_close = latest.get("close")
        return _safe_float(latest_close) < _safe_float(averages.get("ma255")) and _safe_float(
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


def _freeze_snapshot(snapshot: FrozenInputSnapshot) -> FrozenInputSnapshot:
    """Return a cheap immutable top-level snapshot view.

    ``FrozenInputSnapshot`` already freezes its top-level data mapping.  Do
    not recursively copy the full-market evidence tree for every lane/stage;
    that would defeat the cache and memory improvements this callback exists
    to provide.  Enrichers must treat nested evidence as read-only by contract.
    """

    return FrozenInputSnapshot(
        snapshot_id=snapshot.snapshot_id,
        data=snapshot.data,
        snapshot_hash=snapshot.snapshot_hash,
        as_of=snapshot.as_of,
    )


def _with_a2_bottleneck_context(
    snapshot: FrozenInputSnapshot,
    gate: DeterministicGateResult,
) -> FrozenInputSnapshot:
    """Attach the server-computed A2 scorecard inputs to the model view."""

    context: dict[str, dict[str, Any]] = {}
    for item in gate.decisions:
        symbol = str(item.get("symbol") or "")
        if not symbol or not isinstance(item.get("bottleneck_context"), Mapping):
            continue
        context[symbol] = {
            **dict(item.get("bottleneck_context") or {}),
            "eligible_routes": list(item.get("eligible_routes") or ()),
            "preferred_route": item.get("route"),
            "route_eligibility": dict(item.get("route_eligibility") or {}),
        }
    overlay_hash = _sha256_json({
        "base_snapshot_hash": snapshot.snapshot_hash,
        "stage": "A2_BOTTLENECK_CONTEXT",
        "context": context,
    })
    data = dict(snapshot.data)
    data["A2_BOTTLENECK_CONTEXT"] = context
    return FrozenInputSnapshot(
        snapshot_id=f"{snapshot.snapshot_id}:a2-bottleneck:{overlay_hash[:12]}",
        data=data,
        snapshot_hash=overlay_hash,
        as_of=snapshot.as_of,
    )


def _progress_status(audit: StageAudit) -> str:
    if audit.status == STATUS_NOT_RUN_UPSTREAM_BLOCKED:
        return "NOT_RUN"
    if not _stage_completed(audit.status):
        return "FAILED"
    diagnostics = audit.diagnostics if isinstance(audit.diagnostics, Mapping) else {}
    return "REUSED" if diagnostics.get("checkpoint_reused") is True else "COMPLETED"


def _safe_progress_reason_codes(values: Sequence[str] | None) -> list[str]:
    """Bound progress reason codes before handing them to a user callback."""

    if values is None:
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


def _safe_progress_diagnostics(value: Any) -> dict[str, Any]:
    """Expose bounded shape/count diagnostics, never model payload text."""

    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    shape = value.get("last_invalid_output_shape")
    if isinstance(shape, Mapping):
        safe_shape: dict[str, Any] = {}
        shape_type = shape.get("type")
        if isinstance(shape_type, str):
            safe_shape["type"] = shape_type[:40]
        fields = shape.get("fields")
        if isinstance(fields, list):
            safe_shape["fields"] = [
                field for field in fields
                if isinstance(field, str) and field in _SAFE_OUTPUT_FIELDS
            ][:20]
        for key in ("unknown_field_count", "envelope_unknown_field_count"):
            raw = shape.get(key)
            if isinstance(raw, int) and not isinstance(raw, bool):
                safe_shape[key] = max(0, raw)
        if safe_shape:
            result["last_invalid_output_shape"] = safe_shape
    for key in (
        "semantic_attempts",
        "theme_count",
        "node_count",
        "mapping_count",
        "expected_mapping_count",
    ):
        raw = value.get(key)
        if isinstance(raw, int) and not isinstance(raw, bool):
            result[key] = max(0, raw)
    missing = value.get("missing_mapping_codes")
    if isinstance(missing, (list, tuple, set, frozenset)):
        result["missing_mapping_count"] = sum(
            isinstance(item, str) and bool(item.strip()) for item in missing
        )
    return result


def _discovery_progress_diagnostics(
    diagnostics: Any,
    monthly_context: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert safe output-shape lengths into durable discovery counters."""

    result = dict(diagnostics) if isinstance(diagnostics, Mapping) else {}
    invalid_shape = result.get("last_invalid_output_shape")
    array_lengths = invalid_shape.get("array_lengths") if isinstance(invalid_shape, Mapping) else None
    if isinstance(array_lengths, Mapping):
        for diagnostic_key, output_key in (
            ("theme_count", "structural_themes"),
            ("node_count", "industry_chain_graph"),
            ("mapping_count", "industry_theme_mappings"),
        ):
            if output_key in array_lengths:
                result.setdefault(diagnostic_key, _safe_int(array_lengths.get(output_key)))
    canonical_decisions = monthly_context.get("monthly_industry_decisions")
    if isinstance(canonical_decisions, list):
        result.setdefault(
            "expected_mapping_count",
            sum(
                1
                for item in canonical_decisions
                if isinstance(item, Mapping)
                and str(item.get("base_decision") or item.get("decision") or "").strip().upper() == "INCLUDE"
            ),
        )
    return result


def _stage_completed(status: Any) -> bool:
    """Return whether a stage produced a complete, self-contained outcome."""

    return str(status or "") in _COMPLETED_STAGE_STATUSES


def _stage_checkpointable(status: Any) -> bool:
    """Only successful terminal outcomes may be reused after a restart."""

    return _stage_completed(status)


def _lane_status_from_stage(status: Any) -> str:
    """Map a terminal A3 outcome to the lane publication state."""

    text = str(status or "")
    if text == STATUS_VALIDATED_UNDERFILLED_MARKET:
        return "READY_DEGRADED"
    return "READY" if _stage_completed(text) else "BLOCKED"


def _lane_status_from_stages(stages: Sequence[StageAudit]) -> str:
    """Expose degraded/data-gap outcomes at lane level."""

    statuses = tuple(str(stage.status or "") for stage in stages)
    if any(status == STATUS_DEGRADED_UNDERFILLED_DATA_GAP for status in statuses):
        return "READY_DEGRADED"
    if any(status == STATUS_VALIDATED_UNDERFILLED_MARKET for status in statuses):
        return "READY_DEGRADED"
    return "READY" if statuses and all(_stage_completed(status) for status in statuses) else "BLOCKED"


def _progress_status_for_stage_status(status: Any) -> str:
    """Use a progress vocabulary that preserves no-op vs failure semantics."""

    text = str(status or "")
    if text == STATUS_NOT_RUN_UPSTREAM_BLOCKED:
        return "NOT_RUN"
    if not _stage_completed(text):
        return "FAILED"
    if text in {
        STATUS_VALIDATED_NO_OPPORTUNITY,
        STATUS_VALIDATED_NO_ACTION,
        STATUS_VALIDATED_NO_SETUP,
        STATUS_VALIDATED_UNDERFILLED_MARKET,
        STATUS_DEGRADED_UNDERFILLED_DATA_GAP,
    }:
        return text
    return "COMPLETED"


def _classify_stage_outcome(
    stage: str,
    output: Mapping[str, Any],
    *,
    reasons: Sequence[str],
    gate: Any | None = None,
    reviewed_output: Mapping[str, Any] | None = None,
) -> tuple[str, tuple[str, ...]]:
    """Derive detailed terminal semantics without changing model conclusions.

    ``output`` may include deterministic gate rows that are appended after the
    model review.  For A2, only evidence-gap codes in ``reviewed_output`` are
    attributable to the model review and may block a zero-focus result.  The
    optional argument keeps direct callers and legacy tests backwards
    compatible; production downstream review passes the pre-append view.
    """

    validation_reasons = tuple(dict.fromkeys(str(item) for item in reasons if str(item)))
    if validation_reasons:
        if stage == "A1" and (
            "A1_ACTIVE_COVERAGE_UNDERFILLED" in validation_reasons
            or any(reason.startswith("A1_MONTHLY_") for reason in validation_reasons)
        ):
            return STATUS_BLOCKED_DATA_COVERAGE, ()
        if stage == "A2" and set(validation_reasons).intersection(_A2_EVIDENCE_GAP_REASONS):
            return STATUS_DEGRADED_UNDERFILLED_DATA_GAP, tuple(
                sorted(set(validation_reasons).intersection(_A2_EVIDENCE_GAP_REASONS))
            )
        if stage == "A3" and set(validation_reasons).intersection(_A3_TECHNICAL_DATA_REASONS):
            return STATUS_BLOCKED_TECHNICAL_DATA, ()
        return "BLOCKED", ()

    if stage == "A1":
        if not _approved_symbols(output, "A1"):
            return STATUS_VALIDATED_NO_OPPORTUNITY, ("A1_NO_ACTIVE_RESEARCH",)
        return STATUS_VALIDATED, ()

    if stage == "A2":
        focus_count = len(output.get("focus_pool")) if isinstance(output.get("focus_pool"), list) else 0
        targets = output.get("analysis_summary")
        targets = targets if isinstance(targets, Mapping) else {}
        target = targets.get("pool_target")
        minimum = (
            _safe_int(target.get("minimum"))
            if isinstance(target, Mapping)
            else 30
        )
        minimum = max(1, minimum or 30)
        review_view = reviewed_output if reviewed_output is not None else output
        gap_reasons = _output_reason_codes(review_view).intersection(_A2_EVIDENCE_GAP_REASONS)
        gate_summary = gate.summary if isinstance(gate, DeterministicGateResult) else {}
        if gate_summary.get("data_sufficiency_state") == "INSUFFICIENT":
            gate_gaps = _gate_reason_codes(gate).intersection(_A2_EVIDENCE_GAP_REASONS)
            gap_reasons.update(gate_gaps or {"A2_CRITICAL_DATA_INSUFFICIENT"})
        if focus_count == 0:
            if gap_reasons:
                return STATUS_DEGRADED_UNDERFILLED_DATA_GAP, tuple(sorted(gap_reasons))
            return STATUS_VALIDATED_NO_OPPORTUNITY, ("A2_NO_FOCUS_OPPORTUNITY",)
        if focus_count < minimum:
            if gap_reasons:
                return STATUS_DEGRADED_UNDERFILLED_DATA_GAP, tuple(sorted(gap_reasons))
            return STATUS_VALIDATED_UNDERFILLED_MARKET, ("A2_FOCUS_POOL_UNDERFILLED_MARKET",)
        return STATUS_VALIDATED, ()

    if stage == "A3" and not _approved_symbols(output, "A3"):
        gate_reasons = _gate_reason_codes(gate)
        if gate_reasons.intersection(_A3_TECHNICAL_DATA_REASONS):
            return STATUS_BLOCKED_TECHNICAL_DATA, tuple(sorted(gate_reasons.intersection(_A3_TECHNICAL_DATA_REASONS)))
        return STATUS_VALIDATED_NO_SETUP, ("A3_NO_TECHNICAL_SETUP",)
    return STATUS_VALIDATED, ()


def _output_reason_codes(output: Mapping[str, Any]) -> set[str]:
    """Collect only explicit reason-code fields from bounded model output."""

    result: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            raw = value.get("reason_codes")
            if isinstance(raw, str):
                raw = [raw]
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                result.update(str(item).strip().upper() for item in raw if isinstance(item, str) and item.strip())
            for key, item in value.items():
                if str(key) in {"analysis_summary", "active_themes", "focus_pool", "watch_only_pool", "core_watch_pool", "secondary_watch_pool", "rejected_candidates"}:
                    visit(item)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for item in value:
                visit(item)

    visit(output)
    return result


def _gate_reason_codes(gate: Any | None) -> set[str]:
    result: set[str] = set()
    decisions = getattr(gate, "decisions", ()) if gate is not None else ()
    for decision in decisions:
        if not isinstance(decision, Mapping):
            continue
        raw = decision.get("reason_codes")
        if isinstance(raw, str):
            raw = [raw]
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            result.update(str(item).strip().upper() for item in raw if isinstance(item, str) and item.strip())
    return result


def _invoke_stage_snapshot_enricher(
    callback: Callable[..., Any],
    *,
    stage: str,
    lane_id: str,
    model: str,
    upstream_symbols: frozenset[str],
    snapshot: FrozenInputSnapshot,
) -> Any:
    """Call an enricher using the richest compatible signature.

    The primary contract is keyword-friendly ``(stage, lane_id, model,
    upstream_symbols, snapshot)``.  The shorter positional variants are
    retained for small adapters and tests.
    """

    keyword_candidates = (
        {
            "stage": stage,
            "lane_id": lane_id,
            "model": model,
            "upstream_symbols": upstream_symbols,
            "snapshot": snapshot,
        },
        {
            "stage": stage,
            "lane": lane_id,
            "model": model,
            "upstream_symbols": upstream_symbols,
            "snapshot": snapshot,
        },
        {"stage": stage, "lane_id": lane_id, "upstream_symbols": upstream_symbols, "snapshot": snapshot},
        {"stage": stage, "lane": lane_id, "upstream_symbols": upstream_symbols, "snapshot": snapshot},
    )
    positional_candidates = (
        (stage, lane_id, model, upstream_symbols, snapshot),
        (stage, lane_id, upstream_symbols, snapshot),
        (stage, upstream_symbols, snapshot),
    )
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(stage, lane_id, model, upstream_symbols, snapshot)
    for kwargs in keyword_candidates:
        try:
            signature.bind(**kwargs)
        except TypeError:
            continue
        return callback(**kwargs)
    for args in positional_candidates:
        try:
            signature.bind(*args)
        except TypeError:
            continue
        return callback(*args)
    raise TypeError("stage snapshot enricher signature is unsupported")


__all__ = [
    "FrozenInputSnapshot",
    "FileResearchCheckpointStore",
    "InMemoryResearchCheckpointStore",
    "LaneResult",
    "ResearchPipeline",
    "ResearchCheckpointKey",
    "ResearchCheckpointStore",
    "ResearchPipelineError",
    "ResearchRunResult",
    "StageAudit",
]
