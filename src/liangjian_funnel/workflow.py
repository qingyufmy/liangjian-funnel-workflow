"""End-to-end orchestration for the standalone shadow/simulation workflow."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Event, RLock, Thread
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .data.cache import MinuteBarStore
from .data.a2_market import (
    collect_eastmoney_board_flow,
    collect_eastmoney_capital_flow,
    collect_tencent_capital_flow,
    unavailable_capital_flow_snapshot,
    with_capital_flow_provider_attempts,
)
from .data.bse import BseClient
from .data.cninfo import CninfoAnnouncement, CninfoClient, CninfoFetchResult
from .data.cninfo_pdf import CninfoPdfClient, CninfoPdfEvidence
from .data.gov_policy import GovPolicyClient
from .data.mootdx import MootdxAdapter, MootdxNode, MinuteBar, detect_missing_bars, map_symbol
from .data.tencent_minute import ResilientIntradayAdapter, TencentIntradayAdapter
from .data.open_news import OpenNewsClient, OpenNewsFetchResult
from .data.open_macro import OpenMacroDataCollector
from .data.ths_industry import (
    collect_ths_industry_history,
    collect_ths_industry_membership,
    select_industry_diversified_symbols,
)
from .data.ths_taxonomy import collect_ths_taxonomy_membership
from .facts import (
    FactStore,
    collect_market_results,
    compact_cninfo_pdf_evidence,
    manifest_projection,
    merge_fact_manifests,
    normalize_cninfo_results,
    normalize_gov_policy_result,
    normalize_hithink_results,
    normalize_open_news_results,
    select_cninfo_pdf_candidates,
)
from .evaluation.broker_gold import BrokerGoldContractError, evaluate_broker_gold, import_broker_gold
from .pipeline.data_source import HithinkClient, HithinkFetchResult
from .pipeline.data_readiness import evaluate_data_readiness
from .pipeline.data_sync import HithinkIncrementalSynchronizer
from .pipeline.a2_features import build_a2_feature_snapshot
from .pipeline.a1_sources import (
    A1SourceRegistryError,
    build_a1_source_context,
    load_a1_source_registry,
    unavailable_a1_source_context,
)
from .pipeline.a1_registry import (
    A1_FULL,
    A1_INCREMENTAL,
    DEFAULT_A1_DEGRADED_AFTER,
    DEFAULT_A1_MAX_AGE,
    A1Generation,
    A1Registry,
    A1RegistryError,
    build_a1_manifest,
    compute_incremental_scope,
    default_a1_registry_path,
    merge_a1_partitions,
)
from .pipeline.factors import FactorEngine
from .pipeline.feature_store import ResearchFeatureStore
from .pipeline.feature_maintenance import materialize_live_source
from .pipeline.local_fact_cache import LocalFactCache
from .pipeline.market_aggregates import (
    build_crowding_snapshot,
    build_market_emotion,
    build_news_heat_snapshot,
    build_sector_health_snapshot,
    build_sector_cycle_and_permissions,
)
from .pipeline.model_client import ModelCallResult, OpenAICompatibleModelClient
from .pipeline.prompts import PromptRepository
from .pipeline.research import FrozenInputSnapshot as ResearchSnapshot
from .pipeline.research import ResearchPipeline, ResearchRunResult
from .pipeline.research_checkpoint import FileResearchCheckpointStore
from .pipeline.research_consensus import (
    load_research_consensus,
    project_a2_research_hypotheses,
    unavailable_research_consensus,
)
from .pipeline.research_reports import write_stage_markdown_reports
from .pipeline.snapshot import FrozenInputSnapshot, UniverseGatePolicy, UniverseSnapshot
from .pipeline.technical_aggregates import build_technical_aggregates
from .redaction import digest_text, sanitize
from .reporting import atomic_write_json, atomic_write_json_streaming, atomic_write_text
from .runtime.monitor import MonitorBatchResult, MonitorEngine, rebuild_effective_markdown
from .runtime.progress import WorkflowProgress
from .runtime.resource_guard import evaluate_resources, measure_resources
from .runtime.calendar import ExchangeTradingCalendar, TradingCalendarError
from .runtime.scheduler import ScheduleKind, Scheduler
from .runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from .runtime.state import MonitorAction, PlanStatus, RuntimeStore
from .settings import Settings, load_yaml


SHANGHAI = ZoneInfo("Asia/Shanghai")
_A4_FILE = "agent_4_intraday_veto_v3.txt"
_G0_SCOPE_CONTRACT = "CONFIGURED_RESEARCH_UNIVERSE_V1"
_RESEARCH_RESUME_SCHEMA = "liangjian-research-resume/1.2.0"
_A1_MAX_AGE = DEFAULT_A1_MAX_AGE
_A1_DEGRADED_AFTER = DEFAULT_A1_DEGRADED_AFTER
_A1_MAINTENANCE_TIME = (18, 0)
_COMPARISON_REQUEST_SCHEMA = "liangjian-comparison-request/1.0.0"
_COMPARISON_REQUEST_STATUSES = frozenset({"PENDING", "RUNNING", "RETRYABLE", "SUCCEEDED", "FAILED", "CANCELLED"})
_COMPARISON_RETRYABLE_STATUSES = frozenset({"PENDING", "RETRYABLE", "RUNNING"})
_COMPARISON_OWNER_STALE_SECONDS = 15 * 60
_CNINFO_PDF_PERMANENT_FAILURES = frozenset(
    {
        "CNINFO_PDF_URL_REJECTED",
        "CNINFO_PDF_REDIRECT_REJECTED",
        "CNINFO_PDF_HTTP_4XX",
        "CNINFO_PDF_CONTENT_TYPE_INVALID",
        "CNINFO_PDF_MAGIC_INVALID",
        "CNINFO_PDF_TOO_LARGE",
        "CNINFO_PDF_ENCRYPTED",
        "CNINFO_PDF_TEXT_EMPTY",
        "CNINFO_PDF_PARSE_FAILED",
    }
)
_CNINFO_PDF_TRANSIENT_FAILURES = frozenset(
    {
        "CNINFO_PDF_RATE_LIMITED",
        "CNINFO_PDF_HTTP_5XX",
        "CNINFO_PDF_REQUEST_FAILED",
        "CNINFO_PDF_TIMEOUT",
        "CNINFO_PDF_BSE_CDN_BLOCKED",
    }
)


class WorkflowError(RuntimeError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class PreparedSnapshot:
    snapshot: ResearchSnapshot
    path: Path
    full_universe_count: int
    research_universe_count: int
    trade_universe_count: int
    selected_count: int
    factor_ready_count: int
    feature_source: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot.snapshot_id,
            "snapshot_hash": self.snapshot.snapshot_hash,
            "path": str(self.path),
            "full_universe_count": self.full_universe_count,
            "research_universe_count": self.research_universe_count,
            "trade_universe_count": self.trade_universe_count,
            "selected_count": self.selected_count,
            "factor_ready_count": self.factor_ready_count,
            "feature_source": dict(self.feature_source or {}),
        }


@dataclass(frozen=True, slots=True)
class A1MaintenancePlan:
    """One due 18:00 A1 maintenance slot."""

    mode: str
    trade_date: date
    due: datetime
    dispatch_key: str
    reason_code: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "trade_date": self.trade_date.isoformat(),
            "due": self.due.isoformat(),
            "dispatch_key": self.dispatch_key,
            "reason_code": self.reason_code,
        }


def decide_a1_maintenance(
    now: datetime,
    trading_day: Any,
    *,
    has_active_generation: bool = True,
    active_full_period: str | None = None,
) -> A1MaintenancePlan | None:
    """Return the bootstrap/monthly/weekly A1 slot, if due."""

    current = _aware(now)
    if current.hour < _A1_MAINTENANCE_TIME[0] or (
        current.hour == _A1_MAINTENANCE_TIME[0] and current.minute < _A1_MAINTENANCE_TIME[1]
    ):
        return None
    try:
        if not bool(trading_day(current.date())):
            return None
        first = date(current.year, current.month, 1)
        while first.month == current.month and not trading_day(first):
            first += timedelta(days=1)
        is_first = current.date() == first
        # The next Monday is exclusive.  No later session means this is the
        # last exchange session of the current week (including holiday weeks).
        days_to_monday = 7 - current.weekday()
        next_day = current.date() + timedelta(days=1)
        next_monday = current.date() + timedelta(days=days_to_monday)
        is_last_week_session = not any(
            bool(trading_day(next_day + timedelta(days=offset)))
            for offset in range(max(0, (next_monday - next_day).days))
        )
    except Exception as exc:
        raise WorkflowError("TRADING_CALENDAR_UNAVAILABLE") from exc
    if not has_active_generation:
        due = current.replace(hour=18, minute=0, second=0, microsecond=0)
        return A1MaintenancePlan(
            mode=A1_FULL,
            trade_date=current.date(),
            due=due,
            dispatch_key=f"a1-maintenance:{current.date().isoformat()}:bootstrap-full",
            reason_code="A1_BOOTSTRAP_FULL_DUE",
        )
    current_period = f"{current.year:04d}-{current.month:02d}"
    if active_full_period is not None and str(active_full_period).strip() != current_period:
        due = current.replace(hour=18, minute=0, second=0, microsecond=0)
        return A1MaintenancePlan(
            mode=A1_FULL,
            trade_date=current.date(),
            due=due,
            dispatch_key=f"a1-maintenance:{current.date().isoformat()}:monthly-full-catchup",
            reason_code="A1_MONTHLY_FULL_CATCHUP_DUE",
        )
    if not (is_first or is_last_week_session):
        return None
    mode = A1_FULL if is_first else A1_INCREMENTAL
    reason = "A1_MONTHLY_FULL_DUE" if is_first else "A1_WEEKLY_INCREMENTAL_DUE"
    due = current.replace(hour=18, minute=0, second=0, microsecond=0)
    return A1MaintenancePlan(
        mode=mode,
        trade_date=current.date(),
        due=due,
        dispatch_key=f"a1-maintenance:{current.date().isoformat()}:{mode.lower()}",
        reason_code=reason,
    )


def _primary_model_for_settings(settings: Settings) -> tuple[str, int, str]:
    """Resolve the configured primary model and its stable lane index."""

    lane_id = str(getattr(settings, "research_primary_lane_id", "lane_1") or "lane_1")
    try:
        lane_index = int(lane_id.rsplit("_", 1)[-1])
    except (TypeError, ValueError):
        lane_index = 1
        lane_id = "lane_1"
    models = tuple(getattr(settings, "research_models", ()) or ())
    model_index = lane_index - 1
    if not (0 <= model_index < len(models)):
        model_index = 0
        lane_index = 1
        lane_id = "lane_1"
    if not models:
        raise WorkflowError("RESEARCH_MODEL_CONFIG_INVALID")
    return str(models[model_index]), lane_index, lane_id


def _a1_full_period(generation: A1Generation | None) -> str | None:
    if generation is None:
        return None
    explicit = str(generation.manifest.get("last_full_period") or "").strip()
    if explicit:
        return explicit
    if generation.mode == A1_FULL:
        return f"{generation.as_of.year:04d}-{generation.as_of.month:02d}"
    # An older incremental generation without this marker cannot prove that
    # the current month's mandatory FULL was published. Fail safe by making
    # the next 18:00 wake-up a monthly catch-up.
    return "UNKNOWN"


class WorkflowApplication:
    """Join data, research, A4 and paper simulation without external orders."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = RuntimeStore(settings.state_db_path)
        self.minute_store = MinuteBarStore(settings.minute_cache_dir)
        self.fact_cache = LocalFactCache(settings.fact_cache_db_path)
        # A1 maintenance has an independent immutable registry.  The feature
        # store remains a deterministic projection cache and must not be used
        # as the close workflow's A1 source of truth.
        self.a1_registry = A1Registry(default_a1_registry_path(settings))
        # The feature store is a rebuildable projection cache.  Data sync only
        # marks symbols dirty after a successful provider write; maintenance
        # publishes a new generation separately, so a partial sync cannot
        # contaminate the active research generation.
        self.feature_store = ResearchFeatureStore(settings.feature_store_db_path)
        self.fact_synchronizer = HithinkIncrementalSynchronizer(
            self.fact_cache,
            fundamental_refresh_hours=settings.fundamental_refresh_hours,
            fundamental_refresh_symbols_per_run=settings.fundamental_refresh_symbols_per_run,
            daily_refresh_hours=settings.daily_refresh_hours,
            progress_every=settings.data_progress_every,
            batch_size=settings.data_sync_batch_size,
        )
        self._stage_technical_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._stage_technical_lock = RLock()
        self.research_checkpoints = FileResearchCheckpointStore(settings.research_checkpoint_dir)
        self.prompts = PromptRepository(settings.prompt_dir)
        self.model_client = OpenAICompatibleModelClient(
            settings,
            max_attempts=8,
            thinking_enabled=settings.research_thinking_enabled,
        )
        self.monitor_model_client = OpenAICompatibleModelClient(
            settings.model_copy(
                update={
                    "model_timeout_seconds": 45.0,
                    "model_max_output_tokens": 2_048,
                    "model_fallback_output_tokens": 1_024,
                    "model_secondary_fallback_output_tokens": 1_024,
                }
            ),
            max_attempts=2,
            thinking_enabled=settings.monitor_thinking_enabled,
        )
        self.trading_calendar = ExchangeTradingCalendar()
        mootdx = MootdxAdapter(
            tuple(MootdxNode(host=host, port=port) for host, port in settings.mootdx_servers),
            page_size=settings.mootdx_page_size,
            max_pages=settings.mootdx_max_pages,
            timeout_seconds=settings.mootdx_timeout_seconds,
        )
        self.market_data = ResilientIntradayAdapter(
            mootdx,
            TencentIntradayAdapter(timeout_seconds=min(12.0, settings.timeout_seconds)),
        )
        # Compatibility alias for existing probes/tests.  All workflow call
        # sites now receive the same primary/fallback behavior.
        self.mootdx = self.market_data
        self.brokers = {
            f"lane_{index}": PaperBroker(
                self.store,
                account_id=f"paper:lane_{index}",
                model=model,
                config=SimulationConfig(initial_cash=settings.simulation_initial_cash),
            )
            for index, model in enumerate(settings.research_models, start=1)
        }

    def prepare_snapshot(
        self,
        *,
        as_of: datetime | None = None,
        market_data_as_of: datetime | None = None,
        progress: WorkflowProgress | None = None,
    ) -> PreparedSnapshot:
        current = _aware(as_of or datetime.now(SHANGHAI))
        market_current = _aware(market_data_as_of or current)
        wall_now = datetime.now(SHANGHAI)
        if abs((current - wall_now).total_seconds()) > 600:
            raise WorkflowError("LIVE_FACTS_POINT_IN_TIME_UNSUPPORTED")
        if market_current > current:
            raise WorkflowError("MARKET_DATA_AS_OF_IN_FUTURE")

        source_config = load_yaml(self.settings.source_config_path)
        gate_config = source_config.get("universe_gate", {})
        if not isinstance(gate_config, Mapping):
            raise WorkflowError("UNIVERSE_GATE_CONFIG_INVALID")
        gate_policy = UniverseGatePolicy(
            minimum_daily_turnover_cny=gate_config.get("minimum_daily_turnover_cny", 0),
            newly_listed_min_days=gate_config.get("newly_listed_min_days", 0),
            block_suspended=gate_config.get("block_suspended", False),
            block_no_price_limit_new_listing=gate_config.get("block_no_price_limit_new_listing", False),
        )
        a1_config = source_config.get("agent_1", {})
        if not isinstance(a1_config, Mapping):
            raise WorkflowError("A1_CONFIG_INVALID")
        top_n_per_node = int(a1_config.get("top_n_per_node", 8))
        node_count_target = a1_config.get("node_count_target", [40, 80])
        if not isinstance(node_count_target, list):
            raise WorkflowError("A1_NODE_TARGET_INVALID")
        source_failures: dict[str, list[str]] = {}
        if progress is not None:
            progress.set_phase("UNIVERSE_SYNC")
            _progress_stdout(progress.snapshot())
        with HithinkClient(self.settings) as client:
            catalog = client.ticker_catalog(limit=1000, max_pages=10)
            market = client.market_snapshot(limit=1000, max_pages=10)
            if current.hour < 15:
                market = _market_snapshot_with_closed_turnover(
                    market,
                    cache=self.fact_cache,
                    cutoff=current.replace(hour=0, minute=0, second=0, microsecond=0),
                )
            universe = UniverseSnapshot.from_records(
                catalog,
                market,
                as_of=current,
                gate_policy=gate_policy,
            )
            if not universe.ready:
                raise WorkflowError("UNIVERSE_NOT_READY")
            if progress is not None:
                progress.update_data(
                    processed=0,
                    total=len(universe.research_candidates),
                    cache_hits=0,
                    cache_misses=0,
                    failures=0,
                )
                _progress_stdout(progress.snapshot())

            # A1 is node-first. Build the full THS reverse membership graph
            # before ordering companies. Industry nodes provide deterministic
            # grouping and intra-node turnover order. The formal research call
            # uses the configured quality-filtered research universe. Beijing
            # securities remain research-only after passing the same quality
            # gate; execution permission is enforced at plan publication.
            research_records = _research_universe_records(universe)
            # THS taxonomy rows do not carry row-level effective timestamps.
            # Bind their event time to the requested frozen cutoff while
            # retaining the later HTTP fetch_time for provenance. Otherwise a
            # normal multi-second request is misclassified as future data.
            industry_catalog = _bind_reference_fact_event_time(
                client.ths_index_catalog(tag="industry"),
                as_of=current,
            )
            concept_catalog = _bind_reference_fact_event_time(
                client.ths_index_catalog(tag="cn_concept"),
                as_of=current,
            )
            full_membership = collect_ths_industry_membership(
                client,
                industry_catalog,
                [candidate.symbol for candidate in research_records],
                cache_dir=self.settings.fact_store_dir / "ths_industry",
                as_of=current,
            )
            if not full_membership.ok or not full_membership.complete:
                raise WorkflowError(f"THS_INDUSTRY_MEMBERSHIP_NOT_READY:{full_membership.reason_code}")

            # Freeze the complete deterministic G0 research universe. Industry
            # membership controls ordering and grouping only; it must not act
            # as a performance cap for the formal workflow.
            selection_limit = len(research_records)
            selected_symbols, g0_selection = select_industry_diversified_symbols(
                research_records,
                full_membership,
                limit=selection_limit,
                top_n_per_node=top_n_per_node,
                node_count_target=node_count_target,
            )
            if set(selected_symbols) != {candidate.symbol for candidate in research_records}:
                raise WorkflowError("A1_RESEARCH_UNIVERSE_SELECTION_INCOMPLETE")
            record_by_symbol = {candidate.symbol: candidate for candidate in research_records}
            selected = tuple(record_by_symbol[symbol] for symbol in selected_symbols)
            if progress is not None:
                progress.set_phase("MARKET_FACT_SYNC")
                _progress_stdout(progress.snapshot())
            market_fact_results = collect_market_results(
                client,
                [candidate.symbol for candidate in selected] if _auction_window(current) else [],
            )
            market_fact_results["THS_INDUSTRY_CATALOG"] = industry_catalog
            market_fact_results["THS_CONCEPT_CATALOG"] = concept_catalog
            market_fact_results["THS_INDUSTRY_HISTORY"] = collect_ths_industry_history(
                client,
                industry_catalog,
                cache_dir=self.settings.fact_store_dir / "ths_industry",
                as_of=current,
            )
            market_fact_results["THS_INDUSTRY_MEMBERSHIP"] = collect_ths_industry_membership(
                client,
                industry_catalog,
                [candidate.symbol for candidate in selected],
                cache_dir=self.settings.fact_store_dir / "ths_industry",
                as_of=current,
            )
            market_fact_results["THS_CONCEPT_MEMBERSHIP"] = collect_ths_taxonomy_membership(
                client,
                concept_catalog,
                [candidate.symbol for candidate in selected],
                taxonomy="concept",
                cache_dir=self.settings.fact_store_dir / "ths_taxonomy",
                as_of=current,
            )
            if not market_fact_results["THS_INDUSTRY_HISTORY"].ok:
                raise WorkflowError(
                    "THS_INDUSTRY_HISTORY_NOT_READY:"
                    f"{market_fact_results['THS_INDUSTRY_HISTORY'].reason_code}"
                )
            required_market_facts = (
                "LIMIT_UP_POOL",
                "LIMIT_DOWN_POOL",
                "LIMIT_BREAK_POOL",
                "LIMIT_UP_LADDER",
            )
            if any(
                not market_fact_results[name].ok or not market_fact_results[name].complete
                for name in required_market_facts
            ):
                raise WorkflowError("MARKET_EMOTION_FACTS_NOT_READY")
            if _auction_window(current):
                auction = market_fact_results.get("AUCTION_FINAL")
                if auction is None or not auction.ok or not auction.complete:
                    raise WorkflowError("AUCTION_FACTS_NOT_READY")
            hithink_manifest = normalize_hithink_results(
                market_fact_results,
                base_url=self.settings.hithink_base_url,
                as_of=market_current,
            )
            market_current = _advance_live_market_cutoff(
                market_as_of=market_current,
                research_as_of=current,
                included_fact_as_of=hithink_manifest.as_of,
            )
            current = max(current, hithink_manifest.as_of)
            def sync_progress(event: Mapping[str, Any]) -> None:
                if progress is None:
                    return
                progress.update_data(
                    processed=int(event.get("processed") or 0),
                    total=int(event.get("total") or len(selected)),
                    cache_hits=int(event.get("cache_hits") or 0),
                    cache_misses=int(event.get("cache_misses") or 0),
                    failures=int(event.get("failures") or 0),
                    current_symbol=str(event.get("current_symbol") or "") or None,
                    daily_updates=int(event.get("daily_updates") or 0),
                    financial_refreshes=int(event.get("financial_refreshes") or 0),
                    deferred_financial_refreshes=int(
                        event.get("deferred_financial_refreshes") or 0
                    ),
                )
                _progress_stdout(progress.snapshot())

            sync_result = self.fact_synchronizer.sync(
                client,
                [candidate.symbol for candidate in selected],
                as_of=market_current,
                lookback_days=800,
                compact_daily_bars=30,
                fundamental_projector=_compact_fundamental_rows,
                progress=sync_progress,
            )
            for symbol, reasons in sync_result.failures.items():
                source_failures.setdefault(symbol, []).extend(reasons)
            daily: dict[str, Any] = sync_result.daily
            fundamental: dict[str, Any] = sync_result.fundamental
            cache_coverage = self.fact_cache.get_coverage(as_of=market_current)
            data_readiness = evaluate_data_readiness(
                {
                    "daily": {
                        **dict(cache_coverage.get("daily") or {}),
                        "symbols": len(daily),
                    },
                    "financial": {
                        **dict(cache_coverage.get("financial") or {}),
                        "symbols": len(fundamental),
                    },
                },
                expected_symbols=len(selected),
                as_of=market_current,
            )
            if not data_readiness.ready:
                raise WorkflowError(data_readiness.reason_codes[0] or "RESEARCH_DATA_NOT_READY")
            technical: dict[str, Any] = {}

        # A1 is a structural macro/policy layer. A six-day window only shows
        # incidental recent notices and cannot support policy lifecycle or
        # a 90-day catalyst calendar.
        policy_start = (current.date() - timedelta(days=90)).isoformat()
        policy_end = current.date().isoformat()
        with GovPolicyClient(
            timeout_seconds=self.settings.timeout_seconds,
            base_url=self.settings.gov_policy_base_url,
            min_request_interval_seconds=self.settings.gov_policy_min_request_interval_seconds,
        ) as policy_client:
            policy_result = policy_client.fetch_documents(policy_start, policy_end)
        if not policy_result.ok or not policy_result.complete:
            raise WorkflowError(f"GOV_POLICY_FACTS_NOT_READY:{policy_result.reason_code}")
        policy_manifest = normalize_gov_policy_result(policy_result, as_of=current)

        query_start = (current.date() - timedelta(days=10)).isoformat()
        business_query_start = (current.date() - timedelta(days=450)).isoformat()
        query_end = current.date().isoformat()
        cninfo_results: dict[str, Any] = {}
        cninfo_hits = 0
        cninfo_misses = 0
        if progress is not None:
            progress.set_phase("CNINFO_SYNC")
            _progress_stdout(progress.snapshot())
        with CninfoClient(
            timeout_seconds=self.settings.timeout_seconds,
            base_url=self.settings.cninfo_base_url,
            min_request_interval_seconds=self.settings.cninfo_min_request_interval_seconds,
        ) as cninfo, BseClient(
            timeout_seconds=self.settings.timeout_seconds,
            min_request_interval_seconds=self.settings.cninfo_min_request_interval_seconds,
        ) as bse:
            # Each candidate is one independent unit containing the recent and
            # business-history queries.  The shared client owns the global
            # request throttle, so workers hide network latency without
            # creating a per-thread request burst.
            query_futures = {}
            with ThreadPoolExecutor(max_workers=self.settings.cninfo_workers) as executor:
                for index, candidate in enumerate(selected):
                    future = executor.submit(
                        self._fetch_cninfo_candidate_queries,
                        cninfo,
                        candidate.symbol,
                        query_start,
                        query_end,
                        business_query_start,
                        bse_client=bse,
                    )
                    query_futures[future] = index
                completed_queries: dict[
                    int,
                    tuple[str, CninfoFetchResult, bool, CninfoFetchResult, bool],
                ] = {}
                completed_count = 0
                for future in as_completed(query_futures):
                    index = query_futures[future]
                    completed_queries[index] = future.result()
                    completed_count += 1
                    symbol, recent_result, recent_hit, business_result, business_hit = completed_queries[index]
                    cninfo_hits += int(recent_hit) + int(business_hit)
                    cninfo_misses += int(not recent_hit) + int(not business_hit)
                    if progress is not None and (
                        completed_count == len(selected)
                        or completed_count % self.settings.data_progress_every == 0
                    ):
                        completed_query_failures = sum(
                            int(
                                not recent.ok
                                or not recent.complete
                                or not business.ok
                                or not business.complete
                            )
                            for _, recent, _, business, _ in completed_queries.values()
                        )
                        progress.update_data(
                            processed=completed_count,
                            total=len(selected),
                            cache_hits=cninfo_hits,
                            cache_misses=cninfo_misses,
                            failures=(
                                sum(1 for reasons in source_failures.values() if reasons)
                                + completed_query_failures
                            ),
                            current_symbol=symbol,
                        )
                        _progress_stdout(progress.snapshot())

            # Rebuild all maps and P0 failure side effects in candidate order;
            # completion order must never affect snapshot bytes or failure
            # reason ordering.
            for index in range(len(selected)):
                symbol, recent_result, recent_hit, business_result, business_hit = completed_queries[index]
                result = _merge_cninfo_query_results(recent_result, business_result)
                cninfo_results[symbol] = result
                disclosure_source = "BSE" if symbol.upper().endswith(".BJ") else "CNINFO"
                if not recent_result.ok or not recent_result.complete:
                    source_failures.setdefault(symbol, []).append(
                        f"{disclosure_source}:{recent_result.reason_code}"
                    )
                if not business_result.ok or not business_result.complete:
                    source_failures.setdefault(symbol, []).append(
                        f"{disclosure_source}_MAIN_BUSINESS:{business_result.reason_code}"
                    )
        # Freeze the complete PDF work list before opening the client.  The
        # candidate selector is deterministic, so this gives the control
        # plane an honest document-level total instead of leaving the prior
        # CNINFO announcement-query counters on screen while PDF work runs.
        pdf_tasks = _build_cninfo_pdf_tasks(
            cninfo_results,
            limit=self.settings.cninfo_pdf_max_documents_per_symbol,
        )
        unique_pdf_tasks = _deduplicate_cninfo_pdf_tasks(pdf_tasks)
        pdf_evidence_by_index: dict[int, CninfoPdfEvidence] = {}
        pdf_cache_hits = 0
        pdf_cache_misses = 0
        pdf_documents_succeeded = 0
        pdf_documents_failed = 0
        pdf_progress_last_at = 0.0
        if progress is not None:
            progress.set_phase("CNINFO_PDF_SYNC")
            progress.update_data(
                processed=0,
                total=len(unique_pdf_tasks),
                cache_hits=0,
                cache_misses=0,
                failures=0,
                documents_succeeded=0,
                documents_failed=0,
            )
            _progress_stdout(progress.snapshot())
        if unique_pdf_tasks:
            with CninfoPdfClient(
                self.settings.cninfo_pdf_cache_dir,
                timeout_seconds=self.settings.timeout_seconds,
            ) as pdf_client:
                # Read all evidence rows in one connection/query set.  Only
                # valid cache hits enter the completion set; misses are the
                # only documents submitted to the bounded worker pool.
                cache_keys = [announcement.announcement_id for _, announcement in unique_pdf_tasks]
                index_by_id = {
                    announcement.announcement_id: index
                    for index, (_, announcement) in enumerate(unique_pdf_tasks)
                }
                for identifier, cached in self.fact_cache.iter_cached_results(
                    "CNINFO_PDF_EVIDENCE",
                    cache_keys,
                    fresh_at=datetime.now(SHANGHAI),
                    chunk_size=100,
                ):
                    index = index_by_id[identifier]
                    _, announcement = unique_pdf_tasks[index]
                    cached_evidence = self._cached_cninfo_pdf_evidence_from_record(
                        announcement,
                        cached,
                    )
                    if cached_evidence is not None:
                        pdf_evidence_by_index[index] = compact_cninfo_pdf_evidence(cached_evidence)
                pending: list[tuple[int, str, CninfoAnnouncement]] = [
                    (index, symbol, announcement)
                    for index, (symbol, announcement) in enumerate(unique_pdf_tasks)
                    if index not in pdf_evidence_by_index
                ]

                pdf_cache_hits = len(pdf_evidence_by_index)
                pdf_cache_misses = len(pending)
                pdf_documents_succeeded = sum(
                    int(evidence.available) for evidence in pdf_evidence_by_index.values()
                )
                pdf_documents_failed = pdf_cache_hits - pdf_documents_succeeded
                latest_cached = unique_pdf_tasks[max(pdf_evidence_by_index)] if pdf_evidence_by_index else (None, None)
                if progress is not None and pdf_cache_hits:
                    progress.update_data(
                        processed=pdf_cache_hits,
                        total=len(unique_pdf_tasks),
                        cache_hits=pdf_cache_hits,
                        cache_misses=pdf_cache_misses,
                        failures=pdf_documents_failed,
                        current_symbol=latest_cached[0],
                        current_document=(
                            latest_cached[1].announcement_id if latest_cached[1] is not None else None
                        ),
                        documents_succeeded=pdf_documents_succeeded,
                        documents_failed=pdf_documents_failed,
                    )
                    _progress_stdout(progress.snapshot())
                    pdf_progress_last_at = time.monotonic()

                with ThreadPoolExecutor(max_workers=self.settings.cninfo_pdf_workers) as executor:
                    pdf_futures = {
                        executor.submit(
                            self._fetch_and_cache_cninfo_pdf_evidence,
                            pdf_client,
                            announcement,
                        ): (index, symbol, announcement)
                        for index, symbol, announcement in pending
                    }
                    for future in as_completed(pdf_futures):
                        index, symbol, announcement = pdf_futures[future]
                        try:
                            evidence = future.result()
                        except Exception:
                            # A worker-level error is isolated to its
                            # document, but remains visible as a stable
                            # source failure instead of being silently lost.
                            evidence = self._cninfo_pdf_worker_failure(announcement)
                        pdf_evidence_by_index[index] = compact_cninfo_pdf_evidence(evidence)
                        if evidence.available:
                            pdf_documents_succeeded += 1
                        else:
                            pdf_documents_failed += 1
                        completed = len(pdf_evidence_by_index)
                        progress_now = time.monotonic()
                        if progress is not None and (
                            completed == len(unique_pdf_tasks)
                            or progress_now - pdf_progress_last_at >= 2.0
                        ):
                            progress.update_data(
                                processed=completed,
                                total=len(unique_pdf_tasks),
                                cache_hits=pdf_cache_hits,
                                cache_misses=pdf_cache_misses,
                                failures=pdf_documents_failed,
                                current_symbol=symbol,
                                current_document=announcement.announcement_id,
                                documents_succeeded=pdf_documents_succeeded,
                                documents_failed=pdf_documents_failed,
                            )
                            _progress_stdout(progress.snapshot())
                            pdf_progress_last_at = progress_now

        # Keep one bounded in-memory projection per deduplicated document.
        # Complete extraction objects remain durable in SQLite/the file cache.
        evidence_by_id = {
            unique_pdf_tasks[index][1].announcement_id: pdf_evidence_by_index[index]
            for index in range(len(unique_pdf_tasks))
        }
        pdf_ids_by_symbol: dict[str, list[str]] = {}
        for symbol, announcement in pdf_tasks:
            pdf_ids_by_symbol.setdefault(symbol, []).append(announcement.announcement_id)
            evidence = evidence_by_id[announcement.announcement_id]
            if not evidence.available:
                source_failures.setdefault(symbol, []).append(
                    f"CNINFO_PDF:{announcement.announcement_id}:{evidence.reason_code}"
                )
        if progress is not None:
            progress.set_phase("FACT_MANIFEST_SYNC")
            _progress_stdout(progress.snapshot())
        cninfo_manifest = normalize_cninfo_results(
            cninfo_results,
            pdf_evidence_by_id=evidence_by_id,
            pdf_ids_by_symbol=pdf_ids_by_symbol,
            as_of=current,
        )
        # Market-wide/RSS news is frozen once. Stock-specific T3 news belongs
        # to A2 and is fetched only for that lane's A1-approved subset.
        news_results = self._collect_open_news([])
        news_manifest = normalize_open_news_results(news_results, as_of=current)
        for source_id, result in news_results.items():
            if result.ok and result.complete:
                continue
            if source_id.startswith("open_news.eastmoney_stock."):
                symbol_key = source_id.rsplit(".", 1)[-1].replace("_", ".").upper()
                source_failures.setdefault(symbol_key, []).append(f"OPEN_NEWS:{result.reason_code}")
        fact_manifest = merge_fact_manifests((hithink_manifest, policy_manifest, cninfo_manifest, news_manifest))
        current = max(current, fact_manifest.as_of)
        fact_store = FactStore(self.settings.fact_store_dir)
        fact_manifest_path = fact_store.write_manifest(fact_manifest)
        fact_payload = manifest_projection(fact_manifest)
        fact_payload["store_relative_path"] = str(
            fact_manifest_path.relative_to(self.settings.fact_store_dir)
        )
        fact_payload["data_readiness"] = data_readiness.as_dict()
        if progress is not None:
            progress.set_phase("OPEN_MACRO_SYNC")
            _progress_stdout(progress.snapshot())
        if self.settings.open_macro_enabled:
            try:
                open_macro_bundle = OpenMacroDataCollector(
                    cache_dir=self.settings.open_macro_cache_dir,
                ).collect(current)
            except (OSError, RuntimeError, TypeError, ValueError):
                # Supplemental open sources may degrade, but must never erase
                # the authoritative THS/CNINFO/government facts already frozen.
                open_macro_bundle = {
                    "schema_version": "open-macro-contract/1.0.0",
                    "as_of": current.isoformat(),
                    "available": False,
                    "reason_code": "OPEN_MACRO_COLLECTION_FAILED",
                }
        else:
            open_macro_bundle = {
                "schema_version": "open-macro-contract/1.0.0",
                "as_of": current.isoformat(),
                "available": False,
                "reason_code": "SOURCE_DISABLED",
            }
        # This payload is included before FrozenInputSnapshot.freeze, so the
        # contracts and their source manifest participate in the facts hash.
        fact_payload["open_macro_bundle"] = open_macro_bundle

        if progress is not None:
            progress.set_phase("SNAPSHOT")
            _progress_stdout(progress.snapshot())
        frozen = FrozenInputSnapshot.freeze(
            universe,
            as_of=current,
            daily_payload=daily,
            fundamental_payload=fundamental,
            fact_payload=fact_payload,
            max_candidates=selection_limit,
            candidate_symbols=selected_symbols,
            candidate_domain="research",
            retain_incomplete=True,
        )
        accepted_symbols = {candidate.symbol for candidate in frozen.g0_candidates}
        g0_symbols = [symbol for symbol in selected_symbols if symbol in accepted_symbols]
        if not g0_symbols:
            raise WorkflowError("NO_FUNDAMENTAL_READY_CANDIDATES")
        g0_records = [record_by_symbol[symbol] for symbol in g0_symbols]
        g0_order, final_g0_selection = select_industry_diversified_symbols(
            g0_records,
            full_membership,
            limit=len(g0_records),
            top_n_per_node=top_n_per_node,
            node_count_target=node_count_target,
        )
        if set(g0_order) != set(g0_symbols):
            raise WorkflowError("A1_FUNDAMENTAL_READY_NODE_LINEAGE_INVALID")
        g0_symbols = list(g0_order)

        # A3 technical enrichment is intentionally deferred until A2 has
        # produced its per-lane focus pool.  Loading 12,240 five-minute bars
        # for the complete G0 here would violate the funnel boundary.
        factor_ready: list[str] = []
        # ``frozen`` already contains the same daily, fundamental, technical
        # and fact payloads.  Re-freezing it here used to duplicate the full
        # market object tree and recompute all checksums immediately before
        # serialization, which materially raised the VM peak RSS.
        raw_path = self.settings.snapshot_dir / "raw" / f"{frozen.snapshot_id}.json"
        frozen.write_json(raw_path)
        data = sanitize(self._research_input(
            frozen=frozen,
            universe=universe,
            technical=technical,
            g0_symbols=g0_symbols,
            source_failures=source_failures,
            g0_selection={
                **final_g0_selection,
                "selection_strategy": g0_selection.get("strategy"),
                "research_universe_selected_count": len(selected_symbols),
                "selected_count": len(g0_symbols),
                "factor_ready_symbols": factor_ready,
                "factor_ready_count": len(factor_ready),
                "technical_readiness_is_a3_only": True,
            },
            raw_snapshot_path=raw_path,
            as_of=current,
            market_data_as_of=market_current,
        ))
        snapshot_hash = _hash_json(data)
        snapshot_id = f"snapshot-{current.strftime('%Y%m%dT%H%M%S%z')}-{snapshot_hash[:12]}"
        snapshot = ResearchSnapshot(
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot_hash,
            as_of=current,
            data=data,
        )
        path = self.settings.snapshot_dir / f"{snapshot_id}.json"
        atomic_write_json_streaming(
            path,
            {
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_hash": snapshot.snapshot_hash,
                "as_of": current.isoformat(),
                "data": data,
            },
        )
        if progress is not None:
            progress.set_phase("FEATURE_SOURCE_GENERATION")
            progress.update_resources(measure_resources(self.settings.root).as_dict())
            _progress_stdout(progress.snapshot())
        if self.settings.feature_maintenance_enabled:
            try:
                feature_source = materialize_live_source(
                    self.feature_store,
                    snapshot_id=snapshot.snapshot_id,
                    snapshot_hash=snapshot.snapshot_hash,
                    as_of=current,
                    market_trade_date=market_current.date().isoformat(),
                    data=data,
                    batch_size=self.settings.feature_source_batch_size,
                ).as_dict()
            except Exception as exc:
                # Source materialisation is a maintenance-plane result.  The
                # already frozen research snapshot remains valid, while the
                # maintenance capability fails closed and reports a stable
                # reason instead of rerunning from the large JSON artifact.
                feature_source = {
                    "status": "BLOCKED_SOURCE_GENERATION",
                    "generation_id": None,
                    "snapshot_id": snapshot.snapshot_id,
                    "snapshot_hash": snapshot.snapshot_hash,
                    "market_trade_date": market_current.date().isoformat(),
                    "reason_code": getattr(exc, "reason_code", type(exc).__name__.upper()),
                }
        else:
            feature_source = {
                "status": "DISABLED",
                "generation_id": None,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_hash": snapshot.snapshot_hash,
                "market_trade_date": market_current.date().isoformat(),
                "reason_code": "FEATURE_MAINTENANCE_DISABLED",
            }
        if progress is not None:
            progress.update_resources(measure_resources(self.settings.root).as_dict())
            _progress_stdout(progress.snapshot())
        return PreparedSnapshot(
            snapshot=snapshot,
            path=path,
            full_universe_count=len(universe.records),
            research_universe_count=len(universe.research_candidates),
            trade_universe_count=len(universe.trade_candidates),
            selected_count=len(g0_symbols),
            factor_ready_count=len(factor_ready),
            feature_source=feature_source,
        )

    def run_next_session_prep(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Prepare one clean close run for the nearest future session.

        This is the only supported path for a weekend/holiday pre-open run.
        Facts are collected at the wall-clock timestamp supplied by the caller,
        while the execution horizon is explicitly bound to the next exchange
        session.  It never starts a paper trading day, performs historical
        replay, accepts a persisted snapshot id, or queues comparison lanes.
        """

        source_as_of = _aware(now or datetime.now(SHANGHAI))
        try:
            if self.trading_calendar.is_trading_day(source_as_of.date()):
                raise WorkflowError("NEXT_SESSION_PREP_REQUIRES_NON_TRADING_DAY")
            target_trade_date = self.trading_calendar.next_trading_day(source_as_of.date())
            market_trade_date = self.trading_calendar.previous_trading_day(source_as_of.date())
        except TradingCalendarError as exc:
            raise WorkflowError(exc.reason_code) from exc
        market_data_as_of = datetime(
            market_trade_date.year,
            market_trade_date.month,
            market_trade_date.day,
            15,
            10,
            tzinfo=SHANGHAI,
        )

        # One deterministic receipt per target session makes a second command
        # invocation safe: it cannot publish a second pending plan set for the
        # same morning review.  A blocked/incomplete run has no receipt and may
        # be retried through the same entry point.
        run_id = f"next-session-prep-{target_trade_date.isoformat()}"
        summary_path = self.settings.workflow_output_dir / "runs" / f"{run_id}.json"
        try:
            existing = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, TypeError):
            existing = None
        if (
            isinstance(existing, Mapping)
            and existing.get("preparation_mode") == "NEXT_SESSION_PRODUCTION_PREP"
            and str(existing.get("status") or "").upper() in {"READY", "READY_DEGRADED"}
        ):
            raise WorkflowError("NEXT_SESSION_PREP_ALREADY_COMPLETED")

        return self.run_research(
            "close",
            as_of=source_as_of,
            historical_replay=False,
            snapshot_id=None,
            primary_only=True,
            schedule_comparison=False,
            publish_plans=True,
            run_id_override=run_id,
            target_trade_date=target_trade_date,
            market_data_as_of=market_data_as_of,
            allow_non_trading_source=True,
            reuse_resume_snapshot=False,
            from_active_a1=True,
        )

    def sync_data_cache(self, *, as_of: datetime | None = None) -> dict[str, Any]:
        """Bootstrap or incrementally refresh reusable daily/fundamental facts."""

        current = _aware(as_of or datetime.now(SHANGHAI))
        progress = WorkflowProgress(
            self.settings.workflow_progress_path,
            run_id=f"data-sync-{current.strftime('%Y%m%dT%H%M%S%z')}",
            job="data_sync",
        )
        progress.set_phase("UNIVERSE_SYNC")
        _progress_stdout(progress.snapshot())
        try:
            source_config = load_yaml(self.settings.source_config_path)
            raw_gate = source_config.get("universe_gate", {})
            if not isinstance(raw_gate, Mapping):
                raise WorkflowError("UNIVERSE_GATE_CONFIG_INVALID")
            gate = UniverseGatePolicy(
                minimum_daily_turnover_cny=raw_gate.get("minimum_daily_turnover_cny", 0),
                newly_listed_min_days=raw_gate.get("newly_listed_min_days", 0),
                block_suspended=raw_gate.get("block_suspended", False),
                block_no_price_limit_new_listing=raw_gate.get("block_no_price_limit_new_listing", False),
            )
            with HithinkClient(self.settings) as client:
                universe = UniverseSnapshot.from_records(
                    client.ticker_catalog(limit=1000, max_pages=10),
                    client.market_snapshot(limit=1000, max_pages=10),
                    as_of=current,
                    gate_policy=gate,
                )
                if not universe.ready:
                    raise WorkflowError("UNIVERSE_NOT_READY")
                symbols = [candidate.symbol for candidate in _research_universe_records(universe)]

                def on_progress(event: Mapping[str, Any]) -> None:
                    progress.update_data(
                        processed=int(event.get("processed") or 0),
                        total=int(event.get("total") or len(symbols)),
                        cache_hits=int(event.get("cache_hits") or 0),
                        cache_misses=int(event.get("cache_misses") or 0),
                        failures=int(event.get("failures") or 0),
                        current_symbol=str(event.get("current_symbol") or "") or None,
                    )
                    _progress_stdout(progress.snapshot())

                progress.set_phase("DATA_SYNC")
                result = self.fact_synchronizer.sync(
                    client,
                    symbols,
                    as_of=current,
                    lookback_days=800,
                    compact_daily_bars=30,
                    progress=on_progress,
                )
            coverage = self.fact_cache.get_coverage(as_of=current)
            readiness = evaluate_data_readiness(
                coverage,
                expected_symbols=len(symbols),
                as_of=current,
            )
            updated_symbols = tuple(result.updated_symbols)
            dirty_source_version = f"HITHINK_FACTS_{current.isoformat()}"
            for symbol in updated_symbols:
                self.feature_store.mark_dirty(
                    entity_type="STOCK",
                    entity_id=symbol,
                    reason_code="FACT_UPDATE",
                    source_version=dirty_source_version,
                    created_at=current,
                    priority=10,
                )
            summary = {
                "status": (
                    "BLOCKED" if not readiness.ready
                    else "READY" if readiness.status == "READY" and not result.failures
                    else "PARTIAL"
                ),
                "as_of": current.isoformat(),
                "universe_count": len(universe.records),
                "catalog_universe_count": len(universe.records),
                "data_universe_count": len(symbols),
                "research_universe_count": len(universe.research_candidates),
                "trade_universe_count": len(universe.trade_candidates),
                "processed": result.processed,
                "cache_hits": result.cache_hits,
                "cache_misses": result.cache_misses,
                "updated_symbols_count": len(updated_symbols),
                "updated_symbols": list(updated_symbols),
                "feature_dirty_marked_count": len(updated_symbols),
                "failure_count": len(result.failures),
                "coverage": coverage,
                "readiness": readiness.as_dict(),
            }
            output = self.settings.workflow_output_dir / "data_sync" / f"{current.strftime('%Y%m%dT%H%M%S%z')}.json"
            atomic_write_json(output, summary)
            progress.finish(
                status=str(summary["status"]),
                phase=(
                    "DATA_BLOCKED" if not readiness.ready
                    else "DATA_READY" if summary["status"] == "READY"
                    else "DATA_PARTIAL"
                ),
                reason_code=(
                    readiness.reason_codes[0] if not readiness.ready
                    else None if summary["status"] == "READY"
                    else "SYMBOL_DATA_PARTIAL"
                ),
            )
            _progress_stdout(progress.snapshot())
            return {**summary, "report": str(output)}
        except Exception as exc:
            progress.finish(status="BLOCKED", phase="FAILED", reason_code=_safe_reason_code(exc))
            _progress_stdout(progress.snapshot())
            raise

    def _cached_cninfo_result(
        self,
        client: CninfoClient | BseClient,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        semantic_key: str,
        ttl: timedelta,
        search_keyword: str | None = None,
    ) -> tuple[CninfoFetchResult, bool]:
        cache_key = f"{symbol}:{semantic_key}"
        now = datetime.now(SHANGHAI)
        cached = self.fact_cache.get_cached_result(
            "CNINFO_ANNOUNCEMENTS",
            cache_key,
            fresh_at=now,
        )
        if cached is not None:
            try:
                return CninfoFetchResult.model_validate(cached["payload"]), True
            except Exception:
                pass
        result = client.fetch_announcements(
            symbol,
            start_date,
            end_date,
            search_keyword=search_keyword or "",
        )
        if result.ok and result.complete:
            self.fact_cache.put_cached_result(
                "CNINFO_ANNOUNCEMENTS",
                cache_key,
                result.model_dump(mode="json"),
                fetched_at=result.fetched_at,
                expires_at=result.fetched_at + ttl,
            )
        return result, False

    def _fetch_cninfo_candidate_queries(
        self,
        cninfo_client: CninfoClient,
        symbol: str,
        query_start: str,
        query_end: str,
        business_query_start: str,
        *,
        bse_client: BseClient | None = None,
    ) -> tuple[str, CninfoFetchResult, bool, CninfoFetchResult, bool]:
        """Fetch both official disclosure-query lanes for one candidate."""

        client: CninfoClient | BseClient = cninfo_client
        if symbol.upper().endswith(".BJ") and bse_client is not None:
            client = bse_client

        recent_result, recent_hit = self._cached_cninfo_result(
            client,
            symbol=symbol,
            start_date=query_start,
            end_date=query_end,
            semantic_key="RECENT_10D",
            ttl=timedelta(hours=6),
        )
        business_result, business_hit = self._cached_cninfo_result(
            client,
            symbol=symbol,
            start_date=business_query_start,
            end_date=query_end,
            semantic_key="ANNUAL_REPORT_450D",
            ttl=timedelta(days=7),
            search_keyword="年度报告",
        )
        return symbol, recent_result, recent_hit, business_result, business_hit

    def _cached_cninfo_pdf_evidence(
        self,
        client: CninfoPdfClient,
        announcement: Any,
    ) -> CninfoPdfEvidence:
        cache_key = str(announcement.announcement_id)
        cached = self.fact_cache.get_cached_result(
            "CNINFO_PDF_EVIDENCE",
            cache_key,
            fresh_at=datetime.now(SHANGHAI),
        )
        cached_evidence = self._cached_cninfo_pdf_evidence_from_record(announcement, cached)
        if cached_evidence is not None:
            return cached_evidence
        evidence = client.fetch_evidence(announcement)
        self._persist_cninfo_pdf_evidence(evidence)
        return evidence

    def _cached_cninfo_pdf_evidence_from_record(
        self,
        announcement: CninfoAnnouncement,
        cached: Mapping[str, Any] | None,
    ) -> CninfoPdfEvidence | None:
        """Validate one bulk-loaded PDF evidence row using the old cache rules."""

        if cached is None:
            return None
        try:
            evidence = CninfoPdfEvidence.model_validate(cached["payload"])
            if evidence.announcement_id != announcement.announcement_id:
                return None
            if evidence.available:
                raw_path = (
                    self.settings.cninfo_pdf_cache_dir / str(evidence.cache_relative_path)
                ).resolve()
                root = self.settings.cninfo_pdf_cache_dir.resolve()
                if not (
                    not self.settings.cninfo_pdf_retain_raw
                    or (
                        raw_path.is_relative_to(root)
                        and raw_path.is_file()
                        and raw_path.stat().st_size == evidence.byte_size
                    )
                ):
                    return None
                if not self.settings.cninfo_pdf_retain_raw:
                    self._prune_cninfo_pdf_raw(evidence)
            # Failed evidence is intentionally a cache hit too.  The caller
            # still records its reason in source_failures, but avoids retrying
            # a known permanent (or short-lived transient) failure until TTL.
            return evidence.model_copy(update={"cache_hit": True})
        except (OSError, TypeError, ValueError, KeyError):
            return None

    def _fetch_and_cache_cninfo_pdf_evidence(
        self,
        client: CninfoPdfClient,
        announcement: CninfoAnnouncement,
    ) -> CninfoPdfEvidence:
        """Fetch one known cache miss and persist the evidence from its worker."""

        evidence = client.fetch_evidence(announcement)
        self._persist_cninfo_pdf_evidence(evidence)
        return evidence

    def _persist_cninfo_pdf_evidence(self, evidence: CninfoPdfEvidence) -> None:
        """Persist successful evidence and bounded-TTL deterministic failures."""

        if evidence.available:
            ttl = timedelta(days=3650)
        elif evidence.reason_code in _CNINFO_PDF_PERMANENT_FAILURES:
            ttl = timedelta(days=7)
        elif evidence.reason_code in _CNINFO_PDF_TRANSIENT_FAILURES:
            ttl = timedelta(minutes=15)
        else:
            # Parser availability and cache/provider implementation errors
            # should not be retried in a hot loop, but are short-lived.
            ttl = timedelta(minutes=15)
        self.fact_cache.put_cached_result(
            "CNINFO_PDF_EVIDENCE",
            str(evidence.announcement_id),
            evidence.model_dump(mode="json"),
            fetched_at=evidence.fetched_at,
            expires_at=evidence.fetched_at + ttl,
        )
        if not self.settings.cninfo_pdf_retain_raw:
            self._prune_cninfo_pdf_raw(evidence)

    @staticmethod
    def _cninfo_pdf_worker_failure(announcement: CninfoAnnouncement) -> CninfoPdfEvidence:
        """Create a redacted, stable outcome for an isolated worker exception."""

        return CninfoPdfEvidence(
            announcement_id=announcement.announcement_id,
            pdf_url=announcement.pdf_url,
            available=False,
            reason_code="CNINFO_PDF_WORKER_FAILED",
            fetched_at=datetime.now(SHANGHAI),
        )

    def _prune_cninfo_pdf_raw(self, evidence: CninfoPdfEvidence) -> None:
        """Remove re-downloadable PDF bytes after durable evidence extraction."""

        if not evidence.cache_relative_path:
            return
        root = self.settings.cninfo_pdf_cache_dir.resolve()
        raw_path = (root / evidence.cache_relative_path).resolve()
        metadata_path = (root / "metadata" / f"{raw_path.stem}.json").resolve()
        for path, expected_parent in (
            (raw_path, root / "raw"),
            (metadata_path, root / "metadata"),
        ):
            try:
                if path.parent == expected_parent.resolve():
                    path.unlink(missing_ok=True)
            except OSError:
                # Evidence has already been persisted. A leftover raw cache is
                # safe and can be pruned by a later run.
                continue

    def _collect_open_news(
        self,
        symbols: list[str],
        *,
        include_market: bool = True,
    ) -> dict[str, OpenNewsFetchResult]:
        """Collect independent media sources without making them P0 gates."""

        try:
            config = json.loads(self.settings.news_source_config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            config = {}
        raw_sources = config.get("sources") if isinstance(config, Mapping) else None
        rss_sources = [
            item
            for item in (raw_sources or ())
            if isinstance(item, Mapping)
            and item.get("type") == "rss"
            and isinstance(item.get("url"), str)
            and item.get("url")
        ]
        fetch_config = config.get("fetch") if isinstance(config, Mapping) else None
        rss_page_size = 6
        if isinstance(fetch_config, Mapping):
            value = fetch_config.get("per_source")
            if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 50:
                rss_page_size = value

        results: dict[str, OpenNewsFetchResult] = {}
        with OpenNewsClient(timeout_seconds=self.settings.open_news_timeout_seconds) as client:
            if include_market:
                flash = client.fetch_cls_roll(page_size=self.settings.open_news_flash_limit)
                results[flash.source_id] = flash
                global_news = client.fetch_eastmoney_7x24(page_size=self.settings.open_news_flash_limit)
                results[global_news.source_id] = global_news
            for symbol in symbols:
                cache_key = f"{symbol}:STOCK_NEWS"
                now = datetime.now(SHANGHAI)
                cached = self.fact_cache.get_cached_result(
                    "OPEN_NEWS",
                    cache_key,
                    fresh_at=now,
                )
                item: OpenNewsFetchResult
                if cached is not None:
                    try:
                        item = OpenNewsFetchResult.model_validate(cached["payload"])
                    except Exception:
                        cached = None
                if cached is None:
                    item = client.fetch_eastmoney_stock_news(
                        symbol,
                        page_size=self.settings.open_news_stock_limit,
                    )
                    if item.ok and item.complete:
                        self.fact_cache.put_cached_result(
                            "OPEN_NEWS",
                            cache_key,
                            item.model_dump(mode="json"),
                            fetched_at=item.fetched_at,
                            expires_at=item.fetched_at + timedelta(hours=2),
                        )
                results[item.source_id] = item

            def fetch_rss(source: Mapping[str, Any]) -> OpenNewsFetchResult:
                url = str(source["url"])
                hint = str(source.get("hint") or "rss").strip().lower() or "rss"
                digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
                result = client.fetch_rss(
                    url,
                    source_id=f"open_news.rss.{hint}.{digest}",
                    channel=hint,
                    page_size=rss_page_size,
                )
                metadata = dict(result.metadata)
                metadata.update({
                    "source_name": str(source.get("name") or "")[:120],
                    "industry_hint": hint,
                    "catalog": "Vibe-Research/backend/news_sources.json",
                })
                return result.model_copy(update={"metadata": metadata})

            with ThreadPoolExecutor(max_workers=self.settings.open_news_rss_workers) as executor:
                futures = [
                    executor.submit(fetch_rss, source)
                    for source in (rss_sources if include_market else [])
                ]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception:
                        # Provider methods are fail-closed; this guard only
                        # keeps an unexpected worker failure from blocking
                        # official facts and the remaining news channels.
                        continue
                    results[result.source_id] = result
        return results

    def run_a1_maintenance(
        self,
        *,
        now: datetime | None = None,
        mode: str | None = None,
        run_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        """Build and atomically publish the monthly/weekly A1 generation."""

        current = _aware(now or datetime.now(SHANGHAI))
        normalized_mode = str(mode or "").strip().upper()
        active = self.a1_registry.get_active_generation()
        plan: A1MaintenancePlan | None = None
        if not normalized_mode:
            plan = decide_a1_maintenance(
                current,
                self.trading_calendar.is_trading_day,
                has_active_generation=active is not None,
                active_full_period=_a1_full_period(active),
            )
            if plan is None:
                return {
                    "status": "NOT_DUE",
                    "reason_code": "A1_MAINTENANCE_NOT_DUE",
                    "time": current.isoformat(),
                }
            normalized_mode = plan.mode
        if normalized_mode not in {A1_FULL, A1_INCREMENTAL}:
            raise WorkflowError("A1_MAINTENANCE_MODE_INVALID")
        if normalized_mode == A1_INCREMENTAL and active is None:
            return {
                "status": "BLOCKED",
                "mode": normalized_mode,
                "reason_code": "A1_INCREMENTAL_BASELINE_MISSING",
                "time": current.isoformat(),
            }
        maintenance_run_id = run_id or f"{current.date()}-a1-{normalized_mode.lower()}"
        progress = WorkflowProgress(
            self.settings.workflow_progress_path,
            run_id=maintenance_run_id,
            job="a1",
        )
        progress.set_phase("DATA_SYNC")
        resource_decision = evaluate_resources(self.settings.root)
        progress.update_resources(resource_decision.snapshot.as_dict())
        _progress_stdout(progress.snapshot())
        if not resource_decision.allowed:
            reason = resource_decision.reason_codes[0]
            progress.finish(status="BLOCKED", phase="FAILED", reason_code=reason)
            _progress_stdout(progress.snapshot())
            return {
                "status": "FAILED",
                "mode": normalized_mode,
                "reason_code": reason,
                "time": current.isoformat(),
            }
        try:
            if snapshot_id is not None:
                progress.set_phase("SNAPSHOT_REUSE")
                prepared = self._load_research_snapshot_by_id(
                    snapshot_id,
                    expected_date=current.date().isoformat(),
                )
                progress.update_data(
                    processed=prepared.selected_count,
                    total=prepared.selected_count,
                    cache_hits=prepared.selected_count,
                    cache_misses=0,
                    failures=0,
                )
                _progress_stdout(progress.snapshot())
            else:
                prepared = self.prepare_snapshot(
                    as_of=current,
                    market_data_as_of=current,
                    progress=progress,
                )
        except Exception as exc:
            reason = _safe_reason_code(exc)
            progress.finish(status="BLOCKED", phase="FAILED", reason_code=reason)
            _progress_stdout(progress.snapshot())
            return {
                "status": "FAILED",
                "mode": normalized_mode,
                "reason_code": reason,
                "time": current.isoformat(),
            }
        base_payload = dict(active.payload) if active is not None else {}
        base_outputs = _a1_outputs_by_lane(base_payload)
        scope = None
        maintenance_scope_symbols: tuple[str, ...] | None = None
        macro_revalidation_symbols: tuple[str, ...] = ()
        if normalized_mode == A1_INCREMENTAL:
            if active is None or not isinstance(active.manifest, Mapping):
                return {
                    "status": "BLOCKED",
                    "mode": normalized_mode,
                    "reason_code": "A1_INCREMENTAL_BASELINE_INVALID",
                    "time": current.isoformat(),
                }
            scope = compute_incremental_scope(
                prepared.snapshot.data,
                active.manifest,
                base_output=next(iter(base_outputs.values()), None),
                changed_theme_ids=prepared.snapshot.data.get("A1_CHANGED_THEME_IDS"),
                new_theme_ids=prepared.snapshot.data.get("A1_NEW_THEME_IDS"),
            )
            if not scope.symbols and not scope.global_input_changed:
                return {
                    "status": "NOOP",
                    "mode": normalized_mode,
                    "reason_code": "A1_INCREMENTAL_NO_CHANGES",
                    "base_generation_id": active.generation_id if active else None,
                    "delta": scope.as_dict(),
                    "snapshot_id": prepared.snapshot.snapshot_id,
                    "snapshot_hash": prepared.snapshot.snapshot_hash,
                }
            maintenance_scope = set(scope.symbols)
            if scope.global_input_changed:
                # A weekly policy/macro change must revalidate the existing
                # research and monitor pools, even when no company's
                # low-frequency facts changed.  Rejected outsiders remain a
                # monthly-full responsibility; this keeps the weekly job a
                # genuine delta instead of silently widening back to G0.
                current_g0 = {
                    str(symbol).strip().upper()
                    for symbol in prepared.snapshot.data.get("g0_symbols", ())
                    if str(symbol).strip()
                }
                prior_research = set()
                for output in base_outputs.values():
                    prior_research.update(
                        _a1_output_partition_symbols(
                            output,
                            ("active_research_pool", "monitor_pool"),
                        )
                    )
                macro_revalidation_symbols = tuple(sorted(prior_research.intersection(current_g0)))
                maintenance_scope.update(macro_revalidation_symbols)
            maintenance_scope_symbols = tuple(sorted(maintenance_scope))
        primary_model, primary_lane_index, primary_lane_id = _primary_model_for_settings(self.settings)
        generation = self.a1_registry.create_generation(
            mode=normalized_mode,
            snapshot_id=prepared.snapshot.snapshot_id,
            snapshot_hash=prepared.snapshot.snapshot_hash,
            as_of=current,
            base_generation_id=active.generation_id if active is not None else None,
            manifest={
                "schema_version": "liangjian-a1-registry/1.0.0",
                "status": "STAGING",
                "mode": normalized_mode,
                "snapshot_id": prepared.snapshot.snapshot_id,
            },
            payload={"schema_version": "liangjian-a1-registry/1.0.0", "lanes": {}},
        )
        try:
            def research_progress(event: Mapping[str, Any]) -> None:
                progress.research_event(event)
                _progress_stdout(progress.snapshot())

            pipeline = ResearchPipeline(
                self.settings,
                prompt_repository=self.prompts,
                model_client=self.model_client,
                output_dir=self.settings.workflow_output_dir / "research",
                parallel_lanes=False,
                runtime_store=self.store,
                slot="A1_MAINTENANCE",
                batch_workers=1,
                checkpoint_store=self.research_checkpoints,
                stage_snapshot_enricher=self._stage_snapshot_enricher,
                progress_callback=research_progress,
            )
            heartbeat_stop = Event()

            def progress_heartbeat() -> None:
                while not heartbeat_stop.wait(55.0):
                    try:
                        progress.update_resources(measure_resources(self.settings.root).as_dict())
                        _progress_stdout(progress.snapshot())
                    except Exception:
                        continue

            heartbeat_thread = Thread(
                target=progress_heartbeat,
                name="liangjian-a1-progress-heartbeat",
                daemon=True,
            )
            heartbeat_thread.start()
            try:
                result = pipeline.run_a1_only(
                    prepared.snapshot,
                    run_id=maintenance_run_id,
                    generated_at=current,
                    models=(primary_model,),
                    lane_start_index=primary_lane_index,
                    primary_lane_ids=(primary_lane_id,),
                    scope_symbols=maintenance_scope_symbols,
                )
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join(timeout=2.0)
            if result.status not in {"READY", "READY_DEGRADED"}:
                raise WorkflowError("A1_MAINTENANCE_RESULT_BLOCKED")
            delta_outputs = _a1_outputs_by_result(result)
            merged_outputs: dict[str, dict[str, Any]] = {}
            updated_symbols = maintenance_scope_symbols if scope is not None else tuple(prepared.snapshot.data.get("g0_symbols", ()))
            removed_symbols = scope.removed_symbols if scope is not None else ()
            for lane_id, delta_output in delta_outputs.items():
                if normalized_mode == A1_INCREMENTAL:
                    base_output = base_outputs.get(lane_id)
                    if not isinstance(base_output, Mapping):
                        raise WorkflowError("A1_INCREMENTAL_BASELINE_LANE_MISSING")
                    merged_outputs[lane_id] = merge_a1_partitions(
                        base_output,
                        delta_output,
                        updated_symbols=updated_symbols,
                        removed_symbols=removed_symbols,
                    )
                else:
                    merged_outputs[lane_id] = dict(delta_output)
            if normalized_mode == A1_INCREMENTAL:
                # Maintenance is intentionally primary-only.  Preserve any
                # optional comparison lanes from a pre-existing generation so
                # a primary refresh cannot erase their complete partitions;
                # they are never used by the close primary lane.
                for lane_id, base_output in base_outputs.items():
                    if lane_id not in merged_outputs:
                        merged_outputs[lane_id] = dict(base_output)
            if not merged_outputs:
                raise WorkflowError("A1_MAINTENANCE_OUTPUT_EMPTY")
            delta_payload = scope.as_dict() if scope is not None else {
                "processed_count": len(updated_symbols),
                "added_count": len(updated_symbols),
                "changed_count": 0,
                "theme_affected_count": 0,
                "removed_count": 0,
                "unchanged_count": 0,
            }
            if scope is not None:
                delta_payload.update(
                    {
                        "processed_symbols": list(updated_symbols),
                        "processed_count": len(updated_symbols),
                        "macro_revalidation_symbols": list(macro_revalidation_symbols),
                        "macro_revalidation_count": len(macro_revalidation_symbols),
                    }
                )
            manifest = build_a1_manifest(
                prepared.snapshot.data,
                merged_outputs,
                mode=normalized_mode,
                snapshot_id=prepared.snapshot.snapshot_id,
                snapshot_hash=prepared.snapshot.snapshot_hash,
                as_of=current,
                base_generation_id=active.generation_id if active is not None else None,
                delta=delta_payload,
            )
            manifest["last_full_period"] = (
                f"{current.year:04d}-{current.month:02d}"
                if normalized_mode == A1_FULL
                else _a1_full_period(active)
            )
            iso_year, iso_week, _ = current.date().isocalendar()
            manifest["maintenance_week"] = f"{iso_year:04d}-W{iso_week:02d}"
            base_lane_records = base_payload.get("lanes") if isinstance(base_payload.get("lanes"), Mapping) else {}
            lane_records: dict[str, Mapping[str, Any]] = {}
            for lane_id, output in merged_outputs.items():
                if lane_id in delta_outputs:
                    lane_records[lane_id] = _a1_lane_record(result, lane_id, output)
                    continue
                raw_lane = base_lane_records.get(lane_id) if isinstance(base_lane_records, Mapping) else None
                if isinstance(raw_lane, Mapping):
                    preserved_lane = dict(raw_lane)
                    preserved_lane["output"] = dict(output)
                    lane_records[lane_id] = preserved_lane
                else:
                    lane_records[lane_id] = _a1_lane_record(result, lane_id, output)
            payload = {
                "schema_version": "liangjian-a1-registry/1.0.0",
                "generation_id": generation.generation_id,
                "mode": normalized_mode,
                "snapshot_id": prepared.snapshot.snapshot_id,
                "snapshot_hash": prepared.snapshot.snapshot_hash,
                "as_of": current.isoformat(),
                "base_generation_id": active.generation_id if active is not None else None,
                "delta": delta_payload,
                "lanes": dict(lane_records),
            }
            sealed = self.a1_registry.seal_generation(
                generation.generation_id,
                manifest=manifest,
                payload=payload,
                sealed_at=current,
            )
            activated = self.a1_registry.activate_generation(
                sealed.generation_id,
                expected_current_id=active.generation_id if active is not None else None,
                activated_at=current,
            )
            progress.update_resources(measure_resources(self.settings.root).as_dict())
            progress.finish(
                status=result.status,
                phase="COMPLETED",
                reason_code=("RESEARCH_READY_DEGRADED" if result.status == "READY_DEGRADED" else None),
                outcome=result.outcome().as_dict(),
            )
            _progress_stdout(progress.snapshot())
            return {
                "status": "PUBLISHED",
                "mode": normalized_mode,
                "generation_id": activated.generation_id,
                "previous_generation_id": active.generation_id if active is not None else None,
                "snapshot_id": prepared.snapshot.snapshot_id,
                "snapshot_hash": prepared.snapshot.snapshot_hash,
                "delta": delta_payload,
                "plan": plan.as_dict() if plan is not None else None,
                "research_run_id": result.run_id,
            }
        except Exception as exc:
            reason = _safe_reason_code(exc)
            try:
                self.a1_registry.fail_generation(generation.generation_id, reason, failed_at=current)
            except Exception:
                pass
            progress.finish(status="BLOCKED", phase="FAILED", reason_code=reason)
            _progress_stdout(progress.snapshot())
            return {
                "status": "FAILED",
                "mode": normalized_mode,
                "generation_id": generation.generation_id,
                "previous_generation_id": active.generation_id if active is not None else None,
                "reason_code": reason,
                "plan": plan.as_dict() if plan is not None else None,
            }

    def run_research(
        self,
        slot: str,
        *,
        as_of: datetime | None = None,
        historical_replay: bool = False,
        snapshot_id: str | None = None,
        models: tuple[str, ...] | None = None,
        lane_start_index: int = 1,
        primary_lane_ids: tuple[str, ...] | None = None,
        primary_only: bool = False,
        schedule_comparison: bool = False,
        publish_plans: bool = True,
        comparison_run: bool = False,
        run_id_override: str | None = None,
        snapshot_expected_date: str | None = None,
        snapshot_expected_hash: str | None = None,
        record_runtime: bool = True,
        target_trade_date: date | None = None,
        market_data_as_of: datetime | None = None,
        allow_non_trading_source: bool = False,
        reuse_resume_snapshot: bool = True,
        from_active_a1: bool = False,
        active_a1_generation_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_slot = _slot(slot)
        current = _aware(as_of or datetime.now(SHANGHAI))
        next_session_prep = target_trade_date is not None or allow_non_trading_source
        if next_session_prep:
            # This escape hatch is intentionally private to the dedicated
            # next-session preparation entry point.  It must not become a way
            # for normal close/morning calls to bypass the exchange calendar,
            # historical isolation, or the publication gate.
            if (
                normalized_slot != "close"
                or not allow_non_trading_source
                or target_trade_date is None
                or market_data_as_of is None
                or isinstance(target_trade_date, datetime)
                or not isinstance(target_trade_date, date)
                or historical_replay
                or snapshot_id is not None
                or comparison_run
                or not primary_only
                or schedule_comparison
                or not publish_plans
            ):
                raise WorkflowError("NEXT_SESSION_PREP_ARGUMENTS_INVALID")
            try:
                if self.trading_calendar.is_trading_day(current.date()):
                    raise WorkflowError("NEXT_SESSION_PREP_REQUIRES_NON_TRADING_DAY")
                expected_target = self.trading_calendar.next_trading_day(current.date())
                expected_market_date = self.trading_calendar.previous_trading_day(current.date())
            except TradingCalendarError as exc:
                raise WorkflowError(exc.reason_code) from exc
            if target_trade_date != expected_target:
                raise WorkflowError("NEXT_SESSION_PREP_TARGET_INVALID")
            market_cutoff = _aware(market_data_as_of)
            if market_cutoff.date() != expected_market_date or market_cutoff > current:
                raise WorkflowError("NEXT_SESSION_PREP_MARKET_DATE_INVALID")
        elif market_data_as_of is not None:
            raise WorkflowError("MARKET_DATA_AS_OF_REQUIRES_NEXT_SESSION_PREP")
        if historical_replay:
            if as_of is None:
                raise WorkflowError("HISTORICAL_AS_OF_REQUIRED")
            if current.date() >= datetime.now(SHANGHAI).date():
                raise WorkflowError("HISTORICAL_AS_OF_NOT_PAST")
        elif snapshot_id is not None and not comparison_run:
            raise WorkflowError("SNAPSHOT_ID_REQUIRES_HISTORICAL_REPLAY")
        if comparison_run and not snapshot_id:
            raise WorkflowError("COMPARISON_SNAPSHOT_REQUIRED")
        if not isinstance(from_active_a1, bool):
            raise WorkflowError("ACTIVE_A1_ARGUMENTS_INVALID")
        if from_active_a1 and (
            historical_replay
            or (comparison_run and not active_a1_generation_id)
        ):
            raise WorkflowError("ACTIVE_A1_ARGUMENTS_INVALID")
        if not next_session_prep:
            self._ensure_trading_day(
                current,
                synchronize_accounts=not historical_replay and not comparison_run,
            )
        if normalized_slot == "morning" and current.hour == 9 and current.minute < 26:
            time.sleep(max(0.0, (_at_time(current, 9, 26) - current).total_seconds()))
            current = datetime.now(SHANGHAI)
        # A comparison is an audit child of the primary run.  It must not
        # overwrite the single control-plane progress projection that the UI
        # uses for the next morning/close job.  Keep a durable child progress
        # file under workflow output instead; the child run summary remains
        # the authoritative comparison record.
        comparison_progress_id = re.sub(
            r"[^A-Za-z0-9_.-]",
            "_",
            str(run_id_override or f"{current.date()}-{normalized_slot}-comparison"),
        )[:180]
        progress_path = (
            self.settings.workflow_output_dir / "comparison_progress" / f"{comparison_progress_id}.json"
            if comparison_run
            else self.settings.workflow_progress_path
        )
        progress = WorkflowProgress(
            progress_path,
            run_id=run_id_override or f"{current.date()}-{normalized_slot}",
            job=normalized_slot,
        )
        progress.set_phase("DATA_SYNC")
        resource_decision = evaluate_resources(self.settings.root)
        progress.update_resources(resource_decision.snapshot.as_dict())
        _progress_stdout(progress.snapshot())
        if not resource_decision.allowed:
            progress.finish(
                status="BLOCKED",
                phase="FAILED",
                reason_code=resource_decision.reason_codes[0],
            )
            _progress_stdout(progress.snapshot())
            raise WorkflowError(resource_decision.reason_codes[0])
        _progress_stdout(progress.snapshot())
        active_a1_generation: A1Generation | None = None
        a1_reuse_degraded = False
        a1_reuse_age_seconds: int | None = None
        if from_active_a1:
            try:
                # Check the immutable pointer before preparing any new
                # snapshot.  A close without a current A1 must fail closed
                # rather than doing work that could be mistaken for an
                # implicit A1 refresh.
                if active_a1_generation_id:
                    active_a1_generation = self.a1_registry.get_generation(
                        active_a1_generation_id
                    )
                    if active_a1_generation is None or not active_a1_generation.is_sealed:
                        raise A1RegistryError("A1_REQUESTED_GENERATION_INVALID")
                else:
                    active_a1_generation = self.a1_registry.require_active(
                        as_of=current,
                        max_age=_A1_MAX_AGE,
                    )
                a1_reuse_age_seconds = max(
                    0,
                    int((current - active_a1_generation.as_of).total_seconds()),
                )
                a1_reuse_degraded = current - active_a1_generation.as_of > _A1_DEGRADED_AFTER
            except A1RegistryError as exc:
                progress.finish(status="BLOCKED", phase="FAILED", reason_code=exc.reason_code)
                _progress_stdout(progress.snapshot())
                raise WorkflowError(exc.reason_code) from exc
        try:
            prepared = (
                self._load_research_snapshot_by_id(
                    snapshot_id,
                    expected_date=snapshot_expected_date or current.date().isoformat(),
                )
                if snapshot_id is not None
                else None if historical_replay or not reuse_resume_snapshot
                else self._load_research_resume_snapshot(normalized_slot, current)
            )
            if prepared is None:
                prepared = self.prepare_snapshot(
                    as_of=current,
                    market_data_as_of=market_data_as_of,
                    progress=progress,
                )
                if not historical_replay and not comparison_run:
                    self._write_research_resume_marker(
                        normalized_slot,
                        prepared,
                        status="ACTIVE",
                    )
            else:
                progress.set_phase("SNAPSHOT_RESUMED")
                progress.update_resources(measure_resources(self.settings.root).as_dict())
                _progress_stdout(progress.snapshot())
            if snapshot_expected_hash and prepared.snapshot.snapshot_hash != snapshot_expected_hash:
                raise WorkflowError("COMPARISON_SNAPSHOT_HASH_MISMATCH")
        except Exception as exc:
            progress.finish(
                status="BLOCKED",
                phase="FAILED",
                reason_code=_safe_reason_code(exc),
            )
            _progress_stdout(progress.snapshot())
            raise
        run_id = run_id_override or f"{current.date()}-{normalized_slot}-{prepared.snapshot.snapshot_hash[:12]}"

        def research_progress(event: Mapping[str, Any]) -> None:
            progress.research_event(event)
            _progress_stdout(progress.snapshot())

        pipeline = ResearchPipeline(
            self.settings,
            prompt_repository=self.prompts,
            model_client=self.model_client,
            output_dir=self.settings.workflow_output_dir / "research",
            # A full lane must finish and release its projected prompt data
            # before the next model starts. Parallel lanes multiply the
            # 200+ MiB frozen snapshot during JSON projection.
            parallel_lanes=False,
            runtime_store=self.store if record_runtime else None,
            slot=normalized_slot,
            batch_workers=1,
            progress_callback=research_progress,
            checkpoint_store=self.research_checkpoints,
            stage_snapshot_enricher=self._stage_snapshot_enricher,
        )
        heartbeat_stop = Event()

        def progress_heartbeat() -> None:
            while not heartbeat_stop.wait(55.0):
                try:
                    progress.update_resources(measure_resources(self.settings.root).as_dict())
                    _progress_stdout(progress.snapshot())
                except Exception:
                    # Observability is deliberately best-effort and must not
                    # affect model decisions or the fail-closed pipeline.
                    continue

        heartbeat_thread = Thread(
            target=progress_heartbeat,
            name="liangjian-progress-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            primary_model, primary_lane_index, primary_lane_id = _primary_model_for_settings(self.settings)
            selected_models = (
                tuple(models)
                if models is not None
                else (primary_model,)
                if primary_only
                else tuple(self.settings.research_models)
            )
            selected_lane_start_index = (
                primary_lane_index
                if primary_only and models is None
                else lane_start_index
            )
            selected_primary_lane_ids = (
                tuple(primary_lane_ids)
                if primary_lane_ids is not None
                else (primary_lane_id,)
                if primary_only
                else (self.settings.research_primary_lane_id,)
            )
            if from_active_a1:
                result = pipeline.run_from_active_a1(
                    prepared.snapshot,
                    active_a1_generation,
                    run_id=run_id,
                    generated_at=current,
                    historical_replay=historical_replay,
                    models=selected_models,
                    lane_start_index=selected_lane_start_index,
                    primary_lane_ids=selected_primary_lane_ids,
                    generation_id=active_a1_generation.generation_id if active_a1_generation else None,
                )
            else:
                result = pipeline.run(
                    prepared.snapshot,
                    run_id=run_id,
                    generated_at=current,
                    historical_replay=historical_replay,
                    models=selected_models,
                    lane_start_index=selected_lane_start_index,
                    primary_lane_ids=selected_primary_lane_ids,
                )
        except Exception as exc:
            progress.finish(
                status="BLOCKED",
                phase="FAILED",
                reason_code=_safe_reason_code(exc),
            )
            _progress_stdout(progress.snapshot())
            raise
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=2.0)
        progress.update_resources(measure_resources(self.settings.root).as_dict())
        broker_benchmark = _write_broker_gold_benchmark(
            result,
            as_of=current,
            benchmark_dir=self.settings.broker_gold_dir,
            output_dir=self.settings.workflow_output_dir / "research",
        )
        stage_markdown = write_stage_markdown_reports(
            result,
            self.settings.workflow_output_dir / "research",
        )
        publication = (
            self._publish_plans(
                result,
                normalized_slot,
                current,
                snapshot_data=prepared.snapshot.data,
                minimum_trade_date=target_trade_date,
            )
            if publish_plans
            else {
                "atomic": True,
                "created": [],
                "activated": [],
                "blocked": [],
                "publication": "COMPARISON_ONLY",
            }
        )
        summary = {
            "run_id": run_id,
            "slot": normalized_slot,
            "status": result.status,
            "run_role": "comparison" if comparison_run else "primary" if primary_only else "full",
            "models": [lane.model for lane in result.lanes],
            "primary_lane_ids": list(result.primary_lane_ids),
            "source_as_of": current.isoformat(),
            "target_trade_date": (
                target_trade_date.isoformat() if target_trade_date is not None else None
            ),
            "market_trade_date": (
                market_data_as_of.astimezone(SHANGHAI).date().isoformat()
                if market_data_as_of is not None
                else current.date().isoformat()
            ),
            "market_data_as_of": (
                market_data_as_of.astimezone(SHANGHAI).isoformat()
                if market_data_as_of is not None
                else current.isoformat()
            ),
            "preparation_mode": (
                "NEXT_SESSION_PRODUCTION_PREP" if next_session_prep else None
            ),
            "a1_generation_id": active_a1_generation.generation_id if active_a1_generation else None,
            "a1_reused": bool(active_a1_generation),
            "a1_reuse_age_seconds": a1_reuse_age_seconds,
            "a1_reuse_degraded": a1_reuse_degraded,
            "a1_reuse_reason_code": "A1_ACTIVE_STALE_DEGRADED" if a1_reuse_degraded else None,
            "outcome_v2": result.outcome().as_dict(),
            "snapshot": prepared.as_dict(),
            "research_markdown": str(result.markdown_path) if result.markdown_path else None,
            "stage_markdown": stage_markdown,
            "broker_gold_benchmark": broker_benchmark,
            "plan_publication": publication,
        }
        atomic_write_json(self.settings.workflow_output_dir / "runs" / f"{run_id}.json", summary)
        research_ready = result.status in {"READY", "READY_DEGRADED"}
        ready_reason = "RESEARCH_READY_DEGRADED" if result.status == "READY_DEGRADED" else None
        if schedule_comparison and primary_only and research_ready and not historical_replay and not comparison_run:
            comparison_request_kwargs: dict[str, Any] = {
                "parent_run_id": run_id,
                "prepared": prepared,
                "slot": normalized_slot,
                "primary_status": result.status,
            }
            if active_a1_generation is not None:
                comparison_request_kwargs["a1_generation_id"] = active_a1_generation.generation_id
            request = self._create_comparison_request(
                **comparison_request_kwargs,
            )
            summary["comparison_request"] = request
            atomic_write_json(self.settings.workflow_output_dir / "runs" / f"{run_id}.json", summary)
        if not historical_replay and not comparison_run:
            self._write_research_resume_marker(
                normalized_slot,
                prepared,
                status="COMPLETED" if research_ready else "RETRYABLE",
                reason_code=ready_reason if research_ready else "RESEARCH_NOT_READY",
            )
        progress.finish(
            status=result.status,
            phase="COMPLETED" if research_ready else "BLOCKED",
            reason_code=ready_reason if research_ready else "RESEARCH_NOT_READY",
            # Persist the canonical lane outcomes at the same lifecycle
            # boundary as the run status.  Without this, the last per-stage
            # event can leave a lane presented as RUNNING after the result has
            # already been written and published.
            outcome=result.outcome().as_dict(),
        )
        _progress_stdout(progress.snapshot())
        return summary

    # ------------------------------------------------------------------
    # Optional comparison-lane queue
    # ------------------------------------------------------------------
    def _comparison_request_dir(self) -> Path:
        return self.settings.workflow_output_dir / "comparison_requests"

    def _comparison_request_path(self, parent_run_id: str) -> Path:
        safe_parent = re.sub(r"[^A-Za-z0-9_.-]", "_", str(parent_run_id))[:180]
        if not safe_parent:
            raise WorkflowError("COMPARISON_PARENT_RUN_ID_INVALID")
        return self._comparison_request_dir() / f"{safe_parent}.json"

    def _read_comparison_request(self, path: Path) -> dict[str, Any] | None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, TypeError):
            return None
        if not isinstance(raw, Mapping):
            return None
        if raw.get("schema_version") != _COMPARISON_REQUEST_SCHEMA:
            return None
        if not isinstance(raw.get("parent_run_id"), str) or not raw["parent_run_id"]:
            return None
        if str(raw.get("status") or "") not in _COMPARISON_REQUEST_STATUSES:
            return None
        return dict(raw)

    def _create_comparison_request(
        self,
        *,
        parent_run_id: str,
        prepared: PreparedSnapshot,
        slot: str,
        primary_status: str,
        a1_generation_id: str | None = None,
    ) -> dict[str, Any]:
        """Create the idempotent durable hand-off from primary to comparison.

        The primary summary and plans are written before this marker.  The
        marker therefore acts as the durable queue commit: a restarted Node
        process can safely discover it without re-running the primary lane.
        """

        path = self._comparison_request_path(parent_run_id)
        existing = self._read_comparison_request(path) if path.is_file() else None
        if existing is not None:
            if (
                existing.get("snapshot_id") != prepared.snapshot.snapshot_id
                or existing.get("snapshot_hash") != prepared.snapshot.snapshot_hash
                or (
                    a1_generation_id is not None
                    and existing.get("a1_generation_id") != a1_generation_id
                )
            ):
                raise WorkflowError("COMPARISON_REQUEST_IMMUTABLE_MISMATCH")
            return existing
        now = datetime.now(SHANGHAI).isoformat()
        request = {
            "schema_version": _COMPARISON_REQUEST_SCHEMA,
            "request_id": str(parent_run_id),
            "parent_run_id": str(parent_run_id),
            "snapshot_id": prepared.snapshot.snapshot_id,
            "snapshot_hash": prepared.snapshot.snapshot_hash,
            "snapshot_as_of": prepared.snapshot.as_of.isoformat(),
            "trade_date": prepared.snapshot.as_of.astimezone(SHANGHAI).date().isoformat(),
            "slot": str(slot),
            "models": list(self.settings.research_models[1:]),
            "lane_start_index": 2,
            "primary_lane_ids": ["lane_2", "lane_3"],
            "primary_status": str(primary_status),
            "a1_generation_id": str(a1_generation_id or "") or None,
            "status": "PENDING",
            "attempts": 0,
            "child_run_id": None,
            "reason_code": None,
            "owner_pid": None,
            "created_at": now,
            "updated_at": now,
        }
        atomic_write_json(path, request)
        return request

    def list_comparison_requests(
        self,
        *,
        statuses: frozenset[str] | set[str] | tuple[str, ...] = _COMPARISON_RETRYABLE_STATUSES,
    ) -> tuple[dict[str, Any], ...]:
        """List safe, durable comparison requests for recovery/inspection."""

        wanted = {str(value).upper() for value in statuses}
        directory = self._comparison_request_dir()
        try:
            paths = tuple(directory.glob("*.json"))
        except OSError:
            return ()
        rows = [
            request
            for path in paths
            if (request := self._read_comparison_request(path)) is not None
            and str(request.get("status") or "").upper() in wanted
        ]
        rows.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""))
        return tuple(rows)

    @staticmethod
    def _comparison_owner_alive(owner_pid: Any) -> bool:
        try:
            pid = int(owner_pid)
        except (TypeError, ValueError):
            return False
        if pid <= 0 or pid == os.getpid():
            return pid == os.getpid()
        try:
            os.kill(pid, 0)
        except (OSError, ProcessLookupError):
            return False
        return True

    def _claim_comparison_request(self, request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]] | None:
        parent_run_id = str(request.get("parent_run_id") or "")
        path = self._comparison_request_path(parent_run_id)
        lock_path = path.with_suffix(".claim.lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                try:
                    if datetime.now(SHANGHAI).timestamp() - lock_path.stat().st_mtime > _COMPARISON_OWNER_STALE_SECONDS:
                        lock_path.unlink(missing_ok=True)
                        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    else:
                        return None
                except OSError:
                    return None
            current = self._read_comparison_request(path)
            if current is None:
                return None
            status = str(current.get("status") or "").upper()
            if status not in _COMPARISON_RETRYABLE_STATUSES:
                return None
            if status == "RUNNING" and self._comparison_owner_alive(current.get("owner_pid")):
                return None
            try:
                attempts = int(current.get("attempts") or 0)
            except (TypeError, ValueError):
                attempts = 0
            claimed = {
                **current,
                "status": "RUNNING",
                "attempts": max(0, attempts) + 1,
                "owner_pid": os.getpid(),
                "updated_at": datetime.now(SHANGHAI).isoformat(),
            }
            atomic_write_json(path, claimed)
            return path, claimed
        finally:
            if lock_fd is not None:
                try:
                    os.close(lock_fd)
                except OSError:
                    pass
                # Only the process that successfully created this lock owns
                # its lifecycle.  A contender that observed a fresh foreign
                # lock must never unlink it in ``finally``.
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def _update_comparison_request(
        self,
        path: Path,
        *,
        status: str,
        reason_code: str | None = None,
        child_run_id: str | None = None,
        expected_attempt: int | None = None,
    ) -> dict[str, Any] | None:
        current = self._read_comparison_request(path)
        if current is None:
            return None
        if expected_attempt is not None:
            try:
                current_attempt = int(current.get("attempts") or 0)
            except (TypeError, ValueError):
                return None
            if current_attempt != expected_attempt or not self._comparison_owner_alive(current.get("owner_pid")):
                # A stale child must never finalize a request reclaimed by a
                # later process.  The attempt number is the durable fencing
                # token; PID is only an additional liveness check.
                return None
        updated = {
            **current,
            "status": str(status).upper(),
            "reason_code": reason_code,
            "child_run_id": child_run_id or current.get("child_run_id"),
            "owner_pid": None,
            "updated_at": datetime.now(SHANGHAI).isoformat(),
        }
        atomic_write_json(path, updated)
        return updated

    def run_comparison(self, *, parent_run_id: str | None = None) -> dict[str, Any]:
        """Run one pending optional comparison request, never the primary lane."""

        if not self.settings.comparison_enabled:
            return {"status": "NOOP", "reason_code": "COMPARISON_DISABLED_STABLE_MODE"}

        if parent_run_id:
            path = self._comparison_request_path(parent_run_id)
            request = self._read_comparison_request(path)
            candidates = (
                (request,)
                if request is not None
                and str(request.get("status") or "").upper() in _COMPARISON_RETRYABLE_STATUSES
                else ()
            )
        else:
            candidates = self.list_comparison_requests()
        if not candidates or candidates[0] is None:
            return {"status": "NOOP", "reason_code": "NO_PENDING_COMPARISON"}
        claimed = self._claim_comparison_request(candidates[0])
        if claimed is None:
            return {"status": "SKIPPED", "reason_code": "COMPARISON_BUSY"}
        path, request = claimed
        parent = str(request["parent_run_id"])
        attempt = int(request.get("attempts") or 1)
        child_run_id = f"{parent}-comparison-{attempt}"
        try:
            snapshot_as_of = _aware(datetime.fromisoformat(str(request.get("snapshot_as_of") or "")))
            models = tuple(str(item) for item in request.get("models", ()) if str(item).strip())
            if len(models) != 2:
                raise WorkflowError("COMPARISON_MODEL_SET_INVALID")
            summary = self.run_research(
                str(request.get("slot") or "close"),
                as_of=snapshot_as_of,
                snapshot_id=str(request.get("snapshot_id") or ""),
                snapshot_expected_hash=str(request.get("snapshot_hash") or ""),
                models=models,
                lane_start_index=int(request.get("lane_start_index") or 2),
                primary_lane_ids=tuple(str(item) for item in request.get("primary_lane_ids", ("lane_2", "lane_3"))),
                publish_plans=False,
                comparison_run=True,
                run_id_override=child_run_id,
                snapshot_expected_date=str(request.get("trade_date") or snapshot_as_of.date().isoformat()),
                record_runtime=False,
                from_active_a1=bool(request.get("a1_generation_id")),
                active_a1_generation_id=(
                    str(request.get("a1_generation_id"))
                    if request.get("a1_generation_id")
                    else None
                ),
            )
            child_path = self.settings.workflow_output_dir / "runs" / f"{child_run_id}.json"
            summary = {**summary, "parent_run_id": parent, "comparison_request_id": parent}
            atomic_write_json(child_path, summary)
            result_status = str(summary.get("status") or "")
            final_status = "SUCCEEDED" if result_status in {"READY", "READY_DEGRADED"} else "FAILED"
            updated = self._update_comparison_request(
                path,
                status=final_status,
                reason_code=None if final_status == "SUCCEEDED" else "COMPARISON_NOT_READY",
                child_run_id=child_run_id,
                expected_attempt=attempt,
            )
            return {
                "status": final_status,
                "parent_run_id": parent,
                "child_run_id": child_run_id,
                "request": updated,
            }
        except KeyboardInterrupt:
            self._update_comparison_request(
                path,
                status="CANCELLED",
                reason_code="RUN_CANCELLED",
                child_run_id=child_run_id,
                expected_attempt=attempt,
            )
            raise
        except Exception as exc:
            reason_code = _safe_reason_code(exc)
            updated = self._update_comparison_request(
                path,
                status="FAILED",
                reason_code=reason_code,
                child_run_id=child_run_id,
                expected_attempt=attempt,
            )
            return {
                "status": "FAILED",
                "parent_run_id": parent,
                "child_run_id": child_run_id,
                "reason_code": reason_code,
                "request": updated,
            }

    def _load_research_snapshot_by_id(
        self,
        snapshot_id: str,
        *,
        expected_date: str,
    ) -> PreparedSnapshot:
        """Load one immutable snapshot for replay without fetching live facts."""

        if not re.fullmatch(r"snapshot-[A-Za-z0-9+._-]{8,180}", str(snapshot_id or "")):
            raise WorkflowError("SNAPSHOT_ID_INVALID")
        path = (self.settings.snapshot_dir / f"{snapshot_id}.json").resolve()
        root = self.settings.snapshot_dir.resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise WorkflowError("SNAPSHOT_NOT_FOUND")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, TypeError) as exc:
            raise WorkflowError("SNAPSHOT_INVALID") from exc
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
            raise WorkflowError("SNAPSHOT_INVALID")
        data = dict(payload["data"])
        observed_id = str(payload.get("snapshot_id") or "")
        observed_hash = str(payload.get("snapshot_hash") or "")
        if observed_id != snapshot_id or not observed_hash or observed_hash != _hash_json(data):
            raise WorkflowError("SNAPSHOT_HASH_MISMATCH")
        try:
            frozen_at = _aware(datetime.fromisoformat(str(payload.get("as_of") or "")))
        except (TypeError, ValueError) as exc:
            raise WorkflowError("SNAPSHOT_AS_OF_INVALID") from exc
        if frozen_at.date().isoformat() != expected_date:
            raise WorkflowError("SNAPSHOT_TRADE_DATE_MISMATCH")
        g0_symbols = data.get("g0_symbols")
        if not isinstance(g0_symbols, list) or not g0_symbols:
            raise WorkflowError("SNAPSHOT_G0_INVALID")
        manifest = data.get("snapshot_manifest")
        manifest = manifest if isinstance(manifest, Mapping) else {}
        full_count = _positive_count(
            manifest.get("full_universe_count"),
            fallback=_container_length(data.get("universe_candidates")),
        )
        research_count = _positive_count(
            manifest.get("research_universe_count"),
            fallback=max(len(g0_symbols), _container_length(data.get("universe_candidates"))),
        )
        trade_count = _positive_count(
            manifest.get("trade_universe_count"),
            fallback=_container_length(data.get("trade_candidates")),
        )
        factor_ready = data.get("factor_ready_symbols")
        return PreparedSnapshot(
            snapshot=ResearchSnapshot(
                snapshot_id=observed_id,
                snapshot_hash=observed_hash,
                as_of=frozen_at,
                data=data,
            ),
            path=path,
            full_universe_count=full_count,
            research_universe_count=research_count,
            trade_universe_count=trade_count,
            selected_count=len(g0_symbols),
            factor_ready_count=_container_length(factor_ready),
            feature_source=None,
        )

    def _research_resume_marker_path(self, slot: str, trade_date: str) -> Path:
        return self.settings.research_checkpoint_dir / "active_runs" / f"{trade_date}-{slot}.json"

    def _write_research_resume_marker(
        self,
        slot: str,
        prepared: PreparedSnapshot,
        *,
        status: str,
        reason_code: str | None = None,
    ) -> None:
        trade_date = prepared.snapshot.as_of.astimezone(SHANGHAI).date().isoformat()
        atomic_write_json(
            self._research_resume_marker_path(slot, trade_date),
            {
                "schema_version": _RESEARCH_RESUME_SCHEMA,
                "slot": slot,
                "trade_date": trade_date,
                "status": status,
                "reason_code": reason_code,
                "updated_at": datetime.now(SHANGHAI).isoformat(),
                "prepared_snapshot": prepared.as_dict(),
            },
        )

    def _load_research_resume_snapshot(
        self,
        slot: str,
        current: datetime,
    ) -> PreparedSnapshot | None:
        trade_date = current.astimezone(SHANGHAI).date().isoformat()
        marker_path = self._research_resume_marker_path(slot, trade_date)
        try:
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return None
        if (
            not isinstance(marker, Mapping)
            or marker.get("schema_version") != _RESEARCH_RESUME_SCHEMA
            or marker.get("slot") != slot
            or marker.get("trade_date") != trade_date
            or marker.get("status") not in {"ACTIVE", "RETRYABLE"}
        ):
            return None
        raw = marker.get("prepared_snapshot")
        if not isinstance(raw, Mapping):
            return None
        try:
            path = Path(str(raw["path"])).resolve()
            snapshot_root = self.settings.snapshot_dir.resolve()
            if not path.is_relative_to(snapshot_root) or not path.is_file():
                return None
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
                return None
            data = dict(payload["data"])
            snapshot_hash = str(payload.get("snapshot_hash") or "")
            snapshot_id = str(payload.get("snapshot_id") or "")
            if (
                not snapshot_hash
                or snapshot_hash != _hash_json(data)
                or snapshot_hash != str(raw.get("snapshot_hash") or "")
                or snapshot_id != str(raw.get("snapshot_id") or "")
            ):
                return None
            as_of = _aware(datetime.fromisoformat(str(payload["as_of"])))
            if as_of.date().isoformat() != trade_date:
                return None
            full_count = int(raw["full_universe_count"])
            research_count = int(raw["research_universe_count"])
            selected_count = int(raw["selected_count"])
            g0_symbols = data.get("g0_symbols")
            if (
                data.get("G0_SCOPE_CONTRACT") != _G0_SCOPE_CONTRACT
                or selected_count != research_count
                or not isinstance(g0_symbols, list)
                or len(g0_symbols) != research_count
                or len(set(map(str, g0_symbols))) != research_count
            ):
                return None
            return PreparedSnapshot(
                snapshot=ResearchSnapshot(
                    snapshot_id=snapshot_id,
                    snapshot_hash=snapshot_hash,
                    as_of=as_of,
                    data=data,
                ),
                path=path,
                full_universe_count=full_count,
                research_universe_count=research_count,
                trade_universe_count=int(raw["trade_universe_count"]),
                selected_count=selected_count,
                factor_ready_count=int(raw["factor_ready_count"]),
                feature_source=(
                    dict(raw["feature_source"])
                    if isinstance(raw.get("feature_source"), Mapping)
                    else None
                ),
            )
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def _stage_snapshot_enricher(
        self,
        *,
        stage: str,
        lane_id: str,
        model: str,
        upstream_symbols: frozenset[str],
        snapshot: ResearchSnapshot,
    ) -> Mapping[str, Any] | None:
        """Load stage-specific evidence only for the upstream lane subset."""

        del lane_id, model
        if not upstream_symbols:
            return None
        current = _aware(snapshot.as_of or datetime.now(SHANGHAI))
        if stage == "A2":
            # Historical replay must remain bound to its immutable snapshot.
            # The open-news endpoints are current-only and cannot answer an
            # arbitrary prior trade date, so querying them here would mix
            # today's articles into a point-in-time A2 result.  The frozen
            # market/news facts remain available in the base snapshot.
            if current.date() != datetime.now(SHANGHAI).date():
                return None
            stage_current = datetime.now(SHANGHAI)
            results = self._collect_open_news(
                sorted(upstream_symbols),
                include_market=False,
            )
            manifest = normalize_open_news_results(results, as_of=stage_current)
            stock_news = build_news_heat_snapshot(
                manifest_projection(manifest),
                sorted(upstream_symbols),
                as_of=stage_current,
            )
            base_news = snapshot.data.get("NEWS_HEAT_SNAPSHOT")
            merged_news = _merge_news_heat_snapshots(base_news, stock_news)
            scoped_symbols = sorted(upstream_symbols)
            merged_data = {
                **dict(snapshot.data),
                "NEWS_HEAT_SNAPSHOT": merged_news,
                "A2_STOCK_NEWS_SCOPE": scoped_symbols,
            }
            # The base snapshot is already content-addressed. Hash only its
            # digest plus the A2 overlay; serializing the complete 200MB+
            # snapshot once per lane creates an avoidable multi-GB peak.
            stage_hash = _hash_json(
                {
                    "base_snapshot_hash": snapshot.snapshot_hash,
                    "stage": "A2",
                    "NEWS_HEAT_SNAPSHOT": merged_news,
                    "A2_STOCK_NEWS_SCOPE": scoped_symbols,
                }
            )
            return {
                "snapshot_id": f"{snapshot.snapshot_id}:a2:{stage_hash[:12]}",
                "snapshot_hash": stage_hash,
                "as_of": stage_current,
                "data": merged_data,
            }
        if stage != "A3":
            return None

        def build(symbol: str) -> tuple[str, dict[str, Any] | None]:
            cache_key = (symbol, current.isoformat())
            with self._stage_technical_lock:
                retained = self._stage_technical_cache.get(cache_key)
            if retained is not None:
                return symbol, retained

            daily_rows = self.fact_cache.query_daily_bars(
                symbol,
                adjust="none",
                end=current + timedelta(days=1),
                limit=800,
                descending=True,
            )
            daily_bars = [dict(item["payload"]) for item in reversed(daily_rows)]
            # A3 is a monthly/weekly/daily planning stage.  Five-minute bars
            # are useful only as a legacy observation and must never make the
            # high-timeframe factor unavailable or trigger an N-symbol network
            # backfill.  A4 owns live intraday data acquisition.  Reuse an
            # already complete local minute window when present; otherwise
            # compute the A3 contract from the persisted daily history alone.
            required_bars = self.settings.mootdx_history_5m_required_bars
            cached_bars = self.minute_store.load_latest(symbol, "5m", limit=required_bars)
            minute_bars = (
                cached_bars
                if _minute_cache_ready(cached_bars, required_bars=required_bars, as_of=current)
                else []
            )
            factor = FactorEngine(symbol).compute(
                daily_bars=daily_bars,
                minute_bars=minute_bars,
                as_of=current,
            )
            minimum_reward_risk = _workflow_float(snapshot.data.get("MIN_REWARD_RISK", 2.0)) or 2.0
            max_stop_distance = _workflow_float(snapshot.data.get("MAX_STOP_DISTANCE", 0.06)) or 0.06
            aggregates = build_technical_aggregates(
                factor,
                minimum_reward_risk=minimum_reward_risk,
                max_stop_distance_pct=max_stop_distance,
            )
            value = {
                **_compact_factor(factor.model_dump(mode="json")),
                "kline_patterns": aggregates["KLINE_PATTERNS"],
                "price_levels": aggregates["PRICE_LEVELS"],
            }
            with self._stage_technical_lock:
                self._stage_technical_cache[cache_key] = value
            return symbol, value

        technical: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=min(8, len(upstream_symbols))) as executor:
            futures = [executor.submit(build, symbol) for symbol in sorted(upstream_symbols)]
            for future in as_completed(futures):
                symbol, value = future.result()
                if value is not None:
                    technical[symbol] = value
        missing = {"available": False, "reason_code": "TECHNICAL_DATA_NOT_READY"}
        return {
            "FACTOR_SNAPSHOT": {
                symbol: {
                    key: item
                    for key, item in value.items()
                    if key not in {"kline_patterns", "price_levels"}
                }
                for symbol, value in sorted(technical.items())
            },
            "KLINE_PATTERNS": {
                symbol: value.get("kline_patterns", missing)
                for symbol, value in sorted(technical.items())
            },
            "PRICE_LEVELS": {
                symbol: value.get("price_levels", missing)
                for symbol, value in sorted(technical.items())
            },
            "A3_TECHNICAL_SCOPE": sorted(upstream_symbols),
            "A3_TECHNICAL_READY": sorted(technical),
        }

    def monitor_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = _aware(now or datetime.now(SHANGHAI)).replace(second=0, microsecond=0)
        self._ensure_trading_day(current)
        minute_snapshot_id = f"minute-{current.strftime('%Y%m%dT%H%M%S%z')}"
        lane_plans = {
            lane_id: self.store.list_active_plans(lane_id, at=current)
            for lane_id in self.brokers
        }
        lane_scopes: dict[str, set[str]] = {}
        for lane_id, plans in lane_plans.items():
            scope = {str(plan["symbol"]) for plan in plans}
            scope.update(
                str(position["symbol"])
                for position in self.store.list_positions(f"paper:{lane_id}")
            )
            lane_scopes[lane_id] = scope

        # Market data is frozen once per symbol/minute and shared by every
        # isolated lane.  The two intervals are fetched together per symbol;
        # no lane can observe a later quote than another lane.
        market: dict[str, dict[str, Any]] = {}
        all_symbols = sorted(set().union(*lane_scopes.values())) if lane_scopes else []

        def fetch_symbol(symbol: str) -> tuple[str, dict[str, Any]]:
            # A4 derives closed 5m/15m structure from the complete current
            # session.  This remains bounded to one trading day per active
            # plan; it is not a full-market history refresh.
            one = self.market_data.fetch_bars(symbol, "1m", 240, as_of=current)
            five = self.market_data.fetch_bars(symbol, "5m", 60, as_of=current)
            return symbol, {"1m": one, "5m": five}

        if all_symbols:
            with ThreadPoolExecutor(max_workers=min(8, len(all_symbols))) as executor:
                futures = [executor.submit(fetch_symbol, symbol) for symbol in all_symbols]
                for future in as_completed(futures):
                    symbol, fetched = future.result()
                    market[symbol] = fetched
                    for interval in ("1m", "5m"):
                        result = fetched[interval]
                        if result.bars:
                            self.minute_store.write(result.bars)

        simulation: list[dict[str, Any]] = []
        lane_inputs: dict[
            str,
            tuple[dict[str, MinuteBar], bool, dict[str, Any], dict[str, tuple[MinuteBar, ...]]],
        ] = {}
        for lane_id, scope_symbols in lane_scopes.items():
            bars: dict[str, MinuteBar] = {}
            contexts: dict[str, Any] = {}
            histories: dict[str, tuple[MinuteBar, ...]] = {}
            data_ok = True
            for symbol in sorted(scope_symbols):
                fetched = market.get(symbol, {})
                one = fetched.get("1m")
                five = fetched.get("5m")
                one_bars = tuple(
                    bar for bar in (tuple(one.bars) if one is not None else ())
                    if bar.bar_end.astimezone(SHANGHAI).date() == current.date()
                )
                five_bars = tuple(
                    bar for bar in (tuple(five.bars) if five is not None else ())
                    if bar.bar_end.astimezone(SHANGHAI).date() == current.date()
                )
                one_gaps = detect_missing_bars(one_bars, "1m", as_of=current) if one_bars else ()
                if not one_bars or one_bars[-1].bar_end != current or one_gaps:
                    data_ok = False
                else:
                    bars[symbol] = one_bars[-1]
                    histories[symbol] = one_bars
                    simulation.extend(self._settle_prior_signals(lane_id, symbol, one_bars[-1]))
                contexts[symbol] = _intraday_market_context(
                    symbol,
                    one_bars,
                    five_bars,
                    current=current,
                )
            lane_inputs[lane_id] = (bars, data_ok, contexts, histories)

        def process_lane(lane_id: str) -> tuple[str, MonitorBatchResult]:
            plans = lane_plans[lane_id]
            bars, data_ok, contexts, histories = lane_inputs[lane_id]
            engine = MonitorEngine(
                self.store,
                llm_veto=self._a4_callback(lane_id, plans, contexts, current),
                max_seconds=50,
            )
            batch = engine.process_minute(
                lane_id,
                bars,
                minute_snapshot_id=minute_snapshot_id,
                now=current,
                data_ok=data_ok,
                snapshot_contiguous=data_ok,
                bar_histories=histories,
                market_contexts=contexts,
            )
            return lane_id, batch

        lane_batches: dict[str, MonitorBatchResult] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(self.brokers))) as executor:
            futures = [executor.submit(process_lane, lane_id) for lane_id in self.brokers]
            for future in as_completed(futures):
                lane_id, batch = future.result()
                lane_batches[lane_id] = batch
        results = [_batch_dict(lane_batches[lane_id]) for lane_id in self.brokers]
        rebuild_effective_markdown(
            self.store,
            self.settings.workflow_output_dir / "monitor" / "effective_signals.md",
        )
        payload = {
            "minute_snapshot_id": minute_snapshot_id,
            "time": current.isoformat(),
            "lanes": results,
            "simulation": simulation,
        }
        atomic_write_json(self.settings.workflow_output_dir / "monitor" / "latest.json", payload)
        return payload

    def review_pending_morning(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Perform the time-bounded deterministic review of close A3 plans."""

        current = _aware(now or datetime.now(SHANGHAI)).replace(second=0, microsecond=0)
        self._ensure_trading_day(current)
        clock = current.time().replace(tzinfo=None)
        if clock < datetime.strptime("09:26", "%H:%M").time():
            raise WorkflowError("MORNING_REVIEW_BEFORE_AUCTION_FINAL")
        if clock > datetime.strptime("09:40", "%H:%M").time():
            raise WorkflowError("MORNING_REVIEW_DEADLINE_MISSED")
        pending = tuple(
            plan
            for lane_id in self.brokers
            for plan in self.store.list_execution_plans(
                lane_id=lane_id,
                status=PlanStatus.PENDING_MORNING_REVIEW,
            )
        )
        if not pending:
            return {
                "status": "READY",
                "reviewed_at": current.isoformat(),
                "activated": [],
                "reason_code": "NO_PENDING_MORNING_PLANS",
            }

        failures: list[dict[str, str]] = []
        evidence: dict[str, Any] = {}
        symbols = sorted({str(plan["symbol"]) for plan in pending})
        for symbol in symbols:
            # 09:26 is still the auction phase; the first closed continuous
            # 1m bar does not exist until 09:31.  Review the provider-owned,
            # timestamped auction quote instead of requiring an impossible
            # 09:26 minute bar.
            quote_result = self.market_data.fetch_quote(symbol, as_of=current)
            if not quote_result.complete or quote_result.quote is None:
                failures.append({"symbol": symbol, "reason_code": quote_result.reason_code})
                continue
            evidence[symbol] = quote_result.quote.model_dump(mode="json")

        for plan in pending:
            symbol = str(plan["symbol"])
            if symbol not in evidence:
                continue
            try:
                payload = json.loads(str(plan.get("payload_json") or "{}"))
                stop_level = float(payload["stop_level"])
                trigger_high = float(payload["trigger_high"])
                price = float(evidence[symbol]["price"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                failures.append({"symbol": symbol, "reason_code": "PLAN_PRICE_CONTRACT_INVALID"})
                continue
            if price <= stop_level:
                failures.append({"symbol": symbol, "reason_code": "PLAN_INVALIDATED_AT_OPEN"})
            elif price > trigger_high * 1.05:
                failures.append({"symbol": symbol, "reason_code": "OPEN_PRICE_CHASE_BLOCK"})

        if failures:
            payload = {
                "status": "BLOCKED",
                "reviewed_at": current.isoformat(),
                "activated": [],
                "failures": failures,
            }
            atomic_write_json(
                self.settings.workflow_output_dir / "runs" / f"{current.date()}-morning-review.json",
                payload,
            )
            return payload
        activated = self.store.activate_pending_plan_batch(
            [str(plan["plan_id"]) for plan in pending],
            valid_from=_at_time(current, 9, 32),
        )
        payload = {
            "status": "READY",
            "reviewed_at": current.isoformat(),
            "atomic": True,
            "activated": [str(plan["plan_id"]) for plan in activated],
            "evidence_symbols": symbols,
        }
        atomic_write_json(
            self.settings.workflow_output_dir / "runs" / f"{current.date()}-morning-review.json",
            payload,
        )
        return payload

    def run_due(self, *, now: datetime | None = None) -> dict[str, Any]:
        return self.run_scheduled(now=now)

    def run_scheduled(
        self,
        kind: ScheduleKind | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = _aware(now or datetime.now(SHANGHAI))
        scheduler = Scheduler(
            self.store,
            callbacks={
                ScheduleKind.MORNING_0925: lambda _job: self.review_pending_morning(now=current),
                ScheduleKind.CLOSE_1510: lambda _job: self.run_research(
                    "close",
                    as_of=current,
                    primary_only=True,
                    # The close slot is a downstream-only consumer.  A1 is
                    # maintained at 18:00 and must be present/within TTL;
                    # this callback may never fall back to an implicit A1.
                    schedule_comparison=False,
                    from_active_a1=True,
                ),
                ScheduleKind.MONITOR: lambda _job: self.monitor_once(now=current),
            },
            owner="liangjian-runtime",
            trading_day=self.trading_calendar.is_trading_day,
        )
        maintenance_payload: dict[str, Any] | None = None
        # A1 has a separate 18:00 maintenance slot and therefore does not
        # become another ``ScheduleKind`` in the intraday scheduler.  Attach a
        # leased maintenance receipt only for the all-due command; explicit
        # run-close/run-monitor calls remain isolated to their requested job.
        if kind is None:
            active_a1 = self.a1_registry.get_active_generation()
            plan = decide_a1_maintenance(
                current,
                self.trading_calendar.is_trading_day,
                has_active_generation=active_a1 is not None,
                active_full_period=_a1_full_period(active_a1),
            )
            if plan is not None:
                lease_name = "scheduler:a1-maintenance"
                acquired = self.store.acquire_lease(
                    lease_name,
                    "liangjian-runtime",
                    now=current,
                    ttl_seconds=90.0,
                    dispatch_key=plan.dispatch_key,
                )
                if not acquired:
                    maintenance_payload = {
                        "status": "LEASE_BUSY",
                        "mode": plan.mode,
                        "reason_code": "A1_MAINTENANCE_LEASE_BUSY",
                        "plan": plan.as_dict(),
                    }
                else:
                    maintenance = self.run_a1_maintenance(now=current, mode=plan.mode)
                    maintenance_payload = maintenance
                    if maintenance.get("status") in {"PUBLISHED", "NOOP"}:
                        self.store.complete_lease(
                            lease_name,
                            "liangjian-runtime",
                            dispatch_key=plan.dispatch_key,
                            now=current,
                        )
                    else:
                        self.store.release_lease(lease_name, "liangjian-runtime")
        records = scheduler.dispatch_once(current, kinds=(kind,) if kind is not None else None)
        payload: dict[str, Any] = {
            "time": current.isoformat(),
            "dispatch": [record.model_dump(mode="json") for record in records],
        }
        if maintenance_payload is not None:
            payload["a1_maintenance"] = maintenance_payload
        return payload

    def _research_input(
        self,
        *,
        frozen: FrozenInputSnapshot,
        universe: UniverseSnapshot,
        technical: Mapping[str, Any],
        g0_symbols: list[str],
        source_failures: Mapping[str, Any],
        raw_snapshot_path: Path,
        as_of: datetime,
        market_data_as_of: datetime | None = None,
        g0_selection: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Research/text evidence is allowed to use the wall-clock ``as_of``.
        # Every quote, flow, ladder, breadth and technical aggregate must bind
        # to the latest completed market session instead.  This distinction is
        # material during weekend/holiday next-session preparation: labelling
        # Friday provider rows as Sunday observations creates false point-in-
        # time evidence and bypasses the valid Friday cache.
        market_as_of = _aware(market_data_as_of or as_of)
        if market_as_of > _aware(as_of):
            raise WorkflowError("MARKET_DATA_AS_OF_IN_FUTURE")
        source_config = load_yaml(self.settings.source_config_path) if self.settings.source_config_path.is_file() else {}
        config_hash = digest_text(json.dumps(source_config, ensure_ascii=False, sort_keys=True, default=str))
        selected_records = [
            item.model_dump(mode="json")
            for item in frozen.g0_candidates
            if item.symbol in set(g0_symbols)
        ]
        trade_records = [item for item in selected_records if item.get("trade_eligible") is True]
        missing = {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"}
        try:
            broker_research_consensus = load_research_consensus(
                self.settings.research_consensus_dir,
                as_of=as_of,
            )
        except Exception:
            # Institutional strategy views are optional T2 context.  A malformed
            # or temporarily unreadable directory must not block the frozen
            # snapshot or get substituted with news/model prose.
            broker_research_consensus = unavailable_research_consensus(
                as_of=as_of,
                source_dir=getattr(self.settings, "research_consensus_dir", None),
            )
        try:
            a1_source_registry = load_a1_source_registry(
                self.settings.a1_research_source_registry_path
            )
        except A1SourceRegistryError as exc:
            a1_source_registry = None
            a1_source_context_error = exc.reason_code
        except Exception:
            a1_source_registry = None
            a1_source_context_error = "A1_SOURCE_REGISTRY_LOAD_FAILED"
        else:
            a1_source_context_error = None
        facts = frozen.fact_payload.get("facts", {})
        if not isinstance(facts, Mapping):
            facts = {}
        open_macro_bundle = frozen.fact_payload.get("open_macro_bundle")
        open_macro_bundle = open_macro_bundle if isinstance(open_macro_bundle, Mapping) else {}
        market_emotion = build_market_emotion(universe.records, facts, as_of=market_as_of)
        if market_emotion.get("available") is not True:
            raise WorkflowError("MARKET_EMOTION_AGGREGATE_NOT_READY")
        breadth = float(market_emotion["breadth"])
        auction = _available_fact(facts, "AUCTION_FINAL") if _auction_window(market_as_of) else None
        dragon_tiger = _available_fact(facts, "DRAGON_TIGER_LIST")
        hot_stocks = _available_fact(facts, "HOT_STOCK_LIST")
        industry_catalog = _available_fact(facts, "THS_INDUSTRY_CATALOG")
        industry_membership = _available_fact(facts, "THS_INDUSTRY_MEMBERSHIP")
        concept_catalog = _available_fact(facts, "THS_CONCEPT_CATALOG")
        concept_membership = _available_fact(facts, "THS_CONCEPT_MEMBERSHIP")
        disclosure_events = _official_event_snapshot(
            frozen.fact_payload,
            "DISCLOSURE_EVENT",
            g0_symbols,
        )
        risk_events = _official_event_snapshot(
            frozen.fact_payload,
            "RISK_EVENT",
            g0_symbols,
        )
        main_business_evidence = _main_business_evidence(disclosure_events, g0_symbols)
        macro_policy_feed = _macro_policy_feed(frozen.fact_payload)
        news_heat = build_news_heat_snapshot(
            frozen.fact_payload,
            g0_symbols,
            as_of=as_of,
        )
        industry_news_feed = {
            "available": news_heat.get("available") is True,
            "reason_code": news_heat.get("reason_code"),
            "as_of": news_heat.get("as_of"),
            "evidence_tier": "T3",
            "untrusted_text": True,
            "items": [
                item
                for item in news_heat.get("items", ())
                if isinstance(item, Mapping) and item.get("fact_type") == "INDUSTRY_RSS_ITEM"
            ],
        }
        crowding = build_crowding_snapshot(facts, g0_symbols, as_of=market_as_of)
        sector_cycle, sector_permissions = build_sector_cycle_and_permissions(
            facts,
            g0_symbols,
            as_of=market_as_of,
        )
        if self.settings.a2_capital_flow_enabled:
            capital_attempts: list[dict[str, Any]] = []
            try:
                capital_flow = collect_tencent_capital_flow(
                    as_of=market_as_of,
                    expected_symbols=g0_symbols,
                    cache_dir=self.settings.fact_store_dir / "a2_market" / "tencent",
                    minimum_coverage=self.settings.a2_capital_flow_minimum_coverage,
                    workers=self.settings.a2_capital_flow_workers,
                )
            except Exception:
                capital_flow = unavailable_capital_flow_snapshot(
                    as_of=market_as_of,
                    reason_code="TENCENT_CAPITAL_FLOW_NORMALIZATION_FAILED",
                    expected_symbols=g0_symbols,
                    source_id="TENCENT_QQ_FINANCE_FUND_FLOW",
                )
            capital_attempts.append(capital_flow)
            if capital_flow.get("available") is not True:
                try:
                    eastmoney_fallback = collect_eastmoney_capital_flow(
                        as_of=market_as_of,
                        expected_symbols=g0_symbols,
                        # Preserve the established point-in-time cache path;
                        # Tencent uses its own subdirectory so provider files
                        # can never overwrite each other.
                        cache_dir=self.settings.fact_store_dir / "a2_market",
                        minimum_coverage=self.settings.a2_capital_flow_minimum_coverage,
                    )
                except Exception:
                    eastmoney_fallback = unavailable_capital_flow_snapshot(
                        as_of=market_as_of,
                        reason_code="EASTMONEY_CAPITAL_FLOW_NORMALIZATION_FAILED",
                        expected_symbols=g0_symbols,
                    )
                capital_attempts.append(eastmoney_fallback)
                if eastmoney_fallback.get("available") is True:
                    capital_flow = eastmoney_fallback
            capital_flow = with_capital_flow_provider_attempts(capital_flow, capital_attempts)
        else:
            capital_flow = unavailable_capital_flow_snapshot(
                as_of=market_as_of,
                reason_code="SOURCE_NOT_CONFIGURED",
                expected_symbols=g0_symbols,
            )
        board_capital_flow: dict[str, Any] = {
            "schema_version": "a2-board-capital-flow-bundle/1.0.0",
            "as_of": market_as_of.isoformat(),
            "source_id": "EASTMONEY_BOARD_CAPITAL_FLOW",
            "available": False,
            "reason_code": "SOURCE_NOT_CONFIGURED",
            "by_taxonomy": {},
        }
        if self.settings.a2_capital_flow_enabled:
            observed = 0
            failures: list[str] = []
            for taxonomy in ("industry", "concept"):
                periods: dict[str, Any] = {}
                for period in ("today", "5d", "10d"):
                    try:
                        snapshot = collect_eastmoney_board_flow(
                            as_of=market_as_of,
                            board_type=taxonomy,
                            period=period,
                            cache_dir=self.settings.fact_store_dir / "a2_market",
                        )
                    except Exception:
                        snapshot = {
                            "available": False,
                            "availability_state": "SOURCE_UNAVAILABLE",
                            "reason_code": "BOARD_CAPITAL_FLOW_NORMALIZATION_FAILED",
                            "records": [],
                        }
                    periods[period] = snapshot
                    if snapshot.get("available") is True and snapshot.get("records"):
                        observed += 1
                    elif snapshot.get("available") is not True:
                        failures.append(f"{taxonomy}:{period}:{snapshot.get('reason_code') or 'SOURCE_UNAVAILABLE'}")
                board_capital_flow["by_taxonomy"][taxonomy] = periods
            board_capital_flow.update({
                "available": observed > 0,
                "reason_code": "OK" if observed == 6 else "PARTIAL_FACTS" if observed > 0 else "SOURCE_UNAVAILABLE",
                "observed_period_count": observed,
                "expected_period_count": 6,
                "failures": failures,
            })
        sector_health = build_sector_health_snapshot(
            facts,
            selected_records,
            as_of=market_as_of,
            symbols=g0_symbols,
            board_capital_flow_snapshot=board_capital_flow,
        )
        # Keep the existing A2 prompt placeholder authoritative while exposing
        # the new normalized contract as a separate frozen field.  No model or
        # selector is allowed to recompute these facts from a different scope.
        sector_cycle = {
            **sector_cycle,
            "sector_health_snapshot": sector_health,
        }
        a2_features = build_a2_feature_snapshot(
            candidates=selected_records,
            daily_bars={
                key: value
                for key, value in frozen.daily_payload.items()
                if key in g0_symbols and isinstance(value, list)
            },
            industry_membership=industry_membership,
            concept_membership=concept_membership,
            ladder_snapshot=_available_fact(facts, "LIMIT_UP_LADDER"),
            dragon_tiger_snapshot=dragon_tiger,
            attention_snapshot=hot_stocks,
            sector_cycle_snapshot=sector_cycle,
            capital_flow_snapshot=capital_flow,
            as_of=market_as_of,
        )
        regime_config = source_config.get("market_regime")
        regime_settings = regime_config if isinstance(regime_config, Mapping) else {}
        regime, regime_evidence = _determine_market_regime(
            market_emotion,
            sector_cycle,
            rotation_overlap_threshold=float(regime_settings.get("rotation_overlap_threshold", 0.40)),
        )
        regime_matrix = source_config.get("regime_parameter_matrix")
        if not isinstance(regime_matrix, Mapping):
            regime_matrix = source_config.get("regime_overrides", {})
        regime_parameters = regime_matrix.get(regime, {}) if isinstance(regime_matrix, Mapping) else {}
        if not isinstance(regime_parameters, Mapping):
            regime_parameters = {}
        exchange_rules = _exchange_rules_for(self.settings.exchange_rules_path, as_of)
        values: dict[str, Any] = {
            "G0_SCOPE_CONTRACT": _G0_SCOPE_CONTRACT,
            "DETERMINISTIC_RESEARCH_V2_ENABLED": self.settings.research_pipeline_mode == "deterministic_v2",
            "research_pipeline_mode": self.settings.research_pipeline_mode,
            "snapshot_manifest": {
                "as_of": as_of.isoformat(),
                "market_data_as_of": market_as_of.isoformat(),
                "frozen": True,
                "full_universe_count": len(universe.records),
                "research_universe_count": len(universe.research_candidates),
                "trade_universe_count": len(universe.trade_candidates),
                "selected_count": len(selected_records),
                "g0_selection": dict(g0_selection or {}),
                "source_checksums": frozen.source_checksums,
                "source_failures": source_failures,
                "raw_snapshot_hash": frozen.snapshot_hash,
                "raw_snapshot_path": str(raw_snapshot_path),
                "fact_snapshot_id": frozen.fact_payload.get("snapshot_id"),
                "fact_manifest_hash": frozen.fact_payload.get("manifest_hash"),
                "fact_store_relative_path": frozen.fact_payload.get("store_relative_path"),
                "fact_coverage": frozen.fact_payload.get("coverage_by_fact_type", {}),
                "open_macro": {
                    "schema_version": open_macro_bundle.get("schema_version"),
                    "content_hash": open_macro_bundle.get("content_hash"),
                    "cache_status": open_macro_bundle.get("cache_status"),
                    "reason_code": open_macro_bundle.get("reason_code"),
                    "quality": open_macro_bundle.get("quality"),
                },
            },
            "g0_symbols": g0_symbols,
            "g0_candidates": selected_records,
            "trade_candidates": trade_records,
            "universe_candidates": selected_records,
            "RECENT_DAILY_BARS": {
                key: value[-30:]
                for key, value in frozen.daily_payload.items()
                if key in g0_symbols and isinstance(value, list)
            },
            "COMPANY_FUNDAMENTALS": {key: value for key, value in frozen.fundamental_payload.items() if key in g0_symbols},
            "MAIN_BUSINESS_EVIDENCE": main_business_evidence,
            "FACTOR_SNAPSHOT": {
                key: {
                    item_key: item_value
                    for item_key, item_value in value.items()
                    if item_key not in {"kline_patterns", "price_levels"}
                }
                for key, value in technical.items()
                if key in g0_symbols and isinstance(value, Mapping)
            },
            "MARKET_REGIME_SNAPSHOT": {
                "regime": regime,
                "position_cap_pct": 0.5,
                "risk_warnings": [],
                "evidence": regime_evidence,
            },
            "MARKET_EMOTION_SNAPSHOT": market_emotion,
            "LIQUIDITY_SNAPSHOT": {item.symbol: {"turnover": item.amount} for item in universe.records if item.symbol in g0_symbols},
            "TRADABILITY_FLAGS": {
                item.symbol: {
                    "available": True,
                    "tradable": item.trade_eligible,
                    "simulation_only": True,
                    "exclusion_reasons": list(item.exclusion_reasons),
                    "source": "frozen_g0_universe",
                }
                for item in frozen.g0_candidates
                if item.symbol in g0_symbols
            },
            "EXCHANGE_RULES": exchange_rules,
            "DATA_SLA_POLICY": {"closed_bars_only": True, "fail_closed": True},
            "REGIME_PARAM_SET": dict(regime_parameters),
            "KLINE_PATTERNS": {
                key: value.get("kline_patterns", missing)
                for key, value in technical.items()
                if key in g0_symbols and isinstance(value, Mapping)
            },
            "PRICE_LEVELS": {
                key: value.get("price_levels", missing)
                for key, value in technical.items()
                if key in g0_symbols and isinstance(value, Mapping)
            },
            "MARKET_CONTEXT": {"regime": regime, "breadth": breadth, "regime_evidence": regime_evidence},
            "MARKET_DATA_AS_OF": market_as_of.isoformat(),
            "AUCTION_SNAPSHOT": auction or {
                "available": False,
                "reason_code": "OUTSIDE_0926_REVIEW_WINDOW" if not _auction_window(market_as_of) else "SOURCE_UNAVAILABLE",
            },
            "THS_INDUSTRY_CATALOG": industry_catalog or missing,
            "THS_INDUSTRY_MEMBERSHIP": industry_membership or missing,
            "THS_CONCEPT_CATALOG": concept_catalog or missing,
            "THS_CONCEPT_MEMBERSHIP": concept_membership or missing,
            "DRAGON_TIGER_SNAPSHOT": dragon_tiger or missing,
            "MARKET_ATTENTION_SNAPSHOT": hot_stocks or missing,
            "DISCLOSURE_EVENTS": disclosure_events,
            "RISK_EVENTS": risk_events,
            "MACRO_POLICY_FEED": macro_policy_feed,
            "MACRO_ECONOMIC_DATA": _supplemental_contract(open_macro_bundle, "MACRO_ECONOMIC_DATA") or {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "required_series": ["GDP", "CPI", "PPI", "PMI", "SOCIAL_FINANCING", "NEW_LOANS"],
                "substitution_forbidden": True,
            },
            "ASSET_ROTATION_SNAPSHOT": _supplemental_contract(open_macro_bundle, "ASSET_ROTATION_SNAPSHOT") or {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "required_assets": ["EQUITY", "GOLD", "BOND", "CASH"],
                "required_factors": ["MOMENTUM_20D", "MOMENTUM_60D", "FUND_FLOW"],
            },
            "GLOBAL_MACRO_SNAPSHOT": _supplemental_contract(open_macro_bundle, "GLOBAL_MACRO_SNAPSHOT") or {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "required_series": ["USD_INDEX", "FED_EASING_EXPECTATION", "US_RATE", "CN_RATE"],
            },
            "CROSS_MARKET_LEAD_SNAPSHOT": _supplemental_contract(open_macro_bundle, "CROSS_MARKET_LEAD_SNAPSHOT") or {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "required_markets": ["US", "KOREA", "TAIWAN", "JAPAN"],
            },
            "INDUSTRY_ACTIVITY_DATA": _supplemental_contract(open_macro_bundle, "INDUSTRY_ACTIVITY_DATA") or {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "metric_scope": "INDUSTRIAL_VALUE_ADDED_GROWTH_NOT_PROFIT",
            },
            "BROKER_RESEARCH_CONSENSUS": broker_research_consensus,
            "A2_RESEARCH_HYPOTHESES": project_a2_research_hypotheses(
                broker_research_consensus
            ),
            "INDUSTRY_NEWS_FEED": industry_news_feed,
            "NEWS_HEAT_SNAPSHOT": news_heat,
            "CROWDING_SNAPSHOT": crowding,
            "SECTOR_CYCLE_SNAPSHOT": sector_cycle,
            "A2_SECTOR_HEALTH_SNAPSHOT": sector_health,
            "SECTOR_PERMISSIONS": sector_permissions,
            "CAPITAL_FLOW_SNAPSHOT": capital_flow,
            "BOARD_CAPITAL_FLOW_SNAPSHOT": board_capital_flow,
            "A2_FACTOR_SNAPSHOT": a2_features,
            "A2_THEME_METRICS": {
                "available": a2_features.get("available") is True,
                "reason_code": a2_features.get("reason_code"),
                "as_of": a2_features.get("as_of"),
                "content_hash": a2_features.get("content_hash"),
                "theme_metrics": a2_features.get("theme_metrics", {}),
            },
            "TIER_STRUCTURE_SNAPSHOT": {
                "available": a2_features.get("available") is True,
                "reason_code": a2_features.get("reason_code"),
                "as_of": a2_features.get("as_of"),
                "by_symbol": {
                    symbol: row.get("tier_structure", {})
                    for symbol, row in a2_features.get("by_symbol", {}).items()
                    if isinstance(row, Mapping)
                },
            },
            "config_version": source_config.get("version")
            or source_config.get("funnel_version")
            or "funnel-config-v2",
            "config_hash": config_hash,
        }
        if a1_source_registry is None:
            values["A1_RESEARCH_SOURCE_CONTEXT"] = unavailable_a1_source_context(
                a1_source_context_error or "A1_SOURCE_REGISTRY_LOAD_FAILED"
            )
        else:
            values["A1_RESEARCH_SOURCE_CONTEXT"] = build_a1_source_context(
                a1_source_registry,
                snapshot=values,
                research_consensus=broker_research_consensus,
            )
        values["snapshot_manifest"]["a1_research_sources"] = {
            "schema_version": values["A1_RESEARCH_SOURCE_CONTEXT"].get("schema_version"),
            "content_hash": values["A1_RESEARCH_SOURCE_CONTEXT"].get("content_hash"),
            "reason_code": values["A1_RESEARCH_SOURCE_CONTEXT"].get("reason_code"),
            "source_count": values["A1_RESEARCH_SOURCE_CONTEXT"].get("source_count", 0),
            "usable_source_count": values["A1_RESEARCH_SOURCE_CONTEXT"].get(
                "usable_source_count", 0
            ),
        }
        for key in (
            "MACRO_POLICY_FEED", "MACRO_ECONOMIC_DATA", "ASSET_ROTATION_SNAPSHOT", "GLOBAL_MACRO_SNAPSHOT", "CROSS_MARKET_LEAD_SNAPSHOT", "BROKER_RESEARCH_CONSENSUS", "A1_RESEARCH_SOURCE_CONTEXT", "A2_RESEARCH_HYPOTHESES", "INDUSTRY_NEWS_FEED", "INDUSTRY_ACTIVITY_DATA", "INDUSTRY_PROFIT_DATA", "THS_INDUSTRY_MEMBERSHIP", "THS_CONCEPT_MEMBERSHIP", "EXISTING_CHAIN_GRAPH",
            "THEME_REGISTRY", "DISCLOSURE_EVENTS", "RISK_EVENTS", "RESEARCH_CONSENSUS", "FUND_HOLDINGS",
            "FAST_TRACK_REQUESTS", "PRIOR_OUTCOME_FEEDBACK", "SECTOR_CYCLE_SNAPSHOT", "CAPITAL_FLOW_SNAPSHOT",
            "NEWS_HEAT_SNAPSHOT", "CROWDING_SNAPSHOT", "A2_SECTOR_HEALTH_SNAPSHOT", "BOARD_CAPITAL_FLOW_SNAPSHOT", "AUCTION_SNAPSHOT", "SECTOR_PERMISSIONS",
        ):
            values.setdefault(key, missing)
        values.update(_prompt_parameters(source_config))
        # Regime parameters are executable policy, not prompt-only metadata.
        # Apply the stricter per-regime A3 floor to the server-owned technical
        # gate so the model, deterministic scorer and published constraints all
        # evaluate the same reward/risk contract.
        _apply_effective_regime_parameters(values, regime_parameters)
        return values

    def _publish_plans(
        self,
        result: ResearchRunResult,
        slot: str,
        now: datetime,
        *,
        snapshot_data: Mapping[str, Any],
        minimum_trade_date: date | None = None,
    ) -> dict[str, Any]:
        batch: list[dict[str, Any]] = []
        blocked: list[dict[str, str]] = []
        ready_lanes: list[str] = []
        comparison_lanes: list[dict[str, str]] = []
        settings = getattr(self, "settings", None)
        primary_lane = str(getattr(settings, "research_primary_lane_id", "lane_1"))
        publish_comparisons = bool(getattr(settings, "publish_comparison_lanes", False))
        tradability_flags = snapshot_data.get("TRADABILITY_FLAGS")
        if not isinstance(tradability_flags, Mapping):
            tradability_flags = {}
        for lane in result.lanes:
            if lane.lane != primary_lane and not publish_comparisons:
                comparison_lanes.append({
                    "lane": lane.lane,
                    "status": lane.status,
                    "publication": "COMPARISON_ONLY",
                })
                continue
            if lane.status not in {"READY", "READY_DEGRADED"} or not isinstance(lane.final_output, Mapping):
                blocked.append({"lane": lane.lane, "reason": "LANE_NOT_READY"})
                continue
            core_plans = lane.final_output.get("core_watch_pool")
            secondary_plans = lane.final_output.get("secondary_watch_pool")
            if not isinstance(core_plans, list) and not isinstance(secondary_plans, list):
                blocked.append({"lane": lane.lane, "reason": "A3_PLAN_POOLS_MISSING"})
                continue
            ready_lanes.append(lane.lane)
            previous = {
                str(item["symbol"]): item
                for item in self.store.list_execution_plans(lane_id=lane.lane, status=PlanStatus.PENDING_MORNING_REVIEW)
            }
            for pool_name, plans in (
                ("core_watch_pool", core_plans),
                ("secondary_watch_pool", secondary_plans),
            ):
                if not isinstance(plans, list):
                    continue
                for raw in plans:
                    if not isinstance(raw, Mapping):
                        continue
                    payload = {
                        **_plan_payload(raw),
                        "source_run_id": result.run_id,
                    }
                    symbol = payload.get("symbol")
                    if pool_name == "secondary_watch_pool":
                        blocked.append({
                            "lane": lane.lane,
                            "symbol": symbol or "-",
                            "reason": "A3_SECONDARY_NON_EXECUTABLE",
                        })
                        continue
                    strategy_profile = str(raw.get("strategy_profile") or "").strip().upper()
                    if (
                        str(raw.get("eligibility") or "").strip().upper() != "QUALIFIED"
                        or strategy_profile not in {"LEADER_INTRADAY", "MA520_SWING", "TREND_MA5"}
                        or str(raw.get("review_status") or "PASS").strip().upper() != "PASS"
                    ):
                        blocked.append({
                            "lane": lane.lane,
                            "symbol": symbol or "-",
                            "reason": "A3_STRATEGY_CONTRACT_NOT_EXECUTABLE",
                        })
                        continue
                    if (
                        not symbol
                        or payload.get("risk_unit") == "NO_ENTRY"
                        or payload.get("trigger_low") is None
                        or payload.get("trigger_high") is None
                        or payload.get("stop_level") is None
                    ):
                        blocked.append({"lane": lane.lane, "symbol": symbol or "-", "reason": "PLAN_NOT_EXECUTABLE"})
                        continue
                    flag = tradability_flags.get(symbol)
                    if not isinstance(flag, Mapping):
                        blocked.append(
                            {"lane": lane.lane, "symbol": symbol, "reason": "PLAN_TRADABILITY_EVIDENCE_MISSING"}
                        )
                        continue
                    if flag.get("tradable") is not True:
                        blocked.append({"lane": lane.lane, "symbol": symbol, "reason": "PLAN_SYMBOL_NOT_TRADABLE"})
                        continue
                    logical = str(raw.get("plan_id") or _hash_json(raw)[:16])
                    plan_id = f"{result.run_id}:{lane.lane}:{logical}"
                    if slot == "close":
                        batch.append(
                            {
                                "plan_id": plan_id,
                                "lane_id": lane.lane,
                                "symbol": symbol,
                                "status": PlanStatus.PENDING_MORNING_REVIEW.value,
                                "expires_at": _plan_expiry(
                                    payload.get("plan_expiry"),
                                    now,
                                    slot,
                                    minimum_trade_date=minimum_trade_date,
                                ),
                                "payload": payload,
                            }
                        )
                        continue
                    parent = previous.get(symbol)
                    if now.time().replace(tzinfo=None) > datetime.strptime("09:40", "%H:%M").time():
                        blocked.append({"lane": lane.lane, "symbol": symbol, "reason": "MORNING_PUBLICATION_DEADLINE"})
                        continue
                    if parent is None or not _tightens(parent, payload):
                        blocked.append({"lane": lane.lane, "symbol": symbol, "reason": "MORNING_NOT_TIGHTEN_ONLY"})
                        continue
                    batch.append(
                        {
                            "plan_id": plan_id,
                            "lane_id": lane.lane,
                            "symbol": symbol,
                            "status": PlanStatus.ACTIVE_TODAY.value,
                            "valid_from": _at_time(now, 9, 32),
                            "expires_at": _plan_expiry(
                                payload.get("plan_expiry"),
                                now,
                                slot,
                                minimum_trade_date=minimum_trade_date,
                            ),
                            "payload": payload,
                            "parent_plan_id": str(parent["plan_id"]),
                        }
                    )
        published = self.store.publish_plan_batch(
            batch,
            expire_active_lanes=ready_lanes if slot == "close" else (),
        )
        self.store.mark_workflow_runs_published(result.run_id, ready_lanes)
        created = [str(item["plan_id"]) for item in published]
        activated = [
            str(item["plan_id"])
            for item in published
            if item.get("status") == PlanStatus.ACTIVE_TODAY.value
        ]
        return {
            "atomic": True,
            "created": created,
            "activated": activated,
            "blocked": blocked,
            "primary_lane": primary_lane,
            "comparison_lanes": comparison_lanes,
        }

    def _a4_callback(
        self,
        lane_id: str,
        plans: tuple[dict[str, Any], ...],
        market_context: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ):
        if not plans:
            return None
        bundle = self.prompts.bundle()
        plan_by_id = {str(plan["plan_id"]): plan for plan in plans}

        def callback(context: Mapping[str, Any]) -> Mapping[str, Any]:
            contexts = context.get("plans") if isinstance(context.get("plans"), (list, tuple)) else [context]
            replacements = {name: None for name in bundle.document(_A4_FILE).placeholders}
            prompt_plans = [_a4_prompt_plan(plan) for plan in plans]
            replacements.update(
                {
                    "EXECUTION_PLANS": prompt_plans,
                    "TRIGGER_ENGINE_RESULT": contexts,
                    "MARKET_CONTEXT": {
                        "time": now.isoformat(),
                        "minute_snapshot_id": context.get("minute_snapshot_id"),
                        "symbols": market_context,
                    },
                    "EXCHANGE_RULES": _exchange_rules_for(self.settings.exchange_rules_path, now),
                    "CURRENT_TIME": now.isoformat(),
                }
            )
            system = bundle.render(_A4_FILE, replacements)
            model_result: ModelCallResult = self.monitor_model_client.complete(
                self.settings.monitor_model,
                [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps({"lane_id": lane_id, "plans": contexts}, ensure_ascii=False, default=str)},
                ],
                prompt_hash=digest_text(system),
                input_hash=_hash_json(contexts),
                snapshot_id=f"a4-{now.isoformat()}",
                stage="A4",
            )
            veto_by_plan: dict[str, bool] = {
                str(item.get("plan_id")): True
                for item in contexts
                if isinstance(item, Mapping) and item.get("eligible") is True and item.get("plan_id")
            }
            signals = model_result.output.get("signals")
            if not isinstance(signals, list):
                raise WorkflowError("A4_SIGNALS_INVALID")
            for signal in signals:
                if not isinstance(signal, Mapping):
                    continue
                plan_id = str(signal.get("plan_id") or "")
                if plan_id in plan_by_id:
                    veto_by_plan[plan_id] = bool(signal.get("llm_veto", True))
            return {"vetoes": veto_by_plan}

        return callback

    def _settle_prior_signals(self, lane_id: str, symbol: str, bar: MinuteBar) -> list[dict[str, Any]]:
        broker = self.brokers[lane_id]
        results: list[dict[str, Any]] = []
        for event in self.store.list_monitor_events(lane_id=lane_id, effective_only=True):
            if event.get("action") not in {
                MonitorAction.BUY_SIGNAL.value,
                MonitorAction.ADD_SIGNAL.value,
                MonitorAction.SELL_SIGNAL.value,
                MonitorAction.REDUCE_SIGNAL.value,
                MonitorAction.FORCED_RISK_EXIT.value,
            }:
                continue
            payload = json.loads(event.get("payload_json") or "{}")
            if payload.get("symbol") != symbol:
                continue
            plan = self.store.get_execution_plan(str(payload.get("plan_id") or ""))
            plan_payload = json.loads(plan.get("payload_json") or "{}") if plan else {}
            action = {
                MonitorAction.BUY_SIGNAL.value: "BUY",
                MonitorAction.ADD_SIGNAL.value: "ADD",
                MonitorAction.SELL_SIGNAL.value: "SELL",
                MonitorAction.REDUCE_SIGNAL.value: "REDUCE",
                MonitorAction.FORCED_RISK_EXIT.value: "FORCED_RISK_EXIT",
            }[str(event["action"])]
            signal_time = datetime.fromisoformat(str(event["minute_end"]))
            if bar.bar_end != _next_closed_minute(signal_time):
                continue
            simulation_action = SimulationAction(
                account_id=f"paper:{lane_id}",
                signal_id=str(event["event_key"]),
                symbol=symbol,
                action=action,
                signal_bar_end=signal_time,
                entry_reference=(
                    plan_payload.get("trigger_low")
                    if action in {"BUY", "ADD"}
                    else bar.open
                ) or bar.open,
                stop_level=plan_payload.get("stop_level"),
                risk_unit=0.33 if plan_payload.get("risk_unit") == "PROBE" else 1.0,
                plan_id=payload.get("plan_id"),
            )
            intent_key = f"{simulation_action.account_id}:{simulation_action.signal_id}:{simulation_action.action.value}"
            if self.store.get_fill_by_intent_key(intent_key) is not None:
                continue
            outcome = broker.apply(simulation_action, bar)
            results.append(outcome.model_dump(mode="json"))
        return results

    def _ensure_trading_day(self, current: datetime, *, synchronize_accounts: bool = True) -> None:
        try:
            is_session = self.trading_calendar.is_trading_day(current.date())
        except TradingCalendarError as exc:
            raise WorkflowError(exc.reason_code) from exc
        if not is_session:
            raise WorkflowError("NON_TRADING_DAY")
        if synchronize_accounts:
            for broker in self.brokers.values():
                broker.start_trading_day(current.date())


def _a4_prompt_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Project one durable plan into the bounded A4 veto contract.

    The state row may contain the full A1-A3 lineage in ``payload_json``.
    A4 needs only immutable execution constraints and compact sector context;
    sending the complete research payload increases latency without granting
    the veto model any additional authority.
    """

    try:
        payload = json.loads(str(plan.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "plan_id": str(plan.get("plan_id") or ""),
        "lane_id": str(plan.get("lane_id") or ""),
        "symbol": str(plan.get("symbol") or payload.get("symbol") or ""),
        "name": str(payload.get("name") or ""),
        "status": str(plan.get("status") or ""),
        "valid_from": plan.get("valid_from"),
        "expires_at": plan.get("expires_at"),
        "setup_type": payload.get("setup_type"),
        "strategy_profile": payload.get("strategy_profile"),
        "strategy_version": payload.get("strategy_version"),
        "eligibility": payload.get("eligibility"),
        "trigger_low": payload.get("trigger_low"),
        "trigger_high": payload.get("trigger_high"),
        "stop_level": payload.get("stop_level"),
        "no_chase": payload.get("no_chase", payload.get("no_chase_price")),
        "confirmation_bars": payload.get("confirmation_bars", payload.get("confirm_bars")),
        "confirmation_conditions": payload.get("confirmation_conditions"),
        "required_conditions": payload.get("required_conditions"),
        "met_conditions": payload.get("met_conditions"),
        "unmet_conditions": payload.get("unmet_conditions"),
        "veto_conditions": payload.get("veto_conditions"),
        "a4_required_entry_rules": payload.get("a4_required_entry_rules"),
        "a4_exit_rules": payload.get("a4_exit_rules"),
        "sector_context": payload.get("sector_context"),
    }


def _next_closed_minute(value: datetime) -> datetime:
    """Return the only bar eligible to settle an A4 signal."""

    current = _aware(value).replace(second=0, microsecond=0)
    clock = current.time().replace(tzinfo=None)
    if clock == datetime.strptime("11:30", "%H:%M").time():
        return current.replace(hour=13, minute=1)
    return current + timedelta(minutes=1)


def _intraday_market_context(
    symbol: str,
    one_minute: tuple[MinuteBar, ...],
    five_minute: tuple[MinuteBar, ...],
    *,
    current: datetime,
) -> dict[str, Any]:
    """Build bounded deterministic A4 evidence from closed bars only."""

    current_bar = one_minute[-1] if one_minute and one_minute[-1].bar_end == current else None

    def compact(bars: tuple[MinuteBar, ...]) -> list[dict[str, Any]]:
        return [bar.model_dump(mode="json") for bar in bars[-21:]]

    fifteen_minute = _aggregate_closed_15m(five_minute)

    def statistics(bars: list[Mapping[str, Any]]) -> dict[str, Any]:
        closes = [float(bar["close"]) for bar in bars]
        total_volume = sum(float(bar["volume"]) for bar in bars)
        total_amount = sum(float(bar["amount"]) for bar in bars)
        return {
            "available": bool(bars),
            "ma5": sum(closes[-5:]) / 5 if len(closes) >= 5 else None,
            "ma20": sum(closes[-20:]) / 20 if len(closes) >= 20 else None,
            "vwap": total_amount / total_volume if total_volume > 0 else None,
            "closed_bar_count": len(bars),
        }

    return {
        "symbol": symbol,
        "as_of": current.isoformat(),
        "realtime_quote": (
            current_bar.model_dump(mode="json")
            if current_bar is not None
            else {"available": False, "reason_code": "CURRENT_1M_BAR_UNAVAILABLE"}
        ),
        "closed_bars": {
            "1m": compact(one_minute),
            "5m": compact(five_minute),
            "15m": fifteen_minute[-21:],
        },
        "moving_averages": {
            "1m": statistics(compact(one_minute)),
            "5m": statistics(compact(five_minute)),
            "15m": statistics(fifteen_minute),
        },
        "tradability": {
            "available": current_bar is not None,
            "tradable": current_bar is not None and current_bar.volume > 0,
            "reason_code": (
                "CURRENT_1M_BAR_AVAILABLE"
                if current_bar is not None and current_bar.volume > 0
                else "CURRENT_1M_BAR_ZERO_VOLUME"
                if current_bar is not None
                else "CURRENT_1M_BAR_UNAVAILABLE"
            ),
            "source": "mootdx_closed_1m",
        },
    }


def _aggregate_closed_15m(bars: tuple[MinuteBar, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int], list[MinuteBar]] = {}
    for bar in sorted(bars, key=lambda item: item.bar_end):
        value = bar.bar_end.astimezone(SHANGHAI)
        clock = value.time().replace(tzinfo=None)
        if datetime.strptime("09:30", "%H:%M").time() < clock <= datetime.strptime("11:30", "%H:%M").time():
            origin = value.replace(hour=9, minute=30)
            session = "AM"
        elif datetime.strptime("13:00", "%H:%M").time() < clock <= datetime.strptime("15:00", "%H:%M").time():
            origin = value.replace(hour=13, minute=0)
            session = "PM"
        else:
            continue
        elapsed = int((value - origin).total_seconds() // 60)
        bucket = (elapsed + 14) // 15
        groups.setdefault((value.date().isoformat(), session, bucket), []).append(bar)
    result: list[dict[str, Any]] = []
    for key in sorted(groups):
        group = sorted(groups[key], key=lambda item: item.bar_end)
        if len(group) != 3:
            continue
        result.append(
            {
                "symbol": group[-1].symbol,
                "interval": "15m",
                "bar_end": group[-1].bar_end.isoformat(),
                "open": group[0].open,
                "high": max(item.high for item in group),
                "low": min(item.low for item in group),
                "close": group[-1].close,
                "volume": sum(item.volume for item in group),
                "amount": sum(item.amount for item in group),
                "source_id": "DERIVED:MOOTDX_CLOSED_5M",
                "adjust_mode": "none",
            }
        )
    return result


def _exchange_rules_for(path: Path, as_of: datetime) -> dict[str, Any]:
    if not path.is_file():
        raise WorkflowError("EXCHANGE_RULE_SNAPSHOT_MISSING")
    try:
        rules = load_yaml(path)
        effective = datetime.fromisoformat(str(rules["effective_from"])).date()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise WorkflowError("EXCHANGE_RULE_SNAPSHOT_INVALID") from exc
    sources = rules.get("sources")
    if (
        rules.get("schema_version") != "liangjian-exchange-rules/1.0.0"
        or not isinstance(rules.get("snapshot_id"), str)
        or rules.get("simulation_only") is not True
        or rules.get("external_orders") is not False
        or rules.get("t_plus_one") is not True
        or int(rules.get("lot_size", 0)) != 100
        or not isinstance(sources, Mapping)
        or set(sources) != {"sse", "szse", "bse"}
    ):
        raise WorkflowError("EXCHANGE_RULE_SNAPSHOT_INVALID")
    if effective > as_of.astimezone(SHANGHAI).date():
        raise WorkflowError("EXCHANGE_RULE_SNAPSHOT_NOT_EFFECTIVE")
    return dict(rules)


def _prompt_parameters(config: Mapping[str, Any]) -> dict[str, Any]:
    a1 = config.get("agent_1", {}) if isinstance(config.get("agent_1"), Mapping) else {}
    a2 = config.get("agent_2", {}) if isinstance(config.get("agent_2"), Mapping) else {}
    a3 = config.get("agent_3", {}) if isinstance(config.get("agent_3"), Mapping) else {}
    data_policy = config.get("data_policy", {}) if isinstance(config.get("data_policy"), Mapping) else {}
    market_regime = config.get("market_regime", {}) if isinstance(config.get("market_regime"), Mapping) else {}
    theme_registry = config.get("theme_registry", {}) if isinstance(config.get("theme_registry"), Mapping) else {}
    a1_policy = a1.get("policy_research", {}) if isinstance(a1.get("policy_research"), Mapping) else {}
    a1_fast_track = a1.get("fast_track", {}) if isinstance(a1.get("fast_track"), Mapping) else {}
    a2_selection = a2.get("stock_selection", {}) if isinstance(a2.get("stock_selection"), Mapping) else {}
    a3_ma = a3.get("moving_average_system", {}) if isinstance(a3.get("moving_average_system"), Mapping) else {}
    a3_blackout = a3.get("earnings_blackout", {}) if isinstance(a3.get("earnings_blackout"), Mapping) else {}
    return {
        "TOP_N_PER_NODE": a1.get("top_n_per_node", 8),
        "A1_POOL_TARGETS": {
            "pool_min": a1.get("pool_min", 300),
            "pool_max": a1.get("pool_max", 1000),
            "clue_pool_target": a1.get("clue_pool_target", [300, 800]),
            "active_research_target": a1.get("active_research_target", [100, 250]),
            "node_count_target": a1.get("node_count_target", [40, 80]),
            "quota_forbidden": True,
        },
        "A2_POOL_TARGETS": _pool_targets(a2.get("candidate_pool_target"), default=(100, 200)),
        "A3_POOL_TARGETS": {
            "core_watch": _pool_targets(a3.get("core_watch_target"), default=(5, 10)),
            "shadow_watch": _pool_targets(a3.get("shadow_watch_target"), default=(3, 8)),
        },
        "A1_MINIMUMS": {
            "structural_score": a1.get("minimum_score", 65),
            "data_quality_score": a1.get("minimum_data_quality", 75),
            "evidence_confidence": a1.get("minimum_evidence_confidence", 0.70),
            "minimum_available_weight": a1.get("minimum_available_weight", 0.70),
        },
        "A1_DRIVER_LINEAGE_REQUIRED": True,
        "STRICT_AGENT_RULES": True,
        "PRIOR_CONTRIBUTION_CAP": theme_registry.get("prior_contribution_cap", 10),
        "THEME_EXPIRY_DAYS": theme_registry.get("theme_expiry_without_confirmation_days", 10),
        "POLICY_CALENDAR_HORIZON_DAYS": a1_policy.get("policy_calendar_horizon_days", 90),
        "BOTTLENECK_MIN_EVIDENCE": a1.get("bottleneck_minimum_evidence_classes", 3),
        "SCORE_WEIGHTS": a1.get("score_weights", {}),
        "FAST_TRACK_DAILY_QUOTA": 0,
        "FAST_TRACK_COOLDOWN_DAYS": a1_fast_track.get("cooldown_days", 30),
        "CLIMAX_NEW_ENTRY_POLICY": a2.get("climax_new_entry_policy", "WATCH_ONLY"),
        "DIVERGENCE_NEW_ENTRY_POLICY": a2.get("divergence_new_entry_policy", "NO_NEW_ENTRY"),
        "MIN_SECTOR_COVERAGE": data_policy.get("minimum_sector_coverage", 0.80),
        "ROTATION_LOOKBACK_DAYS": market_regime.get("rotation_lookback_days", 5),
        "LEADER_MIN_CRITERIA": a2_selection.get("leader_min_criteria", 4),
        "LOW_IDENTITY_TRIGGER_COUNT": a2_selection.get("low_identity_trigger_count", 2),
        "MIN_FREE_FLOAT_CAP": a2_selection.get("min_free_float_cap_cny", 3_000_000_000),
        "MIN_IDENTIFIABILITY_SCORE": a2_selection.get("min_identifiability_score", 60),
        "MAX_LEADERS_PER_THEME": a2_selection.get("max_leaders_per_theme", 2),
        "MIN_THEME_SCORE": a2.get("minimum_theme_score", 60),
        "THEME_SCORE_WEIGHTS": a2.get("score_weights", {}),
        "A2_FACTOR_COVERAGE_MINIMUM": a2.get("factor_coverage_minimum", 0.65),
        "PENALTY_RULES": a2.get("penalty_rules", {}),
        "MIN_REWARD_RISK": a3.get("minimum_reward_risk", 2.0),
        "MAX_STOP_DISTANCE": a3.get("max_stop_distance_pct", 0.06),
        "A3_STRATEGY_VERSION": a3.get("strategy_version", "a3-a4-three-strategy/1.1.0"),
        "A3_ALLOWED_STRATEGIES": a3.get(
            "allowed_strategy_profiles",
            ["LEADER_INTRADAY", "MA520_SWING", "TREND_MA5"],
        ),
        "A3_DECISION_TIMEFRAMES": a3.get(
            "decision_timeframes",
            ["MONTHLY_CLOSED", "WEEKLY_CLOSED", "DAILY_CLOSED"],
        ),
        "EARNINGS_BLACKOUT": {
            "days_before": a3_blackout.get("days_before", 3),
            "days_after": a3_blackout.get("days_after", 1),
            "action": a3_blackout.get("action", "FORCE_PROBE"),
        },
        "NORMAL_GAP_RANGE": [-0.02, 0.03],
        "NO_CHASE_THRESHOLD": 0.05,
    }


def _pool_targets(value: Any, *, default: tuple[int, int]) -> dict[str, Any]:
    """Normalize advisory pool capacity without turning it into a quota."""

    values = value if isinstance(value, list) else list(default)
    if len(values) != 2 or any(isinstance(item, bool) or not isinstance(item, int) for item in values):
        values = list(default)
    minimum, maximum = values
    if minimum < 0 or maximum < minimum:
        minimum, maximum = default
    return {"pool_min": minimum, "pool_max": maximum, "quota_forbidden": True}


def _apply_effective_regime_parameters(
    values: dict[str, Any],
    regime_parameters: Mapping[str, Any],
) -> None:
    """Project stricter regime policy into server-owned execution fields."""

    agent_3_regime = regime_parameters.get("agent_3")
    if not isinstance(agent_3_regime, Mapping):
        return
    regime_minimum_rr = _workflow_float(agent_3_regime.get("minimum_reward_risk"))
    if regime_minimum_rr is None or regime_minimum_rr <= 0:
        return
    base_minimum_rr = _workflow_float(values.get("MIN_REWARD_RISK")) or 2.0
    values["MIN_REWARD_RISK"] = max(base_minimum_rr, regime_minimum_rr)


def _plan_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _canonical_symbol(raw.get("symbol"))
    zone = raw.get("trigger_zone") if isinstance(raw.get("trigger_zone"), Mapping) else {}
    return {
        **dict(raw),
        "symbol": symbol,
        "trigger_low": _float_or_none(zone.get("low")),
        "trigger_high": _float_or_none(zone.get("high")),
        "stop_level": _float_or_none(raw.get("invalidation_level")),
        "no_chase": _float_or_none(raw.get("no_chase_price")),
        # Strategy plans own confirmation at closed 15m/5m granularity.  The
        # legacy consecutive-1m counter is retained only for old plans that
        # have no strategy_profile.
        "confirmation_bars": 1 if raw.get("strategy_profile") else 2,
        "action": MonitorAction.BUY_SIGNAL.value,
    }


def _workflow_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _tightens(parent: Mapping[str, Any], new_payload: Mapping[str, Any]) -> bool:
    try:
        old = json.loads(parent.get("payload_json") or "{}")
        return (
            float(new_payload["trigger_low"]) >= float(old["trigger_low"])
            and float(new_payload["trigger_high"]) <= float(old["trigger_high"])
            and {"NO_ENTRY": 0, "PROBE": 1, "STANDARD": 2}.get(str(new_payload.get("risk_unit")), 0)
            <= {"NO_ENTRY": 0, "PROBE": 1, "STANDARD": 2}.get(str(old.get("risk_unit")), 0)
        )
    except (KeyError, TypeError, ValueError):
        return False


def _canonical_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text.startswith(("SHSE.", "SZSE.", "BJSE.")):
        exchange, code = text.split(".", 1)
        suffix = {"SHSE": "SH", "SZSE": "SZ", "BJSE": "BJ"}[exchange]
        text = f"{code}.{suffix}"
    try:
        return map_symbol(text).canonical
    except Exception:
        return None


def _market_snapshot_with_closed_turnover(
    market: HithinkFetchResult,
    *,
    cache: LocalFactCache,
    cutoff: datetime,
) -> HithinkFetchResult:
    """Use the prior closed daily amount for a research run started intraday."""

    symbols: list[str] = []
    row_symbols: list[str | None] = []
    for row in market.items:
        data = row.model_dump(mode="python")
        symbol = None
        for key in ("thscode", "ths_code", "thsCode", "symbol", "ticker", "security_code", "code"):
            if data.get(key) not in (None, ""):
                symbol = _canonical_symbol(data[key])
                if symbol:
                    break
        row_symbols.append(symbol)
        if symbol:
            symbols.append(symbol)
    closed = cache.latest_daily_bars_before(symbols, end=cutoff, adjust="none")
    updated = []
    overrides = 0
    for row, symbol in zip(market.items, row_symbols, strict=True):
        bar = closed.get(symbol or "")
        payload = bar.get("payload") if isinstance(bar, Mapping) else None
        turnover = payload.get("turnover") if isinstance(payload, Mapping) else None
        if not isinstance(turnover, (int, float)) or isinstance(turnover, bool) or turnover < 0:
            updated.append(row)
            continue
        data = row.model_dump(mode="python")
        data["amount"] = float(turnover)
        data["turnover"] = float(turnover)
        updated.append(row.__class__.model_validate(data))
        overrides += 1
    metadata = {
        **market.metadata,
        "turnover_metric": "LATEST_CLOSED_DAILY_BAR",
        "turnover_cutoff": cutoff.isoformat(),
        "turnover_override_count": overrides,
    }
    return market.model_copy(update={"items": tuple(updated), "metadata": metadata})


def _row_change(item: Any, _frozen: FrozenInputSnapshot) -> float:
    value = getattr(item, "change_ratio_pct", None)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def _research_universe_records(universe: UniverseSnapshot) -> tuple[Any, ...]:
    """Return the configured A1 quality domain, retaining eligible BJ stocks."""

    records = universe.research_candidates
    if len(records) != universe.lineage.research_candidate_count or not records:
        raise WorkflowError("RESEARCH_DATA_UNIVERSE_INCOMPLETE")
    return records


def _a1_outputs_by_lane(payload: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    """Read lane A1 outputs from the registry payload shape."""

    if not isinstance(payload, Mapping):
        return {}
    lanes = payload.get("lanes")
    if not isinstance(lanes, Mapping):
        return {}
    result: dict[str, Mapping[str, Any]] = {}
    for lane_id, raw in lanes.items():
        if not isinstance(raw, Mapping):
            continue
        output = raw.get("output")
        if isinstance(output, Mapping):
            result[str(lane_id)] = dict(output)
    return result


def _a1_stage_completed(status: Any) -> bool:
    return str(status or "").upper() in {
        "VALIDATED",
        "VALIDATED_NO_OPPORTUNITY",
        "VALIDATED_NO_ACTION",
        "DEGRADED_UNDERFILLED_DATA_GAP",
        "VALIDATED_UNDERFILLED_MARKET",
        "VALIDATED_NO_SETUP",
    }


def _a1_output_partition_symbols(
    output: Mapping[str, Any],
    partitions: tuple[str, ...],
) -> tuple[str, ...]:
    symbols = {
        str(item.get("symbol") or "").strip().upper()
        for partition in partitions
        for item in (
            output.get(partition)
            if isinstance(output.get(partition), (list, tuple))
            else ()
        )
        if isinstance(item, Mapping)
        and str(item.get("symbol") or "").strip()
    }
    return tuple(sorted(symbols))


def _a1_output_symbols(output: Mapping[str, Any]) -> tuple[str, ...]:
    return _a1_output_partition_symbols(output, ("active_research_pool",))


def _a1_outputs_by_result(result: ResearchRunResult) -> dict[str, Mapping[str, Any]]:
    outputs: dict[str, Mapping[str, Any]] = {}
    for lane in result.lanes:
        stage = next((item for item in lane.stages if item.stage == "A1"), None)
        if stage is not None and isinstance(stage.output, Mapping) and _a1_stage_completed(stage.status):
            outputs[str(lane.lane)] = dict(stage.output)
    return outputs


def _a1_lane_record(
    result: ResearchRunResult,
    lane_id: str,
    output: Mapping[str, Any],
) -> dict[str, Any]:
    lane = next((item for item in result.lanes if item.lane == lane_id), None)
    stage = next((item for item in lane.stages if item.stage == "A1"), None) if lane else None
    if stage is None:
        return {
            "lane": lane_id,
            "model": lane.model if lane else None,
            "status": "VALIDATED",
            "symbols": list(_a1_output_symbols(output)),
            "output": dict(output),
        }
    return {
        "lane": lane_id,
        "model": stage.model,
        "status": stage.status,
        "prompt_hash": stage.prompt_hash,
        "input_hash": stage.input_hash,
        "output_hash": _hash_json(output),
        "latency_ms": stage.latency_ms,
        "attempts": stage.attempts,
        "thinking_variant": stage.thinking_variant,
        "symbols": list(_a1_output_symbols(output)),
        "reason_codes": list(stage.reason_codes),
        "diagnostics": dict(stage.diagnostics or {}),
        "output": dict(output),
    }


def _determine_market_regime(
    market_emotion: Mapping[str, Any],
    sector_cycle: Mapping[str, Any],
    *,
    rotation_overlap_threshold: float,
) -> tuple[str, dict[str, Any]]:
    """Determine regime only from declared deterministic evidence.

    Breadth can prove a broad risk retreat, but it can never prove that a
    persistent market main line exists.  TREND_MAINLINE requires the frozen
    THS sector-history overlap and at least one persistent candidate.
    """

    breadth = float(market_emotion.get("breadth") or 0.0)
    if breadth <= 0.35:
        return "RISK_OFF_RETREAT", {
            "algorithm": "market-regime/2.0.0",
            "reason_code": "BREADTH_RISK_OFF_THRESHOLD",
            "breadth": breadth,
        }
    if sector_cycle.get("available") is not True:
        raise WorkflowError("MARKET_REGIME_SECTOR_HISTORY_UNAVAILABLE")
    metrics = sector_cycle.get("history_metrics")
    if not isinstance(metrics, Mapping):
        raise WorkflowError("MARKET_REGIME_SECTOR_HISTORY_INVALID")
    overlap = metrics.get("top3_daily_overlap")
    persistent = metrics.get("persistent_mainline_candidates")
    if not isinstance(overlap, (int, float)) or isinstance(overlap, bool) or not isinstance(persistent, list):
        raise WorkflowError("MARKET_REGIME_SECTOR_METRICS_INVALID")
    evidence = {
        "algorithm": "market-regime/2.0.0",
        "breadth": breadth,
        "top3_daily_overlap": float(overlap),
        "rotation_overlap_threshold": rotation_overlap_threshold,
        "persistent_mainline_count": len(persistent),
        "persistent_mainline_industries": [
            str(item.get("industry_thscode") or "")
            for item in persistent
            if isinstance(item, Mapping)
        ],
        "turnover_metric_role": metrics.get("turnover_metric_role"),
    }
    if float(overlap) >= rotation_overlap_threshold and persistent:
        return "TREND_MAINLINE", {**evidence, "reason_code": "PERSISTENT_TOP3_SECTOR_OVERLAP"}
    return "ROTATION_NO_MAINLINE", {**evidence, "reason_code": "MAINLINE_PERSISTENCE_NOT_PROVEN"}


def _auction_window(value: datetime) -> bool:
    local = _aware(value)
    return local.hour == 9 and 26 <= local.minute <= 30


def _available_fact(facts: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    value = facts.get(key)
    if not isinstance(value, Mapping) or value.get("available") is not True:
        return None
    return dict(value)


def _record_count(value: Mapping[str, Any] | None) -> int | None:
    if value is None:
        return None
    count = value.get("record_count")
    return count if isinstance(count, int) and not isinstance(count, bool) and count >= 0 else None


def _merge_cninfo_query_results(
    recent: CninfoFetchResult,
    business_history: CninfoFetchResult,
) -> CninfoFetchResult:
    """Attach point-in-time periodic reports without weakening recent-event health.

    The ten-day query remains the P0 announcement boundary.  A separate,
    keyword-bounded historical query only enriches A1 with the latest annual
    or half-year report needed for main-business/revenue mapping.
    """

    metadata = {
        **recent.metadata,
        "recent_query": {
            "start_date": recent.start_date,
            "end_date": recent.end_date,
            "reason_code": recent.reason_code,
            "announcement_count": len(recent.announcements),
        },
        "main_business_query": {
            "start_date": business_history.start_date,
            "end_date": business_history.end_date,
            "search_keyword": "年度报告",
            "reason_code": business_history.reason_code,
            "announcement_count": len(business_history.announcements),
        },
    }
    if not recent.ok or not recent.complete or not business_history.ok or not business_history.complete:
        return recent.model_copy(update={"metadata": metadata})
    combined = {item.announcement_id: item for item in recent.announcements}
    for item in business_history.announcements:
        combined.setdefault(item.announcement_id, item)
    announcements = tuple(
        sorted(
            combined.values(),
            key=lambda item: (item.publish_time, item.announcement_id),
            reverse=True,
        )
    )
    return recent.model_copy(
        update={
            "start_date": business_history.start_date,
            "announcements": announcements,
            "total": len(announcements),
            "total_pages": recent.total_pages,
            "pages": recent.pages + business_history.pages,
            "attempts": recent.attempts + business_history.attempts,
            "fetched_at": max(recent.fetched_at, business_history.fetched_at),
            "metadata": metadata,
        }
    )


def _build_cninfo_pdf_tasks(
    cninfo_results: Mapping[str, CninfoFetchResult],
    *,
    limit: int,
) -> tuple[tuple[str, CninfoAnnouncement], ...]:
    """Build a stable, bounded PDF task list before any document is fetched."""

    if limit < 0:
        raise ValueError("CNINFO PDF candidate limit must be non-negative")
    tasks: list[tuple[str, CninfoAnnouncement]] = []
    for symbol, result in sorted(cninfo_results.items(), key=lambda item: str(item[0])):
        tasks.extend(
            (str(symbol), announcement)
            for announcement in select_cninfo_pdf_candidates(result, limit=limit)
        )
    return tuple(tasks)


def _deduplicate_cninfo_pdf_tasks(
    tasks: tuple[tuple[str, CninfoAnnouncement], ...],
) -> tuple[tuple[str, CninfoAnnouncement], ...]:
    """Deduplicate downloads by announcement ID while retaining input order."""

    unique: list[tuple[str, CninfoAnnouncement]] = []
    index_by_id: set[str] = set()
    for symbol, announcement in tasks:
        announcement_id = announcement.announcement_id
        if announcement_id in index_by_id:
            continue
        index_by_id.add(announcement_id)
        unique.append((symbol, announcement))
    return tuple(unique)


def _merge_news_heat_snapshots(base: Any, stock: Any) -> dict[str, Any]:
    """Merge bounded T3 market/RSS and stock-news projections deterministically."""

    base_value = dict(base) if isinstance(base, Mapping) else {}
    stock_value = dict(stock) if isinstance(stock, Mapping) else {}
    retained: dict[str, Mapping[str, Any]] = {}
    for source in (base_value, stock_value):
        items = source.get("items")
        if not isinstance(items, (list, tuple)):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            identity = str(
                item.get("content_hash")
                or item.get("provider_item_id")
                or item.get("source_ref")
                or _hash_json(item)
            )
            retained[identity] = dict(item)
    merged_items = sorted(
        retained.values(),
        key=lambda item: (
            str(item.get("publish_time") or item.get("observed_at") or ""),
            str(item.get("source_id") or ""),
            str(item.get("title") or ""),
        ),
        reverse=True,
    )[:200]
    available = (
        bool(merged_items)
        or base_value.get("available") is True
        or stock_value.get("available") is True
    )
    return {
        **base_value,
        **stock_value,
        "available": available,
        "reason_code": "OK" if available else str(
            stock_value.get("reason_code")
            or base_value.get("reason_code")
            or "NO_NEWS_AVAILABLE"
        ),
        "items": merged_items,
        "item_count": len(merged_items),
        "untrusted_text": True,
    }


def _official_event_snapshot(
    fact_payload: Mapping[str, Any],
    fact_type: str,
    symbols: list[str],
) -> dict[str, Any]:
    wanted = sorted(set(symbols))
    raw_groups = fact_payload.get("fact_groups", {})
    groups = raw_groups if isinstance(raw_groups, Mapping) else {}
    records = groups.get(fact_type, ())
    if not isinstance(records, list):
        records = []
    by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in wanted}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        symbol = str(record.get("symbol") or "")
        if symbol in by_symbol:
            by_symbol[symbol].append(dict(record))
    for symbol in by_symbol:
        ordered = sorted(
            by_symbol[symbol],
            key=lambda item: (str(item.get("publish_time") or ""), str(item.get("fact_id") or "")),
            reverse=True,
        )
        if fact_type == "DISCLOSURE_EVENT":
            periodic = [
                item
                for item in ordered
                if item.get("pdf_evidence_available") is True
                and re.search(
                    r"(?:19|20)\d{2}年(?:半年度|年度)报告(?:全文)?$",
                    re.sub(r"\s+", "", str(item.get("announcement_title") or "")),
                )
            ]
            retained = [*periodic[:1], *ordered[:20]]
            unique: dict[str, dict[str, Any]] = {}
            for item in retained:
                key = str(item.get("announcement_id") or item.get("fact_id") or _hash_json(item))
                unique.setdefault(key, item)
            by_symbol[symbol] = list(unique.values())[:20]
        else:
            by_symbol[symbol] = ordered[:20]

    raw_health = fact_payload.get("source_health", ())
    health = raw_health if isinstance(raw_health, list) else []
    healthy_sources = {
        str(item.get("source_id"))
        for item in health
        if isinstance(item, Mapping)
        and str(item.get("source_id") or "").startswith("cninfo.public.")
        and item.get("available") is True
    }
    healthy_symbols = {
        symbol
        for symbol in wanted
        if f"cninfo.public.{symbol.replace('.', '_').lower()}" in healthy_sources
    }
    available = len(healthy_symbols) == len(wanted)
    return {
        "available": available,
        "reason_code": "OK" if available else "CNINFO_QUERY_INCOMPLETE",
        "source": "CNINFO_PUBLIC_ANNOUNCEMENTS",
        "coverage": len(healthy_symbols) / len(wanted) if wanted else 0.0,
        "by_symbol": by_symbol,
        "query_confirmed_symbols": sorted(healthy_symbols),
        "untrusted_text_policy": "BLOCK_SUSPECTED_INJECTION",
    }


def _main_business_evidence(
    disclosure_events: Mapping[str, Any],
    symbols: list[str],
) -> dict[str, Any]:
    """Project hash-bound filing snippets that can prove revenue exposure."""

    raw_by_symbol = disclosure_events.get("by_symbol")
    by_symbol = raw_by_symbol if isinstance(raw_by_symbol, Mapping) else {}
    result: dict[str, Any] = {}
    strong_terms = ("主营业务分行业", "主营业务分产品", "主营业务分地区")
    supporting_terms = ("分行业", "分产品", "营业收入", "营业成本", "毛利率")
    for symbol in sorted(set(symbols)):
        selected: list[dict[str, Any]] = []
        records = by_symbol.get(symbol)
        for record in records if isinstance(records, list) else ():
            if not isinstance(record, Mapping) or record.get("pdf_evidence_available") is not True:
                continue
            snippets = record.get("pdf_evidence_snippets")
            if not isinstance(snippets, list):
                continue
            for snippet in snippets:
                if not isinstance(snippet, Mapping):
                    continue
                text_value = str(snippet.get("text") or "")
                compact = re.sub(r"\s+", "", text_value)
                structured_revenue_share = (
                    "占营业收入的" in compact
                    and any(term in compact for term in ("客户", "产品", "地区", "业务板块"))
                )
                if (
                    not structured_revenue_share
                    and not any(term in compact for term in strong_terms)
                    and sum(term in compact for term in supporting_terms) < 2
                ):
                    continue
                page_number = snippet.get("page_number")
                announcement_id = str(record.get("announcement_id") or "")
                selected.append(
                    {
                        "announcement_id": announcement_id,
                        "announcement_title": record.get("announcement_title"),
                        "publish_time": record.get("publish_time"),
                        "page_number": page_number,
                        "source_ref": f"cninfo:{announcement_id}:page:{page_number}",
                        "text": text_value[:1_500],
                        "content_hash": record.get("content_hash"),
                    }
                )
        result[symbol] = {
            "available": bool(selected),
            "reason_code": "OK" if selected else "MAIN_BUSINESS_BREAKDOWN_NOT_FOUND",
            "evidence": selected[:3],
        }
    return result


def _macro_policy_feed(fact_payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_groups = fact_payload.get("fact_groups", {})
    groups = raw_groups if isinstance(raw_groups, Mapping) else {}
    records = groups.get("MACRO_POLICY_EVENT", ())
    if not isinstance(records, list):
        records = []
    ordered = sorted(
        (dict(item) for item in records if isinstance(item, Mapping)),
        key=lambda item: (str(item.get("publish_time") or ""), str(item.get("fact_id") or "")),
        reverse=True,
    )[:100]
    raw_health = fact_payload.get("source_health", ())
    health = raw_health if isinstance(raw_health, list) else []
    source = next((
        item for item in health
        if isinstance(item, Mapping) and item.get("source_id") == "gov.policy_library"
    ), None)
    available = isinstance(source, Mapping) and source.get("available") is True
    return {
        "available": available,
        "reason_code": str(source.get("reason_code") or "GOV_POLICY_QUERY_UNAVAILABLE")
        if isinstance(source, Mapping) else "GOV_POLICY_QUERY_UNAVAILABLE",
        "source": "STATE_COUNCIL_POLICY_LIBRARY_PUBLIC_WEB",
        "official_documents": ordered,
        "document_count": len(ordered),
        "excluded_categories": ["POLICY_INTERPRETATION", "STATE_COUNCIL_GAZETTE_DUPLICATE"],
        "direct_stock_mapping_allowed": False,
        "financial_transmission_evidence": False,
        "untrusted_text_policy": "BLOCK_SUSPECTED_INJECTION",
    }


def _minute_cache_ready(
    bars: tuple[MinuteBar, ...],
    *,
    required_bars: int,
    as_of: datetime,
) -> bool:
    if len(bars) < required_bars:
        return False
    expected = _latest_required_5m_end(as_of)
    if expected is None or bars[-1].bar_end != expected:
        return False
    return all(
        bar.interval == "5m"
        and bar.adjust_mode in {"none", "raw"}
        and bar.bar_end <= expected
        for bar in bars
    )


def _latest_required_5m_end(value: datetime) -> datetime | None:
    current = _aware(value)
    day = current.date()
    if current.time() >= datetime.strptime("15:00", "%H:%M").time():
        return datetime.combine(day, datetime.strptime("15:00", "%H:%M").time(), SHANGHAI)
    if current.time() >= datetime.strptime("13:05", "%H:%M").time():
        minutes = min(120, ((current.hour * 60 + current.minute) - (13 * 60)) // 5 * 5)
        return datetime.combine(day, datetime.strptime("13:00", "%H:%M").time(), SHANGHAI) + timedelta(minutes=minutes)
    if current.time() >= datetime.strptime("11:30", "%H:%M").time():
        return datetime.combine(day, datetime.strptime("11:30", "%H:%M").time(), SHANGHAI)
    if current.time() >= datetime.strptime("09:35", "%H:%M").time():
        minutes = min(120, ((current.hour * 60 + current.minute) - (9 * 60 + 30)) // 5 * 5)
        return datetime.combine(day, datetime.strptime("09:30", "%H:%M").time(), SHANGHAI) + timedelta(minutes=minutes)
    return None


def _batch_dict(batch: MonitorBatchResult) -> dict[str, Any]:
    return batch.model_dump(mode="json")


def _compact_factor(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep model-relevant factor facts; raw bars remain in SQLite only."""

    compact_frames: dict[str, Any] = {}
    frames = value.get("timeframes")
    if isinstance(frames, Mapping):
        for name, raw in frames.items():
            if not isinstance(raw, Mapping):
                continue
            compact_frames[str(name)] = {
                "latest": raw.get("latest"),
                "latest_partial": raw.get("latest_partial"),
                "partial_bars": raw.get("partial_bars"),
                "moving_averages": raw.get("moving_averages"),
                "previous_moving_averages": raw.get("previous_moving_averages"),
                "ma_slopes": raw.get("ma_slopes"),
                "ma_alignment": raw.get("ma_alignment"),
                "ma_event": raw.get("ma_event"),
                "ma_bias": raw.get("ma_bias"),
                "vwap": raw.get("vwap"),
                "ready": raw.get("ready"),
                "reasons": raw.get("reasons"),
            }
    return {
        "symbol": value.get("symbol"),
        "as_of": value.get("as_of"),
        "ready": value.get("ready"),
        "a3_ready": value.get("a3_ready"),
        "a3_reasons": value.get("a3_reasons"),
        "reasons": value.get("reasons"),
        "timeframes": compact_frames,
        "technical_summary": value.get("technical_summary"),
    }


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None


def _plan_expiry(
    value: Any,
    now: datetime,
    slot: str,
    *,
    minimum_trade_date: date | None = None,
) -> datetime:
    current = _aware(now)
    if minimum_trade_date is not None and (
        isinstance(minimum_trade_date, datetime) or not isinstance(minimum_trade_date, date)
    ):
        raise ValueError("minimum trade date must be a date")
    minimum_day = (
        minimum_trade_date
        if minimum_trade_date is not None
        else current.date() if slot == "morning" else (current + timedelta(days=1)).date()
    )
    while minimum_day.weekday() >= 5:
        minimum_day += timedelta(days=1)
    minimum = datetime(
        minimum_day.year,
        minimum_day.month,
        minimum_day.day,
        15,
        0,
        tzinfo=SHANGHAI,
    )
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                parsed = parsed.astimezone(SHANGHAI)
                # A model may propose a same-day/overnight expiry.  The
                # server owns the publication horizon: close plans remain
                # valid through the next trading day at 15:00 at minimum.
                if parsed > current and parsed >= minimum:
                    return parsed
        except ValueError:
            pass
    return minimum


def _hash_json(value: Any) -> str:
    # ``json.dumps(...).encode(...)`` simultaneously retains the Python
    # object tree, one complete Unicode JSON string and one complete UTF-8
    # byte string. Full-market snapshots exceed 200MB, making that transient
    # duplication large enough to trigger the VM OOM killer. ``iterencode``
    # produces byte-identical canonical JSON incrementally.
    encoder = json.JSONEncoder(
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256()
    for chunk in encoder.iterencode(value):
        digest.update(chunk.encode("utf-8"))
    return digest.hexdigest()


def _supplemental_contract(bundle: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    """Return one immutable-by-copy supplemental contract when well formed."""

    value = bundle.get(key)
    if not isinstance(value, Mapping):
        return None
    contract = dict(value)
    if str(contract.get("contract") or key) != key:
        return None
    contract.setdefault("contract", key)
    contract.setdefault("available", False)
    contract.setdefault("reason_code", "SOURCE_UNAVAILABLE")
    return contract


def _progress_stdout(state: Mapping[str, Any]) -> None:
    """Emit one bounded progress line for the Node log stream."""

    data = state.get("data") if isinstance(state.get("data"), Mapping) else {}
    payload = {
        "event": "WORKFLOW_PROGRESS",
        "run_id": state.get("run_id"),
        "status": state.get("status"),
        "phase": state.get("phase"),
        "elapsed_seconds": state.get("elapsed_seconds"),
        "eta_seconds": state.get("eta_seconds"),
        "processed": data.get("processed"),
        "total": data.get("total"),
        "cache_hits": data.get("cache_hits"),
        "cache_misses": data.get("cache_misses"),
        "failures": data.get("failures"),
        "current_symbol": data.get("current_symbol"),
        "current_document": data.get("current_document"),
        "documents_succeeded": data.get("documents_succeeded"),
        "documents_failed": data.get("documents_failed"),
    }
    print(json.dumps(sanitize(payload), ensure_ascii=False, separators=(",", ":")), flush=True)


_COMPACT_FUNDAMENTAL_FIELDS: Mapping[str, tuple[str, ...]] = {
    "INCOME": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "operating_income", "operating_costs", "operating_profit",
        "parent_holder_net_profit", "basic_eps", "research_and_development_expenses",
        "sales_fee", "manage_fee",
    ),
    "BALANCE": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "assets_total", "total_debt", "holder_equity_total", "cash",
        "accounts_receivable", "total_current_assets", "non_current_nets_total",
    ),
    "CASH_FLOW": (
        "_dataset", "fiscal_year", "fiscal_period", "period_end_ms", "report_date_ms",
        "act_cash_flow_net", "invest_cash_flow_net", "financing_cash_flow_net",
        "cash_equivalents_net_addition", "pay_fixed_assets_etc_cash",
    ),
    "INDICATORS": ("_dataset", "ability", "index_id", "value"),
}
_PREFERRED_FUNDAMENTAL_INDICATORS = frozenset({
    "calculate_operating_income_yoy_growth_ratio",
    "calculate_parent_holder_net_profit_yoy_growth_ratio",
    "sale_gross_margin",
    "net_profit_margin",
    "index_weighted_avg_roe",
    "roe",
    "assets_debt_ratio",
    "debt_to_assets",
    "receive_account_turnover_ratio",
    "net_profit_cash_content",
    "cashflow_net_income_ratio",
    "operating_cash_flow_net_divide_income",
})


def _compact_fundamental_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep bounded statement history while the durable cache retains all rows."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        grouped.setdefault(str(row.get("_dataset") or "UNKNOWN"), []).append(dict(row))

    missing_datasets = [
        dataset
        for dataset in ("INCOME", "BALANCE", "CASH_FLOW", "INDICATORS")
        if not grouped.get(dataset)
    ]
    result: dict[str, Any] = {
        "statements": {},
        "indicators": [],
        "dataset_coverage": {
            "core_reports_complete": not any(
                dataset in missing_datasets for dataset in ("INCOME", "BALANCE", "CASH_FLOW")
            ),
            "indicators_available": "INDICATORS" not in missing_datasets,
            "missing_datasets": missing_datasets,
        },
    }
    for dataset in ("INCOME", "BALANCE", "CASH_FLOW"):
        ordered = sorted(
            grouped.get(dataset, ()),
            key=lambda item: (
                _int_or_zero(item.get("report_date_ms")),
                _int_or_zero(item.get("period_end_ms")),
                str(item.get("report_period") or ""),
            ),
            reverse=True,
        )
        fields = _COMPACT_FUNDAMENTAL_FIELDS[dataset]
        result["statements"][dataset] = [
            {key: item.get(key) for key in fields if key in item}
            for item in ordered[:4]
        ]
    indicators = sorted(
        grouped.get("INDICATORS", ()),
        key=lambda item: (
            0
            if str(item.get("index_id") or "").strip().lower()
            in _PREFERRED_FUNDAMENTAL_INDICATORS
            else 1,
            str(item.get("ability") or ""),
            str(item.get("index_id") or ""),
        ),
    )
    indicator_fields = _COMPACT_FUNDAMENTAL_FIELDS["INDICATORS"]
    result["indicators"] = [
        {key: item.get(key) for key in indicator_fields if key in item}
        for item in indicators[:40]
    ]
    return result


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _container_length(value: Any) -> int:
    return len(value) if isinstance(value, (list, tuple, set, frozenset, Mapping)) else 0


def _positive_count(value: Any, *, fallback: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = 0
    return number if number > 0 else max(0, int(fallback))


def _write_broker_gold_benchmark(
    result: ResearchRunResult,
    *,
    as_of: datetime,
    benchmark_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Persist a post-run broker-gold blind benchmark without mutating pools."""

    month = as_of.astimezone(SHANGHAI).strftime("%Y-%m")
    sources = (benchmark_dir / f"{month}.json", benchmark_dir / f"{month}.csv")
    source = next((path for path in sources if path.is_file()), None)
    lane_reports: dict[str, Any] = {}
    status = "NOT_CONFIGURED"
    reason_code = "BROKER_GOLD_BENCHMARK_NOT_CONFIGURED"
    if source is not None:
        try:
            dataset = import_broker_gold(source, as_of=as_of)
            for lane in result.lanes:
                a1 = next((stage for stage in lane.stages if stage.stage == "A1"), None)
                if a1 is None or not isinstance(a1.output, Mapping):
                    lane_reports[lane.lane] = {
                        "status": "A1_RESULT_UNAVAILABLE",
                        "model": lane.model,
                        "benchmark_not_runtime_input": True,
                    }
                    continue
                lane_reports[lane.lane] = {
                    "model": lane.model,
                    **evaluate_broker_gold(dataset, a1.output, as_of=as_of, month=month),
                }
            status = "EVALUATED"
            reason_code = "OK"
        except (BrokerGoldContractError, OSError, UnicodeError, ValueError):
            status = "INVALID_BENCHMARK"
            reason_code = "BROKER_GOLD_BENCHMARK_INVALID"

    payload = {
        "schema_version": "liangjian-broker-gold-run-report/1.0.0",
        "run_id": result.run_id,
        "as_of": as_of.isoformat(),
        "month": month,
        "status": status,
        "reason_code": reason_code,
        "benchmark_not_runtime_input": True,
        "source_ref": str(source) if source is not None else None,
        "lanes": lane_reports,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"research_{result.run_id}_broker_benchmark.json"
    markdown_path = output_dir / f"research_{result.run_id}_broker_benchmark.md"
    atomic_write_json(json_path, payload)
    lines = [
        "# 券商月度金股盲测",
        "",
        "> 仅用于运行后评估，不参与A1运行时选股，不连接券商或交易账户。",
        "",
        f"- run_id：`{result.run_id}`",
        f"- 月份：`{month}`",
        f"- 状态：`{status}`",
        f"- 原因：`{reason_code}`",
        "- benchmark_not_runtime_input：`true`",
    ]
    if lane_reports:
        lines.extend(["", "| Lane | 模型 | 金股数 | A1覆盖率 | ACTIVE覆盖率 |", "|---|---|---:|---:|---:|"])
        for lane_id, report in lane_reports.items():
            dataset_summary = report.get("dataset") if isinstance(report, Mapping) else {}
            symbol_coverage = report.get("symbol_coverage") if isinstance(report, Mapping) else {}
            active_coverage = report.get("active_coverage") if isinstance(report, Mapping) else {}
            lines.append(
                f"| {lane_id} | {report.get('model', '-')} | "
                f"{dataset_summary.get('eligible_symbol_count', 0) if isinstance(dataset_summary, Mapping) else 0} | "
                f"{_benchmark_percent(symbol_coverage)} | {_benchmark_percent(active_coverage)} |"
            )
    atomic_write_text(markdown_path, "\n".join(lines) + "\n")
    return {
        "status": status,
        "reason_code": reason_code,
        "benchmark_not_runtime_input": True,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }


def _benchmark_percent(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "—"
    raw = value.get("coverage")
    if not isinstance(raw, (int, float)):
        return "—"
    return f"{float(raw) * 100:.1f}%"


def _safe_reason_code(exc: BaseException) -> str:
    candidate = getattr(exc, "reason_code", None)
    if isinstance(candidate, str) and re.fullmatch(r"[A-Z0-9_:.+-]{1,120}", candidate):
        return candidate
    name = type(exc).__name__.upper()
    return re.sub(r"[^A-Z0-9_]+", "_", name)[:120] or "UNEXPECTED_RUNTIME_ERROR"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WorkflowError("TIMEZONE_REQUIRED")
    return value.astimezone(SHANGHAI)


def _bind_reference_fact_event_time(
    result: HithinkFetchResult,
    *,
    as_of: datetime,
) -> HithinkFetchResult:
    """Bind undated reference data to its requested point-in-time cutoff.

    ``fetch_time`` remains the actual response time. This helper is limited to
    reference snapshots such as taxonomy catalogs; timestamped market facts
    must continue to use their provider event timestamps.
    """

    cutoff = _aware(as_of)
    return result.model_copy(
        update={
            "metadata": {
                **dict(result.metadata),
                "timestamp": cutoff.isoformat(),
                "event_time_basis": "REQUESTED_REFERENCE_CUTOFF",
            }
        }
    )


def _advance_live_market_cutoff(
    *,
    market_as_of: datetime,
    research_as_of: datetime,
    included_fact_as_of: datetime,
) -> datetime:
    """Advance a same-session live cutoff to the latest included event.

    A request-start timestamp is not a valid upper bound for facts returned a
    few seconds later. Historical and next-session runs retain their explicit
    completed-session cutoff; only a live same-date collection may advance.
    """

    market = _aware(market_as_of)
    research = _aware(research_as_of)
    included = _aware(included_fact_as_of)
    if market.date() == research.date() == included.date():
        return max(market, included)
    return market


def _at_time(value: datetime, hour: int, minute: int) -> datetime:
    return value.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _slot(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {"morning", "close"}:
        raise WorkflowError("SLOT_INVALID")
    return normalized


__all__ = [
    "A1MaintenancePlan",
    "PreparedSnapshot",
    "WorkflowApplication",
    "WorkflowError",
    "decide_a1_maintenance",
]
