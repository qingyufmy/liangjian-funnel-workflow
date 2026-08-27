"""End-to-end orchestration for the standalone shadow/simulation workflow."""

from __future__ import annotations

import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .data.cache import MinuteBarStore
from .data.bse import BseClient
from .data.cninfo import CninfoAnnouncement, CninfoClient, CninfoFetchResult
from .data.cninfo_pdf import CninfoPdfClient, CninfoPdfEvidence
from .data.gov_policy import GovPolicyClient
from .data.mootdx import MootdxAdapter, MootdxNode, MinuteBar, map_symbol
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
    manifest_projection,
    merge_fact_manifests,
    normalize_cninfo_results,
    normalize_gov_policy_result,
    normalize_hithink_results,
    normalize_open_news_results,
    select_cninfo_pdf_candidates,
)
from .pipeline.data_source import HithinkClient
from .pipeline.data_sync import HithinkIncrementalSynchronizer
from .pipeline.factors import FactorEngine
from .pipeline.local_fact_cache import LocalFactCache
from .pipeline.market_aggregates import (
    build_crowding_snapshot,
    build_market_emotion,
    build_news_heat_snapshot,
    build_sector_cycle_and_permissions,
)
from .pipeline.model_client import ModelCallResult, OpenAICompatibleModelClient
from .pipeline.prompts import PromptRepository
from .pipeline.research import FrozenInputSnapshot as ResearchSnapshot
from .pipeline.research import ResearchPipeline, ResearchRunResult
from .pipeline.research_checkpoint import FileResearchCheckpointStore
from .pipeline.snapshot import FrozenInputSnapshot, UniverseGatePolicy, UniverseSnapshot
from .pipeline.technical_aggregates import build_technical_aggregates
from .redaction import digest_text, sanitize
from .reporting import atomic_write_json, atomic_write_text
from .runtime.monitor import MonitorBatchResult, MonitorEngine, rebuild_effective_markdown
from .runtime.progress import WorkflowProgress
from .runtime.calendar import ExchangeTradingCalendar, TradingCalendarError
from .runtime.scheduler import ScheduleKind, Scheduler
from .runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from .runtime.state import MonitorAction, PlanStatus, RuntimeStore
from .settings import Settings, load_yaml


SHANGHAI = ZoneInfo("Asia/Shanghai")
_A4_FILE = "agent_4_intraday_signal_v2.txt"
_G0_SCOPE_CONTRACT = "CONFIGURED_RESEARCH_UNIVERSE_V1"
_RESEARCH_RESUME_SCHEMA = "liangjian-research-resume/1.2.0"
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
        }


class WorkflowApplication:
    """Join data, research, A4 and paper simulation without external orders."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = RuntimeStore(settings.state_db_path)
        self.minute_store = MinuteBarStore(settings.minute_cache_dir)
        self.fact_cache = LocalFactCache(settings.fact_cache_db_path)
        self.fact_synchronizer = HithinkIncrementalSynchronizer(
            self.fact_cache,
            fundamental_refresh_hours=settings.fundamental_refresh_hours,
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
            settings.model_copy(update={"model_timeout_seconds": 45.0}),
            max_attempts=2,
            thinking_enabled=settings.monitor_thinking_enabled,
        )
        self.trading_calendar = ExchangeTradingCalendar()
        self.mootdx = MootdxAdapter(
            tuple(MootdxNode(host=host, port=port) for host, port in settings.mootdx_servers),
            page_size=settings.mootdx_page_size,
            max_pages=settings.mootdx_max_pages,
            timeout_seconds=settings.mootdx_timeout_seconds,
        )
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
        progress: WorkflowProgress | None = None,
    ) -> PreparedSnapshot:
        current = _aware(as_of or datetime.now(SHANGHAI))
        wall_now = datetime.now(SHANGHAI)
        if abs((current - wall_now).total_seconds()) > 600:
            raise WorkflowError("LIVE_FACTS_POINT_IN_TIME_UNSUPPORTED")

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
            industry_catalog = client.ths_index_catalog(tag="industry")
            concept_catalog = client.ths_index_catalog(tag="cn_concept")
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
                as_of=current,
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
                )
                _progress_stdout(progress.snapshot())

            sync_result = self.fact_synchronizer.sync(
                client,
                [candidate.symbol for candidate in selected],
                as_of=current,
                lookback_days=800,
                compact_daily_bars=30,
                progress=sync_progress,
            )
            for symbol, reasons in sync_result.failures.items():
                source_failures.setdefault(symbol, []).extend(reasons)
            daily: dict[str, Any] = sync_result.daily
            fundamental: dict[str, Any] = {
                symbol: _compact_fundamental_rows(rows)
                for symbol, rows in sync_result.fundamental.items()
            }
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
        cninfo_pdf_evidence: dict[tuple[str, str], CninfoPdfEvidence] = {}
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
                cached_rows = self.fact_cache.get_cached_results(
                    "CNINFO_PDF_EVIDENCE",
                    cache_keys,
                    fresh_at=datetime.now(SHANGHAI),
                )
                pending: list[tuple[int, str, CninfoAnnouncement]] = []
                for index, (symbol, announcement) in enumerate(unique_pdf_tasks):
                    cached_evidence = self._cached_cninfo_pdf_evidence_from_record(
                        announcement,
                        cached_rows.get(announcement.announcement_id),
                    )
                    if cached_evidence is None:
                        pending.append((index, symbol, announcement))
                        continue
                    pdf_evidence_by_index[index] = cached_evidence

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
                        pdf_evidence_by_index[index] = evidence
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

                # Expand one fetched document back to every symbol occurrence.
                # This keeps the full fact boundary while avoiding duplicate
                # network/download/parse work for repeated announcement IDs.
                ordered_evidence = {
                    unique_pdf_tasks[index][1].announcement_id: pdf_evidence_by_index[index]
                    for index in range(len(unique_pdf_tasks))
                }
                for symbol, announcement in pdf_tasks:
                    evidence = ordered_evidence[announcement.announcement_id]
                    cninfo_pdf_evidence[(symbol, announcement.announcement_id)] = evidence
                    if not evidence.available:
                        source_failures.setdefault(symbol, []).append(
                            f"CNINFO_PDF:{announcement.announcement_id}:{evidence.reason_code}"
                        )
        cninfo_manifest = normalize_cninfo_results(
            cninfo_results,
            pdf_evidence=cninfo_pdf_evidence,
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
        frozen = FrozenInputSnapshot.freeze(
            universe,
            as_of=current,
            daily_payload=daily,
            fundamental_payload=fundamental,
            technical_payload=technical,
            fact_payload=fact_payload,
            max_candidates=selection_limit,
            candidate_symbols=selected_symbols,
            candidate_domain="research",
            retain_incomplete=True,
        )
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
        atomic_write_json(
            path,
            {
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_hash": snapshot.snapshot_hash,
                "as_of": current.isoformat(),
                "data": data,
            },
        )
        return PreparedSnapshot(
            snapshot=snapshot,
            path=path,
            full_universe_count=len(universe.records),
            research_universe_count=len(universe.research_candidates),
            trade_universe_count=len(universe.trade_candidates),
            selected_count=len(g0_symbols),
            factor_ready_count=len(factor_ready),
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
            coverage = self.fact_cache.get_coverage()
            summary = {
                "status": "READY" if not result.failures else "PARTIAL",
                "as_of": current.isoformat(),
                "universe_count": len(universe.records),
                "catalog_universe_count": len(universe.records),
                "data_universe_count": len(symbols),
                "research_universe_count": len(universe.research_candidates),
                "trade_universe_count": len(universe.trade_candidates),
                "processed": result.processed,
                "cache_hits": result.cache_hits,
                "cache_misses": result.cache_misses,
                "failure_count": len(result.failures),
                "coverage": coverage,
            }
            output = self.settings.workflow_output_dir / "data_sync" / f"{current.strftime('%Y%m%dT%H%M%S%z')}.json"
            atomic_write_json(output, summary)
            progress.finish(
                status=str(summary["status"]),
                phase="DATA_READY" if not result.failures else "DATA_PARTIAL",
                reason_code=None if not result.failures else "SYMBOL_DATA_PARTIAL",
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

    def run_research(
        self,
        slot: str,
        *,
        as_of: datetime | None = None,
        historical_replay: bool = False,
    ) -> dict[str, Any]:
        normalized_slot = _slot(slot)
        current = _aware(as_of or datetime.now(SHANGHAI))
        if historical_replay:
            if as_of is None:
                raise WorkflowError("HISTORICAL_AS_OF_REQUIRED")
            if current.date() >= datetime.now(SHANGHAI).date():
                raise WorkflowError("HISTORICAL_AS_OF_NOT_PAST")
        self._ensure_trading_day(current, synchronize_accounts=not historical_replay)
        if normalized_slot == "morning" and current.hour == 9 and current.minute < 26:
            time.sleep(max(0.0, (_at_time(current, 9, 26) - current).total_seconds()))
            current = datetime.now(SHANGHAI)
        progress = WorkflowProgress(
            self.settings.workflow_progress_path,
            run_id=f"{current.date()}-{normalized_slot}",
            job=normalized_slot,
        )
        progress.set_phase("DATA_SYNC")
        _progress_stdout(progress.snapshot())
        try:
            prepared = None if historical_replay else self._load_research_resume_snapshot(
                normalized_slot,
                current,
            )
            if prepared is None:
                prepared = self.prepare_snapshot(as_of=current, progress=progress)
                if not historical_replay:
                    self._write_research_resume_marker(
                        normalized_slot,
                        prepared,
                        status="ACTIVE",
                    )
            else:
                progress.set_phase("SNAPSHOT_RESUMED")
                _progress_stdout(progress.snapshot())
        except Exception as exc:
            progress.finish(
                status="BLOCKED",
                phase="FAILED",
                reason_code=_safe_reason_code(exc),
            )
            _progress_stdout(progress.snapshot())
            raise
        run_id = f"{current.date()}-{normalized_slot}-{prepared.snapshot.snapshot_hash[:12]}"

        def research_progress(event: Mapping[str, Any]) -> None:
            progress.research_event(event)
            _progress_stdout(progress.snapshot())

        pipeline = ResearchPipeline(
            self.settings,
            prompt_repository=self.prompts,
            model_client=self.model_client,
            output_dir=self.settings.workflow_output_dir / "research",
            parallel_lanes=True,
            runtime_store=self.store,
            slot=normalized_slot,
            batch_workers=self.settings.research_batch_workers,
            progress_callback=research_progress,
            checkpoint_store=self.research_checkpoints,
            stage_snapshot_enricher=self._stage_snapshot_enricher,
        )
        try:
            result = pipeline.run(prepared.snapshot, run_id=run_id, generated_at=current)
        except Exception as exc:
            progress.finish(
                status="BLOCKED",
                phase="FAILED",
                reason_code=_safe_reason_code(exc),
            )
            _progress_stdout(progress.snapshot())
            raise
        publication = self._publish_plans(
            result,
            normalized_slot,
            datetime.now(SHANGHAI),
            snapshot_data=prepared.snapshot.data,
        )
        summary = {
            "run_id": run_id,
            "slot": normalized_slot,
            "status": result.status,
            "snapshot": prepared.as_dict(),
            "research_markdown": str(result.markdown_path) if result.markdown_path else None,
            "plan_publication": publication,
        }
        atomic_write_json(self.settings.workflow_output_dir / "runs" / f"{run_id}.json", summary)
        if not historical_replay:
            self._write_research_resume_marker(
                normalized_slot,
                prepared,
                status="COMPLETED" if result.status == "READY" else "RETRYABLE",
                reason_code=None if result.status == "READY" else "RESEARCH_NOT_READY",
            )
        progress.finish(
            status=result.status,
            phase="COMPLETED" if result.status == "READY" else "BLOCKED",
            reason_code=None if result.status == "READY" else "RESEARCH_NOT_READY",
        )
        _progress_stdout(progress.snapshot())
        return summary

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
            required_bars = self.settings.mootdx_history_5m_required_bars
            cached_bars = self.minute_store.load_latest(symbol, "5m", limit=required_bars)
            if _minute_cache_ready(cached_bars, required_bars=required_bars, as_of=current):
                minute_bars = cached_bars
            else:
                minutes = self.mootdx.fetch_bars(symbol, "5m", required_bars, as_of=current)
                if not minutes.complete:
                    return symbol, None
                self.minute_store.write(minutes.bars)
                minute_bars = minutes.bars
            factor = FactorEngine(symbol).compute(
                daily_bars=daily_bars,
                minute_bars=minute_bars,
                as_of=current,
            )
            aggregates = build_technical_aggregates(factor)
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
            one = self.mootdx.fetch_bars(symbol, "1m", 21, as_of=current)
            five = self.mootdx.fetch_bars(symbol, "5m", 60, as_of=current)
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
        lane_inputs: dict[str, tuple[dict[str, MinuteBar], bool, dict[str, Any]]] = {}
        for lane_id, scope_symbols in lane_scopes.items():
            bars: dict[str, MinuteBar] = {}
            contexts: dict[str, Any] = {}
            data_ok = True
            for symbol in sorted(scope_symbols):
                fetched = market.get(symbol, {})
                one = fetched.get("1m")
                five = fetched.get("5m")
                one_bars = tuple(one.bars) if one is not None else ()
                five_bars = tuple(five.bars) if five is not None else ()
                if not one_bars or one_bars[-1].bar_end != current:
                    data_ok = False
                else:
                    bars[symbol] = one_bars[-1]
                    simulation.extend(self._settle_prior_signals(lane_id, symbol, one_bars[-1]))
                contexts[symbol] = _intraday_market_context(
                    symbol,
                    one_bars,
                    five_bars,
                    current=current,
                )
            lane_inputs[lane_id] = (bars, data_ok, contexts)

        def process_lane(lane_id: str) -> tuple[str, MonitorBatchResult]:
            plans = lane_plans[lane_id]
            bars, data_ok, contexts = lane_inputs[lane_id]
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
            fetched = self.mootdx.fetch_bars(symbol, "1m", 2, as_of=current)
            if fetched.bars:
                self.minute_store.write(fetched.bars)
            if not fetched.complete or not fetched.bars or fetched.bars[-1].bar_end != current:
                failures.append({"symbol": symbol, "reason_code": "CURRENT_1M_BAR_UNAVAILABLE"})
                continue
            bar = fetched.bars[-1]
            if bar.volume <= 0:
                failures.append({"symbol": symbol, "reason_code": "CURRENT_1M_BAR_ZERO_VOLUME"})
                continue
            evidence[symbol] = bar.model_dump(mode="json")

        for plan in pending:
            symbol = str(plan["symbol"])
            if symbol not in evidence:
                continue
            try:
                payload = json.loads(str(plan.get("payload_json") or "{}"))
                stop_level = float(payload["stop_level"])
                trigger_high = float(payload["trigger_high"])
                price = float(evidence[symbol]["close"])
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
                ScheduleKind.CLOSE_1510: lambda _job: self.run_research("close", as_of=current),
                ScheduleKind.MONITOR: lambda _job: self.monitor_once(now=current),
            },
            owner="liangjian-runtime",
            trading_day=self.trading_calendar.is_trading_day,
        )
        records = scheduler.dispatch_once(current, kinds=(kind,) if kind is not None else None)
        return {
            "time": current.isoformat(),
            "dispatch": [record.model_dump(mode="json") for record in records],
        }

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
        g0_selection: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        source_config = load_yaml(self.settings.source_config_path) if self.settings.source_config_path.is_file() else {}
        config_hash = digest_text(json.dumps(source_config, ensure_ascii=False, sort_keys=True, default=str))
        selected_records = [
            item.model_dump(mode="json")
            for item in frozen.g0_candidates
            if item.symbol in set(g0_symbols)
        ]
        trade_records = [item for item in selected_records if item.get("trade_eligible") is True]
        missing = {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"}
        facts = frozen.fact_payload.get("facts", {})
        if not isinstance(facts, Mapping):
            facts = {}
        open_macro_bundle = frozen.fact_payload.get("open_macro_bundle")
        open_macro_bundle = open_macro_bundle if isinstance(open_macro_bundle, Mapping) else {}
        market_emotion = build_market_emotion(universe.records, facts, as_of=as_of)
        if market_emotion.get("available") is not True:
            raise WorkflowError("MARKET_EMOTION_AGGREGATE_NOT_READY")
        breadth = float(market_emotion["breadth"])
        auction = _available_fact(facts, "AUCTION_FINAL") if _auction_window(as_of) else None
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
        crowding = build_crowding_snapshot(facts, g0_symbols, as_of=as_of)
        sector_cycle, sector_permissions = build_sector_cycle_and_permissions(
            facts,
            g0_symbols,
            as_of=as_of,
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
            "AUCTION_SNAPSHOT": auction or {
                "available": False,
                "reason_code": "OUTSIDE_0926_REVIEW_WINDOW" if not _auction_window(as_of) else "SOURCE_UNAVAILABLE",
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
            "BROKER_RESEARCH_CONSENSUS": {
                "available": False,
                "reason_code": "AUTHORIZED_RESEARCH_SOURCE_NOT_CONFIGURED",
                "primary_evidence": False,
            },
            "INDUSTRY_NEWS_FEED": industry_news_feed,
            "NEWS_HEAT_SNAPSHOT": news_heat,
            "CROWDING_SNAPSHOT": crowding,
            "SECTOR_CYCLE_SNAPSHOT": sector_cycle,
            "SECTOR_PERMISSIONS": sector_permissions,
            "config_version": source_config.get("version")
            or source_config.get("funnel_version")
            or "funnel-config-v2",
            "config_hash": config_hash,
        }
        for key in (
            "MACRO_POLICY_FEED", "MACRO_ECONOMIC_DATA", "ASSET_ROTATION_SNAPSHOT", "GLOBAL_MACRO_SNAPSHOT", "CROSS_MARKET_LEAD_SNAPSHOT", "BROKER_RESEARCH_CONSENSUS", "INDUSTRY_NEWS_FEED", "INDUSTRY_ACTIVITY_DATA", "INDUSTRY_PROFIT_DATA", "THS_INDUSTRY_MEMBERSHIP", "THS_CONCEPT_MEMBERSHIP", "EXISTING_CHAIN_GRAPH",
            "THEME_REGISTRY", "DISCLOSURE_EVENTS", "RISK_EVENTS", "RESEARCH_CONSENSUS", "FUND_HOLDINGS",
            "FAST_TRACK_REQUESTS", "PRIOR_OUTCOME_FEEDBACK", "SECTOR_CYCLE_SNAPSHOT", "CAPITAL_FLOW_SNAPSHOT",
            "NEWS_HEAT_SNAPSHOT", "CROWDING_SNAPSHOT", "AUCTION_SNAPSHOT", "SECTOR_PERMISSIONS",
        ):
            values.setdefault(key, missing)
        values.update(_prompt_parameters(source_config))
        return values

    def _publish_plans(
        self,
        result: ResearchRunResult,
        slot: str,
        now: datetime,
        *,
        snapshot_data: Mapping[str, Any],
    ) -> dict[str, Any]:
        batch: list[dict[str, Any]] = []
        blocked: list[dict[str, str]] = []
        ready_lanes: list[str] = []
        tradability_flags = snapshot_data.get("TRADABILITY_FLAGS")
        if not isinstance(tradability_flags, Mapping):
            tradability_flags = {}
        for lane in result.lanes:
            if lane.status != "READY" or not isinstance(lane.final_output, Mapping):
                blocked.append({"lane": lane.lane, "reason": "LANE_NOT_READY"})
                continue
            plans = lane.final_output.get("core_watch_pool")
            if not isinstance(plans, list):
                blocked.append({"lane": lane.lane, "reason": "CORE_WATCH_POOL_MISSING"})
                continue
            ready_lanes.append(lane.lane)
            previous = {
                str(item["symbol"]): item
                for item in self.store.list_execution_plans(lane_id=lane.lane, status=PlanStatus.PENDING_MORNING_REVIEW)
            }
            for raw in plans:
                if not isinstance(raw, Mapping):
                    continue
                payload = _plan_payload(raw)
                symbol = payload.get("symbol")
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
                            "expires_at": _plan_expiry(payload.get("plan_expiry"), now, slot),
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
                        "expires_at": _plan_expiry(payload.get("plan_expiry"), now, slot),
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
            replacements = {name: None for name in bundle.shared.placeholders + bundle.document(_A4_FILE).placeholders}
            replacements.update(
                {
                    "EXECUTION_PLANS": list(plans),
                    "TRIGGER_ENGINE_RESULT": contexts,
                    "REALTIME_QUOTE": {
                        key: value.get("realtime_quote") for key, value in market_context.items()
                    },
                    "CLOSED_BARS": {
                        key: value.get("closed_bars") for key, value in market_context.items()
                    },
                    "REALTIME_MA": {
                        key: value.get("moving_averages") for key, value in market_context.items()
                    },
                    "OPEN_SIGNAL_STATE": {"lane_id": lane_id},
                    "MARKET_CONTEXT": {
                        "time": now.isoformat(),
                        "minute_snapshot_id": context.get("minute_snapshot_id"),
                        "symbols": market_context,
                    },
                    "SECTOR_CONTEXT": {
                        "available": any(
                            isinstance(plan_by_id.get(str(item.get("plan_id"))), Mapping)
                            and bool(plan_by_id[str(item.get("plan_id"))].get("sector_context"))
                            for item in contexts
                            if isinstance(item, Mapping) and item.get("plan_id")
                        ),
                        "by_plan": {
                            plan_id: plan.get("sector_context")
                            for plan_id, plan in plan_by_id.items()
                            if plan.get("sector_context") is not None
                        },
                    },
                    "TRADABILITY_FLAGS": {
                        key: value.get("tradability") for key, value in market_context.items()
                    },
                    "EXCHANGE_RULES": _exchange_rules_for(self.settings.exchange_rules_path, now),
                    "CURRENT_TIME": now.isoformat(),
                    "PRIOR_OUTCOME_FEEDBACK": None,
                    "TIGHTEN_AFTER": "13:45:00",
                    "NO_NEW_ENTRY_AFTER": "14:45:00",
                    "AFTER_HOURS_ENTRY_ENABLED": False,
                    "REQUIRED_CONFIRMATIONS": 2,
                    "CONFIRMATION_MINUTES": 2,
                    "DATA_SLA_POLICY": {"closed_bars_only": True, "fail_closed": True},
                }
            )
            system = bundle.render("00_shared_system_v2.txt", replacements) + "\n\n" + bundle.render(_A4_FILE, replacements)
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
            if bar.bar_end <= signal_time:
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
        "PENALTY_RULES": a2.get("penalty_rules", {}),
        "MAX_MA_BIAS": a3_ma.get("max_ma_bias_pct", 0.12),
        "MAX_ATR_EXTENSION": a3.get("max_atr_extension", 3.0),
        "MIN_REWARD_RISK": a3.get("minimum_reward_risk", 2.0),
        "MAX_STOP_DISTANCE": a3.get("max_stop_distance_pct", 0.06),
        "MIN_TECHNICAL_SCORE": a3.get("minimum_technical_score", 70),
        "TECHNICAL_SCORE_WEIGHTS": a3.get("score_weights", {}),
        "EARNINGS_BLACKOUT": {
            "days_before": a3_blackout.get("days_before", 3),
            "days_after": a3_blackout.get("days_after", 1),
            "action": a3_blackout.get("action", "FORCE_PROBE"),
        },
        "NORMAL_GAP_RANGE": [-0.02, 0.03],
        "NO_CHASE_THRESHOLD": 0.05,
        "REQUIRED_CONFIRMATIONS": a3.get("required_confirmations", []),
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


def _plan_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _canonical_symbol(raw.get("symbol"))
    zone = raw.get("trigger_zone") if isinstance(raw.get("trigger_zone"), Mapping) else {}
    return {
        **dict(raw),
        "symbol": symbol,
        "trigger_low": _float_or_none(zone.get("low")),
        "trigger_high": _float_or_none(zone.get("high")),
        "stop_level": _float_or_none(raw.get("invalidation_level")),
        "confirmation_bars": 2,
        "action": MonitorAction.BUY_SIGNAL.value,
    }


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


def _row_change(item: Any, _frozen: FrozenInputSnapshot) -> float:
    value = getattr(item, "change_ratio_pct", None)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def _research_universe_records(universe: UniverseSnapshot) -> tuple[Any, ...]:
    """Return the configured A1 quality domain, retaining eligible BJ stocks."""

    records = universe.research_candidates
    if len(records) != universe.lineage.research_candidate_count or not records:
        raise WorkflowError("RESEARCH_DATA_UNIVERSE_INCOMPLETE")
    return records


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
                "moving_averages": raw.get("moving_averages"),
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
        "reasons": value.get("reasons"),
        "timeframes": compact_frames,
    }


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None


def _plan_expiry(value: Any, now: datetime, slot: str) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None and parsed.utcoffset() is not None and parsed > now:
                return parsed.astimezone(SHANGHAI)
        except ValueError:
            pass
    day = now.date() if slot == "morning" else (now + timedelta(days=1)).date()
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime(day.year, day.month, day.day, 15, 0, tzinfo=SHANGHAI)


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
        result["statements"][dataset] = ordered[:8]
    indicators = sorted(
        grouped.get("INDICATORS", ()),
        key=lambda item: (str(item.get("ability") or ""), str(item.get("index_id") or "")),
    )
    result["indicators"] = indicators[:128]
    return result


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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


def _at_time(value: datetime, hour: int, minute: int) -> datetime:
    return value.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _slot(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {"morning", "close"}:
        raise WorkflowError("SLOT_INVALID")
    return normalized


__all__ = ["PreparedSnapshot", "WorkflowApplication", "WorkflowError"]
