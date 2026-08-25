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
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .data.cache import MinuteBarStore
from .data.cninfo import CninfoClient, CninfoFetchResult
from .data.cninfo_pdf import CninfoPdfClient, CninfoPdfEvidence
from .data.gov_policy import GovPolicyClient
from .data.mootdx import MootdxAdapter, MootdxNode, MinuteBar, map_symbol
from .data.open_news import OpenNewsClient, OpenNewsFetchResult
from .data.ths_industry import (
    collect_ths_industry_history,
    collect_ths_industry_membership,
    select_industry_diversified_symbols,
)
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
from .pipeline.factors import FactorEngine
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
from .pipeline.snapshot import FrozenInputSnapshot, UniverseGatePolicy, UniverseSnapshot
from .pipeline.technical_aggregates import build_technical_aggregates
from .redaction import digest_text, sanitize
from .reporting import atomic_write_json, atomic_write_text
from .runtime.monitor import MonitorBatchResult, MonitorEngine, rebuild_effective_markdown
from .runtime.calendar import ExchangeTradingCalendar, TradingCalendarError
from .runtime.scheduler import ScheduleKind, Scheduler
from .runtime.simulation import PaperBroker, SimulationAction, SimulationConfig
from .runtime.state import MonitorAction, PlanStatus, RuntimeStore
from .settings import Settings, load_yaml


SHANGHAI = ZoneInfo("Asia/Shanghai")
_A4_FILE = "agent_4_intraday_signal_v2.txt"


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
        self.prompts = PromptRepository(settings.prompt_dir)
        self.model_client = OpenAICompatibleModelClient(settings)
        self.monitor_model_client = OpenAICompatibleModelClient(
            settings.model_copy(update={"model_timeout_seconds": 45.0}),
            max_attempts=2,
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
        max_candidates: int | None = None,
        as_of: datetime | None = None,
    ) -> PreparedSnapshot:
        current = _aware(as_of or datetime.now(SHANGHAI))
        wall_now = datetime.now(SHANGHAI)
        if abs((current - wall_now).total_seconds()) > 600:
            raise WorkflowError("LIVE_FACTS_POINT_IN_TIME_UNSUPPORTED")
        candidate_limit = max_candidates or self.settings.research_max_candidates
        if not 1 <= candidate_limit <= 300:
            raise WorkflowError("CANDIDATE_LIMIT_INVALID")

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
        source_failures: dict[str, list[str]] = {}
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
            a1_config = source_config.get("agent_1", {})
            if not isinstance(a1_config, Mapping):
                raise WorkflowError("A1_CONFIG_INVALID")
            top_n_per_node = int(a1_config.get("top_n_per_node", 8))
            node_count_target = a1_config.get("node_count_target", [40, 80])
            if not isinstance(node_count_target, list):
                raise WorkflowError("A1_NODE_TARGET_INVALID")

            # A1 is node-first.  Build the full THS reverse membership graph
            # before selecting companies, then take a deterministic Top-N per
            # specific industry node.  A global turnover slice would collapse
            # the macro/fundamental pool into the day's hottest few sectors.
            industry_catalog = client.ths_index_catalog(tag="industry")
            full_membership = collect_ths_industry_membership(
                client,
                industry_catalog,
                [candidate.symbol for candidate in universe.trade_candidates],
                cache_dir=self.settings.fact_store_dir / "ths_industry",
                as_of=current,
            )
            if not full_membership.ok or not full_membership.complete:
                raise WorkflowError(f"THS_INDUSTRY_MEMBERSHIP_NOT_READY:{full_membership.reason_code}")

            # Keep a bounded reserve so source-level failures can be backfilled
            # without violating the same node selection rule.
            reserve_limit = min(len(universe.trade_candidates), candidate_limit + 10)
            selected_symbols, g0_selection = select_industry_diversified_symbols(
                universe.trade_candidates,
                full_membership,
                limit=reserve_limit,
                top_n_per_node=top_n_per_node,
                node_count_target=node_count_target,
            )
            if len(selected_symbols) < reserve_limit:
                raise WorkflowError("A1_NODE_DIVERSIFICATION_INSUFFICIENT")
            record_by_symbol = {candidate.symbol: candidate for candidate in universe.trade_candidates}
            selected = tuple(record_by_symbol[symbol] for symbol in selected_symbols)
            market_fact_results = collect_market_results(
                client,
                [candidate.symbol for candidate in selected],
            )
            market_fact_results["THS_INDUSTRY_CATALOG"] = industry_catalog
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
            start_ms = int((current - timedelta(days=800)).timestamp() * 1000)
            end_ms = int(current.timestamp() * 1000)
            daily: dict[str, Any] = {}
            fundamental: dict[str, Any] = {}
            technical: dict[str, Any] = {}
            for candidate in selected:
                symbol = candidate.symbol
                history = client.history_1d(
                    symbol,
                    start=start_ms,
                    end=end_ms,
                    adjust="none",
                    limit=1000,
                    max_pages=3,
                )
                income = client.income_statements(symbol, limit=20, max_pages=3)
                indicators = client.financial_indicators(symbol, limit=100, max_pages=3)
                balance = client.balance_sheets(symbol, limit=20)
                cash_flow = client.cash_flow_statements(symbol, limit=20)
                if history.ok and history.complete:
                    daily[symbol] = [row.model_dump(mode="json") for row in history.items]
                    if len(history.items) < gate_policy.newly_listed_min_days:
                        source_failures.setdefault(symbol, []).append("G0:LISTING_HISTORY_INSUFFICIENT")
                        daily.pop(symbol, None)
                else:
                    source_failures.setdefault(symbol, []).append(f"DAILY:{history.reason_code}")
                financial_rows: list[dict[str, Any]] = []
                financial_results = (
                    ("INCOME", income),
                    ("INDICATORS", indicators),
                    ("BALANCE", balance),
                    ("CASH_FLOW", cash_flow),
                )
                financial_complete = True
                for dataset, result in financial_results:
                    if result.ok and result.complete and result.items:
                        financial_rows.extend(
                            {"_dataset": dataset, **row.model_dump(mode="json")}
                            for row in result.items
                        )
                    else:
                        financial_complete = False
                        source_failures.setdefault(symbol, []).append(
                            f"{dataset}:{result.reason_code if result.items or not result.ok else 'EMPTY_DATA'}"
                        )
                if financial_complete:
                    fundamental[symbol] = financial_rows

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
        with CninfoClient(
            timeout_seconds=self.settings.timeout_seconds,
            base_url=self.settings.cninfo_base_url,
            min_request_interval_seconds=self.settings.cninfo_min_request_interval_seconds,
        ) as cninfo:
            for candidate in selected:
                symbol = candidate.symbol
                recent_result = cninfo.fetch_announcements(symbol, query_start, query_end)
                business_result = cninfo.fetch_announcements(
                    symbol,
                    business_query_start,
                    query_end,
                    search_keyword="年度报告",
                )
                result = _merge_cninfo_query_results(recent_result, business_result)
                cninfo_results[symbol] = result
                if not recent_result.ok or not recent_result.complete:
                    source_failures.setdefault(symbol, []).append(f"CNINFO:{recent_result.reason_code}")
                    # Announcement query success is a P0 company boundary.
                    # Removing fundamentals excludes this symbol in the
                    # canonical freeze without changing the full universe.
                    fundamental.pop(symbol, None)
                if not business_result.ok or not business_result.complete:
                    source_failures.setdefault(symbol, []).append(
                        f"CNINFO_MAIN_BUSINESS:{business_result.reason_code}"
                    )
        if self.settings.cninfo_pdf_max_documents_per_symbol:
            with CninfoPdfClient(
                self.settings.cninfo_pdf_cache_dir,
                timeout_seconds=self.settings.timeout_seconds,
            ) as pdf_client:
                for symbol, result in sorted(cninfo_results.items()):
                    for announcement in select_cninfo_pdf_candidates(
                        result,
                        limit=self.settings.cninfo_pdf_max_documents_per_symbol,
                    ):
                        evidence = pdf_client.fetch_evidence(announcement)
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
        news_results = self._collect_open_news([candidate.symbol for candidate in selected])
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

        frozen = FrozenInputSnapshot.freeze(
            universe,
            as_of=current,
            daily_payload=daily,
            fundamental_payload=fundamental,
            fact_payload=fact_payload,
            max_candidates=reserve_limit,
            candidate_symbols=selected_symbols,
        )
        for candidate in frozen.trade_candidates:
            symbol = candidate.symbol
            required_bars = self.settings.mootdx_history_5m_required_bars
            cached_bars = self.minute_store.load_latest(symbol, "5m", limit=required_bars)
            if _minute_cache_ready(cached_bars, required_bars=required_bars, as_of=current):
                minute_bars = cached_bars
            else:
                minutes = self.mootdx.fetch_bars(
                    symbol,
                    "5m",
                    required_bars,
                    as_of=current,
                )
                if not minutes.complete:
                    source_failures.setdefault(symbol, []).append(f"MOOTDX:{minutes.reason_code}")
                    continue
                self.minute_store.write(minutes.bars)
                minute_bars = minutes.bars
            factor = FactorEngine(symbol).compute(
                daily_bars=daily.get(symbol, ()),
                minute_bars=minute_bars,
                as_of=current,
            )
            aggregates = build_technical_aggregates(factor)
            technical[symbol] = {
                **_compact_factor(factor.model_dump(mode="json")),
                "kline_patterns": aggregates["KLINE_PATTERNS"],
                "price_levels": aggregates["PRICE_LEVELS"],
            }
            if sum(item.get("ready") is True for item in technical.values()) >= candidate_limit:
                break

        factor_ready = sorted(symbol for symbol, item in technical.items() if item.get("ready") is True)
        if not factor_ready:
            raise WorkflowError("NO_FACTOR_READY_CANDIDATES")
        factor_ready_order, factor_ready_selection = select_industry_diversified_symbols(
            [record_by_symbol[symbol] for symbol in factor_ready],
            full_membership,
            limit=len(factor_ready),
            top_n_per_node=top_n_per_node,
            node_count_target=node_count_target,
        )
        if set(factor_ready_order) != set(factor_ready):
            raise WorkflowError("A1_FACTOR_READY_NODE_LINEAGE_INVALID")
        frozen = FrozenInputSnapshot.freeze(
            universe,
            as_of=current,
            daily_payload=daily,
            fundamental_payload=fundamental,
            technical_payload=technical,
            fact_payload=fact_payload,
            max_candidates=reserve_limit,
            candidate_symbols=selected_symbols,
        )
        raw_path = self.settings.snapshot_dir / "raw" / f"{frozen.snapshot_id}.json"
        frozen.write_json(raw_path)
        data = sanitize(self._research_input(
            frozen=frozen,
            universe=universe,
            technical=technical,
            g0_symbols=factor_ready,
            source_failures=source_failures,
            g0_selection={
                **factor_ready_selection,
                "reserve_strategy": g0_selection.get("strategy"),
                "reserve_selected_count": len(selected_symbols),
                "selected_count": len(factor_ready),
                "factor_ready_symbols": factor_ready,
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
            selected_count=len(factor_ready),
            factor_ready_count=len(factor_ready),
        )

    def _collect_open_news(self, symbols: list[str]) -> dict[str, OpenNewsFetchResult]:
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
            flash = client.fetch_cls_roll(page_size=self.settings.open_news_flash_limit)
            results[flash.source_id] = flash
            global_news = client.fetch_eastmoney_7x24(page_size=self.settings.open_news_flash_limit)
            results[global_news.source_id] = global_news
            for symbol in symbols:
                item = client.fetch_eastmoney_stock_news(
                    symbol,
                    page_size=self.settings.open_news_stock_limit,
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
                futures = [executor.submit(fetch_rss, source) for source in rss_sources]
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
        max_candidates: int | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        normalized_slot = _slot(slot)
        current = _aware(as_of or datetime.now(SHANGHAI))
        self._ensure_trading_day(current)
        if normalized_slot == "morning" and current.hour == 9 and current.minute < 26:
            time.sleep(max(0.0, (_at_time(current, 9, 26) - current).total_seconds()))
            current = datetime.now(SHANGHAI)
        prepared = self.prepare_snapshot(max_candidates=max_candidates, as_of=current)
        run_id = f"{current.date()}-{normalized_slot}-{prepared.snapshot.snapshot_hash[:12]}"
        pipeline = ResearchPipeline(
            self.settings,
            prompt_repository=self.prompts,
            model_client=self.model_client,
            output_dir=self.settings.workflow_output_dir / "research",
            parallel_lanes=True,
            runtime_store=self.store,
            slot=normalized_slot,
        )
        result = pipeline.run(prepared.snapshot, run_id=run_id, generated_at=current)
        publication = self._publish_plans(result, normalized_slot, datetime.now(SHANGHAI))
        summary = {
            "run_id": run_id,
            "slot": normalized_slot,
            "status": result.status,
            "snapshot": prepared.as_dict(),
            "research_markdown": str(result.markdown_path) if result.markdown_path else None,
            "plan_publication": publication,
        }
        atomic_write_json(self.settings.workflow_output_dir / "runs" / f"{run_id}.json", summary)
        return summary

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
            for item in frozen.trade_candidates
            if item.symbol in set(g0_symbols)
        ]
        missing = {"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"}
        facts = frozen.fact_payload.get("facts", {})
        if not isinstance(facts, Mapping):
            facts = {}
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
            },
            "g0_symbols": g0_symbols,
            "trade_candidates": selected_records,
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
                for item in frozen.trade_candidates
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
            "DRAGON_TIGER_SNAPSHOT": dragon_tiger or missing,
            "MARKET_ATTENTION_SNAPSHOT": hot_stocks or missing,
            "DISCLOSURE_EVENTS": disclosure_events,
            "RISK_EVENTS": risk_events,
            "MACRO_POLICY_FEED": macro_policy_feed,
            "MACRO_ECONOMIC_DATA": {
                "available": False,
                "reason_code": "SOURCE_NOT_CONFIGURED",
                "required_series": ["GDP", "CPI", "PPI", "PMI", "SOCIAL_FINANCING", "NEW_LOANS"],
                "substitution_forbidden": True,
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
            "MACRO_POLICY_FEED", "MACRO_ECONOMIC_DATA", "INDUSTRY_NEWS_FEED", "INDUSTRY_PROFIT_DATA", "THS_INDUSTRY_MEMBERSHIP", "EXISTING_CHAIN_GRAPH",
            "THEME_REGISTRY", "DISCLOSURE_EVENTS", "RISK_EVENTS", "RESEARCH_CONSENSUS", "FUND_HOLDINGS",
            "FAST_TRACK_REQUESTS", "PRIOR_OUTCOME_FEEDBACK", "SECTOR_CYCLE_SNAPSHOT", "CAPITAL_FLOW_SNAPSHOT",
            "NEWS_HEAT_SNAPSHOT", "CROWDING_SNAPSHOT", "AUCTION_SNAPSHOT", "SECTOR_PERMISSIONS",
        ):
            values.setdefault(key, missing)
        values.update(_prompt_parameters(source_config))
        return values

    def _publish_plans(self, result: ResearchRunResult, slot: str, now: datetime) -> dict[str, Any]:
        batch: list[dict[str, Any]] = []
        blocked: list[dict[str, str]] = []
        ready_lanes: list[str] = []
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

    def _ensure_trading_day(self, current: datetime) -> None:
        try:
            is_session = self.trading_calendar.is_trading_day(current.date())
        except TradingCalendarError as exc:
            raise WorkflowError(exc.reason_code) from exc
        if not is_session:
            raise WorkflowError("NON_TRADING_DAY")
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
            "pool_min": a1.get("pool_min", 120),
            "pool_max": a1.get("pool_max", 300),
            "node_count_target": a1.get("node_count_target", [40, 80]),
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
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
