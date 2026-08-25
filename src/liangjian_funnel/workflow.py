"""End-to-end orchestration for the standalone shadow/simulation workflow."""

from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .data.cache import MinuteBarStore
from .data.cninfo import CninfoClient
from .data.cninfo_pdf import CninfoPdfClient, CninfoPdfEvidence
from .data.gov_policy import GovPolicyClient
from .data.mootdx import MootdxAdapter, MootdxNode, MinuteBar, map_symbol
from .data.open_news import OpenNewsClient, OpenNewsFetchResult
from .data.ths_industry import collect_ths_industry_membership
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
from .pipeline.snapshot import FrozenInputSnapshot, UniverseSnapshot
from .pipeline.technical_aggregates import build_technical_aggregates
from .redaction import digest_text
from .reporting import atomic_write_json, atomic_write_text
from .runtime.monitor import MonitorBatchResult, MonitorEngine
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
            max_attempts=1,
        )
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

        source_failures: dict[str, list[str]] = {}
        with HithinkClient(self.settings) as client:
            catalog = client.ticker_catalog(limit=1000, max_pages=10)
            market = client.market_snapshot(limit=1000, max_pages=10)
            universe = UniverseSnapshot.from_records(catalog, market, as_of=current)
            if not universe.ready:
                raise WorkflowError("UNIVERSE_NOT_READY")
            selected = universe.deterministic_preselect(candidate_limit)
            market_fact_results = collect_market_results(
                client,
                [candidate.symbol for candidate in selected],
            )
            market_fact_results["THS_INDUSTRY_MEMBERSHIP"] = collect_ths_industry_membership(
                client,
                market_fact_results["THS_INDUSTRY_CATALOG"],
                [candidate.symbol for candidate in selected],
                cache_dir=self.settings.fact_store_dir / "ths_industry",
                as_of=current,
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

        policy_start = (current.date() - timedelta(days=6)).isoformat()
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
                result = cninfo.fetch_announcements(symbol, query_start, query_end)
                cninfo_results[symbol] = result
                if not result.ok or not result.complete:
                    source_failures.setdefault(symbol, []).append(f"CNINFO:{result.reason_code}")
                    # Announcement query success is a P0 company boundary.
                    # Removing fundamentals excludes this symbol in the
                    # canonical freeze without changing the full universe.
                    fundamental.pop(symbol, None)
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
            max_candidates=candidate_limit,
        )
        for candidate in frozen.trade_candidates:
            symbol = candidate.symbol
            minutes = self.mootdx.fetch_bars(
                symbol,
                "5m",
                self.settings.mootdx_history_5m_required_bars,
                as_of=current,
            )
            if not minutes.complete:
                source_failures.setdefault(symbol, []).append(f"MOOTDX:{minutes.reason_code}")
                continue
            self.minute_store.write(minutes.bars)
            factor = FactorEngine(symbol).compute(
                daily_bars=daily.get(symbol, ()),
                minute_bars=minutes.bars,
                as_of=current,
            )
            aggregates = build_technical_aggregates(factor)
            technical[symbol] = {
                **_compact_factor(factor.model_dump(mode="json")),
                "kline_patterns": aggregates["KLINE_PATTERNS"],
                "price_levels": aggregates["PRICE_LEVELS"],
            }

        factor_ready = sorted(symbol for symbol, item in technical.items() if item.get("ready") is True)
        if not factor_ready:
            raise WorkflowError("NO_FACTOR_READY_CANDIDATES")
        frozen = FrozenInputSnapshot.freeze(
            universe,
            as_of=current,
            daily_payload=daily,
            fundamental_payload=fundamental,
            technical_payload=technical,
            fact_payload=fact_payload,
            max_candidates=candidate_limit,
        )
        raw_path = self.settings.snapshot_dir / "raw" / f"{frozen.snapshot_id}.json"
        frozen.write_json(raw_path)
        data = self._research_input(
            frozen=frozen,
            universe=universe,
            technical=technical,
            g0_symbols=factor_ready,
            source_failures=source_failures,
            raw_snapshot_path=raw_path,
            as_of=current,
        )
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
            selected_count=len(selected),
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
        if normalized_slot == "morning":
            for broker in self.brokers.values():
                broker.start_trading_day()
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
        minute_snapshot_id = f"minute-{current.strftime('%Y%m%dT%H%M%S%z')}"
        results: list[dict[str, Any]] = []
        simulation: list[dict[str, Any]] = []
        for lane_id in self.brokers:
            plans = self.store.list_active_plans(lane_id, at=current)
            bars: dict[str, MinuteBar] = {}
            data_ok = True
            scope_symbols = {str(plan["symbol"]) for plan in plans}
            scope_symbols.update(
                str(position["symbol"])
                for position in self.store.list_positions(f"paper:{lane_id}")
            )
            for symbol in sorted(scope_symbols):
                fetched = self.mootdx.fetch_bars(symbol, "1m", 2, as_of=current)
                if not fetched.complete:
                    data_ok = False
                    continue
                self.minute_store.write(fetched.bars)
                bars[symbol] = fetched.bars[-1]
                simulation.extend(self._settle_prior_signals(lane_id, symbol, fetched.bars[-1]))

            engine = MonitorEngine(
                self.store,
                llm_veto=self._a4_callback(lane_id, plans, bars, current),
                effective_md_path=self.settings.workflow_output_dir / "monitor" / "effective_signals.md",
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
            results.append(_batch_dict(batch))
        payload = {
            "minute_snapshot_id": minute_snapshot_id,
            "time": current.isoformat(),
            "lanes": results,
            "simulation": simulation,
        }
        atomic_write_json(self.settings.workflow_output_dir / "monitor" / "latest.json", payload)
        return payload

    def run_due(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = _aware(now or datetime.now(SHANGHAI))
        scheduler = Scheduler(
            self.store,
            callbacks={
                ScheduleKind.MORNING_0925: lambda _job: self.run_research("morning", as_of=current),
                ScheduleKind.CLOSE_1510: lambda _job: self.run_research("close", as_of=current),
                ScheduleKind.MONITOR: lambda _job: self.monitor_once(now=current),
            },
            owner="liangjian-runtime",
        )
        records = scheduler.dispatch_once(current)
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
        regime = "TREND_MAINLINE" if breadth >= 0.58 else "RISK_OFF_RETREAT" if breadth <= 0.35 else "ROTATION_NO_MAINLINE"
        regime_matrix = source_config.get("regime_parameter_matrix")
        if not isinstance(regime_matrix, Mapping):
            regime_matrix = source_config.get("regime_overrides", {})
        regime_parameters = regime_matrix.get(regime, {}) if isinstance(regime_matrix, Mapping) else {}
        if not isinstance(regime_parameters, Mapping):
            regime_parameters = {}
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
        exchange_rules = {
            "snapshot_id": "CN-A-SIMULATION-20260824",
            "external_orders": False,
            "simulation_only": True,
            "lot_size": 100,
            "t_plus_one": True,
            "bj_trade_enabled": False,
        }
        values: dict[str, Any] = {
            "snapshot_manifest": {
                "as_of": as_of.isoformat(),
                "frozen": True,
                "full_universe_count": len(universe.records),
                "research_universe_count": len(universe.research_candidates),
                "trade_universe_count": len(universe.trade_candidates),
                "selected_count": len(selected_records),
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
            "FACTOR_SNAPSHOT": {
                key: {
                    item_key: item_value
                    for item_key, item_value in value.items()
                    if item_key not in {"kline_patterns", "price_levels"}
                }
                for key, value in technical.items()
                if key in g0_symbols and isinstance(value, Mapping)
            },
            "MARKET_REGIME_SNAPSHOT": {"regime": regime, "position_cap_pct": 0.5, "risk_warnings": []},
            "MARKET_EMOTION_SNAPSHOT": market_emotion,
            "LIQUIDITY_SNAPSHOT": {item.symbol: {"turnover": item.amount} for item in universe.records if item.symbol in g0_symbols},
            "TRADABILITY_FLAGS": {symbol: {"tradable": True, "simulation_only": True} for symbol in g0_symbols},
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
            "MARKET_CONTEXT": {"regime": regime, "breadth": breadth},
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
            "MACRO_POLICY_FEED", "INDUSTRY_NEWS_FEED", "INDUSTRY_PROFIT_DATA", "THS_INDUSTRY_MEMBERSHIP", "EXISTING_CHAIN_GRAPH",
            "THEME_REGISTRY", "DISCLOSURE_EVENTS", "RISK_EVENTS", "RESEARCH_CONSENSUS", "FUND_HOLDINGS",
            "FAST_TRACK_REQUESTS", "PRIOR_OUTCOME_FEEDBACK", "SECTOR_CYCLE_SNAPSHOT", "CAPITAL_FLOW_SNAPSHOT",
            "NEWS_HEAT_SNAPSHOT", "CROWDING_SNAPSHOT", "AUCTION_SNAPSHOT", "SECTOR_PERMISSIONS",
        ):
            values.setdefault(key, missing)
        values.update(_prompt_parameters(source_config))
        return values

    def _publish_plans(self, result: ResearchRunResult, slot: str, now: datetime) -> dict[str, Any]:
        created: list[str] = []
        activated: list[str] = []
        blocked: list[dict[str, str]] = []
        for lane in result.lanes:
            if lane.status != "READY" or not isinstance(lane.final_output, Mapping):
                blocked.append({"lane": lane.lane, "reason": "LANE_NOT_READY"})
                continue
            plans = lane.final_output.get("core_watch_pool")
            if not isinstance(plans, list):
                blocked.append({"lane": lane.lane, "reason": "CORE_WATCH_POOL_MISSING"})
                continue
            previous = {
                str(item["symbol"]): item
                for item in self.store.list_execution_plans(lane_id=lane.lane, status=PlanStatus.PENDING_MORNING_REVIEW)
            }
            if slot == "close":
                for active in self.store.list_execution_plans(lane_id=lane.lane, status=PlanStatus.ACTIVE_TODAY):
                    self.store.invalidate_plan(str(active["plan_id"]), status=PlanStatus.EXPIRED)
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
                    self.store.create_execution_plan(
                        plan_id,
                        lane.lane,
                        symbol,
                        status=PlanStatus.DRAFT_CLOSE,
                        expires_at=_plan_expiry(payload.get("plan_expiry"), now, slot),
                        payload=payload,
                    )
                    self.store.set_plan_pending_morning_review(plan_id)
                    created.append(plan_id)
                    continue
                parent = previous.get(symbol)
                if now.time().replace(tzinfo=None) > datetime.strptime("09:40", "%H:%M").time():
                    blocked.append({"lane": lane.lane, "symbol": symbol, "reason": "MORNING_PUBLICATION_DEADLINE"})
                    continue
                if parent is None or not _tightens(parent, payload):
                    blocked.append({"lane": lane.lane, "symbol": symbol, "reason": "MORNING_NOT_TIGHTEN_ONLY"})
                    continue
                self.store.create_execution_plan(
                    plan_id,
                    lane.lane,
                    symbol,
                    status=PlanStatus.DRAFT_CLOSE,
                    expires_at=_plan_expiry(payload.get("plan_expiry"), now, slot),
                    payload=payload,
                )
                self.store.set_plan_pending_morning_review(plan_id)
                self.store.activate_plan(plan_id, valid_from=_at_time(now, 9, 32))
                self.store.invalidate_plan(str(parent["plan_id"]))
                created.append(plan_id)
                activated.append(plan_id)
        return {"created": created, "activated": activated, "blocked": blocked}

    def _a4_callback(
        self,
        lane_id: str,
        plans: tuple[dict[str, Any], ...],
        bars: Mapping[str, MinuteBar],
        now: datetime,
    ):
        if not plans:
            return None
        bundle = self.prompts.bundle()
        plan_by_id = {str(plan["plan_id"]): plan for plan in plans}

        def callback(context: Mapping[str, Any]) -> Mapping[str, Any]:
            contexts = context.get("plans") if isinstance(context.get("plans"), list) else [context]
            replacements = {name: None for name in bundle.shared.placeholders + bundle.document(_A4_FILE).placeholders}
            replacements.update(
                {
                    "EXECUTION_PLANS": list(plans),
                    "TRIGGER_ENGINE_RESULT": contexts,
                    "REALTIME_QUOTE": {key: value.model_dump(mode="json") for key, value in bars.items()},
                    "CLOSED_BARS": {key: value.model_dump(mode="json") for key, value in bars.items()},
                    "REALTIME_MA": {"available": False, "reason_code": "NOT_RECOMPUTED_THIS_MINUTE"},
                    "OPEN_SIGNAL_STATE": {"lane_id": lane_id},
                    "MARKET_CONTEXT": {"time": now.isoformat()},
                    "SECTOR_CONTEXT": {"available": False},
                    "TRADABILITY_FLAGS": {key: {"tradable": True} for key in bars},
                    "EXCHANGE_RULES": {"simulation_only": True, "external_orders": False, "t_plus_one": True},
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
            if event.get("action") not in {MonitorAction.BUY_SIGNAL.value, MonitorAction.ADD_SIGNAL.value, MonitorAction.FORCED_RISK_EXIT.value}:
                continue
            payload = json.loads(event.get("payload_json") or "{}")
            if payload.get("symbol") != symbol:
                continue
            plan = self.store.get_execution_plan(str(payload.get("plan_id") or ""))
            plan_payload = json.loads(plan.get("payload_json") or "{}") if plan else {}
            action = "SELL" if event["action"] == MonitorAction.FORCED_RISK_EXIT.value else "ADD" if event["action"] == MonitorAction.ADD_SIGNAL.value else "BUY"
            signal_time = datetime.fromisoformat(str(event["minute_end"]))
            if bar.bar_end <= signal_time:
                continue
            simulation_action = SimulationAction(
                account_id=f"paper:{lane_id}",
                signal_id=str(event["event_key"]),
                symbol=symbol,
                action=action,
                signal_bar_end=signal_time,
                entry_reference=plan_payload.get("trigger_low") or bar.open,
                stop_level=plan_payload.get("stop_level"),
                risk_unit=0.33 if plan_payload.get("risk_unit") == "PROBE" else 1.0,
                plan_id=payload.get("plan_id"),
            )
            outcome = broker.apply(simulation_action, bar)
            results.append(outcome.model_dump(mode="json"))
        return results


def _prompt_parameters(config: Mapping[str, Any]) -> dict[str, Any]:
    a1 = config.get("agent_1", {}) if isinstance(config.get("agent_1"), Mapping) else {}
    a2 = config.get("agent_2", {}) if isinstance(config.get("agent_2"), Mapping) else {}
    a3 = config.get("agent_3", {}) if isinstance(config.get("agent_3"), Mapping) else {}
    return {
        "TOP_N_PER_NODE": a1.get("top_n_per_node", 8),
        "PRIOR_CONTRIBUTION_CAP": 0.20,
        "THEME_EXPIRY_DAYS": 5,
        "POLICY_CALENDAR_HORIZON_DAYS": 90,
        "BOTTLENECK_MIN_EVIDENCE": 2,
        "SCORE_WEIGHTS": a1.get("score_weights", {}),
        "FAST_TRACK_DAILY_QUOTA": 0,
        "FAST_TRACK_COOLDOWN_DAYS": 30,
        "CLIMAX_NEW_ENTRY_POLICY": "PROBE_ONLY",
        "DIVERGENCE_NEW_ENTRY_POLICY": "NO_NEW_ENTRY",
        "MIN_SECTOR_COVERAGE": 0.7,
        "ROTATION_LOOKBACK_DAYS": 5,
        "LEADER_MIN_CRITERIA": 3,
        "LOW_IDENTITY_TRIGGER_COUNT": 2,
        "MIN_FREE_FLOAT_CAP": 3_000_000_000,
        "MIN_IDENTIFIABILITY_SCORE": 70,
        "MAX_LEADERS_PER_THEME": 5,
        "THEME_SCORE_WEIGHTS": a2.get("score_weights", {}),
        "PENALTY_RULES": a2.get("penalty_rules", {}),
        "MAX_MA_BIAS": a3.get("max_ma_bias", 0.10),
        "MAX_ATR_EXTENSION": a3.get("max_atr_extension", 3.0),
        "MIN_REWARD_RISK": a3.get("minimum_reward_risk", 2.0),
        "MAX_STOP_DISTANCE": a3.get("max_stop_distance_pct", 0.06),
        "EARNINGS_BLACKOUT": 3,
        "NORMAL_GAP_RANGE": [-0.02, 0.03],
        "NO_CHASE_THRESHOLD": 0.05,
        "REQUIRED_CONFIRMATIONS": 2,
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
        by_symbol[symbol] = sorted(
            by_symbol[symbol],
            key=lambda item: (str(item.get("publish_time") or ""), str(item.get("fact_id") or "")),
            reverse=True,
        )[:20]

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
        "technical_summary": value.get("technical_summary"),
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
