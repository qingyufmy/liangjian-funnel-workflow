from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.deterministic import local_active_items, screen_a1, screen_a2, screen_a3
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore
from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 27, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


def _frame() -> dict:
    return {
        "ready": True,
        "ma_alignment": "BULL_STACK",
        "ma_event": "NONE",
        "ma_bias": {"close_vs_ma20_pct": 0.02},
        "moving_averages": {"ma5": 11, "ma20": 10},
    }


def _snapshot(symbol_count: int = 8) -> dict:
    symbols = [f"{600000 + index:06d}.SH" for index in range(symbol_count)]
    industry_rows = []
    candidates = []
    fundamentals = {}
    evidence = {}
    factors = {}
    levels = {}
    flags = {}
    for index, symbol in enumerate(symbols):
        code = "884001.TI" if index < symbol_count - 1 else "884999.TI"
        name = "算力设备" if code == "884001.TI" else "食品加工"
        industry_rows.append({
            "thscode": symbol,
            "mapping_status": "MAPPED",
            "memberships": [{"industry_thscode": code, "industry_name": name}],
        })
        candidates.append({"symbol": symbol, "name": f"公司{index}", "amount": 5_000_000_000 - index * 100_000_000})
        fundamentals[symbol] = {
            "dataset_coverage": {"core_reports_complete": True, "indicators_available": True},
            "indicators": [
                {"index_id": "index_weighted_avg_roe", "value": 18 - index * 0.1},
                {"index_id": "sale_gross_margin", "value": 42 - index * 0.1},
                {"index_id": "net_profit_cash_content", "value": 1.1},
            ],
        }
        evidence[symbol] = {
            "available": index != symbol_count - 2,
            "evidence": ([{
                "source_ref": f"cninfo:{symbol}:page:1",
                "page_number": 1,
                "publish_time": "2026-04-30",
                "text": "算力设备业务占公司营业收入65.5%",
            }] if index != symbol_count - 2 else []),
        }
        factors[symbol] = {
            "ready": True,
            "timeframes": {name: _frame() for name in ("weekly", "daily", "120m", "15m", "5m")},
            "technical_summary": {"relative_strength_score": 80 - index},
        }
        levels[symbol] = {
            "available": True,
            "trigger_zone": {"low": 10, "high": 10.5},
            "invalidation": 9.5,
            "stop_distance_pct": 0.05,
            "first_resistance": 12,
            "reward_risk": 3.0,
        }
        flags[symbol] = {"available": True, "tradable": True, "exclusion_reasons": []}
    return {
        "DETERMINISTIC_RESEARCH_V2_ENABLED": True,
        "g0_symbols": symbols,
        "g0_candidates": candidates,
        "snapshot_manifest": {"as_of": NOW.isoformat(), "source_checksums": {"daily": "d" * 64}},
        "THS_INDUSTRY_CATALOG": {
            "available": True,
            "records": [
                {"thscode": "884001.TI", "name": "算力设备"},
                {"thscode": "884999.TI", "name": "食品加工"},
            ],
        },
        "THS_CONCEPT_CATALOG": {"available": True, "records": []},
        "THS_INDUSTRY_MEMBERSHIP": {"available": True, "records": industry_rows},
        "THS_CONCEPT_MEMBERSHIP": {"available": True, "records": []},
        "COMPANY_FUNDAMENTALS": fundamentals,
        "MAIN_BUSINESS_EVIDENCE": evidence,
        "RISK_EVENTS": {"available": True, "records": []},
        "TRADABILITY_FLAGS": flags,
        "FACTOR_SNAPSHOT": factors,
        "PRICE_LEVELS": levels,
        "MARKET_ATTENTION_SNAPSHOT": {"available": True, "records": []},
        "DRAGON_TIGER_SNAPSHOT": {"available": True, "records": []},
        "MACRO_POLICY_FEED": {"official_documents": [{"fact_id": "policy-ref"}]},
        "SCORE_WEIGHTS": {
            "structural_theme": 0.2,
            "business_mapping": 0.2,
            "barrier_and_bottleneck": 0.15,
            "financial_quality": 0.2,
            "cash_flow_quality": 0.1,
            "evidence_quality": 0.15,
        },
        "A1_MINIMUMS": {"minimum_score": 65, "minimum_data_quality": 75},
        "STRICT_AGENT_RULES": False,
        "config_hash": "c" * 64,
    }


def _discovery() -> dict:
    return {
        "structural_themes": [{"theme_id": "theme-compute", "display_name": "算力", "source_refs": ["policy-ref"]}],
        "industry_chain_graph": [{
            "node_id": "node-compute-device",
            "theme_ids": ["theme-compute"],
            "demand_driver": "算力设备",
            "source_refs": ["policy-ref"],
        }],
        "taxonomy_links": [{
            "node_id": "node-compute-device",
            "industry_thscodes": ["884001.TI"],
            "concept_thscodes": [],
        }],
    }


def test_a1_evaluates_every_g0_and_keeps_local_research_coverage():
    snapshot = _snapshot()
    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=5, llm_top_n_per_theme=2)

    assert result.summary["evaluated_count"] == len(snapshot["g0_symbols"])
    assert len(result.review_symbols) == 2
    assert len({item["symbol"] for item in result.decisions}) == len(snapshot["g0_symbols"])
    assert next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][-1])["status"] == "OUTSIDE_THEME"
    assert next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][-2])["status"] == "LOCAL_MONITOR"
    assert all(item["sent_to_llm"] is (item["symbol"] in result.review_symbols) for item in result.decisions)
    # The two per-theme representatives are sent to the model; the remaining
    # four rows with exact disclosed business exposure stay in the local A1
    # research layer instead of being discarded by the review budget.
    assert len(local_active_items(result)) == 4


def test_a1_missing_factor_weight_stays_monitor_and_zero_without_proxy():
    snapshot = _snapshot(2)
    snapshot["SCORE_WEIGHTS"] = {
        "structural_theme": 0.20,
        "business_mapping": 0.20,
        "barrier_and_bottleneck": 0.15,
        "financial_quality": 0.20,
        "catalyst_confirmation": 0.15,
        "valuation_expectation_gap": 0.10,
    }
    snapshot["A1_MINIMUMS"]["minimum_available_weight"] = 0.70

    result = screen_a1(snapshot, _discovery(), local_top_n_per_node=1, llm_top_n_per_theme=1)
    item = next(item for item in result.decisions if item["symbol"] == snapshot["g0_symbols"][0])

    assert item["status"] == "LOCAL_MONITOR"
    # This fixture deliberately makes the only themed symbol lack disclosed
    # business evidence.  Only structural theme and financial quality remain
    # available; missing business mapping must not be counted as coverage.
    assert item["available_weight"] == 0.40
    assert "A1_FACTOR_COVERAGE_BELOW_MINIMUM" in item["reason_codes"]
    for factor_name in (
        "barrier_and_bottleneck",
        "catalyst_confirmation",
        "valuation_expectation_gap",
    ):
        assert item["factor_details"][factor_name]["available"] is False
        assert item["factor_details"][factor_name]["score"] == 0.0
    assert local_active_items(result) == []


def test_a2_and_a3_never_expand_the_upstream_pool():
    snapshot = _snapshot(4)
    a1_output = {
        "active_research_pool": [
            {"symbol": symbol, "candidate_id": f"a1:{symbol}", "primary_theme": "theme-compute", "structural_score": 80}
            for symbol in snapshot["g0_symbols"][:3]
        ]
    }
    a2 = screen_a2(snapshot, a1_output, minimum_identifiability_score=40, llm_top_n_per_theme=2)
    assert set(a2.review_symbols).issubset(set(snapshot["g0_symbols"][:3]))
    a2_output = {"focus_pool": [{"symbol": symbol, "theme_id": "theme-compute"} for symbol in a2.review_symbols]}
    a3 = screen_a3(snapshot, a2_output)
    assert set(a3.review_symbols).issubset(set(a2.review_symbols))


def test_a2_market_core_uses_real_local_market_factors_without_inventing_capital_flow():
    snapshot = _snapshot(4)
    for index, candidate in enumerate(snapshot["g0_candidates"]):
        candidate["change_ratio_pct"] = 2.0 - index * 0.5
    snapshot["THEME_SCORE_WEIGHTS"] = {
        "breadth": 0.15,
        "turnover_share": 0.12,
        "capital_flow": 0.13,
        "leader_structure": 0.15,
        "tier_structure": 0.15,
        "profit_effect": 0.10,
        "catalyst_freshness": 0.08,
        "index_chain_resonance": 0.07,
        "agent_1_quality": 0.05,
    }
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": False,
        "reason_code": "SOURCE_NOT_CONFIGURED",
        "turnover_is_capital_flow": False,
    }
    snapshot["SECTOR_CYCLE_SNAPSHOT"] = {
        "available": True,
        "history_metrics": {
            "monthly_rotation_candidates": [{
                "industry_thscode": "884001.TI",
                "relative_strength_percentile_20d": 0.9,
                "top10_appearance_count": 8,
            }],
        },
    }
    symbol = snapshot["g0_symbols"][0]
    snapshot["DISCLOSURE_EVENTS"] = {
        symbol: [{
            "announcement_title": "重大订单合同落地",
            "source_ref": f"cninfo:{symbol}:notice:1",
        }],
    }
    a1_output = {
        "active_research_pool": [{
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": "theme-compute",
            "industry_chain_node": "node-compute-device",
            "structural_score": 85,
            "data_quality_score": 90,
            "business_exposure": {
                "revenue_exposure_pct": 65.5,
                "source_ref": f"cninfo:{symbol}:page:1",
            },
            "source_refs": [f"cninfo:{symbol}:page:1"],
        }],
    }

    result = screen_a2(snapshot, a1_output, minimum_identifiability_score=60, llm_top_n_per_theme=2)
    item = result.decisions[0]

    assert item["status"] == "DATA_GAP"
    assert result.review_symbols == ()
    assert result.monitor_symbols == (symbol,)
    assert "MARKET_CORE" in item["eligible_routes"]
    assert item["factor_coverage"]["ratio"] >= 0.65
    assert item["critical_factor_coverage"]["sufficient"] is False
    assert item["data_sufficiency_state"] == "INSUFFICIENT"
    assert item["a2_factor_scores"]["capital_flow"]["available"] is False
    assert item["a2_factor_scores"]["capital_flow"]["source"] == "CAPITAL_FLOW_SNAPSHOT"
    assert item["a2_factor_scores"]["turnover_share"]["source"] == "FROZEN_G0_INDUSTRY_TURNOVER_SHARE"


def test_feature_store_replaces_one_lane_stage_atomically(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    gate = screen_a1(_snapshot(5), _discovery(), local_top_n_per_node=4, llm_top_n_per_theme=2)
    assert store.replace_stage_decisions(
        run_id="run-v2",
        lane_id="lane_1",
        stage=gate.stage,
        decisions=gate.decisions,
        updated_at=NOW,
    ) == 5
    summary = store.stage_summary("run-v2", "lane_1", gate.stage)
    assert summary["evaluated_count"] == 5
    assert summary["sent_to_llm_count"] == 2
    assert len(store.stage_decisions("run-v2", "lane_1", gate.stage)) == 5

    snapshot = _snapshot(5)
    assert store.replace_taxonomy_memberships(
        taxonomy="INDUSTRY",
        snapshot=snapshot["THS_INDUSTRY_MEMBERSHIP"],
        as_of=NOW,
    ) == 5
    assert store.record_fundamental_features(as_of=NOW, decisions=gate.decisions) == 5

    a1_output = {
        "active_research_pool": [
            {"symbol": symbol, "primary_theme": "theme-compute", "structural_score": 80}
            for symbol in gate.review_symbols
        ],
    }
    a2 = screen_a2(snapshot, a1_output, minimum_identifiability_score=40, llm_top_n_per_theme=2)
    assert store.record_market_role_features(
        run_id="run-v2",
        lane_id="lane_1",
        decisions=a2.decisions,
    ) == len(a2.decisions)

    store.mark_dirty(
        entity_type="SYMBOL",
        entity_id="600000.SH",
        reason_code="DAILY_BAR_CHANGED",
        source_version="v2",
        created_at=NOW,
    )
    assert store.resolve_dirty(entity_type="SYMBOL", entity_id="600000.SH", resolved_at=NOW) == 1


def _prompt_dir(tmp_path: Path) -> Path:
    path = tmp_path / "prompts"
    path.mkdir()
    for filename in PROMPT_FILENAMES:
        path.joinpath(filename).write_text("prompt " + filename, encoding="utf-8")
    return path


def _envelope(model: str, stage: str, snapshot_id: str) -> dict:
    return {
        "schema_version": "test/2",
        "stage_id": {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}[stage],
        "status": "OK",
        "input_snapshot_ids": [snapshot_id],
        "model_name": model,
        "config_version": "test-v2",
        "prompt_version": "test-v2",
        "market_regime": "REPAIR",
    }


class V2Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []

    def complete(self, model: str, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        symbols = tuple(runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"])
        self.calls.append((model, stage, symbols))
        output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
        if stage == "A1" and not symbols:
            output.update(_discovery())
            output.update(active_research_pool=[], monitor_pool=[], rejected_candidates=[])
        elif stage == "A1":
            output.update(
                structural_themes=_discovery()["structural_themes"],
                industry_chain_graph=_discovery()["industry_chain_graph"],
                taxonomy_links=_discovery()["taxonomy_links"],
                active_research_pool=[
                    {
                        "symbol": symbol,
                        "candidate_id": f"{model}:a1:{symbol}",
                        "primary_theme": "theme-compute",
                        "industry_chain_node": "node-compute-device",
                        "structural_score": 80,
                        "score_breakdown": {"structural_theme": 80, "business_mapping": 80, "barrier_and_bottleneck": 80, "financial_quality": 80, "cash_flow_quality": 80, "evidence_quality": 80},
                        "data_quality_score": 90,
                        "evidence_confidence": 0.9,
                        "business_exposure": {"revenue_exposure_pct": 50, "source_ref": f"cninfo:{symbol}:page:1"},
                        "source_refs": ["policy-ref", f"cninfo:{symbol}:page:1"],
                    }
                    for symbol in symbols
                ],
                monitor_pool=[],
                rejected_candidates=[],
            )
        elif stage == "A2":
            output.update(
                active_themes=[],
                focus_pool=[
                    {
                        "symbol": symbol,
                        "upstream_candidate_id": f"{model}:a1:{symbol}",
                        "theme_id": "theme-compute",
                        "theme_score": 70,
                        "identifiability_score": 75,
                    }
                    for symbol in symbols
                ],
                watch_only_pool=[],
            )
        else:
            output.update(
                core_watch_pool=[
                    {
                        "symbol": symbol,
                        "parent_candidate_id": f"{model}:a2:{symbol}",
                        "technical_score": 80,
                        "reward_risk": 3.0,
                        "stop_distance_pct": 0.05,
                        "risk_unit": "STANDARD",
                    }
                    for symbol in symbols
                ],
                secondary_watch_pool=[],
                rejected_candidates=[],
            )
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=1,
            attempts=1,
            thinking_variant="thinking_object",
        )


def test_v2_pipeline_does_not_send_the_full_g0_to_a1(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "test",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "deterministic_v2",
            "LIANGJIAN_A1_LOCAL_TOP_N_PER_NODE": "5",
            "LIANGJIAN_A1_LLM_TOP_N_PER_NODE": "2",
            "LIANGJIAN_A1_LLM_REPRESENTATIVES_PER_THEME": "2",
            "LIANGJIAN_A2_LLM_TOP_N_PER_THEME": "2",
        },
        root=tmp_path,
    )
    client = V2Client()
    progress_events: list[dict] = []
    snapshot_data = _snapshot(8)
    snapshot_data["A1_DRIVER_LINEAGE_REQUIRED"] = True
    snapshot = FrozenInputSnapshot("snapshot-v2", snapshot_data, as_of=NOW)
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        output_dir=tmp_path / "outputs",
        progress_callback=progress_events.append,
        now=lambda: NOW,
    ).run(snapshot, run_id="run-v2", generated_at=NOW)

    assert result.status == "READY_DEGRADED"
    assert all(lane.status == "READY_DEGRADED" for lane in result.lanes)
    for model in MODELS:
        a1_calls = [symbols for called_model, stage, symbols in client.calls if called_model == model and stage == "A1"]
        assert a1_calls[0] == ()
        assert max(map(len, a1_calls[1:])) <= 2
        assert all(len(symbols) < len(snapshot_data["g0_symbols"]) for symbols in a1_calls)
    assert all(lane.stages[0].diagnostics["local_screen"]["evaluated_count"] == 8 for lane in result.lanes)
    for lane_id in ("lane_1", "lane_2", "lane_3"):
        discovery_events = [
            event
            for event in progress_events
            if event["lane"] == lane_id and event["stage"] == "MACRO_DISCOVERY"
        ]
        assert discovery_events[0]["status"] == "RUNNING"
        assert discovery_events[0]["completed"] == 0
        assert discovery_events[-1]["status"] == "COMPLETED"
        assert discovery_events[-1]["completed"] == 1
        review_events = [
            event
            for event in progress_events
            if event["lane"] == lane_id and event["stage"] == "A1_LLM_REVIEW"
        ]
        assert review_events[0]["status"] == "RUNNING"
        assert review_events[-1]["completed"] == review_events[-1]["total"]
        assert not any(
            event["lane"] == lane_id and event["stage"] == "A1"
            for event in progress_events
        )


def test_empty_a3_stage_emits_terminal_no_action_progress(tmp_path: Path):
    events: list[dict] = []
    settings = Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "test"}, root=tmp_path)
    snapshot = FrozenInputSnapshot("snapshot-empty-a3", _snapshot(1), as_of=NOW)
    pipeline = ResearchPipeline(
        settings,
        progress_callback=events.append,
        now=lambda: NOW,
    )

    audit = pipeline._empty_stage(
        run_id="run-empty-a3",
        lane_id="lane_1",
        model="deepseek-v4-pro-0813",
        stage="A3",
        snapshot=snapshot,
        upstream_output={"focus_pool": []},
        status="VALIDATED_NO_ACTION",
        outcome="NO_ACTION_UPSTREAM_NO_OPPORTUNITY",
    )

    assert audit.status == "VALIDATED_NO_ACTION"
    assert events == [
        {
            "run_id": "run-empty-a3",
            "lane": "lane_1",
            "stage": "A3_LLM_REVIEW",
            "batch": {"completed": 0, "total": 0},
            "completed": 0,
            "total": 0,
            "batch_completed": 0,
            "batch_total": 0,
            "completed_batches": 0,
            "total_batches": 0,
            "status": "VALIDATED_NO_ACTION",
            "attempts": 0,
            "model": "deepseek-v4-pro-0813",
            "outcome": "VALIDATED_NO_ACTION",
            "processed_symbols": 0,
            "total_symbols": 0,
            "selected_symbols": 0,
        }
    ]


def _complete_a2_factor_scores(score: float) -> dict[str, dict[str, float]]:
    """Build explicit observed factors for deterministic gate boundary tests."""

    return {
        name: {"score": score}
        for name in (
            "breadth",
            "turnover_share",
            "capital_flow",
            "leader_structure",
            "tier_structure",
            "profit_effect",
            "catalyst_freshness",
            "index_chain_resonance",
            "agent_1_quality",
        )
    }


def test_screen_a2_partitions_no_route_low_identity_and_llm_rank_overflow() -> None:
    """A2 must preserve every A1 row while distinguishing actionable routes."""

    snapshot = _snapshot(4)
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True,
        "source_id": "TEST_CAPITAL_FLOW",
        "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90}
            for symbol in snapshot["g0_symbols"]
        },
    }
    symbols = snapshot["g0_symbols"][:3]
    rows = [
        {
            "symbol": symbols[0],
            "candidate_id": "a1:high",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 65, "source_ref": "cninfo:high"},
            "a2_factor_scores": _complete_a2_factor_scores(95),
            "data_quality_score": 95,
        },
        {
            "symbol": symbols[1],
            "candidate_id": "a1:low",
            "primary_theme": "theme-core",
            "industry_chain_node": "node-core",
            "business_exposure": {"revenue_exposure_pct": 60, "source_ref": "cninfo:low"},
            "a2_factor_scores": _complete_a2_factor_scores(65),
            "data_quality_score": 80,
        },
        {
            # Complete evidence can still be held locally when the upstream
            # row has no theme/chain/evidence route contract.
            "symbol": symbols[2],
            "candidate_id": "a1:unmapped",
            "a2_factor_scores": _complete_a2_factor_scores(80),
            "data_quality_score": 80,
        },
        {"symbol": ""},  # malformed upstream row must not become a decision
    ]

    result = screen_a2(
        snapshot,
        {"active_research_pool": rows},
        minimum_identifiability_score=0,
        llm_top_n_per_theme=1,
    )
    by_symbol = {item["symbol"]: item for item in result.decisions}

    assert set(by_symbol) == set(symbols)
    assert by_symbol[symbols[0]]["status"] == "REVIEW_CANDIDATE"
    assert by_symbol[symbols[0]]["sent_to_llm"] is True
    assert by_symbol[symbols[0]]["theme_rank"] == 1
    assert by_symbol[symbols[1]]["status"] == "LOCAL_MONITOR"
    assert by_symbol[symbols[1]]["sent_to_llm"] is False
    assert "A2_NOT_SENT_TO_LLM" in by_symbol[symbols[1]]["reason_codes"]
    assert by_symbol[symbols[2]]["status"] == "LOCAL_MONITOR"
    assert "A2_NO_ROUTE_READY" in by_symbol[symbols[2]]["reason_codes"]

    # A high identity threshold is an explicit deterministic rejection, not
    # a data-gap claim.  The same complete source contract remains auditable.
    rejected = screen_a2(
        snapshot,
        {"active_research_pool": [rows[0]]},
        minimum_identifiability_score=101,
        llm_top_n_per_theme=1,
    ).decisions[0]
    assert rejected["status"] == "HARD_REJECT"
    assert "A2_LOW_IDENTITY_EXCLUDED" in rejected["reason_codes"]


def test_screen_a2_rejects_non_positive_review_budget() -> None:
    with pytest.raises(ValueError, match="Top-N value must be positive"):
        screen_a2(_snapshot(1), {"active_research_pool": []}, llm_top_n_per_theme=0)
