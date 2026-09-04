import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult, ModelNetworkError, StrictJSONError
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.feature_store import ResearchFeatureStore
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES, PromptRepository, PromptRepositoryError
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    ResearchPipeline,
    ResearchPipelineError,
    _a1_batch_is_splittable,
    _a2_batch_is_splittable,
    _a2_theme_reasons,
    _a1_discovery_context_reasons,
    _a1_discovery_evidence_reasons,
    _a1_reviewed_hypothesis_coverage_reasons,
    _authorized_discovery_source_refs,
    _a3_semantic_price_reasons,
    _a3_origin_only_veto_reasons,
    _a3_secondary_probe_contract_reasons,
    _a3_watch_only_candidate_eligible,
    _apply_stage_threshold_policy,
    _apply_a2_lineage_policy,
    _apply_a3_candidate_origin_policy,
    _apply_a3_pool_limits,
    _annotate_a2_pool_target,
    _build_a2_theme_batches,
    _build_a3_candidate_domain,
    _canonicalize_a3_price_fields,
    _canonicalize_a1_driver_context,
    _canonicalize_a1_local_candidate_facts,
    _canonicalize_a2_contract_semantics,
    _canonicalize_stage_scores,
    _canonicalize_stage_pool_fields,
    _canonicalize_stage_lineage,
    _demote_a2_llm_rejects,
    _enrich_a2_decision_facts,
    _gate_rejected_items,
    _gate_outside_rotation_items,
    _gate_secondary_items,
    _move_a2_hard_rejects_to_rejected,
    _refresh_analysis_counts,
    _a2_item_route,
    _discovery_progress_diagnostics,
    _estimate_message_tokens,
    _merge_a1_outputs,
    _merge_a2_outputs,
    _normalize_a1_discovery_source_refs,
    _normalize_server_envelope,
    _output_shape,
    _project_disclosures,
    _project_capital_flow,
    _project_crowding,
    _project_fundamentals,
    _project_factor_snapshot,
    _project_macro_policy,
    _project_news,
    _prompt_replacements,
    _project_sector_cycle,
    _scan_symbols,
    _semantic_retry_instruction,
    _semantic_total_timeout_seconds,
    _safe_progress_diagnostics,
    _snapshot_discovery_evidence_refs,
    _stage_execution_budget,
    _validate_output,
    _valid_a1_discovery_output,
    _with_a2_bottleneck_context,
)
from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot as CanonicalFrozenInputSnapshot
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 24, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


def test_discovery_semantic_retry_restates_exact_coverage_and_evidence_contract():
    instruction = _semantic_retry_instruction(
        "A1",
        (
            "A1_DISCOVERY_THEME_EVIDENCE_INVALID",
            "A1_DISCOVERY_NODE_EVIDENCE_INVALID",
            "A1_MONTHLY_THEME_COVERAGE_INSUFFICIENT",
            "A1_MONTHLY_CHAIN_COVERAGE_INSUFFICIENT",
        ),
    )

    assert "12-18 structural_themes" in instruction
    assert "40-80 industry_chain_graph nodes" in instruction
    assert "allowed_primary_source_refs" in instruction
    assert "copy at least one source_ref verbatim" in instruction
    assert "RUNTIME_INPUT.A1_BATCH_CONTEXT" not in instruction
    assert "RUNTIME_INPUT.a1_discovery_context.allowed_primary_source_refs" in instruction


def test_discovery_semantic_retry_requires_reviewed_hypothesis_dispositions():
    instruction = _semantic_retry_instruction(
        "A1",
        ("A1_REVIEWED_HYPOTHESIS_COVERAGE_INCOMPLETE",),
    )

    assert "document_id and hypothesis_theme exactly" in instruction
    assert "MAPPED|MONITOR|REJECTED" in instruction
    assert "cannot create a theme or select a stock" in instruction


def test_policy_macro_discovery_gets_one_bounded_window_per_semantic_attempt():
    context = {"mode": "POLICY_MACRO_DISCOVERY"}

    assert _semantic_total_timeout_seconds(
        "A1",
        context,
        model_timeout_seconds=600.0,
        semantic_limit=2,
    ) == 1200.0
    assert _semantic_total_timeout_seconds(
        "A1",
        {"mode": "COMPANY_MAPPING"},
        model_timeout_seconds=600.0,
        semantic_limit=2,
    ) == 600.0
    assert _semantic_total_timeout_seconds(
        "A3",
        None,
        model_timeout_seconds=600.0,
        semantic_limit=3,
    ) == 600.0


def test_reviewed_hypotheses_cannot_silently_disappear_from_a1_discovery():
    snapshot = {
        "REVIEWED_PUBLIC_RESEARCH_LEADS": {
            "available": True,
            "documents": [
                {
                    "document_id": "premarket-20260902",
                    "theme_hypotheses": [
                        {"theme": "AI应用与国产算力"},
                        {"theme": "银行保险与红利资产"},
                    ],
                }
            ],
        }
    }
    output = {
        "structural_themes": [{"theme_id": "TH_AI", "display_name": "AI产业链"}],
        "unresolved_questions": [
            {
                "document_id": "premarket-20260902",
                "hypothesis_theme": "AI应用与国产算力",
                "disposition": "MAPPED",
                "matched_theme_ids": ["TH_AI"],
                "reason": "独立产业与市场事实已验证",
                "needed_data": "后续订单与盈利",
                "blocking": False,
            }
        ],
    }

    assert _a1_reviewed_hypothesis_coverage_reasons(output, snapshot) == [
        "A1_REVIEWED_HYPOTHESIS_COVERAGE_INCOMPLETE"
    ]

    output["unresolved_questions"].append({
        "document_id": "premarket-20260902",
        "hypothesis_theme": "银行保险与红利资产",
        "disposition": "MONITOR",
        "matched_theme_ids": [],
        "reason": "等待资金与行业强度确认",
        "needed_data": "板块资金、广度与资产质量",
        "blocking": False,
    })
    assert _a1_reviewed_hypothesis_coverage_reasons(output, snapshot) == []


def test_reviewed_hypothesis_mapped_disposition_requires_existing_theme():
    snapshot = {
        "REVIEWED_PUBLIC_RESEARCH_LEADS": {
            "available": True,
            "documents": [{
                "document_id": "lead-1",
                "theme_hypotheses": [{"theme": "农业种植"}],
            }],
        }
    }
    output = {
        "structural_themes": [{"theme_id": "TH_OTHER"}],
        "unresolved_questions": [{
            "document_id": "lead-1",
            "hypothesis_theme": "农业种植",
            "disposition": "MAPPED",
            "matched_theme_ids": ["TH_MISSING"],
            "reason": "声称已映射",
        }],
    }

    assert _a1_reviewed_hypothesis_coverage_reasons(output, snapshot) == [
        "A1_REVIEWED_HYPOTHESIS_THEME_MAPPING_INVALID"
    ]


def test_server_envelope_normalization_preserves_explicit_blocked_and_model_extensions():
    required = {
        "stage_id": "AGENT_1",
        "model_name": MODELS[0],
        "input_snapshot_ids": ["snapshot-a1"],
        "config_version": "funnel-config-v2",
        "prompt_version": "research-runtime-contract-v2",
        "market_regime": "ROTATION_NO_MAINLINE",
        "status": "DEGRADED",
    }
    output, changed = _normalize_server_envelope(
        {
            "envelope": {
                "stage_id": "MODEL_STAGE",
                "model_name": "model-name",
                "status": "blocked",
                "external_orders": True,
            },
            "structural_themes": [{"theme_id": "theme-1"}],
        },
        required,
    )

    assert changed == 1
    assert output["envelope"]["status"] == "BLOCKED"
    assert output["envelope"]["stage_id"] == "AGENT_1"
    assert output["envelope"]["model_name"] == MODELS[0]
    assert output["envelope"]["external_orders"] is True
    assert list(output) == ["envelope", "structural_themes"]


def test_discovery_evidence_allowlist_is_packet_snapshot_intersection_and_rejects_expansion():
    snapshot = {
        "MACRO_POLICY_FEED": {
            "official_documents": [{"fact_id": "policy-1"}],
        },
        "INDUSTRY_ACTIVITY_DATA": {
            "items": [{"source_ref": "activity-1"}],
        },
        "SECTOR_CYCLE_SNAPSHOT": {
            "source_ref": "ths-cycle-1",
        },
        "BROKER_RESEARCH_CONSENSUS": {
            "documents": [{"source_url": "broker-consensus-1"}],
        },
        "REVIEWED_PUBLIC_RESEARCH_LEADS": {
            "documents": [{"source_url": "t3-lead-1"}],
        },
        "DISCLOSURE_EVENTS": [{"source_ref": "company-1"}],
    }
    packet = {
        "source_index": {
            "policy-1": {"section": "policy"},
            "ths-cycle-1": {"section": "industry"},
            "broker-consensus-1": {"section": "broker"},
            "t3-lead-1": {"section": "reviewed_public_leads"},
            "company-1": {"section": "company"},
            "packet-only": {"section": "packet"},
        }
    }
    authorized = _authorized_discovery_source_refs(snapshot, packet)
    assert authorized == ("broker-consensus-1", "policy-1", "ths-cycle-1")
    assert isinstance(authorized, tuple)

    valid = {
        "structural_themes": [{"theme_id": "theme-1", "source_refs": ["policy-1"]}],
        "industry_chain_graph": [{
            "node_id": "node-1",
            "theme_ids": ["theme-1"],
            "source_refs": ["policy-1"],
        }],
    }
    assert _a1_discovery_evidence_reasons(valid, snapshot, authorized_source_refs=authorized) == []

    expanded = {
        "structural_themes": [{"theme_id": "theme-1", "source_refs": ["policy-1", "company-1"]}],
        "industry_chain_graph": [{
            "node_id": "node-1",
            "theme_ids": ["theme-1"],
            "source_refs": ["policy-1"],
        }],
    }
    assert _a1_discovery_evidence_reasons(
        expanded,
        snapshot,
        authorized_source_refs=authorized,
    ) == ["A1_DISCOVERY_THEME_EVIDENCE_INVALID"]


def test_discovery_scalar_source_refs_are_normalized_only_for_exact_authorized_values():
    output = {
        "structural_themes": [{"theme_id": "theme-1", "source_refs": "policy-1"}],
        "industry_chain_graph": [{
            "node_id": "node-1",
            "theme_ids": ["theme-1"],
            "source_ref": "policy-1",
        }],
    }
    normalized, changed = _normalize_a1_discovery_source_refs(output, ("policy-1",))
    assert changed == 2
    assert normalized["structural_themes"][0]["source_refs"] == ["policy-1"]
    assert normalized["industry_chain_graph"][0]["source_ref"] == "policy-1"
    assert normalized["industry_chain_graph"][0]["source_refs"] == ["policy-1"]
    assert _a1_discovery_evidence_reasons(
        normalized,
        {},
        authorized_source_refs=("policy-1",),
    ) == []

    unknown = {
        "structural_themes": [{"theme_id": "theme-1", "source_refs": "not-authorized"}],
        "industry_chain_graph": [{
            "node_id": "node-1",
            "theme_ids": ["theme-1"],
            "source_ref": "not-authorized",
        }],
    }
    untouched, unknown_changed = _normalize_a1_discovery_source_refs(unknown, ("policy-1",))
    assert unknown_changed == 0
    assert untouched == unknown
    assert set(_a1_discovery_evidence_reasons(
        untouched,
        {},
        authorized_source_refs=("policy-1",),
    )) == {"A1_DISCOVERY_THEME_EVIDENCE_INVALID", "A1_DISCOVERY_NODE_EVIDENCE_INVALID"}


def test_discovery_mapping_evidence_must_use_same_authorized_allowlist():
    output = {
        "structural_themes": [{"theme_id": "theme-1", "source_refs": ["policy-1"]}],
        "industry_chain_graph": [{
            "node_id": "node-1",
            "theme_ids": ["theme-1"],
            "source_refs": ["policy-1"],
        }],
        "industry_theme_mappings": [{
            "industry_thscode": "884001.TI",
            "mapped_theme_ids": ["theme-1"],
            "mapping_status": "MAPPED",
            "supporting_source_refs": ["not-authorized"],
        }],
    }

    assert _a1_discovery_evidence_reasons(
        output,
        {},
        authorized_source_refs=("policy-1",),
    ) == ["A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID"]

    output["industry_theme_mappings"][0]["supporting_source_refs"] = ["policy-1"]
    assert _a1_discovery_evidence_reasons(
        output,
        {},
        authorized_source_refs=("policy-1",),
    ) == []


def test_progress_diagnostics_expose_only_safe_shape_and_counts(tmp_path: Path):
    diagnostics = _safe_progress_diagnostics({
        "last_invalid_output_shape": {
            "type": "object",
            "fields": ["envelope", "private-output-key", "structural_themes"],
            "unknown_field_count": -3,
            "envelope_unknown_field_count": 2,
            "raw_model_content": "must-not-leak",
        },
        "semantic_attempts": 2,
        "theme_count": 8,
        "node_count": 40,
        "mapping_count": 20,
        "expected_mapping_count": 20,
        "missing_mapping_codes": ["884001.TI", "", 123],
        "raw_model_content": "must-not-leak",
    })
    assert diagnostics == {
        "last_invalid_output_shape": {
            "type": "object",
            "fields": ["envelope", "structural_themes"],
            "unknown_field_count": 0,
            "envelope_unknown_field_count": 2,
        },
        "semantic_attempts": 2,
        "theme_count": 8,
        "node_count": 40,
        "mapping_count": 20,
        "expected_mapping_count": 20,
        "missing_mapping_count": 1,
    }

    events: list[dict] = []
    pipeline = ResearchPipeline(_settings(tmp_path), progress_callback=events.append)
    pipeline._emit_progress(
        run_id="run-progress",
        lane="lane_1",
        model=MODELS[0],
        stage="MACRO_DISCOVERY",
        completed=0,
        total=1,
        status="FAILED",
        attempts=2,
        diagnostics={
            "last_invalid_output_shape": {"type": "object", "fields": ["envelope", "secret"]},
            "theme_count": 8,
            "node_count": 40,
            "mapping_count": 20,
        },
    )
    assert events[0]["stage"] == "MACRO_DISCOVERY"
    assert events[0]["diagnostics"] == {
        "last_invalid_output_shape": {"type": "object", "fields": ["envelope"]},
        "theme_count": 8,
        "node_count": 40,
        "mapping_count": 20,
    }
    assert "secret" not in json.dumps(events[0])


def test_discovery_progress_diagnostics_derive_only_counts_from_safe_shape():
    diagnostics = _discovery_progress_diagnostics(
        {
            "last_invalid_output_shape": {
                "array_lengths": {
                    "structural_themes": 12,
                    "industry_chain_graph": 44,
                    "industry_theme_mappings": 17,
                    "private_payload": 999,
                },
                "fields": ["structural_themes"],
            },
            "semantic_attempts": 2,
        },
        {
            "monthly_industry_decisions": [
                {"base_decision": "INCLUDE"},
                {"decision": "INCLUDE"},
                {"base_decision": "EXCLUDE"},
            ]
        },
    )

    assert diagnostics["theme_count"] == 12
    assert diagnostics["node_count"] == 44
    assert diagnostics["mapping_count"] == 17
    assert diagnostics["expected_mapping_count"] == 2
    safe = _safe_progress_diagnostics(diagnostics)
    assert safe["theme_count"] == 12
    assert safe["node_count"] == 44
    assert safe["mapping_count"] == 17
    assert safe["expected_mapping_count"] == 2
    assert "private_payload" not in json.dumps(safe)


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "model-secret"}, root=tmp_path)


def _prompt_dir(tmp_path: Path) -> Path:
    path = tmp_path / "prompts"
    path.mkdir()
    for filename in PROMPT_FILENAMES:
        path.joinpath(filename).write_text("prompt " + filename, encoding="utf-8")
    return path


def _snapshot() -> dict:
    return {
        "snapshot_id": "snap-20260824",
        "snapshot_hash": "s" * 64,
        "g0": ["600519.SH", "000001.SZ", "300750.SZ"],
        "snapshot_manifest": {"snapshot_id": "snap-20260824"},
    }


def _envelope(model: str, stage: str, snapshot_id: str) -> dict:
    return {
        "schema_version": "test/1",
        "stage_id": {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}[stage],
        "status": "OK",
        "input_snapshot_ids": [snapshot_id],
        "model_name": model,
        "config_version": "test-config",
        "prompt_version": "test-prompt",
        "market_regime": "REPAIR",
    }


def _qualifying_item(symbol: str, stage: str) -> dict:
    item = {"symbol": symbol}
    if stage == "A1":
        item.update(structural_score=80, data_quality_score=80, evidence_confidence=0.8)
    elif stage == "A2":
        item["theme_score"] = 65
    else:
        item.update(technical_score=80, reward_risk=3.0, stop_distance_pct=0.03)
    return item


class FakeResearchClient:
    def __init__(self, symbols_by_model: dict[str, str], *, outside_a2: bool = False, escalate: bool = False):
        self.symbols_by_model = symbols_by_model
        self.outside_a2 = outside_a2
        self.escalate = escalate
        self.calls: list[tuple[str, str, dict]] = []

    def complete(self, model: str, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        self.calls.append((model, stage, runtime))
        symbol = self.symbols_by_model[model]
        if stage == "A2" and self.outside_a2:
            symbol = "999999.SH"
        output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
        if self.escalate:
            output["envelope"]["external_orders"] = True
        if stage == "A1":
            output["active_research_pool"] = [
                {**_qualifying_item(symbol, stage), "candidate_id": f"{model}:a1"}
            ]
            output["monitor_pool"] = []
            output["rejected_candidates"] = [
                {"symbol": candidate, "reason_codes": ["TEST_REJECTED"]}
                for candidate in runtime["g0_symbols"]
                if candidate != symbol
            ]
        elif stage == "A2":
            output["focus_pool"] = [
                {**_qualifying_item(symbol, stage), "upstream_candidate_id": f"{model}:a1"}
            ]
        else:
            output["core_watch_pool"] = [
                {**_qualifying_item(symbol, stage), "parent_candidate_id": f"{model}:a2"}
            ]
            output["reasoning_content"] = "do-not-persist-this"
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=4,
            attempts=1,
            thinking_variant="thinking_object",
        )


def test_three_lanes_are_isolated_and_run_in_stage_order(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = FakeResearchClient(symbols)
    pipeline = ResearchPipeline(_settings(tmp_path), prompt_repository=_prompt_dir(tmp_path), model_client=client, now=lambda: NOW)
    result = pipeline.run(_snapshot(), run_id="run-isolation", generated_at=NOW)

    assert result.status == "READY"
    assert len(result.lanes) == 3
    for model in MODELS:
        assert [stage for called_model, stage, _ in client.calls if called_model == model] == ["A1", "A2", "A3"]
    for lane in result.lanes:
        stages = {stage.stage: stage for stage in lane.stages}
        assert all(stage.status == "VALIDATED" for stage in stages.values())
        assert stages["A1"].symbols == stages["A2"].symbols == stages["A3"].symbols
        assert stages["A3"].output and "do-not-persist-this" not in json.dumps(stages["A3"].output)
    assert all(runtime["upstream_output"] is None for _, _, runtime in client.calls)
    assert all("snapshot_data" not in runtime for _, _, runtime in client.calls)

    assert result.markdown_path and result.markdown_path.exists()
    markdown = result.markdown_path.read_text(encoding="utf-8")
    assert "内部模拟、非投资建议" in markdown
    assert "## 阶段审计" in markdown
    assert "请求次数" in markdown
    assert all(path.exists() for path in result.audit_paths)
    assert not list((tmp_path / "outputs" / "research").glob("*.tmp"))


def test_a1_large_universe_is_batched_and_merged_before_a2(tmp_path: Path):
    class BatchClient:
        def __init__(self):
            self.calls = []

        def complete(self, model: str, messages, **metadata):
            runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
            self.calls.append((model, runtime["stage"], tuple(runtime["g0_symbols"])))
            stage = runtime["stage"]
            domain = runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"]
            symbol = domain[0]
            output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
            if stage == "A1":
                output["active_research_pool"] = [_qualifying_item(symbol, stage)]
                output["monitor_pool"] = []
                output["rejected_candidates"] = [
                    {"symbol": candidate, "reason_codes": ["TEST_REJECTED"]}
                    for candidate in domain
                    if candidate != symbol
                ]
            elif stage == "A2":
                output["focus_pool"] = [_qualifying_item(symbol, stage)]
            else:
                output["core_watch_pool"] = [_qualifying_item(symbol, stage)]
            return ModelCallResult(
                model=model,
                output=output,
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="thinking_object",
            )

    symbols = [f"6005{index:02d}.SH" for index in range(6)]
    snapshot = {"snapshot_id": "batch-snapshot", "snapshot_hash": "b" * 64, "g0": symbols}
    client = BatchClient()
    settings = _settings(tmp_path).model_copy(update={"research_a1_batch_size": 5})
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(snapshot, run_id="run-batched", generated_at=NOW)

    assert result.status == "READY"
    assert all(
        lane.stages[0].diagnostics
        == {
            "batch_count": 2,
            "completed_batches": 2,
            "request_groups": 2,
            "split_count": 0,
            "pool_counts": {
                "active_research_pool": 2,
                "monitor_pool": 0,
                "rejected_candidates": 4,
            },
        }
        for lane in result.lanes
    )
    for model in MODELS:
        calls = [item for item in client.calls if item[0] == model]
        assert [stage for _, stage, _ in calls] == ["A1", "A1", "A2", "A3"]
        assert sorted(len(scope) for _, stage, scope in calls if stage == "A1") == [1, 5]


def test_failed_large_a1_batch_is_split_without_repeating_successful_groups(tmp_path: Path):
    class SplitClient:
        def __init__(self):
            self.calls = []

        def complete(self, model: str, messages, **metadata):
            runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
            stage = runtime["stage"]
            domain = runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"]
            self.calls.append((model, stage, tuple(domain)))
            if stage == "A1" and len(domain) > 2:
                raise ModelNetworkError(attempts=1)
            output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
            pool = {"A1": "active_research_pool", "A2": "focus_pool", "A3": "core_watch_pool"}[stage]
            output[pool] = [_qualifying_item(domain[0], stage)] if domain else []
            if stage == "A1":
                output["monitor_pool"] = []
                output["rejected_candidates"] = [
                    {"symbol": candidate, "reason_codes": ["TEST_REJECTED"]}
                    for candidate in domain[1:]
                ]
            return ModelCallResult(
                model=model,
                output=output,
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="reasoning_effort_low",
            )

    symbols = [f"6005{index:02d}.SH" for index in range(6)]
    client = SplitClient()
    settings = _settings(tmp_path).model_copy(update={"research_a1_batch_size": 5})
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(
        {"snapshot_id": "split-snapshot", "snapshot_hash": "s" * 64, "g0": symbols},
        run_id="run-split",
        generated_at=NOW,
    )

    assert result.status == "READY"
    for lane in result.lanes:
        diagnostics = lane.stages[0].diagnostics
        assert diagnostics == {
            "batch_count": 2,
            "completed_batches": 4,
            "request_groups": 6,
            "split_count": 2,
            "pool_counts": {
                "active_research_pool": 4,
                "monitor_pool": 0,
                "rejected_candidates": 2,
            },
        }
        groups = [scope for model, stage, scope in client.calls if model == lane.model and stage == "A1"]
        assert [len(group) for group in groups] == [5, 2, 3, 1, 2, 1]


def test_a3_large_focus_pool_is_batched_and_deterministically_merged(tmp_path: Path):
    class A3BatchClient:
        def __init__(self):
            self.calls: list[tuple[str, str, tuple[str, ...]]] = []

        def complete(self, model: str, messages, **metadata):
            runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
            stage = runtime["stage"]
            domain = runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"]
            self.calls.append((model, stage, tuple(domain)))
            output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
            if stage == "A1":
                output.update({
                    "active_research_pool": [_qualifying_item(symbol, stage) for symbol in domain],
                    "monitor_pool": [],
                    "rejected_candidates": [],
                })
            elif stage == "A2":
                output["focus_pool"] = [_qualifying_item(symbol, stage) for symbol in domain]
            else:
                output["core_watch_pool"] = [_qualifying_item(symbol, stage) for symbol in domain]
            return ModelCallResult(
                model=model,
                output=output,
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="thinking_object",
            )

    symbols = [f"6006{index:02d}.SH" for index in range(17)]
    client = A3BatchClient()
    settings = _settings(tmp_path).model_copy(update={"research_a1_batch_size": 20})
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(
        {"snapshot_id": "a3-batch-snapshot", "snapshot_hash": "a" * 64, "g0": symbols},
        run_id="run-a3-batched",
        generated_at=NOW,
    )
    assert result.status == "READY"
    for lane in result.lanes:
        assert lane.stages[2].diagnostics["batch_count"] == 2
        assert lane.stages[2].diagnostics["completed_batches"] == 2
        assert set(lane.stages[2].symbols) == set(symbols)
    for model in MODELS:
        a3_calls = [domain for called_model, stage, domain in client.calls if called_model == model and stage == "A3"]
        assert [len(domain) for domain in a3_calls] == [16, 1]


def test_schema_invalid_output_is_not_persisted_and_stage_retries_once(tmp_path: Path):
    class SemanticRetryClient:
        def __init__(self):
            self.counts = {}
            self.repair_messages = []

        def complete(self, model: str, messages, **metadata):
            runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
            stage = runtime["stage"]
            key = (model, stage)
            self.counts[key] = self.counts.get(key, 0) + 1
            if len(messages) > 2:
                self.repair_messages.append(messages[2]["content"])
            if stage == "A1" and self.counts[key] == 1:
                output = {"envelope": {"private-model-self-correction": "must-not-persist"}}
            else:
                symbol = runtime["g0_symbols"][0] if stage == "A1" else runtime["upstream_symbols"][0]
                output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
                output[{"A1": "active_research_pool", "A2": "focus_pool", "A3": "core_watch_pool"}[stage]] = [
                    _qualifying_item(symbol, stage)
                ]
                if stage == "A1":
                    output["monitor_pool"] = []
                    output["rejected_candidates"] = [
                        {"symbol": candidate, "reason_codes": ["TEST_REJECTED"]}
                        for candidate in runtime["g0_symbols"]
                        if candidate != symbol
                    ]
            return ModelCallResult(
                model=model,
                output=output,
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="reasoning_effort_low",
            )

    client = SemanticRetryClient()
    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(_snapshot(), run_id="run-semantic-retry", generated_at=NOW)

    assert result.status == "READY"
    assert all(lane.stages[0].attempts == 2 for lane in result.lanes)
    assert all(
        lane.stages[0].diagnostics
        == {
            "semantic_attempts": 2,
            "canonicalized_price_items": 0,
            "trend_veto_items": 0,
            "pool_counts": {
                "active_research_pool": 1,
                "monitor_pool": 0,
                "rejected_candidates": 2,
            },
        }
        for lane in result.lanes
    )
    assert all("must-not-persist" not in path.read_text(encoding="utf-8") for path in result.audit_paths)
    assert len(client.repair_messages) == 3
    assert all("private-model-self-correction" not in message for message in client.repair_messages)


def test_output_shape_never_exposes_unknown_model_field_names():
    shape = _output_shape(
        {
            "envelope": {"private-self-correction": "hidden", "status": "DEGRADED"},
            "private-output-key": "hidden",
        }
    )
    assert shape["fields"] == ["envelope"]
    assert shape["unknown_field_count"] == 1
    assert shape["envelope_fields"] == ["status"]
    assert shape["envelope_unknown_field_count"] == 1
    assert "private" not in json.dumps(shape)


def test_output_shape_accepts_prompt_authorized_macro_discovery_fields():
    shape = _output_shape(
        {
            "envelope": {"status": "OK"},
            "macro_regime": {},
            "policy_dossiers": [],
            "policy_calendar": [],
        }
    )

    assert shape["unknown_field_count"] == 0
    assert shape["fields"] == [
        "envelope",
        "macro_regime",
        "policy_calendar",
        "policy_dossiers",
    ]


def test_a1_batch_merge_recomputes_counts_and_removes_rejected_from_monitor():
    envelope = _envelope(MODELS[0], "A1", "snap")
    merged = _merge_a1_outputs(
        [
            {
                "envelope": envelope,
                "analysis_summary": {"approved_count": 1, "monitor_count": 1, "rejected_count": 0},
                "active_research_pool": [{"symbol": "SHSE.600519", "structural_score": 90}],
                "monitor_pool": ["000001.SZ"],
                "rejected_candidates": [],
            },
            {
                "envelope": envelope,
                "active_research_pool": [{"symbol": "SZSE.300750", "structural_score": 80}],
                "monitor_pool": ["000001.SZ"],
                "rejected_candidates": [{"symbol": "000001.SZ"}],
            },
        ]
    )
    assert [item["symbol"] for item in merged["active_research_pool"]] == ["600519.SH", "300750.SZ"]
    assert merged["monitor_pool"] == []
    assert merged["analysis_summary"]["approved_count"] == 2
    assert merged["analysis_summary"]["monitor_count"] == 0
    assert merged["analysis_summary"]["rejected_count"] == 1
    assert merged["analysis_summary"]["batch_count"] == 2
    assert merged["analysis_summary"]["outcome"] == "A1_BATCHES_MERGED"


def test_a1_batch_merge_does_not_apply_a_global_five_stock_cap():
    envelope = _envelope(MODELS[0], "A1", "snap")
    outputs = [
        {
            "envelope": envelope,
            "active_research_pool": [
                {"symbol": f"6005{batch * 5 + offset:02d}.SH", "structural_score": 90 - offset}
                for offset in range(5)
            ],
            "monitor_pool": [],
            "rejected_candidates": [],
        }
        for batch in range(3)
    ]
    merged = _merge_a1_outputs(outputs)
    assert len(merged["active_research_pool"]) == 15


def test_stage_execution_budget_keeps_a1_broad_and_uses_downstream_regime_caps():
    regime = {
        "REGIME_PARAM_SET": {
            "agent_2": {"focus_pool_max": 12},
            "agent_3": {"core_watch_max": 5, "total_watch_max": 8},
        }
    }
    a1 = _stage_execution_budget("A1", 20, regime)
    a2 = _stage_execution_budget("A2", 20, regime)
    a3 = _stage_execution_budget("A3", 20, regime)
    assert "approved pool <= 20; secondary/watch pool <= 20" in a1
    assert "approved pool <= 12; secondary/watch pool <= 20" in a2
    assert "approved pool <= 20; secondary/watch pool <= 20" in a3
    assert "business_exposure with revenue_exposure_pct" in a1
    assert "identifiability_score, identifiability_breakdown" in a2
    assert "Every supplied symbol must appear exactly once" in a2


def test_a1_incomplete_partition_can_split_to_smaller_transport_groups():
    assert _a1_batch_is_splittable(["A1_POOL_PARTITION_INCOMPLETE"])
    assert _a1_batch_is_splittable(["MODEL_PROMPT_TOO_LARGE"])


def test_input_token_estimate_is_conservative_for_mixed_chinese_json():
    messages = (
        {"role": "system", "content": "abcd"},
        {"role": "user", "content": "基本面"},
    )
    # ASCII: 1 token; CJK: 3 * 2; framing: 8 per message.
    assert _estimate_message_tokens(messages) == 23


def test_discovery_evidence_catalog_excludes_company_only_disclosures():
    refs = _snapshot_discovery_evidence_refs({
        "MACRO_POLICY_FEED": {"items": [{"fact_id": "policy-1"}]},
        "INDUSTRY_PROFIT_DATA": [{"source_url": "https://stats.example/industry"}],
        "DISCLOSURE_EVENTS": {"600519.SH": [{"fact_id": "company-only"}]},
    })
    assert refs == {"policy-1", "https://stats.example/industry"}


def test_a2_prompt_receives_non_scoring_research_hypotheses(tmp_path: Path):
    prompt_dir = Path(__file__).resolve().parents[1] / "prompts"
    bundle = PromptRepository(prompt_dir).bundle()
    hypotheses = {
        "schema_version": "a2-research-hypotheses/1.0.0",
        "available": True,
        "evidence_tier": "T2",
        "viewpoint_only": True,
        "deterministic_score_influence_allowed": False,
        "documents": [{"document_id": "weekly-private"}],
    }
    snapshot = FrozenInputSnapshot(
        snapshot_id="a2-research",
        data={"A2_RESEARCH_HYPOTHESES": hypotheses},
    )

    replacements = _prompt_replacements(
        bundle,
        "A2",
        snapshot,
        {"active_research_pool": []},
        projection_symbols=set(),
    )
    rendered = bundle.render_stage("A2", replacements)

    assert replacements["A2_RESEARCH_HYPOTHESES"] == hypotheses
    assert '"document_id":"weekly-private"' in rendered
    assert "不参与确定性打分" in rendered


def test_a3_prompt_names_the_full_candidate_domain_without_focus_pool_alias():
    prompt_dir = Path(__file__).resolve().parents[1] / "prompts"
    bundle = PromptRepository(prompt_dir).bundle()
    upstream = {
        "focus_pool": [
            {"symbol": "600001.SH", "candidate_origin": "FOCUS"},
            {"symbol": "600002.SH", "candidate_origin": "WATCH_ONLY"},
        ]
    }
    snapshot = FrozenInputSnapshot(snapshot_id="a3-candidate-domain", data={})

    replacements = _prompt_replacements(
        bundle,
        "A3",
        snapshot,
        upstream,
        projection_symbols={"600001.SH", "600002.SH"},
    )
    rendered = bundle.render_stage("A3", replacements)

    assert "A3_CANDIDATE_POOL" in replacements
    assert "UPSTREAM_FOCUS_POOL" not in bundle.document("agent_3_technical_planner_v2.txt").placeholders
    assert '"candidate_origin":"WATCH_ONLY"' in rendered


def test_prompt_budget_failure_reports_real_size_before_model_call(tmp_path: Path):
    settings = _settings(tmp_path).model_copy(update={"model_max_input_tokens": 16})
    pipeline = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=object(),
        now=lambda: NOW,
    )
    snapshot = FrozenInputSnapshot(
        snapshot_id="prompt-budget",
        snapshot_hash="p" * 64,
        data={"g0": ["600519.SH"], "snapshot_manifest": {"snapshot_id": "prompt-budget"}},
    )

    with pytest.raises(ResearchPipelineError) as exc_info:
        pipeline._prepare_stage_request(
            lane_id="lane_1",
            model=MODELS[0],
            stage="A1",
            snapshot=snapshot,
            upstream_output=None,
            upstream_symbols={"600519.SH"},
            bundle=pipeline.prompts.bundle(),
            projection_symbols={"600519.SH"},
        )

    assert exc_info.value.reason_code == "MODEL_PROMPT_TOO_LARGE"
    assert exc_info.value.diagnostics["prompt_chars"] > 0
    assert exc_info.value.diagnostics["estimated_input_tokens"] > 16
    assert exc_info.value.diagnostics["input_token_limit"] == 16


def test_a2_theme_batches_preserve_scope_and_pool_target_is_advisory():
    symbols = {f"6008{index:02d}.SH" for index in range(45)}
    active = [
        {
            "symbol": symbol,
            "primary_theme": "theme-a" if index < 30 else "theme-b",
            "structural_score": 90 - index,
        }
        for index, symbol in enumerate(sorted(symbols))
    ]
    batches = _build_a2_theme_batches({"active_research_pool": active}, symbols, 20)
    assert sorted(len(batch) for batch in batches) == [10, 15, 20]
    assert set().union(*batches) == symbols
    assert sum(len(batch) for batch in batches) == len(symbols)
    assert _a2_batch_is_splittable(["A2_POOL_PARTITION_INCOMPLETE"])

    annotated = _annotate_a2_pool_target(
        {"analysis_summary": {}, "focus_pool": active[:80]},
        {"A2_POOL_TARGETS": {"pool_min": 100, "pool_max": 200}},
    )
    assert annotated["analysis_summary"]["pool_target_underfilled_by"] == 55
    assert annotated["analysis_summary"]["reason_codes"] == ["POOL_TARGET_UNDERFILLED"]

    effective = _annotate_a2_pool_target(
        {
            "analysis_summary": {},
            "focus_pool": active[:3],
            "watch_only_pool": active[3:],
            "rejected_candidates": [{"symbol": "600000.SH"}],
        },
        {"A2_POOL_TARGETS": {"pool_min": 30, "pool_max": 80}},
    )
    summary = effective["analysis_summary"]
    assert summary["focus_pool_count"] == 3
    assert summary["watch_only_pool_count"] == 42
    assert summary["effective_research_pool_count"] == 45
    assert summary["rejected_candidate_count"] == 1
    assert summary["pool_target_underfilled_by"] == 0
    assert "POOL_TARGET_UNDERFILLED" not in summary["reason_codes"]


def test_a2_batch_merge_has_no_hidden_small_global_cap():
    envelope = _envelope(MODELS[0], "A2", "snap")
    outputs = [
        {
            "envelope": envelope,
            "active_themes": [{"theme_id": f"theme-{batch}"}],
            "focus_pool": [
                {
                    "symbol": f"6009{batch * 20 + offset:02d}.SH",
                    "theme_score": 80,
                    "identifiability_score": 70,
                }
                for offset in range(20)
            ],
            "watch_only_pool": [],
            "rejected_candidates": [],
        }
        for batch in range(5)
    ]
    merged = _merge_a2_outputs(outputs)
    assert len(merged["focus_pool"]) == 100
    assert len(merged["active_themes"]) == 5
    assert merged["analysis_summary"]["outcome"] == "A2_BATCHES_MERGED"


def test_institutional_scale_funnel_keeps_1000_a1_200_a2_then_filters_in_a3(tmp_path: Path):
    class ScaleClient:
        def __init__(self):
            self.calls: list[tuple[str, str, int]] = []

        def complete(self, model: str, messages, **metadata):
            runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
            stage = runtime["stage"]
            domain = runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"]
            self.calls.append((model, stage, len(domain)))
            output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
            if stage == "A1":
                output.update({
                    "structural_themes": [{"theme_id": "theme-cycle"}],
                    "industry_chain_graph": [],
                    "active_research_pool": [
                        {
                            **_qualifying_item(symbol, stage),
                            "primary_theme": "theme-cycle",
                            "candidate_id": f"a1:{symbol}",
                        }
                        for symbol in domain
                    ],
                    "monitor_pool": [],
                    "rejected_candidates": [],
                })
            elif stage == "A2":
                output.update({
                    "active_themes": [{
                        "theme_id": "theme-cycle",
                        "stage": "CONFIRMATION",
                        "new_entry_policy": "ALLOW",
                        "supporting_evidence": ["breadth"],
                        "contradicting_evidence": ["rotation risk"],
                        "theme_score": 80,
                    }],
                    "focus_pool": [
                        {
                            **_qualifying_item(symbol, stage),
                            "theme_id": "theme-cycle",
                            "theme_score": 80,
                            "market_role": "CORE_ARMY",
                            "identifiability_score": 80,
                            "upstream_candidate_id": f"a1:{symbol}",
                        }
                        for symbol in domain
                    ],
                    "watch_only_pool": [],
                    "rejected_candidates": [],
                })
            else:
                output.update({
                    "core_watch_pool": [
                        {
                            **_qualifying_item(symbol, stage),
                            "parent_candidate_id": f"a2:{symbol}",
                        }
                        for symbol in domain
                    ],
                    "secondary_watch_pool": [],
                    "rejected_candidates": [],
                })
            return ModelCallResult(
                model=model,
                output=output,
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=1,
                attempts=1,
                thinking_variant="thinking_object",
            )

    symbols = [f"{600000 + index:06d}.SH" for index in range(1000)]
    snapshot = {
        "snapshot_id": "institutional-scale",
        "snapshot_hash": "i" * 64,
        "g0": symbols,
        "STRICT_AGENT_RULES": True,
        "A2_POOL_TARGETS": {"pool_min": 100, "pool_max": 200},
        "REGIME_PARAM_SET": {
            "agent_2": {"focus_pool_max": 200},
            "agent_3": {"core_watch_max": 12, "total_watch_max": 20},
        },
    }
    client = ScaleClient()
    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(snapshot, run_id="run-institutional-scale", generated_at=NOW)

    assert result.status == "READY"
    for lane in result.lanes:
        assert lane.stages[0].diagnostics["pool_counts"]["active_research_pool"] == 1000
        assert lane.stages[1].diagnostics["pool_counts"] == {
            "focus_pool": 200,
            "watch_only_pool": 800,
            "rejected_candidates": 0,
        }
        assert lane.stages[1].output["analysis_summary"]["pool_target_underfilled_by"] == 0
        assert lane.stages[2].diagnostics["pool_counts"] == {
            "core_watch_pool": 200,
            "secondary_watch_pool": 0,
            "rejected_candidates": 0,
        }


def test_a3_watch_only_alias_is_canonicalized_without_losing_items():
    output, changed = _canonicalize_stage_pool_fields(
        {
            "core_watch_pool": [],
            "watch_only_pool": [{"symbol": "300502.SZ", "risk_unit": "NO_ENTRY"}],
        },
        "A3",
    )
    assert changed == 2
    assert output["secondary_watch_pool"] == [
        {"symbol": "300502.SZ", "risk_unit": "NO_ENTRY"}
    ]
    assert output["rejected_candidates"] == []
    assert "watch_only_pool" not in output


def test_server_threshold_policy_keeps_a1_broad_but_demotes_weak_active_items():
    output, changed = _apply_stage_threshold_policy(
        {
            "active_research_pool": [
                {
                    "symbol": "600183.SH",
                    "structural_score": 72,
                    "data_quality_score": 78,
                    "evidence_confidence": 0.72,
                    "status": "ACTIVE",
                },
                {
                    "symbol": "300308.SZ",
                    "structural_score": 80,
                    "data_quality_score": 70,
                    "evidence_confidence": 0.72,
                    "status": "ACTIVE",
                },
            ],
            "monitor_pool": [{"symbol": "000725.SZ"}],
        },
        "A1",
        {},
    )
    assert changed == 1
    assert [item["symbol"] for item in output["active_research_pool"]] == ["600183.SH"]
    assert {item["symbol"] for item in output["monitor_pool"]} == {"300308.SZ", "000725.SZ"}
    demoted = next(item for item in output["monitor_pool"] if item["symbol"] == "300308.SZ")
    assert demoted["status"] == "MONITOR"
    assert "A1_DATA_QUALITY_BELOW_MINIMUM" in demoted["reason_codes"]
    assert output["analysis_summary"]["pool_counts"]["active_research_pool"] == 1


def test_a1_active_requires_hash_bound_main_business_revenue_evidence():
    evidence_ref = "cninfo:annual-1:page:11"
    output, changed = _apply_stage_threshold_policy(
        {
            "active_research_pool": [
                {
                    "symbol": "600183.SH",
                    "structural_score": 72,
                    "data_quality_score": 78,
                    "evidence_confidence": 0.72,
                    "business_exposure": {
                        "revenue_exposure_pct": 65.0,
                        "source_ref": evidence_ref,
                    },
                    "status": "ACTIVE",
                },
                {
                    "symbol": "300308.SZ",
                    "structural_score": 80,
                    "data_quality_score": 80,
                    "evidence_confidence": 0.8,
                    "business_exposure": {
                        "revenue_exposure_pct": None,
                        "source_ref": "THS_INDUSTRY_MEMBERSHIP",
                    },
                    "status": "ACTIVE",
                },
            ],
            "monitor_pool": [],
        },
        "A1",
        {
            "MAIN_BUSINESS_EVIDENCE": {
                "600183.SH": {"available": True, "evidence": [{"source_ref": evidence_ref}]},
                "300308.SZ": {"available": False, "evidence": []},
            }
        },
    )

    assert changed == 1
    assert [item["symbol"] for item in output["active_research_pool"]] == ["600183.SH"]
    assert output["monitor_pool"][0]["reason_codes"] == ["A1_MAIN_BUSINESS_EVIDENCE_MISSING"]


def test_a1_active_requires_theme_and_chain_lineage_bound_to_snapshot_evidence():
    policy_ref = "sha256:policy-1"
    business_ref = "cninfo:annual-1:page:11"
    base_item = {
        "symbol": "600183.SH",
        "primary_theme": "theme-policy",
        "industry_chain_node": "node-material",
        "structural_score": 80,
        "score_breakdown": {"structural_theme": 80, "business_mapping": 80},
        "data_quality_score": 80,
        "evidence_confidence": 0.8,
        "business_exposure": {"revenue_exposure_pct": 65.0, "source_ref": business_ref},
        "status": "ACTIVE",
    }
    snapshot = {
        "A1_DRIVER_LINEAGE_REQUIRED": True,
        "SCORE_WEIGHTS": {"structural_theme": 0.5, "business_mapping": 0.5},
        "MACRO_POLICY_FEED": {"official_documents": [{"fact_id": policy_ref}]},
        "MAIN_BUSINESS_EVIDENCE": {
            "600183.SH": {"available": True, "evidence": [{"source_ref": business_ref}]},
        },
    }

    valid, valid_changed = _apply_stage_threshold_policy(
        {
            "structural_themes": [{
                "theme_id": "theme-policy",
                "display_name": "政策主线",
                "origin": "POLICY_PRIOR",
                "source_refs": [policy_ref],
            }],
            "industry_chain_graph": [{
                "node_id": "node-material",
                "theme_ids": ["theme-policy"],
                "source_refs": [policy_ref],
            }],
            "active_research_pool": [base_item],
            "monitor_pool": [],
        },
        "A1",
        snapshot,
    )
    assert valid_changed == 0
    assert [item["symbol"] for item in valid["active_research_pool"]] == ["600183.SH"]

    free_form, changed = _apply_stage_threshold_policy(
        {
            "structural_themes": [{
                "theme_id": "unrelated-theme",
                "display_name": "自由发挥题材",
                "source_refs": ["invented-ref"],
            }],
            "industry_chain_graph": [{
                "node_id": "invented-node",
                "theme_ids": ["unrelated-theme"],
                "source_refs": ["invented-ref"],
            }],
            "active_research_pool": [base_item],
            "monitor_pool": [],
        },
        "A1",
        snapshot,
    )
    assert changed == 1
    reasons = set(free_form["monitor_pool"][0]["reason_codes"])
    assert "A1_STRUCTURAL_THEME_LINEAGE_MISSING" in reasons
    assert "A1_CHAIN_NODE_LINEAGE_MISSING" in reasons

    rss_only_snapshot = {
        **snapshot,
        "MACRO_POLICY_FEED": {"official_documents": []},
        "INDUSTRY_NEWS_FEED": {
            "evidence_tier": "T3",
            "items": [{"fact_id": "rss:story-1", "source_url": "https://example.test/story"}],
        },
    }
    rss_only, rss_changed = _apply_stage_threshold_policy(
        {
            "structural_themes": [{
                "theme_id": "theme-policy",
                "display_name": "媒体题材",
                "source_refs": ["rss:story-1"],
            }],
            "industry_chain_graph": [{
                "node_id": "node-material",
                "theme_ids": ["theme-policy"],
                "source_refs": ["rss:story-1"],
            }],
            "active_research_pool": [base_item],
            "monitor_pool": [],
        },
        "A1",
        rss_only_snapshot,
    )
    assert rss_changed == 1
    rss_reasons = set(rss_only["monitor_pool"][0]["reason_codes"])
    assert "A1_THEME_DRIVER_EVIDENCE_INVALID" in rss_reasons
    assert "A1_CHAIN_NODE_EVIDENCE_INVALID" in rss_reasons


def test_a1_active_score_must_follow_configured_weighted_breakdown():
    item = {
        "symbol": "600183.SH",
        "structural_score": 90,
        "data_quality_score": 80,
        "evidence_confidence": 0.8,
        "score_breakdown": {"structural_theme": 80, "business_mapping": 60},
    }
    output, changed = _apply_stage_threshold_policy(
        {"active_research_pool": [item], "monitor_pool": []},
        "A1",
        {"SCORE_WEIGHTS": {"structural_theme": 0.5, "business_mapping": 0.5}},
    )
    assert changed == 1
    assert output["monitor_pool"][0]["reason_codes"] == ["A1_STRUCTURAL_SCORE_MISMATCH"]


def test_server_canonicalizes_raw_and_legacy_contribution_scores():
    weights = {"structural_theme": 0.2, "business_mapping": 0.8}
    output, changed = _canonicalize_stage_scores(
        {
            "active_research_pool": [
                {
                    "symbol": "600183.SH",
                    "structural_score": 99,
                    "score_breakdown": {"structural_theme": 80, "business_mapping": 60},
                },
                {
                    "symbol": "000001.SZ",
                    "structural_score": 70,
                    "score_breakdown": {"structural_theme": 10, "business_mapping": 60},
                },
            ]
        },
        "A1",
        {"SCORE_WEIGHTS": weights},
    )
    assert changed == 2
    raw, contribution = output["active_research_pool"]
    assert raw["structural_score"] == 64
    assert contribution["structural_score"] == 70
    assert contribution["score_breakdown"] == {
        "structural_theme": 50,
        "business_mapping": 75,
    }


def test_a2_score_is_recomputed_with_penalties_and_propagated_to_candidates():
    output, changed = _canonicalize_stage_scores(
        {
            "active_themes": [{
                "theme_id": "theme-1",
                "theme_score": 90,
                "score_breakdown": {"breadth": 80, "capital_flow": 20},
                "penalties": [{"points": -10}],
            }],
            "focus_pool": [{"symbol": "600183.SH", "theme_id": "theme-1", "theme_score": 90}],
            "watch_only_pool": [],
        },
        "A2",
        {
            "THEME_SCORE_WEIGHTS": {"breadth": 0.5, "capital_flow": 0.5},
            "CAPITAL_FLOW_SNAPSHOT": {"available": True},
        },
    )
    assert changed == 3
    assert output["active_themes"][0]["theme_score"] == 40
    assert output["active_themes"][0]["factor_coverage"] == {
        "ratio": 1.0,
        "available_weight": 1.0,
        "total_weight": 1.0,
        "unavailable_factors": [],
        "normalized_over_available_weight": True,
    }
    assert output["focus_pool"][0]["theme_score"] == 40


def test_a2_missing_optional_capital_flow_normalizes_over_available_weight():
    snapshot = {
        "THEME_SCORE_WEIGHTS": {"breadth": 0.87, "capital_flow": 0.13},
        "CAPITAL_FLOW_SNAPSHOT": {"available": False, "reason_code": "SOURCE_UNAVAILABLE"},
    }
    output, changed = _canonicalize_stage_scores(
        {
            "active_themes": [{
                "theme_id": "theme-agri",
                "theme_score": 60.9,
                "score_breakdown": {"breadth": 70, "capital_flow": 0},
                "penalties": [],
                "supporting_evidence": ["breadth"],
                "contradicting_evidence": ["capital flow unavailable"],
                "stage": "ACCELERATION",
                "new_entry_policy": "PROBE_ONLY",
                "rotation_overlap_ratio": 0,
            }],
            "focus_pool": [{"symbol": "600001.SH", "theme_id": "theme-agri", "theme_score": 60.9}],
            "watch_only_pool": [],
        },
        "A2",
        snapshot,
    )

    theme = output["active_themes"][0]
    assert changed >= 2
    assert theme["theme_score"] == 70
    assert theme["factor_coverage"] == {
        "ratio": 0.87,
        "available_weight": 0.87,
        "total_weight": 1.0,
        "unavailable_factors": ["capital_flow"],
        "normalized_over_available_weight": True,
    }
    assert output["focus_pool"][0]["theme_score"] == 70
    assert "A2_THEME_SCORE_MISMATCH" not in _a2_theme_reasons(theme, snapshot)


def test_a2_deterministic_context_exposes_role_and_optional_gap_without_invention():
    snapshot = FrozenInputSnapshot(
        snapshot_id="a2-context",
        snapshot_hash="a" * 64,
        data={"g0_symbols": ["600001.SH"]},
    )
    gate = DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=({
            "symbol": "600001.SH",
            "score": 72,
            "role": "CORE_ARMY",
            "identifiability_score": 81,
            "role_breakdown": {"liquidity_capacity": 90},
            "a2_factor_scores": {"capital_flow": {"available": False, "score": None}},
            "factor_coverage": {"ratio": 0.87},
            "critical_factor_coverage": {"sufficient": True},
            "data_sufficiency_state": "DEGRADED",
            "missing_optional_factors": ["capital_flow"],
            "reason_codes": ["A2_CAPITAL_FLOW_UNAVAILABLE", "A2_OPTIONAL_FACTS_DEGRADED"],
            "bottleneck_context": {"scarcity_claim_allowed": False},
            "eligible_routes": ["MARKET_CORE"],
            "route": "MARKET_CORE",
            "route_eligibility": {"MARKET_CORE": {"eligible": True}},
        },),
        review_symbols=("600001.SH",),
        monitor_symbols=(),
        rejected_symbols=(),
    )

    enriched = _with_a2_bottleneck_context(snapshot, gate)
    context = enriched.data["A2_BOTTLENECK_CONTEXT"]["600001.SH"]
    assert context["deterministic_market_role"] == "CORE_ARMY"
    assert context["factor_coverage"] == {"ratio": 0.87}
    assert context["critical_factor_coverage"] == {"sufficient": True}
    assert context["eligible_routes"] == ["MARKET_CORE"]


def test_a2_decision_fact_enrichment_canonicalizes_server_facts_and_fills_gaps():
    snapshot = {
        "A2_BOTTLENECK_CONTEXT": {
            "600001.SH": {
                "preferred_route": "MARKET_CORE",
                "deterministic_market_role": "TREND_LEADER",
                "a2_factor_scores": {
                    "capital_flow": {
                        "available": False,
                        "score": None,
                        "source": "CAPITAL_FLOW_SNAPSHOT",
                        "reason_code": "A2_CAPITAL_FLOW_UNAVAILABLE",
                    },
                    "tier_structure": {
                        "available": True,
                        "score": 72,
                        "source": "TIER_STRUCTURE_SNAPSHOT",
                        "availability_state": "OBSERVED_VALUE",
                        "ladder_height": 2,
                        "tier": "T2",
                    },
                    "leader_structure": {
                        "available": True,
                        "score": 84,
                        "source": "A2_FACTOR_SNAPSHOT",
                    },
                    "index_chain_resonance": {
                        "available": False,
                        "score": None,
                        "source": "A2_THEME_METRICS",
                        "reason_code": "A2_FACTOR_UNAVAILABLE",
                    },
                },
                "factor_coverage": {"ratio": 0.72},
                "missing_optional_factors": ["capital_flow", "index_chain_resonance"],
                "data_sufficiency_state": "DEGRADED",
                "gate_results": {
                    "IDENTIFIABILITY_MIN": {
                        "available": True,
                        "passed": False,
                        "blocks_decision": True,
                    }
                },
                "first_blocking_gate": "IDENTIFIABILITY_MIN",
                "all_failed_gates": ["IDENTIFIABILITY_MIN"],
            },
            "600002.SH": {
                "preferred_route": "MARKET_CORE",
                "a2_factor_scores": {},
                "factor_coverage": {"ratio": 0.70},
                "data_sufficiency_state": "DEGRADED",
            },
        },
        "CROWDING_SNAPSHOT": {
            "available": False,
            "reason_code": "PARTIAL_PROXY_ONLY",
            "source": "DETERMINISTIC_FROZEN_FACTS",
        },
    }
    model_capital = {"available": True, "score": 91, "source": "MODEL"}
    output = {
        "focus_pool": [{
            "symbol": "600001.SH",
            "market_role": "LEADER",
            "capital_flow": model_capital,
        }],
        "watch_only_pool": [{"symbol": "600002.SH"}],
    }

    enriched, changed = _enrich_a2_decision_facts(output, snapshot)
    focus = enriched["focus_pool"][0]
    watch = enriched["watch_only_pool"][0]

    assert changed > 0
    assert focus["capital_flow"] != model_capital
    assert focus["capital_flow"]["source"] == "CAPITAL_FLOW_SNAPSHOT"
    assert focus["capital_flow"]["available"] is False
    assert focus["capital_flow_available"] is False
    assert focus["market_role"] == "TREND_LEADER"
    assert focus["tier_structure"]["score"] == 72
    assert focus["tier_structure"]["ladder_height"] == 2
    assert focus["tier_structure"]["availability_state"] == "OBSERVED_VALUE"
    assert focus["leader_structure"]["score"] == 84
    assert focus["index_chain_resonance"]["available"] is False
    assert focus["supply_chain_role"]["reason_code"] == "NOT_REQUIRED_FOR_MARKET_CORE"
    assert focus["crowding"]["available"] is False
    assert focus["crowding"]["score"] is None
    assert set(focus["weak_evidence_fields"]) >= {
        "capital_flow",
        "index_chain_resonance",
        "supply_chain_role",
        "crowding",
    }
    assert watch["supply_chain_role"]["reason_code"] == "NOT_REQUIRED_FOR_MARKET_CORE"
    assert watch["factor_coverage"] == {"ratio": 0.70}
    assert focus["first_blocking_gate"] == "IDENTIFIABILITY_MIN"
    assert focus["all_failed_gates"] == ["IDENTIFIABILITY_MIN"]
    assert enriched["analysis_summary"]["gate_block_counts"] == {
        "IDENTIFIABILITY_MIN": 1
    }


def test_a2_contract_normalizes_cooling_to_weekly_state_and_legal_lifecycle_stage():
    original = {
        "active_themes": [{
            "theme_id": "theme-policy",
            "stage": "COOLING",
            "weekly_momentum_state": "PERSISTENT",
        }],
        "focus_pool": [{"symbol": "600001.SH", "theme_stage": "COOLING"}],
        "watch_only_pool": [{"symbol": "600002.SH", "stage": "cooling"}],
        "rejected_candidates": [],
    }

    normalized, changed = _canonicalize_a2_contract_semantics(original)

    assert changed >= 6
    assert original["active_themes"][0]["stage"] == "COOLING"
    assert normalized["active_themes"][0]["stage"] == "DIVERGENCE"
    assert normalized["active_themes"][0]["weekly_momentum_state"] == "COOLING"
    assert normalized["active_themes"][0]["reason_codes"] == [
        "A2_THEME_STAGE_CANONICALIZED_FROM_COOLING",
    ]
    assert normalized["focus_pool"][0]["theme_stage"] == "DIVERGENCE"
    assert normalized["focus_pool"][0]["weekly_momentum_state"] == "COOLING"
    assert normalized["watch_only_pool"][0]["stage"] == "DIVERGENCE"
    assert normalized["watch_only_pool"][0]["weekly_momentum_state"] == "COOLING"


def test_a2_batch_capacity_reason_is_ignored_without_promoting_the_row():
    output = {
        "active_themes": [],
        "focus_pool": [{"symbol": "600001.SH", "reason_codes": ["POOL_CAPACITY_FULL"]}],
        "watch_only_pool": [{"symbol": "600002.SH", "reason_codes": ["POOL_CAPACITY_FULL", "MODEL_WEAK"]}],
        "rejected_candidates": [{"symbol": "600003.SH", "reason_codes": ["POOL_CAPACITY_FULL"]}],
    }

    normalized, changed = _canonicalize_a2_contract_semantics(output)

    assert changed == 6
    assert [item["symbol"] for item in normalized["focus_pool"]] == ["600001.SH"]
    assert [item["symbol"] for item in normalized["watch_only_pool"]] == ["600002.SH"]
    assert [item["symbol"] for item in normalized["rejected_candidates"]] == ["600003.SH"]
    for pool in ("focus_pool", "watch_only_pool", "rejected_candidates"):
        for item in normalized[pool]:
            assert "POOL_CAPACITY_FULL" not in item["reason_codes"]
            assert "A2_BATCH_CAPACITY_REASON_IGNORED" in item["reason_codes"]


def test_a2_market_core_model_reject_remains_watch_only_when_route_is_eligible():
    snapshot = {
        "A2_BOTTLENECK_CONTEXT": {
            "600001.SH": {
                "deterministic_status": "REVIEW_CANDIDATE",
                "preferred_route": "MARKET_CORE",
                "eligible_routes": ["MARKET_CORE"],
                "deterministic_reason_codes": [],
            },
        },
    }
    output, _ = _canonicalize_a2_contract_semantics({
        "focus_pool": [],
        "watch_only_pool": [],
        "rejected_candidates": [{
            "symbol": "600001.SH",
            "reason_codes": ["POOL_CAPACITY_FULL"],
        }],
    })

    demoted, changed = _demote_a2_llm_rejects(output, snapshot)

    assert changed == 1
    assert demoted["rejected_candidates"] == []
    assert demoted["watch_only_pool"][0]["status"] == "WATCH_ONLY"
    assert "POOL_CAPACITY_FULL" not in demoted["watch_only_pool"][0]["reason_codes"]
    assert "A2_BATCH_CAPACITY_REASON_IGNORED" in demoted["watch_only_pool"][0]["reason_codes"]


def test_stage_lineage_is_server_owned_for_a2_and_locks_model_identity():
    upstream = {
        "active_research_pool": [{
            "symbol": "600001.SH",
            "candidate_id": "a1:600001.SH",
            "company_name": "上游名称",
            "primary_theme": "theme-policy",
            "industry_chain_node": "node-material",
        }],
    }
    snapshot = {
        "A2_BOTTLENECK_CONTEXT": {
            "600001.SH": {
                "company_name": "服务器名称",
                "theme_id": "theme-policy",
                "industry_chain_node": "node-material",
                "upstream_candidate_id": "a1:600001.SH",
                "preferred_route": "MARKET_CORE",
                "eligible_routes": ["MARKET_CORE"],
                "deterministic_market_role": "INSTITUTIONAL_CORE",
            },
        },
    }
    canonical, changed = _canonicalize_stage_lineage(
        {
            "focus_pool": [{
                "symbol": "600001.SH",
                "company_name": "模型伪造名称",
                "theme_id": "model-theme",
                "industry_chain_node": "model-node",
                "a2_route": "SUPPLY_CHAIN_ALPHA",
                "market_role": "LEADER",
                "upstream_candidate_id": "model-id",
            }],
            "watch_only_pool": [],
            "rejected_candidates": [],
        },
        "A2",
        upstream,
        snapshot,
    )

    item = canonical["focus_pool"][0]
    assert changed > 0
    assert item["company_name"] == "服务器名称"
    assert item["theme_id"] == "theme-policy"
    assert item["industry_chain_node"] == "node-material"
    assert item["a2_route"] == "MARKET_CORE"
    assert item["market_role"] == "INSTITUTIONAL_CORE"
    assert item["upstream_candidate_id"] == "a1:600001.SH"
    assert item["lineage_status"] == "COMPLETE"
    assert item["lineage_missing_fields"] == []


def test_a2_lineage_overlay_publishes_server_behavior_contract_over_model_unresolved():
    """A2 model rows cannot erase a deterministic TREND route contract."""

    upstream = {
        "active_research_pool": [{
            "symbol": "000998.SZ",
            "candidate_id": "a1:000998.SZ",
            "company_name": "隆平高科",
            "primary_theme": "TH_AGRI_FOREST",
            "industry_chain_node": "node-agri",
        }],
    }
    snapshot = {
        "A2_BOTTLENECK_CONTEXT": {
            "000998.SZ": {
                "route_context_schema": "a2-route-lineage/2",
                "company_name": "隆平高科",
                "theme_id": "TH_AGRI_FOREST",
                "industry_chain_node": "node-agri",
                "upstream_candidate_id": "a1:000998.SZ",
                "preferred_route": "MARKET_CORE",
                "eligible_routes": ["MARKET_CORE"],
                "deterministic_market_role": "TREND_LEADER",
                "stock_behavior_type": "TREND",
                "route_permission": ["TREND_MA5", "MA520_SWING"],
                "decision_id": "a2:000998.SZ:decision",
            },
        },
    }

    canonical, changed = _canonicalize_stage_lineage(
        {
            "focus_pool": [{
                "symbol": "000998.SZ",
                "company_name": "模型名称",
                "theme_id": "模型主题",
                "industry_chain_node": "模型节点",
                "a2_route": "SUPPLY_CHAIN_ALPHA",
                "market_role": "UNRESOLVED",
                "stock_behavior_type": "UNRESOLVED",
                "route_permission": [],
                "upstream_candidate_id": "模型ID",
            }],
            "watch_only_pool": [],
            "rejected_candidates": [],
        },
        "A2",
        upstream,
        snapshot,
    )

    item = canonical["focus_pool"][0]
    assert changed > 0
    assert item["market_role"] == "TREND_LEADER"
    assert item["stock_behavior_type"] == "TREND"
    assert item["route_permission"] == ["TREND_MA5", "MA520_SWING"]
    assert item["lineage_status"] == "COMPLETE"
    assert item["lineage_missing_fields"] == []
    assert "A2_STAGE_LINEAGE_MISSING" not in item.get("reason_codes", [])


def test_a2_lineage_treats_explicit_empty_route_permission_as_resolved_no_route():
    """UNRESOLVED is a complete fail-closed decision, not missing lineage."""

    upstream = {
        "active_research_pool": [{
            "symbol": "000998.SZ",
            "candidate_id": "a1:000998.SZ",
            "company_name": "隆平高科",
            "primary_theme": "TH_AGRI_FOREST",
            "industry_chain_node": "node-agri",
        }],
    }
    snapshot = {
        "A2_BOTTLENECK_CONTEXT": {
            "000998.SZ": {
                "route_context_schema": "a2-route-lineage/2",
                "company_name": "隆平高科",
                "theme_id": "TH_AGRI_FOREST",
                "industry_chain_node": "node-agri",
                "upstream_candidate_id": "a1:000998.SZ",
                "preferred_route": "MARKET_CORE",
                "eligible_routes": ["MARKET_CORE"],
                "deterministic_market_role": "UNRESOLVED",
                "stock_behavior_type": "UNRESOLVED",
                "route_permission": [],
                "decision_id": "a2:000998.SZ:unresolved",
            },
        },
    }

    canonical, _ = _canonicalize_stage_lineage(
        {
            "focus_pool": [{"symbol": "000998.SZ"}],
            "watch_only_pool": [],
            "rejected_candidates": [],
        },
        "A2",
        upstream,
        snapshot,
    )

    item = canonical["focus_pool"][0]
    assert item["route_permission"] == []
    assert item["lineage_status"] == "COMPLETE"
    assert item["lineage_missing_fields"] == []
    assert "A2_STAGE_LINEAGE_MISSING" not in item.get("reason_codes", [])


def test_stage_lineage_marks_missing_v2_fields_instead_of_accepting_model_values():
    canonical, _ = _canonicalize_stage_lineage(
        {
            "focus_pool": [{
                "symbol": "600001.SH",
                "company_name": "模型名称",
                "theme_id": "模型主题",
                "industry_chain_node": "模型节点",
                "a2_route": "MARKET_CORE",
                "market_role": "LEADER",
                "upstream_candidate_id": "模型ID",
            }],
            "watch_only_pool": [],
            "rejected_candidates": [],
        },
        "A2",
        {"active_research_pool": [{"symbol": "600001.SH"}]},
        {"A2_BOTTLENECK_CONTEXT": {"600001.SH": {"eligible_routes": []}}},
    )

    item = canonical["focus_pool"][0]
    assert item["route"] is None
    assert item["market_role"] is None
    assert item["upstream_candidate_id"] is None
    assert "route" in item["lineage_missing_fields"]
    assert "market_role" in item["lineage_missing_fields"]
    assert "upstream_candidate_id" in item["lineage_missing_fields"]
    assert "A2_STAGE_LINEAGE_MISSING" in item["reason_codes"]


def test_a3_lineage_locks_all_partitions_to_a2_upstream_and_origin_map():
    upstream = {
        "focus_pool": [{
            "symbol": "600001.SH",
            "upstream_candidate_id": "a1:600001.SH",
            "company_name": "A2名称",
            "theme_id": "theme-policy",
            "industry_chain_node": "node-material",
            "a2_route": "MARKET_CORE",
            "market_role": "TREND_LEADER",
        }],
    }
    snapshot = {
        "A3_CANDIDATE_ORIGIN": {"600001.SH": "FOCUS"},
        "A3_DETERMINISTIC_CONTEXT": {
            "600001.SH": {
                "status": "REVIEW_CANDIDATE",
                "strategy_profile": "TREND_MA5",
                "stock_behavior_type": "TREND",
                "route_permission": "ALLOW_A4",
                "decision_id": "a3:600001.SH:trend-ma5",
                "eligibility": "QUALIFIED",
                "required_conditions": ["MONTH_CLOSED", "WEEK_CLOSED"],
                "met_conditions": ["MONTH_CLOSED", "WEEK_CLOSED"],
                "unmet_conditions": [],
                "veto_conditions": [],
                "reward_risk": 2.8,
                "stop_distance_pct": 0.04,
                "minimum_reward_risk": 2.5,
                "maximum_stop_distance_pct": 0.06,
                "price_levels_hash": "p" * 64,
                "factor_snapshot_hash": "f" * 64,
                "reason_codes": [],
            },
        },
    }
    output = {
        "core_watch_pool": [{"symbol": "600001.SH", "company_name": "模型名称"}],
        "secondary_watch_pool": [{"symbol": "600001.SH", "theme_id": "模型主题"}],
        "rejected_candidates": [{"symbol": "600001.SH", "parent_candidate_id": "模型ID"}],
    }
    canonical, _ = _canonicalize_stage_lineage(output, "A3", upstream, snapshot)

    for pool in ("core_watch_pool", "secondary_watch_pool", "rejected_candidates"):
        item = canonical[pool][0]
        assert item["company_name"] == "A2名称"
        assert item["theme_id"] == "theme-policy"
        assert item["industry_chain_node"] == "node-material"
        assert item["a2_route"] == "MARKET_CORE"
        assert item["market_role"] == "TREND_LEADER"
        assert item["upstream_candidate_id"] == "a1:600001.SH"
        assert item["parent_candidate_id"] == "a1:600001.SH"
        assert item["candidate_origin"] == "FOCUS"
        assert item["lineage_status"] == "COMPLETE"
        assert item["route_permission"] == "ALLOW_A4"
        assert item["decision_id"] == "a3:600001.SH:trend-ma5"
        assert item["deterministic_strategy_profile"] == "TREND_MA5"
        assert item["deterministic_eligibility"] == "QUALIFIED"
        assert item["deterministic_met_conditions"] == ["MONTH_CLOSED", "WEEK_CLOSED"]
        assert item["deterministic_gate_status"] == "REVIEW_CANDIDATE"
        assert item["deterministic_price_evidence"]["minimum_reward_risk"] == 2.5


def test_a2_model_reject_is_demoted_only_for_soft_deterministic_context():
    output, changed = _demote_a2_llm_rejects(
        {
            "focus_pool": [],
            "watch_only_pool": [],
            "rejected_candidates": [
                {"symbol": "600001.SH", "reason_codes": ["MODEL_THEME_WEAK"]},
                {"symbol": "600002.SH", "reason_codes": ["A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE"]},
            ],
        },
        {
            "A2_BOTTLENECK_CONTEXT": {
                "600001.SH": {
                    "deterministic_status": "REVIEW_CANDIDATE",
                    "deterministic_reason_codes": ["A2_OPTIONAL_FACTS_DEGRADED"],
                },
                "600002.SH": {
                    "deterministic_status": "REVIEW_CANDIDATE",
                    "deterministic_reason_codes": ["A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE"],
                },
            },
        },
    )

    assert changed == 1
    assert output["rejected_candidates"][0]["symbol"] == "600002.SH"
    demoted = output["watch_only_pool"][0]
    assert demoted["symbol"] == "600001.SH"
    assert "MODEL_THEME_WEAK" in demoted["reason_codes"]
    assert "A2_LLM_REJECT_DEMOTED_TO_WATCH" in demoted["reason_codes"]
    assert demoted["status"] == "WATCH_ONLY"


def test_a2_gate_hard_reject_is_not_projected_as_watch_only():
    gate = DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=(
            {"symbol": "600001.SH", "status": "HARD_REJECT", "reason_codes": ["A2_LOW_IDENTITY_EXCLUDED"]},
            {
                "symbol": "600002.SH",
                "status": "LOCAL_MONITOR",
                "top_rotation_theme": True,
                "reason_codes": ["A2_NOT_SENT_TO_LLM"],
            },
            {
                "symbol": "600003.SH",
                "status": "LOCAL_MONITOR",
                "top_rotation_theme": False,
                "reason_codes": ["A2_OUTSIDE_ROTATION_TOP_THEMES"],
            },
        ),
        review_symbols=(),
        monitor_symbols=("600002.SH",),
        rejected_symbols=("600001.SH",),
    )

    assert [item["symbol"] for item in _gate_secondary_items(gate, "A2")] == ["600002.SH"]
    rejected = _gate_rejected_items(gate, "A2")
    assert [item["symbol"] for item in rejected] == ["600001.SH"]
    assert rejected[0]["status"] == "REJECTED"
    outside = _gate_outside_rotation_items(gate)
    assert [item["symbol"] for item in outside] == ["600003.SH"]
    assert outside[0]["status"] == "OUTSIDE_ROTATION"


def test_a2_effective_pool_keeps_only_top3_and_hot100_emotion_with_capacity_bound():
    gate = DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=(
            {
                "symbol": "600001.SH",
                "status": "REVIEW_CANDIDATE",
                "top_rotation_theme": True,
            },
            {
                "symbol": "600002.SH",
                "status": "LOCAL_MONITOR",
                "top_rotation_theme": True,
                "theme_rotation_rank": 2,
                "theme_rotation_score": 80,
            },
            {
                "symbol": "600003.SH",
                "status": "LOCAL_MONITOR",
                "stock_behavior_type": "EMOTION",
                "eastmoney_hot100": {"rank": 1},
            },
            {
                "symbol": "600005.SH",
                "status": "LOCAL_MONITOR",
                "a1_formal_member": False,
                "stock_behavior_type": "EMOTION",
                "eastmoney_hot100": {"rank": 2},
            },
            {
                "symbol": "600004.SH",
                "status": "LOCAL_MONITOR",
                "top_rotation_theme": None,
            },
        ),
        review_symbols=("600001.SH",),
        monitor_symbols=("600002.SH", "600003.SH", "600004.SH"),
        rejected_symbols=(),
    )

    rows = _gate_secondary_items(gate, "A2", pool_max=2)

    assert [row["symbol"] for row in rows] == ["600002.SH"]
    expanded_rows = _gate_secondary_items(gate, "A2", pool_max=5)
    assert [row["symbol"] for row in expanded_rows] == ["600002.SH", "600003.SH"]
    assert [row["symbol"] for row in _gate_outside_rotation_items(gate)] == [
        "600005.SH",
        "600004.SH",
    ]


def test_a2_provider_hard_reject_is_repaired_into_rejected_partition():
    gate = DeterministicGateResult(
        stage="A2_LOCAL_ROLE",
        decisions=({
            "symbol": "600001.SH",
            "status": "HARD_REJECT",
            "name": "服务器名称",
            "theme_id": "theme-policy",
            "node_id": "node-material",
            "role": "LOW_IDENTITY",
            "reason_codes": ["A2_LOW_IDENTITY_EXCLUDED"],
        },),
        review_symbols=(),
        monitor_symbols=(),
        rejected_symbols=("600001.SH",),
    )
    repaired, changed = _move_a2_hard_rejects_to_rejected(
        {"focus_pool": [{"symbol": "600001.SH", "theme_id": "模型主题"}], "rejected_candidates": []},
        gate,
    )

    assert changed == 1
    assert repaired["focus_pool"] == []
    rejected = repaired["rejected_candidates"][0]
    assert rejected["status"] == "REJECTED"
    assert rejected["theme_id"] == "theme-policy"
    assert "A2_DETERMINISTIC_HARD_REJECT" in rejected["reason_codes"]


def test_refresh_analysis_counts_replaces_stale_nested_pool_counts():
    refreshed = _refresh_analysis_counts(
        {
            "focus_pool": [{"symbol": "600001.SH"}],
            "watch_only_pool": [{"symbol": "600002.SH"}, {"symbol": "600003.SH"}],
            "rejected_candidates": [],
            "analysis_summary": {
                "pool_counts": {"focus_pool": 99, "watch_only_pool": 99, "rejected_candidates": 99},
            },
        },
        "A2",
    )
    assert refreshed["analysis_summary"]["pool_counts"] == {
        "focus_pool": 1,
        "watch_only_pool": 2,
        "rejected_candidates": 0,
    }


def test_a1_company_batches_cannot_invent_themes_after_discovery():
    discovery = {
        "structural_themes": [{"theme_id": "theme-policy"}],
        "industry_chain_graph": [{"node_id": "node-material", "theme_ids": ["theme-policy"]}],
    }
    assert _valid_a1_discovery_output(discovery)
    assert _a1_discovery_context_reasons(
        discovery,
        {"mode": "COMPANY_MAPPING", **discovery},
    ) == []
    invented = {
        "structural_themes": [{"theme_id": "free-form-theme"}],
        "industry_chain_graph": [{"node_id": "free-form-node", "theme_ids": ["free-form-theme"]}],
    }
    assert set(_a1_discovery_context_reasons(
        invented,
        {"mode": "COMPANY_MAPPING", **discovery},
    )) == {"A1_BATCH_THEME_OUTSIDE_DISCOVERY", "A1_BATCH_NODE_OUTSIDE_DISCOVERY"}
    canonical, changed = _canonicalize_a1_driver_context(
        invented,
        {"mode": "COMPANY_MAPPING", **discovery},
    )
    assert changed == 2
    assert canonical["structural_themes"] == discovery["structural_themes"]
    assert canonical["industry_chain_graph"] == discovery["industry_chain_graph"]
    assert _a1_discovery_context_reasons(
        canonical,
        {"mode": "COMPANY_MAPPING", **discovery},
    ) == []
    assert set(_a1_discovery_evidence_reasons(
        discovery,
        {"MACRO_POLICY_FEED": {"official_documents": [{"fact_id": "policy-ref"}]}},
    )) == {"A1_DISCOVERY_THEME_EVIDENCE_INVALID", "A1_DISCOVERY_NODE_EVIDENCE_INVALID"}
    evidenced = {
        "structural_themes": [{"theme_id": "theme-policy", "source_refs": ["policy-ref"]}],
        "industry_chain_graph": [{
            "node_id": "node-material", "theme_ids": ["theme-policy"], "source_refs": ["policy-ref"],
        }],
    }
    assert _a1_discovery_evidence_reasons(
        evidenced,
        {"MACRO_POLICY_FEED": {"official_documents": [{"fact_id": "policy-ref"}]}},
    ) == []


def test_a1_company_mapping_restores_frozen_facts_before_threshold_policy():
    local_candidates = {
        "000426.SZ": {
            "symbol": "000426.SZ",
            "theme_id": "theme-monthly",
            "node_id": "node-defense",
            "monthly_direction_id": "theme-monthly",
            "monthly_direction_name": "月度军工方向",
            "monthly_direction_matches": [{"theme_id": "theme-monthly"}],
            "sector_index_taxonomy": "INDUSTRY",
            "sector_index_code": "801740",
            "sector_index_name": "国防军工",
            "sector_constituent_confirmed": True,
            "taxonomy_matches": [{"taxonomy_code": "801740"}],
            "financial_quality_score": 92.1594,
            "fundamental_support": {"supported": True, "coverage_ratio": 0.8},
            "disclosed_business_match": {"structured_match_confirmed": True},
            "financial_subfactor_coverage": 0.8,
            "minimum_financial_subfactor_coverage": 0.6,
        },
        "600999.SH": {
            "symbol": "600999.SH",
            "theme_id": "theme-monthly",
            "node_id": "node-defense",
            "monthly_direction_id": "theme-monthly",
            "monthly_direction_name": "月度军工方向",
            "sector_index_taxonomy": "INDUSTRY",
            "sector_index_code": "801740",
            "sector_index_name": "国防军工",
            "sector_constituent_confirmed": True,
            "taxonomy_matches": [{"taxonomy_code": "801740"}],
            "financial_quality_score": 40.0,
            "fundamental_support": {"supported": False, "coverage_ratio": 0.4},
            "disclosed_business_match": {"structured_match_confirmed": True},
            "financial_subfactor_coverage": 0.4,
            "minimum_financial_subfactor_coverage": 0.6,
        },
    }
    output = {
        "active_research_pool": [
            {
                "symbol": "000426.SZ",
                "primary_theme": "模型错误主题",
                "industry_chain_node": "模型错误节点",
                "monthly_direction_id": "模型错误方向",
                "sector_constituent_confirmed": False,
                "financial_quality_score": 0.0,
                "financial_subfactor_coverage": 0.0,
                "status": "ACTIVE",
                "reason_codes": ["MODEL_SEMANTIC_VETO"],
                "structural_score": 80,
                "data_quality_score": 80,
                "evidence_confidence": 0.8,
            },
            {
                "symbol": "600999.SH",
                "primary_theme": "模型错误主题",
                "industry_chain_node": "模型错误节点",
                "sector_constituent_confirmed": False,
                "financial_quality_score": 99.0,
                "financial_subfactor_coverage": 0.99,
                "status": "ACTIVE",
                "reason_codes": ["MODEL_REVIEW_NOTE"],
                "structural_score": 80,
                "data_quality_score": 80,
                "evidence_confidence": 0.8,
            },
        ],
        "monitor_pool": [],
        "rejected_candidates": [
            {"symbol": "300001.SZ", "status": "REJECTED", "reason_codes": ["MODEL_REJECT"]},
        ],
    }
    canonical, changed = _canonicalize_a1_local_candidate_facts(
        output,
        {"mode": "COMPANY_MAPPING", "local_candidates": local_candidates},
    )

    assert changed > 0
    valid = canonical["active_research_pool"][0]
    assert valid["primary_theme"] == "theme-monthly"
    assert valid["industry_chain_node"] == "node-defense"
    assert valid["monthly_direction_id"] == "theme-monthly"
    assert valid["sector_constituent_confirmed"] is True
    assert valid["financial_quality_score"] == 92.1594
    assert valid["financial_subfactor_coverage"] == 0.8
    assert valid["reason_codes"] == ["MODEL_SEMANTIC_VETO"]
    assert valid["status"] == "ACTIVE"
    assert canonical["rejected_candidates"] == output["rejected_candidates"]

    thresholded, demotions = _apply_stage_threshold_policy(
        canonical,
        "A1",
        {"A1_POOL_TARGETS": {"monthly_chain_only": True}},
    )
    assert demotions == 1
    assert [item["symbol"] for item in thresholded["active_research_pool"]] == ["000426.SZ"]
    demoted = thresholded["monitor_pool"][0]
    assert demoted["symbol"] == "600999.SH"
    assert "A1_FINANCIAL_QUALITY_BELOW_MINIMUM" in demoted["reason_codes"]
    assert "A1_FINANCIAL_COVERAGE_BELOW_MINIMUM" in demoted["reason_codes"]


def test_a1_company_mapping_fact_canonicalization_is_fail_closed_without_context():
    output = {"active_research_pool": [{"symbol": "000426.SZ", "financial_quality_score": 99.0}]}

    canonical, changed = _canonicalize_a1_local_candidate_facts(output, {})

    assert changed == 0
    assert canonical == output


def test_macro_policy_projection_prefers_relevant_official_documents_and_retains_counts():
    feed = {
        "official_documents": [
            {"fact_id": "p-old", "title": "人工智能产业行动方案", "publish_time": "2026-08-01"},
            {"fact_id": "p-new", "title": "一般行政通知", "publish_time": "2026-08-25"},
            {"fact_id": "p-risk", "title": "产业政策", "prompt_injection_suspected": True},
        ]
    }
    projected = _project_macro_policy(feed, item_limit=1)
    assert projected["official_documents"][0]["fact_id"] == "p-old"
    assert projected["prompt_document_count"] == 1
    assert projected["full_document_count"] == 3


def test_a2_focus_must_reuse_a1_theme_and_cannot_invent_missing_capital_flow():
    upstream = {"structural_themes": [{"theme_id": "theme-policy"}]}
    valid_theme = {
        "theme_id": "theme-policy",
        "stage": "CONFIRMATION",
        "new_entry_policy": "ALLOW",
        "supporting_evidence": ["sector breadth"],
        "contradicting_evidence": ["capital flow unavailable"],
        "score_breakdown": {"breadth": 80, "capital_flow": 0},
        "theme_score": 80,
        "rotation_overlap_ratio": 0.2,
        "penalties": [],
    }
    item = {
        "symbol": "600183.SH",
        "theme_id": "theme-policy",
        "market_role": "CORE_ARMY",
        "identifiability_score": 80,
        "theme_score": 80,
    }
    snapshot = {
        "MIN_IDENTIFIABILITY_SCORE": 60,
        "THEME_SCORE_WEIGHTS": {"breadth": 0.5, "capital_flow": 0.5},
        "CAPITAL_FLOW_SNAPSHOT": {"available": False},
        "SECTOR_CYCLE_SNAPSHOT": {"history_metrics": {"available": True, "top3_daily_overlap": 0.2}},
    }
    valid, changed = _apply_a2_lineage_policy(
        {"active_themes": [valid_theme], "focus_pool": [item], "watch_only_pool": []},
        upstream,
        snapshot,
    )
    assert changed == 0
    assert valid["focus_pool"] == [item]

    invented_theme = {**valid_theme, "score_breakdown": {"breadth": 80, "capital_flow": 80}, "theme_score": 80}
    invalid, invalid_changed = _apply_a2_lineage_policy(
        {
            "active_themes": [invented_theme],
            "focus_pool": [{**item, "market_role": "LOW_IDENTITY", "theme_score": 80}],
            "watch_only_pool": [],
        },
        upstream,
        snapshot,
    )
    assert invalid_changed == 1
    reasons = set(invalid["watch_only_pool"][0]["reason_codes"])
    assert "A2_THEME_LINEAGE_INVALID" in reasons
    assert "A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE" in reasons

    invalid_stage, invalid_stage_changed = _apply_a2_lineage_policy(
        {
            "active_themes": [{**valid_theme, "stage": "ROTATION_ACTIVE"}],
            "focus_pool": [item],
            "watch_only_pool": [],
        },
        upstream,
        snapshot,
    )
    assert invalid_stage_changed == 1
    assert invalid_stage["active_themes"][0]["reason_codes"] == ["A2_THEME_STAGE_INVALID"]
    assert "A2_THEME_LINEAGE_INVALID" in invalid_stage["watch_only_pool"][0]["reason_codes"]


def test_a2_lineage_accepts_server_owned_active_row_theme_and_defers_entry_policy():
    """A baseline industry theme in A1 ACTIVE may reach A3 for risk review."""

    symbol = "002827.SZ"
    theme_id = "INDUSTRY:881109.TI"
    upstream = {
        "structural_themes": [{"theme_id": "TH_CHEMICAL"}],
        "active_research_pool": [{
            "symbol": symbol,
            "candidate_id": f"a1:{symbol}",
            "primary_theme": theme_id,
            "industry_chain_node": "BASELINE:881109.TI",
        }],
    }
    snapshot = {
        "MIN_IDENTIFIABILITY_SCORE": 60,
        "A2_BOTTLENECK_CONTEXT": {
            symbol: {
                "theme_id": theme_id,
                "preferred_route": "MARKET_CORE",
                "eligible_routes": ["MARKET_CORE"],
            },
        },
    }
    active_theme = {
        "theme_id": theme_id,
        "stage": "RETREAT",
        "new_entry_policy": "NO_NEW_ENTRY",
        "supporting_evidence": ["server-owned rotation facts"],
        "contradicting_evidence": ["risk-off"],
        "theme_score": 70,
    }
    candidate = {
        "symbol": symbol,
        "theme_id": theme_id,
        "market_role": "TREND_LEADER",
        "identifiability_score": 80,
        "theme_score": 70,
        "bottleneck_status": "NOT_REQUIRED_FOR_MARKET_CORE",
    }

    output, changed = _apply_a2_lineage_policy(
        {
            "active_themes": [active_theme],
            "focus_pool": [candidate],
            "watch_only_pool": [],
        },
        upstream,
        snapshot,
    )

    assert changed == 0
    assert output["focus_pool"] == [candidate]
    assert output["watch_only_pool"] == []
    assert "A2_THEME_OUTSIDE_A1" not in output["active_themes"][0].get("reason_codes", [])


def test_factor_projection_removes_duplicate_summary_and_raw_bar_payload():
    projected = _project_factor_snapshot({
        "600183.SH": {
            "symbol": "600183.SH",
            "technical_summary": {"duplicated": True},
            "timeframes": {
                "120m": {
                    "latest": {
                        "symbol": "600183.SH", "open": 10, "high": 11, "low": 9,
                        "close": 10.5, "end": "2026-08-25T15:00:00+08:00", "volume": 100,
                    },
                    "moving_averages": {"ma20": 10},
                    "ma_alignment": "BULL_PARTIAL",
                    "ma_event": "PULLBACK_HOLD_MA20",
                    "ma_bias": {"close_vs_ma20_pct": 0.05},
                }
            },
        }
    }, {"600183.SH"})
    factor = projected["600183.SH"]
    assert "technical_summary" not in factor
    assert "volume" not in factor["timeframes"]["120m"]["latest"]
    assert factor["timeframes"]["120m"]["ma_alignment"] == "BULL_PARTIAL"


def test_a3_candidate_domain_includes_only_eligible_watch_only_roles():
    projected, origins = _build_a3_candidate_domain(
        {
            "focus_pool": [{"symbol": "600519.SH", "market_role": "INSTITUTIONAL_CORE"}],
            "watch_only_pool": [
                {
                    "symbol": "002957.SZ",
                    "market_role": "LEADER",
                    "reason_codes": ["A2_THEME_SCORE_BELOW_MINIMUM"],
                    # An empty/missing route alias is resolved again by A3;
                    # it is not itself a hard data gap.
                    "eligible_routes": [],
                },
                {
                    "symbol": "300661.SZ",
                    "role": "CORE_ARMY",
                    "reason_codes": ["A2_NOT_SENT_TO_LLM", "A2_CAPITAL_FLOW_WEAK"],
                },
                {
                    "symbol": "000001.SZ",
                    "market_role": "TREND_CORE",
                    "reason_codes": ["A2_DATA_GAP"],
                    "local_decision": True,
                },
                {
                    "symbol": "000002.SZ",
                    "market_role": "LOW_IDENTITY",
                    "reason_codes": ["A2_WATCH_ONLY"],
                },
                {
                    "symbol": "000003.SZ",
                    "market_role": "EMOTION_LEADER",
                    "eligible_routes": ["UNSUPPORTED_ROUTE"],
                },
                {
                    "symbol": "000004.SZ",
                    "market_role": "EMOTION_LEADER",
                    "a2_route": "NO_ROUTE_READY",
                },
                {
                    "symbol": "000005.SZ",
                    "market_role": "TREND_CORE",
                    "top_rotation_theme": False,
                    "eligible_routes": ["MARKET_CORE"],
                },
            ],
        }
    )

    assert set(origins) == {"600519.SH", "002957.SZ", "300661.SZ"}
    assert origins["002957.SZ"] == "WATCH_ONLY"
    assert {item["symbol"] for item in projected["focus_pool"]} == set(origins)
    assert projected["watch_only_pool"] == []
    assert _a3_watch_only_candidate_eligible({
        "market_role": "TREND_CORE",
        "reason_codes": ["A2_NO_TIER"],
    })
    assert _a3_watch_only_candidate_eligible({
        "market_role": "TREND_CORE",
        "reason_codes": ["A2_IDENTIFIABILITY_BELOW_THRESHOLD"],
        "llm_decision": "REJECT",
    })
    assert not _a3_watch_only_candidate_eligible({
        "market_role": "TREND_CORE",
        "deterministic_reason_codes": ["A2_IDENTIFIABILITY_BELOW_THRESHOLD"],
    })
    assert not _a3_watch_only_candidate_eligible({
        "market_role": "TREND_CORE",
        "top_rotation_theme": False,
        "eligible_routes": ["MARKET_CORE"],
    })


def test_a3_watch_only_core_remains_executable_but_is_capped_to_probe():
    output, changed = _apply_a3_candidate_origin_policy(
        {
            "core_watch_pool": [
                {"symbol": "002957.SZ", "risk_unit": "STANDARD"},
            ],
            "secondary_watch_pool": [
                {"symbol": "600519.SH", "risk_unit": "NO_ENTRY"},
            ],
        },
        {"A3_CANDIDATE_ORIGIN": {"002957.SZ": "WATCH_ONLY"}},
    )

    assert changed > 0
    retained = output["core_watch_pool"][0]
    assert retained["candidate_origin"] == "WATCH_ONLY"
    assert retained["risk_unit"] == "PROBE"
    assert "A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE" in retained["reason_codes"]
    assert [item["symbol"] for item in output["secondary_watch_pool"]] == ["600519.SH"]


def test_a3_watch_only_origin_cannot_be_the_only_model_veto_for_qualified_setup():
    reasons = _a3_origin_only_veto_reasons(
        {
            "secondary_watch_pool": [
                {
                    "symbol": "002957.SZ",
                    "candidate_origin": "WATCH_ONLY",
                    "review_status": "VETO",
                    "reason_codes": [
                        "A2_LLM_REJECT_DEMOTED_TO_WATCH",
                        "CANDIDATE_ORIGIN_WATCH_ONLY_NON_EXECUTABLE",
                    ],
                }
            ]
        },
        {
            "A3_CANDIDATE_ORIGIN": {"002957.SZ": "WATCH_ONLY"},
            "A3_DETERMINISTIC_CONTEXT": {
                "002957.SZ": {"eligibility": "QUALIFIED", "strategy_profile": "TREND_MA5"}
            },
        },
    )

    assert reasons == ["A3_ORIGIN_ONLY_VETO_CONTRADICTS_TECHNICAL_QUALIFICATION"]


def test_a3_watch_only_model_veto_with_independent_risk_remains_valid():
    reasons = _a3_origin_only_veto_reasons(
        {
            "secondary_watch_pool": [
                {
                    "symbol": "002957.SZ",
                    "candidate_origin": "WATCH_ONLY",
                    "review_status": "VETO",
                    "reason_codes": ["HIGH_VOLUME_DISTRIBUTION"],
                }
            ]
        },
        {
            "A3_CANDIDATE_ORIGIN": {"002957.SZ": "WATCH_ONLY"},
            "A3_DETERMINISTIC_CONTEXT": {
                "002957.SZ": {"eligibility": "QUALIFIED", "strategy_profile": "TREND_MA5"}
            },
        },
    )

    assert reasons == []


def test_a3_secondary_probe_is_canonicalized_and_thresholded():
    raw = {
        "secondary_watch_pool": [
            {
                "symbol": "002957.SZ",
                "risk_unit": "PROBE",
                "trigger_zone": {"low": 9.9, "high": 10.1},
                "invalidation_level": 9.5,
                "stop_distance_pct": 0.05,
                "first_resistance": 11.0,
                "reward_risk": 2.0,
                "technical_score": 80,
            }
        ]
    }
    frozen = {
        "PRICE_LEVELS": {
            "002957.SZ": {
                "available": True,
                "trigger_zone": {"low": 10.0, "high": 10.2},
                "invalidation": 9.8,
                "stop_distance_pct": 0.02,
                "first_resistance": 11.0,
                "reward_risk": 2.5,
            }
        },
        "MIN_REWARD_RISK": 2.0,
        "MAX_STOP_DISTANCE": 0.06,
    }
    canonical, count, _ = _canonicalize_a3_price_fields(raw, frozen)
    assert count == 1
    assert canonical["secondary_watch_pool"][0]["reward_risk"] == 2.5
    assert canonical["secondary_watch_pool"][0]["stop_distance_pct"] == 0.02

    thresholded, changed = _apply_stage_threshold_policy(canonical, "A3", frozen)
    assert changed == 0
    assert thresholded["secondary_watch_pool"][0]["risk_unit"] == "PROBE"


def test_a3_semantic_veto_contradicting_frozen_reward_is_retry_reason():
    reasons = _a3_semantic_price_reasons(
        {
            "rejected_candidates": [
                {
                    "symbol": "002957.SZ",
                    "reason_codes": ["A3_REWARD_RISK_BELOW_MINIMUM"],
                }
            ]
        },
        {
            "PRICE_LEVELS": {
                "002957.SZ": {
                    "available": True,
                    "reward_risk": 2.5,
                    "stop_distance_pct": 0.02,
                }
            },
            "MIN_REWARD_RISK": 2.0,
            "MAX_STOP_DISTANCE": 0.06,
        },
    )
    assert reasons == ["A3_REWARD_RISK_REJECTION_CONTRADICTS_FROZEN_FACTS"]


def test_a3_secondary_probe_missing_score_breakdown_is_retryable_not_silent_no_entry():
    output = {
        "secondary_watch_pool": [
            {
                "symbol": "002156.SZ",
                "candidate_origin": "WATCH_ONLY",
                "risk_unit": "PROBE",
                "setup_type": "TREND_PULLBACK",
                "confirmation_conditions": ["FIVE_MIN_HIGHER_LOW"],
                "scenarios": {"normal_open_plan": {}, "weak_open_plan": {},
                              "high_gap_no_chase_plan": {}, "invalidation_plan": {}},
                "plan_expiry": "2026-08-31T15:00:00+08:00",
                "technical_score": 70,
            }
        ]
    }
    snapshot = {
        "TECHNICAL_SCORE_WEIGHTS": {
            "higher_timeframe_trend": 0.2,
            "structure_quality": 0.2,
            "volume_price": 0.15,
            "relative_strength": 0.1,
            "location_and_extension": 0.15,
            "room_and_reward_risk": 0.15,
            "liquidity": 0.05,
        }
    }
    assert _a3_secondary_probe_contract_reasons(output, snapshot) == []
    output["secondary_watch_pool"][0]["score_breakdown"] = {}
    assert _a3_secondary_probe_contract_reasons(output, snapshot) == []


def test_a3_global_pool_limits_are_applied_after_batch_merge():
    core = [
        {"symbol": f"6007{index:02d}.SH", "technical_score": 90 - index, "risk_unit": "STANDARD"}
        for index in range(5)
    ]
    limited, changed = _apply_a3_pool_limits(
        {"core_watch_pool": core, "secondary_watch_pool": [], "rejected_candidates": []},
        {"REGIME_PARAM_SET": {"agent_3": {"core_watch_max": 2, "total_watch_max": 3}}},
    )
    assert changed == 0
    assert [item["symbol"] for item in limited["core_watch_pool"]] == [
        "600700.SH", "600701.SH", "600702.SH", "600703.SH", "600704.SH"
    ]
    assert limited["secondary_watch_pool"] == []
    assert limited["rejected_candidates"] == []


def test_server_threshold_policy_demotes_low_theme_score_and_rejects_bad_a3_payoff():
    a2, a2_changed = _apply_stage_threshold_policy(
        {
            "focus_pool": [
                {"symbol": "300308.SZ", "theme_score": 65},
                {"symbol": "600183.SH", "theme_score": 56},
            ],
            "watch_only_pool": [],
        },
        "A2",
        {},
    )
    assert a2_changed == 1
    assert [item["symbol"] for item in a2["focus_pool"]] == ["300308.SZ"]
    assert a2["watch_only_pool"][0]["reason_codes"] == ["A2_THEME_SCORE_BELOW_MINIMUM"]
    assert a2["analysis_summary"]["policy_demotions"] == 1

    a3, a3_changed = _apply_stage_threshold_policy(
        {
            "core_watch_pool": [
                {
                    "symbol": "600183.SH",
                    "technical_score": 80,
                    "reward_risk": 0.75,
                    "stop_distance_pct": 0.05,
                },
                {
                    "symbol": "300502.SZ",
                    "technical_score": 50,
                    "reward_risk": 4.0,
                    "stop_distance_pct": 0.04,
                },
            ],
            "secondary_watch_pool": [],
            "rejected_candidates": [],
        },
            "A3",
            {
                "DETERMINISTIC_RESEARCH_V2_ENABLED": True,
                "MIN_REWARD_RISK": 2.0,
                "A3_DETERMINISTIC_CONTEXT": {
                    "600183.SH": {"eligibility": "QUALIFIED", "strategy_profile": "TREND_MA5"},
                    "300502.SZ": {"eligibility": "QUALIFIED", "strategy_profile": "MA520_SWING"},
                },
            },
        )
    assert a3_changed == 1
    assert [item["symbol"] for item in a3["core_watch_pool"]] == ["300502.SZ"]
    assert a3["secondary_watch_pool"][0]["risk_unit"] == "NO_ENTRY"
    assert "A3_REWARD_RISK_BELOW_MINIMUM" in a3["secondary_watch_pool"][0]["reason_codes"]
    assert "A3_TECHNICAL_SCORE_BELOW_MINIMUM" not in a3["secondary_watch_pool"][0]["reason_codes"]
    assert a3["analysis_summary"]["pool_counts"]["core_watch_pool"] == 1


def test_a2_relative_top5_below_strong_confirmation_stays_focus_without_padding():
    symbols = [
        "600001.SH",
        "600002.SH",
        "600003.SH",
        "600004.SH",
        "600005.SH",
        "600006.SH",
    ]
    output, changed = _apply_stage_threshold_policy(
        {
            "focus_pool": [
                {"symbol": symbols[0], "theme_id": "theme-first", "theme_score": 74},
                {
                    "symbol": symbols[1],
                    "theme_id": "theme-second",
                    "theme_score": 58,
                    "reason_codes": ["A2_THEME_SCORE_BELOW_FOCUS_THRESHOLD"],
                },
                {"symbol": symbols[2], "theme_id": "theme-third", "theme_score": 55},
                {"symbol": symbols[3], "theme_id": "theme-fourth", "theme_score": 54},
                {"symbol": symbols[4], "theme_id": "theme-fifth", "theme_score": 53},
                {"symbol": symbols[5], "theme_id": "theme-sixth", "theme_score": 52},
            ],
            "watch_only_pool": [],
        },
        "A2",
        {
            "MIN_THEME_SCORE": 60,
            "A2_BOTTLENECK_CONTEXT": {
                symbol: {
                    "top_rotation_theme": index < 5,
                    "deterministic_status": "REVIEW_CANDIDATE",
                    "eligible_routes": ["MARKET_CORE"],
                    "route_eligibility": {"MARKET_CORE": {"eligible": True}},
                    "deterministic_reason_codes": [],
                    "all_failed_gates": [],
                }
                for index, symbol in enumerate(symbols)
            },
        },
    )

    assert changed == 1  # only the sixth-theme row is actually demoted
    assert [item["symbol"] for item in output["focus_pool"]] == symbols[:5]
    assert all(
        "A2_RELATIVE_TOP3_BELOW_STRONG_CONFIRMATION" in item["reason_codes"]
        for item in output["focus_pool"][1:]
    )
    assert "A2_THEME_SCORE_BELOW_FOCUS_THRESHOLD" not in output["focus_pool"][1]["reason_codes"]
    assert "reason_codes" not in output["focus_pool"][0]
    assert [item["symbol"] for item in output["watch_only_pool"]] == [symbols[5]]
    assert output["watch_only_pool"][0]["reason_codes"] == ["A2_THEME_SCORE_BELOW_MINIMUM"]
    assert output["analysis_summary"]["policy_demotions"] == 1
    assert output["analysis_summary"]["a2_theme_score_reference"] == {
        "strong_confirmation_score": 60.0,
        "below_reference_observations": 4,
        "relative_top5_exception": True,
    }


def test_a2_relative_top3_score_exception_never_bypasses_hard_veto():
    output, changed = _apply_stage_threshold_policy(
        {
            "focus_pool": [{"symbol": "600001.SH", "theme_score": 55}],
            "watch_only_pool": [],
        },
        "A2",
        {
            "MIN_THEME_SCORE": 60,
            "A2_BOTTLENECK_CONTEXT": {
                "600001.SH": {
                    "top_rotation_theme": True,
                    "deterministic_status": "REVIEW_CANDIDATE",
                    "eligible_routes": ["MARKET_CORE"],
                    "route_eligibility": {"MARKET_CORE": {"eligible": True}},
                    "deterministic_reason_codes": ["A2_IDENTIFIABILITY_BELOW_MINIMUM"],
                    "all_failed_gates": ["IDENTIFIABILITY_MIN"],
                }
            },
        },
    )

    assert changed == 1
    assert output["focus_pool"] == []
    assert output["watch_only_pool"][0]["reason_codes"] == ["A2_THEME_SCORE_BELOW_MINIMUM"]


def test_a1_partition_reads_only_declared_symbol_not_evidence_references():
    output = {
        "envelope": _envelope(MODELS[0], "A1", "snap"),
        "active_research_pool": [
            {
                "symbol": "600519.SH",
                "company_name": "A",
                "primary_theme": "T",
                "industry_chain_node": "N",
                "core_thesis": ["peer 000001.SZ is not another pool member"],
                "bear_case": ["risk"],
                "structural_score": 80,
                "status": "ACTIVE",
                "source_refs": ["ref"],
            }
        ],
        "monitor_pool": [{"symbol": "000001.SZ"}],
        "rejected_candidates": [{"symbol": "300750.SZ"}],
    }
    reasons = _validate_output(
        output,
        stage="A1",
        model=MODELS[0],
        snapshot_id="snap",
        upstream_symbols={"600519.SH", "000001.SZ", "300750.SZ"},
        snapshot_data={},
    )
    assert "A1_POOL_PARTITION_OVERLAP" not in reasons
    assert "A1_POOL_PARTITION_INCOMPLETE" not in reasons


def test_pool_outside_upstream_blocks_only_downstream_stages(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = FakeResearchClient(symbols, outside_a2=True)
    result = ResearchPipeline(_settings(tmp_path), prompt_repository=_prompt_dir(tmp_path), model_client=client, now=lambda: NOW).run(
        _snapshot(), run_id="run-outside", generated_at=NOW
    )
    assert result.status == "BLOCKED"
    assert all(lane.stages[0].status == "VALIDATED" for lane in result.lanes)
    assert all(lane.stages[1].status == "BLOCKED" for lane in result.lanes)
    assert all("POOL_OUTSIDE_UPSTREAM" in lane.stages[1].reason_codes for lane in result.lanes)
    assert all(lane.stages[2].reason_codes == ("UPSTREAM_STAGE_BLOCKED",) for lane in result.lanes)
    assert all(stage for _, stage, _ in client.calls) and all(stage != "A3" for _, stage, _ in client.calls)


def test_permission_escalation_and_reasoning_are_fail_closed(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = FakeResearchClient(symbols, escalate=True)
    result = ResearchPipeline(_settings(tmp_path), prompt_repository=_prompt_dir(tmp_path), model_client=client, now=lambda: NOW).run(
        _snapshot(), run_id="run-permission", generated_at=NOW
    )
    assert result.status == "BLOCKED"
    assert all("PERMISSION_ESCALATION" in lane.stages[0].reason_codes for lane in result.lanes)
    for path in result.audit_paths:
        assert "do-not-persist-this" not in path.read_text(encoding="utf-8")


def test_canonical_frozen_snapshot_trade_candidates_are_g0(tmp_path: Path):
    from liangjian_funnel.pipeline.snapshot import SecurityRecord

    record = SecurityRecord(
        symbol="600519.SH",
        code="600519",
        exchange="SH",
        name="test",
        price=1,
        volume=1,
        amount=1,
        research_eligible=True,
        trade_eligible=True,
    )
    canonical = CanonicalFrozenInputSnapshot.model_construct(
        snapshot_id="canonical-snapshot",
        as_of=NOW,
        fetch_timestamps={},
        source_checksums={},
        universe_candidates=(record,),
        research_candidates=(record,),
        trade_candidates=(record,),
        daily_payload={},
        fundamental_payload={},
        candidate_failures=(),
        max_candidates=1,
        snapshot_hash="h" * 64,
    )
    symbols = dict(zip(MODELS, ("600519.SH",) * 3))
    client = FakeResearchClient(symbols)
    result = ResearchPipeline(_settings(tmp_path), prompt_repository=_prompt_dir(tmp_path), model_client=client, now=lambda: NOW).run(
        canonical, run_id="run-canonical", generated_at=NOW
    )
    assert result.status == "READY"


def test_explicit_g0_symbols_take_priority_over_trade_subset():
    from liangjian_funnel.pipeline.research import _extract_g0

    assert _extract_g0(
        {
            "g0_symbols": ["600519.SH", "830001.BJ"],
            "trade_candidates": [{"symbol": "600519.SH"}],
        }
    ) == {"600519.SH", "830001.BJ"}


def test_symbol_scanner_normalizes_prompt_prefix_exchange_format():
    assert _scan_symbols({"a": "SHSE.600519", "b": ["SZSE.000001", "BJSE.430047"]}) == {
        "600519.SH",
        "000001.SZ",
        "430047.BJ",
    }


def test_prompt_projection_bounds_history_and_filters_upstream_symbols():
    fundamentals = {
        "600519.SH": [
            {
                "_dataset": "INCOME",
                "report_date_ms": index,
                "period_end_ms": index,
                "operating_income": index,
                "unused_raw_column": "x" * 100,
            }
            for index in range(12)
        ]
        + [
            {"_dataset": "INDICATORS", "index_id": "sale_gross_margin", "value": "50"},
            {"_dataset": "INDICATORS", "index_id": "unused_indicator", "value": "1"},
        ],
        "000001.SZ": [{"_dataset": "INCOME", "report_date_ms": 1, "operating_income": 1}],
    }
    projected = _project_fundamentals(fundamentals, {"600519.SH"})
    assert set(projected) == {"600519.SH"}
    income = projected["600519.SH"]["latest_statements"]["income"]
    assert income["report_date_ms"] == 11
    assert "unused_raw_column" not in income
    indicators = projected["600519.SH"]["indicators"]
    assert set(indicators) == {"sale_gross_margin"}

    news = {
        "items": [
            {"title": "600519.SH event", "body": "a" * 2_000},
            {"title": "000001.SZ event", "body": "b" * 2_000},
        ],
        "by_symbol": {"600519.SH": 2, "000001.SZ": 3},
    }
    projected_news = _project_news(news, item_limit=1, symbols={"600519.SH"})
    assert projected_news["prompt_item_count"] == 1
    assert projected_news["full_item_count"] == 2
    assert set(projected_news["by_symbol"]) == {"600519.SH"}
    assert len(projected_news["items"][0]["body"]) <= 801


def test_a2_prompt_projection_filters_market_maps_and_preserves_batch_sector_evidence():
    symbols = {"600519.SH"}
    capital = _project_capital_flow(
        {"available": True, "by_symbol": {"600519.SH": {"score": 1}, "000001.SZ": {"score": 2}}},
        symbols,
    )
    assert set(capital["by_symbol"]) == symbols
    assert capital["prompt_symbol_count"] == 1
    assert capital["full_symbol_count"] == 2

    crowding = _project_crowding(
        {
            "scope_symbols": ["600519.SH", "000001.SZ"],
            "dragon_tiger_component": {
                "records": [{"symbol": "600519.SH"}, {"symbol": "000001.SZ"}]
            },
            "market_attention_component": {
                "records": [{"symbol": "600519.SH"}, {"symbol": "000001.SZ"}]
            },
        },
        symbols,
    )
    assert crowding["scope_symbols"] == ["600519.SH"]
    assert crowding["dragon_tiger_component"]["records"] == [{"symbol": "600519.SH"}]
    assert crowding["market_attention_component"]["prompt_record_count"] == 1

    def sector(code: str, *, percentile: float, returns: list[float]) -> dict:
        return {
            "taxonomy_code": code,
            "taxonomy_name": code,
            "health_state": "HEALTHY",
            "relative_strength_percentile": percentile,
            "breadth": 0.8,
            "strength": {"amount_total": 100},
            "history": {"return_5d": 0.1, "returns": returns},
        }

    cycle = _project_sector_cycle(
        {
            "available": True,
            "sector_health_snapshot": {
                "by_taxonomy": {"duplicated": True},
                "industry": {
                    "sector_count": 3,
                    "healthy_sectors": [sector("I1", percentile=99, returns=[1])],
                    "sectors": [
                        sector("I1", percentile=99, returns=[1, 2]),
                        sector("I2", percentile=98, returns=[3, 4]),
                        sector("I3", percentile=1, returns=[5, 6]),
                    ],
                },
                "concept": {"sector_count": 0, "sectors": []},
            },
        },
        symbols,
        {
            "THS_INDUSTRY_MEMBERSHIP": {
                "records": [{
                    "thscode": "600519.SH",
                    "memberships": [{"industry_thscode": "I3"}],
                }]
            },
            "THS_CONCEPT_MEMBERSHIP": {"records": []},
        },
        global_sector_limit=1,
    )
    health = cycle["sector_health_snapshot"]
    assert "by_taxonomy" not in health
    assert {item["taxonomy_code"] for item in health["industry"]["sectors"]} == {"I1", "I3"}
    assert all("returns" not in item["history"] for item in health["industry"]["sectors"])
    assert health["industry"]["full_sector_count"] == 3
    assert health["industry"]["batch_linked_sector_count"] == 1


def test_disclosure_projection_prioritizes_full_report_pdf_business_evidence():
    projected = _project_disclosures(
        {
            "by_symbol": {
                "600183.SH": [
                    {
                        "announcement_id": "new-general",
                        "announcement_title": "最新董事会公告",
                        "publish_time": "2026-08-25T10:00:00+08:00",
                        "event_tags": ["GENERAL_DISCLOSURE"],
                        "pdf_evidence_available": False,
                    },
                    {
                        "announcement_id": "half-year",
                        "announcement_title": "生益科技2026年半年度报告",
                        "publish_time": "2026-08-22T10:00:00+08:00",
                        "event_tags": ["EARNINGS"],
                        "pdf_evidence_available": True,
                        "pdf_evidence_snippets": [{"page_number": 11, "text": "主营业务分产品"}],
                    },
                ]
            }
        },
        {"600183.SH"},
    )

    assert projected["by_symbol"]["600183.SH"][0]["announcement_id"] == "half-year"


def test_a3_numeric_plan_must_match_deterministic_price_levels():
    output = {
        "envelope": _envelope(MODELS[0], "A3", "snap-1"),
        "core_watch_pool": [
            {
                "symbol": "600519.SH",
                "risk_unit": "PROBE",
                "trigger_zone": {"low": 9.9, "high": 10.1},
                "invalidation_level": 9.5,
                "stop_distance_pct": 0.05,
                "first_resistance": 11.0,
                "reward_risk": 2.0,
            }
        ],
    }
    snapshot = {
        "PRICE_LEVELS": {
            "600519.SH": {
                "available": True,
                "trigger_zone": {"low": 9.9, "high": 10.1},
                "invalidation": 9.5,
                "stop_distance_pct": 0.05,
                "first_resistance": 11.0,
                "reward_risk": 2.0,
            }
        }
    }
    reasons = _validate_output(
        output,
        stage="A3",
        model=MODELS[0],
        snapshot_id="snap-1",
        upstream_symbols={"600519.SH"},
        snapshot_data=snapshot,
    )
    assert reasons == []
    output["core_watch_pool"][0]["reward_risk"] = 99
    reasons = _validate_output(
        output,
        stage="A3",
        model=MODELS[0],
        snapshot_id="snap-1",
        upstream_symbols={"600519.SH"},
        snapshot_data=snapshot,
    )
    assert "A3_REWARD_RISK_PROVENANCE_MISMATCH" in reasons


def test_a3_model_rounding_is_replaced_with_frozen_server_values():
    output = {
        "core_watch_pool": [
            {
                "symbol": "300750.SZ",
                "trigger_zone": {"low": 376.47, "high": 376.73},
                "invalidation_level": 376.06,
                "stop_distance_pct": 0.001778,
                "first_resistance": 394.4,
                "reward_risk": 26.37,
                "scenarios": {
                    "normal_open_plan": {"action": "PROBE", "risk_unit": "PROBE"},
                    "invalidation_plan": {"action": "CANCEL_PLAN"},
                },
            }
        ]
    }
    frozen = {
        "PRICE_LEVELS": {
            "300750.SZ": {
                "available": True,
                "trigger_zone": {"low": 376.47, "high": 376.73},
                "invalidation": 376.06,
                "stop_distance_pct": 0.001778462028508523,
                "first_resistance": 394.4,
                "reward_risk": 26.373134328357523,
            }
        },
        "FACTOR_SNAPSHOT": {
            "300750.SZ": {
                "technical_summary": {
                    "timeframes": {
                        "daily": {"latest_close": 376.73, "ma": {"ma255": 377.43}},
                        "120m": {"latest_close": 376.73, "ma": {"ma255": 398.58}},
                    }
                }
            }
        },
    }
    canonical, count, trend_veto_count = _canonicalize_a3_price_fields(output, frozen)
    assert count == 1
    assert trend_veto_count == 1
    item = canonical["core_watch_pool"][0]
    assert item["stop_distance_pct"] == 0.001778462028508523
    assert item["reward_risk"] == 26.373134328357523
    assert item["risk_unit"] == "NO_ENTRY"
    assert "MAJOR_TREND_REPAIR_REQUIRED" in item["reason_codes"]
    assert item["scenarios"]["normal_open_plan"] == {
        "action": "NO_ENTRY",
        "risk_unit": "NO_ENTRY",
    }
    assert item["scenarios"]["invalidation_plan"] == {"action": "CANCEL_PLAN"}
    assert output["core_watch_pool"][0]["reward_risk"] == 26.37


def test_missing_prompt_or_invalid_client_response_blocks(tmp_path: Path):
    prompt_dir = tmp_path / "missing-prompts"
    prompt_dir.mkdir()
    prompt_dir.joinpath(PROMPT_FILENAMES[0]).write_text("{{MISSING}}", encoding="utf-8")
    pipeline = ResearchPipeline(_settings(tmp_path), prompt_repository=prompt_dir, model_client=object(), now=lambda: NOW)
    result = pipeline.run(_snapshot(), run_id="run-prompt-block", generated_at=NOW)
    assert result.status == "BLOCKED"
    assert all(lane.stages[0].reason_codes == ("PROMPT_REPOSITORY_BLOCKED",) for lane in result.lanes)

    with pytest.raises(PromptRepositoryError):
        PromptRepository(prompt_dir).load()


class InvalidJSONClient(FakeResearchClient):
    def complete(self, model: str, messages, **metadata):
        raise StrictJSONError()


def test_invalid_json_blocks_lane_and_does_not_retry_stage(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = InvalidJSONClient(symbols)
    result = ResearchPipeline(_settings(tmp_path), prompt_repository=_prompt_dir(tmp_path), model_client=client, now=lambda: NOW).run(
        _snapshot(), run_id="run-invalid-json", generated_at=NOW
    )
    assert result.status == "BLOCKED"
    assert all(lane.stages[0].reason_codes == ("STRICT_JSON_INVALID",) for lane in result.lanes)
    assert all(len([stage for model, stage, _ in client.calls if model == lane.model]) == 0 for lane in result.lanes)


class EmptyA1Client(FakeResearchClient):
    def complete(self, model: str, messages, **metadata):
        result = super().complete(model, messages, **metadata)
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        if runtime["stage"] == "A1":
            return ModelCallResult(
                model=model,
                output={
                    "envelope": _envelope(model, "A1", runtime["snapshot_id"]),
                    "active_research_pool": [],
                    "monitor_pool": [],
                    "rejected_candidates": [
                        {"symbol": candidate, "reason_codes": ["TEST_REJECTED"]}
                        for candidate in runtime["g0_symbols"]
                    ],
                },
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="thinking_object",
            )
        return result


def test_empty_validated_a1_pool_yields_deterministic_no_action_downstream(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = EmptyA1Client(symbols)
    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(_snapshot(), run_id="run-empty-a1", generated_at=NOW)

    assert result.status == "READY"
    assert all(lane.stages[0].status == "VALIDATED" for lane in result.lanes)
    assert all(lane.stages[1].status == "VALIDATED" for lane in result.lanes)
    assert all(lane.stages[2].status == "VALIDATED" for lane in result.lanes)
    assert all(lane.stages[1].diagnostics == {"outcome_code": "NO_ACTION_UPSTREAM_POOL_EMPTY"} for lane in result.lanes)
    assert all(lane.stages[2].output["core_watch_pool"] == [] for lane in result.lanes)
    assert all([stage for called_model, stage, _ in client.calls if called_model == model] == ["A1"] for model in MODELS)


def test_research_run_seals_private_generation_without_replacing_live_active(tmp_path: Path):
    settings = _settings(tmp_path)
    store = ResearchFeatureStore(settings.feature_store_db_path)
    store.create_feature_generation(
        generation_id="maintenance-live",
        as_of=NOW,
        contract_version="maintenance/1",
        algorithm_version="fixture",
        source_manifest_hash="maintenance-source",
        purpose="LIVE_FULL",
        activation_eligible=True,
    )
    store.validate_feature_generation("maintenance-live", validation={"fixture": True})
    store.seal_generation(
        "maintenance-live",
        validation_manifest={"fixture": True},
        purpose="LIVE_FULL",
        activation_eligible=True,
    )
    store.activate_generation("maintenance-live", None, "fixture-bootstrap")

    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    result = ResearchPipeline(
        settings,
        prompt_repository=_prompt_dir(tmp_path),
        model_client=FakeResearchClient(symbols),
        now=lambda: NOW,
    ).run(_snapshot(), run_id="run-private-generation", generated_at=NOW)

    assert result.status == "READY"
    assert store.get_active_feature_generation()["generation_id"] == "maintenance-live"
    binding = store.get_run_feature_binding(run_id=result.run_id, strict=True)
    generation = store.get_feature_generation(binding["generation_id"])
    assert generation["status"] == "SEALED"
    assert generation["purpose"] == "RUN_SNAPSHOT"
    assert generation["activation_eligible"] is False
