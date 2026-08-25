import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult, StrictJSONError
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES, PromptRepository, PromptRepositoryError
from liangjian_funnel.pipeline.research import (
    ResearchPipeline,
    _canonicalize_a3_price_fields,
    _merge_a1_outputs,
    _project_fundamentals,
    _project_news,
    _scan_symbols,
    _validate_output,
)
from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot as CanonicalFrozenInputSnapshot
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 24, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


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
            output["active_research_pool"] = [{"symbol": symbol, "candidate_id": f"{model}:a1"}]
        elif stage == "A2":
            output["focus_pool"] = [{"symbol": symbol, "upstream_candidate_id": f"{model}:a1"}]
        else:
            output["core_watch_pool"] = [{"symbol": symbol, "parent_candidate_id": f"{model}:a2"}]
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
    assert all(client.calls[index][2]["upstream_output"] is None for index in (0, 3, 6))
    assert all("snapshot_data" not in runtime for _, _, runtime in client.calls)
    assert client.calls[1][2]["upstream_output"]["active_research_pool"][0]["symbol"] == "600519.SH"
    assert client.calls[4][2]["upstream_output"]["active_research_pool"][0]["symbol"] == "000001.SZ"

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
                output["active_research_pool"] = [{"symbol": symbol, "structural_score": 80}]
            elif stage == "A2":
                output["focus_pool"] = [{"symbol": symbol}]
            else:
                output["core_watch_pool"] = [{"symbol": symbol}]
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
    assert all(lane.stages[0].diagnostics == {"batch_count": 2, "completed_batches": 2} for lane in result.lanes)
    for model in MODELS:
        calls = [item for item in client.calls if item[0] == model]
        assert [stage for _, stage, _ in calls] == ["A1", "A1", "A2", "A3"]
        assert sorted(len(scope) for _, stage, scope in calls if stage == "A1") == [1, 5]


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
                },
                prompt_hash=metadata.get("prompt_hash"),
                input_hash=metadata.get("input_hash"),
                latency_ms=4,
                attempts=1,
                thinking_variant="thinking_object",
            )
        return result


def test_empty_validated_a1_pool_stops_downstream_before_model_call(tmp_path: Path):
    symbols = dict(zip(MODELS, ("600519.SH", "000001.SZ", "300750.SZ")))
    client = EmptyA1Client(symbols)
    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: NOW,
    ).run(_snapshot(), run_id="run-empty-a1", generated_at=NOW)

    assert result.status == "BLOCKED"
    assert all(lane.stages[0].status == "VALIDATED" for lane in result.lanes)
    assert all(lane.stages[1].reason_codes == ("UPSTREAM_POOL_EMPTY",) for lane in result.lanes)
    assert all(lane.stages[2].reason_codes == ("UPSTREAM_STAGE_BLOCKED",) for lane in result.lanes)
    assert all([stage for called_model, stage, _ in client.calls if called_model == model] == ["A1"] for model in MODELS)
