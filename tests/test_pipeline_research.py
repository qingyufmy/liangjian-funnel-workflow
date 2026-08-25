import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult, StrictJSONError
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES, PromptRepository, PromptRepositoryError
from liangjian_funnel.pipeline.research import ResearchPipeline, _scan_symbols
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
