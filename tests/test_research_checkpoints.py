import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import (
    InMemoryResearchCheckpointStore,
    ResearchPipeline,
)
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 8, 24, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


def _prompt_dir(tmp_path: Path) -> Path:
    path = tmp_path / "prompts"
    path.mkdir(exist_ok=True)
    for filename in PROMPT_FILENAMES:
        path.joinpath(filename).write_text("prompt " + filename, encoding="utf-8")
    return path


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "model-secret"}, root=tmp_path)


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


def _item(symbol: str, stage: str) -> dict:
    if stage == "A1":
        return {"symbol": symbol, "structural_score": 80, "data_quality_score": 80, "evidence_confidence": 0.8}
    if stage == "A2":
        return {"symbol": symbol, "theme_score": 65}
    return {"symbol": symbol, "technical_score": 80, "reward_risk": 3.0, "stop_distance_pct": 0.03}


class _Client:
    def __init__(self, *, delay: bool = False):
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.delay = delay

    def complete(self, model, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        symbols = tuple(runtime["g0_symbols"] if stage == "A1" else runtime["upstream_symbols"])
        self.calls.append((model, stage, symbols))
        if self.delay and symbols:
            time.sleep((int(symbols[0][3:5]) % 3) * 0.003)
        output = {"envelope": _envelope(model, stage, runtime["snapshot_id"])}
        if stage == "A1":
            output["active_research_pool"] = [_item(symbols[0], stage)] if symbols else []
            output["monitor_pool"] = []
            output["rejected_candidates"] = [
                {"symbol": symbol, "reason_codes": ["TEST_REJECTED"]} for symbol in symbols[1:]
            ]
        elif stage == "A2":
            output["focus_pool"] = [_item(symbol, stage) for symbol in symbols]
        else:
            output["core_watch_pool"] = [_item(symbol, stage) for symbol in symbols]
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=1,
            attempts=1,
            thinking_variant="thinking_object",
        )


def _snapshot(count: int = 3, *, snapshot_hash: str | None = None) -> dict:
    symbols = [f"6005{index:02d}.SH" for index in range(count)]
    return {
        "snapshot_id": "checkpoint-snapshot",
        "snapshot_hash": snapshot_hash or "c" * 64,
        "g0": symbols,
    }


def test_successful_batches_resume_and_prompt_change_invalidates(tmp_path: Path):
    store = InMemoryResearchCheckpointStore()
    prompt_dir = _prompt_dir(tmp_path)
    client = _Client()
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=prompt_dir,
        model_client=client,
        checkpoint_store=store,
        now=lambda: NOW,
    )
    first = pipeline.run(_snapshot(), run_id="checkpoint-run", generated_at=NOW)
    assert first.status == "READY"
    assert len(client.calls) == 9
    assert len(store) == 9

    resumed_client = _Client()
    resumed = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=prompt_dir,
        model_client=resumed_client,
        checkpoint_store=store,
        now=lambda: NOW,
    ).run(_snapshot(), run_id="checkpoint-run", generated_at=NOW)
    assert resumed.status == "READY"
    assert resumed_client.calls == []
    assert all(
        stage.diagnostics and stage.diagnostics.get("checkpoint_reused") is True
        for lane in resumed.lanes
        for stage in lane.stages
    )

    prompt_dir.joinpath("agent_1_macro_chain_v2.txt").write_text("changed prompt", encoding="utf-8")
    changed_client = _Client()
    changed = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=prompt_dir,
        model_client=changed_client,
        checkpoint_store=store,
        now=lambda: NOW,
    ).run(_snapshot(), run_id="checkpoint-run", generated_at=NOW)
    assert changed.status == "READY"
    assert len(changed_client.calls) == 3


def test_parallel_batches_merge_deterministically_and_emit_redacted_progress(tmp_path: Path):
    symbols = [f"6005{index:02d}.SH" for index in range(34)]
    snapshot = {**_snapshot(len(symbols)), "g0": symbols}
    serial = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=_Client(delay=True),
        batch_workers=1,
        now=lambda: NOW,
    ).run(snapshot, run_id="serial", generated_at=NOW)
    events: list[dict] = []
    parallel = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=_Client(delay=True),
        batch_workers=3,
        progress_callback=events.append,
        now=lambda: NOW,
    ).run(snapshot, run_id="parallel", generated_at=NOW)
    assert serial.status == parallel.status == "READY"
    assert [lane.as_dict()["final_output"] for lane in serial.lanes] == [
        lane.as_dict()["final_output"] for lane in parallel.lanes
    ]
    assert events
    assert all({"run_id", "lane", "stage", "batch", "completed", "total", "status", "attempts"} <= event.keys() for event in events)
    assert all("reasoning" not in json.dumps(event).lower() for event in events)
    assert all(event["batch"]["completed"] <= event["batch"]["total"] for event in events)


def test_stage_snapshot_enricher_receives_only_current_upstream_pool(tmp_path: Path):
    seen: list[tuple[str, str, frozenset[str], str]] = []

    def enrich(stage, lane_id, model, upstream_symbols, snapshot):
        seen.append((stage, lane_id, upstream_symbols, snapshot.snapshot_id))
        return {"stage_enrichment": stage}

    result = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=_Client(),
        stage_snapshot_enricher=enrich,
        now=lambda: NOW,
    ).run(_snapshot(3), run_id="enrich-run", generated_at=NOW)
    assert result.status == "READY"
    assert len(seen) == 6
    for lane_id in {entry[1] for entry in seen}:
        a2 = next(entry for entry in seen if entry[0] == "A2" and entry[1] == lane_id)
        a3 = next(entry for entry in seen if entry[0] == "A3" and entry[1] == lane_id)
        assert a2[2] == a3[2] == frozenset({"600500.SH"})
        assert "600501.SH" not in a3[2]
        assert a2[3] == a3[3] == "checkpoint-snapshot"
    for lane in result.lanes:
        assert lane.stages[1].snapshot_id.startswith("checkpoint-snapshot:a2:")
        assert lane.stages[2].snapshot_id.startswith("checkpoint-snapshot:a3:")
