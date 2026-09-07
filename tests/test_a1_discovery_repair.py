from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline import research
from liangjian_funnel.pipeline.a1_contract import validate_discovery_output
from liangjian_funnel.pipeline.a1_packet import build_a1_research_packet
from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline
from liangjian_funnel.settings import Settings


NOW = datetime(2026, 9, 7, 18, 6, tzinfo=ZoneInfo("Asia/Shanghai"))
MODEL = "deepseek-v4-pro-0813"


def response(bad=False):
    return {
        "envelope": {"status": "OK"},
        "structural_themes": [{"theme_id": "theme-1", "source_refs": ["t3-not-authorized" if bad else "policy-1"]}],
        "industry_chain_graph": [{"node_id": "node-1", "theme_ids": ["unknown" if bad else "theme-1"], "source_refs": ["policy-1"]}],
        "industry_theme_mappings": [],
        "reasoning_content": "private-reasoning-must-not-be-saved",
    }


def test_production_mapping_snapshot_preserves_packet_identity():
    snapshot = FrozenInputSnapshot("actual-id", {"MACRO_POLICY_FEED": {}}, "a" * 64, NOW)
    packet = build_a1_research_packet(snapshot, monthly_strategy_context={})
    assert packet["snapshot_id"] == snapshot.snapshot_id
    assert packet["snapshot_hash"] == snapshot.snapshot_hash
    assert packet["as_of"] == str(NOW)


def test_mixed_known_unknown_theme_links_cannot_pass():
    output = response()
    output["industry_chain_graph"][0]["theme_ids"] = ["theme-1", "invented"]
    output["industry_theme_mappings"] = [{"industry_thscode": "881001.TI", "mapping_status": "MAPPED",
        "mapped_theme_ids": ["theme-1", "invented"], "supporting_source_refs": ["policy-1"]}]
    reasons = validate_discovery_output(output, require_targets=False).reason_codes
    assert "A1_DISCOVERY_NODE_THEME_LINK_INVALID" in reasons
    assert "A1_INDUSTRY_THEME_MAPPING_THEME_UNKNOWN" in reasons


def test_discovery_issues_pinpoint_paths_without_granting_evidence():
    bad = response(True)
    issues = research._a1_discovery_validation_issues(bad, ("policy-1",))
    assert [v["path"] for v in issues] == ["structural_themes[0].source_refs", "industry_chain_graph[0].theme_ids"]
    assert issues[0]["unauthorized_refs"] == ["t3-not-authorized"]
    assert issues[1]["allowed_theme_ids"] == ["theme-1"]
    assert bad["structural_themes"][0]["source_refs"] == ["t3-not-authorized"]


def stage_fixture(tmp_path, monkeypatch, *, always_bad=False):
    settings = Settings.from_env({}, root=tmp_path)
    captured = []

    class Client:
        def complete(self, model, messages, **kwargs):
            captured.append(messages)
            return {"output": response(always_bad or len(captured) == 1), "attempts": 1}

    pipeline = ResearchPipeline(settings, model_client=Client(), output_dir=tmp_path / "audit", now=lambda: NOW)
    prepared = SimpleNamespace(prompt_hash="p" * 64, input_hash="i" * 64,
        messages=({"role": "system", "content": "frozen-system"}, {"role": "user", "content": "frozen-input"}),
        prompt_chars=30, estimated_input_tokens=20, input_token_limit=100000, replacement_chars={})
    monkeypatch.setattr(pipeline, "_prepare_stage_request", lambda **kw: prepared)
    snapshot = FrozenInputSnapshot("snap", {}, "s" * 64, NOW)
    kwargs = dict(lane_id="lane_1", model=MODEL, stage="A1", snapshot=snapshot, upstream_output=None,
        upstream_symbols=set(), bundle=object(), run_id="replay", projection_symbols=set(),
        a1_discovery_context={"mode": "POLICY_MACRO_DISCOVERY", "g0_symbol_count": 500,
                              "authorized_discovery_source_refs": ("policy-1",)})
    return pipeline, captured, kwargs


def test_discovery_retry_has_prior_json_exact_issues_and_durable_audit(tmp_path, monkeypatch):
    pipeline, calls, kwargs = stage_fixture(tmp_path, monkeypatch)
    audit = pipeline._run_stage(**kwargs)
    assert audit.status == "VALIDATED"
    assert len(calls) == 2
    assert calls[1][-2]["role"] == "assistant"
    assert "t3-not-authorized" in calls[1][-2]["content"]
    assert "structural_themes[0].source_refs" in calls[1][-1]["content"]
    assert "industry_chain_graph[0].theme_ids" in calls[1][-1]["content"]
    paths = audit.diagnostics["discovery_audit_paths"]
    assert len(paths) == 2
    for path in paths:
        text = Path(path).read_text(encoding="utf-8")
        assert "private-reasoning-must-not-be-saved" not in text
        record = json.loads(text)
        digest = record.pop("record_hash")
        assert research._sha256_json(record) == digest
        assert record["executable"] is False
        assert record["authorized_source_refs"] == ["policy-1"]
    first = json.loads(Path(paths[0]).read_text(encoding="utf-8"))
    assert first["parsed_output"]["structural_themes"][0]["source_refs"] == ["t3-not-authorized"]
    assert first["validation_issues"]
    assert audit.output["structural_themes"][0]["source_refs"] == ["policy-1"]


def test_two_invalid_responses_stay_blocked_and_keep_nonexecutable_evidence(tmp_path, monkeypatch):
    pipeline, calls, kwargs = stage_fixture(tmp_path, monkeypatch, always_bad=True)
    audit = pipeline._run_stage(**kwargs)
    assert audit.status == "BLOCKED"
    assert audit.output is None
    assert audit.symbols == ()
    assert len(calls) == 2
    assert len(audit.diagnostics["discovery_audit_paths"]) == 2
    assert audit.diagnostics["validation_issues"]


def test_audit_storage_failure_cannot_publish_or_repeat_model(tmp_path, monkeypatch):
    pipeline, calls, kwargs = stage_fixture(tmp_path, monkeypatch)
    def fail(*args, **kw):
        raise OSError("disk-full")
    monkeypatch.setattr(research, "atomic_write_json", fail)
    audit = pipeline._run_stage(**kwargs)
    assert audit.reason_codes == ("A1_DISCOVERY_AUDIT_WRITE_FAILED",)
    assert audit.output is None
    assert len(calls) == 1


def test_failed_discovery_does_not_invent_secondary_coverage_errors(tmp_path, monkeypatch):
    pipeline, calls, kwargs = stage_fixture(tmp_path, monkeypatch, always_bad=True)
    rejected = pipeline._run_stage(**kwargs)
    monkeypatch.setattr(research, "build_monthly_strategy_context", lambda *a, **k: {"g0_symbol_count": 3907, "status": "READY"})
    monkeypatch.setattr(pipeline, "_run_stage_with_checkpoint", lambda **k: rejected)
    lane = pipeline._run_lane_v2(lane_id="lane_1", model=MODEL, snapshot=kwargs["snapshot"], g0=set(),
        bundle=object(), run_id="outer", global_reason=None)
    assert lane.status == "BLOCKED"
    assert "A1_MONTHLY_THEME_COVERAGE_INSUFFICIENT" not in lane.stages[0].reason_codes
    assert "A1_DISCOVERY_THEME_EVIDENCE_INVALID" in lane.stages[0].reason_codes
    assert all(s.status == "NOT_RUN_UPSTREAM_BLOCKED" for s in lane.stages[1:])
