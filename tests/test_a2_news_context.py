from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
import json

import pytest

from liangjian_funnel.pipeline.a2_news_context import build_a2_news_shadow
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.pipeline.market_aggregates import build_news_heat_snapshot
from liangjian_funnel.pipeline.research import FrozenInputSnapshot, ResearchPipeline, _project_news

NOW = datetime.fromisoformat("2026-09-29T15:00:00+08:00")


def item(**kwargs):
    return {"fact_id": "f1", "content_hash": "h1", "source_url": "https://example.test/1",
            "source_id": "open_news.one", "title": "公司发布订单公告", "summary": "合同待履约",
            "symbol": "600001.SH", "publish_time": "2026-09-29T10:00:00+08:00",
            "fetch_time": "2026-09-29T10:02:00+08:00", "ingest_time": "2026-09-29T10:03:00+08:00",
            **kwargs}


def build(items, decisions=(), **kwargs):
    return build_a2_news_shadow({"available": True, "items": items}, decisions,
                               as_of=NOW, snapshot_hash="frozen", **kwargs)


def test_reposts_do_not_mean_independent_confirmation_and_revision_stays_separate():
    records = [item(), item(fact_id="f2", source_id="open_news.two"),
               item(fact_id="f3", summary="合同取消", content_hash="h3")]
    original = deepcopy(records)
    report = build(records)
    assert report["event_count"] == 2
    assert report["eligible_fact_count"] == 3
    assert max(e["report_count"] for e in report["events"]) == 2
    assert all(e["independent_confirmation_count"] is None for e in report["events"])
    assert records == original
    assert build(list(reversed(records)))["report_hash"] == report["report_hash"]


@pytest.mark.parametrize("change,reason", [
    ({"ingest_time": None}, "ARRIVAL_OR_PUBLICATION_TIME_MISSING"),
    ({"ingest_time": "2026-09-29T15:01:00+08:00"}, "NOT_KNOWN_AT_CUTOFF"),
    ({"fetch_time": "2026-09-29T09:00:00+08:00"}, "INVALID_TIME_ORDER"),
    ({"publish_time": "2026-09-01T10:00:00+08:00"}, "OUTSIDE_NEWS_WINDOW"),
    ({"prompt_injection_suspected": True}, "UNTRUSTED_TEXT_BLOCKED"),
    ({"summary": "[UNTRUSTED_TEXT_BLOCKED]"}, "UNTRUSTED_TEXT_BLOCKED"),
    ({"content_hash": None}, "EVIDENCE_REFERENCE_MISSING"),
])
def test_unusable_evidence_has_explicit_reason(change, reason):
    report = build([item(**change)])
    assert report["events"] == []
    assert report["excluded_counts"] == {reason: 1}


def test_news_does_not_change_gate_or_admit_outside_symbols():
    decisions = [{"symbol": "600001.SH", "status": "REJECTED", "reason_codes": ["RISK"]},
                 {"symbol": "600002.SH", "status": "REVIEW_CANDIDATE"}]
    original = deepcopy(decisions)
    report = build([item(), item(fact_id="outside", symbol="600003.SH", title="另一个事件")], decisions)
    assert decisions == original
    assert len(report["candidates"]) == 2
    assert report["candidates"][0]["quant_status"] == "REJECTED"
    assert report["candidates"][1]["association"] == "NO_LINKED_NEWS"
    assert len(report["unlinked_event_ids"]) == 1
    assert report["ranking_changed"] is report["llm_input_enabled"] is False


def test_count_reconciliation_and_bounded_projection():
    report = build([item(), item(), item(fact_id="f2", title="另一公告"), item(ingest_time=None)], max_events=1)
    assert report["input_item_count"] == report["eligible_fact_count"] + sum(report["excluded_counts"].values())
    assert report["event_count"] == 2 and report["omitted_event_count"] == 1
    assert len(report["events"]) == 1


def test_heat_preserves_clocks_but_model_projection_stays_unchanged():
    raw = item()
    heat = build_news_heat_snapshot({"fact_groups": {"STOCK_NEWS_ITEM": [raw]}}, ["600001.SH"], as_of=NOW)
    assert heat["items"][0]["ingest_time"] == raw["ingest_time"]
    assert build_a2_news_shadow(heat, (), as_of=NOW, snapshot_hash="h")["event_count"] == 1
    legacy = deepcopy(heat)
    for k in ("ingest_time", "fetch_time", "content_hash", "prompt_injection_suspected"):
        legacy["items"][0].pop(k)
    assert _project_news(heat, item_limit=3) == _project_news(legacy, item_limit=3)


def test_real_gate_persistence_writes_sidecar_without_feature_store(tmp_path):
    pipeline = SimpleNamespace(output_dir=tmp_path, feature_store=None)
    snapshot = FrozenInputSnapshot("s", {"NEWS_HEAT_SNAPSHOT": {"items": [item()]}}, as_of=NOW)
    gate = DeterministicGateResult("A2_LOCAL_ROLE", (), (), (), ())
    ResearchPipeline._persist_gate(pipeline, "run", "lane", gate, snapshot)
    report = json.loads((tmp_path / "a2_news_shadow/run/lane.json").read_text(encoding="utf-8"))
    assert report["event_count"] == 1
    assert report["base_snapshot_hash"] == snapshot.snapshot_hash


def test_sidecar_disk_failure_does_not_block_a2(tmp_path, monkeypatch, caplog):
    def fail(*args, **kwargs):
        raise OSError("disk")
    monkeypatch.setattr("liangjian_funnel.pipeline.research.atomic_write_json", fail)
    ResearchPipeline._persist_gate(SimpleNamespace(output_dir=tmp_path, feature_store=None), "r", "l",
        DeterministicGateResult("A2_LOCAL_ROLE", (), (), (), ()),
        FrozenInputSnapshot("s", {}, as_of=NOW))
    assert "A2_NEWS_SHADOW_WRITE_FAILED" in caplog.text


def test_workflow_merge_retains_separate_fact_references_and_omissions():
    from liangjian_funnel.workflow import _merge_news_heat_snapshots
    merged = _merge_news_heat_snapshots({"items": [item()], "omitted_item_count": 3},
        {"items": [item(fact_id="f2")], "omitted_item_count": 4})
    assert len(merged["items"]) == 2
    assert merged["omitted_item_count"] == 7
    assert build(merged["items"])["eligible_fact_count"] == 2
