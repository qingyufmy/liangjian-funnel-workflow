from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
import json

import pytest

from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.pipeline.a2_news_context import build_a2_news_shadow, _hash
from liangjian_funnel.pipeline.a2_news_review import (
    SYSTEM_PROMPT, enrich_news_shadow, build_review_packet, validate_review, review_with_client, _matches,
)

NOW = datetime.fromisoformat("2026-09-29T15:00:00+08:00")


def report():
    raw = {"fact_id": "n1", "content_hash": "nh1", "source_url": "https://example.test/1",
           "title": "液冷服务器订单取消", "summary": "公司公告尚待核实",
           "symbol": "600001.SH", "source_id": "open_news.test",
           "publish_time": "2026-09-29T10:00:00+08:00", "fetch_time": "2026-09-29T10:01:00+08:00",
           "ingest_time": "2026-09-29T10:02:00+08:00"}
    return build_a2_news_shadow({"items": [raw]}, (), as_of=NOW, snapshot_hash="frozen")


def output(packet):
    return {"reviews": [{"event_id": e["event_id"], "fact_ids": [e["references"][0]["fact_id"]],
                         "stance": "RISK_CLUE", "analysis": "报道涉及订单取消，需核查原始公告。",
                         "uncertainties": ["尚未确认具体经营影响"]} for e in packet["events"]]}


def test_mapping_is_versioned_negative_news_not_automatically_bullish():
    base = report()
    original = deepcopy(base)
    enriched = enrich_news_shadow(base, load_rotation_theme_config())
    assert base == original
    event = enriched["events"][0]
    assert "AI_LIQUID_COOLING" in {r["theme_id"] for r in event["theme_clues"]}
    assert event["business_gap_count"] == 1
    assert event["business_relationship_verified"] is False
    assert enriched["taxonomy_hash"] == _hash(enriched["taxonomy"])
    assert enriched["report_hash"] != base["report_hash"]
    assert not _matches("RAID", "AI") and _matches("AI 服务器", "AI")


def test_business_evidence_requires_receipt_clock_and_hash():
    row = {"text": "公司液冷业务占主营收入40%", "source_ref": "announcement-1", "content_hash": "b1",
           "publish_time": "2026-08-01T09:00:00+08:00", "ingest_time": "2026-08-01T10:00:00+08:00"}
    business = {"600001.SH": {"evidence": [row]}}
    enriched = enrich_news_shadow(report(), load_rotation_theme_config(), business)
    assert len(enriched["events"][0]["business_excerpts"]) == 1
    for patch in ({"ingest_time": None}, {"content_hash": None}, {"ingest_time": "2026-09-30T00:00:00+08:00"}):
        blocked = enrich_news_shadow(report(), load_rotation_theme_config(),
            {"600001.SH": {"evidence": [{**row, **patch}]}})
        assert blocked["events"][0]["business_excerpts"] == []


def test_packet_budget_and_tampering():
    r = report()
    packet = build_review_packet(r, max_chars=1000)
    assert len(SYSTEM_PROMPT) + len(json.dumps(packet, ensure_ascii=False)) <= 1000
    assert packet["omitted_event_count"] == 1
    r["events"][0]["title"] = "修改"
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        build_review_packet(r)


@pytest.mark.parametrize("mutation,reason", [
    (lambda o: o["reviews"][0].update(fact_ids=["invented"]), "CITATION_INVALID"),
    (lambda o: o["reviews"][0].update(event_id="another"), "EVENT_INVALID"),
    (lambda o: o["reviews"].clear(), "COVERAGE_MISMATCH"),
    (lambda o: o["reviews"].append(deepcopy(o["reviews"][0])), "EVENT_INVALID"),
    (lambda o: o["reviews"][0].update(action="BUY"), "ROW_INVALID"),
    (lambda o: o["reviews"][0].update(stance="STRONG_BUY"), "STANCE_INVALID"),
    (lambda o: o["reviews"][0].update(uncertainties=[]), "UNCERTAINTY_REQUIRED"),
])
def test_model_output_fails_closed(mutation, reason):
    packet = build_review_packet(report())
    response = output(packet)
    mutation(response)
    with pytest.raises(ValueError, match=reason):
        validate_review(response, packet)


def test_client_is_opt_in_and_reuses_bounded_existing_interface(tmp_path):
    packet = build_review_packet(report())
    calls = []
    class FakeClient:
        def complete(self, model, messages, **kwargs):
            calls.append((model, kwargs))
            return SimpleNamespace(output=output(packet))
    with pytest.raises(ValueError, match="NOT_AUTHORIZED"):
        review_with_client(packet, FakeClient())
    assert calls == []
    accepted = review_with_client(packet, FakeClient(), allow_model_call=True, receipt_root=tmp_path)
    assert calls[0][0] == "deepseek-v4-pro"
    assert calls[0][1]["timeout_seconds"] == 90
    assert accepted["citation_contract_valid"] and not accepted["semantic_truth_verified"]


def test_end_to_end_news_fact_projection_preserves_evidence():
    from liangjian_funnel.data.open_news import OpenNewsFetchResult, OpenNewsItem
    from liangjian_funnel.facts.open_news import normalize_open_news_results
    from liangjian_funnel.facts.hithink import manifest_projection
    from liangjian_funnel.pipeline.market_aggregates import build_news_heat_snapshot
    # Actual adapter result contracts; synthetic content, not a live source claim.
    news = OpenNewsItem(source_id="open_news.test", provider_item_id="one",
        title="液冷服务器订单取消", summary="公告待核实", publish_time=NOW - timedelta(minutes=5),
        url="https://example.test/news/1", symbol="600001.SH", channel="eastmoney_stock", fetched_at=NOW)
    result = OpenNewsFetchResult(source_id="open_news.test", channel="eastmoney_stock",
        source_url="https://example.test/feed", ok=True, complete=True, reason_code="OK",
        items=(news,), fetched_at=NOW, http_status=200)
    manifest = normalize_open_news_results([result], as_of=NOW, ingest_time=NOW)
    heat = build_news_heat_snapshot(manifest_projection(manifest), ["600001.SH"], as_of=NOW)
    shadow = build_a2_news_shadow(heat, (), as_of=NOW, snapshot_hash=manifest.manifest_hash)
    enriched = enrich_news_shadow(shadow, load_rotation_theme_config())
    packet = build_review_packet(enriched)
    assert len(packet["events"]) == 1
    assert packet["events"][0]["references"][0]["fact_id"] == manifest.facts[0].fact_id
    assert packet["events"][0]["references"][0]["ingest_time"] == NOW.isoformat()
    assert validate_review(output(packet), packet)["citation_contract_valid"]
