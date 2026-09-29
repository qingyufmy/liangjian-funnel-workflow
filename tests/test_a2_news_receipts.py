from copy import deepcopy
from types import SimpleNamespace
import json
import pytest

from liangjian_funnel.pipeline.a2_news_context import _hash
from liangjian_funnel.pipeline.a2_news_receipts import review_with_receipt
from liangjian_funnel.pipeline.a2_news_review import enrich_news_shadow
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config


def packet():
    value = {"events": [{"event_id": "e", "references": [{"fact_id": "f"}]}]}
    return {**value, "input_hash": _hash(value)}


class Client:
    def __init__(self, fail=False, invalid=False):
        self.calls = 0
        self.fail, self.invalid = fail, invalid

    def complete(self, *args, **kwargs):
        self.calls += 1
        if self.fail:
            raise TimeoutError()
        return SimpleNamespace(output={"reviews": [{"event_id": "e", "fact_ids": ["bad" if self.invalid else "f"],
            "stance": "UNCLEAR", "analysis": "尚待核实", "uncertainties": ["缺少公告"]}]})


def test_receipt_reuse_no_second_call(tmp_path):
    client = Client()
    first = review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    second = review_with_receipt(packet(), client, receipt_root=tmp_path)
    assert client.calls == 1
    assert not first["receipt_reused"] and second["receipt_reused"]


def test_timeout_claim_never_retries(tmp_path):
    client = Client(fail=True)
    with pytest.raises(TimeoutError):
        review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    with pytest.raises(ValueError, match="UNKNOWN_NO_RETRY"):
        review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    assert client.calls == 1


def test_rejected_response_persisted_no_repair_or_recall(tmp_path):
    client = Client(invalid=True)
    for _ in range(2):
        with pytest.raises(ValueError, match="CITATION_INVALID"):
            review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    assert client.calls == 1
    assert len(list(tmp_path.glob("*/response.json"))) == 1
    assert not list(tmp_path.glob("*/validated.json"))


def test_corrupt_receipt_never_calls_model(tmp_path):
    client = Client()
    review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    path = next(tmp_path.glob("*/response.json"))
    value = json.loads(path.read_text(encoding="utf-8"))
    value["output_hash"] = "wrong"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="CORRUPT"):
        review_with_receipt(packet(), client, receipt_root=tmp_path, allow_model_call=True)
    assert client.calls == 1


def test_customer_name_is_context_not_sector_catalyst():
    report = {"as_of": "2026-09-29T16:00:00+08:00", "events": [{"title": "金螳螂中标装修项目",
        "summary": "中标华泰证券研发中心装修工程", "symbols": []}]}
    result = enrich_news_shadow(report, load_rotation_theme_config())
    clue = next(c for c in result["events"][0]["theme_clues"] if c["theme_id"] == "SECURITIES")
    assert clue["mention_scope"] == "SUMMARY_CONTEXT_ONLY"
    assert not clue["theme_catalyst_confirmed"]
