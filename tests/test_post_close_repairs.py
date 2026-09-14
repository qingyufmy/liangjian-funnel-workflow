import copy
import json
import urllib.error
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.eastmoney_hot import reconcile_guba_ranks, EastmoneyHot100Error
from liangjian_funnel.review.audit_reader import read_audit_stage
from liangjian_funnel.runtime.lark import LarkNotifier


NOW = datetime(2026, 9, 14, 16, 50, tzinfo=ZoneInfo("Asia/Shanghai"))


def rank_fixture():
    key = "GUBA_TOP_REAL_TIME{2026-09-14}"
    rows = [{"SECURITY_CODE": f"{i:06}", "MARKET_SHORT_NAME": "SZ",
             "SECURITY_SHORT_NAME": f"股票{i}", key: i if i != 29 else 28} for i in range(1, 101)]
    original = {"rows": [{"sc": f"SZ{i:06}", "rk": i} for i in range(1, 101)],
                "dated_checks": [{"srcSecurityCode": f"SZ{i:06}", "rank": i,
                                  "calcTime": "2026-09-14 16:40:00"} for i in (1, 100)]}
    return {"code": 100, "data": {"result": {"dataList": rows}}}, original


def test_original_rank_restores_vendor_order_without_mutating_raw():
    raw, original = rank_fixture()
    frozen = copy.deepcopy(raw)
    result = reconcile_guba_ranks(raw, original, as_of=NOW, expected_trade_date=NOW.date())
    assert result["record_count"] == 100 and result["available"]
    assert [r["rank"] for r in result["records"]] == list(range(1, 101))
    assert result["screener_rank_values"]["000029.SZ"] == 28
    assert result["rank_source_url"].endswith("getAllCurrentList")
    assert raw == frozen


@pytest.mark.parametrize("defect", ["duplicate", "membership", "old_date", "future", "missing_check", "wrong_check"])
def test_original_rank_requires_complete_matching_dated_identities(defect):
    raw, original = rank_fixture()
    if defect == "duplicate": original["rows"][28]["rk"] = 28
    if defect == "membership": original["rows"][28]["sc"] = "SZ999999"
    if defect == "old_date": original["dated_checks"][0]["calcTime"] = "2026-09-11 16:40:00"
    if defect == "future": original["dated_checks"][0]["calcTime"] = "2026-09-14 17:40:00"
    if defect == "missing_check": original["dated_checks"].pop()
    if defect == "wrong_check": original["dated_checks"][0]["rank"] = 2
    with pytest.raises(EastmoneyHot100Error):
        reconcile_guba_ranks(raw, original, as_of=NOW, expected_trade_date=NOW.date())


def test_stream_audit_finds_stage_after_output_and_preserves_only_target(tmp_path):
    path = tmp_path / "audit.json"
    target = {"output": {"focus_pool": [{"symbol": "600001.SH", "value": 1.25}]}, "stage": "A2"}
    path.write_text(json.dumps({"stages": [{"output": {"blob": "x" * 200_000}, "stage": "A1"},
                                         target, {"stage": "A3", "output": {}}]}, sort_keys=True))
    assert read_audit_stage(path, "A2", max_stage_bytes=1000) == target
    assert read_audit_stage(path, "A9") == {}
    with pytest.raises(ValueError, match="FILE_LIMIT"):
        read_audit_stage(path, "A2", max_file_bytes=100)
    assert read_audit_stage(path, "A1", max_stage_bytes=1000) == {"stage": "A1", "output": {}}
    with pytest.raises(ValueError, match="STAGE_LIMIT"):
        read_audit_stage(path, "A2", max_stage_bytes=10)


def test_stream_a1_preserves_all_counterexample_fields_without_evidence_graph(tmp_path):
    from liangjian_funnel.review.daily import _a1_market_universe
    path = tmp_path / "audit.json"
    row = {"symbol": "600001.SH", "name": "测试", "primary_theme": "theme",
           "primary_theme_name": "方向", "core_thesis": ["盈利增长"], "bear_case": ["风险"],
           "evidence_graph": {"blob": "x" * 200_000}}
    original = {"stages": [{"stage": "A1", "output": {
        pool: [dict(row, symbol=f"60000{i}.SH")] for i, pool in enumerate(
            ("active_research_pool", "monitor_pool", "rejected_candidates"))}}]}
    path.write_text(json.dumps(original, sort_keys=True))
    streamed = {"stages": [read_audit_stage(path, "A1", max_stage_bytes=4000)]}
    assert _a1_market_universe(streamed) == _a1_market_universe(original)
    assert "evidence_graph" not in json.dumps(streamed)


def test_stream_audit_rejects_duplicate_stages(tmp_path):
    path = tmp_path / "audit.json"
    path.write_text(json.dumps({"stages": [{"stage": "A2"}, {"stage": "A2"}]}))
    with pytest.raises(ValueError, match="AMBIGUOUS"):
        read_audit_stage(path, "A2")


def test_stream_audit_rejects_truncated_file(tmp_path):
    path = tmp_path / "audit.json"
    path.write_text('{"stages":[{"stage":"A2"}')
    with pytest.raises(ValueError, match="JSON_INVALID"):
        read_audit_stage(path, "A2")


@pytest.mark.parametrize("error", [TimeoutError(), urllib.error.URLError("temporary")])
def test_lark_transient_network_failure_has_only_one_retry(error):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        raise error
    result = LarkNotifier("https://open.larksuite.com/open-apis/bot/v2/hook/test-only",
                          opener=opener, retry_delay_seconds=0).send("复盘", "测试")
    assert not result.ok and result.attempts == 2 and len(calls) == 2


def test_a5_strategy_summary_uses_frozen_plan_counts():
    from test_a5_daily_review import _report
    from liangjian_funnel.review.daily import A5ReviewReport
    from liangjian_funnel.review.fact_guard import reconcile_report
    raw = _report()
    raw["a3_review"]["summary"] = "32个520计划全部通过"
    report = A5ReviewReport.model_validate(raw)
    reconcile_report(report, {"metrics": {"a3_plan_count": 32, "a3_strategy_counts": {"TREND_MA5": 32}}})
    assert "趋势五日线32个" in report.a3_review.summary
    assert "520" not in report.a3_review.summary


def test_grouped_evidence_ids_round_trip_without_removing_candidates():
    from liangjian_funnel.review.daily import _model_fact_projection
    rows = [{"evidence_id": f"A2:OUTSIDE_ROTATION:{i:06}.SZ", "pool": "OUTSIDE_ROTATION",
             "symbol": f"{i:06}.SZ", "name": "名称", "score": i / 10,
             "selection_reasons": ["A2_ROLE_EVIDENCE_INSUFFICIENT"], "risk_reasons": []}
            for i in range(1600)]
    rows.append(dict(rows[0], evidence_id="nonstandard-id", pool="WATCH", symbol="600001.SH"))
    facts = {"a2": {"candidates": rows}}
    original = copy.deepcopy(facts)
    packed = _model_fact_projection(facts)["a2"]["candidates"]
    restored = []
    for group in packed["groups"]:
        for stock in group["stocks"]:
            row = {**group["common"], **stock}
            if group.get("derive_evidence_id"):
                row["evidence_id"] = f"A2:{row['pool']}:{row['symbol']}"
            restored.append(row)
    assert restored == rows
    assert facts == original
    assert len(json.dumps(packed)) < len(json.dumps(rows)) * .6


def test_compact_strings_round_trip_reserved_values_and_nested_evidence():
    from liangjian_funnel.review.context import pack_compact_strings
    value = [{"~3": "~0", "literal": "~~x", "code": "000538.SZ", "id": "plan:" + "abc" * 30,
              "nothing": None, "number": 3, "flag": False} for _ in range(100)]
    value.append({"literal": "~" * 30, "escape": "~not-a-number"})
    original = copy.deepcopy(value)
    packed = pack_compact_strings(value)
    def decode(node):
        if isinstance(node, str):
            if node.startswith("~~"): return node[1:]
            if node.startswith("~"): return packed["dictionary"][int(node[1:])]
        if isinstance(node, dict): return {key: decode(child) for key, child in node.items()}
        if isinstance(node, list): return [decode(child) for child in node]
        return node
    assert decode(packed["data"]) == original == value
    assert len(json.dumps(packed)) < len(json.dumps(value)) * .7


def test_a3_formula_success_cannot_become_cross_source_success():
    from test_a5_daily_review import _report
    from liangjian_funnel.review.daily import A5ReviewReport
    from liangjian_funnel.review.fact_guard import reconcile_report
    report = A5ReviewReport.model_validate(_report())
    report.a3_review.strengths = ["通达信核价100%通过"]
    report.a3_review.verdict = "HEALTHY"
    facts = {"independent_verification": {"a3": {"not_verified_fields": ["KDJ", "VOLUME"],
        "plans": [{"formula_status": "MATCH", "cross_source_price_status": "DATA_LIMITED",
                   "route_contract_match": True, "price_levels_valid": True,
                   "daily_macd_verification": {"formula_status": "MATCH", "input_hash_status": "MATCH"}}] * 32}}}
    reconcile_report(report, facts)
    assert report.a3_review.verdict == "DATA_LIMITED"
    assert "跨源收盘价格核验一致0/32份" in report.a3_review.strengths[-1]
    assert "100%" not in str(report.a3_review.strengths)
