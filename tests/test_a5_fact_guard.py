import copy
import json
from pathlib import Path

import pytest

from liangjian_funnel.review.daily import A5ReviewReport, _model_fact_projection, _enforce_verified_findings, _validate_evidence
from liangjian_funnel.review.fact_guard import normalize_quality, reconcile_report, verification_totals
from liangjian_funnel.review.verification import _field_comparison

ROOT = Path(__file__).resolve().parents[1]


def test_amount_limitation_keeps_comparison_scopes_separate():
    from test_a5_daily_review import _report
    payload = copy.deepcopy(_report())
    payload["a4_review"]["defects"] = ["金额字段全部数据受限，无法比较"]
    report = A5ReviewReport.model_validate(payload)
    facts = {"metrics": {}, "independent_verification": {"a4": {"plans": [{
        "expected_observation_minutes": 1, "recorded_observation_minutes": 1,
        "cross_source_field_checks": {"AMOUNT": {
            "compared_count": 0, "mismatch_count": 0, "not_comparable_count": 5280}},
        "archived_tdx_field_checks": {"AMOUNT": {
            "compared_count": 3360, "mismatch_count": 0, "not_comparable_count": 1920}},
    }]}}}
    reconcile_report(report, facts)
    A5ReviewReport.model_validate(report.model_dump())
    assert not any("金额字段全部" in item for item in report.a4_review.defects)
    assert any("两源比较不可比较：成交金额5280组" in item for item in report.a4_review.data_limitations)
    assert any("归档对通达信不可比较：成交金额1920组" in item for item in report.a4_review.data_limitations)
    assert verification_totals(facts)["fields"]["archived_tdx_field_checks:AMOUNT"]["compared_count"] == 3360


def test_fact_guard_preserves_real_exit_and_indicator_defects():
    from test_a5_daily_review import _report
    payload = copy.deepcopy(_report())
    payload["a4_review"]["defects"] = ["15分钟MACD未预热", "T+1退出状态不正确", "共5020分钟"]
    payload["core_defects"] = [{"layer": "A4", "severity": "HIGH", "confidence": "HIGH",
        "blocked_by_data": False, "problem": "15分钟MACD未预热", "evidence_ids": ["METRICS:DAILY"]}]
    report = A5ReviewReport.model_validate(payload)
    facts = {"metrics": {}, "independent_verification": {"a4": {"plans": [
        {"expected_observation_minutes": 1, "recorded_observation_minutes": 1}]}}}
    reconcile_report(report, facts)
    A5ReviewReport.model_validate(report.model_dump())
    assert "15分钟MACD未预热" in report.a4_review.defects
    assert "T+1退出状态不正确" in report.a4_review.defects
    assert "共5020分钟" not in report.a4_review.defects
    assert report.core_defects[0].problem == "15分钟MACD未预热"
    assert report.a4_review.verdict == "NEEDS_ATTENTION"


def test_status_reason_is_not_missing_component_and_history_not_reused():
    facts = {"data_quality": {"status": "DEGRADED", "missing_components": ["A5_INDEPENDENT_VERIFICATION_DEGRADED"]},
        "review_history": [{"review_id": "old", "evidence_id": "A5H:old", "defects": ["stale counts 330 336"]}]}
    original = copy.deepcopy(facts)
    projected = _model_fact_projection(facts)
    assert projected["data_quality"]["missing_components"] == []
    assert projected["data_quality"]["limitation_reasons"] == ["A5_INDEPENDENT_VERIFICATION_DEGRADED"]
    assert "stale counts" not in json.dumps(projected)
    assert facts == original


def test_volume_classification_does_not_clear_mismatch_or_change_tolerance():
    def bar(volume):
        return {"volume": volume, "volume_unit": "shares"}
    left = {"2026-09-10T09:31:00+08:00": bar(2000), "2026-09-10T10:00:00+08:00": bar(500),
        "2026-09-10T10:01:00+08:00": bar(10000)}
    right = {"2026-09-10T09:31:00+08:00": bar(1000), "2026-09-10T10:00:00+08:00": bar(600),
        "2026-09-10T10:01:00+08:00": bar(100)}
    result = _field_comparison(left, right)["VOLUME"]
    assert result["mismatch_count"] == 3 and result["status"] == "MISMATCH"
    assert result["difference_patterns"] == {"OPENING_MINUTE_BOUNDARY": 1,
        "DIFFERENCE_AT_MOST_ONE_LOT": 1, "RATIO_NEAR_100_NEEDS_UNIT_EVIDENCE": 1}


def test_partial_verification_does_not_claim_all_plan_coverage():
    facts = {"metrics": {"a3_plan_count": 2}, "independent_verification": {"a4": {"plans": [
        {"expected_observation_minutes": 10, "recorded_observation_minutes": 10}]}}}
    assert verification_totals(facts)["scope_verified"] is False


def test_live_report_reconciles_false_minutes_old_counts_and_theme_when_available():
    path = ROOT / "outputs/audits/session-20260910/ark-post-close-report.json"
    if not path.exists():
        pytest.skip("Frozen production fixture unavailable")
    original = json.loads(path.read_text(encoding="utf-8"))
    facts = original["facts"]
    report = A5ReviewReport.model_validate(original["report"])
    _enforce_verified_findings(report, facts)
    notes = reconcile_report(report, facts)
    report = A5ReviewReport.model_validate(report.model_dump())
    _validate_evidence(report, facts)
    assert verification_totals(facts)["expected_plan_observations"] == 5019
    assert "实际5019条，编排遗漏0条" in report.a4_review.summary
    assert "628/5280" in " ".join(report.a4_review.defects)
    assert "41/5280" in " ".join(report.a4_review.defects)
    assert "330" not in report.a4_review.summary and "336" not in report.a4_review.summary
    assert not any("组件" in p.hypothesis for p in report.improvement_proposals)
    assert not any("MACD" in q.question for q in report.unresolved_questions)
    assert next(r for r in report.missed_opportunity_reviews if r.symbol == "002204.SZ").theme != "NATIONAL_DEFENSE"
    assert notes
    assert original["report"]["a4_review"]["summary"] != report.a4_review.summary
