import copy

from liangjian_funnel.review.daily import A5Defect, A5ReviewReport, _enforce_verified_findings
from test_a5_daily_review import _report


def test_volume_only_difference_is_not_reported_as_price_defect():
    report = A5ReviewReport.model_validate(_report())
    facts = {"independent_verification": {"a4": {"plans": [{
        "evidence_id": "A5V:A4:PLAN:volume", "cross_source_status": "MISMATCH",
        "cross_source_field_checks": {"CLOSE": {"status": "MATCH"},
                                      "VOLUME": {"status": "MISMATCH", "mismatch_count": 4}},
        "archived_tdx_field_checks": {"VOLUME": {"status": "MISMATCH", "mismatch_count": 4}},
    }]}}}
    original = copy.deepcopy(facts)
    _enforce_verified_findings(report, facts)
    finding = report.core_defects[0]
    assert "可比字段异源差异" in finding.problem
    assert "成交量1个计划" in finding.problem
    assert "价格超容差" not in finding.problem
    assert "成交量2" not in finding.problem  # Same plan, two sources, not two incidents.
    assert facts == original


def test_price_fields_are_separately_identified_without_treating_missing_amount_as_mismatch():
    report = A5ReviewReport.model_validate(_report())
    facts = {"independent_verification": {"a4": {"plans": [{
        "evidence_id": "A5V:A4:PLAN:price", "archived_tdx_status": "MISMATCH",
        "archived_tdx_field_checks": {"OPEN": {"status": "MISMATCH", "mismatch_count": 1},
                                      "AMOUNT": {"status": "DATA_LIMITED", "mismatch_count": 0}},
    }]}}}
    _enforce_verified_findings(report, facts)
    assert "开盘价1个计划" in report.core_defects[0].problem
    assert "成交金额" not in report.core_defects[0].problem


def test_legacy_summary_does_not_invent_field_identity_or_duplicate_old_generated_claim():
    report = A5ReviewReport.model_validate(_report())
    report.core_defects = [A5Defect(layer="A4", severity="MEDIUM", confidence="HIGH",
        problem="1个计划存在异源价格超容差差异；行情覆盖完整不等于数值一致。",
        evidence_ids=["A5V:A4:PLAN:legacy"])]
    facts = {"independent_verification": {"a4": {"plans": [{
        "evidence_id": "A5V:A4:PLAN:legacy", "cross_source_status": "MISMATCH",
    }]}}}
    _enforce_verified_findings(report, facts)
    _enforce_verified_findings(report, facts)
    assert len(report.core_defects) == 1
    assert "字段未细分" in report.core_defects[0].problem
    assert "价格超容差" not in report.core_defects[0].problem
