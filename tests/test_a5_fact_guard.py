import copy
import json
from pathlib import Path

import pytest

from liangjian_funnel.review.daily import A5ReviewReport, _model_fact_projection, _enforce_verified_findings, _validate_evidence, _canonicalize_report_output
from liangjian_funnel.review.fact_guard import normalize_quality, reconcile_report, verification_totals
from liangjian_funnel.review.verification import _field_comparison

ROOT = Path(__file__).resolve().parents[1]


def test_collection_task_known_object_preserves_meaning_without_mutating_raw():
    from test_a5_daily_review import _report
    payload = copy.deepcopy(_report())
    task = {"task": "核对金额口径", "target": "A4", "priority": "MEDIUM"}
    payload["data_collection_tasks"] = [task]
    result = A5ReviewReport.model_validate(_canonicalize_report_output(payload))
    assert result.data_collection_tasks == ["【A4；优先级：中】核对金额口径"]
    assert payload["data_collection_tasks"] == [task]


@pytest.mark.parametrize("task", [{"task": "采集", "unknown": "不能丢失"},
    {"task": "采集", "priority": []}, {"task": "采集", "target": {}}, {"task": ""}])
def test_unknown_collection_task_shape_still_fails(task):
    from test_a5_daily_review import _report
    from pydantic import ValidationError
    payload = copy.deepcopy(_report())
    payload["data_collection_tasks"] = [task]
    with pytest.raises(ValidationError):
        A5ReviewReport.model_validate(_canonicalize_report_output(payload))


def test_archived_response_requires_identical_prompt_input_and_output(tmp_path):
    import hashlib
    import importlib.util
    from liangjian_funnel.pipeline.model_client import ModelCallResult
    spec = importlib.util.spec_from_file_location("rerun_a5_frozen", ROOT / "scripts/rerun_a5_frozen.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prompt = "frozen prompt"
    prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
    result = ModelCallResult(model="deepseek-v4-pro", output={"ok": True}, prompt_hash=prompt_hash,
        input_hash="input", latency_ms=100, attempts=1, thinking_variant="original")
    raw = {"model": result.model, "output": result.output, "prompt_hash": prompt_hash,
        "input_hash": "input", "output_hash": result.output_hash, "thinking_variant": "original"}
    path = tmp_path / "response.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    client = module.ArchivedResponseClient(path)
    kwargs = {"prompt_hash": prompt_hash, "input_hash": "input"}
    messages = [{"role": "system", "content": prompt}]
    assert client.complete(result.model, messages, **kwargs).attempts == 0
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        client.complete(result.model, messages, **(kwargs | {"input_hash": "other"}))
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        client.complete(result.model, [{"role": "system", "content": "different"}], **kwargs)
    raw["output"] = {"ok": False}
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        module.ArchivedResponseClient(path)


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


def test_source_outage_proposal_is_not_replaced_with_volume_mismatch_theory():
    from test_a5_daily_review import _report
    report = A5ReviewReport.model_validate(copy.deepcopy(_report()))
    from liangjian_funnel.review.daily import A5Proposal
    proposal = A5Proposal(proposal_id="tdx-recover", type="DATA_FIX", target="A4",
        hypothesis="通达信取数失败导致价格和成交量不能比较", evidence_ids=["METRICS:DAILY"],
        proposed_change="探查协议握手并验证节点返回真实行情", validation_method="同日只读节点请求",
        success_criteria="取得可核验分钟线", falsification_criteria="仍无行情", min_shadow_days=0,
        risk="不修改历史判断", automatic_production_change=False)
    report.improvement_proposals = [proposal]
    original = proposal.model_dump()
    reconcile_report(report, {"metrics": {}, "independent_verification": {"a4": {"status": "UNAVAILABLE", "plans": []}}})
    assert report.improvement_proposals[0].model_dump() == original


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
