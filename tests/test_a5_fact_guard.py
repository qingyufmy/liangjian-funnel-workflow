import copy
import json
from pathlib import Path

import pytest

from liangjian_funnel.review.daily import (
    A5ReviewReport,
    _canonicalize_report_output,
    _enforce_verified_findings,
    _model_fact_projection,
    _projection_evidence_ids,
    _validate_evidence,
)
from liangjian_funnel.review.fact_guard import normalize_quality, reconcile_report, verification_totals
from liangjian_funnel.review.verification import _field_comparison

ROOT = Path(__file__).resolve().parents[1]


def test_frozen_legacy_data_block_counts_are_display_only():
    from liangjian_funnel.review.fact_guard import business_metrics
    facts = {'metrics': {'a4_effective_event_count': 23,
        'a4_effective_action_counts': {'DATA_BLOCK': 21, 'BUY_SIGNAL': 2}, 'a4_trade_signal_count': 2}}
    original = copy.deepcopy(facts)
    metrics = business_metrics(facts)
    assert metrics['a4_effective_event_count'] == 2
    assert metrics['a4_trade_signal_count'] == 2
    assert metrics['a4_effective_action_counts'] == {'BUY_SIGNAL': 2}
    assert facts == original


def test_frozen_response_revalidation_binds_facts_template_model_and_bytes(tmp_path):
    import hashlib
    import importlib.util
    from liangjian_funnel.pipeline.model_client import ModelCallResult
    from liangjian_funnel.review.daily import _canonical_hash
    spec = importlib.util.spec_from_file_location('rerun_frozen_facts', ROOT/'scripts/rerun_a5_frozen.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    facts = {'review_contract': {'model': 'test', 'prompt_sha256': 'template'}, 'value': 1}
    facts['input_hash'] = _canonical_hash(facts)
    result = ModelCallResult(model='test', output={'ok': True}, prompt_hash='original',
        input_hash=facts['input_hash'], latency_ms=0, attempts=0, thinking_variant='original')
    path = tmp_path/'response.json'
    path.write_text(json.dumps({'model': result.model, 'output': result.output, 'output_hash': result.output_hash,
        'prompt_hash': result.prompt_hash, 'input_hash': result.input_hash, 'thinking_variant': result.thinking_variant}), encoding='utf-8')
    client = module.ArchivedResponseClient(path, revalidate_facts=True)
    kwargs = {'facts': facts, 'model': 'test', 'template_hash': 'template', 'rendered_prompt_hash': 'reordered'}
    assert client.revalidate_frozen(**kwargs).output == {'ok': True}
    assert client.archive_validation_provenance['exact_prompt_bytes_reproduced'] is False
    for invalid in ({'model': 'different'}, {'template_hash': 'different'}, {'facts': facts | {'value': 2}}):
        with pytest.raises(ValueError, match='FACT_IDENTITY'):
            client.revalidate_frozen(**(kwargs | invalid))
    strict = module.ArchivedResponseClient(path)
    with pytest.raises(ValueError, match='FACT_IDENTITY'):
        strict.revalidate_frozen(**kwargs)
    path.write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError, match='FACT_IDENTITY'):
        client.revalidate_frozen(**kwargs)


@pytest.mark.parametrize('kind,extra', [
    ('SOURCE_HEALTH_EVENT', {'state': 'BLOCKED'}),
    ('POSITION_DATA_HEALTH_EVENT', {'state': 'BLOCKED'}),
    ('JOB_FAILED', {'job': 'close'}),
])
@pytest.mark.parametrize('count', [20, 22, 69])
def test_operational_citation_overflow_never_drops_full_evidence_or_fails_report(kind, extra, count):
    from test_a5_daily_review import _report
    report = A5ReviewReport.model_validate(_report())
    rows = [{'kind': kind, 'evidence_id': f'ENGINEERING:{i}', **extra} for i in range(count)]
    facts = {'operational_evidence': rows}
    original = copy.deepcopy(facts)
    _enforce_verified_findings(report, facts)
    A5ReviewReport.model_validate(report.model_dump())
    finding = report.core_defects[0]
    assert str(count) in finding.problem
    assert len(finding.evidence_ids) == min(count, 20)
    assert finding.evidence_ids[0] == 'ENGINEERING:0'
    assert finding.evidence_ids[-1] == f'ENGINEERING:{count-1}'
    if kind == "JOB_FAILED":
        assert finding.layer == "ORCHESTRATOR"
        assert "1个失败周期" in finding.problem
        assert f"{count}条失败或超时记录" in finding.problem
    assert facts == original


def test_collection_task_known_object_preserves_meaning_without_mutating_raw():
    from test_a5_daily_review import _report
    payload = copy.deepcopy(_report())
    task = {"task": "核对金额口径", "target": "A4", "priority": "MEDIUM"}
    payload["data_collection_tasks"] = [task]
    result = A5ReviewReport.model_validate(_canonicalize_report_output(payload))
    assert result.data_collection_tasks == ["【A4；优先级：中】核对金额口径"]
    assert payload["data_collection_tasks"] == [task]


def test_projection_aggregate_evidence_is_traceable_but_fabricated_id_is_rejected():
    from test_a5_daily_review import _report

    facts = {"operational_evidence": [
        {"kind": "JOB_FAILED", "job": "close", "reason": "TIMEOUT",
         "time": "2026-09-21T16:10:00+08:00", "evidence_id": "ENGINEERING:JOB:raw"},
    ]}
    original = copy.deepcopy(facts)
    projection = _model_fact_projection(facts)
    allowed_projection = _projection_evidence_ids(projection)
    aggregate_id = projection["operational_evidence"]["job_failure_groups"][0]["evidence_id"]
    assert aggregate_id in allowed_projection
    assert aggregate_id not in {"ENGINEERING:JOB:raw"}

    payload = copy.deepcopy(_report())
    payload["signal_reviews"] = []
    payload["a2_review"]["evidence_ids"] = [aggregate_id]
    report = A5ReviewReport.model_validate(payload)
    _validate_evidence(report, facts, allowed_projection_evidence=allowed_projection)

    report.a2_review.evidence_ids = ["ENGINEERING:JOB_GROUP:fabricated"]
    with pytest.raises(Exception, match="A5_OUTPUT_EVIDENCE_INVALID"):
        _validate_evidence(report, facts, allowed_projection_evidence=allowed_projection)
    assert facts == original


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


def test_missing_observations_are_grouped_by_global_minute_incident():
    stamp = "2026-09-21T09:33:00+08:00"
    facts = {"metrics": {"a3_plan_count": 2}, "independent_verification": {"a4": {"plans": [
        {"symbol": "000001.SZ", "expected_observation_minutes": 10,
         "recorded_observation_minutes": 9, "missing_observation_count": 1,
         "missing_observation_samples": [stamp]},
        {"symbol": "000002.SZ", "expected_observation_minutes": 10,
         "recorded_observation_minutes": 9, "missing_observation_count": 1,
         "missing_observation_samples": [stamp]},
    ]}}}
    totals = verification_totals(facts)
    assert totals["missing_observation_count"] == 2
    assert totals["missing_observation_incident_count"] == 1
    assert totals["missing_observation_incidents"][0]["affected_plan_count"] == 2


def test_reconciliation_appends_every_server_counterexample_missing_from_model():
    from test_a5_daily_review import _report
    report = A5ReviewReport.model_validate(copy.deepcopy(_report()))
    report.missed_opportunity_reviews = []
    facts = {"metrics": {}, "independent_verification": {"counterexamples": [
        {"evidence_id": "A5V:MISS:300110.SZ", "symbol": "300110.SZ", "name": "华仁药业",
         "theme_id": "HEALTH", "intraday_return": 0.2007, "performance_rank": 1,
         "drop_stage": "A1_NOT_ACTIVE", "selection_audit": {"explanation": "A1原时点处于观察池"}},
        {"evidence_id": "A5V:MISS:688112.SH", "symbol": "688112.SH", "name": "鼎阳科技",
         "theme_id": "ROBOT", "intraday_return": 0.2, "performance_rank": 2,
         "drop_stage": "A2_QUANT_FILTERED", "selection_audit": {"explanation": "未匹配当时轮动板块"}},
    ], "a4": {"plans": []}}}
    notes = reconcile_report(report, facts)
    assert [row.symbol for row in report.missed_opportunity_reviews] == ["300110.SZ", "688112.SH"]
    assert all(row.is_confirmed_defect is False for row in report.missed_opportunity_reviews)
    assert any("服务器按冻结事实补齐" in note for note in notes)


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
