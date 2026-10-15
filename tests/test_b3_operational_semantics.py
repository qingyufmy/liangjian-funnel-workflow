import copy

from liangjian_funnel.review.business_audit import business_audit
from liangjian_funnel.review.observability import timing_summary


def audit(rows, replay=None):
    return business_audit(events=[], plans=[], delivery_counts={"total": 0, "statuses": {}, "truncated": False},
                          operational=rows, replay=replay or {})


def test_stable_publication_and_recovery_do_not_claim_data_incidents():
    rows = [{"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "stable",
             "state_counts": {"OBSERVED_STABLE": 359}, "reason_counts": {}, "unreadable_files": 0},
            {"kind": "SOURCE_HEALTH_EVENT", "evidence_id": "recovered", "state": "RECOVERED",
             "reasons": ["OLD_FAILURE"]},
            {"kind": "SOURCE_HEALTH_EVENT", "evidence_id": "wait", "state": "WAITING_BAR_CLOSE"}]
    original = copy.deepcopy(rows)
    result = audit(rows)
    assert result["incident_candidates"] == result["gap_cause_evidence"] == []
    assert result["healthy_observations"]["state_counts"] == {"OBSERVED_STABLE": 359, "RECOVERED": 1, "WAITING_BAR_CLOSE": 1}
    assert result["unknown_observations"] == []
    assert rows == original


def test_mixed_publication_keeps_healthy_counts_and_only_proven_block_candidate():
    result = audit([{"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "mixed", "time": "now",
                     "state_counts": {"OBSERVED_STABLE": 8, "INPUT_UNAVAILABLE": 2},
                     "reason_counts": {"SOURCE_MISSING": 2}, "symbols": ["s"]}])
    assert result["healthy_observations"]["state_counts"] == {"OBSERVED_STABLE": 8}
    incident = result["incident_candidates"][0]
    assert incident["cause"] == "DATA_SOURCE" and incident["evidence_id"] == "mixed"
    assert incident["plan_ids"] == [] and incident["minute_range"] is None
    assert incident["gap_proven"] is False and result["missing_execution_records"] == []


def test_unknown_and_unreadable_are_pending_not_healthy_or_source_failure():
    result = audit([{"kind": "SOURCE_HEALTH_EVENT", "evidence_id": "novel", "state": "NOVEL"},
                    {"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "bad", "state_counts": {}, "unreadable_files": 1}])
    assert result["incident_candidates"] == []
    assert result["healthy_observations"]["observation_count"] == 0
    assert len(result["unknown_observations"]) == 2
    assert all(row["cause"] == "UNKNOWN_PENDING" for row in result["unknown_observations"])


def test_sigterm_exact_minute_attribution_retained_without_incident_implying_gap():
    rows = [{"kind": "JOB_TERMINATED", "evidence_id": "term", "run_id": "monitor-2026-10-15T02-01-00-001Z-x"}]
    result = audit(rows, {"trigger_windows": [
        {"classification": "MISSING_EXECUTION_RECORD", "minute_end": "2026-10-15T10:01:00+08:00", "plan_id": "p"},
        {"classification": "MISSING_EXECUTION_RECORD", "minute_end": "2026-10-15T10:02:00+08:00", "plan_id": "p"}]})
    assert [r["cause"] for r in result["missing_execution_records"]] == ["TASK_SIGTERM", "UNKNOWN"]
    assert result["incident_candidates"][0]["gap_proven"] is False
    assert audit(rows)["missing_execution_records"] == []


def test_parent_near_budget_is_strictly_over_50_seconds_not_python_wall():
    def row(parent, elapsed=1000):
        return {"phase_timings": dict.fromkeys(("startup", "fetch", "judge", "persist", "notify"), 0),
                "elapsed_ms": elapsed, "parent_duration_ms": parent}
    assert timing_summary([row(50000)])["budget_warnings"] == ["MONITOR_MINUTE_OVER_45S"]
    assert timing_summary([row(50001)])["budget_warnings"] == ["MONITOR_MINUTE_OVER_45S", "MONITOR_MINUTE_NEAR_BUDGET"]
    assert timing_summary([row(None, 55000)])["budget_warnings"] == ["MONITOR_MINUTE_OVER_45S"]


def test_persistent_pending_error_and_normal_pending_are_separate_unknown_stays_pending():
    rows = [{"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "normal-wait",
             "state_counts": {"PENDING_PUBLICATION": 2}, "reason_counts": {}},
            {"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "blocked-wait",
             "state_counts": {"PENDING_PUBLICATION": 1}, "reason_counts": {"MINUTE_PUBLICATION_UNCONFIRMED": 1}},
            {"kind": "SOURCE_HEALTH_EVENT", "evidence_id": "recovery", "state": "READY"},
            {"kind": "NODE_PROCESS_RESTART", "evidence_id": "restart"},
            {"kind": "JOB_FAILED", "evidence_id": "failed"},
            {"kind": "NOVEL", "evidence_id": "unknown"}]
    result = audit(rows)
    assert [r["evidence_id"] for r in result["incident_candidates"]] == ["blocked-wait", "restart", "failed"]
    assert result["healthy_observations"]["state_counts"] == {"PENDING_PUBLICATION": 2, "READY": 1}
    assert [r["evidence_id"] for r in result["unknown_observations"]] == ["unknown"]


def test_real_writer_normal_wait_reasons_in_mixed_publication_are_not_faults():
    # workflow writes WAITING_BAR_CLOSE with decision_error WAITING_BAR_CLOSE;
    # first bounded PENDING_PUBLICATION is not escalated until UNCONFIRMED.
    result = audit([{"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "mixed-real",
                     "state_counts": {"OBSERVED_STABLE": 8, "WAITING_BAR_CLOSE": 2, "PENDING_PUBLICATION": 2},
                     "reason_counts": {"WAITING_BAR_CLOSE": 2, "MINUTE_PUBLICATION_PENDING": 1,
                                       "MINUTE_PUBLICATION_UNCONFIRMED": 1}},
                    {"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "normal-real",
                     "state_counts": {"WAITING_BAR_CLOSE": 1}, "reason_counts": {"WAITING_BAR_CLOSE": 1}}])
    assert [r["evidence_id"] for r in result["incident_candidates"]] == ["mixed-real"]
    assert result["healthy_observations"]["state_counts"] == {"OBSERVED_STABLE": 8, "WAITING_BAR_CLOSE": 3, "PENDING_PUBLICATION": 1}
    assert result["unknown_observations"] == []


def test_malformed_publication_counts_become_unknown_not_a5_crash():
    result = audit([{"kind": "MINUTE_PUBLICATION_AUDIT", "evidence_id": "malformed",
                     "state_counts": ["bad"], "reason_counts": ["also-bad"]}])
    assert result["incident_candidates"] == []
    assert result["healthy_observations"]["observation_count"] == 0
    assert result["unknown_observations"][0]["cause"] == "UNKNOWN_PENDING"


def test_projection_keeps_all_original_operational_ids_and_bounded_references():
    from liangjian_funnel.review.daily import _model_fact_projection, _projection_evidence_ids
    rows = [{"kind": "SOURCE_HEALTH_EVENT", "evidence_id": f"source:{i}", "state": "BLOCKED"} for i in range(10)]
    facts = {"operational_evidence": rows, "business_audit": audit(rows)}
    original = copy.deepcopy(facts)
    projected = _model_fact_projection(facts)
    assert facts == original
    assert {r["evidence_id"] for r in rows} <= _projection_evidence_ids(projected)
    assert projected["business_audit"]["incident_candidates_reference"]["count"] == 10
    assert "incident_candidates" not in projected["business_audit"]
