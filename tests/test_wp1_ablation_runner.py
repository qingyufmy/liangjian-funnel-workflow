import json

from liangjian_funnel.evaluation.ablation.runner import run_experiment


def dataset():
    return {
        "trade_date": "2026-09-30", "source": {"path": "FIXTURE", "sha256": "0"*64},
        "coverage": {"status": "COMPLETE", "plans": [], "source_evidence_gaps": []},
        "export_manifest": {"lifecycle_count": 0},
        "plans": [{"plan_id": "p", "payload": {"symbol": "600001.SH", "strategy_profile": "TREND_MA5"},
                   "activated_at": "2026-09-30T09:31:00+08:00", "expires_at": "2026-09-30T15:00:00+08:00"}],
        "minute_archives": {}, "windows": [{"plan_id": "p", "decision_time": "2026-09-30T10:00:00+08:00",
            "observation_time": "2026-09-30T10:00:00+08:00", "bars": [], "market_context": {},
            "input_sha256": "1"*64, "recorded_action": "NO_ACTION", "recorded_reason": "DIFFERENT_REASON"}],
    }


def test_baseline_mismatch_blocks_counterfactual_returns_for_the_day(tmp_path, monkeypatch):
    def baseline(*args, **kwargs):
        return {"action": "BUY_SIGNAL", "state": "SIGNAL_READY", "reason_codes": ["FORMAL_REASON"],
                "met_conditions": [], "unmet_conditions": [], "ablation": {"status": "OK"}}
    def forbidden(*args, **kwargs):
        raise AssertionError("unmatched frozen baseline must not become a simulated return")
    monkeypatch.setattr("liangjian_funnel.evaluation.ablation.engine.evaluate_window", baseline)
    monkeypatch.setattr("liangjian_funnel.evaluation.ablation.outcomes.evaluate_outcome", forbidden)
    report = run_experiment([dataset()], output=tmp_path/"mismatch")
    assert len(report["baseline_action_mismatches"]) == 1
    assert len(report["baseline_reason_mismatches"]) == 1
    assert report["acceptance"]["REPLAY"] == "PENDING_EVIDENCE"
    assert report["day_audits"][0]["status"] == "BASELINE_MISMATCH"
    rows = json.loads((tmp_path/"mismatch"/"rows.json").read_text())
    assert all(row["outcome"] is None and row["ablation"]["status"] == "DATA_LIMITED" for row in rows)


def test_nonempty_position_lifecycle_rejects_empty_position_replay(tmp_path, monkeypatch):
    data = dataset()
    data["export_manifest"]["lifecycle_count"] = 1
    def forbidden(*args, **kwargs):
        raise AssertionError("position lifecycle day must not be replayed as an empty position")
    monkeypatch.setattr("liangjian_funnel.evaluation.ablation.engine.evaluate_window", forbidden)
    report = run_experiment([data], output=tmp_path/"position")
    assert report["windows_evaluated"] == 0
    assert report["day_audits"][0]["status"] == "POSITION_REPLAY_REQUIRES_FULL_LIFECYCLE"
    assert report["acceptance"]["REPLAY"] == "PENDING_EVIDENCE"


def test_incomplete_day_keeps_matched_baseline_but_cannot_create_returns(tmp_path, monkeypatch):
    data = dataset()
    data["coverage"] = {"status": "DATA_LIMITED", "source_evidence_gaps": [{"reason": "SOURCE_WINDOW_MISSING"}]}
    data["windows"][0].update(recorded_action="BUY_SIGNAL", recorded_reason="FORMAL_REASON")
    def baseline(*args, **kwargs):
        return {"action": "BUY_SIGNAL", "state": "SIGNAL_READY", "reason_codes": ["FORMAL_REASON"],
                "met_conditions": [], "unmet_conditions": [], "ablation": {"status": "OK"}}
    def forbidden(*args, **kwargs):
        raise AssertionError("incomplete day cannot contribute formal counterfactual returns")
    monkeypatch.setattr("liangjian_funnel.evaluation.ablation.engine.evaluate_window", baseline)
    monkeypatch.setattr("liangjian_funnel.evaluation.ablation.outcomes.evaluate_outcome", forbidden)
    report = run_experiment([data], output=tmp_path/"limited")
    assert report["baseline_action_mismatches"] == report["baseline_reason_mismatches"] == []
    assert report["day_audits"][0]["baseline_status"] == "MATCHED"
    assert report["day_audits"][0]["status"] == "INCOMPLETE_DAY_SOURCE_EVIDENCE"
    rows = json.loads((tmp_path/"limited"/"rows.json").read_text())
    assert len(rows) == 1 and rows[0]["outcome"] is None
    assert rows[0]["ablation"]["status"] == "DATA_LIMITED"
