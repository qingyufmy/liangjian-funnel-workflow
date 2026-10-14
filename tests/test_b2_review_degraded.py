from liangjian_funnel.pipeline.review_degraded import rotation_review_failed_twice


def test_two_rotation_failures_only():
    reason = ["A2_ROTATION_REVIEW_INVALID:X:UNKNOWN_DIRECTION"]
    assert rotation_review_failed_twice([reason, reason])
    assert not rotation_review_failed_twice([reason])
    assert not rotation_review_failed_twice([reason, ["MODEL_TIMEOUT"]])
    assert not rotation_review_failed_twice([reason, [*reason, "A2_PARTITION_MISSING"]])
    assert not rotation_review_failed_twice([[], reason])


def test_degraded_outcome_is_completed_but_not_model_validated():
    from liangjian_funnel.pipeline.outcomes import stage_outcome_from_legacy
    from liangjian_funnel.runtime.state import STAGE_COMPLETED_VALUES
    result=stage_outcome_from_legacy("A2_REVIEW_DEGRADED",stage="A2",counts={"input":30,"evaluated":30,"selected":30})
    assert result.quality_state.value=="DEGRADED"
    assert result.job_status.value=="SUCCEEDED"
    assert "A2_REVIEW_DEGRADED" in STAGE_COMPLETED_VALUES


def test_actual_downstream_review_preserves_failed_replies_and_returns_quantitative_watch(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    from datetime import datetime
    from liangjian_funnel.settings import Settings
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    from liangjian_funnel.pipeline.research.common import ResearchPipeline, StageAudit, FrozenInputSnapshot
    fixtures = Path(__file__).parent / "fixtures/a2_review_domain_20261013"
    records = [json.loads(p.read_bytes()) for p in sorted(fixtures.glob("attempt-*.json"))]
    history = [record["validation_reasons"] for record in records]
    original_reviews = [len(record["model_output"]["rotation_reviews"]) for record in records]
    symbol = "002693.SZ"
    row = dict(symbol=symbol, name="双成药业", status="REVIEW_CANDIDATE", role="TREND_CORE",
               theme_id="MEDICINE", node_id="MEDICINE_NODE", upstream_candidate_id="a1:002693",
               stock_behavior_type="TREND", route_permission=["TREND_MA5", "MA520_SWING"],
               route="MARKET_CORE", eligible_routes=["MARKET_CORE"], score=80,
               top_rotation_theme=False, ROTATION_GATE_EMPTY=True, trend_pre_rotation_eligible=True,
               reason_codes=[], data_sufficiency_state="SUFFICIENT", bottleneck_context={})
    gate = DeterministicGateResult("A2_LOCAL_ROLE", (row,), (symbol,), (), ())
    from liangjian_funnel.pipeline.research import _with_a2_bottleneck_context
    data={"STRICT_AGENT_RULES":True, "A2_POOL_TARGETS":{"pool_min":0,"pool_max":60}}
    snap = _with_a2_bottleneck_context(FrozenInputSnapshot("test",data,as_of=datetime.fromisoformat("2026-10-13T16:00+08:00")),gate)
    failed=StageAudit(lane="lane_1",model="offline",stage="A2",status="BLOCKED",snapshot_id=snap.snapshot_id,
                     prompt_hash="a"*64,input_hash="b"*64,output_hash=None,latency_ms=0,attempts=2,
                     thinking_variant="original-frozen",symbols=(),reason_codes=tuple(history[-1]),output=None,
                     diagnostics={"semantic_attempts":2,"validation_history":history})
    engine=ResearchPipeline(Settings.from_env(root=tmp_path),model_client=object(),output_dir=tmp_path/"audit",enable_feature_store=False)
    monkeypatch.setattr(engine,"_run_stage_with_checkpoint",lambda **kw:failed)
    audit=engine._run_v2_downstream_review(lane_id="lane_1",model="offline",stage="A2",snapshot=snap,
        upstream_output={"active_research_pool":[{"symbol":symbol,"company_name":"双成药业","theme_id":"MEDICINE","node_id":"MEDICINE_NODE","candidate_id":"a1:002693"}]},
        full_upstream_symbols={symbol},gate=gate,bundle=None,run_id="isolated-degraded")
    assert audit.status == "A2_REVIEW_DEGRADED", audit.reason_codes
    assert audit.output["focus_pool"] == []
    assert audit.output["watch_only_pool"][0]["A2_REVIEW_DEGRADED"] is True
    assert audit.diagnostics["model_review_passed"] is False
    assert audit.diagnostics["validation_history"] == history
    assert [len(record["model_output"]["rotation_reviews"]) for record in records] == original_reviews == [11,11]


def test_fallback_only_bypasses_board_not_technical_risk():
    from liangjian_funnel.pipeline.research.a3 import _a3_candidate_eligible
    row={"stock_behavior_type":"TREND","route_permission":["TREND_MA5"],"top_rotation_theme":False,
         "data_sufficiency_state":"SUFFICIENT", "a2_route":"MARKET_CORE"}
    assert not _a3_candidate_eligible(row,origin="WATCH_ONLY")
    row.update(ROTATION_GATE_EMPTY=True,trend_pre_rotation_eligible=True)
    assert _a3_candidate_eligible(row,origin="WATCH_ONLY")
    row["hard_reason_codes"]=["A3_HARD_RISK_EVENT"]
    assert not _a3_candidate_eligible(row,origin="WATCH_ONLY")
