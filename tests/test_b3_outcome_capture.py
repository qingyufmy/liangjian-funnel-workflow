"""Future decision capture must not depend on building a factor database."""
from datetime import datetime
from copy import deepcopy
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline import research
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.settings import Settings

NOW = datetime(2026, 10, 15, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
FOCUS, WATCH, INELIGIBLE, REJECT = "600000.SH", "600001.SH", "600002.SH", "600003.SH"


@pytest.fixture
def engine(tmp_path):
    settings = Settings.from_env({}, root=tmp_path)
    store = RuntimeStore(settings.state_db_path)
    return research.ResearchPipeline(settings, model_client=object(), runtime_store=store,
        output_dir=tmp_path / "audits", enable_feature_store=False, now=lambda: NOW)


def snapshot(name="frozen"):
    return research.FrozenInputSnapshot(name, {"MARKET_DATA_AS_OF": NOW.isoformat()}, as_of=NOW)


def gate(stage="A2_LOCAL_ROLE", symbols=(FOCUS, WATCH, INELIGIBLE, REJECT)):
    rows = tuple({"symbol": s, "status": "LOCAL_REJECTED" if s == REJECT else "REVIEW_CANDIDATE",
        "reason_codes": ["EXPLICIT_REJECT"] if s == REJECT else [],
        "all_failed_gates": [], "factor_payload": {"not_needed": "x" * 10000}}
        for s in symbols)
    return DeterministicGateResult(stage, rows, tuple(s for s in symbols if s != REJECT), (),
        (REJECT,) if REJECT in symbols else ())


def audit(stage="A2", status="VALIDATED", sid="frozen"):
    route = {"stock_behavior_type": "TREND", "route_permission": ["TREND_MA5"]}
    output = ({"focus_pool": [{"symbol": FOCUS, **route}],
        "watch_only_pool": [{"symbol": WATCH, **route}, {"symbol": INELIGIBLE, "stock_behavior_type": "UNRESOLVED"}],
        "rejected_candidates": [{"symbol": REJECT}]} if stage == "A2" else
        {"core_watch_pool": [{"symbol": WATCH}], "secondary_watch_pool": [],
         "rejected_candidates": [{"symbol": REJECT}]})
    return SimpleNamespace(stage=stage, status=status, snapshot_id=sid,
        symbols=(FOCUS,) if stage == "A2" else (WATCH,), output=output)


def record(engine, audits, *, run="r", lane="lane_1", snap=None):
    engine._record_lane_outcome_labels(run_id=run,
        lane=SimpleNamespace(lane=lane, stages=audits), snapshot=snap or snapshot(), config_hash="config")


def rows(engine, **kw):
    return list(engine.runtime_store.list_outcome_labels(**kw))


def test_disabled_feature_store_records_real_a2_a3_and_freezes_only_compact_gate(engine):
    original = gate()
    engine._persist_gate("r", "lane_1", original, snapshot())
    engine._persist_gate("r", "lane_1", gate("A3_LOCAL_TECHNICAL", (WATCH, REJECT)), snapshot())
    record(engine, (audit(), audit("A3")))
    assert engine.feature_store is None
    assert len(rows(engine, stage="A2")) == 4
    assert len(rows(engine, stage="A3")) == 2
    assert {r["decision"] for r in rows(engine, stage="A3")} == {"PASSED", "REJECTED"}
    assert "factor_payload" not in json.dumps(list(engine._outcome_gate_captures.values()))


@pytest.mark.parametrize("status", ["BLOCKED", "NOT_RUN_UPSTREAM_BLOCKED", "UNKNOWN", "BLOCKED_EVIDENCE_GAP"])
def test_failed_or_unrun_stage_never_becomes_rejections(engine, status):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    record(engine, (audit(status=status),))
    assert rows(engine) == []


def test_capture_is_bound_to_run_lane_and_snapshot_not_old_feature_generation(engine):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    engine._feature_run_id = "old-run"
    engine._feature_generation_id = "old-generation"
    record(engine, (audit(),), run="other")
    record(engine, (audit(),), lane="lane_2")
    record(engine, (audit(sid="other-snapshot"),))
    assert rows(engine) == []
    record(engine, (audit(),))
    assert {(r["run_id"], r["lane_id"]) for r in rows(engine)} == {("r", "lane_1")}


def test_historical_replay_does_not_write_outcome_labels(engine):
    engine._outcome_capture_historical_replay = True
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    record(engine, (audit(),))
    assert rows(engine) == []


def test_public_run_entry_resets_gate_identity_and_sets_replay_guard_before_any_input_or_model(engine, monkeypatch):
    engine._persist_gate("old", "lane_1", gate(), snapshot())
    assert engine._outcome_gate_captures
    class InputReached(Exception):
        pass
    def coerce(_):
        assert engine._outcome_capture_historical_replay is True
        assert engine._outcome_gate_captures == {}
        raise InputReached
    monkeypatch.setattr(research, "_coerce_snapshot", coerce)
    with pytest.raises(InputReached):
        engine.run(snapshot(), run_id="replay", historical_replay=True)
    assert rows(engine) == []


def test_optional_feature_store_is_not_a_label_source_and_old_a1_is_not_relabelled(engine):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    class ForbiddenFeatureRead:
        def stage_decisions(self, *args, **kwargs):
            raise AssertionError("must not read a previous generation")
    engine.feature_store = ForbiddenFeatureRead()
    record(engine, (audit("A1"), audit()))
    assert len(rows(engine, stage="A2")) == 4
    assert rows(engine, stage="A1") == []


def test_missing_gate_is_not_reconstructed_from_final_plan_or_reused_a1(engine):
    record(engine, (audit(), audit("A3"), audit("A1")))
    assert rows(engine) == []


def test_focus_watch_and_a3_eligibility_are_distinct_not_inferred_from_focus_symbols(engine):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    record(engine, (audit(),))
    by_symbol = {r["symbol"]: r for r in rows(engine)}
    assert by_symbol[WATCH]["decision"] == "PASSED"
    assert by_symbol[INELIGIBLE]["decision"] == "PASSED"
    for symbol, expected in ((FOCUS, (True, False, True)), (WATCH, (False, True, True)),
                             (INELIGIBLE, (False, True, False)), (REJECT, (False, False, False))):
        meta = json.loads(by_symbol[symbol]["metadata_json"])
        assert (meta["a2_focus"], meta["a2_watch_only"], meta["a3_eligible"]) == expected
    assert by_symbol[REJECT]["decision"] == "REJECTED"


def test_unknown_missing_final_partition_is_not_false_rejected_and_raw_gate_is_immutable(engine):
    raw = gate(symbols=(FOCUS, WATCH))
    engine._persist_gate("r", "lane_1", raw, snapshot())
    raw.decisions[0]["reason_codes"].append("LATE_MUTATION")
    final = audit()
    final.output["watch_only_pool"] = []
    record(engine, (final,))
    assert [r["symbol"] for r in rows(engine)] == [FOCUS]
    assert "LATE_MUTATION" not in json.dumps(rows(engine))
    reports = list(engine.output_dir.glob("outcome_capture/**/*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["unknown_symbols"] == [WATCH]
    assert report["coverage_status"] == "PARTIAL_UNKNOWN"


def test_degraded_a2_watch_partition_is_retained_but_does_not_claim_focus_or_model_approval(engine):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    item = audit(status="A2_REVIEW_DEGRADED")
    item.output["watch_only_pool"].extend(item.output.pop("focus_pool"))
    item.output["focus_pool"] = []
    item.symbols = ()
    record(engine, (item,))
    by_symbol = {r["symbol"]: r for r in rows(engine)}
    assert by_symbol[FOCUS]["decision"] == "PASSED"
    metadata = json.loads(by_symbol[FOCUS]["metadata_json"])
    assert metadata["a2_focus"] is False
    assert metadata["a2_watch_only"] is True
    assert metadata["stage_review_status"] == "A2_REVIEW_DEGRADED"


def test_audit_from_another_lane_is_not_adopted(engine):
    engine._persist_gate("r", "lane_1", gate(), snapshot())
    wrong_lane = audit()
    wrong_lane.lane = "lane_2"
    record(engine, (wrong_lane,))
    assert rows(engine) == []


def test_capture_does_not_modify_gate_or_frozen_data_or_model_output(engine):
    frozen, original_gate, final = snapshot(), gate(), audit()
    before = deepcopy((dict(frozen.data), original_gate.decisions, final.output))
    engine._persist_gate("r", "lane_1", original_gate, frozen)
    record(engine, (final,), snap=frozen)
    assert (dict(frozen.data), original_gate.decisions, final.output) == before


@pytest.mark.parametrize('failure_target', ['record_stage_decisions', 'atomic_write_json'])
def test_label_write_failure_keeps_actual_plan_publication(engine, tmp_path, monkeypatch, caplog, failure_target):
    import sqlite3
    from test_b3_publication_end_to_end import current_call, RUN, SYMBOL
    calls = []

    def fail(*args, **kwargs):
        calls.append(failure_target)
        raise OSError('private failure body must not enter audit or logs')

    def before_publish(lane, base, a3):
        actual_gate = DeterministicGateResult('A3_LOCAL_TECHNICAL',
            ({'symbol':SYMBOL, 'status':'REVIEW_CANDIDATE', 'reason_codes':[]},), (SYMBOL,), (), ())
        engine._persist_gate(RUN, lane.lane, actual_gate, a3)
        before = deepcopy(lane.as_dict())
        monkeypatch.setattr(research, failure_target, fail)
        engine._record_lane_outcome_labels(run_id=RUN, lane=lane, snapshot=base, config_hash='isolated')
        assert calls and lane.as_dict() == before

    db, _, _ = current_call(tmp_path, monkeypatch, before_publish=before_publish)
    with sqlite3.connect(db.path) as connection:
        assert connection.execute('SELECT status FROM workflow_runs WHERE run_id=?', (RUN,)).fetchone()[0] == 'PUBLISHED'
        assert connection.execute('SELECT count(*) FROM execution_plans').fetchone()[0] == 1
    failures = [r.getMessage() for r in caplog.records if 'LABEL_CAPTURE_FAILED' in r.getMessage()]
    assert len(failures) == 1 and 'private failure body' not in failures[0]
    if failure_target == 'record_stage_decisions':
        failure = json.loads(next(engine.output_dir.glob('outcome_capture/**/A3.json')).read_text())
        assert failure['status'] == 'LABEL_CAPTURE_FAILED' and failure['error_type'] == 'OSError'
