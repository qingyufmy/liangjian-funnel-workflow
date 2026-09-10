from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.review.context import A5ReviewError, pack_evidence, render_a5_prompt
from liangjian_funnel.review.daily import _model_fact_projection
from liangjian_funnel.review.daily import A5DailyReviewService, A5ReviewKind, build_a5_fact_snapshot
from liangjian_funnel.review.plan_scope import carryover_evidence, select_review_plans
from liangjian_funnel.runtime.scheduler import Scheduler
from liangjian_funnel.runtime.state import RuntimeStore

ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 9, 9, 11, 30, tzinfo=ZoneInfo("Asia/Shanghai"))


def unpack(value):
    if isinstance(value, dict):
        if value.get("encoding") == "a5-column-table/1":
            missing = {tuple(pair) for pair in value.get("missing_cells", [])}
            return [{key: unpack(row[j]) for j, key in enumerate(value["columns"]) if (i, j) not in missing}
                    for i, row in enumerate(value["rows"])]
        return {key: unpack(item) for key, item in value.items()}
    if isinstance(value, list):
        return [unpack(item) for item in value]
    return value


def plan(identity, **kwargs):
    return {"plan_id": identity, "symbol": "000001.SZ", "status": "PENDING_MORNING_REVIEW",
            "valid_from": None, "expires_at": "2026-09-09T15:00:00+08:00",
            "updated_at": "2026-09-08T19:00:00+08:00", "created_at": "2026-09-08T18:00:00+08:00",
            "payload_json": {"source_run_id": identity}, **kwargs}


def test_scope_excludes_retired_version_not_intraday_invalidation_or_unknown_timestamp():
    rows = [plan("old", status="INVALIDATED"), plan("new"),
            plan("intraday", status="INVALIDATED", updated_at="2026-09-09T09:32:00+08:00"),
            plan("observed", status="INVALIDATED"), plan("unknown", status="INVALIDATED", updated_at=None),
            plan("tomorrow", expires_at="2026-09-10T15:00:00+08:00"),
            plan("future", created_at="2026-09-09T16:00:00+08:00")]
    original = copy.deepcopy(rows)
    selected, carried, retired = select_review_plans(rows, cutoff=CUTOFF, observed_ids={"observed"}, carryover_ids=set())
    assert {r["plan_id"] for r in selected} == {"new", "intraday", "observed", "unknown"}
    assert carried == [] and [r["plan_id"] for r in retired] == ["old"]
    assert rows == original


def test_scope_retired_target_day_does_not_override_retirement_and_held_plan_is_retained():
    rows = [plan("old", status="INVALIDATED", payload_json={"target_trade_date": "2026-09-09"}),
            plan("held", status="INVALIDATED", expires_at="2026-09-08T15:00:00+08:00")]
    selected, carried, retired = select_review_plans(rows, cutoff=CUTOFF, observed_ids={"held"}, carryover_ids={"held"})
    assert selected == []
    assert [r["plan_id"] for r in carried] == ["held"]
    assert [r["plan_id"] for r in retired] == ["old"]


def test_carryover_does_not_leak_afternoon_exit_into_midday():
    row = {"lifecycle_id": "old-entry", "plan_id": "held", "status": "CLOSED", "remaining_qty": 0,
           "entry_time": "2026-09-08T13:01:00+08:00", "exit_time": "2026-09-09T14:00:00+08:00",
           "updated_at": "2026-09-09T14:00:01+08:00", "net_return": 0.1}
    noon = carryover_evidence([row], CUTOFF)[0]
    assert noon["status"] == "AS_OF_STATE_UNAVAILABLE"
    assert noon["exit_time"] is None and noon["remaining_qty"] is None and noon["net_return"] is None
    assert carryover_evidence([row], CUTOFF.replace(hour=15))[0]["remaining_qty"] == 0
    assert carryover_evidence([{**row, "exit_time": "2026-09-08T15:00:00+08:00"}], CUTOFF) == []


def test_pack_roundtrip_preserves_missing_null_nested_exceptions_and_order():
    values = [{"evidence_id": f"A5V:{i}", "very_long_repeated_field_name": i,
               "explicit_nullable_value": None, "status": "MISMATCH" if i == 37 else "MATCH",
               "nested": [{"repeated_nested_column": j, "extra": [None, False, 0]} for j in range(20)]}
              for i in range(100)]
    del values[5]["explicit_nullable_value"]
    original = copy.deepcopy(values)
    packed = pack_evidence(values)
    assert packed["encoding"] == "a5-column-table/1"
    assert unpack(packed) == values == original


def test_raw_signal_market_is_archived_not_sent_even_without_other_verifier_sections():
    facts = {"independent_verification": {"signal_market": {"bars": ["RAW_MINUTE_PATH"]}}}
    projected = _model_fact_projection(facts)
    assert "RAW_MINUTE_PATH" not in json.dumps(projected)
    assert facts["independent_verification"]["signal_market"]["bars"] == ["RAW_MINUTE_PATH"]


def test_archive_transport_index_is_summarized_but_failures_and_findings_remain():
    facts = {"independent_verification": {
        "market_data_evidence_archives": {"tdx_1m": {
            "600000.SH": {"relative_path": "DO_NOT_SEND_FILE_PATH", "sha256": "f"*64},
            "600001.SH": {"status": "ARCHIVE_FAILED"}, "600002.SH": None}},
        "a4": {"plans": [{"evidence_id":"A5V:A4:1", "discrepancy_class":"PRODUCTION_ARCHIVE_DIVERGENCE"}]}}}
    original = copy.deepcopy(facts)
    projected = _model_fact_projection(facts)
    summary = projected['independent_verification']['market_evidence_archive_summary']['sources']['tdx_1m']
    assert summary['requested_count'] == 3 and summary['archived_count'] == 1
    assert len(summary['failures']) == 2 and summary['failures'][0]['status'] == 'ARCHIVE_FAILED'
    assert 'DO_NOT_SEND_FILE_PATH' not in json.dumps(projected)
    assert projected['independent_verification']['a4'] == facts['independent_verification']['a4']
    assert facts == original


def test_compact_prompt_json_preserves_nested_values_without_indentation_cost():
    class Prompts:
        def render(self, filename, replacements):
            value = replacements['A5_FACT_SNAPSHOT']
            return value if isinstance(value, str) else json.dumps(value,ensure_ascii=False,indent=2)
    projection={'a4':{'checks':[{'id':str(i),'observations':{'value':i,'null':None}} for i in range(100)]}}
    prompt, diagnostics = render_a5_prompt(Prompts(),'test',projection)
    assert json.loads(prompt) == projection
    assert diagnostics['prompt_chars'] < diagnostics['unpacked_prompt_chars']


def test_key_dictionary_roundtrip_preserves_reserved_keys_and_values():
    from liangjian_funnel.review.context import pack_key_dictionary
    value={'k1':{'$a5_value':1,'normal':None},'rows':[{'reason_code':'k1','nullable':False}]*30}
    packed=pack_key_dictionary(value)
    def decode(node):
        if isinstance(node,dict):return {packed['keys'][int(k[1:])]:decode(v) for k,v in node.items()}
        if isinstance(node,list):return [decode(v) for v in node]
        return node
    assert decode(packed['data']) == value


def test_frozen_retry_rejects_wrong_day_or_hash_before_model_call(tmp_path):
    service=A5DailyReviewService(store=RuntimeStore(tmp_path/'runtime.db'),
        prompts=PromptRepository(ROOT/'prompts'),model_client=None,output_dir=tmp_path,lane_id='lane_1',model='deepseek')
    with pytest.raises(A5ReviewError,match='A5_FROZEN_FACT_IDENTITY_OR_HASH_MISMATCH'):
        service.run(review_kind=A5ReviewKind.MIDDAY,now=CUTOFF.replace(minute=35),
                    frozen_facts={'trade_date':'2026-09-08','input_hash':'bad'})


def test_large_synthetic_input_preserves_all_effective_events_and_error_details():
    events = [{"evidence_id": f"A4:E:{i}", "event_id": str(i), "plan_id": str(i),
               "effective": True, "action": "BUY_SIGNAL", "minute_end": CUTOFF.isoformat(),
               "strategy_reason_codes": ["TEST"], "unmet_conditions": []} for i in range(600)]
    facts = {"a4": {"events": events}, "independent_verification": {"counterexamples": [
        {"evidence_id": f"A5V:MISS:{i}", "symbol": str(i), "drop_stage": "A4_NO_EFFECTIVE_SIGNAL"} for i in range(20)]}}
    # Force packing without using an unrealistic giant single prose field.
    facts["a3"] = {"plans": [{"plan_id": str(i), "selection_reasons": ["evidence " * 10],
                             "daily_macd": {"dif": 0, "dea": None, "hist": -0.1}} for i in range(600)]}
    proj = _model_fact_projection(facts)
    prompt, diag = render_a5_prompt(PromptRepository(ROOT / "prompts"), "agent_5_daily_reviewer_v1.txt", proj)
    assert diag["prompt_chars"] <= 250000
    assert unpack(pack_evidence(proj)) == proj
    assert len(proj["a4"]["events"]) == 600
    assert all(row["evidence_id"] in prompt for row in events)
    assert all(row["evidence_id"] in prompt for row in facts["independent_verification"]["counterexamples"])


def test_budget_failure_has_safe_diagnostics_not_arbitrary_exception_text():
    proj = {"input_hash": "a" * 64, "a3": {"cannot_discard": "x" * 260000}}
    with pytest.raises(A5ReviewError) as caught:
        render_a5_prompt(PromptRepository(ROOT / "prompts"), "agent_5_daily_reviewer_v1.txt", proj)
    exc = caught.value
    exc.diagnostics["secret"] = "DO_NOT_LOG"
    exc.diagnostics["section_chars"]["DO_NOT_LOG"] = 1
    assert Scheduler._callback_reason(exc) == "A5_MODEL_CONTEXT_TOO_LARGE"
    diag = Scheduler._callback_diagnostics(exc)
    assert diag["prompt_chars"] > 250000 and diag["limit_chars"] == 250000
    assert diag["input_hash"] == "a" * 64
    assert "DO_NOT_LOG" not in json.dumps(diag)
    assert Scheduler._callback_reason(A5ReviewError("A5_OUTPUT_SCHEMA_INVALID")) == "A5_OUTPUT_SCHEMA_INVALID"


def test_fact_builder_passes_only_session_plans_to_verifier_and_keeps_prior_inventory(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path / "runtime.db")
    rows = [plan("old", status="INVALIDATED"), plan("new"),
            plan("invalidated_today", status="INVALIDATED", updated_at="2026-09-09T09:32:00+08:00"),
            plan("held", status="INVALIDATED", expires_at="2026-09-08T15:00:00+08:00")]
    monkeypatch.setattr(store, "list_execution_plans", lambda **kw: rows)
    carry = {"lifecycle_id": "previous-entry", "plan_id": "held", "status": "CLOSED", "remaining_qty": 0,
             "entry_time": "2026-09-08T13:01:00+08:00", "exit_time": "2026-09-09T09:31:00+08:00",
             "updated_at": "2026-09-09T09:31:01+08:00"}
    monkeypatch.setattr(store, "list_a4_signal_lifecycles", lambda **kw: [carry] if "status" in kw else [])

    class Verifier:
        def verify(self, **kw):
            assert [r["plan_id"] for r in kw["plan_rows"]] == ["new", "invalidated_today"]
            return {"status": "READY", "counterexamples": []}

    facts = build_a5_fact_snapshot(store, tmp_path, trade_date=CUTOFF.date(), cutoff_at=CUTOFF,
        review_kind=A5ReviewKind.MIDDAY, lane_id="lane_1", independent_verifier=Verifier())
    assert facts["metrics"]["a3_plan_count"] == 2
    assert facts["metrics"]["a5_independent_verification_status"] == "READY"
    assert facts["a3"]["plan_scope"]["retired_before_session_count"] == 1
    assert [p["plan_id"] for p in facts["a4"]["carryover_plans"]] == ["held"]
    assert facts["a4"]["carryover_lifecycles"][0]["exit_time"] == carry["exit_time"]
    assert "old" not in facts["source_run_ids"] and "held" not in facts["source_run_ids"]


def test_service_archives_budget_failure_and_never_calls_model(tmp_path, monkeypatch):
    facts = {"input_hash": "b" * 64, "a3": {"non_discardable_fact": "z" * 260000}}
    monkeypatch.setattr("liangjian_funnel.review.daily.build_a5_fact_snapshot", lambda *a, **kw: facts)

    class ForbiddenModel:
        def complete(self, *a, **kw):
            pytest.fail("Oversize input must not reach the model")

    store = RuntimeStore(tmp_path / "runtime.db")
    service = A5DailyReviewService(store=store, prompts=PromptRepository(ROOT / "prompts"),
        model_client=ForbiddenModel(), output_dir=tmp_path, lane_id="lane_1", model="deepseek")
    with pytest.raises(A5ReviewError, match="A5_MODEL_CONTEXT_TOO_LARGE"):
        service.run(review_kind=A5ReviewKind.MIDDAY, now=CUTOFF.replace(minute=35))
    artifacts = tmp_path / "a5/2026-09-09"
    assert len(list(artifacts.glob("*-facts.json"))) == 1
    context = json.loads(next(artifacts.glob("*-context.json")).read_text(encoding="utf-8"))
    assert context["reason_code"] == "A5_MODEL_CONTEXT_TOO_LARGE"
    assert store.list_a5_reviews() == ()


@pytest.mark.parametrize("kind", ["midday", "post-close"])
def test_frozen_production_inputs_when_available(kind):
    files = list((ROOT / "outputs/audits/a5-signal-20260909").glob(f"{kind}-*-facts.json"))
    if not files:
        pytest.skip("Local frozen production fixture not present")
    facts = json.loads(files[0].read_text(encoding="utf-8"))
    original = copy.deepcopy(facts)
    proj = _model_fact_projection(facts)
    _, diag = render_a5_prompt(PromptRepository(ROOT / "prompts"), "agent_5_daily_reviewer_v1.txt", proj)
    assert diag["unpacked_prompt_chars"] > 250000 and diag["prompt_chars"] < 210000
    assert unpack(pack_evidence(proj)) == proj
    cutoff = datetime.fromisoformat(facts["cutoff_at"])
    selected, carry, retired = select_review_plans(facts["a3"]["plans"], cutoff=cutoff,
        observed_ids={r["plan_id"] for r in facts["a4"]["events"]}, carryover_ids=set())
    assert len(selected) == 46 and len(retired) == 30 and carry == []
    assert sum(r["status"] == "INVALIDATED" for r in selected) == 8
    assert all(r["plan_id"] not in {x["plan_id"] for x in retired} for r in facts["a4"]["events"])
    assert facts == original
