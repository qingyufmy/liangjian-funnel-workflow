"""Pure Hot100 consumer defenses; no provider, model, DB or application run."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.research.a2 import _project_a2_bottleneck_context
from liangjian_funnel.pipeline.research.common import (
    _project_eastmoney_hot100, _project_prompt_value, _with_daily_emotion_overlay,
)
from test_deterministic_pipeline_v2 import _snapshot, _complete_a2_factor_scores, NOW


def records_hash(records):
    return hashlib.sha256(json.dumps(records, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def test_surrogate_record_is_invalid_evidence_not_consumer_crash():
    from liangjian_funnel.data.hot100_observation import observe_hot100
    source = full_source()
    source['records'][0]['name'] = '\ud800'
    observed = observe_hot100(source, decision_as_of=NOW)
    assert observed.validation_state == 'INVALID_SOURCE'
    assert observed.reason_code == 'HOT100_RECORDS_NOT_STRICT_JSON'
    assert observed.records == ()


def full_source(*, members=(), at=NOW):
    symbols = list(members)
    symbols += [f"{601000+i:06d}.SH" for i in range(100) if f"{601000+i:06d}.SH" not in symbols]
    records = [{"symbol": symbol, "rank": rank, "name": f"Fixture{rank}"}
               for rank, symbol in enumerate(symbols[:100], 1)]
    return {"schema_version": "eastmoney-guba-hot100/1.0.0",
            "source_id": "EASTMONEY_GUBA_POPULARITY_TOP100", "available": True,
            "reason_code": "OK", "trade_date": at.date().isoformat(), "as_of": at.isoformat(),
            "record_count": 100, "records": records, "content_hash": records_hash(records),
            "point_in_time": True}


def gate_input(source):
    snapshot = _snapshot(3)
    emotion, trend, _ = snapshot["g0_symbols"]
    snapshot["A2_SCORE_WEIGHTS"] = {name: 1.0 for name in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {"available": True, "by_symbol": {
        s: {"available": True, "capital_flow_score": 90} for s in snapshot["g0_symbols"]}}
    snapshot["MARKET_EMOTION_SNAPSHOT"] = {"available": True,
        "emotion_cycle_stage": "STARTUP", "new_long_permission": "PROBE_ONLY"}
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = source
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {"available": True, "by_symbol": {trend: [{
        "board_code": "801807", "strategy_theme_id": "theme-core", "board_name": "算力",
        "strength": 3238, "main_net_inflow_cny": 2_467_000_000,
        "selected_for_rotation": True, "primary_rank": 2}]}}
    rows = []
    for symbol in snapshot["g0_symbols"]:
        factors = _complete_a2_factor_scores(90)
        factors["tier_structure"] = {"score": 90 if symbol == emotion else 20,
            "available": True, "availability_state": "OBSERVED_VALUE" if symbol == emotion else "OBSERVED_ABSENT",
            "ladder_height": 1 if symbol == emotion else 0, "first_board_observed": symbol == emotion,
            "event_source": "HITHINK_LIMIT_UP_POOL" if symbol == emotion else "HITHINK_LIMIT_UP_LADDER"}
        factors["weekly_confirmation"] = {"score": 90, "available": True}
        rows.append({"symbol": symbol, "candidate_id": f"a1:{symbol}", "primary_theme": "theme-core",
            "industry_chain_node": "node-core", "business_exposure": {
                "revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
            "a2_factor_scores": factors, "data_quality_score": 90})
    return snapshot, {"active_research_pool": rows}


def decisions(source, *, overlay=False):
    snapshot, output = gate_input(source)
    if overlay:
        output["active_research_pool"][0].update(status="ACTIVE", selection_basis="DAILY_EMOTION_OVERLAY",
            research_route="DAILY_EMOTION_OVERLAY", emotion_attention_eligible=True,
            downstream_trade_eligible=True, business_exposure="Fixture emotion route", business_exposure_facts=[])
    result = screen_a2(snapshot, output, minimum_identifiability_score=0, review_all_eligible=True)
    return {row["symbol"]: row for row in result.decisions}


def invalid_source(case):
    source = full_source()
    if case == "false_empty": source.update(available=False, records=[], reason_code="HTTP_502")
    elif case == "false_residual": source.update(available=False, reason_code="HTTP_502")
    elif case == "empty": source["records"] = []
    elif case == "99": source["records"] = source["records"][:99]
    elif case == "duplicate": source["records"][99]["symbol"] = source["records"][0]["symbol"]
    elif case == "duplicate_rank": source["records"][99]["rank"] = 99
    elif case == "old": source["trade_date"] = "2026-08-26"
    elif case == "future": source["as_of"] = "2026-08-27T15:11:00+08:00"
    elif case == "wrong_hash": source["content_hash"] = "0" * 64
    elif case == "missing_hash": source.pop("content_hash")
    elif case == "naive": source["as_of"] = "2026-08-27T15:10:00"
    elif case == "wrong_count": source["record_count"] = 99
    elif case == "spoof_projection": source.update(records=source["records"][:10],
        projection_scope="TOP10_PLUS_BATCH_MATCHES", full_snapshot_validated=True)
    if case in {"duplicate", "duplicate_rank"}: source["content_hash"] = records_hash(source["records"])
    return source


@pytest.mark.parametrize("case", ["false_empty", "false_residual", "empty", "99", "duplicate",
    "duplicate_rank", "old", "future", "wrong_hash", "missing_hash", "naive", "wrong_count", "spoof_projection"])
def test_invalid_or_unavailable_full_source_never_creates_verified_not_in(case):
    by_symbol = decisions(invalid_source(case), overlay=True)
    row = by_symbol["600000.SH"]
    assert "A2_EMOTION_NOT_IN_EASTMONEY_HOT100" not in row["reason_codes"]
    assert "A2_EMOTION_NOT_IN_EASTMONEY_HOT100" not in row["daily_a1_member_reason_codes"]
    assert row["channel_source_health"]["emotion"]["available"] is False
    assert row["channel_source_health"]["emotion"]["absence_is_not_popularity_evidence"] is True
    assert row["emotion_core_eligible"] is False


def test_only_full_verified_current_source_can_prove_target_not_in():
    row = decisions(full_source(), overlay=True)["600000.SH"]
    assert "A2_EMOTION_NOT_IN_EASTMONEY_HOT100" in row["daily_a1_member_reason_codes"]
    assert row["channel_source_health"]["emotion"]["validation_state"] == "COMPLETE"


def test_source_failure_preserves_independent_trend_qualification_and_score():
    healthy = decisions(full_source(members=["600000.SH"]))["600001.SH"]
    failed = decisions(invalid_source("false_residual"))["600001.SH"]
    for key in ("trend_core_eligible", "score", "a2_pool_channel", "a2_factor_scores"):
        assert failed[key] == healthy[key]
    assert failed["trend_core_eligible"] is True
    assert failed["eastmoney_hot100"] is None


def test_missing_source_is_explicit_health_not_legacy_complete():
    snapshot, output = gate_input(None)
    snapshot.pop("EASTMONEY_HOT100_SNAPSHOT")
    result = screen_a2(snapshot, output, minimum_identifiability_score=0, review_all_eligible=True)
    assert all(r["channel_source_health"]["emotion"]["validation_state"] == "UNAVAILABLE" for r in result.decisions)


def test_compact_source_health_survives_reason_truncation():
    health = {"emotion": {"available": False, "validation_state": "UNAVAILABLE",
        "reason_code": "HTTP_502", "absence_is_not_popularity_evidence": True}}
    projected = _project_a2_bottleneck_context({"600000.SH": {"channel_source_health": health,
        "deterministic_reason_codes": ["R1", "R2", "R3", "A2_EMOTION_HOT100_UNAVAILABLE"]}}, {"600000.SH"})
    assert projected["600000.SH"]["channel_source_health"] == health
    assert len(projected["600000.SH"]["reason_codes"]) == 3


@pytest.mark.parametrize("case", ["false_residual", "empty", "99", "old", "wrong_hash", "spoof_projection"])
def test_overlay_and_prompt_projection_recheck_full_source(case):
    source = invalid_source(case)
    output = {"active_research_pool": [{"symbol": "601000.SH"}]}
    snapshot = {"snapshot_manifest": {"as_of": NOW.isoformat()}, "EASTMONEY_HOT100_SNAPSHOT": source}
    before = deepcopy((output, snapshot))
    result, summary = _with_daily_emotion_overlay(output, snapshot, {"601000.SH"})
    assert summary["available"] is False
    assert result == output
    projected = _project_prompt_value("EASTMONEY_HOT100_SNAPSHOT", source, {"601000.SH"}, snapshot_data=snapshot)
    assert projected["records"] == []
    assert projected["full_snapshot_validated"] is False
    assert projected["source_health"]["absence_is_not_popularity_evidence"] is True
    assert (output, snapshot) == before


def test_projection_is_bounded_view_of_revalidated_full_source_not_claimed_boolean():
    source = full_source()
    snapshot = {"snapshot_manifest": {"as_of": NOW.isoformat()}}
    projected = _project_prompt_value("EASTMONEY_HOT100_SNAPSHOT", source, {"601087.SH"}, snapshot_data=snapshot)
    assert projected["full_snapshot_validated"] is True
    assert projected["full_record_count"] == 100
    assert len(projected["records"]) == 11
    assert projected["content_hash"] == source["content_hash"]
    forged = deepcopy(projected)
    assert _project_prompt_value("EASTMONEY_HOT100_SNAPSHOT", forged, {"601087.SH"}, snapshot_data=snapshot)["full_snapshot_validated"] is False


def test_legacy_without_decision_clock_does_not_certify_full_source():
    source = full_source()
    projected = _project_eastmoney_hot100(source, {"601087.SH"})
    assert projected["full_snapshot_validated"] is False
    assert projected["source_health"]["validation_state"] == "DATA_LIMITED"


def test_pure_observation_hash_is_collector_records_hash_not_envelope_hash():
    from liangjian_funnel.data.hot100_observation import observe_hot100
    source = full_source()
    assert observe_hot100(source, decision_as_of=NOW).complete
    source["content_hash"] = records_hash(source)
    assert not observe_hot100(source, decision_as_of=NOW).complete


def test_no_target_membership_claim_on_incomplete_source():
    from liangjian_funnel.data.hot100_observation import observe_hot100
    observation = observe_hot100(invalid_source("99"), decision_as_of=NOW)
    assert observation.membership("601000.SH") == "UNKNOWN"
    assert observation.membership("600000.SH") == "UNKNOWN"


def test_leader_contract_does_not_read_hot100_or_bypass_ladder_hard_gate():
    from test_runtime_strategies import _base, _leader_bars
    from liangjian_funnel.runtime.strategies import StrategyProfile, evaluate_a4_plan
    base = _base(StrategyProfile.LEADER_INTRADAY.value, leader_context={
        "valid": True, "theme_stage": "IGNITION", "ladder_intact": True,
        "market_role": "LEADER", "board_count": 2})
    normal = evaluate_a4_plan(base, _leader_bars())
    failed = evaluate_a4_plan({**base, "EASTMONEY_HOT100_SNAPSHOT": invalid_source("false_empty")}, _leader_bars())
    assert failed == normal
    broken = deepcopy(base)
    broken["leader_context"].update(ladder_intact=False, ladder_broken=True)
    result = evaluate_a4_plan(broken, _leader_bars())
    assert result["action"] != "BUY_SIGNAL"


@pytest.mark.parametrize("name", ["agent_2_theme_sentiment_v2.txt",
    "agent_2_theme_sentiment_transport_v2.txt", "agent_2_transport_system_v1.txt"])
def test_all_actual_a2_prompts_state_unavailable_is_not_negative_attention(name):
    text = (Path(__file__).resolve().parents[1] / "prompts" / name).read_text(encoding="utf-8")
    assert "热榜不可用≠榜外≠热度低" in text
    assert "channel_source_health" in text


def actual_context_fixture():
    from liangjian_funnel.pipeline.research.a2 import _with_a2_bottleneck_context
    from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot, _sha256_json
    data, upstream = gate_input(invalid_source("false_residual"))
    gate = screen_a2(data, upstream, minimum_identifiability_score=0, review_all_eligible=True)
    frozen = FrozenInputSnapshot("hot100-fixture", data, _sha256_json(data), NOW)
    return gate, upstream, _with_a2_bottleneck_context(frozen, gate)


def test_actual_gate_context_compact_chain_keeps_source_health():
    gate, _, context = actual_context_fixture()
    expected = gate.decisions[0]["channel_source_health"]
    compact = _project_prompt_value("A2_BOTTLENECK_CONTEXT", context.data["A2_BOTTLENECK_CONTEXT"],
                                  {"600000.SH"}, snapshot_data=context.data)
    assert compact["600000.SH"]["channel_source_health"] == expected


def test_actual_gate_partition_and_canonical_chain_preserve_server_health():
    from liangjian_funnel.pipeline.research.common import _gate_item_from_decision, _canonicalize_stage_lineage
    gate, upstream, context = actual_context_fixture()
    row = gate.decisions[0]
    projected = _gate_item_from_decision(row, "A2", "WATCH_ONLY")
    assert projected["channel_source_health"] == row["channel_source_health"]
    canonical, _ = _canonicalize_stage_lineage({"watch_only_pool": [{
        "symbol": row["symbol"], "channel_source_health": {"emotion": {"available": True}}}]},
        "A2", upstream, context.data)
    assert canonical["watch_only_pool"][0]["channel_source_health"] == row["channel_source_health"]


def test_actual_outside_rotation_partition_explains_missing_source_without_upgrade():
    from liangjian_funnel.pipeline.research.common import _gate_outside_rotation_items
    gate, _, _ = actual_context_fixture()
    rows = _gate_outside_rotation_items(gate)
    assert rows
    original = {r["symbol"]: r for r in gate.decisions}
    for row in rows:
        assert row["status"] == "OUTSIDE_ROTATION"
        assert row["channel_source_health"] == original[row["symbol"]]["channel_source_health"]
        assert row["emotion_core_eligible"] is False
        assert "A2_EMOTION_NOT_IN_EASTMONEY_HOT100" not in row["reason_codes"]


@pytest.mark.parametrize("case", ["false_residual", "99", "old", "wrong_hash", "missing_dates", "complete"])
def test_actual_offline_prefilter_never_restores_unverified_hot_members(case):
    import runpy
    from liangjian_funnel.pipeline.research.common import _sha256_json
    source = full_source(members=["600000.SH"])
    if case == "false_residual": source.update(available=False, reason_code="HTTP_502")
    elif case == "99": source["records"] = source["records"][:99]
    elif case == "old": source["trade_date"] = "2026-08-26"
    elif case == "wrong_hash": source["content_hash"] = "0" * 64
    elif case == "missing_dates":
        source.pop("as_of")
        source.pop("trade_date")
    data, output = gate_input(source)
    snapshot = {"snapshot_id": "hot100-fixture", "snapshot_hash": _sha256_json(data),
                "as_of": NOW.isoformat(), "data": data}
    lane = {"stages": [{"stage": "A1", "snapshot_id": snapshot["snapshot_id"],
                        "status": "VALIDATED", "output": output, "output_hash": _sha256_json(output)}]}
    audit = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts" / "audit_disclosure_batch_offline.py"))
    result = audit["audit_projected_batch"](snapshot, lane)
    expected = sorted(r["symbol"] for r in source["records"]) if case == "complete" else []
    assert result["prefilter"]["source_sets"]["hot100"] == expected
    assert result["hot100_source_health"]["available"] is (case == "complete")
    assert result["changes_query_scope"] is False and result["execution_authority"] is False


def test_actual_auction_optional_hot_failure_still_keeps_required_source_gates():
    from types import SimpleNamespace
    from liangjian_funnel.pipeline.research.common import FrozenInputSnapshot, _sha256_json
    from liangjian_funnel.runtime.auction_base import project_auction_delta
    from liangjian_funnel.workflow import WorkflowError
    data, upstream = gate_input(full_source(members=["600000.SH"]))
    base = SimpleNamespace(snapshot=FrozenInputSnapshot("auction-fixture", data, _sha256_json(data), NOW))
    boards = {**data["SELECTED_BOARD_SNAPSHOT"], "trade_date": NOW.date().isoformat()}
    quotes = {"available": True, "trade_date": NOW.date().isoformat()}
    projected = project_auction_delta(base, hot=invalid_source("false_residual"),
                                      boards=boards, quotes=quotes, observed_at=NOW)
    rows = screen_a2(projected, upstream, minimum_identifiability_score=0, review_all_eligible=True).decisions
    assert all(not r["channel_source_health"]["emotion"]["available"] for r in rows)
    assert any(r["trend_core_eligible"] for r in rows)
    with pytest.raises(WorkflowError, match="AUCTION_CURRENT_ROTATION_UNAVAILABLE"):
        project_auction_delta(base, hot=invalid_source("false_empty"), boards={"available": False},
                              quotes=quotes, observed_at=NOW)
    with pytest.raises(WorkflowError, match="AUCTION_QUOTE_COVERAGE_INCOMPLETE"):
        project_auction_delta(base, hot=invalid_source("false_empty"), boards=boards,
                              quotes={"available": False}, observed_at=NOW)
