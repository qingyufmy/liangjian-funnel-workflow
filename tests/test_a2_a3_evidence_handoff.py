from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from test_deterministic_pipeline_v2 import (
    _complete_hot100_fixture, _complete_a2_factor_scores, _snapshot, NOW,
)

from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes
from liangjian_funnel.pipeline.research import _with_daily_emotion_overlay
from liangjian_funnel.pipeline.factors import FactorEngine
from liangjian_funnel.pipeline.a3_strategy import _macd
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult, _market_core_route_result, screen_a2
from liangjian_funnel.workflow import _compact_factor
import pytest


def inputs():
    link = dict(theme_id="CHEMICAL", node_id="chemical-core", taxonomy="INDUSTRY",
                taxonomy_code="881263.TI", taxonomy_name="农化制品",
                match_method="MATURE_THEME_REGISTRY_EXACT_NAME")
    output = {"active_research_pool": [{
        "symbol": "000912.SZ",
        "research_route": "HALF_YEAR_FUNDAMENTAL",
        "selection_basis": "HALF_YEAR_FUNDAMENTAL",
    }], "taxonomy_links": [link]}
    snapshot = {
        "snapshot_manifest": {"as_of": "2026-09-08T15:10:00+08:00"},
        "EASTMONEY_HOT100_SNAPSHOT": _complete_hot100_fixture([
            {"symbol": "000912.SZ", "rank": 1}], at=datetime(2026, 9, 8, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))),
        "THS_INDUSTRY_MEMBERSHIP": {"records": [{"symbol": "000912.SZ", "memberships": [
            {"industry_thscode": "881263.TI", "industry_name": "农化制品"}]}]},
    }
    return output, snapshot


def test_overlay_uses_frozen_theme_not_popularity_label_and_preserves_monthly():
    output, snapshot = inputs()
    original = deepcopy(output)
    result, _ = _with_daily_emotion_overlay(output, snapshot, {"000912.SZ"})
    row = result["active_research_pool"][0]
    assert row["primary_theme"] == "CHEMICAL"
    assert row["industry_chain_node"] == "chemical-core"
    assert row["research_route"] == "HALF_YEAR_FUNDAMENTAL"
    assert row["a1_pool_channels"] == ["DAILY_EMOTION"]
    assert row["emotion_theme_binding"]["source_hash"]
    assert row["emotion_theme_binding"]["confirms_theme_stage"] is False
    assert "theme_stage" not in row
    assert output == original


def test_model_alias_cannot_override_stable_registry_binding():
    output, snapshot = inputs()
    output["taxonomy_links"].append({**output["taxonomy_links"][0], "theme_id": "MODEL_ALIAS",
                                     "match_method": "MODEL_GENERATED"})
    assert bind_emotion_themes(output, snapshot, {"000912.SZ"})["000912.SZ"]["theme_id"] == "CHEMICAL"


def test_multiple_industry_bindings_are_explicit_not_cherry_picked():
    output, snapshot = inputs()
    output["taxonomy_links"].append({**output["taxonomy_links"][0], "theme_id": "SECOND"})
    a = bind_emotion_themes(output, snapshot, {"000912.SZ"})["000912.SZ"]
    output["taxonomy_links"].reverse()
    assert bind_emotion_themes(output, snapshot, {"000912.SZ"})["000912.SZ"] == a
    assert not a["resolved"] and a["reason_code"] == "EMOTION_THEME_AMBIGUOUS"
    assert len(a["matches"]) == 2


def test_daily_classification_uses_unactivated_registry_and_explicit_leaf_hierarchy():
    output,snapshot=inputs()
    snapshot['A1_MATURE_THEME_REGISTRY']={'enabled':True,'themes':[{
        'canonical_id':'CHEMICAL_NEW_MATERIALS','display_name':'化工新材料',
        'industry_names':['农化制品'],'concept_names':[],'activation_keywords':['化工']}]}
    snapshot['THS_INDUSTRY_CATALOG']={'records':[{'name':'农化制品','thscode':'881263.TI'}]}
    result=bind_emotion_themes({},snapshot,{'000912.SZ'})['000912.SZ']
    assert result['resolved'] and result['node_id']=='MTR:CHEMICAL_NEW_MATERIALS:CORE'
    parent={**output['taxonomy_links'][0],'theme_id':'PARENT','taxonomy_code':'881000.TI'}
    output['taxonomy_links']=[parent,{**parent,'theme_id':'LEAF','taxonomy_code':'884000.TI'}]
    snapshot.pop('A1_MATURE_THEME_REGISTRY')
    snapshot['THS_INDUSTRY_MEMBERSHIP']={'records':[{'symbol':'000912.SZ','memberships':[
        {'industry_thscode':'881000.TI'},{'industry_thscode':'884000.TI'}]}]}
    snapshot['snapshot_manifest']={'g0_selection':{'nodes':[{'industry_thscode':'884000.TI','parent_industry_thscode':'881000.TI'}]}}
    result=bind_emotion_themes(output,snapshot,{'000912.SZ'})['000912.SZ']
    assert result['theme_id']=='LEAF' and result['selection_basis']=='UNIQUE_LEAF_INDUSTRY'


def test_missing_membership_does_not_infer_from_stock_name():
    output, snapshot = inputs()
    snapshot["THS_INDUSTRY_MEMBERSHIP"] = {}
    result, _ = _with_daily_emotion_overlay(output, snapshot, {"000912.SZ"})
    assert result["active_research_pool"][0]["primary_theme"] == "UNMAPPED"
    route = _market_core_route_result(result["active_research_pool"][0], 90, 60, {}, {}, False, .65)
    assert "A1_THEME_MISSING" in route["missing_reason_codes"]
    assert route["eligible"] is False


@pytest.mark.parametrize("research_route", ["HALF_YEAR_FUNDAMENTAL", "DAILY_EMOTION_OVERLAY"])
def test_proven_ambiguous_attention_theme_is_selection_work_not_missing_data(research_route):
    output, snapshot = inputs()
    output["taxonomy_links"].append({**output["taxonomy_links"][0], "theme_id": "SECOND"})
    result, _ = _with_daily_emotion_overlay(output, snapshot, {"000912.SZ"})
    item = result["active_research_pool"][0]
    item["research_route"] = research_route
    assert item["emotion_theme_binding"]["requires_theme_selection"] is True
    assert item["emotion_theme_binding"]["research_resolved"] is True
    facts = {key: {"available": True, "score": 80} for key in
             ("breadth", "turnover_share", "leader_structure")}
    route = _market_core_route_result(item, 90, 60, facts, {}, False, .65)
    # The standalone helper leaves the overlay-only business exemption to
    # screen_a2, exactly as before. Neither route may call ambiguity absence.
    assert route["eligible"] is (research_route == "HALF_YEAR_FUNDAMENTAL")
    assert set(route["missing_reason_codes"]) == (
        {"A1_BUSINESS_EVIDENCE_MISSING"} if research_route == "DAILY_EMOTION_OVERLAY" else set())
    assert route["bottleneck_status"] == "NOT_REQUIRED_FOR_MARKET_CORE"
    assert "A2_EMOTION_THEME_SELECTION_REQUIRED" in route["diagnostic_reason_codes"]
    assert "A1_THEME_MISSING" not in route["missing_reason_codes"]
    assert "A1_CHAIN_NODE_MISSING" not in route["missing_reason_codes"]
    assert item["research_route"] == research_route
    assert not item["emotion_theme_binding"]["resolved"]  # no arbitrary theme selection


@pytest.mark.parametrize("defect", ["no_attention", "no_membership", "no_candidates", "no_market_facts"])
def test_monthly_attention_ambiguity_exception_keeps_real_gaps_closed(defect):
    output, snapshot = inputs()
    output["taxonomy_links"].append({**output["taxonomy_links"][0], "theme_id": "SECOND"})
    item = _with_daily_emotion_overlay(output, snapshot, {"000912.SZ"})[0]["active_research_pool"][0]
    facts = {key: {"available": True, "score": 80} for key in
             ("breadth", "turnover_share", "leader_structure")}
    if defect == "no_attention":
        item.pop("emotion_attention_eligible")
    elif defect == "no_membership":
        item["emotion_theme_binding"]["research_resolved"] = False
    elif defect == "no_candidates":
        item["emotion_theme_binding"]["candidate_routes"] = []
    else:
        facts = {}
    assert _market_core_route_result(item, 90, 60, facts, {}, False, .65)["eligible"] is False


@pytest.mark.parametrize("research_route", ["HALF_YEAR_FUNDAMENTAL", "DAILY_EMOTION_OVERLAY"])
def test_ambiguous_attention_reaches_a2_review_but_cannot_grant_execution(research_route):
    snapshot = _snapshot(1)
    symbol = snapshot["g0_symbols"][0]
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = _complete_hot100_fixture([
        {"symbol": symbol, "rank": 1}])
    snapshot["SELECTED_BOARD_SNAPSHOT"] = {"available": True, "by_symbol": {}}
    snapshot["MARKET_EMOTION_SNAPSHOT"] = {
        "available": True, "emotion_cycle_stage": "ICE_POINT", "new_long_permission": "NO_NEW_ENTRY"}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {"available": False, "by_symbol": {}}
    link = dict(theme_id="ROBOTICS_ADVANCED_MANUFACTURING", node_id="robot-core",
        taxonomy="CONCEPT", taxonomy_code="885517.TI", taxonomy_name="机器人概念",
        match_method="MATURE_THEME_REGISTRY_EXACT_NAME")
    snapshot["THS_CONCEPT_MEMBERSHIP"] = {"records": [{"symbol": symbol, "memberships": [
        {"concept_thscode": "885517.TI", "concept_name": "机器人概念"}]}]}
    snapshot["THS_INDUSTRY_MEMBERSHIP"] = {}
    facts = _complete_a2_factor_scores(90)
    facts["tier_structure"] = dict(score=90, available=True, availability_state="OBSERVED_VALUE",
        ladder_height=1, first_board_observed=True, event_source="HITHINK_LIMIT_UP_POOL")
    item = dict(symbol=symbol, candidate_id="a1:"+symbol, status="ACTIVE",
        primary_theme="ROBOTICS_ADVANCED_MANUFACTURING", industry_chain_node="robot-core",
        research_route=research_route, selection_basis=research_route,
        a2_factor_scores=facts, data_quality_score=90)
    upstream = {"active_research_pool": [item], "taxonomy_links": [link, {**link,
        "theme_id": "SECOND", "node_id": "second-core"}]}
    upstream, _ = _with_daily_emotion_overlay(upstream, snapshot, {symbol})
    original = deepcopy(upstream)
    gate = screen_a2(snapshot, upstream, minimum_identifiability_score=0, review_all_eligible=True)
    decision = next(r for r in gate.decisions if r["symbol"] == symbol)
    assert decision["status"] == "REVIEW_CANDIDATE"
    assert symbol in gate.review_symbols
    assert decision["stock_behavior_type"] == "EMOTION"
    assert decision["execution_permission"] == "BLOCKED"
    assert decision["research_only_reason"] == "A2_EMOTION_THEME_SELECTION_REQUIRED"
    assert decision["route_eligibility"]["MARKET_CORE"]["eligible"] is True
    assert decision["route_eligibility"]["SUPPLY_CHAIN_ALPHA"]["eligible"] is False
    assert "A2_DATA_GAP" not in decision["reason_codes"]
    assert upstream == original


def test_source_degradation_does_not_discard_healthy_trend_route():
    decision = {"status": "REVIEW_CANDIDATE", "data_sufficiency_state": "SUFFICIENT",
                "route_eligibility": {"MARKET_CORE": {"eligible": True}},
                "channel_source_health": {"emotion": {"available": False}, "trend": {"available": True}}}
    gate = DeterministicGateResult("A2_LOCAL_ROLE", (decision,), ("000912.SZ",), (), ())
    assert gate.summary["data_sufficiency_state"] == "DEGRADED"
    assert gate.review_symbols == ("000912.SZ",)
    assert gate.summary["channel_source_health"]["emotion"]["available"] is False


def test_macd_survives_full_factor_compaction_and_a3_without_minutes():
    cutoff = datetime(2026, 9, 8, 16, tzinfo=ZoneInfo("Asia/Shanghai"))
    bars = []
    for i in range(120):
        day = cutoff - timedelta(days=180-i)
        if day.weekday() >= 5:
            continue
        price = 10 + i * .03
        bars.append({"symbol": "000912.SZ", "date": day.date().isoformat(),
                     "open": price, "high": price + .2, "low": price - .2,
                     "close": price, "volume": 10000, "amount": price * 10000})
    factor = FactorEngine("000912.SZ").compute(daily_bars=bars, as_of=cutoff).model_dump(mode="json")
    compact = _compact_factor(factor)
    daily = compact["timeframes"]["daily"]
    assert daily["macd"]["available"] is True
    assert daily["macd"] == factor["timeframes"]["daily"]["macd"]
    assert daily["macd"]["input_hash"]
    assert all(value is not None for value in _macd(daily, compact).values())
    assert "bars" not in daily
    # An unfinished/future daily bar cannot alter closed-bar indicator evidence.
    future = {**bars[-1], "date": "2026-09-09", "close": 9999, "high": 9999}
    changed = FactorEngine("000912.SZ").compute(daily_bars=bars + [future], as_of=cutoff)
    assert changed.timeframes["daily"].macd == daily["macd"]
