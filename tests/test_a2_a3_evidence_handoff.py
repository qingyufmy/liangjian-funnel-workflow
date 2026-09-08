from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes
from liangjian_funnel.pipeline.research import _with_daily_emotion_overlay
from liangjian_funnel.pipeline.factors import FactorEngine
from liangjian_funnel.pipeline.a3_strategy import _macd
from liangjian_funnel.pipeline.deterministic import DeterministicGateResult, _market_core_route_result
from liangjian_funnel.workflow import _compact_factor


def inputs():
    link = dict(theme_id="CHEMICAL", node_id="chemical-core", taxonomy="INDUSTRY",
                taxonomy_code="881263.TI", taxonomy_name="农化制品",
                match_method="MATURE_THEME_REGISTRY_EXACT_NAME")
    output = {"active_research_pool": [], "taxonomy_links": [link]}
    snapshot = {
        "EASTMONEY_HOT100_SNAPSHOT": {"available": True, "trade_date": "2026-09-08",
                                    "records": [{"symbol": "000912.SZ", "rank": 1}]},
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
    assert row["research_route"] == "DAILY_EMOTION_OVERLAY"
    assert row["business_exposure_facts"] == []
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


def test_missing_membership_does_not_infer_from_stock_name():
    output, snapshot = inputs()
    snapshot["THS_INDUSTRY_MEMBERSHIP"] = {}
    result, _ = _with_daily_emotion_overlay(output, snapshot, {"000912.SZ"})
    assert result["active_research_pool"][0]["primary_theme"] == "UNMAPPED"
    route = _market_core_route_result(result["active_research_pool"][0], 90, 60, {}, {}, False, .65)
    assert "A1_THEME_MISSING" in route["missing_reason_codes"]
    assert route["eligible"] is False


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
