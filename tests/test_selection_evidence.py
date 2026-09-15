from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.review.daily import _a2_projection, _a3_candidates, _model_fact_projection
from liangjian_funnel.review.selection_evidence import counterexample_selection_audit
from liangjian_funnel.review.verification import A5IndependentVerifier


def test_a3_rejected_strategy_and_unmet_predicate_are_not_lost():
    item = {"symbol": "601579.SH", "deterministic_strategy_profile": "TREND_MA5",
            "deterministic_eligibility": "WATCH", "deterministic_unmet_conditions": ["TREND_DAILY_PATH_CONFIRMED"],
            "deterministic_technical_evidence": {"daily_close": 23.41, "daily_moving_averages": {"ma5": 23.91}}}
    result = _a3_candidates({"stages": [{"stage": "A3", "output": {"rejected_candidates": [item]}}]})[0]
    assert result["strategy_profile"] == "TREND_MA5"
    assert result["technical_evidence"]["daily_close"] == 23.41
    audit = counterexample_selection_audit({}, result)
    assert "继续观察" in audit["explanation"]
    assert "未确认日线" in audit["explanation"]
    for condition, label in [("BOARD_NOT_HIGH_RISK_4_PLUS", "四板及以上"),
                              ("BOARD_NOT_FIRST_OBSERVATION_ONLY", "首板")]:
        explanation = counterexample_selection_audit({}, {"strategy_profile": "LEADER_INTRADAY",
            "unmet_conditions": [condition]})["explanation"]
        assert label in explanation and condition not in explanation


def test_a2_numeric_reason_survives_into_selected_counterexample_only():
    item = {"symbol": "688216.SH", "behavior_type_decision": {"required_facets": {
        "medium_term_trend": {"available": True, "met": False,
            "value": {"score": 46.0773, "threshold": 50, "source_factor": "trend_strength_proxy"}}}}}
    a2, _ = _a2_projection({"stages": [{"stage": "A2", "output": {"outside_rotation_pool": [item]}}]})
    audit = counterexample_selection_audit(a2["candidates"][0], None)
    assert "46.08" in audit["explanation"] and "不等于均线趋势" in audit["explanation"]
    facts = {"a2": a2, "independent_verification": {"counterexamples": [{"symbol": "688216.SH", "selection_audit": audit}]}}
    projected = _model_fact_projection(facts)
    assert projected["independent_verification"]["counterexamples"][0]["selection_audit"] == audit
    assert facts["a2"]["candidates"][0]["quant_gate_evidence"]


def test_a3_price_match_requires_same_day_final_bar_and_all_ma_fields():
    tz = ZoneInfo("Asia/Shanghai")
    day = datetime(2026, 9, 14, tzinfo=tz)
    daily = [{"timestamp": (day-timedelta(days=59-i)).isoformat(), "close": 10} for i in range(60)]
    payload = {"symbol": "600001.SH", "ma_analysis": {"daily": {"ma5": 10}},
               "stock_behavior_type": "TREND", "strategy_profile": "TREND_MA5"}
    def verify(stamp):
        return A5IndependentVerifier._verify_a3([{"plan_id": "p", "payload_json": payload}],
            {"600001.SH": daily}, {"600001.SH": {"bars": [{"bar_end": stamp.isoformat(), "close": 10}]}},
            day+timedelta(days=1, hours=15))["plans"][0]
    assert verify(day-timedelta(days=1)+timedelta(hours=15))["cross_source_price_status"] == "DATA_LIMITED"
    assert verify(day+timedelta(hours=14, minutes=55))["cross_source_price_status"] == "DATA_LIMITED"
    valid = verify(day+timedelta(hours=15))
    assert valid["cross_source_price_status"] == "MATCH"
    assert valid["formula_status"] == "DATA_LIMITED"
