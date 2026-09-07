from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.deterministic import screen_a3


def _inputs():
    symbol = "001366.SZ"
    snapshot = {
        "DETERMINISTIC_RESEARCH_V2_ENABLED": True,
        "FACTOR_SNAPSHOT": {symbol: {"timeframes": {
            "monthly": {"closed": True, "state": "BULL"},
            "weekly": {"closed": True, "state": "BULL"},
            "daily": {"closed": True, "state": "BULL", "close": 10.5, "low": 10,
                      "moving_averages": {"ma5": 10.1, "ma10": 9.8, "ma20": 9.4, "ma60": 8.9},
                      "ma_slopes": {"ma5": .1, "ma10": .08, "ma20": .05}},
        }}},
        "PRICE_LEVELS": {symbol: {"trigger_zone": {"low": 10, "high": 10.2}, "invalidation": 9.6,
                                  "max_chase_price": 10.6, "first_resistance": 12}},
        "TRADABILITY_FLAGS": {symbol: {"tradable": True}},
        "KLINE_PATTERNS": {symbol: {"labels": ["PLATFORM_BREAKOUT"]}},
        "MARKET_EMOTION_SNAPSHOT": {"available": True, "emotion_cycle_stage": "ACCELERATION",
                                    "new_long_permission": "ALLOW_CORE"},
    }
    output = {"active_themes": [{"theme_id": "AGRICULTURE", "stage": "ACCELERATION"}],
              "focus_pool": [{"symbol": symbol, "primary_theme": "AGRICULTURE",
                              "market_role": "EMOTION_LEADER", "ladder_height": 2, "ladder_intact": True}]}
    return snapshot, output


def test_a3_joins_a2_theme_stage_without_mutating_upstream():
    snapshot, output = _inputs()
    original = deepcopy(output)
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == "ACCELERATION"
    assert decision["strategy_profile"] == "LEADER_INTRADAY"
    assert decision["status"] == "REVIEW_CANDIDATE"
    assert decision["strategy_facts"]["a2_theme_stage_binding"]["source_hash"]
    assert output == original


@pytest.mark.parametrize("themes", [[], [{"theme_id": "OTHER", "stage": "ACCELERATION"}],
    [{"theme_id": "AGRICULTURE", "stage": "UNKNOWN"}],
    [{"theme_id": "AGRICULTURE", "stage": "ACCELERATION"}, {"theme_id": "AGRICULTURE", "stage": "RETREAT"}]])
def test_missing_or_ambiguous_theme_does_not_borrow_market_emotion(themes):
    snapshot, output = _inputs()
    output["active_themes"] = themes
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == "UNKNOWN"
    assert decision["status"] == "DATA_GAP"
    assert not decision["sent_to_llm"]


@pytest.mark.parametrize("location", ["candidate", "context", "theme"])
def test_retreat_is_not_promoted_by_stage_handoff(location):
    snapshot, output = _inputs()
    if location == "candidate":
        output["focus_pool"][0]["theme_stage"] = "RETREAT"
    elif location == "context":
        snapshot["A2_BOTTLENECK_CONTEXT"] = {"001366.SZ": {"theme_stage": "RETREAT"}}
    else:
        output["active_themes"][0]["stage"] = "RETREAT"
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == "RETREAT"
    assert decision["status"] == "HARD_REJECT"
    assert not decision["sent_to_llm"]
