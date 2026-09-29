from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.deterministic import screen_a3
from liangjian_funnel.pipeline.research import _expand_a2_compact_output


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
    output = {"active_themes": [{
                  "theme_id": "AGRICULTURE", "stage": "ACCELERATION",
                  "stage_since": "2026-09-01",
                  "supporting_evidence": ["breadth rising"],
                  "contradicting_evidence": ["crowding rising"],
                  "source_refs": ["A2_THEME_METRICS:AGRICULTURE"],
              }],
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
        output["focus_pool"][0]["theme_stage_evidence"] = {
            "source_hash": "candidate-stage-hash", "as_of": "2026-09-16"
        }
    elif location == "context":
        snapshot["A2_BOTTLENECK_CONTEXT"] = {"001366.SZ": {
            "theme_stage": "RETREAT",
            "theme_stage_evidence": {
                "source_hash": "context-stage-hash", "as_of": "2026-09-16"
            },
        }}
    else:
        output["active_themes"][0]["stage"] = "RETREAT"
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == "RETREAT"
    assert decision["status"] == "HARD_REJECT"
    assert not decision["sent_to_llm"]


def test_theme_stage_without_theme_specific_evidence_is_data_gap():
    snapshot, output = _inputs()
    output["active_themes"][0] = {"theme_id": "AGRICULTURE", "stage": "CLIMAX"}
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == "UNKNOWN"
    assert decision["status"] == "DATA_GAP"
    binding = decision["strategy_facts"]["a2_theme_stage_binding"]
    assert binding["reason_code"] == "A3_THEME_STAGE_EVIDENCE_INSUFFICIENT"


@pytest.mark.parametrize("stage,expected", [("ACCELERATION", "REVIEW_CANDIDATE"), ("RETREAT", "HARD_REJECT")])
def test_real_compact_transport_keeps_dated_theme_evidence(stage, expected):
    snapshot, output = _inputs()
    snapshot["A2_MARKET_REFERENCE"] = {"market_trade_date": "2026-09-28"}
    snapshot["A2_BOTTLENECK_CONTEXT"] = {"001366.SZ": {"theme_id": "AGRICULTURE", "score": 80}}
    expanded = _expand_a2_compact_output({"theme_reviews": [{
        "theme_id": "AGRICULTURE", "stage": stage, "support_codes": ["BREADTH"],
        "risk_codes": ["CROWDING"],
    }]}, snapshot, {"001366.SZ"})
    output["active_themes"] = expanded["active_themes"]
    assert "stage_since" not in output["active_themes"][0]
    decision = screen_a3(snapshot, output).decisions[0]
    assert decision["theme_stage"] == stage
    assert decision["status"] == expected


@pytest.mark.parametrize("missing", ["A2_BOTTLENECK_CONTEXT", "A2_MARKET_REFERENCE"])
def test_compact_transport_cannot_invent_missing_source_or_date(missing):
    snapshot, output = _inputs()
    snapshot.update({"A2_MARKET_REFERENCE": {"market_trade_date": "2026-09-28"},
                     "A2_BOTTLENECK_CONTEXT": {"001366.SZ": {"theme_id": "AGRICULTURE"}}})
    snapshot.pop(missing)
    expanded = _expand_a2_compact_output({"theme_reviews": [{
        "theme_id": "AGRICULTURE", "stage": "ACCELERATION", "support_codes": ["BREADTH"],
        "risk_codes": ["CROWDING"],
    }]}, snapshot, {"001366.SZ"})
    output["active_themes"] = expanded["active_themes"]
    assert screen_a3(snapshot, output).decisions[0]["status"] == "DATA_GAP"
