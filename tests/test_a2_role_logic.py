from __future__ import annotations

from liangjian_funnel.pipeline.a2_role_logic import (
    EMOTION,
    LEADER_INTRADAY,
    MA520_SWING,
    TREND,
    TREND_MA5,
    UNRESOLVED,
    classify_a2_stock,
)


AS_OF = "2026-08-29T15:00:00+08:00"


def _fact(
    met: bool,
    value: object,
    source: str,
    *,
    reason: str = "point-in-time test fact",
) -> dict[str, object]:
    return {
        "available": True,
        "met": met,
        "value": value,
        "source_refs": [source],
        "as_of": AS_OF,
        "reason": reason,
    }


def test_emotion_dragon_is_routeable_only_to_leader_intraday() -> None:
    result = classify_a2_stock(
        symbol="600001.SH",
        name="题材龙头",
        as_of=AS_OF,
        evidence={
            "supply_chain_position": _fact(True, {"theme": "贵金属"}, "ths:theme"),
            "capital_flow": _fact(True, {"today_net_inflow": 1.2}, "ths:capital-flow"),
            "ladder_structure": _fact(True, {"board_num": 3, "theme_leader": True}, "hithink:ladder"),
            "crowding": _fact(False, {"percentile": 95}, "local:crowding", reason="too crowded"),
            "index_chain_resonance": _fact(True, {"theme": "贵金属"}, "local:theme-resonance"),
            "identifiability_liquidity": _fact(True, {"liquid": True}, "ths:quote"),
        },
    )

    assert result["stock_behavior_type"] == EMOTION
    assert result["market_role"] == "EMOTION_LEADER"
    assert result["route_permission"] == [LEADER_INTRADAY]
    assert result["data_gaps"] == []
    assert "A2_KNOWN_NEGATIVE_CROWDING" in result["known_negatives"]
    for item in result["evidence"].values():
        assert set(("available", "met", "value", "source_refs", "as_of", "reason")) <= set(item)


def test_trend_core_requires_medium_trend_relative_strength_and_industry_logic() -> None:
    result = classify_a2_stock(
        symbol="000001.SZ",
        name="趋势中军",
        as_of=AS_OF,
        evidence={
            "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
            "capital_flow": _fact(True, {"five_day_net_inflow": 2.0}, "ths:capital-flow"),
            # Ladder is not required for the trend route, but its gap remains
            # explicit rather than being turned into a negative observation.
            "ladder_structure": {"available": False, "reason": "provider timeout"},
            "crowding": _fact(True, {"percentile": 52}, "local:crowding"),
            "index_chain_resonance": _fact(
                True,
                {"medium_term_trend": True, "relative_strength": True},
                "local:trend-resonance",
            ),
            "identifiability_liquidity": _fact(True, {"liquid": True}, "ths:quote"),
        },
    )

    assert result["stock_behavior_type"] == TREND
    assert result["market_role"] == "TREND_CORE"
    assert result["route_permission"] == [TREND_MA5, MA520_SWING]
    assert "A2_DATA_GAP_LADDER_STRUCTURE" in result["data_gaps"]
    assert "A2_MEDIUM_TERM_TREND_CONFIRMED" in result["reason_codes"]


def test_confirmed_emotion_leader_takes_one_explicit_route_even_with_trend_evidence() -> None:
    result = classify_a2_stock(
        symbol="300001.SZ",
        as_of=AS_OF,
        evidence={
            "ladder_structure": _fact(True, {"board_num": 2, "theme_leader": True}, "hithink:ladder"),
            "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
            "index_chain_resonance": _fact(
                True,
                {"medium_term_trend": True, "relative_strength": True},
                "local:resonance",
            ),
        },
    )

    assert result["stock_behavior_type"] == EMOTION
    assert result["market_role"] == "EMOTION_LEADER"
    assert result["route_permission"] == [LEADER_INTRADAY]
    assert result["conflicts"] == []
    assert "A2_EMOTION_PRECEDENCE_OVER_TREND" in result["reason_codes"]


def test_first_board_pool_fact_enters_leader_candidate_route_without_confirmation() -> None:
    result = classify_a2_stock(
        symbol="600002.SH",
        name="首板观察",
        as_of=AS_OF,
        evidence={
            "ladder_structure": _fact(
                True,
                {
                    "board_num": 1,
                    "ladder_height": 1,
                    "first_board_observed": True,
                    "continuation_confirmed": False,
                    "event_source": "HITHINK_LIMIT_UP_POOL",
                },
                "HITHINK_LIMIT_UP_POOL",
                reason="first-board limit-up pool observation",
            ),
        },
    )

    assert result["stock_behavior_type"] == EMOTION
    assert result["market_role"] == "EMOTION_LEADER"
    assert result["route_permission"] == [LEADER_INTRADAY]
    assert result["decision_basis"]["first_board_observed"] is True
    assert "A2_EMOTION_FIRST_BOARD_OBSERVED" in result["reason_codes"]
    assert result["evidence"]["ladder_structure"]["source_refs"] == ["HITHINK_LIMIT_UP_POOL"]


def test_missing_data_is_a_gap_and_never_a_negative_or_route() -> None:
    result = classify_a2_stock(
        symbol="688001.SH",
        as_of=AS_OF,
        evidence={
            "ladder_structure": {"available": False, "reason": "not collected"},
            "index_chain_resonance": {
                "available": True,
                "met": True,
                "value": {"medium_term_trend": True},
                "source_refs": ["local:resonance"],
                "as_of": AS_OF,
            },
        },
    )

    assert result["stock_behavior_type"] == UNRESOLVED
    assert result["route_permission"] == []
    assert "A2_DATA_GAP_RELATIVE_STRENGTH" in result["data_gaps"]
    assert "A2_DATA_GAP_INDUSTRY_LOGIC" in result["data_gaps"]
    assert "A2_DATA_GAP_LADDER_STRUCTURE" in result["data_gaps"]
    assert "A2_KNOWN_NEGATIVE_LADDER_STRUCTURE" not in result["known_negatives"]
    assert result["decision_basis"]["scoring_used"] is False


def test_known_negative_required_fact_is_not_misreported_as_missing() -> None:
    result = classify_a2_stock(
        symbol="002001.SZ",
        as_of=AS_OF,
        evidence={
            "ladder_structure": _fact(False, {"board_num": 0}, "hithink:ladder", reason="no ladder event"),
            "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
            "index_chain_resonance": _fact(
                True,
                {"medium_term_trend": False, "relative_strength": True},
                "local:resonance",
                reason="daily trend below medium-term line",
            ),
        },
    )

    assert result["stock_behavior_type"] == UNRESOLVED
    assert result["route_permission"] == []
    assert "A2_KNOWN_NEGATIVE_LADDER_STRUCTURE" in result["known_negatives"]
    assert "A2_KNOWN_NEGATIVE_MEDIUM_TERM_TREND" in result["known_negatives"]
    assert "A2_DATA_GAP_LADDER_STRUCTURE" not in result["data_gaps"]
    assert "A2_DATA_GAP_MEDIUM_TERM_TREND" not in result["data_gaps"]
    assert "A2_ROLE_KNOWN_NEGATIVE" in result["reason_codes"]


def test_partial_trend_candidate_keeps_one_weak_facet_for_llm_review() -> None:
    result = classify_a2_stock(
        symbol="002002.SZ",
        as_of=AS_OF,
        trend_candidate=True,
        evidence={
            "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
            "index_chain_resonance": _fact(
                True,
                {"medium_term_trend": True, "relative_strength": False},
                "local:resonance",
                reason="relative strength is below confirmation line",
            ),
        },
    )

    assert result["stock_behavior_type"] == TREND
    assert result["decision_basis"]["partial_trend_qualified"] is True
    assert "A2_TREND_PARTIAL_CONFIRMATION" in result["reason_codes"]
    assert "A2_WEAK_RELATIVE_STRENGTH" in result["reason_codes"]
    assert "A2_KNOWN_NEGATIVE_RELATIVE_STRENGTH" in result["known_negatives"]


def test_partial_trend_candidate_is_opt_in_and_does_not_relax_emotion_rules() -> None:
    evidence = {
        "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
        "index_chain_resonance": _fact(
            True,
            {"medium_term_trend": True, "relative_strength": False},
            "local:resonance",
        ),
    }
    without_scope = classify_a2_stock(symbol="002003.SZ", as_of=AS_OF, evidence=evidence)
    with_emotion = classify_a2_stock(
        symbol="002003.SZ",
        as_of=AS_OF,
        trend_candidate=True,
        evidence={
            **evidence,
            "ladder_structure": _fact(True, {"board_num": 2, "theme_leader": True}, "hithink:ladder"),
        },
    )

    assert without_scope["stock_behavior_type"] == UNRESOLVED
    assert with_emotion["stock_behavior_type"] == EMOTION
    assert with_emotion["route_permission"] == [LEADER_INTRADAY]


def test_partial_trend_candidate_still_requires_medium_term_anchor() -> None:
    result = classify_a2_stock(
        symbol="002004.SZ",
        as_of=AS_OF,
        trend_candidate=True,
        evidence={
            "supply_chain_position": _fact(True, {"industry_logic": True}, "research:industry"),
            "index_chain_resonance": _fact(
                True,
                {"relative_strength": True},
                "local:resonance",
            ),
        },
    )

    assert result["stock_behavior_type"] == UNRESOLVED
    assert result["decision_basis"]["partial_trend_qualified"] is False
    assert "A2_DATA_GAP_MEDIUM_TERM_TREND" in result["data_gaps"]
