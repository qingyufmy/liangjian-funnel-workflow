from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.a2_features import build_a2_feature_snapshot
from liangjian_funnel.pipeline.a2_features import _percentiles
from liangjian_funnel.pipeline.deterministic import screen_a2


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _bars(growth: float) -> list[dict]:
    return [
        {
            "date_ms": int((NOW - timedelta(days=30 - index)).timestamp() * 1000),
            "close_price": 10 * (1 + growth) ** index,
        }
        for index in range(31)
    ]


def _membership(kind: str) -> dict:
    code_key = "industry_thscode" if kind == "industry" else "concept_thscode"
    name_key = "industry_name" if kind == "industry" else "concept_name"
    return {
        "available": True,
        "records": [
            {"thscode": symbol, "memberships": [{code_key: "881001.TI", name_key: "主线"}]}
            for symbol in ("600001.SH", "000002.SZ", "300003.SZ")
        ],
    }


def test_a2_features_materialize_tier_leader_and_chain_without_heat_as_leader() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[
            {"symbol": "600001.SH", "amount": 1000},
            {"symbol": "000002.SZ", "amount": 500},
            {"symbol": "300003.SZ", "amount": 100},
        ],
        daily_bars={"600001.SH": _bars(0.02), "000002.SZ": _bars(0.01), "300003.SZ": _bars(-0.01)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={
            "records": [{
                "date": "2026-08-29",
                "boards": {"two_board": [{"thscode": "600001.SH", "board_num": 2}]},
            }],
        },
        dragon_tiger_snapshot={"records": [{"thscode": "600001.SH"}]},
        attention_snapshot={"records": [{"thscode": "000002.SZ"}]},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={
            "available": True,
            "source_id": "TEST_VENDOR",
            "provider_method": "VENDOR_DERIVED",
            "by_symbol": {
                "600001.SH": {"available": True, "capital_flow_score": 90, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
                "000002.SZ": {"available": True, "capital_flow_score": 50, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
                "300003.SZ": {"available": True, "capital_flow_score": 10, "availability_state": "OBSERVED_VALUE", "reason_code": "OK"},
            },
        },
        as_of=NOW,
    )

    assert snapshot["available"] is True
    first = snapshot["by_symbol"]["600001.SH"]
    assert first["tier"] == "T2"
    assert first["leader_role"] == "EMOTION_LEADER"
    assert first["factors"]["index_chain_resonance"]["available"] is True
    # Attention is only a bounded confirmation.  It cannot overwrite the
    # cross-sectional role with an artificial leader label.
    assert snapshot["by_symbol"]["000002.SZ"]["leader_role"] != "EMOTION_LEADER"


def test_missing_capital_flow_stays_unavailable_while_other_factors_remain_auditable() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.01)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED"},
        as_of=NOW,
    )

    row = snapshot["by_symbol"]["600001.SH"]
    assert row["factors"]["capital_flow"]["available"] is False
    assert row["factors"]["capital_flow"]["score"] is None
    assert row["factors"]["tier_structure"]["available"] is True
    assert snapshot["capital_flow_available"] is False


def test_cross_section_percentiles_average_ties() -> None:
    scores = _percentiles({"600001.SH": 1.0, "000002.SZ": 1.0, "300003.SZ": 2.0})

    assert scores["600001.SH"] == scores["000002.SZ"] == 25.0
    assert scores["300003.SZ"] == 100.0


def test_deterministic_a2_consumes_materialized_real_factor_projection() -> None:
    capital = {
        "available": True,
        "source_id": "TEST_VENDOR_DERIVED",
        "provider_method": "VENDOR_DERIVED",
        "by_symbol": {
            "600001.SH": {
                "available": True,
                "capital_flow_score": 90,
                "availability_state": "OBSERVED_VALUE",
                "reason_code": "OK",
            },
        },
    }
    factors = build_a2_feature_snapshot(
        candidates=[{"symbol": "600001.SH", "amount": 1000}],
        daily_bars={"600001.SH": _bars(0.02)},
        industry_membership=_membership("industry"),
        concept_membership=None,
        ladder_snapshot={"records": []},
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot=capital,
        as_of=NOW,
    )
    snapshot = {
        "CAPITAL_FLOW_SNAPSHOT": capital,
        "A2_FACTOR_SNAPSHOT": factors,
        "TIER_STRUCTURE_SNAPSHOT": factors,
        "A2_THEME_METRICS": factors["theme_metrics"],
        "g0_candidates": [{"symbol": "600001.SH", "amount": 1000, "change_ratio_pct": 2}],
    }
    a1 = {
        "active_research_pool": [{
            "symbol": "600001.SH",
            "candidate_id": "a1:600001.SH",
            "primary_theme": "theme-main",
            "structural_score": 85,
            "data_quality_score": 90,
            "source_refs": ["fixture:a1"],
        }],
    }

    decision = screen_a2(snapshot, a1, minimum_identifiability_score=0, llm_top_n_per_theme=1).decisions[0]
    assert decision["a2_factor_scores"]["capital_flow"]["available"] is True
    assert decision["a2_factor_scores"]["capital_flow"]["score"] == 90
    assert decision["a2_factor_scores"]["tier_structure"]["available"] is True
    assert decision["a2_factor_scores"]["leader_structure"]["available"] is True
