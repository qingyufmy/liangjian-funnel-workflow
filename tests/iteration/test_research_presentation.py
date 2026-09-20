from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.presentation import (
    attach_research_presentations,
    project_position_presentation,
    project_research_presentation,
    project_unified_status,
)


NOW = datetime(2026, 9, 18, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_ui_01_a3_daily_setup_is_not_immediate_entry_and_target_claim_is_bounded() -> None:
    row = {
        "symbol": "600001.SH",
        "eligibility": "QUALIFIED",
        "execution_permission": "REQUIRES_A3_A4_CONFIRMATION",
        "reason_codes": ["A3_REWARD_RISK_BELOW_MINIMUM"],
        "a4_deferred_conditions": ["LIVE_REWARD_RISK", "CURRENT_STOP_DISTANCE"],
        "plan_id": "p1",
        "plan_expiry": "2026-09-18T15:00:00+08:00",
        "price_discovery": True,
        "strategy_facts": {
            "observation_targets": {
                "r2": 12.0,
                "target_basis": "R_MULTIPLE_NO_RESISTANCE_REQUIRED",
            }
        },
    }

    view = project_research_presentation(row, stage="A3", pool="approved", now=NOW)

    assert view["a3"]["daily_setup_state"] == "QUALIFIED"
    assert view["a3"]["a4_confirmation_state"] == "REQUIRED"
    assert view["a3"]["current_entry_eligibility"] == "PENDING_A4"
    assert view["a3"]["current_entry_eligibility"] != "BUY_NOW"
    assert view["a3"]["target"]["kind"] == "FIXED_R_OBSERVATION"
    assert view["a3"]["target"]["claim"] == "OBSERVATION_NOT_MARKET_PROOF"


def test_ui_02_a2_theme_strength_is_not_a_fake_stock_total_score() -> None:
    first = project_research_presentation({
        "symbol": "600001.SH", "theme_score": 88, "relative_strength_score": 91,
        "market_role": "LEADER", "a2_route": "EMOTION_LEADER",
    }, stage="A2", pool="approved", now=NOW)
    second = project_research_presentation({
        "symbol": "600002.SH", "theme_score": 88, "relative_strength_score": 63,
        "market_role": "FOLLOWER", "a2_route": "TREND",
    }, stage="A2", pool="approved", now=NOW)

    assert first["a2"]["theme_strength"] == second["a2"]["theme_strength"] == 88
    assert first["a2"]["stock_relative_strength"] == 91
    assert second["a2"]["stock_relative_strength"] == 63
    assert first["a2"]["market_role"] == "LEADER"
    assert second["a2"]["market_role"] == "FOLLOWER"
    assert first["a2"]["individual_total_score"] is None


def test_ui_03_unified_status_keeps_job_data_opportunity_and_unknown_separate() -> None:
    blocked = project_unified_status(
        job_status="SUCCEEDED", data_state="INSUFFICIENT", opportunity_state="UNKNOWN",
        actionability_state="UNKNOWN", critical_data=True, coverage=None,
    )
    degraded = project_unified_status(
        job_status="SUCCEEDED", data_state="PARTIAL", opportunity_state="ABSENT",
        actionability_state="NO_ACTION", critical_data=False, coverage=None,
    )

    assert blocked["overall_state"] == "BLOCKED_DATA"
    assert degraded["overall_state"] == "SUCCEEDED_DEGRADED_NO_OPPORTUNITY"
    assert blocked["coverage"] is None and degraded["coverage"] is None

    position = project_position_presentation(
        data_health={"status": "UNOBSERVABLE", "current_price_trusted": False},
        sellable_qty=0,
        exit_signal_pending=True,
    )
    assert position["verification_state"] == "QUOTE_UNVERIFIED"
    assert position["sell_state"] == "T1_NOT_SELLABLE"
    assert position["exit_state"] == "EXIT_TRIGGERED_NOT_FILLED"
    unknown = project_position_presentation(
        data_health={"status": "UNKNOWN", "current_price_trusted": False},
        sellable_qty=None,
        exit_signal_pending=False,
    )
    assert unknown["sell_state"] == "SELLABILITY_UNKNOWN"


def test_ui_04_attached_projection_is_the_single_persisted_stage_projection() -> None:
    output = {
        "focus_pool": [{
            "symbol": "600001.SH", "theme_score": 80, "relative_strength_score": 77,
            "market_role": "LEADER", "source_refs": ["A2:FROZEN:1"],
        }],
        "watch_only_pool": [],
        "rejected_candidates": [],
    }
    projected = attach_research_presentations(output, stage="A2", now=NOW)
    row = projected["focus_pool"][0]

    assert row["presentation"]["schema_version"] == "research-presentation/1.0.0"
    assert row["presentation"]["source_identities"] == ["A2:FROZEN:1"]
    assert row["presentation"]["a2"]["individual_total_score"] is None
    assert output["focus_pool"][0].get("presentation") is None
