from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.weekly_strategy import (
    build_weekly_strategy_context,
    weekly_rotation_state,
)


AS_OF = datetime(2026, 8, 30, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_weekly_context_separates_evidence_from_subjective_market_calls() -> None:
    context = build_weekly_strategy_context(
        {
            "GLOBAL_MACRO_SNAPSHOT": {
                "available": True,
                "as_of": AS_OF.isoformat(),
                "values": {"usd_momentum_percentile": 40},
            },
            "CROSS_MARKET_LEAD_SNAPSHOT": {"available": False, "reason_code": "SOURCE_UNAVAILABLE"},
        },
        as_of=AS_OF,
        monthly_rotations=[{
            "industry_thscode": "881001.TI",
            "industry_name": "测试行业",
            "return_5d": 3,
            "return_10d": 2,
            "return_20d": 1,
            "source_ref": "derived:ths:weekly",
        }],
        policy_documents=[{
            "fact_id": "policy-1",
            "title": "正式政策",
            "publish_time": "2026-08-28T10:00:00+08:00",
            "source_url": "https://gov.example/policy-1",
        }],
        macro_asset_quadrant={"status": "READY", "leading_asset": "EQUITY"},
    )

    assert context["status"] == "READY"
    assert context["industry_rotation"][0]["weekly_state"] == "ACCELERATING"
    assert context["weekly_policy_impulse"]["document_count"] == 1
    assert context["decision_rules"]["orders_and_shipments_are_separate_realization_stages"] is True
    assert "SUBJECTIVE_INDEX_TARGET_AS_DETERMINISTIC_SIGNAL" in context["prohibited_claims"]


def test_weekly_rotation_states_keep_missing_data_unknown() -> None:
    assert weekly_rotation_state({"return_5d": 2, "return_20d": -1}) == "EARLY_REVERSAL"
    assert weekly_rotation_state({"return_5d": -1, "return_20d": 4}) == "COOLING"
    assert weekly_rotation_state({"return_20d": 4}) == "UNKNOWN"
