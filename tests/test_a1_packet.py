from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.a1_packet import (
    A1_RESEARCH_PACKET_SCHEMA_VERSION,
    build_a1_research_packet,
    packet_diagnostics,
)


AS_OF = datetime(2026, 8, 28, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


def _snapshot() -> dict:
    decisions = [
        {
            "rank": rank,
            "industry_thscode": f"881{rank:03d}.TI",
            "industry_name": f"行业{rank}",
            "decision": "INCLUDE" if rank <= 4 else "DEFER",
            "reason_codes": ["TEST"],
            "supporting_source_refs": [f"derived:rotation:{rank}"],
            "metrics": {"return_20d": rank / 100},
        }
        for rank in range(1, 21)
    ]
    return {
        "snapshot_id": "snap-a1",
        "snapshot_hash": "s" * 64,
        "as_of": AS_OF.isoformat(),
        "g0_symbols": ["SHSE.600000"],
        "MACRO_ECONOMIC_DATA": {
            "available": True,
            "quality": {"tier": "T2_OPEN_OBSERVATION_DATE_ONLY"},
            "series": [
                {"id": "PMI", "observation_date": f"2026-{month:02d}-01", "value": 49 + month / 100, "source_ref": "nbs:pmi"}
                for month in range(1, 25)
            ],
        },
        "MACRO_POLICY_FEED": {"available": True},
        "INDUSTRY_ACTIVITY_DATA": {
            "available": True,
            "items": [
                {"industry_thscode": f"881{index:03d}.TI", "industry_name": f"行业{index}", "period": "2026-07", "yoy": index / 10, "source_ref": "nbs:industry"}
                for index in range(1, 83)
            ],
        },
        "THS_INDUSTRY_CATALOG": {
            "records": [
                {"thscode": f"881{index:03d}.TI", "name": f"行业{index}"}
                for index in range(1, 83)
            ]
        },
        "SECTOR_CYCLE_SNAPSHOT": {
            "available": True,
            "history_metrics": {"monthly_rotation_candidates": decisions},
        },
    }


def _context() -> dict:
    snapshot = _snapshot()
    return {
        "strategy_month": "2026-08",
        "status": "READY",
        "monthly_industry_rotation": [
            {"industry_thscode": f"881{rank:03d}.TI", "industry_name": f"行业{rank}", "return_20d": rank / 100, "source_ref": f"derived:rotation:{rank}"}
            for rank in range(1, 21)
        ],
        "monthly_industry_decisions": [
            {
                "rank": rank,
                "industry_thscode": f"881{rank:03d}.TI",
                "industry_name": f"行业{rank}",
                "decision": "INCLUDE" if rank <= 4 else "DEFER",
                "reason_codes": ["TEST"],
                "supporting_source_refs": [f"derived:rotation:{rank}"],
            }
            for rank in range(1, 21)
        ],
        "monthly_rotation_coverage": {"requested_top_n": 20, "status": "READY"},
        "macro_asset_quadrant": {"regime": "EQUITY_PREFERENCE"},
        "weekly_strategy_context": {
            "schema_version": "weekly-strategy-context/1.0.0",
            "status": "READY",
            "industry_rotation": [{"industry_thscode": "881001.TI", "weekly_state": "PERSISTENT"}],
        },
        "policy_window": {"official_documents": [{"fact_id": "policy-1", "title": "测试政策", "summary": "摘要", "source_url": "gov:1"}]},
        "prior_theme_registry": {"themes": [{"theme_id": "prior"}], "nodes": []},
    }


def test_packet_has_complete_industry_and_decision_coverage_without_raw_history():
    packet = build_a1_research_packet(_snapshot(), as_of=AS_OF, monthly_strategy_context=_context())
    assert packet["schema_version"] == A1_RESEARCH_PACKET_SCHEMA_VERSION
    assert len(packet["industry_features"]) == 82
    assert len(packet["canonical_monthly_decisions"]) == 20
    serialized = str(packet)
    assert "'series':" not in serialized
    assert "'items':" not in serialized
    assert packet["coverage"]["raw_history_projected_out"] is True
    assert packet["coverage"]["weekly_strategy_status"] == "READY"
    assert packet["weekly_strategy_context"]["industry_rotation"][0]["weekly_state"] == "PERSISTENT"
    assert packet["diagnostics"]["section_chars"]["industry_features"] > 0


def test_packet_hash_changes_when_raw_history_changes_but_dict_order_does_not():
    base = _snapshot()
    first = build_a1_research_packet(base, as_of=AS_OF, monthly_strategy_context=_context())
    reordered = dict(reversed(list(base.items())))
    second = build_a1_research_packet(reordered, as_of=AS_OF, monthly_strategy_context=_context())
    assert first["packet_hash"] == second["packet_hash"]

    changed = _snapshot()
    changed["MACRO_ECONOMIC_DATA"]["series"][0]["value"] = 99
    third = build_a1_research_packet(changed, as_of=AS_OF, monthly_strategy_context=_context())
    assert third["packet_hash"] != first["packet_hash"]
    assert third["macro_features"][0]["latest_observation"]


def test_packet_diagnostics_are_sectioned_and_budget_is_explicit():
    packet = build_a1_research_packet(_snapshot(), as_of=AS_OF, monthly_strategy_context=_context())
    diagnostics = packet_diagnostics(packet)
    assert diagnostics["packet_chars"] > 0
    assert diagnostics["estimated_input_tokens"] > 0
    assert diagnostics["largest_sections"]
    assert packet["coverage"]["budget"]["within_budget"] is True
