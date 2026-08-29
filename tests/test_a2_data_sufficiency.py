from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import liangjian_funnel.pipeline.a2_features as a2_features
from liangjian_funnel.pipeline.a2_features import (
    _aware,
    _board_height,
    _date,
    _dataset_observation,
    _ladder_by_symbol,
    _membership_by_symbol,
    _number,
    _records,
    _symbol,
    build_a2_feature_snapshot,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _bars(growth: float, count: int = 31) -> list[dict[str, object]]:
    return [
        {
            "date_ms": int((NOW - timedelta(days=count - 1 - index)).timestamp() * 1000),
            "close_price": 10 * (1 + growth) ** index,
        }
        for index in range(count)
    ]


def _membership(symbols: list[str], *, nested: bool = False) -> dict[str, object]:
    records = [
        {
            "thscode": symbol,
            "memberships": [
                {"industry_thscode": "I001", "industry_name": "周期制造"},
                {"concept_thscode": "C001", "concept_name": "产业升级"},
            ],
        }
        for symbol in symbols
    ]
    return {"available": True, "payload": {"records": records}} if nested else {"available": True, "records": records}


def _capital(symbols: list[str], *, available: bool = True) -> dict[str, object]:
    return {
        "available": available,
        "source_id": "TEST_VENDOR_DERIVED",
        "provider_method": "VENDOR_DERIVED",
        "by_symbol": {
            symbol: {
                "available": available,
                "capital_flow_score": 70,
                "availability_state": "OBSERVED_VALUE" if available else "SOURCE_FAILED",
                "reason_code": "OK" if available else "SOURCE_NOT_CONFIGURED",
            }
            for symbol in symbols
        },
    }


def test_empty_or_malformed_sources_are_insufficient_not_zero_observations() -> None:
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": "invalid"}, {"symbol": "600001"}, {"symbol": ""}],
        daily_bars={},
        industry_membership={"records": [{"thscode": "600001", "memberships": "not-a-list"}]},
        concept_membership={"payload": {"records": [{"symbol": "600001", "memberships": [None, {}]}]}},
        ladder_snapshot={"available": True, "records": []},
        dragon_tiger_snapshot={"records": [None, {"symbol": "bad"}]},
        attention_snapshot=None,
        sector_cycle_snapshot=None,
        capital_flow_snapshot={"available": False, "reason_code": "SOURCE_NOT_CONFIGURED", "by_symbol": {}},
        as_of=NOW,
    )

    assert snapshot["symbol_count"] == 1
    assert snapshot["available"] is False
    assert snapshot["data_sufficiency_state"] == "INSUFFICIENT"
    row = snapshot["by_symbol"]["600001.SH"]
    assert row["factors"]["trend_strength_proxy"]["reason_code"] == "A2_TREND_DAILY_BARS_MISSING"
    assert row["factors"]["tier_structure"]["availability_state"] == "OBSERVED_ABSENT"
    assert row["factors"]["index_chain_resonance"]["available"] is False


def test_ladder_is_point_in_time_and_tier_unknown_when_record_shape_fails() -> None:
    symbols = ["600001.SH", "000002.SZ"]
    ladder = {
        "available": True,
        "records": [
            {"date": "20260828", "boards": {"one_board": [{"thscode": symbols[0]}]}},
            {"date": "2026-08-29", "boards": {"two_board": [{"thscode": symbols[1], "board_num": 2}]}},
            {"date": "2026-08-30", "boards": {"seven_board": [{"thscode": symbols[0], "board_num": 7}]}},
        ],
    }
    selected = _ladder_by_symbol(ladder, NOW.date())
    # Only the latest eligible trade date is authoritative; prior-day ladder
    # members must not leak into today's tier decision.
    assert symbols[0] not in selected
    assert selected[symbols[1]]["trade_date"] == "2026-08-29"
    assert selected[symbols[1]]["board_num"] == 2

    malformed = {"available": True, "records": [{"date": "2026-08-29", "boards": "bad"}]}
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": symbol, "amount": 1000} for symbol in symbols],
        daily_bars={symbol: _bars(0.01) for symbol in symbols},
        industry_membership=_membership(symbols),
        concept_membership=None,
        ladder_snapshot=malformed,
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot=_capital(symbols),
        as_of=NOW,
    )
    assert snapshot["ladder_dataset_state"] == "OBSERVED_VALUE"
    assert all(snapshot["by_symbol"][symbol]["tier"] == "NONE" for symbol in symbols)

    failed = {"available": False, "availability_state": "TIMEOUT", "reason_code": "PROVIDER_TIMEOUT", "records": []}
    observed, state, reason = _dataset_observation(failed)
    assert (observed, state, reason) == (False, "TIMEOUT", "PROVIDER_TIMEOUT")


def test_theme_metrics_use_nested_taxonomy_and_cycle_evidence_at_coverage_boundary() -> None:
    symbols = ["600001.SH", "600002.SH", "600003.SH", "600004.SH", "600005.SH"]
    daily = {symbol: _bars(0.01) for symbol in symbols[:4]}
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": symbol, "amount": 1000 + index * 100} for index, symbol in enumerate(symbols)],
        daily_bars=daily,
        industry_membership=_membership(symbols, nested=True),
        concept_membership=None,
        ladder_snapshot={"available": True, "records": []},
        dragon_tiger_snapshot={"records": [{"symbol": symbols[0]}]},
        attention_snapshot={"records": [{"symbol": symbols[1]}]},
        sector_cycle_snapshot={
            "history_metrics": {
                "monthly_rotation_candidates": [
                    {"industry_thscode": "I001", "rotation_score": 130},
                    {"industry_thscode": "OTHER", "score": 1},
                ]
            }
        },
        capital_flow_snapshot=_capital(symbols),
        as_of=NOW,
    )

    metric = snapshot["theme_metrics"]["INDUSTRY:I001"]
    assert metric["member_count"] == len(symbols)
    assert metric["return_coverage"] == 0.8
    assert metric["available"] is True
    assert metric["cycle_score"] == 100.0
    assert snapshot["by_symbol"][symbols[0]]["factors"]["index_chain_resonance"]["available"] is True
    assert snapshot["factor_coverage"]["index_chain_resonance"] == 1.0
    # Overall sufficiency still fails because daily bars require 95% coverage;
    # a locally complete theme is not allowed to hide that deficit.
    assert snapshot["available"] is False
    assert snapshot["reason_code"] == "A2_CRITICAL_DATA_INSUFFICIENT"


def test_a2_private_normalizers_cover_invalid_types_without_silent_facts() -> None:
    assert _records(None) == ()
    assert _records({"records": "not-a-list"}) == ()
    assert _records({"payload": {"records": [{"symbol": "600001"}, "bad"]}}) == ({"symbol": "600001"},)
    assert _membership_by_symbol({"records": [{"symbol": "600001", "memberships": ["bad", {"industry_thscode": "i001"}]}]}, taxonomy="INDUSTRY")["600001.SH"][0]["taxonomy_code"] == "I001"
    assert _symbol({"code": "600001"}) == "600001.SH"
    assert _symbol("not-a-symbol") == ""
    assert _number(True) is None
    assert _number("not-a-number") is None
    assert _date("2026/08/29").isoformat() == "2026-08-29"
    assert _date("bad") is None
    assert _board_height("seven_board") == 7
    assert _board_height("unknown") is None
    with pytest.raises(ValueError, match="timezone-aware"):
        _aware(datetime(2026, 8, 29, 15, 10))


def test_dataset_observation_rejects_malformed_records_as_source_failure() -> None:
    """A malformed provider envelope is not evidence of an empty event set."""

    assert _dataset_observation({"available": True, "records": "not-a-list"}) == (
        False,
        "SOURCE_FAILED",
        "A2_TIER_RECORDS_MALFORMED",
    )
    assert _dataset_observation({"available": True, "records": [None]}) == (
        False,
        "SOURCE_FAILED",
        "A2_TIER_RECORDS_MALFORMED",
    )


def test_leader_roles_cover_trend_capacity_and_unconfirmed_boundaries() -> None:
    """Role labels must reflect observed dimensions, never missing data as a score."""

    symbols = ("600001.SH", "000002.SZ", "300003.SZ", "600004.SH")
    snapshot = build_a2_feature_snapshot(
        candidates=[
            {"symbol": symbols[0], "amount": 100},
            {"symbol": symbols[1], "amount": 1_000},
            {"symbol": symbols[2], "amount": 500},
            {"symbol": symbols[3]},  # no market, liquidity, ladder or flow fact
        ],
        daily_bars={
            symbols[0]: _bars(0.05),
            symbols[1]: _bars(0.01),
            symbols[2]: _bars(0.005),
        },
        industry_membership=_membership(list(symbols)),
        concept_membership=None,
        ladder_snapshot=None,
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot={
            "available": True,
            "source_id": "TEST_FLOW",
            "by_symbol": {
                symbols[0]: {"available": True, "capital_flow_score": 20},
                symbols[1]: {"available": True, "capital_flow_score": 20},
                symbols[2]: {"available": True, "capital_flow_score": 20},
            },
        },
        as_of=NOW,
    )

    assert snapshot["by_symbol"][symbols[0]]["leader_role"] == "TREND_LEADER"
    assert snapshot["by_symbol"][symbols[1]]["leader_role"] == "CAPACITY_CORE"
    assert snapshot["by_symbol"][symbols[3]]["leader_role"] == "UNCONFIRMED"
    assert snapshot["by_symbol"][symbols[3]]["leader_structure"]["available"] is False


def test_build_snapshot_defensively_ignores_empty_taxonomy_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """Even a corrupted normalized membership row must not create a fake theme."""

    symbol = "600001.SH"

    def malformed_membership(_value: object, *, taxonomy: str):
        return {symbol: ({"taxonomy": taxonomy, "taxonomy_code": ""},)}

    monkeypatch.setattr(a2_features, "_membership_by_symbol", malformed_membership)
    snapshot = build_a2_feature_snapshot(
        candidates=[{"symbol": symbol}],
        daily_bars={},
        industry_membership={"records": []},
        concept_membership={"records": []},
        ladder_snapshot=None,
        dragon_tiger_snapshot={"records": []},
        attention_snapshot={"records": []},
        sector_cycle_snapshot=None,
        capital_flow_snapshot=None,
        as_of=NOW,
    )

    assert snapshot["theme_metrics"] == {}
    assert snapshot["by_symbol"][symbol]["index_chain_resonance"]["available"] is False
