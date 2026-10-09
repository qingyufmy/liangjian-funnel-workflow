"""Local qfq evidence contract; fixtures do not certify a live provider."""
from copy import deepcopy
import hashlib
import json

import pytest

from liangjian_funnel.data.corporate_actions import (
    build_adjustment_evidence, compare_forward, pending_reset_evidence,
)


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


def fixture(symbol="688808.SH"):
    bars = [{"symbol": symbol, "date": day, "open": close, "high": close + 1,
             "low": close - 1, "close": close, "volume": 100, "amount": 1234,
             "adjust_mode": "raw"} for day, close in
            [("2026-09-01", 100), ("2026-09-02", 80), ("2026-09-03", 75)]]
    event = {"symbol": symbol, "ex_date": "2026-09-02",
             "previous_trade_date": "2026-09-01", "preclose": 100,
             "cash_paid": 0.3, "cash_reference": 0.3,
             "differential_distribution": False, "bonus_total": 0.48,
             "rights_ratio": 0, "rights_price": 0,
             "source_event": {"fixture": "EXPLICIT_LOCAL_SAMPLE_NOT_LIVE", "bonus": 0.48},
             "reference_sha256": "a" * 64, "verification_scope": "SAMPLE_VERIFIED",
             "verified_fields": ["cash_reference", "differential_distribution",
                                 "bonus_total", "rights_ratio", "rights_price"]}
    event["source_event_sha256"] = sha(event["source_event"])
    coverage = {"complete": True, "symbol": symbol, "from": bars[0]["date"],
                "through": bars[-1]["date"], "expected_event_dates": [event["ex_date"]],
                "evidence_sha256": "b" * 64,
                "trade_dates": [bar["date"] for bar in bars]}
    return dict(symbol=symbol, raw_bars=bars, events=[event], coverage=coverage)


def test_ratio_not_affine_and_volume_amount_execution_remain_raw():
    args = fixture()
    original = deepcopy(args)
    result = build_adjustment_evidence(**args)
    ratio = (100 - 0.3) / 1.48 / 100
    assert result["status"] == "LOCAL_READY"
    assert result["formula_version"] == "qfq-ratio/1"
    assert result["factor_chain"][0]["ratio"] == pytest.approx(ratio)
    assert result["technical_rows"][0]["close"] == pytest.approx(100 * ratio)
    assert result["technical_rows"][0]["high"] == pytest.approx(101 * ratio)
    assert result["technical_rows"][1]["close"] == 80
    assert result["technical_rows"][0]["volume"] == 100
    assert result["technical_rows"][0]["amount"] == 1234
    assert result["execution_price_basis"] == "RAW_ONLY"
    assert result["raw_rows"] == original["raw_bars"] and args == original
    assert result["raw_series_sha256"] == sha(original["raw_bars"])
    assert result["input_events_sha256"] == sha(original["events"])
    assert result["original_payload_sha256s"] == [sha(original["events"][0]["source_event"])]
    assert len(result["technical_series_sha256"]) == 64
    assert result["live_verified"] is False


def test_normal_daily_append_changes_series_not_corporate_factor_version():
    args = fixture()
    first = build_adjustment_evidence(**args)
    args["raw_bars"].append({**args["raw_bars"][-1], "date":"2026-09-04"})
    args["coverage"]["trade_dates"].append("2026-09-04")
    args["coverage"]["through"] = "2026-09-04"
    second = build_adjustment_evidence(**args)
    assert first["status"] == second["status"] == "LOCAL_READY"
    assert first["series_version"] != second["series_version"]
    assert first["corporate_action_version"] == second["corporate_action_version"]
    assert first["factor_version"] == second["factor_version"]
    assert pending_reset_evidence(first["factor_version"], second["factor_version"])["pending_reset_required"] is False


def test_event_reference_revision_changes_factor_and_pure_reset_reason():
    args = fixture()
    first = build_adjustment_evidence(**args)
    args["events"][0].update(cash_reference=0.4, cash_paid=0.4)
    second = build_adjustment_evidence(**args)
    assert first["factor_version"] != second["factor_version"]
    reset = pending_reset_evidence(first["factor_version"], second["factor_version"])
    assert reset["reason"] == "ADJUSTMENT_FACTOR_CHANGED"
    assert reset["state_mutated"] is False


def test_multiple_events_apply_only_after_bar_date():
    args = fixture()
    second = deepcopy(args["events"][0])
    second.update(ex_date="2026-09-03", previous_trade_date="2026-09-02",
                  preclose=80, cash_reference=0.5, cash_paid=0.5,
                  bonus_total=0, rights_ratio=0.1, rights_price=20)
    args["events"].append(second)
    args["coverage"]["expected_event_dates"].append("2026-09-03")
    result = build_adjustment_evidence(**args)
    first_ratio = (100 - 0.3) / 1.48 / 100
    second_ratio = (80 - 0.5 + 0.1 * 20) / 1.1 / 80
    assert result["factors"] == pytest.approx([first_ratio * second_ratio, second_ratio, 1])


@pytest.mark.parametrize("fault", ["missing_event", "missing_coverage", "bonus_unverified",
    "rights_missing", "cash_reference_missing", "hash_tampered", "wrong_symbol",
    "preclose_mismatch", "previous_date_wrong", "future_event", "duplicate_event",
    "bad_date", "nonfinite", "negative_cash", "bool_number", "adjusted_input",
    "unordered_bars", "invalid_ohlc", "cash_conflict", "unhashable_mode",
    "global_semantics_claim", "unverified_differential", "zero_reference_price"])
def test_unknown_or_invalid_evidence_is_gap_not_reject_or_factor_one(fault):
    args = fixture()
    event = args["events"][0]
    if fault == "missing_event": args["events"] = []
    elif fault == "missing_coverage": args["coverage"]["complete"] = False
    elif fault == "bonus_unverified": event["verified_fields"].remove("bonus_total")
    elif fault == "rights_missing": event.pop("rights_ratio")
    elif fault == "cash_reference_missing": event.pop("cash_reference")
    elif fault == "hash_tampered": event["source_event"]["bonus"] = 0.5
    elif fault == "wrong_symbol": event["symbol"] = "600000.SH"
    elif fault == "preclose_mismatch": event["preclose"] = 99
    elif fault == "previous_date_wrong": event["previous_trade_date"] = "2026-08-31"
    elif fault == "future_event": event["ex_date"] = "2026-09-04"
    elif fault == "duplicate_event": args["events"].append(deepcopy(event))
    elif fault == "bad_date": event["ex_date"] = "2026-02-30"
    elif fault == "nonfinite": event["cash_reference"] = float("nan")
    elif fault == "negative_cash": event["cash_reference"] = -1
    elif fault == "bool_number": event["rights_ratio"] = False
    elif fault == "adjusted_input": args["raw_bars"][0]["adjust_mode"] = "qfq"
    elif fault == "unordered_bars": args["raw_bars"].reverse()
    elif fault == "invalid_ohlc": args["raw_bars"][0]["high"] = 0
    elif fault == "cash_conflict": event["cash_paid"] = 0.4
    elif fault == "unhashable_mode": args["raw_bars"][0]["adjust_mode"] = {}
    elif fault == "global_semantics_claim": event["verification_scope"] = "GLOBAL_LIVE"
    elif fault == "unverified_differential": event["verified_fields"].remove("differential_distribution")
    elif fault == "zero_reference_price": event["cash_paid"] = event["cash_reference"] = 100
    result = build_adjustment_evidence(**args)
    assert result["status"] == "ADJUSTMENT_DATA_GAP"
    assert result["technical_rows"] == [] and result["factors"] == []
    assert result["raw_rows"] == args["raw_bars"]
    assert result["technical_series_sha256"] is None
    assert result["original_payload_sha256s"] == [sha(item["source_event"]) for item in args["events"]]
    assert result["reasons"] and result["live_verified"] is False


def test_differential_300196_requires_reference_not_paid_cash():
    args = fixture("300196.SZ")
    event = args["events"][0]
    event.update(cash_paid=0.2, cash_reference=0.1974617,
                 differential_distribution=True, bonus_total=0)
    result = build_adjustment_evidence(**args)
    assert result["status"] == "LOCAL_READY"
    assert result["factor_chain"][0]["ratio"] == pytest.approx((100 - 0.1974617) / 100)
    event.pop("cash_reference")
    assert build_adjustment_evidence(**args)["status"] == "ADJUSTMENT_DATA_GAP"


def test_no_event_requires_positive_complete_coverage_evidence():
    args = fixture()
    args["events"] = []
    args["coverage"]["expected_event_dates"] = []
    result = build_adjustment_evidence(**args)
    assert result["status"] == "LOCAL_READY" and result["factors"] == [1, 1, 1]
    args["coverage"].pop("evidence_sha256")
    assert build_adjustment_evidence(**args)["status"] == "ADJUSTMENT_DATA_GAP"


def test_versions_bind_raw_source_event_normalization_formula_and_coverage():
    args = fixture()
    first = build_adjustment_evidence(**args)
    args["events"][0]["cash_reference"] = args["events"][0]["cash_paid"] = 0.4
    second = build_adjustment_evidence(**args)
    assert first["series_version"] != second["series_version"]
    reset = pending_reset_evidence(first["factor_version"], second["factor_version"])
    assert reset["pending_reset_required"] is True and reset["state_mutated"] is False
    assert pending_reset_evidence(second["factor_version"], second["factor_version"])["pending_reset_required"] is False
    args["raw_bars"][0]["amount"] += 1
    third = build_adjustment_evidence(**args)
    assert third["series_version"] != second["series_version"]
    assert third["factor_version"] == second["factor_version"]
    args["events"][0]["source_event"]["revision"] = 2
    args["events"][0]["source_event_sha256"] = sha(args["events"][0]["source_event"])
    fourth = build_adjustment_evidence(**args)
    assert fourth["series_version"] != third["series_version"]
    args["coverage"]["evidence_sha256"] = "c" * 64
    assert build_adjustment_evidence(**args)["series_version"] != fourth["series_version"]


def test_independent_forward_full_scope_half_percent_only_local_match():
    result = build_adjustment_evidence(**fixture())
    forward = deepcopy(result["technical_rows"])
    comparison = compare_forward(result, forward, source_id="independent-fixture", raw_source_id="raw-fixture")
    assert comparison["status"] == "LOCAL_MATCH" and comparison["live_verified"] is False
    forward[0]["close"] *= 1.006
    assert compare_forward(result, forward, source_id="independent-fixture", raw_source_id="raw-fixture")["status"] == "ADJUSTMENT_DATA_GAP"
    assert compare_forward(result, forward[1:], source_id="independent-fixture", raw_source_id="raw-fixture")["status"] == "ADJUSTMENT_DATA_GAP"
    assert compare_forward(result, result["technical_rows"], source_id="same", raw_source_id="same")["status"] == "ADJUSTMENT_DATA_GAP"


def test_forward_exact_half_percent_and_tampered_local_series():
    result = build_adjustment_evidence(**fixture())
    forward = deepcopy(result["technical_rows"])
    forward[1]["close"] = 80.4
    assert compare_forward(result, forward, source_id="forward", raw_source_id="raw")["status"] == "LOCAL_MATCH"
    result["technical_rows"][0]["close"] += 0.01
    assert compare_forward(result, forward, source_id="forward", raw_source_id="raw")["status"] == "ADJUSTMENT_DATA_GAP"


def test_no_network_adapter_can_certify_bonus_from_one_sample_for_other_events():
    args = fixture()
    args["events"][0]["verified_fields"].remove("bonus_total")
    # Known API per_share_bonus is deliberately not a normalized contract field.
    args["events"][0]["source_event"] = {"per_share_bonus": 0.48}
    args["events"][0]["source_event_sha256"] = sha(args["events"][0]["source_event"])
    assert build_adjustment_evidence(**args)["status"] == "ADJUSTMENT_DATA_GAP"


@pytest.mark.parametrize("versions", [(None, None), ("old", "new"), ("a" * 64, None)])
def test_unproven_version_never_mutates_pending_state(versions):
    result = pending_reset_evidence(*versions)
    assert result["status"] == "UNPROVEN" and result["state_mutated"] is False


def test_forward_extreme_finite_inputs_never_emit_infinite_evidence():
    args = fixture()
    args["events"] = []
    args["coverage"]["expected_event_dates"] = []
    for row in args["raw_bars"]:
        for field in ("open", "high", "low", "close"):
            row[field] = 1e-308
    result = build_adjustment_evidence(**args)
    forward = deepcopy(result["technical_rows"])
    for row in forward:
        for field in ("open", "high", "low", "close"):
            row[field] = 1e308
    comparison = compare_forward(result, forward, source_id="forward", raw_source_id="raw")
    assert comparison["status"] == "ADJUSTMENT_DATA_GAP"
    assert comparison["maximum_relative_error"] is None
    assert comparison["reasons"] == ["FORWARD_ERROR_NOT_FINITE"]
