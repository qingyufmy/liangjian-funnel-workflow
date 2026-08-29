"""Acceptance-boundary coverage for the point-in-time replay evaluator."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.evaluation.replay_window import (
    ReplayWindowContractError,
    _as_nonnegative_int,
    _as_number,
    _ratio,
    evaluate_replay_window,
)


TZ = ZoneInfo("Asia/Shanghai")
CUTOFF = datetime(2026, 8, 29, 16, 0, tzinfo=TZ)


def _stage(
    name: str,
    *,
    status: str,
    symbols: list[str],
    input_count: int,
    snapshot_id: str = "snapshot-replay-1",
    quality: str = "VALIDATED",
    opportunity: str = "PRESENT",
    publication: str = "READY",
    reasons: list[str] | None = None,
) -> dict[str, object]:
    pools = {
        "A1": ("active_research_pool", "monitor_pool"),
        "A2": ("focus_pool", "watch_only_pool"),
        "A3": ("core_watch_pool", "secondary_watch_pool"),
    }
    output = {
        pools[name][0]: [{"symbol": symbol} for symbol in symbols],
        pools[name][1]: [],
    }
    return {
        "stage": name,
        "status": status,
        "snapshot_id": snapshot_id,
        "symbols": symbols,
        "input_count": input_count,
        "evaluated_count": input_count,
        "output_count": len(symbols),
        "output": output,
        "reason_codes": reasons or [],
        "outcome_v2": {
            "schema_version": "research-outcome/3.0.0",
            "stage": name,
            "lifecycle_state": "TERMINAL",
            "quality_state": quality,
            "opportunity_state": opportunity,
            "publication_state": publication,
            "reason_codes": reasons or [],
        },
    }


def _summary(
    trade_date: date,
    *,
    run_id: str = "replay-run",
    audit_status: str = "READY",
    stages: list[dict[str, object]] | None = None,
    snapshot_id: str = "snapshot-replay-1",
    snapshot_hash: str = "a" * 64,
    embedded_broker: dict[str, object] | None = None,
) -> dict[str, object]:
    as_of = datetime(trade_date.year, trade_date.month, trade_date.day, 15, 10, tzinfo=TZ).isoformat()
    audit: dict[str, object] = {
        "lane": "lane_1",
        "model": "deepseek-v4-pro-0813",
        "status": audit_status,
        "snapshot_id": snapshot_id,
        "snapshot_hash": snapshot_hash,
        "as_of": as_of,
        "trade_date": trade_date.isoformat(),
        "stages": stages
        or [
            _stage("A1", status="VALIDATED", symbols=["600519.SH", "000001.SZ"], input_count=100),
            _stage("A2", status="VALIDATED", symbols=["600519.SH"], input_count=2),
            _stage("A3", status="VALIDATED", symbols=["600519.SH"], input_count=1),
        ],
    }
    if embedded_broker is not None:
        audit["broker_gold"] = embedded_broker
    return {
        "run_id": run_id,
        "slot": "close",
        "status": audit_status,
        "snapshot": {
            "snapshot_id": snapshot_id,
            "snapshot_hash": snapshot_hash,
            "as_of": as_of,
            "trade_date": trade_date.isoformat(),
            "research_universe_count": 100,
        },
        "lane_audits": {"lane_1": audit},
    }


def test_embedded_replay_distinguishes_empty_opportunity_from_data_gap_and_failure() -> None:
    empty = _summary(
        date(2026, 8, 28),
        run_id="empty-run",
        snapshot_id="snapshot-empty",
        stages=[
            _stage("A1", status="VALIDATED_NO_OPPORTUNITY", symbols=[], input_count=100, snapshot_id="snapshot-empty", opportunity="ABSENT"),
            _stage("A2", status="NOT_RUN_UPSTREAM_BLOCKED", symbols=[], input_count=0, snapshot_id="snapshot-empty", opportunity="NOT_APPLICABLE"),
            _stage("A3", status="NOT_RUN_UPSTREAM_BLOCKED", symbols=[], input_count=0, snapshot_id="snapshot-empty", opportunity="NOT_APPLICABLE"),
        ],
    )
    gap = _summary(
        date(2026, 8, 27),
        run_id="gap-run",
        audit_status="READY_DEGRADED",
        snapshot_id="snapshot-gap",
        stages=[
            _stage("A1", status="VALIDATED", symbols=["600519.SH"], input_count=100, snapshot_id="snapshot-gap"),
            _stage(
                "A2",
                status="DEGRADED",
                symbols=[],
                input_count=1,
                snapshot_id="snapshot-gap",
                quality="DEGRADED",
                opportunity="UNKNOWN",
                reasons=["A2_CRITICAL_DATA_INSUFFICIENT"],
            ),
            _stage("A3", status="NOT_RUN_UPSTREAM_BLOCKED", symbols=[], input_count=0, snapshot_id="snapshot-gap", opportunity="NOT_APPLICABLE"),
        ],
    )
    failure = _summary(
        date(2026, 8, 26),
        run_id="failure-run",
        audit_status="FAILED",
        snapshot_id="snapshot-failure",
        stages=[
            _stage("A1", status="VALIDATED", symbols=["600519.SH"], input_count=100, snapshot_id="snapshot-failure"),
            _stage("A2", status="FAILED", symbols=[], input_count=1, snapshot_id="snapshot-failure", quality="FAILED", opportunity="UNKNOWN"),
            _stage("A3", status="NOT_RUN_UPSTREAM_BLOCKED", symbols=[], input_count=0, snapshot_id="snapshot-failure", opportunity="NOT_APPLICABLE"),
        ],
    )

    report = evaluate_replay_window([empty, gap, failure], minimum_days=3, cutoff=CUTOFF)
    by_run = {day["run_id"]: day for day in report["days"]}
    assert by_run["empty-run"]["classification"] == "EMPTY_OPPORTUNITY"
    assert by_run["gap-run"]["classification"] == "DATA_INSUFFICIENT"
    assert by_run["failure-run"]["classification"] == "EXECUTION_FAILURE"
    assert report["status"] == "READY"
    # The report is explicitly observational: replay cannot submit plans or
    # reach a provider even when a day contains a technical failure.
    assert report["source"] == {
        "runs": "memory",
        "audits": None,
        "broker_gold": None,
        "network_used": False,
        "models_called": False,
        "runtime_mutation": False,
    }


def test_replay_broker_benchmark_can_be_embedded_or_reported_invalid(tmp_path: Path) -> None:
    embedded = _summary(
        date(2026, 8, 29),
        embedded_broker={"status": "READY", "counts": {"benchmark": 2}, "symbol_coverage": {"active": 1}},
    )
    embedded_report = evaluate_replay_window([embedded], minimum_days=1, cutoff=CUTOFF)
    assert embedded_report["days"][0]["broker_gold"]["status"] == "AVAILABLE"
    assert embedded_report["days"][0]["broker_gold"]["source"] == "embedded_report"

    broker_dir = tmp_path / "broker-gold"
    broker_dir.mkdir()
    (broker_dir / "2026-08.json").write_text("{not-json", encoding="utf-8")
    invalid_report = evaluate_replay_window(
        [_summary(date(2026, 8, 29))],
        minimum_days=1,
        cutoff=CUTOFF,
        broker_gold_dir=broker_dir,
    )
    assert invalid_report["days"][0]["broker_gold"]["status"] == "INVALID"
    assert invalid_report["days"][0]["broker_gold"]["reason_code"] == "BROKER_GOLD_INVALID_JSON"


def test_replay_rejects_malformed_stage_set_and_identity_reuse(tmp_path: Path) -> None:
    missing_stage = _summary(date(2026, 8, 29), run_id="missing-stage")
    missing_stage["lane_audits"]["lane_1"]["stages"] = missing_stage["lane_audits"]["lane_1"]["stages"][:2]  # type: ignore[index]
    report = evaluate_replay_window([missing_stage], minimum_days=1, cutoff=CUTOFF)
    assert report["status"] == "BLOCKED_REPLAY_WINDOW_INSUFFICIENT"
    assert any(reason["code"] == "REPLAY_STAGE_SET_INVALID" for reason in report["blocking_reasons"])

    first = _summary(date(2026, 8, 28), run_id="identity-1", snapshot_hash="b" * 64)
    second = _summary(date(2026, 8, 29), run_id="identity-2", snapshot_hash="c" * 64)
    # Same snapshot id with different content is a point-in-time identity
    # violation even though the trading dates are distinct.
    report = evaluate_replay_window([first, second], minimum_days=2, cutoff=CUTOFF)
    assert report["status"] == "BLOCKED_REPLAY_VALIDATION"
    assert any(reason["code"] == "REPLAY_SNAPSHOT_ID_REUSED_WITH_DIFFERENT_CONTENT" for reason in report["blocking_reasons"])

    bad_json = tmp_path / "runs"
    bad_json.mkdir()
    (bad_json / "broken.json").write_text("[1, 2, 3]", encoding="utf-8")
    malformed = evaluate_replay_window(bad_json, minimum_days=1, cutoff=CUTOFF)
    assert malformed["status"] == "BLOCKED_REPLAY_WINDOW_INSUFFICIENT"
    assert any(reason["code"] == "REPLAY_JSON_INVALID" for reason in malformed["blocking_reasons"])


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, 0), ("12", 12), (12.0, 12), (-1, None), (True, None), ("no", None)],
)
def test_replay_numeric_normalizers_are_conservative(value: object, expected: int | None) -> None:
    assert _as_nonnegative_int(value) == expected


def test_replay_input_contract_and_cutoff_timezone_are_fail_closed() -> None:
    valid = [_summary(date(2026, 8, 29))]
    with pytest.raises(ReplayWindowContractError) as minimum_error:
        evaluate_replay_window(valid, minimum_days=True, cutoff=CUTOFF)
    assert minimum_error.value.reason_code == "REPLAY_MINIMUM_DAYS_INVALID"
    with pytest.raises(ReplayWindowContractError) as lane_error:
        evaluate_replay_window(valid, primary_lane_id="", cutoff=CUTOFF)
    assert lane_error.value.reason_code == "REPLAY_PRIMARY_LANE_ID_MISSING"
    with pytest.raises(ReplayWindowContractError) as cutoff_error:
        evaluate_replay_window(valid, cutoff=datetime(2026, 8, 29, 16, 0))
    assert cutoff_error.value.reason_code == "REPLAY_CUTOFF_TIMEZONE_REQUIRED"
    with pytest.raises(ReplayWindowContractError) as output_error:
        evaluate_replay_window(valid, cutoff=CUTOFF, output_json="report.json")
    assert output_error.value.reason_code == "REPLAY_OUTPUT_PAIR_REQUIRED"
    assert _as_number("1.25") == 1.25
    assert _as_number("nan") is None
    assert _ratio(1, 3) == 0.333333
    assert _ratio(1, 0) is None
