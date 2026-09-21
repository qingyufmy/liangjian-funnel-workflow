from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.a1_coverage import (
    A1CoverageLedger,
    A1GapReason,
    CoverageObservation,
    CoverageRequirement,
    materialize_snapshot_coverage,
    minimum_evidence_contract,
)
from liangjian_funnel.pipeline.a1_packet import build_a1_research_packet


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 9, 3, 15, 0, tzinfo=TZ)
ANNOUNCED = AS_OF - timedelta(days=5)


def requirement(
    symbol: str = "600519.SH",
    *,
    field: str = "net_profit",
    period: str = "2026H1",
    applicable: bool = True,
) -> CoverageRequirement:
    return CoverageRequirement(
        symbol=symbol,
        dataset="FINANCIAL_STATEMENT",
        field=field,
        report_period=period,
        as_of=AS_OF,
        source_version="cninfo:v1",
        applicable=applicable,
        expected_unit="CNY",
        expected_currency="CNY",
        expected_scope="CONSOLIDATED",
    )


def observation(value=100.0, **overrides) -> CoverageObservation:
    payload = {
        "requested": True,
        "raw_found": True,
        "parsed": True,
        "value": value,
        "unit": "CNY",
        "currency": "CNY",
        "consolidation_scope": "CONSOLIDATED",
        "announced_at": ANNOUNCED,
        "available_at": ANNOUNCED,
        "feature_ready": True,
        "feature_generation": "feature-1",
        "packet_ready": True,
        "evidence_ref": "fact://cninfo/600519/2026H1",
        "attempted_at": AS_OF,
    }
    payload.update(overrides)
    return CoverageObservation(**payload)


def test_a1_01_parse_mapping_gap_then_fix_reaches_real_packet(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "a1.sqlite3")
    req = requirement()
    broken = ledger.record(req, observation(parsed=False, gap_reason=A1GapReason.SCHEMA_CHANGED.value))
    assert broken["raw_found"] == 1 and broken["parsed"] == 0
    assert broken["gap_reason"] == A1GapReason.SCHEMA_CHANGED.value

    fixed = ledger.record(req, observation())
    assert fixed["packet_ready"] == 1 and fixed["feature_generation"] == "feature-1"
    report = ledger.coverage_report()
    packet = build_a1_research_packet(
        {
            "snapshot_id": "coverage-snapshot",
            "snapshot_hash": "a" * 64,
            "as_of": AS_OF.isoformat(),
            "A1_COVERAGE_PROJECTION": report,
        },
        monthly_strategy_context={"monthly_industry_decisions": []},
    )
    assert packet["coverage_ledger"]["packet_ready"] == 1
    assert packet["coverage"]["field_coverage_status"] == "READY"


@pytest.mark.parametrize(
    ("value", "overrides", "expected_state", "expected_gap"),
    [
        (0, {}, "ZERO", None),
        (-1, {}, "NEGATIVE", None),
        (None, {}, "MISSING", A1GapReason.FIELD_MISSING.value),
        (float("inf"), {}, "NON_FINITE", A1GapReason.FIELD_MISSING.value),
        (100, {"requested": False, "raw_found": False, "gap_reason": A1GapReason.ACCESS_NOT_AUTHORIZED.value}, "POSITIVE", A1GapReason.ACCESS_NOT_AUTHORIZED.value),
        (None, {"raw_found": False, "parsed": False, "gap_reason": A1GapReason.REPORT_NOT_PUBLISHED.value}, "MISSING", A1GapReason.REPORT_NOT_PUBLISHED.value),
    ],
)
def test_a1_02_value_and_gap_states_are_not_collapsed(
    tmp_path: Path,
    value,
    overrides,
    expected_state,
    expected_gap,
) -> None:
    ledger = A1CoverageLedger(tmp_path / f"{expected_state}-{expected_gap}.sqlite3")
    row = ledger.record(requirement(), observation(value, **overrides))
    assert row["value_state"] == expected_state
    assert row["gap_reason"] == expected_gap


def test_a1_02_not_applicable_is_na_not_success(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "na.sqlite3")
    row = ledger.record(requirement(applicable=False), observation())
    assert row["gap_reason"] == A1GapReason.NOT_APPLICABLE.value
    report = ledger.coverage_report()
    assert report["status"] == "N_A"
    assert report["required_field_coverage"] is None
    assert report["not_applicable"] == 1


def test_a1_03_thousand_symbol_backfill_is_fair_under_new_high_priority_work(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "fair.sqlite3")
    universe_keys: set[str] = set()
    for index in range(1000):
        req = requirement(f"{index:06d}.SZ")
        ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
        ledger.enqueue_gap(req.key, priority_class="UNIVERSE", now=AS_OF)
        universe_keys.add(req.key)

    completed_universe: set[str] = set()
    for round_index in range(30):
        # New high-priority work arrives every round; universe aging/quota must
        # still make progress and eventually clear the original 1,000 tasks.
        for extra in range(20):
            req = requirement(f"9{round_index:02d}{extra:03d}.SH", field="revenue")
            ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
            ledger.enqueue_gap(req.key, priority_class="HOLDING", now=AS_OF + timedelta(minutes=round_index))
        plan = ledger.plan_backfill(limit=100, now=AS_OF + timedelta(hours=1, minutes=round_index))
        assert len(plan) <= 100
        for task in plan:
            token = ledger.claim(task["task_key"], owner="worker", now=AS_OF + timedelta(hours=1, minutes=round_index))
            assert token is not None
            assert ledger.complete(
                task["task_key"], owner="worker", fencing_token=token, success=True,
                now=AS_OF + timedelta(hours=1, minutes=round_index),
            )
            if task["coverage_key"] in universe_keys:
                completed_universe.add(task["coverage_key"])
        if completed_universe == universe_keys:
            break
    assert completed_universe == universe_keys


def test_a1_04_restart_resumes_only_deferred_task_and_keeps_success(tmp_path: Path) -> None:
    path = tmp_path / "resume.sqlite3"
    first = A1CoverageLedger(path)
    keys = []
    for index in range(2):
        req = requirement(f"60000{index}.SH")
        first.record(req, observation(requested=False, raw_found=False, parsed=False))
        first.enqueue_gap(req.key, now=AS_OF)
        keys.append(req.key)
    token = first.claim(keys[0], owner="first", now=AS_OF)
    assert token and first.complete(keys[0], owner="first", fencing_token=token, success=True, now=AS_OF)
    restarted = A1CoverageLedger(path)
    plan = restarted.plan_backfill(limit=10, now=AS_OF + timedelta(minutes=1))
    assert [row["task_key"] for row in plan] == [keys[1]]

    # Re-projecting the same frozen attempt after a process restart must not
    # manufacture another failure.
    repeated = restarted.record(
        requirement("600001.SH"),
        observation(requested=False, raw_found=False, parsed=False, attempted_at=AS_OF),
        recorded_at=AS_OF + timedelta(minutes=1),
    )
    repeated_again = restarted.record(
        requirement("600001.SH"),
        observation(requested=False, raw_found=False, parsed=False, attempted_at=AS_OF),
        recorded_at=AS_OF + timedelta(minutes=2),
    )
    assert repeated_again["failure_count"] == repeated["failure_count"]


def test_a1_05_late_announcement_cannot_enter_historical_cutoff(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "pit.sqlite3")
    row = ledger.record(
        requirement(),
        observation(announced_at=AS_OF + timedelta(days=1), available_at=AS_OF + timedelta(days=1)),
    )
    assert row["point_in_time_ready"] == 0
    assert row["gap_reason"] == A1GapReason.TIME_UNVERIFIED.value


def test_a1_06_one_f10_period_cannot_claim_multi_period_or_strict_pit() -> None:
    only_latest = [{"report_period": "2026H1", "point_in_time_ready": True, "announced_at": None, "available_at": None}]
    result = minimum_evidence_contract(only_latest, required_periods=3)
    assert result == {"status": "INCOMPLETE", "reason_code": "TIME_UNVERIFIED", "period_count": 1}


def test_a1_07_negative_is_valid_but_missing_is_unknown(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "negative.sqlite3")
    negative = ledger.record(requirement("600001.SH"), observation(-50))
    missing = ledger.record(requirement("600002.SH"), observation(None))
    assert negative["point_in_time_ready"] == 1 and negative["value_state"] == "NEGATIVE"
    assert missing["point_in_time_ready"] == 0 and missing["gap_reason"] == "FIELD_MISSING"


def test_a1_08_enqueue_is_idempotent_and_retry_after_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "retry.sqlite3"
    ledger = A1CoverageLedger(path)
    req = requirement()
    ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
    assert ledger.enqueue_gap(req.key, now=AS_OF) is True
    assert ledger.enqueue_gap(req.key, now=AS_OF) is False
    token = ledger.claim(req.key, owner="agent-a", now=AS_OF)
    retry_after = AS_OF + timedelta(hours=2)
    assert token and ledger.complete(
        req.key, owner="agent-a", fencing_token=token, success=False,
        reason_code="RATE_LIMITED", retry_after=retry_after, now=AS_OF,
    )
    restarted = A1CoverageLedger(path)
    assert restarted.claim(req.key, owner="agent-b", now=AS_OF + timedelta(hours=1)) is None
    assert restarted.claim(req.key, owner="agent-b", now=retry_after) is not None


def test_a1_08_bounded_runner_persists_worker_result_before_task_success(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "runner.sqlite3")
    req = requirement()
    ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
    ledger.enqueue_gap(req.key, now=AS_OF)

    result = ledger.run_backfill(
        lambda _task: observation(attempted_at=AS_OF + timedelta(minutes=1)),
        owner="local-fixture",
        limit=1,
        now=AS_OF + timedelta(minutes=1),
        retry_budget=2,
    )

    assert result["counts"] == {"planned": 1, "succeeded": 1, "deferred": 0, "skipped": 0}
    assert ledger.get(req.key)["packet_ready"] == 1
    assert ledger.task_report()["by_status"] == {"SUCCEEDED": 1}


def test_a1_09_packet_budget_never_hides_critical_gap_projection(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "packet.sqlite3")
    for index in range(220):
        req = requirement(f"{600000 + index:06d}.SH")
        ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
    report = ledger.coverage_report()
    packet = build_a1_research_packet(
        {"A1_COVERAGE_PROJECTION": report, "as_of": AS_OF.isoformat()},
        monthly_strategy_context={"monthly_industry_decisions": []},
        max_estimated_tokens=1,
    )
    assert len(packet["coverage_ledger"]["critical_gaps"]) == 200
    assert packet["coverage_ledger"]["projected_out_gap_count"] == 20
    assert packet["coverage_ledger"]["projection_reason"] == "PACKET_GAP_LIMIT"
    assert packet["coverage"]["budget"]["within_budget"] is False


def test_a1_10_arbitrary_821_scope_reconciles_every_layer(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "reconcile.sqlite3")
    for index in range(821):
        req = requirement(f"{index:06d}.SZ")
        if index < 800:
            ledger.record(req, observation())
        else:
            ledger.record(req, observation(requested=True, raw_found=True, parsed=False))
    report = ledger.coverage_report()
    assert report["denominator"] == 821
    assert report["layers"]["RAW_FOUND"]["count"] == 821
    assert report["layers"]["PARSED"]["count"] == 800
    assert len(report["gaps"]) == 21


def test_a1_12_failed_fields_remain_in_denominator_and_empty_group_is_na(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "denominator.sqlite3")
    failed = requirement("600001.SH")
    not_applicable = requirement("600002.SH", applicable=False)
    ledger.record(failed, observation(requested=True, raw_found=False, parsed=False))
    ledger.record(not_applicable, observation())

    report = ledger.coverage_report()

    assert report["denominator"] == 1
    assert report["packet_ready"] == 0
    assert report["required_field_coverage"] == 0
    assert report["not_applicable"] == 1
    assert report["denominator_version"]
    assert ledger.coverage_report(consumer_path="NOT_PRESENT")["status"] == "N_A"


def test_coverage_report_can_resolve_latest_version_without_mixing_history(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "versions.sqlite3")
    old = requirement("600001.SH")
    current = CoverageRequirement(
        **{**old.__dict__, "source_version": "cninfo:v2", "as_of": AS_OF + timedelta(days=1)}
    )
    ledger.record(old, observation(), recorded_at=AS_OF)
    ledger.record(current, observation(), recorded_at=AS_OF + timedelta(days=1))

    latest = ledger.latest_source_version(consumer_path="A1_BASE")
    report = ledger.coverage_report(consumer_path="A1_BASE", source_version=latest)

    assert latest == "cninfo:v2"
    assert report["denominator"] == 1


def test_snapshot_projection_inherits_statement_disclosure_time_and_keeps_missing_fields_visible(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "snapshot.sqlite3")
    snapshot = {
        "g0_symbols": ["600519.SH"],
        "COMPANY_FUNDAMENTALS": {
            "600519.SH": {
                "statements": {
                    "INCOME": [{
                        "fiscal_year": 2026,
                        "fiscal_period": "Q2",
                        "report_date_ms": int(ANNOUNCED.timestamp() * 1000),
                        "operating_income": 100.0,
                        "parent_holder_net_profit": -5.0,
                    }],
                    "CASH_FLOW": [],
                },
                "indicators": [{"index_id": "roe", "value": 0}],
            }
        },
        "MAIN_BUSINESS_EVIDENCE": {
            "600519.SH": {
                "latest_full_report_publish_time": ANNOUNCED.isoformat(),
                "evidence": [{
                    "publish_time": ANNOUNCED.isoformat(),
                    "evidence_fetched_at": ANNOUNCED.isoformat(),
                    "source_ref": "cninfo:600519:page:8",
                }],
            }
        },
    }

    report = materialize_snapshot_coverage(
        ledger,
        snapshot,
        as_of=AS_OF,
        source_version="snapshot:test",
    )

    assert report["denominator"] == 8
    assert report["packet_ready"] == 4
    assert report["path_symbol_denominator"] == 1
    assert report["path_research_ready"] == 0
    rows = ledger.rows(source_version="snapshot:test")
    by_field = {row["field"]: row for row in rows}
    assert by_field["parent_holder_net_profit"]["value_state"] == "NEGATIVE"
    assert by_field["parent_holder_net_profit"]["packet_ready"] == 1
    disclosed_at = ANNOUNCED.astimezone(timezone.utc).isoformat()
    assert by_field["parent_holder_net_profit"]["announced_at"] == disclosed_at
    assert by_field["parent_holder_net_profit"]["available_at"] == disclosed_at
    assert by_field["roe"]["value_state"] == "ZERO"
    assert by_field["roe"]["packet_ready"] == 1
    assert by_field["act_cash_flow_net"]["gap_reason"] == "FIELD_MISSING"
    assert by_field["disclosed_business_evidence"]["packet_ready"] == 1


def test_snapshot_projection_rejects_financial_disclosure_after_cutoff(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "future.sqlite3")
    future = AS_OF + timedelta(days=1)
    snapshot = {
        "g0_symbols": ["600519.SH"],
        "COMPANY_FUNDAMENTALS": {"600519.SH": {
            "statements": {"INCOME": [{
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "report_date_ms": int(future.timestamp() * 1000),
                "operating_income": 100.0,
                "parent_holder_net_profit": 10.0,
            }], "CASH_FLOW": []},
            "indicators": [{"index_id": "roe", "value": 8.0}],
        }},
        "MAIN_BUSINESS_EVIDENCE": {},
    }

    materialize_snapshot_coverage(
        ledger, snapshot, as_of=AS_OF, source_version="snapshot:future",
    )

    rows = ledger.rows(source_version="snapshot:future")
    assert {row["gap_reason"] for row in rows if row["field"] in {
        "operating_income", "parent_holder_net_profit", "roe",
    }} == {"TIME_UNVERIFIED"}


def test_ready_projection_reconciles_stale_backfill_task(tmp_path: Path) -> None:
    ledger = A1CoverageLedger(tmp_path / "reconcile.sqlite3")
    req = requirement("600001.SH")
    ledger.record(req, observation(requested=False, raw_found=False, parsed=False))
    assert ledger.enqueue_gap(req.key, now=AS_OF)

    ledger.record(req, observation(10.0), recorded_at=AS_OF + timedelta(minutes=1))

    assert ledger.task_report()["by_status"] == {"SUCCEEDED": 1}
    assert ledger.plan_backfill(limit=10, now=AS_OF + timedelta(minutes=2)) == ()
