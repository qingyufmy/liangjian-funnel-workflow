from datetime import datetime, timezone

import pytest

from liangjian_funnel.pipeline.data_readiness import (
    BLOCKED,
    READY,
    READY_DEGRADED,
    evaluate_data_readiness,
    namespace_readiness,
)


AS_OF = datetime(2026, 8, 28, 15, 10, tzinfo=timezone.utc)


def test_readiness_freezes_stable_hash_and_full_market_counts():
    coverage = {
        "daily": {"symbols": 4017, "max_timestamp": "2026-08-28T07:00:00+00:00"},
        "financial": {"symbols": 3900, "max_published_at": "2026-08-27T00:00:00+00:00"},
    }
    first = evaluate_data_readiness(coverage, expected_symbols=4017, as_of=AS_OF)
    second = evaluate_data_readiness(coverage, expected_symbols=4017, as_of=AS_OF)
    assert first.status == READY
    assert first.version_hash == second.version_hash
    assert first.namespaces[0].covered_symbols == 4017


def test_daily_is_hard_gate_but_financial_gap_is_degraded():
    missing_daily = evaluate_data_readiness(
        {"daily": {"symbols": 3000}, "financial": {"symbols": 4017}},
        expected_symbols=4017,
        as_of=AS_OF,
    )
    assert missing_daily.status == BLOCKED
    assert "DAILY_BARS_COVERAGE_BELOW_THRESHOLD" in missing_daily.reason_codes

    partial_financial = evaluate_data_readiness(
        {"daily": {"symbols": 4017}, "financial": {"symbols": 3000}},
        expected_symbols=4017,
        as_of=AS_OF,
    )
    assert partial_financial.status == READY_DEGRADED
    assert partial_financial.ready is True


def test_supplemental_namespace_preserves_criticality():
    report = evaluate_data_readiness(
        {"daily": {"symbols": 10}, "financial": {"symbols": 10}},
        expected_symbols=10,
        as_of=AS_OF,
        supplemental=(namespace_readiness(
            "cninfo_recent",
            covered_symbols=0,
            expected_symbols=10,
            required=False,
            minimum_ratio=0.8,
            unavailable_reason="CNINFO_TEMPORARILY_UNAVAILABLE",
        ),),
    )
    assert report.status == READY_DEGRADED
    assert report.reason_codes == ("CNINFO_TEMPORARILY_UNAVAILABLE",)


def test_readiness_rejects_invalid_contract_inputs():
    with pytest.raises(ValueError):
        evaluate_data_readiness({}, expected_symbols=0, as_of=AS_OF)
    with pytest.raises(ValueError):
        evaluate_data_readiness({}, expected_symbols=1, as_of=datetime(2026, 8, 28))
