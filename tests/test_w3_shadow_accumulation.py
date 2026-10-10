"""Shadow observations must never be labelled real production trades."""
import copy

import pytest

from liangjian_funnel.evaluation.ablation.strategy_accumulation import (
    accumulate_strategy_days, render_strategy_accumulation,
)
from test_wp7_strategy_accumulation import DATES, digest, record, seal


def shadow(day):
    row = record(day, kind="REALTIME_SHADOW")
    for source in (row["summary"]["strategies"], row["coverage"]["strategies"]):
        source.pop("LEADER_INTRADAY")
    for profile in row["summary"]["strategies"]:
        row["summary"]["strategies"][profile]["metrics"]["flash_veto_events"]["count"] = None
        row["coverage"]["strategies"][profile]["flash_veto_events"] = "UNKNOWN"
    return seal(row["summary"], row["coverage"])


def run(rows, *, cohort="fixture-cohort", days=DATES):
    return accumulate_strategy_days(rows, trading_days=days, calendar_sha256=digest(days),
        as_of=days[-1], source_kind="REALTIME_SHADOW", source_version="daily/1", cohort_id=cohort)


def test_shadow_complete_twenty_days_without_fabricated_flash_or_leader():
    report = run([shadow(d) for d in DATES])
    assert report["coverage_status"] == "DECLARED_COMPLETE"
    assert report["excluded_profiles"] == ["LEADER_INTRADAY"]
    assert report["strategies"]["TREND_MA5"]["metrics"]["filled_plan_days"]["total"] == 20
    assert report["strategies"]["TREND_MA5"]["metrics"]["flash_veto_events"]["total"] is None
    text = render_strategy_accumulation(report)
    assert "影子首触发" in text and "影子模拟成交" in text
    assert "实际入场信号" not in text and "实际模拟成交" not in text
    assert "龙头：不适用" in text and report["account_pnl"] is None


def test_shadow_cannot_mix_actual_paper_or_research():
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        run([record(d, kind="REALTIME_PAPER_LEDGER") for d in DATES])
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        run([record(d) for d in DATES])


def test_shadow_different_variant_cohorts_do_not_mix():
    rows = [shadow(d) for d in DATES]
    row = rows[0]
    row["summary"]["cohort_id"] = row["coverage"]["cohort_id"] = "V2"
    rows[0] = seal(row["summary"], row["coverage"])
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        run(rows)


@pytest.mark.parametrize("metric", ["counterfactual_first_triggers", "flash_veto_events"])
def test_shadow_does_not_fake_flash_or_offline_counterfactual_counts(metric):
    rows = [shadow(d) for d in DATES]
    rows[0]["summary"]["strategies"]["TREND_MA5"]["metrics"][metric]["count"] = 1
    rows[0] = seal(rows[0]["summary"], rows[0]["coverage"])
    with pytest.raises(ValueError, match="CANNOT_MIX"):
        run(rows)


def test_missing_shadow_day_is_unknown_and_five_days_not_positive_expectancy():
    rows = [shadow(d) for d in DATES[-5:]]
    before = copy.deepcopy(rows)
    report = run(rows)
    assert rows == before and report["coverage_status"] == "DATA_LIMITED"
    count = report["strategies"]["TREND_MA5"]["metrics"]["filled_plan_days"]
    assert count["total"] is None and count["observed_total"] == 5
    assert count["status"] == "UNKNOWN"
