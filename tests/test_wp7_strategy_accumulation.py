import copy
import hashlib
import json

import pytest

from liangjian_funnel.evaluation.ablation.strategy_accumulation import (
    accumulate_strategy_days, render_strategy_accumulation,
)


DATES = [f"2026-09-{i:02d}" for i in range(1, 21)]  # Explicit fixture calendar, not exchange evidence.
PROFILES = ("LEADER_INTRADAY", "MA520_SWING", "TREND_MA5")
COUNT_METRICS = ("activated_plan_days", "triggered_plan_days", "filled_plan_days",
                 "counterfactual_first_triggers", "counterfactual_fill_samples", "flash_veto_events")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def record(day, *, kind="WP1_RESEARCH", n=1):
    research = kind == "WP1_RESEARCH"
    metrics = {m: {"count": n, "basis": m, "source_ref": "fixture:"+m,
                   "evidence_sha256": "a"*64} for m in COUNT_METRICS}
    for m in (("triggered_plan_days", "filled_plan_days", "flash_veto_events") if research
              else ("counterfactual_first_triggers", "counterfactual_fill_samples")):
        metrics[m]["count"] = None
    metrics["opportunity_cost"] = {"sum": 0.2*n, "sample_count": n,
        "eligible_count": n, "basis": "VALIDITY_WINDOW_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE",
        "source_ref": "fixture:opportunity", "evidence_sha256": "b"*64}
    # Opportunity belongs to untriggered plans; fixture uses twice n activations.
    metrics["activated_plan_days"]["count"] = 2*n
    p = {"metrics":metrics, "strategy_versions": {"v1":2*n},
         "version_source_ref":"fixture:versions", "version_evidence_sha256":"c"*64}
    summary = {"schema":"wp7-strategy-day/1", "trade_date":day, "source_kind":kind,
        "source_version":"daily/1", "cohort_id":"fixture-cohort", "strategies":{s:copy.deepcopy(p) for s in PROFILES}}
    statuses={s:{m:("COMPLETE" if v.get("count",0) is not None else "UNKNOWN")
                   for m,v in metrics.items()} for s in PROFILES}
    for st in statuses.values():st["strategy_versions"]="COMPLETE"
    coverage={"schema":"wp7-strategy-day-coverage/1", "trade_date":day,
        "source_kind":kind,"source_version":"daily/1", "cohort_id":"fixture-cohort",
        "summary_sha256":digest(summary), "run_id":"fixture-run:"+day,
        "run_status":"COMPLETE", "pit_status":"MATCHED", "source_input_sha256":"d"*64,
        "strategies":statuses}
    return seal(summary,coverage)


def seal(summary, coverage):
    coverage["summary_sha256"]=digest(summary)
    return {"summary":summary,"summary_sha256":digest(summary),
            "coverage":coverage,"coverage_sha256":digest(coverage)}


def accumulate(rows, days=DATES, **kwargs):
    return accumulate_strategy_days(rows, trading_days=days, calendar_sha256=digest(days),
        as_of=DATES[-1], source_kind="WP1_RESEARCH", source_version="daily/1",
        cohort_id="fixture-cohort", **kwargs)


def group(report):
    return report["strategies"]["TREND_MA5"]


def test_twenty_days_research_is_not_actual_trigger_or_account_pnl():
    result=accumulate([record(d) for d in DATES]); g=group(result)
    assert g["metrics"]["activated_plan_days"]["total"]==40
    assert g["metrics"]["counterfactual_first_triggers"]["total"]==20
    assert g["metrics"]["triggered_plan_days"]["total"] is None
    assert g["metrics"]["filled_plan_days"]["status"]=="UNKNOWN"
    assert g["metrics"]["flash_veto_events"]["total"] is None
    assert g["metrics"]["opportunity_cost"]["mean"]==pytest.approx(.2)
    assert g["strategy_versions"]["distribution"]=={"v1":40}
    assert result["account_pnl"] is None
    assert result["historical_pit_authenticated"] is False


def test_nineteen_samples_are_insufficient_not_twenty_minutes():
    rows=[record(d, n=0 if i==0 else 1) for i,d in enumerate(DATES)]
    g=group(accumulate(rows))
    assert g["metrics"]["counterfactual_fill_samples"]["status"]=="INSUFFICIENT_EVIDENCE"
    assert g["metrics"]["counterfactual_fill_samples"]["total"]==19
    assert g["metrics"]["opportunity_cost"]["mean"] is None


def test_missing_day_keeps_total_null_and_observed_separate():
    r=accumulate([record(d) for d in DATES[1:]])
    assert r["missing_days"]==[DATES[0]]
    m=group(r)["metrics"]["activated_plan_days"]
    assert m["total"] is None and m["observed_total"]==38
    assert m["unknown_days"]==[DATES[0]]


def test_short_calendar_does_not_invent_dates():
    r=accumulate([record(d) for d in DATES[-3:]],days=DATES[-3:])
    assert r["missing_calendar_sessions"]==17
    assert len(r["window_days"])==3
    assert group(r)["metrics"]["activated_plan_days"]["total"] is None


@pytest.mark.parametrize("status_field,status", [("run_status","FAILED"),("run_status","DATA_LIMITED"),("pit_status","UNKNOWN")])
def test_independent_bad_coverage_cannot_promote_numbers(status_field,status):
    rows=[record(d) for d in DATES]; rows[0]["coverage"][status_field]=status
    rows[0]=seal(rows[0]["summary"],rows[0]["coverage"])
    m=group(accumulate(rows))["metrics"]["counterfactual_fill_samples"]
    assert m["total"] is None and m["observed_total"]==19


def test_missing_flash_logs_stay_unknown_even_on_twenty_complete_paper_days():
    rows=[record(d,kind="REALTIME_PAPER_LEDGER") for d in DATES]
    for r in rows:
        r["summary"]["strategies"]["TREND_MA5"]["metrics"]["flash_veto_events"]["count"]=None
        r["coverage"]["strategies"]["TREND_MA5"]["flash_veto_events"]="UNKNOWN"
        r.update(seal(r["summary"],r["coverage"]))
    report=accumulate_strategy_days(rows,trading_days=DATES,calendar_sha256=digest(DATES),
        as_of=DATES[-1],source_kind="REALTIME_PAPER_LEDGER",source_version="daily/1",cohort_id="fixture-cohort")
    g=group(report)
    assert g["metrics"]["triggered_plan_days"]["total"]==20
    assert g["metrics"]["flash_veto_events"]["total"] is None
    assert g["metrics"]["counterfactual_first_triggers"]["total"] is None


@pytest.mark.parametrize("mutation", ["duplicate_day","future_day","noncalendar_day","source_version","source_kind","cohort_id","summary_hash","coverage_hash","coverage_date","coverage_binding","negative","bool_count","float_count","opportunity_nan","bad_basis","missing_metric_ref","versions_total","inconsistent_trigger","duplicate_calendar","unsorted_calendar","calendar_hash"])
def test_refuses_ambiguous_or_tampered_inputs(mutation):
    rows=[record(d) for d in DATES]; days=DATES.copy(); kwargs={}
    s=rows[0]["summary"]; c=rows[0]["coverage"]; p=s["strategies"]["TREND_MA5"]
    if mutation=="duplicate_day": rows.append(copy.deepcopy(rows[0]))
    elif mutation=="future_day":s["trade_date"]="2026-09-21";c["trade_date"]="2026-09-21"
    elif mutation=="noncalendar_day":s["trade_date"]="2026-08-31";c["trade_date"]="2026-08-31"
    elif mutation in ("source_version","source_kind","cohort_id"):s[mutation]="OTHER";c[mutation]="OTHER"
    elif mutation=="summary_hash":rows[0]["summary_sha256"]="0"*64
    elif mutation=="coverage_hash":rows[0]["coverage_sha256"]="0"*64
    elif mutation=="coverage_date":c["trade_date"]=DATES[1]
    elif mutation=="coverage_binding":c["summary_sha256"]="0"*64
    elif mutation in ("negative","bool_count","float_count"):p["metrics"]["activated_plan_days"]["count"]={"negative":-1,"bool_count":True,"float_count":2.0}[mutation]
    elif mutation=="opportunity_nan":p["metrics"]["opportunity_cost"]["sum"]=float("nan")
    elif mutation=="bad_basis":p["metrics"]["counterfactual_first_triggers"]["basis"]="ACTUAL_TRADES"
    elif mutation=="missing_metric_ref":p["metrics"]["counterfactual_fill_samples"]["source_ref"]=""
    elif mutation=="versions_total":p["strategy_versions"]={"v1":999}
    elif mutation=="inconsistent_trigger":p["metrics"]["counterfactual_first_triggers"]["count"]=99
    elif mutation=="duplicate_calendar":days.append(days[-1])
    elif mutation=="unsorted_calendar":days.reverse()
    elif mutation=="calendar_hash":kwargs["calendar_sha256"]="0"*64
    if mutation not in ("summary_hash","coverage_hash","coverage_binding","opportunity_nan"):
        rows[0]=seal(s,c)
    with pytest.raises(ValueError):
        if "calendar_sha256" in kwargs:
            accumulate_strategy_days(rows,trading_days=days,as_of=DATES[-1],source_kind="WP1_RESEARCH",source_version="daily/1",cohort_id="fixture-cohort",**kwargs)
        else:accumulate(rows,days=days)


def test_rolls_twenty_calendar_sessions_and_excludes_older_day():
    days=["2026-08-31"]+DATES
    r=accumulate([record(d) for d in days],days=days)
    assert r["window_days"]==DATES
    assert r["outside_window_days"]==["2026-08-31"]
    assert group(r)["metrics"]["activated_plan_days"]["total"]==40


def test_partial_opportunity_coverage_does_not_report_subset_mean():
    rows=[record(d) for d in DATES]
    for r in rows:
        p=r["summary"]["strategies"]["TREND_MA5"]["metrics"]["opportunity_cost"]
        p["eligible_count"]=2
        r["summary"]["strategies"]["TREND_MA5"]["metrics"]["activated_plan_days"]["count"]=3
        r["summary"]["strategies"]["TREND_MA5"]["strategy_versions"]={"v1":3}
        r.update(seal(r["summary"],r["coverage"]))
    m=group(accumulate(rows))["metrics"]["opportunity_cost"]
    assert m["sample_count"]==20 and m["eligible_count"]==40
    assert m["mean"] is None and m["status"]=="UNKNOWN"


def test_strategy_version_changes_are_distribution_not_source_schema_switch():
    rows=[record(d) for d in DATES]
    rows[0]["summary"]["strategies"]["TREND_MA5"]["strategy_versions"]={"v2":2}
    rows[0]=seal(rows[0]["summary"],rows[0]["coverage"])
    assert group(accumulate(rows))["strategy_versions"]["distribution"]=={"v1":38,"v2":2}


def test_renderer_has_aggregate_unknown_and_no_minute_or_plan_ids():
    r=accumulate([record(d) for d in DATES]); text=render_strategy_accumulation(r)
    assert "策略累计" in text and "龙头" in text and "520" in text and "趋势" in text
    assert "WP1_RESEARCH" in text and "UNKNOWN" in text and "非账户收益" in text
    assert "fixture-run" not in text and "fixture-cohort" not in text
    assert "fixture:opportunity" not in text


def test_input_is_unchanged_and_metric_denominators_are_bound():
    rows=[record(d) for d in DATES]; before=copy.deepcopy(rows)
    r=accumulate(rows); assert rows==before
    m=group(r)["metrics"]["counterfactual_fill_samples"]
    assert len(m["evidence"])==20
    assert m["denominator_basis"]=="counterfactual_fill_samples"
    assert all(e["summary_sha256"] and e["coverage_sha256"] and e["evidence_sha256"] for e in m["evidence"])


def test_untriggered_opportunity_denominator_cannot_include_triggered_plans():
    r=record(DATES[0]); r["summary"]["strategies"]["TREND_MA5"]["metrics"]["opportunity_cost"]["eligible_count"]=2
    r=seal(r["summary"],r["coverage"])
    with pytest.raises(ValueError):accumulate([r])


def test_finite_daily_sums_cannot_overflow_window_json():
    rows=[record(d) for d in DATES]
    for r in rows:
        r["summary"]["strategies"]["TREND_MA5"]["metrics"]["opportunity_cost"]["sum"]=1e308
        r.update(seal(r["summary"],r["coverage"]))
    with pytest.raises(ValueError):accumulate(rows)


def test_empty_coverage_has_null_denominators_not_fabricated_zero():
    r=accumulate([]); g=group(r)
    assert g["metrics"]["opportunity_cost"]["sample_count"] is None
    assert g["metrics"]["opportunity_cost"]["eligible_count"] is None
    assert g["metrics"]["counterfactual_fill_samples"]["observed_total"] is None


def test_known_zero_requires_complete_proof_and_still_insufficient():
    g=group(accumulate([record(d,n=0) for d in DATES]))
    assert g["metrics"]["counterfactual_fill_samples"]["total"]==0
    assert g["metrics"]["counterfactual_fill_samples"]["status"]=="INSUFFICIENT_EVIDENCE"


def test_missing_profile_or_version_is_not_zero_distribution():
    rows=[record(d) for d in DATES]; rows[0]["summary"]["strategies"].pop("LEADER_INTRADAY")
    rows[1]["summary"]["strategies"]["TREND_MA5"].pop("strategy_versions")
    for r in rows[:2]:r.update(seal(r["summary"],r["coverage"]))
    report=accumulate(rows)
    assert report["strategies"]["LEADER_INTRADAY"]["metrics"]["activated_plan_days"]["total"] is None
    assert group(report)["strategy_versions"]["distribution"] is None


def test_research_cannot_claim_real_trigger_even_with_bound_coverage():
    r=record(DATES[0]);r["summary"]["strategies"]["TREND_MA5"]["metrics"]["triggered_plan_days"]["count"]=1
    r=seal(r["summary"],r["coverage"])
    with pytest.raises(ValueError):accumulate([r])


def test_report_json_is_finite_and_renderer_preserves_version_identity():
    r=accumulate([record(d) for d in DATES]);json.dumps(r,allow_nan=False)
    assert "v1=40" in render_strategy_accumulation(r)


def test_unknown_flash_does_not_claim_all_metrics_complete():
    r=accumulate([record(d) for d in DATES])
    assert r["coverage_status"]=="DATA_LIMITED"
    assert r["run_pit_coverage_status"]=="DECLARED_COMPLETE"
