import copy
from datetime import datetime,timedelta

import pytest

from liangjian_funnel.evaluation.ablation.strategy_accumulation import canonical_sha256 as sha
from liangjian_funnel.evaluation.ablation.shadow_week import build_shadow_week,render_shadow_week
from liangjian_funnel.evaluation.ablation.shadow_day_adapter import build_shadow_day
from test_w3_shadow_day_adapter import source

DATES=["2026-10-12","2026-10-13","2026-10-14","2026-10-15","2026-10-16"]


def rows():
    daily=build_shadow_day(*source(),as_of="2026-10-12T15:30:00+08:00")
    result=[]
    for day in DATES:
        report=copy.deepcopy(daily)
        report["trade_date"]=day;report["as_of"]=day+"T15:30:00+08:00"
        result.append({"report":report,"report_sha256":sha(report)})
    return result


def test_week_has_five_complete_days_not_twenty_sample_profitability():
    inputs=rows();before=copy.deepcopy(inputs)
    result=build_shadow_week(inputs,trading_days=DATES,as_of="2026-10-16T16:30:00+08:00")
    assert inputs==before and result["status"]=="COMPLETE"
    group=result["groups"]["V1"]["TREND_MA5"]
    assert group["activated_plan_days"]==5 and group["first_trigger_plan_days"]==0
    assert group["returns"]["T1"]["win_rate"] is None
    assert result["positive_expectancy"]=="NOT_ESTABLISHED"
    assert "非账户收益" in render_shadow_week(result)


def test_missing_day_is_unknown_not_zero():
    result=build_shadow_week(rows()[:-1],trading_days=DATES,as_of="2026-10-16T16:30:00+08:00")
    assert result["status"]=="DATA_LIMITED"
    assert result["groups"]["V1"]["TREND_MA5"]["first_trigger_plan_days"] is None
    assert result["missing_days"]==[DATES[-1]]


@pytest.mark.parametrize("mutation",["hash","duplicate_day","wrong_kind","future","calendar_gap"])
def test_week_rejects_conflicting_hash_identity_and_calendar(mutation):
    inputs=rows();days=DATES.copy()
    if mutation=="hash":inputs[0]["report_sha256"]="0"*64
    if mutation=="duplicate_day":inputs.append(copy.deepcopy(inputs[0]))
    if mutation=="wrong_kind":inputs[0]["report"]["source_kind"]="REALTIME_PAPER_LEDGER"
    if mutation=="future":inputs[0]["report"]["as_of"]="2026-10-19T16:30:00+08:00"
    if mutation=="calendar_gap":days.pop(1)
    if mutation in ("wrong_kind","future"):inputs[0]["report_sha256"]=sha(inputs[0]["report"])
    with pytest.raises(ValueError):build_shadow_week(inputs,trading_days=days,as_of="2026-10-16T16:30:00+08:00")
