"""Five-session shadow report; no paper ledger, account P&L or parameter choice."""
from datetime import date

from .shadow_day_adapter import PROFILES,VARIANTS,VERSION,_stamp,_statistics
from .strategy_accumulation import canonical_sha256 as sha
from ...runtime.calendar import ExchangeTradingCalendar


def build_shadow_week(inputs, *, trading_days, as_of):
    now=_stamp(as_of)
    if len(trading_days)!=5 or len(set(trading_days))!=5:
        raise ValueError("FIVE_EXPLICIT_SESSIONS_REQUIRED")
    calendar=ExchangeTradingCalendar()
    days=[date.fromisoformat(d) for d in trading_days]
    if (days!=sorted(days) or not all(calendar.is_trading_day(d) for d in days)
            or any(calendar.next_trading_day(a)!=b for a,b in zip(days,days[1:]))
            or _stamp(trading_days[-1]+"T15:00:00+08:00")>now):
        raise ValueError("WEEK_CALENDAR_OR_CLOSED_TIME_INVALID")
    by_day={}
    for item in inputs:
        report=item["report"]
        day=report.get("trade_date")
        if sha(report)!=item.get("report_sha256"):
            raise ValueError("SHADOW_DAY_REPORT_HASH_MISMATCH")
        if (report.get("schema")!=VERSION or report.get("source_kind")!="REALTIME_SHADOW"
                or report.get("account_pnl") is not None or day not in trading_days
                or _stamp(report["as_of"])>now or day in by_day):
            raise ValueError("SHADOW_WEEK_DAY_IDENTITY_OR_TIME_CONFLICT")
        by_day[day]=report
    missing=sorted(set(trading_days)-set(by_day))
    complete=not missing and all(r["status"]=="COMPLETE" for r in by_day.values())
    groups={}
    for variant in VARIANTS:
        groups[variant]={}
        for profile in PROFILES:
            if profile=="MA520_SWING" and variant not in VARIANTS[:3]:
                groups[variant][profile]={"status":"NOT_APPLICABLE"};continue
            selected=[r["groups"][variant][profile] for r in by_day.values()]
            ready=complete and all(g["status"]=="COMPLETE" for g in selected)
            observations=[];seen=set()
            for day,report in sorted(by_day.items()):
                for obs in report["first_trigger_observations"][variant][profile]:
                    key=(day,obs["plan_id"])
                    if key in seen:raise ValueError("DUPLICATE_WEEK_FIRST_TRIGGER_PLAN_DAY")
                    seen.add(key);observations.append(obs)
            filled=[o for o in observations if o.get("fill_status")=="FILLED"]
            returns={}
            for n in (1,3,5):
                values=[o["outcome"][f"t{n}_close_return"] for o in filled
                    if o["outcome"].get(f"t{n}_close_return") is not None]
                returns[f"T{n}"]=_statistics(values,len(filled))
            costs=[]
            cost_ready=ready and all(g["opportunity_status"]!="UNKNOWN" for g in selected)
            for g in selected:costs.extend(g["opportunity_cost"]["observations"].values())
            groups[variant][profile]={"status":"COMPLETE" if ready else "DATA_LIMITED",
                "observed_activated_plan_days":sum(g["activated_count"] for g in selected),
                "activated_plan_days":sum(g["activated_count"] for g in selected) if ready else None,
                "first_trigger_plan_days":len(observations) if ready else None,
                "fill_plan_days":len(filled) if ready else None,
                "fill_rate":len(filled)/len(observations) if ready and observations else None,
                "returns":returns,"opportunity_cost":_statistics(costs,
                    sum(g["opportunity_cost"]["eligible_count"] for g in selected)) if cost_ready else None}
    proofs=[r["production_equivalence"] for r in by_day.values()]
    return {"schema":"shadow-week/1","source_kind":"REALTIME_SHADOW","trading_days":trading_days,
        "as_of":now.isoformat(),"status":"COMPLETE" if complete else "DATA_LIMITED",
        "missing_days":missing,"groups":groups,"input_report_sha256":{d:sha(r) for d,r in by_day.items()},
        "production_equivalence":"MATCHED" if not missing and all(p["status"]=="MATCHED" for p in proofs) else "DATA_LIMITED",
        "budget_exceeded_minutes":sum(r["budget_exceeded_minutes"] for r in by_day.values()),
        "observed_minutes":sum(r["observed_minutes"] for r in by_day.values()),
        "customer_notifications":"NOT_AUDITED","account_pnl":None,
        "positive_expectancy":"NOT_ESTABLISHED","production_parameter_change":False}


def render_shadow_week(report):
    if report.get("schema")!="shadow-week/1":raise ValueError("SHADOW_WEEK_REQUIRED")
    lines=["# A4 影子周报 "+report["trading_days"][-1],"",
        "REALTIME_SHADOW，与正式模拟账户分开；非账户收益；不自动批准参数。",
        "覆盖："+report["status"]+"；生产外层对照："+report["production_equivalence"]+"。",
        "", "| 变体 | 策略 | 活动计划日 | 首触发 | 模拟成交 | T1样本 | T3样本 | T5样本 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for variant,profiles in report["groups"].items():
        for profile,g in profiles.items():
            if g["status"]=="NOT_APPLICABLE":continue
            lines.append("| "+" | ".join(map(str,[variant,profile,g["activated_plan_days"],
                g["first_trigger_plan_days"],g["fill_plan_days"],
                *(g["returns"][f"T{n}"]["sample_count"] for n in (1,3,5))]))+" |")
    lines.extend(["","缺日/缺腿为null，不补零；不足20成交不报告正式均值/胜率。",
        "价格收益不含费用；五日采集不自动证明正期望；客户通知需独立账本审计。"])
    return "\n".join(lines)+"\n"
