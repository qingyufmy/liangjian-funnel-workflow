"""Pure W3 day projection of a verified independent shadow ledger.

The explicit census and production rows are caller-frozen inputs. Binding their
hashes does not authenticate original provider identities or customer channels.
No providers, stores, strategies, models, notifications or filesystem writes.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import math
import statistics
from zoneinfo import ZoneInfo

from .strategy_accumulation import canonical_sha256 as sha
from ...runtime.calendar import ExchangeTradingCalendar
from ...runtime.simulation import _first_complete_bar_end

VERSION = "shadow-day-adapter/1"
PROFILES = ("TREND_MA5", "MA520_SWING")
VARIANTS = tuple(f"V{i}" for i in range(1, 7))
_VIEW_META = {"snapshot_canonical_sha256", "ok", "hash_chain_status", "mirror_status",
    "mirror_file_sha256", "source_db_sha256", "source_db_sha256_status"}


def _stamp(value):
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("AWARE_TIMESTAMP_REQUIRED")
    return parsed.astimezone(ZoneInfo("Asia/Shanghai"))


def _index(records):
    result = {}
    for row in records:
        key = (row["plan_id"], _stamp(row["minute"]).isoformat())
        if key in result:
            raise ValueError("DUPLICATE_BASELINE_WINDOW")
        result[key] = row
    return result


def production_equivalence(production, shadow_baselines):
    """Compare full frozen outer events, NOT inner strategy warmup actions."""
    left, right = _index(production), _index(shadow_baselines)
    missing = sorted(set(left)-set(right))
    extra = sorted(set(right)-set(left))
    mismatch, unknown = [], []
    for key in sorted(set(left)&set(right)):
        a, b = left[key], right[key]
        fields = ("symbol", "action", "first_cause", "event_id", "event_sha256")
        if (not _baseline_ready(a) or not _baseline_ready(b)
                or not a.get("event_id") or not a.get("event_sha256")):
            unknown.append(key)
        elif any(a.get(k) != b.get(k) for k in fields):
            mismatch.append(key)
    status = ("MISMATCH" if mismatch or extra else "DATA_LIMITED"
        if missing or unknown or not left else "MATCHED")
    return {"schema":"shadow-production-equivalence/1", "status":status,
        "production_count":len(left), "shadow_baseline_count":len(right),
        "difference_count":len(mismatch), "missing_windows":missing, "extra_windows":extra,
        "unknown_windows":unknown, "mismatch_windows":mismatch,
        "production_input_sha256":sha(production), "shadow_input_sha256":sha(shadow_baselines),
        "scope":"FROZEN_OUTER_ACTION_CAUSE_AND_EVENT_IDENTITY_NOT_RECOMPUTED_STRATEGY"}


def _baseline_ready(row):
    statuses = row.get("field_status")
    if statuses == "OK":
        return True
    return (isinstance(statuses,dict)
        and all(statuses.get(k)=="PRESENT" for k in ("outer_baseline","action","event_id","event_sha256"))
        and (statuses.get("reason") in {"PRESENT","NULL"} or statuses.get("reason_codes") in {"PRESENT","EMPTY"}))


def _statistics(values, expected):
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
        raise ValueError("NONFINITE_SHADOW_RETURN")
    sufficient = len(values) >= 20 and len(values) == expected
    return {"sample_count":len(values), "missing_count":expected-len(values),
        "observed_mean":statistics.mean(values) if values else None,
        "observed_median":statistics.median(values) if values else None,
        "mean":statistics.mean(values) if sufficient else None,
        "median":statistics.median(values) if sufficient else None,
        "win_rate":sum(v>0 for v in values)/len(values) if sufficient else None,
        "status":"SUFFICIENT_SAMPLE" if sufficient else "UNKNOWN"
            if len(values)!=expected else "INSUFFICIENT_EVIDENCE",
        "basis":"PRICE_ONLY_EXCLUDES_FEES_NOT_ACCOUNT_PNL"}


def _validity_opportunities(archive, plans, now):
    """Full closed-minute census; never use a partial high as a whole window."""
    if archive is None:
        return {}, None
    body = {k:v for k,v in archive.items() if k!="sha256"}
    if (archive.get("schema")!="shadow-validity-price-archive/1"
            or not archive.get("source_ref") or sha(body)!=archive.get("sha256")):
        raise ValueError("VALIDITY_PRICE_ARCHIVE_HASH_REQUIRED")
    allowed_symbols = {p["symbol"] for p in plans.values()}
    bars = {}
    for bar in archive["bars"]:
        end, arrival = _stamp(bar["bar_end"]), _stamp(bar["captured_at"])
        key = (bar["symbol"],end.isoformat())
        values = [bar.get(k) for k in ("open","high","low","close")]
        if (key in bars or bar["symbol"] not in allowed_symbols
                or end.date().isoformat()!=archive["trade_date"] or end>arrival or arrival>now
                or bar.get("interval")!="1m" or bar.get("adjust_mode")!="none"
                or not bar.get("source_ref") or len(str(bar.get("source_input_sha256","")))!=64
                or any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in values)
                or not bar["low"]<=min(bar["open"],bar["close"])<=max(bar["open"],bar["close"])<=bar["high"]):
            raise ValueError("VALIDITY_PRICE_ROW_INVALID")
        bars[key] = bar
    costs = {}
    for pid,plan in plans.items():
        begin = _stamp(plan["activated_at"])
        finish = min(_stamp(plan["expires_at"]),_stamp(plan.get("invalidated_at",plan["expires_at"])))
        if begin.date().isoformat()!=archive["trade_date"]:
            raise ValueError("VALIDITY_PRICE_DATE_CONFLICT")
        upper = plan.get("entry_zone_upper")
        if finish>now or type(upper) not in (int,float) or not math.isfinite(upper) or upper<=0:
            continue
        expected, clock = [], begin
        while (clock := _first_complete_bar_end(clock)) is not None and clock<=finish:
            expected.append((plan["symbol"],clock.isoformat()))
        if expected and all(key in bars for key in expected):
            costs[pid] = max(bars[key]["high"] for key in expected)/upper-1
    return costs, archive["sha256"]


def build_shadow_day(view, census, production, *, as_of, price_archive=None):
    now = _stamp(as_of)
    if (view.get("ok") is not True or view.get("schema") != "shadow-evidence/1"
            or view.get("hash_chain_status") != "MATCHED"
            or sha({k:v for k,v in view.items() if k not in _VIEW_META}) != view.get("snapshot_canonical_sha256")):
        raise ValueError("SHADOW_SOURCE_HASH_OR_CHAIN_UNPROVEN")
    if census.get("schema") != "shadow-day-census/1" or not census.get("source_ref"):
        raise ValueError("EXPLICIT_ACTIVATED_CENSUS_REQUIRED")
    day = date.fromisoformat(census["trade_date"])
    if day > now.date():
        raise ValueError("FUTURE_TRADE_DATE")
    version = census.get("variant_set_version")
    if version != "a4-shadow-variants/1":
        raise ValueError("SHADOW_VARIANT_SET_UNSUPPORTED")
    plans = {}
    for plan in census["plans"]:
        pid = plan["plan_id"]
        if pid in plans:
            raise ValueError("DUPLICATE_ACTIVATED_PLAN")
        start, end = _stamp(plan["activated_at"]), _stamp(plan["expires_at"])
        if start.date()!=day or end.date()!=day or start>=end:
            raise ValueError("ACTIVATED_PLAN_VALIDITY_INVALID")
        if plan["strategy_profile"] not in (*PROFILES,"LEADER_INTRADAY"):
            raise ValueError("PROFILE_UNSUPPORTED")
        plans[pid] = plan
    expected = _index(census["expected_windows"])
    for (pid, minute) in expected:
        if pid not in plans or _stamp(minute).date()!=day:
            raise ValueError("CENSUS_WINDOW_IDENTITY_INVALID")
        plan = plans[pid]
        end = min(_stamp(plan["expires_at"]),_stamp(plan.get("invalidated_at",plan["expires_at"])))
        if not _stamp(plan["activated_at"]) <= _stamp(minute) <= end:
            raise ValueError("CENSUS_WINDOW_OUTSIDE_PLAN_VALIDITY")
    minutes, baselines = {}, []
    for item in view["minutes"]:
        minute = _stamp(item["minute"])
        if minute > now:
            raise ValueError("FUTURE_SHADOW_MINUTE")
        if minute.date()!=day:
            continue
        if minute.isoformat() in minutes:
            raise ValueError("DUPLICATE_SHADOW_MINUTE")
        if item.get("variant_set_version")!=version:
            raise ValueError("MINUTE_VARIANT_VERSION_CONFLICT")
        minutes[minute.isoformat()] = item
        baselines.extend(item.get("baseline_records", []))
    relevant_production = [r for r in production if _stamp(r["minute"]).date()==day]
    proof = production_equivalence(relevant_production, baselines)
    expected_keys = set(expected)
    production_keys = set(_index(relevant_production))
    gaps = []
    if view.get("mirror_status") not in (None,"NOT_REQUESTED","SYNCED"):
        gaps.append("SHADOW_MIRROR_NOT_SYNCED")
    if expected_keys!=production_keys:
        gaps.append("ACTIVATED_CENSUS_PRODUCTION_WINDOWS_MISMATCH")
    if proof["status"]!="MATCHED":
        gaps.append("PRODUCTION_EQUIVALENCE_"+proof["status"])
    if not expected_keys and plans:
        gaps.append("ACTIVE_PLANS_WITHOUT_EXPECTED_WINDOWS")
    for minute in {k[1] for k in expected_keys}:
        item = minutes.get(minute)
        pids = {p for p,m in expected_keys if m==minute and plans[p]["strategy_profile"] in PROFILES}
        requested = sum(6 if plans[p]["strategy_profile"]=="TREND_MA5" else 3 for p in pids)
        if (item is None or item.get("status")!="OK"
                or item.get("evaluated_variant_count")!=requested
                or item.get("requested_variant_count")!=requested
                or item.get("evaluated_plan_count")!=len(pids)):
            gaps.append("INCOMPLETE_SHADOW_MINUTE:"+minute)
    identities = {x["plan_id"]:x for x in view["price_limit_evidence"] if x["trade_date"]==str(day)}
    for pid, p in plans.items():
        if p["strategy_profile"] not in PROFILES:
            continue
        identity = identities.get(pid)
        if (identity is None or identity.get("limits",{}).get("status")!="KNOWN"
                or identity.get("original_evidence",{}).get("symbol")!=p["symbol"]
                or _stamp(identity["captured_at"])>_stamp(p["activated_at"])):
            gaps.append("PIT_IDENTITY_UNPROVEN:"+pid)
    by_variant = {v:{p:[] for p in PROFILES} for v in VARIANTS}
    seen = set()
    for entry in view["signals"]:
        row = entry["signal"]
        minute = _stamp(row["minute"])
        if minute>now:
            raise ValueError("FUTURE_SHADOW_SIGNAL")
        if minute.date()!=day:
            continue
        if entry.get("source_kind")!="REALTIME_SHADOW" or row.get("variant_set_version")!=version:
            raise ValueError("SHADOW_COHORT_OR_VERSION_CONFLICT")
        if entry["id"] in seen:
            raise ValueError("DUPLICATE_SHADOW_SIGNAL")
        seen.add(entry["id"])
        pid, variant, profile = row["plan_id"], row["variant_id"], row["profile"]
        if (pid not in plans or profile!=plans[pid]["strategy_profile"] or row["symbol"]!=plans[pid]["symbol"]
                or variant not in VARIANTS or profile not in PROFILES
                or (profile=="MA520_SWING" and variant not in VARIANTS[:3])):
            raise ValueError("SHADOW_SIGNAL_PLAN_IDENTITY_CONFLICT")
        if (pid, minute.isoformat()) not in expected_keys:
            raise ValueError("SHADOW_SIGNAL_OUTSIDE_CENSUS")
        if row.get("status")!="OK":
            gaps.append("SHADOW_INPUT_OR_EVALUATION_LIMITED:"+entry["id"])
        outcome = entry.get("outcome")
        if outcome and outcome.get("available_at") and _stamp(outcome["available_at"])>now:
            raise ValueError("FUTURE_SHADOW_OUTCOME")
        if outcome and outcome.get("fill_status")=="FILLED":
            fill_time = _stamp(outcome.get("fill_bar_end"))
            if fill_time.date()!=day or fill_time>now:
                raise ValueError("FILL_TIME_INVALID_OR_FUTURE")
            available = _stamp(outcome["available_at"]) if outcome.get("available_at") else None
            calendar, horizon = ExchangeTradingCalendar(), day
            for n in range(1,6):
                horizon = calendar.next_trading_day(horizon)
                if n in (1,3,5) and outcome.get(f"t{n}_close_return") is not None:
                    close = _stamp(horizon.isoformat()+"T15:00:00+08:00")
                    if available is None or close>available or close>now:
                        raise ValueError("HORIZON_NOT_CLOSED_OR_ARRIVAL_UNPROVEN")
        by_variant[variant][profile].append(entry)
    for pid, plan in plans.items():
        applicable = VARIANTS if plan["strategy_profile"]=="TREND_MA5" else VARIANTS[:3]
        if plan["strategy_profile"] not in PROFILES:
            continue
        for variant in applicable:
            if not any(r["signal"]["plan_id"]==pid for r in by_variant[variant][plan["strategy_profile"]]):
                gaps.append("SHADOW_INITIAL_STATE_MISSING:"+pid+":"+variant)
    complete = not gaps and bool(expected_keys)
    opportunities, archive_hash = _validity_opportunities(price_archive,plans,now)
    groups, envelopes, observations = {}, [], {}
    census_hash, input_hash = sha(census), view["snapshot_canonical_sha256"]
    for variant in VARIANTS:
        groups[variant], observations[variant], summaries, declarations = {}, {}, {}, {}
        for profile in PROFILES:
            applicable = profile=="TREND_MA5" or variant in VARIANTS[:3]
            population = [p for p in plans.values() if p["strategy_profile"]==profile] if applicable else []
            rows = by_variant[variant][profile]
            triggered = [r for r in rows if r["event_kind"]=="FIRST_TRIGGER"]
            if len({r["signal"]["plan_id"] for r in triggered})!=len(triggered):
                raise ValueError("DUPLICATE_FIRST_TRIGGER_PLAN_DAY")
            filled = [r for r in triggered if (r.get("outcome") or {}).get("fill_status")=="FILLED"]
            triggered_ids = {r["signal"]["plan_id"] for r in triggered}
            eligible_ids = {p["plan_id"] for p in population}-triggered_ids
            known_costs = {pid:opportunities[pid] for pid in sorted(eligible_ids) if pid in opportunities}
            cost_ready = complete and len(known_costs)==len(eligible_ids)
            cost_sum = sum(known_costs.values()) if known_costs or not eligible_ids else None
            cost_status = "UNKNOWN" if not cost_ready else "SUFFICIENT_SAMPLE" if len(known_costs)>=20 else "INSUFFICIENT_EVIDENCE"
            causes = Counter()
            for r in rows:
                reasons = r["signal"].get("variant_conditions",{}).get("reason_codes") or []
                if reasons: causes[reasons[0]]+=1
            returns = {f"T{n}":_statistics([r["outcome"][f"t{n}_close_return"] for r in filled
                if r["outcome"].get(f"t{n}_close_return") is not None],len(filled)) for n in (1,3,5)}
            group = {"status":"NOT_APPLICABLE" if not applicable else "COMPLETE" if complete else "DATA_LIMITED",
                "activated_count":len(population),"observed_first_trigger_count":len(triggered),
                "observed_fill_count":len(filled),"first_trigger_count":len(triggered) if complete else None,
                "fill_count":len(filled) if complete else None,
                "fill_rate":len(filled)/len(triggered) if complete and triggered else None,
                "first_cause_state_changes":dict(causes), "cause_count_basis":"INITIAL_AND_STATE_CHANGES_NOT_MINUTES",
                "returns":returns,"mae_median":statistics.median([r["outcome"]["mae"] for r in filled
                    if r["outcome"].get("mae") is not None]) if any(r["outcome"].get("mae") is not None for r in filled) else None,
                "mfe_median":statistics.median([r["outcome"]["mfe"] for r in filled
                    if r["outcome"].get("mfe") is not None]) if any(r["outcome"].get("mfe") is not None for r in filled) else None,
                "opportunity_cost":{"observed_sum":cost_sum,"sample_count":len(known_costs),
                    "eligible_count":len(eligible_ids),"mean":cost_sum/len(known_costs)
                        if cost_ready and len(known_costs)>=20 else None,
                    "observations":known_costs,"basis":"VALIDITY_WINDOW_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE"},
                "opportunity_status":cost_status}
            groups[variant][profile] = group
            observations[variant][profile] = [{"signal_id":r["id"],"plan_id":r["signal"]["plan_id"],
                "trigger_at":r.get("trigger_at"),"fill_status":(r.get("outcome") or {}).get("fill_status"),
                "outcome":r.get("outcome")} for r in triggered]
            values = {"activated_plan_days":len(population),"triggered_plan_days":len(triggered) if complete else None,
                "filled_plan_days":len(filled) if complete else None,"flash_veto_events":None,
                "counterfactual_first_triggers":None,"counterfactual_fill_samples":None}
            metrics = {k:{"count":v,"basis":k,"source_ref":census["source_ref"],"evidence_sha256":input_hash}
                for k,v in values.items()}
            metrics["opportunity_cost"] = {"sum":cost_sum if cost_ready else None,
                "sample_count":len(known_costs),"eligible_count":len(eligible_ids),
                "basis":"VALIDITY_WINDOW_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE",
                "source_ref":price_archive["source_ref"] if price_archive else census["source_ref"],
                "evidence_sha256":archive_hash or input_hash}
            summaries[profile] = {"metrics":metrics,"strategy_versions":dict(Counter(p["strategy_version"] for p in population)),
                "version_source_ref":census["source_ref"],"version_evidence_sha256":census_hash}
            declarations[profile] = {k:"COMPLETE" if v is not None and complete else "UNKNOWN" for k,v in values.items()}
            declarations[profile].update(opportunity_cost="COMPLETE" if cost_ready else "UNKNOWN",
                strategy_versions="COMPLETE" if complete else "UNKNOWN")
        identity = {"trade_date":str(day),"source_kind":"REALTIME_SHADOW","source_version":VERSION,
            "cohort_id":version+":"+variant+":PUBLISHED_PLANS"}
        summary = {"schema":"wp7-strategy-day/1",**identity,"strategies":summaries}
        coverage = {"schema":"wp7-strategy-day-coverage/1",**identity,"summary_sha256":sha(summary),
            "run_id":"shadow:"+str(day)+":"+variant,"run_status":"COMPLETE" if complete else "DATA_LIMITED",
            "pit_status":"MATCHED" if not any(g.startswith("PIT_") for g in gaps) else "UNKNOWN",
            "source_input_sha256":input_hash,"strategies":declarations}
        envelopes.append({"summary":summary,"summary_sha256":sha(summary),"coverage":coverage,"coverage_sha256":sha(coverage)})
    return {"schema":VERSION,"trade_date":str(day),"as_of":now.isoformat(),"source_kind":"REALTIME_SHADOW",
        "status":"COMPLETE" if complete else "DATA_LIMITED","source_input_sha256":input_hash,
        "census_sha256":census_hash,"production_equivalence":proof,"groups":groups,"day_envelopes":envelopes,
        "gap_codes":sorted(set(gaps)),"expected_window_count":len(expected_keys),
        "budget_exceeded_minutes":sum(m.get("shadow_budget_exceeded_count",0)>0 for m in minutes.values()),
        "observed_minutes":len(minutes),"customer_notifications":"NOT_AUDITED","account_pnl":None,
        "source_authenticated":False,"original_pit_source_authenticated":False,
        "price_archive_sha256":archive_hash,"first_trigger_observations":observations}


def render_shadow_day(report):
    if report.get("schema")!=VERSION:
        raise ValueError("SHADOW_DAY_REPORT_REQUIRED")
    lines = ["# A4 影子日报 "+report["trade_date"],"",
        "来源 REALTIME_SHADOW；与正式计划、WP1 研究、实时模拟账户分别统计，非账户收益。",
        "覆盖："+report["status"]+"；生产外层动作/首因对照："+report["production_equivalence"]["status"]+"。",
        "收益为价格回报，不含费用；不足20成交样本不报告正式均值或胜率。缺腿null不补零。",
        "", "| 变体 | 策略 | 活动 | 首触发 | 成交 | T1样本 | T3样本 | T5样本 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for variant, profiles in report["groups"].items():
        for profile, group in profiles.items():
            if group["status"]=="NOT_APPLICABLE": continue
            cells = [variant,profile,str(group["activated_count"]),str(group["first_trigger_count"]),
                str(group["fill_count"]),*(str(group["returns"][f"T{n}"]["sample_count"]) for n in (1,3,5))]
            lines.append("| "+" | ".join(cells)+" |")
    lines.extend(["","首因计数只含首次状态及状态变化，不冒充逐分钟门槛次数。",
        "机会成本只使用完整有效期归档；部分窗口为UNKNOWN，不补零。客户飞书零投递需独立通知审计。"])
    return "\n".join(lines)+"\n"
