"""Pure hash-bound WP7 chapter; caller declarations are not PIT authentication.

No stores, files, providers, models, or existing WP1 runner imports. Distinct
research cohorts and realtime paper ledgers must be accumulated separately.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
import hashlib
import json
import math
import re

PROFILES = {"LEADER_INTRADAY": "龙头", "MA520_SWING": "520", "TREND_MA5": "趋势"}
COUNT_METRICS = ("activated_plan_days", "triggered_plan_days", "filled_plan_days",
                 "counterfactual_first_triggers", "counterfactual_fill_samples", "flash_veto_events")
METRICS = (*COUNT_METRICS, "opportunity_cost")
WINDOW = 20
MIN_SAMPLES = 20
_SHA = re.compile(r"^[0-9a-f]{64}$")
_OPPORTUNITY_BASIS = "VALIDITY_WINDOW_HIGH_OVER_ENTRY_ZONE_UPPER_MINUS_ONE"
_KINDS = {"WP1_RESEARCH", "REALTIME_PAPER_LEDGER", "REALTIME_SHADOW"}


def _fail(reason):
    raise ValueError(reason)


def canonical_sha256(value):
    """Canonical JSON payload SHA, not original transport bytes or provenance."""
    try:
        body = json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError):
        _fail("CANONICAL_JSON_INVALID")
    return hashlib.sha256(body).hexdigest()


def _day(value):
    try:
        if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
            _fail("ISO_TRADE_DATE_REQUIRED")
    except ValueError:
        _fail("ISO_TRADE_DATE_REQUIRED")
    return value


def _count(value):
    if type(value) is not int or value < 0:
        _fail("NONNEGATIVE_INTEGER_COUNT_REQUIRED")
    return value


def _hash(value):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        _fail("SHA256_REQUIRED")
    return value


def _text(value):
    if not isinstance(value, str) or not value.strip():
        _fail("NONEMPTY_IDENTITY_REQUIRED")
    return value


def _bound_metric(metric, basis):
    if metric.get("basis") != basis:
        _fail("METRIC_DENOMINATOR_BASIS_MISMATCH")
    _text(metric.get("source_ref"))
    _hash(metric.get("evidence_sha256"))


def _validate_record(record, *, calendar, as_of, kind, version, cohort):
    if not isinstance(record, Mapping):
        _fail("DAY_ENVELOPE_REQUIRED")
    summary, coverage = record.get("summary"), record.get("coverage")
    if not isinstance(summary, Mapping) or not isinstance(coverage, Mapping):
        _fail("INDEPENDENT_SUMMARY_AND_COVERAGE_REQUIRED")
    for key, value in (("summary", summary), ("coverage", coverage)):
        if _hash(record.get(key + "_sha256")) != canonical_sha256(value):
            _fail("DAY_PAYLOAD_HASH_MISMATCH")
    if (summary.get("schema") != "wp7-strategy-day/1"
            or coverage.get("schema") != "wp7-strategy-day-coverage/1"):
        _fail("DAY_SCHEMA_MISMATCH")
    day = _day(summary.get("trade_date"))
    if day > as_of:
        _fail("FUTURE_TRADE_DATE")
    if day not in calendar:
        _fail("DAY_NOT_IN_EXPLICIT_CALENDAR")
    expected = {"trade_date": day, "source_kind": kind, "source_version": version, "cohort_id": cohort}
    if any(summary.get(k) != v or coverage.get(k) != v for k, v in expected.items()):
        _fail("DAY_SOURCE_OR_COVERAGE_IDENTITY_MISMATCH")
    if coverage.get("summary_sha256") != record["summary_sha256"]:
        _fail("COVERAGE_SUMMARY_BINDING_MISMATCH")
    _text(coverage.get("run_id"))
    _hash(coverage.get("source_input_sha256"))
    if coverage.get("run_status") not in {"COMPLETE", "DATA_LIMITED", "FAILED"}:
        _fail("RUN_COVERAGE_STATUS_INVALID")
    if coverage.get("pit_status") not in {"MATCHED", "UNKNOWN"}:
        _fail("PIT_DECLARATION_STATUS_INVALID")
    strategies, declarations = summary.get("strategies"), coverage.get("strategies")
    if not isinstance(strategies, Mapping) or not isinstance(declarations, Mapping):
        _fail("STRATEGY_SUMMARIES_REQUIRED")
    if (set(strategies) | set(declarations)) - set(PROFILES):
        _fail("UNSUPPORTED_STRATEGY_PROFILE")
    if kind == "REALTIME_SHADOW" and "LEADER_INTRADAY" in strategies:
        _fail("SHADOW_LEADER_PROFILE_NOT_APPLICABLE")
    prohibited = ({"triggered_plan_days", "filled_plan_days", "flash_veto_events"} if kind == "WP1_RESEARCH"
                  else {"counterfactual_first_triggers", "counterfactual_fill_samples",
                        *({"flash_veto_events"} if kind == "REALTIME_SHADOW" else set())})
    for profile, payload in strategies.items():
        if not isinstance(payload, Mapping) or not isinstance(payload.get("metrics"), Mapping):
            _fail("STRATEGY_METRICS_REQUIRED")
        metrics = payload["metrics"]
        if set(metrics) - set(METRICS):
            _fail("UNSUPPORTED_METRIC")
        statuses = declarations.get(profile, {})
        if not isinstance(statuses, Mapping) or any(v not in {"COMPLETE", "UNKNOWN"} for v in statuses.values()):
            _fail("METRIC_COVERAGE_STATUS_INVALID")
        if set(statuses) - {*METRICS, "strategy_versions"}:
            _fail("UNSUPPORTED_METRIC_COVERAGE")
        values = {}
        for metric, entry in metrics.items():
            if not isinstance(entry, Mapping):
                _fail("METRIC_OBJECT_REQUIRED")
            if metric == "opportunity_cost":
                if entry.get("sum") is not None:
                    _bound_metric(entry, _OPPORTUNITY_BASIS)
                    if type(entry["sum"]) not in (int, float) or not math.isfinite(entry["sum"]):
                        _fail("FINITE_OPPORTUNITY_SUM_REQUIRED")
                    n, eligible = _count(entry.get("sample_count")), _count(entry.get("eligible_count"))
                    if n > eligible or (n == 0 and entry["sum"] != 0):
                        _fail("OPPORTUNITY_SAMPLE_DENOMINATOR_INVALID")
            elif entry.get("count") is not None:
                if metric in prohibited:
                    _fail("RESEARCH_AND_REALTIME_METRICS_CANNOT_MIX")
                _bound_metric(entry, metric)
                values[metric] = _count(entry["count"])
        activation = values.get("activated_plan_days")
        trigger_key = "counterfactual_first_triggers" if kind == "WP1_RESEARCH" else "triggered_plan_days"
        fill_key = "counterfactual_fill_samples" if kind == "WP1_RESEARCH" else "filled_plan_days"
        if activation is not None and values.get(trigger_key, 0) > activation:
            _fail("TRIGGERS_EXCEED_ACTIVATED_PLAN_DAYS")
        if trigger_key in values and values.get(fill_key, 0) > values[trigger_key]:
            _fail("FILLS_EXCEED_TRIGGER_DENOMINATOR")
        opportunity = metrics.get("opportunity_cost", {})
        if (opportunity.get("sum") is not None and activation is not None and trigger_key in values
                and opportunity["eligible_count"] != activation - values[trigger_key]):
            _fail("OPPORTUNITY_ELIGIBLE_NOT_UNTRIGGERED_PLAN_DAYS")
        versions = payload.get("strategy_versions")
        if versions is not None:
            if not isinstance(versions, Mapping):
                _fail("STRATEGY_VERSION_DISTRIBUTION_REQUIRED")
            for key, count in versions.items():
                _text(key)
                if not re.fullmatch(r"[A-Za-z0-9_./:+-]{1,96}", key):
                    _fail("STRATEGY_VERSION_ID_INVALID")
                _count(count)
            _text(payload.get("version_source_ref"))
            _hash(payload.get("version_evidence_sha256"))
            if activation is not None and sum(versions.values()) != activation:
                _fail("STRATEGY_VERSION_PLAN_DAY_DENOMINATOR_MISMATCH")
    return day


def accumulate_strategy_days(records: Sequence[Mapping], *, trading_days: Sequence[str],
                             calendar_sha256: str, as_of: str, source_kind: str,
                             source_version: str, cohort_id: str) -> dict:
    """Accumulate a single source/schema/cohort over the last 20 named sessions.

    Counts are deduplicated PLAN-DAYS by caller contract, not minutes or unique
    plans across twenty dates. No adapter converts existing WP1 groups for you.
    All input days are validated, including days outside the rolling window.
    """
    as_of = _day(as_of)
    _text(source_version)
    _text(cohort_id)
    if source_kind not in _KINDS:
        _fail("SOURCE_KIND_INVALID")
    if isinstance(records, (str, bytes, Mapping)) or not isinstance(records, Sequence):
        _fail("DAY_ENVELOPE_SEQUENCE_REQUIRED")
    if isinstance(trading_days, (str, bytes)) or not isinstance(trading_days, Sequence):
        _fail("EXPLICIT_CALENDAR_REQUIRED")
    days = [_day(value) for value in trading_days]
    if days != sorted(set(days)):
        _fail("CALENDAR_MUST_BE_SORTED_UNIQUE")
    if _hash(calendar_sha256) != canonical_sha256(days):
        _fail("CALENDAR_HASH_MISMATCH")
    by_day = {}
    for record in records:
        day = _validate_record(record, calendar=set(days), as_of=as_of,
                               kind=source_kind, version=source_version, cohort=cohort_id)
        if day in by_day:
            _fail("DUPLICATE_DAY_SUMMARY")
        by_day[day] = record
    window = [day for day in days if day <= as_of][-WINDOW:]
    short = WINDOW - len(window)
    missing = [day for day in window if day not in by_day]
    groups = {}
    for profile in PROFILES:
        aggregated = {}
        for metric in METRICS:
            evidence, unknown, total, n, eligible = [], [], 0, 0, 0
            for day in window:
                row = by_day.get(day)
                payload = row["summary"]["strategies"].get(profile, {}) if row else {}
                entry = payload.get("metrics", {}).get(metric, {})
                coverage = row["coverage"] if row else {}
                declaration = coverage.get("strategies", {}).get(profile, {}).get(metric)
                value = entry.get("sum" if metric == "opportunity_cost" else "count")
                ready = (coverage.get("run_status") == "COMPLETE" and coverage.get("pit_status") == "MATCHED"
                         and declaration == "COMPLETE" and value is not None)
                if not ready:
                    unknown.append(day)
                    continue
                total += value
                if metric == "opportunity_cost":
                    n += entry["sample_count"]
                    eligible += entry["eligible_count"]
                evidence.append({"trade_date": day, "run_id": coverage["run_id"],
                    "summary_sha256": row["summary_sha256"], "coverage_sha256": row["coverage_sha256"],
                    "source_input_sha256": coverage["source_input_sha256"],
                    "source_ref": entry["source_ref"], "evidence_sha256": entry["evidence_sha256"]})
            complete = not unknown and not short
            if metric == "opportunity_cost":
                if not math.isfinite(total):
                    _fail("AGGREGATE_OPPORTUNITY_NOT_FINITE")
                complete = complete and n == eligible
                status = "UNKNOWN" if not complete else "SUFFICIENT_SAMPLE" if n >= MIN_SAMPLES else "INSUFFICIENT_EVIDENCE"
                aggregated[metric] = {"mean": total/n if complete and n >= MIN_SAMPLES else None,
                    "observed_sum": total if evidence else None, "sample_count": n if evidence else None,
                    "eligible_count": eligible if evidence else None,
                    "denominator_basis": _OPPORTUNITY_BASIS, "status": status,
                    "unknown_days": unknown, "evidence": evidence}
            else:
                aggregated[metric] = {"total": total if complete else None,
                    "observed_total": total if evidence else None,
                    "sample_count": total if evidence else None,
                    "denominator_basis": metric,
                    "status": "UNKNOWN" if not complete else "SUFFICIENT_SAMPLE" if total >= MIN_SAMPLES else "INSUFFICIENT_EVIDENCE",
                    "unknown_days": unknown, "evidence": evidence}
        versions, version_unknown, version_evidence = Counter(), [], []
        for day in window:
            row = by_day.get(day)
            payload = row["summary"]["strategies"].get(profile, {}) if row else {}
            coverage = row["coverage"] if row else {}
            if (coverage.get("run_status") == "COMPLETE" and coverage.get("pit_status") == "MATCHED"
                    and coverage.get("strategies", {}).get(profile, {}).get("strategy_versions") == "COMPLETE"
                    and payload.get("strategy_versions") is not None
                    and day not in aggregated["activated_plan_days"]["unknown_days"]):
                versions.update(payload["strategy_versions"])
                version_evidence.append({"trade_date": day, "summary_sha256": row["summary_sha256"],
                    "coverage_sha256": row["coverage_sha256"], "source_ref": payload["version_source_ref"],
                    "evidence_sha256": payload["version_evidence_sha256"]})
            else:
                version_unknown.append(day)
        groups[profile] = {"metrics": aggregated, "strategy_versions": {
            "distribution": dict(sorted(versions.items())) if not version_unknown and not short else None,
            "observed_distribution": dict(sorted(versions.items())) if version_evidence else None,
            "status": "UNKNOWN" if version_unknown or short else "SUFFICIENT_SAMPLE" if sum(versions.values()) >= MIN_SAMPLES else "INSUFFICIENT_EVIDENCE",
            "unknown_days": version_unknown, "denominator_basis": "activated_plan_days", "evidence": version_evidence}}
    run_complete = not (short or missing) and all(
        row["coverage"]["run_status"] == "COMPLETE" and row["coverage"]["pit_status"] == "MATCHED"
        for day, row in by_day.items() if day in window)
    applicable = ("activated_plan_days", "opportunity_cost",
                  *(('flash_veto_events',) if source_kind != 'REALTIME_SHADOW' else ()),
                  *(('counterfactual_first_triggers', 'counterfactual_fill_samples') if source_kind == 'WP1_RESEARCH'
                    else ('triggered_plan_days', 'filled_plan_days')))
    metric_complete = run_complete and all(
        all(group["metrics"][metric]["status"] != "UNKNOWN" for metric in applicable)
        and group["strategy_versions"]["status"] != "UNKNOWN" for profile, group in groups.items()
        if not (source_kind == "REALTIME_SHADOW" and profile == "LEADER_INTRADAY"))
    result = {"schema": "wp7-strategy-accumulation/1", "source_kind": source_kind,
        "source_version": source_version, "cohort_id": cohort_id, "as_of": as_of,
        "calendar_sha256": calendar_sha256, "window_days": window, "window_sessions": WINDOW,
        "missing_calendar_sessions": short, "missing_days": missing,
        "outside_window_days": sorted(set(by_day)-set(window)), "sample_minimum": MIN_SAMPLES,
        "run_pit_coverage_status": "DECLARED_COMPLETE" if run_complete else "DATA_LIMITED",
        "coverage_status": "DECLARED_COMPLETE" if metric_complete else "DATA_LIMITED",
        "strategies": groups, "account_pnl": None, "historical_pit_authenticated": False,
        "source_declarations_authenticated": False,
        "count_unit": "DEDUPE_PLAN_DAY; NOT_MINUTES_NOT_UNIQUE_PLAN_ACROSS_DAYS",
        "evidence_scope": "CALLER_HASH_BOUND_DECLARATIONS_NOT_ORIGINAL_SOURCE_AUTHENTICATION"}
    if source_kind == "REALTIME_SHADOW":
        result["excluded_profiles"] = ["LEADER_INTRADAY"]
        result["excluded_metrics"] = ["flash_veto_events"]
    return result


def render_strategy_accumulation(report: Mapping) -> str:
    """Bounded aggregate-only chapter: no evidence IDs/refs or minute records."""
    if report.get("schema") != "wp7-strategy-accumulation/1":
        _fail("ACCUMULATION_REPORT_SCHEMA_REQUIRED")
    research = report["source_kind"] == "WP1_RESEARCH"
    shadow = report["source_kind"] == "REALTIME_SHADOW"
    trigger = "counterfactual_first_triggers" if research else "triggered_plan_days"
    fill = "counterfactual_fill_samples" if research else "filled_plan_days"
    def cell(metric, key="total"):
        value = metric.get(key)
        return "UNKNOWN" if value is None and metric["status"] == "UNKNOWN" else (
            "null" if value is None else f"{value:.6g}" if isinstance(value, float)
            else str(value)) + f" ({metric['status']})"
    lines = ["## 策略累计", "",
        f"来源：{report['source_kind']}；滚动 {WINDOW} 交易日；已列日历 {len(report['window_days'])} 日；运行/PIT声明 {report['run_pit_coverage_status']}；指标覆盖 {report['coverage_status']}。",
        "研究观察与实时模拟账本分别统计，非账户收益。计数为计划日，不是分钟或跨日唯一计划；hash绑定声明不认证原来源。",
        "", "| 策略 | 激活计划日 | "+("研究 CF 首触发" if research else "影子首触发" if shadow else "实际入场信号计划日")+" | "+("研究填入样本" if research else "影子模拟成交" if shadow else "实际模拟成交计划日")+" | Flash 否决事件 | 未触发机会成本均值 | 策略版本 |",
        "| --- | --- | --- | --- | --- | --- | --- |"]
    for profile, label in PROFILES.items():
        if shadow and profile == "LEADER_INTRADAY":
            lines.append("| 龙头：不适用 | — | — | — | — | — | — |")
            continue
        group = report["strategies"][profile]
        metrics, versions = group["metrics"], group["strategy_versions"]
        # Version IDs are bounded contract metadata, not plan/minute records.
        distribution = versions["distribution"]
        version_cell = "UNKNOWN" if distribution is None else ("; ".join(
            f"{version}={count}" for version, count in distribution.items()) or "{}") + f" ({versions['status']})"
        lines.append("| "+" | ".join((label, cell(metrics["activated_plan_days"]), cell(metrics[trigger]),
            cell(metrics[fill]), cell(metrics["flash_veto_events"]), cell(metrics["opportunity_cost"], "mean"), version_cell))+" |")
    lines.extend(["", "不足 20 样本显示 INSUFFICIENT_EVIDENCE；缺日/缺腿/缺 Flash 日志为 UNKNOWN，未补零。"])
    return "\n".join(lines) + "\n"


__all__ = ["accumulate_strategy_days", "render_strategy_accumulation", "canonical_sha256"]
