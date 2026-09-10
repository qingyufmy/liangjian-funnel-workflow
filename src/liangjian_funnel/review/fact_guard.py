"""Server-owned A5 facts. Model hypotheses cannot rewrite counts or lineage."""
from __future__ import annotations

from typing import Any, Mapping


def normalize_quality(value: Mapping[str, Any]) -> dict[str, Any]:
    quality = dict(value)
    reasons = list(quality.get("limitation_reasons") or [])
    components = []
    for item in quality.get("missing_components") or []:
        if str(item).startswith("A5_INDEPENDENT_VERIFICATION_"):
            reasons.append(item)
        else:
            components.append(item)
    quality["missing_components"] = components
    quality["limitation_reasons"] = list(dict.fromkeys(reasons))
    return quality


def verification_totals(facts: Mapping[str, Any]) -> dict[str, Any]:
    plans = (facts.get("independent_verification") or {}).get("a4", {}).get("plans") or []
    fields: dict[str, dict[str, int]] = {}
    for plan in plans:
        for side in ("cross_source_field_checks", "archived_tdx_field_checks"):
            for name, values in (plan.get(side) or {}).items():
                counts = fields.setdefault(f"{side}:{name}", {"compared_count": 0, "mismatch_count": 0, "not_comparable_count": 0})
                for key in counts:
                    counts[key] += int(values.get(key) or 0)
    return {"fields": fields,
        "expected_plan_observations": sum(int(p.get("expected_observation_minutes") or 0) for p in plans),
        "recorded_plan_observations": sum(int(p.get("recorded_observation_minutes") or 0) for p in plans),
        "omission_count": sum(int(p.get("orchestration_omission_count") or 0) for p in plans),
        "verified_plan_count": len(plans),
        "scope_verified": bool(plans)
            and len(plans) == int((facts.get("metrics") or {}).get("a3_plan_count", len(plans)))
            and all("expected_observation_minutes" in p and "recorded_observation_minutes" in p for p in plans)}


def reconcile_report(report: Any, facts: Mapping[str, Any]) -> list[str]:
    """Reconcile factual fields; preserve original model output in its archive.

    No strategy parameters, decisions, prices or source snapshots are changed.
    Free-form hypotheses remain proposals, never confirmed numeric evidence.
    """
    notes = []
    metrics = facts.get("metrics") or {}
    verification = facts.get("independent_verification") or {}
    totals = verification_totals(facts)
    quality = normalize_quality(facts.get("data_quality") or {})
    themes = {r.get("theme_id"): r.get("theme_name") for r in (facts.get("a2") or {}).get("themes", [])}
    counterexamples = {r.get("symbol"): r for r in verification.get("counterexamples", [])}
    for item in report.missed_opportunity_reviews:
        row = counterexamples.get(item.symbol)
        if not row:
            continue
        item.name = str(row.get("name") or item.name)
        item.theme = str(row.get("theme_name") or themes.get(row.get("theme_id")) or row.get("theme_id") or "未映射")
        value = row.get("intraday_return")
        if isinstance(value, (int, float)):
            item.observed_performance = f"截至复盘时点相对昨收{value:+.2%}；不是策略已实现收益"
    if counterexamples:
        notes.append("反例名称、主题和表现按冻结事实回填，模型不得重新映射行业。")

    def component_fiction(text: str) -> bool:
        return not quality.get("missing_components") and (
            "A5_INDEPENDENT_VERIFICATION_DEGRADED" in text
            or ("组件" in text and ("缺失" in text or "补充" in text or "集成" in text)))

    removed = [r for r in report.core_defects if r.layer == "ORCHESTRATOR" and component_fiction(r.problem)]
    report.core_defects = [r for r in report.core_defects if r not in removed]
    proposals = []
    for proposal in report.improvement_proposals:
        text = " ".join(str(getattr(proposal, k)) for k in ("hypothesis", "proposed_change", "success_criteria"))
        if proposal.target == "ORCHESTRATOR" and component_fiction(text):
            removed.append(proposal)
            continue
        if proposal.target == "A4" and proposal.type == "DATA_FIX" and ("成交量" in text or "VOLUME" in text):
            proposal.hypothesis = "成交量差异成因尚未确认，需分别核对首分钟归属、手数取整、行情修订及实际单位；差异不等于单位转换错误。"
            proposal.proposed_change = "先建立逐时间戳差异分类与原值追踪；只有实际单位或时间归属证据确认适配缺陷后，才实施对应修复，不统一清洗差异或放宽比较容差。"
            proposal.validation_method = "按相同时间戳保留两源原值、既有容差和完整差异计数，分类后用冻结策略复算验证动作影响；不覆盖原始判断。"
            proposal.success_criteria = "全部差异有可追溯原值和分类，能够复现；可比字段单独验收，来源未提供的金额继续标为不可比较，不要求凭空补出。"
        proposals.append(proposal)
    report.improvement_proposals = proposals
    if removed:
        notes.append("已拦截把核验降级状态误当程序组件缺失的缺陷或提案。")
    if metrics.get("a4_m15_macd_applicable_plan_count") == 0:
        report.unresolved_questions = [q for q in report.unresolved_questions if not (
            ("M15" in q.question or "15分钟" in q.question) and "MACD" in q.question)]
        notes.append("无适用520计划不构成15分钟MACD预热故障。")

    if totals["scope_verified"]:
        aggregate_terms = ("成交量", "金额", "VOLUME", "AMOUNT", "分钟", "理论值")
        def aggregate_claim(text: str) -> bool:
            # Never erase exit/T+1 or indicator findings just because their
            # descriptions also mention a minute timeframe or volume.
            protected = ("MACD", "KDJ", "T+1", "退出", "卖出", "减仓", "止损", "生命周期")
            return any(t in text for t in aggregate_terms) and not any(t in text for t in protected)
        retained_defects = [s for s in report.a4_review.defects if not aggregate_claim(s)]
        retained_limits = [s for s in report.a4_review.data_limitations if not aggregate_claim(s)]
        expected = totals["expected_plan_observations"]
        recorded = totals["recorded_plan_observations"]
        missing = totals["omission_count"]
        summary = (f"共{metrics.get('a4_monitor_observation_count', 0)}条判断记录，不是同等数量的交易分钟；"
            f"按激活窗口应观察{expected}条，实际{recorded}条，编排遗漏{missing}条。"
            f"有效事件{metrics.get('a4_effective_event_count', 0)}个、生命周期{metrics.get('a4_lifecycle_count', 0)}个。")
        defects = []
        limits = []
        labels = {"OPEN": "开盘价", "HIGH": "最高价", "LOW": "最低价", "CLOSE": "收盘价", "VOLUME": "成交量", "AMOUNT": "成交金额"}
        for side, name in (("cross_source_field_checks", "两源比较"), ("archived_tdx_field_checks", "归档对通达信")):
            mismatch_parts = []
            missing_parts = []
            for field, label in labels.items():
                counts = totals["fields"].get(f"{side}:{field}", {})
                if counts.get("mismatch_count"):
                    mismatch_parts.append(f"{label}{counts['mismatch_count']}/{counts['compared_count']}组")
                if counts.get("not_comparable_count"):
                    missing_parts.append(f"{label}{counts['not_comparable_count']}组")
            if mismatch_parts:
                defects.append(name + "存在差异：" + "、".join(mismatch_parts) + "；成因需分类核验。")
            if missing_parts:
                limits.append(name + "不可比较：" + "、".join(missing_parts) + "；不能算作不匹配。")
        if missing:
            defects.insert(0, f"按计划激活窗口发现{missing}条编排遗漏。")
        report.a4_review.summary = summary
        report.a4_review.defects = (retained_defects + defects)[:5]
        report.a4_review.strengths = [f"按计划激活窗口完成{recorded}/{expected}条观察记录核对。"]
        report.a4_review.data_limitations = [*retained_limits, *limits, "不同来源的差异可能重叠，不能相加为独立异常数量；价格一致不代表策略动作已全部异源复算。"][:8]
        report.a4_review.verdict = "NEEDS_ATTENTION" if defects or retained_defects else "DATA_LIMITED" if limits or retained_limits else "HEALTHY"
        # Aggregate A4 facts are code-owned; detailed signal/lifecycle reviews
        # remain in their separate evidence-bound section.
        report.core_defects = [r for r in report.core_defects if not (
            r.layer == "A4" and aggregate_claim(r.problem)
            and not any(e.startswith(("A4:EVENT:", "A4:LIFECYCLE:", "A4:SIGNAL:")) for e in r.evidence_ids))]
        if defects and len(report.core_defects) < 8:
            from .daily import A5Defect
            report.core_defects.append(A5Defect(layer="A4", severity="HIGH" if missing else "MEDIUM",
                confidence="HIGH", blocked_by_data=True, problem=" ".join(defects)[:500],
                evidence_ids=[str(verification.get("a4", {}).get("evidence_id") or "METRICS:DAILY")]))
        report.executive_summary = (f"A2聚焦{metrics.get('a2_focus_count', 0)}只、观察{metrics.get('a2_watch_count', 0)}只；"
            f"A3正式计划{metrics.get('a3_plan_count', 0)}个。{summary}"
            "核验差异和不可比较字段见下文；单日强势反例不能确认门槛错误或授权修改策略。")
        if defects or limits:
            report.overall_verdict = "NEEDS_ATTENTION" if report.overall_verdict != "INCIDENT" else "INCIDENT"
        notes.append("摘要和A4聚合事实由本次冻结计数生成，未采用模型推算的理论分钟或历史差异数字。")
    if not report.unresolved_questions:
        from .daily import A5UnresolvedQuestion
        report.unresolved_questions = [A5UnresolvedQuestion(question="当前差异是否影响完整策略动作？",
            reason="INSUFFICIENT_SAMPLE", resolution="按冻结输入与明确的数据修订版本分别复算，保留原始判断。")]
    return notes
