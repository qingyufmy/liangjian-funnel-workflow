"""Explain frozen selection predicates without inventing a counterfactual trade."""
from collections.abc import Mapping


def a2_gate_evidence(row):
    behavior = row.get("behavior_type_decision") or {}
    if not isinstance(behavior, Mapping) or not behavior:
        return {}
    facets = behavior.get("required_facets") or {}
    return {
        "decision_as_of": row.get("decision_as_of"),
        "local_partition": row.get("local_partition"),
        "classification_basis": behavior.get("decision_basis") or {},
        "facets": {key: {field: value.get(field) for field in
                   ("available", "met", "value", "reason", "as_of", "source_refs")}
                   for key, value in facets.items() if isinstance(value, Mapping)},
        "ladder": (behavior.get("evidence") or {}).get("ladder_structure"),
        "known_negatives": behavior.get("known_negatives") or [],
        "data_gaps": behavior.get("data_gaps") or [],
        "selected_board_match": row.get("selected_board_theme_match"),
        "selected_board": row.get("selected_board") or {},
        "reason_semantics": "Diagnostic reasons are not necessarily blocking gates; identifiability alone does not decide admission.",
    }


def counterexample_selection_audit(candidate, technical):
    quant = candidate.get("quant_gate_evidence") or {}
    facts = {"a2": quant, "a3": technical or {},
             "scope": "FROZEN_PRODUCTION_PREDICATES_NOT_COUNTERFACTUAL_BUY_POINT"}
    parts = []
    if technical:
        names = {"LEADER_INTRADAY": "龙头", "TREND_MA5": "趋势五日线", "MA520_SWING": "520"}
        profile = names.get(technical.get("strategy_profile"), "未识别策略")
        unmet = technical.get("unmet_conditions") or []
        labels = {"THEME_IN_EARLY_CYCLE": "题材尚未满足启动或加速阶段",
                  "BOARD_NOT_HIGH_RISK_4_PLUS": "已处于四板及以上高风险阶段，仅作观察",
                  "BOARD_NOT_FIRST_OBSERVATION_ONLY": "首板尚未满足启动试探条件，仅作观察",
                  "TREND_DAILY_PATH_CONFIRMED": "未确认日线主升、平台突破、强势回踩或创新高路径"}
        parts.append(f"当时核验的是{profile}策略")
        parts.extend(labels.get(code, str(code)) for code in unmet)
        if technical.get("eligibility") == "WATCH":
            parts.append("量化结论为继续观察，不是已确认技术失效")
    else:
        medium = (quant.get("facets") or {}).get("medium_term_trend") or {}
        value = medium.get("value") or {}
        if medium.get("available") is True and medium.get("met") is False:
            score, threshold = value.get("score"), value.get("threshold")
            if isinstance(score, (int, float)) and isinstance(threshold, (int, float)):
                parts.append(f"趋势分类代理值{score:.2f}，未达到现行分类线{threshold:g}")
                if value.get("source_factor") == "trend_strength_proxy":
                    parts.append("该值是20日涨幅横截面排名，不等于均线趋势或主营质量；其口径需单独评估")
        reasons = candidate.get("selection_reasons") or candidate.get("reason_codes") or []
        if "A2_TREND_OUTSIDE_SELECTED_BOARD_TOP5" in reasons:
            parts.append("趋势类别已确认，但未匹配当时前五且净流入为正的板块范围")
    facts["explanation"] = "；".join(parts)
    facts["requires_rule_reproduction"] = not bool(parts)
    facts["investment_value_not_proven"] = True
    return facts
