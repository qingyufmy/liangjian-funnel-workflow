"""Read-only Chinese explanation; never imported by execution eligibility gates."""
from collections.abc import Mapping
from typing import Any

A3_REASON_LABELS = {
    "HIGHER_TIMEFRAME_CONDITIONAL_PROBE": "月／周线背景偏弱（非淘汰条件）",
    "TREND_DAILY_PATH_MISSING": "日线主升、突破或强势回踩条件尚未确认",
    "MA520_DEAD_CROSS": "五日与二十日均线死叉",
    "MA520_RIGHT_SIDE_NOT_CONFIRMED": "日线520右侧重启条件尚未确认",
    "TREND_TOP_RISK_CONFIRMED": "量化形态提示趋势顶部风险",
    "THEME_STAGE_NOT_EARLY": "题材阶段不符合龙头策略的早期条件",
    "THEME_STAGE_MISSING": "缺少题材阶段证据，暂不能完成判断",
    "FIRST_BOARD_OBSERVE_ONLY": "首板暂列观察，未满足当前龙头接力条件",
    "HIGHER_TIMEFRAME_BEARISH": "已闭合周线与日线同时偏空",
    "DAILY_TREND_WEAK": "日线趋势偏弱",
    "HIGH_VOLUME_DISTRIBUTION": "量化形态提示放量派发风险",
    "LEADER_DAILY_TREND_WEAK": "龙头策略所需日线趋势偏弱",
    "A4_RETEST_CONFIRMATION_REQUIRED": "由A4等待盘中回踩确认（非A3淘汰条件）",
}
BACKGROUND = {"HIGHER_TIMEFRAME_CONDITIONAL_PROBE", "MARKET_RISK_OFF", "SECTOR_NO_NEW_ENTRY_PRIOR"}
DEFERRED = {"A4_RETEST_CONFIRMATION_REQUIRED", "A4_WILL_CONFIRM_DAILY_MA5_PULLBACK"}
HARD = {"MA520_DEAD_CROSS", "TREND_TOP_RISK_CONFIRMED", "HIGHER_TIMEFRAME_BEARISH", "DAILY_TREND_WEAK", "HIGH_VOLUME_DISTRIBUTION", "LEADER_DAILY_TREND_WEAK"}


def a3_nonqualified_explanation(row: Mapping[str, Any]) -> dict[str, str]:
    eligibility = row.get("deterministic_eligibility") or row.get("eligibility")
    state = {"WATCH": "技术待观察", "REJECTED": "技术否决", "DATA_GAP": "证据待核"}.get(str(eligibility), "分类待核")
    codes = list(dict.fromkeys([*(row.get("reason_codes") or []), *(row.get("deterministic_reason_codes") or [])]))
    veto = list(row.get("deterministic_veto_conditions") or row.get("veto_conditions") or [])
    veto.extend(code for code in codes if code in HARD and code not in veto)
    blocking = list(dict.fromkeys([*veto, *(code for code in codes if code not in BACKGROUND | DEFERRED)]))
    reason = A3_REASON_LABELS.get(blocking[0], "当前记录缺少可读的阻断说明") if blocking else "当前记录缺少可核验的阻断说明"
    if eligibility == "DATA_GAP" and "THEME_STAGE_NOT_EARLY" in codes and not veto:
        reason = "题材阶段判断存在数据缺口；仍需核对首板等其他条件"
    return {
        "状态": state, "主要原因": reason,
        "其他条件": "；".join(A3_REASON_LABELS.get(code, "存在未翻译的条件") for code in blocking[1:]),
        "背景风险": "；".join(A3_REASON_LABELS.get(code, "市场背景需关注") for code in codes if code in BACKGROUND),
        "盘中待确认": "；".join(A3_REASON_LABELS.get(code, "由A4确认盘中条件") for code in codes if code in DEFERRED),
    }
