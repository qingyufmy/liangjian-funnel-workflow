"""Read-only Chinese explanation; never imported by execution eligibility gates."""
from collections.abc import Mapping
from typing import Any

A3_REASON_LABELS = {
    "DAILY_SHORT_MACD_NOT_BEARISH": "日线MACD（5、10、5）动能未偏空",
    "DAILY_SHORT_MACD_BEARISH": "日线MACD（5、10、5）快线低于慢线，趋势计划暂不合格",
    "DAILY_SHORT_MACD_MISSING": "缺少可核验的日线MACD（5、10、5）数据",
    "HIGHER_TIMEFRAME_CONDITIONAL_PROBE": "月／周线背景偏弱（非淘汰条件）",
    "TREND_DAILY_PATH_MISSING": "日线主升、突破或强势回踩条件尚未确认",
    "MA520_DEAD_CROSS": "五日与二十日均线死叉",
    "MA520_RIGHT_SIDE_NOT_CONFIRMED": "日线520右侧重启条件尚未确认",
    "TREND_TOP_RISK_CONFIRMED": "量化形态提示趋势顶部风险",
    "THEME_STAGE_NOT_EARLY": "题材阶段不符合龙头策略的早期条件",
    "THEME_STAGE_MISSING": "缺少题材阶段证据，暂不能完成判断",
    "A3_THEME_STAGE_MISSING_OR_AMBIGUOUS": "题材阶段缺失或存在多个冲突记录",
    "A3_THEME_STAGE_UNAVAILABLE": "题材阶段记录不可用",
    "A3_THEME_STAGE_EVIDENCE_INSUFFICIENT": "题材阶段缺少日期、正反证据或来源",
    "FIRST_BOARD_OBSERVE_ONLY": "首板暂列观察，未满足当前龙头接力条件",
    "FOUR_PLUS_BOARD_WATCH_ONLY": "四板及以上处于高位观察，不进入次日接力计划",
    "LEADER_THEME_RETREAT_OR_CLIMAX": "题材处于高潮、分化或退潮阶段，龙头策略停止新增计划",
    "HIGHER_TIMEFRAME_BEARISH": "已闭合周线与日线同时偏空",
    "DAILY_TREND_WEAK": "日线趋势偏弱",
    "HIGH_VOLUME_DISTRIBUTION": "量化形态提示放量派发风险",
    "LEADER_DAILY_TREND_WEAK": "龙头策略所需日线趋势偏弱",
    "A4_RETEST_CONFIRMATION_REQUIRED": "由A4等待盘中回踩确认（非A3淘汰条件）",
    "A4_WILL_CONFIRM_DAILY_MA5_PULLBACK": "由A4确认盘中五日线回踩条件",
    "TREND_DAILY_PATH_CONFIRMED": "日线主升、突破或强势回踩条件",
    "MA520_RIGHT_SIDE_CONFIRMED": "日线520右侧重启条件",
    "THEME_IN_EARLY_CYCLE": "题材处于龙头策略适用阶段",
    "BOARD_NOT_FIRST_OBSERVATION_ONLY": "满足龙头接力而非仅首板观察",
    "HIGHER_TIMEFRAME_NOT_BEARISH": "周线与日线未同时偏空",
    "DAILY_NOT_BEARISH": "日线趋势未偏空",
    "LEADER_DAILY_NOT_BEARISH": "龙头日线趋势未偏空",
    "NOT_DISTRIBUTION": "未出现量化派发风险形态",
    "MARKET_RISK_OFF": "市场处于防守背景",
    "SECTOR_NO_NEW_ENTRY_PRIOR": "板块背景提示谨慎新开仓",
    "A3_REWARD_RISK_BELOW_MINIMUM": "研究盈亏比低于参考下限",
    "A3_STOP_DISTANCE_OUTSIDE_LIMIT": "研究止损距离超出参考区间",
    "A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE": "来自A2观察池，日线技术研究合格；试探执行仍需正式计划和A4确认",
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
