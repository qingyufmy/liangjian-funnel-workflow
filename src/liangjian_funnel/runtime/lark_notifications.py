"""Direct Lark notifications for A3 plans, A4 events and A5 reviews."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .lark import LarkConfigurationError, LarkNotifier
from .state import RuntimeStore
from .a4_explain import build_a4_decision_context
from ..pipeline.a3_display import A3_REASON_LABELS
from ..pipeline.presentation import SCHEMA_VERSION as PRESENTATION_SCHEMA_VERSION, project_research_presentation


_ACTION_LABELS = {
    "BUY": "买入",
    "ADD": "加仓",
    "BUY_SIGNAL": "买入触发",
    "ADD_SIGNAL": "加仓触发",
    "SELL_SIGNAL": "离场触发",
    "REDUCE_SIGNAL": "减仓触发",
    "LLM_VETO": "模型否决",
    "PLAN_INVALIDATED": "计划失效",
    "DATA_BLOCK": "数据阻断",
    "JUDGE_TIMEOUT": "模型复核超时",
    "FORCED_RISK_EXIT": "强制风控离场",
}
_A4_TRADE_ACTIONS = frozenset({
    "BUY_SIGNAL", "ADD_SIGNAL", "SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT",
})
# Consequential to the state machine does not mean actionable for the user.
# These remain in monitor_events and A5, without individual Lark cards.
_A4_SILENT_ACTIONS = frozenset({
    "PLAN_INVALIDATED", "LLM_VETO", "NO_ACTION", "START_CONFIRMATION", "EMPTY_SCOPE",
})
_STRATEGY_LABELS = {
    "LEADER_INTRADAY": "龙头战法",
    "MA520_SWING": "520 均线波段",
    "TREND_MA5": "趋势 5 日线",
}

_A5_ATTRIBUTION_LABELS = {
    "GOOD_EXECUTION": "执行符合计划",
    "SELECTION_ERROR": "选股环节存在问题",
    "PLAN_ERROR": "日线计划存在问题",
    "CONFIRM_ERROR": "盘中确认存在问题",
    "DATA_ERROR": "行情或事实数据存在问题",
    "MARKET_REVERSAL": "盘中市场发生反转",
    "DATA_LIMITED": "数据不足，暂缓归因",
    "NOT_AN_ERROR": "不是策略错误",
    "UNCLASSIFIED": "暂未完成归因",
}

_A5_DROP_STAGE_LABELS = {
    "A1": "未进入月度研究池",
    "A2": "未进入板块聚焦池",
    "A3": "未形成日线计划",
    "A4": "盘中条件未触发",
    "UNRESOLVED": "尚未定位漏斗位置",
}

_A5_PROPOSAL_LABELS = {
    "ENGINEERING_FIX": "工程修复",
    "DATA_FIX": "数据修复",
    "SHADOW_TEST": "影子验证·待审建议",
}

_LIVE_MARKET_STATUS_LABELS = {
    "READY": "资料完整",
    "READY_DEGRADED": "降级可用",
    "DATA_BLOCKED": "数据阻断",
    "DATA_BLOCK": "数据阻断",
    "NOT_ATTEMPTED": "未执行",
}
_LIVE_MARKET_SOURCE_LABELS = {
    "HITHINK_FULL_MARKET": "同花顺全市场行情",
    "TENCENT_INDEX_FALLBACK": "腾讯指数兜底行情",
    "NONE": "未取得行情源",
}
_LIVE_MARKET_REASON_LABELS = {
    "A4_LIVE_MARKET_COVERAGE_INSUFFICIENT": "全市场行情覆盖不足",
    "A4_LIVE_MARKET_SOURCE_UNAVAILABLE": "全市场与指数行情均不可用",
    "A4_LIVE_MARKET_FULL_REQUEST_FAILED": "全市场行情请求失败",
    "A4_LIVE_MARKET_INDEX_REQUEST_FAILED": "指数兜底请求失败",
    "FULL_MARKET_READY": "全市场行情已就绪",
    "A4_LIVE_INDEX_CAUTION": "指数显示市场偏弱轮动",
    "A4_LIVE_INDEX_SYSTEMIC_SELL_OFF": "指数确认系统性下跌",
}

_DISPLAY_LABELS = {
    'REQUIRED':'必须满足','PENDING_A4':'等待盘中确认','NOT_PUBLISHED':'未发布',
    'A3_PULLBACK_ZONE':'原计划回踩区间',
    'A3_RIGHT_SIDE_CONFIRMATION':'原计划右侧确认条件',
    'A3_RIGHT_SIDE_CONFIRMATION_MISSING':'原计划缺少右侧确认条件',
    'A3_TRIGGER_ZONE':'原计划触发区间',
    'A4_BEHAVIOR_ROUTE_CONFLICT':'股票行为与执行路线冲突',
    'A4_LIVE_ENTRY_GEOMETRY_READY':'实时入场价格结构已备齐',
    'A4_LIVE_MARKET_BLOCK_WAIT':'实时市场门阻断，等待条件',
    'A4_RUNTIME_POLICY_INVALID':'盘中执行策略配置无效',
    'AS_OF_TIMESTAMP':'事实截止时间',
    'AS_OF_TIMESTAMP_MISSING':'事实截止时间缺失',
    'AWARE_DECISION_TIME':'含时区的决策时间',
    'BEHAVIOR_ROUTE_COMPATIBLE':'股票行为与路线相容',
    'B_CURRENT_QUOTE_OUTSIDE_STOP_OR_NO_CHASE':'当前报价越过B形态止损或禁止追价边界',
    'CURRENT_1M_RISK_LEVEL_TOUCHED':'当前一分钟触及风险边界',
    'CURRENT_CLOSED_1M':'当前闭合一分钟行情',
    'CURRENT_QUOTE_WITHIN_BOUND_B_RISK':'当前报价处于B形态绑定风险范围',
    'DAILY_CLOSE_ABOVE_MA20':'日线收盘价高于二十日均线',
    'DAILY_MA5_ABOVE_MA20':'五日均线高于二十日均线',
    'DAILY_MA5_MA20_ONLY':'仅使用日线五日与二十日均线',
    'DAILY_MA5_MA20_SNAPSHOT':'日线五日与二十日均线快照',
    'DECISION_TIME_INVALID':'决策时间无效',
    'DECISION_TIME_NOT_BEFORE_CLOSED_BAR':'决策时间不早于闭合行情',
    'DECISION_TIME_TIMEZONE_MISSING':'决策时间缺少时区',
    'ENTRY_CONFIRMATION_CONFIG_INVALID':'入场确认配置无效',
    'ENTRY_PATTERN_BINDING_INVALID':'入场形态绑定无效',
    'ENTRY_PATTERN_B_BINDING_VALID':'B形态绑定有效',
    'ENTRY_PATTERN_B_EXECUTION_BOUND':'B形态执行价格已绑定',
    'ENTRY_PATTERN_B_EXECUTION_UNBOUND':'B形态执行价格尚未绑定',
    'EXECUTABLE_NOT_LOCKED_LIMIT_UP':'可执行且非封死涨停',
    'FUTURE_BAR_DETECTED':'发现未来行情',
    'HISTORICAL_5M_PREVIOUS_SESSION_UNPROVEN':'前一交易日五分钟历史未获证明',
    'INVALID_ACTION':'动作无效',
    'LEADER_15M_NOT_WEAK':'龙头十五分钟结构未转弱',
    'LEADER_15M_WEAK':'龙头十五分钟结构转弱',
    'LEADER_5M_DIVERGENCE_TO_STRENGTH':'龙头五分钟分歧转强',
    'LEADER_5M_RESEAL':'龙头五分钟回封',
    'LEADER_5M_TRIGGER':'龙头五分钟触发条件',
    'LEADER_BOARD_CONFIRMATION':'龙头封板确认',
    'LEADER_BOARD_RISK':'龙头封板风险',
    'LEADER_CLIMAX_NO_NEW_ENTRY':'龙头高潮阶段不新增入场',
    'LEADER_CONTEXT':'龙头上下文',
    'LEADER_CONTEXT_CURRENT':'龙头上下文属于当前时段',
    'LEADER_CONTEXT_EXPIRED':'龙头上下文已过期',
    'LEADER_CONTEXT_INCOMPLETE':'龙头上下文不完整',
    'LEADER_CONTEXT_MISSING':'龙头上下文缺失',
    'LEADER_CONTEXT_STRENGTH':'龙头上下文强度',
    'LEADER_FIRST_15M_BREAKOUT':'龙头首段十五分钟突破',
    'LEADER_FIRST_BOARD_OBSERVE':'龙头首板观察',
    'LEADER_HIGH_VOLUME_UPPER_SHADOW':'龙头放量上影风险',
    'LEADER_TRIGGER_NOT_MET':'龙头触发条件未满足',
    'LIVE_MARKET_ENTRY_PERMISSION':'实时市场入场许可',
    'LIVE_MARKET_STATE_READY':'实时市场状态就绪',
    'LOCKED_LIMIT_UP':'封死涨停不可执行',
    'MA520_15M_STABLE':'五二零十五分钟结构稳定',
    'MA520_15M_WEAK':'五二零十五分钟结构转弱',
    'MA520_5M_HIGHER_LOW':'五二零五分钟低点抬高',
    'MA520_5M_NO_HIGHER_LOW':'五二零五分钟低点未抬高',
    'MA520_5M_VWAP_NOT_RECLAIMED':'五二零五分钟未收复成交均价',
    'MA520_5M_VWAP_RECLAIM':'五二零五分钟收复成交均价',
    'MA520_BELOW_MA20':'五二零跌至二十日均线下方',
    'MA520_CONFIRMATION_PENDING':'五二零确认仍在等待',
    'MA520_DAILY_SNAPSHOT_MISSING':'五二零日线快照缺失',
    'MA520_DEATH_CROSS':'五二零均线死叉',
    'MA520_INTRADAY_HOLD_MA20':'五二零盘中守住二十日均线',
    'MA520_TWO_CLOSED_5M_CONFIRMATIONS':'五二零两个闭合五分钟确认',
    'MA520_VOLUME_NOT_OVERHEATED':'五二零成交量未过热',
    'MIXED_TRADING_DATES':'行情混入多个交易日',
    'NO_1M_BARS':'没有一分钟行情',
    'NO_FUTURE_BARS':'没有未来行情',
    'NO_SELLABLE_POSITION':'没有可卖持仓',
    'OUTSIDE_SESSION':'不在交易时段',
    'PLAN_DATA_READY':'计划数据就绪',
    'POSITION_OPEN':'已有持仓',
    'PRICE_DISCOVERY_TREND_ROUTE':'价格发现趋势路线',
    'PROFITABLE_POSITION_FOR_ADD':'加仓要求原持仓盈利',
    'SAME_TRADING_DATE':'属于同一交易日',
    'SELLABLE_POSITION':'存在可卖持仓',
    'STALE_1M':'一分钟行情已陈旧',
    'STANDARD_TREND_ROUTE':'标准趋势路线',
    'STOCK_BEHAVIOR_TYPE':'股票行为类型',
    'STRATEGY_PROFILE':'策略类型',
    'STRATEGY_TRIGGER_NOT_MET':'策略触发条件未满足',
    'TRADE_DATE_MISMATCH':'交易日不一致',
    'TREND_15M_PRESSURE_EASING':'趋势十五分钟抛压减轻',
    'TREND_5M_REVERSAL_CONFIRMATION':'趋势五分钟反转确认',
    'TREND_ADD_REQUIRES_PROFIT':'趋势加仓要求已有浮盈',
    'TREND_AVERAGE_DOWN_FORBIDDEN':'趋势禁止摊低亏损持仓',
    'TREND_ENTRY_RULE_V2':'趋势入场规则第二版',
    'TREND_INTRADAY_HOLD_MA5':'趋势盘中守住五日均线',
    'TREND_VOLUME_NOT_OVERHEATED':'趋势成交量未过热',
    'TRIGGER_ZONE_NOT_MET':'触发区间未满足',
    'UNKNOWN_STRATEGY_PROFILE':'策略类型不在已登记范围',
    'VALID_CLOSED_1M_BARS':'有效闭合一分钟行情',
    'VALID_STRUCTURE_FAIL_CONFIRM_BARS_15M':'有效十五分钟结构失效确认根数',
    "A3_TREND_ROUTE_APPROVED": "趋势路线已通过计划审核",
    "A4_LIVE_ENTRY_GEOMETRY_ACCEPTED": "本轮入场几何已接受",
    "A4_MARKET_CAUTION_REDUCED_POSITION": "市场谨慎，收紧仓位",
    "A4_SESSION_WARMUP": "等待当日闭合行情准备完成",
    "A_15M_SUPPORT": "十五分钟支撑",
    "A_CONTRACTION": "回踩量能收缩",
    "A_PULLBACK": "价格回踩",
    "A_STABILIZED": "回踩止跌确认",
    "A_SUPPORT": "支撑结构有效",
    "B_ACCEPTED": "突破回踩形态确认",
    "B_ATTEMPT_PREFIX": "突破尝试前缀",
    "B_EFFECTIVE_BREAKOUT": "有效突破",
    "B_KEY_BELOW_NO_CHASE": "关键位低于禁追价",
    "B_KEY_LEVEL": "日线关键位可用",
    "B_NO_CHASE": "突破回踩未触禁追价",
    "B_RETEST": "突破后回踩",
    "B_RETEST_VOLUME": "回踩量能确认",
    "B_STOP_ONLY_RAISED": "止损仅向上收紧",
    "CLOSED_15M": "十五分钟已闭合",
    "CLOSED_5M": "五分钟已闭合",
    "CONFIRM_CURRENT_MINUTE_NO_TRADES": "当分钟无成交，按原证据确认",
    "CURRENT_MINUTE_HAS_TRADES": "当分钟有成交",
    "DETERMINISTIC_STRATEGY_CONFIRMATION": "确定性策略确认",
    "EMPTY_SCOPE": "当前无待监测对象",
    "ENGINE_REVIEW_CANDIDATE": "引擎候选待模型复核",
    "ENTRY_CONFIRMATION_NOT_COMPLETE": "入场确认尚未完成",
    "ENTRY_GEOMETRY_NOT_CONFIRMED": "入场几何尚未确认",
    "ENTRY_GEOMETRY_NOT_MET_PRICE_ABOVE_NO_CHASE": "实时价超过禁追价",
    "ENTRY_GEOMETRY_NOT_MET_PRICE_BELOW_ZONE": "实时价低于入场区间",
    "ENTRY_GEOMETRY_VIOLATED_NO_CHASE": "入场几何触及禁追边界",
    "ENTRY_RR": "入场盈亏比",
    "ENTRY_ZONE": "入场区间",
    "ENTRY_ZONE_NOT_REACHED": "尚未到达入场区间",
    "ENTRY_ZONE_NOT_REACHED_AND_MARKET_RISK_OFF": "未达入场区间且市场收缩风险",
    "ENTRY_ZONE_NOT_REACHED_PRICE_ABOVE_TRIGGER_HIGH": "实时价高于计划区间上沿",
    "LLM_UNAVAILABLE": "模型复核不可用",
    "MONITOR_OVERRUN": "模型复核超时，未形成有效裁决",
    "NETWORK_RETRY_EXHAUSTED": "模型网络尝试失败，详见逐次回执",
    "NO_CLOSED_15M": "尚无闭合十五分钟行情",
    "NO_CLOSED_5M": "尚无闭合五分钟行情",
    "PREVIOUS_CLOSE_RESEARCH_FAILED": "上一交易日研究尚未就绪",
    "PRICE_ABOVE_ENTRY_ZONE_AND_MARKET_RISK_OFF": "实时价高于入场区间且市场收缩风险",
    "TEAM_MODEL_CALL_FAILED": "团队模型调用失败",
    "TRADER_TECH_REFERENCE_INVALID": "模型技术引用未通过校验",
    "VOLUME_NOT_OVERHEATED": "成交量未过热",
    "VOLUME_OVERHEATED": "成交量过热",
    "WAITING_CONDITION": "等待策略条件确认",
    "ORCHESTRATOR": "任务编排",
    "JUDGE_TIMEOUT": "模型复核超时",
    "A4_REVIEW_CAPACITY_TRUNCATED": "本节拍模型容量不足，未复核对象已单独通知",
    "TRUNCATED": "本节拍未复核，已显式记录",
    "PM_NOT_REVIEWED_TIME_BUDGET": "晨会预算不足，未进入组合审核",
    "NOTHING_PENDING_ALL_ACTIVE": "当日计划均已激活，无待补激活计划",
    "ENGINE_ONLY_HALF_POSITION": "仅引擎降级，总仓位上限减半",
    "ENGINE_ONLY_NO_CHASE": "仅引擎降级，触及严格禁追价",
    "ENGINE_ONLY_OUTSIDE_STRICT_ENTRY_RANGE": "仅引擎降级，超出更严入场区间",
    "ENGINE_ONLY_HALF_POSITION_NO_LOT": "减半后不足整手或已有仓位占满",
    "ENGINE_ONLY_GEOMETRY_UNPROVEN": "仅引擎降级，原区间证据不完整",
    "READY": "资料完整",
    "DEGRADED": "部分资料待完善",
    "READY_DEGRADED": "可用但需留意",
    "VALIDATED": "已验证",
    "ACTIVE_CURRENT_SESSION": "当日盘中监测中",
    "PENDING_0926": "等待早盘复核",
    "ALREADY_ACTIVE": "已进入盘中监测",
    "BULL": "多头",
    "BEAR": "空头",
    "BEAR_RISK": "偏弱防守",
    "WEAK_ROTATION": "弱势轮动",
    "NEUTRAL": "中性",
    "TREND": "趋势票",
    "EMOTION": "情绪票",
    "TREND_CORE": "趋势核心",
    "EMOTION_LEADER": "情绪龙头",
    "LEADER": "龙头",
    "FOLLOWER": "跟随",
    "PERSISTENT": "趋势延续",
    "ACCELERATING": "加速增强",
    "EARLY_REVERSAL": "初步转强",
    "MIXED": "多空交织",
    "COOLING": "热度降温",
    "REPAIR": "修复阶段",
    "CONFIRMATION": "确认阶段",
    "ICE_POINT": "情绪冰点",
    "LIQUIDITY_CONTRACTION": "流动性收缩",
    "INCREMENTAL_EXPANSION": "增量资金扩张",
    "EXISTING_FUNDS_ROTATION": "存量资金轮动",
    "ALLOW": "允许关注",
    "NO_NEW_ENTRY": "暂不追高开仓",
    "BLOCK_NEW_ENTRY": "暂停新增仓",
    "CAUTION": "谨慎参与",
    "HIGH": "较高",
    "MEDIUM": "中等",
    "LOW": "较低",
    "NONE": "暂无",
    "SINGLE_STOCK": "单股带动",
    "QUALIFIED": "符合计划条件",
    "WATCH_ONLY": "继续观察",
    "QUALIFIED_STANDARD": "常规计划条件合格",
    "QUALIFIED_PROBE": "试探计划条件合格",
    "HIGHER_TIMEFRAME_CONDITIONAL_PROBE": "月／周线背景偏弱（非淘汰条件）",
    "A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE": "观察池中技术条件合格，可小仓试探",
    "A3_STAGE_LINEAGE_MISSING": "上游阶段追溯信息不完整",
    "A1_ACTIVE_REUSED": "沿用本月有效研究池",
    "A2_FOCUS_POOL_UNDERFILLED_MARKET": "当日强势板块数量较少",
    "POOL_UNDERFILLED_MARKET": "市场机会数量较少",
    "FIRST_RESISTANCE": "第一压力位",
    "R2_OBSERVATION": "第二压力位观察",
    "MONTH_CLOSED": "月线数据完整",
    "WEEK_CLOSED": "周线数据完整",
    "DAILY_CLOSED": "日线数据完整",
    "TRADABLE": "当前可交易",
    "DAILY_CLOSE_AVAILABLE": "参考收盘价可用",
    "HIGHER_TIMEFRAME_RISK_CLASSIFIED": "大周期风险已分类",
    "PRICE_GEOMETRY_VALID": "价格结构有效",
    "TREND_DAILY_PATH_CONFIRMED": "日线趋势路径确认",
    "DAILY_MA5_AVAILABLE_FOR_A4": "五日线可供盘中择时",
    "NOT_OVEREXTENDED_OR_RETEST_CONFIRMED": "未明显超涨或回踩已确认",
    "NOT_DISTRIBUTION": "未发现明显派发",
    "A4_WILL_CONFIRM_DAILY_MA5_PULLBACK": "盘中继续确认五日线回踩",
    "DAILY_NOT_BEARISH": "个股日线未转空",
    "DETERMINISTIC_TRIGGER_PASS": "确定性触发条件通过",
    "DETERMINISTIC_EXIT_TRIGGER": "确定性离场条件触发",
    "HARD_STOP_BEFORE_ENTRY": "入场前触及风险线",
    "CURRENT_1M_HARD_STOP": "当前一分钟价格触及持仓硬止损",
    "PRE_ENTRY_RISK_LEVEL_TOUCHED": "入场前触及风险线，等待闭合周期确认",
    "ENTRY_BLOCKED_CURRENT_MINUTE": "当前分钟暂停入场",
    "TREND_PRE_ENTRY_STRUCTURE_INVALIDATED": "趋势结构经闭合周期确认失效",
    "MA520_PRE_ENTRY_STRUCTURE_INVALIDATED": "五二零结构经闭合周期确认失效",
    "A4_FORCED_EXIT_WITHOUT_POSITION": "策略产生了无持仓离场指令，已作为异常阻断",
    "A4_BEHAVIOR_TYPE_MISSING": "股票类型尚未确定，不能选择盘中策略",
    "BLOCKED_T1": "受 A 股当日买入次日可卖规则限制，等待下一交易日离场",
    "T1_LOCKED": "受 A 股当日买入次日可卖规则限制，等待下一交易日按新价格重评",
    "ENTRY_NEXT_BAR_MISSED": "入场信号后的下一根完整分钟线未能成交",
    "LIMIT_TOUCH_WITHOUT_FILL_EVIDENCE": "仅触及限价，缺少可成交证据",
    "LIMIT_NOT_REACHED": "有效分钟内价格未到买入限价",
    "PRICE_OUTSIDE_BAR": "旧版模拟指定价未落入该分钟价格范围",
    "ENTRY_CONTRACT_INVALID": "入场参考价或风险线缺失，执行契约无效",
    "ENTRY_AT_OR_BELOW_STOP": "拟成交价已触及风险线，不再入场",
    "TREND_5M_FAILED_MA5_RECLAIM": "趋势股连续跌破五日线参考且回抽失败",
    "TREND_HIGH_VOLUME_MA5_BREAK": "趋势股放量跌破五日线参考",
    "TREND_HIGH_VOLUME_UPPER_SHADOW": "趋势股放量长上影，触发减仓",
    "MA520_5M_FAILED_MA20_RECLAIM": "五二零策略跌破二十日线参考且回抽失败",
    "MA520_HIGH_VOLUME_MA20_BREAK": "五二零策略放量跌破二十日线参考",
    "TREND_5M_REVERSAL_NOT_CONFIRMED": "五分钟转强尚未确认",
    "TREND_15M_PRESSURE_NOT_EASING": "十五分钟压力尚未缓解",
    "TREND_PULLBACK_VOLUME_CONTRACTION_NOT_MET": "回踩阶段尚未出现缩量",
    "TREND_PRIOR_5M_REVERSAL_NOT_MET": "前一根完整五分钟线未形成反转结构",
    "TREND_SUBSEQUENT_5M_CONFIRMATION_NOT_MET": "反转后尚未出现温和放量的五分钟阳线确认",
    "TREND_VWAP_RECLAIMED_NOT_MET": "价格尚未收复当日成交均价",
    "A2_ROTATION_RESERVE_RESEARCH_ONLY": "轮动候补方向，进入技术研究，暂不执行",
    "A3_ROTATION_RESERVE_RESEARCH_ONLY": "候补技术研究计划，未开放盘中执行",
    "A3_STRONG_TREND_OBSERVATION_ONLY": "板块前五之外的强趋势研究观察，未开放盘中执行",
    "A2_STRONG_TREND_OBSERVATION_ONLY": "基本面与趋势证据满足补充研究条件，仅进入观察",
    "A2_STRONG_TREND_OBSERVATION_BUDGET": "强趋势研究超出本轮复核数量上限，保留待复核",
    "A2_EMOTION_AND_TREND_INDEPENDENT_REVIEW": "同时具备情绪与趋势证据，分别核验策略资格",
    "A3_EMOTION_RESEARCH_ONLY_NO_ENTRY": "情绪研究候选，暂未开放执行",
    "A2_EMOTION_CYCLE_NO_NEW_ENTRY": "情绪阶段限制新开仓，保留研究观察",
    "A2_EMOTION_THEME_SELECTION_REQUIRED": "存在多个有依据的主题，需明确本轮研究题材",
    "MINUTE_FETCH_BUDGET_EXHAUSTED": "本分钟取数超时，等待后续完整行情",
    "MINUTE_PROVIDER_BACKPRESSURE": "本地分钟请求槽仍被未结束任务占用，受影响股票暂停新增仓",
    "MINUTE_PROVIDER_TIMEOUT": "分钟源请求未在本次截止前返回，不使用迟到行情",
    "MINUTE_FETCH_BACKPRESSURE": "本地分钟取数入口拥塞，未启动部分请求",
    "MINUTE_FETCH_DEADLINE_EXCEEDED": "本轮分钟取数截止已到，拒绝迟到结果",
    "MINUTE_PUBLICATION_PENDING": "本分钟行情版本待确认，暂不新增仓",
    "MINUTE_PUBLICATION_UNCONFIRMED": "连续分钟行情版本未确认，暂停受影响股票新增仓",
    "MINUTE_PUBLICATION_REPLAY_MISMATCH": "同一决策的行情版本已变化，不重算历史入场",
    "MINUTE_PUBLICATION_AUDIT_UNAVAILABLE": "行情确认记录不可用，暂停新增仓",
    "EXECUTION_NATIVE_5M_CONFLICT": "一分钟聚合与原生五分钟行情不一致",
    "AUXILIARY_NATIVE_5M_CONFLICT": "辅助五分钟复核与执行口径存在差异",
    "CURRENT_SESSION_WINDOW_INVALID": "当日分钟窗口不完整或时间不符",
    "CLOSE_BAR_FINALIZATION_UNCONFIRMED": "收盘行情待确认，盘后补采归档",
    "TREND_PULLBACK_ZONE_NOT_MET": "尚未进入趋势回踩区",
    "PLAN_INVALIDATED_AT_OPEN": "开盘价格触发计划失效",
    "LLM_VETO": "盘中复核模型否决",
    "HARD_STOP": "价格触及硬止损",
    "DATA_BLOCK": "数据条件未满足",
    "INSUFFICIENT": "不足",
    "SUFFICIENT": "充足",
    "PARTIAL": "部分可用",
    "NOT_APPLICABLE": "不适用",
    "TERMINAL": "已结束",
    "MISSING": "缺失",
    "RECOVERY": "修复阶段",
    "ROTATION": "轮动行情",
    "MAIN_RISE": "主升趋势",
    "STRONG_SETUP": "强势形态",
    "STRONG_TREND": "强势趋势",
    "PLATFORM_BREAKOUT": "平台突破",
    "NEW_HIGH": "创新高",
    "HEALTHY": "运行健康",
    "NEEDS_ATTENTION": "需要关注",
    "DATA_LIMITED": "数据受限",
    "INCIDENT": "存在执行事故",
    "UNAVAILABLE": "证据不可用",
    "NOT_AN_ERROR": "不是策略错误",
    "GOOD_EXECUTION": "执行符合计划",
    "SELECTION_ERROR": "选股环节存在问题",
    "PLAN_ERROR": "日线计划存在问题",
    "CONFIRM_ERROR": "盘中确认存在问题",
    "DATA_ERROR": "行情或事实数据存在问题",
    "MARKET_REVERSAL": "盘中市场发生反转",
    "UNCLASSIFIED": "暂未完成归因",
    "ENGINEERING_FIX": "工程修复",
    "DATA_FIX": "数据修复",
    "SHADOW_TEST": "影子验证",
    "INSUFFICIENT_SAMPLE": "样本不足",
    "MISSING_DATA": "数据缺失",
    "CONFOUNDED": "影响因素相互干扰",
    "REGIME_NOT_OBSERVED": "尚未观察到对应市场环境",
    "MATCH": "交叉核验一致",
    "MISMATCH": "交叉核验不一致",
    "A1_NOT_ACTIVE": "未进入 A1 有效研究池",
    "A2_NOT_EVALUATED": "A2 尚未完成评估",
    "A2_QUANT_FILTERED": "A2量化已评价，未进入模型复核池",
    "A2_NOT_FOCUSED": "未进入 A2 聚焦池",
    "A3_NOT_PLANNED": "A3 未形成日线计划",
    "A4_NO_EFFECTIVE_SIGNAL": "A4 未触发有效信号",
    "PREVIOUS_CLOSE": "以前一日收盘价为基准",
    "FIRST_INTRADAY_OPEN": "以当日第一笔盘中开盘价为基准",
    **_STRATEGY_LABELS,
    **_ACTION_LABELS,
    "FOCUS": "聚焦",
    "WATCH": "观察",
    "OUTSIDE_ROTATION": "轮动范围外",
    "UNFILLED": "未成交",
    "FILLED": "已模拟成交",
    "CANCELLED": "委托已终止",
    "ENTRY_NO_NEXT_TRADING_MINUTE": "当日已无下一可交易分钟，仅记录观察",
    "ENTRY_NEXT_MINUTE_AFTER_PLAN_EXPIRY": "下一分钟已超过计划有效期",
    "ENTRY_PRICE_CONTRACT_INVALID": "入场价格契约不成立",
    "ENTRY_PLAN_EXPIRY_INVALID": "计划有效期无法核实",
    "INVALIDATED": "计划已失效",
    "CLOSE": "收盘价",
    "OPEN": "开盘价",
    "VOLUME": "成交量",
    "AMOUNT": "成交额",
    "OHLC": "开高低收价格",
    "MA": "均线",
    "M15": "十五分钟",
    "MA520": "520均线策略",
    "DIF": "快慢均线差",
    "DEA": "均线差的平滑值",
    "HIST": "柱状值",
    "ROUTE": "策略路线",
    "PRICE_LEVELS": "计划价格位",
    "PRICE_OUTSIDE_BAR": "委托参考价不在该分钟价格区间",
    "MA_AND_CLOSE": "均线及收盘价",
    "MA_AND_CLOSE_CHECKS": "仅核验均线及收盘价",
    "CANDIDATE_PRICE_BREADTH_NOT_BOARD_STRENGTH_OR_NET_FLOW": "候选股价格广度（非板块强度或资金排名）",
    "A5V:A4:SUMMARY": "A4独立核验汇总",
    "A5V:A3:SUMMARY": "A3独立核验汇总",
    "A5V:A2:SUMMARY": "A2独立核验汇总",
}

_THEME_LABELS = {
    "TH_ELEC_COMPONENTS": "电子元件",
    "TH_NONMETAL_MATERIALS": "非金属材料与电子化学品",
    "TH_AGRI_FOREST": "种植业与林业",
    "TH_PRECIOUS_METAL": "贵金属",
    "TH_COMM_EQUIP": "通信设备",
    "TH_SEMI_LOCALIZATION": "半导体国产替代",
    "TH_MEDICAL_SERVICE": "医疗服务",
    "INDUSTRY:881278.TI": "线缆部件与电网设备",
    "INDUSTRY:884073.TI": "制冷空调设备",
    "INDUSTRY:881118.TI": "专用设备",
    "INDUSTRY:881177.TI": "互联网服务",
    "INDUSTRY:884202.TI": "房地产服务",
    "CONCEPT:885999.TI": "智能座舱",
    "AI_COMPUTE_INFRASTRUCTURE": "人工智能算力基础设施",
    "CONSUMER_ELECTRONICS": "消费电子",
    "FINANCIAL_HIGH_DIVIDEND": "金融与高股息",
    "AGRICULTURE_FOOD_SECURITY": "农业与粮食安全",
    "INFRASTRUCTURE_DOMESTIC_DEMAND": "基建与内需",
    "CHEMICAL_NEW_MATERIALS": "化工与新材料",
}

_TEXT_REPLACEMENTS = {
    "SOCIAL_FINANCING": "社会融资",
    "NEW_CREDIT": "新增信贷",
    "M2_YOY": "广义货币同比",
    "M1_YOY": "狭义货币同比",
    "business_purity": "主营纯度",
    "MARKET_CORE": "市场核心",
    "eligible=true": "符合入选条件",
    "eligible=false": "不符合入选条件",
    "PMI": "制造业采购经理指数",
    "PPI": "工业生产者出厂价格指数",
    "tier": "梯队",
    "24h": "24小时",
    "Tencent": "腾讯行情",
    "TENCENT": "腾讯行情",
    "HITHINK": "同花顺",
    "TDX": "通达信",
    "mootdx": "通达信",
    "MA5": "五日均线",
    "MA20": "二十日均线",
    "MA60": "六十日均线",
    "MACD": "指数平滑异同移动平均线",
    "KDJ": "随机指标",
    "OPEN/HIGH/LOW": "开盘价/最高价/最低价",
    "lineage_complete=true": "阶段追溯完整",
    "lineage_complete=false": "阶段追溯不完整",
    "ranking_comparable_to_production=false": "不可直接对比生产板块排名",
    "ranking_comparable_to_production=true": "可对比生产板块排名",
    "llm_reviewed": "模型复核数量",
    "quant_evaluated": "量化评价数量",
    "ranking_basis": "排名依据",
    "session_plan_count": "当日计划数",
    "daily_bar_count": "日线数量",
    "route_contract_match": "策略路线契约一致",
    "not_verified_fields": "未验证字段",
    "indicator_verification_scope": "指标核验范围",
    "cross_source_verified_fields": "跨源已核验字段",
    "lineage": "阶段追溯",
}


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(row.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _a3_presentation(payload: Mapping[str, Any], *, now: datetime) -> Mapping[str, Any]:
    persisted = payload.get("presentation")
    if isinstance(persisted, Mapping) and persisted.get("schema_version") == PRESENTATION_SCHEMA_VERSION:
        return persisted.get("a3") if isinstance(persisted.get("a3"), Mapping) else {}
    projected = project_research_presentation(payload, stage="A3", pool="approved", now=now)
    return projected.get("a3") if isinstance(projected.get("a3"), Mapping) else {}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _text(value: Any, *, limit: int = 300, fallback: str = "—") -> str:
    rendered = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    return rendered[:limit] if rendered else fallback


def _items(value: Any, *, limit: int = 4) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [_text(item, limit=160) for item in value[:limit] if str(item or "").strip()]


def _display_text(value: Any, *, limit: int = 300, fallback: str = "—") -> str:
    raw = _text(value, limit=max(limit * 2, 320), fallback=fallback)
    if raw == fallback:
        return raw
    direct = A3_REASON_LABELS.get(raw.strip().upper()) or _DISPLAY_LABELS.get(raw.strip().upper()) or _THEME_LABELS.get(raw.strip().upper())
    if direct:
        return direct[:limit]
    rendered = raw
    for source, target in _TEXT_REPLACEMENTS.items():
        if not re.fullmatch(r"[A-Z][A-Z0-9_:-]*", source):
            rendered = rendered.replace(source, target)
    rendered = rendered.replace("AI算力", "人工智能算力").replace("AI应用", "人工智能应用")
    rendered = re.sub(
        r"\b[A-Z][A-Z0-9_:-]{2,}(?:\.[A-Z]{2})?\b",
        lambda match: (
            A3_REASON_LABELS.get(match.group(0).replace("-", "_"))
            or _DISPLAY_LABELS.get(match.group(0).replace("-", "_"))
            or _THEME_LABELS.get(match.group(0).replace("-", "_"))
            or _TEXT_REPLACEMENTS.get(match.group(0).replace("-", "_"))
            or ("原码："+match.group(0))
        ),
        rendered,
    )
    return rendered[:limit]


def _display_items(value: Any, *, limit: int = 4) -> list[str]:
    return [_display_text(item, limit=180) for item in _items(value, limit=limit)]


def _condition_summary(value, *, key_limit=4):
    if value is None:
        return '不可用（该轮条件原件未留存）'
    rows=list(value) if isinstance(value,(list,tuple)) else []
    # The count and every required condition refer to this same event.
    # A short key-condition preview must never hide the rest of the evidence.
    shown=_display_items(rows,limit=len(rows))
    return f"共{len(rows)}项；"+('；'.join(shown) if shown else '无')


def _a4_price_discipline(payload, strategy, event_payload):
    """Never relabel an authored A3 range as an original Team admission."""
    team=event_payload.get('team_execution_binding')
    lines=[]
    if isinstance(team,Mapping) and isinstance(team.get('plan'),Mapping):
        p=team['plan']
        lines.extend([f"• Team绑定价：{_number(p.get('price'))}；区间：{_number(p.get('entry_low'))}–{_number(p.get('entry_high'))}",
            f"• Team止损：{_number(p.get('stop_price'))}；禁止追价：{_number(p.get('no_chase'))}"])
    else:
        lines.append('• Team准入价格绑定未留存；不能用计划价格代替当时执行价格。')
        proof=strategy.get('entry_pattern_binding')
        if isinstance(proof,Mapping):
            from .entry_pattern_binding import checked_binding
            try:proof=checked_binding(proof)
            except ValueError:lines.append('• 本轮形态价格绑定未通过哈希校验。')
            else:lines.append(f"• 本轮确定性形态证据：确认价{_number(proof['entry_price'])}；止损{_number(proof['stop'])}；禁止追价{_number(proof['no_chase'])}，不等同Team已准入。")
    lines.extend([f"• 计划区间：{_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}（原A3计划）",
        f"• 计划止损：{_number(payload.get('stop_level'))}；计划禁止追价：{_number(payload.get('no_chase') or payload.get('no_chase_price') or payload.get('max_chase_price'))}"])
    return lines


def _percentage(value: Any) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "—"


def _theme_label(value: Mapping[str, Any]) -> str:
    name = value.get("theme_name") or value.get("theme") or value.get("industry")
    if not name and (value.get("theme_id") or value.get("code")):
        name = value.get("name")
    if name:
        return _display_text(name, limit=40)
    code = str(value.get("theme_id") or "").strip().upper()
    if code in _THEME_LABELS:
        return _THEME_LABELS[code]
    if code.startswith("INDUSTRY:"):
        return f"行业方向（{code.split(':', 1)[1].split('.', 1)[0]}）"
    if code.startswith("CONCEPT:"):
        return f"题材方向（{code.split(':', 1)[1].split('.', 1)[0]}）"
    return "未标注方向"


def _model_label(value: Any) -> str:
    raw = str(value or "").lower()
    if "deepseek" in raw:
        return "深度求索研究模型"
    if "kimi" in raw or "moonshot" in raw:
        return "月之暗面研究模型"
    if "glm" in raw or "z-ai" in raw:
        return "智谱研究模型"
    return "主研究模型"


def _stock_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    return raw.split(".", 1)[0] if raw else "代码未提供"


def _priority_label(value: Any) -> str:
    return {
        "P1": "最高优先",
        "P2": "常规优先",
        "P3": "试探观察",
    }.get(str(value or "").strip().upper(), "优先级待确认")


def _number(value: Any) -> str:
    try:
        return f"{float(value):.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "—"


def _time_label(value: Any, *, fallback: str = "—") -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return _display_text(raw, limit=40, fallback=fallback)
    return (parsed.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y年%m月%d日 %H:%M 北京时间")
            if parsed.tzinfo is not None else parsed.strftime("%Y年%m月%d日 %H:%M（原件未标时区）"))


def _percentile(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if 0 <= number <= 1:
        number *= 100
    return f"{number:.1f}%"


def _minute_source_label(value: Any) -> str:
    raw = str(value or "").upper()
    if "MOOTDX" in raw or "TDX" in raw:
        return "通达信一分钟行情"
    if "TENCENT" in raw:
        return "腾讯一分钟行情"
    if "SINA" in raw:
        return "新浪一分钟行情"
    return "本地标准化一分钟行情"


def _a4_decision_lines(context: Mapping[str, Any]) -> list[str]:
    environment = context.get("environment") if isinstance(context.get("environment"), Mapping) else {}
    sector = context.get("sector") if isinstance(context.get("sector"), Mapping) else {}
    capital = context.get("capital") if isinstance(context.get("capital"), Mapping) else {}
    strategy = context.get("strategy") if isinstance(context.get("strategy"), Mapping) else {}
    price = context.get("price") if isinstance(context.get("price"), Mapping) else {}

    live_decision = _display_text(environment.get("live_decision"), limit=30, fallback="实时许可资料未提供")
    prior_market = _display_text(
        environment.get("market_environment") or environment.get("market_regime"),
        limit=40,
        fallback="前一收盘环境未标注",
    )
    emotion = _display_text(environment.get("emotion_cycle_stage"), limit=30, fallback="情绪阶段未标注")
    theme = _display_text(
        sector.get("theme_name") or sector.get("theme_id"), limit=40, fallback="板块未标注"
    )
    role = _display_text(sector.get("market_role"), limit=30, fallback="角色未标注")
    stage = _display_text(sector.get("theme_stage"), limit=30, fallback="阶段未标注")
    relative = _percentile(sector.get("relative_strength_percentile"))

    funding_state = _display_text(capital.get("state"), limit=40, fallback="资金状态资料不足")
    amount_ratio = _percentage(capital.get("amount_ratio"))
    coverage = _percentage(capital.get("coverage"))
    flow_score = capital.get("sector_flow_score")
    flow_text = (
        f"板块资金评分{_number(flow_score)}"
        if flow_score is not None and capital.get("sector_flow_source")
        else "板块净流入未形成可核验数值"
    )

    profile = _STRATEGY_LABELS.get(
        str(strategy.get("profile") or ""),
        _display_text(strategy.get("profile"), limit=30, fallback="策略未标注"),
    )
    setup = _display_text(strategy.get("setup_pattern"), limit=50, fallback="按冻结计划确认")
    met = _condition_summary(strategy.get("met_conditions"))
    closed = "、".join(
        item for item in (
            f"五分钟{_time_label(strategy.get('closed_5m_end'))}" if strategy.get("closed_5m_end") else "",
            f"十五分钟{_time_label(strategy.get('closed_15m_end'))}" if strategy.get("closed_15m_end") else "",
        ) if item
    ) or "闭合周期时间未提供"
    reward_risk = _number(price.get("live_reward_risk"))
    confirmed_price = _number(price.get("live_entry_price") or price.get("signal_reference"))
    stop_price = _number(price.get("stop_level"))
    no_chase_price = _number(price.get("no_chase_price"))
    target_price = _number(price.get("target_price"))

    return [
        "",
        "**入场理由**",
        f"• 市场环境：当日实时许可为{live_decision}（{_time_label(environment.get('live_as_of'))}）；前一收盘为{prior_market}，情绪处于{emotion}。",
        f"• 板块位置：{theme}；角色为{role}；阶段为{stage}；个股相对强度分位{relative}。",
        f"• 资金条件：{funding_state}；市场成交额比{amount_ratio}，覆盖率{coverage}；{flow_text}。",
        f"• 价格结构：实时确认价{confirmed_price}；止损{stop_price}；禁止追价{no_chase_price}；第一目标{target_price}；实时盈亏比{reward_risk}。",
        f"• 技术策略：{profile}；形态为{setup}；依据闭合周期：{closed}。",
        f"• 已确认：{met if met else '确定性策略条件已通过，细项未单独列出'}。",
    ]


def _reference_price_as_of(
    payload: Mapping[str, Any],
    context: Mapping[str, Any],
) -> str:
    """Project a daily reference price onto its frozen market date.

    Historical recovery plans created before the source fix may carry the
    recovery wall clock even though ``reference_price`` is the previous close.
    Correct only that cross-date mismatch at the presentation boundary; keep a
    same-date stock-specific bar timestamp unchanged.
    """

    raw = str(payload.get("reference_price_as_of") or "").strip()
    market_date = str(context.get("market_trade_date") or "").strip()
    if len(market_date) == 10 and market_date[4:5] == "-" and market_date[7:8] == "-":
        if not raw or raw[:10] != market_date:
            return f"{market_date}T15:00:00+08:00"
    return raw or "—"


def _plan_sort_key(row: Mapping[str, Any]) -> tuple[int, str, str]:
    payload = _payload(row)
    rank = {"P1": 0, "P2": 1, "P3": 2}.get(
        str(payload.get("plan_priority") or "").strip().upper(),
        3,
    )
    return rank, str(row.get("symbol") or ""), str(row.get("plan_id") or "")


def _monthly_line(value: Mapping[str, Any]) -> str:
    name = _display_text(value.get("name") or value.get("code"), limit=30)
    strength = _number(value.get("relative_strength_percentile_20d"))
    try:
        return_5d = f"{float(value.get('return_5d')) * 100:.1f}%"
    except (TypeError, ValueError):
        return_5d = "—"
    return f"{name}：近5日 {return_5d}，20日强度分位 {strength}"


class WorkflowLarkPublisher:
    """Send synchronously, then persist only a safe delivery summary."""

    def __init__(
        self,
        store: RuntimeStore,
        webhook_url: Any,
        *,
        webhook_path: Path | None = None,
        timeout_seconds: float = 8.0,
        clock: Callable[[], datetime] | None = None,
        policy_root: Path | None = None,
        internal_sink: Callable[..., Mapping[str, Any]] | None = None,
    ):
        self.store = store
        self.webhook_path = webhook_path
        self.timeout_seconds = timeout_seconds
        self.clock = clock or (lambda: datetime.now().astimezone())
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self.internal_sink = internal_sink
        self.configuration_reason: str | None = None
        try:
            self.notifier = LarkNotifier(webhook_url, timeout_seconds=timeout_seconds)
        except (LarkConfigurationError, ValueError) as exc:
            self.notifier = LarkNotifier(None, timeout_seconds=timeout_seconds)
            self.configuration_reason = str(getattr(exc, "reason_code", "LARK_CONFIGURATION_INVALID"))

    @property
    def enabled(self) -> bool:
        notifier, _ = self._resolve_notifier()
        return notifier.enabled

    def _resolve_notifier(self) -> tuple[LarkNotifier, str | None]:
        # Tests and explicit callers may inject a notifier. Production uses the
        # file path and reloads it for every send so a UI edit needs no restart.
        if self.webhook_path is None or self.notifier.enabled:
            return self.notifier, self.configuration_reason
        try:
            if not self.webhook_path.is_file() or self.webhook_path.stat().st_size > 4_096:
                return self.notifier, "LARK_WEBHOOK_NOT_CONFIGURED"
            payload = json.loads(self.webhook_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("schemaVersion") != 1:
                return self.notifier, "LARK_WEBHOOK_CONFIGURATION_INVALID"
            notifier = LarkNotifier(
                payload.get("webhookUrl"),
                timeout_seconds=self.timeout_seconds,
            )
            return notifier, None
        except (OSError, ValueError, TypeError, json.JSONDecodeError, LarkConfigurationError):
            return self.notifier, "LARK_WEBHOOK_CONFIGURATION_INVALID"

    def _send_legacy(
        self,
        *,
        delivery_key: str,
        kind: str,
        source_id: str,
        title: str,
        lines: Sequence[str],
        summary: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        notifier, configuration_reason = self._resolve_notifier()
        if not notifier.enabled:
            return {"status": "DISABLED", "reason_code": configuration_reason or "LARK_WEBHOOK_NOT_CONFIGURED"}
        existing = self.store.get_delivery_by_key(delivery_key)
        if existing is not None:
            return {"status": str(existing["status"]), "duplicate": True, "delivery_id": existing["delivery_id"]}
        unknown_counts={}
        for code in re.findall(r'原码：([A-Z][A-Z0-9_:-]*)','\n'.join(lines)):
            unknown_counts[code]=unknown_counts.get(code,0)+1
        if unknown_counts:
            lines=[*lines,'',f"• 标签映射告警：{len(unknown_counts)}种原码，共{sum(unknown_counts.values())}处；原码已保留，需补齐映射。"]
            summary={**summary,'unmapped_code_counts':unknown_counts,'label_mapping_alert':True}
        color = self.store.next_notification_color()
        send_started = self.clock()
        from .card_archive import archive_card
        archive = archive_card(directory=self.store.path.parent / "lark_cards",
            card=LarkNotifier.build_card(title, lines, color), delivery_key=delivery_key,
            source_run_id=str(summary.get("source_run_id") or (source_id if kind == "PREMARKET_A3" else "UNRESOLVED")),
            plan_ids=summary.get("plan_ids") or ([summary["plan_id"]] if summary.get("plan_id") else []),
            trade_date=str(summary.get("trade_date") or now.date().isoformat()), observed_at=send_started)
        result = notifier.send(title, lines, color)
        acknowledged_at = self.clock()
        row, _ = self.store.record_delivery(
            delivery_key=delivery_key,
            kind=kind,
            source_id=source_id,
            title=title,
            status="SENT" if result.ok else "FAILED",
            color=color,
            attempt_count=result.attempts,
            last_reason_code=None if result.ok else result.reason_code,
            payload={**summary, **archive, "business_at": now.isoformat()},
            created_at=send_started,
            sent_at=acknowledged_at if result.ok else None,
        )
        return {
            "status": str(row["status"]),
            "delivery_id": str(row["delivery_id"]),
            "reason_code": row.get("last_reason_code"),
        }

    def _internal_notice(self, *, key, kind, title, lines, now):
        if self.internal_sink is not None:
            return dict(self.internal_sink(key=key, kind=kind, title=title, lines=lines, now=now))
        from .team_v2_store import TeamMeetingStore
        from .team_v2_internal import InternalOutbox
        root = self.policy_root
        assert root is not None
        directory = root / 'state' / 'team_v2'
        if directory.is_symlink() or (directory / 'team.db').is_symlink():
            raise ValueError('INTERNAL_NOTIFICATION_SCOPE_INVALID')
        # Delivery belongs to the native notification command, never this
        # minute trading call. Enqueued is explicitly different from SENT.
        event_id = InternalOutbox(TeamMeetingStore(directory/'team.db')).enqueue(
            account_id='local-paper',trade_date=now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat(),
            kind=kind,source_id=hashlib.sha256(key.encode()).hexdigest(),observed_at=now,
            skip_policy=True,
            body=dict(msg_type='text',content=dict(text=title+'\n'+'\n'.join(lines))))
        return dict(status='QUEUED_INTERNAL',event_id=event_id)

    def _send(self, *, delivery_key, kind, source_id, title, lines, summary, now):
        # The optional root preserves the public library API. Both production
        # composition roots explicitly install the 0881 contract below.
        if self.policy_root is None:
            return self._send_legacy(delivery_key=delivery_key,kind=kind,source_id=source_id,
                title=title,lines=lines,summary=summary,now=now)
        existing = self.store.get_delivery_by_key(delivery_key)
        if existing is not None:
            return dict(status=existing['status'],duplicate=True,delivery_id=existing['delivery_id'])
        from .notification_policy import NotificationPolicy
        policy = NotificationPolicy(self.policy_root/'state'/'notification_policy.sqlite3')
        title = title + '｜' + now.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%H:%M 北京时间')
        body = list(lines)
        trace = []
        def visible(match):
            code = match.group(0)
            if code in {'A1','A2','A3','A4','A5','A6','PM','CAS','VWAP','MACD','T1','T2'}:
                return code
            trace.append(code)
            translated=(_DISPLAY_LABELS.get(code) or _ACTION_LABELS.get(code) or _STRATEGY_LABELS.get(code)
                or {'READY':'已就绪','MET':'已满足','UNMET':'未满足','NO_ACTION':'不动',
                    'PROBE':'试探仓','WATCH':'观察','SIGNAL_READY':'技术条件成立',
                    'BLOCKED':'已拒绝','FAILED':'失败','PUBLISHED':'已发布','NOT_PUBLISHED':'未发布',
                    'PENDING_A4':'等待盘中确认','REQUIRED':'必须满足'}.get(code))
            if translated is None:
                raise ValueError('NOTIFICATION_LABEL_MISSING:'+code)
            return translated
        body = [re.sub(r'\b[A-Z][A-Z0-9]{2,}(?:_[A-Z0-9]+)*\b',visible,line) for line in body]
        if any(re.search(r'\b(?:None|null)\b|未识别',line) for line in body):
            raise ValueError('NOTIFICATION_VALUE_CONTRACT_INVALID')
        if not summary.get('symbol') and not summary.get('symbols') and kind not in {'A5_MIDDAY_REVIEW','A5_POST_CLOSE_REVIEW'}:
            body.append('• 单股当前价、入场区间、止损、禁追价及形态条件：不适用。')
        if not any(token in '\n'.join(body) for token in ('接下来','下一步','下一','等待','无需','处理：','执行状态','系统将','交运维','不下单','不重跑')):
            body.append('• 接下来：系统按原计划与数据、仓位和风控门处理；此通知不构成放行。')
        body.append('追溯：'+source_id+('；'+','.join(sorted(set(trace))) if trace else ''))
        payload=dict(title=title,lines=body,summary=dict(summary),source_id=source_id)
        state = str(summary.get('state') or summary.get('output_state') or summary.get('status') or
            summary.get('action') or kind)
        observed = policy.observe(event_key=delivery_key,kind=kind,symbol=str(summary.get('symbol') or ''),
            state=state,now=now,payload=payload)
        if observed['disposition'] != 'IMMEDIATE':
            return dict(status=observed['disposition'],group_id=observed['group_id'])
        channels=observed['routes']
        if summary.get('native_internal_event_id'):
            channels=tuple(c for c in channels if c!='INTERNAL')
            policy.receipt(delivery_key,'INTERNAL',str(summary.get('native_internal_status') or 'QUEUED_INTERNAL'),
                dict(event_id=summary['native_internal_event_id'],existing_native_event=True))
        return self._deliver_policy(policy,delivery_key,kind,payload,channels,now)

    def _deliver_policy(self, policy, key, kind, payload, channels, now):
        results = {}
        for channel in channels:
            if channel == 'INTERNAL':
                result = self._internal_notice(key=key,kind=kind,title=payload['title'],lines=payload['lines'],now=now)
            else:
                result = self._send_legacy(delivery_key=key,kind=kind,source_id=payload['source_id'],
                    title=payload['title'],lines=payload['lines'],summary=payload['summary'],now=now)
            policy.receipt(key,channel,result['status'],result)
            results[channel] = result
        return dict(status=results.get('CUSTOMER',results.get('INTERNAL',{})).get('status'),routes=results)

    def flush_notification_summaries(self, *, now, close_day=False):
        if self.policy_root is None:
            return []
        from .notification_policy import NotificationPolicy
        policy=NotificationPolicy(self.policy_root/'state'/'notification_policy.sqlite3')
        output=[]
        for group in policy.claim_summaries(now=now,close_day=close_day,owner='workflow'):
            payload=group['payload']
            first=_time_label(group['first_at']);last=_time_label(group['last_at'])
            payload={**payload,'title':'重复事件汇总｜'+payload['title'],
                'lines':[f"• 同类同状态共{group['count']}次；首条{first}，末条{last}。",*payload['lines']]}
            output.append(self._deliver_policy(policy,group['event_key'],group['kind'],payload,group['routes'],now))
        return output

    def publish_premarket(
        self,
        plans: Sequence[Mapping[str, Any]],
        *,
        reviewed_at: datetime,
        evidence: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        if not plans:
            return []
        ordered = sorted(plans, key=lambda row: (str(row.get("lane_id") or ""), str(row.get("symbol") or "")))
        plan_ids = [str(row.get("plan_id") or "") for row in ordered]
        batch_hash = hashlib.sha256("|".join(plan_ids).encode("utf-8")).hexdigest()[:16]
        plan_payloads = [_payload(row) for row in ordered]
        themes = []
        strategies = []
        for payload in plan_payloads:
            theme = _display_text(
                payload.get("theme") or payload.get("theme_name") or payload.get("industry"),
                limit=50,
                fallback="",
            )
            if theme and theme != "—" and theme not in themes:
                themes.append(theme)
            strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), "")
            if strategy and strategy not in strategies:
                strategies.append(strategy)
        outputs: list[dict[str, Any]] = []
        chunks = [ordered[index : index + 3] for index in range(0, len(ordered), 3)]
        for page, chunk in enumerate(chunks, start=1):
            lines = [
                "**早盘复核结果**",
                f"• {reviewed_at.strftime('%m月%d日 %H:%M')} 完成复核，{len(ordered)} 只计划进入盘中监测。",
                f"• 适用策略：{'、'.join(strategies) if strategies else '按个股计划执行'}。",
            ]
            if themes:
                lines.insert(2,f"• 重点方向：{'、'.join(themes[:5])}。")
            symbols: list[str] = []
            source_ids: list[str] = []
            for row in chunk:
                payload = _payload(row)
                presentation = _a3_presentation(payload, now=reviewed_at)
                symbol_raw = _text(row.get("symbol"), limit=20)
                symbol = _stock_code(symbol_raw)
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                source_ids.append(_text(payload.get("source_run_id"), limit=160, fallback=""))
                strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _display_text(payload.get("strategy_profile"), limit=40, fallback="按个股计划执行"))
                reason_codes=_items(payload.get("selection_reasons") or payload.get("reason_codes"),limit=100)
                risks=[];reasons=[]
                for code in reason_codes:
                    rendered=_display_text(code,limit=180)
                    if any(token in str(code) for token in ('BELOW','MISSING','NOT_READY','RISK','REJECT','OVEREXTEND','CONFLICT','DEGRADED')) or any(token in rendered for token in ('低于','缺失','风险','不足','超限','未通过')):
                        risks.append(rendered)
                    else:reasons.append(rendered)
                confirmation=payload.get('entry_confirmation')
                confirmation=confirmation.get('required_conditions') if isinstance(confirmation,Mapping) else None
                conditions = _display_items(confirmation or payload.get('a4_required_entry_rules') or payload.get('confirmation_conditions'), limit=3)
                actual_status={'ACTIVE_TODAY':'已激活，仍待盘中条件确认','PENDING_MORNING_REVIEW':'等待自然晨会审核',
                    'PENDING':'待激活','INVALIDATED':'已失效，停止新入场'}.get(str(row.get('status')),'状态尚未核实')
                auction = evidence.get(symbol_raw) or evidence.get(symbol) or {}
                auction_price = auction.get("price") if isinstance(auction, Mapping) else None
                lines.extend(
                    [
                        f"\n**{name}（{symbol}）｜{strategy}**",
                        f"• 入选依据：{'；'.join(reasons[:2]) if reasons else 'A3 日线计划通过'}",
                        f"• 风险提示：{'；'.join(risks[:3]) if risks else '仍需盘中确认，未触发不交易。'}",
                        f"• 计划状态：{actual_status}。",
                        f"• 价格计划：触发区 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}；止损 {_number(payload.get('stop_level'))}；禁止追价 {_number(payload.get('no_chase_price') or payload.get('max_chase_price'))}",
                        f"• 早盘价格：{_number(auction_price)}；入场确认：{'；'.join(conditions) if conditions else '原计划未留存具体盘中确认条件，待核验。'}",
                    ]
                )
                if str(payload.get("strategy_profile") or "") == "LEADER_INTRADAY":
                    lines.append(f"• 跳空预检：若开盘价 > 追价线 {_number(payload.get('no_chase_price') or payload.get('max_chase_price'))} 自动失效。")
                if payload.get("ROTATION_GATE_EMPTY") is True:
                    lines.append("• 研究标记：ROTATION_GATE_EMPTY；仍需 A3/A4 技术确认。")
                if payload.get("A2_REVIEW_DEGRADED") is True:
                    lines.append("• 研究标记：A2_REVIEW_DEGRADED；量化结果，不代表模型审核通过。")
            lines.extend(["", "**执行纪律**", "• 未触发不交易，超过禁止追价位不追，跌破失效价立即按风控处理。"])
            source_id = next((item for item in source_ids if item and item != "—"), f"premarket-{reviewed_at.date().isoformat()}")
            outputs.append(
                self._send(
                    delivery_key=f"premarket:{reviewed_at.date().isoformat()}:{batch_hash}:{page}",
                    kind="PREMARKET_A3",
                    source_id=source_id,
                    title=f"A股盘前计划｜{reviewed_at.date().isoformat()}｜{page}/{len(chunks)}",
                    lines=lines,
                    summary={"trade_date": reviewed_at.date().isoformat(), "symbols": symbols, "plan_ids": [str(row.get("plan_id") or "") for row in chunk], "plan_count": len(ordered), "page": page},
                    now=reviewed_at,
                )
            )
        return outputs

    def publish_a3_premarket_status(
        self,
        *,
        analyzed_at: datetime,
        reason_code: str,
    ) -> dict[str, Any]:
        """Report a missing plan scope without inventing a trade plan."""

        if reason_code=='NOTHING_PENDING_ALL_ACTIVE':
            return dict(status='NOOP',reason_code=reason_code)
        local=analyzed_at.astimezone(ZoneInfo('Asia/Shanghai'))
        if (local.hour,local.minute)<(8,45):
            return dict(status='NOOP',reason_code='PREMARKET_AWAITING_NATURAL_CAS',
                availability='IN_PROGRESS',description='晨会进行中，等待自然切换；最晚北京时间08:45。')

        explanation = {
            "NO_PENDING_A3_PLANS": "没有待早盘复核的有效技术计划；需核查上一交易日的收盘研究和计划发布。",
            "NO_CURRENT_A3_PLANS": "没有属于本交易日的有效技术计划。",
            "A3_PENDING_PLAN_SOURCE_UNAVAILABLE": "待复核计划缺少可核对的研究来源，不能进入盘中执行。",
        }.get(reason_code, "盘前计划尚未就绪，需核对研究与发布记录。")
        day = analyzed_at.date().isoformat()
        return self._send(
            delivery_key=f"premarket-a3-status:{day}",
            kind="PREMARKET_A3_STATUS",
            source_id=f"premarket:{day}",
            title=f"A股盘前计划未就绪｜{day}",
            lines=[
                "**今日有效 A3 计划：0 只**",
                f"• 原因：{explanation}",
                "• A4 不启用过期计划，不补发早盘交易信号。",
                "• 本消息仅说明任务状态，不构成买入信号。",
            ],
            summary={"trade_date": day, "plan_count": 0, "reason_code": reason_code},
            now=analyzed_at,
        )

    def publish_batch_switch_status(self,*,trade_date,scope_id,plan_count,body_sha256,observed_at):
        return self._send(delivery_key=f'reviewed-batch-ready:{trade_date}:{scope_id}',
            kind='PREMARKET_BATCH_READY',source_id=scope_id,
            title=f'晨会计划已就绪｜{trade_date}',lines=[
                f'自然批次切换已完成：{plan_count}只计划。',f'• 批次：{scope_id}',
                '• 计划等待09:26自然激活；待激活不代表计划缺失，也不代表已成交。'],
            summary=dict(trade_date=trade_date,scope_id=scope_id,plan_count=plan_count,
                candidate_body_sha256=body_sha256,availability='READY_PENDING_ACTIVATION'),now=observed_at)

    def publish_a3_premarket_analysis(
        self,
        plans: Sequence[Mapping[str, Any]],
        *,
        analyzed_at: datetime,
        source_run_id: str | None = None,
        research_context: Mapping[str, Any] | None = None,
        activation_state: str = "PENDING_0926",
    ) -> list[dict[str, Any]]:
        """Publish a professional read-only A1-A3 premarket brief.

        This is intentionally separate from :meth:`publish_premarket`, which
        is called by the 09:26 auction review after plans are activated.  No
        auction quote or current-session fact is accepted here.  The compact
        context was persisted by the source research run, so this task never
        reopens a large model audit or performs an implicit data sync.
        """

        if not plans:
            return []
        ordered = sorted(plans, key=_plan_sort_key)
        plan_ids = [str(row.get("plan_id") or "") for row in ordered]
        active_session = activation_state == "ACTIVE_CURRENT_SESSION"
        batch_hash = hashlib.sha256(
            f"{source_run_id or ''}|{activation_state}|{'|'.join(plan_ids)}".encode("utf-8")
        ).hexdigest()[:16]
        context = dict(research_context) if isinstance(research_context, Mapping) else {}
        a1 = context.get("a1") if isinstance(context.get("a1"), Mapping) else {}
        a2 = context.get("a2") if isinstance(context.get("a2"), Mapping) else {}
        a3 = context.get("a3") if isinstance(context.get("a3"), Mapping) else {}
        macro = a1.get("macro") if isinstance(a1.get("macro"), Mapping) else {}
        constraints = a3.get("market_open_constraints") if isinstance(a3.get("market_open_constraints"), Mapping) else {}
        directions = _display_items(macro.get("policy_direction"), limit=4)
        uncertainties = _display_items(macro.get("key_uncertainties"), limit=4)
        monthly = [item for item in a1.get("monthly_industries", ()) if isinstance(item, Mapping)] if isinstance(a1.get("monthly_industries"), (list, tuple)) else []
        themes = [item for item in a2.get("active_themes", ()) if isinstance(item, Mapping)] if isinstance(a2.get("active_themes"), (list, tuple)) else []
        reason_codes = _display_items(context.get("reason_codes"), limit=6)
        report_label = "盘中补发" if active_session else "盘前总览"
        execution_boundary = (
            "本卡为盘中补发，只呈现已完成当日行情复核并处于 A4 监测中的计划；不回填竞价或早盘信号。"
            if active_session
            else "未落库的24h新闻、竞价和实时龙虎榜不推演、不补造；09:26 独立竞价复核前不激活 A4。"
        )
        context_lines = [
            f"**{report_label}｜{analyzed_at.strftime('%m月%d日 %H:%M')}**",
            (
                f"今日发布 {len(ordered)} 只 A3 计划，"
                + ("均已进入盘中监测；只等待实时条件，不回填早盘信号。" if active_session else "等待早盘复核后进入盘中监测。")
            ),
            "",
            "**研究时点**",
            f"• 收盘数据：{_text(context.get('market_trade_date'), limit=20)}",
            f"• 目标交易日：{_text(context.get('target_trade_date'), limit=20)}",
            f"• 研究模型：{_model_label(context.get('model'))}",
            f"• 资料状态：{_display_text(context.get('status'), limit=30, fallback='尚未确认')}",
            "",
            "**宏观与政策**",
            f"• 流动性：{_display_text(macro.get('liquidity_condition'), limit=180, fallback='暂无可核验结论')}",
            f"• 盈利周期：{_display_text(macro.get('profit_cycle_position'), limit=180, fallback='暂无可核验结论')}",
            f"• 政策方向：{'；'.join(directions) if directions else '暂无可核验方向'}",
            f"• A1 研究池：{_number(a1.get('active_count'))} 只",
            "",
            "**本月重点行业**",
        ]
        if monthly:
            context_lines.extend(
                f"{index}. {_monthly_line(item)}"
                for index, item in enumerate(monthly[:5], start=1)
            )
        else:
            context_lines.append("• 暂无已持久化的月度行业结论。")
        context_lines.extend(["", "**A2 板块轮动方向**"])
        if themes:
            for index, item in enumerate(themes[:5], start=1):
                theme_name = _theme_label(item)
                context_lines.append(
                    f"**{index}. {theme_name}｜强度 {_number(item.get('score'))}**\n"
                    f"• 周期：{_display_text(item.get('weekly_state'), limit=24)}；阶段：{_display_text(item.get('stage'), limit=24)}；开仓建议：{_display_text(item.get('new_entry_policy'), limit=24)}\n"
                    f"• 广度 {_number(item.get('breadth'))}；资金 {_number(item.get('capital_flow'))}；龙头 {_number(item.get('leader_structure'))}；梯队 {_number(item.get('tier_structure'))}；产业链共振 {_number(item.get('index_chain_resonance'))}\n"
                    f"• 追高风险：{_display_text(item.get('chase_risk_level'), limit=20)}"
                )
                contradiction = _display_items(item.get("contradicting_evidence"), limit=1)
                if contradiction:
                    context_lines.append(f"• 主要风险：{contradiction[0]}")
        else:
            context_lines.append("• A2 板块资料尚未持久化；这不代表市场没有机会。")
        context_lines.extend(
            [
                "",
                "**仓位与执行边界**",
                f"• 昨日环境：{_display_text(constraints.get('prior_market_environment'), limit=40)}",
                f"• 建议仓位：{_number(constraints.get('recommended_position_min_pct'))}–{_number(constraints.get('recommended_position_max_pct'))}",
                "• A3 负责次日计划；是否开仓由 A4 根据当日实时市场决定。",
                f"• 需要留意：{'；'.join(reason_codes) if reason_codes else '暂无额外事项'}",
                "",
                "**主要不确定性**",
                *(f"• {item}" for item in uncertainties),
                f"• {execution_boundary}",
            ]
        )
        chunks = [ordered[index : index + 3] for index in range(0, len(ordered), 3)]
        from .publication_projection import card_plan_binding
        overview_binding = card_plan_binding(ordered, ordered, source_run_id=source_run_id,
            trade_date=analyzed_at.date().isoformat(),
            displayed_symbols=[str(row.get("symbol") or "") for row in ordered])
        outputs: list[dict[str, Any]] = [
            self._send(
                delivery_key=f"a3-premarket-professional-v4:{analyzed_at.date().isoformat()}:{batch_hash}:overview",
                kind="PREMARKET_A3_ANALYSIS",
                source_id=source_run_id or f"a3-premarket-{analyzed_at.date().isoformat()}",
                title=f"A股专业盘前研究｜{analyzed_at.date().isoformat()}｜总览",
                lines=context_lines,
                summary={
                    "trade_date": analyzed_at.date().isoformat(),
                    "source_run_id": source_run_id,
                    "plan_ids": overview_binding["plan_ids"],
                    "plan_binding": overview_binding,
                    "plan_count": len(ordered),
                    "context_status": context.get("status"),
                    "market_trade_date": context.get("market_trade_date"),
                    "target_trade_date": context.get("target_trade_date"),
                    "card": "overview",
                },
                now=analyzed_at,
            )
        ]
        for page, chunk in enumerate(chunks, start=1):
            lines = [
                f"**A3 个股计划｜第 {page}/{len(chunks)} 页**",
                (
                    f"共 {len(ordered)} 只；"
                    + ("当前已进入 A4 盘中监测，不补造已错过的信号。" if active_session else "早盘复核通过后才进入 A4 盘中监测。")
                ),
            ]
            symbols: list[str] = []
            for row in chunk:
                payload = _payload(row)
                presentation = _a3_presentation(payload, now=analyzed_at)
                target = presentation.get("target") if isinstance(presentation.get("target"), Mapping) else {}
                symbol = _stock_code(row.get("symbol"))
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                strategy = _STRATEGY_LABELS.get(
                    str(payload.get("strategy_profile") or ""),
                    _display_text(payload.get("strategy_profile"), limit=40, fallback="按 A3 技术计划"),
                )
                reasons = _display_items(payload.get("selection_reasons") or payload.get("reason_codes"), limit=3)
                conditions = _display_items(payload.get("required_conditions"), limit=3)
                invalidators = _display_items(
                    payload.get("overnight_invalidators")
                    or payload.get("invalidation_conditions")
                    or payload.get("invalidation_reasons")
                    or payload.get("veto_conditions"),
                    limit=4,
                )
                if not invalidators:
                    invalidators = [
                        "跌破日线失效价",
                        "开盘或盘中超过禁止追价位",
                        "必要条件未成立",
                    ]
                lines.extend(
                    [
                        f"\n**{name}（{symbol}）｜{_priority_label(payload.get('plan_priority'))}**",
                        f"• 类型：{_display_text(payload.get('stock_behavior_type'), limit=30)}；角色：{_display_text(payload.get('market_role'), limit=30)}；方向：{_theme_label(payload)}",
                        f"• 策略：{strategy}；优先依据：{'；'.join(_display_items(payload.get('priority_reasons'), limit=2)) or '按计划成熟度'}",
                        f"• 入选依据：{'；'.join(reasons) if reasons else '已通过确定性日线技术计划'}",
                        f"• 状态边界：日线设置 {_display_text(presentation.get('daily_setup_state'), limit=30)}；A4确认 {_display_text(presentation.get('a4_confirmation_state'), limit=30)}；当前入场资格 {_display_text(presentation.get('current_entry_eligibility'), limit=30)}；计划有效性 {_display_text(presentation.get('plan_validity_state'), limit=30)}",
                         f"• 参考收盘：{_number(payload.get('reference_price'))}（{_time_label(_reference_price_as_of(payload, context))}）",
                        f"• 价格计划：触发区 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}；止损 {_number(payload.get('stop_level') or payload.get('daily_invalidation'))}；禁止追价 {_number(payload.get('no_chase') or payload.get('no_chase_price') or payload.get('max_chase_price'))}",
                        f"• 压力参考：{_number(payload.get('pressure_reduce_price'))}（{_display_text(payload.get('pressure_basis'), limit=32, fallback='暂无明确压力位')}）",
                        f"• 目标证据：{_display_text(target.get('kind'), limit=32)}；{('固定R观察位，不是市场阻力证明' if target.get('claim') == 'OBSERVATION_NOT_MARKET_PROOF' else _display_text(target.get('claim'), limit=40))}",
                        f"• 盘中确认：{'；'.join(conditions) if conditions else ('由 A4 继续监测' if active_session else '等待早盘复核')}",
                        f"• 失效条件：{'；'.join(invalidators)}",
                        "• 三种情景：强势不追高，等待确认；中性进入触发区再判断；弱势跌破止损则计划作废。",
                    ]
                )
            lines.extend(["", "**执行纪律**", "• 未触发不交易；超过禁止追价位不追；跌破止损不等待模型解释。", "• 仅用于本地模拟研究，不连接真实账户。"])
            page_binding = card_plan_binding(ordered, chunk, source_run_id=source_run_id,
                trade_date=analyzed_at.date().isoformat(), displayed_symbols=symbols)
            outputs.append(
                self._send(
                    delivery_key=f"a3-premarket-professional-v4:{analyzed_at.date().isoformat()}:{batch_hash}:plans:{page}",
                    kind="PREMARKET_A3_ANALYSIS",
                    source_id=source_run_id or f"a3-premarket-{analyzed_at.date().isoformat()}",
                    title=f"A股专业盘前研究｜A3计划｜{page}/{len(chunks)}",
                    lines=lines,
                    summary={
                        "trade_date": analyzed_at.date().isoformat(),
                        "source_run_id": source_run_id,
                        "symbols": symbols,
                        "plan_ids": page_binding["plan_ids"],
                        "plan_binding": page_binding,
                        "plan_count": len(ordered),
                        "page": page,
                        "card": "plans",
                        "activation_deferred_to": (
                            "ALREADY_ACTIVE" if active_session else "09:26"
                        ),
                    },
                    now=analyzed_at,
                )
            )
        return outputs

    def publish_a5_review(
        self,
        review: Mapping[str, Any],
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Publish one persisted A5 review as a compact Chinese card.

        The database row is the delivery source of truth.  Raw prompts,
        evidence identifiers, model transport, and internal reason codes are
        deliberately excluded from the card and its safe delivery summary.
        """

        report = _json_mapping(review.get("report") or review.get("report_json"))
        facts = _json_mapping(
            review.get("fact_snapshot") or review.get("fact_snapshot_json")
        )
        review_id = str(review.get("review_id") or "").strip()
        trade_date = str(review.get("trade_date") or report.get("trade_date") or "").strip()
        review_kind = str(
            review.get("review_kind") or report.get("review_kind") or ""
        ).strip().upper()
        if not report or not facts or not review_id or not trade_date:
            return []

        kind_label = "盘中复盘" if review_kind == "MIDDAY" else "盘后复盘"
        notification_kind = (
            "A5_MIDDAY_REVIEW" if review_kind == "MIDDAY" else "A5_POST_CLOSE_REVIEW"
        )
        from ..review.fact_guard import business_metrics
        metrics = business_metrics(facts)
        data_quality = _json_mapping(facts.get("data_quality"))
        verification = _json_mapping(facts.get("independent_verification"))
        verification_status = (
            verification.get("status")
            or metrics.get("a5_independent_verification_status")
            or "UNAVAILABLE"
        )
        lines = [
            "**复盘结论**",
            *(["• 本版含服务器事实审校，计数与归因以本版为准；此前报告和原始交易记录保留。"] if report.get("fact_reconciliation") else []),
            f"• {_display_text(report.get('executive_summary'), limit=420, fallback='本次复盘摘要未提供。')}",
            f"• 总体评价：{_display_text(report.get('overall_verdict'), limit=40, fallback='尚无结论')}",
            f"• 事实截止：{_time_label(review.get('cutoff_at') or facts.get('cutoff_at'))}",
            f"• 证据质量：{_display_text(data_quality.get('status'), limit=40, fallback='尚未确认')}；异源核验：{_display_text(verification_status, limit=40, fallback='尚未确认')}",
            "",
            "**漏斗概况**",
            f"• A2：聚焦 {_number(metrics.get('a2_focus_count'))} 只，观察 {_number(metrics.get('a2_watch_count'))} 只，覆盖 {_number(metrics.get('a2_theme_count'))} 个主题。",
            f"• A3：形成 {_number(metrics.get('a3_plan_count'))} 只日线计划。",
            f"• A4：记录 {_number(metrics.get('a4_effective_event_count'))} 条业务状态事件；交易信号 {_number(metrics.get('a4_trade_signal_count'))} 条，跟踪 {_number(metrics.get('a4_lifecycle_count'))} 个信号生命周期。",
            "",
            "**分层验收**",
        ]
        for label, key in (
            ("A2 板块与选股", "a2_review"),
            ("A3 日线计划", "a3_review"),
            ("A4 盘中择时", "a4_review"),
        ):
            layer = _json_mapping(report.get(key))
            lines.append(
                f"• **{label}｜{_display_text(layer.get('verdict'), limit=32, fallback='尚无结论')}**："
                f"{_display_text(layer.get('summary'), limit=260, fallback='本次没有足够事实形成评价。')}"
            )

        signal_reviews = [
            item for item in report.get("signal_reviews", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("signal_reviews"), (list, tuple)) else []
        if signal_reviews:
            lines.extend(["", f"**信号复盘｜共 {len(signal_reviews)} 条，展示前 5 条**"])
            for item in signal_reviews[:5]:
                name = _text(item.get("name"), limit=24, fallback="名称未提供")
                symbol = _stock_code(item.get("symbol"))
                strategy = _STRATEGY_LABELS.get(
                    str(item.get("strategy_profile") or "").upper(),
                    _display_text(item.get("strategy_profile"), limit=32, fallback="策略待确认"),
                )
                attribution = _A5_ATTRIBUTION_LABELS.get(
                    str(item.get("attribution") or "").upper(),
                    _display_text(item.get("attribution"), limit=32, fallback="暂未完成归因"),
                )
                lines.append(
                    f"• **{name}（{symbol}）｜{strategy}**：{attribution}；"
                    f"{_display_text(item.get('assessment'), limit=220, fallback='评价待补充。')}"
                )

        stock_reviews = [item for item in report.get("signal_stock_reviews", ()) if isinstance(item, Mapping)]
        if stock_reviews:
            lines.extend(["", f"**当日表现与入场审计｜共 {len(stock_reviews)} 个信号事件，展示前5个**"])
            for item in stock_reviews[:5]:
                lines.append(f"• **{_text(item.get('name'), limit=24)}（{_stock_code(item.get('symbol'))}）**")
                lines.append(f"  当日表现：{_text(item.get('performance_summary'), limit=300)}")
                lines.append(f"  入场审计：{_text(item.get('entry_audit_summary'), limit=400)}")
            lines.append("价格表现未扣费，不是已实现收益；完整信号和冻结证据见复盘详情。")

        counterexamples = [
            item for item in report.get("missed_opportunity_reviews", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("missed_opportunity_reviews"), (list, tuple)) else []
        ledger = [item for item in verification.get("top_performance_ledger", ()) if isinstance(item, Mapping)]
        if not ledger:
            ledger = [item for item in _json_mapping(verification.get("a2")).get("top_performance_ledger", ()) if isinstance(item, Mapping)]
        captured = sum(str(item.get("coverage_status") or "") == "CAPTURED_EFFECTIVE_A4" for item in ledger)
        if counterexamples:
            selected: list[Mapping[str, Any]] = []
            for stage in ("A1", "A2", "A3", "A4", "UNRESOLVED"):
                item = next((row for row in counterexamples if str(row.get("funnel_drop_stage") or "UNRESOLVED") == stage and row not in selected), None)
                if item is not None:
                    selected.append(item)
            for item in counterexamples:
                if len(selected) >= 5:
                    break
                if item not in selected:
                    selected.append(item)
            scope = f"全市场强势覆盖 {len(ledger)} 只，其中产生有效盘中信号 {captured} 只；" if ledger else ""
            lines.extend(["", f"**反向拷问｜{scope}需复核 {len(counterexamples)} 只，分阶段展示 {len(selected)} 只**"])
            for item in selected:
                name = _text(item.get("name"), limit=24, fallback="名称未提供")
                symbol = _stock_code(item.get("symbol"))
                drop = _A5_DROP_STAGE_LABELS.get(
                    str(item.get("funnel_drop_stage") or "").upper(),
                    "尚未定位漏斗位置",
                )
                conclusion = "已确认缺陷" if bool(item.get("is_confirmed_defect")) else "需要继续验证"
                lines.append(
                    f"• **{name}（{symbol}）｜{drop}**："
                    f"{_display_text(item.get('observed_performance'), limit=140, fallback='表现资料未提供')}；"
                    f"{_display_text(item.get('assessment'), limit=180, fallback='判断待补充')}（{conclusion}）"
                )
            if len(counterexamples) > len(selected):
                lines.append(f"• 其余 {len(counterexamples) - len(selected)} 只已写入完整复盘和冻结证据账本，未因卡片长度丢弃。")

        defects = [
            item for item in report.get("core_defects", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("core_defects"), (list, tuple)) else []
        lines.extend(["", "**核心缺陷**"])
        if defects:
            for item in defects[:4]:
                severity = _display_text(item.get("severity"), limit=20, fallback="待分级")
                layer = _display_text(item.get("layer"), limit=20, fallback="相关环节")
                blocker_type=item.get('blocker_type')
                data_note = ('，模型复核超时' if blocker_type=='JUDGE_TIMEOUT' else
                    '，模型或网络不可用' if blocker_type=='MODEL_UNAVAILABLE' else
                    '，受数据缺口影响' if bool(item.get("blocked_by_data")) and blocker_type not in {'MIXED','OTHER_OR_UNPROVEN'} else '')
                lines.append(
                    f"• **{layer}｜{severity}**："
                    f"{_display_text(item.get('problem'), limit=240, fallback='问题描述待补充')}{data_note}"
                )
        else:
            lines.append("• 本次未确认可归因的核心缺陷。")

        proposals = [
            item for item in report.get("improvement_proposals", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("improvement_proposals"), (list, tuple)) else []
        lines.extend(["", "**改进与验证**"])
        if proposals:
            for item in proposals[:3]:
                proposal_type = _A5_PROPOSAL_LABELS.get(
                    str(item.get("type") or "").upper(), "待验证建议"
                )
                target = _display_text(item.get("target"), limit=20, fallback="相关环节")
                lines.append(
                    f"• **{target}｜{proposal_type}**："
                    f"{_display_text(item.get('proposed_change'), limit=220, fallback='建议待补充')}；"
                    + (f"待审建议观察 {_number(item.get('min_shadow_days'))} 个交易日，尚未获批。" if item.get("type") == "SHADOW_TEST"
                     else "以确定性回归和数据核对验收，不设统一观察天数。")
                )
        else:
            lines.append("• 当前证据尚不足以提出新的改进实验。")
        lines.extend(
            [
                "",
                "**执行边界**",
                "• 复盘用于发现问题和积累样本；改进建议只进入影子验证，不自动修改生产策略。",
                "• 本系统仅做本地模拟研究，不连接真实账户，不发送真实订单。",
            ]
        )
        engineering=[]
        if self.policy_root is not None:
            engineering.append(self._send(delivery_key=f'a5-engineering:{review_id}',
                kind=notification_kind+'_ENGINEERING',source_id=review_id,
                title=f'A5工程复盘｜{trade_date}',lines=lines,
                summary=dict(trade_date=trade_date,review_kind=review_kind),now=now))
            counts=(facts.get('key_count_reconciliation') or {}).get('actual') or {}
            def number(value):
                return str(value) if type(value) is int else '不可用'
            from ..review.fact_guard import execution_blocker_counts
            blockers=execution_blocker_counts(facts)
            risk=(f"今日旧发布通道{blockers.get('JUDGE_TIMEOUT',0)}条技术候选受复核超时阻断；不能据此认定成交或漏单。"
                if blockers.get('JUDGE_TIMEOUT') else
                'A2审计数据不可用，未将其写成零候选。' if metrics.get('a2_availability')=='NOT_AVAILABLE' else
                '尚无足够已成交样本检验收益，信号不代表成交。')
            lines=[f'• 今日计划：{number(metrics.get("a3_plan_count"))}只。',
                f'• 已发布交易信号：{number(metrics.get("a4_trade_signal_count"))}条；技术候选不等于已发布信号。',
                f'• 成交记录：{number(counts.get("fills"))}笔；以成交账本为准。',
                '• 主要风险：'+risk,
                '• 接下来：系统按自然调度继续核验，复盘提案待审，不自动改变策略。']
        return [
            *engineering,
            self._send(
                delivery_key=f"a5-review:{review_id}",
                kind=notification_kind,
                source_id=review_id,
                title=f"A股 A5 {kind_label}｜{trade_date}",
                lines=lines,
                summary={
                    "trade_date": trade_date,
                    "review_kind": review_kind,
                    "overall_verdict": report.get("overall_verdict"),
                    "signal_count": len(signal_reviews),
                    "counterexample_count": len(counterexamples),
                    "top_performance_ledger_count": len(ledger),
                    "top_performance_captured_count": captured,
                    "defect_count": len(defects),
                    "proposal_count": len(proposals),
                },
                now=now,
            )
        ]

    def publish_a5_task_failure(
        self,
        review_kind: str,
        *,
        reason_code: str,
        diagnostics: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        """Emit one actionable failure card per trade date and A5 period."""

        kind = str(review_kind).upper()
        label = "午间复盘" if kind == "MIDDAY" else "盘后复盘"
        retryable_codes = {
            "MODEL_TOTAL_DEADLINE_EXCEEDED", "MODEL_WALL_CLOCK_TIMEOUT",
            "NETWORK_RETRY_EXHAUSTED", "RATE_LIMIT_RETRY_EXHAUSTED",
            "UPSTREAM_5XX_RETRY_EXHAUSTED", "A5_OUTPUT_SCHEMA_INVALID",
        }
        retryable = reason_code in retryable_codes
        actual = diagnostics.get("prompt_chars")
        limit = diagnostics.get("limit_chars")
        input_hash = str(diagnostics.get("input_hash") or "UNAVAILABLE")
        if reason_code == "A5_MODEL_CONTEXT_TOO_LARGE":
            failure_detail = "模型输入超过本地硬上限，未发起模型调用。"
        elif reason_code == "A5_OUTPUT_EVIDENCE_INVALID":
            failure_detail = "模型已返回结果，但引用的证据编号无效；不是输入超限。"
        else:
            failure_detail = "失败原因见失败码；输入大小仅供定位，不能据此判定超限。"
        input_status = (
            "超限" if isinstance(actual, int) and isinstance(limit, int) and actual > limit
            else "未超限" if isinstance(actual, int) and isinstance(limit, int)
            else "未测量"
        )
        lines = [
            "**任务状态：失败**",
            f"• 任务：A5 {label}",
            f"• 失败码：{reason_code}",
            f"• 原因：{failure_detail}",
            f"• 输入检查：{input_status}",
            f"• 实际输入大小：{actual if actual is not None else '未完成测量'} 字符",
            f"• 硬上限：{limit if limit is not None else '未进入提示词检查'} 字符",
            f"• 输入哈希：{input_hash}",
            f"• 可否直接重试：{'可以，复用同一冻结事实' if retryable else '不可以，需先修复失败原因'}",
            "• 本次未生成复盘结论；原始事实与历史 A4 信号未修改。",
        ]
        return self._send(
            delivery_key=f"a5-task-health:failure:{now.date().isoformat()}:{kind}",
            kind="A5_TASK_FAILURE",
            source_id=input_hash,
            title=f"A股 A5 {label}失败｜{now.date().isoformat()}",
            lines=lines,
            summary={
                "trade_date": now.date().isoformat(),
                "review_kind": kind,
                "reason_code": reason_code,
                "actual_chars": actual,
                "hard_limit_chars": limit,
                "fact_digest": input_hash,
                "retryable": retryable,
            },
            now=now,
        )

    def publish_a5_task_recovery(
        self,
        review_kind: str,
        *,
        input_hash: str,
        now: datetime,
    ) -> dict[str, Any]:
        """Send recovery only when the matching period has a sent failure."""

        kind = str(review_kind).upper()
        failure_key = f"a5-task-health:failure:{now.date().isoformat()}:{kind}"
        failure = self.store.get_delivery_by_key(failure_key)
        if failure is None or str(failure.get("status")) != "SENT":
            return {"status": "NOOP", "reason_code": "NO_SENT_A5_FAILURE"}
        label = "午间复盘" if kind == "MIDDAY" else "盘后复盘"
        return self._send(
            delivery_key=f"a5-task-health:recovery:{now.date().isoformat()}:{kind}",
            kind="A5_TASK_RECOVERY",
            source_id=input_hash,
            title=f"A股 A5 {label}已恢复｜{now.date().isoformat()}",
            lines=[
                "**任务状态：已恢复**",
                f"• 任务：A5 {label}",
                f"• 输入哈希：{input_hash}",
                "• 已使用冻结事实完成复盘；没有重新采集行情，没有修改历史 A4 信号。",
            ],
            summary={
                "trade_date": now.date().isoformat(),
                "review_kind": kind,
                "fact_digest": input_hash,
                "recovered": True,
            },
            now=now,
        )

    def publish_signal_day_review(self, facts: Mapping[str, Any], *, now: datetime) -> list[dict[str, Any]]:
        """Deterministic post-close card; independent of the A5 model result."""
        if facts.get("review_kind") != "POST_CLOSE":
            return []
        rows = list(facts.get("signal_stock_reviews") or [])
        outputs = []
        for offset in range(0, len(rows), 10):
            page = rows[offset:offset + 10]
            digest = hashlib.sha256(json.dumps(page, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
            lines = ["策略信号不等于成交。以下为价格观察，未成交样本也持续跟踪T+N，不计入成交收益。"]
            for row in page:
                audit = row.get("entry_audit") or {}
                lines.extend(["", f"**{_text(row.get('name'))}（{_stock_code(row.get('symbol'))}）**",
                    f"• 信号时间：{_time_label(audit.get('signal_at'))}；参考价：{_number(audit.get('signal_reference_price'))}",
                    f"• {row.get('performance_summary') or '表现资料不足'}",
                    f"• {audit.get('fill_summary') or '未确认成交'}"])
            outputs.append(self._send(
                delivery_key=f"a4-signal-day:{facts.get('trade_date')}:{offset // 10 + 1}:{digest}",
                kind="A4_SIGNAL_DAY_REVIEW", source_id=str(facts.get("input_hash") or digest),
                title=f"当日信号表现｜{facts.get('trade_date')}｜第{offset // 10 + 1}页",
                lines=lines, summary={"signal_count": len(rows), "rows": page}, now=now,
            ))
        return outputs

    def publish_indicator_preparation(self, preparation, *, now):
        missing = [row for row in preparation.get("plans", []) if row.get("status") != "READY"]
        if not missing:
            return []
        digest = hashlib.sha256(json.dumps(missing, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
        return [self._send(delivery_key=f"a4-indicator-history:{digest}", kind="A4_INDICATOR_READINESS",
            source_id=digest, title="盘前数据准备告警｜520历史不足",
            lines=["A3日线计划不因此自动淘汰，但520盘中辅助指标尚未具备完整历史。"] + [
                f"• {_stock_code(row.get('symbol'))}：目标{row.get('target_trade_date')}；五分钟历史{row.get('available_5m_count', 0)}/144。"
                for row in missing], summary=preparation, now=now)]

    def publish_a4_execution_results(self, results, *, now):
        outputs = []
        for row in results:
            if row.get("action") not in {"BUY", "ADD"} or row.get("status") == "DUPLICATE":
                continue
            if row.get("status") not in {"FILLED", "BLOCKED", "CANCELLED"}:
                continue
            filled = row.get("status") == "FILLED"
            symbol = _stock_code(row.get("symbol"))
            name = _text(row.get("name"), limit=30, fallback="名称未提供")
            decision_context = row.get("decision_context") if isinstance(row.get("decision_context"), Mapping) else {}
            execution_bar = row.get("execution_bar") if isinstance(row.get("execution_bar"), Mapping) else {}
            account = row.get("account_snapshot") if isinstance(row.get("account_snapshot"), Mapping) else {}
            gross = float(row.get("qty") or 0) * float(row.get("price") or 0)
            lines = [
                "**成交结论**" if filled else "**执行结论**",
                f"• 标的：{name}（{symbol}）",
                f"• 方向：{_ACTION_LABELS.get(str(row.get('action') or ''), '买入')}；结果：{'已模拟成交' if filled else '未成交终止'}",
                f"• 原因：{_display_text(row.get('reason_code'), fallback='执行条件已满足' if filled else '执行条件未满足')} ",
                "",
                "**价格与数量**",
                f"• 信号时间：{_time_label(row.get('signal_time'))}；信号参考价：{_number(row.get('signal_reference'))}；限价上限：{_number(row.get('limit_price'))}",
                (
                    f"• 模拟成交价：{_number(row.get('price'))}；数量：{row.get('qty', 0)}股；成交额：{_number(gross)}；费用：{_number(row.get('fee'))}"
                    if filled else
                    "• 未生成模拟持仓，未成交信号不自动追价。"
                ),
                f"• 从信号到可执行完整分钟：{_number(row.get('fill_delay_seconds'))}秒。",
                "",
                "**模拟成交依据**",
                "• 执行口径：信号后下一根可交易的完整一分钟K线模拟撮合；这不是交易所真实成交回报。",
            ]
            if execution_bar:
                lines.extend([
                    f"• 行情来源：{_minute_source_label(execution_bar.get('source_id'))}；K线结束：{_time_label(execution_bar.get('bar_end'))}",
                    f"• 开/高/低/收：{_number(execution_bar.get('open'))}/{_number(execution_bar.get('high'))}/{_number(execution_bar.get('low'))}/{_number(execution_bar.get('close'))}；成交量：{_number(execution_bar.get('volume'))}",
                ])
            else:
                lines.append("• 未生成符合价格契约的完整分钟成交记录。")
            if decision_context:
                lines.extend(_a4_decision_lines(decision_context))
            lines.extend([
                "",
                "**模拟账户**",
                f"• 权益：{_number(account.get('equity'))}；现金：{_number(account.get('cash'))}；持仓：{account.get('position_total_qty', 0)}股；当日可卖：{account.get('position_sellable_qty', 0)}股。",
                "• A股当日新买入股份不可卖出；信号表现与模拟成交收益分开跟踪。",
            ])
            outputs.append(self._send(delivery_key=f"a4-execution:{row.get('account_id')}:{row.get('signal_id')}:{row.get('status')}",
                kind="A4_EXECUTION_RESULT", source_id=str(row.get("signal_id")),
                title=f"A4模拟成交回执｜{name}（{symbol}）｜{'已成交' if filled else '未成交'}",
                lines=lines, summary=dict(row), now=now))
        return outputs

    def publish_a4_events(
        self,
        events: Sequence[Mapping[str, Any]],
        *,
        plans: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        for event in events:
            event_payload = _payload(event)
            pending_value = event_payload.get("risk_pending")
            pending = pending_value if isinstance(pending_value, Mapping) else {}
            strategy_value = event_payload.get("strategy")
            event_strategy = strategy_value if isinstance(strategy_value, Mapping) else {}
            eligibility_value = event_strategy.get("execution_eligibility")
            eligibility = eligibility_value if isinstance(eligibility_value, Mapping) else {}
            exit_blocked = event.get("action") in {"SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"} and (
                not eligibility or eligibility.get("reason") is not None
                or type(eligibility.get("sellable_qty")) is not int
                or eligibility.get("sellable_qty", 0) <= 0)
            if pending or exit_blocked:
                plan_id = str(event.get("plan_id") or event_payload.get("plan_id") or "")
                symbol = _stock_code(event.get("symbol") or event_payload.get("symbol"))
                t1_locked = (event.get("reason_code") in {"T1_LOCKED", "T1_RISK_PENDING"}
                    or eligibility.get("reason") in {"T1_LOCKED", "BLOCKED_T1"})
                day = now.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
                source_id = f"{event.get('lane_id') or ''}:{symbol}:{day}:risk-pending"
                outputs.append(self._send(
                    delivery_key=f"a4-risk-pending:{source_id}",
                    kind="A4_RISK_PENDING", source_id=source_id,
                    title=f"A4 风险提示｜{symbol}｜{'T+1锁定' if t1_locked else '可卖数量未核实'}",
                    lines=["**风险待办，不是可执行交易信号**",
                        "当日买入股份受T+1限制，今天不可卖；下一交易日开盘按新价格重新评估，不自动执行旧信号。"
                        if t1_locked else "可卖数量未核实，禁止执行；核实账户及持仓批次后重新评估。",
                        f"• 时间：{_time_label(event.get('minute_end') or now.isoformat())}",
                        f"• 风险观察价：{_number(pending.get('trigger_price'))}；行情时点：{_time_label(pending.get('trigger_bar_end'))}",
                        f"• 风险原因：{_display_text(pending.get('trigger_reason') or event.get('reason_code'), limit=100)}"],
                    summary={"plan_id": plan_id, "symbol": symbol, "action": "NO_ACTION",
                        "reason_code": event.get("reason_code"), "executable": False}, now=now))
                continue
            if not bool(event.get("effective")) and event.get("action") != "DATA_BLOCK":
                continue
            action = str(event.get("action") or "")
            if action in _A4_SILENT_ACTIONS:
                continue
            if action not in _A4_TRADE_ACTIONS | {"DATA_BLOCK"}:
                continue
            event_payload = _payload(event)
            data_reason = str(event.get("reason_code") or event_payload.get("reason_code") or "")
            if action == "DATA_BLOCK" and (data_reason.startswith(("TENCENT_", "NODE_", "MINUTE_PROVIDER_", "MINUTE_FETCH_", "MINUTE_PUBLICATION_RETRY_")) or data_reason in {
                "MINUTE_FETCH_BUDGET_EXHAUSTED", "CURRENT_SESSION_WINDOW_INVALID", "MINUTE_DATA_FETCH_FAILED",
                "CLOSE_BAR_FINALIZATION_UNCONFIRMED",
                "EXECUTION_NATIVE_5M_CONFLICT", "MINUTE_PUBLICATION_PENDING",
                "MINUTE_PUBLICATION_UNCONFIRMED", "MINUTE_DATA_NOT_CURRENT",
                "MINUTE_PUBLICATION_REPLAY_MISMATCH",
                "MINUTE_DATA_GAP", "MINUTE_DATA_UNAVAILABLE",
                "STALE_1M", "BAR_NOT_CURRENT_1M", "AS_OF_TIMESTAMP_MISSING",
                "FUTURE_BAR_DETECTED",
            }):
                # Shared acquisition incidents have their own aggregate card;
                # the 15:00 pending-finality state is a post-close archive task.
                continue
            # A shared live-market outage is reported once by
            # ``publish_a4_system_health``.  Suppress the five equivalent
            # per-plan DATA_BLOCK cards it would otherwise produce.  A
            # symbol-level minute-data block keeps its existing card.
            if _is_system_data_block(event, event_payload):
                continue
            strategy_result = event_payload.get("strategy") if isinstance(event_payload.get("strategy"), Mapping) else {}
            plan_id = str(event.get("plan_id") or event_payload.get("plan_id") or "")
            plan = plans.get(plan_id, {})
            payload = _payload(plan)
            symbol = _stock_code(event.get("symbol") or event_payload.get("symbol") or plan.get("symbol"))
            name = _text(payload.get("name"), limit=30, fallback="名称未提供")
            strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _display_text(payload.get("strategy_profile"), limit=40, fallback="按个股计划执行"))
            # An A3 authored condition list is not an A4 same-tick result.
            # Empty lists are valid zero; absence is explicitly unavailable.
            met_values=event.get("met_conditions") if 'met_conditions' in event else strategy_result.get("met_conditions")
            unmet_values=event.get("unmet_conditions") if 'unmet_conditions' in event else strategy_result.get("unmet_conditions")
            met = _condition_summary(met_values)
            unmet = _condition_summary(unmet_values)
            veto = _display_items(event.get("veto_conditions") or strategy_result.get("veto_conditions") or payload.get("veto_conditions"), limit=3)
            source_id = str(event.get('event_id') or f"{event.get('lane_id') or ''}:{plan_id}:{action}:{event.get('minute_end')}")
            title = f"A4 盘中信号｜{name}（{symbol}）｜{_ACTION_LABELS.get(action, _display_text(action))}"
            contract = event_payload.get("entry_contract") or {}
            decision_context = (
                event_payload.get("decision_context")
                if isinstance(event_payload.get("decision_context"), Mapping)
                else build_a4_decision_context(payload, strategy_result, contract)
            )
            is_data_alert = action == "DATA_BLOCK" or (
                action in {"BUY_SIGNAL", "ADD_SIGNAL"} and contract.get("status") != "READY")
            if is_data_alert:
                title = f"A4 数据告警｜{name}（{symbol}）｜{'行情或执行数据未就绪' if action == 'DATA_BLOCK' else '入场契约未就绪'}"
            judge_timeout=event.get('reason_code') in {'MONITOR_OVERRUN','MODEL_WALL_CLOCK_TIMEOUT','TRADER_GLOBAL_DEADLINE_EXCEEDED'}
            if judge_timeout:title=f"A4 模型超时｜{name}（{symbol}）｜本轮未形成有效裁决"
            network_unavailable=(action=='DATA_BLOCK' and (str(event_payload.get('diagnostic_code') or strategy_result.get('llm_reason_code') or '').startswith('NETWORK')
                or data_reason in {'NETWORK_ERROR','MODEL_NETWORK_ERROR'}))
            if network_unavailable:title=f"A4 模型复核不可用（网络）｜{name}（{symbol}）"
            eligibility = strategy_result.get("execution_eligibility") or {}
            execution_lines: list[str] = []
            if action in {"BUY_SIGNAL", "ADD_SIGNAL"}:
                ready = contract.get("status") == "READY"
                if ready:
                    title += "｜待成交"
                execution_lines = ["", "**执行状态**",
                    "• 条件成立不代表委托已成交，不以信号推定持仓。",
                    f"• 信号参考价：{_number(contract.get('signal_reference'))}；限价买入上限：{_number(contract.get('limit_price'))}",
                    "• 仅下一根可交易完整分钟尝试模拟撮合；未成交即终止，不自动追价。" if ready else "• 缺少有效冻结执行契约，不能认定为可执行委托。",
                    "• 当日新买入股份受T+1限制，当日不可卖出。"]
            if action in {"SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"}:
                blocked = eligibility.get("reason")
                explanation = (
                    "当日买入股份受T+1限制，尚未执行；风险任务保留至后续交易时段。"
                    if blocked in {"T1_LOCKED", "BLOCKED_T1"} else
                    "可卖数量尚未核实，禁止执行。" if not eligibility or blocked else
                    "仅对可卖部分进入执行核验；技术触发不代表已经成交。"
                )
                execution_lines = ["", "**执行状态**",
                    f"• 总持仓：{eligibility.get('total_qty', '未核实')}股；可卖：{eligibility.get('sellable_qty') if eligibility.get('sellable_qty') is not None else '未核实'}股",
                    f"• {explanation}"]
            lines = [
                "**模型复核不可用（网络），不是数据缺失或交易信号**" if network_unavailable else "**模型复核超时，不是数据缺失或交易信号**" if judge_timeout else "**数据条件告警，不是交易信号**" if is_data_alert else "**盘中交易条件触发**",
                 f"• 时间：{_time_label(event.get('minute_end') or now.isoformat())}",
                f"• 股票：{name}（{symbol}）",
                f"• 策略：{strategy}",
                f"• 动作：{'模型复核不可用（网络）' if network_unavailable else _ACTION_LABELS['JUDGE_TIMEOUT'] if judge_timeout else _ACTION_LABELS.get(action, _display_text(action))}",
                f"• 触发原因：{_display_text(event.get('reason_code'), limit=100, fallback='确定性条件成立')}",
                "",
                "**条件核对**",
                f"• 已满足：{met}",
                f"• 未满足：{unmet}",
                f"• 条件计数：满足{len(met_values) if isinstance(met_values,(list,tuple)) else '不可用'} / 未满足{len(unmet_values) if isinstance(unmet_values,(list,tuple)) else '不可用'}；仅引用该轮原件。",
                f"• 否决边界：{'；'.join(veto) if veto else '无新增否决条件'}",
                "",
                "**价格纪律**",
                *_a4_price_discipline(payload,strategy_result,event_payload),
            ]
            if not is_data_alert and action in {"BUY_SIGNAL", "ADD_SIGNAL"}:
                lines.extend(_a4_decision_lines(decision_context))
            if network_unavailable:
                lines.append('• 接下来：本轮模型网络请求未取得可用回复；保留失败证据，按原自然节拍处理，不人工放行。')
            elif judge_timeout:
                lines.append('• 接下来：本轮复核没有有效结论；不以超时推定买入，下一自然节拍按原门复核。')
            elif is_data_alert:
                lines.append('• 接下来：等待该计划所需证据恢复，仍按原数据与风控门拒绝不合格入场。')
            outputs.append(
                self._send(
                    delivery_key=f"a4:{source_id}",
                    kind="A4_JUDGE_TIMEOUT" if judge_timeout else "A4_DATA_ALERT" if is_data_alert else "A4_EFFECTIVE",
                    source_id=source_id,
                    title=title,
                    lines=lines + execution_lines,
                    summary={"minute_end": event.get("minute_end"), "symbol": symbol, "action": action, "reason_code": event.get("reason_code"), "execution_eligibility": eligibility,
                             "plan_id": plan_id, "source_run_id": payload.get("source_run_id"),
                             "output_state":"JUDGE_TIMEOUT" if judge_timeout else action},
                    now=now,
                )
            )
        return outputs

    def publish_rotation_theme_health(
        self,
        snapshot: Mapping[str, Any] | None,
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Alert once per A2 free-source blocker/warning and once on recovery."""

        state = dict(snapshot or {})
        trade_date = _trade_date_label(state, now)
        source_id = str(state.get("source_id") or "LIANGJIAN_FREE_ROTATION_V1").strip().upper()
        available = state.get("available") is True
        quality = state.get("quality") if isinstance(state.get("quality"), Mapping) else {}
        raw_warnings = (
            state.get("warning_codes")
            or state.get("warnings")
            or quality.get("warning_codes")
            or ()
        )
        if isinstance(raw_warnings, str):
            raw_warnings = (raw_warnings,)
        warnings = [str(value).strip().upper() for value in raw_warnings if str(value).strip()]
        source_health = state.get("source_health") if isinstance(state.get("source_health"), Mapping) else {}
        membership_warnings = source_health.get("membership_update_warnings")
        if isinstance(membership_warnings, Mapping):
            warnings.extend(
                str(value).strip().upper()
                for value in membership_warnings.values()
                if str(value).strip()
            )
        for key, public_reason in (
            ("eastmoney_catalog", "EASTMONEY_BOARD_SOURCE_UNAVAILABLE"),
            ("eastmoney_board_flow", "EASTMONEY_BOARD_SOURCE_UNAVAILABLE"),
            ("tencent_flow", "TENCENT_FUND_COVERAGE_INSUFFICIENT"),
        ):
            source_reason = str(source_health.get(key) or "").strip().upper()
            if source_reason and source_reason not in {"OK", "NOT_REQUESTED"}:
                warnings.append(public_reason)
        reasons = warnings if available else [
            str(state.get("reason_code") or "ROTATION_THEME_NOT_READY").strip().upper()
        ]
        status = "WARNING" if available and reasons else ("READY" if available else "BLOCKED")
        selected_count = _bounded_count(
            state.get("selected_board_count"),
            default=len(state.get("selected_primary_boards") or ()),
        )
        member_age = quality.get("membership_age_days")
        coverage = quality.get("coverage") if isinstance(quality.get("coverage"), Mapping) else {}

        if status in {"BLOCKED", "WARNING"}:
            outputs: list[dict[str, Any]] = []
            for reason in dict.fromkeys(reasons):
                reason_key = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:12]
                blocked = status == "BLOCKED"
                title_state = "阻断" if blocked else "降级"
                lines = [
                    "**A2板块数据系统告警**",
                    f"• 交易日期：{trade_date}。",
                    f"• 当前状态：{'趋势通道已失败关闭' if blocked else '继续使用合规的本地成员版本'}。",
                    f"• 原因：{_rotation_health_reason(reason)}。",
                    f"• 已形成一级方向：{selected_count} 个。",
                ]
                if member_age is not None:
                    lines.append(f"• 本地成员版本年龄：{_number(member_age)} 天。")
                if coverage:
                    lines.append(
                        f"• 数据覆盖：资金 {_percentage(coverage.get('fund'))}，"
                        f"行情 {_percentage(coverage.get('price'))}。"
                    )
                lines.append("• 处理：保持既定覆盖门槛，数据恢复并重新核验后再开放趋势筛选。")
                outputs.append(
                    self._send(
                        delivery_key=f"a2-rotation-health:{status.lower()}:{trade_date}:{source_id}:{reason_key}",
                        kind="A2_ROTATION_HEALTH",
                        source_id=f"a2-rotation-health:{trade_date}",
                        title=f"A2系统告警｜板块数据{title_state}｜{trade_date}",
                        lines=lines,
                        summary={
                            "trade_date": trade_date,
                            "state": status,
                            "source_id": source_id,
                            "reason_code": reason,
                            "selected_primary_count": selected_count,
                        },
                        now=now,
                    )
                )
            return outputs

        try:
            latest = self.store.list_notification_deliveries(kind="A2_ROTATION_HEALTH", limit=1)
            prior = _json_mapping(latest[0].get("payload_json")) if latest else {}
        except Exception:
            prior = {}
        if (
            str(prior.get("trade_date") or "") != trade_date
            or str(prior.get("state") or "").upper() not in {"BLOCKED", "WARNING"}
        ):
            return []
        return [
            self._send(
                delivery_key=f"a2-rotation-health:recovery:{trade_date}:{source_id}",
                kind="A2_ROTATION_HEALTH",
                source_id=f"a2-rotation-health:{trade_date}",
                title=f"A2系统恢复｜板块数据可用｜{trade_date}",
                lines=[
                    "**A2板块数据系统恢复**",
                    f"• 交易日期：{trade_date}。",
                    f"• 已形成一级方向：{selected_count} 个。",
                    "• 板块成员、行情、资金覆盖和强度结果已重新核验。",
                ],
                summary={
                    "trade_date": trade_date,
                    "state": "READY",
                    "source_id": source_id,
                    "selected_primary_count": selected_count,
                    "recovery": True,
                },
                now=now,
            )
        ]

    def publish_minute_source_health(self, failures: Mapping[str, str], *, now: datetime,
                                     auxiliary_failures: Mapping[str, str] | None = None,
                                     pending_failures: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
        """One durable incident/recovery per source-window failure, not per plan."""
        day = now.date().isoformat()
        auxiliary = {
            symbol: reason
            for symbol, reason in (auxiliary_failures or {}).items()
            # This is an intentional off-critical-path deferral, already
            # recorded in the minute audit.  Treating it as a source outage
            # caused a false DEGRADED/READY alert cycle every five minutes.
            if reason != "AUXILIARY_5M_DEFERRED"
        }
        # A first bounded publication wait is visible in the minute audit,
        # not a fault/recovery card. Persistent waits arrive as failures.
        if pending_failures and not failures and not auxiliary:
            return []
        state = "BLOCKED" if failures else "DEGRADED" if auxiliary else "READY"
        previous = self.store.list_notification_deliveries(kind="A4_MINUTE_SOURCE_HEALTH", limit=1)
        old = _json_mapping(previous[0].get("payload_json")) if previous else {}
        same_day = old.get("trade_date") == day
        reasons = sorted(set(failures.values()) | set(auxiliary.values()))
        if (same_day and old.get("state") == state and old.get("reasons") == reasons
                and set(failures).issubset(old.get("symbols", []))
                and set(auxiliary).issubset(old.get("auxiliary_symbols", []))
                and state != "READY" and previous[0].get("status") == "SENT"):
            return []
        if (same_day and old.get("state") == state and old.get("reasons") == reasons
                and old.get("symbols") == sorted(failures)
                and old.get("auxiliary_symbols", []) == sorted(auxiliary)
                and previous[0].get("status") == "SENT"):
            return []
        if state == "READY" and not (same_day and old.get("state") in {"BLOCKED", "DEGRADED"}):
            return []
        title = "A4行情校验未通过｜暂停新增仓" if failures else "A4辅助五分钟源降级" if auxiliary else "A4行情校验恢复"
        lines = [f"• 时间：{now.strftime('%H:%M:%S')}；受影响股票：{len(set(failures) | set(auxiliary))}只。",
                 "• 通知类型：行情质量提醒，不是买入信号或委托失败回报。",
                 "• 必需窗口缺失或冲突，受影响股票暂停新增仓；已有持仓按可用风险数据独立监控，其他股票继续。" if failures else
                 "• 当日一分钟及派生五/十五分钟可用，原生五分钟复核暂缺；历史指标预热仍独立校验，不代表全部策略已就绪。" if auxiliary else
                 "• 当日执行及辅助窗口恢复，后续重新判断策略；不代表买点成立或委托成交，不补发历史信号。"]
        if failures:
            lines.append("• 涉及股票：" + "、".join(_stock_code(s) for s in sorted(failures)[:15]))
        if auxiliary:
            lines.append("• 辅助复核缺项：" + "、".join(_stock_code(s) for s in sorted(auxiliary)[:15]))
        if pending_failures:
            lines.append(f"• 另有{len(pending_failures)}只行情版本待确认，仍暂停新增仓；本通知不表示其已恢复。")
        if reasons:
            lines.append("• 原因：" + "、".join(_DISPLAY_LABELS.get(reason, "必需行情数据不可用") for reason in reasons))
        return [self._send(delivery_key=f"a4-minute-source:{now.isoformat()}:{state}",
            kind="A4_MINUTE_SOURCE_HEALTH", source_id=f"minute-source:{day}", title=title,
            lines=lines, summary={"trade_date": day, "state": state, "reasons": reasons,
                                  "event_category": "MARKET_DATA_QUALITY",
                                  "order_outcome": "NOT_AN_ORDER_RESULT",
                                  "symbols": sorted(failures), "affected_count": len(failures),
                                  "auxiliary_symbols": sorted(auxiliary),
                                  "pending_symbols": sorted(pending_failures or {})}, now=now)]

    def publish_position_data_health(self, affected: Mapping[str, Mapping[str, Any]], *, now: datetime) -> list[dict[str, Any]]:
        """Persistent incident/impact-change/recovery notice, never a trade."""
        day = now.date().isoformat()
        detail = {key: dict(value) for key, value in sorted(affected.items())}
        state = "BLOCKED" if detail else "READY"
        previous = self.store.list_notification_deliveries(kind="A4_POSITION_DATA_HEALTH", limit=1)
        old = _json_mapping(previous[0].get("payload_json")) if previous else {}
        same = old.get("trade_date") == day
        if same and old.get("positions") == detail and previous[0].get("status") == "SENT":
            return []
        if not detail and not (same and old.get("state") == "BLOCKED"):
            return []
        lines = [f"• 时间：{now.strftime('%H:%M:%S')}；受影响持仓：{len(detail)}个。"]
        for key, value in list(detail.items())[:15]:
            description = ("当前一分钟可信，只能检查硬止损；多周期退出暂不可核验" if value.get("status") == "HARD_STOP_ONLY"
                           else "当前价格或数据一致性不可确认，持仓风险暂不可观测")
            lines.append(f"• {_stock_code(str(value.get('symbol') or key))}：{description}。")
        lines.append("• 不构造价格、不生成虚假成交；T+1及可卖数量规则保持不变。" if detail else
                     "• 当前已无持仓数据阻断（输入恢复或持仓已结束）；不补发历史交易信号。")
        return [self._send(delivery_key=f"a4-position-health:{now.isoformat()}:{state}",
            kind="A4_POSITION_DATA_HEALTH", source_id=f"position-health:{day}",
            title="A4持仓风险数据受限" if detail else "A4持仓数据阻断解除", lines=lines,
            summary={"trade_date": day, "state": state, "positions": detail}, now=now)]

    def publish_a4_plan_scope(self, *, active_count: int, now: datetime) -> list[dict[str, Any]]:
        """One missing-scope alert and one recovery per session, not per minute."""
        day = now.date().isoformat()
        previous = self.store.list_notification_deliveries(kind="A4_PLAN_SCOPE", limit=20)
        alerted = any(str(row.get("source_id")) == f"a4-scope:{day}:empty"
                      and row.get("status") == "SENT" for row in previous)
        if active_count and not alerted:
            return []
        state = "recovered" if active_count else "empty"
        return [self._send(
            delivery_key=f"a4-scope:{day}:{state}", kind="A4_PLAN_SCOPE",
            source_id=f"a4-scope:{day}:{state}",
            title=f"A4执行池{'恢复' if active_count else '为空'}｜{day}",
            lines=[f"• 当前有效执行计划：{active_count}只。",
                   "• 无有效计划不等于策略逐股检查后没有买点；请核查A3发布、目标交易日和晨审。" if not active_count
                   else "• 已有当日有效计划；仅从当前时点继续检查，不补发历史交易信号。",
                   "• 本消息是运行状态提醒，不是买入信号；已有持仓保护独立执行。"],
            summary={"trade_date": day, "active_count": active_count, "state": state}, now=now)]

    def publish_a4_system_health(
        self,
        live_market_state: Mapping[str, Any] | None,
        *,
        affected_plan_count: int,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Publish one aggregate A4 market-health alert or recovery.

        The key is scoped to the current five-minute bucket.  A blocked state
        therefore produces at most one card for the bucket even when several
        plans receive the same fail-closed DATA_BLOCK result.  Recovery is
        emitted after an in-call retry or after the first later READY state
        following a durable system-health alert.
        """

        state = dict(live_market_state or {})
        status = str(state.get("status") or "").strip().upper()
        if status not in {"DATA_BLOCKED", "READY", "READY_DEGRADED"}:
            return []
        trade_date = _trade_date_label(state, now)
        bucket = _health_bucket_label(state, now)
        diagnostics = state.get("diagnostics")
        diagnostics = dict(diagnostics) if isinstance(diagnostics, Mapping) else {}
        attempt_rows = diagnostics.get("attempts")
        attempt_rows = list(attempt_rows) if isinstance(attempt_rows, (list, tuple)) else []
        total_attempts = _bounded_count(
            diagnostics.get("total_attempts"),
            default=len(attempt_rows) or 1,
        )
        recovered_after_retry = bool(diagnostics.get("recovered_after_retry"))
        affected = max(0, _bounded_count(affected_plan_count, default=0))
        last_attempt = attempt_rows[-1] if attempt_rows and isinstance(attempt_rows[-1], Mapping) else {}
        full = last_attempt.get("full_market") if isinstance(last_attempt.get("full_market"), Mapping) else {}
        index = last_attempt.get("index_fallback") if isinstance(last_attempt.get("index_fallback"), Mapping) else {}
        fallback_only = bool(diagnostics.get("fallback_only"))
        source = str(state.get("source") or "")
        if full:
            full_status = str(full.get("status") or "DATA_BLOCKED").upper()
            full_reason = str(full.get("reason_code") or "A4_LIVE_MARKET_SOURCE_UNAVAILABLE")
        elif fallback_only and diagnostics.get("full_market_reason_code"):
            full_status = "DATA_BLOCKED"
            full_reason = str(diagnostics["full_market_reason_code"])
        elif source == "HITHINK_FULL_MARKET":
            full_status = str(state.get("status") or "DATA_BLOCKED").upper()
            full_reason = str(state.get("reason_code") or "A4_LIVE_MARKET_SOURCE_UNAVAILABLE")
        else:
            full_status = "NOT_ATTEMPTED"
            full_reason = "FULL_MARKET_NOT_ATTEMPTED"
        if index:
            index_status = str(index.get("status") or "DATA_BLOCKED").upper()
            index_reason = str(index.get("reason_code") or "A4_LIVE_MARKET_SOURCE_UNAVAILABLE")
        elif source == "TENCENT_INDEX_FALLBACK":
            index_status = str(state.get("status") or "DATA_BLOCKED").upper()
            index_reason = str(state.get("reason_code") or "A4_LIVE_MARKET_SOURCE_UNAVAILABLE")
        else:
            index_status = "NOT_ATTEMPTED"
            index_reason = "FULL_MARKET_READY"
        full_observed = _bounded_count(full.get("observed_count"), default=(
            state.get("observed_count") if source == "HITHINK_FULL_MARKET" else 0
        ))
        full_expected = _bounded_count(full.get("expected_count"), default=(
            state.get("expected_count") if source == "HITHINK_FULL_MARKET" else 0
        ))
        index_observed = _bounded_count(index.get("observed_count"), default=(
            state.get("observed_count") if source == "TENCENT_INDEX_FALLBACK" else 0
        ))
        index_expected = _bounded_count(index.get("expected_count"), default=4)
        summary = {
            "trade_date": trade_date,
            "bucket": bucket,
            "state": status,
            "affected_plan_count": affected,
            "total_attempts": total_attempts,
            "recovered_after_retry": recovered_after_retry,
            "full_market_status": full_status,
            "full_market_reason_code": full_reason,
            "full_market_observed_count": full_observed,
            "full_market_expected_count": full_expected,
            "index_fallback_status": index_status,
            "index_fallback_reason_code": index_reason,
            "index_fallback_observed_count": index_observed,
            "index_fallback_expected_count": index_expected,
        }

        if status == "DATA_BLOCKED":
            delivery_key = f"a4-system-health:blocked:{trade_date}:{bucket}"
            lines = [
                "**A4盘中系统告警**",
                f"• 发生时间：{_time_label(state.get('as_of') or now.isoformat())}",
                f"• 受影响计划：{affected} 只；本次已停止新开仓判断。",
                f"• {_LIVE_MARKET_SOURCE_LABELS['HITHINK_FULL_MARKET']}：{_live_market_status(full_status)}；观察 {full_observed}/{full_expected}；{_live_market_reason(full_reason)}。",
                f"• {_LIVE_MARKET_SOURCE_LABELS['TENCENT_INDEX_FALLBACK']}：{_live_market_status(index_status)}；观察 {index_observed}/{index_expected}；{_live_market_reason(index_reason)}。",
                f"• 总尝试次数：{total_attempts} 次。",
                f"• 最终原因：{_live_market_reason(state.get('reason_code'))}。",
                "• 处理：保持失败关闭，行情恢复并重新核验后才继续盘中判断。",
            ]
            return [
                self._send(
                    delivery_key=delivery_key,
                    kind="A4_SYSTEM_HEALTH",
                    source_id=f"a4-system-health:{trade_date}:{bucket}",
                    title=f"A4系统告警｜行情数据阻断｜{trade_date} {bucket}",
                    lines=lines,
                    summary=summary,
                    now=now,
                )
            ]

        # The same call may recover after the bounded second attempt.  If it
        # did not, inspect the durable health ledger so a later first READY
        # tick can close the incident with one recovery card.
        prior_blocked = False
        if not recovered_after_retry:
            try:
                # The ledger is newest-first.  Only the latest health result
                # represents the current incident state: once a recovery (or
                # any later ready result) is recorded, later READY ticks must
                # not emit another recovery card for the old outage.
                latest = self.store.list_notification_deliveries(
                    kind="A4_SYSTEM_HEALTH",
                    limit=1,
                )
                if latest:
                    row_payload = _json_mapping(latest[0].get("payload_json"))
                    prior_blocked = (
                        str(row_payload.get("state") or "").upper() == "DATA_BLOCKED"
                        and str(row_payload.get("trade_date") or "") == trade_date
                    )
            except Exception:
                prior_blocked = False
        if not recovered_after_retry and not prior_blocked:
            return []
        delivery_key = f"a4-system-health:recovery:{trade_date}:{bucket}"
        recovery_summary = {**summary, "state": status, "recovery": True}
        lines = [
            "**A4盘中系统恢复**",
            f"• 恢复时间：{_time_label(state.get('as_of') or now.isoformat())}",
            f"• 受影响计划：{affected} 只；行情已重新核验。",
            f"• {_LIVE_MARKET_SOURCE_LABELS['HITHINK_FULL_MARKET']}：{_live_market_status(full_status)}；观察 {full_observed}/{full_expected}。",
            f"• {_LIVE_MARKET_SOURCE_LABELS['TENCENT_INDEX_FALLBACK']}：{_live_market_status(index_status)}；观察 {index_observed}/{index_expected}。",
            f"• 总尝试次数：{total_attempts} 次；{('本次重试后恢复。' if recovered_after_retry else '本次为阻断后的首次可用结果。')}",
            "• 处理：恢复盘中条件判断，但仍须逐只满足 A4 计划与风控边界。",
        ]
        return [
            self._send(
                delivery_key=delivery_key,
                kind="A4_SYSTEM_HEALTH",
                source_id=f"a4-system-health:{trade_date}:{bucket}",
                title=f"A4系统恢复｜行情数据可用｜{trade_date} {bucket}",
                lines=lines,
                summary=recovery_summary,
                now=now,
            )
        ]


def _is_system_data_block(event: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    if str(event.get("action") or "").strip().upper() != "DATA_BLOCK":
        return False
    reason = str(event.get("reason_code") or "").strip().upper()
    if reason == "LIVE_MARKET_STATE_NOT_READY" or reason.startswith("A4_LIVE_MARKET_"):
        return True
    strategy = payload.get("strategy") if isinstance(payload.get("strategy"), Mapping) else {}
    status = str(strategy.get("live_market_state_status") or "").strip().upper()
    return status in {"DATA_BLOCK", "DATA_BLOCKED"} and reason in {
        "LIVE_MARKET_STATE_NOT_READY",
        "A4_LIVE_MARKET_SOURCE_UNAVAILABLE",
    }


def _live_market_status(value: Any) -> str:
    status = str(value or "").strip().upper()
    return _LIVE_MARKET_STATUS_LABELS.get(status, "状态待确认")


def _live_market_reason(value: Any) -> str:
    reason = str(value or "").strip().upper()
    return _LIVE_MARKET_REASON_LABELS.get(reason, "行情原因待确认")


def _rotation_health_reason(value: Any) -> str:
    reason = str(value or "").strip().upper()
    labels = {
        "ROTATION_THEME_NOT_READY": "量见板块强度尚未生成",
        "ROTATION_THEME_REGISTRY_INVALID": "本地板块映射配置无效",
        "ROTATION_MEMBERSHIP_MISSING": "本地板块成分版本缺失",
        "ROTATION_MEMBERSHIP_INCOMPLETE": "东方财富板块成分页未完整采集",
        "ROTATION_MEMBERSHIP_STALE": "本地板块成分版本需要更新",
        "ROTATION_MEMBERSHIP_EXPIRED": "本地板块成分版本已超过允许使用期限",
        "MEMBERSHIP_SNAPSHOT_STALE_WARNING": "本地板块成分版本需要更新",
        "MEMBERSHIP_UPDATE_FAILED_FALLBACK": "成员更新失败，正在使用未超龄的最后成功版本",
        "MEMBERSHIP_SNAPSHOT_EXPIRED": "本地板块成分版本已超过允许使用期限",
        "MEMBERSHIP_UPDATE_FAILED_EXPIRED": "成员更新失败且最后成功版本已经过期",
        "EASTMONEY_BOARD_SOURCE_UNAVAILABLE": "东方财富板块目录或资金数据不可用",
        "TENCENT_FUND_COVERAGE_INSUFFICIENT": "腾讯逐股资金覆盖不足",
        "ROTATION_PRICE_COVERAGE_INSUFFICIENT": "板块行情覆盖不足",
        "ROTATION_FUND_DIRECTION_CONFLICT": "腾讯聚合资金与东方财富板块资金方向冲突",
        "ROTATION_PRICE_SOURCE_CONFLICT": "跨数据源收盘价不一致",
        "ROTATION_SNAPSHOT_DEADLINE_MISSED": "量见板块强度未在收盘时限内生成",
    }
    return labels.get(reason, "板块数据未满足生产要求")


def _bounded_count(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0, min(100_000, parsed))


def _trade_date_label(state: Mapping[str, Any], now: datetime) -> str:
    value = str(state.get("trade_date") or "").strip()
    if len(value) == 10 and value[4:5] == "-" and value[7:8] == "-":
        return value
    return now.date().isoformat()


def _health_bucket_label(state: Mapping[str, Any], now: datetime) -> str:
    raw = str(state.get("cache_bucket") or "").strip()
    try:
        parsed = datetime.fromisoformat(raw)
        return parsed.strftime("%H%M")
    except ValueError:
        return f"{now.hour:02d}{now.minute - now.minute % 5:02d}"


__all__ = ["WorkflowLarkPublisher"]
