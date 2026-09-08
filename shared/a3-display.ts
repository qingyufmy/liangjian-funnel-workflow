/** Read-only research presentation. Never use these fields to authorize a plan. */
export const A3_REASON_LABELS: Record<string, string> = {
  HIGHER_TIMEFRAME_CONDITIONAL_PROBE: "月／周线背景偏弱（非淘汰条件）",
  TREND_DAILY_PATH_MISSING: "日线主升、突破或强势回踩条件尚未确认",
  MA520_DEAD_CROSS: "五日与二十日均线死叉",
  MA520_RIGHT_SIDE_NOT_CONFIRMED: "日线520右侧重启条件尚未确认",
  TREND_TOP_RISK_CONFIRMED: "量化形态提示趋势顶部风险",
  THEME_STAGE_NOT_EARLY: "题材阶段不符合龙头策略的早期条件",
  THEME_STAGE_MISSING: "缺少题材阶段证据，暂不能完成判断",
  FIRST_BOARD_OBSERVE_ONLY: "首板暂列观察，未满足当前龙头接力条件",
  HIGHER_TIMEFRAME_BEARISH: "已闭合周线与日线同时偏空",
  DAILY_TREND_WEAK: "日线趋势偏弱",
  HIGH_VOLUME_DISTRIBUTION: "量化形态提示放量派发风险",
  LEADER_DAILY_TREND_WEAK: "龙头策略所需日线趋势偏弱",
  A4_RETEST_CONFIRMATION_REQUIRED: "由A4等待盘中回踩确认（非A3淘汰条件）",
  TREND_DAILY_PATH_CONFIRMED: "日线主升、突破或强势回踩条件",
  MA520_RIGHT_SIDE_CONFIRMED: "日线520右侧重启条件",
  THEME_IN_EARLY_CYCLE: "题材处于龙头策略适用阶段",
  BOARD_NOT_FIRST_OBSERVATION_ONLY: "满足龙头接力而非仅首板观察",
  HIGHER_TIMEFRAME_NOT_BEARISH: "周线与日线未同时偏空",
  DAILY_NOT_BEARISH: "日线趋势未偏空",
  LEADER_DAILY_NOT_BEARISH: "龙头日线趋势未偏空",
  NOT_DISTRIBUTION: "未出现量化派发风险形态",
  MARKET_RISK_OFF: "市场处于防守背景",
  SECTOR_NO_NEW_ENTRY_PRIOR: "板块背景提示谨慎新开仓",
  A4_WILL_CONFIRM_DAILY_MA5_PULLBACK: "由A4确认盘中五日线回踩条件",
  A3_REWARD_RISK_BELOW_MINIMUM: "研究盈亏比低于参考下限",
  A3_STOP_DISTANCE_OUTSIDE_LIMIT: "研究止损距离超出参考区间",
  A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE: "来自A2观察池，日线技术研究合格；试探执行仍需正式计划和A4确认",
};

export const A3_GROUP_LABELS: Record<string, string> = {
  QUALIFIED: "研究合格", WATCH: "技术待观察", REJECTED: "技术否决",
  DATA_GAP: "证据待核", UNKNOWN: "分类待核",
};
const BACKGROUND = new Set(["HIGHER_TIMEFRAME_CONDITIONAL_PROBE", "MARKET_RISK_OFF", "SECTOR_NO_NEW_ENTRY_PRIOR"]);
const DEFERRED = new Set(["A4_RETEST_CONFIRMATION_REQUIRED", "A4_WILL_CONFIRM_DAILY_MA5_PULLBACK"]);
const HARD = new Set(["MA520_DEAD_CROSS", "TREND_TOP_RISK_CONFIRMED", "HIGHER_TIMEFRAME_BEARISH", "DAILY_TREND_WEAK", "HIGH_VOLUME_DISTRIBUTION", "LEADER_DAILY_TREND_WEAK"]);
const obj = (value: unknown): Record<string, unknown> => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const strings = (value: unknown): string[] => Array.isArray(value) ? value.filter((x): x is string => typeof x === "string" && !!x) : [];
const unique = (values: string[]): string[] => [...new Set(values)];
const label = (code: string): string => A3_REASON_LABELS[code] ?? "存在未翻译的条件，请查看诊断标识";

export interface A3Display {
  disposition: string;
  label: string;
  eligibility: string | null;
  strategy: string | null;
  primaryReason: string;
  blockers: string[];
  background: string[];
  deferred: string[];
  researchNotes: string[];
  diagnosticCodes: string[];
  untranslatedCodes: string[];
  localDecision: boolean;
}

export function a3Display(value: Record<string, unknown>, pool: string): A3Display {
  const plan = obj(value.plan);
  const eligibility = String(value.deterministic_eligibility ?? plan.eligibility ?? value.eligibility ?? "").toUpperCase();
  const local = String(value.local_partition ?? "");
  let disposition = Object.hasOwn(A3_GROUP_LABELS, eligibility) ? eligibility
    : ({ LOCAL_MONITOR: "WATCH", HARD_REJECT: "REJECTED", DATA_GAP: "DATA_GAP" } as Record<string, string>)[local] ?? "UNKNOWN";
  if (eligibility === "QUALIFIED" && pool === "rejected") disposition = "UNKNOWN";
  const codes = unique([...strings(value.reason_codes), ...strings(value.deterministic_reason_codes)]);
  const veto = unique([...strings(value.deterministic_veto_conditions), ...strings(plan.veto_conditions ?? value.veto_conditions), ...codes.filter(x => HARD.has(x))]);
  const unmet = unique([...strings(value.deterministic_unmet_conditions), ...strings(plan.unmet_conditions ?? value.unmet_conditions)]);
  const background = codes.filter(x => BACKGROUND.has(x));
  const deferred = codes.filter(x => DEFERRED.has(x));
  const failures = codes.filter(x => !BACKGROUND.has(x) && !DEFERRED.has(x));
  // A qualified row can contain positive/context reasons. Do not relabel them as failures.
  const aliases: Record<string, string> = {
    TREND_DAILY_PATH_CONFIRMED: "TREND_DAILY_PATH_MISSING",
    MA520_RIGHT_SIDE_CONFIRMED: "MA520_RIGHT_SIDE_NOT_CONFIRMED",
    THEME_IN_EARLY_CYCLE: "THEME_STAGE_NOT_EARLY",
    BOARD_NOT_FIRST_OBSERVATION_ONLY: "FIRST_BOARD_OBSERVE_ONLY",
    HIGHER_TIMEFRAME_NOT_BEARISH: "HIGHER_TIMEFRAME_BEARISH",
    DAILY_NOT_BEARISH: "DAILY_TREND_WEAK",
    LEADER_DAILY_NOT_BEARISH: "LEADER_DAILY_TREND_WEAK",
    NOT_DISTRIBUTION: "HIGH_VOLUME_DISTRIBUTION",
  };
  const blocking = unique([...veto, ...(disposition === "QUALIFIED" ? [] : failures), ...unmet.filter(x => !codes.includes(aliases[x] ?? ""))]);
  const researchNotes = disposition === "QUALIFIED" ? failures : [];
  let primary = blocking.length ? label(blocking[0]!)
    : disposition === "QUALIFIED" && pool !== "rejected" ? "日线研究条件合格；正式发布和盘中执行需另行核验"
    : "当前记录缺少可核验的阻断说明";
  const ma = obj(value.deterministic_daily_ma ?? value.daily_ma);
  const close = value.reference_price ?? plan.reference_price;
  if (!veto.length && codes.includes("TREND_DAILY_PATH_MISSING") && typeof close === "number" && typeof ma.ma5 === "number" && close < ma.ma5) {
    primary = `参考收盘 ${close} 低于五日均线 ${ma.ma5}；其他日线路径尚未确认`;
  }
  if (disposition === "DATA_GAP" && codes.includes("THEME_STAGE_NOT_EARLY") && !veto.length) {
    primary = "题材阶段判断存在数据缺口；仍需核对首板等其他条件";
  }
  const untranslatedCodes = unique([...blocking, ...background, ...deferred, ...researchNotes]).filter(x => !Object.hasOwn(A3_REASON_LABELS, x));
  return {
    disposition, label: A3_GROUP_LABELS[disposition] ?? "分类待核",
    eligibility: eligibility || null,
    strategy: String(value.deterministic_strategy_profile ?? plan.strategy_profile ?? value.strategy_profile ?? "") || null,
    primaryReason: primary, blockers: blocking.map(code => disposition === "DATA_GAP" && code === "THEME_STAGE_NOT_EARLY" ? "题材阶段证据待核，暂不能确认是否满足早期条件" : label(code)), background: background.map(label), deferred: deferred.map(label),
    researchNotes: researchNotes.map(label),
    diagnosticCodes: codes, untranslatedCodes, localDecision: value.sent_to_llm === false,
  };
}
