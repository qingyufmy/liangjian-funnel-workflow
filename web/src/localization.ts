const CODE_LABELS: Record<string, string> = {
  READY: "就绪",
  READY_DEGRADED: "可用但需留意",
  INSUFFICIENT: "不足",
  SUFFICIENT: "充足",
  PARTIAL: "部分可用",
  NOT_APPLICABLE: "不适用",
  TERMINAL: "已结束",
  MISSING: "缺失",
  VALIDATED: "已验证",
  VALIDATED_UNDERFILLED_MARKET: "已验证，市场机会较少",
  ACTIVE: "运行中",
  ACTIVE_TODAY: "今日监测中",
  PENDING_MORNING_REVIEW: "等待早盘复核",
  DRAFT_CLOSE: "收盘草案",
  INVALIDATED: "已失效",
  EXPIRED: "已过期",
  CANCELLED: "已取消",
  SIGNALLED: "已发出入场信号",
  OPEN: "持仓中",
  EXIT_PENDING: "等待离场成交",
  PARTIALLY_CLOSED: "部分离场",
  CLOSED: "已完成",
  UNFILLED: "未成交",
  DATA_ERROR: "数据异常",
  QUALIFIED: "符合计划条件",
  WATCH_ONLY: "继续观察",
  DATA_GAP: "数据不足",
  REJECTED: "不符合",
  TREND: "趋势票",
  EMOTION: "情绪票",
  TREND_CORE: "趋势核心",
  EMOTION_LEADER: "情绪龙头",
  LEADER: "龙头",
  FOLLOWER: "跟随",
  TREND_MA5: "趋势五日线",
  MA520_SWING: "五日与二十日均线波段",
  LEADER_INTRADAY: "龙头战法",
  BEAR_RISK: "偏弱防守",
  WEAK_ROTATION: "弱势轮动",
  BULL: "多头",
  BEAR: "空头",
  NEUTRAL: "中性",
  ICE_POINT: "情绪冰点",
  LIQUIDITY_CONTRACTION: "流动性收缩",
  PERSISTENT: "趋势延续",
  ACCELERATING: "加速增强",
  EARLY_REVERSAL: "初步转强",
  MIXED: "多空交织",
  COOLING: "热度降温",
  REPAIR: "修复阶段",
  CONFIRMATION: "确认阶段",
  NO_NEW_ENTRY: "暂不追高开仓",
  ALLOW: "允许关注",
  CAUTION: "谨慎参与",
  HIGH: "较高",
  MEDIUM: "中等",
  LOW: "较低",
  NONE: "暂无",
  TRUE: "是",
  FALSE: "否",
  PROBE: "试探仓位",
  FULL: "全量维护",
  INCREMENTAL: "增量维护",
  MONTH_CLOSED: "月线数据完整",
  WEEK_CLOSED: "周线数据完整",
  DAILY_CLOSED: "日线数据完整",
  TRADABLE: "当前可交易",
  DAILY_CLOSE_AVAILABLE: "参考收盘价可用",
  HIGHER_TIMEFRAME_RISK_CLASSIFIED: "大周期风险已分类",
  PRICE_GEOMETRY_VALID: "价格结构有效",
  TREND_DAILY_PATH_CONFIRMED: "日线趋势路径确认",
  DAILY_MA5_AVAILABLE_FOR_A4: "五日线可供盘中择时",
  NOT_OVEREXTENDED_OR_RETEST_CONFIRMED: "未明显超涨或回踩已确认",
  NOT_DISTRIBUTION: "未发现明显派发",
  A4_WILL_CONFIRM_DAILY_MA5_PULLBACK: "盘中继续确认五日线回踩",
  DAILY_NOT_BEARISH: "个股日线未转空",
  QUALIFIED_STANDARD: "常规计划条件合格",
  QUALIFIED_PROBE: "试探计划条件合格",
  HIGHER_TIMEFRAME_CONDITIONAL_PROBE: "大周期仍需盘中确认",
  A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE: "观察池中技术条件合格，可小仓试探",
  A3_STAGE_LINEAGE_MISSING: "上游阶段追溯信息不完整",
  A1_ACTIVE_REUSED: "沿用本月有效研究池",
  A2_FOCUS_POOL_UNDERFILLED_MARKET: "A2 有效候选池低于目标区间",
  A2_EFFECTIVE_POOL_UNDERFILLED_MARKET: "A2 有效候选池低于目标区间",
  POOL_UNDERFILLED_MARKET: "有效候选池低于目标区间",
  FIRST_RESISTANCE: "第一压力位",
  R2_OBSERVATION: "第二压力位观察",
  TREND_5M_REVERSAL_NOT_CONFIRMED: "五分钟转强尚未确认",
  TREND_15M_PRESSURE_NOT_EASING: "十五分钟压力尚未缓解",
  TREND_PULLBACK_ZONE_NOT_MET: "尚未进入趋势回踩区",
  PLAN_INVALIDATED_AT_OPEN: "开盘价格触发计划失效",
  DETERMINISTIC_TRIGGER_PASS: "确定性触发条件通过",
  DETERMINISTIC_EXIT_TRIGGER: "确定性离场条件触发",
  LLM_VETO: "盘中复核模型否决",
  HARD_STOP: "价格触及硬止损",
  HARD_STOP_BEFORE_ENTRY: "入场前触及保护位",
  CURRENT_1M_HARD_STOP: "当前一分钟触及保护位",
  PRE_ENTRY_RISK_LEVEL_TOUCHED: "入场前触及风险位",
  ENTRY_BLOCKED_CURRENT_MINUTE: "本分钟暂不入场",
  TREND_PRE_ENTRY_STRUCTURE_INVALIDATED: "趋势策略入场前结构失效",
  MA520_PRE_ENTRY_STRUCTURE_INVALIDATED: "五日与二十日均线入场前结构失效",
  A4_FORCED_EXIT_WITHOUT_POSITION: "策略产生无持仓离场指令，已阻断并记录",
  A4_BEHAVIOR_TYPE_MISSING: "股票类型尚未确定，不能选择盘中策略",
  BLOCKED_T1: "当日买入暂不可卖，等待下一交易日离场",
  ENTRY_NEXT_BAR_MISSED: "入场信号后的下一根完整分钟线未能成交",
  TREND_5M_FAILED_MA5_RECLAIM: "趋势股连续跌破五日线参考且回抽失败",
  TREND_HIGH_VOLUME_MA5_BREAK: "趋势股放量跌破五日线参考",
  TREND_HIGH_VOLUME_UPPER_SHADOW: "趋势股放量长上影，触发减仓",
  MA520_5M_FAILED_MA20_RECLAIM: "五二零策略跌破二十日线参考且回抽失败",
  MA520_HIGH_VOLUME_MA20_BREAK: "五二零策略放量跌破二十日线参考",
  EMPTY_SCOPE: "当前没有可执行计划",
  NO_ACTION: "继续观察",
  START_CONFIRMATION: "开始确认",
  BUY_SIGNAL: "模拟入场",
  ADD_SIGNAL: "模拟加仓",
  SELL_SIGNAL: "模拟离场",
  REDUCE_SIGNAL: "模拟减仓",
  FORCED_RISK_EXIT: "硬止损离场",
  PLAN_INVALIDATED: "计划失效",
  DATA_BLOCK: "数据条件未满足",
  FILLED: "已成交",
  BUY: "买入",
  SELL: "卖出",
  LONG: "多头",
  SHORT: "空头",
  MONTHLY: "月线",
  WEEKLY: "周线",
  DAILY: "日线",
  FIRST_RESISTANCE_PRICE: "第一压力位",
  PLATFORM_BREAKOUT: "平台突破",
  NEW_HIGH: "创新高",
  MAIN_RISE: "主升趋势",
  RECOVERY: "修复阶段",
  ROTATION: "轮动行情",
  LIVE_DEEPSEEK_FLASH_VETO_ONLY: "实时模型仅作否决复核",
  STRATEGY_WAITING: "策略条件尚未满足",
  SIGNAL_ALREADY_EMITTED: "本次信号已记录",
  LOCAL_FACT_CACHE_NOT_READY: "本地事实缓存尚未就绪",
  LARK_SENT: "飞书消息已送达",
  LARK_WEBHOOK_NOT_CONFIGURED: "尚未配置飞书机器人地址",
  LARK_WEBHOOK_CONFIGURATION_INVALID: "飞书机器人地址配置无效",
  LARK_CONFIGURATION_INVALID: "飞书配置无效",
  LARK_NOTIFICATION_FAILED: "飞书消息发送失败",
  LARK_HTTP_RETRYABLE: "飞书服务暂时繁忙，重试后仍未送达",
  LARK_HTTP_REJECTED: "飞书拒绝了本次消息",
  LARK_NETWORK_ERROR: "无法连接飞书服务",
  LARK_TIMEOUT: "连接飞书服务超时",
  ACTIVE_RUN_MISSING: "当前运行记录缺失",
  A1_ACTIVE_MISSING: "A1 有效研究池缺失",
  DEPLOYMENT_NOT_READY: "部署门禁未通过",
  CONFIGURATION_NOT_READY: "运行配置未就绪",
  STATE_DB_UNHEALTHY: "状态库异常",
  DATA_COVERAGE_INSUFFICIENT: "阶段事实覆盖不足",
  A2_DATA_GAP: "A2 阶段事实存在缺口",
  A2_CRITICAL_DATA_INSUFFICIENT: "A2 关键事实不足",
  A2_FACTOR_COVERAGE_BELOW_MINIMUM: "A2 因子覆盖不足",
  LATEST_WORKFLOW_NOT_READY: "最近工作流尚未就绪",
  ENGINEERING_FIX: "工程修复",
  DATA_FIX: "数据修复",
  SHADOW_TEST: "影子验证",
  NEEDS_ATTENTION: "需要关注",
  DATA_LIMITED: "数据受限",
  INCIDENT: "存在事故",
  ORCHESTRATOR: "任务编排",
  INSUFFICIENT_SAMPLE: "样本不足",
  MISSING_DATA: "数据缺失",
  CONFOUNDED: "影响因素纠缠",
  REGIME_NOT_OBSERVED: "尚未观察到对应市场环境",
};

const THEME_LABELS: Record<string, string> = {
  TH_ELEC_COMPONENTS: "电子元件",
  TH_NONMETAL_MATERIALS: "非金属材料与电子化学品",
  TH_AGRI_FOREST: "种植业与林业",
  TH_PRECIOUS_METAL: "贵金属",
  TH_COMM_EQUIP: "通信设备",
  TH_SEMI_LOCALIZATION: "半导体国产替代",
  TH_MEDICAL_SERVICE: "医疗服务",
  "INDUSTRY:881278.TI": "线缆部件与电网设备",
  "INDUSTRY:884073.TI": "制冷空调设备",
  "INDUSTRY:881118.TI": "专用设备",
  "INDUSTRY:881177.TI": "互联网服务",
  "INDUSTRY:884202.TI": "房地产服务",
  "CONCEPT:885999.TI": "智能座舱",
};

const FIELD_LABELS: Record<string, string> = {
  monthly: "月线",
  weekly: "周线",
  daily: "日线",
  state: "状态",
  closed: "数据完整",
  route: "入池路线",
  marketRole: "市场角色",
  market_role: "市场角色",
  themeId: "主题",
  theme_id: "主题",
  score: "评分",
  low: "最低价",
  high: "最高价",
  close: "收盘价",
  moving_averages: "移动均线",
  ma_alignment: "均线排列",
  ma_event: "均线事件",
  ma_bias: "均线偏离",
  source_ref: "事实来源",
  sourceRef: "事实来源",
  source: "数据来源",
  timestamp: "记录时间",
};

const TEXT_REPLACEMENTS: Record<string, string> = {
  SOCIAL_FINANCING: "社会融资",
  NEW_CREDIT: "新增信贷",
  M2_YOY: "广义货币同比",
  M1_YOY: "狭义货币同比",
  business_purity: "主营纯度",
  MARKET_CORE: "市场核心",
  "eligible=true": "符合入选条件",
  "eligible=false": "不符合入选条件",
  PMI: "制造业采购经理指数",
  PPI: "工业生产者出厂价格指数",
  tier: "梯队",
  Node: "控制台",
  "host=": "监听地址=",
  "port=": "端口=",
  LIANGJIAN_SCHEDULER_ENABLED: "调度开关",
};

export function codeLabel(value?: string | null): string {
  if (!value) return "—";
  const key = value.trim().toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  if (CODE_LABELS[key]) return CODE_LABELS[key];
  if (THEME_LABELS[key]) return THEME_LABELS[key];
  if (key.startsWith("INDUSTRY:")) return `行业方向（${key.split(":", 2)[1].split(".", 1)[0]}）`;
  if (key.startsWith("CONCEPT:")) return `题材方向（${key.split(":", 2)[1].split(".", 1)[0]}）`;
  if (key.startsWith("TH_")) return "主题方向";
  return humanizeText(value);
}

export function humanizeText(value?: string | null): string {
  if (!value) return "—";
  const direct = CODE_LABELS[value.trim().toUpperCase()] ?? THEME_LABELS[value.trim().toUpperCase()];
  if (direct) return direct;
  let rendered = value;
  for (const [source, target] of Object.entries(TEXT_REPLACEMENTS).filter(([source]) => !/^[A-Z][A-Z0-9_:-]*$/.test(source))) {
    rendered = rendered.replaceAll(source, target);
  }
  rendered = rendered.replaceAll("AI算力", "人工智能算力").replaceAll("AI应用", "人工智能应用");
  rendered = rendered.replace(/\b[A-Z][A-Z0-9_:-]{2,}(?:\.[A-Z]{2})?\b/g, (token) => {
    const normalized = token.replaceAll("-", "_");
    if (CODE_LABELS[normalized]) return CODE_LABELS[normalized];
    if (THEME_LABELS[normalized]) return THEME_LABELS[normalized];
    if (TEXT_REPLACEMENTS[normalized]) return TEXT_REPLACEMENTS[normalized];
    if (normalized.startsWith("INDUSTRY:")) return "行业方向";
    if (normalized.startsWith("CONCEPT:")) return "题材方向";
    if (normalized.startsWith("TH_")) return "主题方向";
    return "系统内部状态";
  });
  return rendered;
}

function structuredValue(value: unknown, depth: number): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 4 }).format(value);
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") return humanizeText(value);
  if (Array.isArray(value)) return value.length ? value.map((item) => structuredValue(item, depth + 1)).join("、") : "暂无";
  if (typeof value === "object" && depth < 3) {
    return Object.entries(value as Record<string, unknown>)
      .map(([key, item]) => `${FIELD_LABELS[key] ?? codeLabel(key)}：${structuredValue(item, depth + 1)}`)
      .join("；");
  }
  return "结构化资料";
}

export function displayValue(value: unknown): string {
  return structuredValue(value, 0);
}

export function stockSymbolLabel(value?: string | null): string {
  if (!value) return "代码未提供";
  const [code, suffix] = value.trim().toUpperCase().split(".", 2);
  const market = suffix === "SZ" ? "深市" : suffix === "SH" ? "沪市" : suffix === "BJ" ? "北交所" : "";
  return market ? `${code} · ${market}` : code;
}

export function modelNameLabel(value?: string | null): string {
  const raw = String(value ?? "").toLowerCase();
  if (raw.includes("deepseek")) return "深度求索";
  if (raw.includes("kimi") || raw.includes("moonshot")) return "月之暗面";
  if (raw.includes("glm") || raw.includes("z-ai")) return "智谱";
  if (/^lane[_-]?1$/i.test(raw)) return "深度求索";
  if (/^lane[_-]?2$/i.test(raw)) return "月之暗面";
  if (/^lane[_-]?3$/i.test(raw)) return "智谱";
  return value || "研究模型";
}

export function planPriorityText(value?: string | null): string {
  const key = value?.trim().toUpperCase();
  return key === "P1" ? "最高优先" : key === "P2" ? "常规优先" : key === "P3" ? "试探观察" : "优先级待确认";
}

export function jobLabel(value?: string | null): string {
  const labels: Record<string, string> = {
    premarket: "盘前分析",
    morning: "早盘复核",
    close: "收盘研究",
    a1: "A1 研究池维护",
    monitor: "盘中盯盘",
    features: "特征维护",
    comparison: "模型对比",
    "a5-midday": "A5 盘中复盘",
    "a5-close": "A5 盘后复盘",
    scheduler: "任务调度",
    service: "控制台服务",
  };
  return labels[String(value ?? "").toLowerCase()] ?? humanizeText(value || "研究任务");
}

export function slotLabel(value?: string | null): string {
  const labels: Record<string, string> = { MORNING: "早盘", CLOSE: "收盘", PREMARKET: "盘前", INTRADAY: "盘中" };
  return labels[String(value ?? "").toUpperCase()] ?? codeLabel(value);
}

export function logLevelLabel(value?: string | null): string {
  const labels: Record<string, string> = { INFO: "信息", WARN: "警告", WARNING: "警告", ERROR: "错误", DEBUG: "调试" };
  return labels[String(value ?? "").toUpperCase()] ?? "信息";
}

export function fieldLabel(value: string): string {
  return FIELD_LABELS[value] ?? codeLabel(value);
}
