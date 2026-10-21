/** Display-only explanations. Never changes the original event or trading rule. */
export const REASONS=Object.freeze({
  PRICE_OUTSIDE_PLAN_GEOMETRY:'现价不在计划允许的入场区间，继续等待。',
  OPEN_PRICE_CHASE_BLOCK:'开盘价超过不追价线，晨审未激活这条计划。',
  START_CONFIRMATION:'暂缓入场，等待闭合K线确认。',
  ROUND_LOT_UNAVAILABLE:'当前可执行股数不足一手，需核对价位和限额。',
  A2_UNAVAILABLE:'题材评审不可用，本计划走降级路径。',
  A2_REVIEW_DEGRADED:'题材评审已降级，执行仍受降级风险上限约束。',
  CLAMPED_TO_PROBE:'已按试探仓的风险上限执行。',
  ENVELOPE_INELIGIBLE:'本轮风险信封不允许执行这条计划。',
  POSITION_CAP:'单只股票的资金占用上限限制了可买数量。',
  POSITION_ALREADY_OPEN:'这条计划已有持仓，不重复新开。',
  POSITION_RISK_CLEAR:'本轮持仓风险检查未触发退出条件。',
  PRE_ENTRY_RISK_LEVEL_TOUCHED:'价格触及入场前风险线，暂停买入并核验结构。',
  TREND_PRE_ENTRY_STRUCTURE_INVALIDATED:'入场前结构已按规则破坏，这条计划作废。',
  TREND_15M_PRESSURE_NOT_EASING:'闭合15分钟的卖压尚未减弱，继续等待。',
  TREND_PULLBACK_VOLUME_CONTRACTION_NOT_MET:'回调缩量尚未达到规则要求，继续等待形态确认。',
  ROTATION_NO_MAINLINE:'题材轮动，暂未确认市场主线。',
  DETERMINISTIC_TRIGGER_PASS:'确定性入场条件已通过，继续核对风控和执行。',
  SIGNAL_ALREADY_EMITTED:'这一轮入场信号已发出，不重复发送。',
  ENTRY_CONFIRMED_15M_STABILIZED_5M_VWAP_RECLAIM:'模型确认入场，具体数值仍以原始条件和风控核验为准。',
  A4_SESSION_WARMUP:'开盘后仍在积累已闭合K线，暂不判断入场。',
  ENGINE_TRACKING_NO_REVIEW_CANDIDATE:'引擎继续跟踪，本轮没有需要模型评审的候选。',
  INTERNAL_DELIVERY_UNCONFIRMED:'内部通知未确认送达，需检查通知回执。',
  INTERNAL_REJECTED:'内部通知被接收端拒绝，尚未送达。',
  A5_SCORECARD_NUMBER_NOT_IN_METRICS:'复盘回答使用了事实清单之外的数值，报告未获批准。',
  A5_DEGRADED_NO_APPROVED_REPORT:'复盘已降级，本轮没有获批的团队报告。',
  A5_INDEPENDENT_VERIFICATION_DEGRADED:'复盘的独立核验资料不完整，结论需保留这一限制。',
  A2_AUDIT_UNREADABLE:'复盘未能读取题材研究审计，相关结论暂缺。',
  A2_STAGE_NOT_FOUND:'复盘没有读到题材研究阶段的结果。',
  LISTING_PERIOD_UNPROVEN_OR_LT5:'无法证明已上市满五个交易日，或确实不足五日；不能直接当作新股。',
  WORKFLOW_MONITOR_BLOCKED:'本轮监控汇总为阻断状态，需区分策略等待与资料问题。',
  DATA_BLOCK:'本轮资料或条件未具备，不能只凭这个汇总代码判断系统停机。',
  NO_TRADES:'该股票这一分钟没有成交，不代表全账户故障。',
  TEAM_MONITOR_DEGRADED:'团队监控已降级，是否进入只记录保护以账户锁原件为准。',
  VALIDATED:'本次回答已通过校验。',VALIDATION_FAILED:'本次回答未通过校验，不能当作有效交易判断。',
  NO_ACTION:'本轮没有采取动作，具体原因以对应记录为准。',
  BUY_SIGNAL:'本轮产生买入信号，仍需通过风险准入才能下单。',
  PLAN_INVALIDATED:'这条计划已失效，不再新开仓。',
  FILLED:'订单已完成撮合；模拟撮合不代表真实券商成交。',
  REJECTED:'本条记录被拒绝，原因需结合它所属的业务环节查看。',
  SENT:'这条通知已有送达回执。',OK:'本项检查通过。',
  DATA_LIMITED:'现有资料不足，不能据此认定条件已满足。',
  NOT_CONFIRMED:'形态还没有确认，继续等待。',CONFIRMED:'本组条件已确认，仍需通过后续风控。',
  A_SUPPORT:'尚需确认计划支撑位有效。',A_PULLBACK:'回调结构尚未确认。',
  A_CONTRACTION:'回调缩量条件尚未满足。',A_STABILIZED:'止跌形态尚未确认。',
  A_15M_SUPPORT:'闭合15分钟的支撑尚未确认。',
  B_KEY_LEVEL:'突破参考位的依据尚待确认。',B_KEY_BELOW_NO_CHASE:'突破参考位不能高于不追价线。',
  B_ATTEMPT_PREFIX:'开盘起的完整闭合K线前缀尚待核验。',
  B_EFFECTIVE_BREAKOUT:'突破尚未达到价格和量能确认要求。',
  B_RETEST:'突破后的回踩尚未确认。',B_RETEST_VOLUME:'回踩量能尚未得到确认。',
  B_ACCEPTED:'回踩后的承接尚未确认。',B_STOP_ONLY_RAISED:'调整止损时必须只上移、不下移。',
  B_NO_CHASE:'现价需符合不追价限制。',
  C_DECLINE:'此前回落幅度尚未满足反弹形态要求。',
  C_VOLUME_PRICE_SYNCHRONIZED:'放量与价格修复尚未在规定的K线上同时成立。',
  ENTRY_RR:'盈亏比需达到计划规定的门槛。',ENTRY_ZONE:'价格需落在计划允许的入场区间。',
  A3_TREND_ROUTE_APPROVED:'需有研究计划批准的趋势交易资格。',
  A4_LIVE_ENTRY_GEOMETRY_ACCEPTED:'需按实际入场价再次核对止损、目标与盈亏比。',
  VOLUME_NOT_OVERHEATED:'需确认量能没有过热。',STANDARD_TREND_ROUTE:'按既有趋势通道核对条件。',
  TREND_ENTRY_RULE_V2:'按趋势入场规则逐项核对，不由标签本身放行。',
  TREND_PRE_ENTRY_CLOSED_BREAKDOWN:'闭合K线已跌破入场前风险位。',
  TREND_5M_FAILED_RECLAIM:'闭合5分钟结构未能收回风险位。',
  TREND_15M_BREAKDOWN_CONFIRMED:'闭合15分钟结构已确认破位。',
  TREND_15M_PRESSURE_EASING:'需确认闭合15分钟卖压减弱。',
  TREND_5M_REVERSAL_CONFIRMATION:'需确认闭合5分钟止跌结构。',
  TREND_PRIOR_5M_REVERSAL:'需有前一根闭合5分钟的止跌依据。',
  TREND_SUBSEQUENT_5M_CONFIRMATION:'需有后续闭合5分钟的确认。',
  TREND_PULLBACK_VOLUME_CONTRACTION:'需确认回调期间成交量收缩。',
  TREND_VOLUME_NOT_OVERHEATED:'需确认趋势入场时量能没有过热。',
  TREND_VWAP_RECLAIMED:'需确认价格收回成交均价线。'
});
export const CONDITION_NAMES=Object.freeze({
  A_SUPPORT:'支撑位',A_PULLBACK:'回调结构',A_CONTRACTION:'回调缩量',A_STABILIZED:'止跌形态',A_HIGHER_LOWS:'低点抬高',A_LOWER_WICK:'下影线止跌',A_15M_SUPPORT:'15分钟支撑',
  B_KEY_LEVEL:'突破参考位',B_KEY_BELOW_NO_CHASE:'参考位不高于不追线',B_ATTEMPT_PREFIX:'开盘起的完整K线前缀',B_EFFECTIVE_BREAKOUT:'有效突破',B_RETEST:'突破后回踩',B_RETEST_VOLUME:'回踩量能',B_ACCEPTED:'回踩承接',B_STOP_ONLY_RAISED:'止损只上移',B_NO_CHASE:'不追价限制',B_CANDLE_ACCEPTANCE:'K线承接',B_VWAP_RECLAIM:'收回成交均价线',
  C_DECLINE:'此前回落',C_VOLUME_PRICE_SYNCHRONIZED:'放量与价格修复同步',ENTRY_RR:'盈亏比',ENTRY_ZONE:'计划区间',
  C_VOLUME:'反弹放量',C_PRICE_REPAIR:'价格修复',C_NO_LONG_UPPER:'没有过长上影线'
});
function normalized(key){return typeof key==='string'?key.replace(/_(?:\d+)$/,''):'';}
export function reasonText(key){
  if(typeof key!=='string'||!key)return '原因尚未提供。';
  const base=normalized(key);
  return REASONS[key]??REASONS[base]??(CONDITION_NAMES[base]?`仍需核验${CONDITION_NAMES[base]}。`:'该状态尚无核实过的中文说明，请查看详情。');
}
export const conditionName=key=>CONDITION_NAMES[key]??CONDITION_NAMES[normalized(key)]??null;
export function translateReasonMessage(value){
  if(value==null)return '';
  return String(value).replace(/\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b/g,code=>reasonText(code)).replace(/\bPROBE\b/g,'试探仓').replace(/\bRECORD\b/g,'只记录').replace(/\bCONTROL\b/g,'受控执行');
}
