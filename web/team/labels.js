/** Display-only labels. Raw contracts stay in evidence details, never rewritten. */
const conditions={
  TREND_PULLBACK_TO_SUPPORT:'回踩支撑',
  A3_RIGHT_SIDE_CONFIRMATION:'计划要求的右侧确认',A3_TREND_ROUTE_APPROVED:'计划已准入趋势通道',
  DAILY_CLOSE_ABOVE_MA20:'日线收盘位于20日均线上方',DAILY_MA5_ABOVE_MA20:'5日均线位于20日均线上方',DAILY_MA5_MA20_ONLY:'日线双均线依据',
  MA520_15M_STABLE:'闭合15分钟结构企稳',MA520_5M_HIGHER_LOW:'闭合5分钟低点抬高',MA520_5M_VWAP_RECLAIM:'闭合5分钟收回均价线',
  MA520_TWO_CLOSED_5M_CONFIRMATIONS:'两根闭合5分钟确认',MA520_VOLUME_NOT_OVERHEATED:'量能没有过热',STANDARD_TREND_ROUTE:'趋势通道资格',
  TREND_15M_PRESSURE_EASING:'闭合15分钟卖压减弱',TREND_5M_REVERSAL_CONFIRMATION:'闭合5分钟止跌确认',TREND_ENTRY_RULE_V2:'趋势入场规则组合',
  TREND_PRIOR_5M_REVERSAL:'前一闭合5分钟止跌',TREND_PULLBACK_VOLUME_CONTRACTION:'回调缩量',TREND_SUBSEQUENT_5M_CONFIRMATION:'后续闭合5分钟确认',
  TREND_VOLUME_NOT_OVERHEATED:'量能没有过热',TREND_VWAP_RECLAIMED:'价格收回均价线',VOLUME_OVERHEATED:'量能过热检查'
};
export const conditionLabel=key=>conditions[key]??'条件说明待补充（原条件在详情）';
export const strategyLabel=key=>({TREND_MA5:'趋势回调',MA520:'均线趋势',MA520_SWING:'均线波段',MA5_MA20:'均线趋势'})[key]??'策略说明待补充';
export const planStatusLabel=key=>({ACTIVE_TODAY:'已激活',PENDING_MORNING_REVIEW:'待晨审',INVALIDATED:'已失效',EXPIRED:'已到期'})[key]??'状态说明待补充';
export const clusterLabel=key=>({AI_APPLICATIONS_DIGITAL_ECONOMY:'人工智能应用与数字经济',AI_COMPUTE_INFRASTRUCTURE:'算力基础设施',CHEMICAL_NEW_MATERIALS:'化工新材料',FINANCIAL_HIGH_DIVIDEND:'金融与高股息',INFRASTRUCTURE_DOMESTIC_DEMAND:'基建与内需',INNOVATIVE_MEDICINE_HEALTHCARE:'创新药与医疗',NATIONAL_DEFENSE:'国防军工',POWER_EQUIPMENT_NEW_ENERGY:'电力设备与新能源',RESOURCES_ENERGY:'资源与能源',ROBOTICS_ADVANCED_MANUFACTURING:'机器人与先进制造',SEMICONDUCTOR_LOCALIZATION:'半导体国产化'})[key]??'分组说明待补充';
