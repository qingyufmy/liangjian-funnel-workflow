/** Display-only projections. Identity, raw prices and evidence stay unchanged. */
import {conditionLabel} from './labels.js';
import {reasonText} from './copy/reasons.js';
export function priceText(value){const number=typeof value==='number'?value:typeof value==='string'&&/^\d+(?:\.\d+)?$/.test(value)?Number(value):NaN;return Number.isFinite(number)?number.toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2}):'未提供';}
/** Ledger prices are originals, never a two-decimal rounded quote. */
export function ledgerPriceText(value){
  const raw=typeof value==='string'?value:typeof value==='number'&&Number.isFinite(value)?String(value):null;
  if(raw===null||!/^\d+(?:\.\d+)?$/.test(raw))return '未提供';
  const [integer,fraction='']=raw.split('.'),trimmed=fraction.replace(/0+$/,'');
  if(trimmed.length>3)return '原价精度超过3位（见来源）';
  return integer.replace(/^0+(?=\d)/,'')+(trimmed?`.${trimmed}`:'');
}
export function timeText(value){
  if(typeof value==='string'&&/^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/.test(value))return value;
  if(typeof value!=='string'||!/(?:Z|[+-]\d{2}:\d{2})$/.test(value)||!Number.isFinite(Date.parse(value)))return '时刻缺源';
  return new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).format(new Date(value));
}
export function dateText(value){
  if(typeof value!=='string'||!/(?:Z|[+-]\d{2}:\d{2})$/.test(value)||!Number.isFinite(Date.parse(value)))return '日期缺源';
  return new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
}
export function zoneText(plan){
  const values=Array.isArray(plan.zone_values)?plan.zone_values:Array.isArray(plan.zone)?plan.zone:null;
  if(values?.length===2)return values.map(priceText).join('–');
  if(typeof plan.zone==='string'&&/^\d+(?:\.\d+)?[–—~-]\d+(?:\.\d+)?$/.test(plan.zone))return plan.zone.split(/[–—~-]/).map(Number).map(priceText).join('–');
  return '区间缺源';
}
export const riskUnitLabel=value=>({PROBE:'试探仓',STANDARD:'标准仓',FULL:'足额仓',OBSERVE:'只观察',NO_TRADE:'不新开',NONE:'不适用'})[value]??'仓位上限缺源';
export function securityNames(state){
  const names=new Map(),conflicts=new Set();
  const rows=[...(state.plans??[]),...(state.positions??[]),...(state.orders??[]),...(state.fills??[]),...(state.admissions??[])];
  for(const [code,entry]of Object.entries(state.dashboard?.security_names??{}))if(entry?.source?.id&&entry?.source?.sha256&&typeof entry.name==='string')rows.push({code,name:entry.name});
  for(const row of rows){const code=row.code??row.symbol,name=row.name??row.company_name;if(typeof code!=='string'||typeof name!=='string'||!name.trim())continue;const key=code.toUpperCase();if(names.has(key)&&names.get(key)!==name.trim())conflicts.add(key);else names.set(key,name.trim());}
  for(const key of conflicts)names.delete(key);
  return names;
}
export function securityName(row,names=new Map()){
  const code=row.code??row.symbol;
  if(typeof row.name==='string'&&row.name.trim())return row.name.trim();
  if(typeof code!=='string')return null;
  return names.get(code.toUpperCase())??null;
}
export function securityText(row,names=new Map()){
  return `${securityName(row,names)??'名称缺源'} ${row.code??row.symbol??'代码缺源'}`;
}
export function annotateSecurities(value,names=new Map()){
  if(typeof value!=='string')return value;
  return value.replace(/(?<![A-Za-z0-9])(\d{6}(?:\.(?:SZ|SH|BJ))?)(?![A-Za-z0-9])/g,(code)=>{
    let name=names.get(code);
    if(!name&&!code.includes('.')){const matches=[...names].filter(([key])=>key.startsWith(`${code}.`));if(matches.length===1)name=matches[0][1];}
    if(name)return value.includes(name)?code:`${code}（${name}）`;
    return code.includes('.')?`${code}（名称缺源）`:code;
  });
}
export function dataStatusText(plan){
  const status=plan.data_status;
  if(status?.status==='AVAILABLE'&&status.source?.id&&status.source?.sha256)return status.label??'盘中观测已留存';
  if(['PARTIAL','DATA_BLOCKED','UNPROVEN'].includes(status?.status)&&status.source?.id&&status.source?.sha256)return status.label??'盘中观测不完整';
  if(plan.data_limits?.data_limited===true)return '研究数据受限';
  if(plan.data_limits?.data_limited===false)return '原计划未标数据受限';
  return '研究完整性标记缺源';
}
export function marketFacts(data){
  const market=data?.market??{},emotion=market.emotion;
  const known=emotion&&emotion.source?.id&&emotion.source?.sha256&&typeof emotion.stage_label==='string';
  return {stage:known?emotion.stage_label:'情绪数据缺源',note:known?`原观测 ${timeText(emotion.observed_at)}`:'尚未绑定当日情绪原件，不由账户或交易信号推断',date:known?dateText(emotion.observed_at):null,source:known?emotion.source:null,indices:Array.isArray(market.indices)?market.indices:[]};
}
export function confirmationText(plan){
  if(!plan.conditions?.length)return '待逐项原件';
  const met=plan.conditions.filter(c=>c.status==='MET'&&c.source_valid!==false).length;
  const raw=plan.confirmation_basis?.observed_at??plan.confirmation_basis?.closed_5m_end??plan.condition_as_of;
  const at=typeof raw==='string'&&/(?:Z|[+-]\d{2}:\d{2})$/.test(raw)&&Number.isFinite(Date.parse(raw))?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(raw)):'时刻缺源';
  return `${met}/${plan.conditions.length} · ${at}`;
}
export function rrDisplay(plan){
  const basis=plan.rr_basis??{},price=Number(basis.price),stop=Number(basis.stop);
  const validDistance=basis.price!=null&&basis.stop!=null&&Number.isFinite(price)&&Number.isFinite(stop)&&price>stop&&stop>0;
  const distancePct=validDistance?(price-stop)/price*100:null;
  const distorted=distancePct!==null&&distancePct<.5;
  const raw=typeof basis.observed_at==='string'?basis.observed_at:basis.closed_bar_end;
  const clock=typeof raw==='string'&&/(?:Z|[+-]\d{2}:\d{2})$/.test(raw)&&Number.isFinite(Date.parse(raw))?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(raw)):null;
  const value=plan.rr!=null?`${priceText(plan.rr)}${distorted?'（失真）':''}`:plan.planned_rr!=null?`${priceText(plan.planned_rr)}（计划）`:'未提供';
  return {value,distorted,distance_pct:distancePct,clock,explanation:`${clock?`按${clock}价格计算`:'价格观测时刻缺源'}${distorted?`；止损距离${priceText(distancePct)}%，小于0.5%，盈亏比被放大`:'；不是全天最高盈亏比'}`};
}
export function tradeRecordText(record){
  const security=record.symbol||record.code?`${securityText(record)}：`:'';
  if(record.kind==='PLAN_CONDITIONS'&&Array.isArray(record.unmet_conditions)){const missing=new Set(record.unproven_conditions??[]),unmet=record.unmet_conditions.filter(code=>!missing.has(code)),parts=[];if(unmet.length)parts.push(`未满足：${unmet.map(conditionLabel).join('、')}`);if(missing.size)parts.push(`缺证：${[...missing].map(conditionLabel).join('、')}`);return `${security}${parts.length?parts.join('；'):Array.isArray(record.unproven_conditions)?'条件记录无缺项（不代表已触发信号）':'缺项列表为空，缺证分类未绑定'}`;}
  if(record.kind==='EXECUTION_ADMISSION'&&typeof record.shares==='number'){const reasons=Array.isArray(record.reason_labels)&&record.reason_labels.length?record.reason_labels:(record.reason_codes??[]).map(reasonText);return `${security}执行准入${({APPROVED:'批准',REDUCED:'收紧',REJECTED:'拒绝'})[record.status]??'状态缺源'} ${record.shares.toLocaleString('zh-CN')} 股${reasons.length?` · ${reasons.slice(0,2).join('；')}`:''}`;}
  if(record.kind==='EXECUTION_FILL'&&typeof record.qty==='number')return `${security}${record.side==='SELL'?'卖出':record.side==='BUY'?'买入':'方向缺源'}成交 ${record.qty.toLocaleString('zh-CN')} 股，价格 ${ledgerPriceText(record.raw_price??record.price)}`;
  return `${security}${record.title?`${record.title}：`:''}${record.text??'内容缺源'}`;
}
