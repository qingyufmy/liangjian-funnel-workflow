import {conditionLabel,planStatusLabel,strategyLabel} from './labels.js';
import {reasonText} from './copy/reasons.js';
import {priceText,zoneText,riskUnitLabel,securityText,annotateSecurities,rrDisplay,timeText,dateText} from './display.js';
const num=priceText;
/** Rounded display projection only; originals stay in the collapsed source. */
export function conditionDisplayValue(value){
  if(value==null)return '未提供';
  if(typeof value==='number')return Number.isFinite(value)?Number(value.toFixed(2)):'未提供';
  if(typeof value==='string'&&/^-?\d+\.\d+$/.test(value)){const number=Number(value);return Number.isFinite(number)?Number(number.toFixed(2)):'未提供';}
  if(Array.isArray(value))return value.map(conditionDisplayValue);
  if(typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,conditionDisplayValue(item)]));
  return value;
}
let detailNames=new Map();
const el=(tag,content,cls)=>{const n=document.createElement(tag);if(content!=null)n.textContent=tag==='pre'?content:annotateSecurities(String(content),detailNames);if(cls)n.className=cls;return n;};
export function chartModel(plan){
  const bars=(plan.bars??[]).filter(b=>typeof b.close==='number'&&Number.isFinite(b.close)&&b.close>0&&typeof b.at==='string'&&Number.isFinite(Date.parse(b.at))&&b.closed===true).slice().sort((a,b)=>a.at.localeCompare(b.at));
  const zone=Array.isArray(plan.zone_values)&&plan.zone_values.length===2&&plan.zone_values.every(v=>typeof v==='number'&&Number.isFinite(v)&&v>0)?plan.zone_values:[];
  const levels=[...bars.map(x=>x.close),...zone,plan.stop,plan.target].filter(v=>typeof v==='number'&&Number.isFinite(v));
  if(!bars.length||!levels.length)return null;
  const min=Math.min(...levels),max=Math.max(...levels),range=max-min||1;
  const first=Date.parse(bars[0].at),last=Date.parse(bars.at(-1).at),span=last-first||300000;
  const x=b=>8+(Date.parse(b.at)-first)/span*304,y=v=>8+(max-v)/range*110;
  const segments=[];let current=[];
  for(const b of bars){const prev=current.at(-1);if(prev&&Date.parse(b.at)-Date.parse(prev.at)!==300000){segments.push(current);current=[];}current.push(b);}if(current.length)segments.push(current);
  return {bars,min,max,segments:segments.map(s=>s.map(b=>`${x(b)},${y(b.close)}`).join(' ')),points:bars.map(b=>({x:x(b),y:y(b.close),bar:b})),zone:zone.length===2?{y:y(zone[1]),h:Math.max(.5,y(zone[0])-y(zone[1]))}:null,stopY:typeof plan.stop==='number'&&Number.isFinite(plan.stop)?y(plan.stop):null,targetY:typeof plan.target==='number'&&Number.isFinite(plan.target)?y(plan.target):null,maxVolume:Math.max(0,...bars.map(b=>typeof b.volume==='number'?b.volume:0)),coverage:{first:bars[0].at,last:bars.at(-1).at,count:bars.length,gapped:segments.length>1}};
}
function svgEl(tag,attrs){const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,String(v));return n;}
export function planChart(plan,compact=false){
  const model=chartModel(plan);if(!model)return el('span','5分钟原件待接入','chart-missing');
  const svg=svgEl('svg',{viewBox:'0 0 380 180',role:'img','aria-label':`${securityText(plan)}，${model.coverage.count}根已闭合5分钟价格观测；仅${timeText(model.coverage.first)}至${timeText(model.coverage.last)}局部，不补线`,class:compact?'plan-spark':'plan-chart'}),plot=svgEl('g',{transform:'translate(52 8)'});
  const label=(value,x,y,anchor='start')=>{const t=svgEl('text',{x,y,fill:'#d4d4d8','font-size':11,'text-anchor':anchor});t.textContent=value;svg.append(t);};
  for(const[f,y]of[[1,8],[.5,63],[0,118]]){plot.append(svgEl('line',{x1:0,x2:320,y1:y,y2:y,stroke:'#3f3f46','stroke-width':.5}));label(num(model.min+(model.max-model.min)*f),44,y+12,'end');}
  if(model.zone)plot.append(svgEl('rect',{x:0,y:model.zone.y,width:320,height:model.zone.h,fill:'#f59e0b','fill-opacity':.18}));
  if(model.stopY!==null)plot.append(svgEl('line',{x1:0,x2:320,y1:model.stopY,y2:model.stopY,stroke:'#f87171','stroke-width':1.2,'stroke-dasharray':'4 3'}));
  if(model.targetY!==null)plot.append(svgEl('line',{x1:0,x2:320,y1:model.targetY,y2:model.targetY,stroke:'#fcd34d','stroke-width':1,'stroke-dasharray':'2 4'}));
  for(const p of model.points){if(p.bar.volume!=null&&model.maxVolume>0)plot.append(svgEl('rect',{x:p.x-2,y:146-p.bar.volume/model.maxVolume*20,width:4,height:p.bar.volume/model.maxVolume*20,fill:'#a1a1aa','fill-opacity':.65}));const dot=svgEl('circle',{cx:p.x,cy:p.y,r:compact?2:4,fill:'#fafafa',tabindex:0});dot.append(svgEl('title',{}));dot.firstChild.textContent=`${securityText(plan)} · 日期 ${dateText(p.bar.at)}\n${timeText(p.bar.at)} 开${num(p.bar.open)} / 高${num(p.bar.high)} / 低${num(p.bar.low)} / 收${num(p.bar.close)} / 量${num(p.bar.volume)}；SHA ${p.bar.sha256}`;plot.append(dot);}
  for(const points of model.segments)plot.append(svgEl('polyline',{points,fill:'none',stroke:'#fafafa','stroke-width':1.8}));
  svg.append(plot);label(timeText(model.coverage.first),60,174);label(timeText(model.coverage.last),364,174,'end');label(model.maxVolume>0?'成交量':'量缺源',8,154);
  return svg;
}
export function openPlanDetail(plan){
  detailNames=new Map(plan.name&&plan.code?[[plan.code,plan.name]]:[]);
  let dialog=document.getElementById('plan-drawer');if(!dialog){dialog=el('dialog',null,'plan-drawer');dialog.id='plan-drawer';dialog.setAttribute('aria-labelledby','plan-drawer-title');document.body.append(dialog);}
  const title=el('h2',securityText(plan));title.id='plan-drawer-title';
  const close=el('button','关闭');close.type='button';close.onclick=()=>dialog.close();const head=el('div',null,'drawer-head');head.append(title,close);
  const stats=el('dl',null,'drawer-stats');for(const[label,value]of [['计划区间',zoneText(plan)],['止损',num(plan.stop)],['目标',num(plan.target)],['盈亏比',rrDisplay(plan).value]]){const group=el('div');group.append(el('dt',label),el('dd',value));stats.append(group);}
  const observed=plan.condition_as_of??plan.updated_at??plan.source_at;
  const content=el('div',null,'drawer-content');content.append(el('p',`${strategyLabel(plan.strat)} · ${planStatusLabel(plan.status)} · ${riskUnitLabel(plan.risk_unit)}`),el('p',`数据日期 ${dateText(observed)}`),el('p',`数据时间 ${timeText(observed)}；计划状态为当前持久状态，不是完整历史。`),stats,el('h3','闭合5分钟走势'),planChart(plan),el('p',plan.bar_source?.basis==='EXECUTION_AGGREGATED_5M_NOT_NATIVE'?`执行聚合5m / 非原生采集；冻结1m SHA ${plan.bar_source.snapshot_sha256}；截止 ${timeText(plan.bar_source.cutoff)}，日期 ${dateText(plan.bar_source.cutoff)}。不是全天或原生48根证明。`:'监控原件中的闭合5m价格观测；不是原生48根或全天覆盖。','chart-legend'),el('p','价格线：浅灰白 · 计划区间：浅金 · 止损：红色虚线 · 目标：浅金点线 · 成交量：灰柱。仅绘已采到的闭合原件；时间缺口不连线，不代表全天完整覆盖。','chart-legend'),el('h3',`形态条件（原件 ${plan.conditions?.length??0} 项，不补造为12项）`));
  if(plan.rr_basis){const basis=plan.rr_basis;content.insertBefore(el('p',`盈亏比口径：${rrDisplay(plan).explanation}。${basis.label??'按原末次闭合观测记录'}；${basis.formula??'计算公式缺源'}。观测价 ${num(basis.price)}，止损 ${num(basis.stop)}，目标 ${num(basis.target)}；闭合观测 ${timeText(basis.observed_at??basis.closed_bar_end)}，评审记录 ${timeText(basis.decision_at)}。日期 ${dateText(basis.decision_at)}。仅作显示标注，不改变交易阈值。`,'chart-legend'),stats);}
  if(plan.confirmation_basis){const b=plan.confirmation_basis;content.append(el('p',`确认进度取末次原评审，不取全天最高：闭合5分钟 ${timeText(b.closed_5m_end??b.observed_at)}；评审记录 ${timeText(b.decision_at)}；实际满足 ${b.met_count??'缺源'}/${b.required_count??'缺源'}。日期 ${dateText(b.decision_at)}。`,'chart-legend'));}
  const conditions=el('ul',null,'drawer-conditions');
  content.append(el('p','以下阈值与观测为最多两位小数的展示值；原始精度见“原始原因码与来源”，判断仍使用原值。','chart-legend'));
  for(const c of plan.conditions??[]){const item=el('li');item.append(el('strong',conditionLabel(c.code)),el('span',`${({MET:'满足',UNMET:'未满足',UNPROVEN:'证据不足'})[c.status]??'证据不足'}${c.source_valid===false?'（来源时间无效，不计满足）':''}`));const source=c.source_kind==='FROZEN_PLAN'?'计划冻结来源，非当前行情':c.source_times?.length?`原始来源时刻 ${c.source_times.map(timeText).join('、')}`:'原始来源时刻未记录，不补当前时间';const fact=el('p',`阈值 ${JSON.stringify(conditionDisplayValue(c.threshold))}；观测 ${JSON.stringify(conditionDisplayValue(c.value))}；${source}`);item.append(fact);if(c.source_times?.length)item.append(el('p',`来源日期 ${[...new Set(c.source_times.map(dateText))].join('、')}`));conditions.append(item);}
  if(!conditions.children.length)conditions.append(el('li','未接入该计划的逐项原件，不能由价格或状态推断形态已满足。'));content.append(conditions);
  if(plan.pm_coverage)content.append(el('p',`${plan.pm_coverage.label}；${plan.pm_coverage.status==='NOT_SENT_TO_PM'?(plan.pm_coverage.exclusion_basis==='SHORTLIST_CAP'?'服务端短名单容量截断，未送PM模型，不是模型拒绝':'服务端资格检查未纳入短名单，未送PM模型，不是模型拒绝'):'短名单仅证明送审范围，不代表获得分配'}。`));
  content.append(el('h3','激活与失效原件'));const activations=el('ol');for(const a of plan.activation_decisions??[]){const li=el('li',`${timeText(a.observed_at)} · ${a.action} · ${reasonText(a.reason_code)}；触发报价 ${num(a.price)}，原报价时刻 ${timeText(a.quote_as_of)}。`);const detail=el('details');detail.append(el('summary','查看当时依据'),el('pre',JSON.stringify(a,null,2)));li.append(detail);activations.append(li);}if(!activations.children.length)activations.append(el('li','缺少当时决策原件，不用后来的分钟线补造触发报价。'));content.append(activations);
  content.append(el('h3','事件时间线'));const timeline=el('ol');for(const item of plan.timeline??[]){const li=el('li',`${item.at?.slice(11,19)??'时间未知'} · ${(item.reason_codes??[]).map(reasonText).join('；')||'已有观测记录'}`);const proof=el('details');proof.append(el('summary','查看来源'),el('p',`记录 ${item.source_id??'未知'}；SHA ${item.sha256??'未知'}`));li.append(proof);timeline.append(li);}if(!timeline.children.length)timeline.append(el('li','缺少事件变迁原件，不由当前状态补历史。'));content.append(timeline,el('h3','仓位计算'));
  if(plan.sizing){const sizing=el('dl',null,'drawer-stats');for(const[label,key]of[['风险预算原股数','raw_risk_qty'],['风险预算整手股数','risk_round_lot_qty'],['单票20%上限股数','cap20_qty'],['试探仓10%上限股数','probe10_qty'],['最终股数','final_qty']]){const group=el('div');group.append(el('dt',label),el('dd',plan.sizing.raw_decimal_strings?.[key]??num(plan.sizing[key])));sizing.append(group);}content.append(sizing,el('p',`实际绑定项 ${(plan.sizing.binding_limits??[]).map(reasonText).join('；')||'原件未记录'}；原准入 ${plan.sizing.at??'时刻未知'}。不是从当前配置反推。`));const proof=el('details');proof.append(el('summary','仓位计算来源'),el('p',`记录 ${plan.sizing.source?.id??'未知'}；SHA ${plan.sizing.source?.sha256??'未知'}`));content.append(proof);}else content.append(el('p','尚未绑定该计划的原始风险预算、20%/10%上限与最终股数；不由标签倒推。'));
  if(plan.data_status){content.append(el('h3','盘中数据与条件'),el('p',`${plan.data_status.label??'状态说明缺源'}；原观测 ${timeText(plan.data_status.observed_at)}。日期 ${dateText(plan.data_status.observed_at)}。`));}
  content.append(el('h3','研究数据完整性（独立于盘中行情）'),el('p',plan.data_limits?.data_limited===true?`研究数据受限；${(plan.data_limits.reason_codes??[]).map(reasonText).join('；')||'缺源明细未提供'}。A6显审：${plan.data_limits.a6_explicit_review_required===true?'要求逐项审查':'未记录'}`:plan.data_limits?.data_limited===false?'原计划记录未标研究数据受限。':'原计划未记录研究完整性标记，不代表盘中行情缺失，也不证明研究数据齐全。'));
  const raw=el('details');raw.append(el('summary','原始原因码与来源'),el('pre',JSON.stringify({plan_id:plan.source_id,sha256:plan.source_sha256,updated_at:plan.source_at,condition_as_of:plan.condition_as_of,rr_basis:plan.rr_basis,confirmation_basis:plan.confirmation_basis,reason_codes:plan.reason_codes??[],conditions:(plan.conditions??[]).map(c=>({code:c.code,explanation:reasonText(c.code),threshold:c.threshold,value:c.value,source_times:c.source_times,source_id:c.source_id,sha256:c.sha256}))},null,2)));content.append(raw);dialog.dataset.planId=plan.id??plan.source_id??'';dialog.replaceChildren(head,content);if(!dialog.open)dialog.showModal();
}
