import {conditionLabel,planStatusLabel,strategyLabel} from './labels.js';
import {reasonText} from './copy/reasons.js';
const num=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('zh-CN',{maximumFractionDigits:4}):'未提供';
const el=(tag,content,cls)=>{const n=document.createElement(tag);if(content!=null)n.textContent=content;if(cls)n.className=cls;return n;};
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
  return {bars,segments:segments.map(s=>s.map(b=>`${x(b)},${y(b.close)}`).join(' ')),points:bars.map(b=>({x:x(b),y:y(b.close),bar:b})),zone:zone.length===2?{y:y(zone[1]),h:Math.max(.5,y(zone[0])-y(zone[1]))}:null,stopY:typeof plan.stop==='number'&&Number.isFinite(plan.stop)?y(plan.stop):null,targetY:typeof plan.target==='number'&&Number.isFinite(plan.target)?y(plan.target):null,maxVolume:Math.max(0,...bars.map(b=>typeof b.volume==='number'?b.volume:0)),coverage:{first:bars[0].at,last:bars.at(-1).at,count:bars.length,gapped:segments.length>1}};
}
function svgEl(tag,attrs){const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,String(v));return n;}
export function planChart(plan,compact=false){
  const model=chartModel(plan);if(!model)return el('span','5分钟原件待接入','chart-missing');
  const svg=svgEl('svg',{viewBox:'0 0 320 150',role:'img','aria-label':`${model.coverage.count}根已闭合5分钟价格观测；仅${model.coverage.first.slice(11,16)}至${model.coverage.last.slice(11,16)}局部，不补线`,class:compact?'plan-spark':'plan-chart'});
  if(model.zone)svg.append(svgEl('rect',{x:0,y:model.zone.y,width:320,height:model.zone.h,fill:'#f59e0b','fill-opacity':.14}));
  if(model.stopY!==null)svg.append(svgEl('line',{x1:0,x2:320,y1:model.stopY,y2:model.stopY,stroke:'#f87171','stroke-width':1,'stroke-dasharray':'4 3'}));
  if(model.targetY!==null)svg.append(svgEl('line',{x1:0,x2:320,y1:model.targetY,y2:model.targetY,stroke:'#fcd34d','stroke-width':1,'stroke-dasharray':'2 4'}));
  for(const p of model.points){if(p.bar.volume!=null&&model.maxVolume>0)svg.append(svgEl('rect',{x:p.x-2,y:146-p.bar.volume/model.maxVolume*20,width:4,height:p.bar.volume/model.maxVolume*20,fill:'#71717a','fill-opacity':.4}));const dot=svgEl('circle',{cx:p.x,cy:p.y,r:compact?1.7:5,fill:'#e4e4e7','fill-opacity':compact?1:.5,tabindex:compact?-1:0});dot.append(svgEl('title',{}));dot.firstChild.textContent=`${p.bar.at} 开${num(p.bar.open)} / 高${num(p.bar.high)} / 低${num(p.bar.low)} / 收${num(p.bar.close)} / 量${num(p.bar.volume)}；SHA ${p.bar.sha256}`;svg.append(dot);}
  for(const points of model.segments)svg.append(svgEl('polyline',{points,fill:'none',stroke:'#e4e4e7','stroke-width':1.8}));
  return svg;
}
export function openPlanDetail(plan){
  let dialog=document.getElementById('plan-drawer');if(!dialog){dialog=el('dialog',null,'plan-drawer');dialog.id='plan-drawer';dialog.setAttribute('aria-labelledby','plan-drawer-title');document.body.append(dialog);}
  const title=el('h2',`${plan.name??plan.code} · ${plan.code}`);title.id='plan-drawer-title';
  const close=el('button','关闭');close.type='button';close.onclick=()=>dialog.close();const head=el('div',null,'drawer-head');head.append(title,close);
  const stats=el('dl',null,'drawer-stats');for(const[label,value]of [['计划区间',(plan.zone_values??[]).map(num).join('–')||plan.zone||'未提供'],['止损',num(plan.stop)],['目标',num(plan.target)],['盈亏比',num(plan.rr)]]){const group=el('div');group.append(el('dt',label),el('dd',value));stats.append(group);}
  const content=el('div',null,'drawer-content');content.append(el('p',`${strategyLabel(plan.strat)} · ${planStatusLabel(plan.status)} · ${plan.risk_unit??'仓位上限未提供'}`),el('p',`数据时间 ${(plan.condition_as_of??plan.updated_at??plan.source_at)?.slice(11,19)??'未知'}；计划状态为当前持久状态，不是完整历史。`),stats,el('h3','闭合5分钟走势'),planChart(plan),el('p',plan.bar_source?.basis==='EXECUTION_AGGREGATED_5M_NOT_NATIVE'?`执行聚合5m / 非原生采集；冻结1m SHA ${plan.bar_source.snapshot_sha256}；截止 ${plan.bar_source.cutoff}。不是全天或原生48根证明。`:'监控原件中的闭合5m价格观测；不是原生48根或全天覆盖。','chart-legend'),el('p','价格线：浅灰白 · 计划区间：浅金 · 止损：红色虚线 · 目标：浅金点线 · 成交量：灰柱。仅绘已采到的闭合原件；时间缺口不连线，不代表全天完整覆盖。','chart-legend'),el('h3',`形态条件（原件 ${plan.conditions?.length??0} 项，不补造为12项）`));
  const conditions=el('ul',null,'drawer-conditions');
  for(const c of plan.conditions??[]){const item=el('li');item.append(el('strong',conditionLabel(c.code)),el('span',({MET:'满足',UNMET:'未满足',UNPROVEN:'证据不足'})[c.status]??'证据不足'));const fact=el('p',`阈值 ${JSON.stringify(c.threshold??'未提供')}；观测 ${JSON.stringify(c.value??'未提供')}；${c.at??'时刻未提供'}`);item.append(fact);conditions.append(item);}
  if(!conditions.children.length)conditions.append(el('li','未接入该计划的逐项原件，不能由价格或状态推断形态已满足。'));content.append(conditions);
  content.append(el('h3','事件时间线'));const timeline=el('ol');for(const item of plan.timeline??[]){const li=el('li',`${item.at?.slice(11,19)??'时间未知'} · ${(item.reason_codes??[]).map(reasonText).join('；')||'已有观测记录'}`);const proof=el('details');proof.append(el('summary','查看来源'),el('p',`记录 ${item.source_id??'未知'}；SHA ${item.sha256??'未知'}`));li.append(proof);timeline.append(li);}if(!timeline.children.length)timeline.append(el('li','缺少事件变迁原件，不由当前状态补历史。'));content.append(timeline,el('h3','仓位计算'));
  if(plan.sizing){const sizing=el('dl',null,'drawer-stats');for(const[label,key]of[['风险预算原股数','raw_risk_qty'],['风险预算整手股数','risk_round_lot_qty'],['单票20%上限股数','cap20_qty'],['PROBE10%上限股数','probe10_qty'],['最终股数','final_qty']]){const group=el('div');group.append(el('dt',label),el('dd',plan.sizing.raw_decimal_strings?.[key]??num(plan.sizing[key])));sizing.append(group);}content.append(sizing,el('p',`实际绑定项 ${(plan.sizing.binding_limits??[]).map(reasonText).join('；')||'原件未记录'}；原准入 ${plan.sizing.at??'时刻未知'}。不是从当前配置反推。`));const proof=el('details');proof.append(el('summary','仓位计算来源'),el('p',`记录 ${plan.sizing.source?.id??'未知'}；SHA ${plan.sizing.source?.sha256??'未知'}`));content.append(proof);}else content.append(el('p','尚未绑定该计划的原始风险预算、20%/10%上限与最终股数；不由标签倒推。'));
  content.append(el('h3','数据受限'),el('p',plan.data_limits?.data_limited===true?`数据受限；${(plan.data_limits.reason_codes??[]).map(reasonText).join('；')||'缺源明细未提供'}。A6显审：${plan.data_limits.a6_explicit_review_required===true?'要求逐项审查':'未记录'}`:plan.data_limits?.data_limited===false?'原计划记录未标数据受限。':'原计划未记录完整性标记，不代表数据齐全。'));
  const raw=el('details');raw.append(el('summary','原始原因码与来源'),el('pre',JSON.stringify({plan_id:plan.source_id,sha256:plan.source_sha256,updated_at:plan.source_at,reason_codes:plan.reason_codes??[],conditions:(plan.conditions??[]).map(c=>({code:c.code,explanation:reasonText(c.code),source_id:c.source_id,sha256:c.sha256}))},null,2)));content.append(raw);dialog.dataset.planId=plan.id??plan.source_id??'';dialog.replaceChildren(head,content);if(!dialog.open)dialog.showModal();
}
